"""Validating an already-stored dataset, without contacting a provider."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import ClassVar

import pyarrow as pa
import pyarrow.parquet as pq

from marketdata.calendar import AlwaysOpenCalendar, ForexCalendar
from marketdata.cli import main
from marketdata.models.candle import Candle
from marketdata.quality.dataset import schema_differences, validate_dataset
from marketdata.quality.report import QualityStatus
from marketdata.storage.manifest import create_manifest, write_manifest
from marketdata.storage.parquet import CANDLE_SCHEMA, ParquetStorage

# A Monday, so the forex calendar is open across the whole window.
START = datetime(2026, 8, 10, 0, 0, tzinfo=UTC)
MINUTE = timedelta(minutes=1)
HOUR = timedelta(hours=1)
END = START + HOUR


def make_candle(
    timestamp: datetime,
    *,
    close: str = "1.1705",
    high: str = "1.1710",
    low: str = "1.1690",
) -> Candle:
    return Candle(
        timestamp=timestamp,
        symbol="EUR/USD",
        open=Decimal("1.1700"),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal(100),
    )


def store(tmp_path, candles) -> ParquetStorage:
    storage = ParquetStorage(tmp_path / "processed")
    storage.write(candles, symbol="EUR/USD", timeframe="1min")
    return storage


def full_hour() -> list[Candle]:
    return [make_candle(START + MINUTE * index) for index in range(60)]


def validate(tmp_path, **kwargs):
    kwargs.setdefault("calendar", AlwaysOpenCalendar())
    kwargs.setdefault("manifest_root", tmp_path / "manifests")

    return validate_dataset(
        symbol="EUR/USD",
        timeframe="1min",
        data_root=tmp_path / "processed",
        **kwargs,
    )


def write_dataset_manifest(tmp_path, *, rows: int, start=START, end=END, files=None):
    storage = ParquetStorage(tmp_path / "processed")

    manifest = create_manifest(
        symbol="EUR/USD",
        timeframe="1min",
        candles_count=rows,
        start=start,
        end=end,
        provider="stub",
        files=(
            files
            if files is not None
            else storage.partition_files(symbol="EUR/USD", timeframe="1min")
        ),
        root=storage.root,
    )

    return write_manifest(
        manifest,
        tmp_path / "manifests" / "EUR_USD" / "1min_test.json",
    )


# --------------------------------------------------------------------------
# A healthy dataset
# --------------------------------------------------------------------------


def test_a_complete_dataset_validates(tmp_path):
    store(tmp_path, full_hour())

    report = validate(tmp_path, start=START, end=END)

    assert report.status is QualityStatus.OK
    assert report.ok is True
    assert report.candles == 60
    assert report.expected_candles == 60
    assert report.missing_candles == 0
    assert report.duplicate_candles == 0
    assert report.invalid_rows == 0
    assert report.out_of_range_rows == 0
    assert report.unordered_rows == 0
    assert report.problems == []


def test_the_report_describes_the_dataset(tmp_path):
    store(tmp_path, full_hour())

    report = validate(tmp_path, start=START, end=END)

    assert report.symbol == "EUR/USD"
    assert report.timeframe == "1min"
    assert report.calendar == "24x7"
    assert report.actual_start == START
    assert report.actual_end == END - MINUTE
    assert report.cadence_seconds == 60
    assert report.files == 1
    assert report.schema_consistent is True


def test_partition_coverage_is_reported(tmp_path):
    store(
        tmp_path,
        [
            make_candle(datetime(2026, 8, 31, 23, 59, tzinfo=UTC)),
            make_candle(datetime(2026, 9, 1, 0, 0, tzinfo=UTC)),
        ],
    )

    report = validate(tmp_path, manifest_root=None)

    assert report.files == 2
    assert [(part.year, part.month) for part in report.partitions] == [
        (2026, 8),
        (2026, 9),
    ]
    assert [part.rows for part in report.partitions] == [1, 1]
    assert all(part.schema_matches for part in report.partitions)


def test_an_empty_dataset_is_reported_as_empty(tmp_path):
    report = validate(tmp_path)

    assert report.status is QualityStatus.EMPTY
    assert report.candles == 0
    assert report.files == 0
    assert report.actual_start is None
    assert report.range_source == "empty"


# --------------------------------------------------------------------------
# Defects
# --------------------------------------------------------------------------


def test_missing_candles_are_detected(tmp_path):
    store(tmp_path, [make_candle(START + MINUTE * index) for index in range(10)])

    report = validate(tmp_path, start=START, end=END)

    assert report.status is QualityStatus.INCOMPLETE
    assert report.missing_candles == 50
    assert len(report.missing_intervals) == 1
    assert report.missing_intervals[0].start == START + MINUTE * 10
    assert "50 expected trading candles are missing" in report.problems


def test_invalid_ohlc_rows_are_detected(tmp_path):
    candles = full_hour()
    candles[3] = make_candle(START + MINUTE * 3, high="1.1600", low="1.1690")

    store(tmp_path, candles)

    report = validate(tmp_path, start=START, end=END)

    assert report.status is QualityStatus.INVALID
    assert report.invalid_rows == 1
    assert report.violations[0].reason == "high cannot be below low"
    assert "break an OHLC invariant" in report.problems[0]
    assert "2026-08-10T00:03:00Z" in report.problems[0]


def test_out_of_range_rows_are_detected(tmp_path):
    store(tmp_path, [*full_hour(), make_candle(END + MINUTE)])

    report = validate(tmp_path, start=START, end=END)

    assert report.status is QualityStatus.INVALID
    assert report.out_of_range_rows == 1
    assert report.candles == 60
    assert "1 rows fall outside the requested range" in report.problems


def test_duplicate_candles_are_detected(tmp_path):
    """A second file in a partition can reintroduce a timestamp."""
    storage = store(tmp_path, full_hour())

    partition = storage.partition_files(symbol="EUR/USD", timeframe="1min")[0]
    duplicate = partition.with_name("extra.parquet")
    pq.write_table(pq.read_table(partition), duplicate)

    report = validate(tmp_path, start=START, end=END, manifest_root=None)

    assert report.status is QualityStatus.INVALID
    assert report.duplicate_candles == 60
    assert report.candles == 120
    assert len(report.duplicate_timestamps) == 20
    assert "60 duplicate timestamps" in report.problems
    assert report.files == 2


def test_an_incompatible_schema_is_detected(tmp_path):
    """A partition written with inferred types cannot join the dataset."""
    storage = store(tmp_path, full_hour())

    directory = storage.dataset_path("EUR/USD", "1min") / "year=2026" / "month=09"
    directory.mkdir(parents=True)

    pq.write_table(
        pa.Table.from_pylist([{"timestamp": START, "close": Decimal("1.17")}]),
        directory / "candles.parquet",
    )

    report = validate(tmp_path, start=START, end=END, manifest_root=None)

    assert report.schema_consistent is False
    assert report.status is QualityStatus.INVALID
    assert any("expected" in problem for problem in report.problems)


def test_schema_differences_names_each_departure():
    other = pa.schema(
        [
            ("timestamp", pa.timestamp("us", tz="UTC")),
            ("symbol", pa.string()),
            ("open", pa.decimal128(5, 4)),
            ("high", pa.decimal128(18, 8)),
            ("low", pa.decimal128(18, 8)),
            ("close", pa.decimal128(18, 8)),
            ("extra", pa.string()),
        ]
    )

    differences = schema_differences(other)

    assert "open is decimal128(5, 4), expected decimal128(18, 8)" in differences
    assert "missing column volume" in differences
    assert "unexpected column extra" in differences
    assert schema_differences(CANDLE_SCHEMA) == []


# --------------------------------------------------------------------------
# Calendar awareness
# --------------------------------------------------------------------------


def test_a_weekend_is_not_reported_as_missing_data(tmp_path):
    friday_close = datetime(2026, 8, 14, 21, 0, tzinfo=UTC)
    sunday_open = datetime(2026, 8, 16, 22, 0, tzinfo=UTC)

    store(tmp_path, [])

    report = validate_dataset(
        symbol="EUR/USD",
        data_root=tmp_path / "processed",
        manifest_root=None,
        calendar=ForexCalendar(),
        start=friday_close,
        end=sunday_open,
    )

    assert report.expected_candles == 0
    assert report.missing_candles == 0
    assert len(report.market_closed_intervals) == 1
    assert report.market_closed_intervals[0].reason == "weekend"


def test_the_same_window_is_incomplete_without_calendar_awareness(tmp_path):
    friday_close = datetime(2026, 8, 14, 21, 0, tzinfo=UTC)
    sunday_open = datetime(2026, 8, 16, 22, 0, tzinfo=UTC)

    store(tmp_path, [make_candle(friday_close)])

    report = validate_dataset(
        symbol="EUR/USD",
        data_root=tmp_path / "processed",
        manifest_root=None,
        calendar=AlwaysOpenCalendar(),
        start=friday_close,
        end=sunday_open,
    )

    assert report.status is QualityStatus.INCOMPLETE
    assert report.missing_candles == 49 * 60 - 1


# --------------------------------------------------------------------------
# Range resolution
# --------------------------------------------------------------------------


def test_the_manifest_range_is_used_by_default(tmp_path):
    store(tmp_path, full_hour())
    write_dataset_manifest(tmp_path, rows=60)

    report = validate(tmp_path)

    assert report.range_source == "manifest"
    assert report.range_start == START
    assert report.range_end == END
    assert report.status is QualityStatus.OK


def test_an_explicit_range_wins_over_the_manifest(tmp_path):
    store(tmp_path, full_hour())
    write_dataset_manifest(tmp_path, rows=60)

    report = validate(tmp_path, start=START, end=START + MINUTE * 10)

    assert report.range_source == "requested"
    assert report.candles == 10
    assert report.out_of_range_rows == 50


def test_without_a_manifest_the_data_judges_itself(tmp_path):
    store(tmp_path, full_hour())

    report = validate(tmp_path, manifest_root=None)

    assert report.range_source == "data"
    assert report.range_start == START
    assert report.range_end == END
    assert report.status is QualityStatus.OK


def test_manifests_can_be_ignored(tmp_path):
    store(tmp_path, full_hour())
    write_dataset_manifest(tmp_path, rows=999)

    assert validate(tmp_path).status is QualityStatus.INVALID
    assert validate(tmp_path, manifest_root=None).status is QualityStatus.OK


# --------------------------------------------------------------------------
# Manifest agreement
# --------------------------------------------------------------------------


def test_a_manifest_that_agrees_is_reported(tmp_path):
    store(tmp_path, full_hour())
    path = write_dataset_manifest(tmp_path, rows=60)

    report = validate(tmp_path)

    assert len(report.manifests) == 1
    assert report.manifests[0].path == str(path)
    assert report.manifests[0].agrees is True
    assert report.manifests[0].claimed_rows == 60
    assert report.manifests[0].stored_rows == 60


def test_a_manifest_claiming_the_wrong_row_count_is_caught(tmp_path):
    store(tmp_path, full_hour())
    write_dataset_manifest(tmp_path, rows=100)

    report = validate(tmp_path)

    assert report.status is QualityStatus.INVALID
    assert report.manifests[0].agrees is False
    assert "claims 100 rows but 60 are stored" in report.problems[0]


def test_a_manifest_referencing_a_missing_file_is_caught(tmp_path):
    store(tmp_path, full_hour())
    write_dataset_manifest(
        tmp_path,
        rows=60,
        files=[tmp_path / "processed" / "EUR_USD" / "gone.parquet"],
    )

    report = validate(tmp_path)

    assert report.status is QualityStatus.INVALID
    assert report.manifests[0].missing_files
    assert "references 1 missing files" in report.problems[0]


def test_manifest_paths_survive_a_moved_dataset(tmp_path):
    """Paths are stored relative to the dataset root, so a copy still checks."""
    store(tmp_path, full_hour())
    write_dataset_manifest(tmp_path, rows=60)

    moved = tmp_path / "elsewhere"
    moved.mkdir()
    (tmp_path / "processed").rename(moved / "processed")
    (tmp_path / "manifests").rename(moved / "manifests")

    report = validate_dataset(
        symbol="EUR/USD",
        data_root=moved / "processed",
        manifest_root=moved / "manifests",
        calendar=AlwaysOpenCalendar(),
    )

    assert report.manifests[0].agrees is True
    assert report.status is QualityStatus.OK


def test_manifests_for_other_timeframes_are_ignored(tmp_path):
    store(tmp_path, full_hour())
    write_dataset_manifest(tmp_path, rows=60)

    other = create_manifest(
        symbol="EUR/USD",
        timeframe="1hour",
        candles_count=1,
        start=START,
        end=END,
        provider="stub",
        files=[],
    )
    write_manifest(other, tmp_path / "manifests" / "EUR_USD" / "1hour_test.json")

    report = validate(tmp_path)

    assert len(report.manifests) == 1
    assert report.manifests[0].requested_start == START


def test_an_unreadable_manifest_is_skipped(tmp_path):
    store(tmp_path, full_hour())
    write_dataset_manifest(tmp_path, rows=60)

    broken = tmp_path / "manifests" / "EUR_USD" / "broken.json"
    broken.write_text("{not json")

    report = validate(tmp_path)

    assert len(report.manifests) == 1
    assert report.status is QualityStatus.OK


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def validate_argv(tmp_path, *extra):
    return [
        "validate",
        "--symbol",
        "EUR/USD",
        "--calendar",
        "24x7",
        "--data-root",
        str(tmp_path / "processed"),
        "--manifest-root",
        str(tmp_path / "manifests"),
        *extra,
    ]


def test_the_command_reports_a_healthy_dataset(tmp_path, capsys):
    store(tmp_path, full_hour())
    write_dataset_manifest(tmp_path, rows=60)

    exit_code = main(validate_argv(tmp_path))

    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Symbol:            EUR/USD" in output
    assert "Candles:           60" in output
    assert "Expected candles:  60" in output
    assert "Missing candles:   0" in output
    assert "Duplicate candles: 0" in output
    assert "Invalid OHLC rows: 0" in output
    assert "Parquet files:     1" in output
    assert "Schema consistent: yes" in output
    assert "agrees (60 claimed, 60 stored)" in output
    assert "Status:            ok" in output


def test_the_command_exits_non_zero_on_a_defect(tmp_path, capsys):
    candles = full_hour()
    candles[0] = make_candle(START, high="1.1000", low="1.1690")
    store(tmp_path, candles)

    exit_code = main(validate_argv(tmp_path))

    assert exit_code == 1
    assert "Status:            invalid" in capsys.readouterr().out


def test_incomplete_can_be_tolerated(tmp_path, capsys):
    store(tmp_path, [make_candle(START + MINUTE * index) for index in range(10)])

    strict = main(
        validate_argv(
            tmp_path, "--start", "2026-08-10T00:00:00Z", "--end", "2026-08-10T01:00:00Z"
        )
    )
    capsys.readouterr()
    tolerant = main(
        validate_argv(
            tmp_path,
            "--start",
            "2026-08-10T00:00:00Z",
            "--end",
            "2026-08-10T01:00:00Z",
            "--allow-incomplete",
        )
    )

    assert strict == 1
    assert tolerant == 0
    assert "Status:            incomplete" in capsys.readouterr().out


def test_allow_incomplete_does_not_hide_a_structural_defect(tmp_path):
    candles = full_hour()
    candles[0] = make_candle(START, high="1.1000", low="1.1690")
    store(tmp_path, candles)

    assert main(validate_argv(tmp_path, "--allow-incomplete")) == 1


def test_the_command_can_emit_json(tmp_path, capsys):
    store(tmp_path, full_hour())
    write_dataset_manifest(tmp_path, rows=60)

    exit_code = main(validate_argv(tmp_path, "--json"))

    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["symbol"] == "EUR/USD"
    assert payload["timeframe"] == "1min"
    assert payload["candles"] == 60
    assert payload["expected_candles"] == 60
    assert payload["status"] == "ok"
    assert payload["files"] == 1
    assert payload["partitions"][0]["rows"] == 60
    assert payload["manifests"][0]["agrees"] is True
    assert payload["problems"] == []


def test_the_command_lists_gaps(tmp_path, capsys):
    store(tmp_path, [make_candle(START), make_candle(START + MINUTE * 30)])

    main(
        validate_argv(
            tmp_path,
            "--start",
            "2026-08-10T00:00:00Z",
            "--end",
            "2026-08-10T01:00:00Z",
        )
    )

    output = capsys.readouterr().out

    assert "gap 2026-08-10T00:01:00Z -> 2026-08-10T00:30:00Z (29 candles)" in output


def test_manifests_can_be_ignored_from_the_command_line(tmp_path, capsys):
    store(tmp_path, full_hour())
    write_dataset_manifest(tmp_path, rows=999)

    assert main(validate_argv(tmp_path)) == 1
    capsys.readouterr()
    assert main(validate_argv(tmp_path, "--no-manifests")) == 0


def test_validating_a_symbol_that_was_never_downloaded(tmp_path, capsys):
    exit_code = main(validate_argv(tmp_path))

    assert exit_code == 1
    assert "Status:            empty" in capsys.readouterr().out


def test_an_unreadable_dataset_is_reported_not_raised(tmp_path):
    """A validator that dies on a broken dataset is no use."""
    storage = store(tmp_path, full_hour())

    directory = storage.dataset_path("EUR/USD", "1min") / "year=2026" / "month=09"
    directory.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([{"timestamp": START, "close": Decimal("1.17")}]),
        directory / "candles.parquet",
    )

    report = validate(tmp_path, manifest_root=None)

    assert report.readable is False
    assert report.read_error is not None
    assert report.status is QualityStatus.INVALID
    assert any("could not be read" in problem for problem in report.problems)


def test_a_truncated_partition_is_reported(tmp_path):
    storage = store(tmp_path, full_hour())
    partition = storage.partition_files(symbol="EUR/USD", timeframe="1min")[0]
    partition.write_bytes(b"not parquet at all")

    report = validate(tmp_path, manifest_root=None)

    assert report.readable is False
    assert report.status is QualityStatus.INVALID


# --------------------------------------------------------------------------
# Validating what the pipeline actually produced
# --------------------------------------------------------------------------


def build_pipeline(tmp_path, api, **kwargs):
    import httpx
    from conftest import instant_limiter

    from marketdata.downloader.chunks import DurationChunkSize
    from marketdata.downloader.pipeline import DownloadPipeline
    from marketdata.providers.dukascopy import DukascopyProvider
    from marketdata.providers.retry import RetryPolicy

    provider = DukascopyProvider(
        client=httpx.Client(transport=httpx.MockTransport(api.handler)),
        retry_policy=RetryPolicy(attempts=2, backoff_seconds=0),
        rate_limiter=instant_limiter(),
    )

    return DownloadPipeline(
        provider,
        output_root=tmp_path / "processed",
        manifest_root=tmp_path / "manifests",
        quality_root=tmp_path / "quality",
        checkpoint_root=tmp_path / "checkpoints",
        chunk_size=DurationChunkSize(amount=1, unit="h"),
        calendar=AlwaysOpenCalendar(),
        **kwargs,
    )


class HourlyApi:
    """Serves two hours of one-minute candles, optionally failing at first."""

    instruments: ClassVar[list[dict]] = [{"id": 1, "name": "EUR/USD"}]

    def __init__(self, *, failures: int = 0) -> None:
        self.remaining_failures = failures

    def handler(self, request):
        import httpx

        params = request.url.params

        if params["path"] == "api/instrumentList":
            return httpx.Response(200, json=self.instruments)

        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            return httpx.Response(503)

        start_ms = int(params["start"])
        end_ms = int(params["end"])

        candles = []
        moment = START

        while moment < START + HOUR * 2:
            stamp = int(moment.timestamp() * 1000)

            if start_ms <= stamp <= end_ms:
                candles.append(
                    {
                        "timestamp": stamp,
                        "bid_open": 1.17,
                        "bid_high": 1.1710,
                        "bid_low": 1.1690,
                        "bid_close": 1.1705,
                    }
                )

            moment += MINUTE

        return httpx.Response(200, json={"candles": candles})


def download(tmp_path, api, *, resume=True, **kwargs):
    return build_pipeline(tmp_path, api, **kwargs).run(
        symbol="EUR/USD",
        start=START,
        end=START + HOUR * 2,
        resume=resume,
    )


def test_a_freshly_downloaded_dataset_validates(tmp_path):
    download(tmp_path, HourlyApi())

    report = validate(tmp_path)

    assert report.status is QualityStatus.OK
    assert report.candles == 120
    assert report.manifests[0].agrees is True
    assert report.problems == []


def test_a_resumed_run_leaves_a_consistent_manifest(tmp_path):
    """The manifest of a run that downloaded nothing still describes the data."""
    download(tmp_path, HourlyApi())
    result = download(tmp_path, HourlyApi())

    assert result.chunks_skipped == 2
    assert result.files_written == []

    manifest = json.loads(result.manifest.read_text())

    assert manifest["row_count"] == 120
    assert len(manifest["files"]) == 1

    report = validate(tmp_path)

    assert report.status is QualityStatus.OK
    assert report.manifests[0].agrees is True


def test_a_dataset_completed_by_a_resumed_run_validates(tmp_path):
    # The first run loses its second chunk to an exhausted retry.
    first = download(tmp_path, HourlyApi(failures=10))

    assert first.chunks_failed > 0

    partial = validate(tmp_path)

    assert partial.status is not QualityStatus.OK

    # A later run finishes the job, and the dataset becomes whole.
    download(tmp_path, HourlyApi())

    report = validate(tmp_path)

    assert report.status is QualityStatus.OK
    assert report.candles == 120
    assert report.duplicate_candles == 0
    assert report.manifests[0].agrees is True


def test_repeated_downloads_do_not_duplicate_candles(tmp_path):
    download(tmp_path, HourlyApi())
    download(tmp_path, HourlyApi(), resume=False)
    download(tmp_path, HourlyApi(), resume=False)

    report = validate(tmp_path)

    assert report.candles == 120
    assert report.duplicate_candles == 0
    assert report.status is QualityStatus.OK


def test_the_quality_report_and_the_validator_agree(tmp_path):
    result = download(tmp_path, HourlyApi())

    report = validate(tmp_path)

    assert report.candles == result.quality.retained_rows
    assert report.actual_start == result.quality.actual_start
    assert report.actual_end == result.quality.actual_end
    assert report.missing_candles == result.quality.missing_candles
    assert report.status is result.quality.status
