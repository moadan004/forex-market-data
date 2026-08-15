"""The whole ingestion path, driven by the offline CSV provider.

    CSV provider -> DownloadPipeline -> chunk planner -> checkpoint/resume
      -> partition merge -> Parquet -> manifest -> validator -> quality report

Every stage below is the production one. Only the provider differs from a
live run, which is the point: the pipeline is not supposed to know or care
where candles come from.
"""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from marketdata.calendar import AlwaysOpenCalendar, ForexCalendar
from marketdata.cli import main
from marketdata.downloader.checkpoint import CheckpointStore, ChunkStatus
from marketdata.downloader.chunks import DurationChunkSize, MonthlyChunkSize
from marketdata.downloader.pipeline import DownloadPipeline
from marketdata.providers.csv import CsvMarketDataProvider, CsvProviderError
from marketdata.quality.dataset import validate_dataset
from marketdata.quality.report import QualityStatus
from marketdata.storage.parquet import ParquetStorage

FIXTURES = Path(__file__).parent / "fixtures" / "csv"

START = datetime(2026, 8, 10, 0, 0, tzinfo=UTC)
MINUTE = timedelta(minutes=1)
HOUR = timedelta(hours=1)
TWO_HOURS = START + HOUR * 2


class CountingCsvProvider(CsvMarketDataProvider):
    """The CSV provider, recording the window of every chunk it serves."""

    def __init__(self, source, **kwargs) -> None:
        super().__init__(source, **kwargs)
        self.windows: list[tuple[datetime, datetime]] = []

    def fetch_candles(self, symbol, start, end, timeframe="1min"):
        self.windows.append((start, end))
        return super().fetch_candles(symbol, start, end, timeframe)


class FlakyCsvProvider(CountingCsvProvider):
    """A CSV source that is briefly unreadable, to exercise chunk failure."""

    def __init__(self, source, *, failures: int = 0, **kwargs) -> None:
        super().__init__(source, **kwargs)
        self.remaining_failures = failures

    def fetch_candles(self, symbol, start, end, timeframe="1min"):
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            self.windows.append((start, end))
            raise CsvProviderError(f"{self.source} is temporarily unreadable")

        return super().fetch_candles(symbol, start, end, timeframe)


def build_pipeline(tmp_path, provider, *, chunk_size=None, **kwargs):
    return DownloadPipeline(
        provider,
        output_root=tmp_path / "processed",
        manifest_root=tmp_path / "manifests",
        quality_root=tmp_path / "quality",
        checkpoint_root=tmp_path / "checkpoints",
        chunk_size=chunk_size or DurationChunkSize(amount=30, unit="min"),
        calendar=AlwaysOpenCalendar(),
        **kwargs,
    )


def download(tmp_path, provider, *, start=START, end=TWO_HOURS, resume=True, **kwargs):
    pipeline = build_pipeline(tmp_path, provider, **kwargs)

    return pipeline.run(
        symbol="EUR/USD",
        start=start,
        end=end,
        timeframe="1min",
        resume=resume,
    )


def validate(tmp_path, **kwargs):
    kwargs.setdefault("calendar", AlwaysOpenCalendar())

    return validate_dataset(
        symbol="EUR/USD",
        timeframe="1min",
        data_root=tmp_path / "processed",
        manifest_root=tmp_path / "manifests",
        **kwargs,
    )


def stored_timestamps(tmp_path) -> list[datetime]:
    return ParquetStorage(tmp_path / "processed").read_timestamps(
        symbol="EUR/USD",
        timeframe="1min",
    )


def checkpoint_for(tmp_path, *, start=START, end=TWO_HOURS):
    return CheckpointStore(tmp_path / "checkpoints").load(
        symbol="EUR/USD",
        timeframe="1min",
        start=start,
        end=end,
    )


def source(name: str = "dataset") -> Path:
    return FIXTURES / name


# --------------------------------------------------------------------------
# Fresh download
# --------------------------------------------------------------------------


def test_a_fresh_download_stores_the_csv(tmp_path):
    result = download(tmp_path, CsvMarketDataProvider(source()))

    assert result.provider == "csv"
    assert result.downloaded_count == 120
    assert result.retained_count == 120
    assert result.chunks_failed == 0
    assert result.quality.status is QualityStatus.OK
    assert len(stored_timestamps(tmp_path)) == 120


def test_a_single_chunk_download(tmp_path):
    result = download(
        tmp_path,
        CsvMarketDataProvider(source()),
        end=START + HOUR,
        chunk_size=MonthlyChunkSize(),
    )

    assert result.chunks_total == 1
    assert result.retained_count == 60


def test_the_default_forex_calendar_sees_an_open_market(tmp_path):
    """The fixtures start on a Monday, so nothing is a closure."""
    pipeline = DownloadPipeline(
        CsvMarketDataProvider(source()),
        output_root=tmp_path / "processed",
        manifest_root=tmp_path / "manifests",
        quality_root=tmp_path / "quality",
        checkpoint_root=tmp_path / "checkpoints",
        chunk_size=DurationChunkSize(amount=1, unit="h"),
        calendar=ForexCalendar(),
    )

    result = pipeline.run(symbol="EUR/USD", start=START, end=TWO_HOURS)

    assert result.quality.market_closed_intervals == []
    assert result.quality.expected_rows == 120
    assert result.quality.status is QualityStatus.OK


# --------------------------------------------------------------------------
# Chunk planning
# --------------------------------------------------------------------------


def test_a_multi_chunk_download_asks_for_each_window_once(tmp_path):
    provider = CountingCsvProvider(source())

    result = download(tmp_path, provider)

    assert result.chunks_total == 4
    assert provider.windows == [
        (START + MINUTE * 30 * index, START + MINUTE * 30 * (index + 1))
        for index in range(4)
    ]
    assert result.retained_count == 120


def test_chunks_cover_the_range_without_overlap(tmp_path):
    provider = CountingCsvProvider(source())

    download(tmp_path, provider)

    timestamps = stored_timestamps(tmp_path)

    assert len(timestamps) == len(set(timestamps)) == 120
    assert timestamps[0] == START
    assert timestamps[-1] == TWO_HOURS - MINUTE


# --------------------------------------------------------------------------
# Checkpoint and resume
# --------------------------------------------------------------------------


def test_a_completed_run_is_checkpointed(tmp_path):
    download(tmp_path, CsvMarketDataProvider(source()))

    checkpoint = checkpoint_for(tmp_path)

    assert checkpoint.is_complete
    assert [record.status for record in checkpoint.chunks] == [
        ChunkStatus.COMPLETED
    ] * 4
    assert [record.row_count for record in checkpoint.chunks] == [30] * 4


def test_rerunning_fetches_nothing(tmp_path):
    download(tmp_path, CsvMarketDataProvider(source()))

    provider = CountingCsvProvider(source())
    result = download(tmp_path, provider)

    assert provider.windows == []
    assert result.chunks_skipped == 4
    assert result.downloaded_count == 0
    assert result.retained_count == 120
    assert result.quality.status is QualityStatus.OK


def test_a_partial_failure_is_resumed(tmp_path):
    # The source is unreadable for the first two chunks.
    failing = FlakyCsvProvider(source(), failures=2)

    first = download(tmp_path, failing)

    assert first.chunks_failed == 2
    assert first.chunks_completed == 2
    assert first.retained_count == 60
    assert first.quality.status is QualityStatus.FAILED

    recovered = CountingCsvProvider(source())
    second = download(tmp_path, recovered)

    assert second.chunks_skipped == 2
    assert recovered.windows == [
        (START, START + MINUTE * 30),
        (START + MINUTE * 30, START + HOUR),
    ]
    assert second.chunks_failed == 0
    assert second.retained_count == 120
    assert second.quality.status is QualityStatus.OK


def test_a_failed_chunk_records_a_permanent_error(tmp_path):
    download(tmp_path, FlakyCsvProvider(source(), failures=1))

    record = checkpoint_for(tmp_path).chunks[0]

    assert record.status is ChunkStatus.FAILED
    assert record.error_kind == "CsvProviderError"
    assert record.retryable is False
    assert record.retries == 0


def test_a_missing_source_fails_every_chunk_without_storing_anything(tmp_path):
    result = download(tmp_path, CsvMarketDataProvider(tmp_path / "absent"))

    assert result.chunks_failed == 4
    assert result.retained_count == 0
    assert result.files == []
    assert result.quality.status is QualityStatus.FAILED
    assert "CsvProviderError" in result.failures[0]


# --------------------------------------------------------------------------
# Merging, duplicates and range restriction
# --------------------------------------------------------------------------


def test_repeated_downloads_do_not_duplicate_candles(tmp_path):
    download(tmp_path, CsvMarketDataProvider(source()))
    download(tmp_path, CsvMarketDataProvider(source()), resume=False)
    download(tmp_path, CsvMarketDataProvider(source()), resume=False)

    timestamps = stored_timestamps(tmp_path)

    assert len(timestamps) == 120
    assert len(set(timestamps)) == 120
    assert validate(tmp_path).status is QualityStatus.OK


def test_a_download_spanning_two_months_writes_two_partitions(tmp_path):
    result = download(
        tmp_path,
        CsvMarketDataProvider(source("multi_month_1min.csv")),
        start=datetime(2026, 8, 31, 23, 0, tzinfo=UTC),
        end=datetime(2026, 9, 1, 1, 0, tzinfo=UTC),
        chunk_size=MonthlyChunkSize(),
    )

    assert result.chunks_total == 2
    assert result.retained_count == 4
    assert {path.parent.name for path in result.files} == {"month=08", "month=09"}

    report = validate_dataset(
        symbol="EUR/USD",
        data_root=tmp_path / "processed",
        manifest_root=tmp_path / "manifests",
        calendar=AlwaysOpenCalendar(),
    )

    assert report.files == 2
    assert report.candles == 4
    assert report.schema_consistent is True


def test_duplicates_in_the_source_are_removed_once(tmp_path):
    result = download(
        tmp_path,
        CsvMarketDataProvider(source("messy_1min.csv")),
        end=START + MINUTE * 10,
        chunk_size=MonthlyChunkSize(),
    )

    assert result.duplicate_count == 1
    assert stored_timestamps(tmp_path).count(START + MINUTE) == 1

    stored = ParquetStorage(tmp_path / "processed").read_candles(
        symbol="EUR/USD",
        timeframe="1min",
    )
    kept = next(candle for candle in stored if candle.timestamp == START + MINUTE)

    # Deduplication keeps the last occurrence, which is the later CSV row.
    assert kept.close == Decimal("1.17010000")


def test_invalid_rows_in_the_source_are_dropped_and_reported(tmp_path):
    result = download(
        tmp_path,
        CsvMarketDataProvider(source("messy_1min.csv")),
        end=START + MINUTE * 10,
        chunk_size=MonthlyChunkSize(),
    )

    assert result.invalid_count == 1
    assert result.quality.violations[0].reason == "high cannot be below low"
    assert START + MINUTE * 3 not in stored_timestamps(tmp_path)


def test_rows_outside_the_requested_range_never_reach_storage(tmp_path):
    """The provider filters, and the pipeline restricts again per chunk."""
    end = START + MINUTE * 10

    download(
        tmp_path,
        CsvMarketDataProvider(source("messy_1min.csv")),
        end=end,
        chunk_size=MonthlyChunkSize(),
    )

    assert all(START <= stamp < end for stamp in stored_timestamps(tmp_path))


# --------------------------------------------------------------------------
# Manifest, quality report and validation
# --------------------------------------------------------------------------


def test_the_manifest_describes_the_csv_run(tmp_path):
    result = download(tmp_path, CsvMarketDataProvider(source()))

    manifest = json.loads(result.manifest.read_text())

    assert manifest["provider"] == "csv"
    assert manifest["symbol"] == "EUR/USD"
    assert manifest["row_count"] == 120
    assert manifest["quality_status"] == "ok"
    assert len(manifest["files"]) == 1


def test_the_quality_report_describes_the_csv_run(tmp_path):
    result = download(tmp_path, CsvMarketDataProvider(source()))

    report = json.loads(result.quality_report_path.read_text())

    assert report["provider"] == "csv"
    assert report["downloaded_rows"] == 120
    assert report["retained_rows"] == 120
    assert report["expected_rows"] == 120
    assert report["missing_candles"] == 0
    assert report["chunks_total"] == 4
    assert report["chunks_completed"] == 4
    assert report["status"] == "ok"
    # A local file is neither throttled nor retried.
    assert report["rate_limit_requests_per_second"] is None
    assert report["provider_retries"] == 0


def test_the_validator_agrees_with_the_run(tmp_path):
    result = download(tmp_path, CsvMarketDataProvider(source()))

    report = validate(tmp_path)

    assert report.status is QualityStatus.OK
    assert report.candles == result.quality.retained_rows
    assert report.actual_start == result.quality.actual_start
    assert report.actual_end == result.quality.actual_end
    assert report.manifests[0].agrees is True
    assert report.problems == []


def test_a_gap_in_the_source_is_reported_not_filled(tmp_path):
    download(
        tmp_path,
        CsvMarketDataProvider(source("messy_1min.csv")),
        end=START + MINUTE * 10,
        chunk_size=MonthlyChunkSize(),
    )

    report = validate(tmp_path)

    assert report.status is QualityStatus.INCOMPLETE
    assert report.missing_candles > 0
    assert any(
        interval.start == START + MINUTE * 3 for interval in report.missing_intervals
    )


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def cli_argv(tmp_path, *extra, source_name="dataset"):
    return [
        "download",
        "--provider",
        "csv",
        "--source",
        str(source(source_name)),
        "--symbol",
        "EUR/USD",
        "--start",
        "2026-08-10T00:00:00Z",
        "--end",
        "2026-08-10T02:00:00Z",
        "--chunk-size",
        "30min",
        "--calendar",
        "24x7",
        "--data-root",
        str(tmp_path / "processed"),
        "--manifest-root",
        str(tmp_path / "manifests"),
        "--quality-root",
        str(tmp_path / "quality"),
        "--checkpoint-root",
        str(tmp_path / "checkpoints"),
        *extra,
    ]


def test_the_command_downloads_from_csv_offline(tmp_path, capsys):
    exit_code = main(cli_argv(tmp_path))

    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Provider:          csv" in output
    assert "Rows downloaded:   120" in output
    assert "Rows retained:     120" in output
    assert "Quality status:    ok" in output
    assert "Rate limit:        none" in output


def test_the_downloaded_dataset_validates_from_the_command_line(tmp_path, capsys):
    main(cli_argv(tmp_path))
    capsys.readouterr()

    exit_code = main(
        [
            "validate",
            "--symbol",
            "EUR/USD",
            "--calendar",
            "24x7",
            "--data-root",
            str(tmp_path / "processed"),
            "--manifest-root",
            str(tmp_path / "manifests"),
        ]
    )

    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Candles:           120" in output
    assert "Status:            ok" in output


def test_csv_requires_a_source(tmp_path, capsys):
    exit_code = main(
        [
            "download",
            "--provider",
            "csv",
            "--symbol",
            "EUR/USD",
            "--start",
            "2026-08-10T00:00:00Z",
            "--end",
            "2026-08-10T01:00:00Z",
        ]
    )

    assert exit_code == 1
    assert "--source is required when --provider csv" in capsys.readouterr().out


def test_an_unknown_provider_is_rejected(tmp_path):
    with pytest.raises(SystemExit):
        main(cli_argv(tmp_path, "--provider", "invented"))


def test_the_command_is_resumable(tmp_path, capsys):
    main(cli_argv(tmp_path))
    capsys.readouterr()

    exit_code = main(cli_argv(tmp_path))

    output = capsys.readouterr().out

    assert exit_code == 0
    assert "4 already done" in output
    assert "Rows downloaded:   0" in output
    assert "Rows retained:     120" in output
