"""
Dataset validation is bounded by partition size, not dataset size.

The target dataset is five to seven years of one-minute candles — several
million rows — so a validator that materializes the range it is judging
cannot be run on the dataset it exists for. These tests hold the streaming
design to two promises: that it never loads the whole dataset, and that it
still reports exactly what the whole-list implementation reported.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from marketdata.calendar import AlwaysOpenCalendar, ForexCalendar
from marketdata.models.candle import Candle
from marketdata.models.timeframe import timeframe_cadence
from marketdata.quality.dataset import dataset_bounds, validate_dataset
from marketdata.quality.gaps import (
    TradingGapScanner,
    find_missing_trading_intervals,
)
from marketdata.quality.report import MAX_VIOLATION_SAMPLES, QualityStatus
from marketdata.storage.parquet import ParquetStorage, candles_to_table
from marketdata.validation.candles import (
    duplicate_timestamps,
    partition_candles,
    restrict_to_range,
)

SYMBOL = "EUR/USD"
TIMEFRAME = "1min"
MINUTE = timedelta(minutes=1)

# A Monday, so a short run sits inside an open forex session.
MONDAY = datetime(2026, 8, 10, tzinfo=UTC)


def candle(moment: datetime, *, high: str = "1.1710", low: str = "1.1690") -> Candle:
    return Candle(
        timestamp=moment,
        symbol=SYMBOL,
        open=Decimal("1.1700"),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal("1.1705"),
        volume=Decimal(100),
    )


def series(count: int, *, start: datetime = MONDAY) -> list[Candle]:
    return [candle(start + MINUTE * index) for index in range(count)]


def store(root: Path, candles: list[Candle]) -> ParquetStorage:
    storage = ParquetStorage(root)
    storage.write(candles, symbol=SYMBOL, timeframe=TIMEFRAME)

    return storage


def validate(root: Path, **kwargs):
    kwargs.setdefault("calendar", AlwaysOpenCalendar())
    kwargs.setdefault("manifest_root", None)

    return validate_dataset(
        symbol=SYMBOL,
        timeframe=TIMEFRAME,
        data_root=root,
        **kwargs,
    )


def months(count: int, *, per_month: int = 120, year: int = 2024) -> list[Candle]:
    """Candles at the start of each of ``count`` consecutive months."""
    candles: list[Candle] = []

    for index in range(count):
        start = datetime(year + index // 12, index % 12 + 1, 2, tzinfo=UTC)
        candles.extend(series(per_month, start=start))

    return candles


# --------------------------------------------------------------------------
# The whole dataset is never resident
# --------------------------------------------------------------------------


class Reads:
    """Records every partition read, and how much each one made resident."""

    def __init__(self) -> None:
        self.paths: list[Path] = []
        self.sizes: list[int] = []

    def install(self, monkeypatch) -> None:
        original = ParquetStorage.read_partition

        def spy(storage, path, **kwargs):
            candles = original(storage, path, **kwargs)
            self.paths.append(Path(path))
            self.sizes.append(len(candles))

            return candles

        monkeypatch.setattr(ParquetStorage, "read_partition", spy)

        def refuse(*args, **kwargs):
            raise AssertionError("validation must never read the whole range at once")

        monkeypatch.setattr(ParquetStorage, "read_candles", refuse)
        monkeypatch.setattr(ParquetStorage, "read_table", refuse)
        monkeypatch.setattr(ParquetStorage, "read_timestamps", refuse)


@pytest.fixture
def reads(monkeypatch) -> Reads:
    recorder = Reads()
    recorder.install(monkeypatch)

    return recorder


class TestBoundedReads:
    def test_no_whole_range_read_is_used(self, tmp_path, reads):
        store(tmp_path, months(12))

        report = validate(tmp_path)

        # Reaching here at all proves it: read_candles, read_table and
        # read_timestamps all raise if validation reaches for them.
        assert report.candles == 12 * 120

    def test_partitions_are_read_one_at_a_time(self, tmp_path, reads):
        store(tmp_path, months(12))

        validate(tmp_path)

        assert len(reads.paths) == 12
        assert len(set(reads.paths)) == 12

    def test_partitions_are_read_in_chronological_order(self, tmp_path, reads):
        store(tmp_path, months(14))

        validate(tmp_path)

        assert reads.paths == sorted(reads.paths)

    def test_the_resident_working_set_is_one_partition(self, tmp_path, reads):
        store(tmp_path, months(24, per_month=200))

        report = validate(tmp_path)

        # Every read holds one month; the dataset holds two years of them.
        assert max(reads.sizes) == 200
        assert report.candles == 24 * 200
        assert sum(reads.sizes) == report.candles

    def test_the_working_set_does_not_grow_with_the_dataset(self, tmp_path, reads):
        # The point of the design stated as a measurement: doubling the
        # dataset doubles the number of reads, not the size of one.
        store(tmp_path / "short", months(6, per_month=150))
        store(tmp_path / "long", months(12, per_month=150))

        validate(tmp_path / "short")
        short = max(reads.sizes)

        reads.sizes.clear()
        reads.paths.clear()

        validate(tmp_path / "long")

        assert max(reads.sizes) == short
        assert len(reads.paths) == 12

    def test_the_range_is_resolved_without_reading_any_candles(self, tmp_path, reads):
        store(tmp_path, months(6))

        low, high = dataset_bounds(
            ParquetStorage(tmp_path),
            symbol=SYMBOL,
            timeframe=TIMEFRAME,
        )

        assert low == datetime(2024, 1, 2, tzinfo=UTC)
        assert high == datetime(2024, 6, 2, tzinfo=UTC) + MINUTE * 119
        assert reads.paths == []


class TestLargeDataset:
    """
    A dataset big enough that the difference is the point.

    Sixty thousand candles across twelve partitions is far short of the
    seven-year target, but it is large enough that holding one partition and
    holding the dataset differ by an order of magnitude — and the assertion
    is on the ratio, which is what actually has to hold at seven years.
    """

    def test_peak_residency_is_a_twelfth_of_the_dataset(self, tmp_path, reads):
        store(tmp_path, months(12, per_month=5_000))

        report = validate(tmp_path)

        assert report.candles == 60_000
        assert max(reads.sizes) == 5_000
        assert max(reads.sizes) * 12 == report.candles

        # Correct as well as bounded: each month holds an unbroken run, so
        # every gap found is a month boundary and nothing else.
        assert report.status is QualityStatus.INCOMPLETE
        assert report.missing_interval_count == 11
        assert report.duplicate_candles == 0
        assert report.invalid_rows == 0
        assert report.misfiled_rows == 0


class TestStreamedQualityReport:
    def test_the_report_accepts_a_generator(self, tmp_path):
        from marketdata.quality.report import build_quality_report

        store(tmp_path, series(60))
        storage = ParquetStorage(tmp_path)

        report = build_quality_report(
            provider="stub",
            symbol=SYMBOL,
            timeframe=TIMEFRAME,
            calendar=AlwaysOpenCalendar(),
            requested_start=MONDAY,
            requested_end=MONDAY + MINUTE * 60,
            timestamps=storage.iter_timestamps(
                symbol=SYMBOL,
                timeframe=TIMEFRAME,
                start=MONDAY,
                end=MONDAY + MINUTE * 60,
            ),
            downloaded_rows=60,
            duplicates_removed=0,
            out_of_range_rows=0,
            violations=[],
        )

        assert report.retained_rows == 60
        assert report.actual_start == MONDAY
        assert report.actual_end == MONDAY + MINUTE * 59
        assert report.status is QualityStatus.OK

    def test_a_generator_and_a_list_agree(self, tmp_path):
        from marketdata.quality.report import build_quality_report

        store(tmp_path, [item for item in series(60) if item.timestamp.minute != 30])
        storage = ParquetStorage(tmp_path)

        shared = {
            "provider": "stub",
            "symbol": SYMBOL,
            "timeframe": TIMEFRAME,
            "calendar": AlwaysOpenCalendar(),
            "requested_start": MONDAY,
            "requested_end": MONDAY + MINUTE * 60,
            "downloaded_rows": 59,
            "duplicates_removed": 0,
            "out_of_range_rows": 0,
            "violations": [],
        }
        window = {
            "symbol": SYMBOL,
            "timeframe": TIMEFRAME,
            "start": MONDAY,
            "end": MONDAY + MINUTE * 60,
        }

        streamed = build_quality_report(
            timestamps=storage.iter_timestamps(**window), **shared
        )
        listed = build_quality_report(
            timestamps=storage.read_timestamps(**window), **shared
        )

        assert streamed.model_dump(exclude={"generated_at"}) == listed.model_dump(
            exclude={"generated_at"}
        )

    def test_the_pipeline_report_is_built_from_a_generator(self, tmp_path):
        # Guards the wiring: the pipeline must not quietly go back to
        # materializing every timestamp in the range.
        import inspect

        from marketdata.downloader.pipeline import DownloadPipeline

        source = inspect.getsource(DownloadPipeline._finish)

        assert "iter_timestamps" in source
        assert "read_timestamps" not in source


# --------------------------------------------------------------------------
# Bounded samples
# --------------------------------------------------------------------------


class TestBoundedSamples:
    def test_gap_samples_are_capped_but_the_count_is_exact(self, tmp_path):
        # Every other minute stored: one gap per kept candle.
        kept = [candle(MONDAY + MINUTE * index) for index in range(0, 200, 2)]
        store(tmp_path, kept)

        report = validate(
            tmp_path,
            start=MONDAY,
            end=MONDAY + MINUTE * 200,
        )

        assert report.missing_interval_count == 100
        assert len(report.missing_intervals) == MAX_VIOLATION_SAMPLES
        assert report.missing_intervals_truncated is True
        assert report.missing_candles == 100
        assert report.status is QualityStatus.INCOMPLETE

    def test_violation_samples_are_capped_but_the_count_is_exact(self, tmp_path):
        broken = [
            candle(MONDAY + MINUTE * index, high="1.1600")
            for index in range(MAX_VIOLATION_SAMPLES * 3)
        ]
        store(tmp_path, broken)

        report = validate(tmp_path, start=MONDAY, end=MONDAY + MINUTE * 60)

        assert report.invalid_rows == MAX_VIOLATION_SAMPLES * 3
        assert len(report.violations) == MAX_VIOLATION_SAMPLES
        assert report.violations_truncated is True

    def test_duplicate_samples_are_capped_but_the_count_is_exact(self, tmp_path):
        candles = series(60)
        storage = store(tmp_path, candles)
        path = storage.partition_files(symbol=SYMBOL, timeframe=TIMEFRAME)[0]

        # Written straight to Parquet: the storage layer would merge these.
        pq.write_table(candles_to_table(candles + candles), path)

        report = validate(tmp_path, start=MONDAY, end=MONDAY + MINUTE * 60)

        assert report.duplicate_candles == 60
        assert len(report.duplicate_timestamps) == MAX_VIOLATION_SAMPLES
        assert report.duplicate_timestamps_truncated is True


# --------------------------------------------------------------------------
# Correctness across partition boundaries
# --------------------------------------------------------------------------


class TestCrossPartitionState:
    def test_a_single_partition_validates(self, tmp_path):
        store(tmp_path, series(60))

        report = validate(tmp_path, start=MONDAY, end=MONDAY + MINUTE * 60)

        assert report.status is QualityStatus.OK
        assert report.files == 1
        assert report.candles == 60

    def test_multiple_partitions_validate_as_one_dataset(self, tmp_path):
        august = series(60, start=datetime(2026, 8, 31, 23, 0, tzinfo=UTC))
        september = series(60, start=datetime(2026, 9, 1, 0, 0, tzinfo=UTC))
        store(tmp_path, august + september)

        report = validate(
            tmp_path,
            start=datetime(2026, 8, 31, 23, 0, tzinfo=UTC),
            end=datetime(2026, 9, 1, 1, 0, tzinfo=UTC),
        )

        assert report.files == 2
        assert report.candles == 120
        assert report.status is QualityStatus.OK
        assert report.missing_interval_count == 0

    def test_a_gap_inside_one_partition_is_found(self, tmp_path):
        candles = [item for item in series(60) if item.timestamp.minute not in (30, 31)]
        store(tmp_path, candles)

        report = validate(tmp_path, start=MONDAY, end=MONDAY + MINUTE * 60)

        assert report.missing_interval_count == 1
        assert report.missing_candles == 2
        assert report.missing_intervals[0].start == MONDAY + MINUTE * 30

    def test_a_gap_spanning_two_partitions_is_found(self, tmp_path):
        # August stops at 23:29, September resumes at 00:30: the gap exists
        # only in the seam between two files.
        august = series(30, start=datetime(2026, 8, 31, 23, 0, tzinfo=UTC))
        september = series(30, start=datetime(2026, 9, 1, 0, 30, tzinfo=UTC))
        store(tmp_path, august + september)

        report = validate(
            tmp_path,
            start=datetime(2026, 8, 31, 23, 0, tzinfo=UTC),
            end=datetime(2026, 9, 1, 1, 0, tzinfo=UTC),
        )

        assert report.files == 2
        assert report.missing_interval_count == 1
        assert report.missing_candles == 60
        assert report.missing_intervals[0].start == datetime(
            2026, 8, 31, 23, 30, tzinfo=UTC
        )
        assert report.missing_intervals[0].end == datetime(
            2026, 9, 1, 0, 30, tzinfo=UTC
        )

    def test_gap_detection_is_not_reset_at_each_partition(self, tmp_path):
        # One candle per month: eleven month-long gaps, none of which lives
        # inside a single partition.
        store(tmp_path, months(12, per_month=1))

        report = validate(
            tmp_path,
            start=datetime(2024, 1, 2, tzinfo=UTC),
            end=datetime(2024, 12, 2, 0, 1, tzinfo=UTC),
        )

        assert report.files == 12
        assert report.missing_interval_count == 11
        assert report.candles == 12

    def test_an_empty_month_between_two_full_ones_is_missing(self, tmp_path):
        store(
            tmp_path,
            series(60, start=datetime(2026, 8, 2, tzinfo=UTC))
            + series(60, start=datetime(2026, 10, 2, tzinfo=UTC)),
        )

        report = validate(
            tmp_path,
            start=datetime(2026, 8, 2, tzinfo=UTC),
            end=datetime(2026, 10, 2, 1, 0, tzinfo=UTC),
        )

        assert report.files == 2
        assert report.missing_interval_count == 1
        assert report.candles == 120

    def test_a_calendar_closure_spanning_partitions_is_not_a_gap(self, tmp_path):
        # The forex week closes on Friday evening and reopens on Sunday; here
        # that closure also happens to straddle a month boundary.
        friday = datetime(2026, 7, 31, 20, 0, tzinfo=UTC)
        sunday = datetime(2026, 8, 2, 22, 0, tzinfo=UTC)

        store(tmp_path, series(60, start=friday) + series(60, start=sunday))

        report = validate(
            tmp_path,
            calendar=ForexCalendar(),
            start=friday,
            end=sunday + MINUTE * 60,
        )

        assert report.files == 2
        assert report.missing_interval_count == 0
        assert report.status is QualityStatus.OK
        assert report.market_closed_intervals


class TestDuplicatesAndOrder:
    def test_duplicates_inside_one_partition_are_counted(self, tmp_path):
        candles = series(30)
        storage = store(tmp_path, candles)
        path = storage.partition_files(symbol=SYMBOL, timeframe=TIMEFRAME)[0]
        pq.write_table(candles_to_table([*candles, candles[0], candles[1]]), path)

        report = validate(tmp_path, start=MONDAY, end=MONDAY + MINUTE * 30)

        assert report.duplicate_candles == 2
        assert report.status is QualityStatus.INVALID

    def test_duplicates_across_two_files_of_one_partition_are_counted(self, tmp_path):
        candles = series(30)
        storage = store(tmp_path, candles)
        directory = storage.partition_files(symbol=SYMBOL, timeframe=TIMEFRAME)[
            0
        ].parent

        # A second file inside the same month: both belong to the partition,
        # so the duplicate is inside one bounded batch.
        pq.write_table(candles_to_table(candles[:5]), directory / "extra.parquet")

        report = validate(tmp_path, start=MONDAY, end=MONDAY + MINUTE * 30)

        assert report.files == 2
        assert report.duplicate_candles == 5
        assert report.status is QualityStatus.INVALID

    def test_a_misfiled_row_is_reported(self, tmp_path):
        storage = store(tmp_path, series(30))
        directory = storage.dataset_path(SYMBOL, TIMEFRAME) / "year=2026" / "month=09"
        directory.mkdir(parents=True)

        # An August candle stored under September.
        pq.write_table(
            candles_to_table([candle(MONDAY)]), directory / "candles.parquet"
        )

        report = validate(tmp_path, start=MONDAY, end=MONDAY + MINUTE * 30)

        assert report.misfiled_rows == 1
        assert report.status is QualityStatus.INVALID
        assert any("another month" in problem for problem in report.problems)

    def test_a_duplicate_spanning_partitions_is_still_counted(self, tmp_path):
        # The one way a timestamp can be in two partitions: a misfiled row.
        # It must not be waved through on the strength of the invariant it
        # is itself breaking.
        storage = store(tmp_path, series(30))
        directory = storage.dataset_path(SYMBOL, TIMEFRAME) / "year=2026" / "month=09"
        directory.mkdir(parents=True)
        pq.write_table(
            candles_to_table([candle(MONDAY)]), directory / "candles.parquet"
        )

        report = validate(tmp_path, start=MONDAY, end=MONDAY + MINUTE * 30)

        assert report.misfiled_rows == 1
        assert report.duplicate_candles == 1
        assert report.duplicate_timestamps == [MONDAY]

    def test_partitions_that_regress_in_time_are_reported_as_unordered(self, tmp_path):
        storage = store(tmp_path, series(30, start=datetime(2026, 9, 1, tzinfo=UTC)))
        directory = storage.dataset_path(SYMBOL, TIMEFRAME) / "year=2026" / "month=08"
        directory.mkdir(parents=True)

        # September rows stored under August: reading the partitions in order
        # then yields timestamps that go backwards at the seam.
        pq.write_table(
            candles_to_table(series(5, start=datetime(2026, 9, 1, 1, 0, tzinfo=UTC))),
            directory / "candles.parquet",
        )

        report = validate(
            tmp_path,
            start=datetime(2026, 9, 1, tzinfo=UTC),
            end=datetime(2026, 9, 1, 2, 0, tzinfo=UTC),
        )

        assert report.misfiled_rows == 5
        assert report.unordered_rows == 1
        assert report.status is QualityStatus.INVALID


# --------------------------------------------------------------------------
# Damaged and empty datasets keep their existing semantics
# --------------------------------------------------------------------------


class TestDamagedDatasets:
    def test_a_corrupt_partition_is_a_finding_not_a_crash(self, tmp_path):
        storage = store(tmp_path, months(3))
        path = storage.partition_files(symbol=SYMBOL, timeframe=TIMEFRAME)[1]
        path.write_bytes(b"not parquet at all")

        report = validate(tmp_path)

        assert report.readable is False
        assert report.read_error is not None
        assert report.status is QualityStatus.INVALID
        assert any("could not be read" in problem for problem in report.problems)

    def test_the_readable_partitions_are_still_described(self, tmp_path):
        storage = store(tmp_path, months(3))
        path = storage.partition_files(symbol=SYMBOL, timeframe=TIMEFRAME)[1]
        path.write_bytes(b"not parquet at all")

        report = validate(tmp_path)

        # A damaged file costs the dataset that file, not the whole scan.
        assert report.candles == 240
        assert report.files == 3

    def test_an_incompatible_schema_is_reported(self, tmp_path):
        storage = store(tmp_path, series(30))
        directory = storage.dataset_path(SYMBOL, TIMEFRAME) / "year=2026" / "month=09"
        directory.mkdir(parents=True)
        pq.write_table(
            pa.Table.from_pylist([{"timestamp": MONDAY, "close": Decimal("1.17")}]),
            directory / "candles.parquet",
        )

        report = validate(tmp_path)

        assert report.schema_consistent is False
        assert report.status is QualityStatus.INVALID

    def test_a_missing_partition_leaves_a_gap(self, tmp_path):
        storage = store(tmp_path, months(3, per_month=60))
        storage.partition_files(symbol=SYMBOL, timeframe=TIMEFRAME)[1].unlink()

        report = validate(
            tmp_path,
            start=datetime(2024, 1, 2, tzinfo=UTC),
            end=datetime(2024, 3, 2, 1, 0, tzinfo=UTC),
        )

        assert report.files == 2
        assert report.candles == 120
        assert report.missing_interval_count == 1
        assert report.status is QualityStatus.INCOMPLETE

    def test_an_empty_dataset_is_empty(self, tmp_path):
        report = validate(tmp_path)

        assert report.status is QualityStatus.EMPTY
        assert report.candles == 0
        assert report.range_source == "empty"

    def test_a_fully_closed_window_expects_nothing(self, tmp_path):
        # Saturday: the forex market is closed for the whole window.
        saturday = datetime(2026, 8, 8, tzinfo=UTC)

        store(tmp_path, series(1, start=MONDAY))

        report = validate(
            tmp_path,
            calendar=ForexCalendar(),
            start=saturday,
            end=saturday + timedelta(hours=6),
        )

        assert report.expected_candles == 0
        assert report.missing_interval_count == 0
        assert report.candles == 0
        assert report.status is QualityStatus.INVALID
        assert report.out_of_range_rows == 1

    def test_an_incomplete_dataset_never_looks_healthy(self, tmp_path):
        store(tmp_path, series(30))

        report = validate(tmp_path, start=MONDAY, end=MONDAY + MINUTE * 60)

        assert report.status is QualityStatus.INCOMPLETE
        assert report.missing_candles == 30


# --------------------------------------------------------------------------
# The streaming report equals the whole-list one
# --------------------------------------------------------------------------


def reference_report(root: Path, *, calendar, start=None, end=None) -> dict:
    """
    Recompute the report the way it was computed before streaming.

    Deliberately naive: read the whole dataset, restrict it, and run the
    whole-list helpers over it. If the streaming scan and this disagree, the
    streaming scan is wrong.
    """
    storage = ParquetStorage(root)
    stored = storage.read_candles(symbol=SYMBOL, timeframe=TIMEFRAME)
    cadence = timeframe_cadence(TIMEFRAME)

    if start is None or end is None:
        start = stored[0].timestamp
        end = stored[-1].timestamp + cadence

    inside, outside = restrict_to_range(stored, start, end)
    _, violations = partition_candles(inside)
    duplicates = duplicate_timestamps(inside)

    intervals = find_missing_trading_intervals(
        [item.timestamp for item in inside],
        cadence,
        start=start,
        end=end,
        calendar=calendar,
    )

    return {
        "candles": len(inside),
        "actual_start": inside[0].timestamp if inside else None,
        "actual_end": inside[-1].timestamp if inside else None,
        "expected_candles": calendar.expected_candle_count(start, end, cadence),
        "missing_candles": sum(item.missing_candles for item in intervals),
        "missing_interval_count": len(intervals),
        "missing_intervals": [
            (item.start, item.end, item.missing_candles)
            for item in intervals[:MAX_VIOLATION_SAMPLES]
        ],
        "duplicate_candles": sum(count - 1 for count in duplicates.values()),
        "invalid_rows": len(violations),
        "out_of_range_rows": len(outside),
        "closures": len(calendar.closed_intervals(start, end)),
    }


def streamed_report(root: Path, *, calendar, start=None, end=None) -> dict:
    report = validate(root, calendar=calendar, start=start, end=end)

    return {
        "candles": report.candles,
        "actual_start": report.actual_start,
        "actual_end": report.actual_end,
        "expected_candles": report.expected_candles,
        "missing_candles": report.missing_candles,
        "missing_interval_count": report.missing_interval_count,
        "missing_intervals": [
            (item.start, item.end, item.missing_candles)
            for item in report.missing_intervals
        ],
        "duplicate_candles": report.duplicate_candles,
        "invalid_rows": report.invalid_rows,
        "out_of_range_rows": report.out_of_range_rows,
        "closures": len(report.market_closed_intervals),
    }


def scenario(name: str) -> list[Candle]:
    if name == "complete_hour":
        return series(60)

    if name == "hole_in_the_middle":
        return [
            item for item in series(60) if item.timestamp.minute not in range(20, 30)
        ]

    if name == "two_months":
        return series(90, start=datetime(2026, 8, 31, 23, 0, tzinfo=UTC))

    if name == "sparse_year":
        return months(12, per_month=30)

    if name == "across_a_weekend":
        return series(120, start=datetime(2026, 8, 7, 20, 0, tzinfo=UTC)) + series(
            120, start=datetime(2026, 8, 9, 22, 0, tzinfo=UTC)
        )

    if name == "invalid_rows":
        return [
            candle(
                MONDAY + MINUTE * index, high="1.1600" if index % 7 == 0 else "1.1710"
            )
            for index in range(60)
        ]

    raise AssertionError(f"unknown scenario: {name}")


@pytest.mark.parametrize(
    "name",
    [
        "complete_hour",
        "hole_in_the_middle",
        "two_months",
        "sparse_year",
        "across_a_weekend",
        "invalid_rows",
    ],
)
@pytest.mark.parametrize("calendar_name", ["24x7", "forex"])
def test_the_streaming_report_matches_the_whole_list_one(
    tmp_path,
    name,
    calendar_name,
):
    calendar = AlwaysOpenCalendar() if calendar_name == "24x7" else ForexCalendar()
    store(tmp_path, scenario(name))

    assert streamed_report(tmp_path, calendar=calendar) == reference_report(
        tmp_path, calendar=calendar
    )


def test_the_streaming_report_matches_over_an_explicit_range(tmp_path):
    store(tmp_path, scenario("across_a_weekend"))

    window = {
        "start": datetime(2026, 8, 7, 18, 0, tzinfo=UTC),
        "end": datetime(2026, 8, 10, 6, 0, tzinfo=UTC),
    }
    calendar = ForexCalendar()

    assert streamed_report(tmp_path, calendar=calendar, **window) == reference_report(
        tmp_path, calendar=calendar, **window
    )


# --------------------------------------------------------------------------
# The gap scanner agrees with the whole-list function it replaced
# --------------------------------------------------------------------------


class TestGapScanner:
    def feed_in_batches(self, stamps, size, **kwargs):
        scanner = TradingGapScanner(**kwargs)

        for index in range(0, len(stamps), size):
            scanner.feed(stamps[index : index + size])

        return scanner

    def test_batching_does_not_change_the_result(self):
        calendar = ForexCalendar()
        start = datetime(2026, 8, 7, 12, 0, tzinfo=UTC)
        end = datetime(2026, 8, 11, 12, 0, tzinfo=UTC)

        stamps = [
            start + MINUTE * index
            for index in range(0, 4 * 24 * 60, 7)
            if calendar.is_open(start + MINUTE * index)
        ]

        expected = find_missing_trading_intervals(
            stamps,
            MINUTE,
            start=start,
            end=end,
            calendar=calendar,
        )

        for size in (1, 3, 50, len(stamps)):
            scanner = self.feed_in_batches(
                stamps,
                size,
                cadence=MINUTE,
                start=start,
                end=end,
                calendar=calendar,
            )

            assert scanner.finish() == expected

    def test_a_backwards_timestamp_is_counted_and_skipped(self):
        scanner = TradingGapScanner(
            MINUTE,
            start=MONDAY,
            end=MONDAY + MINUTE * 10,
            calendar=AlwaysOpenCalendar(),
        )

        scanner.feed([MONDAY + MINUTE * index for index in range(10)])
        scanner.feed([MONDAY])

        assert scanner.out_of_order == 1
        assert scanner.observed == 10
        assert scanner.finish() == []

    def test_samples_are_capped_while_counts_stay_exact(self):
        scanner = TradingGapScanner(
            MINUTE,
            start=MONDAY,
            end=MONDAY + MINUTE * 100,
            calendar=AlwaysOpenCalendar(),
            max_samples=5,
        )

        scanner.feed([MONDAY + MINUTE * index for index in range(0, 100, 2)])
        intervals = scanner.finish()

        assert len(intervals) == 5
        assert scanner.interval_count == 50
        assert scanner.missing_candles == 50

    def test_feeding_a_finished_scanner_is_refused(self):
        scanner = TradingGapScanner(
            MINUTE,
            start=MONDAY,
            end=MONDAY + MINUTE * 10,
            calendar=AlwaysOpenCalendar(),
        )
        scanner.finish()

        with pytest.raises(RuntimeError, match="already been finished"):
            scanner.feed_one(MONDAY)

    def test_a_non_positive_cadence_is_refused(self):
        with pytest.raises(ValueError, match="cadence must be positive"):
            TradingGapScanner(
                timedelta(0),
                start=MONDAY,
                end=MONDAY + MINUTE,
                calendar=AlwaysOpenCalendar(),
            )
