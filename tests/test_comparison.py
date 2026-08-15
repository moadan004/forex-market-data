"""
Cross-provider comparison of two already-stored datasets.

Nothing here contacts a provider. The point of the comparison is that two
datasets can be judged against each other by anyone holding the files, so
every test builds Parquet on disk and compares it.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from marketdata.models.candle import Candle
from marketdata.storage.manifest import (
    DatasetProvenance,
    create_manifest,
    write_manifest,
)
from marketdata.storage.parquet import ParquetStorage
from marketdata.verification.comparison import (
    ComparisonThresholds,
    compare_datasets,
    dataset_fingerprint,
    describe_dataset,
    relative_difference,
)
from marketdata.verification.comparison_records import (
    ComparisonRecord,
    ComparisonStore,
    build_comparison_record,
    comparison_fingerprint,
)
from marketdata.verification.status import VerificationStatus

SYMBOL = "EUR_USD"
TIMEFRAME = "1min"

# A Monday, so a short run sits inside an open forex session.
MONDAY = datetime(2026, 8, 10, tzinfo=UTC)


def candle(
    moment: datetime,
    price: str = "1.10000000",
    volume: str = "10",
    symbol: str = SYMBOL,
) -> Candle:
    value = Decimal(price)

    return Candle(
        timestamp=moment,
        symbol=symbol,
        open=value,
        high=value + Decimal("0.00050000"),
        low=value - Decimal("0.00050000"),
        close=value,
        volume=Decimal(volume),
    )


def series(
    count: int,
    *,
    start: datetime = MONDAY,
    price: str = "1.10000000",
    volume: str = "10",
) -> list[Candle]:
    return [
        candle(start + timedelta(minutes=index), price=price, volume=volume)
        for index in range(count)
    ]


def store(root: Path, candles: list[Candle]) -> ParquetStorage:
    storage = ParquetStorage(root)
    storage.write(candles, symbol=SYMBOL, timeframe=TIMEFRAME)

    return storage


def compare(left: Path, right: Path, **kwargs):
    return compare_datasets(
        symbol=SYMBOL,
        timeframe=TIMEFRAME,
        left_root=left,
        right_root=right,
        **kwargs,
    )


def write_provenance(
    manifest_root: Path,
    *,
    provider: str,
    configuration: dict[str, str],
) -> Path:
    """Record who produced a dataset, the way the pipeline does."""
    manifest = create_manifest(
        symbol=SYMBOL,
        timeframe=TIMEFRAME,
        candles_count=0,
        start=MONDAY,
        end=MONDAY + timedelta(hours=1),
        provider=provider,
        files=[],
        provenance=DatasetProvenance(
            provider=provider,
            provider_configuration=configuration,
            symbol=SYMBOL,
            timeframe=TIMEFRAME,
            requested_start=MONDAY,
            requested_end=MONDAY + timedelta(hours=1),
            actual_start=MONDAY,
            actual_end=MONDAY,
            rows=0,
            quality_status="ok",
            calendar="forex",
            chunk_size="1month",
            rate_limit=None,
            retry=None,
            acquired_at=MONDAY,
        ),
    )

    return write_manifest(manifest, manifest_root / SYMBOL / f"{provider}.json")


class TestRelativeDifference:
    def test_equal_values_agree_at_any_scale(self):
        assert relative_difference(Decimal("157.123"), Decimal("157.123")) == 0
        assert relative_difference(Decimal(0), Decimal(0)) == 0

    def test_difference_is_proportional_to_magnitude(self):
        # The same absolute gap is a far bigger disagreement on a small price.
        small = relative_difference(Decimal("1.1000"), Decimal("1.1010"))
        large = relative_difference(Decimal("157.0000"), Decimal("157.0010"))

        assert small > large

    def test_zero_against_a_value_is_a_total_difference(self):
        assert relative_difference(Decimal(0), Decimal(5)) == 1

    def test_uses_decimal_rather_than_float_equality(self):
        # 0.1 + 0.2 != 0.3 in binary floating point; in Decimal it does.
        left = Decimal("0.1") + Decimal("0.2")

        assert relative_difference(left, Decimal("0.3")) == 0


class TestThresholds:
    def test_defaults_are_decimal(self):
        thresholds = ComparisonThresholds()

        assert isinstance(thresholds.price_tolerance, Decimal)
        assert isinstance(thresholds.volume_tolerance, Decimal)

    def test_float_tolerance_is_refused(self):
        with pytest.raises(TypeError, match="price_tolerance must be a Decimal"):
            ComparisonThresholds(price_tolerance=0.0001)

    def test_negative_tolerance_is_refused(self):
        with pytest.raises(ValueError, match="cannot be negative"):
            ComparisonThresholds(price_tolerance=Decimal(-1))

    def test_ratio_outside_zero_to_one_is_refused(self):
        with pytest.raises(ValueError, match="between 0 and 1"):
            ComparisonThresholds(price_mismatch_fail_ratio=1.5)

    def test_cannot_fail_before_it_warns(self):
        with pytest.raises(ValueError, match="cannot exceed"):
            ComparisonThresholds(
                price_mismatch_warn_ratio=0.5,
                price_mismatch_fail_ratio=0.1,
            )

    def test_volume_never_fails_by_default(self):
        # A ratio cannot exceed 1, so a fail ratio of 1 is unreachable.
        assert ComparisonThresholds().volume_mismatch_fail_ratio == 1.0

    def test_configuration_fingerprint_changes_with_the_thresholds(self):
        default = ComparisonThresholds()
        loosened = ComparisonThresholds(price_tolerance=Decimal("0.01"))

        assert default.fingerprint() != loosened.fingerprint()

    def test_description_names_every_bound(self):
        described = ComparisonThresholds().describe()

        assert "prices agree within" in described
        assert "volumes agree within" in described
        assert "coverage differences" in described


class TestAgreement:
    def test_identical_datasets_pass(self, tmp_path):
        candles = series(120)
        store(tmp_path / "left", candles)
        store(tmp_path / "right", candles)

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.status is VerificationStatus.PASS
        assert report.candles_compared == 120
        assert report.matching_candles == 120
        assert report.mismatching_candles == 0
        assert report.missing_from_left == 0
        assert report.missing_from_right == 0
        assert report.max_price_difference == 0
        assert report.problems == []

    def test_differences_inside_the_tolerance_pass(self, tmp_path):
        store(tmp_path / "left", series(60, price="1.10000000"))
        # One pip apart, which is under the default 0.01% relative bound.
        store(tmp_path / "right", series(60, price="1.10005000"))

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.status is VerificationStatus.PASS
        assert report.price_mismatches == 0
        assert report.max_price_difference > 0

    def test_reported_ranges_describe_both_sides(self, tmp_path):
        candles = series(60)
        store(tmp_path / "left", candles)
        store(tmp_path / "right", candles)

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.left_start == MONDAY
        assert report.left_end == MONDAY + timedelta(minutes=59)
        assert report.ranges_match


class TestPriceDisagreement:
    def test_a_price_beyond_the_tolerance_fails(self, tmp_path):
        store(tmp_path / "left", series(60, price="1.10000000"))
        store(tmp_path / "right", series(60, price="1.20000000"))

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.status is VerificationStatus.FAIL
        assert report.price_mismatches == 60
        assert report.matching_candles == 0
        assert report.max_price_difference == Decimal("0.10000000")
        assert any("disagree on price" in problem for problem in report.problems)

    def test_a_single_disagreeing_candle_is_located(self, tmp_path):
        candles = series(600)
        broken = list(candles)
        moment = MONDAY + timedelta(minutes=300)
        broken[300] = candle(moment, price="1.50000000")

        store(tmp_path / "left", candles)
        store(tmp_path / "right", broken)

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.price_mismatches == 1
        assert report.max_price_difference_at == moment
        assert {difference.field for difference in report.differences} == {
            "open",
            "high",
            "low",
            "close",
        }

    def test_one_in_six_hundred_warns_rather_than_fails(self, tmp_path):
        # Below the 0.1% fail ratio but above the 0% warn ratio.
        candles = series(1200)
        broken = list(candles)
        broken[10] = candle(MONDAY + timedelta(minutes=10), price="1.50000000")

        store(tmp_path / "left", candles)
        store(tmp_path / "right", broken)

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.status is VerificationStatus.WARN

    def test_the_tolerance_is_configurable(self, tmp_path):
        store(tmp_path / "left", series(60, price="1.10000000"))
        store(tmp_path / "right", series(60, price="1.20000000"))

        report = compare(
            tmp_path / "left",
            tmp_path / "right",
            thresholds=ComparisonThresholds(price_tolerance=Decimal("0.1")),
        )

        assert report.status is VerificationStatus.PASS
        assert report.price_mismatches == 0

    def test_a_difference_is_reported_not_corrected(self, tmp_path):
        store(tmp_path / "left", series(10, price="1.10000000"))
        store(tmp_path / "right", series(10, price="1.20000000"))

        before = {
            side: dataset_fingerprint(
                ParquetStorage(tmp_path / side).partition_files(
                    symbol=SYMBOL, timeframe=TIMEFRAME
                ),
                tmp_path / side,
            )
            for side in ("left", "right")
        }

        compare(tmp_path / "left", tmp_path / "right")

        after = {
            side: dataset_fingerprint(
                ParquetStorage(tmp_path / side).partition_files(
                    symbol=SYMBOL, timeframe=TIMEFRAME
                ),
                tmp_path / side,
            )
            for side in ("left", "right")
        }

        assert before == after


class TestVolumeDisagreement:
    def test_volume_differences_warn_but_do_not_fail_by_default(self, tmp_path):
        store(tmp_path / "left", series(60, volume="100"))
        store(tmp_path / "right", series(60, volume="250"))

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.status is VerificationStatus.WARN
        assert report.volume_mismatches == 60
        assert report.price_mismatches == 0
        assert report.max_volume_difference == Decimal(150)

    def test_volume_can_be_made_to_fail(self, tmp_path):
        store(tmp_path / "left", series(60, volume="100"))
        store(tmp_path / "right", series(60, volume="250"))

        report = compare(
            tmp_path / "left",
            tmp_path / "right",
            thresholds=ComparisonThresholds(volume_mismatch_fail_ratio=0.5),
        )

        assert report.status is VerificationStatus.FAIL

    def test_small_volume_differences_are_tolerated(self, tmp_path):
        store(tmp_path / "left", series(60, volume="100"))
        store(tmp_path / "right", series(60, volume="102"))

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.status is VerificationStatus.PASS
        assert report.volume_mismatches == 0

    def test_absent_volume_is_reported_as_unavailable(self, tmp_path):
        store(tmp_path / "left", series(60, volume="0"))
        store(tmp_path / "right", series(60, volume="0"))

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.volume_available is False
        assert report.status is VerificationStatus.PASS

    def test_volume_on_one_side_only_is_a_difference(self, tmp_path):
        store(tmp_path / "left", series(60, volume="500"))
        store(tmp_path / "right", series(60, volume="0"))

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.volume_available is True
        assert report.volume_mismatches == 60


class TestCoverage:
    def test_candles_missing_from_the_right_are_counted(self, tmp_path):
        store(tmp_path / "left", series(120))
        store(tmp_path / "right", series(60))

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.missing_from_right == 60
        assert report.missing_from_left == 0
        assert report.candles_compared == 60
        assert report.status is VerificationStatus.FAIL
        assert report.missing_from_right_samples[0] == MONDAY + timedelta(minutes=60)

    def test_candles_missing_from_the_left_are_counted(self, tmp_path):
        store(tmp_path / "left", series(60))
        store(tmp_path / "right", series(120))

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.missing_from_left == 60
        assert report.missing_from_right == 0
        assert not report.ranges_match

    def test_a_tolerable_shortfall_only_warns(self, tmp_path):
        # 1 of 1001 combined candles is under the 1% fail ratio.
        store(tmp_path / "left", series(1001))
        store(tmp_path / "right", series(1000))

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.missing_from_right == 1
        assert report.status is VerificationStatus.WARN

    def test_datasets_with_no_overlap_at_all_fail(self, tmp_path):
        store(tmp_path / "left", series(60, start=MONDAY))
        store(tmp_path / "right", series(60, start=MONDAY + timedelta(hours=5)))

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.candles_compared == 0
        assert report.status is VerificationStatus.FAIL

    def test_the_requested_range_restricts_both_sides(self, tmp_path):
        store(tmp_path / "left", series(120))
        store(tmp_path / "right", series(60))

        report = compare(
            tmp_path / "left",
            tmp_path / "right",
            start=MONDAY,
            end=MONDAY + timedelta(minutes=60),
        )

        assert report.candles_compared == 60
        assert report.missing_from_right == 0
        assert report.status is VerificationStatus.PASS

    def test_an_inverted_range_is_refused(self, tmp_path):
        store(tmp_path / "left", series(10))
        store(tmp_path / "right", series(10))

        with pytest.raises(ValueError, match="start must be before end"):
            compare(
                tmp_path / "left",
                tmp_path / "right",
                start=MONDAY + timedelta(hours=1),
                end=MONDAY,
            )


class TestGaps:
    def test_a_gap_on_one_side_only_is_reported(self, tmp_path):
        candles = series(60)
        holed = [item for item in candles if item.timestamp.minute not in (30, 31)]

        store(tmp_path / "left", candles)
        store(tmp_path / "right", holed)

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.gaps_left == 0
        assert report.gaps_right == 1
        assert report.gaps_only_right == 1
        assert report.gaps_only_left == 0
        assert report.gap_differences[0].side == "right"
        assert report.gap_differences[0].missing_candles == 2

    def test_a_gap_shared_by_both_sides_is_not_a_difference(self, tmp_path):
        candles = [item for item in series(60) if item.timestamp.minute not in (30, 31)]

        store(tmp_path / "left", candles)
        store(tmp_path / "right", candles)

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.gaps_left == 1
        assert report.gaps_right == 1
        assert report.gap_differences == []
        assert report.status is VerificationStatus.PASS

    def test_a_weekend_closure_present_in_both_is_not_a_difference(self, tmp_path):
        # Friday 20:00 to Sunday 22:05 UTC: the forex week closes in between,
        # and neither dataset has candles for it.
        friday = datetime(2026, 8, 7, 20, 0, tzinfo=UTC)
        sunday = datetime(2026, 8, 9, 22, 0, tzinfo=UTC)

        candles = series(30, start=friday) + series(30, start=sunday)

        store(tmp_path / "left", candles)
        store(tmp_path / "right", candles)

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.gaps_left == 1
        assert report.gap_differences == []
        assert report.status is VerificationStatus.PASS

    def test_a_gap_spanning_a_month_boundary_is_found(self, tmp_path):
        august = series(30, start=datetime(2026, 8, 31, 23, 30, tzinfo=UTC))
        september = series(30, start=datetime(2026, 9, 1, 2, 0, tzinfo=UTC))

        store(tmp_path / "left", august + september)
        store(tmp_path / "right", august + september)

        report = compare(tmp_path / "left", tmp_path / "right")

        # The gap crosses the partition boundary, so finding it proves the
        # month-at-a-time reader carries state across batches.
        assert report.gaps_left == 1
        assert report.gaps_right == 1
        assert report.partitions_compared == 2


class TestStructuralDefects:
    def test_duplicate_timestamps_fail(self, tmp_path):
        candles = series(30)
        store(tmp_path / "left", candles)
        store(tmp_path / "right", candles)

        # Write a partition holding the same timestamp twice, which the
        # storage layer would otherwise merge away.
        from marketdata.storage.parquet import candles_to_table

        path = ParquetStorage(tmp_path / "right").partition_files(
            symbol=SYMBOL, timeframe=TIMEFRAME
        )[0]
        pq.write_table(candles_to_table([*candles, candles[0]]), path)

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.duplicate_timestamps_right == 1
        assert report.status is VerificationStatus.FAIL
        assert any("duplicate timestamps" in problem for problem in report.problems)

    def test_a_corrupt_partition_is_a_finding_not_an_exception(self, tmp_path):
        store(tmp_path / "left", series(30))
        store(tmp_path / "right", series(30))

        path = ParquetStorage(tmp_path / "right").partition_files(
            symbol=SYMBOL, timeframe=TIMEFRAME
        )[0]
        path.write_bytes(b"this is not a parquet file")

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.status is VerificationStatus.FAIL
        assert report.right.readable is False
        assert any("unreadable" in problem for problem in report.problems)

    def test_an_unreadable_path_is_a_finding(self, tmp_path):
        store(tmp_path / "left", series(30))

        # A directory where a partition file should be: readable by glob,
        # unreadable by Parquet.
        directory = (
            ParquetStorage(tmp_path / "right").dataset_path(SYMBOL, TIMEFRAME)
            / "year=2026"
            / "month=08"
        )
        (directory / "candles.parquet").mkdir(parents=True)

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.status is VerificationStatus.FAIL
        assert report.right.readable is False

    def test_an_incompatible_schema_fails(self, tmp_path):
        store(tmp_path / "left", series(30))
        store(tmp_path / "right", series(30))

        path = ParquetStorage(tmp_path / "right").partition_files(
            symbol=SYMBOL, timeframe=TIMEFRAME
        )[0]
        pq.write_table(
            pa.table({"timestamp": [1, 2, 3], "close": [1.0, 2.0, 3.0]}),
            path,
        )

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.status is VerificationStatus.FAIL
        assert report.right.schema_differences
        assert any("schema is incompatible" in problem for problem in report.problems)


class TestEmptyDatasets:
    def test_two_empty_datasets_are_blocked_not_passed(self, tmp_path):
        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.status is VerificationStatus.BLOCKED
        assert report.candles_compared == 0
        assert any("no candles to compare" in problem for problem in report.problems)

    def test_one_empty_dataset_fails_rather_than_agreeing(self, tmp_path):
        store(tmp_path / "left", series(60))

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.status is VerificationStatus.FAIL
        assert report.missing_from_right == 60

    def test_a_range_covering_no_stored_candles_is_blocked(self, tmp_path):
        store(tmp_path / "left", series(60))
        store(tmp_path / "right", series(60))

        report = compare(
            tmp_path / "left",
            tmp_path / "right",
            start=datetime(2030, 1, 1, tzinfo=UTC),
            end=datetime(2030, 1, 2, tzinfo=UTC),
        )

        assert report.status is VerificationStatus.BLOCKED


class TestTimestampNormalization:
    def test_the_same_instants_written_at_another_offset_still_match(self, tmp_path):
        offset = timezone_of(hours=2)

        left = series(60)
        right = [
            Candle(
                timestamp=item.timestamp.astimezone(offset),
                symbol=item.symbol,
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                volume=item.volume,
            )
            for item in left
        ]

        store(tmp_path / "left", left)
        store(tmp_path / "right", right)

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.candles_compared == 60
        assert report.matching_candles == 60
        assert report.status is VerificationStatus.PASS

    def test_reported_timestamps_are_utc(self, tmp_path):
        store(tmp_path / "left", series(60))
        store(tmp_path / "right", series(30))

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.missing_from_right_samples
        assert all(
            stamp.utcoffset() == timedelta(0)
            for stamp in report.missing_from_right_samples
        )

    def test_a_local_offset_shift_is_a_real_difference(self, tmp_path):
        # Naively relabelling UTC times as +02:00 moves the instants, which
        # must show up as missing candles rather than being normalized away.
        left = series(60)
        right = [
            Candle(
                timestamp=item.timestamp.replace(tzinfo=timezone_of(hours=2)),
                symbol=item.symbol,
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                volume=item.volume,
            )
            for item in left
        ]

        store(tmp_path / "left", left)
        store(tmp_path / "right", right)

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.candles_compared == 0
        assert report.status is VerificationStatus.FAIL


def timezone_of(*, hours: int):
    from datetime import timezone

    return timezone(timedelta(hours=hours))


class TestBoundedMemory:
    def test_partitions_are_read_one_at_a_time(self, tmp_path, monkeypatch):
        months = [
            series(5, start=datetime(2026, month, 2, tzinfo=UTC))
            for month in (8, 9, 10)
        ]
        candles = [item for month in months for item in month]

        store(tmp_path / "left", candles)
        store(tmp_path / "right", candles)

        reads: list[Path] = []
        original = ParquetStorage.read_partition

        def spy(self, path):
            reads.append(Path(path))
            return original(self, path)

        monkeypatch.setattr(ParquetStorage, "read_partition", spy)

        def refuse(*args, **kwargs):
            raise AssertionError("the whole dataset must never be loaded at once")

        monkeypatch.setattr(ParquetStorage, "read_table", refuse)
        monkeypatch.setattr(ParquetStorage, "read_candles", refuse)

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.partitions_compared == 3
        assert len(reads) == 6
        assert report.candles_compared == 15

    def test_partitions_are_compared_in_chronological_order(self, tmp_path):
        candles = [
            item
            for month in (8, 9, 10)
            for item in series(5, start=datetime(2026, month, 2, tzinfo=UTC))
        ]

        store(tmp_path / "left", candles)
        store(tmp_path / "right", candles)

        from marketdata.verification.comparison import partition_batches

        batches = partition_batches(
            ParquetStorage(tmp_path / "left"),
            ParquetStorage(tmp_path / "right"),
            symbol=SYMBOL,
            timeframe=TIMEFRAME,
            start=None,
            end=None,
        )

        assert [key for key, _, _ in batches] == [(2026, 8), (2026, 9), (2026, 10)]

    def test_a_month_present_on_only_one_side_is_still_compared(self, tmp_path):
        august = series(5, start=datetime(2026, 8, 2, tzinfo=UTC))
        september = series(5, start=datetime(2026, 9, 2, tzinfo=UTC))

        store(tmp_path / "left", august + september)
        store(tmp_path / "right", august)

        report = compare(tmp_path / "left", tmp_path / "right")

        assert report.partitions_compared == 2
        assert report.missing_from_right == 5


class TestDatasetIdentity:
    def test_the_provider_is_read_from_the_manifest(self, tmp_path):
        store(tmp_path / "data", series(10))
        write_provenance(
            tmp_path / "manifests",
            provider="csv",
            configuration={"source": "fixtures/eur_usd.csv"},
        )

        identity = describe_dataset(
            tmp_path / "data",
            symbol=SYMBOL,
            timeframe=TIMEFRAME,
            side="left",
            manifest_root=tmp_path / "manifests",
        )

        assert identity.provider == "csv"
        assert identity.provider_configuration == {"source": "fixtures/eur_usd.csv"}
        assert identity.rows == 10

    def test_an_unidentified_dataset_says_so(self, tmp_path):
        store(tmp_path / "data", series(10))

        identity = describe_dataset(
            tmp_path / "data",
            symbol=SYMBOL,
            timeframe=TIMEFRAME,
            side="left",
        )

        assert identity.provider == "unknown"

    def test_the_dataset_fingerprint_follows_the_bytes(self, tmp_path):
        store(tmp_path / "data", series(10))

        def fingerprint() -> str:
            return describe_dataset(
                tmp_path / "data",
                symbol=SYMBOL,
                timeframe=TIMEFRAME,
                side="left",
            ).dataset_fingerprint

        before = fingerprint()

        # Same row count, same file, one changed price.
        store(tmp_path / "data", [candle(MONDAY, price="1.90000000")])

        assert fingerprint() != before

    def test_the_provider_fingerprint_follows_the_configuration(self, tmp_path):
        store(tmp_path / "data", series(10))

        def fingerprint(source: str) -> str:
            root = tmp_path / f"manifests-{source}"
            write_provenance(root, provider="csv", configuration={"source": source})

            return describe_dataset(
                tmp_path / "data",
                symbol=SYMBOL,
                timeframe=TIMEFRAME,
                side="left",
                manifest_root=root,
            ).provider_fingerprint

        assert fingerprint("one.csv") != fingerprint("two.csv")

    def test_disagreeing_manifests_are_named_rather_than_picked_between(self, tmp_path):
        store(tmp_path / "data", series(10))
        write_provenance(tmp_path / "manifests", provider="csv", configuration={})
        write_provenance(tmp_path / "manifests", provider="dukascopy", configuration={})

        identity = describe_dataset(
            tmp_path / "data",
            symbol=SYMBOL,
            timeframe=TIMEFRAME,
            side="left",
            manifest_root=tmp_path / "manifests",
        )

        assert identity.provider == "mixed:csv,dukascopy"

    def test_both_provider_identities_reach_the_report(self, tmp_path):
        store(tmp_path / "left", series(10))
        store(tmp_path / "right", series(10))
        write_provenance(tmp_path / "left-manifests", provider="csv", configuration={})
        write_provenance(
            tmp_path / "right-manifests",
            provider="dukascopy",
            configuration={"base_url": "https://example.invalid/", "api_key": "unset"},
        )

        report = compare(
            tmp_path / "left",
            tmp_path / "right",
            left_manifest_root=tmp_path / "left-manifests",
            right_manifest_root=tmp_path / "right-manifests",
        )

        assert report.left.provider == "csv"
        assert report.right.provider == "dukascopy"
        assert report.right.provider_configuration["api_key"] == "unset"

    def test_no_credential_is_ever_recorded(self, tmp_path):
        store(tmp_path / "data", series(10))
        write_provenance(
            tmp_path / "manifests",
            provider="dukascopy",
            # A provider reports whether a key is set, never its value.
            configuration={"api_key": "set", "base_url": "https://example.invalid/"},
        )

        identity = describe_dataset(
            tmp_path / "data",
            symbol=SYMBOL,
            timeframe=TIMEFRAME,
            side="left",
            manifest_root=tmp_path / "manifests",
        )

        assert "set" == identity.provider_configuration["api_key"]
        assert not any(
            len(value) > 20 and value.isalnum()
            for value in identity.provider_configuration.values()
        )


def current_fingerprint(report, thresholds: ComparisonThresholds) -> str:
    return comparison_fingerprint(
        left=report.left,
        right=report.right,
        symbol=report.symbol,
        timeframe=report.timeframe,
        start=report.requested_start,
        end=report.requested_end,
        thresholds=thresholds,
    )


class TestComparisonRecord:
    @pytest.fixture
    def situation(self, tmp_path):
        store(tmp_path / "left", series(60))
        store(tmp_path / "right", series(60))
        write_provenance(tmp_path / "left-manifests", provider="csv", configuration={})
        write_provenance(
            tmp_path / "right-manifests",
            provider="dukascopy",
            configuration={"base_url": "https://example.invalid/"},
        )

        return tmp_path

    def build(self, root, *, thresholds=None, **kwargs):
        thresholds = thresholds or ComparisonThresholds()
        report = compare(
            root / "left",
            root / "right",
            left_manifest_root=root / "left-manifests",
            right_manifest_root=root / "right-manifests",
            thresholds=thresholds,
            **kwargs,
        )

        return build_comparison_record(report, thresholds), thresholds

    def test_a_record_holds_the_whole_situation(self, situation):
        record, _ = self.build(situation)

        assert record.report.left.provider == "csv"
        assert record.report.right.provider == "dukascopy"
        assert record.symbol == SYMBOL
        assert record.timeframe == TIMEFRAME
        assert record.status is VerificationStatus.PASS
        assert record.application_version
        assert record.key["thresholds"]

    def test_a_record_survives_a_round_trip(self, situation, tmp_path):
        record, _ = self.build(situation)
        store_ = ComparisonStore(tmp_path / "records")

        path = store_.save(record)
        loaded = store_.load(
            left_provider="csv",
            right_provider="dukascopy",
            symbol=SYMBOL,
            timeframe=TIMEFRAME,
        )

        assert path.exists()
        assert loaded is not None
        assert loaded.fingerprint == record.fingerprint
        assert loaded.report.candles_compared == record.report.candles_compared

    def test_a_record_applies_to_its_own_situation(self, situation):
        record, thresholds = self.build(situation)

        assert record.applies_to(current_fingerprint(record.report, thresholds))

    def test_changing_a_dataset_invalidates_the_record(self, situation):
        record, thresholds = self.build(situation)

        store(situation / "right", [candle(MONDAY, price="1.90000000")])

        replacement, _ = self.build(situation, thresholds=thresholds)

        assert not record.applies_to(
            current_fingerprint(replacement.report, thresholds)
        )

    def test_changing_a_provider_configuration_invalidates_the_record(self, situation):
        record, thresholds = self.build(situation)

        write_provenance(
            situation / "right-manifests",
            provider="dukascopy",
            configuration={"base_url": "https://elsewhere.invalid/"},
        )

        replacement, _ = self.build(situation, thresholds=thresholds)

        assert not record.applies_to(
            current_fingerprint(replacement.report, thresholds)
        )

    def test_changing_the_thresholds_invalidates_the_record(self, situation):
        record, _ = self.build(situation)
        loosened = ComparisonThresholds(price_tolerance=Decimal("0.5"))

        assert not record.applies_to(current_fingerprint(record.report, loosened))

    def test_changing_the_range_invalidates_the_record(self, situation):
        record, thresholds = self.build(situation)

        narrowed, _ = self.build(
            situation,
            thresholds=thresholds,
            start=MONDAY,
            end=MONDAY + timedelta(minutes=30),
        )

        assert not record.applies_to(current_fingerprint(narrowed.report, thresholds))

    def test_changing_the_symbol_invalidates_the_record(self, situation):
        record, thresholds = self.build(situation)

        moved = record.report.model_copy(update={"symbol": "GBP_USD"})

        assert not record.applies_to(current_fingerprint(moved, thresholds))

    def test_changing_the_timeframe_invalidates_the_record(self, situation):
        record, thresholds = self.build(situation)

        moved = record.report.model_copy(update={"timeframe": "1hour"})

        assert not record.applies_to(current_fingerprint(moved, thresholds))

    def test_a_schema_bump_invalidates_the_record(self, situation):
        record, thresholds = self.build(situation)
        fingerprint = current_fingerprint(record.report, thresholds)

        stale = ComparisonRecord.model_validate(
            record.model_dump() | {"version": 0},
        )

        assert not stale.applies_to(fingerprint)

    def test_the_store_refuses_a_stale_record(self, situation, tmp_path):
        record, thresholds = self.build(situation)
        store_ = ComparisonStore(tmp_path / "records")
        store_.save(record)

        store(situation / "right", [candle(MONDAY, price="1.90000000")])
        replacement, _ = self.build(situation, thresholds=thresholds)

        assert (
            store_.valid_record(
                left_provider="csv",
                right_provider="dukascopy",
                symbol=SYMBOL,
                timeframe=TIMEFRAME,
                fingerprint=current_fingerprint(replacement.report, thresholds),
            )
            is None
        )

    def test_the_store_returns_a_failing_record(self, tmp_path):
        # Unlike a stage record, a stored FAIL is exactly what a later reader
        # needs to see, so it is not hidden.
        store(tmp_path / "left", series(30, price="1.10000000"))
        store(tmp_path / "right", series(30, price="1.90000000"))

        thresholds = ComparisonThresholds()
        report = compare(tmp_path / "left", tmp_path / "right", thresholds=thresholds)
        record = build_comparison_record(report, thresholds)

        store_ = ComparisonStore(tmp_path / "records")
        store_.save(record)

        found = store_.valid_record(
            left_provider="unknown",
            right_provider="unknown",
            symbol=SYMBOL,
            timeframe=TIMEFRAME,
            fingerprint=current_fingerprint(report, thresholds),
        )

        assert found is not None
        assert found.status is VerificationStatus.FAIL

    def test_an_unreadable_record_is_not_evidence(self, tmp_path):
        store_ = ComparisonStore(tmp_path / "records")
        path = store_.path_for(
            left_provider="csv",
            right_provider="dukascopy",
            symbol=SYMBOL,
            timeframe=TIMEFRAME,
        )
        path.parent.mkdir(parents=True)
        path.write_text("{}")

        assert (
            store_.load(
                left_provider="csv",
                right_provider="dukascopy",
                symbol=SYMBOL,
                timeframe=TIMEFRAME,
            )
            is None
        )

    def test_a_provider_name_is_confined_to_the_store_root(self, tmp_path):
        store_ = ComparisonStore(tmp_path / "records")

        path = store_.path_for(
            left_provider="../../etc",
            right_provider="csv",
            symbol=SYMBOL,
            timeframe=TIMEFRAME,
        )

        assert store_.root in path.parents
