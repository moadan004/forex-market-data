"""
Gap differences in a cross-provider comparison are memory bounded.

Two datasets that disagree everywhere produce a gap per candle. Collecting
every one of them and truncating afterwards made the report largest for
exactly the comparison least able to afford it. These tests hold the
collector to three promises: the counts stay exact, the samples stay capped,
and the report is otherwise the one the unbounded version produced.
"""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from marketdata.cli import main
from marketdata.models.candle import Candle
from marketdata.quality.report import MAX_VIOLATION_SAMPLES
from marketdata.storage.parquet import ParquetStorage
from marketdata.verification.comparison import (
    GapDifference,
    _BoundedSamples,
    compare_datasets,
)
from marketdata.verification.status import VerificationStatus

SYMBOL = "EUR_USD"
TIMEFRAME = "1min"
MINUTE = timedelta(minutes=1)

# A Monday, so a short run sits inside an open forex session.
MONDAY = datetime(2026, 8, 10, tzinfo=UTC)


def candle(moment: datetime, price: str = "1.10000000") -> Candle:
    value = Decimal(price)

    return Candle(
        timestamp=moment,
        symbol=SYMBOL,
        open=value,
        high=value + Decimal("0.00050000"),
        low=value - Decimal("0.00050000"),
        close=value,
        volume=Decimal(10),
    )


def store(root: Path, candles: list[Candle]) -> ParquetStorage:
    storage = ParquetStorage(root)
    storage.write(candles, symbol=SYMBOL, timeframe=TIMEFRAME)

    return storage


def compare(root: Path, **kwargs):
    return compare_datasets(
        symbol=SYMBOL,
        timeframe=TIMEFRAME,
        left_root=root / "left",
        right_root=root / "right",
        **kwargs,
    )


def run(count: int, *, start: datetime = MONDAY) -> list[Candle]:
    return [candle(start + MINUTE * index) for index in range(count)]


def with_holes(count: int, holes: set[int], *, start: datetime = MONDAY):
    """A run of candles with the given minute offsets removed."""
    return [
        candle(start + MINUTE * index) for index in range(count) if index not in holes
    ]


def alternating(gaps: int, *, start: datetime = MONDAY) -> list[Candle]:
    """
    A run holding every other minute, producing exactly ``gaps`` gaps.

    Keeping n+1 candles two minutes apart leaves n single-candle gaps
    between them.
    """
    return [candle(start + MINUTE * 2 * index) for index in range(gaps + 1)]


# --------------------------------------------------------------------------
# The collector itself
# --------------------------------------------------------------------------


def sample(minute: int, side: str = "left") -> GapDifference:
    return GapDifference(
        side=side,
        start=MONDAY + MINUTE * minute,
        end=MONDAY + MINUTE * (minute + 1),
        missing_candles=1,
    )


class TestBoundedSamples:
    def collect(self, items, limit=3):
        collector = _BoundedSamples(limit, key=lambda gap: (gap.start, gap.side))

        for item in items:
            collector.add(item)

        return collector

    def test_nothing_collected_is_neither_truncated_nor_counted(self):
        collector = self.collect([])

        assert collector.total == 0
        assert collector.samples == []
        assert collector.truncated is False

    def test_under_the_limit_everything_is_kept(self):
        collector = self.collect([sample(0), sample(1)])

        assert collector.total == 2
        assert len(collector.samples) == 2
        assert collector.truncated is False

    def test_exactly_at_the_limit_nothing_is_truncated(self):
        collector = self.collect([sample(index) for index in range(3)])

        assert collector.total == 3
        assert len(collector.samples) == 3
        assert collector.truncated is False

    def test_one_past_the_limit_truncates(self):
        collector = self.collect([sample(index) for index in range(4)])

        assert collector.total == 4
        assert len(collector.samples) == 3
        assert collector.truncated is True

    def test_the_count_is_exact_however_many_arrive(self):
        collector = self.collect([sample(index) for index in range(10_000)])

        assert collector.total == 10_000
        assert len(collector.samples) == 3

    def test_the_retained_samples_are_the_ones_a_full_sort_would_keep(self):
        # Deliberately fed out of order: the collector must still keep the
        # lowest-ranked three, exactly as sorting everything would.
        items = [sample(index) for index in (7, 2, 9, 0, 5, 1)]
        collector = self.collect(items)

        expected = sorted(items, key=lambda gap: (gap.start, gap.side))[:3]

        assert collector.samples == expected

    def test_samples_are_returned_lowest_ranked_first(self):
        collector = self.collect([sample(index) for index in (5, 3, 4)])

        assert [gap.start for gap in collector.samples] == [
            MONDAY + MINUTE * 3,
            MONDAY + MINUTE * 4,
            MONDAY + MINUTE * 5,
        ]

    def test_the_side_breaks_ties_on_the_same_start(self):
        left = sample(1, side="left")
        right = sample(1, side="right")
        collector = self.collect([right, left], limit=1)

        assert collector.samples == [left]

    def test_identical_samples_never_compare_the_items_themselves(self):
        # GapDifference is not orderable; the collector must not rely on it.
        collector = self.collect([sample(1), sample(1), sample(1)], limit=2)

        assert collector.total == 3
        assert len(collector.samples) == 2


# --------------------------------------------------------------------------
# Gap counts and samples in a real comparison
# --------------------------------------------------------------------------


class TestGapDifferenceCounts:
    def test_no_gaps_at_all(self, tmp_path):
        store(tmp_path / "left", run(60))
        store(tmp_path / "right", run(60))

        report = compare(tmp_path)

        assert report.gaps_left == 0
        assert report.gaps_right == 0
        assert report.gaps_only_left == 0
        assert report.gaps_only_right == 0
        assert report.gap_differences == []
        assert report.gap_differences_truncated is False

    def test_one_gap_on_the_right_only(self, tmp_path):
        store(tmp_path / "left", run(60))
        store(tmp_path / "right", with_holes(60, {30}))

        report = compare(tmp_path)

        assert report.gaps_left == 0
        assert report.gaps_right == 1
        assert report.gaps_only_right == 1
        assert report.gaps_only_left == 0
        assert len(report.gap_differences) == 1
        assert report.gap_differences[0].side == "right"
        assert report.gap_differences[0].start == MONDAY + MINUTE * 30
        assert report.gap_differences_truncated is False

    def test_one_gap_on_the_left_only(self, tmp_path):
        store(tmp_path / "left", with_holes(60, {30}))
        store(tmp_path / "right", run(60))

        report = compare(tmp_path)

        assert report.gaps_left == 1
        assert report.gaps_right == 0
        assert report.gaps_only_left == 1
        assert report.gaps_only_right == 0
        assert report.gap_differences[0].side == "left"

    def test_a_gap_both_sides_share_is_not_a_difference(self, tmp_path):
        candles = with_holes(60, {30})
        store(tmp_path / "left", candles)
        store(tmp_path / "right", candles)

        report = compare(tmp_path)

        assert report.gaps_left == 1
        assert report.gaps_right == 1
        assert report.gaps_only_left == 0
        assert report.gaps_only_right == 0
        assert report.gap_differences == []

    def test_gaps_on_both_sides_at_different_places(self, tmp_path):
        # One shared gap, one unique to each side.
        store(tmp_path / "left", with_holes(60, {10, 30}))
        store(tmp_path / "right", with_holes(60, {30, 50}))

        report = compare(tmp_path)

        assert report.gaps_left == 2
        assert report.gaps_right == 2
        assert report.gaps_only_left == 1
        assert report.gaps_only_right == 1
        assert [gap.side for gap in report.gap_differences] == ["left", "right"]
        assert report.gap_differences[0].start == MONDAY + MINUTE * 10
        assert report.gap_differences[1].start == MONDAY + MINUTE * 50

    def test_exactly_the_sample_limit_is_not_truncated(self, tmp_path):
        store(tmp_path / "left", alternating(MAX_VIOLATION_SAMPLES))
        store(tmp_path / "right", run(MAX_VIOLATION_SAMPLES * 2 + 1))

        report = compare(tmp_path)

        assert report.gaps_only_left == MAX_VIOLATION_SAMPLES
        assert len(report.gap_differences) == MAX_VIOLATION_SAMPLES
        assert report.gap_differences_truncated is False

    def test_one_past_the_sample_limit_is_truncated(self, tmp_path):
        store(tmp_path / "left", alternating(MAX_VIOLATION_SAMPLES + 1))
        store(tmp_path / "right", run((MAX_VIOLATION_SAMPLES + 1) * 2 + 1))

        report = compare(tmp_path)

        # The count is exact; only the listing is abridged.
        assert report.gaps_only_left == MAX_VIOLATION_SAMPLES + 1
        assert len(report.gap_differences) == MAX_VIOLATION_SAMPLES
        assert report.gap_differences_truncated is True

    def test_a_very_large_number_of_gaps_stays_exact_and_capped(self, tmp_path):
        store(tmp_path / "left", alternating(5_000))
        store(tmp_path / "right", run(10_001))

        report = compare(tmp_path)

        assert report.gaps_left == 5_000
        assert report.gaps_only_left == 5_000
        assert report.gaps_only_right == 0
        assert len(report.gap_differences) == MAX_VIOLATION_SAMPLES
        assert report.gap_differences_truncated is True

    def test_the_retained_samples_are_the_earliest_ones(self, tmp_path):
        store(tmp_path / "left", alternating(200))
        store(tmp_path / "right", run(401))

        report = compare(tmp_path)

        assert [gap.start for gap in report.gap_differences] == [
            MONDAY + MINUTE * (2 * index + 1) for index in range(MAX_VIOLATION_SAMPLES)
        ]


class TestCrossPartitionGaps:
    def test_a_gap_spanning_two_partitions_is_a_difference(self, tmp_path):
        august = run(30, start=datetime(2026, 8, 31, 23, 0, tzinfo=UTC))
        september = run(30, start=datetime(2026, 9, 1, 0, 30, tzinfo=UTC))

        # The right side bridges the seam the left side leaves open.
        store(tmp_path / "left", august + september)
        store(
            tmp_path / "right",
            run(90, start=datetime(2026, 8, 31, 23, 0, tzinfo=UTC)),
        )

        report = compare(tmp_path)

        assert report.partitions_compared == 2
        assert report.gaps_only_left == 1
        assert report.gap_differences[0].start == datetime(
            2026, 8, 31, 23, 30, tzinfo=UTC
        )
        assert report.gap_differences[0].end == datetime(2026, 9, 1, 0, 30, tzinfo=UTC)

    def test_a_gap_spanning_partitions_that_both_sides_share_is_not_a_difference(
        self,
        tmp_path,
    ):
        august = run(30, start=datetime(2026, 8, 31, 23, 0, tzinfo=UTC))
        september = run(30, start=datetime(2026, 9, 1, 0, 30, tzinfo=UTC))

        store(tmp_path / "left", august + september)
        store(tmp_path / "right", august + september)

        report = compare(tmp_path)

        assert report.partitions_compared == 2
        assert report.gaps_left == 1
        assert report.gaps_right == 1
        assert report.gap_differences == []

    def test_gaps_across_many_partitions_are_counted_and_capped(self, tmp_path):
        # One candle per month on the left, two on the right, for two years.
        # Both sides gap at every month boundary, but the right side's gaps
        # start a minute later because its months end a minute later — and
        # gaps are matched by exact interval, so each side's 23 boundary
        # gaps are unique to it.
        sparse = [
            candle(datetime(2024 + index // 12, index % 12 + 1, 2, tzinfo=UTC))
            for index in range(24)
        ]
        dense = [
            item
            for index in range(24)
            for item in run(
                2, start=datetime(2024 + index // 12, index % 12 + 1, 2, tzinfo=UTC)
            )
        ]

        store(tmp_path / "left", sparse)
        store(tmp_path / "right", dense)

        report = compare(tmp_path)

        assert report.partitions_compared == 24
        assert report.gaps_left == 23
        assert report.gaps_right == 23
        assert report.gaps_only_left == 23
        assert report.gaps_only_right == 23

        # 46 one-sided gaps found across 24 partitions, 20 retained.
        assert len(report.gap_differences) == MAX_VIOLATION_SAMPLES
        assert report.gap_differences_truncated is True

        ranks = [(gap.start, gap.side) for gap in report.gap_differences]
        assert ranks == sorted(ranks)
        # The earliest one-sided gap is the left side's first boundary gap,
        # which opens a minute before the right side's.
        assert report.gap_differences[0].side == "left"
        assert report.gap_differences[0].start == datetime(2024, 1, 2, 0, 1, tzinfo=UTC)
        assert report.gap_differences[1].side == "right"
        assert report.gap_differences[1].start == datetime(2024, 1, 2, 0, 2, tzinfo=UTC)

    def test_samples_stay_ordered_across_partitions(self, tmp_path):
        left = [
            item
            for index in range(6)
            for item in alternating(5, start=datetime(2026, index + 1, 2, tzinfo=UTC))
        ]
        right = [
            item
            for index in range(6)
            for item in run(11, start=datetime(2026, index + 1, 2, tzinfo=UTC))
        ]

        store(tmp_path / "left", left)
        store(tmp_path / "right", right)

        report = compare(tmp_path)

        assert report.gaps_only_left == 30
        assert len(report.gap_differences) == MAX_VIOLATION_SAMPLES
        assert report.gap_differences_truncated is True
        starts = [gap.start for gap in report.gap_differences]
        assert starts == sorted(starts)
        assert starts[0] == datetime(2026, 1, 2, 0, 1, tzinfo=UTC)


# --------------------------------------------------------------------------
# Nothing is materialized
# --------------------------------------------------------------------------


class TestBoundedness:
    def test_no_structure_grows_with_the_number_of_gaps(self, tmp_path, monkeypatch):
        """
        Fails if the implementation collects every gap before truncating.

        The collector is instrumented rather than the process measured: the
        assertion is on how many samples are ever resident, which is the
        property that has to hold, and it holds deterministically.
        """
        peak = 0
        original = _BoundedSamples.add

        def spy(collector, sample):
            nonlocal peak
            original(collector, sample)
            peak = max(peak, len(collector.samples))

        monkeypatch.setattr(_BoundedSamples, "add", spy)

        store(tmp_path / "left", alternating(2_000))
        store(tmp_path / "right", run(4_001))

        report = compare(tmp_path)

        assert report.gaps_only_left == 2_000
        assert peak == MAX_VIOLATION_SAMPLES

    def test_the_gap_tracker_keeps_no_list_of_gaps(self, tmp_path):
        # The tracker used to hold every gap it had ever found. A count is
        # all that may survive a batch now.
        from marketdata.verification.comparison import _GapTracker

        tracker = _GapTracker(MINUTE)

        assert not hasattr(tracker, "gaps")

        first = tracker.feed([MONDAY, MONDAY + MINUTE * 5])
        second = tracker.feed([MONDAY + MINUTE * 10])

        assert len(first) == 1
        assert len(second) == 1
        assert tracker.count == 2

    def test_resident_samples_do_not_grow_with_the_dataset(self, tmp_path):
        store(tmp_path / "small-left", alternating(100))
        store(tmp_path / "small-right", run(201))
        store(tmp_path / "big-left", alternating(3_000))
        store(tmp_path / "big-right", run(6_001))

        small = compare_datasets(
            symbol=SYMBOL,
            timeframe=TIMEFRAME,
            left_root=tmp_path / "small-left",
            right_root=tmp_path / "small-right",
        )
        big = compare_datasets(
            symbol=SYMBOL,
            timeframe=TIMEFRAME,
            left_root=tmp_path / "big-left",
            right_root=tmp_path / "big-right",
        )

        assert small.gaps_only_left == 100
        assert big.gaps_only_left == 3_000
        assert len(small.gap_differences) == len(big.gap_differences)


# --------------------------------------------------------------------------
# Verdicts and the rest of the report are untouched
# --------------------------------------------------------------------------


class TestSemanticsUnchanged:
    def test_capping_gap_samples_does_not_change_the_verdict(self, tmp_path):
        # Thousands of one-sided gaps, and the verdict still comes from the
        # coverage ratio rather than from how many gaps were listed.
        store(tmp_path / "left", alternating(2_000))
        store(tmp_path / "right", run(4_001))

        report = compare(tmp_path)

        assert report.gap_differences_truncated is True
        assert report.status is VerificationStatus.FAIL
        assert report.missing_from_left == 2_000
        assert report.candles_compared == 2_001

    def test_a_small_disagreement_still_only_warns(self, tmp_path):
        store(tmp_path / "left", run(1_001))
        store(tmp_path / "right", with_holes(1_001, {500}))

        report = compare(tmp_path)

        assert report.gaps_only_right == 1
        assert report.gap_differences_truncated is False
        assert report.status is VerificationStatus.WARN

    def test_agreement_is_still_a_pass(self, tmp_path):
        candles = run(120)
        store(tmp_path / "left", candles)
        store(tmp_path / "right", candles)

        report = compare(tmp_path)

        assert report.status is VerificationStatus.PASS
        assert report.problems == []

    def test_prices_volumes_and_coverage_are_untouched(self, tmp_path):
        store(tmp_path / "left", run(60))
        store(
            tmp_path / "right",
            [candle(item.timestamp, "1.20000000") for item in run(60)],
        )

        report = compare(tmp_path)

        assert report.price_mismatches == 60
        assert report.max_price_difference == Decimal("0.10000000")
        assert report.volume_mismatches == 0
        assert report.missing_from_left == 0
        assert report.missing_from_right == 0
        assert report.status is VerificationStatus.FAIL


# --------------------------------------------------------------------------
# CLI output
# --------------------------------------------------------------------------


def compare_argv(tmp_path, *extra):
    return [
        "compare",
        "--left",
        str(tmp_path / "left"),
        "--right",
        str(tmp_path / "right"),
        "--symbol",
        SYMBOL,
        "--timeframe",
        TIMEFRAME,
        *extra,
    ]


@pytest.fixture
def many_gaps(tmp_path):
    store(tmp_path / "left", alternating(500))
    store(tmp_path / "right", run(1_001))

    return tmp_path


class TestCliOutput:
    def test_the_exact_count_is_printed_not_the_sample_count(self, many_gaps, capsys):
        main(compare_argv(many_gaps))
        output = capsys.readouterr().out

        assert "Gaps left/right:   500/0" in output
        assert "Gaps on one side:  500 left only, 0 right only" in output

    def test_the_abridged_listing_says_so(self, many_gaps, capsys):
        main(compare_argv(many_gaps))
        output = capsys.readouterr().out

        assert "further one-sided gaps not listed" in output
        assert "and 490 further one-sided gaps" in output

    def test_a_short_listing_says_nothing_about_truncation(self, tmp_path, capsys):
        store(tmp_path / "left", with_holes(60, {30}))
        store(tmp_path / "right", run(60))

        main(compare_argv(tmp_path))
        output = capsys.readouterr().out

        assert "gap in left only" in output
        assert "further one-sided gaps" not in output

    def test_json_carries_the_exact_counts_and_the_truncation_flag(
        self,
        many_gaps,
        capsys,
    ):
        main(compare_argv(many_gaps, "--json"))
        payload = json.loads(capsys.readouterr().out)

        assert payload["gaps_left"] == 500
        assert payload["gaps_only_left"] == 500
        assert payload["gap_differences_truncated"] is True
        assert len(payload["gap_differences"]) == MAX_VIOLATION_SAMPLES

    def test_json_stays_untruncated_for_a_small_comparison(self, tmp_path, capsys):
        store(tmp_path / "left", with_holes(60, {30}))
        store(tmp_path / "right", run(60))

        main(compare_argv(tmp_path, "--json"))
        payload = json.loads(capsys.readouterr().out)

        assert payload["gap_differences_truncated"] is False
        assert len(payload["gap_differences"]) == 1

    def test_a_recorded_comparison_keeps_the_bounded_samples(self, many_gaps, capsys):
        main(compare_argv(many_gaps, "--record-root", str(many_gaps / "records")))
        capsys.readouterr()

        written = next((many_gaps / "records").rglob("comparison.json"))
        payload = json.loads(written.read_text())["report"]

        assert payload["gaps_only_left"] == 500
        assert len(payload["gap_differences"]) == MAX_VIOLATION_SAMPLES
        assert payload["gap_differences_truncated"] is True
