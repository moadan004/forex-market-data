"""
Comparing two datasets that the real pipeline produced.

Everywhere else the comparison is fed hand-written Parquet. Here two
independent CSV feeds of the same instrument are acquired through the
production download path and then compared, which is what the command is
actually for: the comparison never sees a provider, only what one left on
disk.
"""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from marketdata.calendar.forex import ForexCalendar
from marketdata.cli import main
from marketdata.downloader.pipeline import DownloadPipeline
from marketdata.providers.csv import CsvMarketDataProvider
from marketdata.verification.comparison import ComparisonThresholds, compare_datasets
from marketdata.verification.status import VerificationStatus

FIXTURES = Path(__file__).parent / "fixtures" / "csv"

SYMBOL = "EUR/USD"
START = datetime(2026, 8, 10, tzinfo=UTC)
END = START + timedelta(hours=2)


def acquire(root: Path, source: Path) -> None:
    """Run one CSV feed through the production download pipeline."""
    pipeline = DownloadPipeline(
        CsvMarketDataProvider(source),
        output_root=root / "processed",
        manifest_root=root / "manifests",
        quality_root=root / "quality",
        checkpoint_root=root / "checkpoints",
        calendar=ForexCalendar(),
    )

    pipeline.run(symbol=SYMBOL, start=START, end=END, timeframe="1min")


@pytest.fixture
def feeds(tmp_path):
    acquire(tmp_path / "first", FIXTURES / "dataset")
    acquire(tmp_path / "second", FIXTURES / "second_feed")

    return tmp_path


def compare(root: Path, **kwargs):
    return compare_datasets(
        symbol=SYMBOL,
        timeframe="1min",
        left_root=root / "first" / "processed",
        right_root=root / "second" / "processed",
        left_manifest_root=root / "first" / "manifests",
        right_manifest_root=root / "second" / "manifests",
        **kwargs,
    )


def test_two_real_feeds_agree_on_price_within_the_tolerance(feeds):
    report = compare(feeds)

    # The second feed quotes two hundredths of a pip away throughout, which
    # is what two liquidity pools look like, not a data error.
    assert report.candles_compared == 119
    assert report.price_mismatches == 0
    assert report.max_price_difference == Decimal("0.00002000")


def test_the_second_feed_is_missing_one_minute(feeds):
    report = compare(feeds)

    assert report.missing_from_right == 1
    assert report.missing_from_left == 0
    assert report.missing_from_right_samples == [START + timedelta(minutes=60)]
    assert report.gaps_only_right == 1
    assert report.gaps_only_left == 0


def test_a_single_missing_minute_warns_rather_than_fails(feeds):
    report = compare(feeds)

    assert report.status is VerificationStatus.WARN
    assert any("right is missing 1 candle" in problem for problem in report.problems)


def test_provider_specific_tick_volumes_are_tolerated(feeds):
    report = compare(feeds)

    assert report.volume_available is True
    assert report.volume_mismatches == 0
    assert report.max_volume_difference > 0


def test_a_stricter_price_tolerance_turns_the_difference_into_a_failure(feeds):
    report = compare(
        feeds,
        thresholds=ComparisonThresholds(price_tolerance=Decimal("0.0000001")),
    )

    assert report.price_mismatches == 119
    assert report.status is VerificationStatus.FAIL


def test_both_providers_are_identified_from_their_manifests(feeds):
    report = compare(feeds)

    assert report.left.provider == "csv"
    assert report.right.provider == "csv"
    # The same provider pointed at two different sources is two different
    # setups, and the fingerprints have to say so.
    assert report.left.provider_fingerprint != report.right.provider_fingerprint
    assert report.left.dataset_fingerprint != report.right.dataset_fingerprint


def test_the_comparison_leaves_both_datasets_untouched(feeds):
    def snapshot(side: str) -> dict[str, bytes]:
        root = feeds / side / "processed"

        return {
            str(path.relative_to(root)): path.read_bytes()
            for path in sorted(root.rglob("*.parquet"))
        }

    before = {side: snapshot(side) for side in ("first", "second")}

    compare(feeds)

    assert {side: snapshot(side) for side in ("first", "second")} == before


def test_the_command_line_compares_what_the_pipeline_stored(feeds, capsys):
    code = main(
        [
            "compare",
            "--left",
            str(feeds / "first" / "processed"),
            "--right",
            str(feeds / "second" / "processed"),
            "--left-manifest-root",
            str(feeds / "first" / "manifests"),
            "--right-manifest-root",
            str(feeds / "second" / "manifests"),
            "--symbol",
            SYMBOL,
            "--timeframe",
            "1min",
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "warn"
    assert payload["candles_compared"] == 119
    assert payload["missing_from_right"] == 1
    assert payload["left"]["provider"] == "csv"
