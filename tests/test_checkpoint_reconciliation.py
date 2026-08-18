"""
A checkpoint is reconciled against the data before a chunk is skipped.

A checkpoint records what a run did. What is on disk now is a different
question, and between two runs a partition can be deleted, lost to a failed
copy, or corrupted. Trusting the checkpoint alone would skip the chunk and
report a clean run over a hole — a zero exit code standing in for evidence,
which is the one thing this project refuses to accept.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from marketdata.calendar import AlwaysOpenCalendar
from marketdata.downloader.chunks import DurationChunkSize
from marketdata.downloader.pipeline import DownloadPipeline
from marketdata.models.candle import Candle
from marketdata.providers.base import MarketDataProvider
from marketdata.quality.dataset import validate_dataset
from marketdata.storage.parquet import ParquetStorage

SYMBOL = "EUR/USD"
TIMEFRAME = "1min"
MINUTE = timedelta(minutes=1)

# A Monday, so an always-open calendar and a forex one agree here.
START = datetime(2026, 8, 10, tzinfo=UTC)
END = START + timedelta(hours=4)


class CountingProvider(MarketDataProvider):
    """A deterministic provider that records which windows it was asked for."""

    def __init__(self) -> None:
        self.requested: list[datetime] = []

    @property
    def name(self) -> str:
        return "counting"

    def get_supported_symbols(self) -> list[str]:
        return [SYMBOL]

    def health_check(self) -> bool:
        return True

    def fetch_candles(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = TIMEFRAME,
    ) -> list[Candle]:
        self.requested.append(start)
        price = Decimal("1.10000000")
        minutes = int((end - start).total_seconds() // 60)

        return [
            Candle(
                timestamp=start + MINUTE * index,
                symbol=symbol,
                open=price,
                high=price + Decimal("0.00050000"),
                low=price - Decimal("0.00050000"),
                close=price,
                volume=Decimal(100),
            )
            for index in range(minutes)
        ]


class EmptyProvider(CountingProvider):
    """A provider with nothing to give, as a closed market would have."""

    def fetch_candles(self, symbol, start, end, timeframe=TIMEFRAME):
        self.requested.append(start)

        return []


def pipeline(root: Path, provider: MarketDataProvider) -> DownloadPipeline:
    return DownloadPipeline(
        provider,
        output_root=root / "processed",
        manifest_root=root / "manifests",
        quality_root=root / "quality",
        checkpoint_root=root / "checkpoints",
        calendar=AlwaysOpenCalendar(),
        # One chunk per hour, so several chunks share the one month partition.
        chunk_size=DurationChunkSize(amount=1, unit="h"),
    )


def acquire(root: Path, provider: MarketDataProvider, **kwargs):
    return pipeline(root, provider).run(
        symbol=SYMBOL,
        start=START,
        end=END,
        timeframe=TIMEFRAME,
        **kwargs,
    )


def partitions(root: Path) -> list[Path]:
    return ParquetStorage(root / "processed").partition_files(
        symbol=SYMBOL,
        timeframe=TIMEFRAME,
    )


def validate(root: Path):
    return validate_dataset(
        symbol=SYMBOL,
        timeframe=TIMEFRAME,
        data_root=root / "processed",
        manifest_root=None,
        calendar=AlwaysOpenCalendar(),
        start=START,
        end=END,
    )


@pytest.fixture
def acquired(tmp_path):
    """A complete four-hour dataset, acquired in four hourly chunks."""
    result = acquire(tmp_path, CountingProvider())

    assert result.chunks_completed == 4
    assert result.retained_count == 240

    return tmp_path


class TestStoredDataDecidesResume:
    def test_an_intact_dataset_is_still_skipped_entirely(self, acquired):
        provider = CountingProvider()
        result = acquire(acquired, provider)

        assert result.chunks_skipped == 4
        assert result.chunks_repaired == []
        assert provider.requested == []
        assert result.retained_count == 240

    def test_a_lost_partition_is_re_acquired_rather_than_skipped(self, acquired):
        partitions(acquired)[0].unlink()

        provider = CountingProvider()
        result = acquire(acquired, provider)

        # Every chunk of that month lost its data, so every chunk re-runs.
        assert provider.requested != []
        assert result.chunks_repaired == [0, 1, 2, 3]
        assert result.chunks_skipped == 0

    def test_the_re_acquired_dataset_is_whole_again(self, acquired):
        before = validate(acquired)
        partitions(acquired)[0].unlink()

        assert validate(acquired).candles == 0

        acquire(acquired, CountingProvider())
        after = validate(acquired)

        assert after.candles == before.candles == 240
        assert after.status is before.status
        assert after.missing_candles == 0

    def test_the_old_behaviour_would_have_reported_a_clean_run(self, acquired):
        # The defect this guards: the checkpoint said complete, so the run
        # skipped the chunk and reported success over a hole. Nothing but the
        # stored data can tell the difference.
        partitions(acquired)[0].unlink()

        result = acquire(acquired, CountingProvider())

        assert result.chunks_failed == 0
        assert result.retained_count == 240
        assert validate(acquired).missing_candles == 0

    def test_a_chunk_that_stored_nothing_is_not_re_downloaded_forever(self, tmp_path):
        # A window the market was closed for legitimately has no rows. Asking
        # for it again on every run would be worse than useless.
        first = EmptyProvider()
        acquire(tmp_path, first)

        assert len(first.requested) == 4

        second = EmptyProvider()
        result = acquire(tmp_path, second)

        assert second.requested == []
        assert result.chunks_skipped == 4
        assert result.chunks_repaired == []

    def test_reconciliation_is_skipped_when_resume_is_off(self, acquired):
        provider = CountingProvider()
        result = acquire(acquired, provider, resume=False)

        # Nothing is "repaired" because nothing was believed complete.
        assert result.chunks_repaired == []
        assert result.chunks_skipped == 0
        assert len(provider.requested) == 4


class TestCorruptPartitionRecovery:
    def test_a_corrupt_partition_counts_as_no_stored_data(self, acquired):
        partitions(acquired)[0].write_bytes(b"not a parquet file")

        storage = ParquetStorage(acquired / "processed")

        assert (
            storage.has_stored_rows(
                symbol=SYMBOL,
                timeframe=TIMEFRAME,
                start=START,
                end=END,
            )
            is False
        )

    def test_deleting_a_corrupt_partition_restores_it_on_the_next_run(self, acquired):
        # Before this reconciliation, deleting the file achieved nothing: the
        # chunk stayed "complete" and the month never came back without
        # re-downloading the entire request.
        partitions(acquired)[0].write_bytes(b"not a parquet file")
        partitions(acquired)[0].unlink()

        result = acquire(acquired, CountingProvider())

        assert result.chunks_repaired == [0, 1, 2, 3]
        assert validate(acquired).candles == 240


class TestHasStoredRows:
    def test_an_unwritten_dataset_holds_nothing(self, tmp_path):
        storage = ParquetStorage(tmp_path / "processed")

        assert storage.has_stored_rows(symbol=SYMBOL, timeframe=TIMEFRAME) is False

    def test_a_written_dataset_holds_rows(self, acquired):
        storage = ParquetStorage(acquired / "processed")

        assert storage.has_stored_rows(symbol=SYMBOL, timeframe=TIMEFRAME) is True

    def test_a_range_outside_the_data_holds_nothing(self, acquired):
        storage = ParquetStorage(acquired / "processed")

        assert (
            storage.has_stored_rows(
                symbol=SYMBOL,
                timeframe=TIMEFRAME,
                start=datetime(2030, 1, 1, tzinfo=UTC),
                end=datetime(2030, 2, 1, tzinfo=UTC),
            )
            is False
        )

    def test_it_reads_metadata_rather_than_rows(self, acquired, monkeypatch):
        # Asked once per chunk of a multi-year acquisition, so it must not
        # cost a pass over the data.
        import pyarrow.parquet as pq

        def refuse(*args, **kwargs):
            raise AssertionError("has_stored_rows must not read any rows")

        monkeypatch.setattr(pq, "read_table", refuse)
        monkeypatch.setattr(ParquetStorage, "read_partition", refuse)

        storage = ParquetStorage(acquired / "processed")

        assert storage.has_stored_rows(symbol=SYMBOL, timeframe=TIMEFRAME) is True
