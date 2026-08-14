from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pyarrow.parquet as pq

from marketdata.models.candle import Candle
from marketdata.storage.parquet import ParquetStorage
from marketdata.validation.candles import merge_candles

START = datetime(2026, 8, 3, 0, 0, tzinfo=UTC)
MINUTE = timedelta(minutes=1)


def make_candle(index: int, *, close: str = "1.1705") -> Candle:
    return Candle(
        timestamp=START + MINUTE * index,
        symbol="EUR/USD",
        open=Decimal("1.1700"),
        high=Decimal("1.1710"),
        low=Decimal("1.1690"),
        close=Decimal(close),
        volume=Decimal(100),
    )


def write(storage: ParquetStorage, candles: list[Candle]) -> list:
    return storage.write(candles, symbol="EUR/USD", timeframe="1min")


def stored(storage: ParquetStorage) -> list[Candle]:
    return storage.read_candles(symbol="EUR/USD", timeframe="1min")


def test_initial_write(tmp_path):
    storage = ParquetStorage(tmp_path)

    files = write(storage, [make_candle(0), make_candle(1)])

    assert len(files) == 1
    assert len(stored(storage)) == 2


def test_second_write_with_new_candles_keeps_both(tmp_path):
    storage = ParquetStorage(tmp_path)

    write(storage, [make_candle(0), make_candle(1)])
    write(storage, [make_candle(2), make_candle(3)])

    candles = stored(storage)

    assert len(candles) == 4
    assert [candle.timestamp for candle in candles] == [
        START + MINUTE * index for index in range(4)
    ]


def test_second_write_with_duplicates_does_not_grow_the_partition(tmp_path):
    storage = ParquetStorage(tmp_path)

    write(storage, [make_candle(0), make_candle(1)])
    write(storage, [make_candle(0), make_candle(1)])

    assert len(stored(storage)) == 2


def test_rewriting_a_chunk_is_idempotent(tmp_path):
    """Re-running a completed chunk must leave the partition unchanged."""
    storage = ParquetStorage(tmp_path)
    candles = [make_candle(index) for index in range(10)]

    write(storage, candles)
    first = stored(storage)

    write(storage, candles)
    second = stored(storage)

    assert first == second


def test_overlapping_downloads_are_reconciled(tmp_path):
    storage = ParquetStorage(tmp_path)

    write(storage, [make_candle(index) for index in range(5)])
    write(storage, [make_candle(index) for index in range(3, 8)])

    candles = stored(storage)

    assert len(candles) == 8
    assert [candle.timestamp for candle in candles] == [
        START + MINUTE * index for index in range(8)
    ]


def test_new_candles_win_over_stored_ones(tmp_path):
    storage = ParquetStorage(tmp_path)

    write(storage, [make_candle(0, close="1.1705")])
    write(storage, [make_candle(0, close="1.1799")])

    candles = stored(storage)

    assert len(candles) == 1
    assert candles[0].close == Decimal("1.17990000")


def test_merged_partition_is_chronological(tmp_path):
    storage = ParquetStorage(tmp_path)

    write(storage, [make_candle(index) for index in (7, 3, 5)])
    write(storage, [make_candle(index) for index in (1, 9, 0)])

    timestamps = [candle.timestamp for candle in stored(storage)]

    assert timestamps == sorted(timestamps)
    assert len(timestamps) == 6


def test_merge_only_touches_the_months_being_written(tmp_path):
    storage = ParquetStorage(tmp_path)

    august = make_candle(0)
    september = Candle(
        timestamp=datetime(2026, 9, 1, 0, 0, tzinfo=UTC),
        symbol="EUR/USD",
        open=Decimal("1.1700"),
        high=Decimal("1.1710"),
        low=Decimal("1.1690"),
        close=Decimal("1.1705"),
        volume=Decimal(100),
    )

    write(storage, [august])
    files = write(storage, [september])

    assert len(files) == 1
    assert "month=09" in str(files[0])
    assert len(stored(storage)) == 2


def test_merge_can_be_disabled(tmp_path):
    storage = ParquetStorage(tmp_path)

    write(storage, [make_candle(0), make_candle(1)])
    storage.write(
        [make_candle(5)],
        symbol="EUR/USD",
        timeframe="1min",
        merge=False,
    )

    candles = stored(storage)

    assert len(candles) == 1
    assert candles[0].timestamp == START + MINUTE * 5


def test_partition_stays_readable_after_many_merges(tmp_path):
    storage = ParquetStorage(tmp_path)

    for index in range(20):
        write(storage, [make_candle(index)])

    path = tmp_path / "EUR_USD/timeframe=1min/year=2026/month=08/candles.parquet"
    table = pq.read_table(path)

    assert table.num_rows == 20
    assert list(tmp_path.rglob("*.tmp")) == []


def test_read_back_can_be_restricted_to_a_range(tmp_path):
    storage = ParquetStorage(tmp_path)

    write(storage, [make_candle(index) for index in range(10)])

    candles = storage.read_candles(
        symbol="EUR/USD",
        timeframe="1min",
        start=START + MINUTE * 2,
        end=START + MINUTE * 5,
    )

    assert [candle.timestamp for candle in candles] == [
        START + MINUTE * index for index in (2, 3, 4)
    ]


def test_read_timestamps_returns_ordered_utc_values(tmp_path):
    storage = ParquetStorage(tmp_path)

    write(storage, [make_candle(index) for index in (4, 0, 2)])

    timestamps = storage.read_timestamps(symbol="EUR/USD", timeframe="1min")

    assert timestamps == [START, START + MINUTE * 2, START + MINUTE * 4]
    assert all(value.tzinfo is not None for value in timestamps)


def test_read_timestamps_of_an_unknown_symbol_is_empty(tmp_path):
    storage = ParquetStorage(tmp_path)

    assert storage.read_timestamps(symbol="GBP/USD") == []


def test_merge_candles_prefers_incoming_and_sorts():
    existing = [make_candle(1, close="1.1000"), make_candle(0)]
    incoming = [make_candle(1, close="1.2000")]

    merged = merge_candles(existing, incoming)

    assert [candle.timestamp for candle in merged] == [START, START + MINUTE]
    assert merged[1].close == Decimal("1.2000")


def test_merge_candles_with_empty_inputs():
    assert merge_candles([], []) == []
    assert merge_candles([make_candle(0)], []) == [make_candle(0)]
    assert merge_candles([], [make_candle(0)]) == [make_candle(0)]
