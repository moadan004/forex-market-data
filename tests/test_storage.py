from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from marketdata.models.candle import Candle
from marketdata.storage.parquet import (
    CANDLE_SCHEMA,
    ParquetStorage,
    normalize_symbol_path,
)


def make_candle(
    timestamp: datetime,
    *,
    close: str = "1.1705",
    volume: str = "100",
) -> Candle:
    return Candle(
        timestamp=timestamp,
        symbol="EUR/USD",
        open=Decimal("1.1700"),
        high=Decimal("1.1710"),
        low=Decimal("1.1690"),
        close=Decimal(close),
        volume=Decimal(volume),
    )


def test_symbol_path_is_filesystem_safe():
    assert normalize_symbol_path("eur/usd") == "EUR_USD"


def test_partitioning_scheme(tmp_path):
    storage = ParquetStorage(tmp_path)

    files = storage.write(
        [make_candle(datetime(2026, 8, 14, 12, 0, tzinfo=UTC))],
        symbol="EUR/USD",
        timeframe="1min",
    )

    relative = files[0].relative_to(tmp_path)

    assert relative.parts == (
        "EUR_USD",
        "timeframe=1min",
        "year=2026",
        "month=08",
        "candles.parquet",
    )


def test_written_files_use_the_canonical_schema(tmp_path):
    storage = ParquetStorage(tmp_path)

    files = storage.write(
        [make_candle(datetime(2026, 8, 14, 12, 0, tzinfo=UTC))],
        symbol="EUR/USD",
        timeframe="1min",
    )

    assert pq.read_table(files[0]).schema.equals(CANDLE_SCHEMA)


def test_months_share_a_compatible_schema(tmp_path):
    """Inferred decimal types differ per file; the fixed schema must not."""
    storage = ParquetStorage(tmp_path)

    files = storage.write(
        [
            make_candle(datetime(2026, 8, 31, 23, 59, tzinfo=UTC), close="1.1705"),
            make_candle(datetime(2026, 9, 1, 0, 0, tzinfo=UTC), close="123.456789"),
        ],
        symbol="EUR/USD",
        timeframe="1min",
    )

    assert len(files) == 2

    combined = pa.concat_tables([pq.read_table(path) for path in files])

    assert combined.num_rows == 2


def test_read_back_returns_stored_candles(tmp_path):
    storage = ParquetStorage(tmp_path)

    candles = [
        make_candle(datetime(2026, 8, 14, 12, 0, tzinfo=UTC)),
        make_candle(datetime(2026, 9, 14, 12, 1, tzinfo=UTC), close="1.1720"),
    ]

    storage.write(candles, symbol="EUR/USD", timeframe="1min")

    table = storage.read_table(symbol="EUR/USD", timeframe="1min")

    assert table.num_rows == 2

    restored = storage.read_candles(symbol="EUR/USD", timeframe="1min")

    assert [candle.timestamp for candle in restored] == [
        candle.timestamp for candle in candles
    ]
    assert restored[1].close == Decimal("1.1720")
    assert restored[0].timestamp.tzinfo is not None
    assert restored[0].timestamp.utcoffset().total_seconds() == 0


def test_read_back_exposes_hive_partitions(tmp_path):
    storage = ParquetStorage(tmp_path)

    storage.write(
        [make_candle(datetime(2026, 8, 14, 12, 0, tzinfo=UTC))],
        symbol="EUR/USD",
        timeframe="1min",
    )

    table = storage.read_table(symbol="EUR/USD")

    assert "timeframe" in table.column_names
    assert table.column("timeframe")[0].as_py() == "1min"
    assert table.column("year")[0].as_py() == 2026


def test_read_of_unknown_symbol_returns_empty_table(tmp_path):
    storage = ParquetStorage(tmp_path)

    table = storage.read_table(symbol="GBP/USD")

    assert table.num_rows == 0
    assert table.schema.equals(CANDLE_SCHEMA)
    assert storage.read_candles(symbol="GBP/USD") == []


def test_high_precision_values_are_quantized(tmp_path):
    """Provider floats expand to more digits than the stored scale allows."""
    storage = ParquetStorage(tmp_path)

    candle = Candle(
        timestamp=datetime(2026, 8, 14, 12, 0, tzinfo=UTC),
        symbol="EUR/USD",
        open="1.1700000000000001",
        high="1.1710000000000002",
        low="1.1690000000000001",
        close="1.1705000000000003",
        volume="0.30000000000000004",
    )

    storage.write([candle], symbol="EUR/USD", timeframe="1min")

    restored = storage.read_candles(symbol="EUR/USD", timeframe="1min")

    assert restored[0].open == Decimal("1.17000000")
    assert restored[0].volume == Decimal("0.30000000")


def test_write_of_empty_dataset_creates_nothing(tmp_path):
    storage = ParquetStorage(tmp_path)

    assert storage.write([], symbol="EUR/USD", timeframe="1min") == []
    assert list(tmp_path.iterdir()) == []


def test_partition_files_are_listed_in_order(tmp_path):
    storage = ParquetStorage(tmp_path)

    storage.write(
        [
            make_candle(datetime(2026, 9, 1, tzinfo=UTC)),
            make_candle(datetime(2026, 8, 1, tzinfo=UTC)),
            make_candle(datetime(2027, 1, 1, tzinfo=UTC)),
        ],
        symbol="EUR/USD",
        timeframe="1min",
    )

    files = storage.partition_files(symbol="EUR/USD", timeframe="1min")

    assert [path.parent.parent.name + "/" + path.parent.name for path in files] == [
        "year=2026/month=08",
        "year=2026/month=09",
        "year=2027/month=01",
    ]


def test_partition_files_can_be_restricted_to_a_range(tmp_path):
    storage = ParquetStorage(tmp_path)

    storage.write(
        [
            make_candle(datetime(2026, 8, 15, tzinfo=UTC)),
            make_candle(datetime(2026, 9, 15, tzinfo=UTC)),
            make_candle(datetime(2026, 10, 15, tzinfo=UTC)),
        ],
        symbol="EUR/USD",
        timeframe="1min",
    )

    selected = storage.partition_files(
        symbol="EUR/USD",
        timeframe="1min",
        start=datetime(2026, 9, 10, tzinfo=UTC),
        end=datetime(2026, 9, 20, tzinfo=UTC),
    )

    assert [path.parent.name for path in selected] == ["month=09"]


def test_a_partition_touching_the_range_edge_is_included(tmp_path):
    """Selection is by whole month, because that is the write granularity."""
    storage = ParquetStorage(tmp_path)

    storage.write(
        [
            make_candle(datetime(2026, 8, 31, 23, 59, tzinfo=UTC)),
            make_candle(datetime(2026, 9, 1, 0, 0, tzinfo=UTC)),
        ],
        symbol="EUR/USD",
        timeframe="1min",
    )

    selected = storage.partition_files(
        symbol="EUR/USD",
        timeframe="1min",
        start=datetime(2026, 8, 31, 23, 0, tzinfo=UTC),
        end=datetime(2026, 9, 1, 1, 0, tzinfo=UTC),
    )

    assert len(selected) == 2


def test_partition_files_of_an_unknown_symbol_is_empty(tmp_path):
    assert ParquetStorage(tmp_path).partition_files(symbol="GBP/USD") == []


def test_partition_schemas_are_reported_per_file(tmp_path):
    storage = ParquetStorage(tmp_path)

    storage.write(
        [make_candle(datetime(2026, 8, 14, 12, 0, tzinfo=UTC))],
        symbol="EUR/USD",
        timeframe="1min",
    )

    schemas = storage.partition_schemas(symbol="EUR/USD", timeframe="1min")

    assert len(schemas) == 1
    assert next(iter(schemas.values())).equals(CANDLE_SCHEMA)
