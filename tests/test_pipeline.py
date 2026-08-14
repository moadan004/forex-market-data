from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

from marketdata.models.candle import Candle
from marketdata.normalization.timestamps import normalize_candles
from marketdata.storage.parquet import ParquetStorage
from marketdata.validation.candles import (
    CandleValidationError,
    deduplicate_candles,
    validate_candle,
)


def make_candle(
    timestamp: datetime,
    *,
    close: str = "1.1705",
) -> Candle:
    return Candle(
        timestamp=timestamp,
        symbol="EUR/USD",
        open=Decimal("1.1700"),
        high=Decimal("1.1710"),
        low=Decimal("1.1690"),
        close=Decimal(close),
        volume=Decimal(100),
    )


def test_normalize_candles_to_utc():
    utc_plus_3 = datetime(
        2026,
        8,
        14,
        15,
        0,
        tzinfo=__import__("datetime").timezone(timedelta(hours=3)),
    )

    candle = make_candle(utc_plus_3)

    result = normalize_candles([candle])

    assert result[0].timestamp == datetime(
        2026,
        8,
        14,
        12,
        0,
        tzinfo=UTC,
    )


def test_invalid_ohlc_is_rejected():
    candle = Candle(
        timestamp=datetime(2026, 8, 14, 12, 0, tzinfo=UTC),
        symbol="EUR/USD",
        open="1.1700",
        high="1.1600",
        low="1.1690",
        close="1.1705",
        volume="100",
    )

    with pytest.raises(CandleValidationError, match="high"):
        validate_candle(candle)


def test_duplicate_candles_are_removed():
    timestamp = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)

    first = make_candle(timestamp, close="1.1705")
    second = make_candle(timestamp, close="1.1710")

    result = deduplicate_candles([first, second])

    assert len(result) == 1
    assert result[0].close == Decimal("1.1710")


def test_parquet_storage(tmp_path):
    candles = [
        make_candle(datetime(2026, 8, 14, 12, 0, tzinfo=UTC)),
        make_candle(datetime(2026, 8, 14, 12, 1, tzinfo=UTC)),
    ]

    storage = ParquetStorage(tmp_path)

    files = storage.write(
        candles,
        symbol="EUR/USD",
        timeframe="1min",
    )

    assert len(files) == 1
    assert files[0].exists()

    table = pq.read_table(files[0])

    assert table.num_rows == 2
    assert "timestamp" in table.column_names
    assert "open" in table.column_names
    assert "close" in table.column_names
