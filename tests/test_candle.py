from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from marketdata.models.candle import Candle


def test_candle_accepts_valid_data():
    candle = Candle(
        timestamp=datetime(2026, 8, 14, 12, 0, tzinfo=UTC),
        symbol="eurusd",
        open="1.17000",
        high="1.17100",
        low="1.16900",
        close="1.17050",
        volume="1000",
    )

    assert candle.symbol == "EURUSD"
    assert candle.open == Decimal("1.17000")
    assert candle.high == Decimal("1.17100")
    assert candle.low == Decimal("1.16900")
    assert candle.close == Decimal("1.17050")
    assert candle.volume == Decimal(1000)


def test_candle_rejects_naive_timestamp():
    with pytest.raises(ValidationError, match="timezone-aware"):
        Candle(
            timestamp=datetime(2026, 8, 14, 12, 0),  # noqa: DTZ001,
            symbol="EURUSD",
            open="1.17",
            high="1.18",
            low="1.16",
            close="1.17",
        )
