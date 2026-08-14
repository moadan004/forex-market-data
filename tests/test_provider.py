from datetime import UTC, datetime

import pytest

from marketdata.models.candle import Candle
from marketdata.providers.base import MarketDataProvider


class FakeProvider(MarketDataProvider):
    @property
    def name(self) -> str:
        return "fake"

    def get_supported_symbols(self) -> list[str]:
        return ["EURUSD", "GBPUSD"]

    def fetch_candles(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "1min",
    ) -> list[Candle]:
        return [
            Candle(
                timestamp=start,
                symbol=symbol,
                open="1.1000",
                high="1.1010",
                low="1.0990",
                close="1.1005",
                volume="100",
            )
        ]

    def health_check(self) -> bool:
        return True


def test_provider_contract():
    provider = FakeProvider()

    assert provider.name == "fake"
    assert provider.get_supported_symbols() == ["EURUSD", "GBPUSD"]
    assert provider.health_check() is True


def test_provider_returns_candles():
    provider = FakeProvider()

    start = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
    end = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)

    candles = provider.fetch_candles("EURUSD", start, end)

    assert len(candles) == 1
    assert isinstance(candles[0], Candle)
    assert candles[0].symbol == "EURUSD"
    assert candles[0].timestamp == start


def test_abstract_provider_cannot_be_instantiated():
    with pytest.raises(TypeError):
        MarketDataProvider()
