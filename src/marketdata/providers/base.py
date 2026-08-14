from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import datetime

from marketdata.models.candle import Candle


class MarketDataProvider(ABC):
    """Abstract interface for historical market-data providers."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the provider name."""
        raise NotImplementedError

    @abstractmethod
    def get_supported_symbols(self) -> Sequence[str]:
        """Return symbols supported by this provider."""
        raise NotImplementedError

    @abstractmethod
    def fetch_candles(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "1min",
    ) -> list[Candle]:
        """Fetch candles for a symbol and UTC time range."""
        raise NotImplementedError

    @abstractmethod
    def health_check(self) -> bool:
        """Return True when the provider is reachable and usable."""
        raise NotImplementedError
