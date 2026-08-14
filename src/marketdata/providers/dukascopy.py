from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any, ClassVar, Self

import httpx

from marketdata.models.candle import Candle
from marketdata.providers.base import MarketDataProvider
from marketdata.providers.errors import (
    ProviderDataError,
    ProviderError,
    classify_http_error,
)
from marketdata.providers.rate_limit import (
    RateLimit,
    RateLimiter,
    RateLimitStats,
)
from marketdata.providers.retry import RetryExecutor, RetryPolicy, RetryStats


class DukascopyError(ProviderDataError):
    """
    Raised when Dukascopy answers but cannot provide the requested data.

    An unsupported symbol or an unusable payload will not change on a second
    attempt, so this is a permanent failure. Transport and status failures
    are classified separately in :mod:`marketdata.providers.errors`.
    """


class DukascopyProvider(MarketDataProvider):
    """
    Dukascopy historical market-data provider.

    The public API exposes:
      - instrumentList
      - historicalPrices

    historicalPrices supports a maximum count of 5000 candles per request.
    """

    DEFAULT_BASE_URL = "https://freeserv.dukascopy.com/2.0/"
    MAX_COUNT = 5000
    SUPPORTED_TIMEFRAMES: ClassVar[frozenset[str]] = frozenset(
        {
            "1min",
            "10sec",
            "10m",
            "1hour",
            "1day",
            "1day_eet",
        }
    )

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 30.0,
        client: httpx.Client | None = None,
        retry_policy: RetryPolicy | None = None,
        retry_executor: RetryExecutor | None = None,
        rate_limit: RateLimit | None = None,
        rate_limiter: RateLimiter | None = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/") + "/"
        self._timeout = timeout
        self._client = client
        self._owns_client = client is None
        self._instrument_cache: dict[str, int] = {}
        self._retries = retry_executor or RetryExecutor(retry_policy)
        self._rate_limiter = rate_limiter or RateLimiter(rate_limit)

    @property
    def name(self) -> str:
        return "dukascopy"

    @property
    def retry_policy(self) -> RetryPolicy:
        return self._retries.policy

    @property
    def retry_stats(self) -> RetryStats:
        """Retries performed since the last reset, for the caller to record."""
        return self._retries.stats

    @property
    def rate_limit(self) -> RateLimit:
        return self._rate_limiter.limit

    @property
    def rate_limit_stats(self) -> RateLimitStats:
        """Throttling observed so far, for the caller to report."""
        return self._rate_limiter.stats

    def _get_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                timeout=self._timeout,
                headers={"User-Agent": "forex-market-data/0.1.0"},
            )
        return self._client

    def close(self) -> None:
        """Close the underlying HTTP client when this provider owns it."""
        if self._owns_client and self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def _send(self, path: str, request_params: dict[str, Any]) -> Any:
        """
        Issue one HTTP request and decode it, classifying any failure.

        Every request passes the rate limiter first. This sits inside the
        retried operation on purpose: a retry is another request against an
        endpoint that has just failed, and letting it skip the queue would
        defeat the limit exactly when it matters most.
        """
        context = f"Dukascopy request failed for {path}"

        self._rate_limiter.acquire()

        try:
            response = self._get_client().get(
                self.base_url,
                params={
                    "path": f"api/{path}",
                    **request_params,
                },
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise classify_http_error(exc, context=context) from exc

        try:
            return response.json()
        except ValueError as exc:
            raise DukascopyError(f"Dukascopy returned invalid JSON for {path}") from exc

    def _request(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """
        Issue a request, retrying transient failures.

        Retries wrap the whole exchange rather than the socket alone, so a
        429 or a 5xx is retried the same way a dropped connection is.
        """
        request_params = dict(params or {})

        if self.api_key:
            request_params["key"] = self.api_key

        return self._retries(lambda: self._send(path, request_params))

    def get_supported_symbols(self) -> Sequence[str]:
        """Return all symbol names exposed by Dukascopy."""
        payload = self._request(
            "instrumentList",
            {"fields": "id,name,pipValue,nameLong"},
        )

        if not isinstance(payload, list):
            raise DukascopyError("Unexpected instrumentList response")

        symbols: list[str] = []

        for item in payload:
            if not isinstance(item, dict):
                continue

            name = item.get("name")
            if isinstance(name, str) and name.strip():
                symbols.append(name.strip().upper())

        return sorted(set(symbols))

    def _resolve_symbol(self, symbol: str) -> int:
        normalized = symbol.strip().upper()

        if normalized in self._instrument_cache:
            return self._instrument_cache[normalized]

        payload = self._request(
            "instrumentList",
            {"fields": "id,name,pipValue,nameLong"},
        )

        if not isinstance(payload, list):
            raise DukascopyError("Unexpected instrumentList response")

        for item in payload:
            if not isinstance(item, dict):
                continue

            name = item.get("name")
            instrument_id = item.get("id")

            if (
                isinstance(name, str)
                and name.strip().upper() == normalized
                and isinstance(instrument_id, int)
            ):
                self._instrument_cache[normalized] = instrument_id
                return instrument_id

        raise DukascopyError(f"Unsupported Dukascopy symbol: {normalized}")

    @staticmethod
    def _validate_range(start: datetime, end: datetime) -> None:
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("start and end must be timezone-aware")

        if start >= end:
            raise ValueError("start must be before end")

    @staticmethod
    def _to_milliseconds(value: datetime) -> int:
        return int(value.timestamp() * 1000)

    @staticmethod
    def _from_milliseconds(value: int) -> datetime:
        return datetime.fromtimestamp(value / 1000, tz=__import__("datetime").UTC)

    @staticmethod
    def _parse_candle(item: dict[str, Any], symbol: str) -> Candle:
        timestamp = item.get("timestamp")

        required = (
            timestamp,
            item.get("bid_open"),
            item.get("bid_high"),
            item.get("bid_low"),
            item.get("bid_close"),
        )

        if any(value is None for value in required):
            raise DukascopyError(f"Incomplete candle payload: {item}")

        return Candle(
            timestamp=DukascopyProvider._from_milliseconds(int(timestamp)),
            symbol=symbol,
            open=item["bid_open"],
            high=item["bid_high"],
            low=item["bid_low"],
            close=item["bid_close"],
            volume=item.get("volume", 0),
        )

    def fetch_candles(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "1min",
    ) -> list[Candle]:
        self._validate_range(start, end)

        if timeframe not in self.SUPPORTED_TIMEFRAMES:
            raise ValueError(
                f"Unsupported timeframe: {timeframe}. "
                f"Supported: {sorted(self.SUPPORTED_TIMEFRAMES)}"
            )

        instrument_id = self._resolve_symbol(symbol)
        normalized_symbol = symbol.strip().upper()

        start_ms = self._to_milliseconds(start)
        current_end_ms = self._to_milliseconds(end)

        collected: dict[int, Candle] = {}

        while current_end_ms > start_ms:
            payload = self._request(
                "historicalPrices",
                {
                    "instrument": instrument_id,
                    "timeFrame": timeframe,
                    "count": self.MAX_COUNT,
                    "start": start_ms,
                    "end": current_end_ms,
                    "dayStartTime": "UTC",
                    "offerSide": "B",
                },
            )

            if not isinstance(payload, dict):
                raise DukascopyError("Unexpected historicalPrices response")

            raw_candles = payload.get("candles", [])

            if not raw_candles:
                break

            page: list[Candle] = []

            for raw in raw_candles:
                if isinstance(raw, dict):
                    candle = self._parse_candle(raw, normalized_symbol)

                    if start <= candle.timestamp < end:
                        collected[candle.timestamp] = candle

                    page.append(candle)

            if not page:
                break

            oldest = min(candle.timestamp for candle in page)

            oldest_ms = self._to_milliseconds(oldest)

            if oldest_ms <= start_ms:
                break

            next_end_ms = oldest_ms - 1

            if next_end_ms >= current_end_ms:
                raise DukascopyError("Dukascopy pagination did not make progress")

            current_end_ms = next_end_ms

        return sorted(
            collected.values(),
            key=lambda candle: candle.timestamp,
        )

    def health_check(self) -> bool:
        try:
            self._request(
                "instrumentList",
                {"fields": "id,name"},
            )
        except ProviderError:
            return False

        return True


__all__ = [
    "DukascopyError",
    "DukascopyProvider",
]
