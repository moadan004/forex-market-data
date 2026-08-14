from datetime import UTC, datetime

import httpx
import pytest

from marketdata.models.candle import Candle
from marketdata.providers.dukascopy import DukascopyError, DukascopyProvider


def make_client(handler):
    return httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="https://freeserv.dukascopy.com/2.0/",
    )


def test_symbol_resolution_and_fetch():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.params)

        path = request.url.params["path"]

        if path == "api/instrumentList":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": 1,
                        "name": "EUR/USD",
                        "pipValue": 0.0001,
                        "nameLong": "Euro/US Dollar",
                    }
                ],
            )

        if path == "api/historicalPrices":
            return httpx.Response(
                200,
                json={
                    "candles": [
                        {
                            "timestamp": 1786708800000,
                            "bid_open": 1.1700,
                            "bid_high": 1.1710,
                            "bid_low": 1.1690,
                            "bid_close": 1.1705,
                        },
                        {
                            "timestamp": 1786708860000,
                            "bid_open": 1.1705,
                            "bid_high": 1.1715,
                            "bid_low": 1.1700,
                            "bid_close": 1.1710,
                        },
                    ]
                },
            )

        return httpx.Response(404)

    client = make_client(handler)

    with DukascopyProvider(client=client) as provider:
        candles = provider.fetch_candles(
            "EUR/USD",
            datetime(2026, 8, 14, 12, 0, tzinfo=UTC),
            datetime(2026, 8, 14, 13, 0, tzinfo=UTC),
        )

    assert len(candles) == 2
    assert all(isinstance(candle, Candle) for candle in candles)
    assert candles[0].symbol == "EUR/USD"
    assert candles[0].timestamp < candles[1].timestamp
    assert calls


def test_empty_response_returns_no_candles():
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.params["path"]

        if path == "api/instrumentList":
            return httpx.Response(
                200,
                json=[{"id": 1, "name": "EUR/USD"}],
            )

        if path == "api/historicalPrices":
            return httpx.Response(200, json={"candles": []})

        return httpx.Response(404)

    client = make_client(handler)

    with DukascopyProvider(client=client) as provider:
        candles = provider.fetch_candles(
            "EUR/USD",
            datetime(2026, 8, 14, 12, 0, tzinfo=UTC),
            datetime(2026, 8, 14, 13, 0, tzinfo=UTC),
        )

    assert candles == []


def test_naive_datetime_is_rejected():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[{"id": 1, "name": "EUR/USD"}],
        )

    client = make_client(handler)

    with (
        DukascopyProvider(client=client) as provider,
        pytest.raises(ValueError, match="timezone-aware"),
    ):
        provider.fetch_candles(
            "EUR/USD",
            datetime(2026, 8, 14, 12, 0),  # noqa: DTZ001
            datetime(2026, 8, 14, 13, 0, tzinfo=UTC),
        )


def test_invalid_range_is_rejected():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    client = make_client(handler)

    with (
        DukascopyProvider(client=client) as provider,
        pytest.raises(ValueError, match="before"),
    ):
        provider.fetch_candles(
            "EUR/USD",
            datetime(2026, 8, 14, 13, 0, tzinfo=UTC),
            datetime(2026, 8, 14, 12, 0, tzinfo=UTC),
        )


def test_unknown_symbol_is_rejected():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[{"id": 1, "name": "GBP/USD"}],
        )

    client = make_client(handler)

    with (
        DukascopyProvider(client=client) as provider,
        pytest.raises(DukascopyError, match="Unsupported"),
    ):
        provider.fetch_candles(
            "EUR/USD",
            datetime(2026, 8, 14, 12, 0, tzinfo=UTC),
            datetime(2026, 8, 14, 13, 0, tzinfo=UTC),
        )
