"""Provider preflight, against both offline and mocked-network providers."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from conftest import instant_limiter

from marketdata.providers.csv import CsvMarketDataProvider
from marketdata.providers.dukascopy import DukascopyProvider
from marketdata.providers.retry import RetryPolicy
from marketdata.verification.preflight import (
    check_provider,
    classify_failure,
    default_window,
)
from marketdata.verification.status import VerificationStatus

FIXTURES = Path(__file__).parent / "fixtures" / "csv"

START = datetime(2026, 8, 10, 0, 0, tzinfo=UTC)
HOUR = timedelta(hours=1)
END = START + HOUR

INSTRUMENTS = [{"id": 1, "name": "EUR/USD", "pipValue": 0.0001}]
MINUTE_MS = 60_000
INSTANT = RetryPolicy(attempts=2, backoff_seconds=0)


def candles_payload(start: datetime, end: datetime) -> dict:
    """Build a well-formed historicalPrices payload for a window."""
    candles = []
    moment = start

    while moment < end:
        candles.append(
            {
                "timestamp": int(moment.timestamp() * 1000),
                "bid_open": 1.17,
                "bid_high": 1.1710,
                "bid_low": 1.1690,
                "bid_close": 1.1705,
            }
        )
        moment += timedelta(minutes=1)

    return {"candles": candles}


def dukascopy(handler) -> DukascopyProvider:
    """A Dukascopy provider whose transport is a stub, never the network."""
    return DukascopyProvider(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        retry_policy=INSTANT,
        rate_limiter=instant_limiter(),
    )


def healthy_handler(request: httpx.Request) -> httpx.Response:
    params = request.url.params

    if params["path"] == "api/instrumentList":
        return httpx.Response(200, json=INSTRUMENTS)

    window_start = datetime.fromtimestamp(int(params["start"]) / 1000, tz=UTC)
    window_end = datetime.fromtimestamp(int(params["end"]) / 1000, tz=UTC)

    return httpx.Response(200, json=candles_payload(window_start, window_end))


def status_handler(code: int):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(code)

    return handler


def check(provider, **kwargs):
    kwargs.setdefault("symbol", "EUR/USD")
    kwargs.setdefault("start", START)
    kwargs.setdefault("end", END)

    return check_provider(provider, **kwargs)


def statuses(report) -> dict[str, VerificationStatus]:
    return {result.name: result.status for result in report.checks}


# --------------------------------------------------------------------------
# A provider that works
# --------------------------------------------------------------------------


def test_a_healthy_offline_provider_passes():
    report = check(CsvMarketDataProvider(FIXTURES / "dataset"))

    assert report.status is VerificationStatus.PASS
    assert report.passed is True
    assert report.candles == 60
    assert set(statuses(report).values()) == {VerificationStatus.PASS}


def test_every_question_is_asked_and_recorded():
    report = check(CsvMarketDataProvider(FIXTURES / "dataset"))

    assert [result.name for result in report.checks] == [
        "reachable",
        "symbol_accepted",
        "timeframe_accepted",
        "candles_returned",
        "candles_parsed",
        "timestamps_utc",
        "ohlc_valid",
        "pagination",
    ]


def test_a_mocked_dukascopy_transport_passes_the_client_checks():
    """This proves the client code, not the provider."""
    report = check(dukascopy(healthy_handler))

    assert report.status is VerificationStatus.PASS
    assert report.provider == "dukascopy"
    assert report.candles == 60


def test_the_report_records_the_configuration_without_secrets():
    report = check(dukascopy(healthy_handler))

    assert report.provider_configuration["api_key"] == "unset"
    assert "SECRET" not in report.model_dump_json()


def test_a_provider_without_a_symbol_list_warns_rather_than_failing():
    report = check(CsvMarketDataProvider(FIXTURES / "no_volume.csv"), end=START + HOUR)

    assert statuses(report)["symbol_accepted"] is VerificationStatus.WARN
    assert report.status is VerificationStatus.WARN
    assert report.passed is True


# --------------------------------------------------------------------------
# A provider that cannot be reached
# --------------------------------------------------------------------------


def test_a_403_is_blocked_not_failed():
    """A policy denial says nothing about whether the data would be good."""
    report = check(dukascopy(status_handler(403)))

    assert report.status is VerificationStatus.BLOCKED
    assert report.blocked is True
    assert report.passed is False
    assert "refused by policy" in report.checks[0].detail


def test_a_401_is_blocked():
    report = check(dukascopy(status_handler(401)))

    assert report.status is VerificationStatus.BLOCKED


def test_a_500_is_blocked_after_retries():
    report = check(dukascopy(status_handler(500)))

    assert report.status is VerificationStatus.BLOCKED
    assert "unreachable after retries" in report.checks[0].detail


def test_a_timeout_is_blocked():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow")

    report = check(dukascopy(handler))

    assert report.status is VerificationStatus.BLOCKED
    assert "unreachable" in report.checks[0].detail


def test_a_connection_error_is_blocked():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    report = check(dukascopy(handler))

    assert report.status is VerificationStatus.BLOCKED


def test_a_proxy_denial_is_blocked():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ProxyError("403 Forbidden")

    report = check(dukascopy(handler))

    assert report.status is VerificationStatus.BLOCKED
    assert report.candles == 0


def test_a_missing_csv_source_is_blocked_and_says_why():
    report = check(CsvMarketDataProvider(FIXTURES / "nowhere"))

    assert report.status is VerificationStatus.BLOCKED
    assert statuses(report)["reachable"] is VerificationStatus.BLOCKED
    assert "does not exist" in report.checks[0].detail


# --------------------------------------------------------------------------
# A provider that answers with something unusable
# --------------------------------------------------------------------------


def test_a_malformed_response_fails():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["path"] == "api/instrumentList":
            return httpx.Response(200, json=INSTRUMENTS)

        return httpx.Response(200, content=b"not json at all")

    report = check(dukascopy(handler))

    assert report.status is VerificationStatus.FAIL
    assert statuses(report)["timeframe_accepted"] is VerificationStatus.FAIL


def test_an_incomplete_candle_payload_fails():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["path"] == "api/instrumentList":
            return httpx.Response(200, json=INSTRUMENTS)

        return httpx.Response(200, json={"candles": [{"timestamp": 1786708800000}]})

    report = check(dukascopy(handler))

    assert report.status is VerificationStatus.FAIL


def test_an_empty_response_fails():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["path"] == "api/instrumentList":
            return httpx.Response(200, json=INSTRUMENTS)

        return httpx.Response(200, json={"candles": []})

    report = check(dukascopy(handler))

    assert report.status is VerificationStatus.FAIL
    assert statuses(report)["candles_returned"] is VerificationStatus.FAIL


def test_an_unknown_symbol_fails():
    report = check(dukascopy(healthy_handler), symbol="USD/JPY")

    assert report.status is VerificationStatus.FAIL
    assert statuses(report)["symbol_accepted"] is VerificationStatus.FAIL


def test_invalid_ohlc_fails():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["path"] == "api/instrumentList":
            return httpx.Response(200, json=INSTRUMENTS)

        return httpx.Response(
            200,
            json={
                "candles": [
                    {
                        "timestamp": int(START.timestamp() * 1000),
                        "bid_open": 1.17,
                        "bid_high": 1.1600,
                        "bid_low": 1.1690,
                        "bid_close": 1.1705,
                    }
                ]
            },
        )

    report = check(dukascopy(handler))

    assert statuses(report)["ohlc_valid"] is VerificationStatus.FAIL
    assert report.status is VerificationStatus.FAIL


def test_out_of_range_timestamps_fail():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["path"] == "api/instrumentList":
            return httpx.Response(200, json=INSTRUMENTS)

        return httpx.Response(200, json=candles_payload(START, END))

    class LeakyProvider(DukascopyProvider):
        def fetch_candles(self, symbol, start, end, timeframe="1min"):
            candles = super().fetch_candles(symbol, START, END, timeframe)
            return candles

    provider = LeakyProvider(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        retry_policy=INSTANT,
        rate_limiter=instant_limiter(),
    )

    report = check_provider(
        provider,
        symbol="EUR/USD",
        start=START,
        end=START + timedelta(minutes=10),
    )

    assert statuses(report)["timestamps_utc"] is VerificationStatus.FAIL
    details = {result.name: result.detail for result in report.checks}

    assert "outside the requested window" in details["timestamps_utc"]


# --------------------------------------------------------------------------
# Pagination
# --------------------------------------------------------------------------


def test_pagination_that_loses_a_row_at_the_boundary_fails():
    """The split window must stitch back to the whole."""

    class LosingProvider(CsvMarketDataProvider):
        def fetch_candles(self, symbol, start, end, timeframe="1min"):
            candles = super().fetch_candles(symbol, start, end, timeframe)

            # Drop the first row of any window that is not the original ask.
            if start != START:
                return candles[1:]

            return candles

    report = check(LosingProvider(FIXTURES / "dataset"))

    assert statuses(report)["pagination"] is VerificationStatus.FAIL
    assert report.status is VerificationStatus.FAIL


def test_pagination_that_repeats_a_row_fails():
    class OverlappingProvider(CsvMarketDataProvider):
        def fetch_candles(self, symbol, start, end, timeframe="1min"):
            if start == START and end == END:
                return super().fetch_candles(symbol, start, end, timeframe)

            # Both halves return the whole window.
            return super().fetch_candles(symbol, START, END, timeframe)

    report = check(OverlappingProvider(FIXTURES / "dataset"))

    assert statuses(report)["pagination"] is VerificationStatus.FAIL


def test_a_window_too_small_to_split_warns():
    """A window of one instant holds a candle but cannot be halved."""
    report = check(
        CsvMarketDataProvider(FIXTURES / "dataset"),
        start=START,
        end=START + timedelta(microseconds=1),
    )

    assert report.candles == 1
    assert statuses(report)["candles_returned"] is VerificationStatus.PASS
    assert statuses(report)["pagination"] is VerificationStatus.WARN
    assert report.status is VerificationStatus.WARN


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def test_classify_failure_separates_blocked_from_failed():
    from marketdata.providers.errors import (
        ProviderAuthError,
        ProviderDataError,
        ProviderServerError,
    )

    assert classify_failure(ProviderAuthError("x"))[0] is VerificationStatus.BLOCKED
    assert classify_failure(ProviderServerError("x"))[0] is VerificationStatus.BLOCKED
    assert classify_failure(ProviderDataError("x"))[0] is VerificationStatus.FAIL


def test_the_default_window_is_recent_and_ends_before_now():
    start, end = default_window("1min")

    assert start < end
    assert end < datetime.now(UTC)
    assert end - start == timedelta(minutes=60)


def test_a_partial_window_is_rejected():
    with pytest.raises(ValueError, match="both start and end"):
        check_provider(
            CsvMarketDataProvider(FIXTURES / "dataset"),
            symbol="EUR/USD",
            start=START,
        )


def test_an_inverted_window_is_rejected():
    with pytest.raises(ValueError, match="start must be before end"):
        check_provider(
            CsvMarketDataProvider(FIXTURES / "dataset"),
            symbol="EUR/USD",
            start=END,
            end=START,
        )
