"""Provider error classification and retry behaviour."""

from datetime import UTC, datetime

import httpx
import pytest
from conftest import instant_limiter

from marketdata.providers.dukascopy import DukascopyError, DukascopyProvider
from marketdata.providers.errors import (
    PermanentProviderError,
    ProviderAuthError,
    ProviderClientError,
    ProviderConnectionError,
    ProviderDataError,
    ProviderError,
    ProviderRateLimitError,
    ProviderServerError,
    ProviderTimeoutError,
    TransientProviderError,
    classify_http_error,
)
from marketdata.providers.retry import (
    RetryExecutor,
    RetryPolicy,
    RetryStats,
)

START = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
END = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)

# Every test in this module runs with no real waiting.
INSTANT = RetryPolicy(attempts=3, backoff_seconds=0)

INSTRUMENTS = [{"id": 1, "name": "EUR/USD", "pipValue": 0.0001}]
CANDLES = {
    "candles": [
        {
            "timestamp": int(START.timestamp() * 1000),
            "bid_open": 1.17,
            "bid_high": 1.1710,
            "bid_low": 1.1690,
            "bid_close": 1.1705,
        }
    ]
}


def status_error(code: int, headers: dict | None = None) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://example.test/")
    response = httpx.Response(code, headers=headers or {}, request=request)

    return httpx.HTTPStatusError("boom", request=request, response=response)


def make_provider(handler, *, policy: RetryPolicy = INSTANT) -> DukascopyProvider:
    return DukascopyProvider(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        retry_policy=policy,
        rate_limiter=instant_limiter(),
    )


# --------------------------------------------------------------------------
# Classification
# --------------------------------------------------------------------------


@pytest.mark.parametrize("code", [429, 500, 502, 503, 504, 408, 425])
def test_transient_status_codes(code):
    error = classify_http_error(status_error(code), context="ctx")

    assert isinstance(error, TransientProviderError)
    assert error.retryable is True


@pytest.mark.parametrize("code", [400, 401, 403, 404, 410, 422])
def test_permanent_status_codes(code):
    error = classify_http_error(status_error(code), context="ctx")

    assert isinstance(error, PermanentProviderError)
    assert error.retryable is False


def test_rate_limit_is_its_own_error():
    error = classify_http_error(status_error(429), context="ctx")

    assert isinstance(error, ProviderRateLimitError)
    assert "rate limited" in str(error)


def test_server_error_classification():
    assert isinstance(
        classify_http_error(status_error(503), context="ctx"),
        ProviderServerError,
    )


@pytest.mark.parametrize("code", [401, 403])
def test_auth_errors_are_not_retryable(code):
    error = classify_http_error(status_error(code), context="ctx")

    assert isinstance(error, ProviderAuthError)
    assert error.retryable is False
    assert "not authorized" in str(error)


def test_bad_request_is_a_client_error():
    assert isinstance(
        classify_http_error(status_error(400), context="ctx"),
        ProviderClientError,
    )


def test_retry_after_is_captured():
    error = classify_http_error(
        status_error(429, {"Retry-After": "12"}),
        context="ctx",
    )

    assert error.retry_after == 12


def test_unparseable_retry_after_is_ignored():
    error = classify_http_error(
        status_error(429, {"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}),
        context="ctx",
    )

    assert error.retry_after is None


@pytest.mark.parametrize(
    "exception",
    [
        httpx.ConnectTimeout("slow"),
        httpx.ReadTimeout("slow"),
        httpx.PoolTimeout("slow"),
    ],
)
def test_timeouts_are_transient(exception):
    error = classify_http_error(exception, context="ctx")

    assert isinstance(error, ProviderTimeoutError)
    assert error.retryable is True


@pytest.mark.parametrize(
    "exception",
    [
        httpx.ConnectError("refused"),
        httpx.ReadError("cut"),
        httpx.RemoteProtocolError("truncated"),
    ],
)
def test_connection_failures_are_transient(exception):
    error = classify_http_error(exception, context="ctx")

    assert isinstance(error, ProviderConnectionError)
    assert error.retryable is True


def test_proxy_rejection_is_permanent():
    """An egress policy denial will not change however often we ask."""
    error = classify_http_error(httpx.ProxyError("403 Forbidden"), context="ctx")

    assert isinstance(error, ProviderAuthError)
    assert error.retryable is False


def test_unsupported_protocol_is_permanent():
    assert isinstance(
        classify_http_error(httpx.UnsupportedProtocol("nope"), context="ctx"),
        PermanentProviderError,
    )


def test_classification_keeps_the_context():
    error = classify_http_error(status_error(500), context="fetching candles")

    assert "fetching candles" in str(error)


def test_dukascopy_data_errors_are_permanent():
    assert issubclass(DukascopyError, ProviderDataError)
    assert issubclass(DukascopyError, PermanentProviderError)
    assert DukascopyError("x").retryable is False


# --------------------------------------------------------------------------
# Retry policy and executor
# --------------------------------------------------------------------------


def test_policy_rejects_unbounded_and_negative_settings():
    with pytest.raises(ValueError, match="attempts must be at least 1"):
        RetryPolicy(attempts=0)

    with pytest.raises(ValueError, match="backoff_seconds cannot be negative"):
        RetryPolicy(backoff_seconds=-1)

    with pytest.raises(ValueError, match="max_backoff_seconds cannot be negative"):
        RetryPolicy(max_backoff_seconds=-1)

    with pytest.raises(ValueError, match="multiplier must be at least 1"):
        RetryPolicy(multiplier=0.5)


def test_policy_defaults_are_bounded():
    policy = RetryPolicy()

    assert policy.attempts > 1
    assert policy.attempts < 100
    assert policy.max_backoff_seconds > 0
    assert policy.enabled is True
    assert RetryPolicy(attempts=1).enabled is False


def test_executor_returns_the_result_without_retrying():
    executor = RetryExecutor(INSTANT)

    assert executor(lambda: 42) == 42
    assert executor.stats.retries == 0


def test_transient_failure_then_success():
    executor = RetryExecutor(INSTANT)
    calls = {"count": 0}

    def flaky():
        calls["count"] += 1

        if calls["count"] < 3:
            raise ProviderServerError("HTTP 503")

        return "recovered"

    assert executor(flaky) == "recovered"
    assert calls["count"] == 3
    assert executor.stats.retries == 2
    assert "HTTP 503" in executor.stats.last_error


def test_transient_failure_exhausts_attempts():
    executor = RetryExecutor(INSTANT)
    calls = {"count": 0}

    def always_failing():
        calls["count"] += 1
        raise ProviderServerError("HTTP 500")

    with pytest.raises(ProviderServerError, match="HTTP 500"):
        executor(always_failing)

    assert calls["count"] == INSTANT.attempts
    assert executor.stats.retries == INSTANT.attempts - 1


def test_permanent_failure_is_not_retried():
    executor = RetryExecutor(INSTANT)
    calls = {"count": 0}

    def forbidden():
        calls["count"] += 1
        raise ProviderAuthError("HTTP 403")

    with pytest.raises(ProviderAuthError):
        executor(forbidden)

    assert calls["count"] == 1
    assert executor.stats.retries == 0


def test_retries_can_be_disabled():
    executor = RetryExecutor(RetryPolicy(attempts=1, backoff_seconds=0))
    calls = {"count": 0}

    def failing():
        calls["count"] += 1
        raise ProviderServerError("HTTP 500")

    with pytest.raises(ProviderServerError):
        executor(failing)

    assert calls["count"] == 1


def test_backoff_grows_exponentially_and_is_capped():
    """The waits are computed, not slept through."""
    executor = RetryExecutor(
        RetryPolicy(
            attempts=8,
            backoff_seconds=1,
            max_backoff_seconds=5,
            multiplier=2,
        )
    )

    waits = [executor.backoff_for(attempt) for attempt in range(1, 8)]

    assert waits[:4] == [1, 2, 4, 5]
    assert waits == sorted(waits)
    assert max(waits) == 5


def test_backoff_is_zero_when_configured_off():
    executor = RetryExecutor(RetryPolicy(attempts=3, backoff_seconds=0))

    assert executor.backoff_for(1) == 0
    assert executor.backoff_for(5) == 0


def test_an_absurd_attempt_count_still_returns_the_cap():
    executor = RetryExecutor(RetryPolicy(attempts=2, max_backoff_seconds=30))

    assert executor.backoff_for(100_000) == 30


def test_retry_after_extends_the_wait_within_the_cap():
    executor = RetryExecutor(
        RetryPolicy(attempts=3, backoff_seconds=1, max_backoff_seconds=10)
    )

    assert executor.backoff_for(1, retry_after=8) == 8
    assert executor.backoff_for(1, retry_after=999) == 10
    # A Retry-After shorter than our own backoff does not shorten it.
    assert executor.backoff_for(4, retry_after=1) == 8


def test_stats_reset():
    stats = RetryStats()
    stats.record(ProviderServerError("HTTP 500"))

    assert stats.retries == 1
    assert stats.errors

    stats.reset()

    assert stats.retries == 0
    assert stats.last_error is None
    assert stats.errors == []


# --------------------------------------------------------------------------
# Retries through the Dukascopy request layer
# --------------------------------------------------------------------------


def responder(statuses: list[int]):
    """Answer instrumentList with the given statuses, then serve real data."""
    remaining = list(statuses)
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1

        if remaining:
            return httpx.Response(remaining.pop(0))

        if request.url.params["path"] == "api/instrumentList":
            return httpx.Response(200, json=INSTRUMENTS)

        return httpx.Response(200, json=CANDLES)

    handler.calls = calls

    return handler


def raiser(exception: Exception, *, succeed_after: int = 0):
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1

        if calls["count"] <= succeed_after:
            raise exception

        if request.url.params["path"] == "api/instrumentList":
            return httpx.Response(200, json=INSTRUMENTS)

        return httpx.Response(200, json=CANDLES)

    handler.calls = calls

    return handler


@pytest.mark.parametrize("code", [429, 500, 503])
def test_provider_retries_transient_statuses(code):
    handler = responder([code, code])

    with make_provider(handler) as provider:
        symbols = provider.get_supported_symbols()

    assert symbols == ["EUR/USD"]
    assert handler.calls["count"] == 3
    assert provider.retry_stats.retries == 2


def test_provider_retries_a_timeout():
    handler = raiser(httpx.ReadTimeout("slow"), succeed_after=1)

    with make_provider(handler) as provider:
        provider.get_supported_symbols()

    assert handler.calls["count"] == 2
    assert provider.retry_stats.retries == 1
    assert "Timeout" in provider.retry_stats.last_error


def test_provider_retries_a_connection_error():
    handler = raiser(httpx.ConnectError("refused"), succeed_after=1)

    with make_provider(handler) as provider:
        provider.get_supported_symbols()

    assert handler.calls["count"] == 2
    assert provider.retry_stats.retries == 1


def test_provider_gives_up_after_the_bounded_attempts():
    handler = responder([503] * 10)

    with make_provider(handler) as provider, pytest.raises(ProviderServerError):
        provider.get_supported_symbols()

    assert handler.calls["count"] == INSTANT.attempts


@pytest.mark.parametrize("code", [401, 403])
def test_provider_does_not_retry_auth_failures(code):
    handler = responder([code] * 10)

    with make_provider(handler) as provider, pytest.raises(ProviderAuthError):
        provider.get_supported_symbols()

    assert handler.calls["count"] == 1
    assert provider.retry_stats.retries == 0


def test_provider_does_not_retry_a_bad_request():
    handler = responder([400] * 10)

    with make_provider(handler) as provider, pytest.raises(ProviderClientError):
        provider.get_supported_symbols()

    assert handler.calls["count"] == 1


def test_provider_does_not_retry_an_unknown_symbol():
    """A permanent data error must fail on the first answer."""
    handler = responder([])

    with make_provider(handler) as provider, pytest.raises(DukascopyError):
        provider.fetch_candles("XXX/YYY", START, END)

    assert handler.calls["count"] == 1
    assert provider.retry_stats.retries == 0


def test_fetch_candles_recovers_from_a_transient_failure():
    handler = responder([503])

    with make_provider(handler) as provider:
        candles = provider.fetch_candles("EUR/USD", START, END)

    assert len(candles) == 1
    assert provider.retry_stats.retries == 1


def test_health_check_survives_a_transient_failure():
    with make_provider(responder([500])) as provider:
        assert provider.health_check() is True


def test_health_check_reports_a_permanent_failure():
    with make_provider(responder([403] * 5)) as provider:
        assert provider.health_check() is False


def test_provider_exposes_its_policy():
    with make_provider(responder([])) as provider:
        assert provider.retry_policy is INSTANT

    assert isinstance(DukascopyProvider().retry_policy, RetryPolicy)


def test_every_provider_error_is_a_provider_error():
    for error in (
        ProviderTimeoutError("x"),
        ProviderConnectionError("x"),
        ProviderRateLimitError("x"),
        ProviderServerError("x"),
        ProviderAuthError("x"),
        ProviderClientError("x"),
        ProviderDataError("x"),
    ):
        assert isinstance(error, ProviderError)
