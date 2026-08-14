"""Rate limiting: pacing, concurrency, and its interaction with retries."""

import json
import threading
import time
from datetime import UTC, datetime
from itertools import pairwise

import httpx
import pytest
from conftest import FakeClock, instant_limiter, make_limiter

from marketdata.calendar import AlwaysOpenCalendar
from marketdata.cli import build_parser, main, rate_limit_from_args
from marketdata.downloader.pipeline import DownloadPipeline
from marketdata.providers.base import MarketDataProvider
from marketdata.providers.dukascopy import DukascopyProvider
from marketdata.providers.errors import ProviderAuthError, ProviderServerError
from marketdata.providers.rate_limit import (
    DEFAULT_REQUESTS_PER_SECOND,
    RateLimit,
    RateLimiter,
    RateLimitStats,
)
from marketdata.providers.retry import RetryExecutor, RetryPolicy

START = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
END = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)

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


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------


def test_default_is_a_real_limit():
    limit = RateLimit()

    assert limit.requests_per_second == DEFAULT_REQUESTS_PER_SECOND
    assert limit.requests_per_second > 0
    assert limit.min_interval == pytest.approx(1 / DEFAULT_REQUESTS_PER_SECOND)


def test_rate_and_interval_are_two_views_of_one_setting():
    assert RateLimit(requests_per_second=4).min_interval == 0.25
    assert RateLimit(requests_per_second=0.5).min_interval == 2.0


@pytest.mark.parametrize("rate", [0, -1, -0.001])
def test_a_non_positive_rate_is_rejected(rate):
    with pytest.raises(ValueError, match="greater than 0"):
        RateLimit(requests_per_second=rate)


@pytest.mark.parametrize("rate", [float("inf"), float("-inf"), float("nan")])
def test_there_is_no_unlimited_setting(rate):
    with pytest.raises(ValueError, match="finite"):
        RateLimit(requests_per_second=rate)


def test_a_very_large_rate_is_allowed_and_barely_waits():
    limiter, clock = make_limiter(1_000_000)

    for _ in range(5):
        limiter.acquire()

    assert clock.slept == pytest.approx(4e-6)
    assert limiter.min_interval == pytest.approx(1e-6)


def test_a_very_small_rate_produces_a_long_interval():
    limiter, clock = make_limiter(1 / 3600)

    limiter.acquire()
    limiter.acquire()

    assert clock.sleeps == [3600]


def test_the_limit_describes_itself():
    assert "2 requests/sec" in RateLimit(requests_per_second=2).describe()
    assert "0.5s" in RateLimit(requests_per_second=2).describe()


# --------------------------------------------------------------------------
# Pacing
# --------------------------------------------------------------------------


def test_the_first_request_is_not_delayed():
    limiter, clock = make_limiter(2)

    assert limiter.acquire() == 0
    assert clock.sleeps == []


def test_the_second_request_waits_the_minimum_interval():
    """A request at t=0 means the next cannot happen before t=interval."""
    limiter, clock = make_limiter(2)

    started = clock.now
    limiter.acquire()
    limiter.acquire()

    assert clock.sleeps == [0.5]
    assert clock.now - started == pytest.approx(0.5)


def test_a_burst_is_spread_out():
    limiter, clock = make_limiter(4)

    stamps = []

    for _ in range(5):
        limiter.acquire()
        stamps.append(clock.now)

    gaps = [round(b - a, 6) for a, b in pairwise(stamps)]

    assert gaps == [0.25, 0.25, 0.25, 0.25]
    assert clock.slept == pytest.approx(1.0)


def test_an_idle_period_does_not_bank_a_burst():
    """Waiting longer than the interval must not earn free requests."""
    limiter, clock = make_limiter(2)

    limiter.acquire()
    clock.advance(60)

    assert limiter.acquire() == 0
    assert limiter.acquire() == pytest.approx(0.5)


def test_pacing_is_deterministic():
    first, first_clock = make_limiter(3)
    second, second_clock = make_limiter(3)

    for _ in range(6):
        first.acquire()
        second.acquire()

    assert first_clock.sleeps == second_clock.sleeps
    assert first_clock.now == second_clock.now


def test_timing_uses_the_injected_monotonic_clock():
    """Nothing consults the wall clock, so a clock jump cannot open a burst."""
    clock = FakeClock()
    limiter = RateLimiter(
        RateLimit(requests_per_second=1),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    limiter.acquire()
    limiter.acquire()

    assert clock.sleeps == [1.0]


def test_stats_track_the_cost_of_throttling():
    limiter, _ = make_limiter(2)

    for _ in range(3):
        limiter.acquire()

    stats = limiter.stats

    assert stats.requests == 3
    assert stats.total_wait_seconds == pytest.approx(1.0)
    assert stats.max_wait_seconds == pytest.approx(0.5)


def test_stats_can_be_reset():
    stats = RateLimitStats()
    stats.record(0.5)
    stats.reset()

    assert stats.requests == 0
    assert stats.total_wait_seconds == 0
    assert stats.max_wait_seconds == 0


# --------------------------------------------------------------------------
# Concurrency
# --------------------------------------------------------------------------


def run_concurrently(worker, threads: int) -> None:
    workers = [threading.Thread(target=worker) for _ in range(threads)]

    for thread in workers:
        thread.start()

    for thread in workers:
        thread.join()


def test_concurrent_callers_each_get_their_own_slot():
    """
    A shared limiter must not hand two threads the same slot.

    Without the lock around the reservation, two threads read the same
    ``next_available`` and both write back one interval later, so the
    schedule advances once for two requests. The invariant below is exactly
    what that bug breaks.
    """
    limiter = RateLimiter(RateLimit(requests_per_second=10_000))
    started = limiter._monotonic()
    ready = threading.Barrier(8)

    def worker() -> None:
        ready.wait()
        limiter.acquire()

    run_concurrently(worker, 8)

    assert limiter.stats.requests == 8
    assert limiter.next_available - started >= 8 * limiter.min_interval


def test_concurrent_callers_never_exceed_the_rate():
    """Measured against the real clock, with an interval small enough to run."""
    limiter = RateLimiter(RateLimit(requests_per_second=200))
    requests_each = 5
    threads = 4
    total = requests_each * threads

    def worker() -> None:
        for _ in range(requests_each):
            limiter.acquire()

    started = time.monotonic()
    run_concurrently(worker, threads)
    elapsed = time.monotonic() - started

    assert limiter.stats.requests == total
    # The last of N requests cannot happen before (N-1) intervals have passed.
    assert elapsed >= (total - 1) * limiter.min_interval


def test_the_limiter_is_safe_to_share_between_provider_calls():
    limiter, clock = make_limiter(2)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["path"] == "api/instrumentList":
            return httpx.Response(200, json=INSTRUMENTS)

        return httpx.Response(200, json=CANDLES)

    first = DukascopyProvider(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        rate_limiter=limiter,
    )
    second = DukascopyProvider(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        rate_limiter=limiter,
    )

    first.get_supported_symbols()
    second.get_supported_symbols()

    assert clock.sleeps == [0.5]
    assert limiter.stats.requests == 2


# --------------------------------------------------------------------------
# Provider integration
# --------------------------------------------------------------------------


def responder(statuses: list[int], *, headers: dict | None = None):
    remaining = list(statuses)
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1

        if remaining:
            return httpx.Response(remaining.pop(0), headers=headers or {})

        if request.url.params["path"] == "api/instrumentList":
            return httpx.Response(200, json=INSTRUMENTS)

        return httpx.Response(200, json=CANDLES)

    handler.calls = calls

    return handler


def build_provider(handler, *, rate=2.0, policy=None, clock=None):
    """A provider whose throttling and backoff both run on a virtual clock."""
    limiter, clock = make_limiter(rate, clock)

    return (
        DukascopyProvider(
            client=httpx.Client(transport=httpx.MockTransport(handler)),
            retry_executor=RetryExecutor(
                policy or RetryPolicy(attempts=3, backoff_seconds=0),
                sleep=clock.sleep,
            ),
            rate_limiter=limiter,
        ),
        clock,
    )


def test_the_provider_has_a_limit_by_default():
    provider = DukascopyProvider()

    assert provider.rate_limit.requests_per_second == DEFAULT_REQUESTS_PER_SECOND


def test_a_rate_limit_can_be_passed_without_building_a_limiter():
    provider = DukascopyProvider(rate_limit=RateLimit(requests_per_second=5))

    assert provider.rate_limit.min_interval == 0.2


def test_instrument_list_requests_are_throttled():
    handler = responder([])
    provider, clock = build_provider(handler)

    provider.get_supported_symbols()
    provider.get_supported_symbols()

    assert handler.calls["count"] == 2
    assert clock.sleeps == [0.5]


def test_historical_price_requests_are_throttled():
    handler = responder([])
    provider, clock = build_provider(handler)

    provider.fetch_candles("EUR/USD", START, END)

    # instrumentList then historicalPrices: the second waits its turn.
    assert handler.calls["count"] == 2
    assert clock.sleeps == [0.5]
    assert provider.rate_limit_stats.requests == 2


def test_every_sequential_request_is_paced():
    handler = responder([])
    provider, clock = build_provider(handler, rate=4)

    for _ in range(4):
        provider.get_supported_symbols()

    assert clock.sleeps == [0.25, 0.25, 0.25]
    assert provider.rate_limit_stats.total_wait_seconds == pytest.approx(0.75)


# --------------------------------------------------------------------------
# Retry and rate limit together
# --------------------------------------------------------------------------


def test_retries_are_throttled_too():
    """A retry is another request; it must not skip the queue."""
    handler = responder([503, 503])
    provider, _ = build_provider(handler)

    provider.get_supported_symbols()

    assert handler.calls["count"] == 3
    assert provider.retry_stats.retries == 2
    # Three requests, so two throttle waits — the retries were not exempt.
    assert provider.rate_limit_stats.requests == 3
    assert provider.rate_limit_stats.total_wait_seconds == pytest.approx(1.0)


@pytest.mark.parametrize("status", [429, 500, 503])
def test_transient_failures_are_retried_and_still_throttled(status):
    handler = responder([status])
    provider, _ = build_provider(handler)

    provider.get_supported_symbols()

    assert handler.calls["count"] == 2
    assert provider.rate_limit_stats.requests == 2
    assert provider.rate_limit_stats.total_wait_seconds == pytest.approx(0.5)


def test_a_rate_limited_response_still_passes_through_the_limiter():
    """Retry-After is honoured, and the retry still takes a limiter slot."""
    handler = responder([429], headers={"Retry-After": "3"})
    provider, clock = build_provider(handler)

    provider.get_supported_symbols()

    assert handler.calls["count"] == 2
    assert provider.rate_limit_stats.requests == 2
    assert 3.0 in clock.sleeps
    # The 3s Retry-After already covers the 0.5s interval, so the limiter
    # adds nothing on top rather than waiting a second time.
    assert provider.rate_limit_stats.total_wait_seconds == 0


def test_a_timeout_retry_is_throttled():
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1

        if calls["count"] == 1:
            raise httpx.ReadTimeout("slow")

        return httpx.Response(200, json=INSTRUMENTS)

    provider, _ = build_provider(handler)

    provider.get_supported_symbols()

    assert calls["count"] == 2
    assert provider.rate_limit_stats.requests == 2
    assert provider.rate_limit_stats.total_wait_seconds == pytest.approx(0.5)


def test_backoff_and_throttle_both_apply_in_order():
    """Retry backoff first, then the limiter, then the request."""
    clock = FakeClock()
    limiter, _ = make_limiter(2, clock)

    order: list[str] = []

    def backoff_sleep(seconds: float) -> None:
        order.append(f"backoff:{round(seconds, 6)}")
        clock.sleep(seconds)

    handler = responder([503])
    provider = DukascopyProvider(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        retry_executor=RetryExecutor(
            RetryPolicy(attempts=3, backoff_seconds=0.1),
            sleep=backoff_sleep,
        ),
        rate_limiter=limiter,
    )

    original_acquire = limiter.acquire

    def traced_acquire() -> float:
        waited = original_acquire()
        order.append(f"throttle:{round(waited, 6)}")
        return waited

    limiter.acquire = traced_acquire

    provider.get_supported_symbols()

    # Backoff runs first, then the limiter tops the wait up to a full
    # interval before the retry is allowed out.
    assert order == ["throttle:0.0", "backoff:0.1", "throttle:0.4"]


def test_a_permanent_failure_costs_one_slot_only():
    handler = responder([403] * 5)
    provider, clock = build_provider(handler)

    with pytest.raises(ProviderAuthError):
        provider.get_supported_symbols()

    assert handler.calls["count"] == 1
    assert provider.rate_limit_stats.requests == 1
    assert clock.sleeps == []


def test_exhausted_retries_consume_one_slot_per_attempt():
    policy = RetryPolicy(attempts=4, backoff_seconds=0)
    handler = responder([500] * 10)
    provider, _ = build_provider(handler, policy=policy)

    with pytest.raises(ProviderServerError):
        provider.get_supported_symbols()

    assert handler.calls["count"] == 4
    assert provider.rate_limit_stats.requests == 4
    assert provider.rate_limit_stats.total_wait_seconds == pytest.approx(1.5)


def test_throttling_does_not_replace_retrying():
    """The limiter paces; it does not decide whether to try again."""
    handler = responder([503, 503, 503, 503])
    provider, _ = build_provider(
        handler,
        policy=RetryPolicy(attempts=2, backoff_seconds=0),
    )

    with pytest.raises(ProviderServerError):
        provider.get_supported_symbols()

    assert handler.calls["count"] == 2


def test_the_limiter_is_never_bypassed_across_a_whole_fetch():
    handler = responder([503, 500])
    provider, _ = build_provider(handler)

    provider.fetch_candles("EUR/USD", START, END)

    # Every HTTP call took a slot: two failures, the instrument list and
    # the price request.
    assert provider.rate_limit_stats.requests == handler.calls["count"]
    assert provider.rate_limit_stats.total_wait_seconds == pytest.approx(
        (handler.calls["count"] - 1) * 0.5
    )


def test_instant_limiter_helper_does_not_sleep_for_real():
    limiter = instant_limiter()

    limiter.acquire()
    limiter.acquire()

    assert limiter.stats.requests == 2


# --------------------------------------------------------------------------
# CLI configuration and reporting
# --------------------------------------------------------------------------


def parse(*extra):
    return build_parser().parse_args(
        [
            "download",
            "--symbol",
            "EUR/USD",
            "--start",
            "2026-08-14T12:00:00Z",
            "--end",
            "2026-08-14T13:00:00Z",
            *extra,
        ]
    )


def test_the_cli_default_is_the_conservative_rate():
    limit = rate_limit_from_args(parse())

    assert limit.requests_per_second == DEFAULT_REQUESTS_PER_SECOND


def test_the_cli_rate_is_configurable():
    assert rate_limit_from_args(parse("--rate-limit", "0.5")).min_interval == 2.0
    assert rate_limit_from_args(parse("--rate-limit", "10")).min_interval == 0.1


@pytest.mark.parametrize("value", ["0", "-2", "inf", "nan"])
def test_the_cli_rejects_an_unlimited_or_invalid_rate(value):
    with pytest.raises(ValueError):
        rate_limit_from_args(parse("--rate-limit", value))


def download_argv(tmp_path, *extra):
    return [
        "download",
        "--symbol",
        "EUR/USD",
        "--start",
        "2026-08-14T12:00:00Z",
        "--end",
        "2026-08-14T13:00:00Z",
        "--calendar",
        "24x7",
        "--data-root",
        str(tmp_path / "processed"),
        "--manifest-root",
        str(tmp_path / "manifests"),
        "--quality-root",
        str(tmp_path / "quality"),
        "--checkpoint-root",
        str(tmp_path / "checkpoints"),
        *extra,
    ]


def cli_provider_factory(clock):
    limiter, _ = make_limiter(2, clock)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["path"] == "api/instrumentList":
            return httpx.Response(200, json=INSTRUMENTS)

        return httpx.Response(200, json=CANDLES)

    def factory() -> DukascopyProvider:
        return DukascopyProvider(
            client=httpx.Client(transport=httpx.MockTransport(handler)),
            rate_limiter=limiter,
        )

    return factory


def test_the_summary_shows_the_active_rate_limit(tmp_path, capsys):
    exit_code = main(
        download_argv(tmp_path),
        provider_factory=cli_provider_factory(FakeClock()),
    )

    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Rate limit:        2 requests/sec (min interval 0.5s)" in output
    assert "Throttled:         0.5s" in output


def test_the_json_report_records_the_rate_limit(tmp_path, capsys):
    main(
        download_argv(tmp_path, "--json"),
        provider_factory=cli_provider_factory(FakeClock()),
    )

    payload = json.loads(capsys.readouterr().out)

    assert payload["rate_limit_requests_per_second"] == 2.0
    assert payload["provider_retries"] == 0


class UnthrottledProvider(MarketDataProvider):
    """A provider that does not implement throttling at all."""

    @property
    def name(self) -> str:
        return "unthrottled"

    def get_supported_symbols(self) -> list[str]:
        return ["EUR/USD"]

    def fetch_candles(self, symbol, start, end, timeframe="1min"):
        return []

    def health_check(self) -> bool:
        return True


def test_a_provider_without_a_limiter_is_reported_honestly(tmp_path):
    """Nothing pretends a provider is throttled when it is not."""
    pipeline = DownloadPipeline(
        UnthrottledProvider(),
        output_root=tmp_path / "processed",
        manifest_root=tmp_path / "manifests",
        quality_root=tmp_path / "quality",
        checkpoint_root=tmp_path / "checkpoints",
        calendar=AlwaysOpenCalendar(),
    )

    result = pipeline.run(symbol="EUR/USD", start=START, end=END)

    assert result.rate_limit == "none"
    assert result.throttled_seconds == 0.0
    assert result.quality.rate_limit_requests_per_second is None
