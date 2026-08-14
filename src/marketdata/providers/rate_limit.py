from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

DEFAULT_REQUESTS_PER_SECOND = 2.0
"""Conservative default pace for a free public endpoint.

A seven-year one-minute download runs into the hundreds of requests per
symbol once pagination is counted. Two per second keeps that in the tens of
minutes while staying gentle enough that a provider with no published limit
is unlikely to object.
"""


@dataclass(frozen=True)
class RateLimit:
    """
    How fast a provider may be called.

    The rate is the only knob: a minimum interval of ``1 / rate`` seconds
    between consecutive requests. One setting expressed two ways is simpler
    to reason about than two settings that can disagree, and strict spacing
    is deterministic — there is no bucket to fill, so no burst can slip
    through after an idle period.

    There is no unlimited setting. A rate must be positive and finite, so a
    long download can never be issued as fast as the loop allows.
    """

    requests_per_second: float = DEFAULT_REQUESTS_PER_SECOND

    def __post_init__(self) -> None:
        rate = self.requests_per_second

        if not math.isfinite(rate):
            raise ValueError("requests_per_second must be a finite number")

        if rate <= 0:
            raise ValueError("requests_per_second must be greater than 0")

    @property
    def min_interval(self) -> float:
        """Seconds that must elapse between two requests."""
        return 1.0 / self.requests_per_second

    def describe(self) -> str:
        return (
            f"{self.requests_per_second:g} requests/sec "
            f"(min interval {self.min_interval:.3g}s)"
        )


@dataclass
class RateLimitStats:
    """What throttling has cost so far."""

    requests: int = 0
    total_wait_seconds: float = 0.0
    max_wait_seconds: float = 0.0

    def reset(self) -> None:
        self.requests = 0
        self.total_wait_seconds = 0.0
        self.max_wait_seconds = 0.0

    def record(self, waited: float) -> None:
        self.requests += 1
        self.total_wait_seconds += waited
        self.max_wait_seconds = max(self.max_wait_seconds, waited)


class RateLimiter:
    """
    Pace outbound requests to at most one every ``min_interval`` seconds.

    Timing uses a monotonic clock, so a system clock adjustment mid-download
    cannot make the limiter release a burst or stall for hours.

    Safe to share between callers: the slot for the next request is reserved
    under a lock and the waiting happens outside it, so concurrent callers
    receive distinct, evenly spaced slots and wait in parallel rather than
    queueing behind a held lock.
    """

    def __init__(
        self,
        limit: RateLimit | None = None,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.limit = limit or RateLimit()
        self.stats = RateLimitStats()
        self._monotonic = monotonic
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next_available: float | None = None

    @property
    def min_interval(self) -> float:
        return self.limit.min_interval

    @property
    def next_available(self) -> float | None:
        """
        Monotonic time of the earliest unreserved slot, or ``None`` if unused.

        Every reservation advances this by exactly one interval, so it also
        reports how much schedule the limiter has handed out.
        """
        with self._lock:
            return self._next_available

    def _reserve(self) -> float:
        """Claim the next slot and return how long to wait for it."""
        with self._lock:
            now = self._monotonic()

            if self._next_available is None:
                scheduled = now
            else:
                scheduled = max(now, self._next_available)

            self._next_available = scheduled + self.min_interval

        return max(0.0, scheduled - now)

    def acquire(self) -> float:
        """
        Block until the caller may issue a request; return the wait in seconds.

        Every outbound request goes through here, including retries: a retry
        is another request against the same endpoint, and letting it skip the
        queue would defeat the limit precisely when the provider is already
        struggling.
        """
        delay = self._reserve()

        if delay > 0:
            self._sleep(delay)

        self.stats.record(delay)

        return delay


__all__ = [
    "DEFAULT_REQUESTS_PER_SECOND",
    "RateLimit",
    "RateLimitStats",
    "RateLimiter",
]
