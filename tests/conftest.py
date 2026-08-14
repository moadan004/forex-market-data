"""Shared test helpers.

Rate limiting and retry backoff both work by waiting. Tests inject a virtual
clock instead, so the suite proves the timing rules without spending the
time: a sleep advances the clock and is recorded, and nothing blocks.
"""

import pytest

from marketdata.providers.rate_limit import RateLimit, RateLimiter


class FakeClock:
    """A monotonic clock that only moves when something sleeps."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("cannot sleep for a negative duration")

        self.sleeps.append(seconds)
        self.now += seconds

    def advance(self, seconds: float) -> None:
        """Move time forward without recording a sleep."""
        self.now += seconds

    @property
    def slept(self) -> float:
        return sum(self.sleeps)


def make_limiter(
    requests_per_second: float = 2.0,
    clock: FakeClock | None = None,
) -> tuple[RateLimiter, FakeClock]:
    """Build a limiter driven by a virtual clock."""
    clock = clock or FakeClock()

    limiter = RateLimiter(
        RateLimit(requests_per_second=requests_per_second),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    return limiter, clock


def instant_limiter() -> RateLimiter:
    """A limiter whose waiting is virtual, for tests not about pacing."""
    return make_limiter()[0]


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()
