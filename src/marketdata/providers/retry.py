from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TypeVar

from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
)

from marketdata.providers.errors import TransientProviderError

T = TypeVar("T")

DEFAULT_ATTEMPTS = 4
DEFAULT_BACKOFF_SECONDS = 1.0
DEFAULT_MAX_BACKOFF_SECONDS = 60.0


@dataclass(frozen=True)
class RetryPolicy:
    """
    How hard to try again after a transient provider failure.

    ``attempts`` counts total attempts, not extra ones, and must be finite:
    a multi-year download makes thousands of requests, and an unbounded
    retry turns one unreachable provider into a run that never ends.
    """

    attempts: int = DEFAULT_ATTEMPTS
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS
    max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS
    multiplier: float = 2.0

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("attempts must be at least 1")

        if self.backoff_seconds < 0:
            raise ValueError("backoff_seconds cannot be negative")

        if self.max_backoff_seconds < 0:
            raise ValueError("max_backoff_seconds cannot be negative")

        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")

    @property
    def enabled(self) -> bool:
        return self.attempts > 1

    def describe(self) -> str:
        if not self.enabled:
            return "disabled"

        return (
            f"{self.attempts} attempts, "
            f"{self.backoff_seconds}s backoff x{self.multiplier} "
            f"up to {self.max_backoff_seconds}s"
        )


@dataclass
class RetryStats:
    """Retries observed since the last reset."""

    retries: int = 0
    last_error: str | None = None
    errors: list[str] = field(default_factory=list)

    def reset(self) -> None:
        self.retries = 0
        self.last_error = None
        self.errors.clear()

    def record(self, error: BaseException) -> None:
        self.retries += 1
        self.last_error = f"{type(error).__name__}: {error}"
        self.errors.append(self.last_error)


class RetryExecutor:
    """
    Run provider calls under a retry policy.

    Only :class:`TransientProviderError` is retried. A permanent failure is
    raised on its first occurrence, so a bad symbol or a rejected credential
    fails immediately instead of after every attempt.
    """

    def __init__(self, policy: RetryPolicy | None = None) -> None:
        self.policy = policy or RetryPolicy()
        self.stats = RetryStats()

    def backoff_for(self, attempt: int, retry_after: float | None = None) -> float:
        """
        Return the delay in seconds before the attempt after ``attempt``.

        The delay grows exponentially and is capped, so a provider that is
        down for an hour is polled at a steady interval rather than at an
        ever-doubling one.
        """
        try:
            computed = self.policy.backoff_seconds * self.policy.multiplier ** (
                attempt - 1
            )
        except OverflowError:
            computed = self.policy.max_backoff_seconds

        computed = min(computed, self.policy.max_backoff_seconds)

        if retry_after is None:
            return computed

        # Honour Retry-After when the provider asks for a longer pause than
        # our own backoff, but never longer than the configured ceiling.
        return min(max(computed, float(retry_after)), self.policy.max_backoff_seconds)

    def _wait(self, state: RetryCallState) -> float:
        error = state.outcome.exception() if state.outcome else None

        return self.backoff_for(
            state.attempt_number,
            getattr(error, "retry_after", None),
        )

    def _before_sleep(self, state: RetryCallState) -> None:
        if state.outcome is not None and state.outcome.failed:
            self.stats.record(state.outcome.exception())

    def __call__(self, operation: Callable[[], T]) -> T:
        """Run ``operation``, retrying transient failures."""
        retrying = Retrying(
            stop=stop_after_attempt(self.policy.attempts),
            wait=self._wait,
            retry=retry_if_exception_type(TransientProviderError),
            before_sleep=self._before_sleep,
            reraise=True,
        )

        return retrying(operation)


__all__ = [
    "DEFAULT_ATTEMPTS",
    "DEFAULT_BACKOFF_SECONDS",
    "DEFAULT_MAX_BACKOFF_SECONDS",
    "RetryExecutor",
    "RetryPolicy",
    "RetryStats",
]
