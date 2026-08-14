from __future__ import annotations

from collections import Counter
from datetime import datetime

from pydantic import BaseModel

from marketdata.models.candle import Candle


class CandleValidationError(ValueError):
    """Raised when a candle dataset violates market-data invariants."""


class CandleViolation(BaseModel):
    """A single rejected candle and the invariant it broke."""

    model_config = {"frozen": True}

    timestamp: datetime
    symbol: str
    reason: str


def candle_violation(candle: Candle) -> str | None:
    """Return the first invariant a candle breaks, or ``None`` when valid."""
    if candle.timestamp.tzinfo is None:
        return "timestamp must be timezone-aware"

    if candle.high < candle.low:
        return "high cannot be below low"

    if candle.high < candle.open:
        return "high cannot be below open"

    if candle.high < candle.close:
        return "high cannot be below close"

    if candle.low > candle.open:
        return "low cannot be above open"

    if candle.low > candle.close:
        return "low cannot be above close"

    if candle.volume < 0:
        return "volume cannot be negative"

    return None


def validate_candle(candle: Candle) -> None:
    """Validate a single OHLCV candle."""
    reason = candle_violation(candle)

    if reason is not None:
        raise CandleValidationError(reason)


def validate_candles(candles: list[Candle]) -> None:
    """Validate an entire candle collection."""
    for candle in candles:
        validate_candle(candle)


def partition_candles(
    candles: list[Candle],
) -> tuple[list[Candle], list[CandleViolation]]:
    """
    Split candles into the valid ones and a record of the rejected ones.

    Ingestion needs to report on bad rows rather than abort on the first one,
    so this is the non-raising counterpart of :func:`validate_candles`.
    """
    valid: list[Candle] = []
    violations: list[CandleViolation] = []

    for candle in candles:
        reason = candle_violation(candle)

        if reason is None:
            valid.append(candle)
            continue

        violations.append(
            CandleViolation(
                timestamp=candle.timestamp,
                symbol=candle.symbol,
                reason=reason,
            )
        )

    return valid, violations


def restrict_to_range(
    candles: list[Candle],
    start: datetime,
    end: datetime,
) -> tuple[list[Candle], list[Candle]]:
    """
    Keep only candles inside the half-open range ``[start, end)``.

    Stored datasets must never contain observations outside the range they
    were requested for: a candle beyond ``end`` is future data relative to
    the request and would leak look-ahead information into any backtest
    built on the dataset.
    """
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start and end must be timezone-aware")

    inside: list[Candle] = []
    outside: list[Candle] = []

    for candle in candles:
        if start <= candle.timestamp < end:
            inside.append(candle)
        else:
            outside.append(candle)

    return inside, outside


def deduplicate_candles(candles: list[Candle]) -> list[Candle]:
    """
    Remove duplicate timestamps deterministically.

    When duplicate timestamps exist, the last occurrence is retained.
    """
    deduplicated: dict[tuple[str, object], Candle] = {}

    for candle in candles:
        key = (candle.symbol, candle.timestamp)
        deduplicated[key] = candle

    return sorted(
        deduplicated.values(),
        key=lambda candle: candle.timestamp,
    )


def duplicate_timestamps(candles: list[Candle]) -> dict[object, int]:
    """Return timestamps occurring more than once."""
    counts = Counter(candle.timestamp for candle in candles)
    return {timestamp: count for timestamp, count in counts.items() if count > 1}
