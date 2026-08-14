from collections import Counter

from marketdata.models.candle import Candle


class CandleValidationError(ValueError):
    """Raised when a candle dataset violates market-data invariants."""


def validate_candle(candle: Candle) -> None:
    """Validate a single OHLCV candle."""
    if candle.high < candle.open:
        raise CandleValidationError("high cannot be below open")

    if candle.high < candle.close:
        raise CandleValidationError("high cannot be below close")

    if candle.low > candle.open:
        raise CandleValidationError("low cannot be above open")

    if candle.low > candle.close:
        raise CandleValidationError("low cannot be above close")

    if candle.high < candle.low:
        raise CandleValidationError("high cannot be below low")

    if candle.volume < 0:
        raise CandleValidationError("volume cannot be negative")

    if candle.timestamp.tzinfo is None:
        raise CandleValidationError("timestamp must be timezone-aware")


def validate_candles(candles: list[Candle]) -> None:
    """Validate an entire candle collection."""
    for candle in candles:
        validate_candle(candle)


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
