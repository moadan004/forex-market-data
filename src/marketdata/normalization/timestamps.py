from datetime import UTC, datetime

from marketdata.models.candle import Candle


def ensure_utc(value: datetime) -> datetime:
    """Return a timezone-aware datetime normalized to UTC."""
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")

    return value.astimezone(UTC)


def normalize_candle(candle: Candle) -> Candle:
    """Normalize a candle timestamp to UTC."""
    return candle.model_copy(update={"timestamp": ensure_utc(candle.timestamp)})


def normalize_candles(candles: list[Candle]) -> list[Candle]:
    """Normalize candles and return them chronologically sorted."""
    return sorted(
        (normalize_candle(candle) for candle in candles),
        key=lambda candle: candle.timestamp,
    )
