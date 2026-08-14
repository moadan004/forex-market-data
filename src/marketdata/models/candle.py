from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Candle(BaseModel):
    """Canonical OHLCV candle used throughout the market-data pipeline."""

    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    symbol: str = Field(min_length=1)
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Field(default=Decimal(0), ge=0)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator(
        "open",
        "high",
        "low",
        "close",
        "volume",
        mode="before",
    )
    @classmethod
    def convert_decimal(cls, value) -> Decimal:
        return Decimal(str(value))

    @field_validator("timestamp")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        return value
