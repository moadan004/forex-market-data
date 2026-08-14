from datetime import timedelta
from types import MappingProxyType

TIMEFRAME_CADENCE = MappingProxyType(
    {
        "10sec": timedelta(seconds=10),
        "1min": timedelta(minutes=1),
        "10m": timedelta(minutes=10),
        "1hour": timedelta(hours=1),
        "1day": timedelta(days=1),
    }
)
"""Fixed wall-clock spacing between consecutive candles of a timeframe.

Timeframes whose spacing is not a constant duration are deliberately absent.
``1day_eet`` is anchored to the East European session start and therefore
shifts by an hour across daylight-saving transitions.
"""


def timeframe_cadence(timeframe: str) -> timedelta | None:
    """
    Return the fixed cadence of a timeframe, or ``None`` when it has none.

    A ``None`` result means gap detection cannot be expressed as a constant
    interval; callers must skip cadence-based analysis rather than guess.
    """
    return TIMEFRAME_CADENCE.get(timeframe)
