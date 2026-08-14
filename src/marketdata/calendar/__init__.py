from marketdata.calendar.base import (
    AlwaysOpenCalendar,
    ClosedInterval,
    MarketCalendar,
)
from marketdata.calendar.forex import ForexCalendar

CALENDARS: dict[str, type[MarketCalendar]] = {
    "forex": ForexCalendar,
    "24x7": AlwaysOpenCalendar,
}


def get_calendar(name: str) -> MarketCalendar:
    """Return a calendar by name."""
    try:
        return CALENDARS[name]()
    except KeyError:
        raise ValueError(
            f"Unknown calendar: {name}. Available: {sorted(CALENDARS)}"
        ) from None


__all__ = [
    "CALENDARS",
    "AlwaysOpenCalendar",
    "ClosedInterval",
    "ForexCalendar",
    "MarketCalendar",
    "get_calendar",
]
