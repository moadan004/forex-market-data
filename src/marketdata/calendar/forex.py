from __future__ import annotations

from collections.abc import Collection, Iterator
from datetime import UTC, date, datetime, time, timedelta

from marketdata.calendar.base import (
    ClosedInterval,
    MarketCalendar,
    merge_intervals,
    require_utc_range,
)

FRIDAY = 4
SUNDAY = 6

DEFAULT_CLOSE_TIME = time(21, 0)
DEFAULT_OPEN_TIME = time(22, 0)

WEEKEND_REASON = "weekend"
HOLIDAY_REASON = "holiday"


class ForexCalendar(MarketCalendar):
    """
    Trading calendar for the spot foreign-exchange market.

    Spot FX trades continuously from the Sydney open on Sunday evening to the
    New York close on Friday evening, so the only recurring closure is the
    weekend. The defaults close at 21:00 UTC on Friday and reopen at 22:00
    UTC on Sunday.

    Those boundaries are deliberately conservative. The real weekly boundary
    follows 17:00 New York time, which is 21:00 UTC while the United States
    observes daylight saving and 22:00 UTC otherwise. Treating the widest of
    the two as closed means the calendar never claims a candle was expected
    during an hour the market may not have been trading, at the cost of
    staying silent about an hour that is sometimes tradeable. Both boundaries
    are configurable for callers who need the exact convention of a specific
    venue.

    Holidays are supplied as whole UTC days. No holiday list ships with the
    project; pass one in to have those days treated as closures.
    """

    def __init__(
        self,
        *,
        close_weekday: int = FRIDAY,
        close_time: time = DEFAULT_CLOSE_TIME,
        open_weekday: int = SUNDAY,
        open_time: time = DEFAULT_OPEN_TIME,
        holidays: Collection[date] = (),
    ) -> None:
        for weekday in (close_weekday, open_weekday):
            if not 0 <= weekday <= 6:
                raise ValueError("weekdays must be between 0 (Monday) and 6 (Sunday)")

        self.close_weekday = close_weekday
        self.close_time = close_time
        self.open_weekday = open_weekday
        self.open_time = open_time
        self.holidays = frozenset(holidays)

    @property
    def name(self) -> str:
        return "forex"

    @property
    def _closure_days(self) -> int:
        """Whole days from the weekly close to the following weekly open."""
        return ((self.open_weekday - self.close_weekday) % 7) or 7

    def _weekly_closures(
        self,
        start: datetime,
        end: datetime,
    ) -> Iterator[ClosedInterval]:
        # Start a full week early so a closure that began before the window
        # but still covers part of it is not missed.
        day = (start - timedelta(days=7)).date()

        while day.weekday() != self.close_weekday:
            day += timedelta(days=1)

        while True:
            closes_at = datetime.combine(day, self.close_time, tzinfo=UTC)

            if closes_at >= end:
                return

            opens_at = datetime.combine(
                day + timedelta(days=self._closure_days),
                self.open_time,
                tzinfo=UTC,
            )

            if opens_at > start:
                yield ClosedInterval(
                    start=closes_at,
                    end=opens_at,
                    reason=WEEKEND_REASON,
                )

            day += timedelta(days=7)

    def _holiday_closures(
        self,
        start: datetime,
        end: datetime,
    ) -> Iterator[ClosedInterval]:
        for holiday in sorted(self.holidays):
            closes_at = datetime.combine(holiday, time.min, tzinfo=UTC)
            opens_at = closes_at + timedelta(days=1)

            if opens_at > start and closes_at < end:
                yield ClosedInterval(
                    start=closes_at,
                    end=opens_at,
                    reason=HOLIDAY_REASON,
                )

    def closed_intervals(
        self,
        start: datetime,
        end: datetime,
    ) -> list[ClosedInterval]:
        require_utc_range(start, end)

        if start >= end:
            return []

        merged = merge_intervals(
            [
                *self._weekly_closures(start, end),
                *self._holiday_closures(start, end),
            ]
        )

        return [
            ClosedInterval(
                start=max(interval.start, start),
                end=min(interval.end, end),
                reason=interval.reason,
            )
            for interval in merged
        ]
