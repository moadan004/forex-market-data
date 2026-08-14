from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

from pydantic import BaseModel

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_INSTANT = timedelta(microseconds=1)


class ClosedInterval(BaseModel):
    """A half-open UTC interval during which the market does not trade."""

    model_config = {"frozen": True}

    start: datetime
    end: datetime
    reason: str


def require_utc_range(start: datetime, end: datetime) -> None:
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start and end must be timezone-aware")


def _slot_index(moment: datetime, cadence: timedelta) -> int:
    """Return the index of the first cadence slot at or after ``moment``."""
    return -((EPOCH - moment) // cadence)


def merge_intervals(intervals: Iterable[ClosedInterval]) -> list[ClosedInterval]:
    """Merge overlapping and touching closures into a minimal ordered set."""
    ordered = sorted(intervals, key=lambda interval: (interval.start, interval.end))
    merged: list[ClosedInterval] = []

    for interval in ordered:
        if interval.start >= interval.end:
            continue

        if merged and interval.start <= merged[-1].end:
            previous = merged[-1]
            reason = (
                previous.reason
                if previous.reason == interval.reason
                else "market closure"
            )
            merged[-1] = ClosedInterval(
                start=previous.start,
                end=max(previous.end, interval.end),
                reason=reason,
            )
            continue

        merged.append(interval)

    return merged


class MarketCalendar(ABC):
    """
    Trading calendar for one market.

    Implementations describe when the market is *closed*; everything else —
    open intervals, expected candle counts — is derived from that, so adding
    a new closure rule (a holiday list, a session break) never requires
    touching the pipeline.

    All boundaries are half-open UTC intervals: a closure covering
    ``[start, end)`` excludes ``end`` itself.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the calendar name."""
        raise NotImplementedError

    @abstractmethod
    def closed_intervals(
        self,
        start: datetime,
        end: datetime,
    ) -> list[ClosedInterval]:
        """
        Return the closures overlapping ``[start, end)``, clipped to it.

        The result must be ordered, non-overlapping and free of empty
        intervals.
        """
        raise NotImplementedError

    def is_open(self, moment: datetime) -> bool:
        """Return whether the market trades at ``moment``."""
        if moment.tzinfo is None:
            raise ValueError("moment must be timezone-aware")

        return not self.closed_intervals(moment, moment + _INSTANT)

    def open_intervals(
        self,
        start: datetime,
        end: datetime,
    ) -> list[tuple[datetime, datetime]]:
        """Return the trading stretches of ``[start, end)``."""
        require_utc_range(start, end)

        if start >= end:
            return []

        intervals: list[tuple[datetime, datetime]] = []
        cursor = start

        for closure in self.closed_intervals(start, end):
            if closure.start > cursor:
                intervals.append((cursor, closure.start))

            cursor = max(cursor, closure.end)

        if cursor < end:
            intervals.append((cursor, end))

        return intervals

    def expected_candle_count(
        self,
        start: datetime,
        end: datetime,
        cadence: timedelta,
    ) -> int:
        """
        Return how many candles ``[start, end)`` should contain.

        Slots are anchored to the Unix epoch, so the same wall-clock instant
        always belongs to the same slot regardless of the requested range.
        """
        if cadence <= timedelta(0):
            raise ValueError("cadence must be positive")

        return sum(
            _slot_index(open_end, cadence) - _slot_index(open_start, cadence)
            for open_start, open_end in self.open_intervals(start, end)
        )


class AlwaysOpenCalendar(MarketCalendar):
    """Calendar for a market that never closes."""

    @property
    def name(self) -> str:
        return "24x7"

    def closed_intervals(
        self,
        start: datetime,
        end: datetime,
    ) -> list[ClosedInterval]:
        require_utc_range(start, end)
        return []
