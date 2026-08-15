from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta
from itertools import pairwise

from pydantic import BaseModel

from marketdata.calendar.base import MarketCalendar


class MissingInterval(BaseModel):
    """A contiguous stretch of the requested range with no candles."""

    model_config = {"frozen": True}

    start: datetime
    end: datetime
    missing_candles: int


def _slot_count(span: timedelta, cadence: timedelta) -> int:
    return int(span.total_seconds() // cadence.total_seconds())


def find_missing_intervals(
    timestamps: Iterable[datetime],
    cadence: timedelta,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[MissingInterval]:
    """
    Return the stretches of ``[start, end)`` not covered by ``timestamps``.

    Gaps are derived purely from the cadence: any spacing wider than one
    cadence between consecutive timestamps is reported, as are the leading
    and trailing edges when ``start`` and ``end`` are supplied.

    The result is descriptive, not a defect list. Foreign-exchange venues
    close over the weekend and on holidays, so a range spanning a market
    closure legitimately contains missing intervals.
    """
    if cadence <= timedelta(0):
        raise ValueError("cadence must be positive")

    ordered = sorted(timestamps)

    if not ordered:
        if start is None or end is None or start >= end:
            return []

        return [
            MissingInterval(
                start=start,
                end=end,
                missing_candles=_slot_count(end - start, cadence),
            )
        ]

    intervals: list[MissingInterval] = []

    if start is not None and ordered[0] > start:
        missing = _slot_count(ordered[0] - start, cadence)

        if missing > 0:
            intervals.append(
                MissingInterval(
                    start=start,
                    end=ordered[0],
                    missing_candles=missing,
                )
            )

    for previous, current in pairwise(ordered):
        span = current - previous

        if span <= cadence:
            continue

        missing = _slot_count(span, cadence) - 1

        if missing > 0:
            intervals.append(
                MissingInterval(
                    start=previous + cadence,
                    end=current,
                    missing_candles=missing,
                )
            )

    if end is not None:
        trailing_start = ordered[-1] + cadence

        if trailing_start < end:
            missing = _slot_count(end - trailing_start, cadence)

            if missing > 0:
                intervals.append(
                    MissingInterval(
                        start=trailing_start,
                        end=end,
                        missing_candles=missing,
                    )
                )

    return intervals


class TradingGapScanner:
    """
    Find gaps in expected trading time from a stream of timestamps.

    The incremental form of :func:`find_missing_trading_intervals`. Gaps are
    still searched inside each open session separately — a market closure
    never registers as missing data, and a session boundary never hides a
    genuine gap next to it — but timestamps are consumed one at a time, so
    the caller can feed a multi-year dataset a partition at a time instead
    of materializing every timestamp in it.

    Only bounded state survives between calls: the open sessions of the
    requested range, the last timestamp seen, running counts, and at most
    ``max_samples`` retained intervals. Pass ``max_samples=None`` to retain
    every interval, which is what the whole-list function does.

    **Timestamps must arrive in non-decreasing order.** The month-partitioned
    layout guarantees that when partitions are walked chronologically and
    each partition holds only its own month. A timestamp that goes backwards
    is counted in :attr:`out_of_order` and excluded from gap detection,
    because a gap already reported cannot be retroactively split; callers
    that can encounter such data must report the disorder themselves.
    """

    def __init__(
        self,
        cadence: timedelta,
        *,
        start: datetime,
        end: datetime,
        calendar: MarketCalendar,
        max_samples: int | None = None,
    ) -> None:
        if cadence <= timedelta(0):
            raise ValueError("cadence must be positive")

        self.cadence = cadence
        self.max_samples = max_samples

        self._sessions = calendar.open_intervals(start, end)
        self._index = 0
        self._previous: datetime | None = None
        self._latest: datetime | None = None
        self._finished = False

        self.intervals: list[MissingInterval] = []
        self.interval_count = 0
        self.missing_candles = 0
        self.observed = 0
        self.out_of_order = 0

    def feed(self, timestamps: Iterable[datetime]) -> None:
        """Consume a batch of timestamps in chronological order."""
        for timestamp in timestamps:
            self.feed_one(timestamp)

    def feed_one(self, timestamp: datetime) -> None:
        """Consume one timestamp."""
        if self._finished:
            raise RuntimeError("the scanner has already been finished")

        if self._latest is not None and timestamp < self._latest:
            self.out_of_order += 1
            return

        self._latest = timestamp
        self.observed += 1

        # Every session ending at or before this timestamp is complete.
        while (
            self._index < len(self._sessions)
            and timestamp >= self._sessions[self._index][1]
        ):
            self._close_session()

        if self._index >= len(self._sessions):
            return

        session_start, _ = self._sessions[self._index]

        if timestamp < session_start:
            # The market was closed; nothing was expected here.
            return

        if self._previous is None:
            self._record_span(session_start, timestamp, leading=True)
        else:
            self._record_span(self._previous, timestamp, leading=False)

        self._previous = timestamp

    def finish(self) -> list[MissingInterval]:
        """Close every remaining session and return the retained intervals."""
        while self._index < len(self._sessions):
            self._close_session()

        self._finished = True

        return self.intervals

    def _close_session(self) -> None:
        session_start, session_end = self._sessions[self._index]

        if self._previous is None:
            # A session nothing was stored for is missing in its entirety,
            # recorded even when it is shorter than one candle so that a
            # window expecting nothing is still reported as uncovered.
            self._append(
                session_start,
                session_end,
                _slot_count(session_end - session_start, self.cadence),
            )
        else:
            trailing = self._previous + self.cadence

            if trailing < session_end:
                missing = _slot_count(session_end - trailing, self.cadence)

                if missing > 0:
                    self._append(trailing, session_end, missing)

        self._index += 1
        self._previous = None

    def _record_span(
        self,
        previous: datetime,
        current: datetime,
        *,
        leading: bool,
    ) -> None:
        if leading:
            if current <= previous:
                return

            missing = _slot_count(current - previous, self.cadence)
            gap_start = previous
        else:
            span = current - previous

            if span <= self.cadence:
                return

            missing = _slot_count(span, self.cadence) - 1
            gap_start = previous + self.cadence

        if missing > 0:
            self._append(gap_start, current, missing)

    def _append(self, start: datetime, end: datetime, missing: int) -> None:
        self.interval_count += 1
        self.missing_candles += missing

        if self.max_samples is None or len(self.intervals) < self.max_samples:
            self.intervals.append(
                MissingInterval(start=start, end=end, missing_candles=missing)
            )


def find_missing_trading_intervals(
    timestamps: Iterable[datetime],
    cadence: timedelta,
    *,
    start: datetime,
    end: datetime,
    calendar: MarketCalendar,
) -> list[MissingInterval]:
    """
    Return the stretches of expected *trading* time with no candles.

    The whole-list form of :class:`TradingGapScanner`, kept for callers that
    already hold every timestamp. Sorting first means the two agree by
    construction rather than by two implementations happening to match.
    """
    scanner = TradingGapScanner(
        cadence,
        start=start,
        end=end,
        calendar=calendar,
    )
    scanner.feed(sorted(timestamps))

    return scanner.finish()


def expected_candle_count(
    start: datetime,
    end: datetime,
    cadence: timedelta,
) -> int:
    """Return how many candles fit in the half-open range ``[start, end)``."""
    if cadence <= timedelta(0):
        raise ValueError("cadence must be positive")

    if start >= end:
        return 0

    return _slot_count(end - start, cadence)
