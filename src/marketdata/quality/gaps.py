from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta
from itertools import pairwise

from pydantic import BaseModel


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
