from __future__ import annotations

from calendar import monthrange
from datetime import datetime, timedelta
from enum import StrEnum


class Stage(StrEnum):
    """
    Steps of a staged acquisition, smallest first.

    Each stage exists to prove something the next one relies on: that the
    provider answers at all, that a whole session comes back, that a month
    crosses weekends and partitions correctly, and only then that years of
    data are worth attempting.
    """

    SMOKE = "smoke"
    DAILY = "daily"
    MONTHLY = "monthly"
    HISTORICAL = "historical"

    @property
    def prerequisites(self) -> tuple[Stage, ...]:
        """Stages that must have been verified before this one is allowed."""
        order = ORDER

        return tuple(order[: order.index(self)])


ORDER: tuple[Stage, ...] = (
    Stage.SMOKE,
    Stage.DAILY,
    Stage.MONTHLY,
    Stage.HISTORICAL,
)

STAGE_DESCRIPTIONS: dict[Stage, str] = {
    Stage.SMOKE: "one hour — the provider answers and the row shape is usable",
    Stage.DAILY: "one day — a whole session, including the daily boundary",
    Stage.MONTHLY: "one month — weekends, multiple chunks and partitions",
    Stage.HISTORICAL: "an arbitrary larger range",
}


def add_months(moment: datetime, months: int) -> datetime:
    """
    Return ``moment`` advanced by whole calendar months.

    The day is clamped, so the last day of a long month lands on the last
    day of a shorter one rather than overflowing into the next.
    """
    total = moment.month - 1 + months
    year = moment.year + total // 12
    month = total % 12 + 1

    day = min(moment.day, _days_in_month(year, month))

    return moment.replace(year=year, month=month, day=day)


def _days_in_month(year: int, month: int) -> int:
    return monthrange(year, month)[1]


def stage_end(stage: Stage, start: datetime) -> datetime:
    """
    Return the end of a stage's window, exclusive.

    ``HISTORICAL`` has no fixed size; its range is whatever the caller asks
    for, so it has no derived end.
    """
    if stage is Stage.SMOKE:
        return start + timedelta(hours=1)

    if stage is Stage.DAILY:
        return start + timedelta(days=1)

    if stage is Stage.MONTHLY:
        return add_months(start, 1)

    raise ValueError(
        f"{stage.value} has no fixed duration; supply an explicit end instead"
    )


def stage_for_range(start: datetime, end: datetime) -> Stage:
    """
    Return the smallest stage whose window covers ``[start, end)``.

    Used to decide how much proof a download needs: a request no larger than
    a month is one of the verification stages itself, while anything longer
    is a historical acquisition.
    """
    if start >= end:
        raise ValueError("start must be before end")

    for stage in (Stage.SMOKE, Stage.DAILY, Stage.MONTHLY):
        if end <= stage_end(stage, start):
            return stage

    return Stage.HISTORICAL


__all__ = [
    "ORDER",
    "STAGE_DESCRIPTIONS",
    "Stage",
    "add_months",
    "stage_end",
    "stage_for_range",
]
