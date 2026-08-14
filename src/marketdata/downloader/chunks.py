from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta

DURATION_UNITS: dict[str, timedelta] = {
    "min": timedelta(minutes=1),
    "h": timedelta(hours=1),
    "d": timedelta(days=1),
    "w": timedelta(weeks=1),
}

_CHUNK_SIZE_PATTERN = re.compile(
    r"^(?P<amount>\d+)\s*(?P<unit>month|months|min|h|d|w)$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Chunk:
    """One contiguous half-open slice of a requested download range."""

    index: int
    start: datetime
    end: datetime

    @property
    def duration(self) -> timedelta:
        return self.end - self.start


class ChunkSize(ABC):
    """A rule for placing chunk boundaries on the timeline."""

    @property
    @abstractmethod
    def label(self) -> str:
        """Return the canonical text form of this size."""
        raise NotImplementedError

    @abstractmethod
    def floor(self, moment: datetime) -> datetime:
        """Return the boundary at or before ``moment``."""
        raise NotImplementedError

    @abstractmethod
    def advance(self, boundary: datetime) -> datetime:
        """Return the boundary after ``boundary``."""
        raise NotImplementedError

    def __str__(self) -> str:
        return self.label


@dataclass(frozen=True)
class MonthlyChunkSize(ChunkSize):
    """
    Boundaries on calendar month starts.

    Multi-month sizes are aligned from January, so a three-month size always
    breaks at January, April, July and October regardless of the request.
    """

    months: int = 1

    def __post_init__(self) -> None:
        if self.months < 1:
            raise ValueError("months must be at least 1")

    @property
    def label(self) -> str:
        return f"{self.months}month" if self.months == 1 else f"{self.months}months"

    def floor(self, moment: datetime) -> datetime:
        month = (moment.month - 1) // self.months * self.months + 1

        return moment.replace(
            month=month,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    def advance(self, boundary: datetime) -> datetime:
        total = boundary.year * 12 + (boundary.month - 1) + self.months

        return boundary.replace(
            year=total // 12,
            month=total % 12 + 1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )


@dataclass(frozen=True)
class DurationChunkSize(ChunkSize):
    """
    Boundaries on a fixed duration, anchored to the Unix epoch.

    Anchoring to the epoch rather than to the requested start keeps the
    boundaries identical between a fresh download and a resumed one that
    asks for a different start.
    """

    amount: int
    unit: str

    def __post_init__(self) -> None:
        if self.amount < 1:
            raise ValueError("amount must be at least 1")

        if self.unit not in DURATION_UNITS:
            raise ValueError(
                f"Unknown chunk unit: {self.unit}. Available: {sorted(DURATION_UNITS)}"
            )

    @property
    def delta(self) -> timedelta:
        return DURATION_UNITS[self.unit] * self.amount

    @property
    def label(self) -> str:
        return f"{self.amount}{self.unit}"

    def floor(self, moment: datetime) -> datetime:
        epoch = datetime(1970, 1, 1, tzinfo=moment.tzinfo)
        elapsed = moment - epoch

        return epoch + self.delta * (elapsed // self.delta)

    def advance(self, boundary: datetime) -> datetime:
        return boundary + self.delta


def parse_chunk_size(text: str) -> ChunkSize:
    """
    Parse a chunk size such as ``1month``, ``3months``, ``7d`` or ``12h``.

    Supported units are ``month``/``months``, ``w``, ``d``, ``h`` and
    ``min``.
    """
    match = _CHUNK_SIZE_PATTERN.match(text.strip())

    if match is None:
        raise ValueError(
            f"Invalid chunk size: {text!r}. "
            "Expected a count and a unit, for example 1month, 7d or 12h."
        )

    amount = int(match.group("amount"))
    unit = match.group("unit").lower()

    if unit.startswith("month"):
        return MonthlyChunkSize(months=amount)

    return DurationChunkSize(amount=amount, unit=unit)


def plan_chunks(
    start: datetime,
    end: datetime,
    size: ChunkSize,
) -> list[Chunk]:
    """
    Split ``[start, end)`` into contiguous, non-overlapping chunks.

    Interior boundaries sit on the grid defined by ``size``; the first and
    last chunk are truncated to the requested range. The chunks therefore
    cover the range exactly, with no gaps and no overlap, and the same
    request always produces the same boundaries.
    """
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start and end must be timezone-aware")

    if start >= end:
        raise ValueError("start must be before end")

    chunks: list[Chunk] = []
    cursor = start
    boundary = size.floor(start)

    while cursor < end:
        while boundary <= cursor:
            boundary = size.advance(boundary)

        stop = min(boundary, end)

        chunks.append(Chunk(index=len(chunks), start=cursor, end=stop))

        cursor = stop

    return chunks
