from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel

from marketdata.calendar.base import ClosedInterval, MarketCalendar
from marketdata.models.timeframe import timeframe_cadence
from marketdata.quality.gaps import MissingInterval, TradingGapScanner
from marketdata.validation.candles import CandleViolation

MAX_VIOLATION_SAMPLES = 20


@dataclass(frozen=True)
class Coverage:
    """What a single pass over a timestamp stream established."""

    rows: int
    first: datetime | None
    last: datetime | None


def _scan_coverage(
    timestamps: Iterable[datetime],
    scanner: TradingGapScanner | None,
) -> Coverage:
    """
    Walk a timestamp stream once, counting it and feeding gap detection.

    Nothing is retained: the extent of the data is three values regardless
    of how many timestamps went past.
    """
    rows = 0
    first: datetime | None = None
    last: datetime | None = None

    for timestamp in timestamps:
        rows += 1

        if first is None:
            first = timestamp

        last = timestamp

        if scanner is not None:
            scanner.feed_one(timestamp)

    return Coverage(rows=rows, first=first, last=last)


class QualityStatus(StrEnum):
    """Overall verdict for one downloaded dataset."""

    OK = "ok"
    FAILED = "failed"
    EMPTY = "empty"
    INVALID = "invalid"
    INCOMPLETE = "incomplete"


class QualityReport(BaseModel):
    """
    Machine-readable quality summary for one download.

    Counts fall into two groups. ``downloaded_rows``, ``duplicates_removed``,
    ``invalid_rows`` and ``out_of_range_rows`` describe the chunks this run
    actually fetched, and reconcile as
    ``downloaded_rows - out_of_range_rows - invalid_rows - duplicates_removed``.
    ``retained_rows``, the actual range and the intervals describe the stored
    dataset over the requested range, so a fully resumed run reports no
    downloaded rows and a complete dataset.
    """

    provider: str
    symbol: str
    timeframe: str
    calendar: str
    requested_start: datetime
    requested_end: datetime
    actual_start: datetime | None
    actual_end: datetime | None
    expected_rows: int | None
    downloaded_rows: int
    retained_rows: int
    duplicates_removed: int
    invalid_rows: int
    out_of_range_rows: int
    missing_intervals: list[MissingInterval]
    """Up to ``MAX_VIOLATION_SAMPLES`` gaps, earliest first.

    Bounded on purpose: a systematically broken multi-year acquisition can
    produce a gap per candle, and a report that grows with the defect cannot
    be written for the dataset that needs it most. ``missing_interval_count``
    and ``missing_candles`` remain exact.
    """

    missing_interval_count: int
    missing_intervals_truncated: bool
    missing_candles: int
    market_closed_intervals: list[ClosedInterval]
    cadence_seconds: int | None
    chunks_total: int
    chunks_completed: int
    chunks_failed: int
    provider_retries: int
    rate_limit_requests_per_second: float | None
    status: QualityStatus
    violations: list[CandleViolation]
    violations_truncated: bool
    generated_at: datetime


def _status(
    *,
    retained_rows: int,
    invalid_rows: int,
    chunks_failed: int,
    expected_rows: int | None,
    missing_intervals: int,
) -> QualityStatus:
    if chunks_failed > 0:
        return QualityStatus.FAILED

    # A window that is entirely a market closure expects nothing and is
    # therefore complete, not empty.
    if retained_rows == 0 and expected_rows != 0:
        return QualityStatus.EMPTY

    if invalid_rows > 0:
        return QualityStatus.INVALID

    if missing_intervals > 0:
        return QualityStatus.INCOMPLETE

    return QualityStatus.OK


def build_quality_report(
    *,
    provider: str,
    symbol: str,
    timeframe: str,
    calendar: MarketCalendar,
    requested_start: datetime,
    requested_end: datetime,
    timestamps: Iterable[datetime],
    downloaded_rows: int,
    duplicates_removed: int,
    out_of_range_rows: int,
    violations: list[CandleViolation],
    chunks_total: int = 1,
    chunks_completed: int = 1,
    chunks_failed: int = 0,
    provider_retries: int = 0,
    rate_limit_requests_per_second: float | None = None,
) -> QualityReport:
    """
    Build the quality report for one completed pipeline run.

    ``timestamps`` are the stored candle timestamps covering the requested
    range, in chronological order. It is consumed in a single pass and may
    be a generator over the stored partitions, so a report can be built for
    a multi-year one-minute dataset without holding its timestamps.
    """
    cadence = timeframe_cadence(timeframe)

    expected_rows: int | None = None
    scanner: TradingGapScanner | None = None

    if cadence is not None:
        scanner = TradingGapScanner(
            cadence,
            start=requested_start,
            end=requested_end,
            calendar=calendar,
            max_samples=MAX_VIOLATION_SAMPLES,
        )
        expected_rows = calendar.expected_candle_count(
            requested_start,
            requested_end,
            cadence,
        )

    coverage = _scan_coverage(timestamps, scanner)

    missing_intervals = scanner.finish() if scanner is not None else []
    missing_interval_count = scanner.interval_count if scanner is not None else 0
    missing_candles = scanner.missing_candles if scanner is not None else 0

    return QualityReport(
        provider=provider,
        symbol=symbol,
        timeframe=timeframe,
        calendar=calendar.name,
        requested_start=requested_start,
        requested_end=requested_end,
        actual_start=coverage.first,
        actual_end=coverage.last,
        expected_rows=expected_rows,
        downloaded_rows=downloaded_rows,
        retained_rows=coverage.rows,
        duplicates_removed=duplicates_removed,
        invalid_rows=len(violations),
        out_of_range_rows=out_of_range_rows,
        missing_intervals=missing_intervals,
        missing_interval_count=missing_interval_count,
        missing_intervals_truncated=missing_interval_count > len(missing_intervals),
        missing_candles=missing_candles,
        market_closed_intervals=calendar.closed_intervals(
            requested_start,
            requested_end,
        ),
        cadence_seconds=int(cadence.total_seconds()) if cadence else None,
        chunks_total=chunks_total,
        chunks_completed=chunks_completed,
        chunks_failed=chunks_failed,
        provider_retries=provider_retries,
        rate_limit_requests_per_second=rate_limit_requests_per_second,
        status=_status(
            retained_rows=coverage.rows,
            invalid_rows=len(violations),
            chunks_failed=chunks_failed,
            expected_rows=expected_rows,
            missing_intervals=missing_interval_count,
        ),
        violations=violations[:MAX_VIOLATION_SAMPLES],
        violations_truncated=len(violations) > MAX_VIOLATION_SAMPLES,
        generated_at=datetime.now(UTC),
    )


def write_quality_report(
    report: QualityReport,
    output_path: str | Path,
) -> Path:
    """Persist a quality report as JSON."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report.model_dump_json(indent=2))
    return path
