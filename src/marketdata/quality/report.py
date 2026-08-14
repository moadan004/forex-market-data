from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel

from marketdata.calendar.base import ClosedInterval, MarketCalendar
from marketdata.models.timeframe import timeframe_cadence
from marketdata.quality.gaps import MissingInterval, find_missing_trading_intervals
from marketdata.validation.candles import CandleViolation

MAX_VIOLATION_SAMPLES = 20


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
    missing_intervals: list[MissingInterval],
) -> QualityStatus:
    if chunks_failed > 0:
        return QualityStatus.FAILED

    # A window that is entirely a market closure expects nothing and is
    # therefore complete, not empty.
    if retained_rows == 0 and expected_rows != 0:
        return QualityStatus.EMPTY

    if invalid_rows > 0:
        return QualityStatus.INVALID

    if missing_intervals:
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
    timestamps: list[datetime],
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
    range, ordered chronologically.
    """
    cadence = timeframe_cadence(timeframe)

    missing_intervals: list[MissingInterval] = []
    expected_rows: int | None = None

    if cadence is not None:
        missing_intervals = find_missing_trading_intervals(
            timestamps,
            cadence,
            start=requested_start,
            end=requested_end,
            calendar=calendar,
        )
        expected_rows = calendar.expected_candle_count(
            requested_start,
            requested_end,
            cadence,
        )

    return QualityReport(
        provider=provider,
        symbol=symbol,
        timeframe=timeframe,
        calendar=calendar.name,
        requested_start=requested_start,
        requested_end=requested_end,
        actual_start=timestamps[0] if timestamps else None,
        actual_end=timestamps[-1] if timestamps else None,
        expected_rows=expected_rows,
        downloaded_rows=downloaded_rows,
        retained_rows=len(timestamps),
        duplicates_removed=duplicates_removed,
        invalid_rows=len(violations),
        out_of_range_rows=out_of_range_rows,
        missing_intervals=missing_intervals,
        missing_candles=sum(interval.missing_candles for interval in missing_intervals),
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
            retained_rows=len(timestamps),
            invalid_rows=len(violations),
            chunks_failed=chunks_failed,
            expected_rows=expected_rows,
            missing_intervals=missing_intervals,
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
