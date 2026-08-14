from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel

from marketdata.models.candle import Candle
from marketdata.models.timeframe import timeframe_cadence
from marketdata.quality.gaps import (
    MissingInterval,
    expected_candle_count,
    find_missing_intervals,
)
from marketdata.validation.candles import CandleViolation

MAX_VIOLATION_SAMPLES = 20


class QualityStatus(StrEnum):
    """Overall verdict for one downloaded dataset."""

    OK = "ok"
    EMPTY = "empty"
    INVALID = "invalid"
    INCOMPLETE = "incomplete"


class QualityReport(BaseModel):
    """
    Machine-readable quality summary for one download.

    Every count describes the same run of the ingestion pipeline, so the row
    arithmetic holds:
    ``downloaded_rows - out_of_range_rows - invalid_rows - duplicates_removed
    == final_rows``.
    """

    provider: str
    symbol: str
    timeframe: str
    requested_start: datetime
    requested_end: datetime
    actual_start: datetime | None
    actual_end: datetime | None
    downloaded_rows: int
    final_rows: int
    duplicates_removed: int
    invalid_rows: int
    out_of_range_rows: int
    missing_intervals: list[MissingInterval]
    missing_candles: int
    expected_rows: int | None
    cadence_seconds: int | None
    status: QualityStatus
    violations: list[CandleViolation]
    violations_truncated: bool
    generated_at: datetime


def _status(
    *,
    final_rows: int,
    invalid_rows: int,
    missing_intervals: list[MissingInterval],
) -> QualityStatus:
    if final_rows == 0:
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
    requested_start: datetime,
    requested_end: datetime,
    candles: list[Candle],
    downloaded_rows: int,
    duplicates_removed: int,
    out_of_range_rows: int,
    violations: list[CandleViolation],
) -> QualityReport:
    """Build the quality report for one completed pipeline run."""
    cadence = timeframe_cadence(timeframe)

    missing_intervals: list[MissingInterval] = []
    expected_rows: int | None = None

    if cadence is not None:
        missing_intervals = find_missing_intervals(
            (candle.timestamp for candle in candles),
            cadence,
            start=requested_start,
            end=requested_end,
        )
        expected_rows = expected_candle_count(
            requested_start,
            requested_end,
            cadence,
        )

    return QualityReport(
        provider=provider,
        symbol=symbol,
        timeframe=timeframe,
        requested_start=requested_start,
        requested_end=requested_end,
        actual_start=candles[0].timestamp if candles else None,
        actual_end=candles[-1].timestamp if candles else None,
        downloaded_rows=downloaded_rows,
        final_rows=len(candles),
        duplicates_removed=duplicates_removed,
        invalid_rows=len(violations),
        out_of_range_rows=out_of_range_rows,
        missing_intervals=missing_intervals,
        missing_candles=sum(interval.missing_candles for interval in missing_intervals),
        expected_rows=expected_rows,
        cadence_seconds=int(cadence.total_seconds()) if cadence else None,
        status=_status(
            final_rows=len(candles),
            invalid_rows=len(violations),
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
