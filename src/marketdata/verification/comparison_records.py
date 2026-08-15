from __future__ import annotations

import os
import re
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, Field, ValidationError

from marketdata.storage.manifest import application_version
from marketdata.storage.parquet import normalize_symbol_path
from marketdata.verification.comparison import (
    ComparisonReport,
    ComparisonThresholds,
    DatasetIdentity,
)
from marketdata.verification.records import configuration_fingerprint
from marketdata.verification.status import VerificationStatus

COMPARISON_RECORD_VERSION = 1
"""Schema version of a stored cross-provider comparison record.

A record written by an older, differently-shaped comparison is not evidence
about today's comparison, so a version bump invalidates every stored record
rather than risking a misread.
"""

_UNSAFE_PATH_CHARS = re.compile(r"[^A-Za-z0-9._-]+")


def comparison_key(
    *,
    left: DatasetIdentity,
    right: DatasetIdentity,
    symbol: str,
    timeframe: str,
    start: datetime | None,
    end: datetime | None,
    thresholds: ComparisonThresholds,
) -> dict[str, str]:
    """
    Everything a comparison result depends on.

    A stored comparison is evidence about exactly one situation: these two
    datasets, byte for byte, produced by these two providers under these
    configurations, over this symbol, timeframe and range, judged against
    these thresholds, by this version of the comparison itself. Change any
    of them and the old answer is no longer an answer to the new question.
    """
    return {
        "schema_version": str(COMPARISON_RECORD_VERSION),
        # Normalized the way the dataset directory is, so asking for EUR/USD
        # and EUR_USD does not expire a record about the same instrument.
        "symbol": normalize_symbol_path(symbol),
        "timeframe": timeframe,
        "start": start.astimezone(UTC).isoformat() if start else "",
        "end": end.astimezone(UTC).isoformat() if end else "",
        "left_provider": left.provider,
        "left_provider_fingerprint": left.provider_fingerprint,
        "left_dataset_fingerprint": left.dataset_fingerprint,
        "right_provider": right.provider,
        "right_provider_fingerprint": right.provider_fingerprint,
        "right_dataset_fingerprint": right.dataset_fingerprint,
        "thresholds": thresholds.fingerprint(),
    }


def comparison_fingerprint(
    *,
    left: DatasetIdentity,
    right: DatasetIdentity,
    symbol: str,
    timeframe: str,
    start: datetime | None,
    end: datetime | None,
    thresholds: ComparisonThresholds,
) -> str:
    """Return a stable fingerprint of a comparison's whole situation."""
    return configuration_fingerprint(
        comparison_key(
            left=left,
            right=right,
            symbol=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
            thresholds=thresholds,
        )
    )


class ComparisonRecord(BaseModel):
    """
    Durable evidence of one cross-provider comparison.

    Holds the whole report rather than a summary of it, so the answer and
    the question it answered never drift apart. Provider configurations
    travel as the providers describe themselves, which never includes a
    credential: a key is recorded as set or unset, never by value.
    """

    version: int = COMPARISON_RECORD_VERSION
    fingerprint: str
    key: dict[str, str] = Field(default_factory=dict)
    report: ComparisonReport
    application_version: str = Field(default_factory=application_version)
    recorded_at: datetime

    @property
    def status(self) -> VerificationStatus:
        return self.report.status

    @property
    def verified(self) -> bool:
        return self.report.verified

    @property
    def symbol(self) -> str:
        return self.report.symbol

    @property
    def timeframe(self) -> str:
        return self.report.timeframe

    def applies_to(self, fingerprint: str) -> bool:
        """
        Whether this record is still evidence about a given situation.

        Every input that could change the answer is folded into the
        fingerprint, so a stale dataset, a re-pointed provider, a different
        range or a loosened threshold all invalidate the record here rather
        than being trusted by accident.
        """
        return self.version == COMPARISON_RECORD_VERSION and (
            self.fingerprint == fingerprint
        )


def build_comparison_record(
    report: ComparisonReport,
    thresholds: ComparisonThresholds,
) -> ComparisonRecord:
    """Assemble a durable record from a completed comparison."""
    key = comparison_key(
        left=report.left,
        right=report.right,
        symbol=report.symbol,
        timeframe=report.timeframe,
        start=report.requested_start,
        end=report.requested_end,
        thresholds=thresholds,
    )

    return ComparisonRecord(
        fingerprint=configuration_fingerprint(key),
        key=key,
        report=report,
        recorded_at=datetime.now(UTC),
    )


def _safe(name: str) -> str:
    """Reduce a provider name to something usable as a directory name."""
    cleaned = _UNSAFE_PATH_CHARS.sub("-", name.strip()).strip("-")

    return cleaned or "unnamed"


class ComparisonStore:
    """
    Persist comparison records, one file per provider pair, symbol and
    timeframe.
    """

    def __init__(self, root: str | Path = "data/verification/comparisons") -> None:
        self.root = Path(root)

    def path_for(
        self,
        *,
        left_provider: str,
        right_provider: str,
        symbol: str,
        timeframe: str,
    ) -> Path:
        return (
            self.root
            / f"{_safe(left_provider)}__vs__{_safe(right_provider)}"
            / normalize_symbol_path(symbol)
            / timeframe
            / "comparison.json"
        )

    def path_for_report(self, report: ComparisonReport) -> Path:
        return self.path_for(
            left_provider=report.left.provider,
            right_provider=report.right.provider,
            symbol=report.symbol,
            timeframe=report.timeframe,
        )

    def load(
        self,
        *,
        left_provider: str,
        right_provider: str,
        symbol: str,
        timeframe: str,
    ) -> ComparisonRecord | None:
        """Return a stored record, or ``None`` when there is none to read."""
        path = self.path_for(
            left_provider=left_provider,
            right_provider=right_provider,
            symbol=symbol,
            timeframe=timeframe,
        )

        if not path.exists():
            return None

        try:
            return ComparisonRecord.model_validate_json(path.read_text())
        except ValidationError:
            # A record we cannot read is not evidence of anything.
            return None

    def save(self, record: ComparisonRecord) -> Path:
        """
        Write a record atomically.

        A half-written comparison must never be readable: the whole point of
        the record is that a later reader can rely on it.
        """
        path = self.path_for_report(record.report)
        path.parent.mkdir(parents=True, exist_ok=True)

        temporary = path.with_name(f"{path.name}.tmp")
        temporary.write_text(record.model_dump_json(indent=2))
        os.replace(temporary, path)

        return path

    def valid_record(
        self,
        *,
        left_provider: str,
        right_provider: str,
        symbol: str,
        timeframe: str,
        fingerprint: str,
    ) -> ComparisonRecord | None:
        """
        Return a record only when it still describes the current situation.

        Unlike a stage record this does not also require the comparison to
        have passed: a stored FAIL is exactly the finding a caller needs to
        see again, and hiding it would be worse than useless.
        """
        record = self.load(
            left_provider=left_provider,
            right_provider=right_provider,
            symbol=symbol,
            timeframe=timeframe,
        )

        if record is None or not record.applies_to(fingerprint):
            return None

        return record


__all__ = [
    "COMPARISON_RECORD_VERSION",
    "ComparisonRecord",
    "ComparisonStore",
    "build_comparison_record",
    "comparison_fingerprint",
    "comparison_key",
]
