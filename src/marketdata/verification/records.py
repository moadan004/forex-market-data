from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, Field, ValidationError

from marketdata.storage.parquet import normalize_symbol_path
from marketdata.verification.stages import Stage
from marketdata.verification.status import VerificationStatus

RECORD_VERSION = 1
"""Schema version of a stored verification record.

A record written by an older, differently-shaped verification is not
evidence about today's pipeline, so a version bump invalidates it rather
than risking a misread.
"""


def configuration_fingerprint(configuration: dict[str, str]) -> str:
    """
    Return a stable fingerprint of a provider's configuration.

    Verification is only evidence about the setup it was performed against.
    Pointing at a different endpoint, or a different CSV source, invalidates
    what was proven, and comparing fingerprints is how that is noticed.
    """
    payload = json.dumps(configuration, sort_keys=True, separators=(",", ":"))

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


class VerificationRecord(BaseModel):
    """
    Durable evidence that one stage passed for one provider and instrument.

    Holds enough to answer, months later: what was tested, when, against
    which provider and configuration, over which range, which dataset it
    produced, and what the quality and validation verdicts were.
    """

    version: int = RECORD_VERSION
    stage: Stage
    status: VerificationStatus
    provider: str
    provider_configuration: dict[str, str] = Field(default_factory=dict)
    provider_fingerprint: str
    symbol: str
    timeframe: str
    requested_start: datetime
    requested_end: datetime
    actual_start: datetime | None
    actual_end: datetime | None
    rows: int
    expected_rows: int | None
    missing_candles: int
    duplicate_candles: int
    invalid_rows: int
    out_of_range_rows: int
    quality_status: str
    validation_status: str
    validation_passed: bool
    manifest: str | None
    quality_report: str | None
    dataset_files: list[str] = Field(default_factory=list)
    thresholds: str
    problems: list[str] = Field(default_factory=list)
    live: bool
    verified_at: datetime

    @property
    def verified(self) -> bool:
        return self.status.verified

    def applies_to(self, fingerprint: str) -> bool:
        """Whether this record is still evidence about a given setup."""
        return (
            self.version == RECORD_VERSION and self.provider_fingerprint == fingerprint
        )


class VerificationStore:
    """
    Persist verification records, one file per provider, symbol, timeframe
    and stage.
    """

    def __init__(self, root: str | Path = "data/verification") -> None:
        self.root = Path(root)

    def path_for(
        self,
        *,
        provider: str,
        symbol: str,
        timeframe: str,
        stage: Stage,
    ) -> Path:
        return (
            self.root
            / provider
            / normalize_symbol_path(symbol)
            / timeframe
            / f"{stage.value}.json"
        )

    def load(
        self,
        *,
        provider: str,
        symbol: str,
        timeframe: str,
        stage: Stage,
    ) -> VerificationRecord | None:
        """Return a stored record, or ``None`` when there is none to read."""
        path = self.path_for(
            provider=provider,
            symbol=symbol,
            timeframe=timeframe,
            stage=stage,
        )

        if not path.exists():
            return None

        try:
            return VerificationRecord.model_validate_json(path.read_text())
        except ValidationError:
            # A record we cannot read is not evidence of anything.
            return None

    def save(self, record: VerificationRecord) -> Path:
        """
        Write a record atomically.

        Verification is what a later download is allowed to rely on, so a
        half-written record must never be readable.
        """
        path = self.path_for(
            provider=record.provider,
            symbol=record.symbol,
            timeframe=record.timeframe,
            stage=record.stage,
        )
        path.parent.mkdir(parents=True, exist_ok=True)

        temporary = path.with_name(f"{path.name}.tmp")
        temporary.write_text(record.model_dump_json(indent=2))
        os.replace(temporary, path)

        return path

    def valid_record(
        self,
        *,
        provider: str,
        symbol: str,
        timeframe: str,
        stage: Stage,
        fingerprint: str,
    ) -> VerificationRecord | None:
        """
        Return a record only when it still applies and it passed.

        A record made against a different configuration, or one that did not
        pass, is deliberately not returned: stale evidence is worse than
        none, because it would be trusted.
        """
        record = self.load(
            provider=provider,
            symbol=symbol,
            timeframe=timeframe,
            stage=stage,
        )

        if record is None or not record.applies_to(fingerprint) or not record.verified:
            return None

        return record


def build_record(
    *,
    stage: Stage,
    status: VerificationStatus,
    provider: str,
    configuration: dict[str, str],
    symbol: str,
    timeframe: str,
    requested_start: datetime,
    requested_end: datetime,
    validation,
    quality_status: str,
    validation_passed: bool,
    manifest: Path | None,
    quality_report: Path | None,
    thresholds: str,
    problems: list[str],
    live: bool,
) -> VerificationRecord:
    """Assemble a record from a completed stage run."""
    return VerificationRecord(
        stage=stage,
        status=status,
        provider=provider,
        provider_configuration=configuration,
        provider_fingerprint=configuration_fingerprint(configuration),
        symbol=symbol.strip().upper(),
        timeframe=timeframe,
        requested_start=requested_start,
        requested_end=requested_end,
        actual_start=validation.actual_start,
        actual_end=validation.actual_end,
        rows=validation.candles,
        expected_rows=validation.expected_candles,
        missing_candles=validation.missing_candles,
        duplicate_candles=validation.duplicate_candles,
        invalid_rows=validation.invalid_rows,
        out_of_range_rows=validation.out_of_range_rows,
        quality_status=quality_status,
        validation_status=validation.status.value,
        validation_passed=validation_passed,
        manifest=str(manifest) if manifest else None,
        quality_report=str(quality_report) if quality_report else None,
        dataset_files=[partition.path for partition in validation.partitions],
        thresholds=thresholds,
        problems=problems,
        live=live,
        verified_at=datetime.now(UTC),
    )


__all__ = [
    "RECORD_VERSION",
    "VerificationRecord",
    "VerificationStore",
    "build_record",
    "configuration_fingerprint",
]
