from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from marketdata.verification.records import (
    VerificationRecord,
    VerificationStore,
    configuration_fingerprint,
)
from marketdata.verification.stages import Stage, stage_for_range


@dataclass(frozen=True)
class GuardDecision:
    """Whether an acquisition may proceed, and what it rests on."""

    stage: Stage
    allowed: bool
    required: tuple[Stage, ...] = ()
    satisfied: tuple[Stage, ...] = ()
    missing: tuple[Stage, ...] = ()
    overridden: bool = False
    evidence: dict[str, str] = field(default_factory=dict)

    @property
    def reason(self) -> str:
        if self.overridden:
            return (
                "proceeding without verification because the override was given: "
                f"{', '.join(stage.value for stage in self.missing)} unproven"
            )

        if self.allowed and not self.required:
            return f"{self.stage.value} needs no prior verification"

        if self.allowed:
            return "verified: " + ", ".join(
                f"{stage.value}={self.evidence[stage.value]}"
                for stage in self.satisfied
            )

        return (
            "refusing a "
            f"{self.stage.value} acquisition: "
            + ", ".join(stage.value for stage in self.missing)
            + " not verified for this provider and configuration"
        )


def check_prerequisites(
    store: VerificationStore,
    *,
    provider: str,
    configuration: dict[str, str],
    symbol: str,
    timeframe: str,
    stage: Stage,
    override: bool = False,
) -> GuardDecision:
    """
    Decide whether a stage may run, given what has already been proven.

    A stage may only run once every smaller stage has been verified against
    the same provider configuration. The point is that nobody reaches for
    years of one-minute data before a single hour of it has been shown to
    arrive, parse, store, validate and report correctly.

    The override exists because a judgement call sometimes has to be made by
    a person. It is never implicit: it must be asked for, it is reported in
    the decision, and it is written into the dataset's provenance so the
    resulting data carries the fact that it was unverified.
    """
    required = stage.prerequisites
    fingerprint = configuration_fingerprint(configuration)

    satisfied: list[Stage] = []
    missing: list[Stage] = []
    evidence: dict[str, str] = {}

    for prerequisite in required:
        record = store.valid_record(
            provider=provider,
            symbol=symbol,
            timeframe=timeframe,
            stage=prerequisite,
            fingerprint=fingerprint,
        )

        if record is None:
            missing.append(prerequisite)
            continue

        satisfied.append(prerequisite)
        evidence[prerequisite.value] = _describe(record)

    if missing and override:
        return GuardDecision(
            stage=stage,
            allowed=True,
            required=required,
            satisfied=tuple(satisfied),
            missing=tuple(missing),
            overridden=True,
            evidence=evidence,
        )

    return GuardDecision(
        stage=stage,
        allowed=not missing,
        required=required,
        satisfied=tuple(satisfied),
        missing=tuple(missing),
        evidence=evidence,
    )


def check_download(
    store: VerificationStore,
    *,
    provider: str,
    configuration: dict[str, str],
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    override: bool = False,
) -> GuardDecision:
    """
    Guard a download by the size of the range it asks for.

    A request no larger than a month is itself one of the verification
    stages, so it is allowed unguarded — that is how the evidence gets made
    in the first place, and refusing it would leave no way to start.
    Anything longer is a historical acquisition and has to stand on that
    evidence.
    """
    stage = stage_for_range(start, end)

    if stage is not Stage.HISTORICAL:
        return GuardDecision(stage=stage, allowed=True)

    return check_prerequisites(
        store,
        provider=provider,
        configuration=configuration,
        symbol=symbol,
        timeframe=timeframe,
        stage=stage,
        override=override,
    )


def _describe(record: VerificationRecord) -> str:
    return f"{record.status.value} at {record.verified_at:%Y-%m-%dT%H:%M:%SZ}"


__all__ = ["GuardDecision", "check_download", "check_prerequisites"]
