from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from marketdata.calendar.base import MarketCalendar
from marketdata.downloader.pipeline import DownloadPipeline, DownloadResult
from marketdata.providers.base import MarketDataProvider
from marketdata.providers.errors import ProviderError
from marketdata.quality.dataset import (
    UNREADABLE_DATASET_ERRORS,
    DatasetValidationReport,
    validate_dataset,
)
from marketdata.quality.report import QualityStatus
from marketdata.verification.guard import GuardDecision, check_prerequisites
from marketdata.verification.preflight import classify_failure
from marketdata.verification.records import (
    VerificationRecord,
    VerificationStore,
    build_record,
)
from marketdata.verification.stages import Stage, stage_end
from marketdata.verification.status import (
    QualityThresholds,
    VerificationStatus,
)


@dataclass
class StageOutcome:
    """What running one stage produced."""

    stage: Stage
    status: VerificationStatus
    guard: GuardDecision
    result: DownloadResult | None
    validation: DatasetValidationReport | None
    record: VerificationRecord | None
    record_path: Path | None
    problems: list[str]

    @property
    def verified(self) -> bool:
        return self.status.verified


def resolve_range(
    stage: Stage,
    start: datetime,
    end: datetime | None = None,
) -> tuple[datetime, datetime]:
    """Return the window a stage covers from the requested start."""
    if stage is Stage.HISTORICAL:
        if end is None:
            raise ValueError("a historical stage needs an explicit end")

        return start, end

    if end is not None:
        raise ValueError(
            f"the {stage.value} stage decides its own end; do not supply one"
        )

    return start, stage_end(stage, start)


def _grade(
    validation: DatasetValidationReport,
    thresholds: QualityThresholds,
) -> tuple[VerificationStatus, list[str]]:
    """
    Turn a validation report into a verdict.

    Structural defects are reported by the validator as ``invalid`` and are
    never tolerated. A shortfall of candles is graded against the configured
    ratio, because some minutes genuinely have no ticks.
    """
    problems = list(validation.problems)

    if validation.status is QualityStatus.INVALID:
        return VerificationStatus.FAIL, problems

    if validation.status is QualityStatus.EMPTY:
        return VerificationStatus.FAIL, [*problems, "no candles were stored"]

    missing = thresholds.grade_missing(
        validation.missing_candles,
        validation.expected_candles,
    )

    return missing, problems


def run_stage(
    provider: MarketDataProvider,
    *,
    stage: Stage,
    symbol: str,
    start: datetime,
    end: datetime | None = None,
    timeframe: str = "1min",
    data_root: str | Path = "data/processed",
    manifest_root: str | Path = "data/manifests",
    quality_root: str | Path = "data/quality",
    checkpoint_root: str | Path = "data/checkpoints",
    verification_root: str | Path = "data/verification",
    calendar: MarketCalendar | None = None,
    chunk_size=None,
    thresholds: QualityThresholds | None = None,
    live: bool = False,
    override: bool = False,
    store: VerificationStore | None = None,
) -> StageOutcome:
    """
    Run one acquisition stage through the production pipeline and judge it.

    There is deliberately no shortcut path here: the stage downloads through
    :class:`~marketdata.downloader.pipeline.DownloadPipeline`, so what is
    verified is the same code a real acquisition uses — chunking, rate
    limiting, retries, checkpoints, merging, manifests and quality reporting
    included. The result is then re-inspected from disk with the dataset
    validator, because a command exiting zero is not evidence.
    """
    thresholds = thresholds or QualityThresholds()
    store = store or VerificationStore(verification_root)
    configuration = provider.configuration()

    guard = check_prerequisites(
        store,
        provider=provider.name,
        configuration=configuration,
        symbol=symbol,
        timeframe=timeframe,
        stage=stage,
        override=override,
    )

    if not guard.allowed:
        return StageOutcome(
            stage=stage,
            status=VerificationStatus.BLOCKED,
            guard=guard,
            result=None,
            validation=None,
            record=None,
            record_path=None,
            problems=[guard.reason],
        )

    window_start, window_end = resolve_range(stage, start, end)

    pipeline = DownloadPipeline(
        provider,
        output_root=data_root,
        manifest_root=manifest_root,
        quality_root=quality_root,
        checkpoint_root=checkpoint_root,
        calendar=calendar,
        chunk_size=chunk_size,
    )

    def inspect() -> DatasetValidationReport:
        return validate_dataset(
            symbol=symbol,
            timeframe=timeframe,
            data_root=data_root,
            manifest_root=manifest_root,
            calendar=calendar or pipeline.calendar,
            start=window_start,
            end=window_end,
        )

    try:
        result = pipeline.run(
            symbol=symbol,
            start=window_start,
            end=window_end,
            timeframe=timeframe,
        )
    except ProviderError as exc:
        status, detail = classify_failure(exc)

        return StageOutcome(
            stage=stage,
            status=status,
            guard=guard,
            result=None,
            validation=None,
            record=None,
            record_path=None,
            problems=[detail],
        )
    except UNREADABLE_DATASET_ERRORS as exc:
        # The stored dataset could not be read back, so the run could not
        # finish. The validator survives corruption and names the file, so
        # inspect anyway rather than reporting only the traceback's message.
        return StageOutcome(
            stage=stage,
            status=VerificationStatus.FAIL,
            guard=guard,
            result=None,
            validation=inspect(),
            record=None,
            record_path=None,
            problems=[f"the stored dataset could not be read: {exc}"],
        )

    validation = inspect()

    if result.chunks_failed:
        # The acquisition did not complete. A structural defect in what did
        # arrive is still a finding and stands; otherwise nothing has been
        # proven either way, which is what BLOCKED means.
        structural = validation.status is QualityStatus.INVALID
        status = VerificationStatus.FAIL if structural else VerificationStatus.BLOCKED
        problems = [*validation.problems, *result.failures]
    else:
        status, problems = _grade(validation, thresholds)

    record = build_record(
        stage=stage,
        status=status,
        provider=provider.name,
        configuration=configuration,
        symbol=symbol,
        timeframe=timeframe,
        requested_start=window_start,
        requested_end=window_end,
        validation=validation,
        quality_status=result.quality.status.value,
        validation_passed=validation.ok,
        manifest=result.manifest,
        quality_report=result.quality_report_path,
        thresholds=thresholds.describe(),
        problems=problems,
        live=live,
    )

    record_path = store.save(record)

    return StageOutcome(
        stage=stage,
        status=status,
        guard=guard,
        result=result,
        validation=validation,
        record=record,
        record_path=record_path,
        problems=problems,
    )


__all__ = ["StageOutcome", "resolve_range", "run_stage"]
