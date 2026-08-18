from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from marketdata.calendar import MarketCalendar
from marketdata.calendar.forex import ForexCalendar
from marketdata.downloader.checkpoint import (
    CheckpointStore,
    ChunkCheckpoint,
    DownloadCheckpoint,
    mark_completed,
    mark_failed,
    mark_running,
)
from marketdata.downloader.chunks import (
    Chunk,
    ChunkSize,
    MonthlyChunkSize,
    plan_chunks,
)
from marketdata.normalization.timestamps import ensure_utc, normalize_candles
from marketdata.providers.base import MarketDataProvider
from marketdata.providers.errors import ProviderError
from marketdata.quality.report import (
    QualityReport,
    build_quality_report,
    write_quality_report,
)
from marketdata.storage.manifest import (
    DatasetProvenance,
    create_manifest,
    write_manifest,
)
from marketdata.storage.parquet import ParquetStorage, normalize_symbol_path
from marketdata.validation.candles import (
    CandleValidationError,
    CandleViolation,
    deduplicate_candles,
    partition_candles,
    restrict_to_range,
    validate_candles,
)


@dataclass
class ChunkOutcome:
    """What one chunk contributed to the run."""

    chunk: Chunk
    downloaded: int = 0
    out_of_range: int = 0
    duplicates: int = 0
    retained: int = 0
    retries: int = 0
    files: list[Path] = field(default_factory=list)
    violations: list[CandleViolation] = field(default_factory=list)
    error: str | None = None

    @property
    def failed(self) -> bool:
        return self.error is not None


@dataclass(frozen=True)
class DownloadResult:
    symbol: str
    timeframe: str
    provider: str
    requested_start: datetime
    requested_end: datetime
    actual_start: datetime | None
    actual_end: datetime | None
    downloaded_count: int
    retained_count: int
    duplicate_count: int
    invalid_count: int
    out_of_range_count: int
    chunks_total: int
    chunks_completed: int
    chunks_failed: int
    chunks_skipped: int
    chunks_repaired: list[int]
    """Chunks the checkpoint called complete whose data was no longer stored.

    Re-acquired rather than skipped. A non-empty list means the dataset had
    lost a partition since the run that wrote it — worth seeing, because
    nothing else in the run would have said so.
    """

    retries: int
    files_written: list[Path]
    rate_limit: str
    throttled_seconds: float
    failures: list[str]
    files: list[Path]
    manifest: Path
    quality_report_path: Path
    checkpoint_path: Path
    quality: QualityReport


class DownloadPipeline:
    """
    Orchestrate the production ingestion path.

    A request is planned into chunks, and each chunk runs the full ingestion
    sequence on its own::

        fetch -> normalize -> restrict to range -> validate -> deduplicate
              -> validate again -> merge into Parquet -> checkpoint

    Chunks are independent: one failing leaves the others' data and their
    checkpoint records intact, and a later run resumes from the first chunk
    that did not complete. The manifest and quality report are written once
    at the end and describe the stored dataset, not just this run's chunks.
    """

    def __init__(
        self,
        provider: MarketDataProvider,
        *,
        output_root: str | Path = "data/processed",
        manifest_root: str | Path = "data/manifests",
        quality_root: str | Path = "data/quality",
        checkpoint_root: str | Path = "data/checkpoints",
        calendar: MarketCalendar | None = None,
        chunk_size: ChunkSize | None = None,
        strict: bool = False,
        verification: str | None = None,
        unverified_override: bool = False,
    ) -> None:
        self.provider = provider
        self.storage = ParquetStorage(output_root)
        self.manifest_root = Path(manifest_root)
        self.quality_root = Path(quality_root)
        self.checkpoints = CheckpointStore(checkpoint_root)
        self.calendar = calendar or ForexCalendar()
        self.chunk_size = chunk_size or MonthlyChunkSize()
        self.strict = strict
        # Recorded in provenance so a dataset carries the evidence it was
        # acquired under, including the absence of it.
        self.verification = verification
        self.unverified_override = unverified_override

    def run(
        self,
        *,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "1min",
        resume: bool = True,
    ) -> DownloadResult:
        start = ensure_utc(start)
        end = ensure_utc(end)

        if start >= end:
            raise ValueError("start must be before end")

        chunks = plan_chunks(start, end, self.chunk_size)

        checkpoint = self.checkpoints.open(
            provider=self.provider.name,
            symbol=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
            chunks=chunks,
            chunk_size=self.chunk_size.label,
            resume=resume,
        )

        # Decided before any chunk runs, and never during the loop: chunks
        # share a partition, so a chunk that rewrites one would otherwise
        # convince the chunks after it that their own lost rows were back.
        stored = {
            chunk.index: self._already_stored(
                chunk,
                checkpoint.record(chunk.index),
                symbol=symbol,
                timeframe=timeframe,
            )
            for chunk in chunks
        }

        outcomes: list[ChunkOutcome] = []
        skipped = 0
        repaired = [
            chunk.index
            for chunk in chunks
            if checkpoint.record(chunk.index).is_complete and not stored[chunk.index]
        ]

        for chunk in chunks:
            if stored[chunk.index]:
                skipped += 1
                continue

            outcomes.append(
                self._run_chunk(
                    chunk,
                    symbol=symbol,
                    timeframe=timeframe,
                    checkpoint=checkpoint,
                )
            )

        checkpoint_path = self.checkpoints.save(checkpoint)

        return self._finish(
            symbol=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
            outcomes=outcomes,
            checkpoint=checkpoint,
            checkpoint_path=checkpoint_path,
            skipped=skipped,
            repaired=repaired,
        )

    def _already_stored(
        self,
        chunk: Chunk,
        record: ChunkCheckpoint,
        *,
        symbol: str,
        timeframe: str,
    ) -> bool:
        """
        Decide whether a chunk can be skipped on a resumed run.

        A checkpoint records what a run *did*, which is not the same as what
        is on disk now. Between two runs a partition can be deleted, lost to
        a failed copy, or corrupted, and trusting the checkpoint alone would
        skip the chunk and report a clean run over a hole — the exact
        outcome this project refuses to call success.

        So the checkpoint is reconciled against the data every time. A chunk
        that stored nothing stays complete: a window the market was closed
        for legitimately has no rows, and re-downloading it forever would be
        worse than useless.
        """
        if not record.is_complete:
            return False

        if record.row_count == 0:
            return True

        return self.storage.has_stored_rows(
            symbol=symbol,
            timeframe=timeframe,
            start=chunk.start,
            end=chunk.end,
        )

    def _run_chunk(
        self,
        chunk: Chunk,
        *,
        symbol: str,
        timeframe: str,
        checkpoint: DownloadCheckpoint,
    ) -> ChunkOutcome:
        mark_running(checkpoint, chunk.index)
        self.checkpoints.save(checkpoint)

        outcome = ChunkOutcome(chunk=chunk)

        # Provider retries are counted per chunk, so the checkpoint shows how
        # much work each chunk really cost rather than a running total.
        retry_stats = getattr(self.provider, "retry_stats", None)

        if retry_stats is not None:
            retry_stats.reset()

        try:
            # 1. Fetch this chunk from the provider.
            downloaded = self.provider.fetch_candles(
                symbol=symbol,
                start=chunk.start,
                end=chunk.end,
                timeframe=timeframe,
            )
            outcome.downloaded = len(downloaded)

            # 2. Normalize every timestamp to UTC and order chronologically.
            normalized = normalize_candles(downloaded)

            # 3. Drop anything outside the chunk, so the dataset can never
            #    contain data from beyond the window it was asked for.
            in_range, out_of_range = restrict_to_range(
                normalized,
                chunk.start,
                chunk.end,
            )
            outcome.out_of_range = len(out_of_range)

            # 4. Validate OHLC invariants, recording what was rejected.
            valid, violations = partition_candles(in_range)
            outcome.violations = violations

            if self.strict and violations:
                raise CandleValidationError(
                    f"{len(violations)} invalid candles for {symbol} {timeframe}: "
                    f"{violations[0].reason}"
                )

            # 5. Remove duplicate timestamps.
            deduplicated = deduplicate_candles(valid)
            outcome.duplicates = len(valid) - len(deduplicated)

            # 6. Validate again: the stored dataset must satisfy the
            #    invariants after every earlier transformation.
            validate_candles(deduplicated)
            outcome.retained = len(deduplicated)

            # 7. Merge into the partitioned Parquet dataset.
            outcome.files = self.storage.write(
                deduplicated,
                symbol=symbol,
                timeframe=timeframe,
            )
        except Exception as exc:
            retries = retry_stats.retries if retry_stats is not None else 0
            outcome.retries = retries
            outcome.error = f"{type(exc).__name__}: {exc}"

            mark_failed(
                checkpoint,
                chunk.index,
                outcome.error,
                retries=retries,
                error_kind=type(exc).__name__,
                retryable=(exc.retryable if isinstance(exc, ProviderError) else None),
            )
            self.checkpoints.save(checkpoint)

            if self.strict:
                raise
        else:
            outcome.retries = retry_stats.retries if retry_stats is not None else 0

            mark_completed(
                checkpoint,
                chunk.index,
                row_count=outcome.retained,
                files=outcome.files,
                retries=outcome.retries,
            )

        self.checkpoints.save(checkpoint)

        return outcome

    def _finish(
        self,
        *,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        outcomes: list[ChunkOutcome],
        checkpoint: DownloadCheckpoint,
        checkpoint_path: Path,
        skipped: int,
        repaired: list[int],
    ) -> DownloadResult:
        # The report describes what is on disk for the requested range, so a
        # resumed run reports the whole dataset and not only its own chunks.
        # Streamed a partition at a time: a seven-year one-minute range holds
        # millions of timestamps, and none of them need to be resident.
        timestamps = self.storage.iter_timestamps(
            symbol=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
        )

        violations = [
            violation for outcome in outcomes for violation in outcome.violations
        ]

        # Providers are not required to throttle, so ask rather than assume.
        rate_limit = getattr(self.provider, "rate_limit", None)
        limiter_stats = getattr(self.provider, "rate_limit_stats", None)

        written = sorted({path for outcome in outcomes for path in outcome.files})

        # The manifest describes the dataset covering the requested range, not
        # the subset this run happened to write. A resumed run writes nothing
        # yet still stands behind every file its range depends on.
        files = self.storage.partition_files(
            symbol=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
        )

        report = build_quality_report(
            provider=self.provider.name,
            symbol=symbol,
            timeframe=timeframe,
            calendar=self.calendar,
            requested_start=start,
            requested_end=end,
            timestamps=timestamps,
            downloaded_rows=sum(outcome.downloaded for outcome in outcomes),
            duplicates_removed=sum(outcome.duplicates for outcome in outcomes),
            out_of_range_rows=sum(outcome.out_of_range for outcome in outcomes),
            violations=violations,
            chunks_total=len(checkpoint.chunks),
            chunks_completed=checkpoint.completed_chunks,
            chunks_failed=checkpoint.failed_chunks,
            rate_limit_requests_per_second=(
                rate_limit.requests_per_second if rate_limit else None
            ),
            provider_retries=sum(outcome.retries for outcome in outcomes),
        )

        slug = self._dataset_slug(timeframe, start, end)
        directory = normalize_symbol_path(symbol)
        quality_path = self.quality_root / directory / f"{slug}.json"

        provenance = DatasetProvenance(
            provider=self.provider.name,
            provider_configuration=self.provider.configuration(),
            symbol=symbol,
            timeframe=timeframe,
            requested_start=start,
            requested_end=end,
            actual_start=report.actual_start,
            actual_end=report.actual_end,
            rows=report.retained_rows,
            quality_status=report.status.value,
            calendar=self.calendar.name,
            chunk_size=self.chunk_size.label,
            rate_limit=rate_limit.describe() if rate_limit else None,
            retry=(
                retry_policy.describe()
                if (retry_policy := getattr(self.provider, "retry_policy", None))
                else None
            ),
            acquired_at=datetime.now(UTC),
            verification=self.verification,
            unverified_override=self.unverified_override,
        )

        manifest = create_manifest(
            symbol=symbol,
            timeframe=timeframe,
            candles_count=report.retained_rows,
            start=start,
            end=end,
            actual_start=report.actual_start,
            actual_end=report.actual_end,
            provider=self.provider.name,
            files=files,
            root=self.storage.root,
            quality_report=quality_path,
            quality_status=report.status.value,
            checkpoint=checkpoint_path,
            provenance=provenance,
        )

        manifest_path = write_manifest(
            manifest,
            self.manifest_root / directory / f"{slug}.json",
        )

        write_quality_report(report, quality_path)

        return DownloadResult(
            symbol=symbol,
            timeframe=timeframe,
            provider=self.provider.name,
            requested_start=start,
            requested_end=end,
            actual_start=report.actual_start,
            actual_end=report.actual_end,
            downloaded_count=report.downloaded_rows,
            retained_count=report.retained_rows,
            duplicate_count=report.duplicates_removed,
            invalid_count=report.invalid_rows,
            out_of_range_count=report.out_of_range_rows,
            chunks_total=report.chunks_total,
            chunks_completed=report.chunks_completed,
            chunks_failed=report.chunks_failed,
            chunks_skipped=skipped,
            chunks_repaired=repaired,
            retries=sum(outcome.retries for outcome in outcomes),
            files_written=written,
            rate_limit=rate_limit.describe() if rate_limit else "none",
            throttled_seconds=(
                limiter_stats.total_wait_seconds if limiter_stats else 0.0
            ),
            failures=[
                f"chunk {outcome.chunk.index} "
                f"({outcome.chunk.start:%Y-%m-%dT%H:%M:%SZ} -> "
                f"{outcome.chunk.end:%Y-%m-%dT%H:%M:%SZ}): {outcome.error}"
                for outcome in outcomes
                if outcome.failed
            ],
            files=files,
            manifest=manifest_path,
            quality_report_path=quality_path,
            checkpoint_path=checkpoint_path,
            quality=report,
        )

    @staticmethod
    def _dataset_slug(timeframe: str, start: datetime, end: datetime) -> str:
        return f"{timeframe}_{start:%Y%m%dT%H%M%SZ}_{end:%Y%m%dT%H%M%SZ}"
