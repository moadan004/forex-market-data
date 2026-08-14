from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from marketdata.normalization.timestamps import ensure_utc, normalize_candles
from marketdata.providers.base import MarketDataProvider
from marketdata.quality.report import (
    QualityReport,
    build_quality_report,
    write_quality_report,
)
from marketdata.storage.manifest import (
    create_manifest,
    write_manifest,
)
from marketdata.storage.parquet import ParquetStorage, normalize_symbol_path
from marketdata.validation.candles import (
    CandleValidationError,
    deduplicate_candles,
    partition_candles,
    restrict_to_range,
    validate_candles,
)


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
    final_count: int
    duplicate_count: int
    invalid_count: int
    out_of_range_count: int
    files: list[Path]
    manifest: Path
    quality_report_path: Path
    quality: QualityReport


class DownloadPipeline:
    """
    Orchestrate the production ingestion path.

    Every stage is applied explicitly and in order::

        fetch -> normalize -> validate -> deduplicate -> validate again
              -> storage -> manifest -> quality report
    """

    def __init__(
        self,
        provider: MarketDataProvider,
        *,
        output_root: str | Path = "data/processed",
        manifest_root: str | Path = "data/manifests",
        quality_root: str | Path = "data/quality",
        strict: bool = False,
    ) -> None:
        self.provider = provider
        self.storage = ParquetStorage(output_root)
        self.manifest_root = Path(manifest_root)
        self.quality_root = Path(quality_root)
        self.strict = strict

    def run(
        self,
        *,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "1min",
    ) -> DownloadResult:
        start = ensure_utc(start)
        end = ensure_utc(end)

        if start >= end:
            raise ValueError("start must be before end")

        # 1. Fetch from the provider.
        downloaded = self.provider.fetch_candles(
            symbol=symbol,
            start=start,
            end=end,
            timeframe=timeframe,
        )
        downloaded_count = len(downloaded)

        # 2. Normalize every timestamp to UTC and order chronologically.
        normalized = normalize_candles(downloaded)

        # 3. Drop anything outside the requested range, so the dataset can
        #    never contain data from beyond the window it was asked for.
        in_range, out_of_range = restrict_to_range(normalized, start, end)

        # 4. Validate OHLC invariants, keeping a record of what was rejected.
        valid, violations = partition_candles(in_range)

        if self.strict and violations:
            raise CandleValidationError(
                f"{len(violations)} invalid candles for {symbol} {timeframe}: "
                f"{violations[0].reason}"
            )

        # 5. Remove duplicate timestamps.
        deduplicated = deduplicate_candles(valid)
        duplicate_count = len(valid) - len(deduplicated)

        # 6. Validate again: the stored dataset must satisfy the invariants
        #    after every earlier transformation, not just before them.
        validate_candles(deduplicated)

        final_count = len(deduplicated)

        # 7. Store as partitioned Parquet.
        files = self.storage.write(
            deduplicated,
            symbol=symbol,
            timeframe=timeframe,
        )

        report = build_quality_report(
            provider=self.provider.name,
            symbol=symbol,
            timeframe=timeframe,
            requested_start=start,
            requested_end=end,
            candles=deduplicated,
            downloaded_rows=downloaded_count,
            duplicates_removed=duplicate_count,
            out_of_range_rows=len(out_of_range),
            violations=violations,
        )

        slug = self._dataset_slug(timeframe, start, end)
        directory = normalize_symbol_path(symbol)
        quality_path = self.quality_root / directory / f"{slug}.json"

        # 8. Record the manifest.
        manifest = create_manifest(
            symbol=symbol,
            timeframe=timeframe,
            candles_count=final_count,
            start=start,
            end=end,
            actual_start=report.actual_start,
            actual_end=report.actual_end,
            provider=self.provider.name,
            files=files,
            quality_report=quality_path,
            quality_status=report.status.value,
        )

        manifest_path = write_manifest(
            manifest,
            self.manifest_root / directory / f"{slug}.json",
        )

        # 9. Persist the quality report.
        write_quality_report(report, quality_path)

        return DownloadResult(
            symbol=symbol,
            timeframe=timeframe,
            provider=self.provider.name,
            requested_start=start,
            requested_end=end,
            actual_start=report.actual_start,
            actual_end=report.actual_end,
            downloaded_count=downloaded_count,
            final_count=final_count,
            duplicate_count=duplicate_count,
            invalid_count=len(violations),
            out_of_range_count=len(out_of_range),
            files=files,
            manifest=manifest_path,
            quality_report_path=quality_path,
            quality=report,
        )

    @staticmethod
    def _dataset_slug(timeframe: str, start: datetime, end: datetime) -> str:
        return f"{timeframe}_{start:%Y%m%dT%H%M%SZ}_{end:%Y%m%dT%H%M%SZ}"
