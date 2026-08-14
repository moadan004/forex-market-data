from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from marketdata.providers.base import MarketDataProvider
from marketdata.storage.manifest import (
    create_manifest,
    write_manifest,
)
from marketdata.storage.parquet import ParquetStorage
from marketdata.validation.candles import (
    deduplicate_candles,
    validate_candles,
)


@dataclass(frozen=True)
class DownloadResult:
    symbol: str
    timeframe: str
    provider: str
    requested_start: datetime
    requested_end: datetime
    downloaded_count: int
    final_count: int
    duplicate_count: int
    files: list[Path]
    manifest: Path


class DownloadPipeline:
    """Orchestrate download, normalization, validation and storage."""

    def __init__(
        self,
        provider: MarketDataProvider,
        *,
        output_root: str | Path = "data/processed",
        manifest_root: str | Path = "data/manifests",
    ) -> None:
        self.provider = provider
        self.storage = ParquetStorage(output_root)
        self.manifest_root = Path(manifest_root)

    def run(
        self,
        *,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "1min",
    ) -> DownloadResult:
        candles = self.provider.fetch_candles(
            symbol=symbol,
            start=start,
            end=end,
            timeframe=timeframe,
        )

        downloaded_count = len(candles)

        validate_candles(candles)

        deduplicated = deduplicate_candles(candles)
        final_count = len(deduplicated)
        duplicate_count = downloaded_count - final_count

        files = self.storage.write(
            deduplicated,
            symbol=symbol,
            timeframe=timeframe,
        )

        manifest = create_manifest(
            symbol=symbol,
            timeframe=timeframe,
            candles_count=final_count,
            start=start,
            end=end,
            provider=self.provider.name,
            files=files,
        )

        normalized_symbol = symbol.strip().upper().replace("/", "_")

        manifest_path = write_manifest(
            manifest,
            self.manifest_root
            / normalized_symbol
            / f"{timeframe}_{start:%Y%m%dT%H%M%SZ}_{end:%Y%m%dT%H%M%SZ}.json",
        )

        return DownloadResult(
            symbol=symbol,
            timeframe=timeframe,
            provider=self.provider.name,
            requested_start=start,
            requested_end=end,
            downloaded_count=downloaded_count,
            final_count=final_count,
            duplicate_count=duplicate_count,
            files=files,
            manifest=manifest_path,
        )
