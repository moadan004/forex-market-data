from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel


class DatasetManifest(BaseModel):
    """Metadata describing one downloaded dataset."""

    symbol: str
    timeframe: str
    start: datetime
    end: datetime
    row_count: int
    provider: str
    dataset_version: str = "1.0.0"
    created_at: datetime
    files: list[str]


def create_manifest(
    *,
    symbol: str,
    timeframe: str,
    candles_count: int,
    start: datetime,
    end: datetime,
    provider: str,
    files: list[Path],
) -> DatasetManifest:
    return DatasetManifest(
        symbol=symbol,
        timeframe=timeframe,
        start=start,
        end=end,
        row_count=candles_count,
        provider=provider,
        created_at=datetime.now(UTC),
        files=[str(path) for path in files],
    )


def write_manifest(
    manifest: DatasetManifest,
    output_path: str | Path,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(manifest.model_dump_json(indent=2))
    return path
