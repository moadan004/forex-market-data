from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from pydantic import BaseModel, Field


def application_version() -> str:
    """Return the installed project version, or 'unknown' outside a install."""
    try:
        return version("forex-market-data")
    except PackageNotFoundError:  # pragma: no cover - only in odd installs
        return "unknown"


class DatasetProvenance(BaseModel):
    """
    How a dataset came to exist.

    Enough to reproduce the acquisition, and to judge later whether a result
    built on this data can be trusted. Provider configuration is recorded as
    the provider describes itself, which never includes a credential: a key
    is reported as set or unset, never by value.
    """

    provider: str
    provider_configuration: dict[str, str] = Field(default_factory=dict)
    symbol: str
    timeframe: str
    requested_start: datetime
    requested_end: datetime
    actual_start: datetime | None
    actual_end: datetime | None
    rows: int
    quality_status: str
    calendar: str
    chunk_size: str
    rate_limit: str | None
    retry: str | None
    acquired_at: datetime
    application_version: str = Field(default_factory=application_version)
    verification: str | None = None
    unverified_override: bool = False


class DatasetManifest(BaseModel):
    """Metadata describing one downloaded dataset."""

    symbol: str
    timeframe: str
    start: datetime
    end: datetime
    actual_start: datetime | None = None
    actual_end: datetime | None = None
    row_count: int
    provider: str
    dataset_version: str = "1.0.0"
    created_at: datetime
    files: list[str]
    """Partition files, relative to the dataset root when possible.

    A manifest outlives the working directory it was written from and travels
    with the dataset it describes, so an absolute or CWD-relative path would
    stop resolving as soon as either moved.
    """

    quality_report: str | None = None
    quality_status: str | None = None
    checkpoint: str | None = None
    provenance: DatasetProvenance | None = None


def _relative_to(path: Path, root: str | Path | None) -> str:
    """Express a partition path relative to the dataset root when possible."""
    if root is None:
        return str(path)

    try:
        return path.relative_to(Path(root)).as_posix()
    except ValueError:
        return str(path)


def create_manifest(
    *,
    symbol: str,
    timeframe: str,
    candles_count: int,
    start: datetime,
    end: datetime,
    provider: str,
    files: list[Path],
    root: str | Path | None = None,
    actual_start: datetime | None = None,
    actual_end: datetime | None = None,
    quality_report: str | Path | None = None,
    quality_status: str | None = None,
    checkpoint: str | Path | None = None,
    provenance: DatasetProvenance | None = None,
) -> DatasetManifest:
    return DatasetManifest(
        symbol=symbol,
        timeframe=timeframe,
        start=start,
        end=end,
        actual_start=actual_start,
        actual_end=actual_end,
        row_count=candles_count,
        provider=provider,
        created_at=datetime.now(UTC),
        files=[_relative_to(path, root) for path in files],
        quality_report=str(quality_report) if quality_report is not None else None,
        quality_status=quality_status,
        checkpoint=str(checkpoint) if checkpoint is not None else None,
        provenance=provenance,
    )


def write_manifest(
    manifest: DatasetManifest,
    output_path: str | Path,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(manifest.model_dump_json(indent=2))
    return path
