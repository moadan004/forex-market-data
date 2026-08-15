from __future__ import annotations

from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ValidationError

from marketdata.calendar.base import ClosedInterval, MarketCalendar
from marketdata.calendar.forex import ForexCalendar
from marketdata.models.candle import Candle
from marketdata.models.timeframe import timeframe_cadence
from marketdata.quality.gaps import MissingInterval, find_missing_trading_intervals
from marketdata.quality.report import MAX_VIOLATION_SAMPLES, QualityStatus
from marketdata.storage.manifest import DatasetManifest
from marketdata.storage.parquet import (
    CANDLE_SCHEMA,
    ParquetStorage,
    normalize_symbol_path,
)
from marketdata.validation.candles import (
    CandleViolation,
    duplicate_timestamps,
    partition_candles,
    restrict_to_range,
)

UNREADABLE_DATASET_ERRORS = (
    pa.ArrowException,
    ArithmeticError,
    OSError,
    TypeError,
    ValueError,
)
"""What reading a damaged dataset can raise.

Arrow reports incompatible or corrupt files, the candle model raises a
decimal or validation error on a column of the wrong type, and the
filesystem raises when a file named by a partition has gone.
"""


class PartitionInfo(BaseModel):
    """One Parquet file making up the dataset."""

    model_config = {"frozen": True}

    path: str
    year: int | None
    month: int | None
    rows: int
    schema_matches: bool
    schema_differences: list[str]


class ManifestCheck(BaseModel):
    """Whether a manifest still describes what is stored."""

    model_config = {"frozen": True}

    path: str
    requested_start: datetime
    requested_end: datetime
    claimed_rows: int
    stored_rows: int
    missing_files: list[str]
    agrees: bool


class DatasetValidationReport(BaseModel):
    """
    Machine-readable verdict on an already-stored dataset.

    Produced by reading Parquet from disk. Nothing here contacts a provider,
    so a dataset can be re-checked at any time by anyone holding the files.
    """

    symbol: str
    timeframe: str
    calendar: str
    range_start: datetime | None
    range_end: datetime | None
    range_source: str
    actual_start: datetime | None
    actual_end: datetime | None
    cadence_seconds: int | None
    candles: int
    expected_candles: int | None
    missing_candles: int
    missing_intervals: list[MissingInterval]
    market_closed_intervals: list[ClosedInterval]
    duplicate_candles: int
    duplicate_timestamps: list[datetime]
    invalid_rows: int
    violations: list[CandleViolation]
    violations_truncated: bool
    out_of_range_rows: int
    unordered_rows: int
    partitions: list[PartitionInfo]
    files: int
    schema_consistent: bool
    readable: bool
    read_error: str | None
    manifests: list[ManifestCheck]
    problems: list[str]
    status: QualityStatus
    checked_at: datetime

    @property
    def ok(self) -> bool:
        return self.status is QualityStatus.OK


def schema_differences(schema: pa.Schema) -> list[str]:
    """
    Describe how a stored file's schema departs from the canonical one.

    A partition written with inferred types cannot be read alongside one
    written with the fixed schema, and the failure surfaces far from its
    cause. Naming the difference makes it fixable.
    """
    differences: list[str] = []

    for field in CANDLE_SCHEMA:
        if field.name not in schema.names:
            differences.append(f"missing column {field.name}")
            continue

        stored = schema.field(field.name)

        if stored.type != field.type:
            differences.append(f"{field.name} is {stored.type}, expected {field.type}")

    differences.extend(
        f"unexpected column {name}"
        for name in sorted(schema.names)
        if name not in CANDLE_SCHEMA.names
    )

    return differences


def partition_key(path: Path) -> tuple[int | None, int | None]:
    values = {
        piece.split("=", 1)[0]: piece.split("=", 1)[1]
        for piece in path.parts
        if "=" in piece
    }

    try:
        return int(values["year"]), int(values["month"])
    except (KeyError, ValueError):
        return None, None


def _inspect_partitions(
    storage: ParquetStorage,
    *,
    symbol: str,
    timeframe: str,
) -> list[PartitionInfo]:
    partitions: list[PartitionInfo] = []

    for path in storage.partition_files(symbol=symbol, timeframe=timeframe):
        year, month = partition_key(path)

        # A file that cannot even be opened is a finding, not a crash.
        try:
            differences = schema_differences(pq.read_schema(path))
            rows = pq.read_metadata(path).num_rows
        except UNREADABLE_DATASET_ERRORS as exc:
            differences = [f"unreadable ({type(exc).__name__}: {exc})"]
            rows = 0

        partitions.append(
            PartitionInfo(
                path=str(path),
                year=year,
                month=month,
                rows=rows,
                schema_matches=not differences,
                schema_differences=differences,
            )
        )

    return partitions


def _read_dataset(
    storage: ParquetStorage,
    *,
    symbol: str,
    timeframe: str,
) -> tuple[list[Candle], str | None]:
    """
    Read the stored candles, surviving a dataset that cannot be read.

    Validation exists to describe damaged datasets, so any failure is turned
    into a finding rather than an exception: incompatible partitions, a
    truncated file and a column of the wrong type all have to be reportable.
    """
    try:
        return storage.read_candles(symbol=symbol, timeframe=timeframe), None
    except UNREADABLE_DATASET_ERRORS as exc:
        return [], f"{type(exc).__name__}: {exc}"


def load_manifests(
    manifest_root: Path,
    *,
    symbol: str,
    timeframe: str,
) -> list[tuple[Path, DatasetManifest]]:
    directory = manifest_root / normalize_symbol_path(symbol)

    if not directory.exists():
        return []

    loaded: list[tuple[Path, DatasetManifest]] = []

    for path in sorted(directory.glob("*.json")):
        try:
            manifest = DatasetManifest.model_validate_json(path.read_text())
        except ValidationError:
            # An unreadable manifest is reported as a problem by the caller
            # rather than aborting the whole validation.
            continue

        if manifest.timeframe == timeframe:
            loaded.append((path, manifest))

    return loaded


def _manifest_file_exists(name: str, data_root: Path) -> bool:
    """
    Resolve a manifest's file reference.

    Paths are recorded relative to the dataset root; older manifests hold
    whatever the run wrote, so an as-given path is accepted too.
    """
    candidate = Path(name)

    return (data_root / candidate).exists() or candidate.exists()


def _check_manifests(
    manifests: list[tuple[Path, DatasetManifest]],
    candles: list[Candle],
    data_root: Path,
) -> list[ManifestCheck]:
    checks: list[ManifestCheck] = []

    for path, manifest in manifests:
        inside, _ = restrict_to_range(candles, manifest.start, manifest.end)
        missing_files = [
            name
            for name in manifest.files
            if not _manifest_file_exists(name, data_root)
        ]

        checks.append(
            ManifestCheck(
                path=str(path),
                requested_start=manifest.start,
                requested_end=manifest.end,
                claimed_rows=manifest.row_count,
                stored_rows=len(inside),
                missing_files=missing_files,
                agrees=manifest.row_count == len(inside) and not missing_files,
            )
        )

    return checks


def _resolve_range(
    *,
    start: datetime | None,
    end: datetime | None,
    manifests: list[tuple[Path, DatasetManifest]],
    candles: list[Candle],
    cadence: timedelta | None,
) -> tuple[datetime | None, datetime | None, str]:
    """
    Decide which window the dataset is judged against.

    An explicit request wins. Otherwise the manifests say what was meant to
    be acquired, which is the honest yardstick for completeness. With
    neither, the data can only be judged against its own extent, which by
    construction can never report a missing candle at the edges.
    """
    if start is not None and end is not None:
        return start, end, "requested"

    if manifests:
        return (
            min(manifest.start for _, manifest in manifests),
            max(manifest.end for _, manifest in manifests),
            "manifest",
        )

    if not candles:
        return None, None, "empty"

    return (
        candles[0].timestamp,
        candles[-1].timestamp + (cadence or timedelta(0)),
        "data",
    )


def validate_dataset(
    *,
    symbol: str,
    timeframe: str = "1min",
    data_root: str | Path = "data/processed",
    manifest_root: str | Path | None = "data/manifests",
    calendar: MarketCalendar | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> DatasetValidationReport:
    """
    Inspect a stored dataset and report on its integrity and completeness.

    Reads only from disk. Every check the download pipeline applies on the
    way in is re-applied here on the way out, so a dataset can be trusted on
    its own evidence rather than on the word of the run that produced it.
    """
    calendar = calendar or ForexCalendar()
    storage = ParquetStorage(data_root)

    # Inspect the files before reading them. A dataset worth validating may
    # be damaged, and a schema mismatch reported precisely is far more use
    # than a decimal conversion error raised from inside the candle model.
    partitions = _inspect_partitions(storage, symbol=symbol, timeframe=timeframe)
    schema_consistent = all(partition.schema_matches for partition in partitions)

    stored, read_error = _read_dataset(storage, symbol=symbol, timeframe=timeframe)
    cadence = timeframe_cadence(timeframe)

    manifests = (
        load_manifests(Path(manifest_root), symbol=symbol, timeframe=timeframe)
        if manifest_root is not None
        else []
    )

    range_start, range_end, range_source = _resolve_range(
        start=start,
        end=end,
        manifests=manifests,
        candles=stored,
        cadence=cadence,
    )

    if range_start is not None and range_end is not None:
        in_range, outside = restrict_to_range(stored, range_start, range_end)
    else:
        in_range, outside = stored, []

    unordered = sum(
        1
        for previous, current in pairwise(candle.timestamp for candle in stored)
        if current < previous
    )

    _, violations = partition_candles(in_range)
    duplicates = duplicate_timestamps(in_range)
    duplicate_rows = sum(count - 1 for count in duplicates.values())

    missing_intervals: list[MissingInterval] = []
    expected: int | None = None
    closed_intervals: list[ClosedInterval] = []

    if range_start is not None and range_end is not None:
        closed_intervals = calendar.closed_intervals(range_start, range_end)

        if cadence is not None:
            missing_intervals = find_missing_trading_intervals(
                [candle.timestamp for candle in in_range],
                cadence,
                start=range_start,
                end=range_end,
                calendar=calendar,
            )
            expected = calendar.expected_candle_count(range_start, range_end, cadence)

    manifest_checks = _check_manifests(manifests, stored, storage.root)

    missing_candles = sum(interval.missing_candles for interval in missing_intervals)

    problems = _describe_problems(
        read_error=read_error,
        violations=violations,
        duplicate_rows=duplicate_rows,
        outside=len(outside),
        range_source=range_source,
        unordered=unordered,
        partitions=partitions,
        manifest_checks=manifest_checks,
        missing_candles=missing_candles,
    )

    structural = bool(
        read_error
        or violations
        or duplicate_rows
        or outside
        or unordered
        or not schema_consistent
        or any(not check.agrees for check in manifest_checks)
    )

    return DatasetValidationReport(
        symbol=symbol,
        timeframe=timeframe,
        calendar=calendar.name,
        range_start=range_start,
        range_end=range_end,
        range_source=range_source,
        actual_start=in_range[0].timestamp if in_range else None,
        actual_end=in_range[-1].timestamp if in_range else None,
        cadence_seconds=int(cadence.total_seconds()) if cadence else None,
        candles=len(in_range),
        expected_candles=expected,
        missing_candles=missing_candles,
        missing_intervals=missing_intervals,
        market_closed_intervals=closed_intervals,
        duplicate_candles=duplicate_rows,
        duplicate_timestamps=sorted(duplicates)[:MAX_VIOLATION_SAMPLES],
        invalid_rows=len(violations),
        violations=violations[:MAX_VIOLATION_SAMPLES],
        violations_truncated=len(violations) > MAX_VIOLATION_SAMPLES,
        out_of_range_rows=len(outside),
        unordered_rows=unordered,
        partitions=partitions,
        files=len(partitions),
        schema_consistent=schema_consistent,
        readable=read_error is None,
        read_error=read_error,
        manifests=manifest_checks,
        problems=problems,
        status=_status(
            candles=len(in_range),
            missing=missing_intervals,
            structural=structural,
        ),
        checked_at=datetime.now(UTC),
    )


def _describe_problems(
    *,
    read_error: str | None,
    violations: list[CandleViolation],
    duplicate_rows: int,
    outside: int,
    range_source: str,
    unordered: int,
    partitions: list[PartitionInfo],
    manifest_checks: list[ManifestCheck],
    missing_candles: int,
) -> list[str]:
    """State each defect in terms of what to go and look at."""
    problems: list[str] = []

    if read_error is not None:
        problems.append(f"dataset could not be read: {read_error}")

    if violations:
        problems.append(
            f"{len(violations)} rows break an OHLC invariant "
            f"(first: {violations[0].timestamp:%Y-%m-%dT%H:%M:%SZ} "
            f"{violations[0].reason})"
        )

    if duplicate_rows:
        problems.append(f"{duplicate_rows} duplicate timestamps")

    if outside:
        problems.append(f"{outside} rows fall outside the {range_source} range")

    if unordered:
        problems.append(f"{unordered} rows are out of chronological order")

    problems.extend(
        f"{partition.path}: {'; '.join(partition.schema_differences)}"
        for partition in partitions
        if not partition.schema_matches
    )

    for check in manifest_checks:
        if check.missing_files:
            problems.append(
                f"{check.path} references {len(check.missing_files)} missing files"
            )

        if check.claimed_rows != check.stored_rows:
            problems.append(
                f"{check.path} claims {check.claimed_rows} rows but "
                f"{check.stored_rows} are stored"
            )

    if missing_candles:
        problems.append(f"{missing_candles} expected trading candles are missing")

    return problems


def _status(
    *,
    candles: int,
    missing: list[MissingInterval],
    structural: bool,
) -> QualityStatus:
    """
    Grade the dataset.

    A structural defect outranks incompleteness: a gap may be the provider's
    fault, but a duplicate row or a mismatched schema is ours.
    """
    if structural:
        return QualityStatus.INVALID

    if candles == 0:
        return QualityStatus.EMPTY

    if missing:
        return QualityStatus.INCOMPLETE

    return QualityStatus.OK


__all__ = [
    "UNREADABLE_DATASET_ERRORS",
    "DatasetValidationReport",
    "ManifestCheck",
    "PartitionInfo",
    "load_manifests",
    "partition_key",
    "schema_differences",
    "validate_dataset",
]
