from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ValidationError

from marketdata.calendar.base import ClosedInterval, MarketCalendar
from marketdata.calendar.forex import ForexCalendar
from marketdata.models.candle import Candle
from marketdata.models.timeframe import timeframe_cadence
from marketdata.quality.gaps import MissingInterval, TradingGapScanner
from marketdata.quality.report import MAX_VIOLATION_SAMPLES, QualityStatus
from marketdata.storage.manifest import DatasetManifest
from marketdata.storage.parquet import (
    CANDLE_SCHEMA,
    ParquetStorage,
    normalize_symbol_path,
    partition_month,
)
from marketdata.validation.candles import CandleViolation, candle_violation

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

MAX_TRACKED_MISFILED = 1000
"""How many misfiled timestamps are remembered to catch cross-partition
duplicates.

A row stored under the wrong month breaks the invariant that makes bounded
duplicate detection sound, because the same timestamp can then appear in two
partitions. Remembering those timestamps restores the check without
remembering the dataset — and a dataset with more than a thousand misfiled
rows is already comprehensively broken, so the cap is reported rather than
raised.
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
    """Up to ``MAX_VIOLATION_SAMPLES`` gaps, earliest first.

    Bounded on purpose: a badly broken multi-year dataset can hold a gap per
    candle, and a report that grows with the defect cannot be produced for
    the dataset that most needs one. ``missing_interval_count`` and
    ``missing_candles`` stay exact.
    """

    missing_interval_count: int
    missing_intervals_truncated: bool
    market_closed_intervals: list[ClosedInterval]
    duplicate_candles: int
    duplicate_timestamps: list[datetime]
    duplicate_timestamps_truncated: bool
    invalid_rows: int
    violations: list[CandleViolation]
    violations_truncated: bool
    out_of_range_rows: int
    unordered_rows: int
    misfiled_rows: int
    """Rows stored in a partition belonging to another month.

    The storage layout writes one partition per calendar month, and every
    streaming check relies on it: it is what lets a timestamp be located in
    one partition, duplicates be counted a month at a time, and partitions be
    read in chronological order. A row in the wrong partition breaks that
    invariant, so it is counted and reported rather than absorbed.
    """

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
    """Return a partition's year and month, or ``(None, None)`` when unnamed."""
    return partition_month(path) or (None, None)


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


@dataclass
class _Scan:
    """
    Everything one streaming pass over a dataset establishes.

    Every attribute is either a counter, a single timestamp, or a list
    capped at the report's sample limit. Nothing here grows with the number
    of candles in the dataset, which is the whole point: the same state
    describes an hour and seven years.
    """

    candles: int = 0
    out_of_range: int = 0
    invalid: int = 0
    duplicates: int = 0
    unordered: int = 0
    misfiled: int = 0
    actual_start: datetime | None = None
    actual_end: datetime | None = None
    previous: datetime | None = None
    violations: list[CandleViolation] = dataclass_field(default_factory=list)
    duplicate_samples: list[datetime] = dataclass_field(default_factory=list)
    manifest_rows: list[int] = dataclass_field(default_factory=list)
    errors: list[str] = dataclass_field(default_factory=list)
    misfiled_groups: dict[datetime, int] = dataclass_field(default_factory=dict)
    misfiled_overflowed: bool = False

    def note_violation(self, violation: CandleViolation) -> None:
        self.invalid += 1

        if len(self.violations) < MAX_VIOLATION_SAMPLES:
            self.violations.append(violation)

    def note_duplicate(self, timestamp: datetime, extra: int) -> None:
        self.duplicates += extra

        if len(self.duplicate_samples) < MAX_VIOLATION_SAMPLES:
            self.duplicate_samples.append(timestamp)


def _read_group(
    storage: ParquetStorage,
    paths: list[Path],
    scan: _Scan,
) -> list[Candle]:
    """
    Read one partition's candles, turning damage into a finding.

    Validation exists to describe damaged datasets, so any failure becomes a
    finding rather than an exception: an incompatible partition, a truncated
    file and a column of the wrong type all have to be reportable. A file
    that cannot be read costs the dataset that file, not the whole scan.
    """
    candles: list[Candle] = []

    for path in paths:
        try:
            candles.extend(storage.read_partition(path))
        except UNREADABLE_DATASET_ERRORS as exc:
            scan.errors.append(f"{path} ({type(exc).__name__}: {exc})")

    candles.sort(key=lambda candle: candle.timestamp)

    return candles


def _scan_dataset(
    storage: ParquetStorage,
    *,
    symbol: str,
    timeframe: str,
    range_start: datetime | None,
    range_end: datetime | None,
    manifests: list[tuple[Path, DatasetManifest]],
    scanner: TradingGapScanner | None,
) -> _Scan:
    """
    Walk a stored dataset one partition at a time.

    A partition is read, judged, folded into the running totals and then
    dropped before the next one is opened, so peak memory is one month of
    candles rather than the dataset. Gap detection, ordering and duplicate
    checks all carry the state they need across the boundary, so a defect
    spanning two partitions is found exactly as one inside a single
    partition is.
    """
    scan = _Scan(manifest_rows=[0] * len(manifests))
    windows = [(manifest.start, manifest.end) for _, manifest in manifests]
    ranged = range_start is not None and range_end is not None

    for month, paths in storage.partition_groups(symbol=symbol, timeframe=timeframe):
        candles = _read_group(storage, paths, scan)
        counts: Counter[datetime] = Counter()
        stray: set[datetime] = set()

        for candle in candles:
            stamp = candle.timestamp

            if scan.previous is not None and stamp < scan.previous:
                scan.unordered += 1

            scan.previous = stamp

            misfiled = month != (0, 0) and (stamp.year, stamp.month) != month

            if misfiled:
                scan.misfiled += 1

            for index, (window_start, window_end) in enumerate(windows):
                if window_start <= stamp < window_end:
                    scan.manifest_rows[index] += 1

            if ranged and not (range_start <= stamp < range_end):
                scan.out_of_range += 1
                continue

            scan.candles += 1
            counts[stamp] += 1

            if misfiled:
                stray.add(stamp)

            if scan.actual_start is None:
                scan.actual_start = stamp

            scan.actual_end = stamp

            reason = candle_violation(candle)

            if reason is not None:
                scan.note_violation(
                    CandleViolation(
                        timestamp=stamp,
                        symbol=candle.symbol,
                        reason=reason,
                    )
                )

            if scanner is not None:
                scanner.feed_one(stamp)

        _count_duplicates(scan, counts, stray)

        del candles, counts, stray

    _reconcile_misfiled(scan, storage, symbol=symbol, timeframe=timeframe)

    return scan


def _count_duplicates(
    scan: _Scan,
    counts: Counter[datetime],
    stray: set[datetime],
) -> None:
    """
    Count the duplicates inside one partition and note the misfiled rows.

    A partition's own tally is exact for the rows that belong to it: the
    layout puts every row of a month in one partition, so nothing belonging
    to this month can be hiding elsewhere. Rows that do not belong here are
    the exception to that, and are carried forward to be reconciled once the
    scan has finished.
    """
    for timestamp, count in sorted(counts.items()):
        if count > 1:
            scan.note_duplicate(timestamp, count - 1)

    for timestamp in sorted(stray):
        if timestamp in scan.misfiled_groups:
            scan.misfiled_groups[timestamp] += 1
        elif len(scan.misfiled_groups) < MAX_TRACKED_MISFILED:
            scan.misfiled_groups[timestamp] = 1
        else:
            scan.misfiled_overflowed = True


def _reconcile_misfiled(
    scan: _Scan,
    storage: ParquetStorage,
    *,
    symbol: str,
    timeframe: str,
) -> None:
    """
    Count duplicates that only a misfiled row could have created.

    Counting a month at a time is exact while every row is in its own
    partition. A row stored under the wrong month is precisely the case that
    breaks it, so rather than assume it away, each misfiled timestamp is
    checked against the partition it should have been in — one month read at
    a time, and only for a dataset that already has misfiled rows.

    A timestamp seen misfiled in several partitions is a duplicate among
    those sightings alone; one that also exists where it belongs is a
    duplicate of that too.
    """
    if not scan.misfiled_groups:
        return

    homes: dict[tuple[int, int], list[datetime]] = {}

    for timestamp in scan.misfiled_groups:
        homes.setdefault((timestamp.year, timestamp.month), []).append(timestamp)

    for (year, month), timestamps in sorted(homes.items()):
        stored = _home_timestamps(
            storage,
            symbol=symbol,
            timeframe=timeframe,
            year=year,
            month=month,
        )

        for timestamp in sorted(timestamps):
            extra = scan.misfiled_groups[timestamp] - 1 + (timestamp in stored)

            if extra > 0:
                scan.note_duplicate(timestamp, extra)


def _home_timestamps(
    storage: ParquetStorage,
    *,
    symbol: str,
    timeframe: str,
    year: int,
    month: int,
) -> set[datetime]:
    """Return the timestamps stored in the partition a month belongs to."""
    directory = storage.dataset_path(symbol, timeframe) / f"year={year}"
    directory = directory / f"month={month:02d}"

    if not directory.exists():
        return set()

    stored: set[datetime] = set()

    for path in sorted(directory.glob("*.parquet")):
        try:
            stored.update(storage.partition_timestamps(path))
        except UNREADABLE_DATASET_ERRORS:
            # Already reported by the main scan; nothing to add here.
            continue

    return stored


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
    stored_rows: list[int],
    data_root: Path,
) -> list[ManifestCheck]:
    """Compare each manifest with the rows the scan counted inside its window."""
    checks: list[ManifestCheck] = []

    for index, (path, manifest) in enumerate(manifests):
        rows = stored_rows[index]
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
                stored_rows=rows,
                missing_files=missing_files,
                agrees=manifest.row_count == rows and not missing_files,
            )
        )

    return checks


def dataset_bounds(
    storage: ParquetStorage,
    *,
    symbol: str,
    timeframe: str,
) -> tuple[datetime | None, datetime | None]:
    """
    Return the first and last timestamp a dataset holds, from metadata alone.

    Parquet keeps per-column minima and maxima in each file's footer, so the
    extent of a multi-year dataset is a handful of footer reads rather than
    a pass over its rows. A file whose statistics are missing falls back to
    its own timestamp column, never to the dataset.
    """
    low: datetime | None = None
    high: datetime | None = None

    for path in storage.partition_files(symbol=symbol, timeframe=timeframe):
        bounds = storage.partition_bounds(path)

        if bounds is None:
            continue

        first, last = bounds
        low = first if low is None else min(low, first)
        high = last if high is None else max(high, last)

    return low, high


def _resolve_range(
    *,
    start: datetime | None,
    end: datetime | None,
    manifests: list[tuple[Path, DatasetManifest]],
    bounds: tuple[datetime | None, datetime | None],
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

    first, last = bounds

    if first is None or last is None:
        return None, None, "empty"

    return first, last + (cadence or timedelta(0)), "data"


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

    cadence = timeframe_cadence(timeframe)

    manifests = (
        load_manifests(Path(manifest_root), symbol=symbol, timeframe=timeframe)
        if manifest_root is not None
        else []
    )

    # The window has to be settled before the data is walked, because gap
    # detection needs the trading sessions it will be judged against. Both
    # inputs are metadata: what the manifests asked for, or the extent the
    # partition footers report.
    range_start, range_end, range_source = _resolve_range(
        start=start,
        end=end,
        manifests=manifests,
        bounds=dataset_bounds(storage, symbol=symbol, timeframe=timeframe),
        cadence=cadence,
    )

    closed_intervals: list[ClosedInterval] = []
    expected: int | None = None
    scanner: TradingGapScanner | None = None

    if range_start is not None and range_end is not None:
        closed_intervals = calendar.closed_intervals(range_start, range_end)

        if cadence is not None:
            scanner = TradingGapScanner(
                cadence,
                start=range_start,
                end=range_end,
                calendar=calendar,
                max_samples=MAX_VIOLATION_SAMPLES,
            )
            expected = calendar.expected_candle_count(range_start, range_end, cadence)

    scan = _scan_dataset(
        storage,
        symbol=symbol,
        timeframe=timeframe,
        range_start=range_start,
        range_end=range_end,
        manifests=manifests,
        scanner=scanner,
    )

    missing_intervals = scanner.finish() if scanner is not None else []
    missing_interval_count = scanner.interval_count if scanner is not None else 0
    missing_candles = scanner.missing_candles if scanner is not None else 0

    read_error = scan.errors[0] if scan.errors else None
    manifest_checks = _check_manifests(manifests, scan.manifest_rows, storage.root)

    problems = _describe_problems(
        scan=scan,
        range_source=range_source,
        partitions=partitions,
        manifest_checks=manifest_checks,
        missing_candles=missing_candles,
    )

    structural = bool(
        scan.errors
        or scan.invalid
        or scan.duplicates
        or scan.out_of_range
        or scan.unordered
        or scan.misfiled
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
        actual_start=scan.actual_start,
        actual_end=scan.actual_end,
        cadence_seconds=int(cadence.total_seconds()) if cadence else None,
        candles=scan.candles,
        expected_candles=expected,
        missing_candles=missing_candles,
        missing_intervals=missing_intervals,
        missing_interval_count=missing_interval_count,
        missing_intervals_truncated=missing_interval_count > len(missing_intervals),
        market_closed_intervals=closed_intervals,
        duplicate_candles=scan.duplicates,
        duplicate_timestamps=scan.duplicate_samples,
        duplicate_timestamps_truncated=(
            scan.duplicates > len(scan.duplicate_samples) or scan.misfiled_overflowed
        ),
        invalid_rows=scan.invalid,
        violations=scan.violations,
        violations_truncated=scan.invalid > len(scan.violations),
        out_of_range_rows=scan.out_of_range,
        unordered_rows=scan.unordered,
        misfiled_rows=scan.misfiled,
        partitions=partitions,
        files=len(partitions),
        schema_consistent=schema_consistent,
        readable=not scan.errors,
        read_error=read_error,
        manifests=manifest_checks,
        problems=problems,
        status=_status(
            candles=scan.candles,
            missing=missing_interval_count,
            structural=structural,
        ),
        checked_at=datetime.now(UTC),
    )


def _describe_problems(
    *,
    scan: _Scan,
    range_source: str,
    partitions: list[PartitionInfo],
    manifest_checks: list[ManifestCheck],
    missing_candles: int,
) -> list[str]:
    """State each defect in terms of what to go and look at."""
    problems: list[str] = []

    problems.extend(f"dataset could not be read: {error}" for error in scan.errors)

    if scan.invalid:
        first = scan.violations[0]
        problems.append(
            f"{scan.invalid} rows break an OHLC invariant "
            f"(first: {first.timestamp:%Y-%m-%dT%H:%M:%SZ} {first.reason})"
        )

    if scan.duplicates:
        problems.append(f"{scan.duplicates} duplicate timestamps")

    if scan.out_of_range:
        problems.append(
            f"{scan.out_of_range} rows fall outside the {range_source} range"
        )

    if scan.unordered:
        problems.append(f"{scan.unordered} rows are out of chronological order")

    if scan.misfiled:
        problems.append(
            f"{scan.misfiled} rows are stored in a partition for another month, "
            "which is what lets one timestamp appear in two files"
        )

    if scan.misfiled_overflowed:
        problems.append(
            f"more than {MAX_TRACKED_MISFILED} misfiled rows: duplicates spanning "
            "partitions are no longer counted in full beyond that point"
        )

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
    missing: int,
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

    if missing > 0:
        return QualityStatus.INCOMPLETE

    return QualityStatus.OK


__all__ = [
    "MAX_TRACKED_MISFILED",
    "UNREADABLE_DATASET_ERRORS",
    "DatasetValidationReport",
    "ManifestCheck",
    "PartitionInfo",
    "dataset_bounds",
    "load_manifests",
    "partition_key",
    "schema_differences",
    "validate_dataset",
]
