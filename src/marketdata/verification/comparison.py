from __future__ import annotations

import hashlib
import json
from bisect import insort
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
from pydantic import BaseModel, Field

from marketdata.models.candle import Candle
from marketdata.models.timeframe import timeframe_cadence
from marketdata.quality.dataset import (
    UNREADABLE_DATASET_ERRORS,
    load_manifests,
    schema_differences,
)
from marketdata.quality.gaps import MissingInterval, find_missing_intervals
from marketdata.quality.report import MAX_VIOLATION_SAMPLES
from marketdata.storage.manifest import DatasetManifest
from marketdata.storage.parquet import ParquetStorage
from marketdata.validation.candles import duplicate_timestamps
from marketdata.verification.records import configuration_fingerprint
from marketdata.verification.status import (
    DEFAULT_MISSING_FAIL_RATIO,
    DEFAULT_MISSING_WARN_RATIO,
    VerificationStatus,
    worst,
)

PRICE_FIELDS = ("open", "high", "low", "close")

UNKNOWN_PROVIDER = "unknown"
"""Recorded when no manifest says which provider produced a dataset.

Naming it is deliberate: a comparison between two datasets of unknown
origin is still a useful check of the files, but it must not be mistaken
for evidence about two named providers.
"""

DEFAULT_PRICE_TOLERANCE = Decimal("0.0001")
DEFAULT_VOLUME_TOLERANCE = Decimal("0.05")
DEFAULT_PRICE_MISMATCH_WARN_RATIO = 0.0
DEFAULT_PRICE_MISMATCH_FAIL_RATIO = 0.001
DEFAULT_VOLUME_MISMATCH_WARN_RATIO = 0.0
DEFAULT_VOLUME_MISMATCH_FAIL_RATIO = 1.0

_HASH_BLOCK_BYTES = 1024 * 1024


@dataclass(frozen=True)
class ComparisonThresholds:
    """
    How far two providers may disagree before the disagreement matters.

    **Tolerances are relative, never exact equality.** Two providers quoting
    the same instrument do not agree to the last stored digit, and floating
    comparison of prices is wrong regardless: every value here is a
    ``Decimal`` and every comparison is against a proportional bound, so the
    same setting means the same thing for a 1.08 euro rate and a 157 yen one.

    **Prices: 0.01% by default.** That is roughly one pip on EUR/USD. Two
    feeds differ because they aggregate different liquidity and may quote
    bid where another quotes mid, and a spread-sized difference is normal.
    A stale feed, a wrong scale factor or an off-by-one bar is far larger
    than one pip, so the bound still catches what it is meant to catch.

    **Volumes: 5% by default, and warn-only.** Retail FX has no consolidated
    tape; "volume" is a tick count over one provider's own feed. Two
    providers therefore legitimately report different numbers for the same
    minute, and calling that a data error would be wrong. The default fail
    ratio of 1.0 is unreachable — a ratio cannot exceed 1 — which is how
    "report it, never fail on it" is expressed. Raise the tolerance or lower
    the fail ratio when comparing two feeds that genuinely should agree.

    **Ratios, not counts.** A single disagreeing candle in a month is noise;
    the same count in an hour is a broken feed. Price mismatches warn above
    0% and fail above 0.1% of compared candles. Coverage differences reuse
    the acquisition defaults (fail above 1%) so one policy governs "how much
    missing data is tolerable" across the project.

    These defaults are a **starting policy, not an empirical finding.** No
    real Dukascopy data has ever been observed by this project, so the true
    disagreement between two live FX feeds is unmeasured here. Recalibrate
    from the first real cross-provider comparison and record the reasoning.
    """

    price_tolerance: Decimal = DEFAULT_PRICE_TOLERANCE
    volume_tolerance: Decimal = DEFAULT_VOLUME_TOLERANCE
    price_mismatch_warn_ratio: float = DEFAULT_PRICE_MISMATCH_WARN_RATIO
    price_mismatch_fail_ratio: float = DEFAULT_PRICE_MISMATCH_FAIL_RATIO
    volume_mismatch_warn_ratio: float = DEFAULT_VOLUME_MISMATCH_WARN_RATIO
    volume_mismatch_fail_ratio: float = DEFAULT_VOLUME_MISMATCH_FAIL_RATIO
    missing_warn_ratio: float = DEFAULT_MISSING_WARN_RATIO
    missing_fail_ratio: float = DEFAULT_MISSING_FAIL_RATIO

    def __post_init__(self) -> None:
        for name in ("price_tolerance", "volume_tolerance"):
            value = getattr(self, name)

            if not isinstance(value, Decimal):
                raise TypeError(f"{name} must be a Decimal, not {type(value).__name__}")

            if value < 0:
                raise ValueError(f"{name} cannot be negative")

        for name in (
            "price_mismatch_warn_ratio",
            "price_mismatch_fail_ratio",
            "volume_mismatch_warn_ratio",
            "volume_mismatch_fail_ratio",
            "missing_warn_ratio",
            "missing_fail_ratio",
        ):
            value = getattr(self, name)

            if not 0 <= value <= 1:
                raise ValueError(f"{name} must be between 0 and 1")

        for warn, fail in (
            ("price_mismatch_warn_ratio", "price_mismatch_fail_ratio"),
            ("volume_mismatch_warn_ratio", "volume_mismatch_fail_ratio"),
            ("missing_warn_ratio", "missing_fail_ratio"),
        ):
            if getattr(self, warn) > getattr(self, fail):
                raise ValueError(
                    f"{warn} cannot exceed {fail}: a comparison cannot fail "
                    "before it warns"
                )

    def configuration(self) -> dict[str, str]:
        """Describe the thresholds as flat strings, for records and output."""
        return {
            "price_tolerance": str(self.price_tolerance),
            "volume_tolerance": str(self.volume_tolerance),
            "price_mismatch_warn_ratio": str(self.price_mismatch_warn_ratio),
            "price_mismatch_fail_ratio": str(self.price_mismatch_fail_ratio),
            "volume_mismatch_warn_ratio": str(self.volume_mismatch_warn_ratio),
            "volume_mismatch_fail_ratio": str(self.volume_mismatch_fail_ratio),
            "missing_warn_ratio": str(self.missing_warn_ratio),
            "missing_fail_ratio": str(self.missing_fail_ratio),
        }

    def fingerprint(self) -> str:
        """A stable fingerprint of the whole threshold configuration."""
        return configuration_fingerprint(self.configuration())

    def describe(self) -> str:
        return (
            f"prices agree within {self.price_tolerance} relative, warn above "
            f"{self.price_mismatch_warn_ratio:.2%} of candles and fail above "
            f"{self.price_mismatch_fail_ratio:.2%}; "
            f"volumes agree within {self.volume_tolerance} relative, warn above "
            f"{self.volume_mismatch_warn_ratio:.2%} and fail above "
            f"{self.volume_mismatch_fail_ratio:.2%}; "
            f"coverage differences warn above {self.missing_warn_ratio:.2%} and "
            f"fail above {self.missing_fail_ratio:.2%}; "
            "duplicate timestamps and unreadable files always fail"
        )


class FieldDifference(BaseModel):
    """One field of one candle on which the two datasets disagree."""

    model_config = {"frozen": True}

    timestamp: datetime
    field: str
    left: Decimal
    right: Decimal
    difference: Decimal
    relative_difference: Decimal


class GapDifference(BaseModel):
    """A stretch with no candles in one dataset but not the other."""

    model_config = {"frozen": True}

    side: str
    start: datetime
    end: datetime
    missing_candles: int


class DatasetIdentity(BaseModel):
    """
    Which dataset was compared, and what it consisted of at the time.

    The fingerprints are what makes a stored comparison expire: the dataset
    fingerprint changes when any byte of any partition changes, and the
    provider fingerprint changes when the provider or its configuration
    does. Configuration is copied from the dataset's provenance, which by
    construction never holds a credential.
    """

    model_config = {"frozen": True}

    side: str
    root: str
    provider: str
    provider_configuration: dict[str, str] = Field(default_factory=dict)
    provider_fingerprint: str
    dataset_fingerprint: str
    files: list[str] = Field(default_factory=list)
    rows: int
    manifests: list[str] = Field(default_factory=list)
    schema_differences: list[str] = Field(default_factory=list)
    readable: bool
    errors: list[str] = Field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not self.files or self.rows == 0


class ComparisonReport(BaseModel):
    """
    What two stored datasets say about the same symbol, timeframe and range.

    Produced by reading Parquet only. Nothing here writes to either dataset,
    contacts a provider, or decides that one side is right: a disagreement is
    reported as a disagreement, and reconciling it is a human decision.
    """

    symbol: str
    timeframe: str
    left: DatasetIdentity
    right: DatasetIdentity
    requested_start: datetime | None
    requested_end: datetime | None
    left_start: datetime | None
    left_end: datetime | None
    right_start: datetime | None
    right_end: datetime | None
    ranges_match: bool
    candles_compared: int
    matching_candles: int
    mismatching_candles: int
    missing_from_left: int
    missing_from_right: int
    missing_from_left_samples: list[datetime] = Field(default_factory=list)
    missing_from_right_samples: list[datetime] = Field(default_factory=list)
    duplicate_timestamps_left: int
    duplicate_timestamps_right: int
    price_mismatches: int
    volume_mismatches: int
    volume_available: bool
    max_price_difference: Decimal
    max_price_difference_at: datetime | None
    max_price_relative_difference: Decimal
    max_volume_difference: Decimal
    max_volume_difference_at: datetime | None
    differences: list[FieldDifference] = Field(default_factory=list)
    differences_truncated: bool
    gaps_left: int
    gaps_right: int
    gap_differences: list[GapDifference] = Field(default_factory=list)
    gap_differences_truncated: bool
    gaps_only_left: int
    gaps_only_right: int
    partitions_compared: int
    thresholds: dict[str, str] = Field(default_factory=dict)
    thresholds_description: str
    problems: list[str] = Field(default_factory=list)
    status: VerificationStatus
    compared_at: datetime

    @property
    def verified(self) -> bool:
        return self.status.verified

    @property
    def blocked(self) -> bool:
        return self.status is VerificationStatus.BLOCKED


def relative_difference(left: Decimal, right: Decimal) -> Decimal:
    """
    Return how far apart two values are, in proportion to their size.

    Equal values are zero regardless of scale, and two zeroes agree rather
    than dividing by nothing.
    """
    if left == right:
        return Decimal(0)

    scale = max(abs(left), abs(right))

    if scale == 0:
        return Decimal(0)

    return abs(left - right) / scale


def file_fingerprint(path: Path) -> str:
    """
    Hash a partition file's contents.

    Fingerprinting by size and row count alone would let an edited price slip
    past unnoticed, and a fingerprint that can miss a change is not evidence.
    The file is read in blocks so a multi-gigabyte partition never has to fit
    in memory.
    """
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while block := handle.read(_HASH_BLOCK_BYTES):
            digest.update(block)

    return digest.hexdigest()


def dataset_fingerprint(files: list[Path], root: Path) -> str:
    """Return a fingerprint of every partition making up a dataset."""
    entries: list[list[str]] = []

    for path in sorted(files):
        try:
            entries.append(
                [
                    _relative(path, root),
                    str(path.stat().st_size),
                    file_fingerprint(path),
                ]
            )
        except OSError as exc:
            entries.append([_relative(path, root), "unreadable", str(exc)])

    payload = json.dumps(entries, separators=(",", ":"))

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _relative(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return str(path)


def _provider_from_manifests(
    manifests: list[tuple[Path, DatasetManifest]],
) -> tuple[str, dict[str, str]]:
    """
    Read the provider identity a dataset's manifests claim.

    Several manifests may cover one dataset; they agree in the normal case,
    and when they do not, saying so is more useful than picking one.
    """
    providers = sorted({manifest.provider for _, manifest in manifests})

    if not providers:
        return UNKNOWN_PROVIDER, {}

    configuration: dict[str, str] = {}

    for _, manifest in sorted(manifests, key=lambda item: item[1].created_at):
        provenance = manifest.provenance

        if provenance is not None:
            configuration = dict(provenance.provider_configuration)

    if len(providers) > 1:
        return "mixed:" + ",".join(providers), configuration

    return providers[0], configuration


def describe_dataset(
    root: str | Path,
    *,
    symbol: str,
    timeframe: str,
    side: str,
    start: datetime | None = None,
    end: datetime | None = None,
    manifest_root: str | Path | None = None,
) -> DatasetIdentity:
    """
    Identify one side of a comparison without reading its candles.

    Inspects footers and schemas only, so an unreadable or incompatible
    dataset is described rather than raising, and a large dataset costs a
    footer read per partition instead of a full load.
    """
    storage = ParquetStorage(root)
    files = storage.partition_files(
        symbol=symbol,
        timeframe=timeframe,
        start=start,
        end=end,
    )

    rows = 0
    differences: list[str] = []
    errors: list[str] = []

    for path in files:
        try:
            rows += pq.read_metadata(path).num_rows
            differences.extend(
                f"{_relative(path, storage.root)}: {difference}"
                for difference in schema_differences(pq.read_schema(path))
            )
        except UNREADABLE_DATASET_ERRORS as exc:
            errors.append(
                f"{_relative(path, storage.root)} is unreadable "
                f"({type(exc).__name__}: {exc})"
            )

    manifests = (
        load_manifests(Path(manifest_root), symbol=symbol, timeframe=timeframe)
        if manifest_root is not None
        else []
    )

    provider, configuration = _provider_from_manifests(manifests)

    return DatasetIdentity(
        side=side,
        root=str(storage.root),
        provider=provider,
        provider_configuration=configuration,
        provider_fingerprint=configuration_fingerprint(
            {"provider": provider, **configuration}
        ),
        dataset_fingerprint=dataset_fingerprint(files, storage.root),
        files=[_relative(path, storage.root) for path in files],
        rows=rows,
        manifests=[str(path) for path, _ in manifests],
        schema_differences=differences,
        readable=not errors,
        errors=errors,
    )


def partition_batches(
    left: ParquetStorage,
    right: ParquetStorage,
    *,
    symbol: str,
    timeframe: str,
    start: datetime | None,
    end: datetime | None,
) -> list[tuple[tuple[int, int], list[Path], list[Path]]]:
    """
    Pair up the two datasets' partitions, in chronological order.

    Both sides store one file per calendar month, so a timestamp can only
    appear in the same month on either side. Comparing month by month is
    therefore exact, and it bounds the working set to a month of candles
    rather than a whole multi-year history.
    """
    left_groups = dict(
        left.partition_groups(symbol=symbol, timeframe=timeframe, start=start, end=end)
    )
    right_groups = dict(
        right.partition_groups(symbol=symbol, timeframe=timeframe, start=start, end=end)
    )

    return [
        (key, left_groups.get(key, []), right_groups.get(key, []))
        for key in sorted(set(left_groups) | set(right_groups))
    ]


class _BoundedSamples:
    """
    Keep the lowest-ranked samples of an unbounded stream, and count it all.

    Two datasets that disagree everywhere produce a gap per candle, so
    collecting every difference and truncating afterwards makes the report
    largest for exactly the comparison least able to afford it. This holds
    ``limit`` samples no matter how many arrive.

    The retained samples are the ones a full sort would have put first, so
    the report is identical to one built by sorting everything and slicing —
    it is only the memory that changes. ``limit`` is small, so keeping the
    buffer ordered by insertion costs less than a heap would.
    """

    def __init__(self, limit: int, key) -> None:
        self.limit = limit
        self.total = 0

        self._key = key
        self._sequence = 0
        self._ranked: list[tuple[object, int, object]] = []

    def add(self, sample) -> None:
        self.total += 1
        rank = (self._key(sample), self._sequence, sample)
        self._sequence += 1

        if len(self._ranked) < self.limit:
            insort(self._ranked, rank, key=lambda entry: entry[:2])
            return

        if rank[:2] < self._ranked[-1][:2]:
            insort(self._ranked, rank, key=lambda entry: entry[:2])
            self._ranked.pop()

    @property
    def samples(self) -> list:
        """The retained samples, lowest-ranked first."""
        return [sample for _, _, sample in self._ranked]

    @property
    def truncated(self) -> bool:
        """Whether more samples arrived than are being reported."""
        return self.total > len(self._ranked)


class _GapTracker:
    """
    Find gaps in a stream of partitions without holding the whole stream.

    Each batch is joined to the previous one by carrying its last timestamp
    forward, so a gap spanning a month boundary is found exactly as an
    interior one is. Only the batch's own gaps are returned; the caller
    resolves them immediately rather than accumulating a list that grows
    with the dataset.
    """

    def __init__(self, cadence) -> None:
        self.cadence = cadence
        self.last: datetime | None = None
        self.count = 0

    def feed(self, stamps: list[datetime]) -> list[MissingInterval]:
        """Return the gaps this batch closed, in chronological order."""
        if self.cadence is None:
            return []

        ordered = sorted(set(stamps))

        if self.last is not None:
            ordered = [self.last, *ordered]

        if not ordered:
            return []

        found = find_missing_intervals(ordered, self.cadence)
        self.last = ordered[-1]
        self.count += len(found)

        return found


class _Tally:
    """Running totals of one comparison, updated a partition at a time."""

    def __init__(self, cadence) -> None:
        self.compared = 0
        self.matching = 0
        self.price_mismatches = 0
        self.volume_mismatches = 0
        self.missing_from_left = 0
        self.missing_from_right = 0
        self.missing_from_left_samples: list[datetime] = []
        self.missing_from_right_samples: list[datetime] = []
        self.duplicates_left = 0
        self.duplicates_right = 0
        self.differences: list[FieldDifference] = []
        self.difference_count = 0
        self.max_price_difference = Decimal(0)
        self.max_price_difference_at: datetime | None = None
        self.max_price_relative = Decimal(0)
        self.max_volume_difference = Decimal(0)
        self.max_volume_difference_at: datetime | None = None
        self.volume_available = False
        self.left_start: datetime | None = None
        self.left_end: datetime | None = None
        self.right_start: datetime | None = None
        self.right_end: datetime | None = None
        self.left_gaps = _GapTracker(cadence)
        self.right_gaps = _GapTracker(cadence)
        self.gaps_only_left = 0
        self.gaps_only_right = 0
        self.gap_samples = _BoundedSamples(
            MAX_VIOLATION_SAMPLES,
            key=lambda gap: (gap.start, gap.side),
        )

    def note_gaps(
        self,
        left: list[MissingInterval],
        right: list[MissingInterval],
    ) -> None:
        """
        Record the gaps one batch found on only one of the two sides.

        Resolvable a batch at a time because a gap is closed by the
        timestamp that ends it: two sides holding the identical gap hold the
        identical closing timestamp, which falls in the same month and so is
        read in the same batch. Nothing about a gap therefore has to be
        carried past the batch that found it.
        """
        left_keys = {(gap.start, gap.end) for gap in left}
        right_keys = {(gap.start, gap.end) for gap in right}

        for side, found, other in (
            ("left", left, right_keys),
            ("right", right, left_keys),
        ):
            for gap in found:
                if (gap.start, gap.end) in other:
                    continue

                if side == "left":
                    self.gaps_only_left += 1
                else:
                    self.gaps_only_right += 1

                self.gap_samples.add(
                    GapDifference(
                        side=side,
                        start=gap.start,
                        end=gap.end,
                        missing_candles=gap.missing_candles,
                    )
                )

    def note_range(self, side: str, stamps: list[datetime]) -> None:
        if not stamps:
            return

        low, high = min(stamps), max(stamps)

        if side == "left":
            self.left_start = low if self.left_start is None else self.left_start
            self.left_end = high
        else:
            self.right_start = low if self.right_start is None else self.right_start
            self.right_end = high

    def record_difference(self, difference: FieldDifference) -> None:
        self.difference_count += 1

        if len(self.differences) < MAX_VIOLATION_SAMPLES:
            self.differences.append(difference)


def _normalize(candle: Candle) -> datetime:
    """Return a candle's timestamp as UTC.

    Two datasets may store the same instant with different offsets. Equality
    already compares instants, but every reported timestamp should read as
    the canonical UTC one.
    """
    return candle.timestamp.astimezone(UTC)


def _index(candles: list[Candle]) -> dict[datetime, Candle]:
    return {_normalize(candle): candle for candle in candles}


def _compare_candle(
    timestamp: datetime,
    left: Candle,
    right: Candle,
    thresholds: ComparisonThresholds,
    tally: _Tally,
) -> None:
    price_mismatch = False

    for name in PRICE_FIELDS:
        left_value: Decimal = getattr(left, name)
        right_value: Decimal = getattr(right, name)

        absolute = abs(left_value - right_value)
        relative = relative_difference(left_value, right_value)

        if absolute > tally.max_price_difference:
            tally.max_price_difference = absolute
            tally.max_price_difference_at = timestamp

        tally.max_price_relative = max(tally.max_price_relative, relative)

        if relative > thresholds.price_tolerance:
            price_mismatch = True
            tally.record_difference(
                FieldDifference(
                    timestamp=timestamp,
                    field=name,
                    left=left_value,
                    right=right_value,
                    difference=left_value - right_value,
                    relative_difference=relative,
                )
            )

    if left.volume or right.volume:
        tally.volume_available = True

    volume_absolute = abs(left.volume - right.volume)
    volume_relative = relative_difference(left.volume, right.volume)

    if volume_absolute > tally.max_volume_difference:
        tally.max_volume_difference = volume_absolute
        tally.max_volume_difference_at = timestamp

    volume_mismatch = volume_relative > thresholds.volume_tolerance

    if volume_mismatch:
        tally.record_difference(
            FieldDifference(
                timestamp=timestamp,
                field="volume",
                left=left.volume,
                right=right.volume,
                difference=left.volume - right.volume,
                relative_difference=volume_relative,
            )
        )

    tally.compared += 1

    if price_mismatch:
        tally.price_mismatches += 1

    if volume_mismatch:
        tally.volume_mismatches += 1

    if not price_mismatch and not volume_mismatch:
        tally.matching += 1


def _read_group(
    storage: ParquetStorage,
    paths: list[Path],
    *,
    start: datetime | None,
    end: datetime | None,
    errors: list[str],
) -> list[Candle]:
    """
    Read one month of one dataset, turning damage into a finding.

    A partition that cannot be read is reported by path so it can be
    replaced; raising a PyArrow error out of a comparison would say nothing
    about which file is at fault.
    """
    candles: list[Candle] = []

    for path in paths:
        try:
            candles.extend(storage.read_partition(path))
        except UNREADABLE_DATASET_ERRORS as exc:
            errors.append(
                f"{_relative(path, storage.root)} could not be read "
                f"({type(exc).__name__}: {exc})"
            )

    if start is None and end is None:
        return candles

    return [
        candle
        for candle in candles
        if (start is None or _normalize(candle) >= start)
        and (end is None or _normalize(candle) < end)
    ]


def _grade(ratio: float, warn: float, fail: float) -> VerificationStatus:
    if ratio > fail:
        return VerificationStatus.FAIL

    if ratio > warn:
        return VerificationStatus.WARN

    return VerificationStatus.PASS


def compare_datasets(
    *,
    symbol: str,
    timeframe: str = "1min",
    left_root: str | Path,
    right_root: str | Path,
    left_manifest_root: str | Path | None = None,
    right_manifest_root: str | Path | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    thresholds: ComparisonThresholds | None = None,
) -> ComparisonReport:
    """
    Compare two stored datasets covering the same symbol and timeframe.

    Read-only in both directions: neither dataset is written to, corrected,
    or preferred over the other. A disagreement is counted and reported with
    the timestamps it occurred at, never silently reconciled.

    Works a month at a time, so comparing years of one-minute data holds one
    month of each side in memory rather than the whole history.
    """
    thresholds = thresholds or ComparisonThresholds()
    normalized_symbol = symbol.strip().upper()

    if start is not None and end is not None and start >= end:
        raise ValueError("start must be before end")

    left = describe_dataset(
        left_root,
        symbol=normalized_symbol,
        timeframe=timeframe,
        side="left",
        start=start,
        end=end,
        manifest_root=left_manifest_root,
    )
    right = describe_dataset(
        right_root,
        symbol=normalized_symbol,
        timeframe=timeframe,
        side="right",
        start=start,
        end=end,
        manifest_root=right_manifest_root,
    )

    left_storage = ParquetStorage(left_root)
    right_storage = ParquetStorage(right_root)

    cadence = timeframe_cadence(timeframe)
    tally = _Tally(cadence)
    read_errors: list[str] = []

    batches = partition_batches(
        left_storage,
        right_storage,
        symbol=normalized_symbol,
        timeframe=timeframe,
        start=start,
        end=end,
    )

    for _, left_paths, right_paths in batches:
        left_batch = _read_group(
            left_storage, left_paths, start=start, end=end, errors=read_errors
        )
        right_batch = _read_group(
            right_storage, right_paths, start=start, end=end, errors=read_errors
        )

        tally.duplicates_left += sum(
            count - 1 for count in duplicate_timestamps(left_batch).values()
        )
        tally.duplicates_right += sum(
            count - 1 for count in duplicate_timestamps(right_batch).values()
        )

        left_by_time = _index(left_batch)
        right_by_time = _index(right_batch)

        tally.note_range("left", list(left_by_time))
        tally.note_range("right", list(right_by_time))

        tally.note_gaps(
            tally.left_gaps.feed(list(left_by_time)),
            tally.right_gaps.feed(list(right_by_time)),
        )

        for timestamp in sorted(left_by_time.keys() & right_by_time.keys()):
            _compare_candle(
                timestamp,
                left_by_time[timestamp],
                right_by_time[timestamp],
                thresholds,
                tally,
            )

        for timestamp in sorted(left_by_time.keys() - right_by_time.keys()):
            tally.missing_from_right += 1

            if len(tally.missing_from_right_samples) < MAX_VIOLATION_SAMPLES:
                tally.missing_from_right_samples.append(timestamp)

        for timestamp in sorted(right_by_time.keys() - left_by_time.keys()):
            tally.missing_from_left += 1

            if len(tally.missing_from_left_samples) < MAX_VIOLATION_SAMPLES:
                tally.missing_from_left_samples.append(timestamp)

    return _finish(
        left=left,
        right=right,
        symbol=normalized_symbol,
        timeframe=timeframe,
        start=start,
        end=end,
        thresholds=thresholds,
        tally=tally,
        read_errors=read_errors,
        partitions=len(batches),
    )


def _finish(
    *,
    left: DatasetIdentity,
    right: DatasetIdentity,
    symbol: str,
    timeframe: str,
    start: datetime | None,
    end: datetime | None,
    thresholds: ComparisonThresholds,
    tally: _Tally,
    read_errors: list[str],
    partitions: int,
) -> ComparisonReport:
    problems: list[str] = []
    statuses: list[VerificationStatus] = []

    problems.extend(left.errors)
    problems.extend(right.errors)
    problems.extend(read_errors)

    problems.extend(
        f"{side} schema is incompatible: {difference}"
        for side, identity in (("left", left), ("right", right))
        for difference in identity.schema_differences
    )

    # Structural damage is ours or the file's, never the market's, so it has
    # no tolerance: an unreadable partition, an incompatible schema or a
    # repeated timestamp fails outright.
    if left.errors or right.errors or read_errors:
        statuses.append(VerificationStatus.FAIL)

    if left.schema_differences or right.schema_differences:
        statuses.append(VerificationStatus.FAIL)

    if tally.duplicates_left:
        problems.append(f"left holds {tally.duplicates_left} duplicate timestamps")
        statuses.append(VerificationStatus.FAIL)

    if tally.duplicates_right:
        problems.append(f"right holds {tally.duplicates_right} duplicate timestamps")
        statuses.append(VerificationStatus.FAIL)

    union = tally.compared + tally.missing_from_left + tally.missing_from_right

    if union == 0:
        # Nothing overlapped and nothing was stored in the window. A
        # comparison of nothing against nothing proves nothing, so it is
        # blocked rather than passed.
        problems.extend(
            f"{side} dataset holds no candles to compare"
            for side, identity in (("left", left), ("right", right))
            if identity.empty
        )

        statuses.append(VerificationStatus.BLOCKED)
    else:
        missing = tally.missing_from_left + tally.missing_from_right

        statuses.append(
            _grade(
                missing / union,
                thresholds.missing_warn_ratio,
                thresholds.missing_fail_ratio,
            )
        )

        if tally.missing_from_right:
            problems.append(
                f"right is missing {_plural(tally.missing_from_right, 'candle')} "
                "that left holds"
            )

        if tally.missing_from_left:
            problems.append(
                f"left is missing {_plural(tally.missing_from_left, 'candle')} "
                "that right holds"
            )

    if tally.compared:
        statuses.append(
            _grade(
                tally.price_mismatches / tally.compared,
                thresholds.price_mismatch_warn_ratio,
                thresholds.price_mismatch_fail_ratio,
            )
        )
        statuses.append(
            _grade(
                tally.volume_mismatches / tally.compared,
                thresholds.volume_mismatch_warn_ratio,
                thresholds.volume_mismatch_fail_ratio,
            )
        )

        if tally.price_mismatches:
            problems.append(
                f"{tally.price_mismatches} of {tally.compared} compared candles "
                f"disagree on price beyond {thresholds.price_tolerance} relative "
                f"(largest {tally.max_price_difference})"
            )

        if tally.volume_mismatches:
            problems.append(
                f"{tally.volume_mismatches} of {tally.compared} compared candles "
                f"disagree on volume beyond {thresholds.volume_tolerance} relative "
                f"(largest {tally.max_volume_difference})"
            )

    ranges_match = (tally.left_start, tally.left_end) == (
        tally.right_start,
        tally.right_end,
    )

    if not ranges_match:
        problems.append(
            "the datasets cover different ranges: "
            f"left {_stamp(tally.left_start)} -> {_stamp(tally.left_end)}, "
            f"right {_stamp(tally.right_start)} -> {_stamp(tally.right_end)}"
        )

    return ComparisonReport(
        symbol=symbol,
        timeframe=timeframe,
        left=left,
        right=right,
        requested_start=start,
        requested_end=end,
        left_start=tally.left_start,
        left_end=tally.left_end,
        right_start=tally.right_start,
        right_end=tally.right_end,
        ranges_match=ranges_match,
        candles_compared=tally.compared,
        matching_candles=tally.matching,
        mismatching_candles=tally.compared - tally.matching,
        missing_from_left=tally.missing_from_left,
        missing_from_right=tally.missing_from_right,
        missing_from_left_samples=tally.missing_from_left_samples,
        missing_from_right_samples=tally.missing_from_right_samples,
        duplicate_timestamps_left=tally.duplicates_left,
        duplicate_timestamps_right=tally.duplicates_right,
        price_mismatches=tally.price_mismatches,
        volume_mismatches=tally.volume_mismatches,
        volume_available=tally.volume_available,
        max_price_difference=tally.max_price_difference,
        max_price_difference_at=tally.max_price_difference_at,
        max_price_relative_difference=tally.max_price_relative,
        max_volume_difference=tally.max_volume_difference,
        max_volume_difference_at=tally.max_volume_difference_at,
        differences=tally.differences,
        differences_truncated=tally.difference_count > len(tally.differences),
        gaps_left=tally.left_gaps.count,
        gaps_right=tally.right_gaps.count,
        gap_differences=tally.gap_samples.samples,
        gap_differences_truncated=tally.gap_samples.truncated,
        gaps_only_left=tally.gaps_only_left,
        gaps_only_right=tally.gaps_only_right,
        partitions_compared=partitions,
        thresholds=thresholds.configuration(),
        thresholds_description=thresholds.describe(),
        problems=problems,
        status=worst(statuses),
        compared_at=datetime.now(UTC),
    )


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def _stamp(value: datetime | None) -> str:
    if value is None:
        return "-"

    return f"{value:%Y-%m-%dT%H:%M:%SZ}"


__all__ = [
    "DEFAULT_PRICE_MISMATCH_FAIL_RATIO",
    "DEFAULT_PRICE_MISMATCH_WARN_RATIO",
    "DEFAULT_PRICE_TOLERANCE",
    "DEFAULT_VOLUME_MISMATCH_FAIL_RATIO",
    "DEFAULT_VOLUME_MISMATCH_WARN_RATIO",
    "DEFAULT_VOLUME_TOLERANCE",
    "PRICE_FIELDS",
    "UNKNOWN_PROVIDER",
    "ComparisonReport",
    "ComparisonThresholds",
    "DatasetIdentity",
    "FieldDifference",
    "GapDifference",
    "compare_datasets",
    "dataset_fingerprint",
    "describe_dataset",
    "file_fingerprint",
    "partition_batches",
    "relative_difference",
]
