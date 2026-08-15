from __future__ import annotations

import os
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from marketdata.models.candle import Candle
from marketdata.validation.candles import merge_candles

UNREADABLE_STORAGE_ERRORS = (pa.ArrowException, OSError, ValueError)
"""What opening or decoding a damaged Parquet file can raise.

Metadata inspection is used to describe datasets that may be damaged, so a
truncated footer or a missing file has to be answerable rather than fatal.
"""

PRICE_SCALE = 8
PRICE_PRECISION = 18
VOLUME_SCALE = 8
VOLUME_PRECISION = 28

PRICE_TYPE = pa.decimal128(PRICE_PRECISION, PRICE_SCALE)
VOLUME_TYPE = pa.decimal128(VOLUME_PRECISION, VOLUME_SCALE)

CANDLE_SCHEMA = pa.schema(
    [
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("symbol", pa.string()),
        ("open", PRICE_TYPE),
        ("high", PRICE_TYPE),
        ("low", PRICE_TYPE),
        ("close", PRICE_TYPE),
        ("volume", VOLUME_TYPE),
    ]
)
"""Explicit on-disk schema.

Letting PyArrow infer decimal types produces a different precision and scale
per file, which makes files from different months unreadable as a single
dataset. Fixing the types keeps every partition mutually compatible.
"""

_PRICE_QUANTUM = Decimal(1).scaleb(-PRICE_SCALE)
_VOLUME_QUANTUM = Decimal(1).scaleb(-VOLUME_SCALE)


def normalize_symbol_path(symbol: str) -> str:
    """Return the on-disk directory name for a symbol."""
    return symbol.strip().upper().replace("/", "_")


def partition_month(path: Path) -> tuple[int, int] | None:
    """Read the year and month out of a hive-partitioned file path."""
    parts = {
        piece.split("=", 1)[0]: piece.split("=", 1)[1]
        for piece in path.parts
        if "=" in piece
    }

    try:
        return int(parts["year"]), int(parts["month"])
    except (KeyError, ValueError):
        return None


def _partition_overlaps(
    path: Path,
    start: datetime | None,
    end: datetime | None,
) -> bool:
    """Return whether a partition file can hold rows inside ``[start, end)``."""
    if start is None and end is None:
        return True

    month = partition_month(path)

    if month is None:
        # An unrecognized layout is never silently excluded; validation
        # reports it rather than the reader hiding it.
        return True

    year, index = month
    month_start = datetime(year, index, 1, tzinfo=UTC)
    month_end = datetime(
        year + (index == 12),
        1 if index == 12 else index + 1,
        1,
        tzinfo=UTC,
    )

    if end is not None and month_start >= end:
        return False

    return not (start is not None and month_end <= start)


def _as_utc(value: datetime) -> datetime:
    """Normalize a timestamp read from Parquet metadata to UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)

    return value.astimezone(UTC)


def _quantize(value: Decimal, quantum: Decimal) -> Decimal:
    return value.quantize(quantum, rounding=ROUND_HALF_EVEN)


def candles_to_table(candles: list[Candle]) -> pa.Table:
    """
    Convert candles into an Arrow table using the canonical schema.

    Values are quantized to the stored scale first. Provider payloads arrive
    as JSON floats whose decimal expansion can run to seventeen digits, which
    the fixed-scale decimal columns would otherwise reject.
    """
    records = [
        {
            "timestamp": candle.timestamp,
            "symbol": candle.symbol,
            "open": _quantize(candle.open, _PRICE_QUANTUM),
            "high": _quantize(candle.high, _PRICE_QUANTUM),
            "low": _quantize(candle.low, _PRICE_QUANTUM),
            "close": _quantize(candle.close, _PRICE_QUANTUM),
            "volume": _quantize(candle.volume, _VOLUME_QUANTUM),
        }
        for candle in candles
    ]

    return pa.Table.from_pylist(records, schema=CANDLE_SCHEMA)


def table_to_candles(table: pa.Table) -> list[Candle]:
    """Convert an Arrow table back into canonical candles."""
    columns = [name for name in CANDLE_SCHEMA.names if name in table.column_names]

    return [
        Candle(**record)
        for record in table.select(columns).sort_by("timestamp").to_pylist()
    ]


class ParquetStorage:
    """Store canonical candles as partitioned Parquet files."""

    def __init__(self, root: str | Path = "data/processed") -> None:
        self.root = Path(root)

    def dataset_path(self, symbol: str, timeframe: str | None = None) -> Path:
        """Return the directory holding one symbol's (or dataset's) files."""
        path = self.root / normalize_symbol_path(symbol)

        if timeframe is not None:
            path = path / f"timeframe={timeframe}"

        return path

    def partition_files(
        self,
        *,
        symbol: str,
        timeframe: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Path]:
        """
        Return the Parquet files holding a symbol's data, in partition order.

        Restricting by range selects whole partitions: a month is included
        when any part of it falls inside ``[start, end)``, because that is
        the granularity the files are written at.
        """
        directory = self.dataset_path(symbol, timeframe)

        if not directory.exists():
            return []

        files = [
            path
            for path in sorted(directory.rglob("*.parquet"))
            if _partition_overlaps(path, start, end)
        ]

        return files

    def partition_schemas(
        self,
        *,
        symbol: str,
        timeframe: str | None = None,
    ) -> dict[Path, pa.Schema]:
        """Return the on-disk schema of every partition file."""
        return {
            path: pq.read_schema(path)
            for path in self.partition_files(symbol=symbol, timeframe=timeframe)
        }

    def write(
        self,
        candles: list[Candle],
        *,
        symbol: str,
        timeframe: str,
        merge: bool = True,
    ) -> list[Path]:
        """
        Write candles partitioned by symbol, timeframe, year and month.

        An existing partition is merged with rather than replaced: chunked
        downloads write the same month from several requests, and a resumed
        or overlapping run must not discard what earlier runs already stored.
        New candles win over stored ones for the same timestamp, so
        re-downloading a range corrects it instead of duplicating it.

        Pass ``merge=False`` to replace a partition outright.
        """
        if not candles:
            return []

        written: list[Path] = []

        grouped: dict[tuple[int, int], list[Candle]] = {}

        for candle in candles:
            key = (candle.timestamp.year, candle.timestamp.month)
            grouped.setdefault(key, []).append(candle)

        base = self.dataset_path(symbol, timeframe)

        for (year, month), group in sorted(grouped.items()):
            directory = base / f"year={year}" / f"month={month:02d}"
            directory.mkdir(parents=True, exist_ok=True)

            path = directory / "candles.parquet"

            rows = group

            if merge and path.exists():
                rows = merge_candles(table_to_candles(pq.read_table(path)), group)

            self._write_partition(path, candles_to_table(rows))

            written.append(path)

        return written

    @staticmethod
    def _write_partition(path: Path, table: pa.Table) -> None:
        """
        Replace a partition file atomically.

        Writing in place would leave a merged partition truncated if the
        process died mid-write, losing data that was already safely stored.
        """
        temporary = path.with_name(f"{path.name}.tmp")

        try:
            pq.write_table(table, temporary)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    def partition_groups(
        self,
        *,
        symbol: str,
        timeframe: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[tuple[tuple[int, int], list[Path]]]:
        """
        Return the partition files grouped by the month they hold, in order.

        This is the traversal every streaming consumer shares — validation,
        quality reporting and cross-provider comparison all walk a dataset
        this way rather than each deriving the layout again. A month is one
        group even when several files claim it, and files whose path carries
        no recognizable month are grouped under ``(0, 0)`` so they are
        scanned rather than silently skipped.
        """
        groups: dict[tuple[int, int], list[Path]] = {}

        for path in self.partition_files(
            symbol=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
        ):
            groups.setdefault(partition_month(path) or (0, 0), []).append(path)

        return sorted(groups.items())

    def read_partition(
        self,
        path: str | Path,
        *,
        columns: Sequence[str] | None = None,
    ) -> list[Candle]:
        """
        Read a single partition file.

        Comparing or scanning a multi-year dataset one partition at a time
        keeps the working set to a month of candles rather than the whole
        history.
        """
        return table_to_candles(pq.read_table(Path(path), columns=columns))

    @staticmethod
    def partition_timestamps(path: str | Path) -> list[datetime]:
        """
        Read only the timestamp column of one partition, in order.

        Coverage analysis needs when candles exist, not what they were
        worth, and skipping the five decimal columns is most of the file.
        """
        table = pq.read_table(Path(path), columns=["timestamp"])

        return sorted(table.column("timestamp").to_pylist())

    @staticmethod
    def partition_bounds(path: str | Path) -> tuple[datetime, datetime] | None:
        """
        Return a partition's first and last timestamp without reading it.

        Parquet records per-column minima and maxima in the footer, so the
        extent of a multi-year dataset can be established from metadata
        alone. Statistics are optional, so a file without them falls back to
        reading its timestamp column — one partition, never the dataset.
        """
        target = Path(path)

        try:
            metadata = pq.read_metadata(target)
            index = metadata.schema.names.index("timestamp")
        except UNREADABLE_STORAGE_ERRORS:
            return None

        low: datetime | None = None
        high: datetime | None = None

        for group in range(metadata.num_row_groups):
            statistics = metadata.row_group(group).column(index).statistics

            if statistics is None or not statistics.has_min_max:
                low = high = None
                break

            low = statistics.min if low is None else min(low, statistics.min)
            high = statistics.max if high is None else max(high, statistics.max)

        if low is not None and high is not None:
            return _as_utc(low), _as_utc(high)

        try:
            stamps = ParquetStorage.partition_timestamps(target)
        except UNREADABLE_STORAGE_ERRORS:
            return None

        if not stamps:
            return None

        return stamps[0], stamps[-1]

    def iter_timestamps(
        self,
        *,
        symbol: str,
        timeframe: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Iterator[datetime]:
        """
        Yield the stored timestamps in order, one partition at a time.

        The streaming counterpart of :meth:`read_timestamps`: only the
        current partition's timestamps are resident, so coverage analysis of
        a multi-year one-minute dataset costs a month of values rather than
        millions.
        """
        for _, paths in self.partition_groups(
            symbol=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
        ):
            stamps = sorted(
                stamp for path in paths for stamp in self.partition_timestamps(path)
            )

            for stamp in stamps:
                if start is not None and stamp < start:
                    continue

                if end is not None and stamp >= end:
                    continue

                yield stamp

    def read_table(
        self,
        *,
        symbol: str,
        timeframe: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> pa.Table:
        """
        Read a stored dataset back with PyArrow.

        ``start`` and ``end`` restrict the result to the half-open range
        ``[start, end)``; the filter is pushed into the scan, so a range read
        does not materialize the whole dataset. Returns an empty table with
        the canonical schema when nothing has been written for the symbol yet.
        """
        directory = self.dataset_path(symbol, timeframe)

        if not directory.exists():
            return CANDLE_SCHEMA.empty_table()

        dataset = ds.dataset(
            directory,
            format="parquet",
            partitioning="hive",
        )

        conditions = []

        if start is not None:
            conditions.append(ds.field("timestamp") >= start)

        if end is not None:
            conditions.append(ds.field("timestamp") < end)

        expression = None

        for condition in conditions:
            expression = condition if expression is None else expression & condition

        table = dataset.to_table(filter=expression)

        if table.num_rows == 0:
            return CANDLE_SCHEMA.empty_table()

        return table.sort_by("timestamp")

    def read_candles(
        self,
        *,
        symbol: str,
        timeframe: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Candle]:
        """Read a stored dataset back as canonical candles."""
        return table_to_candles(
            self.read_table(
                symbol=symbol,
                timeframe=timeframe,
                start=start,
                end=end,
            )
        )

    def read_timestamps(
        self,
        *,
        symbol: str,
        timeframe: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[datetime]:
        """
        Read only the timestamps of a stored dataset, in order.

        Materializes the whole range. Prefer :meth:`iter_timestamps` for
        anything that only needs one pass — a multi-year one-minute range
        holds millions of values.
        """
        return list(
            self.iter_timestamps(
                symbol=symbol,
                timeframe=timeframe,
                start=start,
                end=end,
            )
        )
