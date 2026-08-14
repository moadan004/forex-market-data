from __future__ import annotations

from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from marketdata.models.candle import Candle

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

    def write(
        self,
        candles: list[Candle],
        *,
        symbol: str,
        timeframe: str,
    ) -> list[Path]:
        """Write candles partitioned by symbol, timeframe, year and month."""
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

            pq.write_table(candles_to_table(group), path)

            written.append(path)

        return written

    def read_table(
        self,
        *,
        symbol: str,
        timeframe: str | None = None,
    ) -> pa.Table:
        """
        Read a stored dataset back with PyArrow.

        Returns an empty table with the canonical schema when nothing has been
        written for the symbol yet.
        """
        directory = self.dataset_path(symbol, timeframe)

        if not directory.exists():
            return CANDLE_SCHEMA.empty_table()

        dataset = ds.dataset(
            directory,
            format="parquet",
            partitioning="hive",
        )

        table = dataset.to_table()

        if table.num_rows == 0:
            return CANDLE_SCHEMA.empty_table()

        return table.sort_by("timestamp")

    def read_candles(
        self,
        *,
        symbol: str,
        timeframe: str | None = None,
    ) -> list[Candle]:
        """Read a stored dataset back as canonical candles."""
        return table_to_candles(self.read_table(symbol=symbol, timeframe=timeframe))
