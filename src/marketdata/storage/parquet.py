from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from marketdata.models.candle import Candle


class ParquetStorage:
    """Store canonical candles as partitioned Parquet files."""

    def __init__(self, root: str | Path = "data/processed") -> None:
        self.root = Path(root)

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

        normalized_symbol = symbol.strip().upper().replace("/", "_")

        for (year, month), group in sorted(grouped.items()):
            directory = (
                self.root
                / normalized_symbol
                / f"timeframe={timeframe}"
                / f"year={year}"
                / f"month={month:02d}"
            )
            directory.mkdir(parents=True, exist_ok=True)

            path = directory / "candles.parquet"

            records = [
                {
                    "timestamp": candle.timestamp,
                    "symbol": candle.symbol,
                    "open": candle.open,
                    "high": candle.high,
                    "low": candle.low,
                    "close": candle.close,
                    "volume": candle.volume,
                }
                for candle in group
            ]

            table = pa.Table.from_pylist(records)
            pq.write_table(table, path)

            written.append(path)

        return written
