from __future__ import annotations

import csv
from collections.abc import Iterator, Sequence
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from marketdata.models.candle import Candle
from marketdata.models.timeframe import TIMEFRAME_CADENCE
from marketdata.normalization.timestamps import ensure_utc
from marketdata.providers.base import MarketDataProvider
from marketdata.providers.errors import ProviderDataError

REQUIRED_COLUMNS = ("timestamp", "open", "high", "low", "close")
OPTIONAL_COLUMNS = ("volume", "symbol")

CSV_SCHEMA = (*REQUIRED_COLUMNS, *OPTIONAL_COLUMNS)
"""Columns a candle CSV may contain.

``timestamp,open,high,low,close`` are required. ``volume`` defaults to zero
when absent, matching the canonical candle. ``symbol`` lets one file hold
several instruments; without it the file is taken to hold the symbol that
was asked for.
"""


class CsvProviderError(ProviderDataError):
    """
    Raised when a CSV source cannot supply the requested candles.

    A missing file, an unreadable row or an unknown symbol will not change
    on a second attempt, so this is a permanent provider error and the retry
    layer leaves it alone.
    """


class CsvMarketDataProvider(MarketDataProvider):
    """
    Market-data provider backed by local CSV files.

    Serves the same :class:`~marketdata.providers.base.MarketDataProvider`
    contract as a network provider, so the download pipeline, chunk planner,
    checkpoints, storage and validation work against it unchanged — and can
    be exercised end to end with no network at all.

    The source is either a single CSV file or a directory of them. In
    directory mode the file for a request is looked up by symbol and
    timeframe::

        <source>/EUR_USD_1min.csv
        <source>/EUR_USD.csv

    Rows are returned exactly as the file holds them, restricted to the
    requested range. Duplicates, gaps and rows that break an OHLC invariant
    are passed on for the pipeline to deal with: a provider that quietly
    repaired its input would hide the very defects the quality report exists
    to surface. Nothing is ever interpolated or invented.
    """

    def __init__(
        self,
        source: str | Path,
        *,
        name: str = "csv",
    ) -> None:
        self.source = Path(source)
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    # -- source resolution -------------------------------------------------

    def _candidates(self, symbol: str, timeframe: str) -> list[Path]:
        """Return the file names that could serve a request, in order."""
        if self.source.is_file():
            return [self.source]

        stem = symbol.strip().upper().replace("/", "_")

        return [
            self.source / f"{stem}_{timeframe}.csv",
            self.source / f"{stem}.csv",
        ]

    def _resolve(self, symbol: str, timeframe: str) -> Path:
        if not self.source.exists():
            raise CsvProviderError(f"CSV source does not exist: {self.source}")

        candidates = self._candidates(symbol, timeframe)

        for path in candidates:
            if path.is_file():
                return path

        looked_for = ", ".join(str(path) for path in candidates)

        raise CsvProviderError(
            f"No CSV for {symbol} {timeframe}. Looked for: {looked_for}"
        )

    # -- parsing -----------------------------------------------------------

    @staticmethod
    def _require_columns(header: Sequence[str] | None, path: Path) -> None:
        if header is None:
            raise CsvProviderError(f"{path} is empty")

        present = {column.strip().lower() for column in header}
        missing = [column for column in REQUIRED_COLUMNS if column not in present]

        if missing:
            raise CsvProviderError(
                f"{path} is missing required columns: {', '.join(missing)}. "
                f"Expected at least: {', '.join(REQUIRED_COLUMNS)}"
            )

    @staticmethod
    def _parse_timestamp(raw: str, *, path: Path, line: int) -> datetime:
        text = raw.strip()

        if not text:
            raise CsvProviderError(f"{path} line {line}: empty timestamp")

        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise CsvProviderError(
                f"{path} line {line}: invalid ISO-8601 timestamp {text!r}"
            ) from exc

        if parsed.tzinfo is None:
            raise CsvProviderError(
                f"{path} line {line}: timestamp {text!r} has no timezone. "
                "Timestamps must state their offset, for example "
                "2026-08-10T00:00:00Z"
            )

        return ensure_utc(parsed)

    @staticmethod
    def _parse_decimal(
        raw: str | None,
        *,
        field: str,
        path: Path,
        line: int,
    ) -> Decimal:
        text = (raw or "").strip()

        if not text:
            raise CsvProviderError(f"{path} line {line}: empty {field}")

        try:
            return Decimal(text)
        except InvalidOperation as exc:
            raise CsvProviderError(
                f"{path} line {line}: {field} {text!r} is not a number"
            ) from exc

    def _read(self, path: Path, symbol: str) -> Iterator[Candle]:
        """
        Yield every candle a file holds for one symbol.

        A malformed row stops the read rather than being skipped: silently
        dropping rows would turn a broken file into a dataset with an
        unexplained gap.
        """
        normalized = symbol.strip().upper()

        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            self._require_columns(reader.fieldnames, path)

            has_symbol_column = "symbol" in {
                (field or "").strip().lower() for field in reader.fieldnames or ()
            }

            for line, row in enumerate(reader, start=2):
                if None in row:
                    raise CsvProviderError(
                        f"{path} line {line}: more values than the header declares"
                    )

                values = {
                    (key or "").strip().lower(): value for key, value in row.items()
                }

                if has_symbol_column:
                    row_symbol = (values.get("symbol") or "").strip().upper()

                    if row_symbol != normalized:
                        continue

                yield Candle(
                    timestamp=self._parse_timestamp(
                        values.get("timestamp", ""),
                        path=path,
                        line=line,
                    ),
                    symbol=normalized,
                    open=self._parse_decimal(
                        values.get("open"), field="open", path=path, line=line
                    ),
                    high=self._parse_decimal(
                        values.get("high"), field="high", path=path, line=line
                    ),
                    low=self._parse_decimal(
                        values.get("low"), field="low", path=path, line=line
                    ),
                    close=self._parse_decimal(
                        values.get("close"), field="close", path=path, line=line
                    ),
                    volume=(
                        self._parse_decimal(
                            values["volume"], field="volume", path=path, line=line
                        )
                        if (values.get("volume") or "").strip()
                        else Decimal(0)
                    ),
                )

    # -- provider contract -------------------------------------------------

    def get_supported_symbols(self) -> Sequence[str]:
        """Return the symbols this source can serve."""
        if not self.source.exists():
            raise CsvProviderError(f"CSV source does not exist: {self.source}")

        if self.source.is_file():
            return self._symbols_in_file(self.source)

        symbols = {
            path.stem.rsplit("_", 1)[0] if _has_timeframe_suffix(path) else path.stem
            for path in sorted(self.source.glob("*.csv"))
        }

        return sorted(symbol.replace("_", "/") for symbol in symbols)

    @staticmethod
    def _symbols_in_file(path: Path) -> list[str]:
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = {
                (field or "").strip().lower() for field in reader.fieldnames or ()
            }

            if "symbol" not in fields:
                # Without a symbol column the file serves whatever is asked
                # of it, so there is no list to advertise.
                return []

            return sorted(
                {
                    (row.get("symbol") or "").strip().upper()
                    for row in reader
                    if (row.get("symbol") or "").strip()
                }
            )

    def fetch_candles(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "1min",
    ) -> list[Candle]:
        """Return the file's candles for ``symbol`` inside ``[start, end)``."""
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("start and end must be timezone-aware")

        if start >= end:
            raise ValueError("start must be before end")

        start = ensure_utc(start)
        end = ensure_utc(end)

        path = self._resolve(symbol, timeframe)

        selected = [
            candle
            for candle in self._read(path, symbol)
            if start <= candle.timestamp < end
        ]

        return sorted(selected, key=lambda candle: candle.timestamp)

    def health_check(self) -> bool:
        """Return whether the source exists and can be read."""
        try:
            if not self.source.exists():
                return False

            if self.source.is_file():
                self.source.open(encoding="utf-8").close()
                return True

            return any(self.source.glob("*.csv"))
        except OSError:
            return False


def _has_timeframe_suffix(path: Path) -> bool:
    """Return whether a file name ends in a timeframe, as ``EUR_USD_1min``."""
    tail = path.stem.rsplit("_", 1)

    return len(tail) == 2 and tail[1] in TIMEFRAME_CADENCE


__all__ = [
    "CSV_SCHEMA",
    "OPTIONAL_COLUMNS",
    "REQUIRED_COLUMNS",
    "CsvMarketDataProvider",
    "CsvProviderError",
]
