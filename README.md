# forex-market-data

Historical Forex market-data ingestion and quality pipeline.

The project downloads OHLCV candles from a market-data provider, normalizes
them to UTC, validates and deduplicates them, stores them as partitioned
Parquet, and records what it did in a manifest and a machine-readable quality
report. It is intended as the data foundation for later research and
backtesting work, so datasets are reproducible and describe their own gaps
rather than hiding them.

## Architecture

```text
Provider
    ↓
Download
    ↓
Normalize → UTC
    ↓
Validate OHLC
    ↓
Deduplicate
    ↓
Store → Parquet
    ↓
Manifest
    ↓
Quality Report
```

`DownloadPipeline.run` applies every stage explicitly and in that order,
validating once before deduplication and once more on the rows that actually
reach disk.

```text
src/marketdata/
├── models/
│   ├── candle.py         Canonical OHLCV candle
│   └── timeframe.py      Fixed cadence per timeframe
├── providers/
│   ├── base.py           MarketDataProvider abstraction
│   └── dukascopy.py      Dukascopy implementation
├── normalization/
│   └── timestamps.py     UTC normalization
├── validation/
│   └── candles.py        OHLC invariants, dedup, range restriction
├── quality/
│   ├── gaps.py           Missing-interval detection
│   └── report.py         Quality report model
├── storage/
│   ├── parquet.py        Partitioned Parquet read and write
│   └── manifest.py       Dataset manifest
├── downloader/
│   └── pipeline.py       Orchestration
└── cli.py                Command-line entry point
```

Provider-specific concerns — endpoints, pagination, payload shape, symbol
identifiers — stay inside `providers/`. Everything downstream works on
`Candle` objects, so a second provider can be added by implementing
`MarketDataProvider` alone.

## Development setup

Requires Python 3.13 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

## CLI usage

```bash
uv run marketdata download \
  --symbol EUR/USD \
  --start 2026-08-14T12:00:00Z \
  --end 2026-08-14T13:00:00Z
```

`--start` and `--end` must carry timezone information and are converted to
UTC. The range is half-open: `--end` is exclusive.

| Option | Default | Purpose |
| --- | --- | --- |
| `--timeframe` | `1min` | Dukascopy timeframe |
| `--output-root` | `data/processed` | Parquet root |
| `--manifest-root` | `data/manifests` | Manifest root |
| `--quality-root` | `data/quality` | Quality report root |
| `--strict` | off | Fail instead of dropping invalid candles |
| `--json` | off | Print the quality report as JSON |

The command prints the provider, symbol, timeframe, requested and actual
ranges, row counts at each stage, the quality status, and the path of every
artifact it wrote. It exits `1` with a single-line error when the provider
cannot be reached.

## Data layout

Candles are partitioned by symbol, timeframe, year and month:

```text
data/processed/EUR_USD/timeframe=1min/year=2026/month=08/candles.parquet
data/manifests/EUR_USD/1min_20260814T120000Z_20260814T130000Z.json
data/quality/EUR_USD/1min_20260814T120000Z_20260814T130000Z.json
```

Everything below `data/` is generated and git-ignored.

Files use a fixed Arrow schema so partitions from different months stay
mutually readable: `timestamp` as `timestamp[us, tz=UTC]`, `symbol` as
string, prices as `decimal128(18, 8)` and volume as `decimal128(28, 8)`.
Values are quantized to eight decimal places on write, because provider
payloads arrive as JSON floats whose decimal expansion runs longer than the
stored scale.

Partition directories are hive-style, so a dataset reads back directly:

```python
from marketdata.storage.parquet import ParquetStorage

storage = ParquetStorage("data/processed")
table = storage.read_table(symbol="EUR/USD", timeframe="1min")
candles = storage.read_candles(symbol="EUR/USD", timeframe="1min")
```

### Quality report

```json
{
  "provider": "dukascopy",
  "symbol": "EUR/USD",
  "timeframe": "1min",
  "requested_start": "2026-08-14T12:00:00Z",
  "requested_end": "2026-08-14T13:00:00Z",
  "actual_start": "2026-08-14T12:00:00Z",
  "actual_end": "2026-08-14T12:59:00Z",
  "downloaded_rows": 60,
  "final_rows": 60,
  "duplicates_removed": 0,
  "invalid_rows": 0,
  "out_of_range_rows": 0,
  "missing_intervals": [],
  "missing_candles": 0,
  "expected_rows": 60,
  "cadence_seconds": 60,
  "status": "ok",
  "violations": [],
  "violations_truncated": false,
  "generated_at": "2026-08-14T13:05:00.123456Z"
}
```

`status` is one of `ok`, `empty`, `invalid` (rows broke an OHLC invariant) or
`incomplete` (the requested range is not fully covered); when more than one
applies, the first matching in that order wins.

## Testing

```bash
uv run ruff format .
uv run ruff check .
uv run pytest
```

The suite covers the model, the provider contract, the Dukascopy provider
against a mock HTTP transport (including backwards pagination), UTC
normalization, OHLC validation, deduplication, gap detection, the quality
report, Parquet round-tripping and the CLI end to end.

## Data-quality guarantees

For every stored dataset:

- **UTC throughout.** Timestamps are timezone-aware and normalized to UTC on
  ingest, on disk and on read-back. Naive datetimes are rejected at the model
  boundary.
- **OHLC invariants hold.** Every stored candle satisfies `low <= open, close
  <= high` with non-negative volume. Rows that fail are dropped and recorded
  in the quality report; `--strict` fails the run instead.
- **Validated after transformation.** Validation runs again after
  deduplication, so the check covers the rows that actually reach disk.
- **One row per timestamp.** Duplicates are removed deterministically, keeping
  the last occurrence.
- **No data beyond the requested range.** Candles outside `[start, end)` are
  discarded, so a dataset never contains observations from after the window it
  was requested for and cannot leak look-ahead information into work built on
  it.
- **Counts reconcile.** `downloaded_rows - out_of_range_rows - invalid_rows -
  duplicates_removed == final_rows`.
- **Gaps are reported, not filled.** Missing intervals are listed with their
  candle counts; no value is ever interpolated or forward-filled.

### Known limitations

- Missing intervals are derived purely from the timeframe cadence, so a range
  spanning a weekend or holiday closure is reported as `incomplete` even
  though the venue was legitimately shut. Session-calendar awareness is not
  implemented.
- Gap detection is skipped for timeframes without a constant spacing;
  `1day_eet` shifts across daylight-saving transitions. Such reports carry
  `cadence_seconds: null` and an empty `missing_intervals`.
- Repeated downloads of the same symbol, timeframe and month overwrite the
  Parquet file for that partition rather than merging with it.
