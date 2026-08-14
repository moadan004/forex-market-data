# forex-market-data

Historical Forex market-data ingestion and quality pipeline.

The project downloads OHLCV candles from a market-data provider, normalizes
them to UTC, validates and deduplicates them, merges them into partitioned
Parquet, and records what it did in a manifest and a machine-readable quality
report. Multi-year ranges are downloaded in chunks and can be resumed after a
failure. It is intended as the data foundation for later research and
backtesting work, so datasets are reproducible and describe their own gaps
rather than hiding them.

> **Roadmap:** [`planner.md`](planner.md) is the authoritative development
> roadmap — phases, status, blockers and the next milestone. This README
> describes only what is implemented today. No historical dataset has been
> acquired yet, and no backtesting engine exists; both are tracked in the
> planner.

## Architecture

```text
Session calendar
        ↓
Chunk planning
        ↓
Resume/checkpoint lookup
        ↓
Provider download        ─┐
        ↓                 │
Normalize → UTC           │
        ↓                 │
Range restriction         ├─ per chunk
        ↓                 │
Validate OHLC             │
        ↓                 │
Deduplicate               │
        ↓                 │
Merge into Parquet        │
        ↓                 │
Checkpoint completed     ─┘
        ↓
Manifest
        ↓
Quality report
```

Each chunk runs the full ingestion sequence on its own and is checkpointed
independently, so a chunk that fails leaves the others' data intact and a
rerun continues from the first chunk that did not complete. The manifest and
quality report are written once at the end and describe the stored dataset,
not just the chunks the latest run happened to fetch.

```text
src/marketdata/
├── models/
│   ├── candle.py         Canonical OHLCV candle
│   └── timeframe.py      Fixed cadence per timeframe
├── calendar/
│   ├── base.py           MarketCalendar abstraction, 24x7 calendar
│   └── forex.py          Spot FX weekly session and holidays
├── providers/
│   ├── base.py           MarketDataProvider abstraction
│   └── dukascopy.py      Dukascopy implementation
├── normalization/
│   └── timestamps.py     UTC normalization
├── validation/
│   └── candles.py        OHLC invariants, dedup, merge, range restriction
├── quality/
│   ├── gaps.py           Missing-interval detection
│   └── report.py         Quality report model
├── storage/
│   ├── parquet.py        Partitioned Parquet merge, write and read
│   └── manifest.py       Dataset manifest
├── downloader/
│   ├── chunks.py         Chunk planning
│   ├── checkpoint.py     Resume state
│   └── pipeline.py       Orchestration
└── cli.py                Command-line entry point
```

Provider-specific concerns — endpoints, pagination, payload shape, symbol
identifiers — stay inside `providers/`. Everything downstream works on
`Candle` objects, so a second provider can be added by implementing
`MarketDataProvider` alone.

## Session-calendar awareness

A trading calendar decides whether a stretch of time was *expected* to hold
candles or was a market closure. Without it every weekend in a multi-year
range would be reported as missing data.

Calendars describe only when the market is **closed**; open intervals and
expected candle counts are derived from that in the base class, so a new
closure rule never reaches into the pipeline.

- `forex` (default) — spot FX trades continuously from the Sunday evening
  open to the Friday evening close. The defaults close at **21:00 UTC on
  Friday** and reopen at **22:00 UTC on Sunday**, so all of Saturday and most
  of Sunday are closed.
- `24x7` — never closes; useful to see a range without any calendar
  filtering.

The forex boundaries are deliberately conservative. The real weekly boundary
follows 17:00 New York time, which is 21:00 UTC while the United States
observes daylight saving and 22:00 UTC otherwise. Taking the widest of the
two as closed means the calendar never claims a candle was expected during an
hour the market may have been shut. Both boundaries are constructor
arguments for callers who need a specific venue's convention.

`ForexCalendar` also accepts holidays as whole UTC days. **No holiday list
ships with the project** — pass one in to have those days treated as
closures.

## Chunking and resume

A requested range is split into contiguous, non-overlapping chunks that cover
it exactly, and each chunk becomes one provider request that can succeed,
fail and be retried on its own.

Interior boundaries sit on the timeline rather than on the requested start:
month sizes break on calendar month starts (multi-month sizes aligned from
January), duration sizes on a grid anchored to the Unix epoch. The same
request therefore always produces the same chunks, and a later run asking for
a different start still lands on the same boundaries.

Progress is written to one JSON checkpoint per request, keyed by symbol,
timeframe and requested range, after every chunk. A rerun of the same command
skips the chunks already marked completed and retries the rest. Stored
progress is reused only when a chunk's boundaries match the newly planned
chunk exactly — changing `--chunk-size` moves the boundaries, so that
progress is dropped rather than trusted. Checkpoints are written through a
temporary file and renamed into place, since a download is interrupted
precisely when something goes wrong.

By default a failing chunk is recorded and the run continues, so one bad
window does not abandon the rest of a multi-year range; the command exits `1`
and lists the failed chunks. `--strict` stops at the first failure instead.

## Partition merging

Writing a month **merges** with whatever that partition already holds instead
of replacing it: chunked downloads write the same month from several
requests, and a resumed run must not truncate data it already stored.

```text
existing partition + new candles
        ↓
merge, keeping one row per symbol + timestamp (new candles win)
        ↓
sort chronologically
        ↓
rewrite the partition atomically
```

Re-running a completed chunk is therefore a no-op, and overlapping ranges
reconcile instead of duplicating. The file is written to a temporary path and
renamed, so an interrupted write cannot leave a merged partition truncated.

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
| `--chunk-size` | `1month` | Size of each provider request: `1month`, `3months`, `2w`, `7d`, `12h`, `30min` |
| `--calendar` | `forex` | Trading calendar: `forex` or `24x7` |
| `--no-resume` | off | Ignore saved progress and download every chunk again |
| `--data-root` | `data/processed` | Parquet root (also accepted as `--output-root`) |
| `--manifest-root` | `data/manifests` | Manifest root |
| `--quality-root` | `data/quality` | Quality report root |
| `--checkpoint-root` | `data/checkpoints` | Resume checkpoint root |
| `--strict` | off | Stop on the first failing or invalid chunk |
| `--json` | off | Print the quality report as JSON |

The command prints the provider, symbol, timeframe, calendar, requested and
actual ranges, chunk progress, row counts at each stage, the quality status
and the path of every artifact it wrote. It exits `1` when a chunk failed,
listing each failure, and rerunning the same command retries only those
chunks.

## Data layout

Candles are partitioned by symbol, timeframe, year and month:

```text
data/processed/EUR_USD/timeframe=1min/year=2026/month=08/candles.parquet
data/manifests/EUR_USD/1min_20260814T120000Z_20260814T130000Z.json
data/quality/EUR_USD/1min_20260814T120000Z_20260814T130000Z.json
data/checkpoints/EUR_USD/1min_20260814T120000Z_20260814T130000Z.json
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

`read_table`, `read_candles` and `read_timestamps` accept `start` and `end`
to restrict the read; the filter is pushed into the scan.

### Quality report

```json
{
  "provider": "dukascopy",
  "symbol": "EUR/USD",
  "timeframe": "1min",
  "calendar": "forex",
  "requested_start": "2026-08-14T12:00:00Z",
  "requested_end": "2026-08-14T13:00:00Z",
  "actual_start": "2026-08-14T12:00:00Z",
  "actual_end": "2026-08-14T12:59:00Z",
  "expected_rows": 60,
  "downloaded_rows": 60,
  "retained_rows": 60,
  "duplicates_removed": 0,
  "invalid_rows": 0,
  "out_of_range_rows": 0,
  "missing_intervals": [],
  "missing_candles": 0,
  "market_closed_intervals": [],
  "cadence_seconds": 60,
  "chunks_total": 1,
  "chunks_completed": 1,
  "chunks_failed": 0,
  "status": "ok",
  "violations": [],
  "violations_truncated": false,
  "generated_at": "2026-08-14T13:05:00.123456Z"
}
```

`missing_intervals` covers only expected **trading** time; closures are
reported separately in `market_closed_intervals`, each with a reason
(`weekend`, `holiday`, or `market closure` where several overlap).

The counts fall into two groups. `downloaded_rows`, `duplicates_removed`,
`invalid_rows` and `out_of_range_rows` describe the chunks the run actually
fetched. `retained_rows`, the actual range and the intervals describe the
stored dataset over the requested range — so a fully resumed run reports zero
downloaded rows and a complete dataset.

`status` is one of:

| Status | Meaning |
| --- | --- |
| `failed` | at least one chunk did not download |
| `empty` | rows were expected but none are stored |
| `invalid` | rows broke an OHLC invariant and were dropped |
| `incomplete` | expected trading time is not fully covered |
| `ok` | none of the above |

When more than one applies, the first matching in that order wins.

## Testing

```bash
uv run ruff format .
uv run ruff check .
uv run pytest
```

The suite covers the model, the provider contract, the Dukascopy provider
against a mock HTTP transport (including backwards pagination), UTC
normalization, OHLC validation, deduplication, the session calendar, chunk
planning, checkpoint and resume behaviour, partition merging, gap detection,
the quality report, Parquet round-tripping, and the CLI end to end for fresh,
failed and resumed runs.

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
- **One row per timestamp.** Duplicates are removed deterministically within a
  download and again when merging into an existing partition, keeping the
  most recently downloaded row.
- **No data beyond the requested range.** Candles outside a chunk's
  `[start, end)` are discarded, so a dataset never contains observations from
  after the window it was requested for and cannot leak look-ahead
  information into work built on it.
- **Counts reconcile.** `downloaded_rows - out_of_range_rows - invalid_rows -
  duplicates_removed` equals the rows this run contributed.
- **Gaps are reported, not filled.** Missing trading intervals are listed with
  their candle counts; no value is ever interpolated or forward-filled.
- **Interrupted runs do not corrupt stored data.** Partitions and checkpoints
  are written to a temporary file and renamed into place, and a failing chunk
  never rewrites another chunk's partition.

### Known limitations

- **The live provider is unverified.** Every test runs against a mock HTTP
  transport. The development environment's egress policy blocks
  `freeserv.dukascopy.com` with a proxy `403`, so no request has reached
  Dukascopy and the provider's behaviour against the real API — response
  shape, pagination, rate limits, history depth — remains unconfirmed.
- No holiday list ships with the project, so market holidays are reported as
  missing trading candles unless holidays are passed to `ForexCalendar`.
- The forex weekly boundary uses fixed UTC times rather than tracking US
  daylight saving; the hour between 21:00 and 22:00 UTC on Friday is treated
  as closed year-round.
- Gap detection is skipped for timeframes without a constant spacing;
  `1day_eet` shifts across daylight-saving transitions. Such reports carry
  `cadence_seconds: null` and an empty `missing_intervals`.
- Quality reporting reads every stored timestamp in the requested range into
  memory. A seven-year one-minute range is a few million values.
- A chunk is retried whole. There is no partial-chunk recovery and no
  automatic retry within a single run — rerun the command.
- There is no rate limiting. Requests are issued as fast as the download loop
  allows, which is not yet suitable for a multi-year run against a free
  endpoint.

## Roadmap

See [`planner.md`](planner.md) for the full phase breakdown, per-phase
acceptance criteria, current blockers and the next milestone. In short: the
ingestion path through resumable chunked downloads is complete; retry/backoff
and rate limiting are next; live provider access is blocked by the
development environment's egress policy; dataset acquisition, backtesting,
analytics and the UI are planned and not started.
