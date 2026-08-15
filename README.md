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
│   ├── errors.py         Transient vs permanent failure classification
│   ├── retry.py          Retry policy and executor
│   ├── rate_limit.py     Outbound request pacing
│   ├── dukascopy.py      Dukascopy implementation (network)
│   └── csv.py            Local CSV implementation (offline)
├── normalization/
│   └── timestamps.py     UTC normalization
├── validation/
│   └── candles.py        OHLC invariants, dedup, merge, range restriction
├── quality/
│   ├── gaps.py           Missing-interval detection
│   ├── report.py         Quality report model
│   └── dataset.py        Offline validation of a stored dataset
├── storage/
│   ├── parquet.py        Partitioned Parquet merge, write and read
│   └── manifest.py       Dataset manifest
├── downloader/
│   ├── chunks.py         Chunk planning
│   ├── checkpoint.py     Resume state
│   └── pipeline.py       Orchestration
├── verification/
│   ├── status.py         PASS / WARN / FAIL / BLOCKED and thresholds
│   ├── stages.py         smoke, daily, monthly, historical
│   ├── preflight.py      Can this provider supply usable data at all?
│   ├── records.py        Durable evidence that a stage passed
│   ├── guard.py          Refusing a large download without evidence
│   └── runner.py         Running a stage through the real pipeline
└── cli.py                Command-line entry point
```

## Providers

Provider-specific concerns — endpoints, pagination, payload shape, symbol
identifiers, file layout — stay inside `providers/`. Everything downstream
works on `Candle` objects, so a provider is added by implementing
`MarketDataProvider` alone: `name`, `get_supported_symbols`, `fetch_candles`
and `health_check`.

Two implementations ship, and the pipeline cannot tell them apart:

| Provider | `--provider` | Source | Network |
| --- | --- | --- | --- |
| `DukascopyProvider` | `dukascopy` (default) | Dukascopy HTTP API | required |
| `CsvMarketDataProvider` | `csv` | local files | never |

Retries and rate limiting are provider capabilities, not pipeline
requirements: the pipeline asks a provider whether it has them rather than
assuming. A CSV run reports `Rate limit: none` because a local file needs
no pacing.

### The CSV provider

`CsvMarketDataProvider` reads candles from local CSV, which makes the whole
ingestion path — chunking, checkpoints, resume, merging, storage, manifests,
validation and quality reporting — exercisable with no network at all.

**Schema.** `timestamp,open,high,low,close` are required; `volume` and
`symbol` are optional.

```csv
timestamp,open,high,low,close,volume
2026-08-10T00:00:00Z,1.17000,1.17020,1.16990,1.17005,100
2026-08-10T00:01:00Z,1.17001,1.17021,1.16991,1.17006,101
```

- `timestamp` — ISO-8601 **with an offset**. `Z` and `+03:00` are both
  accepted and normalized to UTC through the same path as any other
  provider. A naive timestamp is rejected, not guessed at.
- `open`, `high`, `low`, `close`, `volume` — decimal numbers, read as
  `Decimal` so no precision is lost on the way in. `volume` defaults to `0`
  when the column is absent.
- `symbol` — optional. With it, one file can hold several instruments and
  rows are selected by symbol. Without it, the file serves whatever symbol
  is requested of it.

**Source layout.** The source is a single file, or a directory searched by
symbol and timeframe:

```text
<source>/EUR_USD_1min.csv
<source>/EUR_USD.csv
```

**What it does not do.** It returns what the file holds, restricted to the
requested range. Duplicates, gaps and rows breaking an OHLC invariant are
passed on for the pipeline to judge — a provider that quietly repaired its
input would hide the very defects the quality report exists to surface.
Nothing is interpolated or invented. A malformed row stops the read, naming
the file, line and field, rather than being skipped: silently dropping rows
would turn a broken file into a dataset with an unexplained gap.

**Offline example.**

```bash
uv run marketdata download \
  --provider csv \
  --source tests/fixtures/csv/dataset \
  --symbol EUR/USD \
  --start 2026-08-10T00:00:00Z \
  --end 2026-08-10T02:00:00Z \
  --chunk-size 30min

uv run marketdata validate --symbol EUR/USD --calendar 24x7
```

> **This is not live-provider verification.** Exercising the pipeline against
> CSV proves the pipeline, not Dukascopy. No request has ever reached
> Dukascopy from this environment, and the provider's live behaviour —
> response shape, pagination, rate limits, history depth — remains
> unconfirmed.

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

## Retries and error classification

Every provider failure is classified before anything decides what to do with
it. Only failures that can plausibly succeed on a second attempt are retried.

| Classification | Covers | Retried |
| --- | --- | --- |
| `ProviderTimeoutError` | connect/read/pool timeouts, HTTP 408, 425 | yes |
| `ProviderConnectionError` | connection refused or cut, truncated response | yes |
| `ProviderRateLimitError` | HTTP 429, honouring `Retry-After` | yes |
| `ProviderServerError` | HTTP 5xx | yes |
| `ProviderAuthError` | HTTP 401, 403, proxy rejections | no |
| `ProviderClientError` | HTTP 400 and other 4xx, malformed requests | no |
| `ProviderDataError` | unknown symbol, unusable payload (`DukascopyError`) | no |

A proxy refusing the tunnel is treated as permanent: an egress policy denial
will not change however often it is asked.

Retries wrap the whole HTTP exchange in the Dukascopy request layer, using
`tenacity` with exponential backoff, so a 429 or a 5xx is retried the same
way a dropped connection is. The attempt count is always bounded — a
multi-year download makes thousands of requests, and an unbounded retry turns
one unreachable provider into a run that never ends. `Retry-After` can extend
a wait but never past the configured ceiling.

Each chunk's checkpoint records how many retries it cost, the last error, the
error class, and whether the failure that finally stopped it was transient —
so a chunk that exhausted its retries reads differently from one that failed
on the first permanent error. That history survives a restart and accumulates
across runs.

## Rate limiting

Every outbound provider request passes a limiter that spaces requests at
least `1 / rate` seconds apart. The rate is the only knob — one setting
expressed two ways is easier to reason about than two that can disagree —
and the spacing is strict, so an idle period does not bank a burst that all
arrives at once.

| Property | Value |
| --- | --- |
| Default | **2 requests/second** (a 0.5s minimum interval) |
| Configuration | `--rate-limit REQUESTS_PER_SECOND` |
| Unlimited setting | none; the rate must be positive and finite |
| Clock | monotonic, so a system clock change cannot release a burst |

The default is deliberately conservative. Dukascopy's free endpoint
publishes no limit, and a multi-year one-minute download runs to hundreds of
requests per symbol once pagination is counted; two per second keeps that in
the tens of minutes while staying gentle enough that an unpublished limit is
unlikely to be hit. Raise it only against a provider whose limits you know.

The limiter is safe to share between callers. The slot for the next request
is reserved under a lock and the waiting happens outside it, so concurrent
callers get distinct, evenly spaced slots and wait in parallel rather than
queueing behind a held lock.

### How it works with retries

Rate limiting and retry/backoff solve different problems and both apply.
Backoff decides *when it is worth trying again* after a failure; the limiter
decides *how fast requests may leave* at all.

```text
request → rate limiter → HTTP request
                            │
                     failure? ── no → done
                            │
                           yes
                            ↓
                     retry backoff
                            ↓
                     rate limiter        ← a retry queues like any request
                            ↓
                       next request
```

The limiter sits inside the retried operation, so a retry takes a slot like
any other request. Letting retries skip the queue would lift the limit
exactly when the provider is already struggling. When backoff has already
waited longer than the interval — after a `Retry-After`, for instance — the
limiter adds nothing on top rather than waiting a second time.

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

Four commands: `provider-check` asks whether a provider can supply usable
data, `verify-stage` proves it on a small range, `download` acquires, and
`validate` checks what was acquired.

### download

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
| `--provider` | `dukascopy` | `dukascopy` (network) or `csv` (local files) |
| `--source` | — | CSV file or directory; required for `--provider csv` |
| `--timeframe` | `1min` | Timeframe to request |
| `--chunk-size` | `1month` | Size of each provider request: `1month`, `3months`, `2w`, `7d`, `12h`, `30min` |
| `--calendar` | `forex` | Trading calendar: `forex` or `24x7` |
| `--no-resume` | off | Ignore saved progress and download every chunk again |
| `--data-root` | `data/processed` | Parquet root (also accepted as `--output-root`) |
| `--manifest-root` | `data/manifests` | Manifest root |
| `--quality-root` | `data/quality` | Quality report root |
| `--checkpoint-root` | `data/checkpoints` | Resume checkpoint root |
| `--rate-limit` | `2` | Maximum provider requests per second, applied to every request including retries |
| `--retry-attempts` | `4` | Total attempts per provider request, including the first; `1` disables retries |
| `--retry-backoff` | `1.0` | Seconds before the first retry, doubling thereafter |
| `--retry-max-backoff` | `60.0` | Upper bound on the wait between retries |
| `--strict` | off | Stop on the first failing or invalid chunk |
| `--json` | off | Print the quality report as JSON |

The command prints the provider, symbol, timeframe, calendar, requested and
actual ranges, chunk progress, row counts at each stage, the retries spent,
the active rate limit and time spent throttled, the quality status and the
path of every artifact it wrote. It exits `1` when a chunk failed,
listing each failure, and rerunning the same command retries only those
chunks.

### validate

Check an already-downloaded dataset. This reads Parquet from disk and
contacts no provider, so a dataset can be re-checked at any time by anyone
holding the files — including in CI, and including datasets acquired by
someone else.

```bash
uv run marketdata validate --symbol EUR/USD
```

| Option | Default | Purpose |
| --- | --- | --- |
| `--symbol` | required | Symbol to inspect |
| `--timeframe` | `1min` | Timeframe to inspect |
| `--start` / `--end` | from manifests | Window to judge the dataset against |
| `--calendar` | `forex` | Calendar separating expected candles from closures |
| `--data-root` | `data/processed` | Parquet root |
| `--manifest-root` | `data/manifests` | Manifest root |
| `--no-manifests` | off | Judge the data on its own extent, ignoring manifests |
| `--allow-incomplete` | off | Exit 0 when the only finding is missing candles |
| `--json` | off | Print the report as JSON |

It reports the symbol, timeframe, checked and actual ranges, candles stored
against candles expected, missing candles and the gaps that make them up,
market closures, duplicates, invalid OHLC rows, out-of-range rows,
out-of-order rows, the Parquet files making up the dataset, whether their
schemas agree, and whether each manifest still matches what is stored.

**Which window is it judged against?** An explicit `--start`/`--end` wins.
Otherwise the manifests say what was meant to be acquired, which is the
honest yardstick for completeness. With neither, the data is judged against
its own extent — which by construction can never report a missing candle at
the edges.

**Exit codes.** `0` when the dataset is `ok`; `1` otherwise, so the command
can gate a pipeline. `--allow-incomplete` downgrades missing candles alone to
success, for datasets with known provider or holiday gaps; it never hides a
structural defect.

| Status | Meaning |
| --- | --- |
| `invalid` | a structural defect: duplicates, invalid OHLC, out-of-range or out-of-order rows, a schema mismatch, an unreadable file, or a manifest that disagrees |
| `empty` | nothing is stored |
| `incomplete` | expected trading time is not fully covered |
| `ok` | none of the above |

A structural defect outranks incompleteness: a gap may be the provider's
fault, but a duplicate row or a mismatched schema is ours.

## Staged acquisition

Years of one-minute data is the last thing to attempt, not the first. The
staged workflow proves the provider on an hour, then a day, then a month,
and only then permits a historical acquisition — each stage running through
the **same** production pipeline, and each judged by re-reading what it
actually stored.

```text
provider-check      can this provider supply usable data at all?
      ↓
verify-stage smoke      1 hour
      ↓
verify-stage daily      1 trading day
      ↓
verify-stage monthly    1 calendar month
      ↓
download                arbitrary ranges, now permitted
```

### provider-check

Asks a provider everything that must hold before trusting it with a range:
that it can be reached, that it accepts the symbol and the timeframe, that
candles come back, that they parse into canonical `Candle` objects, that
timestamps are UTC and ordered and inside the window, that OHLC invariants
hold, and that **pagination stitches**: the same range asked for in two
halves must equal the whole, with nothing lost or repeated at the boundary.

```bash
uv run marketdata provider-check --provider dukascopy --symbol EUR/USD --timeframe 1min
```

Each check reports `PASS`, `WARN`, `FAIL` or `BLOCKED`.

### verify-stage

Acquires one stage's window through `DownloadPipeline` — chunking, rate
limiting, retries, checkpoints, merging, manifest and quality report all
included — then re-inspects the result from disk with the dataset
validator, and writes a verification record.

```bash
uv run marketdata verify-stage --stage smoke --symbol EUR/USD --start 2026-08-10T00:00:00Z
```

The stage decides its own end: smoke is an hour, daily a day, monthly a
calendar month. Only `historical` takes an explicit `--end`.

Verification is never "the command exited 0". A stage passes only if the
stored dataset reads back, its schema is consistent, its manifest agrees
with it, its timestamps and OHLC are valid, nothing falls outside the
range, nothing is duplicated, and the calendar-aware shortfall is within
threshold.

### What each status means

| Status | Meaning |
| --- | --- |
| `PASS` | the stage proved what it set out to prove |
| `WARN` | proven, with a tolerated shortfall of candles |
| `FAIL` | data was obtained and something is wrong with it |
| `BLOCKED` | the provider could not be reached, so **nothing was proven** |

`BLOCKED` is deliberately not `FAIL`. A network policy denial says nothing
about whether the provider or this code is correct, and recording it as a
failure would be both a false accusation and a way to lose track of work
that is simply not done yet.

### Quality thresholds

**Structural defects have no tolerance.** Invalid OHLC, duplicate
timestamps, out-of-range rows, an unreadable file, a mismatched schema or a
manifest that disagrees with its data all indicate a bug here or a corrupt
file, not a property of the market. Any occurrence fails.

**Missing candles are tolerated within a ratio** (`--missing-warn-ratio`,
`--missing-fail-ratio`; default warn above 0%, fail above 1%). A minute
with no tick produces no candle, and spot FX genuinely has such minutes.

> The 1% default is a **starting policy, not an empirical finding**. No real
> Dukascopy data has ever been observed by this project, so the tolerable
> gap rate is unknown. It is set conservatively — wide enough for ordinary
> thin-liquidity minutes, narrow enough that a systematically broken
> download cannot pass — and should be recalibrated from the first real
> monthly acquisition, recording the reasoning.

### Verification records

Each stage writes a durable record:

```text
data/verification/dukascopy/EUR_USD/1min/smoke.json
```

It holds what was tested, when, against which provider and configuration,
over which range, which dataset and manifest resulted, and the quality and
validation verdicts. A record is **evidence only about the setup it was made
against**: it carries a fingerprint of the provider's configuration, so
pointing at a different endpoint or a different CSV source invalidates it
rather than being silently reused. A record from an older schema version, an
unreadable one, or one that did not pass is likewise never counted.

### The safety guard

A download longer than a calendar month is a historical acquisition and is
**refused** unless smoke, daily and monthly have all been verified for that
provider, symbol, timeframe and configuration:

```text
refused: refusing a historical acquisition: smoke, daily, monthly not
verified for this provider and configuration
```

Shorter downloads are unguarded — they are the stages themselves, and
refusing them would leave no way to produce the evidence.

`--force-unverified` overrides the refusal. It is never implicit: it must be
asked for, it prints a warning, and it is written into the dataset's
provenance as `unverified_override: true`, so data acquired without evidence
carries that fact permanently.

### Provenance

Every manifest records how its data came to exist: provider and
configuration, symbol, timeframe, requested and actual ranges, rows, quality
status, calendar, chunk size, rate limit, retry policy, acquisition time,
application version, and the verification the run relied on.

Credentials are never stored. A provider reports that a key is `set` or
`unset`, never its value.

## Data layout

Candles are partitioned by symbol, timeframe, year and month:

```text
data/processed/EUR_USD/timeframe=1min/year=2026/month=08/candles.parquet
data/manifests/EUR_USD/1min_20260814T120000Z_20260814T130000Z.json
data/quality/EUR_USD/1min_20260814T120000Z_20260814T130000Z.json
data/checkpoints/EUR_USD/1min_20260814T120000Z_20260814T130000Z.json
data/verification/csv/EUR_USD/1min/smoke.json
```

Everything below `data/` is generated and git-ignored.

A manifest records its partition files relative to the dataset root, so it
keeps resolving after the dataset is copied or moved, and it describes the
files covering its whole range — including on a resumed run that downloaded
nothing itself.

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
  "provider_retries": 0,
  "rate_limit_requests_per_second": 2.0,
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
- **Every guarantee is re-checkable offline.** `marketdata validate` re-applies
  each of the checks above to the stored files, so a dataset is trusted on its
  own evidence rather than on the word of the run that produced it.

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
- A chunk is retried whole. There is no partial-chunk recovery: a chunk whose
  retries are exhausted is re-fetched from its start on the next run.
- The rate limit is per process. Two downloads started separately do not
  share a limiter, so running several at once multiplies the request rate.
- Pacing is uniform: there is no adaptive slowdown that lowers the rate after
  a provider signals overload beyond honouring `Retry-After` on that request.

## Roadmap

See [`planner.md`](planner.md) for the full phase breakdown, per-phase
acceptance criteria, current blockers and the next milestone. In short: the
ingestion path through resumable chunked downloads, retries, error
classification and rate limiting is complete, as are offline dataset
validation and the staged acquisition workflow that gates a large download
on verified smaller ones; live provider verification is next and is blocked
by the development environment's egress policy; dataset acquisition, backtesting,
analytics and the UI are planned and not started.
