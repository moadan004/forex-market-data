# Project Planner

**This file is the authoritative development roadmap for `forex-market-data`.**
When this planner and any other document disagree, the planner wins. When the
planner and the code disagree, the code wins and the planner is corrected —
the planner describes the implementation, never the other way around.

`README.md` documents what is implemented today. This file documents what is
implemented, what is next, and why.

## Status legend

| Mark | Meaning |
| --- | --- |
| ✅ | Completed — present in the code and covered by tests |
| 🟡 | In progress — partially implemented |
| ⬜ | Planned — not started |
| 🔴 | Blocked — cannot proceed for a stated reason |

Nothing is marked ✅ because a document says so. Every ✅ below was checked
against `src/` and `tests/` on the current branch.

Verification is tracked separately from implementation, because the two
diverge here:

| Term | Meaning |
| --- | --- |
| Implemented + unit tested | code exists and its units are covered by the suite |
| Implemented + offline integration tested | exercised end to end through the production path against local fixtures, no network |
| Mock verified | provider behaviour exercised through a mock HTTP transport, never against Dukascopy |
| Real provider verified | a real Dukascopy response was received and checked — **nothing carries this yet** |
| Environment blocked | cannot be attempted from here; the egress policy denies the host |

These are deliberately five levels rather than "tested / not tested". The
ingestion path is at *offline integration tested*; the Dukascopy provider is
at *mock verified*; nothing anywhere is at *real provider verified*.

## Status at a glance

| Phase | Title | Status |
| --- | --- | --- |
| 1 | Environment and repository | ✅ |
| 2 | Market-data foundation | ✅ |
| 3 | Data pipeline | ✅ |
| 4 | Production historical-data ingestion | ✅ |
| 5 | Historical dataset acquisition | 🔴 |
| 6 | Data-quality verification | 🟡 |
| 7 | Backtesting engine | ⬜ |
| 8 | Strategies | ⬜ |
| 9 | Analytics | ⬜ |
| 10 | Robustness | ⬜ |
| 11 | API and UI | ⬜ |
| 12 | Production | ⬜ |

Test suite: **683 passing**. Ruff format and check: clean.

---

## Phase 1 — Environment and repository

**Goal.** A reproducible development environment and a repository that can be
linted and tested by anyone who clones it.

| Item | Status | Evidence |
| --- | --- | --- |
| WSL2 + Ubuntu | ✅ | Developer workstation setup; not verifiable from the repository itself |
| Python 3.13 | ✅ | `.python-version`, `requires-python = ">=3.13"` |
| uv | ✅ | `uv.lock`, `uv_build` backend in `pyproject.toml` |
| Git | ✅ | Repository history |
| GitHub | ✅ | `origin` → `github.com/moadan004/forex-market-data` |
| Repository structure | ✅ | `src/marketdata/` package layout, `tests/`, `configs/`, `docs/`, `scripts/` |
| Testing | ✅ | `pytest`, `pytest-cov` in the dev group |
| Linting | ✅ | `ruff` in the dev group |

**Acceptance criteria**

- `uv sync` produces a working environment from a clean clone.
- `uv run pytest` passes.
- `uv run ruff format .` and `uv run ruff check .` are clean.
- `uv run marketdata --help` works via the installed entry point.
- No generated data is tracked in git.

---

## Phase 2 — Market-data foundation

**Goal.** A canonical in-memory representation of a candle, and a provider
abstraction that keeps vendor details out of everything downstream.

| Item | Status | Module |
| --- | --- | --- |
| Canonical `Candle` model | ✅ | `models/candle.py` |
| Provider interface | ✅ | `providers/base.py` |
| Dukascopy provider | ✅ | `providers/dukascopy.py` — implemented, **mock-tested only** |
| CSV provider (offline) | ✅ | `providers/csv.py` — implemented and tested offline against real files |
| UTC normalization | ✅ | `normalization/timestamps.py` |
| OHLC validation | ✅ | `validation/candles.py` |
| Deduplication | ✅ | `validation/candles.py` |

**Acceptance criteria**

- Candles are immutable, use `Decimal` prices, and reject naive datetimes.
- ✅ A second provider can be added by implementing `MarketDataProvider`
  alone, with no change downstream. Demonstrated: `CsvMarketDataProvider`
  drives the entire pipeline — chunking, checkpoints, resume, merging,
  storage, manifests, validation and quality reporting — and the only
  production change needed was a CLI switch to choose it.
- Provider-specific logic (endpoints, pagination, payload shape, instrument
  identifiers) appears only under `providers/`.
- Validation states the invariant it rejected, not just that it failed.
- Deduplication is deterministic and independent of input order.

---

## Phase 3 — Data pipeline

**Goal.** A single command that turns a request into a stored, described,
reproducible dataset.

| Item | Status | Module |
| --- | --- | --- |
| Parquet storage | ✅ | `storage/parquet.py` |
| Manifest | ✅ | `storage/manifest.py` |
| Quality report | ✅ | `quality/report.py`, `quality/gaps.py` |
| Download pipeline | ✅ | `downloader/pipeline.py` |
| CLI | ✅ | `cli.py` |

**Acceptance criteria**

- The pipeline applies every stage explicitly: fetch, normalize, restrict to
  range, validate, deduplicate, validate again, store, manifest, report.
- Files use a fixed Arrow schema, so partitions written at different times
  remain mutually readable.
- The quality report is machine-readable JSON and its counts reconcile.
- The CLI prints the requested range, actual range, per-stage row counts,
  quality status and the path of every artifact written.
- A provider failure exits non-zero without a traceback.

---

## Phase 4 — Production historical-data ingestion

**Goal.** Make a multi-year download survivable: chunked, resumable, and
honest about market closures.

| Item | Status | Module / note |
| --- | --- | --- |
| Session calendar | ✅ | `calendar/base.py`, `calendar/forex.py` |
| Chunk planner | ✅ | `downloader/chunks.py` |
| Checkpoint / resume | ✅ | `downloader/checkpoint.py` |
| Partition merging | ✅ | `storage/parquet.py` |
| Manifest consistency after resume | ✅ | `downloader/pipeline.py`; manifests list the files covering their range, not just what a run wrote |
| Retry / backoff | ✅ | `providers/retry.py`, wired into the Dukascopy request layer |
| Provider error classification | ✅ | `providers/errors.py` |
| Rate limiting | ✅ | `providers/rate_limit.py`, applied to every request including retries |

**Acceptance criteria**

- Download can resume after interruption.
- Re-running a chunk is idempotent.
- No duplicate timestamps.
- UTC timestamps only.
- Weekend closures are not reported as missing data.
- Parquet can be read back as one logical dataset.
- Manifest and quality report agree with stored rows.
- A failing chunk does not corrupt the chunks that succeeded.
- Chunk boundaries are deterministic and do not depend on the requested
  start.
- A transient provider failure is retried with exponential, bounded backoff
  instead of failing the chunk.
- A permanent error fails fast instead of being retried.
- Retry counts, the last error and whether the final failure was transient
  are recorded in the checkpoint and survive a restart.
- A long run stays within a configured request rate without manual pacing,
  and retries are paced by the same limiter rather than exempt from it.

Every criterion is met and tested. Phase 4 is complete; Phase 5 is gated only
by live provider access.

---

## Phase 5 — Historical dataset acquisition

**Status: 🔴 Blocked.** No candle has ever been downloaded from the live
provider. See *Current blockers*.

**Target dataset** (not yet acquired):

| Property | Target |
| --- | --- |
| History | At least 5 years, preferably 7+ |
| Timeframe | 1-minute |
| Symbols | EUR/USD, GBP/USD, USD/JPY, XAU/USD |

**Staged acquisition.** Each stage must pass before the next is attempted.
No stage may be skipped — this is now enforced by the code, not by
discipline.

| Stage | Range | Tooling | Run against Dukascopy |
| --- | --- | --- | --- |
| `smoke` | 1 hour | ✅ implemented, offline tested | 🔴 |
| `daily` | 1 trading day | ✅ implemented, offline tested | 🔴 |
| `monthly` | 1 calendar month | ✅ implemented, offline tested | 🔴 |
| `historical` | arbitrary, incl. 5–7 years | ✅ implemented, offline tested | 🔴 |

**Readiness machinery** — ✅ implemented and offline tested
(`src/marketdata/verification/`):

| Capability | Module | Note |
| --- | --- | --- |
| Provider preflight | `preflight.py` | reachability, symbol, timeframe, candles, parsing, UTC, OHLC, pagination stitching |
| Stage definitions | `stages.py` | smoke → daily → monthly → historical |
| Quality thresholds | `status.py` | PASS / WARN / FAIL / BLOCKED; structural defects never tolerated |
| Verification records | `records.py` | durable evidence, invalidated by a changed provider configuration |
| Safety guard | `guard.py` | a download longer than a month is refused without evidence |
| Stage runner | `runner.py` | runs the production pipeline, then re-validates from disk |
| Provenance | `storage/manifest.py` | how a dataset came to exist; never records a credential |

`BLOCKED` is tracked separately from `FAIL` throughout: a policy denial is
not evidence that anything is broken, and must never be mistaken for a
verification.

**Acceptance criteria**

- ✅ A stage cannot run before its smaller stages have been verified, and a
  historical download is refused without all three.
- ✅ Verification means re-inspecting the stored dataset, not a zero exit
  code.
- 🟡 An override exists and is explicit. It is recorded in the data's
  provenance on the `marketdata download` path; `verify-stage` does not pass
  `verification` or `unverified_override` into the pipeline, so a stage run
  under `--force-unverified` leaves no trace of that in the dataset's
  provenance. The verification record itself still carries the outcome.
- Each stage's quality report has status `ok`, or every deviation is
  explained and accepted before proceeding.
- Chunk sizing and runtime are recorded at stage 4 and used to estimate
  stage 5 before it is started.
- Stage 5 runs per symbol, and a failure in one symbol does not affect
  another.
- The final dataset's manifest records the provider, the requested range and
  the retained row count for every symbol.

---

## Phase 6 — Data-quality verification

**Goal.** Prove the acquired dataset is trustworthy — on its own evidence,
not on the word of the run that produced it.

The checks and the tool that applies them are implemented and tested. What
remains is running them against a real acquired dataset, which Phase 5 gates.

| Check | Mechanism | Applied offline | Verified on real data |
| --- | --- | --- | --- |
| Coverage | `expected_candles` vs stored | ✅ | 🔴 |
| Missing intervals | `quality/gaps.py` | ✅ | 🔴 |
| Duplicate candles | dedup, merge, and a read-back count | ✅ | 🔴 |
| Invalid OHLC | `validation/candles.py` | ✅ | 🔴 |
| Timestamp ordering | read-back ordering check | ✅ | 🔴 |
| Weekend / market closures | `calendar/forex.py` | ✅ | 🔴 |
| Out-of-range observations | `restrict_to_range` on stored rows | ✅ | 🔴 |
| Timezone correctness | UTC enforced at model, pipeline and storage | ✅ | 🔴 |
| Partition integrity | fixed Arrow schema, atomic writes | ✅ | 🔴 |
| Schema consistency across partitions | per-file schema comparison | ✅ | 🔴 |
| Partition / file coverage | `ParquetStorage.partition_files` | ✅ | 🔴 |
| Manifest agreement | claimed rows and files vs stored | ✅ | 🔴 |
| Read-back validation | partition-at-a-time streaming scan | ✅ | 🔴 |
| Misfiled partition rows | month claimed by the path vs the rows in it | ✅ | 🔴 |
| Scales to the target dataset | bounded by partition, not dataset, size | ✅ | 🔴 |
| Quality status classification | `quality/dataset.py` | ✅ | 🔴 |
| Cross-provider comparison | `verification/comparison.py`, `marketdata compare` | ✅ | 🔴 |
| Comparison scales to the target dataset | partition-bounded reads, capped gap samples | ✅ | 🔴 |

**Memory-bounded validation and quality reporting** — ✅ implemented and
tested. Both paths now walk a dataset one partition at a time. A partition is
read, judged, folded into running counters and dropped before the next is
opened, so peak residency is one calendar month rather than the dataset. The
target dataset — five to seven years of one-minute candles, several million
rows — is the reason: a validator that materializes the range it judges
cannot be run on the dataset it exists for.

`ParquetStorage.partition_groups` is the single traversal every streaming
consumer shares, so validation, quality reporting and cross-provider
comparison all walk a dataset the same way rather than each deriving the
layout again. `TradingGapScanner` is the incremental form of
`find_missing_trading_intervals`, and the whole-list function is now a thin
wrapper over it, so the two agree by construction. The range a dataset is
judged against is resolved from Parquet footer statistics, so establishing
the extent of a multi-year dataset costs a footer read per file and no rows
at all.

**What survives between partitions** is bounded and deliberate: the open
trading sessions of the window, the last timestamp seen, the running
counters, and at most twenty retained samples of each finding. Correctness
across the boundary is preserved — a gap spanning two partitions is found
exactly as one inside a single partition is, duplicates are counted a month
at a time because the layout puts every row of a month in one file, and rows
that break that layout are counted as `misfiled_rows` and reconciled against
their home partition rather than assumed away.

**Report equivalence** is tested rather than asserted: each report is
recomputed the old whole-list way and the two are required to agree, across
six dataset shapes and both calendars. Against the CSV fixture dataset the
before/after JSON is field-for-field identical, with four fields added
(`missing_interval_count`, `missing_intervals_truncated`,
`duplicate_timestamps_truncated`, `misfiled_rows`) and none changed or
removed.

**One behaviour did change, deliberately.** `unordered_rows` was previously
always zero: it was computed over a list the reader had already sorted, so
it could never fire. Streaming reads partitions in path order, so a dataset
whose partitions overlap in time now reports the disorder it actually has.

**Cross-provider comparison tool** — ✅ implemented and tested.
`marketdata compare` and `compare_datasets()` judge two *stored* datasets
against each other for the same symbol, timeframe and UTC range. It is not
coupled to any provider: it reads Parquet, so any two datasets the pipeline
produced can be compared, whoever produced them.

It reports candles compared, matching and mismatching counts, candles present
on only one side in each direction, duplicate timestamps, OHLC and volume
disagreements with the largest observed difference and the timestamp it
occurred at, the actual range each side covers, and gaps unique to either
side. Tolerances are relative and expressed as `Decimal`; prices and volumes
are never compared for exact equality. The verdict is PASS / WARN / FAIL /
BLOCKED, with BLOCKED reserved for having nothing to compare.

The comparison is read-only and works a month at a time, so comparing years
of one-minute data holds one month of each side in memory rather than the
whole history. It never reconciles a disagreement, never prefers one side,
and never writes to either dataset.

**Gap differences are bounded as well.** Two feeds that disagree everywhere
produce a gap per candle, so gaps are resolved and counted in the batch that
found them and fed into a capped sample buffer, rather than accumulated and
truncated afterwards — the pattern that made the report largest for exactly
the comparison least able to afford it. Resolving per batch is exact because
a gap is closed by a timestamp: two sides holding the identical gap hold the
identical closing timestamp, which falls in the same month and so is read in
the same batch.

`gaps_left`, `gaps_right`, `gaps_only_left` and `gaps_only_right` stay exact
however many gaps exist; `gap_differences` retains at most twenty — the same
ones a full sort would have put first — and `gap_differences_truncated`, plus
a line in the human-readable output, says when there were more. The whole
report was diffed field-for-field against the pre-change implementation for a
small disagreement and for a 724-gap one spanning six partitions: identical
in both. This is a scaling property, not a fixed RAM figure.

**Three distinct things, deliberately not conflated:**

| Kind of verification | What it proves | Status |
| --- | --- | --- |
| Offline cross-provider comparison | two stored datasets agree, or exactly how they differ | ✅ implemented and tested |
| Mocked provider testing | our client code handles the responses we *assume* a provider gives | ✅ for Dukascopy, via a mock transport |
| Real Dukascopy verification | a real Dukascopy response was received and checked | 🔴 never achieved — see Current Blockers |

Running `marketdata compare` between a CSV dataset and a Dukascopy dataset
would be a real cross-provider verification. It has never been run, because
no Dukascopy dataset exists: the comparison tool is offline-complete, and the
Dukascopy side of it remains blocked by the egress policy.

**Comparison records.** A comparison can be written to a durable record
(`--record-root`) holding both provider identities and configurations, both
dataset fingerprints, the symbol, timeframe, range, thresholds and the whole
report. The record expires by itself: changing either dataset by a single
byte, either provider configuration, the symbol, the timeframe, the range,
the thresholds, or the record schema version all invalidate it. No credential
is ever stored — a provider reports a key as `set` or `unset`, never by value.

**Comparison thresholds and why they are what they are.** Documented on
`ComparisonThresholds`, and configurable on the command line:

| Threshold | Default | Reasoning |
| --- | --- | --- |
| `price_tolerance` | `0.0001` relative | About one pip on EUR/USD. Two feeds aggregate different liquidity and one may quote bid where another quotes mid, so a spread-sized difference is normal; a stale feed or a wrong scale factor is far larger. |
| `volume_tolerance` | `0.05` relative | FX has no consolidated tape. "Volume" is a tick count over one provider's own feed, so two providers legitimately disagree. |
| `price_mismatch_warn_ratio` / `fail_ratio` | `0.0` / `0.001` | One disagreeing candle in a month is noise; the same count in an hour is a broken feed, so the bound is a ratio of compared candles. |
| `volume_mismatch_warn_ratio` / `fail_ratio` | `0.0` / `1.0` | A ratio cannot exceed 1, so the fail bound is unreachable by construction: volume differences are always reported and never fail. Lower it when comparing two feeds that genuinely should agree. |
| `missing_warn_ratio` / `fail_ratio` | `0.0` / `0.01` | Reuses the acquisition defaults so one policy governs "how much missing data is tolerable" across the project. |

These defaults are a **starting policy, not an empirical finding.** No real
Dukascopy data has ever been observed, so the true disagreement between two
live FX feeds is unmeasured here. Recalibrate from the first real
cross-provider comparison and record the reasoning.

**Dataset validation tool** — ✅ implemented and tested. `marketdata validate`
and `validate_dataset()` inspect a stored dataset without contacting a
provider, reporting every row above plus the gaps, closures and problems
found. It survives damaged datasets: an unreadable or incompatible partition
is a finding, not a crash. Exit code `1` on anything but `ok`, so it can gate
a pipeline.

**Acceptance criteria**

- ✅ A stored dataset can be validated offline, by anyone holding the files.
- ✅ Zero duplicate timestamps per symbol and timeframe, checked on read-back.
- ✅ Zero invalid OHLC rows in stored data, checked on read-back.
- ✅ The dataset reads back as one logical PyArrow dataset per symbol, and a
  partition whose schema would break that is named.
- ✅ A manifest that no longer matches what is stored is reported.
- 🔴 Every reported missing interval on a real dataset is explained: a genuine
  provider gap, a market closure, or a holiday. Needs real data.
- ✅ Any two stored datasets can be compared offline, within documented and
  configurable tolerances, producing a durable record that expires when
  anything it depended on changes.
- ✅ Validating a dataset does not require holding it. Memory is bounded by
  partition size, so the checks above can run against the multi-year target
  dataset and not only against fixtures.
- ✅ Comparing two datasets does not require holding their disagreements
  either. Gap samples are capped while the counts stay exact, so a comparison
  of two badly diverging multi-year datasets no longer produces a report that
  grows with the divergence.
- 🔴 A second provider agrees with **Dukascopy** on a sampled range. The
  comparison tool is built and tested, but one side of the comparison does
  not exist: no Dukascopy dataset has ever been acquired. Blocked by the
  egress policy, not by missing code.

---

## Phase 7 — Backtesting engine

**Status: ⬜ Planned.** No backtesting code exists in the repository.

| Item | Status |
| --- | --- |
| Engine | ⬜ |
| Trade simulator | ⬜ |
| Position sizing | ⬜ |
| Stop loss / take profit | ⬜ |
| Spread | ⬜ |
| Slippage | ⬜ |
| Commission | ⬜ |
| Risk management | ⬜ |

**Acceptance criteria**

- No look-ahead bias: a decision at time `t` may only read candles that
  closed at or before `t`.
- Deterministic results: the same inputs produce byte-identical output.
- Configurable spread, slippage and commission.
- Reproducible dataset version: every result records the dataset manifest it
  ran against.
- Intrabar fill assumptions are stated explicitly rather than implied.

---

## Phase 8 — Strategies

**Status: ⬜ Planned.**

| Order | Strategy | Status |
| --- | --- | --- |
| 1 | Asian Range Liquidity Sweep Reversal | ⬜ |
| 2 | CRT (Candle Range Theory) | ⬜ |
| 3 | FVG (Fair Value Gap) | ⬜ |
| 4 | Order Block | ⬜ |
| 5 | Market Structure | ⬜ |
| 6 | Trend Continuation | ⬜ |

**Acceptance criteria**

- Every strategy is expressed against the engine's interface, with no direct
  file or provider access.
- Session-dependent logic (the Asian range in particular) derives its
  boundaries from the calendar layer, not hard-coded hours.
- Each strategy has unit tests over fixed candle fixtures with known outcomes.
- Parameters are declared, not embedded as literals.

---

## Phase 9 — Analytics

**Status: ⬜ Planned.**

| Metric | Status |
| --- | --- |
| Win rate | ⬜ |
| Profit factor | ⬜ |
| Expectancy | ⬜ |
| R-multiple | ⬜ |
| Maximum drawdown | ⬜ |
| Equity curve | ⬜ |
| Consecutive wins / losses | ⬜ |
| Session analysis | ⬜ |
| Day-of-week analysis | ⬜ |
| Long / short analysis | ⬜ |

**Acceptance criteria**

- Every metric is computed from the trade ledger, not re-derived from prices.
- Metrics are reproducible from a stored backtest result without a re-run.
- Session and day-of-week breakdowns use UTC with the session calendar.
- Sample size accompanies every rate, so a 100% win rate over three trades
  cannot be read as a result.

---

## Phase 10 — Robustness

**Status: ⬜ Planned.**

| Item | Status |
| --- | --- |
| In-sample / out-of-sample split | ⬜ |
| Walk-forward testing | ⬜ |
| Monte Carlo | ⬜ |
| Slippage sensitivity | ⬜ |
| Spread sensitivity | ⬜ |
| Different data providers | ⬜ |
| Overfitting detection | ⬜ |

**Acceptance criteria**

- The out-of-sample period is chosen before optimisation and never reused.
- Walk-forward windows are declared up front and applied without exception.
- Results are reported across a range of spread and slippage assumptions, not
  at a single favourable setting.
- The number of parameter combinations tried is recorded alongside results.

---

## Phase 11 — API and UI

**Status: ⬜ Planned.**

| Item | Status |
| --- | --- |
| FastAPI backend | ⬜ |
| Next.js frontend | ⬜ |
| TypeScript | ⬜ |
| TradingView Lightweight Charts | ⬜ |
| Backtest configuration | ⬜ |
| Trade visualization | ⬜ |
| Analytics dashboard | ⬜ |

**Acceptance criteria**

- The API exposes the existing pipeline and engine; no business logic is
  duplicated in the web layer.
- A backtest is reproducible from the configuration the UI submitted.
- Charts render trades against the exact candles the backtest used.

---

## Phase 12 — Production

**Status: ⬜ Planned.**

| Item | Status |
| --- | --- |
| Paper trading | ⬜ |
| Live-data ingestion | ⬜ |
| Monitoring | ⬜ |
| Versioned datasets | ⬜ |
| Reproducible backtests | ⬜ |

**Acceptance criteria**

- Paper trading uses the same strategy code as backtesting.
- Live ingestion reuses the provider and validation layers.
- Every dataset version is identifiable and a backtest records which it used.
- A stored backtest can be re-run months later and reproduce its result.

---

## Production-readiness audit

Performed against `e723c69` to answer one question: *if this repository were
moved to a network-permitted environment tomorrow, could it safely acquire
and validate 5–7 years of one-minute data without changing the
implementation?*

The whole acquisition path was traced in the code and exercised offline
through the production pipeline against a synthetic three-month, 131,039-row
CSV feed spanning three partitions.

### What the audit confirmed works

| Property | Evidence |
| --- | --- |
| Staged guard refuses an unverified multi-month range | a 3-month `download` was refused until smoke, daily and monthly were verified |
| Resume re-fetches only what is missing | one failed chunk of three; the resumed run asked the provider for that chunk alone |
| Repeat runs are idempotent | a third run made zero provider requests and left the Parquet byte-identical |
| A killed process recovers | a chunk left `running` was re-run and introduced no duplicates |
| Manifests track the stored dataset, not the run | a narrow re-run over fuller data still produced an agreeing manifest |
| A changed provider configuration invalidates verification | pointing the CSV provider at another source re-blocked the historical download |
| Validation memory is flat in dataset size | 1 / 6 / 12 months → 299 / 336 / 344 MiB peak RSS for 44k / 262k / 527k candles |
| A policy denial cannot become a verification | a 403 on every chunk produced a `blocked` record that the guard refuses, no Parquet, and an honest zero-row manifest |

### The defect it found

**A completed chunk whose stored data had gone was skipped, and the run
reported success over the hole.** The checkpoint was the only authority on
whether a chunk was done; nothing reconciled it against the files. Deleting
one partition of a three-month dataset and re-running produced
`3/3 completed, 0 failed, 3 already done`, exit code 0, and a dataset missing
30,000 candles. Recovery required `--no-resume`, i.e. re-downloading the
entire request, because deleting the file alone changed nothing.

For a multi-day, 84-partition acquisition this was the difference between a
resumable run and a silently incomplete one. Fixed on
`claude/checkpoint-data-reconciliation`: the checkpoint is now reconciled
against the stored data before any chunk is skipped, from Parquet footers
only, and re-acquired chunks are reported. A chunk that legitimately stored
zero rows stays complete, so a closed market is not re-downloaded forever.

### Findings accepted rather than fixed

- **A corrupt partition still stops the run with a raw PyArrow message.**
  `_finish` reads every partition in range to build the quality report, so an
  unreadable file raises out of `pipeline.run()` and no manifest is written.
  With the reconciliation fix the recovery is one step — `validate` names the
  file, delete it, re-run, and the month is re-acquired — where before the
  fix deleting it accomplished nothing. Worth its own milestone: a truthful
  quality report for a partially unreadable dataset is a design question, not
  a patch.
- **A permanent, run-wide provider failure does not stop the run.** A 403 on
  the first chunk does not prevent the remaining chunks from being attempted;
  each fails the same way. Bounded and recoverable, but 84 futile requests on
  a seven-year run. `--strict` stops on the first failure but also stops on a
  single invalid candle, which is too blunt for a long acquisition.
- **Candles inside market-closed periods are never challenged.** The calendar
  is used to excuse gaps but never to question unexpected rows. A 24×7 feed
  validated against the forex calendar stored 527,040 rows against 374,160
  expected — 41% excess — and reported `ok` with no problems. Both numbers are
  in the report; nothing compares them.
- **`live` in a verification record means "no stub transport was injected",
  not "real network I/O happened".** `verify-stage --provider csv` writes
  `live: true`. It cannot create false evidence about Dukascopy — records are
  keyed by provider and configuration fingerprint — but the field does not
  mean what its name suggests.
- **`ChunkCheckpoint.files` stores absolute paths**, so it does not survive a
  moved dataset. Nothing reads it today; the reconciliation deliberately asks
  storage instead.
- **No disk-space preflight.** Seven years of one-minute data is roughly
  100 MB per symbol as stored, so this is a small risk, but nothing checks.
- **A very large `--chunk-size` is unguarded.** One chunk is held in memory
  several times over during ingestion; `12months` of one-minute data would be
  hundreds of MB. The `1month` default is safe.

### Readiness verdict

| Scale | Ready? | Reason |
| --- | --- | --- |
| 1 day | 🔴 environment only | Machinery proven offline; needs one real Dukascopy response |
| 1 month | 🔴 environment only | Same; multi-chunk, multi-partition behaviour proven offline |
| 1 year | 🔴 environment only | Same; memory and resume behaviour measured and flat |
| 5–7 years | 🔴 environment only | Same, with the checkpoint reconciliation fix merged |

No scale is blocked by missing code once the reconciliation fix lands. Every
scale is blocked by the same thing: not one real Dukascopy candle has ever
been received.

---

## Current Blockers

### Environment blockers

**Live Dukascopy access**

- **Status:** 🔴 BLOCKED.
- **Symptom:** every request returns `403 Forbidden`; the egress proxy
  reports `connect_rejected — gateway answered 403 to CONNECT (policy
  denial)` for `freeserv.dukascopy.com:443`. `www.dukascopy.com` and
  `datafeed.dukascopy.com` are blocked identically.
- **Cause:** network / egress policy in the development environment.
- **Action:** do **not** bypass the restriction. Run the smoke test from a
  network-permitted environment, or have the host allow the domain.
- **Impact:** blocks Phase 5 entirely and the real-data column of Phase 6.
  Every test in the suite runs against a mock HTTP transport, so the
  provider's live behaviour — response shape, pagination, rate limits,
  history depth — is unconfirmed.

### Implementation blockers

**None outstanding.** One was found by the production-readiness audit and is
fixed on `claude/checkpoint-data-reconciliation`; see *Production-readiness
audit* below. The ingestion path is otherwise feature-complete for
large-scale acquisition: chunked, resumable, merging safely, retrying
transient failures with bounded backoff, and pacing every request including
retries. What remains before Phase 5 is verification against the live
provider, which is an environment blocker rather than missing code.

### Known inconsistencies

- `pydantic-settings` is declared as a dependency in `pyproject.toml` but is
  not imported anywhere in `src/`. It is intended for configuration; drop it
  if configuration lands another way. (`tenacity` was in the same position
  and is now used by `providers/retry.py`.)

### Live status

One `provider-check` was attempted against the real Dukascopy endpoint on
the current branch. It returned:

```text
BLOCKED  reachable: refused by policy or credentials:
         Dukascopy request failed for instrumentList: 403 Forbidden (proxy rejected)
Result:  BLOCKED
```

This is **not** a verification and is not recorded as one. No verification
record exists for any Dukascopy stage, and the safety guard therefore
refuses a historical Dukascopy download — which is the intended behaviour.

### Offline coverage

The ingestion path is exercised end to end without a network by the CSV
provider, over deterministic fixtures holding duplicates, out-of-order rows,
invalid OHLC relationships, out-of-range rows, gaps, several months,
timezone offsets and malformed input. This proves the *pipeline*. It says
nothing about Dukascopy, whose behaviour remains mock-tested only.

### Accepted limitations

- No holiday list ships with the project. Market holidays are reported as
  missing trading candles unless holidays are passed to `ForexCalendar`.
- The forex weekly boundary uses fixed UTC times and does not track US
  daylight saving; 21:00–22:00 UTC on Friday is treated as closed year-round.
- Gap detection is skipped for timeframes without a constant cadence
  (`1day_eet`).
- Validation and quality reporting are bounded by partition size rather than
  dataset size, and a partition is one calendar month — roughly 44,000
  one-minute candles. That is a scaling property, not a fixed RAM figure.
- Cross-partition duplicate detection is exact while every row sits in the
  partition for its own month. Rows that do not are counted as
  `misfiled_rows` and reconciled against their home partition, up to 1000
  distinct timestamps; past that the report says the count is incomplete
  rather than under-reporting silently.
- Gap detection assumes chronological arrival, which the month-partitioned
  layout guarantees. A dataset that breaks it reports non-zero
  `unordered_rows` and `misfiled_rows` and grades `invalid`, so its gap
  breakdown is advisory.
- A chunk is retried whole; there is no partial-chunk recovery.
- Cross-provider comparison matches gaps by exact interval equality, so two
  partially overlapping gaps are reported as unique to each side rather than
  as one shared gap that differs in length. The missing-candle counts, which
  drive the verdict, are exact either way.
- A comparison report retains at most twenty gap differences and twenty field
  differences. The counts beside them are exact and the truncation is
  reported, but a report of a large disagreement is a summary of it, not a
  full listing.
- Comparison fingerprints hash every partition's bytes. That is deliberate —
  a fingerprint that can miss a change is not evidence — but fingerprinting a
  multi-year dataset is I/O bound.
- Comparison assumes both datasets use this project's month-partitioned
  layout. Comparing a dataset written by something else needs it converted
  first.
- A dataset's provider identity comes from the manifests beside it. With no
  manifest root, both sides are compared as provider `unknown`, which is
  honest but weaker evidence.

---

## Next Milestone

In priority order. Items 1–3 are the remaining Phase 4 work; items 4–7 are
the staged acquisition of Phase 5.

| # | Milestone | Status | Done when |
| --- | --- | --- | --- |
| 1 | **Retry / backoff** | ✅ done | A transient provider failure is retried with exponential backoff and a bounded attempt count; retries are recorded in the checkpoint; tests cover success-after-retry and exhaustion. |
| 2 | **Rate limiting** | ✅ done | Requests are paced by a configurable limit; a long run cannot exceed it; the limit is a CLI option with a documented default. |
| 3 | **Live provider verification** | 🔴 next | Run `marketdata provider-check` and `verify-stage --stage smoke` against the real endpoint. The tooling is built and offline tested; only network access is missing. The one-hour smoke test returns real candles from Dukascopy and the response matches the mocked assumptions, or the provider is corrected and regression tests are added. Blocked by egress policy. |
| 4 | **One-day acquisition** | ⬜ | A full trading day of EUR/USD 1-minute data is stored with a quality report of `ok`. |
| 5 | **One-month acquisition** | ⬜ | A calendar month is stored across multiple chunks; weekends appear as closures, not gaps. |
| 6 | **One-year acquisition** | ⬜ | A year completes, survives at least one deliberate interruption and resume, and runtime is recorded. |
| 7 | **5–7 year acquisition** | ⬜ | The target dataset is acquired per symbol and passes Phase 6 verification. |

Rate limiting defaults to **2 requests/second**, applied to every outbound
request including retries. It does not replace retry/backoff: backoff decides
when it is worth trying again, the limiter decides how fast requests may
leave at all.

**Provider error classification** (Phase 4) was delivered with milestone 1:
retry logic cannot be written correctly without knowing which failures are
transient. Transient failures (timeouts, connection loss, 408, 425, 429, 5xx)
are retried with exponential backoff; permanent ones (400, 401, 403, other
4xx, proxy rejections, unusable payloads) fail on the first answer.

Do not begin milestone 4 or later until milestone 3 has actually succeeded
against the live provider. Mocked integration tests are not a substitute.

**Offline work that does not wait on milestone 3.** Cross-provider comparison
and memory-bounded validation (both Phase 6) are complete and required no
network. The comparison between a CSV
dataset and a Dukascopy dataset — the actual cross-provider verification — is
one command away and will run the moment a Dukascopy dataset exists:

```bash
marketdata compare \
  --left  data/dukascopy/processed --left-manifest-root  data/dukascopy/manifests \
  --right data/csv/processed       --right-manifest-root data/csv/manifests \
  --symbol EUR/USD --timeframe 1min --record-root data/verification/comparisons
```
