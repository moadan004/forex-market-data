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
| Implemented and tested | code exists and is covered by the suite |
| Tested with mocks | provider behaviour exercised through a mock HTTP transport, never against Dukascopy |
| Verified against real Dukascopy | a real response was received and checked — **nothing carries this yet** |
| Blocked by environment | cannot be attempted from here; the egress policy denies the host |

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

Test suite: **343 passing**. Ruff format and check: clean.

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
| Dukascopy provider | ✅ | `providers/dukascopy.py` |
| UTC normalization | ✅ | `normalization/timestamps.py` |
| OHLC validation | ✅ | `validation/candles.py` |
| Deduplication | ✅ | `validation/candles.py` |

**Acceptance criteria**

- Candles are immutable, use `Decimal` prices, and reject naive datetimes.
- A second provider can be added by implementing `MarketDataProvider` alone,
  with no change downstream.
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

**Staged acquisition.** Each stage must pass its quality report before the
next is attempted. No stage may be skipped.

| Stage | Range | Status | Purpose |
| --- | --- | --- | --- |
| 1 | 1 hour | 🔴 | Confirm live connectivity, response shape, timestamp alignment |
| 2 | 1 day | ⬜ | Confirm a full session, including the daily boundary |
| 3 | 1 month | ⬜ | Confirm weekend handling and multi-chunk merging |
| 4 | 1 year | ⬜ | Confirm resume behaviour, rate limits and runtime at scale |
| 5 | 5–7 years | ⬜ | Acquire the target dataset, one symbol at a time |

**Acceptance criteria**

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
| Read-back validation | `ParquetStorage.read_table` / `read_candles` | ✅ | 🔴 |
| Quality status classification | `quality/dataset.py` | ✅ | 🔴 |
| Cross-provider validation | ⬜ only one provider exists | ⬜ | ⬜ |

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
- ⬜ A second provider agrees with Dukascopy on a sampled range, within a
  documented tolerance.

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

**None.** The ingestion path is feature-complete for large-scale acquisition:
chunked, resumable, merging safely, retrying transient failures with bounded
backoff, and pacing every request including retries. What remains before
Phase 5 is verification against the live provider, which is an environment
blocker rather than missing code.

### Known inconsistencies

- `pydantic-settings` is declared as a dependency in `pyproject.toml` but is
  not imported anywhere in `src/`. It is intended for configuration; drop it
  if configuration lands another way. (`tenacity` was in the same position
  and is now used by `providers/retry.py`.)

### Accepted limitations

- No holiday list ships with the project. Market holidays are reported as
  missing trading candles unless holidays are passed to `ForexCalendar`.
- The forex weekly boundary uses fixed UTC times and does not track US
  daylight saving; 21:00–22:00 UTC on Friday is treated as closed year-round.
- Gap detection is skipped for timeframes without a constant cadence
  (`1day_eet`).
- Quality reporting reads every stored timestamp in the requested range into
  memory — a few million values for seven years of one-minute data.
- A chunk is retried whole; there is no partial-chunk recovery.

---

## Next Milestone

In priority order. Items 1–3 are the remaining Phase 4 work; items 4–7 are
the staged acquisition of Phase 5.

| # | Milestone | Status | Done when |
| --- | --- | --- | --- |
| 1 | **Retry / backoff** | ✅ done | A transient provider failure is retried with exponential backoff and a bounded attempt count; retries are recorded in the checkpoint; tests cover success-after-retry and exhaustion. |
| 2 | **Rate limiting** | ✅ done | Requests are paced by a configurable limit; a long run cannot exceed it; the limit is a CLI option with a documented default. |
| 3 | **Live provider verification** | 🔴 next | The one-hour smoke test returns real candles from Dukascopy and the response matches the mocked assumptions, or the provider is corrected and regression tests are added. Blocked by egress policy. |
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
