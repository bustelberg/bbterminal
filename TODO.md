# Open follow-ups — resume here

Running list of unfinished / offered-but-not-built work, newest context first.
Last updated **2026-08-11**. Delete items as they're done.

---

## ⚡ Analyse modal — memoized. The remaining cost is REAL work, and one number is unverified.

2026-08-11. Profiled one press of Analyse on BUS_Neutraal_FX: **212 database round trips, 103 of
them byte-identical repeats** (`airs_performance` x9, `airs_model_portfolio` x5, `asset_grid` x3,
the SP500 universe id x6). Nobody wrote that loop — a dozen collaborating loaders each correctly
fetch what they need, and the duplication exists only in their composition. Fixed with a
**per-request read memo** (`common/read_cache.py`, opened at the request boundary in
`compute_portfolio_analysis_async`), covering both transports: PostgREST GETs and direct COPY.
Measured: **212 → 109 round trips, 0 repeats left**, payloads equal to the uncached ones within
1e-9 across 6 portfolios. Pinned by `tests/test_read_cache.py`.

**⚠ 1. THE PRODUCTION GAIN IS ESTIMATED, NOT MEASURED.** Local wall went 3,436 → 2,949 ms
(interleaved medians, 14%) — but a local PostgREST call is ~5ms and prod is a network hop at
~50-80ms, so the 103 removed round trips are worth *seconds* there rather than the ~0.5s here.
Nobody has timed it against prod. The modal already logs `timings_ms` per phase to the console;
read that on a real load before quoting a number.

**⚠ 2. THE PAYLOAD IS NOT BYTE-REPRODUCIBLE, AND IT WASN'T BEFORE EITHER.** Two consecutive
UNCACHED runs of portfolio 1878 differ in the last ULP (`benchmark_pct` 31.472360860646393 vs
...386) — a float sum over rows Postgres returns in an unspecified order. Harmless at 2 dp, but it
means any future equality check here needs a tolerance, and an `ORDER BY` on those reads would be
the real fix.

**What is left, in order:** the phases now sum to the wall clock
(`composition_and_benchmark` 351ms · `book_holdings` 897 · `axes` 765 · `returns_and_benchmark`
1,903). The remaining 109 trips are distinct queries doing real work; the biggest single item is
**8 `asset_price` COPY loads (~1.4s)** over OVERLAPPING id sets and windows — not duplicates, so
the memo cannot touch them. Merging them into one load sliced per consumer is the next real win,
and it is a structural change across `_book_port_items` / `_basis_axes` / `_returns` /
`_benchmark_index`.

**Not done:** the memo is opt-in per endpoint. `/attribution`, the portfolios grid and the
benchmark endpoints have the same shape and would benefit; each needs its own `read_cache()` at
its request boundary.

---

## 🧬 clone-local-to-prod — FK cascade fixed 2026-08-11. The DRY RUN still cannot see it.

The clone died 47 tables in on `company` → `metric_data_company_id_fkey`. Step [5]'s comment
claimed "FK ON DELETE CASCADE/SET NULL cleans their dependents"; **8 edges in this schema are
`NO ACTION`** and do not (`company` ← metric_data / portfolio_weight / earnings_portfolio_member,
`currency` ← fx_rate + gurufocus_exchange, `country` ← gurufocus_exchange,
`gurufocus_exchange` ← company, `portfolio` ← portfolio_weight). Both delete sites now stage the
doomed PKs and walk the blocking edges depth-first (`Remove-RowsWithDependents`), sparing any
parent an **additive** table still points at. Verified by planning all 47 generated statements
against the local catalogue with `EXPLAIN` — nothing executed.

**⚠ 1. NOT YET RUN AGAINST PROD.** Every statement is planned-valid and the walk is proven on the
real FK graph, but no clone has completed with it. Next run: `./scripts/clone-local-to-prod.ps1`.

**⚠ 2. `-DryRun` DOES NOT REPORT THE CASCADE.** It compares row counts per table, so it says
"company: prod has 3 more" and nothing about the ~30k `metric_data` rows that go with them. That
is exactly the number worth seeing before pressing go. It also cannot predict a **spare** (a
parent kept because an additive child references it), which is the one case where the final
verify legitimately shows a surplus.

---

## ⏱ Benchmarks Refresh — the speed-up is DERIVED, not measured. And one bounded slice is left.

Built 2026-08-11 (`/management-dashboard` → Benchmarks → **Refresh**). The button now fills an
index end to end: prices for every constituent, then a **forced** GuruFocus refetch of every
constituent's fundamentals. Force had to defeat **two** caches — the `metric_data` sentinel (a
company loaded once was never selected again) and the Storage blob (`is_cache_fresh` keeps it
fresh for the data's own cadence + 50%, i.e. weeks past the quarter it is missing). Defeating one
gives a press that spends nothing and changes nothing. Pinned by `tests/test_fundamental_refetch.py`.

The price step then went from a serial loop with a hardcoded `time.sleep(0.4)` onto a pool
(`_PRICE_WORKERS`, 2× `YAHOO_CONCURRENCY`, capped at 8), the "what did we hold before?" read was
hoisted into one grouped COPY (`latest_close_by_analysis`), and the cap WRITES were parallelised
(the quotes were always batched at 100; storing them was ~490 serial round trips for the S&P).
Pinned by `tests/test_benchmark_price_pool.py`.

**⚠ 1. NOBODY HAS TIMED IT.** The "S&P ~10–15 min → ~3–4" figure is arithmetic off the pacing
constants (`YAHOO_RPS` 10/s, semaphore 4, the removed 0.4s sleep, ~4 round trips per constituent),
not a stopwatch. Time one AEX run (25 constituents, ~1 min, cheap) and one SP500 run before
quoting it to anyone. The interesting question is whether the governor or our database is now the
limit — if `extend_series`' COPY is, more workers will not help.

**⚠ 2. THE RESOLVE SLICE IS STILL 25 PER PRESS** (`_benchmark_fill._RESOLVE_PER_PRESS`), so a
fresh or heavily-unresolved index still reports "N still unresolved — press again". Deliberately
not widened: that path is `resolve()`, where an overloaded Yahoo returns an EMPTY search rather
than a 429 and the thin foreign listing wins (NVDA-on-Stuttgart, Alphabet-on-Vienna). The price
pool is safe **only** because `extend_series` asks about a symbol we already identified. If this
is widened, loop `process_slice` over this benchmark's ISINs — never a second resolver.

**Offered, not built:** the caps step still writes one row per constituent. A PostgREST `upsert`
of the batch was rejected on purpose — it becomes INSERT … ON CONFLICT, so a constituent with no
`asset_analysis` row would be CREATED from four cap columns, a junk row that then looks like an
instrument.

---

## 📋 AIRS Transacties — measured on ONE account. Two things still unverified.

Built 2026-08-05: `/portfolios` → expand an account → **Transactions** (the sheet, cached in
`airs_transactie_snapshot`) and **Total return** (the year, built from held + sold positions and
checked against AIRS's own `beleggingsresultaat`). `TRANS` is confirmed to be Transacties; the
columns are documented in `airs_transacties`'s docstring from a real download.

Measured on AITopSelectie OFF DYN: held 380,986.94 + realised 6,306.85 + sold-name income 0.00 =
**387,293.79** against the book's **387,293.75** — residual **€0.04** — and 38.729379% against
AIRS's own 38.729375%.

**✅ 1. `Res. YtD` vs `proceeds − Kostprijs` — RESOLVED, on real data (BUS_Offensief_Dyn).** The
first book measured (AITopSelectie) had `Res. voorg. jr.` = 0.00 on every row, so the two formulas
agreed exactly and it could not arbitrate. Bustelberg Offensief settles it — 12 of its 13 sold
names carry prior-year amounts (Novo Nordisk −24,866.94, Wolters Kluwer −20,819.13):

```
Res. YtD                 -28,656.47   -> total +69,792.94  =  +5.83%   AIRS: +5.83%  ✓
Res. voorg. jr.          -97,919.73
proceeds - Kostprijs    -126,576.20   -> total -28,126.79  =  -2.35%              ✗
identity: proceeds - cost == Res. YtD + Res. voorg. jr., to -0.00
```

The intuitive formula is **8pp out and the wrong sign**, and looks entirely plausible. Keep
`Res. YtD`. (The book's own YTD moves as it is re-scanned — these are 2026-08-05 mid-session; the
identity and the size of the error are what matter, not the exact totals.)

**⚠ 2. `Tt='D'` is uninterpreted.** One row: KLA-Tencor, 2026-06-12, 369 shares, every money column
`0.0`. KLA split 9:1 in 2026, so a corporate action is the obvious reading — and obvious is not
measured. It carries no money, so it is excluded from every total and **counted** (surfaced in the
panel). If a `D` ever arrives carrying a value, that count is what will show it.

**✅ The merged position ledger is built** (`airs_capital.py`, Analyse modal → "Every position this
year"). One row per instrument the book touched, held or sold, weighted by **average invested
capital** (Modified Dietz) — the only weight a sold position can carry, and the only one that
describes a book whose composition changed. Contributions sit on `beginvermogen` and sum to the
book's own YTD **exactly** (measured: 5.8267 vs 5.8267, 44.4624 vs 44.4624; residual 0.0000pp).

⚠ A 1-January weight was tried first and is WRONG: AITopSelectie's equities were worth EUR 40,319
on 1 Jan against a EUR 1m opening capital, because it began the year in cash and deployed on
5 January — a start-weighted table calls it 96% cash.

**Still approximate, and surfaced rather than hidden** (`capital_coverage_ratio`, measured
0.980 / 1.023):
- Modified Dietz ignores the price path *within* a position.
- A sold-out parcel's opening value is split proportionally by quantity between shares held at the
  open and shares bought during the year — AIRS does not publish its parcel matching.
- The de-restatement assumes `Beginwaarde ÷ quantity` is the 1-Jan price (linear restatement).

**Not yet done:**
- `start_gap_eur` in the /portfolios reconciliation is still the NET of two opposite effects. The
  ledger now separates them; that panel could read the ledger instead.
- A sold position has no sector, so it is absent from the composition bars and Brinson. Those
  report `realised_share_of_result_pct` instead. Classifying by name → `asset_execution` would
  close it, but a name match is exactly what `_airs_holding_isin` warns against.
- `kosten` is 0 on all 53 accounts, so whether costs sit inside `cumulatief_rendement` but outside
  `beleggingsresultaat` is untested. If a book ever charges them, the residual is where it shows.

---

## 🐞 GuruFocus RENAMED the financials sections — `/earnings` metric codes are drifting

Found 2026-07-13 while building the /asset-pipeline Revenue column. GuruFocus's
`financials` blob changed shape, and our Storage cache now holds **both**:

```
live API today   annuals.income_statement.Revenue        annuals.per_share_data_array.*
cached blobs     annuals["Income Statement"].Revenue     annuals["Per Share Data"].*
```

`ingest/earnings/financials.py::_parse_financials` derives each `metric_code` from the
**section name it happens to see**, so a company re-fetched TODAY writes
`annuals__income_statement__Revenue`, while every constant and every stored row says
`annuals__Income Statement__Revenue`. Nothing errors — the new rows simply land under
codes nobody queries, and the /earnings dashboard shows a company as having no data.

Blast radius: `/earnings` (its whole metric_data contract), `ANNUAL_CODE` /
`QUARTERLY_CODE` in `routers/_asset_dividends.py`, and `has_data` in the dividend
coverage map. NOT the asset-pipeline Div/share or Revenue charts — both read the raw
blob and already accept **both** spellings (`_asset_revenue._SECTIONS`).

Fix: normalise the section names in `_parse_financials` (map snake → the legacy Title
Case codes, or migrate the codes and backfill). Do it before the next earnings refresh
re-fetches anything, or the two schemas interleave inside one company's history.

---

## ⚠️ SESSION 2026-07-10 — NOTHING IS COMMITTED. START HERE.

Branch `dev`, working tree dirty: **19 modified + 19 new paths** (incl. this file and
`CLAUDE.md`). All of it is green — `1032` backend tests, `149` frontend, ruff/tsc/eslint
clean, `openapi.json` + `lib/api-types.ts` regenerated. Review and commit before doing
anything else, or a `git checkout` will destroy a day's work.

Re-verify everything in one go:

```bash
cd backend && SUPABASE_DB_URL= SUPABASE_URL= SUPABASE_SERVICE_KEY= PYTHONPATH=. uv run python -m pytest tests/ -q
cd backend && uv run ruff check .
cd frontend && npx tsc --noEmit && npm test -- --run
cd backend && uv run python scripts/dump_openapi.py && cd ../frontend && npm run gen:types   # must be no-ops
```

### What was built (suggested commit split)

1. **Golden master for the legacy momentum engine** — `tests/test_golden_rebalance.py`
   (34 tests, offline) + `scripts/capture_golden_rebalance.py` +
   `tests/fixtures/golden_rebalance_34*.npz` (13.7 MB, 2 fixtures).
   Replays strategy 34 through `run_current_portfolio` against frozen inputs and asserts
   exact holdings. Mutation-tested: catches `mean()`→`median()` sector aggregation, which
   **all 863 other tests miss**. The second fixture (trading-day `as_of`) is the only one
   that can observe the strict `<` cutoff.
2. **Phase 1 — `timeseries/` façade** (`load_series`, `to_panel`) + `common/pg.py`.
   Four bespoke loaders now delegate. Verified byte-identical against a pre-change baseline.
3. **Phase 2 (partial) — `signal_engine/`** (`registry.py`, `daily.py`, `context.py`).
   One declaration of all 19 signals; `momentum/signals.py` 660→174 lines. AlphaLab's
   Signal Lab can now also score the live `daily_asof` battery (`?include_daily=true`).
4. **GuruFocus API catalogue** — `docs/gurufocus_api.md`, `backend/gurufocus_api.json`,
   `scripts/gurufocus_catalog.py`, `tests/test_gurufocus_catalog.py`.
5. **Div/share column on `/asset-pipeline`** — `routers/_asset_dividends.py`,
   `AssetDividendModal.tsx`, `tests/test_asset_dividends.py`. Native|EUR dual chart,
   three cadences (annual / quarterly / **payments**), lazy fetch on open.
6. **OpenFIGI name-anchor fix** — `asset_pipeline/resolve.py` + `scripts/repoint_primary_listing.py`
   + `scripts/reresolve_asset_mismaps.py` + `tests/test_asset_resolve_anchor.py`. **This one
   changes resolution behavior for the whole pipeline** — commit it separately and read the
   diff.

### Local DB was mutated (dev only, intentional)

- `asset_execution` 1633 (NVIDIA) repointed `NVD.SG` → `NVDA`. Prices refetched.
- `fx_rate`: USD backfilled to 1999-01-04 (596 → 7,045 rows) by opening Apple's dividend chart.
- `metric_data`: dividend rows for a few companies (whitelisted, ~100 rows each).
- `gurufocus-raw` Storage: new `dividend.json` / `financials.json` blobs.
- Throwaway auth users created and deleted; only `reinier7175@gmail.com` +
  `reinier@bustelberg.nl` remain.

---

## 1. Do next — the 302-row primary-listing sweep (HIGH VALUE, NOT RUN)

`scripts/repoint_primary_listing.py` is written, tested, dry-run reviewed, and **only
applied to NVIDIA**. 302 equities are still on thin cross-listings.

```bash
cd backend && PYTHONPATH=. uv run python scripts/repoint_primary_listing.py           # review the diff
cd backend && PYTHONPATH=. uv run python scripts/repoint_primary_listing.py --apply   # ~2,100 Yahoo calls
```

- Detection is **ADV ÷ market cap** (`< 1e-5`; p5 is `1.07e-5`, median `4.6e-3`). NOT an
  exchange-code map (Vienna≠Prague trap) and NOT "ISIN country ≠ listing country" (that
  flags every deliberate ADR→ordinary mapping like `US7595091023`→`RELIANCE.NS`).
- Three gates before any rewrite: different symbol, `same_company()` identity holds,
  ≥2× liquidity gain. The gates work — Exxon's re-resolution proposed *another* Stuttgart
  line and was correctly rejected.
- [ ] Run the dry-run, read it, then `--apply`. **This rewrites the price panel under
      AlphaLab, Signal Lab and every saved universe.** Don't run it while a big ingest
      is competing for the Yahoo throttle.
- [ ] Then re-run `scripts/reresolve_asset_mismaps.py` (dry-run): its suspect filter was
      producing 636 suspects of which 555 were false; the real count is 81.

---

## 2. Engine unification — Phase 2 remainder, then Phase 3

Done: golden master (Phase 0), `timeseries/` façade (Phase 1), `signal_engine/` registry
+ `daily.py` port (Phase 2, most of it).

- [ ] **`asset_pipeline/signals.py` still has its own month-end battery.** It derives from
      the registry, but the two cadences remain separate implementations. Decide whether
      AlphaLab's 9 month-end signals stay, or collapse into the daily engine.
      **Do NOT merge `vol_trend_3m`** — measured spearman **0.58**, 29% sign disagreements,
      opposite signs on the same universe. `daily.mom_12_1` and `me.mom_12_1` ARE the same
      signal (spearman 0.996). Both facts encoded in `signal_engine.registry.PARITY` and
      regenerable via `scripts/signal_divergence.py`.
- [ ] **Phase 3 — `available_at` (knowledge date).** Neither engine has one, so any
      fundamental signal is look-ahead-biased by construction. Price: `available_at = obs_date`.
      Fundamentals: announcement date, or `period_end + lag`. This is the prerequisite for
      any EPS/valuation factor.
- [ ] Optional Phase 4: decide whether `/schedule` moves to Yahoo prices. **Different vendor
      ⇒ different holdings even with identical code.** Make it a deliberate, measured
      decision, never a side effect of a refactor.
- [ ] Cheap wins not yet taken: `alphalab.load_panel` loads FULL history (10.2s) and caches
      30 min, where a windowed load is **24× cheaper** (418ms). Signal compute is ~2% of a
      backtest run — never optimize it.

### Blind spots in the golden master (documented in its docstring)

- `MAX_STALENESS_DAYS` 30→10 is caught by **nothing**. Add a fixture where a company sits
  in the stale band.
- ETF overlay + cash sleeve aren't covered (strategy 34 has neither) — pinned only by
  `tests/test_portfolio_math.py`.

---

## 3. FX coverage — a latent backtest bug

`momentum/data/fx.py::load_fx_rates` does `.reindex(daily).ffill().bfill()`. The **bfill**
silently extends the earliest stored rate backwards to whatever `start_date` you ask for.
And `sync_fx_rates_to_db` only ever extends **forward** (reads stored max, fetches max+1) —
nothing in the codebase backfills earlier history.

Before this session, USD/CZK/GBP/JPY/CHF all started at **2024-03-07** (596 rows), while
ISK/THB/IDR reached back to 2000. A backtest starting 2015 with USD holdings would convert
every pre-2024 price at the 2024-03-07 rate.

- Not currently biting: scheduled strategies start 2025-05-02, and the backtest stream calls
  `sync_fx_rates_to_db` before loading. But `/backtest` lets you pick an earlier start.
- [ ] Backfill `fx_rate` for every actively-used currency (ECB history starts 1999-01-04).
      `routers/_asset_dividends.py::_backfill_fx_history` shows the pattern; it already fixed
      USD as a side effect.
- [ ] Consider making `load_fx_rates` return NaN before coverage instead of `bfill()`, and
      audit the callers.

---

## 4. Smaller follow-ups from this session

- [ ] **`docs/schema.md` is stale** — no `asset_*` tables, no `signal_engine`/`timeseries`.
- [ ] `ingest/earnings/financials.py` now takes `metric_codes=` (a whitelist). The earnings
      dashboard still persists **all 263 fields** — ~36,700 `metric_data` rows per company.
      Whitelisting it to `_DASHBOARD_METRIC_CODES` is a ~10× storage cut for zero behavior
      change. `metric_data` is 6.4 GB for 26M rows (2.3 GB heap, **4.1 GB index**) because
      the PK carries a `varchar metric_code`.
- [ ] **Andritz AG (`AT0000730007`) is stored on exchange `XPRA` (Prague)** — the Vienna≠Prague
      trap. Its dividends therefore render in **CZK**. `GET isin/AT0000730007` returns both
      `WBO:ANDR` (EUR) and `XPRA:ANDR` (CZK); `WBO` is correct. Fix at `company.exchange_id`.
- [ ] `/asset-pipeline` Div/share `Fetch` spends one GuruFocus call per company opened, with
      no per-day cap. Consider a cap or a bulk backfill if it gets used heavily.
- [ ] `signal-lab?include_daily=true` roughly doubles compute (31s → 110s on the 4,006-name
      universe). Defaults to `false`; the daily engine's per-entity loop is the bottleneck.
- [ ] `GET /api/asset-pipeline/dividends/{id}` **writes to `fx_rate`** as a side effect of a
      read (backfills the currency's history). Precedent exists (the backtest stream syncs FX
      before loading), but it's worth naming.

---
## 5. Per-company staleness flags — "stale price" / "stale volume"

**Goal:** every company whose latest price and/or volume data is non-recent
should be flagged **stale price** and/or **stale volume** (the two are
*independent* — a company can be fresh on one and stale on the other), instead
of one stuck name silently dragging a whole universe's "most-stale" line.

**Why (the MASI case, 2026-06-23):** Masimo (`MASI`, cid 5344 local) showed as
LEONTEQ's most-stale at 06-12 while everything else was 06-22. Verified against
the **live GuruFocus API**: `MASI` *and* `NAS:MASI` both return history ending
06-12 (last 179.95), and the `/summary` quote is also frozen (~06-13) — while
AAPL/MSFT/NVDA/TGB all return 06-22. So GuruFocus's **API** is frozen for this
one ticker even though their **website** shows newer prices. Our DB is correct;
we can't pull newer data. But one upstream-frozen stock makes the universe look
stale and makes "Refresh" look broken (it runs, re-fetches 06-12, nothing moves).

**What to build:**
- [ ] Compute per company the latest `target_date` for `close_price` and for
      `volume` (source `gurufocus`), compare to the **global latest** close/volume
      date (mirror the delisting sweep's "N trading days behind" idea in
      `ingest/delisting.py`, but **non-destructive**). Beyond the threshold →
      `stale_price_at` / `stale_volume_at` (two independent markers).
- [ ] Keep distinct from the existing markers: `delisted_at` (dead/acquired),
      `illiquid_at` (manual, rarely trades), `out_of_scope_at` (no GF coverage).
      These new flags are **automatic + per-metric** and mean "still listed &
      priceable but its GF feed has gone stale" (often upstream, e.g. MASI).
- [ ] Surface on `/companies` (a **STALE PRICE** / **STALE VOL** badge, threaded
      through `/api/companies` incl. the COPY path
      `momentum/data/_pg.py::load_companies_via_copy`) and on the `/schedule`
      coverage cards, so an upstream-frozen ticker reads as "stale feed", not
      "our refresh failed".
- [ ] Decide whether stale-flagged companies drop out of the universe
      "most-stale" coverage measure (like `illiquid_at` does) so a single frozen
      feed stops dominating the card. Auto-clear the flag once the feed catches up.
- [ ] (Optional) flag "GuruFocus API frozen for this ticker" specifically — when
      a fetch succeeds (200) but returns no rows newer than what we already have,
      distinct from a 404/ticker-drift (see §2).

---

## 6. Prod data fixes (do soon — local is fixed, prod isn't)

- [ ] **Push the illiquid migration to prod**: `npx supabase db push` →
      `supabase/migrations/20260615005000_company_illiquid.sql` (adds
      `company.illiquid_at`). The /schedule "Mark illiquid" button +
      `POST /api/admin/company-illiquid` + the price-coverage exclusion all need
      the column on prod.
- [ ] **Repoint Bank of New York Mellon ticker `BK → BNY` on prod.** GuruFocus
      renamed the listing (BNY Mellon rebrand). Locally fixed (cid 3327); on prod
      it's still `BK` → every refresh 404s AND the daily delisting sweep will
      **wrongly** mark it `delisted_at` (it's actively trading under BNY). Run the
      same `company.gurufocus_ticker` update against prod.
- [ ] **Re-apply the illiquid marks on prod**: Telecom Italia savings (MIL:TITR)
      via the button once deployed. Covestro (XTER:1COV) is already `delisted_at`
      on prod via the sweep (acquired late 2025) — optionally also mark illiquid.

## 7. Ticker-drift handling (systemic — BK exposed this class)

When GuruFocus renames a ticker, our stored `gurufocus_ticker` 404s, the company
silently stops updating (a 404 loads 0 rows, no error/flag), shows as the
coverage "oldest", and the DB-only delisting sweep can't tell it apart from a
real delisting.

- [ ] **"Fix ticker from GuruFocus" action** — when a company 404s, probe
      GuruFocus by name to find the renamed symbol and repoint
      `gurufocus_ticker` (one click, mirrors the existing "GF name" button →
      `POST /api/admin/gurufocus-company-name`). Surface on /companies and/or the
      /schedule coverage "Oldest" line.
- [ ] **Guard the delisting sweep** (`ingest/delisting.py`) — before stamping
      `delisted_at` on a stale company, probe its GuruFocus symbol; a
      renamed-but-trading listing should be flagged "needs ticker review"
      (`gurufocus_lookup_failed_at`?), not delisted.
- [ ] **Make 404s visible in the price refresh** — a 404 ("Stock not found") for
      a company that HAS prior data currently loads 0 rows silently. Add a
      counter/flag in `ingest/phases/prices.py` so ticker drift isn't invisible.

## 8. Offered UI follow-ups (not built)

- [ ] **/companies "ILLIQUID" badge** — surface `illiquid_at` companies (thread
      it through `/api/companies` incl. the COPY path
      `momentum/data/_pg.py::load_companies_via_copy`, + a badge like
      UNSUBSCRIBED/DUPE).
- [ ] **/schedule "Mark delisted" button** on the coverage **Oldest** line
      (sibling to the new "Mark illiquid" button) — for stale-but-dead listings.
- [ ] **Attribution matrix sector source** (/earnings portfolio mode) — default
      the "Sectors" dropdown to each basket's OWN membership sectors instead of
      always Leonteq (holdings outside Leonteq show "Unclassified").
- [ ] **Mobile-viewport e2e test** (375×667) — assert the nav drawer
      opens/closes + no horizontal overflow. Needs an auth stub so the sidebar
      renders under `E2E_BYPASS_AUTH` (the responsive/drawer work from this
      session is otherwise only structurally verified).

## 9. Data jobs

- [ ] **Re-run the market-cap backfill** — the earlier full run FAILED
      (~73% `market_cap_eur` / 82% `isin` coverage). `uv run python -u -m
      index_universe.backfill_market_cap` (also corrects names) when convenient.
      (The name-only backfill `index_universe.backfill_company_names` completed:
      0 renames — names already correct.)
- [ ] **Re-run the month-end full price refresh after fixing tickers** — the run
      this session showed "0 prices refreshed" partly because stale names like BK
      were 404ing (ticker drift). Re-run to confirm it pulls the long tail once
      tickers are corrected.

## 10. Verify before investing (possibly obsolete)

- [ ] **Earnings basket-aggregate caching** — earlier offer to precompute a
      per-frozen-universe aggregate to speed the ~66s `member-metrics` load.
      Likely **MOOT** after the /earnings single-switch redesign (stock mode =
      stock-vs-stock; portfolio mode = the Allocation×Selection matrix only,
      prices-only). Confirm nothing still triggers the heavy member-metrics
      aggregate before building this.

## 11. Long Equity benchmark performance (2026-08-06)

Context: selecting ACWI as the benchmark on the Long Equity tab fires ~11 requests
(one per card) over 1,514 constituents. All three items below are **shipped and
ruff/tsc/eslint clean**; what is missing is production measurement, not code.

- [ ] **Confirm `SUPABASE_DB_URL` is set on Railway.** Without it the new COPY path
      in `routers/_earnings_pg.py` is INERT and everything silently keeps using the
      PostgREST pager (`common.pg` logs one warning at startup saying so). This is
      the single highest-value check — the COPY win scales with round-trip latency,
      which is ~2ms locally and 50–200ms to Supabase cloud.
- [ ] **Re-benchmark the tab INTERLEAVED, not in blocks.** Local block-measurements
      contradicted each other (COPY+dedupe 6.04s vs dedupe-only 4.55s), which is the
      ~15% run-to-run spread CLAUDE.md already warns about. A 4-way interleaved
      benchmark (PostgREST / +dedupe / COPY / COPY+dedupe, ≥3 rounds) is the only way
      to get a trustworthy number. **Do this on prod, or at least with a warm DB** —
      the local figures understate COPY by design.

### What was built (all in place, no follow-up needed unless the above says otherwise)

1. **Response cache** — `routers/_blend_cache.py`, `@cached_blend` on 13 endpoints.
   Verified: 1.99s → 6.8ms on a repeat; portfolio/holdings requests are NEVER cached;
   `openapi.json` byte-identical; `invalidate()` wired to both fundamentals ingest jobs
   and fires only when data was actually written.
2. **COPY transport** — `routers/_earnings_pg.py::rows_by_company_via_copy`, tried first
   in `_rows_by_company` with the pager as fallback. Verified **identical output**
   (`dict == dict`, 1,512 companies / 16,336 rows) and 3.2× on the raw read locally.
3. **Metric-read dedupe** — `cached_metric_read` with THREAD-based single-flight (the
   reads run inside `asyncio.to_thread`). The tab issues 27 metric reads of which only
   18 are distinct (`sbc` ×5, `fcf` ×4, `revenue` ×3). 60s TTL, 32-entry cap: it exists
   to dedupe a concurrent burst, not to persist — persistence is the response cache's job.

### Measured dead ends — do NOT redo these

- **Collapsing the ~11 card requests into one endpoint is NOT worth it.** The only work
  all 13 endpoints share is `_load_and_expand_members` = **0.100s** (1.3s of 16.6s = 8%);
  the rest is each endpoint reading its own metric codes. And the cards already run
  concurrently (16.6s of work in 11.1s), so serialising them inside one handler would make
  cold wall-clock *worse* unless it re-implements the same fan-out internally.
  ⚠ An earlier claim that this was worth ~72% came from subtracting `_blend_inputs` (1.08s)
  from `*-inputs` endpoints that **never call it** — they call `_load_and_expand_members`.
- **Truncating a benchmark to the top 90% of cap does NOT work for these charts.** Measured
  on ACWI 2025: levels and ratios are 7–53% off (revenue sum −11%, net margin +11%,
  net income/share +53%), because the dropped tail is a systematically different business
  mix (EM/financials/industrials, lower margin), not "the same companies, smaller".
  Single-year cap-weighted *growth* IS accurate (−0.11pp ACWI, +0.60pp SP500), and a
  properly chained per-year version lands at +1.15% (SP500) / −5.42% (ACWI) over a decade —
  so it is only defensible for growth-based series, and only if labelled on the chart.
