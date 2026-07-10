# Open follow-ups — resume here

Running list of unfinished / offered-but-not-built work, newest context first.
Last updated **2026-07-10**. Delete items as they're done.

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
