# BBTerminal

A financial data terminal for wealth management. Analyses stocks using data from LongEquity reports and index universes (S&P 500, ACWI), enriched with price/volume data from GuruFocus. Includes a momentum portfolio backtester and a live "current picks" view.

## Architecture

- **Frontend**: Next.js 16 (App Router) on Vercel — `frontend/`. All components are client components (`'use client'`).
- **Backend**: FastAPI (Python, `uv`) on Railway — `backend/`.
- **Database**: Supabase (Postgres) — schema in `supabase/migrations/`, full ERD in [`docs/schema.md`](docs/schema.md).
- **Auth**: Supabase Auth (email/password).
- **Supabase**: one hosted prod project + a local Supabase for dev. Local is the working dataset; the old hosted dev project no longer exists.

## Running locally

Prereqs: Docker Desktop (local Supabase), Node + npm, Python + uv.

1. `npx supabase start` — boots all Supabase services locally (Studio `:54323`, REST `:54321`, DB `:54322`, Mailpit `:54324`). First run pulls images.
2. Create `.env.local` files (both gitignored). Run `npx supabase status` for the local keys:
   - `backend/.env.local`: `SUPABASE_URL=http://127.0.0.1:54321`, `SUPABASE_SERVICE_KEY=<secret key>`.
   - `frontend/.env.local`: `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_ANON_KEY` (publishable key), `NEXT_PUBLIC_API_URL=http://localhost:8000`, `NEXT_PUBLIC_ALLOWED_EMAILS=<your email>`.
3. Run: `cd backend && uv run uvicorn main:app --reload --port 8000` and `cd frontend && npm run dev` (frontend `:3000`, backend `:8000`).

Useful: `npx supabase status` (URLs/keys), `db reset` (wipe + re-migrate + seed — **ask first, see memory**), `migration new <name>`, `db diff`, `db push` (→ prod), `stop`.

**Env precedence**: backend loads `.env` (prod defaults) then `.env.local` with `override=True`; on Railway `.env.local` is absent so only env vars apply. Frontend follows Next.js convention (`.env.local` overrides `.env`); Vercel uses dashboard env vars.

---

## Verifying changes

Local checks before committing (git hooks run a subset — see **Git hooks**):

- **Frontend** (`cd frontend`): `npx tsc --noEmit` (typecheck; also pre-push) · `npm test` (vitest, `*.test.ts` colocated) · `npm run e2e` (Playwright, production build) · `npx eslint <files>` (pre-commit auto-fixes staged).
- **Backend** (`cd backend`): `uv run pytest tests/` (momentum-engine units) · `uv run ruff check .` (pre-commit auto-fixes staged). After changing a route/Pydantic model, regenerate the API contract — see **API contract pipeline**.

**CI** (`.github/workflows/ci.yml`) gates every push. The key job is **`backend-stack-smoke`**: boots bare Postgres + PostgREST + the real uvicorn backend, applies migrations + `supabase/ci_seed.sql`, probes every safe GET for 2xx, asserts the seeded fixture surfaces (company count, longequity snapshot dates, latest-price-date), and **drives a real momentum backtest** (`POST /api/momentum/backtest` over seeded ACWI, asserting ≥1 period with holdings + a numeric return) — exercising DB load → signals → scoring → selection → SSE. Three real traps:
  - supabase-py addresses PostgREST at `{SUPABASE_URL}/rest/v1/*` (Kong gateway path), so the job runs a tiny nginx **`/rest/v1` proxy** in front of bare PostgREST. Without it *every* backend DB call 404s (and PostgREST's empty `{}` error body crashes postgrest-py's parser → 500).
  - When reproducing locally, **move `backend/.env.local` aside first** — it loads `override=True` and silently points the backend at your local Supabase instead of the throwaway test DB.
  - Admin-gated mutations call `supabase.auth.get_user` against GoTrue, which CI doesn't run — so the proxy **stubs `/auth/v1/user` as an admin user**. Any `Authorization: Bearer …` passes. CI-only.

  When you add a DB-backed endpoint, extend `supabase/ci_seed.sql` + the assertions to cover it.

---

## Backend (`backend/`)

**Entry point**: `main.py` (~110 lines) — thin bootstrap: constructs `FastAPI()`, attaches CORS + the API auth-gate middleware, mounts each domain router, registers APScheduler. All endpoints live under `routers/`. Full endpoint list: `main.py` + `backend/openapi.json` (the source of truth — don't catalog endpoints here).

**Top-level modules**: `deps.py` (Supabase client factory + env loading — routers import from here, never `main`) · `scheduler.py` (in-process APScheduler ticks) · `portfolio.py` (parses AIRS Excel, YTD returns in EUR + local) · `airs_scanner.py` (Playwright AirSPMS scraper) · `fx_rates.py` (ECB/Yahoo FX → `fx_rate` table).

**`routers/`** — one file per domain (flat: `admin`, `airs`, `auth`, `benchmarks`, `companies`, `earnings`, `exchange_fees`, `fx`, `indicators`, `ingest_runs`, `leonteq`, `longequity`, `scheduled_strategies`, `system`, `universe_templates`). Sub-packages: `momentum/` (`signals`, `backtest_crud`, `current_picks`, `backtest_stream/` SSE), `universe/` (`derive`, `derived_metrics`, `labels`, `screening`), `index_universe/` (`acwi`, `sp500`). `_auth_middleware.py` is the **API auth gate** (`enforce_api_auth`): EVERY `/api/*` request needs a valid Supabase JWT, role-gated — admins get everything; non-admins only the API behind their pages (`/api/companies`, `/api/earnings`, `/api/airs`, `/api/usage` reads + `/api/portfolios/parse` & earnings-refresh writes); anonymous → 401. Public tier (no auth): `/api/health`, `/api/hello`, the `*/cron` endpoints (self-check `X-Cron-Secret`). `/api/auth/*` is self-auth (the endpoint verifies its own token; admin user-mgmt calls `_require_admin`). Tokens are verified via `routers.auth.verify_token` (GoTrue `get_user`, cached 60s for polling). **All frontend API calls must attach the JWT — use `lib/apiFetch.ts` (and `runSSE` for SSE; never `EventSource`, which can't send the header).** `ingest_runs.py` is now just the HTTP layer (router + endpoints + `kick_off_refresh`/`_spawn_ingest` dispatch); the pipeline itself lives in `ingest/phases/` (see below).

**`ingest/`** — LongEquity + GuruFocus pipeline: `acquire` (download reports) → `flatten` (grouped Excel headers → flat DF) → `extend_primary` (primary exchange) → `transformation` → `load_into_supabase`; `resolve_tickers` (OpenFIGI), `prices` (daily closes + volumes from GuruFocus, cached in Storage; `_retry_transient` for timeout/5xx on Storage + `metric_data.upsert`), `staleness`, `api_usage`, `prune_companies`. **`ingest/phases/`** — the scheduled-refresh pipeline, split one module per phase so each is unit-testable in isolation: `runlog` (run-row tracking: `_Throttle`, `_create_run`, `_update_run`), `acquisition`, `templates`, `prune` (prune + dedupe), `prices` (+ company loaders), `momentum` (+ daily MTD), and `pipeline` (the three orchestrators — `_run_pipeline_sync`, `_run_daily_mtd_pipeline_sync`, `_run_daily_template_refresh_pipeline_sync`). `routers/ingest_runs.py` imports the orchestrators from here.

**`universe/`** — criteria-driven screening: `criteria` (definitions), `screen` (apply + store derived universes), `derived_metrics` (store metrics in `metric_data` source `derived`).

**`index_universe/`** — index reconstruction: `sp500` (Wikipedia history + OpenFIGI), `acwi` (iShares holdings + MSCI announcements), `templates/` (self-updating canonical universes — `ACWITemplate`, `LeonteqTemplate`; `_cache.py` LRU+TTL membership cache).

**`momentum/`** — backtester engine:
- `data.py` — bulk loaders (`load_universe`, `load_all_prices`, `load_all_volumes`), FX conversion, currency lookups. Queries batched in chunks of `IN_CHUNK_SIZE` (=200, `deps.py`) company IDs (Cloudflare 502 guard).
- `signals.py` — 5 price signals (mom_12_1, mom_6m, volatility_adjusted_return_6m, drawdown_from_recent_high_pct, above_200ma) + 2 volume (vol_20d_vs_60d, vol_trend_3m), each tagged `"group"` price/volume. Pre-indexed `dict[int, pd.Series]`. **Strict `<` cutoff** on `as_of_date` (never see the bar we trade). **30-day staleness guard** drops stale tickers.
- `scoring.py` — per-category 0-100 min-max normalized score, combined via adjustable category weights into `momentum_score`. `random_select` for the random baseline.
- `backtest.py` — three runners: `run_backtest` (monthly rebalance loop), `run_multi_trial_backtest` (N random runs, mean ± std), `run_current_portfolio` ("hold today" + per-trading-day daily picks with MTD return + turnover).

**`tests/`** — pytest (228 tests). Momentum engine (`test_signals`, `test_scoring`, five `test_backtest_*`) **plus** an ingest-data-logic suite added 2026-06-04:
- **Pure data-routing / dedupe** (no DB): `test_exchange_map` (the iShares↔GuruFocus resolution where the listing-misroute incidents live — Vienna≠Prague, override remap/rename/unavailable, HKSE pad, + the 10 historically-broken companies pinned as regression cases), `test_gurufocus_url`, `test_dedupe_canonical` (`canonical_ticker`/`canonical_name`/`exchange_priority`/`pick_winner`).
- **Ingest transforms** (pandas): `test_resolve_tickers` (incl. OpenFIGI `_best_match` primary-listing bias), `test_flatten_headers`, `test_transformation_helpers`, `test_extend_primary`.
- **Phase-level, against an in-memory fake Supabase** (`tests/_fake_supabase.py` — reusable; extend it for more phases): `test_phase_prices_loaders` (`_load_all_companies` stale-first + `_collect_held_companies`), `test_dedupe_match` (`find_canonical_match` two-bucket detector).

Routers + the prune/template/momentum *phase orchestrators* still lack unit tests — covered at HTTP level by `backend-stack-smoke`; the fake-Supabase fixture is the on-ramp to closing the rest. Run `uv run pytest tests/`.

**Non-obvious endpoint behavior** (everything else: `openapi.json`):
- `POST /api/momentum/backtest` (SSE) modes: `mode="backtest"` (default; sets `db_only=True` — never lazy-refreshes), `selection_mode="random"` + `n_trials>1` (random multi-trial), `mode="current_portfolio"` (current picks — short-circuits to cached snapshot for `(strategy_hash, current month)`; `force_recompute=true` bypasses; cache miss persists `current_picks_snapshot` + one `current_picks_day` per trading day; payload always includes `daily_picks_history`).
- `POST /api/momentum/current-picks/cron` + `POST /api/ingest/scheduled-refresh/cron` — `X-Cron-Secret`-protected; force `force_recompute=true` / `triggered_by='auto'`.
- `GET /api/universe-templates/{key}/membership` — strong `ETag` + `If-None-Match` → 304, served from the in-process cache.
- `POST /api/scheduled-strategies` — `frequency` ∈ daily/weekly/monthly/bimonthly/quarterly; `config` is a full BacktestRequest; `next_due_at` = next Tuesday 02:00 UTC tick.
- `/api/admin/*` (external scripts, e.g. IBKR rebalancer) — require a Supabase JWT with `app_metadata.role == 'admin'`. Portfolio (`/portfolio/latest`, `/portfolio/{id}`), monitoring (`/runs/latest`, `/pipeline-runs`, `/data-freshness`), and go/no-go (`/health` with strict + loose, `/sanity-check`). Full usage + `bbterminal_client.py` source live in the in-app `/documentation` page.

**Env vars** (`.env`, overridden by `.env.local`): `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `GURUFOCUS_BASE_URL`, `GURUFOCUS_API_KEY`, `BROKER_USERNAME`/`BROKER_PASSWORD` (AirSPMS Playwright), `CRON_SECRET`.

---

## Frontend (`frontend/`)

**Pages** (App Router; routes map 1:1 to a component in `app/components/` unless noted). Non-obvious notes only:
- `/backtest` (renamed from `/momentum` 2026-05-19) — variant-sweep backtester + saved runs. (The in-page "Current Picks" / saved-snapshot UI was removed 2026-06-03; current-picks is now produced only by the scheduled pipeline + cron, surfaced on `/schedule` and the admin API — the backend `mode=current_portfolio` path is untouched.) Universe dropdown lists only template-managed universes (currently ACWI); on select, start-date defaults to the template's `earliest_date` (ACWI: 2002-01), end-date to latest close-price date. URL is general so more strategies can land without a rename.
- `/acwi` — top: canonical ACWI universe (template-managed) with date scrubber + searchable membership + "Refresh now"; bottom: live iShares holdings + MSCI announcement + net-additions explorers (reconstruction diagnostics). No manual "Save universe" — the pipeline refreshes it.
- `/fees` — per-exchange one-way fees + broker-support toggle (sticky Save, no auto-save). Unsupported exchanges are dropped from the backtest universe entirely (filter in `backtest_stream/stream.py` right after `load_universe`). Backtest stats render `gross (net)` via a trade-aware fee model (buy fee on first appearance, sell fee when not rolled forward, open period never pays sell); net computed client-side in `momentum/feeStats.ts` so fee edits update parentheticals without re-running.
- `/schedule` — scheduled pipeline UI. Top: scheduled-strategies list (expand → per-strategy run history → holdings via `MonthlyHoldingsTable`). Middle: weekly/monthly job cards + "Run now". Bottom: recent runs (five-dot phase pip, expand → per-template diff + searchable membership + per-strategy holdings).
- `/api` — admin-only interactive endpoint explorer (uses the live Supabase session as Bearer). `/documentation` — admin-only external-script reference (full `bbterminal_client.py`, curl/PowerShell quick-start, endpoint table).
- `/login`, `/set-password` — auth; redirect to `/` on success. `frontend/proxy.ts` enforces auth on non-public routes and bounces authed users off `/login`.
- Others (orientation): `/` welcome · `/longequity-universe` · `/airs-portfolio` · `/companies` · `/benchmarks` · `/earnings` · `/universe` (criteria screener) · `/sp500` (S&P 500 index history + freeze-a-copy) · `/fx-rates` · `/request_gurufocus`.

**Component notes** (non-obvious):
- `LongEquityUniverse.tsx` — **nothing fetches on mount**; empty until **Load** or **Run ingest**.
- `MomentumBacktester.tsx` — **being decomposed** into `app/components/momentum/`. State/orchestration in hooks (`useBacktestConfig`, `useBacktestRun`, `useVariantSelection`, `useSectorEtfs`); presentational sub-components (`DateRangeRow`, `StrategyModeSelect`, `RandomParamsInputs`, `SignalWeightSliders`, `RunControls`, `VariantsPanel`). **When adding config/variants features, add/extend a hook or sub-component — don't grow `MomentumBacktester.tsx`.** Covered by `/backtest` e2e; verify with `npm run e2e`. (Un-extracted bulk: saved-runs / current-picks dropdown handlers + `loadBacktest` / `saveVariantsBundle`.)
- **Decomposed god-components (2026-06-04)** — each is now a thin orchestrator (≤~115 lines) over a `app/components/<x>/` subfolder following the same hooks+subcomponents pattern. **When extending these pages, add/extend a hook or section component — don't regrow the orchestrator.**
  - `Schedule.tsx` (75) → `schedule/`: `useScheduledStrategies` hook; `timeline.ts` (`runToTimelineProps`/`PIPELINE_STEPS`), `types.ts`; cards `PipelineActivityCard`, `TemplateUniversesCard` (+ `TemplateRow`, `TemplateRecentChanges`), `ScheduledStrategiesCard`, `StrategyConfigDetail`. External importers (`DailyMtdRefreshCard`, `ScheduledStrategyDetail`, `ScheduleRunDetail`) pull types/`runToTimelineProps`/`StrategyConfigDetail` from `schedule/`.
  - `AcwiUniverse.tsx` (108) → `acwi/`: `useAcwiData` hook (holdings + announcements/SSE detail stream + net-additions + derived memos); section components `FetchProgressBanner`, `BreakdownCards`, `ConstituentChangesSummary`, `NetAdditionsTable`, `AnnouncementsTable`, `OtherCountryCodedTable`, `HoldingsTable`, `FeasibleUniverseTable`, `AdditionTimelineTable`. Each table owns its own search/sort UI state.
  - `CompanyManager.tsx` (115) → `company/`: `useCompanies` (data + Add/Edit/Delete mutations + options) + `useCompanyFilters` (search/filter/sort) hooks; `MultiSelectFilter`, `CompaniesToolbar`, `CompanyTable` (+ `AddRow`/`EditRow`/`CompanyRow`), `VerifyAddModal`. Covered by `/companies` e2e; verify with `npm run e2e`.
  - `UniverseScreener.tsx` (32) → `universe/`: `useUniverses` controller hook (criteria/specs/saved universes + rename/delete mutations + base/derived grouping); `filterConfig.ts` helpers; `CriteriaCard`, `SavedUniverses`, `UniverseCard`, `TightenPanel` (derive SSE), `MonthlySparkline` (the only recharts importer here), `SectorBreakdown`, `Stat`, `InfoTip`.

**Stores** (`frontend/lib/stores/`): lightweight reactive `createStore`. `momentum.ts` holds SSE-driven backtest + current portfolio state.

---

## Database schema

Full ERD: [`docs/schema.md`](docs/schema.md). Key tables: `company`, `metric_data` (time-series: prices, volumes, derived), `universe` + `universe_membership`, `backtest_run`, `benchmark` + `benchmark_price`, `gurufocus_exchange` + `country` + `currency` + `fx_rate`, `airs_performance`.

Non-obvious relationships:
- `current_picks_snapshot` — one row per current-picks compute (locked-at-start holdings + current-month `daily_picks` blob, tagged `strategy_hash`); pipeline rows also carry `ingest_run_id` + `backtest_run_id` FKs (→ /schedule per-strategy history is a clean JOIN).
- `current_picks_day` — per-day row keyed `(strategy_hash, target_date)`; backs the cross-month "Daily picks history" + the cache lookup.
- `universe.template_key` — UNIQUE on template-managed canonical universes (one row per `UniverseTemplate`); NULL for user criteria universes.
- `ingest_run` — per-pipeline-run audit; phase tracking + `templates_summary` + `momentum_summary` JSONB arrays.
- `scheduled_strategy` — one saved backtest pinned to the momentum phase (`{backtest_run_id, enabled}`).
- `exchange_fee` — per-exchange one-way `fee_bps` + `is_broker_supported`.

---

## UI Design System

Single fixed theme: **"Azure Blanc"** (LIGHT) — porcelain-white surfaces, deep-navy ink, a sky-blue accent, over a faint azure gradient-mesh background; frosted-white chrome (sidebar + floating menus), *solid* white data surfaces so dense tables/numbers stay crisp, soft azure button glow. No theme switcher — the whole palette + treatment lives in `frontend/app/globals.css`'s `@theme` block + the few "Glass treatment" rules under it; charts mirror it in `lib/chartTheme.ts` (light grid, white tooltips). **Everything routes through these `@theme` design tokens** — never inline hex or raw Tailwind colour names (`gray-*`, `white`, `indigo-*`, …). Codemods tokenized ~250 surface literals + ~1,000 named-colour classes + ~2,000 grey/white classes. To re-skin, edit token values in `globals.css` (+ `chartTheme.ts`); nothing else. **Polarity note:** this is a light theme — `fg-*` runs dark→light (`fg-strong` = darkest ink); `neutral-*` are mid-slate RGBs used at low alpha (`border-neutral-800/40`) so soft borders still read on white; accent **400** is the on-white text/link blue (darker than the **500** brand), **600** is the button fill (white text); `overlay` is navy ink (hover wash darkens on white).
- **Surfaces** (7): `bg-page` (porcelain base) / `bg-sidebar` + `bg-popover` (translucent white → frosted by the `backdrop-filter` rule) / `bg-card` + `bg-card-alt` (derived-universe tint) + `bg-elevated` + `bg-inset` (all **solid** white). `bg-card/-card-alt/-elevated` get a layered soft drop-shadow for depth; `body` carries the faint azure radial gradient mesh.
- **Semantic ramps**: `accent-*` (brand — blue→cyan, buttons/active nav/links/rings), `pos-*` (positive returns, →emerald), `neg-*` (negative returns/errors, →rose), `warn-*` (warnings, →amber). pos/neg/warn alias Tailwind ramps; accent is a direct blue ramp.
- **Text**: `text-fg-strong` (headings) · `text-fg` (body) · `text-fg-soft`/`-muted`/`-subtle`/`-faint`/`-dim` (decreasing emphasis). **Neutral** (borders/dividers/controls): `border-neutral-800` (default), `-700`/`-600`/`-500` etc. **Washes**: `hover:bg-overlay/5` (hover/active wash) · `bg-scrim/60` (modal backdrop). Opacity modifiers work everywhere (`bg-page/40`, `bg-pos-500/15`, `border-neutral-800/40`).
- **Style presets** (build-time knobs, not user-facing): `data-radius="sharp|round"` + `data-density="compact|comfortable"` on `<html>` reshape corner radius + overall scale (em-based) with zero component changes.
- **Charts** read `lib/chartTheme.ts` (JS hex, dark-tuned) — they look fine on the navy theme but don't follow the tokens (known follow-up to make `chartTheme` read CSS vars).
- **Type**: Geist Sans for UI; Geist Mono only for numeric values.
- **Components**: cards `bg-card rounded-xl border border-neutral-800/40`; tables in card containers, `text-sm`, `py-2.5` rows; primary button `bg-accent-600 hover:bg-accent-500`, ghost `hover:bg-overlay/5`; inputs `bg-page border border-neutral-700 rounded-lg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30`; row hover `hover:bg-overlay/[0.02]`, actions fade in `opacity-0 group-hover:opacity-100`; errors `bg-neg-500/10 border border-neg-500/20 rounded-lg`.
- **Charts**: recharts/SVG can't use the `@theme` utilities, so chart colours are centralized in `frontend/lib/chartTheme.ts` (the chart-side mirror of the ramps — `accent`/`pos`/`neg`/`warn` + chrome greys + 3 tooltip "surfaces" + the qualitative `series` palette). Chart components import `chartTheme` instead of inline hex; `momentum/utils.ts` (`SERIES_COLORS`, `tooltipStyle`) and `earnings/utils.ts` (`tooltipStyle`) re-export from it. `lib/sectorColors.ts` is a separate, intentional qualitative palette — leave it alone.
- **Spacing**: `px-8 py-5` page headers, `px-3 py-2.5` cells, `gap-3`/`gap-4` flex.

---

## Key conventions

- Backend wraps blocking Supabase calls in `asyncio.to_thread()`. SSE for long ops (ingest, scanner, backtest, current portfolio), with `: keepalive\n\n` comments before long work to beat proxy timeouts.
- AIRS portfolio data is parsed server-side, cached client-side in localStorage (no DB).
- Company deletion cascades: removes `metric_data` + `portfolio_weight` first.
- GuruFocus subscription covers USA + Europe + Asia (incl. Middle East); Russia, Africa, LatAm, AU/NZ out of scope — see `FEASIBLE_GF_EXCHANGES` in `backend/index_universe/acwi/exchange_map.py`.
- Price/volume cutoff `1998-01-01`; no earlier data stored.
- Supabase `.in_()` queries batched in chunks of `IN_CHUNK_SIZE` (=200, `deps.py`) — Cloudflare 502 guard. (Note: `ingest/prune_companies.py` still hardcodes its own `_IN_CHUNK = 50` — drift worth unifying.)
- GuruFocus raw responses cached in Storage bucket `gurufocus-raw`; paths `{EXCHANGE}_{TICKER}/indicator__price.json` and `indicator__volume.json`.
- **Momentum signal cutoff is strict `<`** (`signals.py`) — never train on the bar we trade; companies with last trade > 30 days before `as_of_date` are dropped. The panel anchors a cutoff to `searchsorted(side="left") - 1` (the last bar strictly before it), so a non-Monday `rebalance_weekday` (e.g. first-Wednesday) decides on the prior trading day's (Tuesday's) close and enters at the rebalance day's close. `rebalance_weekday` (0=Mon..6=Sun) is a per-variant sweep axis on `/backtest` (`VariantSpec.rebalance_weekday`; UI "Rebalance day" picker, Mon–Fri); the variant key gets a `w<n>` tag. Pinned by `tests/test_rebalance_weekday.py`.
- Transient Storage / `metric_data.upsert` errors retry up to 3× with backoff via `_retry_transient` (`ingest/prices.py`).
- **Current Picks caching**: identity = `_strategy_hash(req)` in `main.py` — 16-char SHA-256 of `signal_weights + category_weights + top_n_sectors + top_n_per_sector + max_companies + universe_label + index_universe + selection_mode`. Date range is *excluded* so the sliding "this month" view caches across date-only changes. `current_picks_day` only gains rows for the current month at compute time — never backfills closed months (past months are read-only).
- **API contract pipeline**: `backend/openapi.json` is the source of truth; `frontend/lib/api-types.ts` is auto-generated (`npm run gen:types`, `--default-non-nullable=false` so Pydantic-defaulted fields stay optional). After changing any route/Pydantic model: `cd backend && uv run python scripts/dump_openapi.py` then `cd frontend && npm run gen:types`; commit both (CI fails on stale). Downstream code imports from `lib/types/api.ts` (curated re-exports — `BacktestRequest`, `VariantSpec`, etc.), not `api-types.ts` directly.
- **Git hooks**: husky + lint-staged at repo root (`npm install` from root wires them). `.husky/pre-commit` → lint-staged (eslint on staged frontend, ruff on staged backend; ~8s); `.husky/pre-push` → frontend tsc when any .ts/.tsx is in the unpushed diff (~10s; skipped on pure-backend pushes). Both auto-fix + re-stage; `--no-verify` to bypass. `scripts/lint-staged-run.js` runs each linter in its sub-package cwd.
- **Playwright e2e** (`frontend/e2e/`): `npm run e2e` (headless), `:headed`, `:ui`. webServer does a production build → `next start :3100` (~30s first, ~10s cached). `E2E_BYPASS_AUTH=1` skips `proxy.ts`'s session check; tests stub `/api/*` via `page.route()` (`e2e/_mocks/*.ts`). Covered: /login, /companies, /backtest. For SSE endpoints, mock with a streaming `text/event-stream` Response.

---

## Deployment

Frontend → Vercel, backend → Railway (long SSE streams OK, no timeout), DB → Supabase (all free tier). Push to `main` to deploy (both auto-deploy from GitHub). Railway runs `uvicorn main:app` with dashboard env vars.

### Weekly current-picks cron (Railway)

Railway Cron fires `POST /api/momentum/current-picks/cron` at **02:00 UTC Monday** with `-H "X-Cron-Secret: $CRON_SECRET" -d @/app/cron-current-picks.json` (config = a `BacktestRequest`). The endpoint forces `mode=current_portfolio` + `force_recompute=true` regardless of body, persists `triggered_by='auto'` + upserts `current_picks_day`. To change the strategy, edit the payload file; to pause, disable in Railway. *(Current-picks is otherwise on-demand from the UI buttons — not scheduled.)*

### In-process pipeline (`backend/scheduler.py`) — split into two operations (2026-06-08)

The pipeline is split into **two independent operations** that the /schedule "Smart pipeline activity" card exposes as two stacked sections (each with its own status + **Run now**). They **never run concurrently** — a module-level `_PIPELINE_LOCK` in `ingest/phases/pipeline.py` serializes them (the contended one shows "Waiting for another pipeline operation to finish…" then runs). Each is its own `ingest_run` row + `job_name`. Orchestrators in `ingest/phases/pipeline.py`:

- **`price_update`** (`_run_price_update_pipeline_sync`) — keep the enabled strategies' **held companies** (~the 24 currently held) priced + re-price open positions (MTD). Scope: held companies only. Phases: prices (`_collect_held_companies` → `_run_prices_phase(companies_override=…)`) → momentum **price-update only** (`_run_momentum_phase(include_rebalances=False)`). No template maintenance, no universe refresh, no rebalance.
- **`rebalance`** (`_run_rebalance_pipeline_sync`) — rebalance the **due** strategies (re-select from a fresh universe). No-op (status ok) when nothing is due. Phases: plan (`build_plan`) → templates (refresh ONLY the due strategies' universes + derived parents) → dedupe (gated on a template rebuilding) → prices (`collect_universe_companies` — the due strategies' full universe) → momentum **rebalance only** (`_run_momentum_phase(include_price_updates=False)`).

`_run_momentum_phase` gates: `include_rebalances`/`include_price_updates` pick which branch runs per strategy (Branch A price_update / Branch B rebalance). Both ops finalize via `_finalize_run`.

**Scheduling** — one daily `BackgroundScheduler` tick at **02:00 UTC** (`id="daily_pipeline"`, fires `_fire_daily_sequence`) runs **price-update then rebalance in order**, in one daemon thread (so they're sequential, not racing). On startup, `_maybe_kickstart_smart` fires the same sequence once if a strategy is due or held prices are stale (catch-up after downtime). Manual Run-now → `POST /api/ingest/scheduled-refresh/trigger?job_name=price_update|rebalance` (admin Bearer via `apiFetch`).

**Dropped vs the old smart pipeline**: template-universe *maintenance* (auto-refreshing ACWI/LongEquity to the calendar month so /backtest + /acwi stay current with zero strategies). The rebalance op refreshes only the *due strategy's own* universe on demand — so universes no enabled strategy uses (ACWI, LongEquity) go stale until a manual full refresh. This intentionally fixed the bug where a snapshot-fed template stuck behind the month (LongEquity at 2026-05) re-priced its whole 401-company membership on **every** tick.

The legacy `_run_smart_pipeline_sync` (job `smart_daily`) + `_run_pipeline_sync` (jobs `manual`/`bootstrap_template_refresh`, full refresh-all) remain valid `job_name`s (dispatch in `routers/ingest_runs.py`), just no longer fired by the scheduler. `manual` = the full refresh-all pipeline (all five phases over every company).

**Trade-offs / gotchas**:
- ✅ Code-managed (deploys with `git push`; cron expression in `scheduler.py`).
- ⚠️ Single-instance assumption — the `_PIPELINE_LOCK` is in-process, so it only serializes within one replica. Set `DISABLE_SCHEDULER=1` on all but one if you scale out (also the pause switch).
- ⚠️ A restart landing exactly at 02:00 UTC drops that tick; the startup kickstart catches up.
- `POST /api/ingest/scheduled-refresh/cron` (X-Cron-Secret) still exists for reverting to Railway Cron — pass `job_name=price_update`/`rebalance`.

---

## Known issues

- Backtest universe uses the current company list retroactively (survivorship bias) when no `index_universe` is selected — LongEquity snapshots only exist from Aug 2025.
- God-components: `Schedule.tsx`, `AcwiUniverse.tsx`, `CompanyManager.tsx`, and `UniverseScreener.tsx` were decomposed on 2026-06-04 (now thin orchestrators over `app/components/{schedule,acwi,company,universe}/` — see **Component notes**). `MomentumBacktester.tsx` (~670 lines) is the remaining one to keep shrinking: config + variants now decomposed and the current-picks UI removed; remaining bulk = saved-runs handlers + `loadBacktest`/`saveVariantsBundle`. (Backend equivalent: `routers/ingest_runs.py` 1,660→374 lines, pipeline extracted to `ingest/phases/`.)
- Frontend test coverage thin but non-zero (vitest for a few helpers + e2e for /login, /companies, /backtest); widen as you touch pages.
- Backend unit tests now cover the momentum engine **+** the ingest data-routing / dedupe / transform logic and two prices/dedupe phase loaders (228 tests — see **tests/**). Still uncovered by unit tests: the prune/template/momentum phase *orchestrators*, `merge_existing_duplicates` FK-rewiring, and all routers/admin paths — `backend-stack-smoke` gives these HTTP-level 2xx coverage (incl. an end-to-end backtest) but no write-path / ingest-edge-case assertions. The reusable `tests/_fake_supabase.py` is the on-ramp to closing the rest.
