# BBTerminal

A financial data terminal for wealth management. Analyses stocks using data from LongEquity reports and index universes (S&P 500, ACWI), enriched with price/volume data from GuruFocus. Includes a momentum portfolio backtester and a live "current picks" view.

## Architecture

- **Frontend**: Next.js 16 (App Router) deployed on Vercel — `frontend/`
- **Backend**: FastAPI (Python, `uv`) deployed on Railway — `backend/`
- **Database**: Supabase (Postgres) — schema in `supabase/migrations/`
- **Auth**: Supabase Auth (email/password)
- **Two Supabase projects**: dev and prod. Dev has all data; prod needs migrations + data sync.

## Running locally

### Prerequisites

- Docker Desktop (for local Supabase)
- Node.js + npm (for frontend)
- Python + uv (for backend)

### 1. Start local Supabase

```bash
npx supabase start
```

This starts all Supabase services locally. First run pulls Docker images and may take a few minutes.

| Service | URL |
|---------|-----|
| Studio (DB GUI) | http://127.0.0.1:54323/project/default |
| REST API | http://127.0.0.1:54321/rest/v1 |
| Database | `postgresql://postgres:postgres@127.0.0.1:54322/postgres` |
| Storage | http://127.0.0.1:54321/storage/v1 |
| Mailpit (email testing) | http://127.0.0.1:54324 |

### 2. Configure environment variables

Create `backend/.env.local` (overrides `backend/.env` for local dev):
```env
SUPABASE_URL=http://127.0.0.1:54321
SUPABASE_SERVICE_KEY=<secret key from `npx supabase status`>
```

Create `frontend/.env.local` (overrides any `.env` for local dev):
```env
NEXT_PUBLIC_SUPABASE_URL=http://127.0.0.1:54321
NEXT_PUBLIC_SUPABASE_ANON_KEY=<publishable key from `npx supabase status`>
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_ALLOWED_EMAILS=<your email>
```

Run `npx supabase status` to get the local keys. Both `.env.local` files are gitignored.

### 3. Start the app

```bash
# Terminal 1 — Backend
cd backend
uv run uvicorn main:app --reload --port 8000

# Terminal 2 — Frontend
cd frontend
npm run dev
```

Frontend runs on `http://localhost:3000`, backend on `http://localhost:8000`.

### Useful Supabase CLI commands

```bash
npx supabase status                # Show all local URLs and keys
npx supabase db reset              # Wipe local DB, re-run migrations + seed
npx supabase migration new <name>  # Create a new migration file
npx supabase db diff               # Generate migration from local schema changes
npx supabase db push               # Push migrations to linked prod project
npx supabase stop                  # Stop all local containers
```

### Local vs. production

The backend loads `backend/.env` first (prod defaults), then `backend/.env.local` with `override=True` (local overrides). When `.env.local` doesn't exist (e.g. on Railway), only `.env` / environment variables are used.

The frontend follows Next.js convention: `.env.local` overrides `.env`. Vercel uses env vars from its dashboard, so `.env.local` only affects local dev.

---

## Backend (`backend/`)

**Entry point**: `main.py` — single FastAPI app with all API endpoints.

**Top-level modules**:
- `portfolio.py` — Parses AIRS Excel exports, computes YTD returns in EUR and local currency per holding
- `airs_scanner.py` — Playwright browser automation: logs in to AirSPMS, scrapes portfolio data
- `fx_rates.py` — ECB/Yahoo FX rate fetchers + `fx_rate` table sync
- `diagnose.py` — One-off diagnostic helpers
- `playground/main.py` — **Ignore for now.** Standalone scratch file.

**`ingest/` — LongEquity + GuruFocus ingest pipeline**:
- `acquire.py` — Downloads LongEquity report files from remote storage
- `flatten.py` — Flattens grouped Excel headers into a flat DataFrame
- `extend_primary.py` — Enriches tickers with primary exchange info
- `transformation.py` — Transforms flattened data into DB schema format
- `load_into_supabase.py` — Loads prepared data into Supabase tables
- `resolve_tickers.py` — Resolves unknown tickers via OpenFIGI
- `prices.py` — Fetches daily closing prices AND volumes from GuruFocus, caches in Supabase Storage. Includes `_retry_transient` for timeout/5xx resilience on Storage and `metric_data.upsert()` calls.
- `staleness.py` — Cache freshness rules
- `api_usage.py` — Tracks GuruFocus API call counts per region/month

**`universe/` — Universe screening (criteria-driven)**:
- `criteria.py` — Per-criterion definitions and metadata
- `screen.py` — Apply criteria to companies, build/store derived universes
- `derived_metrics.py` — Compute and store screening metrics in `metric_data` (source `derived`)

**`index_universe/` — Index reconstruction (S&P 500, ACWI)**:
- `sp500.py` — Scrape Wikipedia S&P 500 history, reconstruct monthly memberships, OpenFIGI resolution
- `acwi.py` — iShares ACWI fund holdings + MSCI announcement parsing
- `discover_overrides.py` — One-off helper for ticker-name override discovery

**`momentum/` — Momentum backtester engine**:
- `data.py` — Bulk loaders: `load_universe()`, `load_all_prices()`, `load_all_volumes()`, FX conversion, currency lookups. Queries batched in chunks of 50 company IDs to avoid Cloudflare 502 on Supabase.
- `signals.py` — Signal computation. 5 price signals (mom_12_1, mom_6m, volatility_adjusted_return_6m, drawdown_from_recent_high_pct, above_200ma) + 2 volume signals (vol_20d_vs_60d, vol_trend_3m). Each signal has a `"group"` field ("price" or "volume"). Pre-indexed `dict[int, pd.Series]` for O(1) lookups. **Strict `<` cutoff** on `as_of_date` so signals never see the close at which we'd enter the trade. **30-day staleness guard** filters companies whose last trade is too old.
- `scoring.py` — Category-based scoring: each category (price, volume) gets independent 0-100 min-max normalized score, then combined via adjustable category weights into final `momentum_score`. Includes `random_select` for the random-baseline mode.
- `backtest.py` — Three runners:
  - `run_backtest` — monthly rebalance loop (signals → score & select → equal-weight → forward 1-month return → cumulative tracking)
  - `run_multi_trial_backtest` — N independent random-selection runs with sequential seeds, aggregates mean ± std for headline stats
  - `run_current_portfolio` — "what would the strategy hold today" + per-trading-day daily picks for the current month. Each daily pick is a `MonthlyHolding`-shaped record with start-of-month → that-day MTD return, plus portfolio-level MTD return and turnover vs the previous day.

**`tests/`** — Pytest unit tests for momentum signals + scoring (`uv run pytest tests/`).

**API endpoints** (selected — see `main.py` for the complete list):

*Auth / system*
- `GET /api/health`, `GET /api/hello`, `DELETE /api/auth/delete-account`
- `GET /api/usage` — GuruFocus API usage counter

*Companies*
- `GET|POST|PUT|DELETE /api/companies` — CRUD (delete cascades `metric_data` + `portfolio_weight`)
- `GET /api/companies/field-options` — Distinct exchanges/countries/sectors

*LongEquity ingest*
- `GET /api/longequity/snapshots` — List loaded months
- `GET /api/longequity/companies?target_date=` — Companies for a snapshot
- `GET /api/longequity/latest-available`
- `POST /api/ingest/long-equity` — SSE: full ingest pipeline
- `POST /api/longequity/save-universe`

*Portfolios*
- `POST /api/portfolios/parse` — Upload AIRS Excel, returns parsed holdings + YTD
- `GET /api/airs/scan` — SSE: Playwright broker scan
- `GET /api/airs/portfolios`, `GET /api/airs/portfolio/{name}`

*Earnings*
- `POST /api/earnings/{company_id}/refresh/{source}`, `POST /api/earnings/{company_id}/refresh-all`
- `GET /api/earnings/{company_id}/metrics`, `GET /api/earnings/{company_id}/metric-codes`

*Momentum*
- `GET /api/momentum/signals` — Signal definitions
- `POST /api/momentum/backtest` — SSE: runs backtest. Modes:
  - `mode="backtest"` (default) — standard backtest
  - `selection_mode="random"`, `n_trials>1` — random multi-trial
  - `mode="current_portfolio"` — current picks. The backend computes a `strategy_hash` from the request and short-circuits to the cached snapshot when one exists for `(hash, current month)`. Set `force_recompute=true` to bypass the cache (the **Recompute** button does this). Cache miss runs the full compute, then persists both a row in `current_picks_snapshot` and one row per trading day in `current_picks_day`. Response payload always includes `daily_picks_history` (all stored days for this strategy, across months).
- `GET|POST|DELETE|PATCH /api/momentum/backtests[/{run_id}]` — Saved backtests CRUD + rename
- `GET /api/momentum/current-picks` — List saved current-picks snapshots (most recent first)
- `GET /api/momentum/current-picks/{id}` — Load one snapshot (full holdings)
- `POST /api/momentum/current-picks/{id}/refresh-mtd` — MTD-only recompute on a stored snapshot's holdings (fast)
- `POST /api/momentum/current-picks/cron` — Cron entry point. Requires `X-Cron-Secret` header. Forces `mode=current_portfolio` and `force_recompute=true`, persists with `triggered_by='auto'`. See Deployment / Cron section.

*Universe (criteria-screened)*
- `GET /api/universe/criteria`, `POST /api/universe/screen`, `POST /api/universe/build`
- `GET /api/universe/labels`, `GET /api/universe/months`
- `GET|DELETE /api/universe/months/{month}`, `PUT|DELETE /api/universe/labels/{label}`
- `GET /api/universe/derived-metrics/{criteria,status}`, `POST /api/universe/derived-metrics/recompute`
- `POST /api/universe/derive/preview`, `POST /api/universe/derive`
- `GET /api/universe/validate`

*Index universe (S&P 500, ACWI)*
- `POST /api/index-universe/import-sp500`, `GET /api/index-universe/{indexes,months,tickers,cumulative,changes}`
- `POST /api/index-universe/check-gurufocus`, `DELETE /api/index-universe/indexes/{index_name}`
- `GET /api/acwi/{holdings,announcements,announcement-detail,net-additions,fetch-all-details}`
- `POST /api/acwi/announcement-details-bulk`, `POST /api/acwi/save-universe`

*Benchmarks*
- `GET|POST /api/benchmarks`, `POST /api/benchmarks/{id}/refresh`, `DELETE /api/benchmarks/{id}`
- `GET /api/benchmarks/{id}/prices`

*FX + indicators*
- `GET /api/fx/{coverage,latest,history/{currency}}`
- `POST /api/indicators/fetch`
- `GET /api/gurufocus/{exchanges,exchange-currencies}`

*Scheduled refresh + pipeline*
- `POST /api/ingest/scheduled-refresh/cron?job_name=<weekly_price_volume|monthly_price_volume>` — X-Cron-Secret protected fallback entry (the in-process APScheduler uses this path's sister `kick_off_refresh()` directly).
- `POST /api/ingest/scheduled-refresh/trigger?job_name=manual` — manual UI trigger (Run-now button on `/schedule`).
- `GET /api/ingest/runs?limit=N` — recent pipeline runs (newest first).
- `GET /api/ingest/runs/{run_id}` — one row including all per-phase result columns.
- `GET /api/ingest/runs/{run_id}/acwi-membership?q=` — searchable membership for the ACWI snapshot the run captured.
- `GET|PUT /api/schedule-config` — the "scheduled strategy" singleton (selected `backtest_run.run_id` drives the pipeline's momentum phase).

*Transaction fees*
- `GET /api/exchange-fees` — every exchange (from `gurufocus_exchange`) joined with its configured `fee_bps` (0 when unset).
- `PUT /api/exchange-fees/{exchange_code}` body `{fee_bps: float}` — upsert.
- `DELETE /api/exchange-fees/{exchange_code}` — drop the row (equivalent to setting it to 0).

*Admin API* — for external scripts (e.g. IBKR rebalancer). Bearer JWT with `app_metadata.role == 'admin'` required.
- `GET /api/admin/portfolio/latest` — latest scheduled-strategy snapshot, IBKR-ready (ticker / exchange / currency / target_weight / side / prices / company_name / sector per holding).
- `GET /api/admin/portfolio/{snapshot_id}` — same shape, specific snapshot.
- `GET /api/admin/runs/latest` — most recent pipeline run summary + most recent successful run.
- `GET /api/admin/pipeline-runs?limit=N` — recent runs list (newest first).
- `GET /api/admin/data-freshness` — per-source freshness (close_price / volume max date + trading-day age, latest snapshot, latest run).
- `GET /api/admin/health` — composite go/no-go: `{is_healthy, is_healthy_strict, checks, problems}`. Strict requires green on every check; loose tolerates a single failed run.
- `GET /api/admin/sanity-check` — coarse counts per major table + recent-run status distribution + latest snapshot summary. For "is everything basically wired up" eyeball checks.

**Environment variables** (`.env`, overridden by `.env.local` if present):
- `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`
- `GURUFOCUS_BASE_URL`, `GURUFOCUS_API_KEY`
- `BROKER_USERNAME`, `BROKER_PASSWORD` — AirSPMS credentials for Playwright scanner
- `CRON_SECRET` — shared secret required by `POST /api/momentum/current-picks/cron`

---

## Frontend (`frontend/`)

**Pages** (App Router, all client components):
- `/` — Welcome page
- `/longequity-universe` — LongEquity Insight: monthly snapshots of stock universe, grouped by region/country
- `/airs-portfolio` — AIRS Portfolio: broker scanner + drag & drop Excel uploads, YTD returns table
- `/companies` — Company management: searchable/filterable table, inline edit, add/delete
- `/momentum` — Momentum Portfolio Backtester + Current Picks
- `/benchmarks` — Benchmark management (create/refresh/delete index benchmarks)
- `/earnings` — Earnings dashboard
- `/universe` — Universe screener (criteria-driven)
- `/universe_index` — Index universe (S&P 500, ACWI) management
- `/acwi` — ACWI holdings / MSCI announcement explorer
- `/fx-rates` — FX rate viewer + sync
- `/request_gurufocus` — GuruFocus indicator fetch UI
- `/schedule` — Scheduled pipeline (ACWI → prices → momentum). Top: "Scheduled strategy" picker that drives the momentum phase. Middle: weekly + monthly job cards with "Run now". Bottom: recent runs list — each row has a three-dot phase pip, expands to show the ACWI diff (counts + click-to-expand lists), the searchable ACWI membership for that run, and the momentum holdings rendered via `MonthlyHoldingsTable` (same look as /momentum's holdings table). Backed by `ingest_run` + `schedule_config` tables.
- `/api` — Admin-only interactive endpoint explorer. Lists each admin API endpoint as a card with description, params, "Try it" button, and "Copy as curl". Uses the user's current Supabase session as the Bearer token so the explorer hits the real endpoints exactly as an external script would.
- `/documentation` — Admin-only reference for calling the admin API from external scripts. Includes the full `bbterminal_client.py` Python source (copy-pasteable), a curl quick-start, env-var setup, a PowerShell one-off variant, and an endpoint reference table cross-linking to `/api`.
- `/fees` — Per-exchange one-way transaction fees + broker-support toggle. Each row has a "Supported" checkbox (default checked) plus a fee_bps input; changes are batched and committed via a sticky Save button (no auto-save). Unsupported exchanges are dropped from the backtest universe entirely — every company on that exchange is excluded before signals are computed (filter applied in `backtest_stream/stream.py` right after `load_universe`). Backtest stats (Total Return, Annualized, Sharpe, Max DD, yearly breakdown, custom range, variants table) render `gross (net)` using a trade-aware fee model: a holding pays the buy fee only when it first appears vs the previous period, the sell fee only when it doesn't roll into the next period, and the open period never pays sell. Net stats are computed client-side in `frontend/app/components/momentum/feeStats.ts` so adjusting fees updates parentheticals on the next backtest render without re-running. Backed by `exchange_fee` table (`fee_bps` + `is_broker_supported`).
- `/login`, `/set-password` — Auth pages. After successful sign-in or password set, the user is redirected to `/` (the welcome page). The proxy middleware in `frontend/proxy.ts` enforces auth on all non-public routes and redirects authenticated users away from `/login`.

**Components** (`frontend/app/components/`):
- `Sidebar.tsx` — Navigation sidebar with auth
- `LongEquityUniverse.tsx` — Snapshot viewer with region/country grouping, ingest pipeline UI. **Nothing fetches on mount** — the page shell renders empty until the user clicks **Load** (fetches saved snapshots + the latest available month) or **Run ingest** (kicks off the full ingest pipeline).
- `AirsPortfolioUpload.tsx` — Portfolio scanner + list/detail views, drag & drop, localStorage cache
- `CompanyManager.tsx` — Company CRUD table with inline editing
- `IngestButton.tsx`, `ProgressTimeline.tsx`, `DialogHost.tsx`, `DatePartsPicker.tsx`, `ApiUsageBadge.tsx` — Shared UI
- `MomentumBacktester.tsx` — Momentum backtest UI: config panel with signal weight sliders (grouped by category), category weight sliders, equity curve chart (Recharts), benchmark comparison, summary stats, monthly portfolio table with per-category scores. Saved backtests CRUD. Strategy mode selector (Momentum / Random baseline) with trial count + seed. **Current Picks** button — hits the SSE backtest endpoint with `mode=current_portfolio`; the backend serves from cache (no recompute) when this strategy already has a snapshot for the current month. **Recompute** button passes `force_recompute=true` for a fresh run. Below the locked-at-start holdings table, the card renders a **Daily picks history** view: months as expandable rows (showing day count + latest MTD); each month expands to the days stored for it; each day expands to full per-holding detail (matching the backtest monthly portfolio table's columns). Past months are read-only — only days already saved are shown.
- `BenchmarkManager.tsx` — Benchmark CRUD: add index tickers (e.g. SPY, ACWI), fetch prices, show date ranges
- `EarningsDashboard.tsx` — Earnings data viewer
- `UniverseScreener.tsx` — Criteria-driven universe screener
- `IndexUniverse.tsx`, `AcwiUniverse.tsx` — Index universe explorers
- `FxRates.tsx` — FX rate coverage / history
- `Indicators.tsx` — GuruFocus indicator fetch trigger

**Stores** (`frontend/lib/stores/`): Lightweight reactive store pattern (`createStore`). `momentum.ts` holds the SSE-driven backtest + current portfolio state.

---

## Database schema

See [`docs/schema.md`](docs/schema.md) for the full ERD and table descriptions.

Key tables: `company`, `metric_data` (time-series for prices, volumes, derived metrics), `universe` + `universe_membership`, `backtest_run`, `benchmark` + `benchmark_price`, `gurufocus_exchange` + `country` + `currency` + `fx_rate`, `airs_performance`, `current_picks_snapshot` (one row per current-picks compute — locked-at-start holdings + current-month daily_picks blob, tagged with `strategy_hash`), `current_picks_day` (per-day row keyed by `(strategy_hash, target_date)` — backs the cross-month "Daily picks history" view and the cache lookup), `ingest_run` (per-pipeline-run audit row backing `/schedule` — carries phase tracking + per-phase result columns: `acwi_universe_id` / `acwi_target_month` / `acwi_summary` jsonb / `momentum_snapshot_id` / `momentum_summary` jsonb), `schedule_config` (singleton row holding the user's selected scheduled-strategy `backtest_run.run_id` driving the pipeline's momentum phase), `exchange_fee` (per-exchange one-way bps backing `/fees`).

---

## UI Design System

Modern fintech dark theme. Key principles:

**Colors**:
- Background: `#0f1117` (page), `#151821` (cards/table surfaces), `#0b0d13` (sidebar)
- Accent: `indigo-600` primary buttons, `indigo-400` active nav/links
- Returns: `emerald-400` positive, `rose-400` negative
- Text: `white` headings, `gray-200` body, `gray-400`/`gray-500` secondary

**Typography**:
- Sans-serif (Geist Sans) for all UI text
- Monospace (Geist Mono) only for numeric data values (prices, percentages, quantities)

**Components**:
- Cards: `bg-[#151821] rounded-xl border border-gray-800/40`
- Tables: Wrapped in card containers, `text-sm`, generous `py-2.5` row padding
- Buttons: `rounded-lg`, primary `bg-indigo-600 hover:bg-indigo-500`, ghost `hover:bg-white/5`
- Inputs: `bg-[#0f1117] border border-gray-700 rounded-lg` with `focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30`
- Row hover: `hover:bg-white/[0.02]`, action buttons fade in with `opacity-0 group-hover:opacity-100`
- Errors: `bg-rose-500/10 border border-rose-500/20 rounded-lg`

**Spacing**: `px-8 py-5` for page headers, `px-3 py-2.5` for table cells, `gap-3`/`gap-4` for flex layouts.

---

## Key conventions

- All frontend components are client components (`'use client'`)
- Backend uses `asyncio.to_thread()` for blocking Supabase calls
- SSE (Server-Sent Events) for long-running operations (ingest pipeline, broker scanner, backtest, current portfolio)
- SSE keepalive comments (`: keepalive\n\n`) sent before long-running operations to prevent proxy timeouts
- AIRS portfolio data is parsed server-side but cached client-side in localStorage (no DB storage)
- Company deletion cascades: removes metric_data and portfolio_weight rows first
- GuruFocus API subscription covers USA + Europe + Asia (incl. Middle East). Russia, Africa, LatAm, AU/NZ are out of scope — see `FEASIBLE_GF_EXCHANGES` in `backend/index_universe/acwi/exchange_map.py`
- Price/volume data cutoff: `1998-01-01` — no data before this date is stored
- Supabase `.in_()` queries batched in chunks of 50 to avoid Cloudflare 502 errors
- GuruFocus raw API responses cached in Supabase Storage bucket `gurufocus-raw` as JSON files
- Storage paths: `{EXCHANGE}_{TICKER}/indicator__price.json` and `indicator__volume.json`
- **Momentum signal cutoff is strict `<`** (data must be from before `as_of_date`) so we never train on the bar we trade — see `signals.py`. Companies with last trade > 30 calendar days before `as_of_date` are filtered out (staleness guard).
- **Momentum tests live in `backend/tests/`** — run with `uv run pytest tests/` from the backend dir.
- Transient Supabase Storage / `metric_data.upsert` errors retry up to 3× with backoff via `_retry_transient` in `ingest/prices.py`.
- **Current Picks caching**: a request's strategy identity is `_strategy_hash(req)` in `backend/main.py` — a 16-char SHA-256 of `signal_weights + category_weights + top_n_sectors + top_n_per_sector + max_companies + universe_label + index_universe + selection_mode`. Date range is intentionally excluded so the sliding "this month" view caches across runs that differ only in dates. Past months are read-only — `current_picks_day` only ever gains rows for the current month at compute time; it never backfills closed months.

---

## Admin API (external scripts)

The `/api/admin/*` endpoints exist so a local script (IBKR rebalancer, monitoring cron, etc.) can pull the latest scheduled-strategy portfolio + monitor pipeline health without opening the BBTerminal web UI. All require a Supabase JWT whose `app_metadata.role == 'admin'` — same gate the UI's admin pages use.

**Sign in (email + password → access_token):**
```bash
ACCESS_TOKEN=$(curl -fsS -X POST \
  "$SUPABASE_URL/auth/v1/token?grant_type=password" \
  -H "apikey: $SUPABASE_ANON_KEY" \
  -H "Content-Type: application/json" \
  -d '{"email":"you@example.com","password":"…"}' \
  | jq -r .access_token)
```

Tokens expire after ~1h. For long-running scripts, hold both `access_token` and `refresh_token` and re-call `/auth/v1/token?grant_type=refresh_token` when the access token expires.

**Get the latest portfolio (for an IBKR rebalancer):**
```bash
curl -fsS -H "Authorization: Bearer $ACCESS_TOKEN" \
  "https://<backend>/api/admin/portfolio/latest"
```

Response shape:
```jsonc
{
  "snapshot_id": 42,
  "as_of_date": "2026-05-15",
  "latest_price_date": "2026-05-14",
  "created_at": "2026-05-15T02:08:11Z",
  "strategy": {
    "name": "ACWI-mei · Momentum · 2002-2026",
    "selection_mode": "momentum",
    "strategy_type": "long_only",
    "top_n_sectors": 4,
    "top_n_per_sector": 3
  },
  "holdings_count": 12,
  "total_weight": 1.0,
  "holdings": [
    {
      "ticker": "NESTE",
      "exchange": "OHEL",          // GuruFocus exchange code, map to IBKR yourself
      "currency": "EUR",
      "side": "long",
      "target_weight": 0.0833,
      "company_id": 782,
      "company_name": "NESTE",
      "sector": "Energy",
      "entry_price_local": 28.52,
      "entry_price_eur": 28.52,
      "entry_date": "2026-05-01",
      "score": 87.4
    }
  ]
}
```

Symbol → IBKR mapping is intentionally left to the script — GuruFocus codes don't 1:1 with IBKR exchange codes (e.g. `OHEL` here → IBKR `HEL`). Maintain the translation in your script so we don't lock in a particular broker convention.

**Pre-trade safety gate:**
```bash
HEALTH=$(curl -fsS -H "Authorization: Bearer $ACCESS_TOKEN" \
  "https://<backend>/api/admin/health")
echo "$HEALTH" | jq -e '.is_healthy_strict == true' >/dev/null || {
  echo "Refusing to trade — bbterminal not healthy: $(echo "$HEALTH" | jq -r .problems)"
  exit 1
}
```

**Monitor cron success:**
```bash
curl -fsS -H "Authorization: Bearer $ACCESS_TOKEN" \
  "https://<backend>/api/admin/runs/latest"
```

Use the `latest.started_at` / `latest.status` fields to alert when no run has happened in the last 8 days, or the most recent one ended in `error`.

## Deployment

- **Frontend**: Vercel (free tier).
- **Backend**: Railway. Long-running SSE streams (ingest, backtest, broker scan) work without timeout limits.
- **Database**: Supabase (free tier).

To deploy, push to `main` — both Vercel and Railway are wired to auto-deploy from the connected GitHub repo. For Railway, the backend service runs `uvicorn main:app` with env vars set in the Railway dashboard.

### Cron: weekly current-picks snapshot

Railway Cron is configured to fire `POST /api/momentum/current-picks/cron` at **02:00 UTC every Monday** (= 03:00 Amsterdam in winter, 04:00 in summer — close enough to the requested 04:00 weekly cadence).

The cron command:
```bash
curl -fsS -X POST "https://<backend>/api/momentum/current-picks/cron" \
  -H "X-Cron-Secret: $CRON_SECRET" \
  -H "Content-Type: application/json" \
  -d @/app/cron-current-picks.json
```

Where `cron-current-picks.json` holds the strategy config (same shape as `BacktestRequest`) — typically the user's preferred signal weights, sectors, top-N, and `index_universe`. The endpoint forces `mode=current_portfolio` and `force_recompute=true` regardless of body content (the cron's purpose is to land a fresh weekly snapshot, so it bypasses the cache). The resulting snapshot lands in `current_picks_snapshot` with `triggered_by='auto'` and the strategy hash. Per-day rows are also upserted into `current_picks_day` so the UI's "Daily picks history" stays current.

To change the strategy the cron uses, edit `cron-current-picks.json` (or whichever payload file Railway is configured to send). To pause the cron, disable it in Railway's service config.

### Schedule: in-process pipeline (ACWI → prices → momentum)

The scheduled refresh runs from an in-process APScheduler defined in `backend/scheduler.py` — **no Railway Cron entries required**. Two `BackgroundScheduler` triggers fire the SAME three-phase pipeline:

| Job name | Trigger | Captures |
|---|---|---|
| `weekly_price_volume` | Tuesday 02:00 UTC | The previous Monday's global closes — US closes at ~21:00 UTC Monday, so 02:00 UTC Tuesday is ~5h later (plenty of GuruFocus settle time). |
| `monthly_price_volume` | 2nd of every month, 02:00 UTC | The first trading day's closes. If the 1st was a weekend/holiday the run still fires but freshness checks short-circuit; the next weekly tick catches up. |

Each fire calls `kick_off_refresh(job_name, "auto")` from `routers/ingest_runs.py`, which inserts an `ingest_run` row tagged `triggered_by='auto'` and spawns the daemon worker `_run_pipeline_sync` that executes:

  **Phase 1 — ACWI refresh** via `run_acwi_save_universe()` (the sync core extracted from `/api/acwi/save-universe`). Reconstructs the last two months of ACWI membership, then diffs this month vs last month to produce `acwi_summary` JSONB with `{additions, removals, renames}` lists. Persisted on the run row as `acwi_universe_id` / `acwi_target_month` / `acwi_summary`.

  **Phase 2 — price + volume refresh** (the original behavior pre-pipeline). Walks every row in `company` through `ensure_prices_for_company` + `ensure_volume_for_company`. Per-class counters checkpoint to the row every 100 companies. Forbidden / delisted / unexpected-error tallies surface on the same row.

  **Phase 3 — momentum compute**. Reads `schedule_config.selected_run_id` to find the "scheduled strategy" (a saved `backtest_run` row). Drains `_momentum_backtest_stream` with `mode=current_portfolio` + that strategy's config. Persists the resulting `current_picks_snapshot` and links it on `ingest_run.momentum_snapshot_id`. Skipped silently when no strategy is selected.

Phases are independent — a failure in one is captured in `error_summary` but the next phase still attempts. `current_phase` reflects the live phase (or `done` when finished). The `/schedule` UI shows three-dot phase pips per run + an expandable detail panel with the ACWI diff lists, a searchable membership viewer, and the holdings (rendered via `MonthlyHoldingsTable` fed a one-period synthetic result, so it matches the `/momentum` look exactly).

**Trade-offs vs Railway Cron:**
- ✅ Code-managed: deploys with `git push`, no UI clicks. Cron expressions live in `scheduler.py`.
- ⚠️ Single-instance assumption. If Railway ever runs the backend with N replicas, each replica fires its own tick — the freshness checks no-op duplicates so it's "wasteful but harmless" rather than broken. Set `DISABLE_SCHEDULER=1` on all-but-one replica if you scale out.
- ⚠️ A restart that lands exactly at 02:00 UTC drops that tick. Acceptable for a recovery cadence in days; the next week catches up.

To pause: set `DISABLE_SCHEDULER=1` in the Railway env and redeploy.

**The `POST /api/ingest/scheduled-refresh/cron` HTTP endpoint still exists** (X-Cron-Secret protected) for parity with the manual `/trigger` endpoint — useful if you ever want to revert to Railway Cron without touching code — but it's not used by the current setup.

**Backtests** (`/api/momentum/backtest`) unconditionally set `db_only=True` for `mode=backtest` — they never refresh data lazily, so results are predictable and fast. If the DB looks stale, click "Run now" on `/schedule` or wait for the next scheduled tick.

**Current-picks is NOT scheduled.** It's an on-demand action the user kicks off from the UI's "Current Picks" / "Recompute" buttons. If the existing Railway-UI Monday 02:00 UTC current-picks cron is still configured, you can disable it — `mode=current_portfolio` keeps its own narrow self-heal for its ~30 held names when the user clicks the button.

Per-company outcomes aren't stored on `ingest_run` — only aggregates (`prices_refreshed`, `volumes_refreshed`, `forbidden_count`, `delisted_count`, `error_count`). The first ~5 unexpected errors are captured in `error_summary` for triage.

---

## Known issues

- Universe for backtesting uses current company list retroactively (survivorship bias) when no `index_universe` (e.g. SP500, ACWI) is selected — LongEquity-derived snapshots only exist from Aug 2025 onward.
