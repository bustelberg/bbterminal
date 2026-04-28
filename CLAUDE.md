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
  - `run_current_portfolio` — single-month "what would the strategy hold today" with month-to-date returns

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
- `POST /api/momentum/backtest` — SSE: runs backtest. Modes: standard backtest (`mode="backtest"`, default), random multi-trial (`selection_mode="random"`, `n_trials>1`), or current portfolio (`mode="current_portfolio"`).
- `GET|POST|DELETE|PATCH /api/momentum/backtests[/{run_id}]` — Saved backtests CRUD + rename
- `GET /api/momentum/current-picks` — List saved current-picks snapshots (most recent first)
- `GET /api/momentum/current-picks/{id}` — Load one snapshot (full holdings)
- `POST /api/momentum/current-picks/{id}/refresh-mtd` — MTD-only recompute on a stored snapshot's holdings (fast)
- `POST /api/momentum/current-picks/cron` — Cron entry point. Requires `X-Cron-Secret` header. Forces `mode=current_portfolio`, persists with `triggered_by='auto'`. See Deployment / Cron section.

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
- `/login`, `/set-password` — Auth pages

**Components** (`frontend/app/components/`):
- `Sidebar.tsx` — Navigation sidebar with auth
- `LongEquityUniverse.tsx` — Snapshot viewer with region/country grouping, ingest pipeline UI
- `AirsPortfolioUpload.tsx` — Portfolio scanner + list/detail views, drag & drop, localStorage cache
- `CompanyManager.tsx` — Company CRUD table with inline editing
- `IngestButton.tsx`, `ProgressTimeline.tsx`, `DialogHost.tsx`, `DatePartsPicker.tsx`, `ApiUsageBadge.tsx` — Shared UI
- `MomentumBacktester.tsx` — Momentum backtest UI: config panel with signal weight sliders (grouped by category), category weight sliders, equity curve chart (Recharts), benchmark comparison, summary stats, monthly portfolio table with per-category scores. Saved backtests CRUD. Strategy mode selector (Momentum / Random baseline) with trial count + seed. "Current Picks" button for live MTD portfolio view.
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

Key tables: `company`, `metric_data` (time-series for prices, volumes, derived metrics), `universe` + `universe_membership`, `backtest_run`, `benchmark` + `benchmark_price`, `gurufocus_exchange` + `country` + `currency` + `fx_rate`, `airs_performance`.

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
- GuruFocus API subscription covers USA + Europe regions only
- Price/volume data cutoff: `1998-01-01` — no data before this date is stored
- Supabase `.in_()` queries batched in chunks of 50 to avoid Cloudflare 502 errors
- GuruFocus raw API responses cached in Supabase Storage bucket `gurufocus-raw` as JSON files
- Storage paths: `{EXCHANGE}_{TICKER}/indicator__price.json` and `indicator__volume.json`
- **Momentum signal cutoff is strict `<`** (data must be from before `as_of_date`) so we never train on the bar we trade — see `signals.py`. Companies with last trade > 30 calendar days before `as_of_date` are filtered out (staleness guard).
- **Momentum tests live in `backend/tests/`** — run with `uv run pytest tests/` from the backend dir.
- Transient Supabase Storage / `metric_data.upsert` errors retry up to 3× with backoff via `_retry_transient` in `ingest/prices.py`.

---

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

Where `cron-current-picks.json` holds the strategy config (same shape as `BacktestRequest`) — typically the user's preferred signal weights, sectors, top-N, and `index_universe`. The endpoint forces `mode=current_portfolio` regardless of body content. The resulting snapshot lands in `current_picks_snapshot` with `triggered_by='auto'`.

To change the strategy the cron uses, edit `cron-current-picks.json` (or whichever payload file Railway is configured to send). To pause the cron, disable it in Railway's service config.

---

## Known issues

- Sidebar occasionally disappears after login — likely hydration/auth state timing issue. Hard refresh fixes it.
- Universe for backtesting uses current company list retroactively (survivorship bias) when no `index_universe` (e.g. SP500, ACWI) is selected — LongEquity-derived snapshots only exist from Aug 2025 onward.
