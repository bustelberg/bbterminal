# BBTerminal

A financial data terminal for wealth management. Analyses stocks using data from LongEquity reports, enriched with price data from GuruFocus.

## Architecture

- **Frontend**: Next.js (App Router) deployed on Vercel — `frontend/`
- **Backend**: FastAPI (Python) with uv — `backend/`
- **Database**: Supabase (Postgres) — schema in `supabase/migrations/`
- **Auth**: Supabase Auth (email/password)

## Running locally

```bash
# Backend
cd backend
uv run uvicorn main:app --reload --port 8000

# Frontend
cd frontend
npm run dev
```

Frontend runs on `http://localhost:3000`, backend on `http://localhost:8000`.

## Backend (`backend/`)

**Entry point**: `main.py` — FastAPI app with all API endpoints.

**Key files**:
- `portfolio.py` — Parses AIRS Excel exports, computes YTD returns in EUR and local currency per holding
- `ingest/acquire.py` — Downloads LongEquity report files from remote storage
- `ingest/flatten.py` — Flattens grouped Excel headers into a flat DataFrame
- `ingest/extend_primary.py` — Enriches tickers with primary exchange info
- `ingest/transformation.py` — Transforms flattened data into DB schema format
- `ingest/load_into_supabase.py` — Loads prepared data into Supabase tables
- `ingest/resolve_tickers.py` — Resolves unknown tickers via OpenFIGI
- `ingest/prices.py` — Fetches daily closing prices from GuruFocus API, caches in Supabase Storage

**API endpoints**:
- `GET /api/companies` — List all companies
- `POST /api/companies` — Create company
- `PUT /api/companies/{id}` — Update company
- `DELETE /api/companies/{id}` — Delete company (cascades metric_data + portfolio_weight)
- `POST /api/portfolios/parse` — Upload AIRS Excel, returns parsed holdings with YTD returns
- `GET /api/longequity/snapshots` — List loaded LongEquity months
- `GET /api/longequity/companies?target_date=` — Companies for a snapshot date
- `POST /api/ingest/long-equity` — SSE stream: runs full ingest pipeline
- `GET /api/companies/field-options` — Distinct exchanges/countries/sectors for dropdowns

**Environment variables** (`.env`):
- `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`
- `GURUFOCUS_BASE_URL`, `GURUFOCUS_API_KEY`

## Frontend (`frontend/`)

**Pages** (App Router):
- `/` — Welcome page
- `/longequity` — LongEquity Insight: monthly snapshots of stock universe, grouped by region/country
- `/airs-portfolio` — AIRS Portfolio: drag & drop Excel uploads, YTD returns table, client-side caching via localStorage
- `/companies` — Company management: searchable/filterable table, inline edit, add/delete
- `/login`, `/set-password` — Auth pages

**Components** (`frontend/app/components/`):
- `Sidebar.tsx` — Navigation sidebar with auth
- `LongEquityInsight.tsx` — Snapshot viewer with region/country grouping, ingest pipeline UI
- `AirsPortfolioUpload.tsx` — Portfolio list + detail views, drag & drop, localStorage cache
- `CompanyManager.tsx` — Company CRUD table with inline editing
- `IngestButton.tsx` — Ingest trigger button

## Database schema

**Tables** (see `supabase/migrations/20260402130000_schema.sql`):
- `company` — Primary key `company_id`, unique on `(primary_ticker, primary_exchange)`
- `metric_data` — Time-series data, PK `(company_id, metric_code, source_code, target_date)`
- `portfolio` — Portfolio metadata, unique on `(portfolio_name, target_date)`
- `portfolio_weight` — Portfolio holdings, PK `(portfolio_id, company_id)`
- `ticker_override` — OpenFIGI ticker resolutions

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

## Key conventions

- All frontend components are client components (`'use client'`)
- Backend uses `asyncio.to_thread()` for blocking Supabase calls
- SSE (Server-Sent Events) for long-running operations (ingest pipeline)
- AIRS portfolio data is parsed server-side but cached client-side in localStorage (no DB storage)
- Company deletion cascades: removes metric_data and portfolio_weight rows first
- GuruFocus API subscription covers USA + Europe regions only
