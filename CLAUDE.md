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

---

## Backend (`backend/`)

**Entry point**: `main.py` — FastAPI app with all API endpoints.

**Key files**:
- `portfolio.py` — Parses AIRS Excel exports, computes YTD returns in EUR and local currency per holding
- `airs_scanner.py` — Playwright browser automation: logs in to AirSPMS, scrapes portfolio data
- `ingest/acquire.py` — Downloads LongEquity report files from remote storage
- `ingest/flatten.py` — Flattens grouped Excel headers into a flat DataFrame
- `ingest/extend_primary.py` — Enriches tickers with primary exchange info
- `ingest/transformation.py` — Transforms flattened data into DB schema format
- `ingest/load_into_supabase.py` — Loads prepared data into Supabase tables
- `ingest/resolve_tickers.py` — Resolves unknown tickers via OpenFIGI
- `ingest/prices.py` — Fetches daily closing prices from GuruFocus API, caches in Supabase Storage
- `playground/main.py` — **Ignore for now.** Standalone scratch file with working logic to compute YTD returns per stock/bond and total portfolio TWR from three AIRS Excel exports (VOLK, MUT, ATT). Useful as reference for the return calculation formulas but not part of the app.

**API endpoints**:
- `GET /api/companies` — List all companies
- `POST /api/companies` — Create company
- `PUT /api/companies/{id}` — Update company
- `DELETE /api/companies/{id}` — Delete company (cascades metric_data + portfolio_weight)
- `POST /api/portfolios/parse` — Upload AIRS Excel, returns parsed holdings with YTD returns
- `GET /api/longequity/snapshots` — List loaded LongEquity months
- `GET /api/longequity/companies?target_date=` — Companies for a snapshot date
- `POST /api/ingest/long-equity` — SSE stream: runs full ingest pipeline
- `GET /api/airs/scan` — SSE stream: runs Playwright broker scan, returns portfolio list
- `GET /api/companies/field-options` — Distinct exchanges/countries/sectors for dropdowns

**Environment variables** (`.env`):
- `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`
- `GURUFOCUS_BASE_URL`, `GURUFOCUS_API_KEY`
- `BROKER_USERNAME`, `BROKER_PASSWORD` — AirSPMS credentials for Playwright scanner

---

## Frontend (`frontend/`)

**Pages** (App Router):
- `/` — Welcome page
- `/longequity` — LongEquity Insight: monthly snapshots of stock universe, grouped by region/country
- `/airs-portfolio` — AIRS Portfolio: broker scanner + drag & drop Excel uploads, YTD returns table
- `/companies` — Company management: searchable/filterable table, inline edit, add/delete
- `/login`, `/set-password` — Auth pages

**Components** (`frontend/app/components/`):
- `Sidebar.tsx` — Navigation sidebar with auth
- `LongEquityInsight.tsx` — Snapshot viewer with region/country grouping, ingest pipeline UI
- `AirsPortfolioUpload.tsx` — Portfolio scanner + list/detail views, drag & drop, localStorage cache
- `CompanyManager.tsx` — Company CRUD table with inline editing
- `IngestButton.tsx` — Ingest trigger button

---

## Database schema

**Tables** (see `supabase/migrations/20260402130000_schema.sql`):
- `company` — Primary key `company_id`, unique on `(primary_ticker, primary_exchange)`
- `metric_data` — Time-series data, PK `(company_id, metric_code, source_code, target_date)`
- `portfolio` — Portfolio metadata, unique on `(portfolio_name, target_date)`
- `portfolio_weight` — Portfolio holdings, PK `(portfolio_id, company_id)`
- `ticker_override` — OpenFIGI ticker resolutions

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
- SSE (Server-Sent Events) for long-running operations (ingest pipeline, broker scanner)
- AIRS portfolio data is parsed server-side but cached client-side in localStorage (no DB storage)
- Company deletion cascades: removes metric_data and portfolio_weight rows first
- GuruFocus API subscription covers USA + Europe regions only

---

## Current Task: AIRS Broker Portfolio Scanner

### Goal
Build a feature in the AIRS portfolio tab that triggers a Playwright browser automation on the backend, streams real-time progress to the frontend, and displays the discovered portfolios in a table. No database storage — everything is ephemeral.

### Flow

```
Frontend (AIRS tab)          Backend API              Playwright
┌──────────────┐       ┌─────────────────┐      ┌──────────────┐
│ "Start Scan" │──────>│ GET /api/airs/   │─────>│ Launch       │
│   button     │       │   scan           │      │ Chromium     │
│              │       │                  │      │              │
│ Progress log │<──SSE─│ SSE stream       │<─────│ Step events  │
│ (real-time)  │       │                  │      │              │
│              │       │                  │      │              │
│ Portfolio    │<──────│ Final payload    │<─────│ Table scrape │
│ table        │       │ in SSE stream    │      │              │
└──────────────┘       └─────────────────┘      └──────────────┘
```

### Backend: `GET /api/airs/scan`

Returns SSE stream (`Content-Type: text/event-stream`). Follow the same SSE pattern as `POST /api/ingest/long-equity`.

**SSE event format** (each line is `data: {json}\n\n`):
```json
{"type": "progress", "step": "login", "status": "in_progress", "message": "Navigating to login page..."}
{"type": "progress", "step": "login", "status": "done", "message": "Logged in successfully"}
{"type": "progress", "step": "navigate", "status": "in_progress", "message": "Opening Rapportage menu..."}
{"type": "progress", "step": "navigate", "status": "done", "message": "Navigated to portfolio selection"}
{"type": "progress", "step": "scrape", "status": "in_progress", "message": "Reading portfolio table..."}
{"type": "portfolios", "data": [{"portefeuille": "BUS_Neutraal_Dyn", "depotbank": "MPF", "client": "ALGBUS", "naam": "Bustelberg Neutraal Dyn MPF"}, ...]}
{"type": "done", "message": "Scan complete. Found 22 portfolios."}
```

On error:
```json
{"type": "error", "message": "Login failed — check credentials"}
```

**Playwright automation** (put in `backend/airs_scanner.py`):

```python
async def scan_portfolios(send_event):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Step 1: Login
        await send_event("progress", step="login", status="in_progress", message="Navigating to login page...")
        await page.goto("https://bustelberg.airspms.cloud/login.php")
        await page.wait_for_load_state("domcontentloaded")

        await send_event("progress", step="login", status="in_progress", message="Entering credentials...")
        await page.fill('input[type="text"]', BROKER_USERNAME)
        await page.fill('input[type="password"]', BROKER_PASSWORD)
        await page.click('input[type="submit"], button[type="submit"]')
        await page.wait_for_load_state("networkidle")
        await send_event("progress", step="login", status="done", message="Logged in successfully")

        # Step 2: Navigate to Front-office
        await send_event("progress", step="navigate", status="in_progress", message="Opening Rapportage menu...")
        await page.hover('a[data-field="Rapportage"]')
        await page.wait_for_timeout(500)

        await send_event("progress", step="navigate", status="in_progress", message="Clicking Front-office...")
        await page.click('a[data-field="Front-Office"]')
        await page.wait_for_load_state("networkidle")
        await send_event("progress", step="navigate", status="done", message="Navigated to portfolio selection")

        # Step 3: Scrape portfolio table
        await send_event("progress", step="scrape", status="in_progress", message="Reading portfolio table...")
        await page.wait_for_selector('tr.list_dataregel', timeout=10000)

        rows = await page.query_selector_all('tr.list_dataregel')
        portfolios = []
        for row in rows:
            cells = await row.query_selector_all('td.listTableData')
            if len(cells) >= 4:
                portfolios.append({
                    "portefeuille": (await cells[0].inner_text()).strip(),
                    "depotbank": (await cells[1].inner_text()).strip(),
                    "client": (await cells[2].inner_text()).strip(),
                    "naam": (await cells[3].inner_text()).strip(),
                })

        await send_event("portfolios", data=portfolios)
        await send_event("done", message=f"Scan complete. Found {len(portfolios)} portfolios.")
        await browser.close()
```

**Credentials**: Read `BROKER_USERNAME` and `BROKER_PASSWORD` from `.env`. Never expose to frontend.

**Dependency**: Add `playwright` to backend deps. Run `playwright install chromium` once.

### Playwright selectors (from actual site HTML)

| Element | Selector |
|---------|----------|
| Username field | `input[type="text"]` |
| Password field | `input[type="password"]` |
| Login submit | `input[type="submit"]` or `button[type="submit"]` |
| Rapportage menu | `a[data-field="Rapportage"]` |
| Front-office link | `a[data-field="Front-Office"]` |
| Portfolio rows | `tr.list_dataregel` |
| Portfolio cells | `td.listTableData` |
| Radio buttons | `input[id="BUS_Neutraal_Dyn"]` (id = portfolio name) |

### Frontend: AIRS portfolio tab

**Location**: `AirsPortfolioUpload.tsx` — wipe current logic, replace with scanner UI.

**UI layout**:
```
┌─────────────────────────────────────────────────────┐
│ AIRS Portfolio Scanner                               │
│                                                      │
│ [Start Scan]                                         │
│                                                      │
│ ┌─ Progress ──────────────────────────────────────┐  │
│ │ ✓ Navigating to login page...                   │  │
│ │ ✓ Entering credentials...                       │  │
│ │ ✓ Logged in successfully                        │  │
│ │ ● Opening Rapportage menu...                    │  │
│ │ ○ Reading portfolio table...                    │  │
│ └─────────────────────────────────────────────────┘  │
│                                                      │
│ ┌─ Portfolios (22 found) ────────────────────────┐  │
│ │ #  Portefeuille              Dp   Client  Naam  │  │
│ │ 1  BUS_Neutraal_Dyn          MPF  ALGBUS  ...   │  │
│ │ 2  BUS_Offensief_Dyn         MPF  ALGBUS  ...   │  │
│ │ ...                                             │  │
│ └─────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

**Behavior**:
1. User clicks "Start Scan" → button disables, shows spinner
2. Progress log streams in via SSE with status icons:
   - `○` pending (gray-500), `●` in progress (indigo-400, pulse), `✓` done (emerald-400), `✗` error (rose-400)
3. When `type: "portfolios"` arrives → render table below progress
4. When `type: "done"` arrives → re-enable button

**SSE connection**:
```javascript
const eventSource = new EventSource(`${API_BASE}/api/airs/scan`);
eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  if (data.type === 'progress') setProgress(prev => [...prev, data]);
  else if (data.type === 'portfolios') setPortfolios(data.data);
  else if (data.type === 'done') { setScanning(false); eventSource.close(); }
  else if (data.type === 'error') { setError(data.message); setScanning(false); eventSource.close(); }
};
```

**Style**: Follow the existing design system (dark theme, card containers, indigo accents). Match the SSE progress pattern from the LongEquity ingest UI.

### What NOT to do
- Do NOT store anything in Supabase yet
- Do NOT persist portfolios — live scan every time
- Do NOT send credentials from frontend — backend .env only
- Do NOT use WebSockets — SSE matches existing patterns