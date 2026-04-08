import asyncio
import json
import os
import re
from datetime import date

from fastapi import FastAPI, UploadFile, File, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from supabase import create_client
from dotenv import load_dotenv

from portfolio import parse_airs_excel
from airs_scanner import scan_portfolios_sync, download_portfolio_sync

from ingest.acquire import acquire_raw_longequity_backfill, check_latest_available_month
from ingest.flatten import flatten_excel
from ingest.extend_primary import enrich_flattened_df_with_primary_listing
from ingest.transformation import prepare_flattened_for_schema
from ingest.load_into_supabase import (
    load_prepared_into_supabase,
    get_ticker_overrides,
    save_ticker_overrides,
    fix_company_primary_keys,
)
from ingest.resolve_tickers import detect_unknown_tickers, resolve_via_openfigi
from ingest.earnings import fetch_financials, fetch_analyst_estimates, fetch_indicators
from ingest.prices import ensure_prices_for_company

load_dotenv()

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_SERVICE_KEY"]
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://bbterminal.vercel.app",
        "https://bbterminal-api.vercel.app",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _as_of_date_from_filename(filename: str) -> date:
    m = re.search(
        r"(january|february|march|april|may|june|july|august|september|october|november|december)"
        r"[-_](\d{4})",
        filename.lower(),
    )
    if not m:
        raise ValueError(f"Could not parse month-year from filename: {filename}")
    month = _MONTHS[m.group(1)]
    year = int(m.group(2))
    return date(year, month, 1)


@app.delete("/api/auth/delete-account")
async def delete_account(authorization: str = Header(...)):
    """Delete the authenticated user's account."""
    token = authorization.replace("Bearer ", "")
    try:
        user_resp = supabase.auth.get_user(token)
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token verification failed: {e}")
    if not user_resp or not user_resp.user:
        raise HTTPException(status_code=401, detail="Invalid token — no user found")
    user_id = user_resp.user.id
    try:
        supabase.auth.admin.delete_user(user_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Admin delete failed: {e}")
    return {"ok": True}


@app.get("/api/hello")
def hello():
    return {"message": "Hello from FastAPI + uv!"}


@app.get("/api/health")
def health():
    """Check Supabase connectivity."""
    try:
        url = os.environ.get("SUPABASE_URL", "NOT SET")
        has_key = "YES" if os.environ.get("SUPABASE_SERVICE_KEY") else "NO"
        # Try a simple query
        resp = supabase.table("company").select("company_id").limit(1).execute()
        return {
            "status": "ok",
            "supabase_url": url,
            "has_service_key": has_key,
            "test_query": "success",
            "rows": len(resp.data or []),
        }
    except Exception as e:
        return {
            "status": "error",
            "supabase_url": os.environ.get("SUPABASE_URL", "NOT SET"),
            "has_service_key": "YES" if os.environ.get("SUPABASE_SERVICE_KEY") else "NO",
            "error": str(e),
        }


@app.get("/api/items")
def get_items():
    try:
        result = supabase.table("items").select("*").execute()
        return {"items": result.data}
    except Exception:
        return {"items": []}


async def _ingest_long_equity_stream():
    def event(msg_type: str, message: str) -> str:
        return f"data: {json.dumps({'type': msg_type, 'message': message})}\n\n"

    # ------------------------------------------------------------------ #
    # Check which months are already in the DB
    # ------------------------------------------------------------------ #
    existing_months = await asyncio.to_thread(_get_db_longequity_months)
    yield event("info", f"{len(existing_months)} month(s) already in DB.")

    # ------------------------------------------------------------------ #
    # Acquire files (only months not yet in DB)
    # ------------------------------------------------------------------ #
    yield event("info", "Acquiring Long Equity files (Storage → remote URL)...")
    try:
        all_files = await asyncio.to_thread(acquire_raw_longequity_backfill, supabase)
    except Exception as e:
        yield event("error", f"Acquire failed: {e}")
        return

    # Filter to only new months
    files: list[tuple[str, bytes]] = []
    for filename, content in all_files:
        try:
            as_of = _as_of_date_from_filename(filename)
            ym = f"{as_of.year:04d}-{as_of.month:02d}"
            if ym in existing_months:
                continue
            files.append((filename, content))
        except ValueError:
            files.append((filename, content))  # can't parse date — process anyway

    if not files:
        yield event("done", f"Pipeline finished — all {len(all_files)} month(s) already loaded.")
        return

    yield event("info", f"{len(files)} new file(s) to process (skipped {len(all_files) - len(files)} already in DB).")

    # ------------------------------------------------------------------ #
    # Load persisted ticker overrides once (shared across all files)
    # ------------------------------------------------------------------ #
    try:
        db_overrides: list[dict] = await asyncio.to_thread(get_ticker_overrides, supabase)
        yield event("info", f"Loaded {len(db_overrides)} ticker override(s) from DB.")
    except Exception as e:
        yield event("info", f"Could not load ticker overrides (table may not exist yet): {e}")
        db_overrides = []

    total_companies = 0
    total_metric_rows = 0
    all_new_resolutions: list[dict] = []

    # ------------------------------------------------------------------ #
    # Process each file
    # ------------------------------------------------------------------ #
    for i, (filename, content) in enumerate(files, 1):
        as_of = _as_of_date_from_filename(filename)

        yield event("info", "")
        yield event("info", f"[{i}/{len(files)}] {filename}  (as_of: {as_of})")

        # Flatten
        yield event("info", "  Flattening grouped headers...")
        try:
            df = await asyncio.to_thread(flatten_excel, content)
        except Exception as e:
            yield event("error", f"  Flatten failed: {e}")
            continue
        yield event("info", f"  {len(df)} rows, {len(df.columns)} columns")

        # Detect unknown tickers (not in fill_ticker.json or DB overrides)
        try:
            unknowns = await asyncio.to_thread(
                detect_unknown_tickers, df, db_overrides=db_overrides
            )
        except Exception as e:
            yield event("info", f"  Ticker detection failed (skipping): {e}")
            unknowns = []

        if unknowns:
            yield event("info", f"  {len(unknowns)} unknown ticker(s): {', '.join(u['ticker'] for u in unknowns)}")
            yield event("info", "  Resolving via OpenFIGI...")
            try:
                resolved = await asyncio.to_thread(resolve_via_openfigi, unknowns)
            except Exception as e:
                yield event("info", f"  OpenFIGI failed (continuing without): {e}")
                resolved = []

            if resolved:
                yield event("info", f"  Resolved {len(resolved)}/{len(unknowns)} ticker(s).")
                # Persist to DB
                try:
                    saved = await asyncio.to_thread(save_ticker_overrides, supabase, resolved)
                    if saved:
                        yield event("info", f"  Saved {saved} new resolution(s) to ticker_override table.")
                except Exception as e:
                    yield event("info", f"  Could not save resolutions to DB: {e}")

                # Update in-memory overrides so later files in this run benefit too
                db_overrides = db_overrides + resolved
                all_new_resolutions.extend(resolved)
            else:
                yield event("info", f"  Could not resolve {len(unknowns)} ticker(s) — will use fallback values.")
        else:
            yield event("info", "  All tickers covered by existing mappings.")

        # Enrich — pass all overrides (DB + newly resolved) as extra_overrides
        yield event("info", "  Enriching tickers...")
        try:
            df = await asyncio.to_thread(
                enrich_flattened_df_with_primary_listing, df,
                extra_overrides=db_overrides if db_overrides else None,
            )
        except Exception as e:
            yield event("error", f"  Enrich failed: {e}")
            continue

        # Transform
        yield event("info", "  Transforming to metric_data...")
        try:
            prepared = await asyncio.to_thread(
                prepare_flattened_for_schema, df,
                as_of_date=as_of, source_code="longequity",
            )
        except Exception as e:
            yield event("error", f"  Transform failed: {e}")
            continue
        yield event("info", f"  {len(prepared.company)} companies, {len(prepared.metric_data)} metric rows")

        # Load
        yield event("info", "  Loading into Supabase...")
        try:
            result = await asyncio.to_thread(load_prepared_into_supabase, prepared, supabase)
        except Exception as e:
            yield event("error", f"  Load failed: {e}")
            continue

        total_companies += result.company_inserted
        total_metric_rows += result.metric_data_inserted

        yield event("info", (
            f"  Inserted: {result.company_inserted} companies, "
            f"{result.metric_data_inserted} metric rows"
        ))

    # ------------------------------------------------------------------ #
    # Fix company rows that were loaded with primary_exchange='UNKNOWN'
    # in previous runs (before Phase 3 existed)
    # ------------------------------------------------------------------ #
    if all_new_resolutions:
        yield event("info", "")
        yield event("info", "Fixing company records from previous runs with UNKNOWN exchange...")
        try:
            fixed = await asyncio.to_thread(fix_company_primary_keys, supabase, all_new_resolutions)
            if fixed:
                yield event("info", f"  Fixed {fixed} company record(s).")
            else:
                yield event("info", "  No records needed fixing.")
        except Exception as e:
            yield event("info", f"  Fix step failed (non-critical): {e}")

    yield event("info", "")
    yield event("done", (
        f"Pipeline complete. {len(files)} file(s) processed. "
        f"Total new rows — companies: {total_companies}, "
        f"metric data: {total_metric_rows}."
    ))


def _get_db_longequity_months() -> set[str]:
    """Return set of 'YYYY-MM' strings already loaded in metric_data for longequity."""
    resp = supabase.rpc("get_distinct_dates", {"p_source_code": "longequity"}).execute()
    return {str(row["target_date"])[:7] for row in (resp.data or [])}


@app.get("/api/longequity/latest-available")
async def get_latest_available():
    try:
        spec = await asyncio.to_thread(check_latest_available_month, supabase=supabase)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"check_latest_available_month failed: {e}")
    if spec is None:
        return {"available": False, "year": None, "month": None}
    return {"available": True, "year": spec.year, "month": spec.month}


@app.get("/api/longequity/snapshots")
def get_longequity_snapshots():
    try:
        resp = supabase.rpc("get_distinct_dates", {"p_source_code": "longequity"}).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"get_longequity_snapshots failed: {e}")
    return [{"target_date": row["target_date"]} for row in (resp.data or [])]


@app.get("/api/longequity/companies")
def get_longequity_companies(target_date: str):
    # Fetch all companies upfront (small table ~400 rows)
    all_companies_resp = (
        supabase.table("company")
        .select("company_id,primary_ticker,primary_exchange,country,company_name,longequity_ticker")
        .limit(10000)
        .execute()
    )
    all_companies: dict[int, dict] = {
        c["company_id"]: c for c in (all_companies_resp.data or [])
    }

    def _company_ids_for_date(td: str) -> set[int]:
        resp = supabase.rpc("get_company_ids_for_date", {
            "p_source_code": "longequity",
            "p_target_date": td,
        }).execute()
        return {r["company_id"] for r in (resp.data or [])}

    current_ids = _company_ids_for_date(target_date)
    companies = [all_companies[cid] for cid in current_ids if cid in all_companies]

    # Find previous month for diff
    all_dates = supabase.rpc("get_distinct_dates", {"p_source_code": "longequity"}).execute()
    dates_list = [r["target_date"] for r in (all_dates.data or [])]
    prev_date = None
    for d in dates_list:
        if d < target_date:
            prev_date = d
        else:
            break

    added: list[dict] = []
    removed: list[dict] = []
    if prev_date:
        prev_ids = _company_ids_for_date(prev_date)
        added   = [all_companies[cid] for cid in (current_ids - prev_ids) if cid in all_companies]
        removed = [all_companies[cid] for cid in (prev_ids - current_ids) if cid in all_companies]

    return {"companies": companies, "added": added, "removed": removed}


@app.post("/api/ingest/long-equity")
async def ingest_long_equity():
    return StreamingResponse(
        _ingest_long_equity_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ─────────────────────────── AIRS endpoints ──────────────────────────────────


def _save_performance_to_db(portfolio_name: str, rows: list[dict]):
    """Upsert performance rows into airs_performance table."""
    if not rows:
        return
    for r in rows:
        supabase.table("airs_performance").upsert({
            "portefeuille": portfolio_name,
            "periode": r["periode"],
            "beginvermogen": r["beginvermogen"],
            "koersresultaat": r["koersresultaat"],
            "opbrengsten": r["opbrengsten"],
            "beleggingsresultaat": r["beleggingsresultaat"],
            "eindvermogen": r["eindvermogen"],
            "rendement": r["rendement"],
            "cumulatief_rendement": r["cumulatief_rendement"],
        }, on_conflict="portefeuille,periode").execute()


def _parse_att_excel(content: bytes) -> list[dict]:
    """Parse ATT Excel bytes into a list of performance row dicts."""
    import io
    import pandas as pd

    df = pd.read_excel(io.BytesIO(content), engine="xlrd")
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "periode": str(r.get("Periode", ""))[:10],
            "beginvermogen": round(float(r["Beginvermogen"]), 2) if pd.notna(r.get("Beginvermogen")) else None,
            "koersresultaat": round(float(r["Koersresultaat"]), 2) if pd.notna(r.get("Koersresultaat")) else None,
            "opbrengsten": round(float(r["Opbrengsten"]), 2) if pd.notna(r.get("Opbrengsten")) else None,
            "beleggingsresultaat": round(float(r["Beleggingsresultaat"]), 2) if pd.notna(r.get("Beleggingsresultaat")) else None,
            "eindvermogen": round(float(r["Eindvermogen"]), 2) if pd.notna(r.get("Eindvermogen")) else None,
            "rendement": round(float(r["Rendement"]), 6) if pd.notna(r.get("Rendement")) else None,
            "cumulatief_rendement": round(float(r["Cumulatief rendement"]), 6) if pd.notna(r.get("Cumulatief rendement")) else None,
        })
    return rows


@app.get("/api/airs/portfolios")
async def airs_portfolios_from_db():
    """Return portfolios we already have performance data for, with their latest YTD."""
    try:
        resp = await asyncio.to_thread(
            lambda: supabase.table("airs_performance")
            .select("portefeuille,cumulatief_rendement,periode,fetched_at")
            .order("portefeuille")
            .order("periode", desc=True)
            .execute()
        )
        # Dedupe to latest row per portfolio
        seen = {}
        for r in (resp.data or []):
            name = r["portefeuille"]
            if name not in seen:
                seen[name] = {
                    "portefeuille": name,
                    "cumulatief_rendement": r["cumulatief_rendement"],
                    "periode": r["periode"],
                    "fetched_at": r["fetched_at"],
                }
        return list(seen.values())
    except Exception:
        return []


async def _airs_scan_stream():
    import threading
    import queue as thread_queue

    q: thread_queue.Queue = thread_queue.Queue()
    def send_event(msg_type: str, **kwargs):
        payload = {"type": msg_type, **kwargs}
        q.put(f"data: {json.dumps(payload)}\n\n")

    def run_scanner():
        try:
            scan_portfolios_sync(send_event)
        except Exception as e:
            q.put(f"data: {json.dumps({'type': 'error', 'message': f'{type(e).__name__}: {e}'})}\n\n")
        finally:
            q.put(None)

    thread = threading.Thread(target=run_scanner, daemon=True)
    thread.start()

    while True:
        item = await asyncio.to_thread(q.get)
        if item is None:
            break
        yield item


@app.get("/api/airs/scan")
async def airs_scan():
    return StreamingResponse(
        _airs_scan_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/airs/portfolio/{portfolio_name}")
async def airs_portfolio_download(
    portfolio_name: str,
    datum_van: str | None = None,
    datum_tot: str | None = None,
    refresh: bool = False,
):
    """Return performance data. Serves from DB cache unless refresh=true or no cache."""
    from datetime import date as dt_date

    today = dt_date.today()
    if not datum_van:
        datum_van = f"{today.year}-01-01"
    if not datum_tot:
        datum_tot = today.isoformat()

    # Check what we have in DB
    db_rows = []
    needs_refresh = True
    try:
        resp = await asyncio.to_thread(
            lambda: supabase.table("airs_performance")
            .select("periode,beginvermogen,koersresultaat,opbrengsten,beleggingsresultaat,eindvermogen,rendement,cumulatief_rendement,fetched_at")
            .eq("portefeuille", portfolio_name)
            .order("periode")
            .execute()
        )
        db_rows = resp.data or []
        if db_rows and not refresh:
            # Fresh if the most recent row was fetched today
            last_fetched = db_rows[-1].get("fetched_at", "")[:10]
            needs_refresh = last_fetched != today.isoformat()
    except Exception:
        pass  # table may not exist yet

    # Download fresh data if needed
    if needs_refresh:
        try:
            content = await asyncio.to_thread(download_portfolio_sync, portfolio_name, datum_van, datum_tot)
            fresh_rows = await asyncio.to_thread(_parse_att_excel, content)
        except Exception as e:
            # If download fails but we have DB data, return that
            if db_rows:
                rows = [{k: v for k, v in r.items() if k != "fetched_at"} for r in db_rows]
                return {
                    "portfolio_name": portfolio_name,
                    "datum_van": datum_van,
                    "datum_tot": datum_tot,
                    "rows": rows,
                    "cached": True,
                }
            raise HTTPException(status_code=500, detail=f"Download failed: {e}")

        # Upsert fresh rows into DB (adds new periods, updates existing)
        try:
            await asyncio.to_thread(_save_performance_to_db, portfolio_name, fresh_rows)
        except Exception:
            pass

        # Re-read full history from DB
        try:
            resp = await asyncio.to_thread(
                lambda: supabase.table("airs_performance")
                .select("periode,beginvermogen,koersresultaat,opbrengsten,beleggingsresultaat,eindvermogen,rendement,cumulatief_rendement")
                .eq("portefeuille", portfolio_name)
                .order("periode")
                .execute()
            )
            return {
                "portfolio_name": portfolio_name,
                "datum_van": datum_van,
                "datum_tot": datum_tot,
                "rows": resp.data or fresh_rows,
                "cached": False,
            }
        except Exception:
            return {
                "portfolio_name": portfolio_name,
                "datum_van": datum_van,
                "datum_tot": datum_tot,
                "rows": fresh_rows,
                "cached": False,
            }

    # Cache is fresh — return full history from DB
    rows = [{k: v for k, v in r.items() if k != "fetched_at"} for r in db_rows]
    return {
        "portfolio_name": portfolio_name,
        "datum_van": datum_van,
        "datum_tot": datum_tot,
        "rows": rows,
        "cached": True,
    }


@app.post("/api/portfolios/parse")
async def parse_portfolio(file: UploadFile = File(...)):
    content = await file.read()
    try:
        holdings = await asyncio.to_thread(parse_airs_excel, content)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    total_start = sum(h.start_value_eur for h in holdings if h.start_value_eur is not None)
    total_current = sum(h.current_value_eur for h in holdings if h.current_value_eur is not None)
    total_ytd_eur = round(total_current - total_start, 2) if total_start else None
    total_ytd_pct = round((total_current - total_start) / abs(total_start), 6) if total_start else None

    return {
        "holdings": [
            {
                "holding_name": h.holding_name,
                "quantity": h.quantity,
                "currency": h.currency,
                "weight": h.weight,
                "start_value_eur": h.start_value_eur,
                "current_value_eur": h.current_value_eur,
                "ytd_return_eur": h.ytd_return_eur,
                "ytd_return_pct": h.ytd_return_pct,
                "ytd_return_local_pct": h.ytd_return_local_pct,
            }
            for h in holdings
        ],
        "total_start_eur": round(total_start, 2) if total_start else None,
        "total_current_eur": round(total_current, 2) if total_current else None,
        "total_ytd_eur": total_ytd_eur,
        "total_ytd_pct": total_ytd_pct,
    }


@app.get("/api/companies/field-options")
async def get_company_field_options():
    resp = (
        supabase.table("company")
        .select("primary_exchange,country,sector")
        .limit(10000)
        .execute()
    )
    rows = resp.data or []
    exchanges = sorted({r["primary_exchange"] for r in rows if r.get("primary_exchange")})
    countries = sorted({r["country"] for r in rows if r.get("country") and r["country"].strip()})
    sectors = sorted({r["sector"] for r in rows if r.get("sector") and r["sector"].strip()})
    return {"exchanges": exchanges, "countries": countries, "sectors": sectors}


class CreateCompanyRequest(BaseModel):
    company_name: str
    primary_ticker: str
    primary_exchange: str
    country: str = ""
    sector: str = ""


class UpdateCompanyRequest(BaseModel):
    company_name: str | None = None
    primary_ticker: str | None = None
    primary_exchange: str | None = None
    country: str | None = None
    sector: str | None = None


@app.get("/api/companies")
async def list_companies():
    resp = (
        supabase.table("company")
        .select("company_id,company_name,primary_ticker,primary_exchange,longequity_ticker,country,sector")
        .order("company_name")
        .limit(10000)
        .execute()
    )
    return resp.data or []


@app.post("/api/companies")
async def create_company(req: CreateCompanyRequest):
    row = {
        "company_name": req.company_name,
        "primary_ticker": req.primary_ticker.upper(),
        "primary_exchange": req.primary_exchange.upper(),
        "country": req.country or None,
        "sector": req.sector or None,
    }
    resp = supabase.table("company").insert(row).execute()
    if not resp.data:
        raise HTTPException(status_code=400, detail="Insert failed")
    return resp.data[0]


@app.put("/api/companies/{company_id}")
async def update_company(company_id: int, req: UpdateCompanyRequest):
    updates = {}
    if req.company_name is not None:
        updates["company_name"] = req.company_name
    if req.primary_ticker is not None:
        updates["primary_ticker"] = req.primary_ticker.upper()
    if req.primary_exchange is not None:
        updates["primary_exchange"] = req.primary_exchange.upper()
    if req.country is not None:
        updates["country"] = req.country or None
    if req.sector is not None:
        updates["sector"] = req.sector or None
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    resp = (
        supabase.table("company")
        .update(updates)
        .eq("company_id", company_id)
        .execute()
    )
    if not resp.data:
        raise HTTPException(status_code=404, detail="Company not found")
    return resp.data[0]


@app.delete("/api/companies/{company_id}")
async def delete_company(company_id: int):
    # Cascade: delete referencing rows first
    supabase.table("portfolio_weight").delete().eq("company_id", company_id).execute()
    supabase.table("metric_data").delete().eq("company_id", company_id).execute()
    supabase.table("company").delete().eq("company_id", company_id).execute()
    return {"ok": True}


# ─────────────────────────── Earnings endpoints ────────────────────────────

def _get_company_or_404(company_id: int) -> dict:
    resp = (
        supabase.table("company")
        .select("company_id,primary_ticker,primary_exchange,company_name")
        .eq("company_id", company_id)
        .limit(1)
        .execute()
    )
    if not resp.data:
        raise HTTPException(status_code=404, detail="Company not found")
    return resp.data[0]


async def _earnings_refresh_stream(company_id: int, sources: list[str], force: bool):
    """SSE stream for earnings data refresh."""
    def event(msg_type: str, message: str) -> str:
        return f"data: {json.dumps({'type': msg_type, 'message': message})}\n\n"

    company = _get_company_or_404(company_id)
    ticker = company["primary_ticker"]
    exchange = company["primary_exchange"]
    name = company.get("company_name") or f"{ticker}.{exchange}"

    yield event("info", f"Refreshing earnings data for {name} ({ticker}.{exchange})")

    for source in sources:
        yield event("info", f"")
        yield event("info", f"--- {source.upper()} ---")

        try:
            if source == "financials":
                r = await asyncio.to_thread(
                    fetch_financials, supabase, company_id, ticker, exchange,
                    force_refresh=force,
                )
            elif source == "analyst_estimates":
                r = await asyncio.to_thread(
                    fetch_analyst_estimates, supabase, company_id, ticker, exchange,
                    force_refresh=force,
                )
            elif source == "indicators":
                r = await asyncio.to_thread(
                    fetch_indicators, supabase, company_id, ticker, exchange,
                    force_refresh=force,
                )
            elif source == "prices":
                pr = await asyncio.to_thread(
                    ensure_prices_for_company, supabase, company_id, ticker, exchange,
                    force_refresh=force,
                )
                for log_line in pr.logs:
                    yield event("info", f"  {log_line}")
                if pr.error:
                    yield event("error", f"  Error: {pr.error}")
                else:
                    yield event("info", f"  Result: {pr.rows_loaded} rows loaded, {pr.total_prices} total prices")
                continue
            else:
                yield event("error", f"Unknown source: {source}")
                continue

            for log_line in r.logs:
                yield event("info", f"  {log_line}")

            if r.error:
                yield event("error", f"  Error: {r.error}")
            else:
                yield event("info", f"  Result: {r.rows_loaded} rows loaded, {r.metrics_found} metrics")

        except Exception as e:
            yield event("error", f"  {source} failed: {e}")

    yield event("info", "")
    yield event("done", "Earnings refresh complete.")


@app.post("/api/earnings/{company_id}/refresh/{source}")
async def refresh_earnings_source(company_id: int, source: str, force: bool = False):
    """Refresh a single earnings data source. SSE stream."""
    valid = {"financials", "analyst_estimates", "indicators", "prices"}
    if source not in valid:
        raise HTTPException(status_code=400, detail=f"source must be one of {valid}")
    return StreamingResponse(
        _earnings_refresh_stream(company_id, [source], force),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/earnings/{company_id}/refresh-all")
async def refresh_earnings_all(company_id: int, force: bool = False):
    """Refresh all earnings data sources. SSE stream."""
    return StreamingResponse(
        _earnings_refresh_stream(
            company_id, ["financials", "analyst_estimates", "indicators", "prices"], force
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


_DASHBOARD_METRIC_CODES = [
    # Financials — Per Share Data
    "annuals__Per Share Data__Month End Stock Price",
    "annuals__Per Share Data__EPS without NRI",
    "annuals__Per Share Data__Dividends per Share",
    "annuals__Per Share Data__Free Cash Flow per Share",
    "annuals__Per Share Data__Earnings per Share (Diluted)",
    # Financials — Balance Sheet
    "annuals__Balance Sheet__Debt-to-Equity",
    # Financials — Ratios
    "annuals__Ratios__Capex-to-Revenue",
    "annuals__Ratios__Capex-to-Operating-Cash-Flow",
    # Financials — Cashflow / Income
    "annuals__Cashflow Statement__Free Cash Flow",
    "annuals__Income Statement__Net Income",
    "annuals__Income Statement__EPS (Diluted)",
    # Financials — Valuation
    "annuals__Valuation Ratios__FCF Yield %",
    # Indicators (quarterly)
    "indicator_q_interest_coverage",
    "indicator_q_roe",
    "indicator_q_roic",
    "indicator_q_gross_margin",
    "indicator_q_net_margin",
    "indicator_q_forward_pe_ratio",
    "indicator_q_peg_ratio",
    "indicator_q_fcf_yield",
    # Daily close prices
    "close_price",
    # Analyst estimates (annual_*)
    # These are fetched with a prefix filter below
]


@app.get("/api/earnings/{company_id}/metrics")
async def get_earnings_metrics(company_id: int):
    """Get dashboard metrics for a company (source=gurufocus, dates >= 2015)."""
    try:
        # Fetch non-price metric codes (low volume, fits in one page)
        non_price_codes = [c for c in _DASHBOARD_METRIC_CODES if c != "close_price"]
        resp = (
            supabase.table("metric_data")
            .select("metric_code,target_date,numeric_value,is_prediction")
            .eq("company_id", company_id)
            .eq("source_code", "gurufocus")
            .gte("target_date", "2015-01-01")
            .in_("metric_code", non_price_codes)
            .order("target_date")
            .limit(5000)
            .execute()
        )
        rows = resp.data or []

        # Fetch daily close prices separately (can be thousands of rows)
        offset = 0
        page_size = 1000
        while True:
            page = (
                supabase.table("metric_data")
                .select("metric_code,target_date,numeric_value,is_prediction")
                .eq("company_id", company_id)
                .eq("source_code", "gurufocus")
                .eq("metric_code", "close_price")
                .gte("target_date", "2015-01-01")
                .order("target_date")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            batch = page.data or []
            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

        # Also fetch analyst estimates (annual_* prefix)
        resp2 = (
            supabase.table("metric_data")
            .select("metric_code,target_date,numeric_value,is_prediction")
            .eq("company_id", company_id)
            .eq("source_code", "gurufocus")
            .eq("is_prediction", True)
            .gte("target_date", "2015-01-01")
            .like("metric_code", "annual_%")
            .order("target_date")
            .limit(2000)
            .execute()
        )
        rows.extend(resp2.data or [])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")
    return rows


@app.get("/api/earnings/{company_id}/metric-codes")
async def get_earnings_metric_codes(company_id: int):
    """Debug: list distinct metric codes stored for a company."""
    try:
        resp = (
            supabase.table("metric_data")
            .select("metric_code")
            .eq("company_id", company_id)
            .eq("source_code", "gurufocus")
            .limit(10000)
            .execute()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")
    codes = sorted(set(r["metric_code"] for r in (resp.data or [])))
    return {"count": len(codes), "codes": codes}


