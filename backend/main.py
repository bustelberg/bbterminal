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
    merge_duplicate_companies,
)
from ingest.resolve_tickers import detect_unknown_tickers, resolve_via_openfigi
from ingest.earnings import fetch_financials, fetch_analyst_estimates, fetch_indicators
from ingest.prices import ensure_prices_for_company, ensure_volume_for_company, PriceResult, _fetch_price_from_api, _parse_price_series
from ingest.api_usage import track_api_call, get_usage
from momentum.data import load_universe, load_all_prices, load_all_volumes
from momentum.signals import PRICE_SIGNAL_DEFS
from momentum.backtest import BacktestConfig, run_backtest
from universe.screen import screen_universe, build_and_store_universes, validate_vs_longequity
from universe.criteria import CRITERIA_NAMES, CRITERIA_DESCRIPTIONS, CRITERIA_MIN_YEARS

load_dotenv()

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_SERVICE_KEY"]
)

app = FastAPI()

_cors_origins = [
    "http://localhost:3000",
    "https://bbterminal.vercel.app",
    "https://bbterminal-api.vercel.app",
]
if os.environ.get("RAILWAY_PUBLIC_DOMAIN"):
    _cors_origins.append(f"https://{os.environ['RAILWAY_PUBLIC_DOMAIN']}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
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

    # ------------------------------------------------------------------ #
    # Merge duplicate companies (same name + exchange, different ticker)
    # ------------------------------------------------------------------ #
    yield event("info", "")
    yield event("info", "Checking for duplicate companies...")
    try:
        merge_logs = await asyncio.to_thread(merge_duplicate_companies, supabase)
        if merge_logs:
            for msg in merge_logs:
                yield event("info", f"  {msg}")
        else:
            yield event("info", "  No duplicates found.")
    except Exception as e:
        yield event("info", f"  Dedup step failed (non-critical): {e}")

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
    import queue as _queue

    def event(msg_type: str, message: str, **extra) -> str:
        payload = {"type": msg_type, "message": message, **extra}
        return f"data: {json.dumps(payload)}\n\n"

    company = _get_company_or_404(company_id)
    ticker = company["primary_ticker"]
    exchange = company["primary_exchange"]
    name = company.get("company_name") or f"{ticker}.{exchange}"
    region = "usa" if exchange.upper() in {"NYSE", "NASDAQ", "AMEX"} else "europe"

    yield event("info", f"Refreshing earnings data for {name} ({ticker}.{exchange})")

    for source in sources:
        yield event("info", f"")
        yield event("info", f"--- {source.upper()} ---")

        try:
            # Use a queue so on_log callbacks stream to SSE in real-time
            log_q: _queue.Queue[str | None] = _queue.Queue()

            def on_log(msg: str):
                log_q.put(msg)

            async def drain_queue():
                """Yield SSE events for any queued log messages."""
                events = []
                while not log_q.empty():
                    try:
                        msg = log_q.get_nowait()
                        if msg is not None:
                            events.append(event("info", f"  {msg}"))
                    except _queue.Empty:
                        break
                return events

            if source == "financials":
                task = asyncio.get_event_loop().run_in_executor(
                    None, lambda: fetch_financials(
                        supabase, company_id, ticker, exchange,
                        force_refresh=force, on_log=on_log,
                    ))
            elif source == "analyst_estimates":
                task = asyncio.get_event_loop().run_in_executor(
                    None, lambda: fetch_analyst_estimates(
                        supabase, company_id, ticker, exchange,
                        force_refresh=force, on_log=on_log,
                    ))
            elif source == "indicators":
                task = asyncio.get_event_loop().run_in_executor(
                    None, lambda: fetch_indicators(
                        supabase, company_id, ticker, exchange,
                        force_refresh=force, on_log=on_log,
                    ))
            elif source == "prices":
                task = asyncio.get_event_loop().run_in_executor(
                    None, lambda: ensure_prices_for_company(
                        supabase, company_id, ticker, exchange,
                        force_refresh=force, on_log=on_log,
                    ))
            else:
                yield event("error", f"Unknown source: {source}")
                continue

            # Poll the queue while the task runs, yielding SSE events in real-time
            while not task.done():
                await asyncio.sleep(0.15)
                for evt in await drain_queue():
                    yield evt
            # Drain remaining messages after task completes
            for evt in await drain_queue():
                yield evt

            r = task.result()

            if source == "prices":
                if r.error:
                    yield event("error", f"  Error: {r.error}")
                else:
                    yield event("info", f"  Result: {r.rows_loaded} rows loaded, {r.total_prices} total prices")
            else:
                if r.error:
                    yield event("error", f"  Error: {r.error}")
                else:
                    yield event("info", f"  Result: {r.rows_loaded} rows loaded, {r.metrics_found} metrics")

            if r.api_calls > 0:
                yield event("api_calls", f"{r.api_calls} API call(s)", region=region, count=r.api_calls)

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
    "annuals__Valuation Ratios__Dividend Yield %",
    # Financials — Ratios (WACC / returns)
    "annuals__Ratios__WACC %",
    "annuals__Ratios__ROIC %",
    # Financials — Income Statement
    "annuals__Income Statement__Tax Rate %",
    # Financials — Valuation and Quality
    "annuals__Valuation and Quality__Net Cash per Share",
    "annuals__Valuation and Quality__Intrinsic Value: Projected FCF",
    "annuals__Valuation and Quality__Beta",
    "annuals__Valuation and Quality__Piotroski F-Score",
    "annuals__Valuation and Quality__Altman Z-Score",
    "annuals__Valuation and Quality__Shares Buyback Ratio %",
    "annuals__Valuation and Quality__YoY Rev. per Sh. Growth",
    "annuals__Valuation and Quality__5-Year EBITDA Growth Rate (Per Share)",
    "annuals__Valuation and Quality__YoY EPS Growth",
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

_LONGEQUITY_METRIC_CODES = [
    "share_price_5yr_cagr",
    "share_price_5yr_rsq",
    "share_price_10yr_cagr",
    "share_price_10yr_rsq",
    "revenue_growth_5yr",
    "revenue_growth_rsq",
    "fcf_growth_5yr",
    "fcf_growth_sd",
    "fcf_growth_rsq",
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

        # Fetch LongEquity metrics
        resp3 = (
            supabase.table("metric_data")
            .select("metric_code,target_date,numeric_value,is_prediction")
            .eq("company_id", company_id)
            .eq("source_code", "longequity")
            .in_("metric_code", _LONGEQUITY_METRIC_CODES)
            .order("target_date")
            .limit(1000)
            .execute()
        )
        rows.extend(resp3.data or [])
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


# ─────────────────────────── Momentum Backtest ─────────────────────────────


@app.get("/api/momentum/signals")
async def get_momentum_signals():
    """Return available signal definitions and categories for the frontend."""
    from momentum.scoring import _get_category_keys
    categories = list(_get_category_keys().keys())
    return {"signals": PRICE_SIGNAL_DEFS, "categories": categories}


_DEFAULT_END = "2026-01-01"
_DEFAULT_START = "2017-01-01"


class BacktestRequest(BaseModel):
    start_date: str = _DEFAULT_START
    end_date: str = _DEFAULT_END  # also used as data cutoff — no data newer than this
    signal_weights: dict[str, float] | None = None
    category_weights: dict[str, float] | None = None  # e.g. {"price": 50, "volume": 50}
    top_n_sectors: int = 4
    top_n_per_sector: int = 6
    skip_price_fetch: bool = False  # skips both price and volume fetch
    max_companies: int = 0  # 0 = all, otherwise limit universe (alphabetical)
    universe_label: str | None = None  # if set, use universe_snapshot for per-month filtering


async def _momentum_backtest_stream(req: BacktestRequest):
    """SSE generator for the momentum backtest."""
    def _emit(data: dict) -> str:
        return f"data: {json.dumps(data)}\n\n"

    def _keepalive() -> str:
        return ": keepalive\n\n"

    try:
        yield _emit({"type": "progress", "pct": 0, "message": "Loading universe..."})
        universe_df = await asyncio.to_thread(load_universe, supabase)
        if universe_df.empty:
            yield _emit({"type": "error", "message": "No companies found in database"})
            return
        yield _emit({"type": "progress", "pct": 5, "message": f"Found {len(universe_df)} companies"})

        # Load universe snapshot if a label is specified
        monthly_eligible: dict[str, set[int]] | None = None
        if req.universe_label:
            yield _emit({"type": "progress", "pct": 6, "message": f"Loading universe snapshot '{req.universe_label}'..."})

            def _load_snapshot():
                rows = []
                offset = 0
                while True:
                    resp = supabase.table("universe_snapshot").select(
                        "target_month, company_id"
                    ).eq("label", req.universe_label).eq(
                        "passes", True
                    ).range(offset, offset + 999).execute()
                    batch = resp.data or []
                    rows.extend(batch)
                    if len(batch) < 1000:
                        break
                    offset += 1000
                result: dict[str, set[int]] = {}
                for r in rows:
                    m = r["target_month"]
                    if m not in result:
                        result[m] = set()
                    result[m].add(r["company_id"])
                return result

            monthly_eligible = await asyncio.to_thread(_load_snapshot)
            n_months = len(monthly_eligible)
            if n_months == 0:
                yield _emit({"type": "error", "message": f"No universe snapshot data for label '{req.universe_label}'"})
                return
            avg_pass = sum(len(v) for v in monthly_eligible.values()) // n_months
            yield _emit({"type": "progress", "pct": 7, "message": f"Universe snapshot: {n_months} months, ~{avg_pass} passing/month"})

        config = BacktestConfig.from_dict({
            "start_date": req.start_date,
            "end_date": req.end_date,
            "signal_weights": req.signal_weights or {s["key"]: s["default_weight"] for s in PRICE_SIGNAL_DEFS},
            "category_weights": req.category_weights,
            "top_n_sectors": req.top_n_sectors,
            "top_n_per_sector": req.top_n_per_sector,
        })

        data_cutoff = date.fromisoformat(req.end_date)

        excluded_ids: set[int] = set()

        if req.skip_price_fetch:
            yield _emit({"type": "progress", "pct": 60, "message": "Skipping data fetch (using existing DB prices & volumes)"})
        else:
            # Ensure price data exists for every company (fetch from GuruFocus if missing)
            total_companies = len(universe_df)
            blocked_exchanges: set[str] = set()
            skipped_count = 0
            ok_count = 0
            max_ok = req.max_companies if req.max_companies > 0 else 0  # 0 = unlimited
            yield _emit({"type": "progress", "pct": 5, "message": f"Ensuring price & volume data for {total_companies} companies (cutoff: {data_cutoff})..."})
            for idx, (_, row) in enumerate(universe_df.iterrows()):
                pct = 5 + round((idx / total_companies) * 55)
                ticker = row["primary_ticker"]
                exchange = row["primary_exchange"]
                symbol = f"{exchange}:{ticker}"

                # Skip companies on blocked exchanges
                if exchange in blocked_exchanges:
                    skipped_count += 1
                    excluded_ids.add(int(row["company_id"]))
                    continue

                yield _emit({"type": "progress", "pct": pct, "message": f"Data: {symbol} ({idx + 1}/{total_companies})"})
                try:
                    cid = int(row["company_id"])
                    yield _keepalive()
                    pr = await asyncio.to_thread(
                        ensure_prices_for_company,
                        supabase, cid, ticker, exchange,
                        data_cutoff=data_cutoff,
                    )
                    # Also fetch volume (non-blocking on failure)
                    vr = None
                    try:
                        yield _keepalive()
                        vr = await asyncio.to_thread(
                            ensure_volume_for_company,
                            supabase, cid, ticker, exchange,
                            data_cutoff=data_cutoff,
                        )
                    except Exception as ve:
                        vr = PriceResult()
                        vr.source = "error"
                        vr.error = str(ve)
                    if pr.is_forbidden:
                        blocked_exchanges.add(exchange)
                        excluded_ids.add(cid)
                        yield _emit({"type": "progress", "pct": pct, "message": f"  {symbol}: unsubscribed region — skipping all {exchange} companies"})
                    elif pr.is_delisted:
                        excluded_ids.add(cid)
                        # Remove delisted companies from DB
                        await asyncio.to_thread(
                            lambda: (
                                supabase.table("metric_data").delete().eq("company_id", cid).execute(),
                                supabase.table("portfolio_weight").delete().eq("company_id", cid).execute(),
                                supabase.table("company").delete().eq("company_id", cid).execute(),
                            )
                        )
                        yield _emit({"type": "progress", "pct": pct, "message": f"  {symbol}: DELISTED — removed from database"})
                    else:
                        ok_count += 1
                        # Build price detail
                        parts: list[str] = []
                        if pr.source == "cache":
                            parts.append(f"price: cache ({pr.rows_loaded})")
                        elif pr.source == "api":
                            parts.append(f"price: API ({pr.rows_loaded})")
                        elif pr.source == "stale_cache":
                            parts.append(f"price: stale cache ({pr.rows_loaded})")
                        elif pr.source == "none":
                            parts.append(f"price: none")
                        else:
                            parts.append(f"price: {pr.source}")
                        # Build volume detail
                        if vr:
                            if vr.source == "cache":
                                parts.append(f"vol: cache ({vr.rows_loaded})")
                            elif vr.source == "api":
                                parts.append(f"vol: API ({vr.rows_loaded})")
                            elif vr.source == "stale_cache":
                                parts.append(f"vol: stale cache ({vr.rows_loaded})")
                            elif vr.source == "error":
                                parts.append(f"vol: error ({vr.error})")
                            else:
                                parts.append(f"vol: none ({vr.error or 'unknown'})")
                        else:
                            parts.append("vol: failed")
                        yield _emit({"type": "progress", "pct": pct, "message": f"  {symbol}: {' | '.join(parts)}"})
                        # Stop once we have enough valid companies
                        if max_ok and ok_count >= max_ok:
                            # Mark remaining companies as excluded
                            remaining_ids = set(universe_df["company_id"].iloc[idx + 1:].astype(int))
                            excluded_ids.update(remaining_ids - {cid for cid in excluded_ids})
                            yield _emit({"type": "progress", "pct": 60, "message": f"Reached {ok_count} valid companies — stopping data fetch"})
                            break
                except Exception as e:
                    yield _emit({"type": "progress", "pct": pct, "message": f"  {symbol}: error — {e}"})

            if blocked_exchanges:
                yield _emit({"type": "progress", "pct": 60, "message": f"Blocked exchanges (unsubscribed): {', '.join(sorted(blocked_exchanges))} — {skipped_count} companies skipped"})

        # Remove excluded companies (blocked exchanges, delisted) from universe
        if excluded_ids:
            universe_df = universe_df[~universe_df["company_id"].isin(excluded_ids)].reset_index(drop=True)
            yield _emit({"type": "progress", "pct": 61, "message": f"Universe after exclusions: {len(universe_df)} companies"})

        # Optionally limit universe size (alphabetical by ticker) — applied after exclusions
        if req.max_companies > 0 and len(universe_df) > req.max_companies:
            universe_df = universe_df.sort_values("primary_ticker").head(req.max_companies).reset_index(drop=True)
            yield _emit({"type": "progress", "pct": 61, "message": f"Limited to {len(universe_df)} companies (alphabetical)"})

        company_ids = universe_df["company_id"].tolist()

        # Load all prices in bulk — capped at data_cutoff
        from datetime import timedelta
        price_start = date.fromisoformat(req.start_date) - timedelta(days=300)
        price_end = date.fromisoformat(req.end_date) + timedelta(days=35)

        yield _emit({"type": "progress", "pct": 62, "message": f"Loading prices from DB ({price_start} to {price_end}, starts early for 200-day MA)..."})
        yield _keepalive()

        # Use a list to collect progress from the sync loader thread
        load_progress: list[dict] = []
        def _on_load_progress(rows_so_far: int, page_num: int):
            load_progress.append({"rows": rows_so_far, "page": page_num})

        prices_df = await asyncio.to_thread(
            load_all_prices, supabase, company_ids, price_start, price_end,
            on_progress=_on_load_progress,
        )

        # Stream the collected page progress
        for lp in load_progress:
            pct = 62 + min(3, round(lp["rows"] / max(1, len(load_progress) * 1000) * 3))
            yield _emit({"type": "progress", "pct": pct, "message": f"  DB page {lp['page']}: {lp['rows']:,} rows loaded so far..."})

        if prices_df.empty:
            yield _emit({"type": "error", "message": "No price data found after ingestion."})
            return

        n_companies_with_prices = prices_df["company_id"].nunique()
        yield _emit({"type": "progress", "pct": 65, "message": f"Loaded {len(prices_df):,} prices for {n_companies_with_prices} companies"})

        # Load volumes from DB
        yield _emit({"type": "progress", "pct": 66, "message": "Loading volumes from DB..."})
        yield _keepalive()
        volumes_df = await asyncio.to_thread(
            load_all_volumes, supabase, company_ids, price_start, price_end,
        )
        n_vol = volumes_df["company_id"].nunique() if not volumes_df.empty else 0
        yield _emit({"type": "progress", "pct": 67, "message": f"Loaded {len(volumes_df):,} volume records for {n_vol} companies"})

        # Run backtest with progress callback
        events: list[dict] = []

        def send_event(event_type: str, **kwargs):
            events.append({"type": event_type, **kwargs})

        yield _emit({"type": "progress", "pct": 68, "message": "Running backtest computation..."})
        yield _keepalive()
        result = await asyncio.to_thread(
            run_backtest, config, prices_df, universe_df, send_event,
            volumes_df=volumes_df,
            monthly_eligible=monthly_eligible,
        )

        # Stream collected progress events
        for evt in events:
            if evt["type"] == "progress":
                scaled_pct = 65 + round(evt.get("pct", 0) * 0.30)
                yield _emit({"type": "progress", "pct": scaled_pct, "message": evt.get("message", "")})

        # Build universe snapshot for saving
        universe_snapshot = [
            {
                "company_id": int(row["company_id"]),
                "ticker": str(row["primary_ticker"]),
                "exchange": str(row["primary_exchange"]),
                "company_name": str(row.get("company_name", "")),
                "sector": str(row.get("sector", "")),
            }
            for _, row in universe_df.iterrows()
        ]

        yield _emit({"type": "result", "data": result.to_dict(), "universe": universe_snapshot})
        yield _emit({"type": "done", "message": "Backtest complete"})

    except Exception as e:
        yield _emit({"type": "error", "message": str(e)})


@app.post("/api/momentum/backtest")
async def momentum_backtest(req: BacktestRequest):
    return StreamingResponse(
        _momentum_backtest_stream(req),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ─── Backtest save / load ────────────────────────────────────────────────────


class SaveBacktestRequest(BaseModel):
    name: str
    config: dict
    summary: dict
    monthly_records: list
    universe: list  # [{company_id, ticker, exchange, company_name, sector}]


@app.post("/api/momentum/backtests")
async def save_backtest(req: SaveBacktestRequest):
    """Save a backtest run to the database."""
    row = {
        "name": req.name.strip(),
        "config": req.config,
        "summary": req.summary,
        "monthly_records": req.monthly_records,
        "universe": req.universe,
    }
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run").insert(row).execute()
    )
    if not resp.data:
        raise HTTPException(500, "Failed to save backtest")
    return resp.data[0]


@app.get("/api/momentum/backtests")
async def list_backtests():
    """List saved backtests (metadata only, no monthly_records)."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .select("run_id, name, created_at, config, summary")
        .order("created_at", desc=True)
        .execute()
    )
    return resp.data


@app.get("/api/momentum/backtests/{run_id}")
async def load_backtest(run_id: int):
    """Load a full backtest run."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .select("*")
        .eq("run_id", run_id)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Backtest not found")
    return resp.data[0]


@app.delete("/api/momentum/backtests/{run_id}")
async def delete_backtest(run_id: int):
    """Delete a saved backtest run."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .delete()
        .eq("run_id", run_id)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Backtest not found")
    return {"ok": True}


# ─── Benchmarks ───────────────────────────────────────────────────────────────


class CreateBenchmarkRequest(BaseModel):
    ticker: str
    name: str


@app.get("/api/benchmarks")
async def list_benchmarks():
    """List all benchmarks with price date range."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("benchmark")
        .select("benchmark_id, ticker, name, created_at")
        .order("name")
        .execute()
    )
    benchmarks = resp.data
    for b in benchmarks:
        bid = b["benchmark_id"]
        min_resp = await asyncio.to_thread(
            lambda bid=bid: supabase.table("benchmark_price")
            .select("target_date")
            .eq("benchmark_id", bid)
            .order("target_date")
            .limit(1)
            .execute()
        )
        max_resp = await asyncio.to_thread(
            lambda bid=bid: supabase.table("benchmark_price")
            .select("target_date")
            .eq("benchmark_id", bid)
            .order("target_date", desc=True)
            .limit(1)
            .execute()
        )
        b["price_from"] = min_resp.data[0]["target_date"] if min_resp.data else None
        b["price_to"] = max_resp.data[0]["target_date"] if max_resp.data else None
    return benchmarks


@app.post("/api/benchmarks")
async def create_benchmark(req: CreateBenchmarkRequest):
    """Create a benchmark and fetch its prices from GuruFocus."""
    ticker = req.ticker.strip().upper()
    name = req.name.strip()
    if not ticker or not name:
        raise HTTPException(400, "Ticker and name are required")

    # Check for duplicate
    existing = await asyncio.to_thread(
        lambda: supabase.table("benchmark").select("benchmark_id").eq("ticker", ticker).execute()
    )
    if existing.data:
        raise HTTPException(409, f"Benchmark {ticker} already exists")

    # Fetch prices from GuruFocus (ETFs are US-listed, no exchange prefix needed)
    data, log, status = await asyncio.to_thread(_fetch_price_from_api, ticker, "NYSE")
    await asyncio.to_thread(track_api_call, supabase, "NYSE")
    if data is None:
        raise HTTPException(502, f"Failed to fetch prices for {ticker}: {log}")

    parsed = _parse_price_series(data)
    if not parsed:
        raise HTTPException(502, f"No prices parsed for {ticker}")

    # Create benchmark record
    resp = await asyncio.to_thread(
        lambda: supabase.table("benchmark").insert({"ticker": ticker, "name": name}).execute()
    )
    if not resp.data:
        raise HTTPException(500, "Failed to create benchmark")
    benchmark_id = resp.data[0]["benchmark_id"]

    # Load prices (honour the same 2015-01-01 cutoff as company prices)
    _BM_CUTOFF = date(2015, 1, 1)
    rows = [
        {"benchmark_id": benchmark_id, "target_date": d.isoformat(), "price": p}
        for d, p in parsed
        if d >= _BM_CUTOFF
    ]
    batch_size = 500
    total_loaded = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        await asyncio.to_thread(
            lambda b=batch: supabase.table("benchmark_price")
            .upsert(b, on_conflict="benchmark_id,target_date")
            .execute()
        )
        total_loaded += len(batch)

    return {**resp.data[0], "prices_loaded": total_loaded, "price_range": f"{parsed[0][0]} to {parsed[-1][0]}"}


@app.post("/api/benchmarks/{benchmark_id}/refresh")
async def refresh_benchmark(benchmark_id: int):
    """Re-fetch prices for an existing benchmark."""
    bm = await asyncio.to_thread(
        lambda: supabase.table("benchmark").select("*").eq("benchmark_id", benchmark_id).execute()
    )
    if not bm.data:
        raise HTTPException(404, "Benchmark not found")
    ticker = bm.data[0]["ticker"]

    data, log, status = await asyncio.to_thread(_fetch_price_from_api, ticker, "NYSE")
    await asyncio.to_thread(track_api_call, supabase, "NYSE")
    if data is None:
        raise HTTPException(502, f"Failed to fetch prices: {log}")

    parsed = _parse_price_series(data)
    if not parsed:
        raise HTTPException(502, f"No prices parsed for {ticker}")

    _BM_CUTOFF = date(2015, 1, 1)
    rows = [
        {"benchmark_id": benchmark_id, "target_date": d.isoformat(), "price": p}
        for d, p in parsed
        if d >= _BM_CUTOFF
    ]
    batch_size = 500
    total_loaded = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        await asyncio.to_thread(
            lambda b=batch: supabase.table("benchmark_price")
            .upsert(b, on_conflict="benchmark_id,target_date")
            .execute()
        )
        total_loaded += len(batch)

    return {"ticker": ticker, "prices_loaded": total_loaded}


@app.delete("/api/benchmarks/{benchmark_id}")
async def delete_benchmark(benchmark_id: int):
    """Delete a benchmark and its prices (cascade)."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("benchmark").delete().eq("benchmark_id", benchmark_id).execute()
    )
    if not resp.data:
        raise HTTPException(404, "Benchmark not found")
    return {"ok": True}


@app.get("/api/benchmarks/{benchmark_id}/prices")
async def get_benchmark_prices(benchmark_id: int, start_date: str = "", end_date: str = ""):
    """Get prices for a benchmark, optionally filtered by date range."""
    query = supabase.table("benchmark_price").select("target_date, price").eq("benchmark_id", benchmark_id).order("target_date")
    if start_date:
        query = query.gte("target_date", start_date)
    if end_date:
        query = query.lte("target_date", end_date)

    # Paginate to handle large datasets
    rows: list[dict] = []
    page_size = 1000
    offset = 0
    while True:
        resp = await asyncio.to_thread(lambda o=offset: query.range(o, o + page_size - 1).execute())
        if not resp.data:
            break
        rows.extend(resp.data)
        if len(resp.data) < page_size:
            break
        offset += page_size

    return rows


@app.get("/api/usage")
async def api_usage():
    """Get GuruFocus API usage for the current month."""
    return await asyncio.to_thread(get_usage, supabase)


# ===========================================================================
# UNIVERSE SCREENING
# ===========================================================================

@app.get("/api/universe/criteria")
async def universe_criteria():
    """Return the list of LongEquity quality criteria."""
    return [
        {
            "key": key,
            "label": label,
            "description": CRITERIA_DESCRIPTIONS.get(key, ""),
            "min_years": CRITERIA_MIN_YEARS.get(key, 1),
        }
        for key, label in CRITERIA_NAMES
    ]


class ScreenRequest(BaseModel):
    as_of_year: str | None = None  # e.g. "2025-12"
    force_refresh: bool = False


@app.post("/api/universe/screen")
async def universe_screen(body: ScreenRequest = ScreenRequest()):
    """Screen all companies against LongEquity criteria. Returns SSE stream."""
    import queue

    as_of = body.as_of_year
    force = body.force_refresh

    def _run(q: queue.Queue):
        # Get all companies
        resp = supabase.table("company").select(
            "company_id, primary_ticker, primary_exchange, company_name, sector, country"
        ).limit(10000).execute()
        companies = resp.data or []
        label = f" as of {as_of}" if as_of else ""
        q.put(json.dumps({"type": "progress", "message": f"Found {len(companies)} companies to screen{label}."}))

        for event in screen_universe(supabase, companies, as_of_year=as_of, force_refresh=force):
            q.put(json.dumps(event))
        q.put(None)  # sentinel

    async def generate():
        yield ": keepalive\n\n"
        q: queue.Queue = queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, q)

        while True:
            try:
                msg = await asyncio.to_thread(q.get, timeout=0.15)
            except Exception:
                if task.done():
                    # Drain remaining messages
                    while not q.empty():
                        msg = q.get_nowait()
                        if msg is not None:
                            yield f"data: {msg}\n\n"
                    break
                continue
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


class BuildUniverseRequest(BaseModel):
    start_month: str  # "YYYY-MM"
    end_month: str    # "YYYY-MM"
    label: str = "default"
    max_companies: int = 5


@app.post("/api/universe/build")
async def universe_build(body: BuildUniverseRequest):
    """Build monthly universes for a date range and store in DB. Returns SSE stream."""
    import queue

    start = body.start_month
    end = body.end_month
    lbl = body.label
    max_co = body.max_companies

    def _run(q: queue.Queue):
        resp = supabase.table("company").select(
            "company_id, primary_ticker, primary_exchange, company_name, sector, country"
        ).limit(10000).execute()
        companies = resp.data or []

        for event in build_and_store_universes(supabase, companies, start, end, label=lbl, max_companies=max_co):
            q.put(json.dumps(event))
        q.put(None)

    async def generate():
        yield ": keepalive\n\n"
        q: queue.Queue = queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, q)

        while True:
            try:
                msg = await asyncio.to_thread(q.get, timeout=0.15)
            except Exception:
                if task.done():
                    while not q.empty():
                        msg = q.get_nowait()
                        if msg is not None:
                            yield f"data: {msg}\n\n"
                    break
                continue
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/api/universe/labels")
async def universe_labels():
    """List all universe labels with date ranges and counts."""
    def _run():
        resp = supabase.rpc("universe_labels").execute()
        return resp.data or []
    return await asyncio.to_thread(_run)


@app.get("/api/universe/months")
async def universe_months(label: str = "default"):
    """List stored universe months with counts for a label."""
    def _run():
        resp = supabase.rpc("universe_month_counts", {"p_label": label}).execute()
        return resp.data or []

    return await asyncio.to_thread(_run)


@app.get("/api/universe/months/{month}")
async def universe_month_detail(month: str, label: str = "default"):
    """Get all companies for a specific month with scores."""
    def _run():
        resp = supabase.table("universe_snapshot").select(
            "company_id, total_score, scores, details, passes"
        ).eq("label", label).eq("target_month", month).limit(10000).execute()
        snapshot_rows = resp.data or []

        if not snapshot_rows:
            return []

        # Get company info
        cids = [r["company_id"] for r in snapshot_rows]
        company_map: dict[int, dict] = {}
        for i in range(0, len(cids), 50):
            batch = cids[i:i + 50]
            cr = supabase.table("company").select(
                "company_id, primary_ticker, primary_exchange, company_name, sector, country"
            ).in_("company_id", batch).execute()
            for c in (cr.data or []):
                company_map[c["company_id"]] = c

        result = []
        for r in snapshot_rows:
            info = company_map.get(r["company_id"], {})
            result.append({
                "company_id": r["company_id"],
                "ticker": info.get("primary_ticker", ""),
                "exchange": info.get("primary_exchange", ""),
                "company_name": info.get("company_name", ""),
                "sector": info.get("sector", ""),
                "country": info.get("country", ""),
                "total_score": r["total_score"],
                "scores": r["scores"],
                "details": r["details"],
                "passes": r["passes"],
            })
        return result

    return await asyncio.to_thread(_run)


@app.delete("/api/universe/months/{month}")
async def universe_delete_month(month: str, label: str = "default"):
    """Delete a specific month's universe snapshot."""
    def _run():
        supabase.table("universe_snapshot").delete().eq(
            "label", label
        ).eq("target_month", month).execute()
        return {"deleted": month}
    return await asyncio.to_thread(_run)


@app.delete("/api/universe/labels/{label}")
async def universe_delete_label(label: str):
    """Delete all months for a specific universe label."""
    def _run():
        supabase.table("universe_snapshot").delete().eq("label", label).execute()
        return {"deleted": label}
    return await asyncio.to_thread(_run)


@app.get("/api/universe/validate")
async def universe_validate():
    """Compare current screening results against LongEquity snapshots."""
    def _run():
        resp = supabase.table("company").select(
            "company_id, primary_ticker, primary_exchange, company_name, sector, country"
        ).limit(10000).execute()
        companies = resp.data or []

        results = []
        for event in screen_universe(supabase, companies):
            if event["type"] == "done":
                results = event["data"]["results"]
                break

        return validate_vs_longequity(supabase, results)

    return await asyncio.to_thread(_run)
