import asyncio
import functools
import json
import os
import re
import time
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
from ingest.prices import ensure_prices_for_company, ensure_volume_for_company, PriceResult, _fetch_price_from_api, _parse_price_series, _ensure_bucket
from ingest.api_usage import track_api_call, get_usage
from momentum.data import (
    load_universe, load_all_prices, load_all_volumes,
    load_company_currency, load_fx_rates, convert_prices_to_eur,
    sync_fx_rates_to_db,
)
from momentum.signals import PRICE_SIGNAL_DEFS
from momentum.backtest import BacktestConfig, run_backtest
from universe.screen import screen_universe, build_and_store_universes, validate_vs_longequity
from universe.criteria import CRITERIA_NAMES, CRITERIA_DESCRIPTIONS, CRITERIA_MIN_YEARS
from index_universe.sp500 import (
    scrape_sp500, reconstruct_monthly_holdings,
    resolve_and_create_companies, store_index_membership,
    check_gurufocus_availability, load_changes,
)
from fx_rates import fetch_all_latest, fetch_history, get_coverage_info
from index_universe.acwi import (
    load_acwi_holdings, get_msci_announcements,
    fetch_announcement_detail_cached, fetch_bulk_details,
    _load_detail_cache, fetch_announcement_detail, _save_detail_cache,
    compute_net_additions, gurufocus_url,
    gurufocus_exchange, gurufocus_exchange_for_db,
    reconstruct_monthly_holdings as reconstruct_acwi_monthly_holdings,
    feasible_holdings_for_db as acwi_feasible_holdings_for_db,
)

load_dotenv()                              # .env (prod defaults)
load_dotenv(".env.local", override=True)   # .env.local (local overrides, if present)

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
            result = await asyncio.to_thread(
                load_prepared_into_supabase, prepared, supabase,
                universe_label="longequity",
            )
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
    # Fix company rows that were loaded with exchange_id=NULL
    # in previous runs (before ticker resolution existed)
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
    """Return months that are fully loaded for longequity.

    A month counts as "done" only if it has BOTH metric_data rows AND
    universe_membership rows. Early ingests populated metric_data before the
    universe_membership write path existed, so relying on metric_data alone
    silently skips months that still need their universe rows.
    """
    md_resp = supabase.rpc("get_distinct_dates", {"p_source_code": "longequity"}).execute()
    metric_months = {str(row["target_date"])[:7] for row in (md_resp.data or [])}

    u_resp = supabase.table("universe").select("universe_id").eq("label", "longequity").limit(1).execute()
    if not u_resp.data:
        return set()
    universe_id = u_resp.data[0]["universe_id"]

    membership_months: set[str] = set()
    offset = 0
    page_size = 1000
    while True:
        resp = (
            supabase.table("universe_membership")
            .select("target_month")
            .eq("universe_id", universe_id)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data or []
        for r in batch:
            if r.get("target_month"):
                membership_months.add(r["target_month"])
        if len(batch) < page_size:
            break
        offset += page_size

    return metric_months & membership_months


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
        .select("company_id,gurufocus_ticker,exchange_id,company_name,gurufocus_exchange:gurufocus_exchange(exchange_code,country:country(country_name))")
        .limit(10000)
        .execute()
    )
    # Flatten nested joins
    for c in (all_companies_resp.data or []):
        exch_info = c.pop("gurufocus_exchange", None) or {}
        country_info = exch_info.pop("country", None) or {}
        c["gurufocus_exchange"] = exch_info.get("exchange_code")
        c["country"] = country_info.get("country_name")
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
    # Exchanges from gurufocus_exchange table
    exch_resp = supabase.table("gurufocus_exchange").select("exchange_code").limit(1000).execute()
    exchanges = sorted({r["exchange_code"] for r in (exch_resp.data or [])})
    # Countries from country table
    country_resp = supabase.table("country").select("country_name").limit(1000).execute()
    countries = sorted({r["country_name"] for r in (country_resp.data or [])})
    # Sectors from universe_membership
    sector_resp = supabase.table("universe_membership").select("sector").limit(10000).execute()
    sectors = sorted({r["sector"] for r in (sector_resp.data or []) if r.get("sector") and r["sector"].strip()})
    return {"exchanges": exchanges, "countries": countries, "sectors": sectors}


class CreateCompanyRequest(BaseModel):
    company_name: str
    gurufocus_ticker: str
    gurufocus_exchange: str  # exchange_code, resolved to exchange_id


class UpdateCompanyRequest(BaseModel):
    company_name: str | None = None
    gurufocus_ticker: str | None = None
    gurufocus_exchange: str | None = None  # exchange_code


def _resolve_exchange_id(exchange_code: str) -> int | None:
    """Look up exchange_id from an exchange_code."""
    resp = (
        supabase.table("gurufocus_exchange")
        .select("exchange_id")
        .eq("exchange_code", exchange_code.upper())
        .limit(1)
        .execute()
    )
    return resp.data[0]["exchange_id"] if resp.data else None


@app.get("/api/companies")
async def list_companies():
    resp = (
        supabase.table("company")
        .select("company_id,company_name,gurufocus_ticker,exchange_id,gurufocus_exchange:gurufocus_exchange(exchange_code,country:country(country_name))")
        .order("company_name")
        .limit(10000)
        .execute()
    )
    # Flatten the nested exchange join
    rows = resp.data or []
    for r in rows:
        exch_info = r.pop("gurufocus_exchange", None) or {}
        country_info = exch_info.pop("country", None) or {}
        r["gurufocus_exchange"] = exch_info.get("exchange_code")
        r["country"] = country_info.get("country_name")
    return rows


@app.post("/api/companies")
async def create_company(req: CreateCompanyRequest):
    exchange_id = _resolve_exchange_id(req.gurufocus_exchange)
    row = {
        "company_name": req.company_name,
        "gurufocus_ticker": req.gurufocus_ticker.upper(),
        "exchange_id": exchange_id,
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
    if req.gurufocus_ticker is not None:
        updates["gurufocus_ticker"] = req.gurufocus_ticker.upper()
    if req.gurufocus_exchange is not None:
        exchange_id = _resolve_exchange_id(req.gurufocus_exchange)
        updates["exchange_id"] = exchange_id
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
    supabase.table("company_source").delete().eq("company_id", company_id).execute()
    supabase.table("universe_membership").delete().eq("company_id", company_id).execute()
    supabase.table("company").delete().eq("company_id", company_id).execute()
    return {"ok": True}


# ─────────────────────────── Earnings endpoints ────────────────────────────

def _get_company_or_404(company_id: int) -> dict:
    resp = (
        supabase.table("company")
        .select("company_id,gurufocus_ticker,exchange_id,company_name,gurufocus_exchange:gurufocus_exchange(exchange_code,is_us)")
        .eq("company_id", company_id)
        .limit(1)
        .execute()
    )
    if not resp.data:
        raise HTTPException(status_code=404, detail="Company not found")
    row = resp.data[0]
    exch_info = row.pop("gurufocus_exchange", None) or {}
    row["gurufocus_exchange"] = exch_info.get("exchange_code")
    row["is_us"] = exch_info.get("is_us", False)
    return row


async def _earnings_refresh_stream(company_id: int, sources: list[str], force: bool):
    """SSE stream for earnings data refresh."""
    import queue as _queue

    def event(msg_type: str, message: str, **extra) -> str:
        payload = {"type": msg_type, "message": message, **extra}
        return f"data: {json.dumps(payload)}\n\n"

    company = _get_company_or_404(company_id)
    ticker = company["gurufocus_ticker"]
    exchange = company["gurufocus_exchange"] or "UNKNOWN"
    name = company.get("company_name") or f"{ticker}.{exchange}"
    region = "usa" if company.get("is_us", False) else "europe"

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
            .gte("target_date", "1998-01-01")
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
                .gte("target_date", "1998-01-01")
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
            .gte("target_date", "1998-01-01")
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
    universe_label: str | None = None  # if set, use universe_membership for per-month filtering
    index_universe: str | None = None  # if set, use universe_membership for per-month filtering (e.g. "SP500")


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

        # Load universe membership if a label is specified
        monthly_eligible: dict[str, set[int]] | None = None
        if req.universe_label:
            yield _emit({"type": "progress", "pct": 6, "message": f"Loading universe '{req.universe_label}'..."})

            def _load_universe_membership():
                # Get universe_id
                u_resp = supabase.table("universe").select("universe_id").eq("label", req.universe_label).limit(1).execute()
                if not u_resp.data:
                    return {}
                universe_id = u_resp.data[0]["universe_id"]
                rows = []
                offset = 0
                page_size = 1000
                while True:
                    resp = supabase.table("universe_membership").select(
                        "target_month, company_id, sector"
                    ).eq("universe_id", universe_id).order(
                        "target_month"
                    ).order("company_id").range(offset, offset + page_size - 1).execute()
                    batch = resp.data or []
                    rows.extend(batch)
                    if len(batch) < page_size:
                        break
                    offset += page_size
                result: dict[str, dict[int, str | None]] = {}
                for r in rows:
                    m = r["target_month"]
                    if m not in result:
                        result[m] = {}
                    result[m][r["company_id"]] = r.get("sector")
                return result

            monthly_eligible = await asyncio.to_thread(_load_universe_membership)
            n_months = len(monthly_eligible)
            if n_months == 0:
                yield _emit({"type": "error", "message": f"No universe data for label '{req.universe_label}'"})
                return
            avg_pass = sum(len(v) for v in monthly_eligible.values()) // n_months
            yield _emit({"type": "progress", "pct": 7, "message": f"Universe: {n_months} months, ~{avg_pass} companies/month"})

            # Diagnose missing sector data: if no membership row has a sector
            # value, sector-based selection will silently pick zero companies.
            # Fail loudly so the user knows to re-save the universe.
            total_sec = sum(
                1 for month_map in monthly_eligible.values()
                for s in month_map.values() if s
            )
            if total_sec == 0:
                yield _emit({"type": "error", "message": f"Universe '{req.universe_label}' has no sector data in universe_membership — re-save this universe from its source page so sectors are populated."})
                return

        # Load index universe if specified (e.g. SP500 — stored as a universe label)
        if req.index_universe and monthly_eligible is None:
            yield _emit({"type": "progress", "pct": 6, "message": f"Loading index universe '{req.index_universe}'..."})

            def _load_index_universe():
                # Index universes are now stored as regular universes
                u_resp = supabase.table("universe").select("universe_id").eq("label", req.index_universe).limit(1).execute()
                if not u_resp.data:
                    return {}
                universe_id = u_resp.data[0]["universe_id"]
                rows: list[dict] = []
                offset = 0
                page_size = 1000
                while True:
                    resp = (
                        supabase.table("universe_membership")
                        .select("target_month, company_id, sector")
                        .eq("universe_id", universe_id)
                        .order("target_month")
                        .range(offset, offset + page_size - 1)
                        .execute()
                    )
                    batch = resp.data or []
                    rows.extend(batch)
                    if len(batch) < page_size:
                        break
                    offset += page_size
                result: dict[str, dict[int, str | None]] = {}
                for r in rows:
                    m = r["target_month"]
                    if m not in result:
                        result[m] = {}
                    result[m][r["company_id"]] = r.get("sector")
                return result

            monthly_eligible = await asyncio.to_thread(_load_index_universe)
            n_months = len(monthly_eligible)
            if n_months == 0:
                yield _emit({"type": "error", "message": f"No index universe data for '{req.index_universe}'"})
                return
            avg_co = sum(len(v) for v in monthly_eligible.values()) // n_months
            yield _emit({"type": "progress", "pct": 7, "message": f"Index universe: {n_months} months, ~{avg_co} companies/month"})

            total_sec = sum(
                1 for month_map in monthly_eligible.values()
                for s in month_map.values() if s
            )
            if total_sec == 0:
                yield _emit({"type": "error", "message": f"Index universe '{req.index_universe}' has no sector data — re-save this universe from its source page so sectors are populated."})
                return

        # Also fail cleanly when no universe was selected at all — the
        # scoring pipeline requires per-company sectors, and `load_universe`
        # leaves them all None in that fallback.
        if monthly_eligible is None and (req.top_n_sectors or 0) > 0:
            yield _emit({"type": "error", "message": "No universe selected. Sector-based selection requires a universe (or index universe) with stored sector data."})
            return

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
            # If max_companies is set, pre-trim the universe alphabetically so we
            # only fetch what we need. (Parallel fetch makes the old "stop at
            # ok_count" optimization hard to preserve.)
            if req.max_companies > 0 and len(universe_df) > req.max_companies:
                universe_df = universe_df.sort_values("gurufocus_ticker").head(req.max_companies).reset_index(drop=True)

            total_companies = len(universe_df)
            concurrency = int(os.environ.get("BACKTEST_FETCH_CONCURRENCY", "16"))
            blocked_exchanges: set[str] = set()
            skipped_count = 0
            ok_count = 0
            fetch_start_ts = time.monotonic()

            # Warm the storage bucket once before launching tasks — otherwise the
            # first N workers would race and each fire a bucket-create HTTP call.
            await asyncio.to_thread(_ensure_bucket, supabase)

            # Each company task submits 2 blocking HTTP calls in parallel (price +
            # volume), so the executor needs 2 slots per concurrent task or the
            # second call queues behind the first and inflates wall-clock timings.
            from concurrent.futures import ThreadPoolExecutor
            pool_size = concurrency * 2 + 4
            executor = ThreadPoolExecutor(max_workers=pool_size, thread_name_prefix="fetch")
            loop = asyncio.get_event_loop()

            yield _emit({"type": "progress", "pct": 5, "message": f"Ensuring price & volume data for {total_companies} companies (cutoff: {data_cutoff}, concurrency: {concurrency}, pool: {pool_size})..."})
            yield _keepalive()

            result_queue: asyncio.Queue = asyncio.Queue()
            sema = asyncio.Semaphore(concurrency)
            inflight = {"count": 0, "peak": 0}

            async def _fetch_one(row_cid: int, row_ticker: str, row_exchange: str):
                sym = f"{row_exchange}:{row_ticker}"
                if row_exchange in blocked_exchanges:
                    await result_queue.put({"cid": row_cid, "symbol": sym, "exchange": row_exchange, "status": "skipped_blocked"})
                    return
                async with sema:
                    if row_exchange in blocked_exchanges:
                        await result_queue.put({"cid": row_cid, "symbol": sym, "exchange": row_exchange, "status": "skipped_blocked"})
                        return
                    inflight["count"] += 1
                    if inflight["count"] > inflight["peak"]:
                        inflight["peak"] = inflight["count"]
                    task_start = time.monotonic()
                    try:
                        # Run price + volume concurrently inside one company task
                        pr_fut = loop.run_in_executor(
                            executor,
                            functools.partial(
                                ensure_prices_for_company,
                                supabase, row_cid, row_ticker, row_exchange,
                                data_cutoff=data_cutoff,
                            ),
                        )
                        vr_fut = loop.run_in_executor(
                            executor,
                            functools.partial(
                                ensure_volume_for_company,
                                supabase, row_cid, row_ticker, row_exchange,
                                data_cutoff=data_cutoff,
                            ),
                        )
                        pr_res, vr_res = await asyncio.gather(pr_fut, vr_fut, return_exceptions=True)
                        if isinstance(pr_res, BaseException):
                            raise pr_res
                        pr = pr_res
                        if isinstance(vr_res, BaseException):
                            vr = PriceResult()
                            vr.source = "error"
                            vr.error = str(vr_res)
                        else:
                            vr = vr_res
                        elapsed_ms = int((time.monotonic() - task_start) * 1000)
                        await result_queue.put({"cid": row_cid, "symbol": sym, "exchange": row_exchange, "pr": pr, "vr": vr, "status": "ok", "ms": elapsed_ms})
                    except Exception as e:
                        elapsed_ms = int((time.monotonic() - task_start) * 1000)
                        await result_queue.put({"cid": row_cid, "symbol": sym, "exchange": row_exchange, "error": str(e), "status": "error", "ms": elapsed_ms})
                    finally:
                        inflight["count"] -= 1

            tasks = [
                asyncio.create_task(_fetch_one(
                    int(row["company_id"]),
                    row["gurufocus_ticker"],
                    row["gurufocus_exchange"] or "UNKNOWN",
                ))
                for _, row in universe_df.iterrows()
            ]

            async def _sentinel():
                await asyncio.gather(*tasks, return_exceptions=True)
                await result_queue.put(None)

            sentinel_task = asyncio.create_task(_sentinel())

            done_count = 0
            try:
                while True:
                    evt = await result_queue.get()
                    if evt is None:
                        break
                    done_count += 1
                    pct = 5 + round((done_count / max(1, total_companies)) * 55)
                    status = evt["status"]
                    cid = evt["cid"]
                    symbol = evt["symbol"]
                    exchange = evt["exchange"]

                    if status == "skipped_blocked":
                        skipped_count += 1
                        excluded_ids.add(cid)
                        continue
                    if status == "error":
                        excluded_ids.add(cid)
                        yield _emit({"type": "warning", "scope": "fetch", "symbol": symbol, "message": f"{symbol}: fetch failed — {evt['error']}"})
                        continue

                    pr = evt["pr"]
                    vr = evt["vr"]

                    if pr.is_forbidden:
                        blocked_exchanges.add(exchange)
                        excluded_ids.add(cid)
                        yield _emit({"type": "progress", "pct": pct, "message": f"  {symbol}: unsubscribed region — future {exchange} calls will be skipped"})
                    elif pr.is_delisted:
                        excluded_ids.add(cid)
                        await asyncio.to_thread(
                            lambda c=cid: (
                                supabase.table("metric_data").delete().eq("company_id", c).execute(),
                                supabase.table("portfolio_weight").delete().eq("company_id", c).execute(),
                                supabase.table("company").delete().eq("company_id", c).execute(),
                            )
                        )
                        yield _emit({"type": "progress", "pct": pct, "message": f"  {symbol}: DELISTED — removed from database"})
                    else:
                        ok_count += 1
                        parts: list[str] = []
                        if pr.source == "cache":
                            parts.append(f"price: cache ({pr.rows_loaded})")
                        elif pr.source == "api":
                            parts.append(f"price: API ({pr.rows_loaded})")
                        elif pr.source == "stale_cache":
                            parts.append(f"price: stale cache ({pr.rows_loaded})")
                        elif pr.source == "none":
                            parts.append("price: none")
                        else:
                            parts.append(f"price: {pr.source}")
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
                        ms = evt.get("ms", 0)
                        peak = inflight["peak"]
                        yield _emit({"type": "progress", "pct": pct, "message": f"  {symbol} ({done_count}/{total_companies}, {ms}ms, peak:{peak}): {' | '.join(parts)}"})
            finally:
                # Make sure the sentinel task completes before we leave this block
                try:
                    await sentinel_task
                except Exception:
                    pass
                executor.shutdown(wait=False)

            total_elapsed = time.monotonic() - fetch_start_ts
            yield _emit({"type": "progress", "pct": 60, "message": f"Fetch complete in {total_elapsed:.1f}s (peak concurrency: {inflight['peak']}/{concurrency})"})

            if blocked_exchanges:
                yield _emit({"type": "warning", "scope": "fetch", "message": f"Blocked exchanges (unsubscribed): {', '.join(sorted(blocked_exchanges))} — {skipped_count} companies skipped"})

        # Remove excluded companies (blocked exchanges, delisted) from universe
        if excluded_ids:
            universe_df = universe_df[~universe_df["company_id"].isin(excluded_ids)].reset_index(drop=True)
            yield _emit({"type": "progress", "pct": 61, "message": f"Universe after exclusions: {len(universe_df)} companies"})

        # Optionally limit universe size (alphabetical by ticker) — applied after exclusions
        if req.max_companies > 0 and len(universe_df) > req.max_companies:
            universe_df = universe_df.sort_values("gurufocus_ticker").head(req.max_companies).reset_index(drop=True)
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

        # ------------------------------------------------------------------ #
        # FX conversion: convert local-currency prices to EUR so signals and
        # returns are expressed in a single currency for a EUR-based investor.
        # Momentum ratios are scale-invariant so signals are unaffected, but
        # forward returns change with FX drift (e.g. JPY weakness vs EUR).
        # ------------------------------------------------------------------ #
        yield _emit({"type": "progress", "pct": 65, "message": "Resolving trading currency per company..."})
        yield _keepalive()
        company_currency = await asyncio.to_thread(
            load_company_currency, supabase, company_ids,
        )
        currencies_needed = sorted({c for c in company_currency.values() if c})
        yield _emit({"type": "progress", "pct": 65, "message": f"Found {len(currencies_needed)} distinct currencies: {', '.join(currencies_needed)}"})

        # Sync fx_rate table from ECB for every currency in range. This is
        # idempotent and cheap — it only fetches what's missing past the
        # highest existing rate_date per currency.
        yield _emit({"type": "progress", "pct": 65, "message": f"Syncing FX rates from ECB (through {price_end})..."})
        yield _keepalive()
        fx_sync = await asyncio.to_thread(
            sync_fx_rates_to_db, supabase, currencies_needed, price_start, price_end,
        )
        synced_codes = sorted(c for c, s in fx_sync.items() if s.get("status") == "synced")
        cached_codes = sorted(c for c, s in fx_sync.items() if s.get("status") == "cached")
        failed_codes = sorted(c for c, s in fx_sync.items() if s.get("status") == "error")
        nodata_codes = sorted(c for c, s in fx_sync.items() if s.get("status") == "no_data")
        total_rows = sum(s.get("rows", 0) for s in fx_sync.values())
        yield _emit({
            "type": "progress",
            "pct": 65,
            "message": (
                f"FX sync done: {len(synced_codes)} updated ({total_rows:,} rows), "
                f"{len(cached_codes)} already current, "
                f"{len(failed_codes)} failed, {len(nodata_codes)} no_data"
            ),
        })
        if failed_codes:
            for code in failed_codes:
                err = fx_sync[code].get("error", "unknown")
                yield _emit({"type": "warning", "scope": "fx", "message": f"FX sync failed for {code}: {err}"})
        if nodata_codes:
            yield _emit({
                "type": "warning",
                "scope": "fx",
                "message": f"No FX data returned for: {', '.join(nodata_codes)} (ECB may not cover these)",
            })

        yield _emit({"type": "progress", "pct": 65, "message": f"Loading FX rates ({price_start} to {price_end}) for {len(currencies_needed)} currencies..."})
        yield _keepalive()
        fx_rates = await asyncio.to_thread(
            load_fx_rates, supabase, currencies_needed, price_start, price_end,
        )
        loaded_codes = [c for c, s in fx_rates.items() if s is not None and not s.empty]
        missing_codes = sorted(set(currencies_needed) - set(loaded_codes))
        yield _emit({"type": "progress", "pct": 65, "message": f"FX rates loaded for {len(loaded_codes)} currencies"})
        if missing_codes:
            yield _emit({
                "type": "warning",
                "scope": "fx",
                "message": f"No FX history for: {', '.join(missing_codes)} — companies on those currencies will be dropped",
            })

        yield _emit({"type": "progress", "pct": 65, "message": f"Converting {len(prices_df):,} price rows to EUR..."})
        yield _keepalive()
        prices_local_df = prices_df
        prices_df, fx_stats = await asyncio.to_thread(
            convert_prices_to_eur, prices_df, company_currency, fx_rates,
        )
        yield _emit({
            "type": "progress",
            "pct": 65,
            "message": (
                f"FX done: {fx_stats['converted_rows']:,} rows converted "
                f"({', '.join(fx_stats['converted_currencies']) or 'none'}), "
                f"{fx_stats['passthrough_rows']:,} already EUR, "
                f"{fx_stats['dropped_no_currency']:,} dropped (no currency), "
                f"{fx_stats['dropped_no_fx']:,} dropped (no FX rate)"
            ),
        })
        if fx_stats["missing_currencies"]:
            yield _emit({
                "type": "warning",
                "scope": "fx",
                "message": f"Currencies with no FX series in date range: {', '.join(fx_stats['missing_currencies'])}",
            })

        if prices_df.empty:
            yield _emit({"type": "error", "message": "No price data left after FX conversion."})
            return

        # Audit price coverage: flag universe companies with zero or sparse price rows
        _price_counts = prices_df.groupby("company_id").size().to_dict() if not prices_df.empty else {}
        _universe_symbol = {
            int(r["company_id"]): f"{r.get('gurufocus_exchange') or '?'}:{r['gurufocus_ticker']}"
            for _, r in universe_df.iterrows()
        }
        _no_price = [cid for cid in company_ids if _price_counts.get(int(cid), 0) == 0]
        _sparse_price = [cid for cid in company_ids if 0 < _price_counts.get(int(cid), 0) < 20]

        # Group no-price companies by exchange. An exchange where every
        # universe company has zero price rows is almost certainly
        # unsubscribed on GuruFocus (or fully blocked) — surface it
        # separately from one-off gaps so the user can tell the difference.
        _universe_exchange = {
            int(r["company_id"]): r.get("gurufocus_exchange") or "UNKNOWN"
            for _, r in universe_df.iterrows()
        }
        _exchange_totals: dict[str, int] = {}
        _exchange_no_price: dict[str, int] = {}
        for cid in company_ids:
            exch = _universe_exchange.get(int(cid), "UNKNOWN")
            _exchange_totals[exch] = _exchange_totals.get(exch, 0) + 1
            if _price_counts.get(int(cid), 0) == 0:
                _exchange_no_price[exch] = _exchange_no_price.get(exch, 0) + 1
        _unsubscribed_exchanges = sorted(
            exch for exch, no_price in _exchange_no_price.items()
            if _exchange_totals.get(exch, 0) > 0 and no_price == _exchange_totals[exch]
        )
        if _unsubscribed_exchanges:
            parts = [f"{exch}({_exchange_no_price[exch]})" for exch in _unsubscribed_exchanges]
            total_unsub = sum(_exchange_no_price[e] for e in _unsubscribed_exchanges)
            yield _emit({
                "type": "info",
                "scope": "prices",
                "message": f"Unsubscribed/blocked exchanges (expected to have no data): {', '.join(parts)} — {total_unsub} companies",
            })

        # Remaining no-price cases: exchanges where some companies have
        # data but specific tickers don't — true one-off gaps.
        _no_price_gap = [
            cid for cid in _no_price
            if _universe_exchange.get(int(cid), "UNKNOWN") not in _unsubscribed_exchanges
        ]
        if _no_price_gap:
            sample = ", ".join(_universe_symbol.get(int(c), str(c)) for c in _no_price_gap[:10])
            more = f" (+{len(_no_price_gap) - 10} more)" if len(_no_price_gap) > 10 else ""
            yield _emit({"type": "warning", "scope": "prices", "message": f"{len(_no_price_gap)} companies on subscribed exchanges have NO price data: {sample}{more}"})
        if _sparse_price:
            sample = ", ".join(
                f"{_universe_symbol.get(int(c), c)}({_price_counts.get(int(c), 0)})" for c in _sparse_price[:10]
            )
            more = f" (+{len(_sparse_price) - 10} more)" if len(_sparse_price) > 10 else ""
            yield _emit({"type": "warning", "scope": "prices", "message": f"{len(_sparse_price)} companies have < 20 price rows (insufficient for signals): {sample}{more}"})

        # Load volumes from DB
        yield _emit({"type": "progress", "pct": 66, "message": "Loading volumes from DB..."})
        yield _keepalive()
        volumes_df = await asyncio.to_thread(
            load_all_volumes, supabase, company_ids, price_start, price_end,
        )
        n_vol = volumes_df["company_id"].nunique() if not volumes_df.empty else 0
        yield _emit({"type": "progress", "pct": 67, "message": f"Loaded {len(volumes_df):,} volume records for {n_vol} companies"})

        # Audit volume coverage. Companies on unsubscribed exchanges (already
        # flagged in the prices info message) are expected to have no volume
        # either, so filter them out of the warning set to avoid noise.
        _vol_counts = volumes_df.groupby("company_id").size().to_dict() if not volumes_df.empty else {}
        _no_vol_all = [cid for cid in company_ids if _vol_counts.get(int(cid), 0) == 0]
        _sparse_vol = [cid for cid in company_ids if 0 < _vol_counts.get(int(cid), 0) < 20]
        _no_vol_gap = [
            cid for cid in _no_vol_all
            if _universe_exchange.get(int(cid), "UNKNOWN") not in _unsubscribed_exchanges
        ]
        if _no_vol_gap:
            sample = ", ".join(_universe_symbol.get(int(c), str(c)) for c in _no_vol_gap[:10])
            more = f" (+{len(_no_vol_gap) - 10} more)" if len(_no_vol_gap) > 10 else ""
            yield _emit({"type": "warning", "scope": "volumes", "message": f"{len(_no_vol_gap)} companies on subscribed exchanges have NO volume data — volume signals will be skipped for them: {sample}{more}"})
        if _sparse_vol:
            sample = ", ".join(
                f"{_universe_symbol.get(int(c), c)}({_vol_counts.get(int(c), 0)})" for c in _sparse_vol[:10]
            )
            more = f" (+{len(_sparse_vol) - 10} more)" if len(_sparse_vol) > 10 else ""
            yield _emit({"type": "warning", "scope": "volumes", "message": f"{len(_sparse_vol)} companies have < 20 volume rows: {sample}{more}"})

        # Run backtest with progress callback via queue for real-time streaming
        import queue as _queue
        progress_queue: _queue.Queue = _queue.Queue()
        backtest_result_holder: list = []
        backtest_error_holder: list = []

        def send_event(event_type: str, **kwargs):
            progress_queue.put({"type": event_type, **kwargs})

        def _run_backtest():
            try:
                r = run_backtest(config, prices_df, universe_df, send_event,
                    volumes_df=volumes_df,
                    monthly_eligible=monthly_eligible,
                    prices_local_df=prices_local_df,
                    company_currency=company_currency,
                )
                backtest_result_holder.append(r)
            except Exception as e:
                backtest_error_holder.append(e)
            finally:
                progress_queue.put(None)  # sentinel

        yield _emit({"type": "progress", "pct": 68, "message": "Running backtest computation..."})
        yield _keepalive()

        loop = asyncio.get_event_loop()
        loop.run_in_executor(None, _run_backtest)

        # Stream progress events in real-time as the backtest runs
        while True:
            try:
                evt = await asyncio.to_thread(progress_queue.get, timeout=0.2)
            except Exception:
                continue
            if evt is None:
                break
            if evt["type"] == "progress":
                scaled_pct = 68 + round(evt.get("pct", 0) * 0.30)
                yield _emit({"type": "progress", "pct": scaled_pct, "message": evt.get("message", "")})
            elif evt["type"] == "warning":
                yield _emit({"type": "warning", "scope": evt.get("scope", "backtest"), "message": evt.get("message", "")})

        if backtest_error_holder:
            raise backtest_error_holder[0]
        result = backtest_result_holder[0]

        # Build universe snapshot for saving
        universe_snapshot = [
            {
                "company_id": int(row["company_id"]),
                "ticker": str(row["gurufocus_ticker"]),
                "exchange": str(row.get("gurufocus_exchange", "")),
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
        "result": {
            "summary": req.summary,
            "monthly_records": req.monthly_records,
            "universe": req.universe,
        },
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
        .select("run_id, name, created_at, config, result")
        .order("created_at", desc=True)
        .execute()
    )
    # Extract summary from result for backward compatibility
    rows = resp.data or []
    for r in rows:
        result = r.get("result") or {}
        r["summary"] = result.get("summary")
    return rows


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


class RenameBacktestRequest(BaseModel):
    name: str


@app.patch("/api/momentum/backtests/{run_id}")
async def rename_backtest(run_id: int, req: RenameBacktestRequest):
    """Rename a saved backtest run."""
    new_name = req.name.strip()
    if not new_name:
        raise HTTPException(400, "Name cannot be empty")
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .update({"name": new_name})
        .eq("run_id", run_id)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Backtest not found")
    return resp.data[0]


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

    # Load prices (honour the same cutoff as company prices)
    _BM_CUTOFF = date(1998, 1, 1)
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

    _BM_CUTOFF = date(1998, 1, 1)
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
            "company_id, gurufocus_ticker, exchange_id, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))"
        ).limit(10000).execute()
        companies = []
        for c in (resp.data or []):
            exch_info = c.pop("gurufocus_exchange", None) or {}
            country_info = exch_info.pop("country", None) or {}
            c["gurufocus_exchange"] = exch_info.get("exchange_code")
            c["country"] = country_info.get("country_name")
            companies.append(c)
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
            "company_id, gurufocus_ticker, exchange_id, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))"
        ).limit(10000).execute()
        companies = []
        for c in (resp.data or []):
            exch_info = c.pop("gurufocus_exchange", None) or {}
            country_info = exch_info.pop("country", None) or {}
            c["gurufocus_exchange"] = exch_info.get("exchange_code")
            c["country"] = country_info.get("country_name")
            companies.append(c)

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
    """List all universes with stats. Aggregation runs in Postgres via the
    `universe_full_stats` RPC so the endpoint is two round trips regardless of
    universe size."""
    def _run():
        u_rows = (
            supabase.table("universe")
            .select("universe_id, label, description, created_at, parent_universe_id, filter_config")
            .order("label")
            .execute()
            .data or []
        )
        stats_rows = supabase.rpc("universe_full_stats").execute().data or []
        stats_by_id: dict[int, dict] = {r["universe_id"]: r for r in stats_rows}

        label_by_id = {u["universe_id"]: u["label"] for u in u_rows}

        result = []
        for u in u_rows:
            uid = u["universe_id"]
            s = stats_by_id.get(uid, {})
            monthly = s.get("monthly_counts") or []
            sectors = s.get("sector_counts") or []

            total_rows = s.get("total_rows", 0)
            month_count = s.get("month_count", 0)
            avg_per_month = round(total_rows / month_count, 1) if month_count else 0

            parent_id = u.get("parent_universe_id")
            result.append({
                "universe_id": uid,
                "label": u["label"],
                "description": u.get("description"),
                "created_at": u.get("created_at"),
                "parent_universe_id": parent_id,
                "parent_label": label_by_id.get(parent_id) if parent_id else None,
                "filter_config": u.get("filter_config"),
                "is_derived": parent_id is not None,
                "start_month": s.get("start_month"),
                "end_month": s.get("end_month"),
                "month_count": month_count,
                "total_rows": total_rows,
                "unique_companies": s.get("unique_companies", 0),
                "unique_tickers": s.get("unique_tickers", 0),
                "avg_per_month": avg_per_month,
                "first_month_count": monthly[0]["count"] if monthly else 0,
                "last_month_count": monthly[-1]["count"] if monthly else 0,
                "monthly_counts": monthly,
                "sectors": sectors,
            })

        return result

    return await asyncio.to_thread(_run)


@app.get("/api/universe/months")
async def universe_months(label: str = "default"):
    """List stored universe months with counts for a label."""
    def _run():
        # Get universe_id for this label
        u_resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
        if not u_resp.data:
            return []
        universe_id = u_resp.data[0]["universe_id"]
        # Get distinct months with counts
        resp = supabase.table("universe_membership").select("target_month").eq("universe_id", universe_id).limit(10000).execute()
        rows = resp.data or []
        from collections import Counter
        counts = Counter(r["target_month"] for r in rows)
        return [{"target_month": m, "count": c} for m, c in sorted(counts.items())]

    return await asyncio.to_thread(_run)


@app.get("/api/universe/months/{month}")
async def universe_month_detail(month: str, label: str = "default"):
    """Get all companies for a specific month in a universe."""
    def _run():
        # Get universe_id
        u_resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
        if not u_resp.data:
            return []
        universe_id = u_resp.data[0]["universe_id"]

        resp = supabase.table("universe_membership").select(
            "company_id, universe_ticker, sector"
        ).eq("universe_id", universe_id).eq("target_month", month).limit(10000).execute()
        membership_rows = resp.data or []

        if not membership_rows:
            return []

        # Get company info
        cids = [r["company_id"] for r in membership_rows]
        company_map: dict[int, dict] = {}
        for i in range(0, len(cids), 50):
            batch = cids[i:i + 50]
            cr = supabase.table("company").select(
                "company_id, gurufocus_ticker, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))"
            ).in_("company_id", batch).execute()
            for c in (cr.data or []):
                exch_info = c.pop("gurufocus_exchange", None) or {}
                country_info = exch_info.pop("country", None) or {}
                c["gurufocus_exchange"] = exch_info.get("exchange_code")
                c["country"] = country_info.get("country_name")
                company_map[c["company_id"]] = c

        result = []
        for r in membership_rows:
            info = company_map.get(r["company_id"], {})
            result.append({
                "company_id": r["company_id"],
                "ticker": info.get("gurufocus_ticker", ""),
                "exchange": info.get("gurufocus_exchange", ""),
                "company_name": info.get("company_name", ""),
                "sector": r.get("sector", ""),
                "country": info.get("country", ""),
                "universe_ticker": r.get("universe_ticker", ""),
            })
        return result

    return await asyncio.to_thread(_run)


@app.delete("/api/universe/months/{month}")
async def universe_delete_month(month: str, label: str = "default"):
    """Delete a specific month's universe membership."""
    def _run():
        u_resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
        if not u_resp.data:
            return {"deleted": month}
        universe_id = u_resp.data[0]["universe_id"]
        supabase.table("universe_membership").delete().eq(
            "universe_id", universe_id
        ).eq("target_month", month).execute()
        return {"deleted": month}
    return await asyncio.to_thread(_run)


@app.delete("/api/universe/labels/{label}")
async def universe_delete_label(label: str):
    """Delete a universe and all its memberships (cascade)."""
    def _run():
        supabase.table("universe").delete().eq("label", label).execute()
        return {"deleted": label}
    return await asyncio.to_thread(_run)


class UniverseRenameRequest(BaseModel):
    new_label: str


@app.put("/api/universe/labels/{label}")
async def universe_rename_label(label: str, req: UniverseRenameRequest):
    """Rename a universe label."""
    def _run():
        new_label = (req.new_label or "").strip()
        if not new_label:
            raise HTTPException(status_code=400, detail="new_label is required")
        existing = supabase.table("universe").select("universe_id").eq("label", new_label).limit(1).execute()
        if existing.data:
            raise HTTPException(status_code=409, detail=f"Label '{new_label}' already exists")
        supabase.table("universe").update({"label": new_label}).eq("label", label).execute()
        return {"old_label": label, "new_label": new_label}
    return await asyncio.to_thread(_run)


@app.delete("/api/universe/labels")
async def universe_delete_all():
    """Delete ALL universes and their memberships (cascade)."""
    def _run():
        resp = supabase.table("universe").select("universe_id").limit(100000).execute()
        ids = [r["universe_id"] for r in (resp.data or [])]
        if not ids:
            return {"deleted": 0}
        for i in range(0, len(ids), 50):
            batch = ids[i:i + 50]
            supabase.table("universe").delete().in_("universe_id", batch).execute()
        return {"deleted": len(ids)}
    return await asyncio.to_thread(_run)


# ---------------------------------------------------------------------------
# Derived universes (tightened base universes via quality-metric thresholds)
# ---------------------------------------------------------------------------

def _cutoff_for_target_month(target_month: str) -> date:
    """Latest fiscal-year-end date to consider for a given target month.

    Matches the existing convention in screen.py: for target month 'YYYY-MM'
    we use the previous calendar year's FY data. Here we express that as a
    cutoff date of (YYYY-1)-12-31.
    """
    year = int(target_month[:4])
    return date(year - 1, 12, 31)


def _load_derived_metrics(
    company_ids: list[int],
    metric_codes: list[str],
) -> dict[int, list[tuple[date, dict[str, float]]]]:
    """Fetch derived metric rows for the given companies+codes.

    Returns {company_id -> [(fy_end_date, {code: value}), ...]}, sorted by date asc.
    Batched in chunks of 50 company_ids (Cloudflare 502 avoidance).
    """
    out: dict[int, dict[str, dict[str, float]]] = {}  # cid -> {iso_date -> {code -> value}}
    for i in range(0, len(company_ids), 50):
        batch = company_ids[i:i + 50]
        resp = (
            supabase.table("metric_data")
            .select("company_id, metric_code, target_date, numeric_value")
            .in_("company_id", batch)
            .eq("source_code", "derived")
            .in_("metric_code", metric_codes)
            .limit(100000)
            .execute()
        )
        for row in (resp.data or []):
            cid = row["company_id"]
            d = row["target_date"]
            code = row["metric_code"]
            v = row["numeric_value"]
            if v is None:
                continue
            out.setdefault(cid, {}).setdefault(d, {})[code] = float(v)

    result: dict[int, list[tuple[date, dict[str, float]]]] = {}
    for cid, by_date in out.items():
        rows = []
        for iso, metrics in by_date.items():
            try:
                rows.append((date.fromisoformat(iso), metrics))
            except ValueError:
                continue
        rows.sort(key=lambda x: x[0])
        result[cid] = rows
    return result


def _applicable_metrics(
    rows: list[tuple[date, dict[str, float]]],
    cutoff: date,
) -> dict[str, float]:
    """Return a merged view of all derived metric values as of `cutoff`.

    Walks FYs in ascending order and overlays each, so later FYs overwrite
    earlier ones. Any code we've ever seen up to the cutoff is returned —
    this matters because a given FY entry may not include every metric.
    """
    merged: dict[str, float] = {}
    for d, metrics in rows:
        if d > cutoff:
            break
        merged.update(metrics)
    return merged


class DeriveUniverseRequest(BaseModel):
    base_universe_id: int
    label: str | None = None  # required for non-preview
    description: str | None = None
    filter_config: dict


@app.get("/api/universe/derived-metrics/criteria")
async def universe_derived_criteria():
    """Return criterion specs + default filter_config for the /universe UI."""
    from universe.derived_metrics import CRITERIA_SPECS, default_filter_config

    specs = []
    for s in CRITERIA_SPECS:
        entry: dict = {
            "key": s.key,
            "label": s.label,
            "default_threshold": s.default_threshold,
            "default_enabled": s.default_enabled,
        }
        if s.components is not None:
            entry["components"] = [
                {"label": label, "code": code, "default": default}
                for label, code, default in s.components
            ]
        else:
            entry["metric"] = s.metric
            entry["op"] = s.op
        specs.append(entry)
    return {"specs": specs, "default_filter_config": default_filter_config()}


@app.get("/api/universe/derived-metrics/status")
async def universe_derived_status():
    """How many companies have at least one derived metric row stored."""
    def _run():
        resp = (
            supabase.table("metric_data")
            .select("company_id", count="exact")
            .eq("source_code", "derived")
            .limit(1)
            .execute()
        )
        total_rows = resp.count or 0

        # Distinct companies: pull company_ids in pages (Supabase has no DISTINCT here)
        seen: set[int] = set()
        offset = 0
        page = 1000
        while True:
            r = (
                supabase.table("metric_data")
                .select("company_id")
                .eq("source_code", "derived")
                .range(offset, offset + page - 1)
                .execute()
            )
            batch = r.data or []
            for row in batch:
                seen.add(row["company_id"])
            if len(batch) < page:
                break
            offset += page
        return {"companies_with_derived_metrics": len(seen), "total_rows": total_rows}

    return await asyncio.to_thread(_run)


class RecomputeRequest(BaseModel):
    universe_ids: list[int] | None = None  # None = all companies in any universe


@app.post("/api/universe/derived-metrics/recompute")
async def universe_derived_recompute(body: RecomputeRequest = RecomputeRequest()):
    """Recompute derived metric values from cached GuruFocus annuals. SSE stream."""
    import queue
    from universe.derived_metrics import precompute_for_companies

    def _collect_companies() -> list[dict]:
        # Scope: companies that appear in at least one universe_membership row.
        # If universe_ids is given, restrict to those.
        cid_set: set[int] = set()
        offset = 0
        page = 1000
        q = supabase.table("universe_membership").select("company_id")
        if body.universe_ids:
            q = q.in_("universe_id", body.universe_ids)
        while True:
            r = q.range(offset, offset + page - 1).execute()
            batch = r.data or []
            for row in batch:
                if row.get("company_id"):
                    cid_set.add(row["company_id"])
            if len(batch) < page:
                break
            offset += page

        ids = sorted(cid_set)
        if not ids:
            return []

        # Hydrate ticker + exchange
        companies: list[dict] = []
        for i in range(0, len(ids), 50):
            batch = ids[i:i + 50]
            r = supabase.table("company").select(
                "company_id, gurufocus_ticker, company_name, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code)"
            ).in_("company_id", batch).execute()
            for c in (r.data or []):
                exch = c.pop("gurufocus_exchange", None) or {}
                c["gurufocus_exchange"] = exch.get("exchange_code")
                companies.append(c)
        return companies

    def _run(q: "queue.Queue"):
        companies = _collect_companies()
        q.put(json.dumps({
            "type": "progress",
            "message": f"Found {len(companies)} companies across selected universes.",
        }))
        for event in precompute_for_companies(supabase, companies):
            q.put(json.dumps(event))
        q.put(None)

    async def generate():
        yield ": keepalive\n\n"
        qq: "queue.Queue" = queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, qq)
        while True:
            try:
                msg = await asyncio.to_thread(qq.get, timeout=0.15)
            except Exception:
                if task.done():
                    while not qq.empty():
                        m = qq.get_nowait()
                        if m is not None:
                            yield f"data: {m}\n\n"
                    break
                continue
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.post("/api/universe/derive/preview")
async def universe_derive_preview(body: DeriveUniverseRequest):
    """Count how many companies per month would survive the filter. No writes."""
    from universe.derived_metrics import company_passes, required_metric_codes

    def _run():
        base_resp = supabase.table("universe").select(
            "universe_id, label"
        ).eq("universe_id", body.base_universe_id).limit(1).execute()
        if not base_resp.data:
            raise HTTPException(status_code=404, detail="base universe not found")

        # Load all memberships of the base
        rows: list[dict] = []
        offset = 0
        page = 1000
        while True:
            r = (
                supabase.table("universe_membership")
                .select("target_month, company_id")
                .eq("universe_id", body.base_universe_id)
                .range(offset, offset + page - 1)
                .execute()
            )
            batch = r.data or []
            rows.extend(batch)
            if len(batch) < page:
                break
            offset += page

        if not rows:
            return {"monthly_counts": [], "base_rows": 0, "passed_rows": 0, "missing_metrics": 0}

        codes = required_metric_codes(body.filter_config)
        cids = sorted({r["company_id"] for r in rows})

        if not codes:
            # Nothing enabled → filter is a no-op, every base row passes.
            by_month: dict[str, int] = {}
            for r in rows:
                by_month[r["target_month"]] = by_month.get(r["target_month"], 0) + 1
            return {
                "monthly_counts": [
                    {"month": m, "count": c} for m, c in sorted(by_month.items())
                ],
                "base_rows": len(rows),
                "passed_rows": len(rows),
                "missing_metrics": 0,
                "base_label": base_resp.data[0]["label"],
            }

        metrics_by_cid = _load_derived_metrics(cids, codes)

        missing = 0
        pass_by_month: dict[str, int] = {}
        base_by_month: dict[str, int] = {}
        for row in rows:
            m = row["target_month"]
            cid = row["company_id"]
            base_by_month[m] = base_by_month.get(m, 0) + 1
            fy_rows = metrics_by_cid.get(cid, [])
            if not fy_rows:
                missing += 1
                continue
            cutoff = _cutoff_for_target_month(m)
            applicable = _applicable_metrics(fy_rows, cutoff)
            if company_passes(body.filter_config, applicable):
                pass_by_month[m] = pass_by_month.get(m, 0) + 1

        months_sorted = sorted(base_by_month.keys())
        return {
            "monthly_counts": [
                {"month": m, "count": pass_by_month.get(m, 0), "base_count": base_by_month[m]}
                for m in months_sorted
            ],
            "base_rows": len(rows),
            "passed_rows": sum(pass_by_month.values()),
            "missing_metrics": missing,
            "base_label": base_resp.data[0]["label"],
        }

    return await asyncio.to_thread(_run)


@app.post("/api/universe/derive")
async def universe_derive_create(body: DeriveUniverseRequest):
    """Create a new derived universe (SSE: precompute derived metrics → filter → insert)."""
    import queue
    from universe.derived_metrics import (
        company_passes,
        required_metric_codes,
        precompute_for_companies,
    )

    def _run(q: "queue.Queue"):
        def emit(step: str, status: str, message: str, **extra):
            q.put(json.dumps({
                "type": "progress", "step": step, "status": status, "message": message, **extra,
            }))

        try:
            label = (body.label or "").strip()
            if not label:
                q.put(json.dumps({"type": "error", "message": "label is required"}))
                return

            emit("validate", "in_progress", "Validating inputs...")
            base_resp = supabase.table("universe").select(
                "universe_id, label"
            ).eq("universe_id", body.base_universe_id).limit(1).execute()
            if not base_resp.data:
                q.put(json.dumps({"type": "error", "message": "base universe not found"}))
                return
            base_label = base_resp.data[0]["label"]

            dup = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
            if dup.data:
                q.put(json.dumps({"type": "error", "message": f"label '{label}' already exists"}))
                return
            emit("validate", "done", f"Base: {base_label} → new label: {label}")

            # --- Load base memberships -------------------------------------------------
            emit("load_base", "in_progress", f"Loading memberships from {base_label}...")
            rows: list[dict] = []
            offset = 0
            page = 1000
            while True:
                r = (
                    supabase.table("universe_membership")
                    .select("target_month, company_id, universe_ticker, sector")
                    .eq("universe_id", body.base_universe_id)
                    .range(offset, offset + page - 1)
                    .execute()
                )
                batch = r.data or []
                rows.extend(batch)
                if len(batch) < page:
                    break
                offset += page
            months = sorted({r["target_month"] for r in rows if r.get("target_month")})
            cids = sorted({r["company_id"] for r in rows})
            emit(
                "load_base", "done",
                f"Loaded {len(rows):,} memberships across {len(months)} months, {len(cids)} companies.",
            )

            # --- Precompute derived metrics for the base companies ---------------------
            codes = required_metric_codes(body.filter_config)
            if codes and cids:
                emit(
                    "precompute", "in_progress",
                    f"Precomputing derived metrics for {len(cids)} companies...",
                )
                companies: list[dict] = []
                for i in range(0, len(cids), 50):
                    batch = cids[i:i + 50]
                    r = supabase.table("company").select(
                        "company_id, gurufocus_ticker, company_name, "
                        "gurufocus_exchange:gurufocus_exchange(exchange_code)"
                    ).in_("company_id", batch).execute()
                    for c in (r.data or []):
                        exch = c.pop("gurufocus_exchange", None) or {}
                        c["gurufocus_exchange"] = exch.get("exchange_code")
                        companies.append(c)

                last_done_summary = ""
                for ev in precompute_for_companies(supabase, companies):
                    etype = ev.get("type")
                    msg = ev.get("message", "")
                    if etype == "progress_update":
                        emit("precompute", "in_progress", msg)
                    elif etype == "done":
                        last_done_summary = msg
                emit("precompute", "done", last_done_summary or "Derived metrics up to date.")
            else:
                emit("precompute", "done", "No filters enabled; skipping precompute.")

            # --- Load metrics and apply filter ----------------------------------------
            emit("filter", "in_progress", f"Applying filter to {len(rows):,} rows...")
            metrics_by_cid = _load_derived_metrics(cids, codes) if codes else {}

            kept: list[dict] = []
            missing = 0
            for row in rows:
                cid = row["company_id"]
                if not codes:
                    kept.append(row)
                    continue
                fy_rows = metrics_by_cid.get(cid, [])
                if not fy_rows:
                    missing += 1
                    continue
                cutoff = _cutoff_for_target_month(row["target_month"])
                applicable = _applicable_metrics(fy_rows, cutoff)
                if company_passes(body.filter_config, applicable):
                    kept.append(row)
            emit(
                "filter", "done",
                f"{len(kept):,} / {len(rows):,} rows pass"
                + (f" ({missing:,} excluded for missing metrics)." if missing else "."),
            )

            if not kept:
                q.put(json.dumps({
                    "type": "error",
                    "message": "Filter matches zero rows — adjust thresholds or precompute metrics.",
                }))
                return

            # --- Create universe row --------------------------------------------------
            emit("create", "in_progress", "Creating universe row...")
            created = supabase.table("universe").insert({
                "label": label,
                "description": body.description,
                "parent_universe_id": body.base_universe_id,
                "filter_config": body.filter_config,
            }).execute()
            new_id = created.data[0]["universe_id"]
            emit("create", "done", f"Universe created (id={new_id}).")

            # --- Insert memberships in batches ----------------------------------------
            payload = [
                {
                    "universe_id": new_id,
                    "company_id": r["company_id"],
                    "target_month": r["target_month"],
                    "universe_ticker": r.get("universe_ticker"),
                    "sector": r.get("sector"),
                }
                for r in kept
            ]
            import time as _time
            from universe.derived_metrics import _fmt_duration as _fmt_dur
            batch_size = 500
            total_inserted = 0
            total_batches = (len(payload) + batch_size - 1) // batch_size
            insert_started = _time.monotonic()
            emit(
                "insert", "in_progress",
                f"Inserting {len(payload):,} rows in {total_batches} batches...",
            )
            for bi, i in enumerate(range(0, len(payload), batch_size), start=1):
                chunk = payload[i:i + batch_size]
                elapsed = _time.monotonic() - insert_started
                rate = (bi - 1) / elapsed if elapsed > 0 and bi > 1 else 0
                remaining = (total_batches - bi + 1) / rate if rate > 0 else 0
                emit(
                    "insert", "in_progress",
                    f"Starting batch {bi}/{total_batches} ({len(chunk):,} rows) · "
                    f"{_fmt_dur(elapsed)} elapsed · ~{_fmt_dur(remaining)} left",
                )
                try:
                    resp = supabase.table("universe_membership").insert(chunk).execute()
                    total_inserted += len(resp.data or [])
                except Exception as batch_exc:
                    emit(
                        "insert", "in_progress",
                        f"Batch {bi}/{total_batches} failed: {batch_exc}. Retrying once...",
                    )
                    resp = supabase.table("universe_membership").insert(chunk).execute()
                    total_inserted += len(resp.data or [])
                elapsed = _time.monotonic() - insert_started
                rate = bi / elapsed if elapsed > 0 else 0
                remaining = (total_batches - bi) / rate if rate > 0 else 0
                emit(
                    "insert", "in_progress",
                    f"Batch {bi}/{total_batches} done · {total_inserted:,}/{len(payload):,} rows · "
                    f"{_fmt_dur(elapsed)} elapsed · ~{_fmt_dur(remaining)} left",
                )
            emit("insert", "done", f"Inserted {total_inserted:,} rows in {_fmt_dur(_time.monotonic() - insert_started)}.")

            q.put(json.dumps({
                "type": "done",
                "message": f"Created '{label}' from {base_label} with {total_inserted:,} rows.",
                "data": {
                    "universe_id": new_id,
                    "label": label,
                    "rows_inserted": total_inserted,
                    "base_universe_id": body.base_universe_id,
                    "base_label": base_label,
                },
            }))
        except Exception as exc:
            logger.exception("universe/derive failed")
            q.put(json.dumps({"type": "error", "message": str(exc)}))
        finally:
            q.put(None)

    async def generate():
        yield ": keepalive\n\n"
        qq: "queue.Queue" = queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, qq)
        while True:
            try:
                msg = await asyncio.to_thread(qq.get, timeout=0.15)
            except Exception:
                if task.done():
                    while not qq.empty():
                        m = qq.get_nowait()
                        if m is not None:
                            yield f"data: {m}\n\n"
                    break
                continue
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/api/universe/validate")
async def universe_validate():
    """Compare current screening results against LongEquity snapshots."""
    def _run():
        resp = supabase.table("company").select(
            "company_id, gurufocus_ticker, exchange_id, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))"
        ).limit(10000).execute()
        companies = []
        for c in (resp.data or []):
            exch_info = c.pop("gurufocus_exchange", None) or {}
            country_info = exch_info.pop("country", None) or {}
            c["gurufocus_exchange"] = exch_info.get("exchange_code")
            c["country"] = country_info.get("country_name")
            companies.append(c)

        results = []
        for event in screen_universe(supabase, companies):
            if event["type"] == "done":
                results = event["data"]["results"]
                break

        return validate_vs_longequity(supabase, results)

    return await asyncio.to_thread(_run)


# ───────────────────────────────────────────────────────────
# Index Universe endpoints
# ───────────────────────────────────────────────────────────

@app.post("/api/index-universe/import-sp500")
async def index_universe_import_sp500():
    """Scrape S&P 500 from Wikipedia, reconstruct monthly holdings, store in DB."""
    import queue as _queue

    def _run(q: _queue.Queue):
        def emit(msg: str):
            q.put(json.dumps({"type": "progress", "message": msg}))

        try:
            emit("Scraping S&P 500 from Wikipedia...")
            current, changes, wiki_company_info = scrape_sp500()
            emit(f"Found {len(current)} current tickers, {len(changes)} historical changes")

            emit("Reconstructing monthly holdings (2000-01 onwards)...")
            monthly, filtered_changes = reconstruct_monthly_holdings(current, changes)
            emit(f"Reconstructed {len(monthly)} months ({min(monthly)}..{max(monthly)}), {len(filtered_changes)} changes")

            # Collect all unique tickers and resolve via OpenFIGI
            all_tickers: set[str] = set()
            for t in monthly.values():
                all_tickers |= t
            emit(f"Resolving {len(all_tickers)} unique tickers...")

            company_lookup = resolve_and_create_companies(
                supabase, all_tickers, on_progress=emit, company_info=wiki_company_info,
            )

            emit("Storing in database...")
            stats = store_index_membership(
                supabase, "SP500", monthly, filtered_changes, company_lookup,
                on_progress=emit,
            )

            q.put(json.dumps({
                "type": "done",
                "message": (
                    f"Import complete. {stats['months']} months, "
                    f"{stats['total_rows']} rows, "
                    f"{stats['unique_tickers']} unique tickers "
                    f"({stats['matched_companies']} matched to companies), "
                    f"{stats['changes_count']} changes stored"
                ),
            }))
        except Exception as e:
            import traceback
            q.put(json.dumps({"type": "error", "message": f"{e}\n{traceback.format_exc()}"}))
        q.put(None)

    async def generate():
        yield ": keepalive\n\n"
        q: _queue.Queue = _queue.Queue()
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


@app.get("/api/index-universe/indexes")
async def index_universe_list():
    """List all stored index universes with month range and unique ticker counts.
    Aggregates are precomputed by the universe_stats view — querying membership
    rows directly and counting in Python ran ~70s for SP500 + ACWI."""
    def _run():
        resp = (
            supabase.table("universe_stats")
            .select("*")
            .order("label")
            .execute()
        )
        result = []
        for r in (resp.data or []):
            if not r.get("start_month"):
                continue  # skip empty universes
            result.append({
                "index_name": r["label"],
                "description": r.get("description"),
                "created_at": r.get("created_at"),
                "start_month": r.get("start_month"),
                "end_month": r.get("end_month"),
                "month_count": r.get("month_count") or 0,
                "total_unique_tickers": r.get("total_unique_tickers") or 0,
            })
        return result
    return await asyncio.to_thread(_run)


@app.get("/api/index-universe/months")
async def index_universe_months(index: str = "SP500"):
    """List months for a given index with ticker counts."""
    def _run():
        u_resp = supabase.table("universe").select("universe_id").eq("label", index).limit(1).execute()
        if not u_resp.data:
            return []
        universe_id = u_resp.data[0]["universe_id"]
        resp = supabase.table("universe_membership").select("target_month").eq("universe_id", universe_id).limit(100000).execute()
        from collections import Counter
        counts = Counter(r["target_month"] for r in (resp.data or []))
        return [{"target_month": m, "count": c} for m, c in sorted(counts.items())]
    return await asyncio.to_thread(_run)


def _enrich_tickers(rows: list[dict]) -> list[dict]:
    """Add company_name, exchange, and gurufocus_url to ticker rows."""
    company_ids = [r["company_id"] for r in rows if r["company_id"]]
    company_info: dict[int, dict] = {}
    for i in range(0, len(company_ids), 50):
        chunk = company_ids[i : i + 50]
        resp = supabase.table("company").select(
            "company_id, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code)"
        ).in_("company_id", chunk).execute()
        for c in resp.data or []:
            exch_info = c.get("gurufocus_exchange") or {}
            company_info[c["company_id"]] = {
                "company_name": c.get("company_name") or "",
                "exchange": exch_info.get("exchange_code") or "",
            }

    result = []
    for r in rows:
        info = company_info.get(r["company_id"], {}) if r["company_id"] else {}
        ticker = r["ticker"]
        result.append({
            "ticker": ticker,
            "company_id": r["company_id"],
            "company_name": info.get("company_name") or None,
            "exchange": info.get("exchange") or None,
            "gurufocus_url": f"https://www.gurufocus.com/stock/{ticker}/summary",
        })
    return result


@app.get("/api/index-universe/tickers")
async def index_universe_tickers(index: str = "SP500", month: str = ""):
    """Get tickers for a specific month of an index."""
    if not month:
        raise HTTPException(400, "month query param required")

    def _run():
        u_resp = supabase.table("universe").select("universe_id").eq("label", index).limit(1).execute()
        if not u_resp.data:
            return []
        universe_id = u_resp.data[0]["universe_id"]
        rows = (
            supabase.table("universe_membership")
            .select("universe_ticker, company_id")
            .eq("universe_id", universe_id)
            .eq("target_month", month)
            .order("universe_ticker")
            .execute()
        ).data or []
        # Map universe_ticker -> ticker for _enrich_tickers compatibility
        for r in rows:
            r["ticker"] = r.pop("universe_ticker", "")
        return _enrich_tickers(rows)

    return await asyncio.to_thread(_run)


@app.get("/api/index-universe/cumulative")
async def index_universe_cumulative(index: str = "SP500"):
    """Get all unique tickers across all months for an index, with company + GF info."""
    def _run():
        u_resp = supabase.table("universe").select("universe_id").eq("label", index).limit(1).execute()
        if not u_resp.data:
            return []
        universe_id = u_resp.data[0]["universe_id"]
        # Get all distinct (universe_ticker, company_id) pairs
        resp = supabase.table("universe_membership").select(
            "universe_ticker, company_id"
        ).eq("universe_id", universe_id).limit(100000).execute()
        # Deduplicate by ticker
        seen: dict[str, dict] = {}
        for r in (resp.data or []):
            t = r.get("universe_ticker")
            if t and t not in seen:
                seen[t] = {"ticker": t, "company_id": r["company_id"]}
        return _enrich_tickers(list(seen.values()))

    return await asyncio.to_thread(_run)


@app.post("/api/index-universe/check-gurufocus")
async def index_universe_check_gf(index: str = "SP500"):
    """Check GuruFocus cache coverage for all tickers in an index. SSE stream."""
    import queue as _queue

    def _run(q: _queue.Queue):
        def emit(msg: str):
            q.put(json.dumps({"type": "progress", "message": msg}))

        try:
            # Collect all unique tickers for this index
            emit(f"Loading tickers for {index}...")
            u_resp = supabase.table("universe").select("universe_id").eq("label", index).limit(1).execute()
            if not u_resp.data:
                q.put(json.dumps({"type": "error", "message": f"Universe '{index}' not found"}))
                q.put(None)
                return
            universe_id = u_resp.data[0]["universe_id"]
            all_tickers: set[str] = set()
            resp = (
                supabase.table("universe_membership")
                .select("universe_ticker")
                .eq("universe_id", universe_id)
                .limit(100000)
                .execute()
            )
            for r in resp.data or []:
                if r.get("universe_ticker"):
                    all_tickers.add(r["universe_ticker"])

            emit(f"Found {len(all_tickers)} unique tickers across all months")

            result = check_gurufocus_availability(supabase, all_tickers, on_progress=emit)
            q.put(json.dumps({"type": "done", "data": result}))
        except Exception as e:
            q.put(json.dumps({"type": "error", "message": str(e)}))
        q.put(None)

    async def generate():
        yield ": keepalive\n\n"
        q: _queue.Queue = _queue.Queue()
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


@app.get("/api/index-universe/changes")
async def index_universe_changes(index: str = "SP500"):
    """Get the changelog (additions/removals) for an index."""
    return await asyncio.to_thread(lambda: load_changes(supabase, index))


@app.delete("/api/index-universe/indexes/{index_name}")
async def index_universe_delete(index_name: str):
    """Delete all data for an index (deletes the universe and cascades memberships)."""
    await asyncio.to_thread(
        lambda: supabase.table("universe").delete().eq("label", index_name).execute()
    )
    return {"ok": True}


@app.get("/api/acwi/holdings")
async def acwi_holdings():
    """Return parsed ACWI ETF holdings from the local XLS file."""
    def work():
        holdings, as_of = load_acwi_holdings()

        # Load exchange→currency from gurufocus_exchange table
        try:
            rows = supabase.table("gurufocus_exchange").select("exchange_code,currency_code").execute()
            db_currencies = {r["exchange_code"]: r["currency_code"] for r in (rows.data or [])}
        except Exception:
            db_currencies = {}

        for h in holdings:
            exch = h.get("Exchange", "")
            h["gurufocus_url"] = gurufocus_url(h.get("Ticker", ""), exch)
            gf_code = gurufocus_exchange(exch)
            h["gf_exchange"] = gf_code if gf_code else None
            # Look up currency from DB using the exchange code
            db_code = gurufocus_exchange_for_db(exch)
            h["gf_currency"] = db_currencies.get(db_code) if db_code else None

        return {"holdings": holdings, "count": len(holdings), "as_of": as_of}

    return await asyncio.to_thread(work)


@app.get("/api/acwi/announcements")
async def acwi_announcements(refresh: bool = False):
    """Get MSCI index announcements (cached locally, 24h TTL)."""
    rows = await asyncio.to_thread(get_msci_announcements, refresh)
    return {"announcements": rows, "count": len(rows)}


@app.get("/api/acwi/announcement-detail")
async def acwi_announcement_detail(url: str):
    """Fetch detail (STANDARD action + EFFECTIVE DATE) from an individual MSCI announcement."""
    detail = await asyncio.to_thread(fetch_announcement_detail_cached, url)
    return detail


@app.post("/api/acwi/announcement-details-bulk")
async def acwi_announcement_details_bulk(body: dict):
    """Fetch details for multiple announcement URLs. Body: {"urls": [...]}."""
    urls = body.get("urls", [])
    results = await asyncio.to_thread(fetch_bulk_details, urls)
    return {"details": results}


@app.get("/api/acwi/net-additions")
async def acwi_net_additions():
    """Compute net additions matched against current holdings."""
    results = await asyncio.to_thread(compute_net_additions)
    matched = sum(1 for r in results if r["matched"])
    return {"net_additions": results, "total": len(results), "matched": matched}


class AcwiSaveUniverseRequest(BaseModel):
    name: str = "ACWI"
    start_date: str
    end_date: str


@app.post("/api/acwi/save-universe")
async def acwi_save_universe(req: AcwiSaveUniverseRequest):
    """SSE stream: reconstruct monthly ACWI feasible-universe holdings and save as a universe.

    The saved universe can be selected in the momentum backtester via `index_universe`.
    """
    import queue as _queue, threading

    def _run(q: _queue.Queue):
        def emit(message: str, pct: int | None = None):
            payload = {"type": "progress", "message": message}
            if pct is not None:
                payload["pct"] = pct
            q.put(json.dumps(payload))

        try:
            emit("Loading feasible ACWI holdings from iShares XLS...", 3)
            feasible = acwi_feasible_holdings_for_db()
            emit(f"Found {len(feasible)} feasible holdings", 5)

            # Load exchange_id map: exchange_code -> exchange_id
            exch_resp = supabase.table("gurufocus_exchange").select("exchange_id, exchange_code").execute()
            exch_id_map = {r["exchange_code"]: r["exchange_id"] for r in (exch_resp.data or [])}

            # Bulk-load existing company rows for all exchanges we care about (single query)
            needed_exchanges = {fh["db_exchange"] for fh in feasible}
            needed_eids = [exch_id_map[e] for e in needed_exchanges if e in exch_id_map]
            existing_by_key: dict[tuple[int, str], int] = {}
            if needed_eids:
                offset = 0
                page_size = 1000
                while True:
                    c_resp = (
                        supabase.table("company")
                        .select("company_id, gurufocus_ticker, exchange_id")
                        .in_("exchange_id", needed_eids)
                        .range(offset, offset + page_size - 1)
                        .execute()
                    )
                    batch = c_resp.data or []
                    for c in batch:
                        if c.get("gurufocus_ticker") and c.get("exchange_id") is not None:
                            existing_by_key[(c["exchange_id"], c["gurufocus_ticker"])] = c["company_id"]
                    if len(batch) < page_size:
                        break
                    offset += page_size
            emit(f"Loaded {len(existing_by_key)} existing company rows across {len(needed_eids)} exchanges", 10)

            company_lookup: dict[str, int] = {}
            sector_lookup: dict[str, str] = {
                fh["symbol"]: fh["sector"] for fh in feasible if fh.get("sector")
            }
            created = 0
            already = 0
            skipped = 0
            unknown_exchanges: set[str] = set()
            for idx, fh in enumerate(feasible):
                eid = exch_id_map.get(fh["db_exchange"])
                if eid is None:
                    skipped += 1
                    unknown_exchanges.add(fh["db_exchange"])
                    continue
                key = (eid, fh["gf_ticker"])
                cid = existing_by_key.get(key)
                if cid is not None:
                    company_lookup[fh["symbol"]] = cid
                    already += 1
                else:
                    try:
                        ins = supabase.table("company").insert({
                            "gurufocus_ticker": fh["gf_ticker"],
                            "exchange_id": eid,
                            "company_name": fh["company_name"] or None,
                        }).execute()
                        if ins.data:
                            cid = ins.data[0]["company_id"]
                            existing_by_key[key] = cid
                            company_lookup[fh["symbol"]] = cid
                            created += 1
                    except Exception as e:
                        skipped += 1
                        emit(f"  failed to create {fh['symbol']} ({fh['company_name']}): {e}", None)
                        continue
                    # Tag with 'acwi' source
                    try:
                        supabase.table("company_source").upsert(
                            {"company_id": cid, "source_code": "acwi"},
                            on_conflict="company_id,source_code",
                            ignore_duplicates=True,
                        ).execute()
                    except Exception:
                        pass

                if (idx + 1) % 200 == 0 or idx == len(feasible) - 1:
                    pct = 10 + round((idx + 1) / len(feasible) * 30)
                    emit(f"Companies: {created} created, {already} existing, {skipped} skipped ({idx + 1}/{len(feasible)})", pct)

            if unknown_exchanges:
                emit(f"Unknown exchanges (missing from gurufocus_exchange): {sorted(unknown_exchanges)}", None)
            emit(f"Company sync done: {created} new, {already} existing, {skipped} skipped", 42)

            emit(f"Reconstructing monthly holdings {req.start_date}..{req.end_date}...", 45)
            monthly, stats = reconstruct_acwi_monthly_holdings(req.start_date, req.end_date)
            emit(
                f"Built {stats['months']} months: {stats['feasible_count']} feasible tickers "
                f"({stats['with_addition']} with matched addition, {stats['grandfathered']} grandfathered)",
                55,
            )

            emit(f"Writing universe '{req.name}' to database...", 60)
            store_stats = store_index_membership(
                supabase, req.name, monthly, [], company_lookup,
                on_progress=lambda m: emit(m, None),
                sector_lookup=sector_lookup,
            )

            q.put(json.dumps({
                "type": "done",
                "message": (
                    f"Saved '{req.name}': {store_stats['months']} months, "
                    f"{store_stats['total_rows']} rows, "
                    f"{store_stats['matched_companies']}/{store_stats['unique_tickers']} tickers matched "
                    f"({created} new companies created, {already} existing)"
                ),
                "stats": {
                    "name": req.name,
                    "months": store_stats["months"],
                    "total_rows": store_stats["total_rows"],
                    "unique_tickers": store_stats["unique_tickers"],
                    "matched_companies": store_stats["matched_companies"],
                    "companies_created": created,
                    "companies_existing": already,
                    "companies_skipped": skipped,
                    "feasible_count": stats["feasible_count"],
                    "grandfathered": stats["grandfathered"],
                    "with_addition": stats["with_addition"],
                },
            }))
        except Exception as e:
            import traceback
            q.put(json.dumps({"type": "error", "message": f"{e}\n{traceback.format_exc()}"}))
        q.put(None)

    q: _queue.Queue = _queue.Queue()
    threading.Thread(target=_run, args=(q,), daemon=True).start()

    async def generate():
        yield ": keepalive\n\n"
        while True:
            msg = await asyncio.to_thread(q.get)
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/api/acwi/fetch-all-details")
async def acwi_fetch_all_details():
    """SSE stream: fetch details for all constituent changes not yet cached."""
    import queue, threading

    def _emit(obj: dict) -> str:
        return json.dumps(obj, default=str)

    q: queue.Queue[str | None] = queue.Queue()

    def worker():
        try:
            anns = get_msci_announcements()
            constituent = [a for a in anns if a.get("is_constituent_change") and a.get("href")]
            cache = _load_detail_cache()

            to_fetch = [a for a in constituent if a["href"] not in cache]
            total = len(to_fetch)
            already_cached = len(constituent) - total

            q.put(_emit({"type": "progress", "message": f"{already_cached} cached, {total} to fetch", "fetched": 0, "total": total}))

            if total == 0:
                q.put(_emit({"type": "done", "message": "All details already cached", "fetched": 0, "total": 0, "errors": 0, "cached": already_cached}))
                q.put(None)
                return

            fetched = 0
            errors = 0
            error_list: list[dict] = []
            for a in to_fetch:
                try:
                    detail = fetch_announcement_detail(a["href"])
                except Exception as e:
                    detail = {"standard": None, "effective_date": None, "error": str(e)}
                    errors += 1
                    error_list.append({"title": a.get("title", ""), "href": a["href"], "error": str(e)})
                cache[a["href"]] = detail
                fetched += 1

                if fetched % 10 == 0 or fetched == total:
                    _save_detail_cache(cache)
                    q.put(_emit({
                        "type": "progress",
                        "message": f"Fetched {fetched}/{total}" + (f" ({errors} errors)" if errors else ""),
                        "fetched": fetched,
                        "total": total,
                        "pct": round(fetched / total * 100),
                        "errors": errors,
                    }))

            _save_detail_cache(cache)
            q.put(_emit({
                "type": "done",
                "message": f"Done. Fetched {fetched}, {errors} errors, {already_cached} were cached",
                "fetched": fetched,
                "total": total,
                "errors": errors,
                "cached": already_cached,
                "error_list": error_list[:50],  # cap to avoid huge payload
            }))
        except Exception as e:
            q.put(_emit({"type": "error", "message": str(e)}))
        q.put(None)

    threading.Thread(target=worker, daemon=True).start()

    async def generate():
        yield ": keepalive\n\n"
        while True:
            msg = await asyncio.to_thread(q.get)
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# FX Rates (ECB)
# ---------------------------------------------------------------------------

@app.get("/api/fx/coverage")
async def fx_coverage():
    """Compare ACWI currencies against ECB FX rate availability."""
    return await asyncio.to_thread(get_coverage_info)


@app.get("/api/fx/latest")
async def fx_latest():
    """Get latest daily rates for all currencies vs EUR (ECB + pegged + TWD)."""
    rates = await asyncio.to_thread(fetch_all_latest)
    return {"rates": rates, "count": len(rates)}


@app.get("/api/fx/history/{currency}")
async def fx_history(currency: str, start_date: str | None = None):
    """Get daily historical FX rates for a currency vs EUR."""
    currency = currency.upper()
    rates = await asyncio.to_thread(fetch_history, currency, start_date)
    return {"currency": currency, "rates": rates, "count": len(rates)}


# ---------------------------------------------------------------------------
# Indicators (GuruFocus fetch + cache + DB)
# ---------------------------------------------------------------------------

class IndicatorRequest(BaseModel):
    exchange: str
    ticker: str
    indicator: str = "price"
    force_refresh: bool = False
    from_date: str | None = None
    to_date: str | None = None


@app.post("/api/indicators/fetch")
async def indicators_fetch(req: IndicatorRequest):
    """Fetch an indicator from GuruFocus, cache in storage, store in DB.

    Looks up or creates a company row, fetches indicator data (from cache
    or API), stores raw JSON in Supabase Storage, and upserts parsed
    time-series into metric_data.
    """
    from ingest.prices import (
        _build_symbol, _storage_path, _ensure_bucket,
        _fetch_from_storage, _upload_to_storage,
        _fetch_indicator_from_api, _parse_price_series,
        _PRICE_CUTOFF,
    )
    from ingest.api_usage import track_api_call

    exchange = req.exchange.upper()
    ticker = req.ticker.upper()
    indicator = req.indicator.lower()

    def work():
        symbol = _build_symbol(ticker, exchange)
        path = _storage_path(ticker, exchange, indicator)
        logs = []

        # 1. Find or create company
        # Look up exchange_id
        exch_resp = supabase.table("gurufocus_exchange").select("exchange_id").eq("exchange_code", exchange).limit(1).execute()
        exchange_id = exch_resp.data[0]["exchange_id"] if exch_resp.data else None

        existing = supabase.table("company").select("company_id").eq(
            "gurufocus_ticker", ticker
        ).eq("exchange_id", exchange_id).execute()

        if existing.data:
            company_id = existing.data[0]["company_id"]
            logs.append(f"Found company {symbol} (id={company_id})")
        else:
            created = supabase.table("company").insert({
                "gurufocus_ticker": ticker,
                "exchange_id": exchange_id,
                "company_name": symbol,
            }).execute()
            company_id = created.data[0]["company_id"]
            logs.append(f"Created company {symbol} (id={company_id})")

        # 2. Check cache
        _ensure_bucket(supabase)
        cached = None
        if not req.force_refresh:
            cached = _fetch_from_storage(supabase, path)
            if cached is not None:
                logs.append(f"Found cached data at {path}")

        # 3. Fetch from API if needed
        api_data = None
        if cached is None or req.force_refresh:
            data, api_log, http_status = _fetch_indicator_from_api(ticker, exchange, indicator)
            track_api_call(supabase, exchange)
            logs.append(api_log)
            if data is not None:
                _upload_to_storage(supabase, path, data)
                logs.append(f"Cached raw response to {path}")
                api_data = data
            elif cached is not None:
                logs.append("API failed, falling back to stale cache")
                api_data = cached
            else:
                return {
                    "success": False,
                    "symbol": symbol,
                    "error": f"No data available for {symbol}/{indicator}",
                    "logs": logs,
                }
        else:
            api_data = cached

        # 4. Parse time series
        parsed = _parse_price_series(api_data)
        logs.append(f"Parsed {len(parsed)} data points")

        if not parsed:
            return {
                "success": False,
                "symbol": symbol,
                "error": "Parsed 0 data points from response",
                "logs": logs,
                "raw_preview": str(api_data)[:500],
            }

        # 5. Map indicator name to metric_code
        metric_code_map = {
            "price": "close_price",
            "volume": "volume",
        }
        metric_code = metric_code_map.get(indicator, indicator)

        # 6. Upsert into metric_data
        rows = [
            {
                "company_id": company_id,
                "metric_code": metric_code,
                "source_code": "gurufocus",
                "target_date": d.isoformat(),
                "numeric_value": v,
            }
            for d, v in parsed
            if d >= _PRICE_CUTOFF
        ]
        total_loaded = 0
        batch_size = 500
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            resp = supabase.table("metric_data").upsert(
                batch,
                on_conflict="company_id,metric_code,source_code,target_date",
                ignore_duplicates=False,
            ).execute()
            total_loaded += len(resp.data)

        logs.append(f"Loaded {total_loaded} rows into metric_data (metric_code={metric_code})")

        # 7. Return summary + sample data
        sorted_parsed = sorted(parsed, key=lambda x: x[0])
        date_range = {
            "first": sorted_parsed[0][0].isoformat(),
            "last": sorted_parsed[-1][0].isoformat(),
        }
        # If from_date/to_date provided, return the full filtered window;
        # otherwise fall back to the last 30 data points.
        def _parse_iso(s: str | None):
            if not s:
                return None
            try:
                return date.fromisoformat(s)
            except ValueError:
                return None
        frm = _parse_iso(req.from_date)
        to = _parse_iso(req.to_date)
        if frm or to:
            window = [
                (d, v) for d, v in sorted_parsed
                if (frm is None or d >= frm) and (to is None or d <= to)
            ]
            recent = [{"date": d.isoformat(), "value": v} for d, v in window]
        else:
            recent = [{"date": d.isoformat(), "value": v} for d, v in sorted_parsed[-30:]]

        return {
            "success": True,
            "symbol": symbol,
            "company_id": company_id,
            "indicator": indicator,
            "metric_code": metric_code,
            "total_points": len(parsed),
            "rows_loaded": total_loaded,
            "date_range": date_range,
            "source": "cache" if cached is not None and not req.force_refresh else "api",
            "recent": recent,
            "logs": logs,
        }

    return await asyncio.to_thread(work)


@app.get("/api/gurufocus/exchanges")
async def gurufocus_exchanges(force_refresh: bool = False):
    """Fetch the list of supported GuruFocus exchanges. Cached in Supabase Storage."""
    from ingest.prices import (
        _ensure_bucket, _fetch_from_storage, _upload_to_storage,
        _BUCKET, _USER_AGENT,
    )

    def work():
        path = "meta/exchange_list.json"
        _ensure_bucket(supabase)

        # Check cache
        if not force_refresh:
            cached = _fetch_from_storage(supabase, path)
            if cached is not None:
                return {"exchanges": cached, "source": "cache"}

        # Fetch from API
        base_url = os.environ.get("GURUFOCUS_BASE_URL", "").strip().rstrip("/")
        if base_url.endswith("/data"):
            base_url = base_url[: -len("/data")]
        api_key = os.environ.get("GURUFOCUS_API_KEY", "")
        if not base_url or not api_key:
            raise HTTPException(status_code=500, detail="GURUFOCUS env vars not set")

        import requests as req
        url = f"{base_url}/public/user/{api_key}/exchange_list"
        resp = req.get(url, timeout=30, headers={"User-Agent": _USER_AGENT})
        resp.raise_for_status()
        data = resp.json()

        # Cache raw response
        _upload_to_storage(supabase, path, data)

        return {"exchanges": data, "source": "api"}

    return await asyncio.to_thread(work)


@app.get("/api/gurufocus/exchange-currencies")
async def gurufocus_exchange_currencies(force_refresh: bool = False):
    """Fetch exchange→currency mapping by joining exchange_list and country_currency
    from GuruFocus. Caches raw responses in storage, stores mapping in DB."""
    from ingest.prices import (
        _ensure_bucket, _fetch_from_storage, _upload_to_storage,
        _USER_AGENT,
    )

    def work():
        _ensure_bucket(supabase)
        import requests as req

        base_url = os.environ.get("GURUFOCUS_BASE_URL", "").strip().rstrip("/")
        if base_url.endswith("/data"):
            base_url = base_url[: -len("/data")]
        api_key = os.environ.get("GURUFOCUS_API_KEY", "")
        if not base_url or not api_key:
            raise HTTPException(status_code=500, detail="GURUFOCUS env vars not set")

        # 1. Get exchange_list (country -> [codes])
        exch_path = "meta/exchange_list.json"
        exchanges = None
        if not force_refresh:
            exchanges = _fetch_from_storage(supabase, exch_path)
        if exchanges is None:
            r = req.get(
                f"{base_url}/public/user/{api_key}/exchange_list",
                timeout=30, headers={"User-Agent": _USER_AGENT},
            )
            r.raise_for_status()
            exchanges = r.json()
            _upload_to_storage(supabase, exch_path, exchanges)

        # 2. Get country_currency ([{country, country_ISO, currency}])
        curr_path = "meta/country_currency.json"
        currencies_raw = None
        if not force_refresh:
            currencies_raw = _fetch_from_storage(supabase, curr_path)
        if currencies_raw is None:
            r = req.get(
                f"{base_url}/public/user/{api_key}/country_currency",
                timeout=30, headers={"User-Agent": _USER_AGENT},
            )
            r.raise_for_status()
            currencies_raw = r.json()
            _upload_to_storage(supabase, curr_path, currencies_raw)

        # 3. Join: exchange_code -> {country, currency}
        country_to_currency = {c["country"]: c["currency"] for c in currencies_raw}
        mapping = []
        unmapped = []
        for country, codes in exchanges.items():
            curr = country_to_currency.get(country)
            if curr:
                for code in codes:
                    mapping.append({
                        "exchange_code": code,
                        "country": country,
                        "currency": curr,
                        "source": "gurufocus",
                    })
            else:
                unmapped.append({"country": country, "codes": codes})

        # 4. Return mapping (exchange_currency table was dropped;
        #    gurufocus_exchange table now holds exchange→currency mapping)
        return {
            "mapping": mapping,
            "total": len(mapping),
            "unmapped": unmapped,
            "source": "cache" if not force_refresh else "api",
        }

    return await asyncio.to_thread(work)
