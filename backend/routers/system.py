"""Small system-level endpoints: health check, hello, GuruFocus API usage.

Endpoints:
    GET /api/hello   sanity ping
    GET /api/health  Supabase connectivity probe (used by uptime checks)
    GET /api/items   demo endpoint kept around for the boilerplate page
    GET /api/usage   GuruFocus API call counter for the current month
"""

from __future__ import annotations

import asyncio
import os

from fastapi import APIRouter, Response

from deps import supabase
from ingest.api_usage import get_usage
from routers._cache_headers import CACHE_PIPELINE

router = APIRouter(tags=["system"])


@router.get("/api/hello")
def hello():
    return {"message": "Hello from FastAPI + uv!"}


@router.get("/api/health")
def health():
    """Probe Supabase connectivity. Returns {status: ok | error, ...}."""
    try:
        url = os.environ.get("SUPABASE_URL", "NOT SET")
        has_key = "YES" if os.environ.get("SUPABASE_SERVICE_KEY") else "NO"
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


@router.get("/api/items")
def get_items():
    try:
        result = supabase.table("items").select("*").execute()
        return {"items": result.data}
    except Exception:
        return {"items": []}


@router.get("/api/usage")
async def api_usage():
    """GuruFocus API usage counter for the current month."""
    return await asyncio.to_thread(get_usage, supabase)


@router.get("/api/data/latest-price-date")
async def latest_price_date(response: Response):
    """Most recent close-price observation across all companies. The
    /backtest page uses this as the default end-date — "test up to
    however current our data is." The `source_code = 'gurufocus'` filter is
    load-bearing: it lets Postgres serve this via the
    `idx_metric_data_source_date` (source_code, target_date) index as a
    backward scan. Without it there's no usable index for
    `metric_code = … ORDER BY target_date DESC`, so prod seq-scans the whole
    `metric_data` table and trips the statement timeout (57014)."""
    response.headers["Cache-Control"] = CACHE_PIPELINE
    def _q() -> dict:
        try:
            resp = (
                supabase.table("metric_data")
                .select("target_date")
                .eq("source_code", "gurufocus")
                .eq("metric_code", "close_price")
                .order("target_date", desc=True)
                .limit(1)
                .execute()
            )
        except Exception as e:
            return {"date": None, "error": f"{type(e).__name__}: {e}"}
        if not resp.data:
            return {"date": None}
        raw = resp.data[0].get("target_date")
        # `target_date` is stored as YYYY-MM-DD; pass through verbatim
        # (slice to 10 chars defensively in case a timestamp ever leaks
        # through).
        return {"date": str(raw)[:10] if raw else None}
    return await asyncio.to_thread(_q)


@router.get("/api/data/price-coverage")
async def price_coverage(response: Response):
    """Freshest + most-stale company by LATEST close-price date — so the
    /schedule month-end refresh can show prices actually moved.

    Reads each company's latest close date from the
    `company_latest_close_price_dates` RPC (the same source the prices phase
    sorts on), then enriches the min/max companies with name / ticker /
    exchange. `newest` = the most recent price held anywhere (should be the last
    trading day right after a refresh); `oldest` = the company whose latest
    price is furthest behind. Both null when no company has prices. Cached (1
    min) since the underlying aggregation isn't cheap."""
    response.headers["Cache-Control"] = CACHE_PIPELINE

    def _q() -> dict:
        latest_by_cid: dict[int, str] = {}
        page, offset = 1000, 0
        for _ in range(20):
            try:
                resp = (
                    supabase.rpc("company_latest_close_price_dates", {})
                    .range(offset, offset + page - 1)
                    .execute()
                )
            except Exception as e:
                return {
                    "newest": None, "oldest": None, "priced_companies": 0,
                    "error": f"{type(e).__name__}: {e}",
                }
            batch = resp.data or []
            if not batch:
                break
            for row in batch:
                cid = row.get("company_id")
                d = row.get("latest_target_date")
                if cid is not None and d:
                    latest_by_cid[int(cid)] = str(d)[:10]
            if len(batch) < page:
                break
            offset += page

        if not latest_by_cid:
            return {"newest": None, "oldest": None, "priced_companies": 0}

        # Exclude companies we've marked as not-validly-priced — delisted /
        # acquired (`delisted_at`), out-of-GuruFocus-coverage (`out_of_scope_at`),
        # or illiquid (`illiquid_at`, trades rarely so GuruFocus serves stale
        # prices). Their perpetually-behind "latest close" isn't a valid measure
        # of how fresh our ACTIVE prices are. (Covestro AG was acquired late
        # 2025; Telecom Italia savings shares MIL:TITR are illiquid — both kept
        # surfacing as the "oldest" until marked.)
        excluded: set[int] = set()
        try:
            ex_page, ex_off = 1000, 0
            for _ in range(20):
                ex = (
                    supabase.table("company").select("company_id")
                    .or_("delisted_at.not.is.null,out_of_scope_at.not.is.null,illiquid_at.not.is.null")
                    .range(ex_off, ex_off + ex_page - 1).execute()
                ).data or []
                if not ex:
                    break
                for r in ex:
                    excluded.add(int(r["company_id"]))
                if len(ex) < ex_page:
                    break
                ex_off += ex_page
        except Exception:
            pass  # best-effort — without exclusion the measure is just noisier
        latest_by_cid = {c: d for c, d in latest_by_cid.items() if c not in excluded}
        if not latest_by_cid:
            return {"newest": None, "oldest": None, "priced_companies": 0}

        oldest_cid = min(latest_by_cid, key=lambda c: latest_by_cid[c])
        newest_cid = max(latest_by_cid, key=lambda c: latest_by_cid[c])

        rows = (
            supabase.table("company")
            .select(
                "company_id, company_name, gurufocus_ticker, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code)"
            )
            .in_("company_id", list({oldest_cid, newest_cid}))
            .execute()
        ).data or []
        info: dict[int, dict] = {}
        for r in rows:
            cid = int(r["company_id"])
            info[cid] = {
                "company_id": cid,
                "company_name": r.get("company_name"),
                "ticker": r.get("gurufocus_ticker"),
                "exchange": (r.get("gurufocus_exchange") or {}).get("exchange_code"),
                "date": latest_by_cid[cid],
            }
        return {
            "newest": info.get(newest_cid),
            "oldest": info.get(oldest_cid),
            "priced_companies": len(latest_by_cid),
        }
    return await asyncio.to_thread(_q)
