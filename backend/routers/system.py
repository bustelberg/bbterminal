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


def _latest_metric_dates_for(cids: set[int], metric_code: str) -> dict[int, str]:
    """`{company_id: latest target_date}` for `metric_code`, scoped to `cids`,
    via the `company_latest_metric_dates_for` RPC. Chunked (≤800 ids/call) so
    each call rides the metric/company index and returns under the db-max-rows
    cap."""
    out: dict[int, str] = {}
    cid_list = list(cids)
    for i in range(0, len(cid_list), 800):
        chunk = cid_list[i:i + 800]
        resp = supabase.rpc(
            "company_latest_metric_dates_for",
            {"p_company_ids": chunk, "p_metric_code": metric_code},
        ).execute()
        for row in resp.data or []:
            cid, d = row.get("company_id"), row.get("latest_target_date")
            if cid is not None and d:
                out[int(cid)] = str(d)[:10]
    return out


def _excluded_company_ids() -> set[int]:
    """Companies excluded from freshness measures — delisted / out-of-scope /
    illiquid (their perpetually-behind close isn't a valid freshness signal)."""
    excluded: set[int] = set()
    page, offset = 1000, 0
    try:
        for _ in range(20):
            ex = (
                supabase.table("company").select("company_id")
                .or_("delisted_at.not.is.null,out_of_scope_at.not.is.null,illiquid_at.not.is.null")
                .range(offset, offset + page - 1).execute()
            ).data or []
            if not ex:
                break
            for r in ex:
                excluded.add(int(r["company_id"]))
            if len(ex) < page:
                break
            offset += page
    except Exception:
        pass  # best-effort
    return excluded


@router.get("/api/data/universe-coverage")
async def universe_coverage(response: Response):
    """Per-universe price + volume freshness: for each frozen snapshot and each
    template-managed universe, the min/max LATEST close-price and volume date
    across its active members. Surfaces, on the /schedule month-end refresh, how
    fresh each tradable universe's data is — and which lag (the daily price
    job only touches held names, so between rebalances the rest goes stale).

    Returns `{universes: [{universe_id, label, frozen_from, template_key,
    members, priced/volumed counts, price:{min,max}, volume:{min,max}}]}`,
    ordered by label. Cached 1 min (the RPC scans are not cheap)."""
    response.headers["Cache-Control"] = CACHE_PIPELINE

    def _q() -> dict:
        from ingest.phases.planner import _latest_membership_company_ids  # noqa: PLC0415

        # Frozen snapshots + template-managed universes — the tradable sets.
        us = (
            supabase.table("universe")
            .select("universe_id, label, frozen_from, template_key")
            .or_("frozen_at.not.is.null,template_key.not.is.null")
            .execute()
        ).data or []

        members_by_uid: dict[int, set[int]] = {}
        for u in us:
            uid = int(u["universe_id"])
            try:
                members_by_uid[uid] = _latest_membership_company_ids(uid)
            except Exception:
                members_by_uid[uid] = set()
        all_cids: set[int] = set().union(*members_by_uid.values()) if members_by_uid else set()

        close = _latest_metric_dates_for(all_cids, "close_price")
        vol = _latest_metric_dates_for(all_cids, "volume")
        excluded = _excluded_company_ids()

        out: list[dict] = []
        for u in us:
            uid = int(u["universe_id"])
            members = members_by_uid.get(uid, set()) - excluded
            if not members:
                continue
            p_dates = [close[c] for c in members if close.get(c)]
            v_dates = [vol[c] for c in members if vol.get(c)]
            out.append({
                "universe_id": uid,
                "label": u.get("label"),
                "frozen_from": u.get("frozen_from"),
                "template_key": u.get("template_key"),
                "members": len(members),
                "price": {
                    "min": min(p_dates) if p_dates else None,
                    "max": max(p_dates) if p_dates else None,
                    "priced": len(p_dates),
                },
                "volume": {
                    "min": min(v_dates) if v_dates else None,
                    "max": max(v_dates) if v_dates else None,
                    "priced": len(v_dates),
                },
            })
        out.sort(key=lambda x: (x.get("label") or "").lower())
        return {"universes": out}

    return await asyncio.to_thread(_q)
