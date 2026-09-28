"""Attributed external API usage, aggregated per month.

Each row answers four independent questions: provider (``source``), billing region, the logical
workflow (``job``), and whether the request succeeded (``outcome``). GuruFocus's regional quota
continues to use only ``source='gurufocus'``; unmetered Yahoo calls are recorded alongside it so the
application's total upstream request volume can finally be reconciled.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from typing import Iterator

from supabase import Client

logger = logging.getLogger(__name__)

MONTHLY_API_LIMIT = 20000

US_EXCHANGES = {"NYSE", "NASDAQ", "AMEX", "CBOE", "OTCPK"}
ASIA_EXCHANGES = {
    "TSE", "HKSE", "SHSE", "SSE", "SZSE", "TPE", "TWSE", "ROCO",
    "XKRX", "NSE", "BOM", "SGX", "XKLS", "ISX", "BKK", "PHS",
    "ASX", "NZSE", "JSE",
}

_EST = timezone(timedelta(hours=-5))
_ACTIVE_JOB: ContextVar[str | None] = ContextVar("api_usage_job", default=None)


def _current_month_est() -> str:
    return datetime.now(_EST).strftime("%Y-%m")


def _region_for_exchange(exchange: str) -> str:
    e = exchange.upper()
    if e in US_EXCHANGES:
        return "usa"
    if e in ASIA_EXCHANGES:
        return "asia"
    return "europe"


def classify_outcome(status: int | None, *, has_data: bool = True) -> str:
    """Stable, low-cardinality outcome bucket for one completed HTTP attempt."""
    if status in (429, 999):
        return "rate_limited"
    if status == 404:
        return "not_found"
    if status in (401, 403):
        return "forbidden"
    if status is None:
        return "transport_error" if not has_data else "success"
    if 200 <= status < 300:
        return "success" if has_data else "empty"
    return "http_error"


@contextmanager
def api_usage_job(job: str) -> Iterator[None]:
    """Attribute nested requests to a workflow; safe to nest and exception-proof."""
    token = _ACTIVE_JOB.set(job.strip().lower() or "unattributed")
    try:
        yield
    finally:
        _ACTIVE_JOB.reset(token)


def current_api_job(default: str = "unattributed") -> str:
    return _ACTIVE_JOB.get() or default


def track_api_call(
    supabase: Client,
    exchange: str = "",
    count: int = 1,
    *,
    source: str = "gurufocus",
    job: str | None = None,
    outcome: str = "unknown",
    region: str | None = None,
) -> None:
    """Atomically add attributed request count; telemetry must never fail real work."""
    if count <= 0:
        return
    month = _current_month_est()
    source = (source or "unknown").strip().lower()
    region = region or (_region_for_exchange(exchange) if source == "gurufocus" else "global")
    job = (job or current_api_job()).strip().lower()
    outcome = (outcome or "unknown").strip().lower()
    dimensions = {
        "month": month, "region": region, "source": source,
        "job": job, "outcome": outcome,
    }

    try:
        supabase.rpc("increment_api_usage_attributed", {
            "p_month": month,
            "p_region": region,
            "p_source": source,
            "p_job": job,
            "p_outcome": outcome,
            "p_count": count,
        }).execute()
    except Exception as exc:
        logger.warning("attributed API usage RPC failed (%s), trying read-then-write", exc)
        try:
            query = supabase.table("api_usage").select("id, request_count")
            for key, value in dimensions.items():
                query = query.eq(key, value)
            row = query.maybe_single().execute()
            if row.data:
                supabase.table("api_usage").update(
                    {"request_count": row.data["request_count"] + count}
                ).eq("id", row.data["id"]).execute()
            else:
                supabase.table("api_usage").insert(
                    {**dimensions, "request_count": count}
                ).execute()
        except Exception as fallback_exc:
            logger.warning("Failed to track API usage: %s", fallback_exc)


def get_usage(supabase: Client) -> dict:
    """Current totals and source/job/outcome breakdown.

    The legacy ``usa/europe/asia`` keys stay GuruFocus-only because callers subtract them from that
    provider's regional caps.
    """
    month = _current_month_est()
    result: dict = {
        "usa": 0, "europe": 0, "asia": 0, "month": month,
        "total": 0, "by_source": {}, "breakdown": [],
    }
    try:
        rows = (supabase.table("api_usage")
                .select("region,source,job,outcome,request_count")
                .eq("month", month).execute().data or [])
        breakdown = []
        for row in rows:
            count = int(row.get("request_count") or 0)
            source = str(row.get("source") or "gurufocus")
            region = str(row.get("region") or "unknown")
            job = str(row.get("job") or "legacy")
            outcome = str(row.get("outcome") or "unknown")
            result["total"] += count
            result["by_source"][source] = result["by_source"].get(source, 0) + count
            if source == "gurufocus" and region in ("usa", "europe", "asia"):
                result[region] += count
            breakdown.append({"source": source, "region": region, "job": job,
                              "outcome": outcome, "request_count": count})
        result["breakdown"] = sorted(
            breakdown, key=lambda row: (-row["request_count"], row["source"], row["job"])
        )
        return result
    except Exception:
        return result


def remaining_budget(supabase: Client) -> dict:
    used = get_usage(supabase)
    return {
        region: max(0, MONTHLY_API_LIMIT - int(used.get(region, 0) or 0))
        for region in ("usa", "europe", "asia")
    }
