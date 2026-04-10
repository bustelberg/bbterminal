"""
Track GuruFocus API usage per month and region (usa / europe).
Stores counts in the `api_usage` Supabase table.

Table schema:
  id          serial primary key
  month       text not null        -- e.g. "2026-04"
  region      text not null        -- "usa" or "europe"
  request_count integer not null default 0
  unique(month, region)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta

from supabase import Client

logger = logging.getLogger(__name__)

US_EXCHANGES = {"NYSE", "NASDAQ", "AMEX"}

# GuruFocus resets usage at midnight EST (UTC-5)
_EST = timezone(timedelta(hours=-5))


def _current_month_est() -> str:
    """Return current month as YYYY-MM in EST timezone."""
    return datetime.now(_EST).strftime("%Y-%m")


def _region_for_exchange(exchange: str) -> str:
    return "usa" if exchange.upper() in US_EXCHANGES else "europe"


def track_api_call(supabase: Client, exchange: str, count: int = 1) -> None:
    """Increment the API usage counter for the current month and region."""
    month = _current_month_est()
    region = _region_for_exchange(exchange)

    try:
        # Use Postgres RPC for atomic increment, fall back to read-then-write
        supabase.rpc("increment_api_usage", {
            "p_month": month,
            "p_region": region,
            "p_count": count,
        }).execute()
    except Exception as e:
        logger.warning(f"RPC increment_api_usage failed ({e}), trying read-then-write")
        try:
            row = (
                supabase.table("api_usage")
                .select("id, request_count")
                .eq("month", month)
                .eq("region", region)
                .maybe_single()
                .execute()
            )
            if row.data:
                supabase.table("api_usage").update(
                    {"request_count": row.data["request_count"] + count}
                ).eq("id", row.data["id"]).execute()
            else:
                supabase.table("api_usage").insert(
                    {"month": month, "region": region, "request_count": count}
                ).execute()
        except Exception as e2:
            logger.warning(f"Failed to track API usage: {e2}")


def get_usage(supabase: Client) -> dict:
    """Return current month's usage: {usa: N, europe: N}."""
    month = _current_month_est()
    try:
        resp = (
            supabase.table("api_usage")
            .select("region, request_count")
            .eq("month", month)
            .execute()
        )
        result = {"usa": 0, "europe": 0, "month": month}
        for row in resp.data or []:
            if row["region"] in result:
                result[row["region"]] = row["request_count"]
        return result
    except Exception:
        return {"usa": 0, "europe": 0, "month": month}
