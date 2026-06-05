"""Data-freshness primitives for the admin health/freshness endpoints.

Extracted from `routers.admin`. `_max_target_date` reads the latest
`metric_data.target_date` for a metric; `_trading_day_age` turns that into a
coarse Mon–Fri age. `_now_utc` is the shared timestamp source.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from deps import supabase


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _max_target_date(metric_code: str) -> date | None:
    """Latest `target_date` in `metric_data` for one metric_code."""
    try:
        resp = (
            supabase.table("metric_data")
            .select("target_date")
            .eq("metric_code", metric_code)
            .order("target_date", desc=True)
            .limit(1)
            .execute()
        )
    except Exception:
        return None
    if not resp.data:
        return None
    raw = resp.data[0].get("target_date")
    try:
        return date.fromisoformat(str(raw)[:10]) if raw else None
    except ValueError:
        return None


def _trading_day_age(latest: date | None) -> int | None:
    """Approximate age of `latest` in trading days (Mon-Fri only).
    Returns None when latest is missing. Used as a coarse signal — a
    Sunday call where `latest` is Friday should read 0, not 2."""
    if latest is None:
        return None
    today = date.today()
    if latest >= today:
        return 0
    days = 0
    cursor = today
    while cursor > latest:
        cursor = cursor - timedelta(days=1)
        if cursor.weekday() < 5:  # 0..4 = Mon..Fri
            days += 1
    return days
