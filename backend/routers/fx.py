"""FX rate endpoints — ECB-fed daily rates, with DB cache.

Endpoints:
    GET  /api/fx/coverage           ACWI currencies vs ECB availability matrix
    GET  /api/fx/latest             latest daily rate per currency
    GET  /api/fx/history/{currency} daily history (paginated)
    POST /api/fx/sync               persist ECB + pegged + TWD into fx_rate

Each read first tries the local `fx_rate` table for speed; falls back to
live ECB only when the cache has no data for that currency (so a fresh
install works without a manual sync step).
"""

from __future__ import annotations

import asyncio
from datetime import date as _date

from fastapi import APIRouter

from deps import supabase
from fx_rates import (
    ECB_CURRENCIES,
    _USD_PEGS,
    fetch_history_from_db,
    fetch_latest_from_db,
    get_coverage_info,
)
from momentum.data import sync_fx_rates_to_db

router = APIRouter(tags=["fx"])


@router.get("/api/fx/coverage")
async def fx_coverage():
    """Compare ACWI currencies against ECB FX rate availability."""
    return await asyncio.to_thread(get_coverage_info)


@router.get("/api/fx/latest")
async def fx_latest():
    """Latest daily rates per currency.

    Reads from `fx_rate` (fast); falls back to live ECB when the table is
    empty so the page renders on a fresh install — user can then click
    "Sync from ECB" to populate the table and get the fast path next time.
    """
    rates = await asyncio.to_thread(fetch_latest_from_db, supabase)
    source = "db"
    if not rates:
        from fx_rates import fetch_all_latest
        rates = await asyncio.to_thread(fetch_all_latest)
        source = "ecb_live"
    return {"rates": rates, "count": len(rates), "source": source}


@router.get("/api/fx/history/{currency}")
async def fx_history(currency: str, start_date: str | None = None):
    """Daily historical FX rates for one currency. Same DB-first /
    ECB-fallback pattern as /latest."""
    currency = currency.upper()
    rates = await asyncio.to_thread(fetch_history_from_db, supabase, currency, start_date)
    source = "db"
    if not rates:
        from fx_rates import fetch_history
        rates = await asyncio.to_thread(fetch_history, currency, start_date)
        source = "ecb_live"
    return {"currency": currency, "rates": rates, "count": len(rates), "source": source}


@router.post("/api/fx/sync")
async def fx_sync(start_date: str | None = None):
    """Sync ECB + pegged + TWD rates into the local fx_rate table.

    Backs the FX page's manual Sync button. Idempotent — each currency
    only fetches the gap between its latest stored date and today, so
    repeated calls are cheap."""
    start = _date.fromisoformat(start_date) if start_date else _date(2000, 1, 1)
    end = _date.today()
    currencies = ECB_CURRENCIES + list(_USD_PEGS.keys()) + ["TWD"]
    status = await asyncio.to_thread(
        sync_fx_rates_to_db, supabase, currencies, start, end,
    )
    synced = [c for c, s in status.items() if s.get("status") == "synced"]
    cached = [c for c, s in status.items() if s.get("status") == "cached"]
    failed = [c for c, s in status.items() if s.get("status") == "error"]
    return {
        "synced": sorted(synced),
        "cached": sorted(cached),
        "failed": sorted(failed),
        "details": status,
    }
