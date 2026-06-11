"""FX rate endpoints — ECB-fed daily rates, with DB cache.

Endpoints:
    GET  /api/fx/coverage                       ACWI currencies vs ECB availability matrix
    GET  /api/fx/latest                         latest daily rate per currency
    GET  /api/fx/history/{currency}             daily history from cache (instant)
    POST /api/fx/history/{currency}/refresh     fetch gap to today, upsert, return full series
    POST /api/fx/sync                           persist ECB + pegged + TWD into fx_rate

History uses stale-while-revalidate: the GET returns whatever is cached so
the chart renders instantly; the POST is fired in the background by the
frontend when the cache is stale or empty, and replaces the chart data
once new rows roll in.
"""

from __future__ import annotations

import asyncio
from datetime import date as _date

from fastapi import APIRouter, HTTPException

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

# Currencies we have no upstream source for (not ECB, not USD-pegged, not on
# Yahoo) — skip refresh attempts so we don't hammer ECB with 4xx misses.
_UNFETCHABLE = {"CLP", "COP", "EGP", "RUB"}
_FETCHABLE = set(ECB_CURRENCIES) | set(_USD_PEGS.keys()) | {"TWD"}


@router.get("/api/fx/coverage")
async def fx_coverage():
    """Compare ACWI currencies against ECB FX rate availability."""
    return await asyncio.to_thread(get_coverage_info)


@router.get("/api/fx/latest")
async def fx_latest():
    """Latest daily rates per currency.

    Reads from `fx_rate` (fast); falls back to live ECB so the page never
    shows a blank rate for a currency we can actually source:
      - empty table → fetch everything live (fresh install).
      - PARTIAL table → fetch live only for the fetchable currencies that
        are missing from the DB and merge them in. A partially-populated
        table is the normal case once a backtest's forward-only FX sync has
        run for some currencies but not others; without this, those covered
        currencies render blank even though their history loads live fine.
    A one-time "Sync from ECB" persists everything for the fast path.
    """
    from fx_rates import fetch_all_latest

    rates = await asyncio.to_thread(fetch_latest_from_db, supabase)
    if not rates:
        rates = await asyncio.to_thread(fetch_all_latest)
        return {"rates": rates, "count": len(rates), "source": "ecb_live"}

    source = "db"
    have = {r["currency"] for r in rates}
    missing = _FETCHABLE - have
    if missing:
        try:
            live = await asyncio.to_thread(fetch_all_latest)
            for r in live:
                if r["currency"] in missing:
                    rates.append(r)
            rates.sort(key=lambda r: r["currency"])
            source = "db+ecb_live"
        except Exception:
            # ECB/Yahoo hiccup — keep the DB rates; the still-missing
            # currencies stay blank rather than failing the whole page.
            pass
    return {"rates": rates, "count": len(rates), "source": source}


@router.get("/api/fx/history/{currency}")
async def fx_history(currency: str, start_date: str | None = None):
    """Daily historical FX rates for one currency from the local cache.

    Returns DB-only so the chart can render instantly. The frontend fires
    POST /refresh in the background when `max_date < today` to fill the
    gap; this endpoint never hits ECB itself.
    """
    currency = currency.upper()
    rates = await asyncio.to_thread(fetch_history_from_db, supabase, currency, start_date)
    max_date = rates[-1]["date"] if rates else None
    is_fetchable = currency in _FETCHABLE
    today_iso = _date.today().isoformat()
    is_stale = (not max_date) or (str(max_date) < today_iso)
    return {
        "currency": currency,
        "rates": rates,
        "count": len(rates),
        "max_date": max_date,
        "is_fetchable": is_fetchable,
        "is_stale": is_stale,
        "source": "db",
    }


@router.post("/api/fx/history/{currency}/refresh")
async def fx_history_refresh(currency: str, start_date: str | None = None):
    """Stale-while-revalidate refresh for one currency.

    Fetches the gap from `fx_rate.max(rate_date) + 1 day` to today, upserts
    new rows, and returns the full history so the UI can swap the chart
    once new data rolls in. The frontend calls this in the background after
    the GET so the user never waits on ECB.
    """
    currency = currency.upper()
    if currency in _UNFETCHABLE or currency not in _FETCHABLE:
        raise HTTPException(
            status_code=400,
            detail=f"{currency} has no upstream source (ECB / pegs / Yahoo do not cover it)",
        )

    start = _date.fromisoformat(start_date) if start_date else _date(2000, 1, 1)
    end = _date.today()
    status = await asyncio.to_thread(
        sync_fx_rates_to_db, supabase, [currency], start, end,
    )
    rates = await asyncio.to_thread(fetch_history_from_db, supabase, currency, start_date)
    max_date = rates[-1]["date"] if rates else None
    return {
        "currency": currency,
        "rates": rates,
        "count": len(rates),
        "max_date": max_date,
        "sync": status.get(currency, {}),
        "source": "db",
    }


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
