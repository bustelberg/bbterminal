"""HTTP endpoints backing the /timezone page.

We trade at the *previous day's close*, but our universe spans ~50 exchanges
across every timezone. This surfaces, per exchange present in a chosen
universe, the regular session hours converted to Amsterdam wall-clock (with a
summer/CEST and winter/CET variant), so we can see by when each market's close
is final in our own day.

  GET /api/timezone/exchanges?universe=<label>
      → one row per exchange that has companies (in the given universe, or
        across the whole DB when `universe` is omitted), enriched with trading
        hours from `index_universe.exchange_hours`. Exchanges we have no hours
        data for are still returned (timezone null) so gaps are visible.

Admin-only (not in the non-admin API surface — see routers/_auth_middleware).
"""
from __future__ import annotations

import asyncio
from collections import Counter

from fastapi import APIRouter, HTTPException

from deps import supabase, fetch_in_chunks
from index_universe.exchange_hours import exchange_amsterdam_hours
from routers.index_universe._helpers import fetch_all_membership

router = APIRouter(tags=["timezone"])


def _exchange_metadata() -> tuple[dict[int, dict], dict[str, dict]]:
    """Load every exchange's display metadata, indexed by both id and code."""
    resp = (
        supabase.table("gurufocus_exchange")
        .select("exchange_id, exchange_code, exchange_name, currency_code, country:country(country_name)")
        .execute()
    )
    by_id: dict[int, dict] = {}
    by_code: dict[str, dict] = {}
    for e in resp.data or []:
        country = (e.get("country") or {}).get("country_name")
        info = {
            "exchange_code": e["exchange_code"],
            "exchange_name": e.get("exchange_name"),
            "currency": e.get("currency_code"),
            "country": country,
        }
        by_id[e["exchange_id"]] = info
        by_code[e["exchange_code"]] = info
    return by_id, by_code


def _count_by_exchange(universe: str, meta_by_id: dict[int, dict]) -> Counter:
    """Companies per exchange_code. Scoped to `universe` (by label) when given,
    else across the whole `company` table. Active rows only — delisted /
    out-of-scope listings are excluded so the picture matches what we'd trade."""
    counts: Counter = Counter()

    def _tally(company_rows: list[dict]) -> None:
        for c in company_rows:
            if c.get("delisted_at") or c.get("out_of_scope_at"):
                continue
            info = meta_by_id.get(c.get("exchange_id"))
            if info:
                counts[info["exchange_code"]] += 1

    if universe:
        u = supabase.table("universe").select("universe_id").eq("label", universe).limit(1).execute()
        if not u.data:
            raise HTTPException(status_code=404, detail=f"Universe '{universe}' not found")
        uid = u.data[0]["universe_id"]
        rows = fetch_all_membership(uid, "company_id")
        cids = list({r["company_id"] for r in rows if r["company_id"]})
        company_rows = fetch_in_chunks(
            cids,
            lambda c: supabase.table("company")
            .select("exchange_id, delisted_at, out_of_scope_at")
            .in_("company_id", c)
            .execute(),
        )
        _tally(company_rows)
        return counts

    # No universe → page the whole company table (paginate past db-max-rows).
    offset, page = 0, 1000
    for _attempt in range(20):
        resp = (
            supabase.table("company")
            .select("exchange_id, delisted_at, out_of_scope_at")
            .order("company_id")
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        _tally(batch)
        if len(batch) < page:
            break
        offset += page
    return counts


@router.get("/api/timezone/exchanges")
async def timezone_exchanges(universe: str = ""):
    """Exchanges (with company counts) + their trading hours in Amsterdam time.

    `universe` is a universe *label* (e.g. a frozen snapshot). Omit it to count
    across every company in the DB. Sorted by Amsterdam CEST (summer) open time,
    earliest to latest — so the markets opening first in our day lead — with the
    day-rollover honoured (a previous-day open like New Zealand's sorts ahead of
    a same-day one); exchanges with no hours data sort last."""
    def _run():
        meta_by_id, meta_by_code = _exchange_metadata()
        counts = _count_by_exchange(universe, meta_by_id)

        result = []
        for code, n in counts.items():
            info = meta_by_code.get(code, {"exchange_code": code, "exchange_name": None, "currency": None, "country": None})
            hours = exchange_amsterdam_hours(code)
            result.append({**info, "company_count": n, "hours": hours})

        def _sort_key(row: dict) -> tuple:
            h = row.get("hours")
            if not h:
                return (1, 0, "")  # unknown-hours rows last
            o = h["amsterdam_summer"]["open"]
            # day_offset first (−1 = previous Amsterdam day → earliest), then time.
            return (0, o["day_offset"], o["time"])

        result.sort(key=_sort_key)
        return {"universe": universe or None, "exchanges": result}

    return await asyncio.to_thread(_run)
