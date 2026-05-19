"""HTTP endpoints backing the /leonteq page.

Refresh is intentionally NOT here — it goes through the existing
template SSE flow at `POST /api/universe-templates/LEONTEQ/refresh`,
which drives `LeonteqTemplate.refresh()` (scrape + reconcile +
persist). The frontend hits that endpoint directly for live progress.

What lives here:
  GET /api/leonteq/equities — flat list of every row in the latest
                              scrape (with sector, industry, GuruFocus
                              link, optional company_id).
  GET /api/leonteq/overview  — pre-aggregated sector → industries →
                              companies tree the /leonteq UI consumes
                              directly. Saves the frontend from doing
                              the grouping client-side on every render.
"""
from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter

from deps import supabase

router = APIRouter(tags=["leonteq"])


def _fetch_all() -> list[dict]:
    """Pull every row in `leonteq_equity`. Paginated against PostgREST's
    default 1000-row cap."""
    out: list[dict] = []
    offset = 0
    page = 1000
    while True:
        resp = (
            supabase.table("leonteq_equity")
            .select(
                "id, name, ticker, isin, sector, industry, "
                "gurufocus_url, company_id, scraped_at"
            )
            .order("sector")
            .order("industry")
            .order("name")
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        out.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return out


@router.get("/api/leonteq/equities")
async def list_equities():
    """Every equity in the latest Leonteq scrape. Newest scrape only —
    the table is replace-all on every refresh."""
    rows = await asyncio.to_thread(_fetch_all)
    return {
        "count": len(rows),
        "scraped_at": rows[0]["scraped_at"] if rows else None,
        "equities": rows,
    }


@router.get("/api/leonteq/overview")
async def overview():
    """Sector → industries → companies tree, plus header counters.

    Response shape:
      {
        "total_equities": int,
        "unique_sectors": int,
        "unique_industries": int,
        "scraped_at": str | null,
        "sectors": [
          {
            "name": str,
            "company_count": int,
            "industries": [
              {
                "name": str,
                "company_count": int,
                "companies": [
                  { "name", "ticker", "isin", "gurufocus_url", "company_id" }
                ]
              }
            ]
          }
        ]
      }

    Industries are guaranteed to map to ONE sector each by construction
    (an industry that appears under multiple sectors in the scrape gets
    bucketed by majority-sector — this should never happen with a clean
    GICS-style source but we defend against it)."""
    rows = await asyncio.to_thread(_fetch_all)

    def _key(s: str | None) -> str:
        s = (s or "").strip()
        return s if s else "—"

    # First pass: count industry → sector counts, pick a single owning
    # sector for each industry (the one it appears under most often).
    ind_sector_counts: dict[str, dict[str, int]] = {}
    for r in rows:
        sec = _key(r.get("sector"))
        ind = _key(r.get("industry"))
        ind_sector_counts.setdefault(ind, {})
        ind_sector_counts[ind][sec] = ind_sector_counts[ind].get(sec, 0) + 1
    industry_to_sector: dict[str, str] = {}
    for ind, sectors in ind_sector_counts.items():
        industry_to_sector[ind] = max(sectors.items(), key=lambda kv: kv[1])[0]

    # Group equities by (sector, industry), using the canonical
    # industry→sector mapping so an industry can't appear under two
    # sectors.
    grouped: dict[str, dict[str, list[dict]]] = {}
    for r in rows:
        ind = _key(r.get("industry"))
        sec = industry_to_sector.get(ind, _key(r.get("sector")))
        grouped.setdefault(sec, {}).setdefault(ind, []).append({
            "name": r.get("name"),
            "ticker": r.get("ticker"),
            "isin": r.get("isin"),
            "gurufocus_url": r.get("gurufocus_url"),
            "company_id": r.get("company_id"),
        })

    sectors_out: list[dict] = []
    for sec_name in sorted(grouped.keys()):
        inds = grouped[sec_name]
        industries: list[dict[str, Any]] = []
        sec_count = 0
        for ind_name in sorted(inds.keys()):
            companies = sorted(inds[ind_name], key=lambda c: (c.get("name") or "").lower())
            industries.append({
                "name": ind_name,
                "company_count": len(companies),
                "companies": companies,
            })
            sec_count += len(companies)
        sectors_out.append({
            "name": sec_name,
            "company_count": sec_count,
            "industries": industries,
        })

    return {
        "total_equities": len(rows),
        "unique_sectors": len(grouped),
        "unique_industries": sum(len(inds) for inds in grouped.values()),
        "scraped_at": rows[0]["scraped_at"] if rows else None,
        "sectors": sectors_out,
    }
