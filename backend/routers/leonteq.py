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

_FROZEN_YAHOO_LABEL = "LEONTEQ (as of 2026-06-17)"
# Human-confirmed exceptions to the automated Yahoo/OpenFIGI name comparison.
# These apply only to the Leonteq mapping audit; the underlying resolver verdict
# remains available to the asset-pipeline screen for operational diagnostics.
_APPROVED_LEONTEQ_IDENTITIES = frozenset({
    "DK0010244508",  # A.P. Møller – Mærsk A/S B -> MAERSK-B.CO
    "FR0010340141",  # Aéroports de Paris SA -> ADP.PA
    "CNE1000003G1",  # ICBC H share -> 1398.HK
    "CH0102484968",  # Julius Baer Gruppe AG -> BAER.SW
    "FR001400AJ45",  # Michelin -> ML.PA
    "DE0008430026",  # Munich Re -> MUV2.DE
    "US9297401088",  # Westinghouse Air Brake Technologies -> WAB
    "US9897011071",  # Zions Bancorp -> ZION
})


def _mapping_status(row: dict | None) -> str:
    """A mapping is only confirmed when the resolver's identity check agrees.

    Keep a usable-but-unverified symbol visible: it is useful for diagnosing a
    gap, but must not look as trustworthy as a verified ISIN-to-symbol match.
    """
    if not row or row.get("status") != "ok" or not row.get("analysis_symbol"):
        return "unmapped"
    if row.get("identity_status") == "verified" or row.get("isin") in _APPROVED_LEONTEQ_IDENTITIES:
        return "verified"
    return "review"


def _fetch_frozen_yahoo_mappings() -> dict:
    """Return the Yahoo bridge for the exact Leonteq snapshot used in backtests."""
    from ingest.gurufocus_url import gurufocus_url  # noqa: PLC0415

    universe = (
        supabase.table("universe").select("universe_id,label")
        .eq("label", _FROZEN_YAHOO_LABEL).limit(1).execute().data or []
    )
    if not universe:
        return {"label": _FROZEN_YAHOO_LABEL, "target_month": None, "count": 0, "rows": []}

    universe_id = universe[0]["universe_id"]
    latest = (
        supabase.table("universe_membership").select("target_month")
        .eq("universe_id", universe_id).order("target_month", desc=True).limit(1).execute().data or []
    )
    if not latest:
        return {"label": _FROZEN_YAHOO_LABEL, "target_month": None, "count": 0, "rows": []}
    target_month = latest[0]["target_month"]
    members = (
        supabase.table("universe_membership").select("company_id,universe_ticker")
        .eq("universe_id", universe_id).eq("target_month", target_month).execute().data or []
    )
    company_ids = sorted({m["company_id"] for m in members if m.get("company_id") is not None})

    companies: dict[int, dict] = {}
    grid: dict[int, dict] = {}
    for offset in range(0, len(company_ids), 500):
        ids = company_ids[offset:offset + 500]
        for company in (
            supabase.table("company").select(
                "company_id,company_name,isin,gurufocus_ticker,"
                "gurufocus_exchange:gurufocus_exchange(exchange_code)"
            )
            .in_("company_id", ids).execute().data or []
        ):
            companies[company["company_id"]] = company
        candidates = (
            supabase.table("asset_grid")
            .select("company_id,isin,analysis_symbol,yahoo_symbol,status,identity_status,bars,is_default")
            .in_("company_id", ids).execute().data or []
        )
        for candidate in candidates:
            cid = candidate.get("company_id")
            if cid is None:
                continue
            previous = grid.get(cid)
            # Prefer a healthy default execution, then a healthy execution.
            rank = (candidate.get("status") == "ok", bool(candidate.get("is_default")))
            old_rank = ((previous or {}).get("status") == "ok", bool((previous or {}).get("is_default")))
            if previous is None or rank > old_rank:
                grid[cid] = candidate

    ticker_by_id = {m["company_id"]: m.get("universe_ticker") for m in members}
    rows = []
    for cid in company_ids:
        company, mapped = companies.get(cid, {}), grid.get(cid)
        exchange = (company.get("gurufocus_exchange") or {}).get("exchange_code")
        rows.append({
            "company_id": cid,
            "company_name": company.get("company_name") or "Unknown company",
            "isin": company.get("isin") or (mapped or {}).get("isin"),
            "universe_ticker": ticker_by_id.get(cid),
            "gurufocus_url": gurufocus_url(company.get("gurufocus_ticker"), exchange),
            "yahoo_ticker": (mapped or {}).get("analysis_symbol"),
            "execution_ticker": (mapped or {}).get("yahoo_symbol"),
            "identity_status": (mapped or {}).get("identity_status"),
            "bars": (mapped or {}).get("bars"),
            "mapping_status": _mapping_status(mapped),
        })
    rows.sort(key=lambda row: (row["company_name"].casefold(), row["company_id"]))
    return {"label": _FROZEN_YAHOO_LABEL, "target_month": target_month, "count": len(rows), "rows": rows}


def enqueue_frozen_yahoo_mappings() -> dict:
    """Put unresolved frozen-Leonteq ISINs through the production resolver.

    ``enqueue(..., skip_existing=True)`` makes this safe on every deploy: the
    1,478 reviewed Yahoo executions are skipped, while a new production
    database (or a newly added frozen constituent) joins the single throttled
    resolver queue.  The queue, rather than startup, performs Yahoo calls.
    """
    universe = (
        supabase.table("universe").select("universe_id")
        .eq("label", _FROZEN_YAHOO_LABEL).limit(1).execute().data or []
    )
    if not universe:
        return {"queued": 0, "skipped_existing": 0, "input": 0}
    universe_id = universe[0]["universe_id"]
    latest = (
        supabase.table("universe_membership").select("target_month")
        .eq("universe_id", universe_id).order("target_month", desc=True).limit(1).execute().data or []
    )
    if not latest:
        return {"queued": 0, "skipped_existing": 0, "input": 0}
    members = (
        supabase.table("universe_membership").select("company_id")
        .eq("universe_id", universe_id).eq("target_month", latest[0]["target_month"]).execute().data or []
    )
    company_ids = [m["company_id"] for m in members if m.get("company_id") is not None]
    isins: list[str] = []
    for offset in range(0, len(company_ids), 500):
        rows = (
            supabase.table("company").select("isin").in_("company_id", company_ids[offset:offset + 500])
            .not_.is_("isin", "null").execute().data or []
        )
        isins.extend(row["isin"] for row in rows if row.get("isin"))
    from asset_pipeline.queue import enqueue  # noqa: PLC0415
    return enqueue(isins, skip_existing=True)


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


@router.get("/api/leonteq/frozen-yahoo-mappings")
async def frozen_yahoo_mappings():
    """Yahoo tickers for the immutable Leonteq snapshot selected in backtests.

    ``analysis_symbol`` is the yfinance instrument whose stored price series is
    used by the backtester.  Rows with a non-verified identity are deliberately
    returned as ``review`` rather than silently treated as correct.
    """
    return await asyncio.to_thread(_fetch_frozen_yahoo_mappings)


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
