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
from datetime import date
from decimal import Decimal
from typing import Any
from typing import Literal

from fastapi import APIRouter, Path, Query
from pydantic import BaseModel, Field

from deps import supabase

router = APIRouter(tags=["leonteq"])


class PortfolioRefreshRequest(BaseModel):
    operation: Literal['list', 'composition', 'airs', 'monthly', 'prices']
    name: str | None = Field(default=None, min_length=1, max_length=300)
    portfolio_id: int | None = Field(default=None, ge=1)
    datum: date | None = None
    lang: Literal['en', 'nl'] = 'en'
    source: Literal['Leonteq', 'AIRS'] | None = None
    isin: str | None = Field(default=None, max_length=12)
    holding_name: str | None = Field(default=None, min_length=1, max_length=500)


@router.post('/api/leonteq/refresh/job')
async def start_portfolio_refresh(body: PortfolioRefreshRequest):
    import jobs
    from fastapi import HTTPException
    from leonteq.refresh_jobs import run

    if (body.operation in ('composition', 'monthly') and not body.name
            or body.operation in ('airs', 'monthly') and body.portfolio_id is None):
        raise HTTPException(422, 'The portfolio identity is required for this refresh')
    if body.source and (body.operation != 'prices' or not body.name or not body.portfolio_id or not body.holding_name):
        raise HTTPException(422, 'The connection and holding identity are required for a row recheck')
    label = ('All linked portfolio prices' if body.operation == 'prices' else f'AIRS #{body.portfolio_id}' if body.operation == 'airs'
             else body.name or 'Leonteq portfolios')
    identity = f'{body.operation}:{body.portfolio_id or ""}:{body.datum or "latest"}'
    if body.source:
        import hashlib
        identity += ':' + hashlib.sha256(f'{body.name}:{body.source}:{body.isin}:{body.holding_name}'.encode()).hexdigest()[:20]
    prefix = 'airs.leonteq.refresh' if body.operation in ('airs', 'monthly', 'prices') else 'leonteq.refresh'
    job, reused = jobs.start(f'{prefix}.{identity}', label,
        lambda ctx: run(ctx, body.operation, name=body.name, portfolio_id=body.portfolio_id,
                          datum=body.datum.isoformat() if body.datum else None, lang=body.lang,
                          source=body.source, isin=body.isin, holding_name=body.holding_name))
    return {'job_id': job.id, 'already_running': reused}


@router.get('/api/leonteq/refresh/jobs/{job_id}/result')
async def portfolio_refresh_result(job_id: str):
    import jobs
    from fastapi import HTTPException
    from leonteq.refresh_jobs import result_for

    job = jobs.get(job_id)
    if not job or not job.kind.startswith(('leonteq.refresh.', 'airs.leonteq.refresh.')):
        raise HTTPException(404, 'Refresh result is no longer available. Reload the saved data.')
    result = result_for(job_id)
    if result is None:
        raise HTTPException(409, job.summary or 'The refresh has not produced a result yet.')
    return result


@router.get('/api/leonteq/airs/model-portfolios/compositions')
async def saved_airs_compositions():
    from leonteq.airs_models import saved_compositions

    return await asyncio.to_thread(saved_compositions)


class HoldingConnectionRequest(BaseModel):
    leonteq_name: str = Field(min_length=1, max_length=300)
    airs_portfolio_id: int = Field(ge=1)
    leonteq_key: str = Field(min_length=1, max_length=512)
    airs_key: str | None = Field(default=None, min_length=1, max_length=512)


@router.get('/api/leonteq/portfolio-links/holdings')
async def read_holding_connections(leonteq_name: str = Query(min_length=1, max_length=300), airs_portfolio_id: int = Query(ge=1)):
    from leonteq.holding_connections import read

    return await asyncio.to_thread(read, leonteq_name, airs_portfolio_id)


@router.put('/api/leonteq/portfolio-links/holdings')
async def save_holding_connection(body: HoldingConnectionRequest):
    from leonteq.holding_connections import save

    return await asyncio.to_thread(save, body.leonteq_name, body.airs_portfolio_id, body.leonteq_key, body.airs_key)


class PortfolioLinkRequest(BaseModel):
    leonteq_name: str = Field(min_length=1, max_length=300)
    airs_portfolio_id: int = Field(ge=1)
    kind: Literal['fixed', 'dynamic'] = 'fixed'


@router.get('/api/leonteq/portfolio-links')
async def portfolio_link_options(kind: Literal['fixed', 'dynamic'] = 'fixed'):
    from leonteq.portfolio_links import options

    return await asyncio.to_thread(options, kind)


@router.put('/api/leonteq/portfolio-links')
async def connect_portfolios(body: PortfolioLinkRequest):
    from leonteq.portfolio_links import connect

    return await asyncio.to_thread(connect, body.leonteq_name, body.airs_portfolio_id, body.kind)


@router.delete('/api/leonteq/portfolio-links')
async def disconnect_portfolios(body: PortfolioLinkRequest):
    from leonteq.portfolio_links import disconnect

    return await asyncio.to_thread(disconnect, body.leonteq_name, body.airs_portfolio_id)


class PercentageEditRequest(BaseModel):
    datum: date
    expected_percentage: Decimal = Field(allow_inf_nan=False)
    percentage: Decimal = Field(ge=0, le=100, max_digits=12, decimal_places=6, allow_inf_nan=False)


@router.patch('/api/leonteq/airs/model-portfolios/{portfolio_id}/positions/{row_id}/percentage')
async def edit_airs_percentage(body: PercentageEditRequest, portfolio_id: int = Path(ge=1), row_id: int = Path(ge=1)):
    from leonteq.airs_edit import update_percentage

    return await asyncio.to_thread(update_percentage, portfolio_id, row_id, body.datum.isoformat(),
                                   body.expected_percentage, body.percentage)


@router.get('/api/leonteq/airs/model-portfolios/{portfolio_id}/composition')
async def airs_model_composition(portfolio_id: int = Path(ge=1), datum: date | None = None, refresh: bool = False, saved_only: bool = False):
    from leonteq.airs_models import composition

    return await asyncio.to_thread(composition, portfolio_id, datum.isoformat() if datum else None, refresh, saved_only)


@router.post('/api/leonteq/airs/model-portfolios/refresh/job')
async def refresh_airs_model_list():
    import jobs as job_registry
    from leonteq.airs_models import refresh_models

    job, reused = job_registry.start('airs.models.list', 'AIRS model portfolios', refresh_models)
    return {'job_id': job.id, 'label': 'AIRS model portfolios', 'already_running': reused}


class PortfolioLoadRequest(BaseModel):
    refresh: bool = False


@router.get('/api/leonteq/portfolios/snapshot')
async def saved_portfolio_list():
    from leonteq.portfolio_cache import saved_portfolios

    return await asyncio.to_thread(saved_portfolios)


@router.post('/api/leonteq/portfolios/login')
async def check_portfolio_login(request: PortfolioLoadRequest | None = None):
    """Return the saved AMC list; only a cache miss or explicit refresh logs in."""
    from leonteq.portfolio_cache import portfolios

    return await asyncio.to_thread(portfolios, bool(request and request.refresh))


class PortfolioCompositionRequest(BaseModel):
    name: str = Field(min_length=1, max_length=300)
    refresh: bool = False
    saved_only: bool = False


class MonthlyPerformanceRequest(PortfolioCompositionRequest):
    airs_portfolio_id: int = Field(ge=1)


@router.post('/api/leonteq/portfolios/monthly-performance')
async def monthly_performance(request: MonthlyPerformanceRequest):
    from leonteq.monthly_performance import comparison
    return await asyncio.to_thread(comparison, request.name, request.airs_portfolio_id, request.refresh)


@router.post('/api/leonteq/portfolios/composition')
async def portfolio_composition(request: PortfolioCompositionRequest):
    """Read the selected AMC index's current positions using backend credentials."""
    from leonteq.composition_cache import composition

    return await asyncio.to_thread(composition, request.name, request.refresh, request.saved_only)

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
