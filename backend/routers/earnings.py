"""Earnings/financials refresh + metric reads.

Endpoints:
    POST /api/earnings/{company_id}/refresh/{source}  SSE: refresh one data source
    POST /api/earnings/{company_id}/refresh-all       SSE: refresh financials + analyst + indicators + prices
    GET  /api/earnings/{company_id}/metrics           dashboard metric rows (paginated reads)
    GET  /api/earnings/{company_id}/metric-codes      distinct metric codes (debug)

The dashboard metric list `_DASHBOARD_METRIC_CODES` is the authoritative
set the frontend renders — additions need a matching ingest fetcher in
`ingest/earnings.py`.
"""

from __future__ import annotations

import asyncio
from routers._sse import sse_message as event
import queue as _queue

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from fastapi.responses import StreamingResponse

from deps import IN_CHUNK_SIZE, supabase
from ingest.earnings import fetch_analyst_estimates, fetch_financials, fetch_indicators
from ingest.prices import ensure_prices_for_company

router = APIRouter(tags=["earnings"])


def _get_company_or_404(company_id: int) -> dict:
    resp = (
        supabase.table("company")
        .select("company_id,gurufocus_ticker,exchange_id,company_name,gurufocus_exchange:gurufocus_exchange(exchange_code,is_us)")
        .eq("company_id", company_id)
        .limit(1)
        .execute()
    )
    if not resp.data:
        raise HTTPException(status_code=404, detail="Company not found")
    row = resp.data[0]
    exch_info = row.pop("gurufocus_exchange", None) or {}
    row["gurufocus_exchange"] = exch_info.get("exchange_code")
    row["is_us"] = exch_info.get("is_us", False)
    return row


async def _earnings_refresh_stream(company_id: int, sources: list[str], force: bool):
    """SSE stream wrapping `ingest.earnings.*` for a company. Each ingest
    fetcher accepts an `on_log` callback; we drain the resulting queue
    in-flight so the UI sees logs as they happen rather than at the end."""
    company = _get_company_or_404(company_id)
    ticker = company["gurufocus_ticker"]
    exchange = company["gurufocus_exchange"] or "UNKNOWN"
    name = company.get("company_name") or f"{ticker}.{exchange}"
    region = "usa" if company.get("is_us", False) else "europe"

    yield event("info", f"Refreshing earnings data for {name} ({ticker}.{exchange})")

    for source in sources:
        yield event("info", "")
        yield event("info", f"--- {source.upper()} ---")

        try:
            log_q: _queue.Queue[str | None] = _queue.Queue()

            def on_log(msg: str):
                log_q.put(msg)

            async def drain_queue():
                events: list[str] = []
                while not log_q.empty():
                    try:
                        msg = log_q.get_nowait()
                        if msg is not None:
                            events.append(event("info", f"  {msg}"))
                    except _queue.Empty:
                        break
                return events

            if source == "financials":
                task = asyncio.get_event_loop().run_in_executor(
                    None, lambda: fetch_financials(
                        supabase, company_id, ticker, exchange,
                        force_refresh=force, on_log=on_log,
                    ))
            elif source == "analyst_estimates":
                task = asyncio.get_event_loop().run_in_executor(
                    None, lambda: fetch_analyst_estimates(
                        supabase, company_id, ticker, exchange,
                        force_refresh=force, on_log=on_log,
                    ))
            elif source == "indicators":
                task = asyncio.get_event_loop().run_in_executor(
                    None, lambda: fetch_indicators(
                        supabase, company_id, ticker, exchange,
                        force_refresh=force, on_log=on_log,
                    ))
            elif source == "prices":
                task = asyncio.get_event_loop().run_in_executor(
                    None, lambda: ensure_prices_for_company(
                        supabase, company_id, ticker, exchange,
                        force_refresh=force, on_log=on_log,
                    ))
            else:
                yield event("error", f"Unknown source: {source}")
                continue

            while not task.done():
                await asyncio.sleep(0.15)
                for evt in await drain_queue():
                    yield evt
            for evt in await drain_queue():
                yield evt

            r = task.result()

            if source == "prices":
                if r.error:
                    yield event("error", f"  Error: {r.error}")
                else:
                    yield event("info", f"  Result: {r.rows_loaded} rows loaded, {r.total_prices} total prices")
            else:
                if r.error:
                    yield event("error", f"  Error: {r.error}")
                else:
                    yield event("info", f"  Result: {r.rows_loaded} rows loaded, {r.metrics_found} metrics")

            if r.api_calls > 0:
                yield event("api_calls", f"{r.api_calls} API call(s)", region=region, count=r.api_calls)

            if getattr(r, "is_forbidden", False):
                yield event("warning", f"  {exchange} is an unsubscribed region on GuruFocus — stopping refresh, remaining sources skipped.")
                break

        except Exception as e:
            yield event("error", f"  {source} failed: {e}")

    yield event("info", "")
    yield event("done", "Earnings refresh complete.")


@router.post("/api/earnings/{company_id}/refresh/{source}")
async def refresh_earnings_source(company_id: int, source: str, force: bool = False):
    """Refresh a single earnings data source. SSE stream."""
    valid = {"financials", "analyst_estimates", "indicators", "prices"}
    if source not in valid:
        raise HTTPException(status_code=400, detail=f"source must be one of {valid}")
    return StreamingResponse(
        _earnings_refresh_stream(company_id, [source], force),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/api/earnings/{company_id}/refresh-all")
async def refresh_earnings_all(company_id: int, force: bool = False):
    """Refresh all earnings data sources. SSE stream."""
    return StreamingResponse(
        _earnings_refresh_stream(
            company_id, ["financials", "analyst_estimates", "indicators", "prices"], force
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# Authoritative list of metrics the dashboard renders. Adding one here
# also requires a matching ingest fetcher in ingest/earnings.py.
_DASHBOARD_METRIC_CODES = [
    # Financials — Per Share Data
    "annuals__Per Share Data__Month End Stock Price",
    "annuals__Per Share Data__EPS without NRI",
    "annuals__Per Share Data__Dividends per Share",
    "annuals__Per Share Data__Free Cash Flow per Share",
    "annuals__Per Share Data__Earnings per Share (Diluted)",
    # Financials — Balance Sheet
    "annuals__Balance Sheet__Debt-to-Equity",
    # Financials — Ratios
    "annuals__Ratios__Capex-to-Revenue",
    "annuals__Ratios__Capex-to-Operating-Cash-Flow",
    "annuals__Ratios__ROE %",
    "annuals__Ratios__Gross Margin %",
    "annuals__Ratios__Net Margin %",
    # Financials — Cashflow / Income
    "annuals__Cashflow Statement__Free Cash Flow",
    "annuals__Income Statement__Revenue",
    "annuals__Income Statement__Operating Income",
    "annuals__Income Statement__Interest Expense",
    "annuals__Income Statement__Net Income",
    "annuals__Income Statement__EPS (Diluted)",
    "annuals__Income Statement__Shares Outstanding (Diluted Average)",
    # Financials — Valuation
    "annuals__Valuation Ratios__FCF Yield %",
    "annuals__Valuation Ratios__Dividend Yield %",
    "annuals__Valuation Ratios__PEG Ratio",
    # Financials — Ratios (WACC / returns)
    "annuals__Ratios__WACC %",
    "annuals__Ratios__ROIC %",
    # Financials — Income Statement
    "annuals__Income Statement__Tax Rate %",
    # Financials — Valuation and Quality
    "annuals__Valuation and Quality__Interest Coverage",
    "annuals__Valuation and Quality__Net Cash per Share",
    "annuals__Valuation and Quality__Intrinsic Value: Projected FCF",
    "annuals__Valuation and Quality__Beta",
    "annuals__Valuation and Quality__Piotroski F-Score",
    "annuals__Valuation and Quality__Altman Z-Score",
    "annuals__Valuation and Quality__Shares Buyback Ratio %",
    "annuals__Valuation and Quality__YoY Rev. per Sh. Growth",
    "annuals__Valuation and Quality__5-Year EBITDA Growth Rate (Per Share)",
    "annuals__Valuation and Quality__YoY EPS Growth",
    # Indicators — only forward-looking metrics not already in the financials
    # JSON. ROE/ROIC/Margins/Interest Coverage/PEG/FCF Yield are derived
    # from financials now (see INDICATOR_KEYS in ingest/earnings.py).
    "indicator_q_forward_pe_ratio",
    # Daily close prices
    "close_price",
    # Analyst estimates (annual_* prefix) — fetched separately below
]

# Quarterly twins of every annuals__ code — fresher point-in-time data
# (e.g. Debt-to-Equity) for SnapshotStats to prefer when more recent.
_DASHBOARD_METRIC_CODES += [
    "quarterly__" + c[len("annuals__"):]
    for c in _DASHBOARD_METRIC_CODES
    if c.startswith("annuals__")
]

_LONGEQUITY_METRIC_CODES = [
    "share_price_5yr_cagr",
    "share_price_5yr_rsq",
    "share_price_10yr_cagr",
    "share_price_10yr_rsq",
    "revenue_growth_5yr",
    "revenue_growth_rsq",
    "fcf_growth_5yr",
    "fcf_growth_sd",
    "fcf_growth_rsq",
]


def load_company_metric_rows(company_id: int) -> list[dict]:
    """Load the dashboard metric rows for one company (source=gurufocus +
    longequity, dates >= 1998). Returns `{metric_code, target_date,
    numeric_value, is_prediction}` dicts.

    Shared by the single-company `/metrics` endpoint and the portfolio
    aggregation endpoint (`routers/earnings_portfolios.py`).

    PostgREST caps a single response at ~1000 rows regardless of `.limit(N)`,
    and our `.order("target_date")` is ascending — so a flat `.limit(5000)`
    silently returns the OLDEST 1000 rows and hides everything recent.
    Every multi-row read here paginates instead.
    """
    def _paginate(builder_factory) -> list[dict]:
        rows: list[dict] = []
        offset = 0
        page_size = 1000
        while True:
            page = builder_factory().range(offset, offset + page_size - 1).execute()
            batch = page.data or []
            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        return rows

    non_price_codes = [c for c in _DASHBOARD_METRIC_CODES if c != "close_price"]
    rows = _paginate(lambda: (
        supabase.table("metric_data")
        .select("metric_code,target_date,numeric_value,is_prediction")
        .eq("company_id", company_id)
        .eq("source_code", "gurufocus")
        .gte("target_date", "1998-01-01")
        .in_("metric_code", non_price_codes)
        .order("target_date")
    ))

    rows.extend(_paginate(lambda: (
        supabase.table("metric_data")
        .select("metric_code,target_date,numeric_value,is_prediction")
        .eq("company_id", company_id)
        .eq("source_code", "gurufocus")
        .eq("metric_code", "close_price")
        .gte("target_date", "1998-01-01")
        .order("target_date")
    )))

    # Analyst estimates (annual_* prefix).
    rows.extend(_paginate(lambda: (
        supabase.table("metric_data")
        .select("metric_code,target_date,numeric_value,is_prediction")
        .eq("company_id", company_id)
        .eq("source_code", "gurufocus")
        .eq("is_prediction", True)
        .gte("target_date", "1998-01-01")
        .like("metric_code", "annual_%")
        .order("target_date")
    )))

    rows.extend(_paginate(lambda: (
        supabase.table("metric_data")
        .select("metric_code,target_date,numeric_value,is_prediction")
        .eq("company_id", company_id)
        .eq("source_code", "longequity")
        .in_("metric_code", _LONGEQUITY_METRIC_CODES)
        .order("target_date")
    )))
    return rows


@router.get("/api/earnings/{company_id}/metrics")
async def get_earnings_metrics(company_id: int):
    """Dashboard metrics for a company (source=gurufocus, dates >= 1998)."""
    try:
        return load_company_metric_rows(company_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")


@router.get("/api/earnings/by-isin/{isin}/metrics")
async def get_earnings_metrics_by_isin(isin: str):
    """Dashboard metrics for a company resolved BY ISIN — the /portfolios
    Fundamental modal bridge (ISIN → `company.isin` → company_id, "Bridge A").

    Only the ~13% of instruments backed by a `company` row have earnings
    metrics; everything else (ETFs, structured products, foreign listings with
    no company row) 404s here, and the modal falls back to its owner-earnings /
    price tabs, which work for every ISIN. Returns
    `{company_id, company_name, currency, metrics}` — `currency` is the
    exchange's `currency_code` (the reporting/trading currency the FCF/share
    chart converts to EUR), mirroring how `/api/companies` derives it.
    """
    def _resolve() -> dict | None:
        # ⚠ SAME ALIAS HOP AS THE COVERAGE REPORT, or a holding reported as covered 404s here —
        # worse than reporting it uncovered, because the disclosure would then be wrong too.
        from asset_pipeline.isin_alias import canonical  # noqa: PLC0415

        resp = (
            supabase.table("company")
            .select("company_id,company_name,gurufocus_exchange:gurufocus_exchange(currency_code)")
            .eq("isin", canonical(isin))
            .limit(1)
            .execute()
        )
        if not resp.data:
            return None
        row = resp.data[0]
        exch = row.pop("gurufocus_exchange", None) or {}
        return {
            "company_id": row["company_id"],
            "company_name": row.get("company_name"),
            "currency": exch.get("currency_code"),
        }

    try:
        info = await asyncio.to_thread(_resolve)
        if info is None:
            raise HTTPException(status_code=404, detail="No company record for this ISIN")
        rows = await asyncio.to_thread(load_company_metric_rows, info["company_id"])
        return {**info, "metrics": rows}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")


@router.get("/api/earnings/{company_id}/metric-codes")
async def get_earnings_metric_codes(company_id: int):
    """Debug: distinct metric codes stored for a company."""
    try:
        resp = (
            supabase.table("metric_data")
            .select("metric_code")
            .eq("company_id", company_id)
            .eq("source_code", "gurufocus")
            .limit(10000)
            .execute()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")
    codes = sorted({r["metric_code"] for r in (resp.data or [])})
    return {"count": len(codes), "codes": codes}


def _EMPTY_COVERAGE() -> dict:
    """⚠ THE SAME KEYS AS A REAL ANSWER. An early return that drops `by_reason_pct` crashes every
    consumer that iterates it — and it does so on the emptiest input, which is the one least likely
    to be tried by hand."""
    return {"holdings": 0, "covered_pct": 0.0, "by_reason_pct": {}, "rows": []}


class FundamentalCoverageRequest(BaseModel):
    """Either a model portfolio's id, or an explicit basket of (isin, weight)."""

    portfolio_id: int | None = None
    holdings: list[dict] | None = None


async def _load_and_expand_members(body: FundamentalCoverageRequest) -> list[dict]:
    """The flat holdings to analyse, with every linked certificate looked THROUGH to the model it
    IS — so its real stocks feed both the coverage table and the blended charts, rather than
    dropping out as one dead CH-ISIN row.

    ⚠ ONE PLACE, THREE ENDPOINTS. coverage / blend / blend-metrics all start from the identical
    member list and must agree on it — the blend renormalises over the coverage the SAME members
    produce. A look-through applied in one but not another would blend over stocks the coverage
    table never admitted. Raises 422 when neither field is set; returns [] for an empty portfolio.
    """
    from routers._airs_portfolio_links import expand_members_through_links  # noqa: PLC0415

    members = body.holdings
    owner_id = 0
    if members is None:
        if body.portfolio_id is None:
            raise HTTPException(status_code=422, detail="portfolio_id or holdings is required")
        owner_id = body.portfolio_id

        def _load() -> list[dict]:
            return [{"isin": p.get("isin"), "name": p.get("fonds"),
                     "weight": float(p.get("percentage") or 0)}
                    for p in (supabase.table("airs_model_portfolio_position")
                              .select("fonds,isin,percentage")
                              .eq("portfolio_id", body.portfolio_id)
                              .limit(500).execute().data or [])]

        members = await asyncio.to_thread(_load)
    if not members:
        return []
    return await asyncio.to_thread(
        expand_members_through_links, supabase, members, owner_id=owner_id)


class FundamentalIngestRequest(BaseModel):
    """One uncovered holding to try to ingest fundamentals for. `name` seeds the `company` row
    when one must be created (a `no_company` holding); `force` re-asks GuruFocus past the cache."""

    isin: str
    name: str | None = None
    force: bool = False


@router.post("/api/earnings/fundamental-coverage/ingest")
async def ingest_fundamental_coverage(body: FundamentalIngestRequest):
    """Fetch + load the GuruFocus fundamentals ONE uncovered holding is missing.

    Handles both gaps the coverage table flags as ours to close: a `no_metrics` holding (a company
    row exists — just fetch) and a `no_company` holding (resolve the ISIN to a listing, CREATE the
    company, then fetch). Every other reason is refused with its own status, never as a failure —
    see `_fundamental_ingest`.

    ⚠ ADMIN-ONLY BY DEFAULT. This CREATES company rows and spends GuruFocus quota, so it is not in
    the earnings-refresh user-write allow-list (the path carries no `/refresh`) and the auth gate
    holds it to admins. The /management-dashboard portfolios page that surfaces it is admin-only.

    The frontend calls this per row and, for "ingest all", once per distinct ingestable ISIN.
    """
    from routers._fundamental_ingest import ingest_fundamentals_for_isin  # noqa: PLC0415

    if not (body.isin or "").strip():
        raise HTTPException(status_code=422, detail="isin is required")
    return await asyncio.to_thread(
        ingest_fundamentals_for_isin, body.isin, body.name, force=body.force)


@router.post("/api/earnings/fundamental-coverage")
async def fundamental_coverage(body: FundamentalCoverageRequest):
    """Which of a portfolio's holdings a fundamentals view can reach, BY WEIGHT, and why not.

    ⚠ COVERAGE IS THE FIRST ANSWER, NOT A FOOTNOTE. Every holding that cannot be reached is weight
    that drops out of any blend, and a blended figure over 61% of a book presented as the book's is
    the same fabrication `MIN_COVERAGE_PCT` already guards against on the AIRS returns.
    """
    from routers._fundamental_coverage import coverage_for_async  # noqa: PLC0415

    members = await _load_and_expand_members(body)
    if not members:
        return _EMPTY_COVERAGE()
    return await coverage_for_async(members)


# The metrics the blended view charts. Deliberately short: each is a metric whose portfolio-level
# meaning is unambiguous once the right rule is applied (see `_fundamental_blend`).
# ⚠ CHOSEN FROM WHAT IS ACTUALLY INGESTED, NOT FROM WHAT WOULD BE NICE. Measured 2026-07-23,
# `ingest/earnings/financials.py` lands Balance Sheet + Cashflow Statement only — no Income
# Statement, no Per Share Data, no Ratios section (the GuruFocus section-rename bug already in
# TODO.md). Listing `PE Ratio` or `Revenue` here would draw four empty panels under a confident
# heading, which is the failure this whole view exists to avoid.
#
# One of each blend kind, so all three rules are exercised on real data.
BLEND_METRICS = [
    "annuals__Cashflow Statement__Free Cash Flow",                      # level -> growth index
    "annuals__Cashflow Statement__Net Income From Continuing Operations",  # level
    "annuals__Balance Sheet__Total Assets",                             # level
    "annuals__Balance Sheet__Debt-to-Equity",                           # ratio -> arithmetic
]

# Human labels for the chart headings — the raw code is a section path, not a title.
BLEND_LABELS = {
    "annuals__Cashflow Statement__Free Cash Flow": "Free cash flow",
    "annuals__Cashflow Statement__Net Income From Continuing Operations": "Net income",
    "annuals__Balance Sheet__Total Assets": "Total assets",
    "annuals__Balance Sheet__Debt-to-Equity": "Debt-to-equity",
}


@router.post("/api/earnings/fundamental-blend")
async def fundamental_blend(body: FundamentalCoverageRequest):
    """A portfolio's fundamentals, blended — with the rule that each metric actually requires.

    ⚠ THREE RULES, NOT ONE. A multiple aggregates HARMONICALLY (a portfolio's P/E is aggregate
    price over aggregate earnings; the arithmetic mean of 10 and 100 is 55 against a true 18.2),
    a yield/margin arithmetically, and a level only after rebasing to an index. See
    `_fundamental_blend` for why each alternative is wrong.

    Coverage rides along and is a FLOOR: a date under it carries no honest value and is omitted
    rather than drawn as a dip.
    """
    from routers._fundamental_blend import blend_series  # noqa: PLC0415
    from routers._fundamental_coverage import coverage_for_async  # noqa: PLC0415

    members = await _load_and_expand_members(body)
    if not members:
        # ⚠ The SAME shape as a populated answer — see `_EMPTY_COVERAGE`.
        return {"coverage": _EMPTY_COVERAGE(), "labels": BLEND_LABELS, "series": {}}

    cov = await coverage_for_async(members)
    covered = [r for r in cov["rows"] if r["reason"] == "covered" and r.get("company_id")]
    if not covered:
        return {"coverage": cov, "labels": BLEND_LABELS, "series": {}}

    def _blend() -> dict:
        ids = [r["company_id"] for r in covered]
        rows: list[dict] = []
        for i in range(0, len(ids), 50):
            rows += (supabase.table("metric_data")
                     .select("company_id,metric_code,target_date,numeric_value")
                     .in_("company_id", ids[i:i + 50])
                     .in_("metric_code", BLEND_METRICS)
                     .limit(50000).execute().data or [])
        # {metric: {company_id: {date: value}}}
        by_metric: dict[str, dict[int, dict[str, float]]] = {}
        for r in rows:
            if r.get("numeric_value") is None:
                continue
            by_metric.setdefault(r["metric_code"], {}).setdefault(r["company_id"], {})[
                str(r["target_date"])[:10]] = float(r["numeric_value"])
        out: dict[str, dict] = {}
        for code in BLEND_METRICS:
            per_company = by_metric.get(code, {})
            out[code] = blend_series(
                [{"weight": r["weight_pct"], "points": per_company.get(r["company_id"], {})}
                 for r in covered], code)
        return out

    return {"coverage": cov, "labels": BLEND_LABELS,
            "series": await asyncio.to_thread(_blend)}


# ⚠ THE BLEND WINDOW, PUSHED DOWN TO THE QUERY. It matches the charts' own start year, so the
# rows dropped here are rows nothing renders — and it is what keeps the read bounded (a company
# carries ~7,200 `annuals__` rows back to the 1990s; from 2015 it is ~2,600). It also fixes WHERE
# a level series is rebased to 100: at the first date on screen, rather than at a 1990s base the
# viewer cannot see.
_BLEND_START = "2015-01-01"

# A forecast series -> the ACTUAL series it continues. Both are the same quantity (one measured,
# one estimated) and the charts index them off a SINGLE base, so the blend must rebase them on a
# single base too. Keys are the codes the charts read; values are their historical counterparts.
_FORECAST_BASE = {
    "annual_eps_nri_estimate": "annuals__Per Share Data__EPS without NRI",
    "annual_per_share_eps_estimate": "annuals__Per Share Data__EPS without NRI",
    "annual_dividend_estimate": "annuals__Per Share Data__Dividends per Share",
}

# ⚠ PostgREST's cap is a SERVER setting, and it is lower in production than locally: 1,000 rows
# cloud, 10,000 local. A page larger than the cap is silently trimmed — no error, no flag — so
# this is deliberately at the cloud cap and the loop stops on a SHORT page, never on a count.
_PAGE = 1000


def _page_metrics(company_id: int, pattern: str, *, exact: bool = False) -> list[dict]:
    """Every matching metric row for one company, paged.

    ⚠ `exact=True` MATCHES THE CODE, NOT A PATTERN — and for a single code that is required, not
    tidier. Metric codes contain both LIKE wildcards: `%` (`annuals__Ratios__ROE %`) and `_`
    (every code). As a pattern, "ROE %" matches "ROE " followed by ANYTHING.

    ⚠ A SINGLE `.limit(n)` HERE IS A SILENT DATA LOSS, NOT AN ERROR — and it does not fail
    loudly anywhere downstream either. Measured 2026-07-23: held companies carry **7,224**
    `annuals__` rows each, so `.limit(5000)` returned 5,000 and dropped 2,224 per company. The
    lost rows are not spread evenly over the metrics: whole codes come back partial, their
    per-year coverage falls under `MIN_BLEND_COVERAGE_PCT`, and the blend correctly refuses to
    draw them. The panel then reads "No data. Refresh to load." — a truthful message about an
    untruthful read, which is why it looked like a charting bug. In production the same code
    would have lost ~86% of every company's rows.
    """
    out: list[dict] = []
    start = 0
    while True:
        page = (supabase.table("metric_data")
                .select("company_id,metric_code,target_date,numeric_value")
                .eq("company_id", company_id).like("metric_code", pattern)
                .gte("target_date", _BLEND_START)
                .order("target_date").range(start, start + _PAGE - 1).execute().data or [])
        out += page
        if len(page) < _PAGE:
            return out
        start += _PAGE


@router.post("/api/earnings/fundamental-blend-metrics")
async def fundamental_blend_metrics(body: FundamentalCoverageRequest):
    """The portfolio as ONE pseudo-company, in the exact shape `/by-isin/{isin}/metrics` returns.

    Same payload, so the whole /earnings chart suite renders for a portfolio with no changes —
    `{company_id, company_name, currency, metrics}` where `metrics` are blended across the covered
    holdings, weighted by their portfolio weight.

    ⚠ EVERY METRIC IS BLENDED BY THE RULE ITS OWN KIND REQUIRES (see `_fundamental_blend`): a
    multiple harmonically, a ratio/margin arithmetically, and a LEVEL only after rebasing to an
    index. Weighting Apple's revenue by 5% and ASML's by 3% is not a portfolio's revenue.

    ⚠ `currency` IS NULL, DELIBERATELY. Members report in their own currencies, and a level series
    has been rebased to an index anyway — there is no currency such a number could be in. The
    charts' EUR conversion is driven off this field, so a currency here would relabel an index as
    money. A null says "not a currency amount", which is the truth.
    """
    from routers._fundamental_blend import blend_series  # noqa: PLC0415
    from routers._fundamental_coverage import coverage_for_async  # noqa: PLC0415

    members = await _load_and_expand_members(body)
    if not members:
        raise HTTPException(status_code=404, detail="no holdings to blend")

    cov = await coverage_for_async(members)
    covered = [r for r in cov["rows"] if r["reason"] == "covered" and r.get("company_id")]
    if not covered:
        raise HTTPException(status_code=404, detail="no holding has fundamentals to blend")

    def _build() -> dict:
        ids = [r["company_id"] for r in covered]
        rows: list[dict] = []
        # ⚠ Paged per company, never one wildcard over the lot: a company carries ~110 codes x
        # ~28 years, so twenty at once is ~60k rows against PostgREST's silent 1,000-row cap and
        # the tail would come back looking like companies with no data.
        # ⚠ TWO PATTERNS, BECAUSE THE ESTIMATES ARE NOT NAMED LIKE THE STATEMENTS. A statement
        # line is `annuals__Section__Line`; an analyst estimate is `annual_pettm_estimate` —
        # SINGULAR, no section, no double underscore. Filtering on `annuals__%` alone silently
        # dropped every estimate, which is exactly the set Forward P/E and the OE Estimate line
        # are built from: those two panels read "no data" while the rest of the suite filled in.
        #
        # ⚠ AND NOT AN UNFILTERED FETCH EITHER. `close_price` alone is ~10k rows per company —
        # a daily series that no blended fiscal-year chart uses and that would blow past
        # PostgREST's cap on its own.
        for cid in ids:
            # Raw strings: _ escapes LIKE's single-char wildcard, and a plain "_" in a normal
            # Python string is an invalid escape sequence (a SyntaxWarning now, an error later).
            # ⚠ THREE PATTERNS, THREE FEEDS. A statement line is `annuals__Section__Line`; an
            # analyst estimate is `annual_pettm_estimate` (SINGULAR, no section); and Forward P/E
            # is `indicator_q_forward_pe_ratio` — an INDICATOR, a third naming scheme again. Miss
            # one and its panels read "no data" while the rest of the suite fills in around them.
            for pattern in (r"annuals__%", r"annual_%estimate", r"indicator%"):
                rows += _page_metrics(cid, pattern)
        by_metric: dict[str, dict[int, dict[str, float]]] = {}
        for r in rows:
            if r.get("numeric_value") is None:
                continue
            (by_metric.setdefault(r["metric_code"], {})
                      .setdefault(r["company_id"], {}))[str(r["target_date"])[:10]] = \
                float(r["numeric_value"])

        out: list[dict] = []
        for code, per_company in by_metric.items():
            # ⚠ A FORECAST INHERITS THE ANCHOR OF THE ACTUAL IT CONTINUES. Both are the same
            # quantity and the chart indexes them off ONE base, so rebasing them independently
            # draws the forecast restarting at 100 beside an actual that has run to 1,808 — a
            # ~94% earnings collapse that exists only in the arithmetic.
            base_code = _FORECAST_BASE.get(code)
            base_by_company = by_metric.get(base_code, {}) if base_code else {}
            s = blend_series([{"weight": r["weight_pct"],
                               "points": per_company.get(r["company_id"], {}),
                               "base_points": base_by_company.get(r["company_id"], {})}
                              for r in covered], code)
            for p in s["points"]:
                # The fiscal-year key becomes a date the charts can plot. 31 Dec is a convention
                # here, not a claim about anyone's year-end — members close on different days,
                # which is exactly why the blend aligns on the year in the first place.
                out.append({"metric_code": code, "target_date": f"{p['period']}-12-31",
                            "numeric_value": p["value"], "is_prediction": False})
        return {"metrics": out, "codes": len(by_metric)}

    built = await asyncio.to_thread(_build)
    return {
        "company_id": None,
        "company_name": f"{len(covered)} holdings · {cov['covered_pct']:.0f}% of weight",
        "currency": None,
        "metrics": built["metrics"],
        "coverage": cov,
    }


class FundamentalBreakdownRequest(FundamentalCoverageRequest):
    """One blended point to take apart: which metric, which fiscal year."""

    metric_code: str
    period: str          # the fiscal YEAR, e.g. "2025" — a full date is accepted and truncated


@router.post("/api/earnings/fundamental-blend-breakdown")
async def fundamental_blend_breakdown(body: FundamentalBreakdownRequest):
    """The holdings behind ONE point of a blended chart, and the ones missing from it.

    ⚠ IT LOADS ONE METRIC, NOT THE SUITE. The blend endpoint reads every charted code for every
    holding; a drill-down needs one code (plus, for a forecast, the actual it is anchored on), so
    it is a small read on click rather than a large one on open.

    ⚠ IT DECOMPOSES THROUGH `blend_breakdown`, WHICH SHARES `_prepare` WITH THE LINE ITSELF. The
    alternative — recomputing the members "the same way" here — is a second copy of the
    harmonic/ratio/level rules, and a drill-down that quietly disagrees with the chart above it
    is worse than none: it is checked once and trusted from then on.
    """
    from routers._fundamental_blend import blend_breakdown  # noqa: PLC0415
    from routers._fundamental_coverage import coverage_for_async  # noqa: PLC0415

    members = await _load_and_expand_members(body)
    if not members:
        raise HTTPException(status_code=404, detail="no holdings to blend")

    cov = await coverage_for_async(members)
    covered = [r for r in cov["rows"] if r["reason"] == "covered" and r.get("company_id")]
    if not covered:
        raise HTTPException(status_code=404, detail="no holding has fundamentals to blend")

    code = body.metric_code
    base_code = _FORECAST_BASE.get(code)
    period = body.period[:4]

    def _build() -> dict:
        per_company: dict[int, dict[str, float]] = {}
        base_by_company: dict[int, dict[str, float]] = {}
        for r in covered:
            cid = r["company_id"]
            for want, sink in ((code, per_company), (base_code, base_by_company)):
                if not want:
                    continue
                pts = {str(m["target_date"])[:10]: float(m["numeric_value"])
                       for m in _page_metrics(cid, want, exact=True)
                       if m.get("numeric_value") is not None}
                sink[cid] = pts
        return blend_breakdown(
            [{"isin": r.get("isin"), "name": r.get("name"), "weight": r["weight_pct"],
              "points": per_company.get(r["company_id"], {}),
              "base_points": base_by_company.get(r["company_id"], {})}
             for r in covered], code, period)

    out = await asyncio.to_thread(_build)
    out["blend_covered_pct"] = cov["covered_pct"]
    return out


# The two level series the Share-Price-vs-Owner-Earnings chart compares.
_RG_PRICE_CODE = "annuals__Per Share Data__Month End Stock Price"
_RG_OE_CODE = "annuals__Per Share Data__EPS without NRI"
# ⚠ TWO SPELLINGS PER METRIC. GuruFocus renamed its statement sections ("Income Statement" → the
# lowercase "income_statement", "Per Share Data" → "per_share_data"), and both live in `metric_data`
# depending on when a company was fetched (ASML capitalised, a fresh OTC fetch lowercase). Match
# BOTH or a whole cohort reads as missing. Same latent rename `_asset_financials._SECTIONS` handles.
_METRIC_CODES: dict[str, tuple[str, ...]] = {
    "revenue": ("annuals__Income Statement__Revenue", "annuals__income_statement__Revenue"),
    "fcf_ps": ("annuals__Per Share Data__Free Cash Flow per Share",
               "annuals__per_share_data__Free Cash Flow per Share"),
    "fcf": ("annuals__Cashflow Statement__Free Cash Flow",
            "annuals__cashflow_statement__Free Cash Flow"),
    "sbc": ("annuals__Cashflow Statement__Stock Based Compensation",
            "annuals__cashflow_statement__Stock Based Compensation"),
    # Balance-sheet lines behind the LTD / (Total Assets − Goodwill) card. ⚠ Some issuers
    # carry NO "Long-Term Debt" line at all (Berkshire) — the ratio is then blank, not 0.
    "long_term_debt": ("annuals__Balance Sheet__Long-Term Debt",
                       "annuals__balance_sheet__Long-Term Debt"),
    "total_assets": ("annuals__Balance Sheet__Total Assets",
                     "annuals__balance_sheet__Total Assets"),
    "goodwill": ("annuals__Balance Sheet__Goodwill",
                 "annuals__balance_sheet__Goodwill"),
    # Behind the Cash-return-on-capital + Invested-capital cards: invested capital = non-current
    # liabilities + total equity (FCF ÷ that = cash return). ⚠ Non-current liabilities is ABSENT
    # for issuers that don't split current/non-current (Berkshire, and banks — JPMorgan has no such
    # line) → both cards are blank there, not 0. `total_equity` is `Total Equity` — INCL. minority
    # interest (Constellation: 4,268 vs 3,576 stockholders'-only), so NCL + Total Equity = Total
    # Assets − Current Liabilities (the balance-sheet identity), a coherent capital base.
    "noncurrent_liabilities": ("annuals__Balance Sheet__Total Long-Term Liabilities",
                               "annuals__balance_sheet__Total Long-Term Liabilities"),
    "total_equity": ("annuals__Balance Sheet__Total Equity",
                     "annuals__balance_sheet__Total Equity"),
    # Behind the interest-burden card: |Interest expense| ÷ Operating income. ⚠ Interest
    # expense is reported NEGATIVE (an outflow); the card takes its magnitude. "Operating
    # profit" is GuruFocus's `Operating Income` line — deliberately NOT `EBIT`, which is a
    # DIFFERENT line (Constellation: OpInc 2,108 vs EBIT 1,195). The ratio is blank when
    # operating income ≤ 0 (a loss — the "% of profit" question doesn't apply).
    "interest_expense": ("annuals__Income Statement__Interest Expense",
                         "annuals__income_statement__Interest Expense"),
    "operating_income": ("annuals__Income Statement__Operating Income",
                         "annuals__income_statement__Operating Income"),
    # Shares outstanding (diluted average, millions of SHARES — not currency, so the holdings
    # drill-down shows a raw count). ⚠ Use the INCOME STATEMENT spelling: it's the one both
    # cohorts share — the Per Share Data section diverges (`per_share_data` vs
    # `per_share_data_array`) between the capitalized and lowercase cohorts.
    "shares": ("annuals__Income Statement__Shares Outstanding (Diluted Average)",
               "annuals__income_statement__Shares Outstanding (Diluted Average)"),
    # Behind the SBC/OCF card: Stock-Based Compensation ÷ Operating Cash Flow (`sbc` above is the
    # numerator). ⚠ Operating cash flow can go NEGATIVE (a bank: JPMorgan) — the ratio is blank
    # there, not computed against a negative denominator.
    "ocf": ("annuals__Cashflow Statement__Cash Flow from Operations",
            "annuals__cashflow_statement__Cash Flow from Operations"),
    # Behind the Capex-margin card: |Capex| ÷ Revenue (capital intensity). ⚠ Capex is GuruFocus's
    # `Capital Expenditure` line (NOT `Purchase Of Property, Plant, Equipment` — capex also picks up
    # intangibles), reported NEGATIVE (an outflow); the card takes its magnitude. Revenue is the
    # `revenue` key above.
    "capex": ("annuals__Cashflow Statement__Capital Expenditure",
              "annuals__cashflow_statement__Capital Expenditure"),
}
_REVENUE_CODES = _METRIC_CODES["revenue"]
_REVENUE_CODE = _REVENUE_CODES[0]   # for blend_kind() only — it classifies, it doesn't query


def _metric_codes(metric: str) -> tuple[str, ...]:
    return _METRIC_CODES.get(metric, _METRIC_CODES["revenue"])


def _metric_rows(company_id: int, metric: str = "revenue") -> list[dict]:
    """A metric's rows for a company under EITHER section spelling (see `_METRIC_CODES`)."""
    out: list[dict] = []
    for code in _metric_codes(metric):
        out += _page_metrics(company_id, code, exact=True)
    return out


def _revenue_rows(company_id: int) -> list[dict]:
    return _metric_rows(company_id, "revenue")


def _metric_by_year(company_id: int, metric: str) -> dict[str, float]:
    """{year: value} for a metric, the LATEST observation in each fiscal year (both spellings)."""
    by: dict[str, tuple[str, float]] = {}
    for m in _metric_rows(company_id, metric):
        v = m.get("numeric_value")
        if v is None:
            continue
        d = str(m["target_date"])[:10]
        y = d[:4]
        if y not in by or d > by[y][0]:
            by[y] = (d, float(v))
    return {y: v for y, (_d, v) in by.items()}


@router.get("/api/earnings/benchmark-revenue")
async def benchmark_revenue(label: str = "AEX", metric: str = "revenue"):
    """A benchmark index's revenue (or any `_METRIC_CODES` metric) as a GROWTH INDEX — its
    constituents' figures blended the same
    way a portfolio's is (a LEVEL → each rebased to 100 at its first year, then weighted).

    ⚠ NOT A SUM OF ABSOLUTE REVENUES. AEX constituents report in different currencies (Shell/RELX/
    Unilever in GBP), so a euro total would silently add pounds to euros. The level-blend sidesteps
    that — it compares GROWTH, which is what the R² read on the /Long Equity tab needs — and drops
    any year under the 60% coverage floor rather than drawing it thin.

    Cap-weighted by `market_cap_eur` where known, else equal-weighted (a benchmark growth reference,
    not a priced index). Returns `{label, series:[{year, value}], members, covered_pct}`.
    """
    from routers._fundamental_blend import blend_series  # noqa: PLC0415

    def _run() -> dict:
        uni = (supabase.table("universe").select("universe_id")
               .eq("label", label).limit(1).execute().data or [])
        if not uni:
            raise HTTPException(status_code=404, detail=f"No universe labelled {label!r}")
        uid = uni[0]["universe_id"]
        ids = sorted({r["company_id"] for r in
                      (supabase.table("universe_membership").select("company_id")
                       .eq("universe_id", uid).execute().data or []) if r.get("company_id")})
        if not ids:
            raise HTTPException(status_code=404, detail=f"{label} has no members")

        caps: dict[int, float] = {}
        for i in range(0, len(ids), IN_CHUNK_SIZE):
            for c in (supabase.table("company").select("company_id,market_cap_eur")
                      .in_("company_id", ids[i:i + IN_CHUNK_SIZE]).execute().data or []):
                if c.get("market_cap_eur"):
                    caps[c["company_id"]] = float(c["market_cap_eur"])

        members = []
        for cid in ids:
            pts = {str(m["target_date"])[:10]: float(m["numeric_value"])
                   for m in _metric_rows(cid, metric)
                   if m.get("numeric_value") is not None}
            if pts:
                members.append({"weight": caps.get(cid, 1.0), "points": pts})
        blend = blend_series(members, _metric_codes(metric)[0])
        # ⚠ `period` is the YEAR AS A STRING ("2015"); return it as an int so the frontend can join
        # it against the company's numeric years (a string/number mismatch = no overlap = no line).
        return {"label": label, "members": len(members),
                "covered_pct": blend["covered_pct"],
                "series": [{"year": int(p["period"]), "value": p["value"]}
                           for p in blend["points"] if str(p["period"]).isdigit()]}

    return await asyncio.to_thread(_run)


@router.get("/api/earnings/benchmark-revenue-matrix")
async def benchmark_revenue_matrix(label: str = "AEX"):
    """The audit grid behind the benchmark revenue line: every constituent's revenue at every year,
    the blended footer that reconciles to the line, AND where each series comes from.

    Same `blend_matrix` the Forward-P/E "All periods" grid uses (revenue is a LEVEL → each rebased
    to a growth index, weighted), so the cells and footer are built from exactly what the line is.
    Each row (and each excluded constituent) carries a `source` — `TICKER@EXCHANGE` — so a reader
    sees which listing's GuruFocus figures fed it. Constituents with no revenue land in `excluded`
    (reason `no_data`): that names the ones we simply have nothing for, which is half the answer.
    """
    from routers._fundamental_blend import blend_matrix  # noqa: PLC0415

    def _run() -> dict:
        uni = (supabase.table("universe").select("universe_id")
               .eq("label", label).limit(1).execute().data or [])
        if not uni:
            raise HTTPException(status_code=404, detail=f"No universe labelled {label!r}")
        uid = uni[0]["universe_id"]
        ids = sorted({r["company_id"] for r in
                      (supabase.table("universe_membership").select("company_id")
                       .eq("universe_id", uid).execute().data or []) if r.get("company_id")})
        if not ids:
            raise HTTPException(status_code=404, detail=f"{label} has no members")

        info: dict[int, dict] = {}
        for i in range(0, len(ids), IN_CHUNK_SIZE):
            for c in (supabase.table("company")
                      .select("company_id,company_name,isin,gurufocus_ticker,market_cap_eur,"
                              "has_financials,"
                              "gurufocus_exchange:gurufocus_exchange(exchange_code,currency_code)")
                      .in_("company_id", ids[i:i + IN_CHUNK_SIZE]).execute().data or []):
                info[c["company_id"]] = c

        members: list[dict] = []
        source_by_key: dict[str, str] = {}
        ccy_by_key: dict[str, str | None] = {}
        fin_by_key: dict[str, bool | None] = {}
        for cid in ids:
            c = info.get(cid, {})
            name = c.get("company_name") or f"company {cid}"
            isin = c.get("isin")
            key = isin or name
            gx = (c.get("gurufocus_exchange") or {}) or {}
            exch = gx.get("exchange_code") or "?"
            source_by_key[key] = f"{c.get('gurufocus_ticker') or '?'}@{exch}"
            ccy_by_key[key] = gx.get("currency_code")
            fin_by_key[key] = c.get("has_financials")
            pts = {str(m["target_date"])[:10]: float(m["numeric_value"])
                   for m in _revenue_rows(cid)
                   if m.get("numeric_value") is not None}
            # Include even the empty ones — blend_matrix routes them to `excluded` as `no_data`,
            # which is exactly the "which constituents do we have nothing for" answer.
            members.append({"isin": isin, "name": name,
                            "weight": c.get("market_cap_eur") or 1.0, "points": pts})

        mx = blend_matrix(members, _REVENUE_CODE)
        for row in mx["members"]:
            key = row.get("isin") or row.get("name")
            row["source"] = source_by_key.get(key)
            row["currency"] = ccy_by_key.get(key)
        for row in mx.get("excluded", []):
            key = row.get("isin") or row.get("name")
            row["source"] = source_by_key.get(key)
            row["currency"] = ccy_by_key.get(key)
            # ⚠ DISTINGUISH "WE NEVER FETCHED IT" FROM "GURUFOCUS HAS NO REVENUE LINE". A member
            # with financials but no Revenue is a template thing (a bank reports Net Interest
            # Income); one with no financials at all simply hasn't been ingested — a gap on our
            # side, fixable, NOT a claim that GuruFocus lacks the data.
            if row.get("reason") == "no_data":
                row["detail"] = ("financials ingested, but no Revenue line (e.g. a bank template)"
                                 if fin_by_key.get(key) else "financials not ingested yet")
        mx["label"] = label
        return mx

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/portfolio-revenue-matrix")
async def portfolio_revenue_matrix(body: FundamentalCoverageRequest, metric: str = "revenue"):
    """Each equity the portfolio HOLDS: its weight, currency, and actual `metric` per fiscal year
    (2015 onwards), in the company's own reporting currency.

    ⚠ THE HOLDINGS, NOT AN INDEX. Members come from the portfolio (looked THROUGH any linked
    certificate via `_load_and_expand_members`), deduped by ISIN (a name held twice is one row with
    summed weight). Weight is the share of the WHOLE book (cash/bonds in the denominator, so the
    shown companies sum to under 100%). Holdings with no company row / no revenue are omitted —
    this lists the companies we can actually show revenue for.
    """
    from asset_pipeline.isin_alias import canonical_map  # noqa: PLC0415
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

    members = await _load_and_expand_members(body)
    if not members:
        raise HTTPException(status_code=404, detail="no holdings")

    def _run() -> dict:
        total_w = sum(abs(float(m.get("weight") or 0)) for m in members) or 1.0
        raw = sorted({m["isin"] for m in members if m.get("isin")})
        alias = canonical_map(raw)

        weight_by: dict[str, float] = {}
        name_by: dict[str, str] = {}
        for m in members:
            isin = (m.get("isin") or "").strip()
            if not isin:
                continue                      # cash / no-ISIN line — not a company
            ci = alias.get(isin, isin)
            weight_by[ci] = weight_by.get(ci, 0.0) + abs(float(m.get("weight") or 0))
            name_by.setdefault(ci, m.get("name") or isin)

        canon = sorted(weight_by)
        comp: dict[str, dict] = {}
        for i in range(0, len(canon), IN_CHUNK_SIZE):
            for c in (supabase.table("company")
                      .select("company_id,company_name,isin,gurufocus_ticker,"
                              "gurufocus_exchange:gurufocus_exchange(exchange_code,currency_code)")
                      .in_("isin", canon[i:i + IN_CHUNK_SIZE]).execute().data or []):
                comp[c["isin"]] = c

        rows: list[dict] = []
        years: set[str] = set()
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            by_year: dict[str, tuple[str, float]] = {}
            for m in _metric_rows(c["company_id"], metric):
                if m.get("numeric_value") is None:
                    continue
                d = str(m["target_date"])[:10]
                y = d[:4]
                if y < "2015":
                    continue
                if y not in by_year or d > by_year[y][0]:     # latest observation in the year
                    by_year[y] = (d, float(m["numeric_value"]))
            rev = {y: v for y, (_d, v) in by_year.items()}
            years |= set(rev)
            # WHY revenue is missing, when it is: a company on an exchange outside the GuruFocus
            # subscription (Brookfield on TSX) can't be fetched at all → `unsubscribed`; one on a
            # subscribed exchange with nothing ingested → `no_data`; otherwise `ok`.
            exch = gx.get("exchange_code")
            subscribed = is_gf_subscribed_exchange(exch) if exch else None
            status = "ok" if rev else ("unsubscribed" if subscribed is False else "no_data")
            rows.append({
                "isin": ci, "name": c.get("company_name") or name_by.get(ci) or ci,
                "weight_pct": round(100.0 * weight_by[ci] / total_w, 2),
                "currency": gx.get("currency_code"),
                "ticker": c.get("gurufocus_ticker"),
                "exchange": exch,
                "status": status,
                "revenue": rev,
            })
        rows.sort(key=lambda r: -r["weight_pct"])
        return {"years": sorted(years), "rows": rows, "holdings": len(members)}

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/margin-inputs")
async def margin_inputs(body: FundamentalCoverageRequest):
    """The base inputs behind the FCF-SBC margin, per holding: Revenue, Free Cash Flow and Stock
    Based Compensation per fiscal year, in the company's own reporting currency (millions).

    ⚠ THE RAW LINES, NOT THE RATIO. The margin `(FCF − SBC) / Revenue` is derived on the client
    from these three so the drill-down shows exactly what it is computed from (3 rows per company).
    Deduped by ISIN, weight is the share of the whole book, holdings with no company row omitted.
    """
    from asset_pipeline.isin_alias import canonical_map  # noqa: PLC0415
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

    members = await _load_and_expand_members(body)
    if not members:
        raise HTTPException(status_code=404, detail="no holdings")

    def _run() -> dict:
        total_w = sum(abs(float(m.get("weight") or 0)) for m in members) or 1.0
        raw = sorted({m["isin"] for m in members if m.get("isin")})
        alias = canonical_map(raw)
        weight_by: dict[str, float] = {}
        name_by: dict[str, str] = {}
        for m in members:
            isin = (m.get("isin") or "").strip()
            if not isin:
                continue
            ci = alias.get(isin, isin)
            weight_by[ci] = weight_by.get(ci, 0.0) + abs(float(m.get("weight") or 0))
            name_by.setdefault(ci, m.get("name") or isin)

        canon = sorted(weight_by)
        comp: dict[str, dict] = {}
        for i in range(0, len(canon), IN_CHUNK_SIZE):
            for c in (supabase.table("company")
                      .select("company_id,company_name,isin,gurufocus_ticker,"
                              "gurufocus_exchange:gurufocus_exchange(exchange_code,currency_code)")
                      .in_("isin", canon[i:i + IN_CHUNK_SIZE]).execute().data or []):
                comp[c["isin"]] = c

        rows: list[dict] = []
        years: set[str] = set()
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            rev = _metric_by_year(c["company_id"], "revenue")
            fcf = _metric_by_year(c["company_id"], "fcf")
            sbc = _metric_by_year(c["company_id"], "sbc")
            years |= set(rev) | set(fcf) | set(sbc)
            exch = gx.get("exchange_code")
            subscribed = is_gf_subscribed_exchange(exch) if exch else None
            status = "ok" if (rev or fcf) else ("unsubscribed" if subscribed is False else "no_data")
            rows.append({
                "isin": ci, "name": c.get("company_name") or name_by.get(ci) or ci,
                "weight_pct": round(100.0 * weight_by[ci] / total_w, 2),
                "currency": gx.get("currency_code"),
                "ticker": c.get("gurufocus_ticker"), "exchange": exch,
                "status": status, "revenue": rev, "fcf": fcf, "sbc": sbc,
            })
        rows.sort(key=lambda r: -r["weight_pct"])
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows}

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/debt-ratio-inputs")
async def debt_ratio_inputs(body: FundamentalCoverageRequest):
    """The base inputs behind the LTD / (Total Assets − Goodwill) ratio, per holding: Long-Term
    Debt, Total Assets and Goodwill per fiscal year, in the company's own reporting currency
    (millions).

    ⚠ THE RAW LINES, NOT THE RATIO. `LTD / (Total Assets − Goodwill)` is derived on the client from
    these three so the drill-down shows exactly what it is computed from (3 rows per company). A
    missing Goodwill is a genuine 0 (no acquisitions); a missing Long-Term Debt line is NOT — the
    ratio is blank there (Berkshire has no such line). Deduped by ISIN, weight is the share of the
    whole book, holdings with no company row omitted.
    """
    from asset_pipeline.isin_alias import canonical_map  # noqa: PLC0415
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

    members = await _load_and_expand_members(body)
    if not members:
        raise HTTPException(status_code=404, detail="no holdings")

    def _run() -> dict:
        total_w = sum(abs(float(m.get("weight") or 0)) for m in members) or 1.0
        raw = sorted({m["isin"] for m in members if m.get("isin")})
        alias = canonical_map(raw)
        weight_by: dict[str, float] = {}
        name_by: dict[str, str] = {}
        for m in members:
            isin = (m.get("isin") or "").strip()
            if not isin:
                continue
            ci = alias.get(isin, isin)
            weight_by[ci] = weight_by.get(ci, 0.0) + abs(float(m.get("weight") or 0))
            name_by.setdefault(ci, m.get("name") or isin)

        canon = sorted(weight_by)
        comp: dict[str, dict] = {}
        for i in range(0, len(canon), IN_CHUNK_SIZE):
            for c in (supabase.table("company")
                      .select("company_id,company_name,isin,gurufocus_ticker,"
                              "gurufocus_exchange:gurufocus_exchange(exchange_code,currency_code)")
                      .in_("isin", canon[i:i + IN_CHUNK_SIZE]).execute().data or []):
                comp[c["isin"]] = c

        rows: list[dict] = []
        years: set[str] = set()
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            ltd = _metric_by_year(c["company_id"], "long_term_debt")
            ta = _metric_by_year(c["company_id"], "total_assets")
            gw = _metric_by_year(c["company_id"], "goodwill")
            years |= set(ltd) | set(ta) | set(gw)
            exch = gx.get("exchange_code")
            subscribed = is_gf_subscribed_exchange(exch) if exch else None
            # `ok` if we have Total Assets (the denominator base) OR any debt to show; a company on
            # an unsubscribed exchange can't be fetched at all; else nothing ingested yet.
            status = "ok" if (ta or ltd) else ("unsubscribed" if subscribed is False else "no_data")
            rows.append({
                "isin": ci, "name": c.get("company_name") or name_by.get(ci) or ci,
                "weight_pct": round(100.0 * weight_by[ci] / total_w, 2),
                "currency": gx.get("currency_code"),
                "ticker": c.get("gurufocus_ticker"), "exchange": exch,
                "status": status,
                "long_term_debt": ltd, "total_assets": ta, "goodwill": gw,
            })
        rows.sort(key=lambda r: -r["weight_pct"])
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows}

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/cash-return-inputs")
async def cash_return_inputs(body: FundamentalCoverageRequest):
    """The base inputs behind Cash return on capital, per holding: Free Cash Flow, non-current
    (long-term) liabilities and total equity per fiscal year, in the company's own reporting
    currency (millions).

    ⚠ THE RAW LINES, NOT THE RATIO. `FCF / (non-current liabilities + total equity)` is derived on
    the client from these three so the drill-down shows exactly what it is computed from (3 rows per
    company). Non-current liabilities absent (a bank / Berkshire doesn't split current from
    non-current) → the ratio is blank there, NOT computed against equity alone. Total equity is
    incl. minority interest (see `_METRIC_CODES`). Deduped by ISIN, weight is the share of the whole
    book, holdings with no company row omitted.
    """
    from asset_pipeline.isin_alias import canonical_map  # noqa: PLC0415
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

    members = await _load_and_expand_members(body)
    if not members:
        raise HTTPException(status_code=404, detail="no holdings")

    def _run() -> dict:
        total_w = sum(abs(float(m.get("weight") or 0)) for m in members) or 1.0
        raw = sorted({m["isin"] for m in members if m.get("isin")})
        alias = canonical_map(raw)
        weight_by: dict[str, float] = {}
        name_by: dict[str, str] = {}
        for m in members:
            isin = (m.get("isin") or "").strip()
            if not isin:
                continue
            ci = alias.get(isin, isin)
            weight_by[ci] = weight_by.get(ci, 0.0) + abs(float(m.get("weight") or 0))
            name_by.setdefault(ci, m.get("name") or isin)

        canon = sorted(weight_by)
        comp: dict[str, dict] = {}
        for i in range(0, len(canon), IN_CHUNK_SIZE):
            for c in (supabase.table("company")
                      .select("company_id,company_name,isin,gurufocus_ticker,"
                              "gurufocus_exchange:gurufocus_exchange(exchange_code,currency_code)")
                      .in_("isin", canon[i:i + IN_CHUNK_SIZE]).execute().data or []):
                comp[c["isin"]] = c

        rows: list[dict] = []
        years: set[str] = set()
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            fcf = _metric_by_year(c["company_id"], "fcf")
            ncl = _metric_by_year(c["company_id"], "noncurrent_liabilities")
            eq = _metric_by_year(c["company_id"], "total_equity")
            years |= set(fcf) | set(ncl) | set(eq)
            exch = gx.get("exchange_code")
            subscribed = is_gf_subscribed_exchange(exch) if exch else None
            # `ok` if we have equity (the capital base) OR any FCF to show; a company on an
            # unsubscribed exchange can't be fetched at all; else nothing ingested yet.
            status = "ok" if (eq or fcf) else ("unsubscribed" if subscribed is False else "no_data")
            rows.append({
                "isin": ci, "name": c.get("company_name") or name_by.get(ci) or ci,
                "weight_pct": round(100.0 * weight_by[ci] / total_w, 2),
                "currency": gx.get("currency_code"),
                "ticker": c.get("gurufocus_ticker"), "exchange": exch,
                "status": status,
                "fcf": fcf, "noncurrent_liabilities": ncl, "total_equity": eq,
            })
        rows.sort(key=lambda r: -r["weight_pct"])
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows}

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/interest-burden-inputs")
async def interest_burden_inputs(body: FundamentalCoverageRequest):
    """The base inputs behind the interest-burden ratio, per holding: Interest expense and
    Operating income per fiscal year, in the company's own reporting currency (millions).

    ⚠ THE RAW LINES, NOT THE RATIO. `|Interest expense| / Operating income` (the % of operating
    profit spent servicing debt) is derived on the client from these two so the drill-down shows
    exactly what it is computed from (2 rows per company). Interest expense is reported NEGATIVE (an
    outflow) — the client takes its magnitude; a 0 is real (nets to nothing). Operating income ≤ 0
    → the ratio is blank (a loss). "Operating profit" is `Operating Income`, NOT `EBIT`. Deduped by
    ISIN, weight is the share of the whole book, holdings with no company row omitted.
    """
    from asset_pipeline.isin_alias import canonical_map  # noqa: PLC0415
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

    members = await _load_and_expand_members(body)
    if not members:
        raise HTTPException(status_code=404, detail="no holdings")

    def _run() -> dict:
        total_w = sum(abs(float(m.get("weight") or 0)) for m in members) or 1.0
        raw = sorted({m["isin"] for m in members if m.get("isin")})
        alias = canonical_map(raw)
        weight_by: dict[str, float] = {}
        name_by: dict[str, str] = {}
        for m in members:
            isin = (m.get("isin") or "").strip()
            if not isin:
                continue
            ci = alias.get(isin, isin)
            weight_by[ci] = weight_by.get(ci, 0.0) + abs(float(m.get("weight") or 0))
            name_by.setdefault(ci, m.get("name") or isin)

        canon = sorted(weight_by)
        comp: dict[str, dict] = {}
        for i in range(0, len(canon), IN_CHUNK_SIZE):
            for c in (supabase.table("company")
                      .select("company_id,company_name,isin,gurufocus_ticker,"
                              "gurufocus_exchange:gurufocus_exchange(exchange_code,currency_code)")
                      .in_("isin", canon[i:i + IN_CHUNK_SIZE]).execute().data or []):
                comp[c["isin"]] = c

        rows: list[dict] = []
        years: set[str] = set()
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            ie = _metric_by_year(c["company_id"], "interest_expense")
            oi = _metric_by_year(c["company_id"], "operating_income")
            years |= set(ie) | set(oi)
            exch = gx.get("exchange_code")
            subscribed = is_gf_subscribed_exchange(exch) if exch else None
            # `ok` if we have operating income (the denominator) OR any interest line to show; a
            # company on an unsubscribed exchange can't be fetched at all; else nothing ingested.
            status = "ok" if (oi or ie) else ("unsubscribed" if subscribed is False else "no_data")
            rows.append({
                "isin": ci, "name": c.get("company_name") or name_by.get(ci) or ci,
                "weight_pct": round(100.0 * weight_by[ci] / total_w, 2),
                "currency": gx.get("currency_code"),
                "ticker": c.get("gurufocus_ticker"), "exchange": exch,
                "status": status,
                "interest_expense": ie, "operating_income": oi,
            })
        rows.sort(key=lambda r: -r["weight_pct"])
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows}

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/sbc-ocf-inputs")
async def sbc_ocf_inputs(body: FundamentalCoverageRequest):
    """The base inputs behind the SBC/OCF ratio, per holding: Stock-Based Compensation and
    Operating Cash Flow per fiscal year, in the company's own reporting currency (millions).

    ⚠ THE RAW LINES, NOT THE RATIO. `SBC / Operating Cash Flow` (the share of operating cash flow
    that is non-cash stock comp) is derived on the client from these two so the drill-down shows
    exactly what it is computed from (2 rows per company). SBC is an add-back, reported positive; a
    0 is real (many report none). Operating cash flow ≤ 0 → the ratio is blank (a bank's OCF goes
    negative). Deduped by ISIN, weight is the share of the whole book, holdings with no company row
    omitted.
    """
    from asset_pipeline.isin_alias import canonical_map  # noqa: PLC0415
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

    members = await _load_and_expand_members(body)
    if not members:
        raise HTTPException(status_code=404, detail="no holdings")

    def _run() -> dict:
        total_w = sum(abs(float(m.get("weight") or 0)) for m in members) or 1.0
        raw = sorted({m["isin"] for m in members if m.get("isin")})
        alias = canonical_map(raw)
        weight_by: dict[str, float] = {}
        name_by: dict[str, str] = {}
        for m in members:
            isin = (m.get("isin") or "").strip()
            if not isin:
                continue
            ci = alias.get(isin, isin)
            weight_by[ci] = weight_by.get(ci, 0.0) + abs(float(m.get("weight") or 0))
            name_by.setdefault(ci, m.get("name") or isin)

        canon = sorted(weight_by)
        comp: dict[str, dict] = {}
        for i in range(0, len(canon), IN_CHUNK_SIZE):
            for c in (supabase.table("company")
                      .select("company_id,company_name,isin,gurufocus_ticker,"
                              "gurufocus_exchange:gurufocus_exchange(exchange_code,currency_code)")
                      .in_("isin", canon[i:i + IN_CHUNK_SIZE]).execute().data or []):
                comp[c["isin"]] = c

        rows: list[dict] = []
        years: set[str] = set()
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            sbc = _metric_by_year(c["company_id"], "sbc")
            ocf = _metric_by_year(c["company_id"], "ocf")
            years |= set(sbc) | set(ocf)
            exch = gx.get("exchange_code")
            subscribed = is_gf_subscribed_exchange(exch) if exch else None
            # `ok` if we have operating cash flow (the denominator) OR any SBC to show; a company on
            # an unsubscribed exchange can't be fetched at all; else nothing ingested yet.
            status = "ok" if (ocf or sbc) else ("unsubscribed" if subscribed is False else "no_data")
            rows.append({
                "isin": ci, "name": c.get("company_name") or name_by.get(ci) or ci,
                "weight_pct": round(100.0 * weight_by[ci] / total_w, 2),
                "currency": gx.get("currency_code"),
                "ticker": c.get("gurufocus_ticker"), "exchange": exch,
                "status": status,
                "sbc": sbc, "ocf": ocf,
            })
        rows.sort(key=lambda r: -r["weight_pct"])
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows}

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/capex-margin-inputs")
async def capex_margin_inputs(body: FundamentalCoverageRequest):
    """The base inputs behind the Capex margin, per holding: Capex and Revenue per fiscal year, in
    the company's own reporting currency (millions).

    ⚠ THE RAW LINES, NOT THE RATIO. `|Capex| / Revenue` (capital intensity — the share of sales
    reinvested in capex) is derived on the client from these two so the drill-down shows exactly
    what it is computed from (2 rows per company). Capex is reported NEGATIVE (an outflow) — the
    client takes its magnitude; a 0 is real (capital-light). Revenue ≤ 0 → the ratio is blank.
    Deduped by ISIN, weight is the share of the whole book, holdings with no company row omitted.
    """
    from asset_pipeline.isin_alias import canonical_map  # noqa: PLC0415
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

    members = await _load_and_expand_members(body)
    if not members:
        raise HTTPException(status_code=404, detail="no holdings")

    def _run() -> dict:
        total_w = sum(abs(float(m.get("weight") or 0)) for m in members) or 1.0
        raw = sorted({m["isin"] for m in members if m.get("isin")})
        alias = canonical_map(raw)
        weight_by: dict[str, float] = {}
        name_by: dict[str, str] = {}
        for m in members:
            isin = (m.get("isin") or "").strip()
            if not isin:
                continue
            ci = alias.get(isin, isin)
            weight_by[ci] = weight_by.get(ci, 0.0) + abs(float(m.get("weight") or 0))
            name_by.setdefault(ci, m.get("name") or isin)

        canon = sorted(weight_by)
        comp: dict[str, dict] = {}
        for i in range(0, len(canon), IN_CHUNK_SIZE):
            for c in (supabase.table("company")
                      .select("company_id,company_name,isin,gurufocus_ticker,"
                              "gurufocus_exchange:gurufocus_exchange(exchange_code,currency_code)")
                      .in_("isin", canon[i:i + IN_CHUNK_SIZE]).execute().data or []):
                comp[c["isin"]] = c

        rows: list[dict] = []
        years: set[str] = set()
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            capex = _metric_by_year(c["company_id"], "capex")
            rev = _metric_by_year(c["company_id"], "revenue")
            years |= set(capex) | set(rev)
            exch = gx.get("exchange_code")
            subscribed = is_gf_subscribed_exchange(exch) if exch else None
            # `ok` if we have revenue (the denominator) OR any capex to show; a company on an
            # unsubscribed exchange can't be fetched at all; else nothing ingested yet.
            status = "ok" if (rev or capex) else ("unsubscribed" if subscribed is False else "no_data")
            rows.append({
                "isin": ci, "name": c.get("company_name") or name_by.get(ci) or ci,
                "weight_pct": round(100.0 * weight_by[ci] / total_w, 2),
                "currency": gx.get("currency_code"),
                "ticker": c.get("gurufocus_ticker"), "exchange": exch,
                "status": status,
                "capex": capex, "revenue": rev,
            })
        rows.sort(key=lambda r: -r["weight_pct"])
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows}

    return await asyncio.to_thread(_run)


@router.get("/api/earnings/benchmark-margin")
async def benchmark_margin(label: str = "AEX"):
    """A benchmark index's FCF-SBC margin per fiscal year: `(FCF − SBC) / Revenue` per constituent,
    then a CAP-WEIGHTED AVERAGE across them.

    ⚠ A WEIGHTED AVERAGE OF MARGINS, NOT Σ(FCF−SBC)/ΣRevenue. The constituents report in different
    currencies (Shell $, RELX £, ASML €), so summing their euros/pounds/dollars would be
    meaningless. Each margin is a pure ratio (currency-free), so averaging them — weighted by
    market cap — is the currency-safe aggregate. SBC missing for a constituent is treated as 0
    (many report none). Returns `{label, series:[{year, margin_pct}], members}`.
    """
    def _run() -> dict:
        uni = (supabase.table("universe").select("universe_id")
               .eq("label", label).limit(1).execute().data or [])
        if not uni:
            raise HTTPException(status_code=404, detail=f"No universe labelled {label!r}")
        uid = uni[0]["universe_id"]
        ids = sorted({r["company_id"] for r in
                      (supabase.table("universe_membership").select("company_id")
                       .eq("universe_id", uid).execute().data or []) if r.get("company_id")})
        if not ids:
            raise HTTPException(status_code=404, detail=f"{label} has no members")

        caps: dict[int, float] = {}
        for i in range(0, len(ids), IN_CHUNK_SIZE):
            for c in (supabase.table("company").select("company_id,market_cap_eur")
                      .in_("company_id", ids[i:i + IN_CHUNK_SIZE]).execute().data or []):
                if c.get("market_cap_eur"):
                    caps[c["company_id"]] = float(c["market_cap_eur"])

        per_member: list[tuple[float, dict[str, float]]] = []
        for cid in ids:
            rev = _metric_by_year(cid, "revenue")
            fcf = _metric_by_year(cid, "fcf")
            sbc = _metric_by_year(cid, "sbc")
            marg = {y: (fcf[y] - sbc.get(y, 0.0)) / rev[y] * 100.0
                    for y in rev if rev[y] and rev[y] > 0 and y in fcf}
            if marg:
                per_member.append((caps.get(cid, 1.0), marg))

        years = sorted({y for _w, m in per_member for y in m if y >= "2015"})
        series = []
        for y in years:
            num = sum(w * m[y] for w, m in per_member if y in m)
            den = sum(w for w, m in per_member if y in m)
            if den > 0:
                series.append({"year": int(y), "margin_pct": round(num / den, 4)})
        return {"label": label, "members": len(per_member), "series": series}

    return await asyncio.to_thread(_run)


class RelativeGrowthRequest(FundamentalCoverageRequest):
    """One year of the Share-Price-vs-Owner-Earnings chart, decomposed per holding."""

    period: str


@router.post("/api/earnings/relative-growth-breakdown")
async def relative_growth_breakdown(body: RelativeGrowthRequest):
    """The holdings behind ONE year of the Share-Price-vs-Owner-Earnings chart: each holding's
    price-growth index, its Owner-Earnings-growth index, and price ÷ OE (its multiple change).

    ⚠ BOTH LINES ARE DECOMPOSED THROUGH THE SAME LEVEL `blend_breakdown` THE CHART IS BUILT FROM —
    price and OE are month-end price and EPS-ex-NRI, both LEVELS, rebased to an index and weighted.
    Merging the two per holding (`merge_relative_growth`) gives the price-vs-OE table without a
    second copy of the growth rules.
    """
    from routers._fundamental_blend import (  # noqa: PLC0415
        blend_breakdown,
        merge_relative_growth,
    )
    from routers._fundamental_coverage import coverage_for_async  # noqa: PLC0415

    members = await _load_and_expand_members(body)
    if not members:
        raise HTTPException(status_code=404, detail="no holdings to blend")

    cov = await coverage_for_async(members)
    covered = [r for r in cov["rows"] if r["reason"] == "covered" and r.get("company_id")]
    if not covered:
        raise HTTPException(status_code=404, detail="no holding has fundamentals to blend")

    period = body.period[:4]

    def _build() -> dict:
        pts: dict[str, dict[int, dict[str, float]]] = {_RG_PRICE_CODE: {}, _RG_OE_CODE: {}}
        for r in covered:
            cid = r["company_id"]
            for code in (_RG_PRICE_CODE, _RG_OE_CODE):
                pts[code][cid] = {str(m["target_date"])[:10]: float(m["numeric_value"])
                                  for m in _page_metrics(cid, code, exact=True)
                                  if m.get("numeric_value") is not None}

        def _members(code: str) -> list[dict]:
            return [{"isin": r.get("isin"), "name": r.get("name"), "weight": r["weight_pct"],
                     "points": pts[code].get(r["company_id"], {})} for r in covered]

        return merge_relative_growth(
            blend_breakdown(_members(_RG_PRICE_CODE), _RG_PRICE_CODE, period),
            blend_breakdown(_members(_RG_OE_CODE), _RG_OE_CODE, period),
            period,
        )

    out = await asyncio.to_thread(_build)
    out["blend_covered_pct"] = cov["covered_pct"]
    return out


class FundamentalMatrixRequest(FundamentalCoverageRequest):
    """The whole blended line taken apart: which metric (every period, every holding)."""

    metric_code: str


@router.post("/api/earnings/fundamental-blend-matrix")
async def fundamental_blend_matrix(body: FundamentalMatrixRequest):
    """The audit grid behind a blended line: every holding's value at every period, plus the
    blended value + coverage per period.

    ⚠ SAME LOADER AND SAME `_prepare` AS THE LINE AND THE PER-POINT DRILL-DOWN. It reads ONE
    metric's rows per covered holding (+ the actual a forecast is anchored on) and hands them to
    `blend_matrix`, so the grid a reader verifies against is built from exactly what the chart
    drew — there is no second computation to disagree with.
    """
    from routers._fundamental_blend import blend_matrix  # noqa: PLC0415
    from routers._fundamental_coverage import coverage_for_async  # noqa: PLC0415

    members = await _load_and_expand_members(body)
    if not members:
        raise HTTPException(status_code=404, detail="no holdings to blend")

    cov = await coverage_for_async(members)
    covered = [r for r in cov["rows"] if r["reason"] == "covered" and r.get("company_id")]
    if not covered:
        raise HTTPException(status_code=404, detail="no holding has fundamentals to blend")

    code = body.metric_code
    base_code = _FORECAST_BASE.get(code)

    def _build() -> dict:
        per_company: dict[int, dict[str, float]] = {}
        base_by_company: dict[int, dict[str, float]] = {}
        for r in covered:
            cid = r["company_id"]
            for want, sink in ((code, per_company), (base_code, base_by_company)):
                if not want:
                    continue
                sink[cid] = {str(m["target_date"])[:10]: float(m["numeric_value"])
                             for m in _page_metrics(cid, want, exact=True)
                             if m.get("numeric_value") is not None}
        return blend_matrix(
            [{"isin": r.get("isin"), "name": r.get("name"), "weight": r["weight_pct"],
              "points": per_company.get(r["company_id"], {}),
              "base_points": base_by_company.get(r["company_id"], {})}
             for r in covered], code)

    out = await asyncio.to_thread(_build)
    out["blend_covered_pct"] = cov["covered_pct"]
    return out
