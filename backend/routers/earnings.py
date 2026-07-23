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

from deps import supabase
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


@router.post("/api/earnings/fundamental-coverage")
async def fundamental_coverage(body: FundamentalCoverageRequest):
    """Which of a portfolio's holdings a fundamentals view can reach, BY WEIGHT, and why not.

    ⚠ COVERAGE IS THE FIRST ANSWER, NOT A FOOTNOTE. Every holding that cannot be reached is weight
    that drops out of any blend, and a blended figure over 61% of a book presented as the book's is
    the same fabrication `MIN_COVERAGE_PCT` already guards against on the AIRS returns.
    """
    from routers._fundamental_coverage import coverage_for_async  # noqa: PLC0415

    members = body.holdings
    if members is None:
        if body.portfolio_id is None:
            raise HTTPException(status_code=422, detail="portfolio_id or holdings is required")

        def _load() -> list[dict]:
            return [{"isin": p.get("isin"), "name": p.get("fonds"),
                     "weight": float(p.get("percentage") or 0)}
                    for p in (supabase.table("airs_model_portfolio_position")
                              .select("fonds,isin,percentage")
                              .eq("portfolio_id", body.portfolio_id)
                              .limit(500).execute().data or [])]

        members = await asyncio.to_thread(_load)
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

    members = body.holdings
    if members is None:
        if body.portfolio_id is None:
            raise HTTPException(status_code=422, detail="portfolio_id or holdings is required")

        def _load() -> list[dict]:
            return [{"isin": p.get("isin"), "name": p.get("fonds"),
                     "weight": float(p.get("percentage") or 0)}
                    for p in (supabase.table("airs_model_portfolio_position")
                              .select("fonds,isin,percentage")
                              .eq("portfolio_id", body.portfolio_id)
                              .limit(500).execute().data or [])]

        members = await asyncio.to_thread(_load)
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

    members = body.holdings
    if members is None:
        if body.portfolio_id is None:
            raise HTTPException(status_code=422, detail="portfolio_id or holdings is required")

        def _load() -> list[dict]:
            return [{"isin": p.get("isin"), "name": p.get("fonds"),
                     "weight": float(p.get("percentage") or 0)}
                    for p in (supabase.table("airs_model_portfolio_position")
                              .select("fonds,isin,percentage")
                              .eq("portfolio_id", body.portfolio_id)
                              .limit(500).execute().data or [])]

        members = await asyncio.to_thread(_load)
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

    members = body.holdings
    if members is None:
        if body.portfolio_id is None:
            raise HTTPException(status_code=422, detail="portfolio_id or holdings is required")

        def _load() -> list[dict]:
            return [{"isin": p.get("isin"), "name": p.get("fonds"),
                     "weight": float(p.get("percentage") or 0)}
                    for p in (supabase.table("airs_model_portfolio_position")
                              .select("fonds,isin,percentage")
                              .eq("portfolio_id", body.portfolio_id)
                              .limit(500).execute().data or [])]

        members = await asyncio.to_thread(_load)
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
