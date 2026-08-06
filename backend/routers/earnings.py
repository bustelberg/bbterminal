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
import contextvars
import logging
from collections import defaultdict
from routers._blend_cache import cached_blend
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
async def get_earnings_metrics_by_isin(isin: str, cadence: str = "annual"):
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
        # `cadence="quarterly"` returns TRAILING-TWELVE-MONTH points under the same metric codes —
        # see `_ttm_metric_rows`. The dashboard and the Long Equity tab share this endpoint, so the
        # default stays annual and only a caller that asks gets the rolled-up view.
        loader = _ttm_metric_rows if cadence == "quarterly" else load_company_metric_rows
        rows = await asyncio.to_thread(loader, info["company_id"])
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
    # ⚠ ON THE SHARED REQUEST, SO EVERY CARD ON THE TAB MOVES TOGETHER. All eleven `*-inputs`
    # endpoints take this model and read their lines through `_metric_by_year`, so one field here
    # is the whole cadence switch — and it is impossible for one card to be showing fiscal years
    # while the card beside it shows trailing twelve months. "quarterly" means TTM, not raw
    # quarters: see `_ttm_by_period` for what each metric's roll-up is and why.
    cadence: str = "annual"
    # ⚠ A BENCHMARK LABEL ("SP500", "ACWI", "AEX") INSTEAD OF a book. Set it and every `*-inputs`
    # endpoint returns that index's cap-weighted constituents in the identical shape, so the client
    # draws the benchmark line with the same helper it uses for the portfolio. Takes precedence
    # over `holdings`/`portfolio_id` — a request is one or the other, never a blend of both.
    universe: str | None = None
    # ⚠ NARROWS THE BLEND TO THESE METRICS, AND IT IS WHAT MAKES A BENCHMARK BLEND POSSIBLE AT ALL.
    # `/fundamental-blend-metrics` normally reads EVERY charted code per company — three paged
    # requests each, which is fine for a 40-name book and is ~1,500 round trips for the S&P 500.
    # Named here (`_METRIC_CODES` keys: "revenue", "fcf_ps", "shares"), the read becomes one
    # chunked, paged query per metric across all constituents — the same 400x that
    # `_metrics_by_company` documents. Omitted = every code, i.e. the behaviour a book still gets.
    metrics: list[str] | None = None


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

    # ⚠ A BENCHMARK IS JUST ANOTHER MEMBER LIST, AND THAT IS THE WHOLE DESIGN. Asking for a
    # universe here makes every `*-inputs` endpoint return the index's constituents instead of a
    # book's holdings — same shape, same columns, same coverage floor — so the CLIENT computes the
    # benchmark line with the identical helper it already runs over the portfolio. There is no
    # second implementation of "FCF-SBC margin" to drift, which is the only way a benchmark line
    # and the line it is drawn beside can be guaranteed to mean the same thing.
    #
    # ⚠ CAP-WEIGHTED, because that is what the index IS. `_members` is the same deduped one-row-
    # per-company list the /benchmarks panel uses (GOOGL+GOOG would otherwise count Alphabet's cap
    # twice, 11.3% of the S&P, fictional).
    #
    # ⚠ NO LOOK-THROUGH. An index constituent is a company, never a Leonteq certificate wrapping a
    # model — running the expansion over 500 names would be work with nothing to find.
    if body.universe:
        from routers._benchmark_index import _members  # noqa: PLC0415

        def _load_universe() -> list[dict]:
            return [{"isin": m["isin"], "name": m.get("company_name"),
                     "weight": float(m.get("market_cap_eur") or 0)}
                    for m in _members(body.universe or "")
                    if m.get("isin") and (m.get("market_cap_eur") or 0) > 0]

        return await asyncio.to_thread(_load_universe)

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
@cached_blend("fundamental-coverage")
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
@cached_blend("fundamental-blend")
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


def _company_metric_rows(cid: int) -> list[dict]:
    """Every charted metric row for ONE company — the unit of work the blend is slow in.

    ⚠ Paged per company, never one wildcard over the lot: a company carries ~110 codes x ~28 years,
    so twenty at once is ~60k rows against PostgREST's silent 1,000-row cap and the tail would come
    back looking like companies with no data.

    ⚠ THREE PATTERNS, THREE FEEDS. A statement line is `annuals__Section__Line`; an analyst estimate
    is `annual_pettm_estimate` (SINGULAR, no section, no double underscore); and Forward P/E is
    `indicator_q_forward_pe_ratio` — an INDICATOR, a third naming scheme again. Miss one and its
    panels read "no data" while the rest of the suite fills in around them.

    ⚠ AND NOT AN UNFILTERED FETCH EITHER. `close_price` alone is ~10k rows per company — a daily
    series no blended fiscal-year chart uses, and enough to blow the cap on its own.

    Raw strings: `_` escapes LIKE's single-char wildcard, and a plain "_" in a normal Python string
    is an invalid escape sequence (a SyntaxWarning now, an error later).
    """
    rows: list[dict] = []
    for pattern in (r"annuals__%", r"annual_%estimate", r"indicator%"):
        rows += _page_metrics(cid, pattern)
    return rows


def _bulk_blend_rows(cids: list[int], metrics: list[str], cadence: str) -> list[dict]:
    """The named metrics' rows for MANY companies, in `_blend_rows`' shape.

    The bulk twin of `_company_metric_rows` / `_ttm_metric_rows`, and it exists for one reason:
    those read per company, which is three paged requests each — fine for a 40-name book, ~1,500
    round trips for the S&P 500, which is what a benchmark overlay asks for. Here it is one
    chunked, paged query per metric.

    ⚠ TTM ROWS ARE EMITTED UNDER THE **ANNUAL** CODE, exactly as `_ttm_metric_rows` does — the
    charts select their line by `annuals__…` and the cadence is a property of the request, not of
    the row. Emit the `quarterly__` spelling here and every card would go blank on the benchmark
    while its own line kept drawing, which reads as "the index has no data".
    """
    out: list[dict] = []
    for m in metrics:
        codes, rule = _codes_and_rule(m, cadence)
        if codes is None:
            continue
        raw = _rows_by_company(cids, codes)
        if rule is None:
            for rows in raw.values():
                out += rows
            continue
        annual_code = _metric_codes(m)[0]
        for cid, rows in raw.items():
            for date, val in _ttm_by_period(rows, rule, key="date").items():
                out.append({"company_id": cid, "metric_code": annual_code,
                            "target_date": date, "numeric_value": val})
    return out


def _blend_rows(rows: list[dict], covered: list[dict]) -> dict:
    """The blend itself, over rows already fetched. Pure of I/O, so the plain endpoint and the
    streaming one cannot drift: they differ only in HOW the rows arrive."""
    from routers._fundamental_blend import blend_series, explain_empty  # noqa: PLC0415

    by_metric: dict[str, dict[int, dict[str, float]]] = {}
    for r in rows:
        if r.get("numeric_value") is None:
            continue
        (by_metric.setdefault(r["metric_code"], {})
                  .setdefault(r["company_id"], {}))[str(r["target_date"])[:10]] = \
            float(r["numeric_value"])

    out: list[dict] = []
    notes: dict[str, dict] = {}
    for code, per_company in by_metric.items():
        # ⚠ A FORECAST INHERITS THE ANCHOR OF THE ACTUAL IT CONTINUES. Both are the same quantity
        # and the chart indexes them off ONE base, so rebasing them independently draws the forecast
        # restarting at 100 beside an actual that has run to 1,808 — a ~94% earnings collapse that
        # exists only in the arithmetic.
        base_code = _FORECAST_BASE.get(code)
        base_by_company = by_metric.get(base_code, {}) if base_code else {}
        blend_members = [{"weight": r["weight_pct"],
                          "points": per_company.get(r["company_id"], {}),
                          "base_points": base_by_company.get(r["company_id"], {})}
                         for r in covered]
        s = blend_series(blend_members, code)
        if not s["points"]:
            # ⚠ AN EMPTY SERIES IS NOT AN EMPTY DATABASE, AND THE CHART CANNOT TELL. Measured on a
            # real book's Dividends per Share: every holding carried the line and the card still
            # read "No dividend/share ingested" — the level rebase drops a series that starts at
            # 0.00. The reason travels with the (absent) series so the card states it instead of
            # sending the reader to re-ingest data they already have.
            why = explain_empty(blend_members, code)
            if why is not None:
                notes[code] = why
        for p in s["points"]:
            # The fiscal-year key becomes a date the charts can plot. 31 Dec is a convention here,
            # not a claim about anyone's year-end — members close on different days, which is
            # exactly why the blend aligns on the year in the first place.
            out.append({"metric_code": code, "target_date": f"{p['period']}-12-31",
                        "numeric_value": p["value"], "is_prediction": False})
    return {"metrics": out, "codes": len(by_metric), "blend_notes": notes}


def _blend_envelope(built: dict, covered: list[dict], cov: dict) -> dict:
    """The response both endpoints return — one shape, written once."""
    return {
        "company_id": None,
        "company_name": f"{len(covered)} holdings · {cov['covered_pct']:.0f}% of weight",
        "currency": None,
        "metrics": built["metrics"],
        # Per metric_code, why a code with member data produced no line. Only codes that drew
        # nothing appear; a code nobody reports is absent (that one IS "not ingested").
        "blend_notes": built["blend_notes"],
        "coverage": cov,
    }


async def _blend_inputs(body: FundamentalCoverageRequest) -> tuple[list[dict], dict]:
    """The covered holdings and the coverage report, or the 404 that says why there are none."""
    from routers._fundamental_coverage import coverage_for_async  # noqa: PLC0415

    members = await _load_and_expand_members(body)
    if not members:
        raise HTTPException(status_code=404, detail="no holdings to blend")
    cov = await coverage_for_async(members)
    covered = [r for r in cov["rows"] if r["reason"] == "covered" and r.get("company_id")]
    if not covered:
        raise HTTPException(status_code=404, detail="no holding has fundamentals to blend")
    return covered, cov


@router.post("/api/earnings/fundamental-blend-metrics")
@cached_blend("fundamental-blend-metrics")
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
    covered, cov = await _blend_inputs(body)

    def _build() -> dict:
        if body.metrics:
            # ⚠ SAME BLEND, DIFFERENT READ. Only the fetch changes — `_blend_rows` is untouched, so
            # a narrowed request cannot blend by a different rule than a full one.
            rows = _bulk_blend_rows([r["company_id"] for r in covered], body.metrics, body.cadence)
            return _blend_rows(rows, covered)
        # The PORTFOLIO path takes the same cadence as the single-company one, through the same
        # roll-up — otherwise a book's Long Equity tab would ignore a toggle its holdings honour.
        load = (_ttm_metric_rows if body.cadence == "quarterly" else _company_metric_rows)
        rows = []
        for r in covered:
            rows += load(r["company_id"])
        return _blend_rows(rows, covered)

    return _blend_envelope(await asyncio.to_thread(_build), covered, cov)


async def _blend_metrics_events(body: FundamentalCoverageRequest):
    """Per-COMPANY progress, then the same payload the plain endpoint returns.

    ⚠ THE UNIT OF PROGRESS IS THE UNIT OF WORK. The blend's cost is three paged reads per holding
    (`_company_metric_rows`) — a 40-name book is 120 round trips — and everything after them is
    arithmetic. So "3 of 40 companies" is a real fraction of the wait, not a guess dressed as one.
    A spinner over that says only "still going", which on a minute-long open reads as broken.

    ⚠ THE ERROR ARRIVES AS AN EVENT, NOT AS A STATUS. The stream's headers are long since sent by
    the time a holding fails, so a raised `HTTPException` here cannot become a 404 the client can
    read — it would truncate the body and surface as a network error. The two real 404s are raised
    before the first byte, but a fault mid-loop has nowhere else to go.
    """
    from routers._asset_financials import _sse  # noqa: PLC0415

    covered, cov = await _blend_inputs(body)     # before the first byte: a real 404 is still a 404
    n = len(covered)
    yield _sse({"type": "progress", "done": 0, "total": n})
    rows: list[dict] = []
    try:
        for i, r in enumerate(covered):
            rows += await asyncio.to_thread(_company_metric_rows, r["company_id"])
            yield _sse({"type": "progress", "done": i + 1, "total": n, "name": r.get("name")})
        built = await asyncio.to_thread(_blend_rows, rows, covered)
    except Exception as e:  # noqa: BLE001 — see the docstring: there is no status code left to use
        yield _sse({"type": "error", "detail": f"{type(e).__name__}: {e}"})
        return
    yield _sse({"type": "result", "payload": _blend_envelope(built, covered, cov)})


@router.post("/api/earnings/fundamental-blend-metrics/stream")
async def fundamental_blend_metrics_stream(body: FundamentalCoverageRequest):
    """SSE twin of `/fundamental-blend-metrics`: `{type:'progress',done,total,name}` per holding,
    then `{type:'result',payload:<the identical response>}`.

    It exists because opening the Fundamental modal on a whole portfolio is a per-company read and
    the modal could only say "Loading…" — which does not distinguish a 40-name book from a hung
    request. Same inputs, same blend, same envelope; only the arrival is different.
    """
    from fastapi.responses import StreamingResponse  # noqa: PLC0415

    return StreamingResponse(_blend_metrics_events(body), media_type="text/event-stream")


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
    # Behind the Gross-margin card: Gross Profit ÷ Revenue.
    # ⚠ A BANK HAS NO GROSS PROFIT LINE AT ALL — GuruFocus's industry template 'B' reports Interest
    # Income / Net Interest Income and has no cost of goods sold, so the key is simply absent
    # (JPMorgan). That is an ANSWER — the concept does not apply — not a data gap, and the card must
    # render it blank rather than 0. Same distinction `_asset_financials._has_line` draws.
    # ⚠ DERIVED, THOUGH GURUFOCUS ALSO PUBLISHES `Ratios__Gross Margin %`. Deriving costs nothing
    # extra (Revenue is already fetched for three other cards), reproduces their figure EXACTLY
    # (ASML 2025: 17,258/32,667.3 = 52.83% vs published 52.83; Apple 46.91 vs 46.905), and — unlike
    # the published ratio — leaves two lines the drill-down can show, so the number can be checked.
    "gross_profit": ("annuals__Income Statement__Gross Profit",
                     "annuals__income_statement__Gross Profit"),
    # Behind the Cash-conversion card: FCF ÷ Net Income (`fcf` above is the numerator).
    # ⚠ THE SHAREHOLDERS' LINE, NOT `Net Income Including Noncontrolling Interests`. Same choice
    # `_asset_financials._ITEMS` makes, and the same trap: the two are IDENTICAL for a company with
    # no minorities (ASML 9,609.4 = 9,609.4; Apple 112,010 = 112,010), so validating on either of
    # those blesses whichever you picked. Mitsui is where they part — 34,378 vs 46,910.
    # ⚠ SO THE RATIO IS SLIGHTLY SCOPE-MISMATCHED BY CONSTRUCTION, and that is a deliberate,
    # documented choice rather than an oversight: FCF is whole-company cash (before anything is
    # paid away to minorities) while this denominator is the parent's share. For a group with large
    # minorities the conversion therefore reads HIGH. The alternative mismatches EPS and every other
    # card on the tab, which would be the worse inconsistency.
    "net_income": ("annuals__Income Statement__Net Income",
                   "annuals__income_statement__Net Income"),
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
    # GuruFocus's OWN ROIC, the alternative mode on the cash-return card. ⚠ READ, NEVER DERIVED.
    # Computing it here would mean choosing a NOPAT numerator (EBIT or Operating Income — different
    # lines: Mitsui 85,035 vs 56,602) and an invested-capital base, i.e. shipping a bespoke ratio
    # under a name every reader already has a definition for. GuruFocus applies one definition to
    # every company, so a cross-company comparison is a comparison. 28 years back for ASML, beside
    # WACC on the same section — ROIC − WACC being the question ROIC exists to answer.
    # ⚠ IT IS A PERCENTAGE. It must never reach the `_ITEMS` FX path, which divides by a rate and
    # would report "23.5% in EUR".
    "roic": ("annuals__Ratios__ROIC %", "annuals__ratios__ROIC %"),
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
    # Dividends per share (per-share currency level; CAGR = the dividend-growth rate). ⚠ THREE
    # per-share section spellings: capitalized cohort `Per Share Data`, lowercase cohort
    # `per_share_data_array` (NOT `per_share_data` — that's the shares-outstanding trap again).
    "div_ps": ("annuals__Per Share Data__Dividends per Share",
               "annuals__per_share_data__Dividends per Share",
               "annuals__per_share_data_array__Dividends per Share"),
    # Behind the dividend-yield card: DPS ÷ the fiscal year-end share price (`div_ps` is the
    # numerator). ⚠ THE PRICE IS WHAT MAKES A DIVIDEND REPORTABLE FOR A PORTFOLIO AT ALL. Dividends
    # per share cannot be aggregated — there is no portfolio share, the amounts are in different
    # currencies, and a level series that starts at 0.00 cannot be rebased to a growth index (which
    # is what left the portfolio card permanently empty). DPS ÷ price is currency-free, so the
    # weighted average IS the portfolio's yield (the weights are value weights), and a non-payer
    # contributes a true 0 instead of being dropped. Same three per-share spellings as `div_ps`.
    "price_ps": ("annuals__Per Share Data__Month End Stock Price",
                 "annuals__per_share_data__Month End Stock Price",
                 "annuals__per_share_data_array__Month End Stock Price"),
    # Behind the FCF-SBC yield card: (FCF − SBC) ÷ Market Cap (`fcf`/`sbc` above are the numerator).
    # Market cap is the fiscal-year-end figure GuruFocus files under Valuation and Quality.
    "market_cap": ("annuals__Valuation and Quality__Market Cap",
                   "annuals__valuation_and_quality__Market Cap"),
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


# ⚠ HOW EACH METRIC ROLLS UP TO TRAILING TWELVE MONTHS — DECLARED, NEVER INFERRED, BECAUSE THE
# WRONG RULE PRODUCES A PLAUSIBLE NUMBER RATHER THAN AN ERROR.
#
#   sum   a FLOW measured over the period. Four quarters of revenue ARE a year of revenue.
#   last  a STOCK measured at an instant. Total assets is a balance, not a flow — summing four
#         quarter-end balance sheets reports a company with 4x its assets, and nothing about the
#         resulting chart looks wrong.
#   mean  a figure that is ALREADY a per-period rate or average, where the trailing-twelve-month
#         view is the average of the four. Two kinds land here:
#           * `shares` is "Shares Outstanding (Diluted Average)" — already an average OVER the
#             quarter, so the TTM figure is the mean of the four, not their sum (which would
#             quadruple the share count and quarter every per-share metric built on it).
#           * `roic` is a RATIO, and GuruFocus publishes its quarterly one ALREADY ANNUALISED.
#             Measured on ASML: quarterly 30.88 / 26.91 / 27.76 / 20.93 against annual 24.67 and
#             26.95 — the same magnitude. A quarterly RATE would read ~6-7%. So four of them
#             summed is ~4x, and the honest trailing figure is their mean.
#
# A metric absent from this map is refused rather than guessed — see `_ttm_by_period`.
_log = logging.getLogger(__name__)

_TTM_RULE: dict[str, str] = {
    # Flows — income statement and cash flow.
    "revenue": "sum", "gross_profit": "sum", "operating_income": "sum", "net_income": "sum",
    "fcf": "sum", "ocf": "sum", "sbc": "sum", "capex": "sum", "interest_expense": "sum",
    "fcf_ps": "sum", "div_ps": "sum",
    # Balances and market values — a point in time.
    "total_assets": "last", "total_equity": "last", "goodwill": "last",
    "long_term_debt": "last", "noncurrent_liabilities": "last",
    "market_cap": "last", "price_ps": "last",
    # Already an average / an annualised rate.
    "shares": "mean", "roic": "mean",
}


def _daily_closes(company_id: int, since: str = "2015-01-01") -> dict[str, float]:
    """{date: close} — GuruFocus's own daily close for one company.

    ⚠ THIS SERIES, NOT yfinance's `asset_price`, AND THE REASON IS CURRENCY. The Long Equity tab
    lives in the `company` world and its per-share lines are in the company's REPORTING currency;
    `asset_price` lives in the `asset_execution` world, reachable only by ISIN, and that bridge
    carries every wrong-listing hazard this repo documents — a US megacap priced on a thin German
    line, or `GBp` pence against fundamentals in `GBP`, which is a 100x error that still looks like
    a number. This series needs no bridge and no conversion: measured on ASML, GuruFocus's annual
    `Month End Stock Price` IS a sample of it (681.7 / 678.7 / 921.4 at the last three year-ends,
    ratio 1.0000), so swapping the annual point for the daily one changes the frequency and
    nothing else.
    """
    return {str(r["target_date"])[:10]: float(r["numeric_value"])
            for r in _page_metrics(company_id, "close_price", exact=True)
            if r.get("numeric_value") is not None and str(r["target_date"])[:10] >= since}


def _step_onto_dates(ttm: dict[str, float], dates: list[str]) -> dict[str, float]:
    """A quarterly TTM series carried across daily dates — the value stays flat until the next
    fiscal period end, which is what makes a daily yield a yield rather than an interpolation.

    ⚠ NOTHING BEFORE THE FIRST FISCAL PERIOD. A date earlier than any reported period gets NO
    value rather than the first one carried backwards: back-filling would draw a company's current
    profitability across years it had not reported, and the line would look like history.

    ⚠ KNOWN LIMITATION, STATED RATHER THAN HIDDEN: the step moves on the fiscal period END, not on
    the PUBLICATION date, because GuruFocus gives us no publication date. A Q1 figure therefore
    appears on the chart some weeks before the market could have known it. That is the ordinary
    construction for a trailing-yield chart and it is fine for reading history; it is NOT safe as a
    backtest signal, where it would be look-ahead.
    """
    ends = sorted(ttm)
    out: dict[str, float] = {}
    i = -1
    for d in dates:                       # both sides sorted → one pass, no bisect per date
        while i + 1 < len(ends) and ends[i + 1] <= d:
            i += 1
        if i >= 0:
            out[d] = ttm[ends[i]]
    return out


def _daily_metric(company_id: int, metric: str, dates: list[str]) -> dict[str, float]:
    """A metric's TTM value carried onto `dates` — the numerator of a daily yield."""
    rule = _TTM_RULE.get(metric)
    if rule is None:
        return {}
    rows: list[dict] = []
    for code in (c.replace("annuals__", "quarterly__") for c in _metric_codes(metric)):
        rows += _page_metrics(company_id, code, exact=True)
    return _step_onto_dates(_ttm_by_period(rows, rule, key="date"), dates)


def _ttm_metric_rows(company_id: int) -> list[dict]:
    """The metric ROWS a growth card reads, rolled to trailing twelve months.

    ⚠ EMITTED UNDER THE ANNUAL CODE NAME, ON PURPOSE. The three growth cards select their line by
    `metric_code` (`annuals__Income Statement__Revenue`), and the /earnings dashboard reads the
    same payload — so returning `quarterly__…` codes would mean every consumer learning a second
    set of names and choosing between them. The cadence is a property of the REQUEST, not of the
    row: ask for quarterly and the same code carries TTM values at quarter-end dates.
    """
    out: list[dict] = []
    for metric, rule in _TTM_RULE.items():
        # ⚠ EVERY METRIC WITH A DECLARED ROLL-UP, not a filtered subset. An earlier version gated
        # on `_LONGEQUITY_METRIC_CODES` — which is NINE share-price CAGR codes, not the financial
        # lines — and returned an empty series for every card while looking perfectly reasonable.
        # The consumer picks the code it wants; there is nothing to gain by guessing here.
        code = _metric_codes(metric)[0]
        qcodes = tuple(c.replace("annuals__", "quarterly__") for c in _metric_codes(metric))
        rows: list[dict] = []
        for qc in qcodes:
            rows += _page_metrics(company_id, qc, exact=True)
        for date, val in _ttm_by_period(rows, rule, key="date").items():
            # ⚠ `company_id` IS NOT DECORATION HERE. These rows also feed `_blend_rows`, which keys
            # every point by the company that reported it — without it a PORTFOLIO's growth cards
            # raised KeyError on `quarterly` (a 500 the moment a book switched cadence) while the
            # single-company path, which never blends, was fine. The synthesised rows have to
            # carry what the read they replace carried.
            out.append({"company_id": company_id, "metric_code": code, "target_date": date,
                        "numeric_value": val, "is_prediction": False})
    return out


def _ttm_by_period(rows: list[dict], rule: str, key: str = "label") -> dict[str, float]:
    """Quarterly rows → {period label: trailing-twelve-month value}, per `rule`.

    ⚠ A POINT NEEDS FOUR QUARTERS OR IT IS NOT A TRAILING YEAR. The first three quarters of a
    company's history produce no TTM point at all — emitting a partial one would draw a line that
    starts at a quarter of the level and "grows" 4x over its first year, which reads as the
    business quadrupling. `last` is the one rule that could tolerate a short window (a balance is
    a balance), but it is held to the same bar so every series on the tab starts at the same
    place; a debt ratio whose numerator begins three quarters before its denominator is worse
    than one that starts late.
    """
    by_date: dict[str, float] = {}
    for m in rows:
        v = m.get("numeric_value")
        if v is None:
            continue
        # Latest observation wins for a given quarter-end — same rule as the annual path.
        by_date[str(m["target_date"])[:10]] = float(v)
    dates = sorted(by_date)
    out: dict[str, float] = {}
    for i in range(3, len(dates)):
        window = [by_date[d] for d in dates[i - 3:i + 1]]
        if rule == "sum":
            val = sum(window)
        elif rule == "mean":
            val = sum(window) / 4.0
        else:                                    # "last"
            val = window[-1]
        # Labelled by the quarter the window ENDS in — the period the figure is as-of. `key="date"`
        # keeps the REAL quarter-end instead, because a fiscal quarter need not end on a calendar
        # one and synthesising 03-31/06-30/09-30/12-31 would move every point of an off-calendar
        # filer.
        d = dates[i]
        out[d if key == "date" else f"{d[:4]}-Q{(int(d[5:7]) - 1) // 3 + 1}"] = val
    return out


# ⚠ A REQUEST-SCOPED PREFETCH, AND IT EXISTS BECAUSE THE SAME LOOP COSTS 50x AT INDEX SCALE.
# `_metric_by_year` is one paged read per company per metric. Over a book's ~50 holdings that is
# fine; over an index's 489 constituents — which is exactly what a benchmark line asks for — it is
# 72 SECONDS for one card, measured, against 0.1 s for the same rows fetched in bulk.
#
# Rather than rewrite eleven endpoint loops, they call `_prefetch(ids, metrics, cadence)` once and
# `_metric_by_year` reads the cache. A ContextVar, not a module global: two requests can be in
# flight in the same process and must never see each other's companies.
_PREFETCH: contextvars.ContextVar[dict[tuple[str, str], dict[int, dict[str, float]]] | None] = (
    contextvars.ContextVar("_earnings_prefetch", default=None))


def _prefetch(company_ids: list[int], metrics: tuple[str, ...], cadence: str = "annual") -> None:
    """Load these metrics for these companies in one bulk read each, for the rest of the request.

    ⚠ SILENTLY OPTIONAL. Anything not prefetched still resolves through the per-company path, so a
    caller that forgets is slow rather than wrong — and a metric fetched here that nobody asks for
    costs one query, not a wrong answer.
    """
    if not company_ids:
        return
    cache = _PREFETCH.get() or {}
    for m in metrics:
        key = (m, cadence)
        if key not in cache:
            cache[key] = _metrics_by_company(company_ids, m, cadence)
    _PREFETCH.set(cache)


def _metric_by_year(company_id: int, metric: str, cadence: str = "annual") -> dict[str, float]:
    """{period: value} for a metric — fiscal YEARS, or trailing-twelve-month points per quarter.

    ⚠ ONE SEAM FOR TWELVE CARDS. Every `*-inputs` endpoint on the Long Equity tab reads its lines
    through this function, so the cadence is honoured in one place and no card can end up plotting
    a different basis from the one beside it.

    ⚠ THE KEYS CHANGE SHAPE ("2025" → "2025-Q3") AND CALLERS MUST NOT PARSE THEM. They are period
    LABELS, ordered lexically (which is why the quarter suffix works), and every consumer treats
    them as opaque categories on an x-axis. A caller that slices `[:4]` for a year would silently
    collapse four TTM points onto one.
    """
    # The request-scoped bulk cache, when the caller filled it — see `_prefetch`.
    cached = (_PREFETCH.get() or {}).get((metric, cadence))
    if cached is not None:
        return cached.get(company_id, {})
    if cadence == "quarterly":
        rule = _TTM_RULE.get(metric)
        if rule is None:
            # Refused, not guessed. A new metric gets a declared roll-up or no quarterly view.
            _log.warning("[earnings] no TTM rule for %r — quarterly view omits it", metric)
            return {}
        codes = tuple(c.replace("annuals__", "quarterly__") for c in _metric_codes(metric))
        rows: list[dict] = []
        for code in codes:
            rows += _page_metrics(company_id, code, exact=True)
        return _ttm_by_period(rows, rule)

    return _latest_per_year(_metric_rows(company_id, metric))


def _latest_per_year_dated(rows: list[dict]) -> dict[str, tuple[str, float]]:
    """{fiscal year: (period-END date, value)} — the annual bucketing rule, stated once.

    ⚠ THE DATE IS KEPT BECAUSE A YEAR LABEL CANNOT BE CONVERTED. Anything that has to price a
    fiscal year in another currency needs the date the period actually ENDED — a September filer's
    "2025" is not 31 December 2025, and converting it at the calendar year-end applies an FX rate
    from three months after the figure was struck. `_latest_per_year` is this with the date
    dropped, so the two can never bucket a year differently (see
    `_benchmark_fundamental_grid`, which is the caller that needs the date).
    """
    by: dict[str, tuple[str, float]] = {}
    for m in rows:
        v = m.get("numeric_value")
        if v is None:
            continue
        d = str(m["target_date"])[:10]
        y = d[:4]
        if y not in by or d > by[y][0]:
            by[y] = (d, float(v))
    return by


def _latest_per_year(rows: list[dict]) -> dict[str, float]:
    """{fiscal year: the LATEST observation in it} — the annual bucketing rule, stated once.

    Shared by the per-company reader and the bulk one below, so a benchmark's series and a
    holding's cannot come to disagree about which observation a year is.
    """
    return {y: v for y, (_d, v) in _latest_per_year_dated(rows).items()}


def _metrics_by_company(company_ids: list[int], metric: str,
                        cadence: str = "annual") -> dict[int, dict[str, float]]:
    """{company_id: {period: value}} for ONE metric across MANY companies.

    ⚠ THE POINT IS THE ROUND TRIPS, NOT THE ROWS. The benchmark endpoints used to call
    `_metric_by_year` in a loop — one paged read per company per metric — and the data was never
    the problem: measured on SP500 (503 members), one metric costs **44.5 s** as a loop and
    **0.1 s** as this, for 2,328 rows. That is 421x, against a LOCAL database; over the network the
    loop degrades further while this stays one request per chunk. A three-metric card was 133 s and
    the twelve-card tab would have been ~19 minutes.

    ⚠ CHUNKED **AND** PAGED, because the two limits are different and both bite. `.in_()` is capped
    at `IN_CHUNK_SIZE` ids (the Cloudflare 502 guard), while PostgREST silently truncates any single
    response at 1,000 rows on cloud — the failure that produces a plausible number rather than an
    error, exactly as the FX reader documents. Advance by what came back and stop on an EMPTY page:
    `len(page) < _PAGE` is only correct while the server's cap is >= the page size, which is the
    assumption that failed there.

    ⚠ ORDERED ON A UNIQUE KEY. Postgres promises nothing about tied rows across separate
    LIMIT/OFFSET queries, so a page boundary inside a tie serves a row twice or never;
    (company_id, target_date, metric_code) is unique here.
    """
    codes, rule = _codes_and_rule(metric, cadence)
    if codes is None:
        return {}
    raw = _rows_by_company(company_ids, codes)
    return {cid: (_ttm_by_period(rows, rule) if rule else _latest_per_year(rows))
            for cid, rows in raw.items()}


def _codes_and_rule(metric: str, cadence: str) -> tuple[list[str] | None, str | None]:
    """A metric's codes for the requested cadence, plus its TTM roll-up rule (None on annual).

    `(None, None)` means the metric has no declared roll-up, so quarterly cannot be answered for
    it — the caller omits it rather than inventing one. Shared so the bulk readers and the blend
    cannot come to disagree about which spelling a cadence uses.
    """
    codes = list(_metric_codes(metric))
    if cadence != "quarterly":
        return codes, None
    rule = _TTM_RULE.get(metric)
    if rule is None:
        _log.warning("[earnings] no TTM rule for %r — omitted", metric)
        return None, None
    return [c.replace("annuals__", "quarterly__") for c in codes], rule


def _rows_by_company(company_ids: list[int], codes: list[str]) -> dict[int, list[dict]]:
    """{company_id: rows} for the named codes across MANY companies — the read the benchmark work
    is bounded by. See `_metrics_by_company` for the measurement and for why this is chunked AND
    paged, ordered on a unique key, and advances by what came back."""
    raw: dict[int, list[dict]] = defaultdict(list)
    for i in range(0, len(company_ids), IN_CHUNK_SIZE):
        chunk = company_ids[i:i + IN_CHUNK_SIZE]
        off = 0
        while True:
            page = (supabase.table("metric_data")
                    .select("company_id,metric_code,target_date,numeric_value")
                    .in_("company_id", chunk).in_("metric_code", codes)
                    .gte("target_date", _BLEND_START)
                    .order("company_id").order("target_date").order("metric_code")
                    .range(off, off + _PAGE - 1).execute().data or [])
            if not page:
                break
            for r in page:
                raw[r["company_id"]].append(r)
            off += len(page)
    return raw


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
        # ⚠ ONE READ FOR THE WHOLE INDEX — see `_metrics_by_company`. As a per-company loop this
        # was 44.5 s for SP500's 503 members; the data it moves is 2,328 rows.
        #
        # ⚠ `blend_series` WANTS RAW DATED POINTS, NOT A YEAR-BUCKETED SERIES, so this one keeps the
        # dates: it rebases each member to an index and does its own period alignment. Handing it
        # `{year: value}` would silently change what it is blending.
        raw_by_cid: dict[int, list[dict]] = defaultdict(list)
        for i in range(0, len(ids), IN_CHUNK_SIZE):
            chunk = ids[i:i + IN_CHUNK_SIZE]
            off = 0
            while True:
                page = (supabase.table("metric_data")
                        .select("company_id,metric_code,target_date,numeric_value")
                        .in_("company_id", chunk).in_("metric_code", list(_metric_codes(metric)))
                        .gte("target_date", _BLEND_START)
                        .order("company_id").order("target_date").order("metric_code")
                        .range(off, off + _PAGE - 1).execute().data or [])
                if not page:
                    break
                for r in page:
                    raw_by_cid[r["company_id"]].append(r)
                off += len(page)
        for cid in ids:
            pts = {str(m["target_date"])[:10]: float(m["numeric_value"])
                   for m in raw_by_cid.get(cid, ())
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
        # ⚠ ONE BULK READ FOR THE WHOLE SET, NOT ONE PER COMPANY — and here it is the difference
        # between a table and a timeout. This loop reads ONE metric per member through
        # `_metric_by_year`, which is ~160 ms each: measured 2026-08-04, the S&P's 489 constituents
        # took **64.5 s**; prefetched they are one chunked, paged query. The endpoint was fine while
        # only a 20-name book could reach it and became the slowest thing on the tab the day the
        # benchmark overlay let it be pointed at an index.
        _prefetch([c["company_id"] for c in comp.values() if c.get("company_id")],
                  (metric,), body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            # ⚠ THROUGH THE SHARED SEAM, NOT A SECOND COPY OF IT. This block used to hand-roll the
            # same "latest observation per fiscal year" bucketing that `_metric_by_year` does — so
            # when cadence arrived, the chart switched to trailing twelve months and the drill-down
            # BEHIND that chart quietly kept showing fiscal years. A table that disagrees with the
            # chart it is supposed to explain is worse than no table, and the duplication is what
            # made it possible.
            rev = {p: v for p, v in
                   _metric_by_year(c["company_id"], metric, body.cadence).items()
                   if p >= "2015"}
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
                # ⚠ THE NUMERATOR OF THE WEIGHT, AND ONLY ON THE INDEX PATH. For a universe,
                # `weight_by[ci]` IS `company.market_cap_eur` (see `_load_and_expand_members`), so
                # this is the figure the percentage beside it was divided out of — cap ÷ Σcap — and
                # a reader can check the division. For a PORTFOLIO the same variable holds the
                # holding's weight, which is not a market cap at all, so the field is omitted
                # rather than filled with a number that would be read as one.
                **({"market_cap_eur": weight_by[ci]} if body.universe else {}),
            })
        rows.sort(key=lambda r: -r["weight_pct"])
        out = {"years": sorted(years), "rows": rows, "holdings": len(members)}
        if body.universe:
            # ⚠ WHO IS **NOT** IN THE INDEX, ON THE ONE SCREEN BUILT FOR CHECKING IT BY HAND. A
            # constituent with no stored market cap is dropped before weighting, and the names
            # that lack one are systematically the ones GuruFocus does not cover — on the AEX that
            # is Shell, Unilever and RELX, which is why ASML renormalises to 51.76%. Absent from
            # the table AND absent from a footnote, that reads as a weight the index really has.
            from routers._benchmark_index import weight_basis  # noqa: PLC0415
            out["weight_basis"] = weight_basis(body.universe)
        return out

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/margin-inputs")
@cached_blend("margin-inputs")
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
        # ⚠ ONE BULK READ PER METRIC, NOT ONE PER COMPANY — see `_prefetch`. A benchmark
        # request carries an index's 489 constituents, where the per-company path is 72s.
        _prefetch([comp[ci]["company_id"] for ci in canon if ci in comp],
                  ('revenue', 'fcf', 'sbc',), body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            rev = _metric_by_year(c["company_id"], "revenue", body.cadence)
            fcf = _metric_by_year(c["company_id"], "fcf", body.cadence)
            sbc = _metric_by_year(c["company_id"], "sbc", body.cadence)
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
@cached_blend("debt-ratio-inputs")
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
        # ⚠ ONE BULK READ PER METRIC, NOT ONE PER COMPANY — see `_prefetch`. A benchmark
        # request carries an index's 489 constituents, where the per-company path is 72s.
        _prefetch([comp[ci]["company_id"] for ci in canon if ci in comp],
                  ('long_term_debt', 'total_assets', 'goodwill',), body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            ltd = _metric_by_year(c["company_id"], "long_term_debt", body.cadence)
            ta = _metric_by_year(c["company_id"], "total_assets", body.cadence)
            gw = _metric_by_year(c["company_id"], "goodwill", body.cadence)
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
@cached_blend("cash-return-inputs")
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
        # ⚠ ONE BULK READ PER METRIC, NOT ONE PER COMPANY — see `_prefetch`. A benchmark
        # request carries an index's 489 constituents, where the per-company path is 72s.
        _prefetch([comp[ci]["company_id"] for ci in canon if ci in comp],
                  ('fcf', 'sbc', 'noncurrent_liabilities', 'total_equity', 'roic',), body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            fcf = _metric_by_year(c["company_id"], "fcf", body.cadence)
            # Carried so the tab-level "SBC correction" toggle can subtract it from FCF without a
            # second request. ⚠ MISSING SBC IS TREATED AS ZERO by the client, not as "unknown":
            # most companies genuinely report none, and blanking the ratio for them would empty
            # the chart for the majority to be pedantic about the minority.
            sbc = _metric_by_year(c["company_id"], "sbc", body.cadence)
            ncl = _metric_by_year(c["company_id"], "noncurrent_liabilities", body.cadence)
            eq = _metric_by_year(c["company_id"], "total_equity", body.cadence)
            # ⚠ A RATIO, NOT A RAW LINE — the one field in this payload that is already the answer.
            # The other three are amounts the client divides; this one is GuruFocus's own
            # percentage and is passed through untouched (see `_METRIC_CODES["roic"]`).
            roic = _metric_by_year(c["company_id"], "roic", body.cadence)
            years |= set(fcf) | set(ncl) | set(eq) | set(roic)
            exch = gx.get("exchange_code")
            subscribed = is_gf_subscribed_exchange(exch) if exch else None
            # `ok` if we have equity (the capital base) OR any FCF to show; a company on an
            # unsubscribed exchange can't be fetched at all; else nothing ingested yet.
            status = ("ok" if (eq or fcf or roic)
                      else ("unsubscribed" if subscribed is False else "no_data"))
            rows.append({
                "isin": ci, "name": c.get("company_name") or name_by.get(ci) or ci,
                "weight_pct": round(100.0 * weight_by[ci] / total_w, 2),
                "currency": gx.get("currency_code"),
                "ticker": c.get("gurufocus_ticker"), "exchange": exch,
                "status": status,
                "fcf": fcf, "sbc": sbc, "noncurrent_liabilities": ncl, "total_equity": eq,
                "roic": roic,
            })
        rows.sort(key=lambda r: -r["weight_pct"])
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows}

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/interest-burden-inputs")
@cached_blend("interest-burden-inputs")
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
        # ⚠ ONE BULK READ PER METRIC, NOT ONE PER COMPANY — see `_prefetch`. A benchmark
        # request carries an index's 489 constituents, where the per-company path is 72s.
        _prefetch([comp[ci]["company_id"] for ci in canon if ci in comp],
                  ('interest_expense', 'operating_income',), body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            ie = _metric_by_year(c["company_id"], "interest_expense", body.cadence)
            oi = _metric_by_year(c["company_id"], "operating_income", body.cadence)
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
@cached_blend("sbc-ocf-inputs")
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
        # ⚠ ONE BULK READ PER METRIC, NOT ONE PER COMPANY — see `_prefetch`. A benchmark
        # request carries an index's 489 constituents, where the per-company path is 72s.
        _prefetch([comp[ci]["company_id"] for ci in canon if ci in comp],
                  ('sbc', 'ocf',), body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            sbc = _metric_by_year(c["company_id"], "sbc", body.cadence)
            ocf = _metric_by_year(c["company_id"], "ocf", body.cadence)
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
@cached_blend("capex-margin-inputs")
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
        # ⚠ ONE BULK READ PER METRIC, NOT ONE PER COMPANY — see `_prefetch`. A benchmark
        # request carries an index's 489 constituents, where the per-company path is 72s.
        _prefetch([comp[ci]["company_id"] for ci in canon if ci in comp],
                  ('capex', 'revenue',), body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            capex = _metric_by_year(c["company_id"], "capex", body.cadence)
            rev = _metric_by_year(c["company_id"], "revenue", body.cadence)
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


@router.post("/api/earnings/gross-margin-inputs")
@cached_blend("gross-margin-inputs")
async def gross_margin_inputs(body: FundamentalCoverageRequest):
    """The base inputs behind the Gross margin, per holding: Gross Profit and Revenue per fiscal
    year, in the company's own reporting currency (millions).

    ⚠ THE RAW LINES, NOT THE RATIO. `Gross Profit / Revenue` is derived on the client from these
    two so the drill-down shows exactly what it is computed from (2 rows per company). Revenue ≤ 0
    → the ratio is blank.

    ⚠ A BANK HAS NO GROSS PROFIT AND THAT IS AN ANSWER, NOT A GAP. GuruFocus's 'B' template has no
    cost of goods sold, so the line is absent (JPMorgan) and the margin is blank there — never 0,
    which would read as "sells at cost".

    ⚠ DERIVED THOUGH GURUFOCUS PUBLISHES `Ratios__Gross Margin %`. It reproduces their figure
    exactly (ASML 2025 52.83% vs 52.83; Apple 46.91 vs 46.905) and leaves two lines the drill-down
    can show — a published ratio has no workings to check it against.

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
        # ⚠ ONE BULK READ PER METRIC, NOT ONE PER COMPANY — see `_prefetch`. A benchmark
        # request carries an index's 489 constituents, where the per-company path is 72s.
        _prefetch([comp[ci]["company_id"] for ci in canon if ci in comp],
                  ('gross_profit', 'revenue',), body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            gp = _metric_by_year(c["company_id"], "gross_profit", body.cadence)
            rev = _metric_by_year(c["company_id"], "revenue", body.cadence)
            years |= set(gp) | set(rev)
            exch = gx.get("exchange_code")
            subscribed = is_gf_subscribed_exchange(exch) if exch else None
            # `ok` if we have revenue (the denominator) OR any gross profit to show; a company
            # on an unsubscribed exchange can't be fetched at all; else nothing ingested yet.
            # ⚠ A BANK IS `ok` WITH REVENUE AND NO GROSS PROFIT — it has been fetched fine, the
            # concept just does not apply. Marking it `no_data` would blame our ingest.
            status = "ok" if (rev or gp) else ("unsubscribed" if subscribed is False else "no_data")
            rows.append({
                "isin": ci, "name": c.get("company_name") or name_by.get(ci) or ci,
                "weight_pct": round(100.0 * weight_by[ci] / total_w, 2),
                "currency": gx.get("currency_code"),
                "ticker": c.get("gurufocus_ticker"), "exchange": exch,
                "status": status,
                "gross_profit": gp, "revenue": rev,
            })
        rows.sort(key=lambda r: -r["weight_pct"])
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows}

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/cash-conversion-inputs")
@cached_blend("cash-conversion-inputs")
async def cash_conversion_inputs(body: FundamentalCoverageRequest):
    """The base inputs behind Cash conversion, per holding: Free Cash Flow and Net Income per
    fiscal year, in the company's own reporting currency (millions).

    ⚠ THE RAW LINES, NOT THE RATIO. `FCF / Net Income` is derived on the client from these two so
    the drill-down shows exactly what it is computed from (2 rows per company).

    ⚠ ABOVE 100% IS NORMAL AND GOOD, not an error — depreciation running ahead of capex converts
    more cash than the accounts book as profit (ASML 2025: 11,027.3 / 9,609.4 = 114.8%).

    ⚠ NET INCOME ≤ 0 → THE RATIO IS BLANK. A loss-making company with positive free cash flow
    would otherwise print a NEGATIVE conversion, which reads as burning cash when the opposite is
    happening. A negative FCF against positive earnings IS kept — earnings without cash is exactly
    what this ratio exists to catch.

    ⚠ NET INCOME IS THE SHAREHOLDERS' LINE while FCF is whole-company; see `_METRIC_CODES`.

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
        # ⚠ ONE BULK READ PER METRIC, NOT ONE PER COMPANY — see `_prefetch`. A benchmark
        # request carries an index's 489 constituents, where the per-company path is 72s.
        _prefetch([comp[ci]["company_id"] for ci in canon if ci in comp],
                  ('fcf', 'sbc', 'net_income',), body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            fcf = _metric_by_year(c["company_id"], "fcf", body.cadence)
            sbc = _metric_by_year(c["company_id"], "sbc", body.cadence)
            ni = _metric_by_year(c["company_id"], "net_income", body.cadence)
            years |= set(fcf) | set(ni)
            exch = gx.get("exchange_code")
            subscribed = is_gf_subscribed_exchange(exch) if exch else None
            # `ok` if we have net income (the denominator) OR any FCF to show; a company on an
            # unsubscribed exchange can't be fetched at all; else nothing ingested yet.
            status = "ok" if (ni or fcf) else ("unsubscribed" if subscribed is False else "no_data")
            rows.append({
                "isin": ci, "name": c.get("company_name") or name_by.get(ci) or ci,
                "weight_pct": round(100.0 * weight_by[ci] / total_w, 2),
                "currency": gx.get("currency_code"),
                "ticker": c.get("gurufocus_ticker"), "exchange": exch,
                "status": status,
                "fcf": fcf, "sbc": sbc, "net_income": ni,
            })
        rows.sort(key=lambda r: -r["weight_pct"])
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows}

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/fcf-sbc-yield-inputs")
@cached_blend("fcf-sbc-yield-inputs")
async def fcf_sbc_yield_inputs(body: FundamentalCoverageRequest):
    """The base inputs behind the FCF-SBC yield, per holding: Free Cash Flow, Stock-Based
    Compensation and Market Cap per fiscal year, in the company's own reporting currency (millions).

    ⚠ THE RAW LINES, NOT THE RATIO. `(FCF − SBC) / Market Cap` (the cash yield a buyer earns, net of
    the non-cash stock comp) is derived on the client from these three so the drill-down shows
    exactly what it is computed from (3 rows per company). SBC missing is treated as 0 (many report
    none); FCF may be negative (yield goes negative); Market Cap must be present and positive.
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
        # ⚠ ONE BULK READ PER METRIC, NOT ONE PER COMPANY — see `_prefetch`. A benchmark
        # request carries an index's 489 constituents, where the per-company path is 72s.
        _prefetch([comp[ci]["company_id"] for ci in canon if ci in comp],
                  ('fcf', 'sbc', 'market_cap',), body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            if body.cadence == "daily":
                # ⚠ MARKET CAP IS RECONSTRUCTED HERE, NOT READ. GuruFocus publishes `Market Cap`
                # only per fiscal period, so a daily denominator has to be
                # `daily close x shares outstanding`. Both legs are already in this company's own
                # reporting currency and both are in MILLIONS (shares is the diluted average, in
                # millions), so the product is a market cap in millions — the same unit the annual
                # line carries, which is what keeps the two cadences on one axis.
                #
                # ⚠ IT WILL NOT EQUAL GURUFOCUS'S OWN FIGURE TO THE DECIMAL. Theirs uses the shares
                # in issue at their own moment; ours uses the TTM diluted average, which is the
                # basis every other per-share number on this tab is on. Consistency with the tab
                # beats agreement with a line we do not otherwise use.
                close = _daily_closes(c["company_id"])
                dates = sorted(close)
                fcf = _daily_metric(c["company_id"], "fcf", dates)
                sbc = _daily_metric(c["company_id"], "sbc", dates)
                sh = _daily_metric(c["company_id"], "shares", dates)
                mc = {d: close[d] * sh[d] for d in sh if d in close and sh[d]}
            else:
                fcf = _metric_by_year(c["company_id"], "fcf", body.cadence)
                sbc = _metric_by_year(c["company_id"], "sbc", body.cadence)
                mc = _metric_by_year(c["company_id"], "market_cap", body.cadence)
            years |= set(fcf) | set(sbc) | set(mc)
            exch = gx.get("exchange_code")
            subscribed = is_gf_subscribed_exchange(exch) if exch else None
            # `ok` if we have market cap (the denominator) OR any FCF to show; a company on an
            # unsubscribed exchange can't be fetched at all; else nothing ingested yet.
            status = "ok" if (mc or fcf) else ("unsubscribed" if subscribed is False else "no_data")
            rows.append({
                "isin": ci, "name": c.get("company_name") or name_by.get(ci) or ci,
                "weight_pct": round(100.0 * weight_by[ci] / total_w, 2),
                "currency": gx.get("currency_code"),
                "ticker": c.get("gurufocus_ticker"), "exchange": exch,
                "status": status,
                "fcf": fcf, "sbc": sbc, "market_cap": mc,
            })
        rows.sort(key=lambda r: -r["weight_pct"])
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows}

    return await asyncio.to_thread(_run)


@router.get("/api/earnings/by-isin/{isin}/growth-estimates")
async def growth_estimates_by_isin(isin: str, force: bool = False):
    """Analysts' 3–5 year growth-rate estimates for a company, from GuruFocus `keyratios`.

    The figures a reverse DCF is judged against — the model says the price implies 24%/yr, and the
    next question is what anyone actually forecasts.

    ⚠ A LIVE FETCH, NOT A METRIC READ. These are scalars with no date, so they never reach
    `metric_data` (the estimates parser only stores list-valued fields). Cached in Storage for a
    week per listing; `force=true` re-asks. See `_growth_estimates`.

    Returns `{symbol, fields: {eps_3_5y, eps_nri_3_5y, ocf_ps_3_5y, revenue_3_5y}, cached}` with the
    rates as PERCENTS. A company with no analyst coverage returns nulls, not an error.
    """
    from asset_pipeline.isin_alias import canonical  # noqa: PLC0415

    from routers._growth_estimates import growth_estimates_for  # noqa: PLC0415

    def _run() -> dict:
        resp = (supabase.table("company")
                .select("company_id,gurufocus_ticker,"
                        "gurufocus_exchange:gurufocus_exchange(exchange_code)")
                .eq("isin", canonical(isin)).limit(1).execute())
        if not resp.data:
            raise HTTPException(status_code=404, detail="No company record for this ISIN")
        return growth_estimates_for(resp.data[0], force=force)

    try:
        return await asyncio.to_thread(_run)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}") from e


@router.post("/api/earnings/dividend-yield-inputs")
@cached_blend("dividend-yield-inputs")
async def dividend_yield_inputs(body: FundamentalCoverageRequest):
    """The two base lines behind the dividend yield, per holding: Dividends per Share and the
    fiscal year-end share price, per fiscal year, in the company's own reporting currency.

    ⚠ THE YIELD IS THE PORTFOLIO-LEVEL PRIMITIVE; DIVIDENDS PER SHARE IS NOT. There is no portfolio
    share to report a per-share amount of, the amounts are in different currencies, and a level
    series that legitimately starts at 0.00 cannot be rebased to a growth index — which is exactly
    why the portfolio's dividend card sat empty while every holding carried the line. `DPS / price`
    is currency-free, so the weight-weighted average IS the book's yield (portfolio yield =
    Σ value·yield ÷ Σ value, and the weights ARE value weights — the arithmetic mean is the
    aggregate here, not an approximation of it).

    ⚠ AN ABSENT DPS IS NOT A ZERO. GuruFocus files an explicit `0.00` for a company that pays
    nothing — a real answer that belongs in the average and drags it down honestly. A MISSING line
    is not that: reading it as zero would let un-ingested holdings quietly deflate the book's yield.
    The client keeps them apart (`dividendYieldOf`), so the raw lines are returned untouched here.

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
        # ⚠ ONE BULK READ PER METRIC, NOT ONE PER COMPANY — see `_prefetch`. A benchmark
        # request carries an index's 489 constituents, where the per-company path is 72s.
        _prefetch([comp[ci]["company_id"] for ci in canon if ci in comp],
                  ('div_ps', 'price_ps',), body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            if body.cadence == "daily":
                # The denominator moves every trading day; the numerator steps at each fiscal
                # period end and is flat between them. That IS a trailing yield — the same shape a
                # terminal draws — and it is why only the two YIELD cards offer this cadence: the
                # other ten have no daily input at all.
                price = _daily_closes(c["company_id"])
                div = _daily_metric(c["company_id"], "div_ps", sorted(price))
                # ⚠ Only days the numerator reaches. `_step_onto_dates` drops anything before the
                # first reported period, so `price` alone would put bare denominators on the chart.
                price = {d: v for d, v in price.items() if d in div}
            else:
                div = _metric_by_year(c["company_id"], "div_ps", body.cadence)
                price = _metric_by_year(c["company_id"], "price_ps", body.cadence)
            years |= set(div) | set(price)
            exch = gx.get("exchange_code")
            subscribed = is_gf_subscribed_exchange(exch) if exch else None
            # `ok` once the DENOMINATOR is there: a company with prices and no dividend line is a
            # non-payer we can still say something about, while one with dividends and no price
            # yields nothing computable.
            status = "ok" if price else ("unsubscribed" if subscribed is False else "no_data")
            rows.append({
                "isin": ci, "name": c.get("company_name") or name_by.get(ci) or ci,
                "weight_pct": round(100.0 * weight_by[ci] / total_w, 2),
                "currency": gx.get("currency_code"),
                "ticker": c.get("gurufocus_ticker"), "exchange": exch,
                "status": status,
                "div_ps": div, "price_ps": price,
            })
        rows.sort(key=lambda r: -r["weight_pct"])
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows}

    return await asyncio.to_thread(_run)


@router.get("/api/earnings/benchmark-margin")
async def benchmark_margin(label: str = "AEX", cadence: str = "annual"):
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

        # ⚠ THREE READS FOR THE WHOLE INDEX, NOT THREE PER MEMBER. This was 503 x 3 paged round
        # trips — ~133 s on SP500 — for data that arrives in a fraction of a second when asked for
        # once per metric. See `_metrics_by_company`.
        rev_all = _metrics_by_company(ids, "revenue", cadence)
        fcf_all = _metrics_by_company(ids, "fcf", cadence)
        sbc_all = _metrics_by_company(ids, "sbc", cadence)
        per_member: list[tuple[float, dict[str, float]]] = []
        for cid in ids:
            rev = rev_all.get(cid, {})
            fcf = fcf_all.get(cid, {})
            sbc = sbc_all.get(cid, {})
            marg = {y: (fcf[y] - sbc.get(y, 0.0)) / rev[y] * 100.0
                    for y in rev if rev[y] and rev[y] > 0 and y in fcf}
            if marg:
                per_member.append((caps.get(cid, 1.0), marg))

        # ⚠ THE COVERAGE FLOOR — THE SAME ONE THE REST OF THE SUITE USES, AND THIS ENDPOINT WAS THE
        # ONLY THING ON THE TAB WITHOUT IT. `benchmark_revenue` gets it free from `blend_series`;
        # this function hand-rolls its own weighted average and tested nothing but `den > 0`.
        #
        # Measured on SP500 before the fix: 2022-2025 were each backed by 98-100% of the charted
        # members, and 2026 by SIX of ninety-two — 7% — because only a handful have filed. That
        # point read 33.62% against 2025's 19.77%, in the same ink, at the right-hand edge where a
        # reader looks first. It is not a move in the index, it is a move in the sample.
        #
        # ⚠ COVERAGE IS BY CAP WEIGHT, NOT HEADCOUNT, because the figure it gates is cap-weighted:
        # six megacaps and six minnows are the same count and nothing like the same coverage.
        #
        # ⚠ AND THE DENOMINATOR IS THE CHARTED SET, NOT THE INDEX — same rule as the portfolio
        # cards. Only 92 of SP500's 503 members have fundamentals ingested at all, so measuring
        # against 503 would put every year under the floor and blank the chart. That 18% is a real
        # and separate caveat, so it is REPORTED (`index_coverage_pct`) rather than folded into a
        # per-year test it would silently dominate.
        from routers._fundamental_blend import MIN_BLEND_COVERAGE_PCT  # noqa: PLC0415

        total_w = sum(w for w, _m in per_member) or 1.0
        years = sorted({y for _w, m in per_member for y in m if y >= "2015"})
        series = []
        for y in years:
            num = sum(w * m[y] for w, m in per_member if y in m)
            den = sum(w for w, m in per_member if y in m)
            cov = 100.0 * den / total_w
            if den <= 0 or cov < MIN_BLEND_COVERAGE_PCT:
                continue
            series.append({"year": int(y), "margin_pct": round(num / den, 4),
                           "coverage_pct": round(cov, 1),
                           "members": sum(1 for _w, m in per_member if y in m)})
        return {"label": label, "members": len(per_member), "series": series,
                # What the per-year test is, so a hidden year reads as withheld rather than absent.
                "floor_pct": MIN_BLEND_COVERAGE_PCT,
                # ...and how much of the INDEX the whole line describes — the standing caveat.
                "index_members": len(ids),
                "index_coverage_pct": round(100.0 * len(per_member) / (len(ids) or 1), 1)}

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
