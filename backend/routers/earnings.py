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
from collections import Counter, defaultdict
# ⚠ ALIASED. Three loops in this file already bind a variable called `date` (they iterate
# `{date: value}` maps), and importing the type under that name shadows it inside them — the same
# word meaning two things a few lines apart is how the wrong one gets called.
from datetime import date as _date
from routers._blend_cache import cached_blend, cached_metric_reads
from routers._earnings_pg import rows_by_company_via_copy
from routers._sse import sse_message as event
import queue as _queue

from fastapi import APIRouter, HTTPException, Request
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
    # ⚠⚠ THE THREE LINES THE REVERSE DCF NORMALISES FCF WITH — added 2026-08-18, and their absence
    # is worth recording because NOTHING SAID THEY WERE MISSING. The rows are in `metric_data` for
    # every company (ASML: SBC 202.3, capex -1631.2, D&A 1025.9); this list is an ALLOWLIST, so
    # codes not on it are simply never sent. `latestObs` then found nothing, `normalisedFcf`
    # correctly reported the correction as not-applicable, and the panel rendered an honest "—"
    # for a company that files all three. Every layer behaved; the data stopped at the door.
    #
    # ⚠ THE CASH-FLOW DEPRECIATION LINE, NOT THE INCOME STATEMENT'S. Capex is a cash figure, so its
    # maintenance proxy has to be one too — see `DEP_CODES` in `egmInputs.ts`.
    "annuals__Cashflow Statement__Stock Based Compensation",
    "annuals__Cashflow Statement__Capital Expenditure",
    "annuals__Cashflow Statement__Cash Flow Depreciation, Depletion and Amortization",
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
        if cadence != "quarterly":
            # ⚠ ANNUAL ONLY. Every point of a quarterly series is already a trailing twelve months,
            # so the newest one needs no second name — see `_ltm_rows` for why it cannot be derived
            # on the client from the raw quarters this payload carries.
            rows = rows + await asyncio.to_thread(_ltm_rows, info["company_id"], rows)
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


async def _load_and_expand_members(body: FundamentalCoverageRequest, *,
                                   all_constituents: bool = False) -> list[dict]:
    """The flat holdings to analyse, with every linked certificate looked THROUGH to the model it
    IS — so its real stocks feed both the coverage table and the blended charts, rather than
    dropping out as one dead CH-ISIN row.

    ⚠ ONE PLACE, THREE ENDPOINTS. coverage / blend / blend-metrics all start from the identical
    member list and must agree on it — the blend renormalises over the coverage the SAME members
    produce. A look-through applied in one but not another would blend over stocks the coverage
    table never admitted. Raises 422 when neither field is set; returns [] for an empty portfolio.

    `all_constituents` (universe only) keeps the constituents with no stored market cap, at weight
    0 — the drill-down table lists the whole index; the weighted lines cannot. See `_load_universe`.
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
            # ⚠⚠ `all_constituents` IS FOR THE DRILL-DOWN TABLE ONLY, AND IT MUST NOT REACH THE
            # LINE. A constituent with no stored market cap cannot be weighted, so it has no place
            # in a cap-weighted average — but it IS in the index, and a table titled "everything
            # behind the chart" that silently lists 22 of the AEX's 25 hides the fact that RELX,
            # Shell and Unilever are unreachable (all LSE, outside the GuruFocus subscription).
            # They come through at weight 0: absent from every average, present as rows whose cells
            # say `Unsubscribed`, which is the answer the reader came for.
            #
            # ⚠ THE FLOORS WOULD MOVE IF THIS WERE THE DEFAULT. `covered_names_pct` divides by the
            # member count, so quietly adding three members that can never report would drop every
            # period from 21/22 to 21/25 and change which periods the CHART draws. Two questions,
            # two member lists, one flag.
            members = _members(body.universe or "", require_market_cap=not all_constituents)
            return [{"isin": m["isin"], "name": m.get("company_name"),
                     "weight": float(m.get("market_cap_eur") or 0)}
                    for m in members
                    if m.get("isin") and (all_constituents
                                          or (m.get("market_cap_eur") or 0) > 0)]

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
async def fundamental_coverage(body: FundamentalCoverageRequest, request: Request):
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
async def fundamental_blend(body: FundamentalCoverageRequest, request: Request):
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
        # ⚠ THE EUROS, ON THIS PATH TOO — asked for as "everything aggregatable" rather than by
        # name, because `BLEND_METRICS` holds metric CODES and `_totals_for` takes metric KEYS. A
        # code passed there matches nothing and this whole path would silently keep the growth
        # chain with no error anywhere, which is the failure this conversion exists to end. The
        # answer comes back keyed by CODE, so the loop below looks its own codes up directly.
        totals = _totals_for(covered, [], body.universe, body.cadence)
        out: dict[str, dict] = {}
        for code in BLEND_METRICS:
            per_company = by_metric.get(code, {})
            fund_for = totals.get(code, {})
            out[code] = blend_series(
                [{"weight": r["weight_pct"], "points": per_company.get(r["company_id"], {}),
                  "fund_points": fund_for.get(r["company_id"], {})}
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

    ⚠⚠ AND THE SORT KEY MUST BE **UNIQUE**, WHICH `target_date` ALONE IS NOT (fixed 2026-08-17).
    Postgres makes no promise about the order of TIED rows across separate LIMIT/OFFSET queries, so
    a page boundary landing inside a tie group serves some rows twice and others never. Here the
    ties are enormous and unavoidable: a company files ~110 metric codes on the SAME
    `target_date`, so with `_PAGE = 1000` every boundary falls inside one.

    Measured on Bustelberg Offensief's FCF/share: ASML, Alphabet and Amazon each silently lost
    their **2018** row and Berkshire its **2019** — a different arbitrary row per company, no
    error, no empty panel, just a blended line missing a point. It moved the book's 10-year
    FCF/share CAGR by 0.14pp (27.86% → 28.00%) and was invisible except as a disagreement with the
    Tables tab, whose `exact=True` reads ONE code (~28 rows) and so never reaches a page boundary
    at all. Same failure and same fix as the FX pager in `_airs_portfolio_perf` — see CLAUDE.md.

    The primary key is `(company_id, metric_code, source_code, target_date)` and `company_id` is
    pinned by the filter, so these three order the rows totally.
    """
    out: list[dict] = []
    start = 0
    while True:
        page = (supabase.table("metric_data")
                .select("company_id,metric_code,target_date,numeric_value")
                .eq("company_id", company_id).like("metric_code", pattern)
                .gte("target_date", _BLEND_START)
                .order("target_date").order("metric_code").order("source_code")
                .range(start, start + _PAGE - 1).execute().data or [])
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
    return out + _ltm_blend_rows(cids, metrics, cadence)


def _ltm_rows(company_id: int, annual_rows: list[dict]) -> list[dict]:
    """The LTM row per metric, for the ANNUAL payload — one trailing-twelve-month point per metric,
    at the newest quarterly period-end, when that reaches PAST the last full fiscal year.

    ⚠⚠ EMITTED UNDER AN `ltm__` CODE, NOT THE ANNUAL ONE. Four other screens read this payload
    (Quick Valuation, Deep Valuation, the old chart suite) and every one of them selects by
    `metric_code`: an LTM row wearing the annual spelling would silently become an extra "fiscal
    year" in each of them, at a quarter-end date, and `_latest_per_year` would let it REPLACE the
    real final year on the same x. A distinct prefix is invisible to everyone who does not ask.

    ⚠ IT EXISTS BECAUSE THE CLIENT CANNOT DERIVE IT. The raw `quarterly__…` rows in this payload are
    single QUARTERS — the roll-up runs only when the request asks for `cadence=quarterly` — so a
    card deriving "the newest quarterly point" plots a quarter and calls it a year: ASML showed
    ~8.8bn against a true 35,327.5. Rebuilding the roll-up client-side would mean a second copy of
    the cadence rules (sum for flows, mean for share counts, `filings_per_year` so a semi-annual
    filer is not summed into 24 months). One implementation, on the server.
    """
    newest_annual: dict[str, str] = {}
    for r in annual_rows:
        code = str(r.get("metric_code") or "")
        if not code.startswith("annuals__") or r.get("numeric_value") is None:
            continue
        d = str(r.get("target_date"))[:10]
        if d > newest_annual.get(code, ""):
            newest_annual[code] = d
    out: list[dict] = []
    newest_ttm: dict[str, dict] = {}
    for r in _ttm_metric_rows(company_id):          # already TTM, cadence-aware, annual-coded
        code = str(r.get("metric_code") or "")
        d = str(r.get("target_date"))[:10]
        if r.get("numeric_value") is not None and d > str(newest_ttm.get(code, {}).get(
                "target_date", ""))[:10]:
            newest_ttm[code] = r
    for code, r in newest_ttm.items():
        # Equal dates mean the trailing twelve months ARE that fiscal year — already in the payload.
        if str(r["target_date"])[:10] > newest_annual.get(code, ""):
            out.append({**r, "metric_code": "ltm__" + code[len("annuals__"):], "is_ltm": True})
    return out


def _ltm_by_company(company_ids: list[int], metric: str,
                    cadence: str) -> dict[int, tuple[str, float]]:
    """`{company_id: LTM value}` — the trailing twelve months to each company's newest quarterly
    filing, for the companies where that reaches PAST their last full fiscal year.

    ⚠ ANNUAL ONLY. Every point of a quarterly series is already a trailing twelve months, so its
    newest one needs no separate name and adding it would duplicate the last column.

    ⚠⚠ QUALIFIED ON THE PERIOD-END **DATES**, NEVER ON THE LABELS. `2026-Q1` is later than `2025`
    for a December filer and IS `2025`'s successor for a March one — Prosus's FY2026 ends
    2026-03-31, which is 2026-Q1. Comparing labels would invent an LTM point for every off-calendar
    filer (a duplicate of the year already drawn) and miss it for others. `_values_with_dates`
    keeps the real period end, which is the only thing that answers "is there a newer filing".

    ⚠ IT COSTS ONE EXTRA BULK READ — the quarterly codes alongside the annual ones. That is the
    same read the quarterly view already does, and it is one chunked query, not one per company.
    """
    return _ltm_multi(company_ids, [metric], cadence).get(metric, {})


def _ltm_multi(company_ids: list[int], metrics: list[str],
               cadence: str) -> dict[str, dict[int, tuple[str, float]]]:
    """`{metric: {company_id: (period_end, LTM value)}}` — `_ltm_by_company` for several metrics on
    ONE pair of bulk reads instead of two per metric.

    ⚠ IT EXISTS SO THE QUALIFYING RULE HAS ONE IMPLEMENTATION. The eleven ratio cards each need
    three or four lines rolled to the same window; asking metric by metric would have re-stated
    "is this quarter newer than the last fiscal year" — the rule with the off-calendar-filer trap in
    it (`_ltm_by_company`) — once per caller, which is how two cards come to disagree about whether
    a company has an LTM at all.
    """
    if cadence == "quarterly" or not company_ids or not metrics:
        return {}
    from routers._benchmark_fundamental_grid import _values_with_dates  # noqa: PLC0415

    annual = _values_with_dates(company_ids, metrics, "annual")
    quarterly = _values_with_dates(company_ids, metrics, "quarterly")
    out: dict[str, dict[int, tuple[str, float]]] = {}
    for metric in metrics:
        per_metric: dict[int, tuple[str, float]] = {}
        for cid, per in (quarterly.get(metric) or {}).items():
            dated = [(d, v) for d, v in per.values() if v is not None]
            if not dated:
                continue
            q_date, q_val = max(dated)
            last_annual = max((d for d, v in ((annual.get(metric) or {}).get(cid) or {}).values()
                               if v is not None), default=None)
            # Equal dates mean the trailing twelve months ARE that fiscal year: already on the chart.
            if last_annual is None or q_date > last_annual:
                per_metric[cid] = (q_date, q_val)
        out[metric] = per_metric
    return out


def _ltm_blend_rows(cids: list[int], metrics: list[str] | None,
                    cadence: str) -> list[dict]:
    """The `LTM` period's rows for `_blend_rows` — one per company per metric that has one.

    ⚠⚠ EVERY PATH INTO `_blend_rows` MUST CALL THIS, AND THE ONE THAT DID NOT IS WHAT BROKE THE
    CHART. `/fundamental-blend-metrics` has two reads: a NARROWED one (`_bulk_blend_rows`, taken
    when the request names `metrics` — the benchmark overlay) and a FULL one (`_company_metric_rows`
    per holding — the portfolio's own line, and the only one the SSE stream can use, since its unit
    of progress is the holding). The LTM rows used to be appended inside the narrowed branch, so a
    book on the Long Equity tab drew an index that ran a quarter past its own line, and the extra x
    rendered through `xToPeriod` as **"2026 Q2"** — a fake fiscal quarter on an annual axis, on the
    benchmark alone. Not an error, and not obviously a bug: it reads as "the index reported and we
    have not", which is exactly what it is not.

    `metrics=None` means EVERY line with a declared roll-up (`_TTM_RULE`) — the full read's
    counterpart to naming them. A metric without one cannot have a trailing twelve months at all
    (`_codes_and_rule` refuses to guess), so the list is the complete set, not a selection.

    ⚠ ONE `_ltm_multi` CALL, NOT ONE PER METRIC. It reads every named line in a single pair of bulk
    queries; the old per-metric loop paid two bulk reads per metric over the whole constituent set —
    eight of them for the four growth cards on a ~1,900-name ACWI.

    ⚠ THE PERIOD IS THE LITERAL DATE KEY `LTM`, and `ltm_date` RIDES ALONG. `year_bucket` is `d[:4]`,
    so `'LTM'` buckets to itself: a real period that sorts after every year (`'2026' < 'LTM'`) which
    the chained series picks up as one more step off the last drawn year. A member with no LTM of its
    own is CARRIED into it (its last fiscal year is still its latest twelve months), the same rule
    every other period follows. `ltm_date` is what stamps the blended point with a REAL quarter-end —
    `_blend_rows` would otherwise use `period_end("LTM")`, which is TODAY, putting the two lines'
    LTM points at different x on a chart that exists to compare them. See `_blend_rows`.
    """
    if cadence == "quarterly" or not cids:
        return []
    out: list[dict] = []
    for metric, per_company in _ltm_multi(cids, list(_TTM_RULE) if metrics is None else metrics,
                                          cadence).items():
        annual_code = _metric_codes(metric)[0]
        for cid, (d, val) in per_company.items():
            out.append({"company_id": cid, "metric_code": annual_code,
                        "target_date": "LTM", "numeric_value": val, "ltm_date": d})
    return out


def ltm_parts_by_company(cids: list[int], metric: str,
                         cadence: str) -> dict[int, list[dict]]:
    """`{company_id: [{date, value}, …]}` — the FILINGS each company's newest trailing year was
    built from. Behind the ⓘ on every cell of the drill-down's LTM column.

    ⚠⚠ THE WINDOW IS NOT "THE LAST FOUR". It is `k` consecutive filings where `k` is how often that
    company reports, and it is refused outright when they span more than `365(k−0.5)/k` days —
    because a hole reaches back past its own year and double-counts the period that comes round
    again, which still looks like four rows. So this does not re-derive the window: it asks
    `_ttm_by_period` for the one it actually used (`parts=`). A tooltip that explains a number with
    filings the number was not computed from is worse than no tooltip — it is checked once and
    believed thereafter.

    ⚠ THE QUALIFYING GATE STAYS IN `_ltm_multi`. Whether a company HAS an LTM at all — is its newest
    quarterly filing past its last full fiscal year — has the off-calendar-filer trap in it
    (Prosus's FY2026 ends 2026-03-31, which IS 2026-Q1), and that rule already has one home. This
    reads the answer and only adds the arithmetic behind it, so a row cannot show filings for a
    number the column does not print.

    ⚠ AND A QUARTER DROPPED AS IMPLAUSIBLE IS ABSENT (`_drop_quarter_outliers` runs first). What is
    shown is what the figure was computed from, which is deliberately not everything the vendor
    filed.
    """
    have = _ltm_multi(cids, [metric], cadence).get(metric, {})
    if not have:
        return {}
    _codes, rule = _codes_and_rule(metric, "quarterly")
    if rule is None:
        return {}
    raw = rows_by_metric(list(have), [metric], "quarterly").get(metric, {})
    out: dict[int, list[dict]] = {}
    for cid, (period_end, _val) in have.items():
        parts: dict[str, list[dict]] = {}
        _ttm_by_period(raw.get(cid, []), rule, key="date", parts=parts)
        if period_end in parts:
            out[cid] = parts[period_end]
    return out


def ltm_aligned(company_ids: list[int], metrics: list[str],
                cadence: str) -> dict[int, tuple[str, dict[str, float]]]:
    """`{company_id: (period_end, {metric: LTM value})}` for the derived-ratio cards — every line
    rolled to the SAME twelve months, or none of them.

    ⚠ THE DATE COMES BACK BECAUSE THE CHART NEEDS AN X. `periodToX` turns a period label into a
    fractional year and `Number("LTM")` is NaN — a valid Map key, which is how nine charts once
    collapsed onto one point without erroring. The client places the LTM point at this real
    quarter-end, the same x the growth cards use, so a card's own line and the benchmark beside it
    cannot sit at different places on the axis while claiming the same period.

    ⚠⚠ A RATIO NEEDS ITS LEGS OVER ONE WINDOW, AND A MISMATCH RENDERS AS A PLAUSIBLE PERCENTAGE.
    Companies do not file every line on the same schedule: revenue can roll to a 30 June quarter-end
    while gross profit is only complete to 31 March. Dividing the first by the second is a
    fourteen-month numerator over a twelve-month denominator, and the result is a margin that looks
    entirely ordinary — there is no shape on the chart that says "these two came from different
    windows". So a company whose lines disagree on the period end gets NO LTM point at all rather
    than a blended one; the last full fiscal year is still drawn, and the chart is a year short
    instead of quietly wrong.

    ⚠ A LINE WITH NO LTM IS NOT A MISMATCH. A company that reports no SBC has no SBC to roll, which
    is the same absence the annual series already carries for that metric — the caller handles it
    exactly as it handles a missing year. Only two DIFFERENT dates disqualify.
    """
    per_metric = _ltm_multi(company_ids, metrics, cadence)
    if not per_metric:
        return {}
    out: dict[int, tuple[str, dict[str, float]]] = {}
    for cid in company_ids:
        got = {m: hit for m in metrics if (hit := per_metric.get(m, {}).get(cid))}
        dates = {d for d, _ in got.values()}
        if not got or len(dates) != 1:
            continue
        out[cid] = (dates.pop(), {m: v for m, (_, v) in got.items()})
    return out


def _attach_ltm(cid: int, ltm: dict[int, tuple[str, dict[str, float]]],
                series: dict[str, dict[str, float]]) -> str | None:
    """Writes each line's LTM value into its per-period dict under the literal period `LTM`, and
    returns the quarter-end it was rolled to (None when this company has no aligned LTM).

    ⚠ THE PERIOD IS A STRING KEY, WHICH IS WHY IT NEEDS NO PLUMBING. `'LTM'` sorts after every year
    (`'2026' < 'LTM'`) and clears the `>= "2015"` floor, so the caller's `years` set, the client's
    period columns and the chart's x order all pick it up as one more period. The alternative — a
    parallel `ltm` field beside the series — would need every consumer to learn about it, and the
    ones that did not would silently draw a chart that stops a year early.
    """
    hit = ltm.get(cid)
    if not hit:
        return None
    date, values = hit
    for metric, by_period in series.items():
        value = values.get(metric)
        if value is not None:
            by_period["LTM"] = value
    return date


def _drop_superseded_forecasts(by_metric: dict[str, dict[int, dict[str, float]]]) -> None:
    """Remove each company's forecast points for years it has ALREADY REPORTED. In place.

    ⚠⚠ THE VENDOR KEEPS THE PRE-ANNOUNCEMENT CONSENSUS, IT DOES NOT REPLACE IT WITH THE RESULT.
    Measured 2026-08-14 on the three companies that had reported a year they were also forecast for:

        cid 280  FY2026   estimate 5.69    reported 5.781
        cid 296  FY2026   estimate 16.78   reported 17.331

    So an estimate row on a closed year is a SUPERSEDED number, not the answer — and blended into
    the forecast leg it draws a figure we know to be wrong beside figures nobody knows yet.

    ⚠ IT MATTERS MOST FOR OFF-CALENDAR FILERS AND IT GROWS THROUGH THE YEAR. KLA files in June, so
    its FY2026 is history while every December filer's is still ahead; come next spring the December
    names report and the same forecast year fills up with stale consensus for them too. The point
    would quietly turn from an expectation into a mixture of expectation and out-of-date guesses,
    with nothing on screen marking the change.

    ⚠ THE COMPANY'S ACTUAL IS NOT SUBSTITUTED IN ITS PLACE. It is already on the solid line at that
    year, and moving it onto the forecast leg would make one point mean "expected" for some members
    and "reported" for others — a line whose meaning depends on who is in it. A member that has
    reported simply stops contributing to the forecast, which is what "no longer a forecast" means.

    ⚠ AND IT LIVES HERE, IN `_blend_rows`, BECAUSE BOTH READS PASS THROUGH IT — the narrowed
    benchmark blend and the full per-holding one. Applied in either fetch instead, the two paths
    would disagree about the same company. The drill-down table applies the same rule when it builds
    its `…e` columns, so the table cannot show a different set of forecasts from the line it explains.
    """
    for fc_code, base_code in _FORECAST_BASE.items():
        fc = by_metric.get(fc_code)
        base = by_metric.get(base_code)
        if not fc or not base:
            continue
        for cid, points in fc.items():
            # ⚠⚠ `LTM` IS IN THIS DICT AND IT IS NOT A DATE — `_ltm_blend_rows` emits the trailing
            # year under the SAME annual code, keyed by the literal string. A plain `max()` over the
            # keys therefore returns `'LTM'` ('L' beats '2'), and `d <= 'LTM'` is true of every real
            # date, so the first version of this deleted EVERY forecast for 22 of 40 companies and
            # the line simply vanished. The same trap `period_end`, `periodToX` and
            # `HoldingsRevenueModal.periodEndDate` each carry their own ⚠ about.
            #
            # ⚠ AND IT WOULD BE THE WRONG BOUND EVEN IF IT PARSED. A trailing twelve months is not a
            # reported fiscal year; it ends mid-year, so it cannot say which years are closed.
            newest = max((d for d in (base.get(cid) or {}) if d[:4].isdigit()), default=None)
            if newest:
                for d in [d for d in points if d <= newest]:
                    del points[d]


#: The per-share lines whose euro total is `value x shares`. ⚠ DECLARED, NOT INFERRED FROM THE
#: NAME. A code ending in "per share" that is not in here keeps the growth chain rather than being
#: multiplied by a share count that may mean something else — the same rule `TTM_RULES` follows,
#: and for the same reason: the wrong guess produces a plausible number, not an error.
_AGGREGATABLE_PER_SHARE = frozenset({"fcf_ps", "eps_nri"})

#: Levels that are ALREADY a company total — no share count involved.
_AGGREGATABLE_TOTAL = frozenset({"revenue"})

#: Metrics whose SUM is meaningless for a financial, and the sectors that means.
#:
#: ⚠⚠ A BANK HAS NO MEANINGFUL FREE CASH FLOW. Its operating cash flow moves with DEPOSIT AND
#: LOAN FLOWS, so `FCF` swings by trillions with no economic content — PT Bank Mandiri showed
#: -1,278bn, -1,482bn and +3,909bn EUR in consecutive years. Averaging growth rates hid that
#: behind a -100% floor, a +10,000% cap and dilution across 1,700 members; a SUM has none of
#: those, and a handful of banks dominate the aggregate outright. Excluding financials from an
#: aggregate FCF is the standard treatment, not a workaround.
#:
#: ⚠ REVENUE AND NET INCOME ARE FINE FOR A BANK and are NOT excluded. The rule is per metric
#: because the defect is per metric: interest income is revenue, and a bank earns profit like
#: anything else. Excluding financials from every line would delete a fifth of the index from
#: charts that had nothing wrong with them.
#:
#: ⚠⚠ BOTH SPELLINGS, AND THIS IS THE TRAP THE PROJECT DOCS ALREADY RECORD. Yahoo speaks two
#: sector vocabularies — measured on ACWI, "Financials" (225 members) AND "Financial Services"
#: (78) are both present. Listing one leaves 78 banks in the sum and the exclusion looks like it
#: worked. Canonicalised through `_airs_portfolio_analysis._sector`, which is the one place that
#: knows the aliases.
#:
#: ⚠ REAL ESTATE IS DELIBERATELY NOT HERE. A REIT's FCF is distorted by property purchases in a
#: way that is arguable, not definitional, and dropping 71 more members is a judgement nobody
#: asked for. Revisit with a measurement, not by extending this set.
_NO_AGGREGATE_FOR_FINANCIALS = frozenset({"fcf_ps"})
_FINANCIAL_SECTORS = frozenset({"Financials", "Financial Services"})


def fundamental_totals(company_ids: list[int], metrics: list[str],
                       weight_by_cid: dict[int, float] | None = None,
                       caps: dict[int, dict[str, float]] | None = None,
                       cadence: str = "annual",
                       ) -> dict[str, dict[int, dict[str, float]]]:
    """`{metric_code: {company_id: {period: EUR}}}` — the euros each member contributes.

    ⚠⚠ NOTHING ON THE REQUEST PATH CALLS THIS. Its only caller is `scripts/acwi_fcf_growth.py`,
    which is where the corrected ACWI figures come from (FCF +7.56%/yr, revenue +4.60%/yr, on a
    fixed 1,240-member basket). It was briefly wired into `_blend_rows` and REVERTED 2026-08-25,
    because only one of the five blend paths was converted: the level line came back as an
    aggregate while `blend_matrix`, `_level_breakdown` and `benchmark_revenue` kept the growth
    chain, so the Contribution column decomposed a move the line no longer made. ⚠ THE WIRING IS
    ALL-OR-NOTHING — a benchmark drawn one way beside a portfolio drawn the other is two answers
    to one question in the same chart with nothing saying so. Keep it that way, or finish all five.

    ⚠⚠ THE TWO CONVERSIONS, NOT ONE, are what make an aggregate possible at all. A
    GuruFocus per-share figure is in the LISTING's trading currency, so it becomes a company
    total only after multiplying by the share count AND converting at the PERIOD's own rate. An
    ACWI cross-section is 26 currencies; summing them raw over-weights Japan by ~150x, and
    converting Apple's September year-end at 31 December's rate applies a rate struck three
    months after the figure.

    ⚠ THE SAME HELPERS `period_caps_eur` USES, deliberately. It is the only other place that
    turns a GuruFocus financial into EUR, so a second definition of "which currency is this filed
    in" would be a second thing to keep true.

    `weight_by_cid` + `caps` are the PORTFOLIO form: a book's claim on a fundamental is
    `w_i x F_i / cap_i`, not `F_i`. For an INDEX both are omitted — a cap-weighted index holds the
    same fraction of every company, so its claim is proportional to the plain sum. ⚠ THE PORTFOLIO
    FORM IS UNMEASURED: it is derived and implemented but has never been run against a real book,
    because the caller that would have exercised it is the reverted one above. ⚠ A MEMBER WITH NO
    CAP FOR A PERIOD IS LEFT OUT OF
    THAT PERIOD rather than falling back to another one: mixing bases inside one sum is the
    failure the per-period basis exists to remove.
    """
    from routers._benchmark_index import _fx_to_eur, _rate  # noqa: PLC0415

    wanted = [m for m in metrics
              if m in _AGGREGATABLE_PER_SHARE or m in _AGGREGATABLE_TOTAL
              or m in _AGGREGATABLE_FORECAST]
    if not wanted or not company_ids:
        return {}

    ccy: dict[int, str] = {}
    for i in range(0, len(company_ids), IN_CHUNK_SIZE):
        for c in (supabase.table("company")
                  .select("company_id,gurufocus_exchange:gurufocus_exchange(currency_code)")
                  .in_("company_id", company_ids[i:i + IN_CHUNK_SIZE]).execute().data or []):
            code = ((c.get("gurufocus_exchange") or {}) or {}).get("currency_code")
            if code:
                ccy[c["company_id"]] = code
    if not ccy:
        return {}
    fx = _fx_to_eur(set(ccy.values()), "2014-01-01", "2026-12-31")

    # ⚠ THE SECTOR COMES THROUGH THE ISIN BRIDGE, and its coverage is stated rather than assumed:
    # measured on ACWI it resolves 1,735 of 1,998 members and 97.9% of cap. A member with NO
    # sector is KEPT — refusing what we cannot classify would silently shrink the index, which is
    # the larger error of the two.
    financial: set[int] = set()
    if any(m in _NO_AGGREGATE_FOR_FINANCIALS for m in wanted):
        from routers._airs_portfolio_analysis import _sector  # noqa: PLC0415

        isin_by_cid: dict[int, str] = {}
        for i in range(0, len(company_ids), IN_CHUNK_SIZE):
            for c in (supabase.table("company").select("company_id,isin")
                      .in_("company_id", company_ids[i:i + IN_CHUNK_SIZE]).execute().data or []):
                if c.get("isin"):
                    isin_by_cid[c["company_id"]] = c["isin"].strip().upper()
        isins = sorted(set(isin_by_cid.values()))
        sector_by_isin: dict[str, str] = {}
        for i in range(0, len(isins), IN_CHUNK_SIZE):
            for r in (supabase.table("asset_grid").select("isin,sector")
                      .in_("isin", isins[i:i + IN_CHUNK_SIZE]).execute().data or []):
                if r.get("sector"):
                    sector_by_isin.setdefault(r["isin"].strip().upper(), r["sector"])
        financial = {cid for cid, isin in isin_by_cid.items()
                     if _sector(sector_by_isin.get(isin)) in _FINANCIAL_SECTORS}

    # ⚠⚠ ONE COMPANY, ONE ROW — AND A SUM IS WHERE THIS BITES HARDEST. A dual-class constituent
    # is two `company` rows (Alphabet A and Alphabet C, Samsung ordinary and preferred), and
    # BOTH carry the whole company's revenue, earnings and cash flow. Averaging growth rates
    # merely double-voted one company; SUMMING adds its entire income statement twice.
    #
    # Measured on ACWI: 42 names appear more than once, 43 extra rows, 5.83% of the index
    # fictional — Alphabet alone reads 7.60% of cap where it is 3.80%.
    #
    # ⚠ THE SAME KEY `_asset_benchmark.members` ALREADY DEDUPES ON, deliberately: the normalised
    # company name. A second notion of "same company" here would disagree with the one the
    # benchmark weights are built from, and the disagreement would show up as growth.
    #
    # ⚠ THE LARGEST CAP WINS, not the first row returned — otherwise which share class survives
    # depends on row order, and the answer changes between runs with nothing to show for it.
    keep: dict[int, bool] = {}
    best_by_name: dict[str, tuple[float, int]] = {}
    for i in range(0, len(company_ids), IN_CHUNK_SIZE):
        for c in (supabase.table("company").select("company_id,company_name,market_cap_eur")
                  .in_("company_id", company_ids[i:i + IN_CHUNK_SIZE]).execute().data or []):
            nm = (c.get("company_name") or "").strip().lower()
            mc = float(c.get("market_cap_eur") or 0)
            if not nm:
                # ⚠ AN UNNAMED ROW IS KEPT. It cannot collide with anything, and dropping what we
                # cannot key would shrink the index for a reason nobody could see.
                keep[c["company_id"]] = True
                continue
            prev = best_by_name.get(nm)
            if prev is None or mc > prev[0]:
                best_by_name[nm] = (mc, c["company_id"])
    for _mc, cid in best_by_name.values():
        keep[cid] = True
    company_ids = [c for c in company_ids if keep.get(c)]

    need_shares = any(m in _AGGREGATABLE_PER_SHARE or m in _AGGREGATABLE_FORECAST for m in wanted)
    shares = _metric_by_company_period(company_ids, "shares") if need_shares else {}

    def _shares_at(cid: int, period: str) -> float | None:
        """The share count to multiply a per-share figure by — filed, or the latest before it.

        ⚠⚠ THE AS-OF FALLBACK IS ONLY FOR A **FORECAST**, and it is the whole reason an estimate
        can be aggregated at all: nobody has filed a share count for a year nobody has lived. Held
        at the latest filed figure, `estimate × shares` is "the consensus applied to today's
        capital base" — which is what a forward earnings total means.

        ⚠ AND IT MAKES THE ACTUAL→FORECAST BOUNDARY EXACT, not approximately so. The latest filed
        share count IS the last actual period's, so the ratio that joins the two legs
        (`Σest(first forecast) ÷ Σactual(last actual)`) has the same `shares` on both sides and the
        step is a pure change in earnings rather than half a change in share count.

        ⚠ A FILED PERIOD NEVER TAKES THE FALLBACK. Only periods past the last filing do, so an
        actual year with a genuinely missing share count is still skipped rather than quietly
        valued at a neighbour's — the gap stays a gap.
        """
        per = shares.get(cid) or {}
        if period in per:
            return per[period]
        earlier = [p for p in per if p <= period]
        return per[max(earlier)] if earlier else None

    out: dict[str, dict[int, dict[str, float]]] = {}
    for metric in wanted:
        forecast = metric in _AGGREGATABLE_FORECAST
        per_share = forecast or metric in _AGGREGATABLE_PER_SHARE
        vals = _metric_by_company_period(company_ids, metric)
        # ⚠ NOT FOR A CONSENSUS — there is no trailing twelve months of a forecast, and asking
        # `_ltm_by_company` for one would roll estimate rows into a period the chart labels as
        # measured. Annual only; see the ⚠⚠ where it is used.
        ltm_by = ({} if (forecast or cadence == "quarterly")
                  else _ltm_by_company(company_ids, metric, cadence))
        for code in _metric_codes(metric):
            per_cid: dict[int, dict[str, float]] = {}
            skip = financial if metric in _NO_AGGREGATE_FOR_FINANCIALS else set()
            for cid, by_period in vals.items():
                if cid in skip:
                    continue
                cur = ccy.get(cid)
                if not cur:
                    continue
                got: dict[str, float] = {}
                for period, v in by_period.items():
                    # ⚠ THE KEY IS ALREADY THE FILING DATE (`target_date`, YYYY-MM-DD), so it
                    # IS the period end and needs no derivation — which is the whole point of
                    # keying on it: `period_end` would re-derive a date we were handed.
                    rate = _rate(fx, cur, period)
                    if rate is None:
                        continue
                    # ⚠ AS-OF ONLY FOR A FORECAST — see `_shares_at`. An actual period with a
                    # missing share count stays missing rather than borrowing a neighbour's.
                    n = ((_shares_at(cid, period) if forecast
                          else (shares.get(cid) or {}).get(period)) if per_share else 1.0)
                    if n is None:
                        continue
                    # ⚠⚠ DIVIDE, AND SCALE BY 1e6 — BOTH, AND I SHIPPED NEITHER. `_rate` returns
                    # UNITS PER EUR (IDR 19,640.83, JPY 184.09, USD 1.175), so multiplying
                    # inflates a rupiah filing by 19,641x. `period_caps_eur` does exactly
                    # `native / rate * 1e6` two hundred lines below and is the convention to
                    # match; mirroring its LOOKUP without its DIRECTION is how this got here.
                    #
                    # ⚠ IT WAS INVISIBLE ON A POSITIVE METRIC. Revenue and EPS are near-always
                    # positive, so the same error only inflated a positive sum and still produced
                    # a plausible CAGR. Only `fcf_ps`, where large negatives are ordinary, pushed
                    # the aggregate below zero and tripped the guard — which is the whole reason
                    # that guard ends the series instead of drawing on.
                    #
                    # ⚠ THE 1e6 IS NOT COSMETIC EITHER: GuruFocus financials are in MILLIONS and
                    # `shares` is a million-share count, so the product is millions-of-millions
                    # until it is scaled. It cancels in a ratio and does NOT cancel in a sum
                    # across companies, which is the only place it has ever mattered.
                    eur = (v * n / rate) * 1e6
                    if weight_by_cid is not None:
                        cap = ((caps or {}).get(cid) or {}).get(period)
                        if not cap:
                            continue
                        eur = (weight_by_cid.get(cid) or 0.0) * eur / cap
                    got[period] = eur
                # ⚠⚠ THE **LTM** PERIOD NEEDS ITS OWN EUROS, AND WITHOUT THEM THE NEWEST POINT ON
                # THE LINE IS A LIE RATHER THAN A GAP. `_metric_by_company_period` reads filed
                # `metric_data` rows, and LTM is not one — this app assembles it. So the fund map
                # has no `LTM` key, and `carry_forward` then holds the previous year's euros into
                # it (`period_end("LTM")` is TODAY, comfortably inside `_MAX_CARRY_DAYS`), drawing
                # the aggregate flat from the last fiscal year to LTM. Not missing, not empty —
                # flat, which reads as "nothing changed" for the one point everybody looks at.
                #
                # ⚠ ANNUAL ONLY, and `_ltm_by_company` already enforces it: every point of a
                # quarterly series is a trailing twelve months, so there is no separate LTM there.
                #
                # ⚠ SHARES AS-OF, like a forecast — the LTM window ends at the newest QUARTERLY
                # filing, past the last annual share count. Same `_shares_at`, same reasoning.
                ltm = ltm_by.get(cid)
                if ltm is not None:
                    ltm_date, ltm_v = ltm
                    ltm_rate = _rate(fx, cur, ltm_date)
                    ltm_n = _shares_at(cid, ltm_date) if per_share else 1.0
                    if ltm_rate is not None and ltm_n is not None:
                        ltm_eur = (ltm_v * ltm_n / ltm_rate) * 1e6
                        if weight_by_cid is not None:
                            per_caps = (caps or {}).get(cid) or {}
                            cap = per_caps.get("LTM") or (
                                per_caps[max(per_caps)] if per_caps else None)
                            ltm_eur = ((weight_by_cid.get(cid) or 0.0) * ltm_eur / cap
                                       if cap else None)
                        if ltm_eur is not None:
                            got["LTM"] = ltm_eur
                if got:
                    per_cid[cid] = got
            if per_cid:
                out[code] = per_cid
    return out


def _metric_by_company_period(company_ids: list[int], metric: str) -> dict[int, dict[str, float]]:
    """`{company_id: {period: value}}`, paged. ⚠ `.range()` — an index's history is far past any
    server row cap, and an unpaged read would silently aggregate a fraction of the members."""
    codes = list(_metric_codes(metric))
    out: dict[int, dict[str, float]] = defaultdict(dict)
    for i in range(0, len(company_ids), IN_CHUNK_SIZE):
        chunk = company_ids[i:i + IN_CHUNK_SIZE]
        off = 0
        while True:
            rows = (supabase.table("metric_data")
                    .select("company_id,target_date,numeric_value")
                    .in_("company_id", chunk).in_("metric_code", codes)
                    .gte("target_date", _BLEND_START)
                    .order("company_id").order("target_date")
                    .range(off, off + 999).execute().data or [])
            if not rows:
                break
            for r in rows:
                if r.get("numeric_value") is not None:
                    out[r["company_id"]][str(r["target_date"])[:10]] = float(r["numeric_value"])
            off += len(rows)
    return out

def aggregatable_metrics(metrics: list[str]) -> list[str]:
    """Which of these metric KEYS may be summed in euros. Empty input ⇒ every eligible metric.

    ⚠⚠ A METRIC IS AGGREGATABLE ONLY IF ITS FORECAST LEG IS TOO, BECAUSE OTHERWISE ONLY HALF OF
    ONE CHART WOULD BE.

    A forecast is a SEPARATE metric code (`annual_eps_nri_estimate`), blended by its own
    `blend_series` call and continuing the actual it extends. Aggregating the ACTUAL leg alone puts
    the two halves of ONE chart on two different scales: the actual ends at the euro-chain level
    and the forecast restarts near the per-share one. Measured on `eps_nri` (2026-08-25), that drew
    a vertical jump from LTM to 2026e. ⚠ IT WAS CAUGHT BY EYE, NOT BY A TEST — every unit here was
    individually right, and the defect lived in the seam between two `blend_series` calls that
    nothing asserts across.

    ⚠ DERIVED FROM `_FORECAST_METRIC` / `_AGGREGATABLE_FORECAST`, NOT HARDCODED. A forecast leg is
    aggregatable when `fundamental_totals` can build its euros (`estimate × latest filed shares`,
    at the latest FX — see `_shares_at`). Add a consensus for a metric whose euros cannot be built
    and this refuses the pair automatically, without anyone remembering to come back here.

    ⚠ AND IT IS DECIDED ONCE, NOT PER REQUEST. Keying it off "is the forecast code in this payload"
    would aggregate a narrowed request and not a full one — the same metric drawing two different
    lines depending on which chart happened to ask for it.
    """
    wanted = list(metrics) or list(_AGGREGATABLE_PER_SHARE | _AGGREGATABLE_TOTAL)
    return [m for m in wanted
            if m not in _FORECAST_METRIC or _FORECAST_METRIC[m] in _AGGREGATABLE_FORECAST]


def _totals_for(covered: list[dict], metrics: list[str], universe: str | None,
                cadence: str) -> dict[str, dict[int, dict[str, float]]]:
    """The euros behind the line, in the form this caller's members require.

    ⚠⚠ ONE HELPER, BECAUSE THE ALTERNATIVE SHIPPED ONCE AND WAS REVERTED. Five call sites blend
    these metrics, and converting some of them leaves the app drawing a benchmark on the aggregate
    beside a portfolio on the growth chain — two constructions in one chart, both plausible, with
    nothing saying so. Whatever this function decides, every path decides identically.

    ⚠ THE TWO FORMS DIFFER AND `fundamental_totals` KNOWS WHICH BY ITS ARGUMENTS. An INDEX holds
    the same fraction of every company, so its claim is the plain sum `F_i`. A PORTFOLIO holds its
    own weights, so its claim is `w_i·F_i/cap_i` — the weight it actually carries, times that
    company's fundamental per euro of it — which needs per-period caps.

    ⚠ A PORTFOLIO'S CAPS ARE LOADED HERE RATHER THAN LEFT TO CHANCE. Callers only compute `caps`
    for a universe (it is the weighting basis, and a holding weight has no cap behind it). Without
    them there is no `cap_i` to divide by and the book would silently fall back to the growth chain
    — exactly the mismatched pair the paragraph above exists to prevent.

    ⚠ IT TAKES METRIC **KEYS** AND RETURNS METRIC **CODES**, which is `fundamental_totals`'s own
    convention (`out[code]` for every `code in _metric_codes(metric)`). An empty `metrics` means
    "every aggregatable one" — what the callers that blend the whole set want, and what lets a
    caller holding CODES (`BLEND_METRICS`) ask for everything and look its codes up directly. ⚠ A
    caller passing a CODE where a key belongs matches nothing, and the path silently keeps the
    growth chain with no error anywhere; pass keys or pass nothing.
    """
    ids = [r["company_id"] for r in covered]
    if not ids:
        return {}
    wanted = aggregatable_metrics(metrics)
    # ⚠ AND THE CONSENSUS LEG OF EACH. A chart with a forecast is TWO `blend_series` calls and both
    # need euros, or the pair lands on two constructions — the LTM→2026e jump. `aggregatable_metrics`
    # has already refused any base whose forecast cannot be built, so this only ever adds legs that
    # `fundamental_totals` can actually price.
    wanted += [_FORECAST_METRIC[m] for m in wanted if m in _FORECAST_METRIC]
    book_caps = None
    if not universe:
        key = "__period_caps_eur"
        book_caps = cached_metric_reads(
            ids, [key], cadence, lambda _ms: {key: period_caps_eur(ids, cadence)})[key]
    return fundamental_totals(
        ids, wanted,
        weight_by_cid=(None if universe
                       else {r["company_id"]: float(r["weight_pct"] or 0) for r in covered}),
        caps=(None if universe else book_caps),
        cadence=cadence)


def _blend_rows(rows: list[dict], covered: list[dict],
                caps: dict[int, dict[str, float]] | None = None,
                cadence: str = "annual",
                totals: dict[str, dict[int, dict[str, float]]] | None = None) -> dict:
    """The blend itself, over rows already fetched. Pure of I/O, so the plain endpoint and the
    streaming one cannot drift: they differ only in HOW the rows arrive.

    `caps` is `period_caps_eur` for an INDEX — the market cap as at each fiscal period, so each
    period is weighted by what the constituents were worth THEN. `None` for a portfolio, where a
    holding weight is not a market cap and does not vary by period; `_weight_at` reads the absence
    as "one basis for every period". See `_fundamental_blend._weight_at`.

    `totals` is `{metric_code: {company_id: {period: EUR}}}` — the euros of the fundamental each
    member contributes. Where it is present for a metric, `blend_series` SUMS them instead of
    averaging per-member growth rates, and `_level_breakdown` decomposes the sum exactly; see the
    aggregate branch there. Absent, every metric keeps the growth chain.

    ⚠ IT ARRIVES AS A PARAMETER BECAUSE THIS FUNCTION IS PURE OF I/O, and building it needs two
    reads (share counts and FX). Computing it here would put I/O back into the one function whose
    docstring promises the plain endpoint and the streaming one cannot drift.

    ⚠⚠ `cadence` DECIDES THE PERIOD ALIGNMENT, AND IT IS NOT COSMETIC. Quarterly rows carry
    trailing-twelve-month points at four dates a year; aligned on the YEAR they collapse to the
    latest one (a quarterly toggle that silently draws an annual line) and — for an INDEX, whose
    `caps` are keyed `2025-Q3` — every weight lookup misses and the series comes back EMPTY.
    Measured 2026-08-12 on AEX quarterly Revenue: 22 constituents, 639 rows, zero points, while the
    table beside it showed 84–93% of the index reporting each quarter. See `quarter_bucket`.
    """
    from routers._fundamental_blend import (  # noqa: PLC0415
        blend_series, explain_empty, period_end, quarter_bucket, year_bucket,
    )

    bucket = quarter_bucket if cadence == "quarterly" else year_bucket
    # The newest constituent filing behind the LTM period — an "as of" for the blended point. Max,
    # not min: the point stands at the latest information any member has published.
    ltm_date = max((str(r["ltm_date"]) for r in rows if r.get("ltm_date")), default=None)

    by_metric: dict[str, dict[int, dict[str, float]]] = {}
    for r in rows:
        if r.get("numeric_value") is None:
            continue
        (by_metric.setdefault(r["metric_code"], {})
                  .setdefault(r["company_id"], {}))[str(r["target_date"])[:10]] = \
            float(r["numeric_value"])

    _drop_superseded_forecasts(by_metric)

    out: list[dict] = []
    notes: dict[str, dict] = {}
    # ⚠⚠ BASES BEFORE FORECASTS, AND THE ORDER IS LOAD-BEARING ON THE AGGREGATE PATH. A forecast
    # leg joins the line its actual reached (`continue_from`), so the actual has to have been
    # blended already. `by_metric` is in row-arrival order, which puts them either way round
    # depending on what the query returned first — a chart that joined correctly or not depending
    # on row order is the worst kind of intermittent.
    #
    # ⚠ It changes nothing on the growth path, where each leg is rebased per member and neither
    # needs the other's result.
    ordered = sorted(by_metric, key=lambda c: 1 if c in _FORECAST_BASE else 0)
    # `{base metric code: {"level": …, "period": …}}` — where each actual leg's line ended.
    joins: dict[str, dict] = {}
    for code in ordered:
        per_company = by_metric[code]
        # ⚠ A FORECAST INHERITS THE ANCHOR OF THE ACTUAL IT CONTINUES. Both are the same quantity
        # and the chart indexes them off ONE base, so rebasing them independently draws the forecast
        # restarting at 100 beside an actual that has run to 1,808 — a ~94% earnings collapse that
        # exists only in the arithmetic.
        base_code = _FORECAST_BASE.get(code)
        base_by_company = by_metric.get(base_code, {}) if base_code else {}
        # ⚠ THE EUR TOTALS FOR **THIS** METRIC ONLY. `totals` is keyed by metric code because a
        # per-share line and a level line become different quantities, and handing a member the
        # wrong metric's euros would sum revenue into an FCF chart with nothing to show for it.
        fund_for = (totals or {}).get(code, {})
        # ⚠ THE ACTUAL'S EUROS TRAVEL WITH THE FORECAST, so the aggregate can measure the real step
        # across the boundary rather than restarting the chain. The euro twin of `base_points`.
        fund_base_for = (totals or {}).get(base_code, {}) if base_code else {}
        blend_members = [{"weight": r["weight_pct"],
                          # Absent for a portfolio — see the `caps` note on this function.
                          **({"weights": caps.get(r["company_id"], {})} if caps else {}),
                          "points": per_company.get(r["company_id"], {}),
                          "fund_points": fund_for.get(r["company_id"], {}),
                          "fund_base_points": fund_base_for.get(r["company_id"], {}),
                          "base_points": base_by_company.get(r["company_id"], {})}
                         for r in covered]
        s = blend_series(blend_members, code, bucket,
                         continue_from=joins.get(base_code) if base_code else None)
        # ⚠ WHERE THIS LEG'S LINE ENDED, for the forecast that continues it. Recorded only on the
        # AGGREGATE path: the growth path joins per member through `base_points` and would be
        # double-continued by this.
        if s.get("aggregate") and s["points"]:
            joins[code] = {"level": s["points"][-1]["value"],
                           "period": s["points"][-1]["period"]}
        if not s["points"]:
            # ⚠ AN EMPTY SERIES IS NOT AN EMPTY DATABASE, AND THE CHART CANNOT TELL. Measured on a
            # real book's Dividends per Share: every holding carried the line and the card still
            # read "No dividend/share ingested" — the level rebase drops a series that starts at
            # 0.00. The reason travels with the (absent) series so the card states it instead of
            # sending the reader to re-ingest data they already have.
            why = explain_empty(blend_members, code, bucket)
            if why is not None:
                notes[code] = why
        for p in s["points"]:
            # The period key becomes a date the charts can plot. The month-end is a CONVENTION, not
            # a claim about anyone's year-end — members close on different days, which is exactly
            # why the blend aligns on a shared period in the first place. `2025` → 2025-12-31,
            # `2025-Q3` → 2025-09-30.
            #
            # ⚠⚠ THE LTM POINT IS THE EXCEPTION, TWICE OVER. It goes out under an `ltm__` code so
            # nothing that selects the annual code picks it up as an extra fiscal year, and it is
            # stamped with the NEWEST constituent filing behind it rather than `period_end("LTM")`
            # (today) — that date is what puts it at the same x as a company's own LTM point, which
            # is the entire reason the two lines can be read against each other there.
            if p["period"] == "LTM":
                out.append({"metric_code": "ltm__" + code[len("annuals__"):],
                            "target_date": ltm_date or period_end("LTM"),
                            "numeric_value": p["value"], "is_prediction": False})
                continue
            out.append({"metric_code": code, "target_date": period_end(p["period"]),
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
async def fundamental_blend_metrics(body: FundamentalCoverageRequest, request: Request):
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
        # ⚠ AN INDEX IS WEIGHTED BY THE CAP IT HAD IN EACH PERIOD; A BOOK BY ITS HOLDING WEIGHT.
        # Only a universe has a market cap history to weight by — and only for a universe is
        # `weight_pct` a cap share in the first place (see `_load_and_expand_members`), so this is
        # the same branch that decides what the weight MEANS, not a new one.
        # ⚠ THROUGH `cached_metric_reads`, NOT A BARE CALL. It is the same derived series
        # `period_caps_by_isin` reads under the same key, so the two blend endpoints and the
        # `/universe-period-caps` read collapse to one computation instead of three — measured at
        # 0.95s each on ACWI. It also means `invalidate()` drops it with everything else; a bare
        # call cached nothing and survived nothing.
        #
        # ⚠ THE ID SET IS THE KEY, AND THIS ONE IS `covered` (1,509) WHERE THE CARDS' IS THE
        # CANONICAL-ISIN SET (1,514). Those are legitimately different questions, so they are
        # different entries — the sharing this buys is between callers asking about the SAME
        # companies, which is the honest kind.
        caps = None
        if body.universe:
            ids = [r["company_id"] for r in covered]
            key = "__period_caps_eur"
            caps = cached_metric_reads(
                ids, [key], body.cadence,
                lambda _ms: {key: period_caps_eur(ids, body.cadence)})[key]
        totals = _totals_for(covered, list(body.metrics or ()), body.universe, body.cadence)
        if body.metrics:
            # ⚠ SAME BLEND, DIFFERENT READ. Only the fetch changes — `_blend_rows` is untouched, so
            # a narrowed request cannot blend by a different rule than a full one.
            rows = _bulk_blend_rows([r["company_id"] for r in covered], body.metrics, body.cadence)
            return _blend_rows(rows, covered, caps, body.cadence, totals)
        # The PORTFOLIO path takes the same cadence as the single-company one, through the same
        # roll-up — otherwise a book's Long Equity tab would ignore a toggle its holdings honour.
        load = (_ttm_metric_rows if body.cadence == "quarterly" else _company_metric_rows)
        rows = []
        for r in covered:
            rows += load(r["company_id"])
        # ⚠ AND THE LTM PERIOD, exactly as the narrowed branch above gets it. Without it this line
        # stops at its last full fiscal year while a benchmark blended through `_bulk_blend_rows`
        # runs a quarter further — see `_ltm_blend_rows`.
        rows += _ltm_blend_rows([r["company_id"] for r in covered], None, body.cadence)
        return _blend_rows(rows, covered, caps, body.cadence, totals)

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
    # ⚠ THE SAME LOADER AND THE SAME CADENCE AS THE PLAIN ENDPOINT. This path used to read
    # `_company_metric_rows` unconditionally and blend without a cadence — so a BOOK on the
    # quarterly toggle was served annual rows over the fast path and trailing-twelve-month ones only
    # when the stream failed and the POST fallback ran. Two answers for one toggle, and the one you
    # got depended on whether SSE worked.
    load = (_ttm_metric_rows if body.cadence == "quarterly" else _company_metric_rows)
    try:
        for i, r in enumerate(covered):
            rows += await asyncio.to_thread(load, r["company_id"])
            yield _sse({"type": "progress", "done": i + 1, "total": n, "name": r.get("name")})
        # ⚠ THE SAME LTM READ THE PLAIN ENDPOINT DOES, and it is NOT part of the per-holding loop:
        # `_ltm_blend_rows` is two bulk queries over the whole book, not one per company, so there is
        # no holding to report progress against. Omitting it here is what made the payload depend on
        # whether SSE worked — the failure this loader's `load` line already documents.
        rows += await asyncio.to_thread(
            _ltm_blend_rows, [r["company_id"] for r in covered], None, body.cadence)
        built = await asyncio.to_thread(_blend_rows, rows, covered, None, body.cadence)
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
    # ⚠ "WITHOUT NRI" IS THE POINT, NOT A DETAIL. GuruFocus publishes three EPS lines and they are
    # near-identical most years, which is exactly what makes picking the wrong one hard to notice:
    # `EPS (Diluted)` and `Earnings per Share (Diluted)` both include non-recurring items, so a
    # single impairment, disposal or tax settlement puts a spike in the series that says nothing
    # about the business — and on a LOG growth chart with a fitted trend, one such year bends the
    # CAGR for every year around it. This is the same line the Share-Price-vs-Owner-Earnings chart
    # uses as its earnings side (`_RG_OE_CODE`), so the two cannot disagree about what "earnings"
    # means.
    "eps_nri": ("annuals__Per Share Data__EPS without NRI",
                "annuals__per_share_data__EPS without NRI"),
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
    # ⚠⚠ A FORECAST, NOT A REPORTED LINE — the analysts' EPS estimate per forward fiscal year, which
    # is what lets the EPS card draw a dotted continuation of its own solid line.
    #
    # ⚠ IT IS THE `eps_nri` TWIN, DELIBERATELY. GuruFocus also publishes `annual_per_share_eps_estimate`,
    # and on almost every company the two agree to a cent (Apple 8.76 vs 8.77; measured here, one
    # company reads 23.23 vs 23.68 for FY2026) — which is exactly why the choice cannot be made by
    # eye. This card's ACTUAL line is `EPS without NRI`, so its forecast is the estimate of THAT
    # line; extending a without-NRI series with an including-NRI forecast would put a one-off
    # impairment on the wrong side of the join and nothing on the chart would say so. Both are
    # nevertheless anchored on the same base in `_FORECAST_BASE`, so either can be drawn as a
    # continuation — the anchoring is not what distinguishes them.
    #
    # ⚠ NAMED ONLY IN THE **NARROWED** READ. The unnarrowed path already pages `annual_%estimate`
    # (see `_company_metric_rows`), so a portfolio's own line has always carried these; a BENCHMARK
    # is a narrowed read and would silently get no forecast at all — one dotted line on the book and
    # none on the index, which reads as "the index has no expectations" rather than "we did not
    # ask". This key is what closes that asymmetry.
    #
    # ⚠ AND IT HAS NO `_TTM_RULE` ENTRY, ON PURPOSE. `_codes_and_rule` therefore refuses it on the
    # quarterly basis and it is simply omitted there — right, because an annual forecast has no
    # trailing-twelve-month reading and rolling one would invent quarters analysts never published.
    "eps_nri_estimate": ("annual_eps_nri_estimate",),
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
#: `{reported metric: its forecast metric}` — DERIVED from `_FORECAST_BASE`, never restated. That
#: map already says which forecast continues which line, by CODE; this is the same fact keyed by
#: `_METRIC_CODES` name, so a second declaration cannot drift from it. A forecast whose base or
#: whose own code has no metric key (`annual_per_share_eps_estimate`, `annual_dividend_estimate`)
#: is simply absent — nothing asks for it, so nothing has to be kept in step.
def _build_forecast_metric_map() -> dict[str, str]:
    by_code = {codes[0]: metric for metric, codes in _METRIC_CODES.items()}
    return {by_code[base]: by_code[fc] for fc, base in _FORECAST_BASE.items()
            if fc in by_code and base in by_code}


def _period_sort_key(period: str) -> tuple[int, str]:
    """Column/period order: reported periods, then `LTM`, then the forecast years.

    ⚠⚠ A PLAIN STRING SORT PUTS THEM IN THE WRONG ORDER, AND THE ORDER IS A CLAIM ABOUT TIME.
    `'LTM' > '2026e'` lexically ('L' beats '2'), so the trailing twelve months — the newest thing we
    actually know — would be listed AFTER five years nobody has lived. The same rule sorts the
    drill-down's columns and the chart's periods, so the table cannot present a different chronology
    from the line it explains.
    """
    if period == "LTM":
        return (1, "")
    if period.endswith("e"):
        return (2, period)
    return (0, period)


_FORECAST_METRIC = _build_forecast_metric_map()

# The CONSENSUS metrics whose euros can be built, so a chart's forecast leg aggregates alongside its
# actual one.
#
# ⚠⚠ IT IS DERIVED FROM THE BASE, NOT LISTED. A forecast is aggregatable exactly when the series it
# CONTINUES is — otherwise the two legs of one chart end up on two constructions, which is the bug
# this whole pair of sets exists to prevent (measured 2026-08-25: the actual ran to the euro-chain
# level and the forecast restarted near the per-share one, a vertical jump from LTM to 2026e).
#
# ⚠ DEFINED HERE, AFTER `_FORECAST_METRIC`, because module-level code runs top-down — the callers
# above read it at RUNTIME, which is why they can sit earlier in the file.
_AGGREGATABLE_FORECAST = frozenset(
    fc for base, fc in _FORECAST_METRIC.items()
    if base in _AGGREGATABLE_PER_SHARE or base in _AGGREGATABLE_TOTAL)
_REVENUE_CODES = _METRIC_CODES["revenue"]
_REVENUE_CODE = _REVENUE_CODES[0]   # for blend_kind() only — it classifies, it doesn't query


def _metric_codes(metric: str) -> tuple[str, ...]:
    """The GuruFocus codes behind a metric KEY. Unknown key ⇒ refused.

    ⚠⚠ IT USED TO FALL BACK TO REVENUE, AND THAT IS THE WORST POSSIBLE DEFAULT. Every caller here
    is a chart or a table that has already decided what it is showing and labelled it accordingly,
    so a key this dict does not carry is a TYPO — and answering a typo with a different, valid,
    plausible series means the label and the numbers come from two different questions with nothing
    on screen to say so.

    Measured 2026-08-17, and it took a reader noticing two figures disagree to find it: the Tables
    tab asked for `fcf_per_share` (the registry key is `fcf_ps`), so its row labelled "FCF / share
    CAGR" was the book's REVENUE growth. Bustelberg Offensief read **+19.0%** there against the
    Long Equity card's **+28.0%** on the same book, same window, same modal. Nothing errored,
    nothing was empty, and both numbers looked entirely reasonable — the only symptom was that they
    disagreed, and the tab's own footnote offered a *credible wrong explanation* for the gap
    (point-to-point vs the card's trend fit), which is what made it survive.

    ⚠ RAISING IS SAFE HERE PRECISELY BECAUSE EVERY CALLER PASSES A LITERAL. The keys come from
    `_METRIC_CODES` itself, from `CARDS[].benchmarkMetric` in the frontend, or from a default of
    `"revenue"`; there is no user-typed metric reaching this. The one HTTP surface that takes it as
    a query parameter (`portfolio-revenue-matrix`) validates it into a 422 before it gets here, so
    a bad request is answered rather than 500'd.
    """
    try:
        return _METRIC_CODES[metric]
    except KeyError:
        raise ValueError(
            f"unknown metric key {metric!r} — expected one of "
            f"{', '.join(sorted(_METRIC_CODES))}") from None


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
    # ⚠ `eps_nri` SUMS, like every other per-share FLOW. A trailing-twelve-month EPS is four
    # quarters of earnings added up — taking `last` would report ONE quarter under an annual label,
    # a quarter of the real figure, on the same axis as full fiscal years. Same trap `MetricGrowthCard`
    # documents for the LTM point it plots.
    "fcf_ps": "sum", "div_ps": "sum", "eps_nri": "sum",
    # Balances and market values — a point in time.
    "total_assets": "last", "total_equity": "last", "goodwill": "last",
    "long_term_debt": "last", "noncurrent_liabilities": "last",
    "market_cap": "last", "price_ps": "last",
    # Already an average / an annualised rate.
    "shares": "mean", "roic": "mean",
}


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


def _daily_closes_bulk(company_ids: list[int],
                       since: str = "2015-01-01") -> dict[int, dict[str, float]]:
    """{company_id: {date: close}} — the bulk twin of `_daily_closes`, in ONE read.

    ⚠⚠ THE DAILY BRANCHES WERE THE ONE PATH THAT NEVER GOT A BULK READ. Both yield cards carry
    "⚠ ONE BULK READ PER METRIC, NOT ONE PER COMPANY — a benchmark request carries an index's 489
    constituents, where the per-company path is 72s" directly above them, and then called
    `_page_metrics` once per company per code two lines below it. Measured 2026-08-18 on a
    19-holding book, LOCALLY: the dividend-yield card alone took 116 round trips and 4.58s. The
    FCF/SBC card reads four series per company, so it is worse. At eu-west-3 latency that is
    another 5-9s of pure network, and the same endpoints serve the BENCHMARK overlay — where the
    constituent count is not 19 but ~500 to ~2,000.

    ⚠ THROUGH `timeseries.load_series`, WHICH IS ONE `COPY`. That façade exists for exactly this
    read (`gf.close` = `company_id` + `metric_data`), and CLAUDE.md records the measurement it was
    built on: 1,080 ms via PostgREST paging against 89 ms via one COPY.

    ⚠⚠ GURUFOCUS'S CLOSE, NOT yfinance's, AND THE REASON IS CURRENCY. The Long Equity tab lives in
    the `company` world and its per-share lines are in the company's REPORTING currency;
    `asset_price` lives in the `asset_execution` world, reachable only by ISIN, and that bridge
    carries every wrong-listing hazard this repo documents — a US megacap priced on a thin German
    line, or `GBp` pence against fundamentals in `GBP`, which is a 100x error that still looks like
    a number. This series needs no bridge and no conversion: measured on ASML, GuruFocus's annual
    `Month End Stock Price` IS a sample of it (681.7 / 678.7 / 921.4 at the last three year-ends,
    ratio 1.0000), so swapping the annual point for the daily one changes the frequency and
    nothing else.
    """
    if not company_ids:
        return {}
    from timeseries import load_series  # noqa: PLC0415

    out: dict[int, dict[str, float]] = {}
    df = load_series(sorted(set(company_ids)), "gf.close", since, order=False)
    if df.empty:
        return out
    for cid, d, v in zip(df["entity_id"], df["date"], df["close"]):
        if v is not None:
            out.setdefault(int(cid), {})[str(d)[:10]] = float(v)
    return out


def _daily_metrics_bulk(company_ids: list[int], metrics: tuple[str, ...],
                        dates_by_company: dict[int, dict[str, float]],
                        ) -> dict[str, dict[int, dict[str, float]]]:
    """{metric: {company_id: {date: TTM value}}} — the bulk twin of `_daily_metric`.

    ⚠⚠ THE BULK PART IS THE READ, NEVER THE MATHS. `metrics_by_company_bulk` looks like the right
    twin for this and is NOT: it calls `_ttm_by_period` WITHOUT `key="date"`, so its keys are
    quarter LABELS (`2015-Q4`) where `_step_onto_dates` matches ISO dates (`2015-10-31`). Measured
    while writing this: the same 43 periods and identical values, and the daily series still came
    out 42 days shorter for one holding and one day shorter for most of the rest — because the step
    function could not line the labels up with trading days. Nothing errored and no cell was empty;
    the chart simply started later. So this reads the ROWS in bulk and then applies the SAME
    per-company arithmetic the per-company path did.

    ⚠ EACH COMPANY IS STEPPED ONTO ITS OWN TRADING DATES, from `dates_by_company`. A shared date
    axis would carry a holiday-closed listing's stale value onto a day it did not trade.
    """
    out: dict[str, dict[int, dict[str, float]]] = {m: {} for m in metrics}
    ids = sorted({c for c in company_ids if c in dates_by_company})
    if not ids:
        return out
    wanted = [m for m in metrics if _TTM_RULE.get(m) is not None]
    if not wanted:
        return out
    raw = rows_by_metric(ids, wanted, "quarterly")
    for metric in wanted:
        rule = _TTM_RULE[metric]
        by_company = raw.get(metric, {})
        for cid in ids:
            ttm = _ttm_by_period(by_company.get(cid) or [], rule, key="date")
            out[metric][cid] = _step_onto_dates(ttm, sorted(dates_by_company[cid]))
    return out


def _ttm_metric_rows(company_id: int) -> list[dict]:
    """The metric ROWS a growth card reads, rolled to trailing twelve months.

    ⚠ IT DOES NOT CARRY THE FILINGS BEHIND EACH WINDOW, AND SHOULD NOT. `_ttm_by_period` can report
    them (`parts=`) and they are free to compute — the quarters were read and rolled here anyway —
    but they are not free to SHIP: this returns ~19 lines x ~45 quarters, so attaching four filings
    to every one is ~140 KB per company of JSON, and this is called PER HOLDING on a blend. The one
    surface that shows them is the drill-down's LTM column, which needs exactly one window per
    company and fetches it there (`ltm_parts_by_company`).

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
        # The window each point was rolled from — computed only when a caller asked, see the ⚠⚠.
        for date, val in _ttm_by_period(rows, rule, key="date").items():
            # ⚠ `company_id` IS NOT DECORATION HERE. These rows also feed `_blend_rows`, which keys
            # every point by the company that reported it — without it a PORTFOLIO's growth cards
            # raised KeyError on `quarterly` (a 500 the moment a book switched cadence) while the
            # single-company path, which never blends, was fine. The synthesised rows have to
            # carry what the read they replace carried.
            out.append({"company_id": company_id, "metric_code": code, "target_date": date,
                        "numeric_value": val, "is_prediction": False})
    return out


def filings_per_year(dates: list[str]) -> int:
    """How many times a year this company reports, from the spacing of its own filings.

    ⚠⚠ NOT EVERY ROW UNDER `quarterly__…` IS A QUARTER, AND ASSUMING SO DOUBLE-COUNTS A WHOLE YEAR.
    Prosus NV files SEMI-ANNUALLY — every one of its 18 revenue rows lands on 03-31 or 09-30, never
    06-30 or 12-31 (fiscal year to March, interims to September) — so the four most recent rows are
    four HALF-years. Summed as "trailing twelve months" that read **13,983.9** against an FY2026
    annual figure of **8,394.8**: 1.67x, and a perfectly plausible number with nothing on screen to
    say it spans two years. Measured 2026-08-12; ASML, a true quarterly filer, has 114 rows on all
    four quarter-ends and is unaffected.

    The frontend's `/earnings` dashboard already learned this (`isQuarterlyCadence` in
    `earnings/utils.ts`, same 120-day rule and the same warning in its comment). This is the seam
    the Long Equity tab reads through, and it had no such gate.

    Returns 4 (quarterly), 2 (semi-annual) or 1 (annual-only, which some companies file into the
    quarterly codes). ⚠ THE MEDIAN, NOT THE MEAN — one restated or duplicated date is enough to
    drag an average across the 120-day boundary and reclassify a company's whole history.
    """
    if len(dates) < 2:
        # ⚠ REFUSED, NOT GUESSED AT 4. One filing carries no spacing, and calling it quarterly is
        # what produces a "trailing year" from three months. The caller emits nothing.
        return 0
    spans = [(_date.fromisoformat(b) - _date.fromisoformat(a)).days
             for a, b in zip(dates, dates[1:], strict=False)]
    gaps = sorted(d for d in spans if d > 0)
    if not gaps:
        return 0
    median = gaps[len(gaps) // 2]
    if median <= 120:
        return 4
    if median <= 220:
        return 2
    return 1


#: How far out of line with its own neighbours a single quarter may be before it stops being a
#: measurement. See `_drop_quarter_outliers`.
_QUARTER_OUTLIER_FACTOR = 50.0


def _drop_quarter_outliers(by_date: dict[str, float], who: str = "?") -> dict[str, float]:
    """Remove quarters that are not plausibly the same series as their neighbours.

    `who` is `company_id/metric_code` — FOR THE WARNING ONLY, and it is not decoration. ⚠⚠ A LOG
    LINE ABOUT DELETED DATA THAT DOES NOT SAY WHOSE DATA IS UNACTIONABLE. The first version printed
    the dropped dates, the values and the median, which is enough to see that something was removed
    and not enough to decide whether it should have been: the same warning fires from seven `*-inputs`
    endpoints at once, so a reader cannot even tell how many distinct series are involved, let alone
    look one up. Naming the row is what turns "a number was discarded" into a question with an
    answer.

    ⚠⚠ THE VENDOR'S QUARTERLY FEED CARRIES OCCASIONAL GARBAGE, AND A TTM SUM LAUNDERS IT INTO A
    CONFIDENT NUMBER. Measured 2026-08-13 on `EPS without NRI`:

        IAG      0.038  0.174  10,987.996  0.265  0.022  0.089  8,748.852  0.184   (annual: ~0.7-1.2)
        Workday  676    2.47   2.32   2.21   2.23   1.92   1.89   1.75            (annual: 9.23)

    Two of IAG's quarters are five orders of magnitude out. Summed into a trailing year that is an
    LTM of 10,988 against annual EPS under 1.20 — and because the ACWI line is a CAP-WEIGHTED index
    of REBASED members, that single 0.03%-weight constituent supplied **+390pp of the index's
    +411pp step**, taking the blended EPS line from 1,015 to 5,186 in one quarter. One bad cell,
    one visibly wrong benchmark, no error anywhere.

    ⚠ JUDGED AGAINST THE COMPANY'S OWN MEDIAN QUARTER, never an absolute threshold. "10,988 is too
    big" is not a fact about a number — some issuers genuinely report per-share figures in the
    thousands (KRW, JPY). It is a fact about IAG, whose other quarters are ~0.17.

    ⚠ AND THE FACTOR IS DELIBERATELY HUGE (50x). Real quarterly EPS swings hard — a loss quarter
    against a profitable one, a seasonal peak, a recovery year — and this must not become a
    smoother that quietly clips genuine volatility. The measured corruptions are 300x (Workday) and
    63,000x (IAG); anything a business actually did lands far below 50x. A guard that fires on real
    data would be worse than the bug.

    ⚠ THE MEDIAN IS OVER **ABSOLUTE** VALUES, so a series that is legitimately negative half the
    time still has a scale. A dropped quarter is not replaced: the windows containing it lose their
    TTM point (they no longer have `k` consecutive filings), which is the honest outcome — a hole
    rather than a fabricated year.

    ⚠⚠ AND BEING OVER THE BAR IS NOT ENOUGH — ONLY AN **ISOLATED** QUARTER IS DROPPED. Size alone
    cannot tell a bad cell from a business that changed size, and the second version of this guard
    was deleting the second one. Measured 2026-08-14 on a local run: a series with a median of 19.15
    reported `960.171 → 1,099.847 → 1,122.77` — its three NEWEST quarters, consecutive, each within
    ~15% of the next, and only 50-59x over. That is not garbage, that is a level shift (an
    acquisition, a redenomination, a reverse split), and dropping it removed the newest year from
    every card built on the line with nothing on screen to say so.

    The discriminator is SHAPE, not magnitude:

        a BAD CELL is alone      0.02   0.17   10,988   0.09   0.18      <- IAG, dropped
        a LEVEL SHIFT is a run   19.1   19.4    960     1,100  1,123     <- kept

    A corrupt value has healthy quarters either side and the series carries on at its own scale
    afterwards. A regime change arrives and STAYS. So a flagged quarter with a flagged NEIGHBOUR is
    kept — both of IAG's are still dropped (they are a year apart, with healthy quarters between),
    and so is Workday's single one.

    ⚠ THE COST IS EXPLICIT: two ADJACENT corrupt cells would now survive. That is accepted, because
    from the numbers alone a sustained excursion and a sustained change are the same thing, and when
    a guard cannot tell it must believe the data rather than delete it. The median is self-limiting
    in the same direction — once more than half the series sits at the new level it becomes the
    median and nothing is flagged at all.

    ⚠ A KEPT RUN IS STILL LOGGED, at WARNING, because it is not a non-event: downstream the blended
    level index chains WEIGHTED GROWTH between periods (`blend_series` / `step_growth`, which floors
    at −100% but has no ceiling), so a constituent that genuinely 50x's its revenue moves the index
    by its weight times ~4,900%. That is the correct arithmetic on correct data and it will still
    look startling on a chart; the line in the log is what connects the two.
    """
    if len(by_date) < 4:
        return by_date                      # too few to say what "out of line" means
    mags = sorted(abs(v) for v in by_date.values())
    mid = len(mags) // 2
    median = mags[mid] if len(mags) % 2 else (mags[mid - 1] + mags[mid]) / 2
    if median <= 0:
        return by_date                      # a mostly-zero series has no scale to judge against
    limit = _QUARTER_OUTLIER_FACTOR * median
    axis = sorted(by_date)
    flagged = {d for d in axis if abs(by_date[d]) > limit}
    if not flagged:
        return by_date
    run = _level_shift(flagged, axis)
    drop = flagged - run
    if run:
        _log.warning(
            "[earnings] %s: KEPT %d quarter(s) over %.0fx this series' own median of %.4g — %s. "
            "They are consecutive AND run to the newest filing, which is the shape of a level "
            "shift; an old shift would already BE the median. Not interpreted further — check "
            "whether this company restated, redenominated or split. Expect a real step in any "
            "index built on this line.",
            who, len(run), _QUARTER_OUTLIER_FACTOR, median,
            {d: by_date[d] for d in sorted(run)})
    if drop:
        # The fiscal position of each dropped quarter, e.g. {'Q4': 3}. ⚠ A COUNT, NOT A VERDICT:
        # a recurring spike can be a seasonal business OR the vendor filing an annual figure in a
        # quarterly slot, and IAG's confirmed garbage recurs annually too (2024-09, 2025-09, both
        # Q3) — so this cannot decide, it can only tell a human where to look.
        # ⚠⚠ GUARDED, BECAUSE A LOG LINE MUST NOT BE ABLE TO KILL THE THING IT DESCRIBES. This
        # parses a MONTH out of the key, and the first version assumed every key was an ISO date —
        # true of every production caller (`_ttm_by_period` builds them from `target_date`) and not
        # true of the tests, where a series is keyed "a".."e". `int("")` raises, so a diagnostic
        # nobody reads took the whole outlier guard down with it and three tests went red on a
        # sentence. Anything unparseable is simply not counted: the periodicity hint is a nudge
        # toward a human eye, never a reason for the guard itself to fail.
        seasons = Counter(f"Q{(int(d[5:7]) - 1) // 3 + 1}" for d in drop
                          if len(d) >= 7 and d[5:7].isdigit() and 1 <= int(d[5:7]) <= 12)
        repeat = max(seasons.values(), default=0)
        _log.warning(
            "[earnings] %s: dropped %d implausible quarter(s) — more than %.0fx this series' own "
            "median of %.4g, and not part of a run to the newest filing: %s.%s A TTM sum would "
            "have carried them into a confident annual figure.",
            who, len(drop), _QUARTER_OUTLIER_FACTOR, median,
            {d: by_date[d] for d in sorted(drop)},
            "" if repeat < 3 else
            f" ⚠ {repeat} of them fall in the SAME fiscal quarter ({seasons.most_common(1)[0][0]})"
            " — worth an eye: either a seasonal business or an annual figure in a quarterly slot.")
    return {d: v for d, v in by_date.items() if d not in drop}


def _level_shift(flagged: set[str], axis: list[str]) -> set[str]:
    """The flagged quarters forming an unbroken run to the NEWEST filing — the only shape a real
    level shift can still have by the time it is flagged.

    ⚠⚠ "ADJACENT" WAS NOT ENOUGH, AND THE SWEEP OF 2026-08-14 SHOWED IT: of eight kept runs, six
    were oscillating garbage that happened to land in consecutive quarters — one series ran
    `+120, +190, +182, −140, +148, −818, −1700, −120, −2184` against a median of 2.11 and was
    preserved as a "level shift". A level does not change sign five times.

    The extra condition is not a fitted heuristic, it follows from the median: a shift that happened
    and PERSISTED becomes the majority of the series and stops being flagged at all (the guard is
    self-limiting — see the caller). So a run that is still flagged can only be RECENT, and a recent
    shift necessarily includes the newest filing. A flagged run that ends mid-history is, by
    construction, an excursion that ENDED — which is exactly what a level shift is not.

    On the measured sweep this separates the set exactly: the two genuine plateaus survive (revenue
    19.15 → 960/1,100/1,123 and a share count 53 → 3,038/3,038/3,046, both running to 2026-03-31)
    and all six oscillating runs, every one of them ending in 2015-2017 with normal quarters after,
    are dropped.

    ⚠ A RUN OF ONE IS NOT A RUN. Workday's confirmed bad cell (676 against a median of 2.2) WAS the
    newest filing, so "reaches the end" alone would have kept it. Two points is the minimum at which
    "it stayed there" means anything.
    """
    run: set[str] = set()
    for d in reversed(axis):
        if d not in flagged:
            break
        run.add(d)
    return run if len(run) >= 2 else set()


def _ttm_by_period(rows: list[dict], rule: str, key: str = "label",
                   parts: dict[str, list[dict]] | None = None) -> dict[str, float]:
    """Quarterly rows → {period label: trailing-twelve-month value}, per `rule`.

    `parts` is an OUT parameter: pass a dict and it is filled with `{label: [{date, value}, …]}` —
    the filings each window was built from.

    ⚠⚠ AN OUT PARAMETER RATHER THAN A SECOND FUNCTION, BECAUSE THE WINDOW IS THE HARD PART. Which
    filings belong to a trailing year is not "the last four": it is `k` consecutive rows where `k`
    is how often THIS company reports, refused entirely when they span more than `365(k−0.5)/k`
    days because a hole would silently reach back past its own year (see below). A `_ttm_parts()`
    that re-derived that would be a second copy of the one rule on this page that is genuinely
    subtle — and the copy would drift, so a panel would explain a number with filings the number
    was not built from. Same reasoning as `blend_breakdown` sharing `_prepare` with `blend_series`.

    ⚠ AND `_drop_quarter_outliers` RUNS BEFORE THE WINDOWS, so a dropped quarter is absent from
    `parts` too. That is the point: the breakdown shows what the figure was computed from, which is
    not the same as everything the vendor filed.

    ⚠ A POINT NEEDS A FULL YEAR OF FILINGS OR IT IS NOT A TRAILING YEAR — which is `k` rows, where
    `k` is how often THIS company reports (see `filings_per_year`), not a hardcoded four. A
    company's first incomplete year produces no TTM point at all: emitting a partial one would draw
    a line that starts at a quarter of the level and "grows" 4x over its first year, which reads as
    the business quadrupling. `last` is the one rule that could tolerate a short window (a balance
    is a balance), but it is held to the same bar so every series on the tab starts at the same
    place; a debt ratio whose numerator begins three quarters before its denominator is worse than
    one that starts late.

    ⚠ AND THE WINDOW MUST NOT REACH BACK PAST ITS OWN YEAR. `k` CONSECUTIVE rows are a year only
    while the company files without interruption; where a period is missing they reach further, so
    the sum covers more than twelve months under a twelve-month label — and it double-counts the
    period that comes round again (a quarterly filer missing Q4 gets two Q1s and no Q4, which is
    still four rows and still looks right).

    So the bar sits halfway between a healthy span and a holed one: `k` consecutive filings span
    `(k-1)/k` of a year — 273 days quarterly, 183 semi-annual, 0 annual — and a hole adds at least
    another `365/k`. `365 * (k - 0.5) / k` is the midpoint (319 / 274 / 182), which clears fiscal
    drift by weeks and catches a missing period every time. A hole in the history is a hole in the
    line, not a bigger number.
    """
    by_date: dict[str, float] = {}
    for m in rows:
        v = m.get("numeric_value")
        if v is None:
            continue
        # Latest observation wins for a given quarter-end — same rule as the annual path.
        by_date[str(m["target_date"])[:10]] = float(v)
    # ⚠ OFF THE ROWS, NOT A PARAMETER. Every caller already has these on the rows it passed
    # (`_page_metrics` / `_rows_by_company` both select `company_id, metric_code`), and threading an
    # id through nine call sites is nine chances for one of them to pass the wrong one — a log line
    # that names the wrong company is worse than one that names none.
    first = rows[0] if rows else {}
    by_date = _drop_quarter_outliers(
        by_date, f"{first.get('company_id', '?')}/{first.get('metric_code', '?')}")
    dates = sorted(by_date)
    k = filings_per_year(dates)
    if not k:
        return {}
    max_span = 365.0 * (k - 0.5) / k
    out: dict[str, float] = {}
    for i in range(k - 1, len(dates)):
        first, last_d = dates[i - k + 1], dates[i]
        if (_date.fromisoformat(last_d) - _date.fromisoformat(first)).days > max_span:
            continue                             # a missing filing — see the ⚠ above
        window = [by_date[d] for d in dates[i - k + 1:i + 1]]
        if rule == "sum":
            val = sum(window)
        elif rule == "mean":
            # ⚠ `len(window)`, NEVER A LITERAL 4. Hardcoded, a semi-annual filer's already-annualised
            # rate came back HALVED — the quieter twin of the sum's doubling, and harder to spot
            # because a margin that reads 6% instead of 12% is still a believable margin.
            val = sum(window) / float(len(window))
        else:                                    # "last"
            val = window[-1]
        # Labelled by the quarter the window ENDS in — the period the figure is as-of. `key="date"`
        # keeps the REAL quarter-end instead, because a fiscal quarter need not end on a calendar
        # one and synthesising 03-31/06-30/09-30/12-31 would move every point of an off-calendar
        # filer.
        label = last_d if key == "date" else f"{last_d[:4]}-Q{(int(last_d[5:7]) - 1) // 3 + 1}"
        out[label] = val
        if parts is not None:
            parts[label] = [{"date": d, "value": by_date[d]} for d in dates[i - k + 1:i + 1]]
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
    want = [m for m in dict.fromkeys(metrics) if (m, cadence) not in cache]
    if not want:
        return
    # ⚠ DEDUPED ACROSS CONCURRENT REQUESTS, NOT JUST WITHIN THIS ONE. `_PREFETCH` is a contextvar,
    # so it only stops a single request re-reading a line it already has — but the Long Equity tab
    # fires ~13 requests AT ONCE and they overlap heavily: 30 metric reads of which only 18 are
    # distinct, with `sbc` wanted by five cards and `fcf` by four. `cached_metric_reads` collapses
    # those to one read plus four waits.
    #
    # ⚠⚠ AND IT READS THE ONES THIS CALLER CLAIMS IN **ONE** QUERY. Per metric, each read is its
    # own COPY — its own connect + TLS + auth against Supabase — so the tab paid 18 handshakes for
    # rows that sit side by side in `metric_data`. Measured on ACWI (1,949 constituents, the 18
    # distinct lines): 18 Postgres connections -> 1, values identical. Locally that is 1.68s ->
    # 1.56s because a Docker connection is ~2ms; the saving is the handshakes, so it lands in
    # production and barely shows here — the same shape as the fundamentals grid's own read.
    #
    # ⚠ THE BATCH IS NOT SIMPLY `want`. See `cached_metric_reads`: batching everything this
    # endpoint wants would re-fetch the lines the other twelve cards are already fetching, turning
    # 18 shared reads into 30 unshared ones. It batches what this caller MISSES and OWNS.
    for m, series in cached_metric_reads(
            company_ids, want, cadence,
            lambda ms: metrics_by_company_bulk(company_ids, ms, cadence)).items():
        cache[(m, cadence)] = series
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


def metrics_by_company_bulk(company_ids: list[int], metrics: list[str],
                            cadence: str = "annual") -> dict[str, dict[int, dict[str, float]]]:
    """`{metric: {company_id: {period: value}}}` — the BULK twin of `_metrics_by_company`.

    Same answer as calling that function once per metric, in one read instead of one per metric.
    The bucketing is the same expression, reached through the same `_codes_and_rule`, so the two
    cannot come to disagree about which observation a period is.

    ⚠ A REFUSED METRIC COMES BACK AS `{}`, NOT ABSENT — deliberately UNLIKE `rows_by_metric`, which
    omits it. That is the shape `_metrics_by_company` already returns for a quarterly line with no
    declared TTM roll-up, and matching it is what lets this drop into the caching layer without a
    third meaning for "nothing here": every requested metric gets a key, so a caller cannot tell a
    refusal from an empty read and neither can be mistaken for "not yet fetched".
    """
    raw_by_metric = rows_by_metric(company_ids, metrics, cadence)
    out: dict[str, dict[int, dict[str, float]]] = {}
    for metric in metrics:
        raw = raw_by_metric.get(metric)
        if raw is None:
            out[metric] = {}
            continue
        _codes, rule = _codes_and_rule(metric, cadence)
        out[metric] = {cid: (_ttm_by_period(rows, rule) if rule else _latest_per_year(rows))
                       for cid, rows in raw.items()}
    return out


def period_caps_eur(company_ids: list[int],
                    cadence: str = "annual") -> dict[int, dict[str, float]]:
    """`{company_id: {period: market cap in EUR}}` — the cap AS AT each fiscal period.

    The weighting basis for anything that aggregates a cross-section of history. `market_cap_eur`
    on `company` is TODAY's cap, and weighting 2018's revenue by it is look-ahead bias: measured on
    the S&P, 30.6% of index weight sits in the wrong place in FY2018, with NVIDIA carried at 7.35%
    of a year it was 0.68% of. GuruFocus publishes `Market Cap` per fiscal period, so the period's
    cap is READ rather than reconstructed from a price ratio.

    ⚠⚠ THE FX RATE IS THE PERIOD'S OWN END, WHICH IS THE ONLY REASON THIS CAN BE DONE AT ALL.
    GuruFocus reports financials in the LISTING's trading currency, per fiscal period — so an ACWI
    cross-section is 19 currencies and summing them raw over-weights Japan by ~150x. Apple's FY2025
    ends in September; converting it at 31 December's rate applies a rate struck three months after
    the figure. `_values_with_dates` keeps the real period-end date for exactly this, and this
    function is the reason it takes a list of metrics rather than one.

    ⚠⚠ THE RESULT IS ABSOLUTE EUR, NOT MILLIONS, AND THE CONVERSION IS THE WHOLE POINT OF THIS
    LINE. GuruFocus financials are in MILLIONS; `company.market_cap_eur` is a plain EUR amount, and
    every consumer of that field divides by 1e9 to print billions. Returning millions here would
    render a EUR 2.9tn company as "EUR 2,890" — a number small enough to look like a rounding
    convention rather than a factor of a million.

    ⚠ A NON-POSITIVE OR UNCONVERTIBLE CAP IS ABSENT, NEVER 0. Absent means "this company has no
    weight in this period and is left out of it"; a 0 would put it in the denominator as a company
    worth nothing. Same rule the grid follows for a missing rate.

    ⚠ THE CURRENCY IS LOOKED UP HERE, NOT PASSED IN, AND THAT IS DELIBERATE. A caller handing over
    the wrong map does not fail — it converts a JPY cap at the USD rate and returns a plausible
    number roughly 150x out, which then becomes a weight. The lookup is one chunked read against
    the same `gurufocus_exchange` join every other consumer uses, and it makes the function
    impossible to misuse.
    """
    from routers._benchmark_fundamental_grid import _values_with_dates  # noqa: PLC0415
    from routers._benchmark_index import _fx_to_eur, _rate  # noqa: PLC0415

    if not company_ids:
        return {}
    # ⚠⚠ THE CAP IS READ ANNUALLY WHATEVER THE CADENCE, AND SPREAD ACROSS THE YEAR'S QUARTERS
    # BELOW. One cap per stock per year, and its weight that year is its share of that year's
    # caps — which is the whole rule, and it is not the same as reading the cap quarterly.
    #
    # Read quarterly, the cap only exists in the quarters the company FILED, so a semi-annual filer
    # had no weight at all in Q1/Q3 and fell out of those periods. Worse at the front of the axis:
    # measured 2026-08-12 on the AEX, only **1 of 22** constituents had a 2026 cap (FY2026 is not
    # filed yet), so 2026-Q1 weighted ONE company and 2026-Q2 none.
    caps_cadence = "annual"
    currency_by_cid: dict[int, str | None] = {}
    for i in range(0, len(company_ids), IN_CHUNK_SIZE):
        for c in (supabase.table("company")
                  .select("company_id,gurufocus_exchange:gurufocus_exchange(currency_code)")
                  .in_("company_id", company_ids[i:i + IN_CHUNK_SIZE]).execute().data or []):
            currency_by_cid[c["company_id"]] = (
                ((c.get("gurufocus_exchange") or {}) or {}).get("currency_code"))
    dated = _values_with_dates(company_ids, ["market_cap"], caps_cadence).get("market_cap", {})
    dates = [d for per in dated.values() for d, _v in per.values()]
    if not dates:
        return {}
    fx = _fx_to_eur({c for c in currency_by_cid.values() if c}, min(dates), max(dates))
    out: dict[int, dict[str, float]] = {}
    for cid, per in dated.items():
        ccy = currency_by_cid.get(cid)
        for period, (date, native) in per.items():
            if not native or native <= 0:
                continue
            rate = _rate(fx, ccy, date)
            if rate is None:
                continue
            out.setdefault(cid, {})[period] = (native / rate) * 1e6
    # ⚠ ONE YEAR OF CARRY, IN THE PAYLOAD ITSELF. `_weight_at` resolves a missing period as-of, but
    # the CLIENT reads these caps by exact key (the ratio cards and the drill-down table), so a cap
    # only the server can resolve is a cap the two sides disagree about. Carrying the newest year
    # forward once puts the current — not yet filed — year in the map for everybody: measured on
    # the AEX, 1 of 22 constituents had a 2026 cap, so without this the current year weights one
    # company on the client and twenty-two on the server.
    for per in out.values():
        newest = max(per)
        per.setdefault(str(int(newest) + 1), per[newest])
    if cadence != "quarterly":
        return out
    # A year's cap applies to each of its quarters — one cap per stock per year, and its weight that
    # year is its share of that year's caps.
    return {cid: {f"{year}-Q{q}": cap for year, cap in per.items() for q in (1, 2, 3, 4)}
            for cid, per in out.items()}


def period_caps_by_isin(comp: dict[str, dict], universe: str | None,
                        cadence: str = "annual") -> dict[str, dict[str, float]]:
    """`{canonical ISIN: {period: cap in EUR}}` — `period_caps_eur` in the shape the card
    endpoints index by.

    ⚠ EMPTY FOR A PORTFOLIO, which is not a degenerate case but the correct answer. A holding
    weight is a share of a book, not a market cap; there is no cap history to weight its periods
    by, and `_weight_at` reads the absence as "one basis for every period".

    ⚠ THE READ IS SHARED, AND `cached_metric_reads` IS STILL WHY. Two callers now want the same
    caps for the same constituents — `/universe-period-caps` (once, for all ten benchmark cards)
    and `portfolio-revenue-matrix` (the drill-down, which renders the caps in its own cells and so
    keeps them inline). Keyed on (company set, cadence) that is ONE query plus one wait, exactly as
    it is for the metric lines.

    ⚠ IT USED TO HAVE ELEVEN CALLERS, one per card, and the sharing was doing MORE work than it
    looked: the read collapsed, but each card then serialised and shipped its own copy of the
    result — 29.9% of every ACWI payload, ten times over. See `/universe-period-caps`.
    """
    if not universe:
        return {}
    ids = sorted({c["company_id"] for c in comp.values() if c.get("company_id")})
    if not ids:
        return {}
    # ⚠ A DERIVED SERIES, CACHED THROUGH THE SAME PRIMITIVE AS A RAW ONE. The key is a metric name
    # the registry does not contain, deliberately — it is `market_cap` plus an FX conversion, and
    # caching the CONVERTED figure is the point: the conversion is the expensive, per-period-dated
    # half. `invalidate()` drops it with everything else when an ingest rewrites the caps.
    key = "__period_caps_eur"
    by_cid = cached_metric_reads(
        ids, [key], cadence, lambda _ms: {key: period_caps_eur(ids, cadence)})[key]
    return {isin: by_cid.get(c["company_id"], {})
            for isin, c in comp.items() if c.get("company_id")}


def rows_by_metric(company_ids: list[int], metrics: list[str],
                   cadence: str = "annual") -> dict[str, dict[int, list[dict]]]:
    """`{metric: {company_id: rows}}` for MANY metrics in **ONE** read.

    ⚠⚠ THE UNIT OF COST IS THE READ, NOT THE METRIC. `_rows_by_company` is already one bulk read
    per line, which is what made the benchmark tabs viable at all — but a caller wanting all
    nineteen fundamentals lines still paid nineteen of them, and on the COPY transport each one
    opens its OWN Postgres connection (`common.pg._run_copy` connects, sets `statement_timeout`,
    streams, disconnects). Nineteen TCP+TLS+auth handshakes to Supabase is most of a second before
    a single row moves, and the nineteen scans hit the same `(company_id, metric_code)` index over
    the same ~1,900 constituents. One `metric_code = ANY(...)` over the union does all of it once.

    ⚠ THE CODES ARE DISJOINT ACROSS METRICS, which is what makes the split back out unambiguous —
    `_METRIC_CODES` maps each line to its own GuruFocus spellings (there are two or three per line,
    for the capitalized and lowercase section cohorts) and no code appears under two keys. A code
    that ever did would land in both buckets here; it would also mean two lines are the same line.

    ⚠ ORDER IS PRESERVED, AND IT MATTERS. `_rows_by_company` sorts on
    `(company_id, target_date, metric_code)`; filtering a list keeps relative order, so each
    metric's per-company rows arrive exactly as its own query would have returned them. That is
    load-bearing rather than tidy: `_latest_per_year_dated` keeps the LAST row it sees for a
    period, so a different order can pick a different value.

    ⚠ A METRIC WITH NO TTM RULE IS ABSENT FROM THE RESULT on the quarterly basis, never present as
    an empty dict — `_codes_and_rule` refuses it rather than guessing a roll-up, and the caller
    must be able to tell "refused" from "we hold nothing".
    """
    codes_by_metric: dict[str, list[str]] = {}
    for metric in metrics:
        codes, _rule = _codes_and_rule(metric, cadence)
        if codes:
            codes_by_metric[metric] = codes
    if not codes_by_metric:
        return {}
    metric_by_code = {code: metric
                      for metric, codes in codes_by_metric.items() for code in codes}
    raw = _rows_by_company(company_ids, sorted(metric_by_code))
    out: dict[str, dict[int, list[dict]]] = {m: defaultdict(list) for m in codes_by_metric}
    for cid, rows in raw.items():
        for r in rows:
            metric = metric_by_code.get(r["metric_code"])
            if metric is not None:
                out[metric][cid].append(r)
    return out


def _rows_by_company(company_ids: list[int], codes: list[str]) -> dict[int, list[dict]]:
    """{company_id: rows} for the named codes across MANY companies — the read the benchmark work
    is bounded by. See `_metrics_by_company` for the measurement and for why this is chunked AND
    paged, ordered on a unique key, and advances by what came back.

    ⚠ ONE COPY FIRST, THIS PAGER AS THE FALLBACK. The paged path below is correct but its cost is
    ROUND TRIPS: PostgREST caps a response at 1,000 rows (a server setting, 1,000 on cloud), and
    one metric across ACWI is ~16,300 rows — ~20 requests, times 27 metric reads across the tab's
    cards. `rows_by_company_via_copy` streams the same rows over one connection and returns the
    identical shape, or None when it cannot run (no `SUPABASE_DB_URL`, no psycopg, any error) —
    so this is a pure speed-up with no second behaviour to keep in step.
    """
    fast = rows_by_company_via_copy(company_ids, codes, _BLEND_START)
    if fast is not None:
        return fast

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
    constituents' figures blended exactly the way a portfolio's is, so the two lines on the
    /Long Equity tab are one construction rather than two.

    ⚠⚠ IT **IS** A SUM OF ABSOLUTE REVENUES NOW, IN EUR — and the objection that used to sit here
    ("AEX constituents report in different currencies, so a euro total would silently add pounds to
    euros") was right about the hazard and wrong about the remedy. The hazard is real: Shell, RELX
    and Unilever file in GBP. The remedy is to CONVERT, at each period's own end rate, which is
    what `fundamental_totals` does — not to avoid summing. Avoiding it cost more than it saved:
    averaging per-member growth rates weighted by market cap is the wrong weight for a fundamental
    and is upward-biased, worth ~5pp/yr on ACWI revenue (~9.95% averaged against +4.60% summed).
    See the aggregate branch in `_fundamental_blend`. Years under the coverage floor are still
    dropped rather than drawn thin.

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
        # ⚠⚠ PER-PERIOD CAPS, NOT ONE OF TODAY'S (2026-08-25). This assembly passed only a scalar
        # `weight`, and `_weight_at` reads the absence of `weights` as "single basis" — correct
        # for a PORTFOLIO, whose holding weight has no history, and look-ahead bias for an INDEX.
        # Every historical step was weighted by the constituent's size TODAY.
        #
        # Measured on ACWI FCF/share (`scripts/diagnose_blend_steps.py`): NVIDIA's 2017->2018
        # step carried a 4.40% weight — its 2026 size — against the ~0.4% it actually was, and
        # one 2023->2024 NVIDIA step moved the whole line by +26.94pp. On a metric whose biggest
        # movers grew INTO the index, weight and growth are correlated by construction, so this
        # inflates the line with no cell being wrong and nothing on the chart to show it.
        #
        # ⚠ THE SAME CLASS AS THE ANCHOR-WEIGHT FIX one level down in `blend_series`: that one
        # corrected WHICH END of a step the cap is taken from, and it could only ever help a
        # caller that supplies per-period caps at all. This one does.
        #
        # ⚠ A MEMBER WITH NO CAP FOR A PERIOD IS DROPPED FROM THAT PERIOD, by `_weight_at` — not
        # fallen back to the scalar. Mixing the two bases inside one column is the failure the
        # whole per-period basis exists to remove.
        period_caps = period_caps_eur(ids, "annual")
        # ⚠⚠ THE EUROS, SO THIS LINE IS THE SAME CONSTRUCTION AS EVERY OTHER. This endpoint feeds
        # the /Long Equity benchmark leg, which is drawn beside a portfolio line built by
        # `_blend_rows` — the one pairing where two constructions in one chart would be invisible
        # and read as a real divergence. It is an INDEX (no `weight_by_cid`, no `caps`), so the
        # claim is the plain sum; see `_totals_for`.
        code = _metric_codes(metric)[0]
        totals = _totals_for([{"company_id": c} for c in ids], [metric], label,
                             "annual").get(code, {})
        for cid in ids:
            pts = {str(m["target_date"])[:10]: float(m["numeric_value"])
                   for m in raw_by_cid.get(cid, ())
                   if m.get("numeric_value") is not None}
            if pts:
                members.append({"weight": caps.get(cid, 1.0), "points": pts,
                                "fund_points": totals.get(cid, {}),
                                "weights": period_caps.get(cid) or None})
        blend = blend_series(members, code)
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
async def portfolio_revenue_matrix(body: FundamentalCoverageRequest, metric: str = "revenue",
                                   only_company_id: int | None = None):
    """Each equity the portfolio HOLDS: its weight, currency, and actual `metric` per fiscal year
    (2015 onwards), in the company's own reporting currency.

    ⚠ THE HOLDINGS, NOT AN INDEX. Members come from the portfolio (looked THROUGH any linked
    certificate via `_load_and_expand_members`), deduped by ISIN (a name held twice is one row with
    summed weight). Weight is the share of the WHOLE book (cash/bonds in the denominator, so the
    shown companies sum to under 100%). Holdings with no company row / no revenue are omitted —
    this lists the companies we can actually show revenue for.

    ⚠⚠ FOR AN INDEX IT LISTS **EVERY** CONSTITUENT (`all_constituents=True`), INCLUDING THE ONES THE
    LINE CANNOT USE. The weighted series drops a constituent with no stored market cap — it cannot
    be weighted — but a table called "everything behind the chart" that shows 22 of the AEX's 25
    hides its most useful fact: RELX, Shell and Unilever are LSE-listed, outside the GuruFocus
    subscription, and unreachable. They arrive at weight 0, so they change no average and no
    coverage figure; their cells say `Unsubscribed` and their weight column is blank, which is the
    difference between "in the index, not in the line" and "not in the index".

    ⚠⚠ `only_company_id` RE-READS ONE ROW, AND IT EXISTS BECAUSE THE PER-ROW REFRESH RE-READ ~1,700.
    Pressing Refresh on a constituent changes exactly that company's `metric_data`; the drill-down
    then reloaded the whole matrix to show it, which on ACWI means every constituent's series, every
    period cap and every LTM window — the most expensive read on the tab — to update one line of a
    table already on screen.

    ⚠ THE WEIGHTS ARE STILL COMPUTED OVER THE **WHOLE** MEMBERSHIP, and that is the entire subtlety.
    `weight_pct` is this company's share of the full book (`weight_by[ci] / total_w`); narrowing the
    member list instead of the READ would hand back a row weighted 100%, and the client would splice
    a confident wrong number into a column that is supposed to sum to the index. So the narrowing is
    applied AFTER the weights are known, and only to `comp` — the dict every expensive per-company
    read is keyed on.

    ⚠ THE RESPONSE IS THEREFORE NOT A WHOLE TABLE and must not be rendered as one: its `years` cover
    the one company, not the union. The caller merges the row and keeps its own columns.
    """
    from asset_pipeline.isin_alias import canonical_map  # noqa: PLC0415
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

    # ⚠⚠ VALIDATED HERE, BECAUSE THIS IS THE ONE PLACE `metric` ARRIVES AS A STRING FROM OUTSIDE.
    # It used to be resolved by a `.get(metric, revenue)` deep in `_metric_codes`, so a caller that
    # misspelt it got REVENUE back under whatever heading it had already written — see that
    # function for the measured case (a "FCF / share CAGR" row that was revenue, off by 9pp).
    # A 422 naming the valid keys turns the same typo into a message on the first request.
    if metric not in _METRIC_CODES:
        raise HTTPException(
            status_code=422,
            detail=f"unknown metric {metric!r} — expected one of {', '.join(sorted(_METRIC_CODES))}")

    members = await _load_and_expand_members(body, all_constituents=True)
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
                      .select("company_id,company_name,isin,gurufocus_ticker,financials_fetched_at,"
                              "gurufocus_exchange:gurufocus_exchange(exchange_code,currency_code)")
                      .in_("isin", canon[i:i + IN_CHUNK_SIZE]).execute().data or []):
                comp[c["isin"]] = c

        # ⚠ AFTER THE WEIGHTS, NEVER BEFORE — see the ⚠ in the docstring. Every expensive read below
        # is keyed on `comp`, so narrowing it here is what turns a whole-index rebuild into one row,
        # while `weight_by` / `total_w` still describe the full membership and the row keeps the
        # share it actually has.
        if only_company_id is not None:
            comp = {k: c for k, c in comp.items() if c.get("company_id") == only_company_id}

        rows: list[dict] = []
        years: set[str] = set()
        ltm = _ltm_by_company([c["company_id"] for c in comp.values() if c.get("company_id")],
                              metric, body.cadence)
        # ⚠⚠ THE FILINGS BEHIND EACH LTM CELL, AND THIS TABLE IS THE ONLY PLACE THEY CAN GO. Every
        # other column is a figure the company FILED; the LTM column is one this app assembled, out
        # of quarters that reach the browser nowhere else — the tab's "Quarterly" toggle looks like
        # the place to check and is not, because `_ttm_metric_rows` rolls those server-side too, so
        # it shows more trailing years rather than the filings under them. One extra bulk read, on a
        # panel that is already a click-through. See `ltm_parts_by_company`.
        ltm_parts = ltm_parts_by_company(list(ltm), metric, body.cadence)
        ltm_rule = _TTM_RULE.get(metric) if body.cadence != "quarterly" else None
        # ⚠⚠ THE ANALYSTS' CONSENSUS, AS ITS OWN COLUMNS PAST LTM — the same series the chart draws
        # striped, so the table explains the whole line rather than the part that has happened.
        #
        # ⚠ SUFFIXED `e`, NEVER MERGED INTO THE REPORTED YEAR. An off-calendar filer can have BOTH a
        # filed FY2026 and a FY2026 consensus; one key for the two would silently overwrite a figure
        # somebody reported with one nobody has yet. The suffix also survives into the header, so a
        # column cannot be read as an actual.
        #
        # ⚠ AND ONLY PAST THE NEWEST FILED PERIOD. GuruFocus keeps publishing an estimate for a year
        # already closed; drawn beside the actual it invites reading the gap as a surprise, which is
        # a different chart. Same rule the card's forecast leg applies.
        fc_metric = _FORECAST_METRIC.get(metric) if body.cadence != "quarterly" else None
        fc_by_company = (metrics_by_company_bulk(
            [c["company_id"] for c in comp.values() if c.get("company_id")],
            [fc_metric], body.cadence).get(fc_metric, {}) if fc_metric else {})
        # The cap as at each period, keyed by canonical ISIN — the weighting basis for the
        # card's benchmark line. Empty for a portfolio; shared across the eleven cards by
        # `cached_metric_reads`, so the tab pays for it once. See `period_caps_by_isin`.
        caps = period_caps_by_isin(comp, body.universe, body.cadence)
        # ⚠ ONE BULK READ FOR THE WHOLE SET, NOT ONE PER COMPANY — and here it is the difference
        # between a table and a timeout. This loop reads ONE metric per member through
        # `_metric_by_year`, which is ~160 ms each: measured 2026-08-04, the S&P's 489 constituents
        # took **64.5 s**; prefetched they are one chunked, paged query. The endpoint was fine while
        # only a 20-name book could reach it and became the slowest thing on the tab the day the
        # benchmark overlay let it be pointed at an index.
        _prefetch([c["company_id"] for c in comp.values() if c.get("company_id")],
                  (metric,), body.cadence)
        # ⚠ INDEX ONLY — the per-period cap is a weighting basis, and for a portfolio the weight is
        # a holding percentage with no market cap behind it (same branch as `market_cap_eur`
        # below). This is what lets the table show the cap the weight beside it was divided out of,
        # per period, instead of one of today's figures repeated across twelve columns.
        caps = (period_caps_eur([c["company_id"] for c in comp.values() if c.get("company_id")],
                                body.cadence)
                if body.universe else {})
        # ⚠⚠ THE EUROS PER PERIOD, SHIPPED TO THE CLIENT — this table's footer and its Contribution
        # column are computed in the BROWSER (`fundamentalBlend.ts::buildBlend`), so the client
        # needs the same `F_i(t)` the server line is built from. Without it the drill-down would
        # decompose a growth chain under a chart drawn as an aggregate: a table that reconciles
        # perfectly to a number nobody is looking at.
        #
        # ⚠ KEYED BY CANONICAL ISIN, LIKE `caps`, because that is what a row is keyed on here —
        # `fundamental_totals` answers by `company_id`, and handing the client that key would join
        # against nothing and silently draw the growth chain instead.
        #
        # ⚠ `only_company_id` NARROWS `comp`, SO THIS IS ALREADY ONE COMPANY on the per-row
        # refresh — the same reason every other read below is keyed on `comp` and not on `canon`.
        # ⚠ THE WEIGHT IS THE ONE THE ROW PRINTS — the share of the WHOLE book, over `total_w`.
        # `_totals_for` needs it for the portfolio form (`w_i·F_i/cap_i`), and a weight computed
        # over anything narrower would hand the browser a decomposition that sums to a different
        # total from the one in the weight column beside it.
        fund_members = [{"company_id": c["company_id"],
                         "weight_pct": 100.0 * weight_by[ci] / total_w}
                        for ci, c in comp.items() if c.get("company_id") and ci in weight_by]
        all_totals = (_totals_for(fund_members, [metric], body.universe, body.cadence)
                      if fund_members else {})
        by_cid = all_totals.get(_metric_codes(metric)[0], {})
        # ⚠⚠ BUCKETED INTO THE TABLE'S OWN PERIOD KEYS, AND THIS IS THE TRAP THAT SILENTLY DISABLES
        # THE WHOLE THING. `fundamental_totals` answers by FILING DATE (`2024-09-28`) because the
        # server-side blend re-buckets it inside `_prepare`; this payload's cells are keyed by
        # PERIOD (`2024`, or `2025-Q3`), the same keys `market_cap_by_period` uses. Shipped raw,
        # every client lookup misses, `fund_by_period` is effectively empty, and `buildBlend`
        # quietly draws the growth chain under a chart the server drew as an aggregate — with no
        # error, no empty cell, and two plausible lines.
        #
        # ⚠ LATEST FILING WINS WITHIN A PERIOD, exactly as `_latest_per_bucket` does it: a company
        # changing its year-end can file twice against one period and must contribute once.
        from routers._fundamental_blend import (  # noqa: PLC0415
            quarter_bucket, year_bucket,
        )
        bucket = quarter_bucket if body.cadence == "quarterly" else year_bucket

        def _by_period(dated: dict[str, float]) -> dict[str, float]:
            latest: dict[str, tuple[str, float]] = {}
            for d, v in dated.items():
                k = bucket(d)
                if k not in latest or d > latest[k][0]:
                    latest[k] = (d, v)
            return {k: v for k, (_d, v) in latest.items()}

        # ⚠ AND THE CONSENSUS COLUMNS TOO, under the SAME `…e` suffix the values use. The forecast
        # is a different metric code, so without this the drill-down's footer would stop at LTM
        # while the chart above it runs on through the estimate years.
        #
        # ⚠ AN ESTIMATE FOR AN ALREADY-FILED YEAR IS LEFT IN AND IS INERT: `rev` only keeps
        # `{p}e` past `newest_filed`, so such a key never enters `data.years` and the client never
        # looks it up. Filtering it here would need `newest_filed`, which is per-company and not
        # known until the row loop below.
        fc_by_cid = (all_totals.get(_metric_codes(fc_metric)[0], {}) if fc_metric else {})
        fund: dict[str, dict[str, float]] = {}
        for ci, c in comp.items():
            cid = c.get("company_id")
            if not cid:
                continue
            got = _by_period(by_cid.get(cid) or {})
            got.update({f"{p}e": v for p, v in _by_period(fc_by_cid.get(cid) or {}).items()})
            if got:
                fund[ci] = got
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
            # ⚠ THE SAME LTM POINT THE CHART DRAWS, so the table explains the line rather than a
            # truncated version of it — see `_ltm_by_company` for what qualifies as one.
            if c["company_id"] in ltm:
                rev["LTM"] = ltm[c["company_id"]][1]
            newest_filed = max((p for p in rev if p != "LTM"), default="")
            for p, v in (fc_by_company.get(c["company_id"]) or {}).items():
                if v is not None and p > newest_filed:
                    rev[f"{p}e"] = v
            years |= set(rev)
            # WHY revenue is missing, when it is: a company on an exchange outside the GuruFocus
            # subscription (Brookfield on TSX) can't be fetched at all → `unsubscribed`; one on a
            # subscribed exchange with nothing ingested → `no_data`; otherwise `ok`.
            exch = gx.get("exchange_code")
            subscribed = is_gf_subscribed_exchange(exch) if exch else None
            status = "ok" if rev else ("unsubscribed" if subscribed is False else "no_data")
            rows.append({
                "isin": ci, "name": c.get("company_name") or name_by.get(ci) or ci,
                # ⚠ A GENUINE `company.company_id`, WHICH IS NOT TRUE EVERYWHERE IN THIS APP — the
                # old constituent table carried an `analysis_id` under that name, and an id off one
                # of those rows 404s against `company` (see the ⚠⚠ on
                # `/api/benchmarks/company/{company_id}/fundamentals/ingest/job`). `comp` here is
                # read straight from `company`, so the drill-down's per-row refresh can key on it.
                # Sent on both paths — a portfolio's rows come from the same table as an index's.
                "company_id": c.get("company_id"),
                # ⚠ WHEN WE ASKED, SO AN EMPTY CELL CAN SAY WHICH KIND OF EMPTY IT IS. A period
                # ending before this date was covered by a real fetch — GuruFocus publishes nothing
                # for it. A period ending after it has never been asked about. NULL = never asked
                # at all, and every cell in the row is then the second kind.
                "financials_fetched_at": c.get("financials_fetched_at"),
                # The filings this row's LTM cell was rolled from, and the rule that rolled them —
                # empty for a company with no LTM, and on the quarterly basis where every cell is
                # already a trailing year and no cell is called LTM.
                "ltm_parts": ltm_parts.get(c["company_id"], []),
                "ltm_rule": ltm_rule,
                "weight_pct": round(100.0 * weight_by[ci] / total_w, 2),
                # ⚠ THE CAP THAT PERIOD, NOT TODAY'S — what each period's weight is
                # actually computed from. Sparse: a period with no filed cap is ABSENT,
                # and that company is left out of that period's average entirely rather
                # than weighted on a different basis from its neighbours.
                **({"market_cap_by_period": caps.get(ci, {})} if caps else {}),
                # ⚠ ABSENT, NOT `{}`, WHEN THERE ARE NO EUROS FOR THIS ROW. The client reads the
                # presence of this field as "this metric is drawn as an aggregate"; an empty map on
                # every row would claim the aggregate and then have nothing to sum, and the line
                # would come back empty rather than falling back to the growth chain.
                **({"fund_by_period": fund[ci]} if fund.get(ci) else {}),
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
                # ⚠ THE CAP AS AT EACH PERIOD — what the per-period weight is actually computed
                # from, so the division shown in the cell can be checked rather than trusted.
                # Sparse on purpose: a period with no filed cap is ABSENT, and the reader sees a
                # dash for both cap and weight because that company is left out of that period's
                # average entirely. Padding it with today's figure would mix two bases inside one
                # column with nothing on screen to tell them apart.
                **({"market_cap_by_period": caps.get(c["company_id"], {})}
                   if body.universe else {}),
            })
        rows.sort(key=lambda r: -r["weight_pct"])
        # ⚠ `_period_sort_key`, NOT A PLAIN SORT — 'LTM' beats '2026e' lexically, which would list
        # the newest thing we know AFTER five years nobody has lived. See the helper.
        out = {"years": sorted(years, key=_period_sort_key), "rows": rows,
               "holdings": len(members)}
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


@router.post("/api/earnings/universe-period-caps")
@cached_blend("universe-period-caps")
async def universe_period_caps(body: FundamentalCoverageRequest, request: Request):
    """`{canonical ISIN: {period: market cap in EUR}}` for one index — ONCE, for all ten cards.

    The weighting basis behind every benchmark line on the Long Equity tab: an index is weighted by
    the cap it HAD in that period, never today's (see `weightAt` / `_weight_at` for why — on the
    S&P, today's cap carries NVIDIA at 7.46% of a year it was 0.63% of).

    ⚠⚠ IT USED TO RIDE ALONG ON EVERY ROW OF ALL TEN `*-inputs` RESPONSES, WHICH IS THE SAME TABLE
    TEN TIMES. Measured 2026-08-19 on ACWI (1,514 constituents, annual): `market_cap_by_period` was
    **29.9%** of each payload — 0.485 MB of `margin-inputs`' 1.62 MB — so ~4.8 MB of the tab's
    13.21 MB was one cap table repeated. Gzip cannot see across separate responses, so compression
    did not touch it; only fetching it once does. The client splices it back onto the rows in
    `useBenchInputs`, so every card still computes both its lines with the identical helper over
    identically shaped rows — the invariant that whole design rests on is untouched.

    ⚠ THE SHAPE IS EXACTLY WHAT THE ROWS CARRIED, INCLUDING THE EMPTY ONES. A constituent we hold
    no cap for gets `{}`, not a missing key, because the client reads those two differently and it
    is not a subtlety it can recover: `{}` means "this company is out of every period's average"
    while ABSENT means "fall back to `weight_pct` for all of them". Ten rows silently switching
    from the first to the second is a benchmark line that still draws, still looks plausible, and
    is weighted wrongly.

    ⚠ INDEX ONLY — 422 for a portfolio rather than an empty answer. A holding weight is a share of
    a book, not a market cap, and there is no cap history to weight its periods by; the `*-inputs`
    endpoints send no `market_cap_by_period` at all for a book, which is what the client's fallback
    to `weight_pct` is for. An empty `{}` here would be indistinguishable from "the index has no
    caps stored", which is a real and different condition.

    ⚠ THE READ ITSELF IS NOT NEW WORK. `period_caps_by_isin` goes through `cached_metric_reads`, so
    the ten cards were already collapsing to ONE query plus nine waits — what they each paid for
    was SERIALISING and SHIPPING the result. This endpoint just gives that one read one caller.
    """
    from asset_pipeline.isin_alias import canonical_map  # noqa: PLC0415

    if not body.universe:
        raise HTTPException(
            status_code=422,
            detail="universe-period-caps is for an index only — a portfolio has no cap history to "
                   "weight its periods by, and its rows carry no market_cap_by_period at all.")
    members = await _load_and_expand_members(body)
    if not members:
        raise HTTPException(status_code=404, detail="no holdings")

    def _run() -> dict:
        # ⚠ THE SAME CANONICALISATION THE CARDS DO, and it has to stay the same: the client looks a
        # row's cap up by `row.isin`, which is the canonical ISIN each card emits. Resolving the
        # alias differently here would return caps under keys no row carries — every constituent
        # would fall to a null weight and the line would simply not draw.
        raw = sorted({(m.get("isin") or "").strip() for m in members if m.get("isin")})
        alias = canonical_map(raw)
        canon = sorted({alias.get(i, i) for i in raw})
        comp: dict[str, dict] = {}
        for i in range(0, len(canon), IN_CHUNK_SIZE):
            for c in (supabase.table("company").select("company_id,isin")
                      .in_("isin", canon[i:i + IN_CHUNK_SIZE]).execute().data or []):
                comp[c["isin"]] = c
        return {"caps": period_caps_by_isin(comp, body.universe, body.cadence)}

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/margin-inputs")
@cached_blend("margin-inputs")
async def margin_inputs(body: FundamentalCoverageRequest, request: Request):
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
        # The trailing twelve months, but only where all three lines roll to the SAME quarter-end —
        # see `ltm_aligned` for why a ratio across two windows is worse than a missing point.
        ltm = ltm_aligned([comp[ci]["company_id"] for ci in canon if ci in comp],
                          ["revenue", "fcf", "sbc"], body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            rev = _metric_by_year(c["company_id"], "revenue", body.cadence)
            fcf = _metric_by_year(c["company_id"], "fcf", body.cadence)
            sbc = _metric_by_year(c["company_id"], "sbc", body.cadence)
            _attach_ltm(c["company_id"], ltm, {"revenue": rev, "fcf": fcf, "sbc": sbc})
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
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows,
                "ltm_date": max((d for d, _ in ltm.values()), default=None)}

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/debt-ratio-inputs")
@cached_blend("debt-ratio-inputs")
async def debt_ratio_inputs(body: FundamentalCoverageRequest, request: Request):
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
async def cash_return_inputs(body: FundamentalCoverageRequest, request: Request):
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
async def interest_burden_inputs(body: FundamentalCoverageRequest, request: Request):
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
        # Both lines to the SAME quarter-end, or neither — see `ltm_aligned`.
        ltm = ltm_aligned([comp[ci]["company_id"] for ci in canon if ci in comp],
                          ["interest_expense", "operating_income"], body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            ie = _metric_by_year(c["company_id"], "interest_expense", body.cadence)
            oi = _metric_by_year(c["company_id"], "operating_income", body.cadence)
            _attach_ltm(c["company_id"], ltm, {"interest_expense": ie, "operating_income": oi})
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
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows,
                "ltm_date": max((d for d, _ in ltm.values()), default=None)}

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/sbc-ocf-inputs")
@cached_blend("sbc-ocf-inputs")
async def sbc_ocf_inputs(body: FundamentalCoverageRequest, request: Request):
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
        # Both lines to the SAME quarter-end, or neither — see `ltm_aligned`.
        ltm = ltm_aligned([comp[ci]["company_id"] for ci in canon if ci in comp],
                          ["sbc", "ocf"], body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            sbc = _metric_by_year(c["company_id"], "sbc", body.cadence)
            ocf = _metric_by_year(c["company_id"], "ocf", body.cadence)
            _attach_ltm(c["company_id"], ltm, {"sbc": sbc, "ocf": ocf})
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
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows,
                "ltm_date": max((d for d, _ in ltm.values()), default=None)}

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/capex-margin-inputs")
@cached_blend("capex-margin-inputs")
async def capex_margin_inputs(body: FundamentalCoverageRequest, request: Request):
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
        # Both lines to the SAME quarter-end, or neither — see `ltm_aligned`.
        ltm = ltm_aligned([comp[ci]["company_id"] for ci in canon if ci in comp],
                          ["capex", "revenue"], body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            capex = _metric_by_year(c["company_id"], "capex", body.cadence)
            rev = _metric_by_year(c["company_id"], "revenue", body.cadence)
            _attach_ltm(c["company_id"], ltm, {"capex": capex, "revenue": rev})
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
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows,
                "ltm_date": max((d for d, _ in ltm.values()), default=None)}

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/gross-margin-inputs")
@cached_blend("gross-margin-inputs")
async def gross_margin_inputs(body: FundamentalCoverageRequest, request: Request):
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
        # Both lines to the SAME quarter-end, or neither — see `ltm_aligned`. This is the pair the
        # rule was written for: a company can complete revenue for a quarter that its cost of sales
        # has not been broken out for yet, and the margin that falls out of the mismatch is a
        # believable number with nothing on the chart to mark it.
        ltm = ltm_aligned([comp[ci]["company_id"] for ci in canon if ci in comp],
                          ["gross_profit", "revenue"], body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            gp = _metric_by_year(c["company_id"], "gross_profit", body.cadence)
            rev = _metric_by_year(c["company_id"], "revenue", body.cadence)
            _attach_ltm(c["company_id"], ltm, {"gross_profit": gp, "revenue": rev})
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
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows,
                "ltm_date": max((d for d, _ in ltm.values()), default=None)}

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/cash-conversion-inputs")
@cached_blend("cash-conversion-inputs")
async def cash_conversion_inputs(body: FundamentalCoverageRequest, request: Request):
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
        # All three lines to the SAME quarter-end, or none — see `ltm_aligned`.
        ltm = ltm_aligned([comp[ci]["company_id"] for ci in canon if ci in comp],
                          ["fcf", "sbc", "net_income"], body.cadence)
        for ci in canon:
            c = comp.get(ci)
            if not c:
                continue
            gx = (c.get("gurufocus_exchange") or {}) or {}
            fcf = _metric_by_year(c["company_id"], "fcf", body.cadence)
            sbc = _metric_by_year(c["company_id"], "sbc", body.cadence)
            ni = _metric_by_year(c["company_id"], "net_income", body.cadence)
            _attach_ltm(c["company_id"], ltm, {"fcf": fcf, "sbc": sbc, "net_income": ni})
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
        return {"years": sorted(y for y in years if y >= "2015"), "rows": rows,
                "ltm_date": max((d for d, _ in ltm.values()), default=None)}

    return await asyncio.to_thread(_run)


@router.post("/api/earnings/fcf-sbc-yield-inputs")
@cached_blend("fcf-sbc-yield-inputs")
async def fcf_sbc_yield_inputs(body: FundamentalCoverageRequest, request: Request):
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
        cids = [comp[ci]["company_id"] for ci in canon if ci in comp]
        # ⚠ ONE BULK READ PER METRIC, NOT ONE PER COMPANY — see `_prefetch`. A benchmark
        # request carries an index's 489 constituents, where the per-company path is 72s.
        # ⚠⚠ AND THE DAILY BRANCH GETS ITS OWN, WHICH IT NEVER HAD. `_prefetch` loads the ANNUAL
        # codes; the daily loop below wants `close_price` and the QUARTERLY codes, so it matched
        # nothing here and fell through to FOUR paged reads per company. See `_daily_closes_bulk`.
        daily_close = _daily_closes_bulk(cids) if body.cadence == "daily" else {}
        daily = _daily_metrics_bulk(cids, ('fcf', 'sbc', 'shares'), daily_close)
        if body.cadence != "daily":
            _prefetch(cids, ('fcf', 'sbc', 'market_cap',), body.cadence)
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
                close = daily_close.get(c["company_id"]) or {}
                fcf = daily['fcf'].get(c["company_id"]) or {}
                sbc = daily['sbc'].get(c["company_id"]) or {}
                sh = daily['shares'].get(c["company_id"]) or {}
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
async def dividend_yield_inputs(body: FundamentalCoverageRequest, request: Request):
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
        cids = [comp[ci]["company_id"] for ci in canon if ci in comp]
        # ⚠ ONE BULK READ PER METRIC, NOT ONE PER COMPANY — see `_prefetch`. A benchmark
        # request carries an index's 489 constituents, where the per-company path is 72s.
        # ⚠⚠ AND THE DAILY BRANCH GETS ITS OWN, WHICH IT NEVER HAD. `_prefetch` loads ANNUAL
        # `div_ps`/`price_ps`; the daily loop below wanted `close_price` and the QUARTERLY div
        # codes, so it matched nothing here and fell through to one paged read per company per
        # code. Two bulk reads now serve every company — see `_daily_closes_bulk`.
        daily_close = _daily_closes_bulk(cids) if body.cadence == "daily" else {}
        daily_div = _daily_metrics_bulk(cids, ('div_ps',), daily_close).get('div_ps', {})
        if body.cadence != "daily":
            _prefetch(cids, ('div_ps', 'price_ps',), body.cadence)
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
                div = daily_div.get(c["company_id"]) or {}
                # ⚠ Only days the numerator reaches. `_step_onto_dates` drops anything before the
                # first reported period, so the close alone would put bare denominators on the
                # chart.
                price = {d: v for d, v in (daily_close.get(c["company_id"]) or {}).items()
                         if d in div}
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
