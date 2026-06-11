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
