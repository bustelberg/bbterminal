"""Benchmark CRUD + sector ETF tagging + price serving.

Endpoints:
    GET    /api/benchmarks                          list + price ranges + sector tag
    POST   /api/benchmarks                          create (fetches prices from GuruFocus)
    POST   /api/benchmarks/{id}/refresh             re-fetch prices for an existing row
    DELETE /api/benchmarks/{id}                     delete (cascades benchmark_price)
    PATCH  /api/benchmarks/{id}                     set / clear the GICS sector tag
    GET    /api/benchmarks/{id}/prices              full price series (paginated)

The sector tag here is what the `selection_mode='sector_etf'` momentum
strategy uses to look up which ETF to hold per picked sector. The DB
enforces a partial unique index on `sector` so only one benchmark can
carry each sector at a time.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from deps import supabase
from ingest.api_usage import track_api_call
from ingest.constants import DATA_CUTOFF
from ingest.prices import _fetch_price_from_api, _parse_price_series

router = APIRouter(tags=["benchmarks"])

# How many constituents the bulk fundamentals fill fetches at once.
#
# ⚠ CHOSEN FROM MEASUREMENT, NOT FROM A FEELING. Against the live GuruFocus API: 6 calls serially
# 15.42s, the same 6 on six threads 4.56s (3.4x), and twelve threads doubled throughput again to
# 2.64 calls/s with no 403 anywhere — the ceiling was never found. What WAS seen at twelve was one
# empty response in twelve, which is not a quota refusal and not proof of a limit either; eight
# keeps nearly all of the gain without probing for the edge, and `_one` retries an empty answer.
_FILL_WORKERS = 8


class CreateBenchmarkRequest(BaseModel):
    ticker: str
    name: str
    sector: str | None = None
    # ISIN for the ETF/bond. Optional — used to show the ISIN column on
    # /schedule for ETF holdings (which carry a negative company_id and so
    # can't resolve an ISIN from the `company` table).
    isin: str | None = None
    # Native trading currency (ISO code, e.g. USD/EUR). Auto-detected from
    # GuruFocus on add; shown on /schedule next to the ETF's local price.
    currency: str | None = None


class UpdateBenchmarkRequest(BaseModel):
    # Partial update: only the fields present in the request body are
    # applied (resolved via `model_fields_set`). For each, an empty string
    # is treated as "clear" (→ NULL) so the frontend needs no separate path.
    sector: str | None = None
    isin: str | None = None
    currency: str | None = None


async def _bulk_upsert_prices(benchmark_id: int, parsed: list[tuple[date, float]]) -> int:
    """Upsert a parsed price series into benchmark_price in batches of 500.
    Returns the number of rows loaded (after applying DATA_CUTOFF — the same
    cutoff as company prices, keeping dot-com-bubble history the strategy
    never references out of benchmark_price)."""
    rows = [
        {"benchmark_id": benchmark_id, "target_date": d.isoformat(), "price": p}
        for d, p in parsed
        if d >= DATA_CUTOFF
    ]
    batch_size = 500
    total_loaded = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        await asyncio.to_thread(
            lambda b=batch: supabase.table("benchmark_price")
            .upsert(b, on_conflict="benchmark_id,target_date")
            .execute()
        )
        total_loaded += len(batch)
    return total_loaded


@router.get("/api/benchmarks")
async def list_benchmarks():
    """List all benchmarks with price date range and sector tag."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("benchmark")
        .select("benchmark_id, ticker, name, sector, isin, currency, created_at")
        .order("name")
        .execute()
    )
    benchmarks = resp.data
    for b in benchmarks:
        bid = b["benchmark_id"]
        min_resp = await asyncio.to_thread(
            lambda bid=bid: supabase.table("benchmark_price")
            .select("target_date")
            .eq("benchmark_id", bid)
            .order("target_date")
            .limit(1)
            .execute()
        )
        max_resp = await asyncio.to_thread(
            lambda bid=bid: supabase.table("benchmark_price")
            .select("target_date")
            .eq("benchmark_id", bid)
            .order("target_date", desc=True)
            .limit(1)
            .execute()
        )
        b["price_from"] = min_resp.data[0]["target_date"] if min_resp.data else None
        b["price_to"] = max_resp.data[0]["target_date"] if max_resp.data else None
    return benchmarks


@router.post("/api/benchmarks")
async def create_benchmark(req: CreateBenchmarkRequest):
    """Create a benchmark and fetch its prices from GuruFocus."""
    ticker = req.ticker.strip().upper()
    name = req.name.strip()
    if not ticker or not name:
        raise HTTPException(400, "Ticker and name are required")

    existing = await asyncio.to_thread(
        lambda: supabase.table("benchmark").select("benchmark_id").eq("ticker", ticker).execute()
    )
    if existing.data:
        raise HTTPException(409, f"Benchmark {ticker} already exists")

    # ETFs are US-listed, so no exchange prefix needed on the GF symbol.
    data, log, _status = await asyncio.to_thread(_fetch_price_from_api, ticker, "NYSE")
    await asyncio.to_thread(track_api_call, supabase, "NYSE")
    if data is None:
        raise HTTPException(502, f"Failed to fetch prices for {ticker}: {log}")

    parsed = _parse_price_series(data)
    if not parsed:
        raise HTTPException(502, f"No prices parsed for {ticker}")

    row = {"ticker": ticker, "name": name}
    sector_clean = (req.sector or "").strip() or None
    if sector_clean:
        row["sector"] = sector_clean
    isin_clean = (req.isin or "").strip().upper() or None
    if isin_clean:
        row["isin"] = isin_clean
    currency_clean = (req.currency or "").strip().upper() or None
    if currency_clean:
        row["currency"] = currency_clean
    resp = await asyncio.to_thread(
        lambda: supabase.table("benchmark").insert(row).execute()
    )
    if not resp.data:
        raise HTTPException(500, "Failed to create benchmark")
    benchmark_id = resp.data[0]["benchmark_id"]

    total_loaded = await _bulk_upsert_prices(benchmark_id, parsed)
    return {**resp.data[0], "prices_loaded": total_loaded, "price_range": f"{parsed[0][0]} to {parsed[-1][0]}"}


@router.post("/api/benchmarks/{benchmark_id}/refresh")
async def refresh_benchmark(benchmark_id: int):
    """Re-fetch prices for an existing benchmark."""
    bm = await asyncio.to_thread(
        lambda: supabase.table("benchmark").select("*").eq("benchmark_id", benchmark_id).execute()
    )
    if not bm.data:
        raise HTTPException(404, "Benchmark not found")
    ticker = bm.data[0]["ticker"]

    data, log, _status = await asyncio.to_thread(_fetch_price_from_api, ticker, "NYSE")
    await asyncio.to_thread(track_api_call, supabase, "NYSE")
    if data is None:
        raise HTTPException(502, f"Failed to fetch prices: {log}")

    parsed = _parse_price_series(data)
    if not parsed:
        raise HTTPException(502, f"No prices parsed for {ticker}")

    total_loaded = await _bulk_upsert_prices(benchmark_id, parsed)
    return {"ticker": ticker, "prices_loaded": total_loaded}


@router.delete("/api/benchmarks/{benchmark_id}")
async def delete_benchmark(benchmark_id: int):
    """Delete a benchmark and its prices (cascade)."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("benchmark").delete().eq("benchmark_id", benchmark_id).execute()
    )
    if not resp.data:
        raise HTTPException(404, "Benchmark not found")
    return {"ok": True}


@router.patch("/api/benchmarks/{benchmark_id}")
async def update_benchmark(benchmark_id: int, req: UpdateBenchmarkRequest):
    """Partial update of a benchmark's `sector` and/or `isin`. Only the
    fields present in the request body are touched (empty string → clear).
    The DB has a partial unique index on sector so only one benchmark can
    carry each sector at a time."""
    provided = req.model_fields_set
    update: dict = {}
    if "sector" in provided:
        update["sector"] = (req.sector or "").strip() or None
    if "isin" in provided:
        update["isin"] = (req.isin or "").strip().upper() or None
    if "currency" in provided:
        update["currency"] = (req.currency or "").strip().upper() or None
    if not update:
        raise HTTPException(400, "Nothing to update (pass `sector`, `isin`, and/or `currency`).")
    try:
        resp = await asyncio.to_thread(
            lambda: supabase.table("benchmark")
            .update(update)
            .eq("benchmark_id", benchmark_id)
            .execute()
        )
    except Exception as e:
        msg = str(e)
        if "benchmark_sector_unique" in msg or "duplicate" in msg.lower():
            raise HTTPException(409, f"Another benchmark already tags sector '{update.get('sector')}'")
        raise
    if not resp.data:
        raise HTTPException(404, "Benchmark not found")
    return resp.data[0]


@router.get("/api/benchmarks/{benchmark_id}/prices")
async def get_benchmark_prices(benchmark_id: int, start_date: str = "", end_date: str = ""):
    """Get prices for a benchmark, optionally filtered by date range.
    Paginated to defeat Supabase's silent 1000-row limit — a typical ETF
    since 1998 has ~6,886 daily bars."""
    query = (
        supabase.table("benchmark_price")
        .select("target_date, price")
        .eq("benchmark_id", benchmark_id)
        .order("target_date")
    )
    if start_date:
        query = query.gte("target_date", start_date)
    if end_date:
        query = query.lte("target_date", end_date)

    rows: list[dict] = []
    page_size = 1000
    offset = 0
    while True:
        resp = await asyncio.to_thread(lambda o=offset: query.range(o, o + page_size - 1).execute())
        if not resp.data:
            break
        rows.extend(resp.data)
        if len(resp.data) < page_size:
            break
        offset += page_size

    return rows


# ── Reconstructed cap-weighted index (currently: the S&P 500) ──────────────────────────


class IndexMember(BaseModel):
    company_id: int
    company_name: str | None = None
    ticker: str | None = None
    isin: str | None = None
    currency: str | None = None
    weight_pct: float                 # as of the START of the year — see `_benchmark_index`
    return_local_pct: float
    return_eur_pct: float
    market_cap_eur: float
    start_date: str
    start_price: float
    end_date: str
    end_price: float
    # ⚠ PROVENANCE FOR `market_cap_eur`, WHICH IS TODAY'S CAP — AND `weight_pct` IS NOT FORMED
    # FROM IT. The weight uses the START-of-window cap, rolled back on the price move (weighting
    # by today's cap is look-ahead bias: measured, it turns the S&P's +9.10% into +21.70%). So
    # `market_cap_eur / Σ market_cap_eur` deliberately does NOT reproduce the Weight column, and
    # the row has to say so rather than leave a reader to discover it.
    #
    # `market_cap_checked_at` is when Yahoo was last asked — the Refresh button stamps every
    # constituent on every run. A cap is a fetched number with an age; without the date, a
    # three-week-old weighting is indistinguishable from today's.
    market_cap_native: float | None = None
    market_cap_currency: str | None = None
    market_cap_checked_at: str | None = None


class SplitAdjustment(BaseModel):
    """A price series we had to rescale. Surfaced, never applied silently."""

    company_name: str | None = None
    ticker: str | None = None
    factor: float


class ReconstructedIndex(BaseModel):
    """A cap-weighted index rebuilt from our own membership + prices + FX.

    Validated against the real thing: for 2026 YTD this returns +9.10% in USD against SPY's
    +9.02% — 8bp apart. It is NOT a replacement for SPY (which is exact); it exists so a
    benchmark is computed the same way a portfolio is, and is therefore comparable to one.
    """

    label: str
    year: int
    member_count: int
    priced_of_universe: str | None = None
    start_date: str | None = None
    as_of: str | None = None
    ytd_eur_pct: float | None = None
    ytd_local_pct: float | None = None
    members: list[IndexMember] = []
    split_adjusted: list[SplitAdjustment] = []
    note: str | None = None


@router.get("/api/benchmarks/index/{label}", response_model=ReconstructedIndex)
async def benchmark_reconstructed_index(label: str, year: int | None = None):
    """Cap-weighted YTD for a reconstructed index (`SP500`, `ACWI`, `AEX`), in EUR and local.

    Weights are as of the START of the period. Weighting by TODAY's market cap would be
    look-ahead bias — measured, it turns the S&P's +9.10% into +21.70%.

    ⚠ THE ASSET PATH, NOT THE GURUFOCUS ONE (2026-07-16). This panel's whole claim is that its
    numbers are comparable to the portfolios beside them — and those are priced from `asset_price`
    (yfinance). Pricing the benchmark from GuruFocus instead compared two price universes and
    called the difference alpha. It was also structurally unable to price two of the three
    indices: GuruFocus is blind to 31.96% of the AEX (Shell, Unilever, RELX are LSE rows with no
    GuruFocus market cap) and to ~7.8% of ACWI, and a cap-weighted rebuild redistributes that
    weight rather than losing it — the GuruFocus AEX printed +14.80% against the true +12.12%,
    and looked entirely plausible doing it. `_benchmark_index.compute_index` remains as the SPY
    cross-check (+9.05% vs SPY's +9.02%), which validates the METHOD; it is not the basis.
    """
    from routers._asset_benchmark import compute_index_async  # noqa: PLC0415

    return await compute_index_async(label, year)


async def _benchmark_refresh_stream(label: str):
    """The Refresh button's three steps, one SSE line each.

    A worker thread does the blocking work and pushes frames onto a queue the async side drains
    — the same shape as the AIRS scan, and for the same reason: this is minutes of paced Yahoo
    calls, not a request.
    """
    import queue as thread_queue  # noqa: PLC0415
    import threading  # noqa: PLC0415

    from routers._benchmark_refresh import refresh_benchmark  # noqa: PLC0415
    from routers._sse import sse_event, sse_message  # noqa: PLC0415

    q: thread_queue.Queue = thread_queue.Queue()

    def emit(msg_type: str, **kw):
        q.put(sse_event({"type": msg_type, **kw}))

    def run():
        try:
            emit("done", summary=refresh_benchmark(label, emit))
        except Exception as e:  # noqa: BLE001 — surface it, don't 500 a stream mid-flight
            q.put(sse_message("error", f"{type(e).__name__}: {e}"))
        finally:
            q.put(None)

    threading.Thread(target=run, daemon=True).start()
    while True:
        item = await asyncio.to_thread(q.get)
        if item is None:
            break
        yield item


@router.get("/api/benchmarks/index/{label}/refresh")
async def benchmark_refresh(label: str):
    """Refresh a reconstructed index: constituents, market caps, then two prices each.

    Exactly three steps, streamed line by line (see `_benchmark_refresh`):

        1. CONSTITUENTS  rebuild the universe if it has none, read its membership, bridge each
                         member into the asset world by ISIN, resolve what is not there yet.
        2. MARKET CAPS   a batched Yahoo quote for EVERY constituent — the cap IS the weight,
                         and a three-week-old cap is a three-week-old index.
        3. PRICES        each constituent's start-of-year close and its current close. Those two
                         numbers are the whole of the YTD the panel shows.

    ⚠ SSE, NOT A POST. Step 3 is one paced Yahoo call per constituent: 491 for the S&P, 1,684 for
    ACWI. That is minutes, and a button that hangs silently for eleven of them is
    indistinguishable from a broken one — so every step reports as it happens.

    ⚠ PRICES ARE FETCHED BY SYMBOL. Identity is decided in step 1 only, through the single paced
    queue worker; nothing in step 3 reopens the question of WHICH listing an instrument is (Yahoo
    answers an overloaded caller with an empty search, which is how Alphabet moved to a Vienna
    line 75,000x thinner).
    """
    from fastapi.responses import StreamingResponse  # noqa: PLC0415

    return StreamingResponse(
        _benchmark_refresh_stream(label),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


class BenchmarkResetResult(BaseModel):
    label: str
    deleted: bool = False
    members_deleted: int = 0
    # The other two thirds of Fill, undone so they can be watched running.
    caps_cleared: int = 0
    price_rows_deleted: int = 0
    # The date the price deletion starts from — the window's lookback, not 1 January: the opening
    # mark is the last close ON OR BEFORE the anchor, so 31 December is what a YTD actually opens at.
    prices_from: str | None = None
    # True when the deleted universe was template-managed — i.e. Fill will rebuild it.
    had_template: bool = False
    note: str | None = None


@router.delete("/api/benchmarks/index/{label}", response_model=BenchmarkResetResult)
async def benchmark_reset(label: str):
    """Delete the LIVE universe behind one reconstructed benchmark, so Fill can rebuild it.

    The inverse of Fill's first step, for watching the whole path run: the benchmark drops to 0
    members and the next Fill re-runs the label's template, re-enqueues what needs resolving and
    re-caps what is already priced.

    ⚠ MEMBERSHIP ONLY. Prices, the asset grid and market caps are shared with every other surface
    and expensive to rebuild — see `reset_benchmark`, which also refuses a frozen snapshot, a
    universe with derived children, and any label Fill has no template to rebuild (SP500).

    422 carries the refusal's reason; it is always a sentence about this label, not a generic error.
    """
    from routers._benchmark_fill import reset_benchmark  # noqa: PLC0415

    try:
        return await asyncio.to_thread(reset_benchmark, label)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e


class ConstituentFundamentalColumn(BaseModel):
    """One RAW GuruFocus line the Long Equity charts consume. Shipped with the data so the table
    renders the set the SERVER knows about — add a line backend-side and its columns appear
    without a frontend change, which is the only way the two cannot drift."""

    key: str
    label: str
    # Why the line matters, and its trap where it has one (a bank has no gross profit; interest
    # expense is reported negative). Shown on the column head rather than kept in the source.
    note: str | None = None


class ConstituentFundamentals(BaseModel):
    """The RAW GuruFocus lines per constituent, and the period span we hold for each.

    ⚠ `covered` IS NOT DECORATION. Only the members whose fundamentals have been ingested appear in
    `rows`; measured 2026-08-04, that was 92 of SP500's 503. A table that simply renders blanks for
    the rest reads as "these companies have no margins", which is a claim about the companies
    rather than about our ingest. The count says which it is.
    """

    label: str
    # ⚠ ECHOED BACK, so a row of spans can never be read under the wrong basis. The two cadences
    # give DIFFERENT periods for the same company ("2025" vs "2025-Q3") and different counts, and a
    # table that shows one while its toggle says the other is a silent lie about the data.
    cadence: str
    columns: list[ConstituentFundamentalColumn]
    members: int
    covered: int
    # ⚠ KEYED BY **ISIN**, NOT BY company_id — AND THAT IS A CORRECTNESS FIX, NOT A PREFERENCE.
    # The constituent table is served by `_asset_benchmark`, which puts the `analysis_id` (an
    # `asset_execution` row) into a field NAMED `company_id`, because it reuses
    # `_benchmark_index._window_rows` and that keys prices by that name. Fundamentals live in the
    # `company` world. The two id spaces are disjoint, so keying this by company_id matched nothing
    # the table could look up and EVERY cell rendered a dash — including the 92 companies that do
    # have data. ISIN is the one identifier both worlds carry, and it is the bridge the rest of the
    # app uses between them.
    rows: dict[str, dict]     # ISIN -> {line key: {from, to, n}}


@router.get("/api/benchmarks/index/{label}/fundamentals", response_model=ConstituentFundamentals)
async def benchmark_constituent_fundamentals(label: str, cadence: str = "annual"):
    """The twelve Long Equity measures for each of an index's constituents.

    ⚠ A SEPARATE CALL FROM `/index/{label}`, DELIBERATELY. That endpoint prices 500 constituents and
    is what the table needs to render at all; this one reads fourteen metric series. Folding them
    together would hold the whole table behind the slower half, so the prices land first and the
    fundamentals fill in — the same progressive shape the /schedule and holdings-count surfaces use.

    `cadence` is `annual` (fiscal years) or `quarterly` (TRAILING TWELVE MONTHS, the basis the tab
    plots) — see `constituent_fundamentals`, which owns what each one means and why the quarterly
    span starts three quarters late. It is a VIEW over data one GuruFocus call already brought;
    switching it never spends quota.
    """
    from routers._benchmark_fundamentals import (  # noqa: PLC0415
        COLUMNS, constituent_fundamentals, normalise_cadence,
    )
    from routers._benchmark_index import _members  # noqa: PLC0415

    # ⚠ NORMALISED, NEVER PASSED THROUGH — see `normalise_cadence` for the failure it prevents.
    cad = normalise_cadence(cadence)

    def _run() -> dict:
        # `_members` is the COMPANY-world list: it carries the real `company_id` the metrics are
        # stored against AND the ISIN the table can be joined on. A member with no ISIN cannot be
        # bridged and is simply absent — honest, since nothing could look it up either.
        members = _members(label)
        by_cid = {m["company_id"]: (m.get("isin") or "").strip().upper()
                  for m in members if m.get("company_id")}
        rows = constituent_fundamentals(sorted(by_cid), cad)
        out: dict[str, dict] = {}
        for cid, spans in rows.items():
            isin = by_cid.get(cid)
            if isin:
                out[isin] = spans
        return {
            "label": label,
            "cadence": cad,
            "columns": [{"key": c["key"], "label": c["label"], "note": c.get("note")}
                        for c in COLUMNS],
            "members": len(by_cid),
            "covered": len(out),
            "rows": out,
        }

    return await asyncio.to_thread(_run)


class FundamentalGridColumn(BaseModel):
    """One line, and what an INDEX-LEVEL total may do with it. `agg` is `sum` for a flow or a
    snapshot (revenue, market cap) and `weighted_mean` for a rate (ROIC %) — summing percentages
    across 500 companies produces a number in the thousands that still renders as a percent.

    `unit` says whether the figure is currency at all: `millions` / `per_share` are EUR-converted,
    `shares` (a count) and `percent` (already a rate) are NOT — see the ⚠⚠ in
    `_benchmark_fundamental_grid`, where converting them produced a plausible wrong share count.
    """

    key: str
    label: str
    note: str | None = None
    unit: str
    agg: str


class FundamentalGridPeriod(BaseModel):
    """What the index looked like in ONE period. `weights_usable` is the gate the table reads
    before showing any weight or total — see `_benchmark_fundamental_grid.MIN_COVERAGE_PCT`."""

    covered: int
    members: int
    covered_pct: float
    with_market_cap: int
    cap_covered_pct: float
    total_market_cap_eur: float | None = None
    weights_usable: bool


class FundamentalGridRow(BaseModel):
    """One constituent. `v` is EUR (what the grid shows), `n` the figure as REPORTED, `fx` the rate
    applied — shipped together so the conversion can be checked rather than trusted."""

    company_id: int
    isin: str | None = None
    name: str | None = None
    ticker: str | None = None
    currency: str | None = None
    v: dict[str, dict[str, float]]
    n: dict[str, dict[str, float]]
    fx: dict[str, float]


class FundamentalGrid(BaseModel):
    """Every constituent x every line x every period, in EUR.

    ⚠ `membership_as_of` IS `today` AND THAT IS A REAL LIMIT, NOT A FORMALITY: scrubbing to 2016
    shows 2016's figures for the companies in the index NOW. Surfaced so the grid can say so.
    """

    label: str
    cadence: str
    periods: list[str]
    columns: list[FundamentalGridColumn]
    members: int
    # ⚠ THE RAW MEMBERSHIP, AND IT IS USUALLY LARGER THAN `members`. A constituent with no stored
    # market cap is dropped by `_members` — on the AEX that is Shell, Unilever and RELX — so it is
    # absent from `members` entirely rather than counted as uncovered. Reported so the total row can
    # state the gap. Not deduped (share classes count twice), so it is context, never a denominator.
    enrolled_members: int = 0
    # How many constituents "Fetch all" would fetch — counted by the FILL's own `needs`/`eligible`,
    # not derived from `covered`. The two disagree (234 vs 206 on SP500) because they use different
    # denominators and different definitions of "has data"; a button must promise what it does.
    fillable: int = 0
    covered: int
    rows: list[FundamentalGridRow]
    by_period: dict[str, FundamentalGridPeriod]
    membership_as_of: str
    min_coverage_pct: float
    # ⚠ SET => THIS INDEX CAPS, AND EVERY WEIGHT IN THIS PAYLOAD WOULD BE WRONG. The AEX caps a
    # constituent at 15%; uncapped, ASML is 37.53% of it. The grid therefore shows no weights and
    # no index row for such an index rather than shipping a second, uncapped weighting — see
    # `INDEX_CAP_PCT`, which is the single declaration this is read from.
    weight_cap_pct: float | None = None


@router.get("/api/benchmarks/index/{label}/fundamentals/grid", response_model=FundamentalGrid)
async def benchmark_fundamental_grid(label: str, cadence: str = "annual"):
    """Every constituent's fundamentals for every period, with the cap that weights each one.

    The VALUES behind `/fundamentals`, which reports only which periods we hold. Rows are
    companies, columns are lines, and the period is the slider — because weighting is
    cross-sectional: FY2021's weights need every constituent's FY2021 cap at once.

    `cadence` is `annual` (fiscal years) or `quarterly`, which — as everywhere else in this app —
    means **trailing twelve months**, not the raw quarter. That keeps both slider axes on one
    12-month basis, so moving the quarter changes the as-of date and never the unit.

    Returned whole, not per period: it is one bulk read per line over data one GuruFocus call
    already brought, and the reader's whole interaction is dragging a slider.
    """
    from routers._benchmark_fundamental_grid import fundamental_grid  # noqa: PLC0415

    return await asyncio.to_thread(fundamental_grid, label, cadence)


class CompanyIngestResult(BaseModel):
    """What one company's backfill did. `feeds` names the calls actually spent."""

    company_id: int
    name: str | None = None
    feeds: list[str] = []
    rows: int = 0
    skipped: str | None = None      # why nothing was fetched (unsubscribed, no ticker)
    error: str | None = None


@router.post("/api/benchmarks/isin/{isin}/fundamentals/ingest",
             response_model=CompanyIngestResult)
async def ingest_company_fundamentals(isin: str, force: bool = False):
    """Fetch the GuruFocus feeds ONE constituent is missing — the per-row button.

    ⚠ BY ISIN, NOT BY THE TABLE'S `company_id`. That field is an `analysis_id` in the constituent
    payload (see `ConstituentFundamentals.rows`), so an id taken straight off the row 404s against
    the `company` table — measured, on analysis_id 1457, which is a real asset row and not a
    company at all. ISIN is the identifier both worlds carry.

    ⚠ ALL THREE FEEDS, unlike `/api/earnings/fundamental-coverage/ingest`, which fetches only the
    statements. A company with financials and no estimates renders a Long Equity tab that fills in
    around two empty panels, which reads as a charting bug. See `_fundamental_backfill`.

    Admin-only: it spends GuruFocus quota, and the auth gate holds any non-`/refresh` write here to
    admins.
    """
    from routers._fundamental_backfill import (  # noqa: PLC0415
        company_rows, eligible, ingest_company, needs,
    )

    def _run() -> dict:
        key = (isin or "").strip().upper()
        hit = (supabase.table("company").select("company_id")
               .eq("isin", key).limit(1).execute().data or [])
        if not hit:
            # ⚠ AN ANSWER, NOT A FAULT. Plenty of constituents are priced from `asset_execution`
            # with no `company` row behind them — there is nothing to fetch fundamentals INTO, and
            # saying so beats a 404 the reader reads as a broken button.
            return {"company_id": 0, "name": None,
                    "skipped": f"no company row for {key} — nothing to ingest into"}
        cid = hit[0]["company_id"]
        comps = company_rows([cid])
        c = comps[cid]
        why = eligible(c)
        if why:
            return {"company_id": cid, "name": c.get("company_name"), "skipped": why}
        # `needs` tells us which feeds are missing; with `force` we re-fetch regardless.
        todo = {**c, **({} if force else next(
            (n for n in needs(comps) if n["company_id"] == cid),
            {"need_fin": False, "need_est": False, "need_ind": False}))}
        r = ingest_company(todo, force=force)
        return {"company_id": cid, "name": c.get("company_name"),
                "feeds": r["done"], "rows": r["rows"], "error": r["error"]}

    return await asyncio.to_thread(_run)


class JobStarted(BaseModel):
    """Just the handle. Everything else arrives on `/api/jobs/{id}/stream`."""

    job_id: str
    label: str


@router.post("/api/benchmarks/company/{company_id}/fundamentals/ingest/job",
             response_model=JobStarted)
async def ingest_company_fundamentals_job(company_id: int, force: bool = False,
                                          feeds: str = "all"):
    """The per-row Fetch button — same work as the by-ISIN endpoint above, as a cancellable JOB.

    ⚠⚠ KEYED ON `company_id`, AND IT USED TO BE KEYED ON ISIN — WHICH SILENTLY DISABLED THE BUTTON
    FOR 12 OF THE S&P's 501 CONSTITUENTS. The by-ISIN form exists because in the OLD constituent
    table `company_id` was secretly an `analysis_id` (the price machinery keys on that name), so an
    id off the row 404'd against `company`. That warning is real and still on the endpoint above —
    it just does not apply here: the fundamentals grid is built from `_members()`, which IS the
    company world, so its `company_id` is genuine.

    Keeping the ISIN detour cost reachability for no safety. Assurant (`company_id` 6414, NYSE,
    `AIZ`) has every field an ingest needs and no ISIN, so its Fetch button was greyed out with a
    tooltip about an identifier the fetch does not actually require. `company.isin` is nullable and
    populated opportunistically; `company_id` is the primary key.

    ⚠⚠ `feeds="statements"` IS ONE API CALL AND FILLS THE WHOLE GRID. Every one of the nineteen
    columns the fundamentals grid draws — market cap included, as
    `annuals__Valuation and Quality__Market Cap` — comes out of `fetch_financials`. The other two
    feeds (analyst estimates, indicators) contribute NOTHING to that table; they supply the Long
    Equity modal's forward EPS and indicator series.

    So the default `all` spends three calls of which two change nothing on the grid. That is the
    right default for "load this company properly", and the wrong one for the triage pass this
    parameter exists for: read the caps cheaply, then spend the other two calls only on the
    constituents whose weight makes them worth it.

    ⚠ THERE IS NO "MARKET CAP ONLY" AND THERE CANNOT BE. GuruFocus returns one financials blob;
    the cap arrives inside it along with revenue, equity and ROIC. `statements` is the smallest
    unit that exists — asking for less would mean discarding data we have already paid for.

    ⚠ WHY A JOB FOR THREE API CALLS. Not for the progress bar: for the CANCEL, and for the fact
    that several rows can now be fetched at once. The plain endpoint holds one HTTP request open
    for as long as GuruFocus takes and gives the caller no way to stop it — abort the fetch and the
    server keeps going, having already decided to spend the quota. Here the three feeds are
    separated by a `should_stop` check, so Cancel takes effect at the next feed boundary and
    whatever was already written stays written (`needs()` will pick the rest up next time).

    ⚠ THE OLD ENDPOINT STAYS. It is what `scripts/` and any external caller use, and it is the
    honest shape for a caller that wants one blocking answer. This is the same `ingest_company`
    underneath — "ingest" must not come to mean two different things depending on which button
    you pressed.
    """
    import jobs as job_registry  # noqa: PLC0415

    from routers._benchmark_fundamentals import constituent_fundamentals  # noqa: PLC0415
    from routers._fundamental_backfill import (  # noqa: PLC0415
        company_rows, eligible, ingest_company, needs,
    )

    # ⚠ THE FEED TAGS ARE INTERNAL AND MUST NOT REACH A READER. `fin`/`est`/`ind` are what the
    # backfill calls the three GuruFocus endpoints; on screen they said nothing except that
    # something technical happened. They stay in the detail line an operator can hover, because
    # WHICH feed was spent is exactly what you want when one of them comes back empty.
    feed_label = {"fin": "statements", "est": "estimates", "ind": "indicators"}

    def _span(cid: int) -> str | None:
        """What the grid will now show for this company — the answer the button was pressed for.

        ⚠ A ROW COUNT IS NOT AN ANSWER. "37,076 rows" is a count of `metric_data` writes: it is
        large, it is true, and it tells the reader nothing about whether the row they were looking
        at will fill in. The PERIOD SPAN does, in the same units the table's own slider uses.

        Read back from the same `constituent_fundamentals` the coverage figures come from, so the
        message cannot claim a span the grid would not draw.
        """
        try:
            spans = (constituent_fundamentals([cid], "annual") or {}).get(cid) or {}
            froms = [s["from"] for s in spans.values() if s.get("from")]
            tos = [s["to"] for s in spans.values() if s.get("to")]
            if not froms or not tos:
                return None
            lo, hi = min(froms), max(tos)
            return f"FY{lo}" if lo == hi else f"FY{lo}–FY{hi}"
        except Exception as e:  # noqa: BLE001
            # A summary is not worth failing a successful ingest over — the data landed either
            # way, and the caller falls back to the row count.
            logging.getLogger(__name__).warning(
                "[job] could not read the span for company %s: %s", cid, e)
            return None

    def _work(ctx) -> str:
        ctx.emit("start", "Looking this company up…", done=0, total=3)
        cid = company_id
        comps = company_rows([cid])
        c = comps.get(cid)
        if not c:
            # An answer, not a fault — the row may have been pruned since the grid was drawn.
            return f"company {cid} — no company record to load fundamentals into"
        name = c.get("company_name") or str(cid)
        why = eligible(c)
        if why:
            return f"{name} — {why}"
        todo = {**c, **({} if force else next(
            (n for n in needs(comps) if n["company_id"] == cid),
            {"need_fin": False, "need_est": False, "need_ind": False}))}
        # ⚠ APPLIED AFTER `needs`/`force`, SO IT CAN ONLY EVER NARROW. Whichever feeds the company
        # is missing, `statements` runs at most the one this grid reads — the cap and the eighteen
        # lines beside it. Folding it into the dict above would let `force=true` widen it back.
        if feeds == "statements":
            todo = {**todo, "need_est": False, "need_ind": False}

        def _step(tag: str, i: int, total: int) -> None:
            ctx.progress(i - 1, total, f"Fetching {feed_label.get(tag, tag)} ({i} of {total})")

        r = ingest_company(todo, force=force, on_step=_step, should_stop=lambda: ctx.cancelled)
        # ⚠ RECORDED BEFORE ANY OF THE EXITS BELOW. A cancelled or failed run has still spent
        # whatever it spent, and those are the two cases where knowing the bill matters most —
        # putting this after the `raise` would report a cost of zero for the runs that cost you
        # something and taught you nothing.
        ctx.spent(r.get("calls", 0))
        # ⚠ THE STOP IS RAISED HERE, NOT RETURNED. `ingest_company` reports it as data because it
        # must never raise mid-run; the JOB wants it as `JobCancelled` so the registry marks the
        # run cancelled rather than done. Two layers, two right answers.
        if r.get("stopped"):
            got = [feed_label.get(d.split()[0], d) for d in r["done"]]
            ctx.emit("info", f"Stopped after {', '.join(got) or 'no feeds'}")
            ctx.check()
        if r["error"]:
            raise RuntimeError(r["error"])

        # The technical breakdown, kept as the last progress line: the toast shows the human
        # summary and carries this on hover, and the console has both. `fin 36378` becomes
        # `statements 36,378`, which is the same fact in words a reader can act on.
        detail = " · ".join(
            f"{feed_label.get(d.split()[0], d.split()[0])} {int(d.split()[1]):,}"
            for d in r["done"] if len(d.split()) == 2)
        ctx.progress(3, 3, detail or "no new data")

        if not r["done"]:
            # ⚠ AN ANSWER, NOT A NON-EVENT. "nothing to do" read as though the button had failed to
            # do anything; what it means is that every feed was already loaded.
            return f"{name} — already up to date"
        span = _span(cid)
        return (f"{name} — loaded {span}" if span
                else f"{name} — loaded {r['rows']:,} data points")

    # ⚠ THE LABEL IS RESOLVED BEFORE THE JOB STARTS, so the toast says a company NAME from its very
    # first frame rather than an id the reader would have to look up.
    row = (supabase.table("company").select("company_name")
           .eq("company_id", company_id).limit(1).execute().data or [])
    label = (row[0].get("company_name") if row else None) or f"company {company_id}"
    job = job_registry.start("fundamentals.company", label, _work)
    return {"job_id": job.id, "label": label}


@router.post("/api/benchmarks/index/{label}/fundamentals/ingest/job",
             response_model=JobStarted)
async def ingest_index_fundamentals_job(label: str, limit: int = 0, feeds: str = "statements"):
    """Backfill every constituent missing the data this page shows, as a cancellable JOB.

    ⚠ IT REPLACED AN SSE ENDPOINT RATHER THAN JOINING ONE. The old
    `GET …/fundamentals/ingest` streamed the same work to a bespoke progress box in the panel, and
    had the defect every such endpoint here had: the client was not attached to the work. Navigate
    away and the box vanished while the thread carried on spending quota — on this run, hundreds of
    calls with no way to stop them. Keeping both would have left two transports for one fill and
    two places for "ingest" to come to mean different things.

    ⚠ CANCEL LANDS BETWEEN COMPANIES, NOT MID-COMPANY. `_one` checks first thing, so a press stops
    everything still queued at once while the eight already in flight finish the company they are
    on. That is the boundary where the database is consistent — and on a 206-company run it is the
    difference between stopping now and spending the rest of the index.

    ⚠ IT REPORTS THE QUOTA BEFORE IT STARTS AND THE SKIPS AS IT GOES. A region at zero means every
    further call is wasted, and a company on an unsubscribed exchange is a refusal with a reason —
    never a failure.

    ⚠⚠ `feeds="statements"` (THE DEFAULT) NARROWS **TWO** THINGS, AND NARROWING ONLY ONE IS A BUG.
    A fill makes two independent decisions: WHO is in the work list (`needs`, which returns anyone
    missing any of three sentinels) and WHICH feeds run for each. Narrowing only the second leaves
    companies selected because they lack estimates or indicators — for whom the narrowed action
    runs nothing at all. Measured on SP500: 216 companies need a feed, 206 need statements, so 10
    would have been iterated, spent zero calls, and reported "nothing to do", which reads as a
    broken button rather than as a deliberate scope.

    So the selection narrows too, to `need_fin`. Measured cost: **637 feed-calls over 216
    companies → 206 over 206**, a 68% saving for an identical result on this page, because all
    nineteen columns of the fundamentals grid come from the statements feed alone.

    What is given up: nothing bulk-loads analyst estimates or indicators any more. They are still
    reachable per company where they are actually drawn — `/api/earnings/{cid}/refresh` takes a
    `source` — and `feeds=all` here restores the old behaviour for a deliberate full load.

    `limit` spends the budget in tranches; 0 is everything that needs it.
    """
    import threading  # noqa: PLC0415

    import jobs as job_registry  # noqa: PLC0415
    from ingest.api_usage import remaining_budget  # noqa: PLC0415
    from routers._benchmark_index import _members  # noqa: PLC0415
    from routers._fundamental_backfill import (  # noqa: PLC0415
        company_rows, eligible, ingest_company, needs,
    )

    def _work(ctx) -> str:
        ids = sorted({m["company_id"] for m in _members(label) if m.get("company_id")})
        comps = company_rows(ids)
        todo = needs(comps)
        # ⚠ SELECTION AND ACTION NARROW TOGETHER — see the ⚠⚠ in the docstring. Dropping the
        # companies that need only estimates/indicators is what stops the run iterating rows
        # it has nothing to do for; clearing the two flags is what stops it fetching feeds this
        # page cannot render. Either alone is incoherent.
        if feeds == "statements":
            todo = [{**c, "need_est": False, "need_ind": False}
                    for c in todo if c.get("need_fin")]
        skipped = [(c, eligible(c)) for c in todo]
        work = [c for c, why in skipped if why is None]
        refused = [(c, why) for c, why in skipped if why]
        if limit:
            work = work[:limit]
        scope = "missing statements" if feeds == "statements" else "missing a feed"
        # ⚠ THE QUOTA WAS A PYTHON dict REPR — `quota {'usa': 16952, 'europe': 19222}`. True, and
        # written for whoever wrote it. It is the one number here that decides whether the run can
        # even finish, so it gets read out.
        budget = remaining_budget(supabase)
        left = " · ".join(f"{k.upper() if k == 'usa' else k.title()} {v:,}"
                          for k, v in sorted(budget.items()))
        ctx.emit(
            "start",
            f"{len(ids)} constituents · {len(todo)} {scope} · {len(work)} to fetch"
            + (f" · {len(refused)} can’t be fetched" if refused else "")
            + f" · quota left: {left}",
            done=0, total=len(work))
        # ⚠ REFUSALS ARE EVENTS, NOT FAILURES — an unsubscribed exchange is an answer. They go
        # into the log the toast carries rather than onto the bar, which counts work done.
        for w, why in refused:
            ctx.emit("skip", f"{w.get('company_name') or w['company_id']}: {why}")

        # ── The fill itself, on a bounded pool.
        #
        # ⚠⚠ THIS WAS A SERIAL `for` AND THE SERIAL PART WAS ALL WAITING. Measured against the
        # live API: 6 calls take 15.42s one at a time and 4.56s on six threads — 3.4x, with
        # zero refusals; at twelve threads throughput doubled again (2.64 calls/s), so we never
        # found GuruFocus's ceiling. A 489-constituent fill goes from ~21 minutes to ~4.
        #
        # ⚠ EIGHT, NOT TWELVE, DELIBERATELY. The 12-thread run returned one empty response in
        # twelve — not a 403, so not a quota refusal, and one sample is not proof of a limit.
        # But the marginal gain past eight is small and the downside of probing for the edge is
        # a fill that silently does less than it says. Eight keeps most of the win.
        #
        # ⚠ AND MY MEASUREMENT WAS API-ONLY. Each company also uploads to Storage and upserts
        # tens of thousands of `metric_data` rows, which lands on OUR database — expect the real
        # speed-up to be smaller than the API numbers alone suggest.
        counter = itertools.count(1)
        tally_lock = threading.Lock()
        ok = failed = rows = calls = 0

        def _one(c: dict) -> None:
            nonlocal ok, failed, rows, calls
            # ⚠ THE CANCEL BOUNDARY, AND IT IS FIRST. Everything still queued raises here the
            # moment Cancel is pressed; the eight already inside `ingest_company` finish the
            # company they are on, because that is where the database is left consistent.
            ctx.check()
            r = ingest_company(c)
            # ⚠ RETRY ONCE ON AN EMPTY ANSWER. `needs()` only selected this company because it
            # is missing the feed, so zero rows with no error means the fetch came back with
            # nothing — the failure the 12-thread run produced. It costs one call to correct
            # and, left alone, would look identical to a company that genuinely has no data.
            if not r["error"] and r["rows"] == 0:
                r = ingest_company(c)
            n = next(counter)
            with tally_lock:
                rows += r["rows"]
                calls += r.get("calls", 0)
                if r["error"]:
                    failed += 1
                else:
                    ok += 1
            ctx.spent(r.get("calls", 0))
            # ⚠ THE COUNTER, NOT THE ARRIVAL ORDER, IS THE POSITION. Eight threads report
            # concurrently, so `[7/206]` can reach the toast before `[6/206]`; `n` is taken
            # from an atomic counter so the bar only ever moves forward.
            # ⚠ THE NAME AND A PLAIN OUTCOME — NOT THE TICKER AND THE FEED TAGS. This line read
            # `[124/206] PKG fin 35604`: a GuruFocus symbol, the internal tag for the statements
            # feed, and a count of `metric_data` writes. Three pieces of our own plumbing, and
            # none of them what a reader watching a ten-minute run wants to know — which is
            # whether the index is filling in, and which company is the one that failed. The row
            # counts are not lost; they are summed into the closing line.
            who = c.get("company_name") or c.get("gurufocus_ticker") or c["company_id"]
            outcome = ("failed — " + r["error"] if r["error"]
                       # ⚠ AN ANSWER, NOT A NON-EVENT: every feed was already loaded.
                       else "already up to date" if not r["done"]
                       else "loaded")
            ctx.progress(
                n, len(work), f"[{n}/{len(work)}] {who} — {outcome}",
                company_id=c["company_id"], failed=bool(r["error"]))

        if work:
            with ThreadPoolExecutor(max_workers=_FILL_WORKERS,
                                    thread_name_prefix="fill") as pool:
                # `list(...)` so exceptions surface here rather than being swallowed by the
                # executor's lazy iterator.
                list(pool.map(_one, work))
        # ⚠ THE CALL COUNT IS ON THE JOB, NOT ONLY IN THIS SENTENCE. `ctx.spent` has been
        # accumulating it per company, so the toast's chip is right even mid-run and even if
        # the run is cancelled — this line just restates it where the outcome is read.
        # ⚠ "data points", NOT "rows" — the same wording the per-company button settled on. A row
        # is a `metric_data` write; a reader is being told how much arrived, not how our storage
        # counts it. `failed` is only mentioned when there were failures: a trailing "0 failed" on
        # every clean run is noise that teaches the eye to skip the part that matters.
        return (f"{label} — {ok} companies loaded"
                + (f", {failed} failed" if failed else "")
                + f", {rows:,} data points"
                + (f", {calls:,} API calls" if calls else ""))

    job = job_registry.start("fundamentals.index", label, _work)
    return {"job_id": job.id, "label": label}
