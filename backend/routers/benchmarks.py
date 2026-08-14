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
import gzip
import logging
import re
from datetime import date

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel

from deps import supabase
from routers import _blend_cache
from ingest.api_usage import track_api_call
from ingest.constants import DATA_CUTOFF
from ingest.prices import _fetch_price_from_api, _parse_price_series

router = APIRouter(tags=["benchmarks"])

# ⚠ THE FILL'S WORKER COUNT MOVED WITH THE FILL — `routers/_fundamental_fill.FILL_WORKERS`, where
# the measurement behind the number lives beside the loop it governs. Two copies of a concurrency
# limit is two places for one of them to be raised.


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
    from common.read_cache import read_cache  # noqa: PLC0415

    from routers._asset_benchmark import compute_index_async  # noqa: PLC0415

    # ⚠ SAME SHAPE AS THE OVERVIEW'S MEMO, AND THE SAME REASON. It removes one round trip of
    # fourteen and takes the endpoint from 1,704ms to 433ms locally: the repeat is a whole-universe
    # read, and the Benchmarks tab fires three of these at once (SP500, ACWI, AEX).
    with read_cache(f"index:{label}"):
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
    # The other half of the GuruFocus identifier — a bare ticker is ambiguous across venues.
    exchange: str | None = None
    currency: str | None = None
    # Built server-side by `_tickers`, because `_build_symbol` drops the prefix for US venues and
    # normalizes the ticker (HKSE zero-pad, `BRK/B` -> `BRK.B`). None when the row has no ticker
    # or no exchange, in which case the UI shows plain text rather than a dead link.
    gf_url: str | None = None
    # Why this row can NEVER be filled, or None when it can. From the same `eligible()` the fill
    # job calls — an unsubscribed exchange, no GuruFocus ticker, or no exchange at all. It is the
    # difference between "nobody has fetched this yet" and "this cannot be fetched", which without
    # it render identically as dashes.
    unavailable: str | None = None
    # The badge text for `unavailable` — `UNSUB` (the venue is outside the subscription, so it is
    # true of every constituent on that exchange) or `NO GF` (this row has no GuruFocus ticker or
    # exchange). Classified server-side so the UI never has to pattern-match a prose message.
    unavailable_label: str | None = None
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
async def benchmark_fundamental_grid(request: Request, label: str, cadence: str = "annual"):
    """Every constituent's fundamentals for every period, with the cap that weights each one.

    The VALUES behind `/fundamentals`, which reports only which periods we hold. Rows are
    companies, columns are lines, and the period is the slider — because weighting is
    cross-sectional: FY2021's weights need every constituent's FY2021 cap at once.

    `cadence` is `annual` (fiscal years) or `quarterly`, which — as everywhere else in this app —
    means **trailing twelve months**, not the raw quarter. That keeps both slider axes on one
    12-month basis, so moving the quarter changes the as-of date and never the unit.

    Returned whole, not per period: it is ONE bulk read for every line over data one GuruFocus call
    already brought, and the reader's whole interaction is dragging a slider.

    ⚠ CACHED IN-PROCESS, AND DROPPED BY THE INGEST JOBS. Both Fetch buttons call
    `_blend_cache.invalidate()` when they have written something, so a filled row shows up on the
    reload the pane does anyway. See `cached_grid` for why this must not be a `Cache-Control`
    header: a copy in the browser is one no invalidation of ours can reach.

    ⚠⚠ GZIPPED HERE RATHER THAN APP-WIDE, AND THAT IS DELIBERATE. ACWI's payload is **16.5 MB** of
    JSON — 1,949 constituents x 12 periods x 19 lines, each carrying its EUR value, its native
    figure and the rate between them — and it compresses to **5.3 MB** in 0.21s (level 1; level 6
    reaches 4.5 MB for three times the CPU, which is the wrong trade for a number this size). By
    the time the server work below is measured in hundreds of milliseconds, the transfer IS the
    load time, and no amount of query tuning touches it.

    A `GZipMiddleware` on the app would have covered this endpoint and every other one — and this
    app is SSE-heavy (ingest, scanner, backtest, every live dashboard). Compression sits between a
    stream and its client and buffers; the whole point of those endpoints is that a frame arrives
    when it is produced. One endpoint that ships megabytes is not a reason to put a buffer in front
    of the ones that ship bytes.

    ⚠ THE `Accept-Encoding` HEADER IS HONOURED, NOT ASSUMED. Every browser sends it and `requests`
    sends it by default, but a plain `curl` does NOT — and `/documentation` publishes curl
    quick-starts against this API. Shipping gzip to a client that did not ask for it hands it
    binary it will render as mojibake.

    ⚠ THE MODEL STILL VALIDATES. Returning a `Response` skips FastAPI's `response_model` check, so
    it is run explicitly below — the schema is what `npm run gen:types` generates the frontend's
    types from, and an endpoint that silently stops conforming to its own contract is worse than a
    slow one. It costs 0.06s on the largest payload here, and only on a cache miss.
    """
    from routers._benchmark_fundamental_grid import fundamental_grid  # noqa: PLC0415
    from routers._benchmark_fundamentals import normalise_cadence  # noqa: PLC0415

    # ⚠ THE KEY IS THE NORMALISED CADENCE, NOT THE RAW QUERY STRING. `normalise_cadence` maps
    # anything that is not "quarterly" onto "annual", so `?cadence=annual`, `?cadence=` and a typo
    # all produce the SAME payload — keying on the raw string would store it three times and
    # compute it three times to prove it.
    cad = normalise_cadence(cadence)

    def _encoded() -> bytes:
        """The gzipped JSON — this is what the cache holds, and it is smaller than the dict.

        ⚠ THE COMPRESSED BYTES, NOT THE PAYLOAD OBJECT. Caching the dict would hold ~250,000
        Python floats across ~60,000 dicts for ACWI, which costs far more resident memory than the
        5.3 MB this is — and it would still pay validation and serialisation on every hit. Caching
        the finished bytes makes a cache hit a memcpy. `_MAX_ENTRIES` is 24 and the entries are
        big; this is the version that fits.
        """
        payload = fundamental_grid(label, cad)
        body = FundamentalGrid.model_validate(payload).model_dump_json().encode()
        return gzip.compress(body, 1)

    blob = await asyncio.to_thread(_blend_cache.cached_grid, label, cad, _encoded)
    accepts = "gzip" in (request.headers.get("accept-encoding") or "").lower()
    if accepts:
        return Response(content=blob, media_type="application/json",
                        headers={"Content-Encoding": "gzip"})
    # ⚠ DECOMPRESSED ON THE WAY OUT, never stored twice. This branch is a curl session, not the
    # app, so it may pay for the round trip through gzip rather than double the cache's footprint.
    return Response(content=gzip.decompress(blob), media_type="application/json")


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
        # ⚠ AND `force` REACHES THE STORAGE BLOB TOO. Selecting a company again while still
        # replaying the bytes we already hold is not a re-fetch — see `ingest_company`'s ⚠⚠.
        todo = {**c, **({} if force else next(
            (n for n in needs(comps) if n["company_id"] == cid),
            {"need_fin": False, "need_est": False, "need_ind": False}))}
        r = ingest_company(todo, force=force, refresh_cache=force)
        return {"company_id": cid, "name": c.get("company_name"),
                "feeds": r["done"], "rows": r["rows"], "error": r["error"]}

    return await asyncio.to_thread(_run)


# The "[n/total]" prefix `_benchmark_refresh._prices` writes on every constituent line. Compiled
# once because it is matched against every line of a 1,684-constituent run.
_STEP_RE = re.compile(r"^\[(\d+)/(\d+)\]")


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
        company_rows, eligible, feed_flags, ingest_company, needs, smart_flags,
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
        # ⚠⚠ THE FLAGS DECIDE, AND `force` MUST NOT REACH `ingest_company` — see `feed_flags`. It
        # used to be passed there as well, which short-circuits the flags and runs all three feeds:
        # the drill-down's per-row Refresh (`force=true&feeds=statements`) spent 3 API calls per
        # company instead of 1, on estimates and indicators that screen does not draw.
        # ⚠⚠ `feeds="smart"` FETCHES WHAT IS MISSING **OR** WHAT CAN HAVE CHANGED, PER FEED.
        # It is the only mode that both spends nothing on a company with nothing new and still
        # picks up figures just filed — the two things a Refresh has to do at once. `all` always
        # spends three calls; an un-forced run tests PRESENCE, so it is a no-op on exactly the
        # company a reader pressed it for. See `smart_flags`.
        todo = {**c, **(smart_flags(cid) if feeds == "smart" else feed_flags(force, feeds, next(
            (n for n in needs(comps) if n["company_id"] == cid), None) if not force else None))}

        def _step(tag: str, i: int, total: int) -> None:
            ctx.progress(i - 1, total, f"Fetching {feed_label.get(tag, tag)} ({i} of {total})")

        # ⚠ `refresh_cache=force`, SO THE FLAG MEANS ONE THING ON EVERY INGEST ENDPOINT: go and
        # look. The grid's per-row Fetch does not pass `force`, so its cheap cache-friendly
        # behaviour is unchanged — only a caller that explicitly asked for a re-fetch pays.
        # ⚠ `refresh_cache=force` ONLY. That is the OTHER cache — the GuruFocus blob in Storage,
        # which `is_cache_fresh` replays for months — and bypassing it is what makes a re-fetch
        # actually re-ask the vendor. "Ignore what `metric_data` holds" is already said by the
        # flags above, and saying it twice is what tripled the bill.
        # ⚠⚠ SMART BYPASSES THE STORAGE CACHE TOO, OR IT DECIDES NOTHING. There are two caches:
        # the `need_*` flags say which feeds to run, and `is_cache_fresh` replays the stored
        # GuruFocus blob for months afterwards. Having judged a feed stale, replaying the same
        # bytes would rewrite identical rows, spend zero calls and leave the table exactly as it
        # was — a press that looks like a no-op. The flags are what keep this cheap; this is what
        # makes the calls it does decide to spend actually re-ask the vendor.
        r = ingest_company(todo, refresh_cache=(force or feeds == "smart"),
                           on_step=_step, should_stop=lambda: ctx.cancelled)
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
            #
            # It is also why the cache is NOT dropped here: nothing was written, so every cached
            # benchmark blend is still correct and throwing them away would cost ~25s of rebuild
            # to reach the identical answer.
            return f"{name} — already up to date"
        # ⚠ WE JUST CHANGED WHAT EVERY BENCHMARK BLEND WOULD COMPUTE. This company may be a
        # constituent of any index, so the cached lines are stale from this moment; the writer
        # clearing them is what makes the cache safe to keep for 30 minutes at a time.
        _blend_cache.invalidate()
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


@router.post("/api/benchmarks/index/{label}/refresh/job", response_model=JobStarted)
async def benchmark_refresh_job(label: str):
    """The same refresh as `GET …/refresh`, as a cancellable JOB.

    ⚠ WHY IT EXISTS: THE SSE FORM CANNOT BE STOPPED. It streams to whoever opened it, so the client
    is attached to the work — navigate away and the progress box vanishes while the thread carries
    on making paced Yahoo calls for another eleven minutes, with no handle to stop it. That is the
    identical defect the fundamentals ingest had before it became a job.

    ⚠ THE SSE ENDPOINT IS LEFT IN PLACE, unlike the fundamentals conversion which replaced its own.
    That one had a single consumer; this one is also how a refresh is watched from `/api` and from
    curl, where a job handle is the inconvenient form. Both call `refresh_benchmark` — ONE
    implementation, two transports, never two refreshes.

    ⚠ CANCEL LANDS BETWEEN CONSTITUENTS — `should_stop` is checked in `_prices`' loop, which is
    where the minutes are. It is deliberately NOT `ctx.check()`: raising would discard the counts
    for work that really happened, and those counts are this job's entire output. A stopped run
    keeps everything it fetched and its summary says how far it got.
    """
    import jobs as job_registry  # noqa: PLC0415

    from routers._benchmark_refresh import refresh_benchmark  # noqa: PLC0415

    def _work(ctx) -> str:
        # ⚠ THE BAR NEEDS A DENOMINATOR AND `emit` HAS NONE. `refresh_benchmark` reports prose, not
        # counts, so the "[n/total]" the price step ALREADY writes into its own line is read back
        # out here rather than changing that module's contract for one consumer. A line that does
        # not match leaves the bar where it was — which is right, because the constituents and caps
        # steps have no meaningful denominator and a bar that resets to 0/0 between phases reads as
        # the run having restarted.
        state = {"done": 0, "total": 0}

        def _emit(_msg_type: str, **kw) -> None:
            msg = (kw.get("message") or "").strip()
            if not msg:
                return
            m = _STEP_RE.match(msg)
            if m:
                state["done"], state["total"] = int(m.group(1)), int(m.group(2))
            ctx.progress(state["done"], state["total"], msg)

        s = refresh_benchmark(label, _emit, should_stop=lambda: ctx.cancelled)
        if s.get("note") and not s.get("priceable"):
            return f"{label} — {s['note']}"

        # ⚠⚠ THIS SENTENCE MOVED HERE FROM THE FRONTEND'S `refreshSummary`, WHICH THE JOB TRANSPORT
        # RETIRED — and it is reproduced rather than shortened because two of its clauses are there
        # under an explicit "never silent" rule that a transport change must not quietly repeal:
        #   * a constituent with NO CAP weighs nothing, so it is absent from a cap-weighted index
        #     while looking perfectly healthy in the grid;
        #   * "already at the vendor's latest" is the ANSWER on a run where nothing moved. Omitting
        #     it reads as a broken button, which is exactly how ING's untouched 30.22 was first
        #     reported.
        bits = [f"{s.get('priceable', 0)} of {s.get('universe_members', 0)} constituents priceable"]
        if s.get("capped"):
            bits.append(f"{s['capped']} market caps")
        if s.get("no_cap"):
            bits.append(f"⚠ {s['no_cap']} with no market cap (they weigh nothing)")
        if s.get("prices_fetched"):
            bits.append(f"{s['prices_fetched']} price series fetched")
        if s.get("prices_moved"):
            bits.append(f"{s['prices_moved']} gained a new close")
        if s.get("prices_unchanged"):
            bits.append(f"{s['prices_unchanged']} already at the vendor's latest")
        if s.get("no_start_price"):
            bits.append(f"{s['no_start_price']} have no start-of-year price (listed later)")
        if s.get("prices_failed"):
            bits.append(f"{s['prices_failed']} failed (see the console)")
        if s.get("needs_resolve"):
            bits.append(f"{s['needs_resolve']} still unresolved — press again")
        if s.get("no_isin"):
            bits.append(f"⚠ {s['no_isin']} members have no ISIN and can never be reached from here")
        if s.get("stopped"):
            bits.append(f"⚠ CANCELLED after {s.get('stopped_at', 0)} — the rest were not fetched")
        out = ", ".join(bits) + "."
        if s.get("market_anchor"):
            out += f" Priced to {s['market_anchor']}."
        return out

    job = job_registry.start("benchmark.refresh", label, _work)
    return {"job_id": job.id, "label": label}


@router.post("/api/benchmarks/index/{label}/fundamentals/ingest/job",
             response_model=JobStarted)
async def ingest_index_fundamentals_job(label: str, limit: int = 0, feeds: str = "statements",
                                        force: bool = False):
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

    ⚠⚠ `force=true` MEANS "EVERY CONSTITUENT", AND THE SENTINEL PROBE IS NOT MERELY BYPASSED — IT
    IS NOT RUN. `needs()` answers *who is missing the feed*, which is the wrong question for a
    forced run: the answer changes nothing, and it is the expensive part of the setup (one read of
    `metric_data` per sentinel across every constituent — on ACWI, ~1,900 of them).

    ⚠ IT EXISTS BECAUSE PRESENT IS NOT CURRENT. The sentinel is a row that EXISTS
    (`annuals__Cashflow Statement__Free Cash Flow`), so a constituent whose statements were loaded
    a year ago is "not missing" for ever and no press of the un-forced fill will ever update it —
    the grid keeps showing last year's figures and looks filled. That is the same reasoning the
    price half already settled (see `_benchmark_refresh`: *a press always fetches, every
    constituent, no staleness tolerance*), and this is what makes the panel's Refresh mean the same
    thing on both halves.

    ⚠ FORCE IS EXPRESSED AS THE `need_*` FLAGS, NEVER AS `ingest_company(force=True)`. That
    argument runs ALL THREE feeds regardless of the flags, so under `feeds="statements"` it would
    quietly triple the spend on data this page cannot draw. Setting the flags keeps *which feeds
    run* decided in exactly one place, and `force` then means only *ignore what we already hold*.

    ⚠⚠ AND IT CARRIES `refresh_cache` TOO, BECAUSE THERE ARE TWO CACHES. Selecting a company is not
    the same as re-asking the vendor: the GuruFocus blob also sits in Storage, and `is_cache_fresh`
    calls it fresh for weeks past the quarter it is missing. Forced selection without the cache
    bypass would rewrite identical rows from the same bytes, spend zero calls and leave the grid
    exactly as it was — a press that looks like a no-op is how a button loses trust. See
    `ingest_company`'s own ⚠⚠ for the two layers side by side.

    Cost, measured shape: one GuruFocus call per eligible constituent per press — ~490 for SP500,
    and on ACWI the unsubscribed exchanges are still refused before a call is spent. The remaining
    quota is read out before the run starts.

    `limit` spends the budget in tranches; 0 is everything that needs it.
    """
    import jobs as job_registry  # noqa: PLC0415

    from routers._benchmark_index import _members  # noqa: PLC0415
    from routers._fundamental_fill import fill_company_ids  # noqa: PLC0415

    def _work(ctx) -> str:
        # ⚠⚠ `require_market_cap=False` IS LOAD-BEARING, AND THE DEFAULT MAKES THIS JOB
        #   SELF-DEFEATING. `_members` drops any constituent with no stored `market_cap_eur` --
        #   correct for a cap-weighted index, catastrophic here, because the market cap comes out
        #   of the SAME statements blob this job fetches. So "has no cap" and "needs fetching" are
        #   very nearly the same set, and filtering on the former removes exactly the companies
        #   the job exists to load. Measured on the S&P: the grid offered 10 fillable, the work
        #   list came back 0, and the button reported "0 loaded" while each of those 10 fetched
        #   fine from its own per-row Fetch. The grid computes `fillable` with the same flag; the
        #   two MUST agree, or the button promises work it then refuses to do.
        #
        # ⚠ SELECTION IS ALL THAT IS LEFT HERE. The fill itself moved to
        #   `routers/_fundamental_fill.py` when the portfolio button needed the identical work over
        #   a different id list -- see the ⚠⚠ at the top of that module for why it is not copied.
        ids = sorted({m["company_id"] for m in _members(label, require_market_cap=False)
                      if m.get("company_id")})
        return fill_company_ids(ctx, label, ids, feeds=feeds, force=force, limit=limit)

    job = job_registry.start("fundamentals.index", label, _work)
    return {"job_id": job.id, "label": label}
