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
from datetime import date

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from deps import supabase
from ingest.api_usage import track_api_call
from ingest.constants import DATA_CUTOFF
from ingest.prices import _fetch_price_from_api, _parse_price_series

router = APIRouter(tags=["benchmarks"])


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
