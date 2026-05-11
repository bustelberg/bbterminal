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
from ingest.prices import _fetch_price_from_api, _parse_price_series

router = APIRouter(tags=["benchmarks"])

# Benchmark prices respect the same cutoff as company prices — keeps the
# benchmark_price table from carrying dot-com-bubble history that the
# strategy never references (start_date defaults to 2002 in the UI).
_BM_CUTOFF = date(1998, 1, 1)


class CreateBenchmarkRequest(BaseModel):
    ticker: str
    name: str
    sector: str | None = None


class UpdateBenchmarkSectorRequest(BaseModel):
    # `null` clears the sector tag; a string sets it. Empty string also
    # treated as clear so the frontend doesn't need a separate clear path.
    sector: str | None


async def _bulk_upsert_prices(benchmark_id: int, parsed: list[tuple[date, float]]) -> int:
    """Upsert a parsed price series into benchmark_price in batches of 500.
    Returns the number of rows loaded (after applying the _BM_CUTOFF)."""
    rows = [
        {"benchmark_id": benchmark_id, "target_date": d.isoformat(), "price": p}
        for d, p in parsed
        if d >= _BM_CUTOFF
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
        .select("benchmark_id, ticker, name, sector, created_at")
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
async def update_benchmark_sector(benchmark_id: int, req: UpdateBenchmarkSectorRequest):
    """Set or clear the GICS sector tag on a benchmark. The DB has a
    partial unique index on sector so only one benchmark can carry each
    sector at a time."""
    sector_clean = (req.sector or "").strip() or None
    try:
        resp = await asyncio.to_thread(
            lambda: supabase.table("benchmark")
            .update({"sector": sector_clean})
            .eq("benchmark_id", benchmark_id)
            .execute()
        )
    except Exception as e:
        msg = str(e)
        if "benchmark_sector_unique" in msg or "duplicate" in msg.lower():
            raise HTTPException(409, f"Another benchmark already tags sector '{sector_clean}'")
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
