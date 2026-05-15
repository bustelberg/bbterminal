"""Parallel per-company ensure-fetch loop (db_only=False path).

Each company task submits price + volume API calls concurrently to the
shared ThreadPoolExecutor. A semaphore caps the in-flight count. Events
are streamed back via an asyncio.Queue so SSE progress can interleave
with completions. Yields SSE event strings; mutates the passed-in
mutable sets (`excluded_ids`, `blocked_exchanges`, `pass1_transient`)
and the int counters in `counters` in place."""
from __future__ import annotations

import asyncio
import functools
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import pandas as pd

from deps import supabase
from ingest.prices import (
    PriceResult,
    _ensure_bucket,
    ensure_prices_for_company,
    ensure_volume_for_company,
)


def _emit(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"


def _keepalive() -> str:
    return ": keepalive\n\n"


async def run_fetch_loop(
    universe_df: pd.DataFrame,
    data_cutoff: date,
    *,
    excluded_ids: set[int],
    blocked_exchanges: set[str],
    pass1_transient: set[int],
    counters: dict[str, int],  # {"ok_count": int, "skipped_count": int}
):
    """Yield SSE event strings while running the parallel ensure-fetch
    loop. Mutates `excluded_ids` / `blocked_exchanges` / `pass1_transient`
    and the values in `counters` in place."""
    total_companies = len(universe_df)
    concurrency = int(os.environ.get("BACKTEST_FETCH_CONCURRENCY", "16"))
    fetch_start_ts = time.monotonic()

    # Warm the storage bucket once before launching tasks — otherwise the
    # first N workers would race and each fire a bucket-create HTTP call.
    await asyncio.to_thread(_ensure_bucket, supabase)

    # Each company task submits 2 blocking HTTP calls in parallel (price +
    # volume), so the executor needs 2 slots per concurrent task or the
    # second call queues behind the first and inflates wall-clock timings.
    pool_size = concurrency * 2 + 4
    executor = ThreadPoolExecutor(max_workers=pool_size, thread_name_prefix="fetch")
    loop = asyncio.get_event_loop()

    yield _emit({"type": "progress", "pct": 5, "message": f"Ensuring price & volume data for {total_companies} companies (cutoff: {data_cutoff}, concurrency: {concurrency}, pool: {pool_size})..."})
    yield _keepalive()

    result_queue: asyncio.Queue = asyncio.Queue()
    sema = asyncio.Semaphore(concurrency)
    inflight = {"count": 0, "peak": 0}

    async def _fetch_one(row_cid: int, row_ticker: str, row_exchange: str):
        sym = f"{row_exchange}:{row_ticker}"
        if row_exchange in blocked_exchanges:
            await result_queue.put({"cid": row_cid, "symbol": sym, "exchange": row_exchange, "status": "skipped_blocked"})
            return
        async with sema:
            if row_exchange in blocked_exchanges:
                await result_queue.put({"cid": row_cid, "symbol": sym, "exchange": row_exchange, "status": "skipped_blocked"})
                return
            inflight["count"] += 1
            if inflight["count"] > inflight["peak"]:
                inflight["peak"] = inflight["count"]
            task_start = time.monotonic()
            try:
                # Run price + volume concurrently inside one company task
                pr_fut = loop.run_in_executor(
                    executor,
                    functools.partial(
                        ensure_prices_for_company,
                        supabase, row_cid, row_ticker, row_exchange,
                        data_cutoff=data_cutoff,
                    ),
                )
                vr_fut = loop.run_in_executor(
                    executor,
                    functools.partial(
                        ensure_volume_for_company,
                        supabase, row_cid, row_ticker, row_exchange,
                        data_cutoff=data_cutoff,
                    ),
                )
                pr_res, vr_res = await asyncio.gather(pr_fut, vr_fut, return_exceptions=True)
                if isinstance(pr_res, BaseException):
                    raise pr_res
                pr = pr_res
                if isinstance(vr_res, BaseException):
                    vr = PriceResult()
                    vr.source = "error"
                    vr.error = str(vr_res)
                else:
                    vr = vr_res
                elapsed_ms = int((time.monotonic() - task_start) * 1000)
                await result_queue.put({"cid": row_cid, "symbol": sym, "exchange": row_exchange, "pr": pr, "vr": vr, "status": "ok", "ms": elapsed_ms})
            except Exception as e:
                elapsed_ms = int((time.monotonic() - task_start) * 1000)
                await result_queue.put({"cid": row_cid, "symbol": sym, "exchange": row_exchange, "error": f"{type(e).__name__}: {e}", "status": "error", "ms": elapsed_ms})
            finally:
                inflight["count"] -= 1

    tasks = [
        asyncio.create_task(_fetch_one(
            int(row["company_id"]),
            row["gurufocus_ticker"],
            row["gurufocus_exchange"] or "UNKNOWN",
        ))
        for _, row in universe_df.iterrows()
    ]

    async def _sentinel():
        await asyncio.gather(*tasks, return_exceptions=True)
        await result_queue.put(None)

    sentinel_task = asyncio.create_task(_sentinel())

    done_count = 0
    try:
        while True:
            evt = await result_queue.get()
            if evt is None:
                break
            done_count += 1
            pct = 5 + round((done_count / max(1, total_companies)) * 55)
            status = evt["status"]
            cid = evt["cid"]
            symbol = evt["symbol"]
            exchange = evt["exchange"]

            if status == "skipped_blocked":
                counters["skipped_count"] += 1
                excluded_ids.add(cid)
                continue
            if status == "error":
                excluded_ids.add(cid)
                yield _emit({"type": "warning", "scope": "fetch", "symbol": symbol, "message": f"{symbol}: fetch failed — {evt['error']}"})
                continue

            pr = evt["pr"]
            vr = evt["vr"]

            if pr.is_forbidden:
                blocked_exchanges.add(exchange)
                excluded_ids.add(cid)
                yield _emit({"type": "progress", "pct": pct, "message": f"  {symbol}: unsubscribed region — future {exchange} calls will be skipped"})
            elif pr.is_delisted:
                excluded_ids.add(cid)
                await asyncio.to_thread(
                    lambda c=cid: (
                        supabase.table("metric_data").delete().eq("company_id", c).execute(),
                        supabase.table("portfolio_weight").delete().eq("company_id", c).execute(),
                        supabase.table("company").delete().eq("company_id", c).execute(),
                    )
                )
                yield _emit({"type": "progress", "pct": pct, "message": f"  {symbol}: DELISTED — removed from database"})
            else:
                counters["ok_count"] += 1
                if pr.source == "stale_cache" or (vr and vr.source == "stale_cache"):
                    pass1_transient.add(cid)
                parts: list[str] = []
                if pr.source == "cache":
                    parts.append(f"price: cache ({pr.rows_loaded})")
                elif pr.source == "api":
                    parts.append(f"price: API ({pr.rows_loaded})")
                elif pr.source == "stale_cache":
                    parts.append(f"price: stale cache ({pr.rows_loaded})")
                elif pr.source == "none":
                    parts.append("price: none")
                else:
                    parts.append(f"price: {pr.source}")
                if vr:
                    if vr.source == "cache":
                        parts.append(f"vol: cache ({vr.rows_loaded})")
                    elif vr.source == "api":
                        parts.append(f"vol: API ({vr.rows_loaded})")
                    elif vr.source == "stale_cache":
                        parts.append(f"vol: stale cache ({vr.rows_loaded})")
                    elif vr.source == "error":
                        parts.append(f"vol: error ({vr.error})")
                    else:
                        parts.append(f"vol: none ({vr.error or 'unknown'})")
                else:
                    parts.append("vol: failed")
                ms = evt.get("ms", 0)
                peak = inflight["peak"]
                yield _emit({"type": "progress", "pct": pct, "message": f"  {symbol} ({done_count}/{total_companies}, {ms}ms, peak:{peak}): {' | '.join(parts)}"})
    finally:
        # Make sure the sentinel task completes before we leave this block
        try:
            await sentinel_task
        except Exception:
            pass
        executor.shutdown(wait=False)

    total_elapsed = time.monotonic() - fetch_start_ts
    yield _emit({"type": "progress", "pct": 60, "message": f"Fetch complete in {total_elapsed:.1f}s (peak concurrency: {inflight['peak']}/{concurrency})"})

    if blocked_exchanges:
        yield _emit({"type": "warning", "scope": "fetch", "message": f"Blocked exchanges (unsubscribed): {', '.join(sorted(blocked_exchanges))} — {counters['skipped_count']} companies skipped"})
