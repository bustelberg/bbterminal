"""Shared helpers for the index-universe routers.

`_enrich_tickers` is the small "join company info onto raw membership
rows" used by the generic per-index reads. The two SSE drainers cover
the existing patterns in the original file — one drains a queue fed by
an executor-launched `_run`, the other drains a queue fed by a daemon
thread (the older ACWI write paths). Behavior is byte-identical to the
inline versions; consolidated here so the per-domain files stay focused
on what they actually do."""
from __future__ import annotations

import asyncio
import queue as _queue

from deps import supabase, IN_CHUNK_SIZE


# Module-level cache for the universe-stats list. The underlying view does
# COUNT(DISTINCT universe_ticker) over the full universe_membership table,
# which sometimes trips Supabase's 8s statement_timeout once the table grows
# past ~500k rows (S&P 500 history × ACWI × monthly entries). Reads change
# rarely (only after an index ingest), so a 5-minute TTL avoids paying that
# cost on every dropdown render. On timeout we fall back to a stale cached
# entry if we have one, then to a cheap universe-table-only read so the UI
# still loads — month/ticker counts come back as 0 in that degraded mode.
_UNIVERSE_STATS_CACHE: dict = {"ts": 0.0, "data": None}
_UNIVERSE_STATS_TTL = 300.0


def _enrich_tickers(rows: list[dict]) -> list[dict]:
    """Add company_name + exchange + GuruFocus URL to ticker rows."""
    from ingest.gurufocus_url import gurufocus_url  # noqa: PLC0415
    company_ids = [r["company_id"] for r in rows if r["company_id"]]
    company_info: dict[int, dict] = {}
    for i in range(0, len(company_ids), IN_CHUNK_SIZE):
        chunk = company_ids[i:i + IN_CHUNK_SIZE]
        resp = supabase.table("company").select(
            "company_id, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code)"
        ).in_("company_id", chunk).execute()
        for c in resp.data or []:
            exch_info = c.get("gurufocus_exchange") or {}
            company_info[c["company_id"]] = {
                "company_name": c.get("company_name") or "",
                "exchange": exch_info.get("exchange_code") or "",
            }

    result = []
    for r in rows:
        info = company_info.get(r["company_id"], {}) if r["company_id"] else {}
        ticker = r["ticker"]
        exchange = info.get("exchange") or None
        result.append({
            "ticker": ticker,
            "company_id": r["company_id"],
            "company_name": info.get("company_name") or None,
            "exchange": exchange,
            "gurufocus_url": gurufocus_url(ticker, exchange),
        })
    return result


async def drain_executor_queue(q: _queue.Queue, task):
    """Drain a queue fed by an executor-launched `_run`. The executor task
    eventually finishes; the queue's sentinel is None. Used by the SSE
    endpoints whose worker is launched via `loop.run_in_executor`."""
    yield ": keepalive\n\n"
    while True:
        try:
            msg = await asyncio.to_thread(q.get, timeout=0.15)
        except Exception:
            if task.done():
                while not q.empty():
                    msg = q.get_nowait()
                    if msg is not None:
                        yield f"data: {msg}\n\n"
                break
            continue
        if msg is None:
            break
        yield f"data: {msg}\n\n"


async def drain_thread_queue(q: _queue.Queue):
    """Drain a queue fed by a daemon `threading.Thread` worker. The thread
    pushes None when done so we just block on `q.get` and exit on the
    sentinel — there's no task handle to inspect."""
    yield ": keepalive\n\n"
    while True:
        msg = await asyncio.to_thread(q.get)
        if msg is None:
            break
        yield f"data: {msg}\n\n"
