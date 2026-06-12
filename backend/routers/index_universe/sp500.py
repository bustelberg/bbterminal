"""S&P 500 import + generic per-index reads.

Endpoints:
    POST   /api/index-universe/import-sp500      SSE: scrape Wikipedia + reconstruct
    GET    /api/index-universe/indexes           list stored indexes with stats (cached)
    GET    /api/index-universe/months            months for one index
    GET    /api/index-universe/tickers           tickers for one (index, month)
    GET    /api/index-universe/cumulative        union of tickers across all months
    POST   /api/index-universe/check-gurufocus   SSE: probe GF cache coverage
    GET    /api/index-universe/changes           historical add/remove changelog
    DELETE /api/index-universe/indexes/{name}    drop an index
"""
from __future__ import annotations

import asyncio
import json
import logging
import queue as _queue
import time
import traceback
from collections import Counter

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from deps import supabase
from index_universe.sp500 import (
    check_gurufocus_availability,
    load_changes,
    reconstruct_monthly_holdings,
    resolve_and_create_companies,
    scrape_sp500,
    store_index_membership,
)

from ._helpers import (
    _UNIVERSE_STATS_CACHE,
    _UNIVERSE_STATS_TTL,
    _enrich_tickers,
    drain_executor_queue,
)

router = APIRouter(tags=["index-universe"])


@router.post("/api/index-universe/import-sp500")
async def index_universe_import_sp500():
    """Scrape S&P 500 from Wikipedia, reconstruct monthly holdings, store. SSE."""
    def _run(q: _queue.Queue):
        def emit(msg: str):
            q.put(json.dumps({"type": "progress", "message": msg}))

        try:
            emit("Scraping S&P 500 from Wikipedia...")
            current, changes, wiki_company_info = scrape_sp500()
            emit(f"Found {len(current)} current tickers, {len(changes)} historical changes")

            emit("Reconstructing monthly holdings (2000-01 onwards)...")
            monthly, filtered_changes = reconstruct_monthly_holdings(current, changes)
            emit(f"Reconstructed {len(monthly)} months ({min(monthly)}..{max(monthly)}), {len(filtered_changes)} changes")

            all_tickers: set[str] = set()
            for t in monthly.values():
                all_tickers |= t
            emit(f"Resolving {len(all_tickers)} unique tickers...")

            company_lookup = resolve_and_create_companies(
                supabase, all_tickers, on_progress=emit, company_info=wiki_company_info,
            )

            emit("Storing in database...")
            stats = store_index_membership(
                supabase, "SP500", monthly, filtered_changes, company_lookup,
                on_progress=emit,
            )

            q.put(json.dumps({
                "type": "done",
                "message": (
                    f"Import complete. {stats['months']} months, "
                    f"{stats['total_rows']} rows, "
                    f"{stats['unique_tickers']} unique tickers "
                    f"({stats['matched_companies']} matched to companies), "
                    f"{stats['changes_count']} changes stored"
                ),
            }))
        except Exception as e:
            q.put(json.dumps({"type": "error", "message": f"{e}\n{traceback.format_exc()}"}))
        q.put(None)

    async def generate():
        q: _queue.Queue = _queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, q)
        async for chunk in drain_executor_queue(q, task):
            yield chunk

    return StreamingResponse(generate(), media_type="text/event-stream")


@router.get("/api/index-universe/indexes")
async def index_universe_list():
    """List stored index universes with month range and unique ticker counts.

    Only genuine *imported index* universes belong here — NOT the
    template-managed universes (ACWI / Leonteq / LongEquity), their frozen
    snapshots, or criteria-derived universes, all of which live on their own
    pages. An index universe is a "bare" `universe` row: no `template_key`,
    no `frozen_at`, no `parent_universe_id`, no `filter_config`.

    Aggregates come from the universe_stats view — querying membership rows
    directly + counting in Python ran ~70s for SP500 + ACWI. Cached 5min;
    falls back to a stale cache entry on timeout, then to a degraded
    universe-table-only read so the UI still loads."""
    def _run():
        now = time.time()
        cached = _UNIVERSE_STATS_CACHE.get("data")
        if cached is not None and (now - _UNIVERSE_STATS_CACHE["ts"]) < _UNIVERSE_STATS_TTL:
            return cached

        # The set of labels that are genuine index imports (everything else —
        # templates, frozen snapshots, derived/criteria universes — is excluded).
        meta = (
            supabase.table("universe")
            .select("label, description, created_at, template_key, frozen_at, parent_universe_id, filter_config")
            .order("label")
            .execute()
        ).data or []

        def _is_index(m: dict) -> bool:
            return (
                not m.get("template_key")
                and not m.get("frozen_at")
                and not m.get("parent_universe_id")
                and not m.get("filter_config")
            )

        index_labels = {m["label"] for m in meta if _is_index(m)}

        try:
            resp = (
                supabase.table("universe_stats")
                .select("*")
                .order("label")
                .execute()
            )
            result = []
            for r in (resp.data or []):
                if r.get("label") not in index_labels:
                    continue  # not an imported index — belongs to another page
                if not r.get("start_month"):
                    continue  # skip empty universes
                result.append({
                    "index_name": r["label"],
                    "description": r.get("description"),
                    "created_at": r.get("created_at"),
                    "start_month": r.get("start_month"),
                    "end_month": r.get("end_month"),
                    "month_count": r.get("month_count") or 0,
                    "total_unique_tickers": r.get("total_unique_tickers") or 0,
                })
            _UNIVERSE_STATS_CACHE["data"] = result
            _UNIVERSE_STATS_CACHE["ts"] = now
            return result
        except Exception as e:
            logging.getLogger(__name__).warning(
                "[index-universe] universe_stats query failed (%s: %s); serving %s",
                type(e).__name__, e,
                "stale cache" if cached is not None else "degraded universe-table fallback",
            )
            if cached is not None:
                return cached
            # Degraded: list the index universes from the metadata we already
            # have (no membership stats available).
            return [
                {
                    "index_name": m["label"],
                    "description": m.get("description"),
                    "created_at": m.get("created_at"),
                    "start_month": None,
                    "end_month": None,
                    "month_count": 0,
                    "total_unique_tickers": 0,
                }
                for m in meta if _is_index(m)
            ]
    return await asyncio.to_thread(_run)


@router.get("/api/index-universe/months")
async def index_universe_months(index: str = "SP500"):
    """Months for a given index with ticker counts per month."""
    def _run():
        u_resp = supabase.table("universe").select("universe_id").eq("label", index).limit(1).execute()
        if not u_resp.data:
            return []
        universe_id = u_resp.data[0]["universe_id"]
        resp = supabase.table("universe_membership").select("target_month").eq("universe_id", universe_id).limit(100000).execute()
        counts = Counter(r["target_month"] for r in (resp.data or []))
        return [{"target_month": m, "count": c} for m, c in sorted(counts.items())]
    return await asyncio.to_thread(_run)


@router.get("/api/index-universe/tickers")
async def index_universe_tickers(index: str = "SP500", month: str = ""):
    """Tickers for a specific (index, month)."""
    if not month:
        raise HTTPException(400, "month query param required")

    def _run():
        u_resp = supabase.table("universe").select("universe_id").eq("label", index).limit(1).execute()
        if not u_resp.data:
            return []
        universe_id = u_resp.data[0]["universe_id"]
        rows = (
            supabase.table("universe_membership")
            .select("universe_ticker, company_id")
            .eq("universe_id", universe_id)
            .eq("target_month", month)
            .order("universe_ticker")
            .execute()
        ).data or []
        # Map universe_ticker -> ticker for _enrich_tickers compatibility.
        for r in rows:
            r["ticker"] = r.pop("universe_ticker", "")
        return _enrich_tickers(rows)

    return await asyncio.to_thread(_run)


@router.get("/api/index-universe/cumulative")
async def index_universe_cumulative(index: str = "SP500"):
    """Union of all unique tickers across all months for an index."""
    def _run():
        u_resp = supabase.table("universe").select("universe_id").eq("label", index).limit(1).execute()
        if not u_resp.data:
            return []
        universe_id = u_resp.data[0]["universe_id"]
        resp = supabase.table("universe_membership").select(
            "universe_ticker, company_id"
        ).eq("universe_id", universe_id).limit(100000).execute()
        seen: dict[str, dict] = {}
        for r in (resp.data or []):
            t = r.get("universe_ticker")
            if t and t not in seen:
                seen[t] = {"ticker": t, "company_id": r["company_id"]}
        return _enrich_tickers(list(seen.values()))

    return await asyncio.to_thread(_run)


@router.post("/api/index-universe/check-gurufocus")
async def index_universe_check_gf(index: str = "SP500"):
    """Check GuruFocus cache coverage for all tickers in an index. SSE."""
    def _run(q: _queue.Queue):
        def emit(msg: str):
            q.put(json.dumps({"type": "progress", "message": msg}))

        try:
            emit(f"Loading tickers for {index}...")
            u_resp = supabase.table("universe").select("universe_id").eq("label", index).limit(1).execute()
            if not u_resp.data:
                q.put(json.dumps({"type": "error", "message": f"Universe '{index}' not found"}))
                q.put(None)
                return
            universe_id = u_resp.data[0]["universe_id"]
            all_tickers: set[str] = set()
            resp = (
                supabase.table("universe_membership")
                .select("universe_ticker")
                .eq("universe_id", universe_id)
                .limit(100000)
                .execute()
            )
            for r in resp.data or []:
                if r.get("universe_ticker"):
                    all_tickers.add(r["universe_ticker"])

            emit(f"Found {len(all_tickers)} unique tickers across all months")

            result = check_gurufocus_availability(supabase, all_tickers, on_progress=emit)
            q.put(json.dumps({"type": "done", "data": result}))
        except Exception as e:
            q.put(json.dumps({"type": "error", "message": str(e)}))
        q.put(None)

    async def generate():
        q: _queue.Queue = _queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, q)
        async for chunk in drain_executor_queue(q, task):
            yield chunk

    return StreamingResponse(generate(), media_type="text/event-stream")


@router.get("/api/index-universe/changes")
async def index_universe_changes(index: str = "SP500"):
    """Historical add/remove changelog for an index."""
    return await asyncio.to_thread(lambda: load_changes(supabase, index))


@router.delete("/api/index-universe/indexes/{index_name}")
async def index_universe_delete(index_name: str):
    """Delete an index (cascades memberships)."""
    await asyncio.to_thread(
        lambda: supabase.table("universe").delete().eq("label", index_name).execute()
    )
    return {"ok": True}


@router.post("/api/index-universe/freeze")
async def index_universe_freeze(index: str = "SP500", as_of: str | None = None):
    """Freeze a static, pipeline-immune copy of a stored index.

    The index (e.g. SP500) lives in the `universe` table but isn't a registered
    template, so this reuses the template-freeze core with an explicit source
    universe id. Two modes (mirroring the /acwi Freeze button):
      - `as_of=YYYY-MM` → FIXED BASKET of that month's constituents.
      - no `as_of`      → full-history copy (survivorship-bias-free), labelled
                          with today's date.
    Idempotent on the resulting label. The snapshot then shows up everywhere
    frozen universes are listed (/universe, /backtest, /earnings)."""
    from routers.universe_templates import _freeze_core  # noqa: PLC0415

    def _do():
        u = (
            supabase.table("universe").select("universe_id")
            .eq("label", index).limit(1).execute()
        )
        if not u.data:
            raise HTTPException(status_code=404, detail=f"Index '{index}' not found — import it first.")
        return _freeze_core(index, as_of, None, src_universe_id=u.data[0]["universe_id"])

    return await asyncio.to_thread(_do)
