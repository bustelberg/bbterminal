"""Per-universe data freshness + bulk earnings/price fetch for the /universe
overview page.

Two endpoints (admin tier — all of /api/universe/* is admin-only):

  GET  /api/universe/{universe_id}/data-freshness
       Coverage (members with data / total members) + the most recent GuruFocus
       data date across the universe's members, so the user can judge whether a
       fetch is needed.

  POST /api/universe/{universe_id}/fetch-data?force=false   (SSE)
       Fetch every /earnings data source (financials + analyst estimates +
       indicators + prices) for EVERY company that has ever been a member of the
       universe (union across all membership months — for a frozen snapshot that
       is just its one member list). Each ingest fetcher self-skips when its
       cache is already fresh, so an incremental run is cheap; `?force=true`
       re-fetches regardless. HEAVY for large universes (≈4 GuruFocus calls per
       not-fresh company), so progress + running totals stream as SSE.
"""
from __future__ import annotations

import asyncio
import json
import queue as _queue
import threading

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from deps import supabase, chunked
from ingest.earnings import fetch_analyst_estimates, fetch_financials, fetch_indicators
from ingest.prices import ensure_prices_for_company
from routers.index_universe._helpers import drain_thread_queue

router = APIRouter(tags=["universe"])

_SOURCES = ("financials", "analyst_estimates", "indicators", "prices")


def _universe_member_ids(universe_id: int) -> list[int]:
    """Distinct company_ids that have EVER been a member of the universe (union
    across all membership months)."""
    cids: set[int] = set()
    offset, page = 0, 1000
    while True:
        resp = (
            supabase.table("universe_membership").select("company_id")
            .eq("universe_id", universe_id)
            .range(offset, offset + page - 1).execute()
        )
        batch = resp.data or []
        cids.update(r["company_id"] for r in batch)
        if len(batch) < page:
            break
        offset += page
    return sorted(cids)


def _company_meta(cids: list[int]) -> dict[int, dict]:
    """ticker + exchange per company, for the ingest calls."""
    out: dict[int, dict] = {}
    for chunk in chunked(cids):
        resp = (
            supabase.table("company")
            .select("company_id, gurufocus_ticker, gurufocus_exchange:gurufocus_exchange(exchange_code)")
            .in_("company_id", chunk).execute()
        )
        for r in resp.data or []:
            exch = r.get("gurufocus_exchange") or {}
            out[r["company_id"]] = {
                "ticker": r.get("gurufocus_ticker"),
                "exchange": exch.get("exchange_code") or "UNKNOWN",
            }
    return out


@router.get("/api/universe/{universe_id}/data-freshness")
async def universe_data_freshness(universe_id: int):
    """Coverage + most-recent GuruFocus data date across the universe's members
    (union of all membership months)."""
    def _q():
        resp = supabase.rpc("universe_data_freshness", {"p_universe_id": universe_id}).execute()
        row = (resp.data or [{}])[0] if resp.data else {}
        latest = row.get("latest_date")
        return {
            "universe_id": universe_id,
            "member_count": int(row.get("member_count") or 0),
            "with_data": int(row.get("with_data") or 0),
            "latest_date": str(latest)[:10] if latest else None,
        }
    return await asyncio.to_thread(_q)


@router.post("/api/universe/{universe_id}/fetch-data")
async def universe_fetch_data(universe_id: int, force: bool = False):
    """SSE: fetch all /earnings data sources for every company that has ever been
    a member of the universe. Self-skips fresh sources unless `force=true`."""
    member_ids = await asyncio.to_thread(_universe_member_ids, universe_id)
    if not member_ids:
        raise HTTPException(status_code=404, detail="Universe has no members (or doesn't exist).")

    q: _queue.Queue = _queue.Queue()

    def _push(payload: dict) -> None:
        q.put(json.dumps(payload))

    def _worker() -> None:
        try:
            meta = _company_meta(member_ids)
            total = len(member_ids)
            _push({"type": "progress", "message": f"Fetching all earnings sources for {total} companies (force={force})…"})
            totals = {"companies": 0, "rows": 0, "api_calls": 0, "errors": 0, "skipped_region": 0}
            for i, cid in enumerate(member_ids, start=1):
                m = meta.get(cid) or {}
                ticker = m.get("ticker")
                exchange = m.get("exchange") or "UNKNOWN"
                if not ticker:
                    totals["errors"] += 1
                    _push({"type": "progress", "message": f"[{i}/{total}] company {cid}: no GuruFocus ticker — skipped"})
                    continue

                c_rows, c_calls, err, forbidden = 0, 0, None, False
                for source in _SOURCES:
                    try:
                        if source == "financials":
                            r = fetch_financials(supabase, cid, ticker, exchange, force_refresh=force)
                        elif source == "analyst_estimates":
                            r = fetch_analyst_estimates(supabase, cid, ticker, exchange, force_refresh=force)
                        elif source == "indicators":
                            r = fetch_indicators(supabase, cid, ticker, exchange, force_refresh=force)
                        else:
                            r = ensure_prices_for_company(supabase, cid, ticker, exchange, force_refresh=force)
                        c_rows += getattr(r, "rows_loaded", 0) or 0
                        c_calls += getattr(r, "api_calls", 0) or 0
                        if getattr(r, "error", None):
                            err = r.error
                        if getattr(r, "is_forbidden", False):
                            forbidden = True
                            break  # unsubscribed region — remaining sources will also 403
                    except Exception as e:  # noqa: BLE001
                        err = f"{source}: {e}"

                totals["companies"] += 1
                totals["rows"] += c_rows
                totals["api_calls"] += c_calls
                label = f"{ticker}.{exchange}"
                if forbidden:
                    totals["skipped_region"] += 1
                    _push({"type": "progress", "message": f"[{i}/{total}] {label}: unsubscribed region — skipped"})
                elif err:
                    totals["errors"] += 1
                    _push({"type": "progress", "message": f"[{i}/{total}] {label}: error — {err} ({c_calls} calls)"})
                else:
                    _push({"type": "progress", "message": f"[{i}/{total}] {label}: {c_rows} rows, {c_calls} calls"})

            _push({
                "type": "done",
                "message": (
                    f"Done. {totals['companies']}/{total} companies · {totals['rows']} rows loaded · "
                    f"{totals['api_calls']} GuruFocus calls · {totals['errors']} errors · "
                    f"{totals['skipped_region']} unsubscribed."
                ),
                "totals": totals,
            })
        except Exception as e:  # noqa: BLE001
            _push({"type": "error", "message": f"{e}"})
        finally:
            q.put(None)

    threading.Thread(target=_worker, daemon=True).start()

    async def _gen():
        async for chunk in drain_thread_queue(q):
            yield chunk
    return StreamingResponse(
        _gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
