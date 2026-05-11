"""Index universe (S&P 500 + ACWI) reconstruction + serving.

Endpoints:
    POST   /api/index-universe/import-sp500      SSE: scrape Wikipedia + reconstruct
    GET    /api/index-universe/indexes           list stored indexes with stats (cached)
    GET    /api/index-universe/months            months for one index
    GET    /api/index-universe/tickers           tickers for one (index, month)
    GET    /api/index-universe/cumulative        union of tickers across all months
    POST   /api/index-universe/check-gurufocus   SSE: probe GF cache coverage
    GET    /api/index-universe/changes           historical add/remove changelog
    DELETE /api/index-universe/indexes/{name}    drop an index

    GET    /api/acwi/holdings                    current iShares ACWI ETF holdings
    GET    /api/acwi/announcements               MSCI index announcements (24h cache)
    GET    /api/acwi/announcement-detail         STANDARD action + EFFECTIVE DATE
    POST   /api/acwi/announcement-details-bulk   batch detail fetch
    GET    /api/acwi/net-additions               net additions matched to current holdings
    POST   /api/acwi/save-universe               SSE: reconstruct monthly ACWI + persist
    GET    /api/acwi/fetch-all-details           SSE: backfill the detail cache

`store_index_membership` is the shared write path — both SP500 import and
ACWI save go through it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import queue as _queue
import threading
import time
import traceback
from collections import Counter

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from deps import supabase
from index_universe.acwi import (
    _load_detail_cache,
    _save_detail_cache,
    compute_net_additions,
    fetch_announcement_detail,
    fetch_announcement_detail_cached,
    fetch_bulk_details,
    feasible_holdings_for_db as acwi_feasible_holdings_for_db,
    get_msci_announcements,
    gurufocus_exchange,
    gurufocus_exchange_for_db,
    gurufocus_url,
    load_acwi_holdings,
    reconstruct_monthly_holdings as reconstruct_acwi_monthly_holdings,
)
from index_universe.sp500 import (
    check_gurufocus_availability,
    load_changes,
    reconstruct_monthly_holdings,
    resolve_and_create_companies,
    scrape_sp500,
    store_index_membership,
)

router = APIRouter(tags=["index-universe"])

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


# ─── S&P 500 import ────────────────────────────────────────────────────────

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
        yield ": keepalive\n\n"
        q: _queue.Queue = _queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, q)

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

    return StreamingResponse(generate(), media_type="text/event-stream")


# ─── Index list + per-index reads ──────────────────────────────────────────

@router.get("/api/index-universe/indexes")
async def index_universe_list():
    """List stored index universes with month range and unique ticker counts.

    Aggregates come from the universe_stats view — querying membership rows
    directly + counting in Python ran ~70s for SP500 + ACWI. Cached 5min;
    falls back to a stale cache entry on timeout, then to a degraded
    universe-table-only read so the UI still loads."""
    def _run():
        now = time.time()
        cached = _UNIVERSE_STATS_CACHE.get("data")
        if cached is not None and (now - _UNIVERSE_STATS_CACHE["ts"]) < _UNIVERSE_STATS_TTL:
            return cached
        try:
            resp = (
                supabase.table("universe_stats")
                .select("*")
                .order("label")
                .execute()
            )
            result = []
            for r in (resp.data or []):
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
            try:
                u_resp = (
                    supabase.table("universe")
                    .select("universe_id, label, description, created_at")
                    .order("label")
                    .execute()
                )
                return [
                    {
                        "index_name": r["label"],
                        "description": r.get("description"),
                        "created_at": r.get("created_at"),
                        "start_month": None,
                        "end_month": None,
                        "month_count": 0,
                        "total_unique_tickers": 0,
                    }
                    for r in (u_resp.data or [])
                ]
            except Exception:
                raise e
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


def _enrich_tickers(rows: list[dict]) -> list[dict]:
    """Add company_name + exchange + GuruFocus URL to ticker rows."""
    company_ids = [r["company_id"] for r in rows if r["company_id"]]
    company_info: dict[int, dict] = {}
    for i in range(0, len(company_ids), 50):
        chunk = company_ids[i:i + 50]
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
        result.append({
            "ticker": ticker,
            "company_id": r["company_id"],
            "company_name": info.get("company_name") or None,
            "exchange": info.get("exchange") or None,
            "gurufocus_url": f"https://www.gurufocus.com/stock/{ticker}/summary",
        })
    return result


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
        yield ": keepalive\n\n"
        q: _queue.Queue = _queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, q)

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


# ─── ACWI (iShares + MSCI announcements) ──────────────────────────────────

@router.get("/api/acwi/holdings")
async def acwi_holdings():
    """Current iShares ACWI ETF holdings parsed from the bundled XLS."""
    def work():
        holdings, as_of = load_acwi_holdings()

        try:
            rows = supabase.table("gurufocus_exchange").select("exchange_code,currency_code").execute()
            db_currencies = {r["exchange_code"]: r["currency_code"] for r in (rows.data or [])}
        except Exception:
            db_currencies = {}

        for h in holdings:
            exch = h.get("Exchange", "")
            h["gurufocus_url"] = gurufocus_url(h.get("Ticker", ""), exch)
            gf_code = gurufocus_exchange(exch)
            h["gf_exchange"] = gf_code if gf_code else None
            db_code = gurufocus_exchange_for_db(exch)
            h["gf_currency"] = db_currencies.get(db_code) if db_code else None

        return {"holdings": holdings, "count": len(holdings), "as_of": as_of}

    return await asyncio.to_thread(work)


@router.get("/api/acwi/announcements")
async def acwi_announcements(refresh: bool = False):
    """MSCI index announcements (cached locally, 24h TTL)."""
    rows = await asyncio.to_thread(get_msci_announcements, refresh)
    return {"announcements": rows, "count": len(rows)}


@router.get("/api/acwi/announcement-detail")
async def acwi_announcement_detail(url: str):
    """STANDARD action + EFFECTIVE DATE from one MSCI announcement page."""
    return await asyncio.to_thread(fetch_announcement_detail_cached, url)


@router.post("/api/acwi/announcement-details-bulk")
async def acwi_announcement_details_bulk(body: dict):
    """Batch detail fetch. Body: {urls: [...]}"""
    urls = body.get("urls", [])
    results = await asyncio.to_thread(fetch_bulk_details, urls)
    return {"details": results}


@router.get("/api/acwi/net-additions")
async def acwi_net_additions():
    """Net additions (companies in current holdings whose last action is ADD)."""
    results = await asyncio.to_thread(compute_net_additions)
    matched = sum(1 for r in results if r["matched"])
    return {"net_additions": results, "total": len(results), "matched": matched}


class AcwiSaveUniverseRequest(BaseModel):
    name: str = "ACWI"
    start_date: str
    end_date: str


@router.post("/api/acwi/save-universe")
async def acwi_save_universe(req: AcwiSaveUniverseRequest):
    """Reconstruct monthly ACWI feasible-universe holdings + save as a universe.
    SSE; result selectable as `index_universe` in the momentum backtester."""
    def _run(q: _queue.Queue):
        def emit(message: str, pct: int | None = None):
            payload = {"type": "progress", "message": message}
            if pct is not None:
                payload["pct"] = pct
            q.put(json.dumps(payload))

        try:
            emit("Loading feasible ACWI holdings from iShares XLS...", 3)
            feasible = acwi_feasible_holdings_for_db()
            emit(f"Found {len(feasible)} feasible holdings", 5)

            exch_resp = supabase.table("gurufocus_exchange").select("exchange_id, exchange_code").execute()
            exch_id_map = {r["exchange_code"]: r["exchange_id"] for r in (exch_resp.data or [])}

            # Bulk-load existing company rows for every needed exchange in
            # one paginated fetch. Two indexes off the same data:
            #   existing_by_key[(eid, gf_ticker)]    → cid (primary)
            #   existing_by_name[(eid, NAME_UPPER)]  → list[cid] (rename fallback)
            # The name index catches override renames (WAR:SPL → WAR:EBP) and
            # prevents the duplicate-row bug that fired when the PK changed
            # but the company is still the same iShares row underneath.
            needed_exchanges = {fh["db_exchange"] for fh in feasible}
            needed_eids = [exch_id_map[e] for e in needed_exchanges if e in exch_id_map]
            existing_by_key: dict[tuple[int, str], int] = {}
            existing_by_name: dict[tuple[int, str], list[int]] = {}
            if needed_eids:
                offset = 0
                page_size = 1000
                while True:
                    c_resp = (
                        supabase.table("company")
                        .select("company_id, gurufocus_ticker, exchange_id, company_name")
                        .in_("exchange_id", needed_eids)
                        .range(offset, offset + page_size - 1)
                        .execute()
                    )
                    batch = c_resp.data or []
                    for c in batch:
                        if c.get("gurufocus_ticker") and c.get("exchange_id") is not None:
                            existing_by_key[(c["exchange_id"], c["gurufocus_ticker"])] = c["company_id"]
                        name_norm = (c.get("company_name") or "").strip().upper()
                        if name_norm and c.get("exchange_id") is not None:
                            existing_by_name.setdefault((c["exchange_id"], name_norm), []).append(c["company_id"])
                    if len(batch) < page_size:
                        break
                    offset += page_size
            emit(f"Loaded {len(existing_by_key)} existing company rows across {len(needed_eids)} exchanges", 10)

            company_lookup: dict[str, int] = {}
            sector_lookup: dict[str, str] = {
                fh["symbol"]: fh["sector"] for fh in feasible if fh.get("sector")
            }
            created = 0
            already = 0
            renamed = 0
            skipped = 0
            unknown_exchanges: set[str] = set()
            for idx, fh in enumerate(feasible):
                eid = exch_id_map.get(fh["db_exchange"])
                if eid is None:
                    skipped += 1
                    unknown_exchanges.add(fh["db_exchange"])
                    continue
                key = (eid, fh["gf_ticker"])
                cid = existing_by_key.get(key)
                if cid is not None:
                    company_lookup[fh["symbol"]] = cid
                    already += 1
                    continue

                # PK miss → try name-based rename fallback. Only honor it
                # when name → cid is unique on this exchange; ambiguous
                # matches fall through to insert.
                name_norm = (fh.get("company_name") or "").strip().upper()
                rename_target: int | None = None
                if name_norm:
                    candidates = existing_by_name.get((eid, name_norm))
                    if candidates and len(candidates) == 1:
                        rename_target = candidates[0]

                if rename_target is not None:
                    try:
                        supabase.table("company").update({
                            "gurufocus_ticker": fh["gf_ticker"],
                            "company_name": fh["company_name"] or None,
                        }).eq("company_id", rename_target).execute()
                        existing_by_key[key] = rename_target
                        company_lookup[fh["symbol"]] = rename_target
                        renamed += 1
                        emit(
                            f"  renamed {fh['db_exchange']}:* → {fh['symbol']} "
                            f"({fh['company_name']}, company_id={rename_target})",
                            None,
                        )
                    except Exception as e:
                        skipped += 1
                        emit(f"  failed to rename to {fh['symbol']} ({fh['company_name']}): {e}", None)
                    continue

                # Genuinely new row.
                try:
                    ins = supabase.table("company").insert({
                        "gurufocus_ticker": fh["gf_ticker"],
                        "exchange_id": eid,
                        "company_name": fh["company_name"] or None,
                    }).execute()
                    if ins.data:
                        cid = ins.data[0]["company_id"]
                        existing_by_key[key] = cid
                        company_lookup[fh["symbol"]] = cid
                        created += 1
                except Exception as e:
                    skipped += 1
                    emit(f"  failed to create {fh['symbol']} ({fh['company_name']}): {e}", None)
                    continue

                try:
                    supabase.table("company_source").upsert(
                        {"company_id": cid, "source_code": "acwi"},
                        on_conflict="company_id,source_code",
                        ignore_duplicates=True,
                    ).execute()
                except Exception:
                    pass

                if (idx + 1) % 200 == 0 or idx == len(feasible) - 1:
                    pct = 10 + round((idx + 1) / len(feasible) * 30)
                    emit(
                        f"Companies: {created} created, {renamed} renamed, "
                        f"{already} existing, {skipped} skipped ({idx + 1}/{len(feasible)})",
                        pct,
                    )

            if unknown_exchanges:
                emit(f"Unknown exchanges (missing from gurufocus_exchange): {sorted(unknown_exchanges)}", None)
            emit(
                f"Company sync done: {created} new, {renamed} renamed, "
                f"{already} existing, {skipped} skipped",
                42,
            )

            emit(f"Reconstructing monthly holdings {req.start_date}..{req.end_date}...", 45)
            monthly, stats = reconstruct_acwi_monthly_holdings(req.start_date, req.end_date)
            emit(
                f"Built {stats['months']} months: {stats['feasible_count']} feasible tickers "
                f"({stats['with_addition']} with matched addition, {stats['grandfathered']} grandfathered)",
                55,
            )

            emit(f"Writing universe '{req.name}' to database...", 60)
            store_stats = store_index_membership(
                supabase, req.name, monthly, [], company_lookup,
                on_progress=lambda m: emit(m, None),
                sector_lookup=sector_lookup,
            )

            q.put(json.dumps({
                "type": "done",
                "message": (
                    f"Saved '{req.name}': {store_stats['months']} months, "
                    f"{store_stats['total_rows']} rows, "
                    f"{store_stats['matched_companies']}/{store_stats['unique_tickers']} tickers matched "
                    f"({created} new companies created, {renamed} renamed, {already} existing)"
                ),
                "stats": {
                    "name": req.name,
                    "months": store_stats["months"],
                    "total_rows": store_stats["total_rows"],
                    "unique_tickers": store_stats["unique_tickers"],
                    "matched_companies": store_stats["matched_companies"],
                    "companies_created": created,
                    "companies_renamed": renamed,
                    "companies_existing": already,
                    "companies_skipped": skipped,
                    "feasible_count": stats["feasible_count"],
                    "grandfathered": stats["grandfathered"],
                    "with_addition": stats["with_addition"],
                },
            }))
        except Exception as e:
            q.put(json.dumps({"type": "error", "message": f"{e}\n{traceback.format_exc()}"}))
        q.put(None)

    q: _queue.Queue = _queue.Queue()
    threading.Thread(target=_run, args=(q,), daemon=True).start()

    async def generate():
        yield ": keepalive\n\n"
        while True:
            msg = await asyncio.to_thread(q.get)
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@router.get("/api/acwi/fetch-all-details")
async def acwi_fetch_all_details():
    """Backfill the announcement-detail cache. SSE."""
    def _emit(obj: dict) -> str:
        return json.dumps(obj, default=str)

    q: _queue.Queue[str | None] = _queue.Queue()

    def worker():
        try:
            anns = get_msci_announcements()
            constituent = [a for a in anns if a.get("is_constituent_change") and a.get("href")]
            cache = _load_detail_cache()

            to_fetch = [a for a in constituent if a["href"] not in cache]
            total = len(to_fetch)
            already_cached = len(constituent) - total

            q.put(_emit({"type": "progress", "message": f"{already_cached} cached, {total} to fetch", "fetched": 0, "total": total}))

            if total == 0:
                q.put(_emit({"type": "done", "message": "All details already cached", "fetched": 0, "total": 0, "errors": 0, "cached": already_cached}))
                q.put(None)
                return

            fetched = 0
            errors = 0
            error_list: list[dict] = []
            for a in to_fetch:
                try:
                    detail = fetch_announcement_detail(a["href"])
                except Exception as e:
                    detail = {"standard": None, "effective_date": None, "error": str(e)}
                    errors += 1
                    error_list.append({"title": a.get("title", ""), "href": a["href"], "error": str(e)})
                cache[a["href"]] = detail
                fetched += 1

                if fetched % 10 == 0 or fetched == total:
                    _save_detail_cache(cache)
                    q.put(_emit({
                        "type": "progress",
                        "message": f"Fetched {fetched}/{total}" + (f" ({errors} errors)" if errors else ""),
                        "fetched": fetched,
                        "total": total,
                        "pct": round(fetched / total * 100),
                        "errors": errors,
                    }))

            _save_detail_cache(cache)
            q.put(_emit({
                "type": "done",
                "message": f"Done. Fetched {fetched}, {errors} errors, {already_cached} were cached",
                "fetched": fetched,
                "total": total,
                "errors": errors,
                "cached": already_cached,
                "error_list": error_list[:50],  # cap payload
            }))
        except Exception as e:
            q.put(_emit({"type": "error", "message": str(e)}))
        q.put(None)

    threading.Thread(target=worker, daemon=True).start()

    async def generate():
        yield ": keepalive\n\n"
        while True:
            msg = await asyncio.to_thread(q.get)
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")
