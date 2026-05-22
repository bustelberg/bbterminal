"""ACWI-specific endpoints: iShares fund + MSCI announcements + save-universe.

Endpoints:
    GET    /api/acwi/holdings                    current iShares ACWI ETF holdings
    GET    /api/acwi/announcements               MSCI index announcements (24h cache)
    GET    /api/acwi/announcement-detail         STANDARD action + EFFECTIVE DATE
    POST   /api/acwi/announcement-details-bulk   batch detail fetch
    GET    /api/acwi/net-additions               net additions matched to current holdings
    POST   /api/acwi/save-universe               SSE: reconstruct monthly ACWI + persist
    GET    /api/acwi/fetch-all-details           SSE: backfill the detail cache

`save-universe` is the heavy one — it walks the feasible ACWI holdings,
reconciles them against existing `company` rows (cross-exchange overrides
get renamed instead of duplicated), reconstructs monthly memberships from
matched MSCI ADDED events, and writes through `store_index_membership`
(shared with the SP500 import path).
"""
from __future__ import annotations

import asyncio
import json
import queue as _queue
import threading

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

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
from index_universe.sp500 import store_index_membership

from ._helpers import drain_thread_queue

router = APIRouter(tags=["index-universe"])


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


@router.get("/api/acwi/xls-age")
async def acwi_xls_age():
    """Age of the bundled `iShares-MSCI-ACWI-ETF_fund.xls` file.

    iShares blocks automated downloads (region-cookie + JS challenge),
    so the XLS file lives in the repo and updates only when someone
    commits a fresh one. This endpoint surfaces the file's mtime so
    the UI can warn when it gets too old. Threshold lives client-side;
    the backend just reports the raw age."""
    import os  # noqa: PLC0415
    from datetime import datetime, timezone  # noqa: PLC0415
    from index_universe.acwi.holdings import _FILE  # noqa: PLC0415
    try:
        mtime = os.path.getmtime(_FILE)
    except OSError as e:
        return {"available": False, "error": str(e)}
    mtime_dt = datetime.fromtimestamp(mtime, timezone.utc)
    age_days = (datetime.now(timezone.utc) - mtime_dt).days
    return {
        "available": True,
        "path": _FILE,
        "mtime": mtime_dt.isoformat(),
        "age_days": age_days,
    }


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


def run_acwi_save_universe(
    name: str,
    start_date: str,
    end_date: str,
    on_progress=None,
) -> dict:
    """Sync core of `POST /api/acwi/save-universe`. Walks the feasible
    ACWI holdings, reconciles companies, reconstructs monthly
    memberships, persists them through `store_index_membership`, and
    returns the same `stats` dict the SSE endpoint emits in its `done`
    event. `on_progress(message: str, pct: int | None)` is called from
    this thread; the SSE wrapper bridges those calls to its event queue.

    Lives at module scope (rather than nested in the endpoint) so the
    in-process scheduler pipeline in `routers/ingest_runs.py` can call
    it directly without going through HTTP.
    """
    def emit(message: str, pct: int | None = None):
        if on_progress is not None:
            on_progress(message, pct)

    emit("Loading feasible ACWI holdings from iShares XLS...", 3)
    feasible = acwi_feasible_holdings_for_db()
    emit(f"Found {len(feasible)} feasible holdings", 5)

    # Cross-exchange dedup pass — iShares lists dual-listed names (e.g.
    # ICBC on HKSE:01398 AND SHSE:601398) as separate holdings. Without
    # this we'd insert both as distinct `company` rows. Group by
    # canonical name, keep the highest-priority exchange (HKSE > SHSE
    # for Chinese names; see EXCHANGE_PRIORITY in ingest.dedupe).
    from collections import defaultdict  # noqa: PLC0415
    from ingest.dedupe import canonical_name, exchange_priority  # noqa: PLC0415

    _by_name: dict[str, list[dict]] = defaultdict(list)
    for fh in feasible:
        nm = canonical_name(fh.get("company_name"))
        if nm:
            _by_name[nm].append(fh)
    _winners: dict[str, dict] = {}
    for nm, grp in _by_name.items():
        if len(grp) > 1:
            _winners[nm] = min(grp, key=lambda h: exchange_priority(h.get("db_exchange")))
    _dropped_log: list[str] = []
    _deduped: list[dict] = []
    for fh in feasible:
        nm = canonical_name(fh.get("company_name"))
        if not nm or nm not in _winners:
            _deduped.append(fh)
            continue
        if fh is _winners[nm]:
            _deduped.append(fh)
        else:
            _dropped_log.append(
                f"{fh.get('db_exchange')}:{fh.get('gf_ticker')} -> "
                f"{_winners[nm].get('db_exchange')}:{_winners[nm].get('gf_ticker')} "
                f"({fh.get('company_name')})"
            )
    if _dropped_log:
        sample = ", ".join(_dropped_log[:5])
        more = f" (+{len(_dropped_log) - 5} more)" if len(_dropped_log) > 5 else ""
        emit(f"Dedup: dropped {len(_dropped_log)} cross-exchange dupe(s), kept higher-priority listing: {sample}{more}", None)
    feasible = _deduped

    exch_resp = supabase.table("gurufocus_exchange").select("exchange_id, exchange_code").execute()
    exch_id_map = {r["exchange_code"]: r["exchange_id"] for r in (exch_resp.data or [])}

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

    emit(f"Reconstructing monthly holdings {start_date}..{end_date}...", 45)
    monthly, stats = reconstruct_acwi_monthly_holdings(start_date, end_date)
    emit(
        f"Built {stats['months']} months: {stats['feasible_count']} feasible tickers "
        f"({stats['with_addition']} with matched addition, {stats['grandfathered']} grandfathered)",
        55,
    )

    emit(f"Writing universe '{name}' to database...", 60)
    store_stats = store_index_membership(
        supabase, name, monthly, [], company_lookup,
        on_progress=lambda m: emit(m, None),
        sector_lookup=sector_lookup,
    )
    return {
        "name": name,
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
    }


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
        async for chunk in drain_thread_queue(q):
            yield chunk

    return StreamingResponse(generate(), media_type="text/event-stream")
