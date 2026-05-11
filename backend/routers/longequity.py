"""LongEquity report ingestion + per-snapshot company listing.

Endpoints:
    GET  /api/longequity/latest-available   "is a newer file available remotely?"
    GET  /api/longequity/snapshots          distinct target_dates we have loaded
    GET  /api/longequity/companies          companies for a snapshot + diff vs previous
    POST /api/ingest/long-equity            SSE: full ingest pipeline (Storage → DB)
    POST /api/longequity/save-universe      SSE: persist a constant-per-month universe

The ingest stream drives the LongEquity Insight page's **Run ingest**
button: acquires raw files from Storage, flattens grouped Excel headers,
resolves unknown tickers via OpenFIGI, enriches with primary listings,
and loads into the metric_data + universe tables. Idempotent — skips
months already loaded (intersection of `metric_data` and
`universe_membership` rows so a partial early ingest doesn't get marked
"done").
"""

from __future__ import annotations

import asyncio
import json
import queue as _queue
import re
import threading
import time
import traceback
from datetime import date

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from deps import supabase
from ingest.acquire import acquire_raw_longequity_backfill, check_latest_available_month
from ingest.extend_primary import enrich_flattened_df_with_primary_listing
from ingest.flatten import flatten_excel
from ingest.load_into_supabase import (
    fix_company_primary_keys,
    get_ticker_overrides,
    load_prepared_into_supabase,
    merge_duplicate_companies,
    save_ticker_overrides,
)
from ingest.resolve_tickers import detect_unknown_tickers, resolve_via_openfigi
from ingest.transformation import prepare_flattened_for_schema

router = APIRouter(tags=["longequity"])


_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _as_of_date_from_filename(filename: str) -> date:
    m = re.search(
        r"(january|february|march|april|may|june|july|august|september|october|november|december)"
        r"[-_](\d{4})",
        filename.lower(),
    )
    if not m:
        raise ValueError(f"Could not parse month-year from filename: {filename}")
    return date(int(m.group(2)), _MONTHS[m.group(1)], 1)


def _get_db_longequity_months() -> set[str]:
    """Months that are fully loaded for longequity.

    A month counts as "done" only when BOTH metric_data AND
    universe_membership rows exist for it. Early ingests populated
    metric_data before the universe_membership write path existed, so
    relying on metric_data alone silently skips months that still need
    their universe rows.
    """
    md_resp = supabase.rpc("get_distinct_dates", {"p_source_code": "longequity"}).execute()
    metric_months = {str(row["target_date"])[:7] for row in (md_resp.data or [])}

    u_resp = supabase.table("universe").select("universe_id").eq("label", "longequity").limit(1).execute()
    if not u_resp.data:
        return set()
    universe_id = u_resp.data[0]["universe_id"]

    membership_months: set[str] = set()
    offset = 0
    page_size = 1000
    while True:
        resp = (
            supabase.table("universe_membership")
            .select("target_month")
            .eq("universe_id", universe_id)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data or []
        for r in batch:
            if r.get("target_month"):
                membership_months.add(r["target_month"])
        if len(batch) < page_size:
            break
        offset += page_size

    return metric_months & membership_months


async def _ingest_long_equity_stream():
    """Full LongEquity ingest pipeline as SSE: acquire → flatten → resolve
    tickers → enrich → transform → load → de-dup. Each step yields its
    own `info`/`error` events; only new months (not already in DB) are
    processed."""
    def event(msg_type: str, message: str) -> str:
        return f"data: {json.dumps({'type': msg_type, 'message': message})}\n\n"

    existing_months = await asyncio.to_thread(_get_db_longequity_months)
    yield event("info", f"{len(existing_months)} month(s) already in DB.")

    yield event("info", "Acquiring Long Equity files (Storage → remote URL)...")
    try:
        all_files = await asyncio.to_thread(acquire_raw_longequity_backfill, supabase)
    except Exception as e:
        yield event("error", f"Acquire failed: {e}")
        return

    files: list[tuple[str, bytes]] = []
    for filename, content in all_files:
        try:
            as_of = _as_of_date_from_filename(filename)
            ym = f"{as_of.year:04d}-{as_of.month:02d}"
            if ym in existing_months:
                continue
            files.append((filename, content))
        except ValueError:
            files.append((filename, content))  # can't parse date — process anyway

    if not files:
        yield event("done", f"Pipeline finished — all {len(all_files)} month(s) already loaded.")
        return

    yield event("info", f"{len(files)} new file(s) to process (skipped {len(all_files) - len(files)} already in DB).")

    # Persisted ticker overrides — shared across all files in this run.
    try:
        db_overrides: list[dict] = await asyncio.to_thread(get_ticker_overrides, supabase)
        yield event("info", f"Loaded {len(db_overrides)} ticker override(s) from DB.")
    except Exception as e:
        yield event("info", f"Could not load ticker overrides (table may not exist yet): {e}")
        db_overrides = []

    total_companies = 0
    total_metric_rows = 0
    all_new_resolutions: list[dict] = []

    for i, (filename, content) in enumerate(files, 1):
        as_of = _as_of_date_from_filename(filename)
        yield event("info", "")
        yield event("info", f"[{i}/{len(files)}] {filename}  (as_of: {as_of})")

        yield event("info", "  Flattening grouped headers...")
        try:
            df = await asyncio.to_thread(flatten_excel, content)
        except Exception as e:
            yield event("error", f"  Flatten failed: {e}")
            continue
        yield event("info", f"  {len(df)} rows, {len(df.columns)} columns")

        # Detect unknown tickers (not in fill_ticker.json or DB overrides).
        try:
            unknowns = await asyncio.to_thread(
                detect_unknown_tickers, df, db_overrides=db_overrides
            )
        except Exception as e:
            yield event("info", f"  Ticker detection failed (skipping): {e}")
            unknowns = []

        if unknowns:
            yield event("info", f"  {len(unknowns)} unknown ticker(s): {', '.join(u['ticker'] for u in unknowns)}")
            yield event("info", "  Resolving via OpenFIGI...")
            try:
                resolved = await asyncio.to_thread(resolve_via_openfigi, unknowns)
            except Exception as e:
                yield event("info", f"  OpenFIGI failed (continuing without): {e}")
                resolved = []

            if resolved:
                yield event("info", f"  Resolved {len(resolved)}/{len(unknowns)} ticker(s).")
                try:
                    saved = await asyncio.to_thread(save_ticker_overrides, supabase, resolved)
                    if saved:
                        yield event("info", f"  Saved {saved} new resolution(s) to ticker_override table.")
                except Exception as e:
                    yield event("info", f"  Could not save resolutions to DB: {e}")

                # Update in-memory overrides so later files benefit too.
                db_overrides = db_overrides + resolved
                all_new_resolutions.extend(resolved)
            else:
                yield event("info", f"  Could not resolve {len(unknowns)} ticker(s) — will use fallback values.")
        else:
            yield event("info", "  All tickers covered by existing mappings.")

        yield event("info", "  Enriching tickers...")
        try:
            df = await asyncio.to_thread(
                enrich_flattened_df_with_primary_listing, df,
                extra_overrides=db_overrides if db_overrides else None,
            )
        except Exception as e:
            yield event("error", f"  Enrich failed: {e}")
            continue

        yield event("info", "  Transforming to metric_data...")
        try:
            prepared = await asyncio.to_thread(
                prepare_flattened_for_schema, df,
                as_of_date=as_of, source_code="longequity",
            )
        except Exception as e:
            yield event("error", f"  Transform failed: {e}")
            continue
        yield event("info", f"  {len(prepared.company)} companies, {len(prepared.metric_data)} metric rows")

        yield event("info", "  Loading into Supabase...")
        try:
            result = await asyncio.to_thread(
                load_prepared_into_supabase, prepared, supabase,
                universe_label="longequity",
            )
        except Exception as e:
            yield event("error", f"  Load failed: {e}")
            continue

        total_companies += result.company_inserted
        total_metric_rows += result.metric_data_inserted
        yield event("info", (
            f"  Inserted: {result.company_inserted} companies, "
            f"{result.metric_data_inserted} metric rows"
        ))

    # Fix company rows from prior runs that loaded with exchange_id=NULL
    # (before ticker resolution existed).
    if all_new_resolutions:
        yield event("info", "")
        yield event("info", "Fixing company records from previous runs with UNKNOWN exchange...")
        try:
            fixed = await asyncio.to_thread(fix_company_primary_keys, supabase, all_new_resolutions)
            yield event("info", f"  Fixed {fixed} company record(s)." if fixed else "  No records needed fixing.")
        except Exception as e:
            yield event("info", f"  Fix step failed (non-critical): {e}")

    # Merge duplicates (same name + exchange, different ticker — happens when
    # a later run resolves a previously-UNKNOWN row to its real ticker).
    yield event("info", "")
    yield event("info", "Checking for duplicate companies...")
    try:
        merge_logs = await asyncio.to_thread(merge_duplicate_companies, supabase)
        if merge_logs:
            for msg in merge_logs:
                yield event("info", f"  {msg}")
        else:
            yield event("info", "  No duplicates found.")
    except Exception as e:
        yield event("info", f"  Dedup step failed (non-critical): {e}")

    yield event("info", "")
    yield event("done", (
        f"Pipeline complete. {len(files)} file(s) processed. "
        f"Total new rows — companies: {total_companies}, "
        f"metric data: {total_metric_rows}."
    ))


@router.get("/api/longequity/latest-available")
async def get_latest_available():
    """Probe remote storage for the newest report; reports whether a month
    newer than what's loaded is available."""
    try:
        spec = await asyncio.to_thread(check_latest_available_month, supabase=supabase)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"check_latest_available_month failed: {e}")
    if spec is None:
        return {"available": False, "year": None, "month": None}
    return {"available": True, "year": spec.year, "month": spec.month}


@router.get("/api/longequity/snapshots")
def get_longequity_snapshots():
    """Distinct target_dates with LongEquity data in metric_data."""
    try:
        resp = supabase.rpc("get_distinct_dates", {"p_source_code": "longequity"}).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"get_longequity_snapshots failed: {e}")
    return [{"target_date": row["target_date"]} for row in (resp.data or [])]


@router.get("/api/longequity/companies")
def get_longequity_companies(target_date: str):
    """Companies present in the LongEquity snapshot for `target_date`, plus
    add/remove diff vs the previous snapshot."""
    all_companies_resp = (
        supabase.table("company")
        .select("company_id,gurufocus_ticker,exchange_id,company_name,gurufocus_exchange:gurufocus_exchange(exchange_code,country:country(country_name))")
        .limit(10000)
        .execute()
    )
    for c in (all_companies_resp.data or []):
        exch_info = c.pop("gurufocus_exchange", None) or {}
        country_info = exch_info.pop("country", None) or {}
        c["gurufocus_exchange"] = exch_info.get("exchange_code")
        c["country"] = country_info.get("country_name")
    all_companies: dict[int, dict] = {
        c["company_id"]: c for c in (all_companies_resp.data or [])
    }

    def _company_ids_for_date(td: str) -> set[int]:
        resp = supabase.rpc("get_company_ids_for_date", {
            "p_source_code": "longequity",
            "p_target_date": td,
        }).execute()
        return {r["company_id"] for r in (resp.data or [])}

    current_ids = _company_ids_for_date(target_date)
    companies = [all_companies[cid] for cid in current_ids if cid in all_companies]

    all_dates = supabase.rpc("get_distinct_dates", {"p_source_code": "longequity"}).execute()
    dates_list = [r["target_date"] for r in (all_dates.data or [])]
    prev_date = None
    for d in dates_list:
        if d < target_date:
            prev_date = d
        else:
            break

    added: list[dict] = []
    removed: list[dict] = []
    if prev_date:
        prev_ids = _company_ids_for_date(prev_date)
        added = [all_companies[cid] for cid in (current_ids - prev_ids) if cid in all_companies]
        removed = [all_companies[cid] for cid in (prev_ids - current_ids) if cid in all_companies]

    return {"companies": companies, "added": added, "removed": removed}


@router.post("/api/ingest/long-equity")
async def ingest_long_equity():
    """SSE: kick off the full ingest pipeline."""
    return StreamingResponse(
        _ingest_long_equity_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


class LongEquitySaveUniverseRequest(BaseModel):
    name: str = "longequity_cumulative"
    description: str | None = None
    start_date: str = "2002-01-01"
    end_date: str | None = None  # defaults to first day of current month


@router.post("/api/longequity/save-universe")
async def longequity_save_universe(req: LongEquitySaveUniverseRequest):
    """SSE: save a constant-per-month universe spanning a date range.

    Every month from start_date to end_date contains the SAME set of
    companies — the union of every company that has ever appeared in any
    LongEquity snapshot. Sector and universe_ticker carry forward from
    the most recent snapshot in which each company appeared. Used as a
    universe option in the momentum backtester.
    """
    def _run(q: _queue.Queue):
        def emit(step: str, status: str, message: str):
            q.put(json.dumps({
                "type": "progress", "step": step, "status": status, "message": message,
            }))

        try:
            label = (req.name or "").strip()
            if not label:
                q.put(json.dumps({"type": "error", "message": "name is required"}))
                return

            try:
                start_d = date.fromisoformat(req.start_date)
            except Exception:
                q.put(json.dumps({"type": "error", "message": f"invalid start_date: {req.start_date!r}"}))
                return
            if req.end_date:
                try:
                    end_d = date.fromisoformat(req.end_date)
                except Exception:
                    q.put(json.dumps({"type": "error", "message": f"invalid end_date: {req.end_date!r}"}))
                    return
            else:
                today = date.today()
                end_d = today.replace(day=1)
            start_d = start_d.replace(day=1)
            end_d = end_d.replace(day=1)
            if end_d < start_d:
                q.put(json.dumps({"type": "error", "message": "end_date must be >= start_date"}))
                return

            # Universe rows elsewhere key target_month as "YYYY-MM" (see
            # universe/screen.py). Match that convention so the backtest
            # loader (which does month_date.isoformat()[:7]) actually hits.
            month_list: list[str] = []
            cur = start_d
            while cur <= end_d:
                month_list.append(cur.strftime("%Y-%m"))
                cur = cur.replace(year=cur.year + 1, month=1) if cur.month == 12 else cur.replace(month=cur.month + 1)

            emit("load", "in_progress", "Locating 'longequity' source universe...")
            u_resp = supabase.table("universe").select("universe_id").eq("label", "longequity").limit(1).execute()
            if not u_resp.data:
                q.put(json.dumps({"type": "error", "message": "'longequity' universe not found — run ingest first."}))
                return
            source_uid = u_resp.data[0]["universe_id"]

            rows: list[dict] = []
            offset = 0
            page = 1000
            while True:
                r = (
                    supabase.table("universe_membership")
                    .select("company_id, target_month, universe_ticker, sector")
                    .eq("universe_id", source_uid)
                    .range(offset, offset + page - 1)
                    .execute()
                )
                batch = r.data or []
                rows.extend(batch)
                if len(batch) < page:
                    break
                offset += page

            source_months = sorted({r["target_month"] for r in rows if r.get("target_month")})
            if not source_months:
                q.put(json.dumps({"type": "error", "message": "no LongEquity snapshots found"}))
                return
            unique_companies = len({r["company_id"] for r in rows})
            emit(
                "load", "done",
                f"Loaded {len(rows):,} memberships across {len(source_months)} source months "
                f"({unique_companies} distinct companies).",
            )

            # Build constant union set + latest-known ticker/sector — walk
            # in ascending month order so the LATEST snapshot's ticker/sector
            # wins per company.
            emit("build", "in_progress", "Building union set across all snapshots...")
            latest_info: dict[int, dict] = {}
            union_set: set[int] = set()
            for r in sorted(rows, key=lambda r: r.get("target_month") or ""):
                cid = r["company_id"]
                union_set.add(cid)
                latest_info[cid] = {
                    "universe_ticker": r.get("universe_ticker"),
                    "sector": r.get("sector"),
                }

            emit("target", "in_progress", f"Preparing target universe '{label}'...")
            t_resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
            if t_resp.data:
                target_uid = t_resp.data[0]["universe_id"]
                # PostgREST may cap rows-affected per delete. Loop until count
                # reads zero so we never re-insert on top of stragglers.
                for _attempt in range(20):
                    supabase.table("universe_membership").delete().eq("universe_id", target_uid).execute()
                    remaining_resp = (
                        supabase.table("universe_membership")
                        .select("company_id", count="exact", head=True)
                        .eq("universe_id", target_uid)
                        .execute()
                    )
                    if (remaining_resp.count or 0) == 0:
                        break
                    emit("target", "in_progress", f"Still {remaining_resp.count:,} rows after delete; looping...")
                emit("target", "done", f"Cleared existing rows in '{label}' (id={target_uid}).")
            else:
                c_resp = supabase.table("universe").insert({
                    "label": label,
                    "description": req.description or "Cumulative LongEquity universe",
                }).execute()
                target_uid = c_resp.data[0]["universe_id"]
                emit("target", "done", f"Created new universe '{label}' (id={target_uid}).")

            # Replicate the union set across every month in [start, end].
            payload: list[dict] = []
            for m in month_list:
                for cid in union_set:
                    info = latest_info.get(cid, {})
                    payload.append({
                        "universe_id": target_uid,
                        "company_id": cid,
                        "target_month": m,
                        "universe_ticker": info.get("universe_ticker"),
                        "sector": info.get("sector"),
                    })
            emit(
                "build", "done",
                f"Prepared {len(payload):,} rows = {len(union_set)} companies × {len(month_list)} months "
                f"({month_list[0]} → {month_list[-1]}).",
            )

            from universe.derived_metrics import _fmt_duration as _fmt_dur
            batch_size = 500
            total_batches = (len(payload) + batch_size - 1) // batch_size
            started = time.monotonic()
            total_inserted = 0
            emit("insert", "in_progress", f"Inserting {len(payload):,} rows in {total_batches} batches...")
            for bi, i in enumerate(range(0, len(payload), batch_size), start=1):
                chunk = payload[i:i + batch_size]
                elapsed = time.monotonic() - started
                rate = (bi - 1) / elapsed if elapsed > 0 and bi > 1 else 0
                remaining = (total_batches - bi + 1) / rate if rate > 0 else 0
                emit(
                    "insert", "in_progress",
                    f"Starting batch {bi}/{total_batches} ({len(chunk):,} rows) · "
                    f"{_fmt_dur(elapsed)} elapsed · ~{_fmt_dur(remaining)} left",
                )
                resp = supabase.table("universe_membership").insert(chunk).execute()
                total_inserted += len(resp.data or [])
                elapsed = time.monotonic() - started
                rate = bi / elapsed if elapsed > 0 else 0
                remaining = (total_batches - bi) / rate if rate > 0 else 0
                emit(
                    "insert", "in_progress",
                    f"Batch {bi}/{total_batches} done · {total_inserted:,}/{len(payload):,} rows · "
                    f"{_fmt_dur(elapsed)} elapsed · ~{_fmt_dur(remaining)} left",
                )
            emit(
                "insert", "done",
                f"Inserted {total_inserted:,} rows in {_fmt_dur(time.monotonic() - started)}.",
            )

            q.put(json.dumps({
                "type": "done",
                "message": (
                    f"Saved '{label}': {total_inserted:,} rows across {len(month_list)} months "
                    f"({len(union_set)} unique companies, {month_list[0]} → {month_list[-1]})."
                ),
                "data": {
                    "universe_id": target_uid,
                    "label": label,
                    "months": len(month_list),
                    "rows_inserted": total_inserted,
                    "total_companies": len(union_set),
                    "start_date": month_list[0],
                    "end_date": month_list[-1],
                },
            }))
        except Exception as e:
            q.put(json.dumps({"type": "error", "message": f"{e}\n{traceback.format_exc()}"}))
        finally:
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
