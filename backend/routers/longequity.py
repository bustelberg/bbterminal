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
import traceback
from datetime import date
from typing import Callable

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from deps import supabase
from routers._sse import sse_keepalive, sse_raw, sse_message as event
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

    Source of truth: `metric_data` rows with `source_code='longequity'`.
    The `universe_membership` table used to be a second signal (an
    early ingest could populate metric_data without universe rows), but
    since the longequity universe is now always rebuilt cumulatively at
    the end of every ingest, every captured month gets membership rows
    regardless — the metric_data check by itself is now sufficient and
    correct.
    """
    md_resp = supabase.rpc("get_distinct_dates", {"p_source_code": "longequity"}).execute()
    return {str(row["target_date"])[:7] for row in (md_resp.data or [])}


# ─── Shared ingest core ────────────────────────────────────────────
#
# `run_longequity_ingest_core` is the SINGLE implementation of the
# acquire → flatten → resolve → enrich → transform → load → fix → merge →
# cumulative-rebuild pipeline. The two entry points are thin wrappers:
#   * `run_longequity_ingest_sync`  — the scheduled pipeline calls this and
#     reads the result dict; it adds the upstream-freshness fast-path.
#   * `_ingest_long_equity_stream`  — the /api/ingest/long-equity SSE endpoint
#     runs the core in a worker thread and relays its progress as events.
# The core reports progress via a `(level, message)` callback so each wrapper
# renders it the way it needs (a pipeline `current_message` line, or a typed
# SSE event). Before this they were two ~160-line near-duplicates.


def run_longequity_ingest_core(
    sb,
    *,
    progress: Callable[[str, str], None],
) -> dict:
    """Run the full LongEquity ingest against `sb`, reporting progress via
    `progress(level, message)` (`level` is "info" or "error"). Per-file and
    post-processing failures are isolated — a bad file is skipped, a failed
    fix/merge/rebuild is logged non-fatally — so one month never aborts the
    run. Returns:
        {status: "no_new_data"|"ok"|"error", files_processed, months_loaded,
         companies_inserted, metric_rows_inserted, error}
    """
    def info(msg: str) -> None:
        progress("info", msg)

    def err(msg: str) -> None:
        progress("error", msg)

    result: dict = {
        "status": "ok",
        "files_processed": 0,
        "months_loaded": [],
        "companies_inserted": 0,
        "metric_rows_inserted": 0,
        "error": None,
    }

    existing_months = _get_db_longequity_months()
    info(f"{len(existing_months)} month(s) already in DB.")

    info("Acquiring Long Equity files (Storage → remote URL)...")
    try:
        all_files = acquire_raw_longequity_backfill(sb)
    except Exception as e:
        result["status"] = "error"
        result["error"] = f"Acquire failed: {e}"
        err(result["error"])
        return result

    # Filter to only the months we don't already have (unparseable filenames
    # are processed anyway rather than silently dropped).
    files: list[tuple[str, bytes]] = []
    for filename, content in all_files:
        try:
            as_of = _as_of_date_from_filename(filename)
            ym = f"{as_of.year:04d}-{as_of.month:02d}"
            if ym in existing_months:
                continue
            files.append((filename, content))
        except ValueError:
            files.append((filename, content))

    if not files:
        result["status"] = "no_new_data"
        info(f"All {len(all_files)} month(s) already loaded.")
        return result

    info(f"{len(files)} new file(s) to process (skipped {len(all_files) - len(files)} already in DB).")

    # Persisted ticker overrides — shared across all files in this run.
    try:
        db_overrides: list[dict] = get_ticker_overrides(sb)
        info(f"Loaded {len(db_overrides)} ticker override(s) from DB.")
    except Exception as e:
        info(f"Could not load ticker overrides (table may not exist yet): {e}")
        db_overrides = []

    all_new_resolutions: list[dict] = []
    for i, (filename, content) in enumerate(files, 1):
        try:
            as_of = _as_of_date_from_filename(filename)
        except ValueError as e:
            err(f"[{i}/{len(files)}] {filename}: bad filename ({e}); skipping")
            continue
        info("")
        info(f"[{i}/{len(files)}] {filename}  (as_of: {as_of})")

        info("  Flattening grouped headers...")
        try:
            df = flatten_excel(content)
        except Exception as e:
            err(f"  Flatten failed: {e}")
            continue
        info(f"  {len(df)} rows, {len(df.columns)} columns")

        # Detect unknown tickers (not in fill_ticker.json or DB overrides).
        try:
            unknowns = detect_unknown_tickers(df, db_overrides=db_overrides)
        except Exception as e:
            info(f"  Ticker detection failed (skipping): {e}")
            unknowns = []

        if unknowns:
            info(f"  {len(unknowns)} unknown ticker(s): {', '.join(u['ticker'] for u in unknowns)}")
            info("  Resolving via OpenFIGI...")
            try:
                resolved = resolve_via_openfigi(unknowns)
            except Exception as e:
                info(f"  OpenFIGI failed (continuing without): {e}")
                resolved = []

            if resolved:
                info(f"  Resolved {len(resolved)}/{len(unknowns)} ticker(s).")
                try:
                    saved = save_ticker_overrides(sb, resolved)
                    if saved:
                        info(f"  Saved {saved} new resolution(s) to ticker_override table.")
                except Exception as e:
                    info(f"  Could not save resolutions to DB: {e}")

                # Update in-memory overrides so later files benefit too.
                db_overrides = db_overrides + resolved
                all_new_resolutions.extend(resolved)
            else:
                info(f"  Could not resolve {len(unknowns)} ticker(s) — will use fallback values.")
        else:
            info("  All tickers covered by existing mappings.")

        info("  Enriching tickers...")
        try:
            df = enrich_flattened_df_with_primary_listing(
                df, extra_overrides=db_overrides if db_overrides else None,
            )
        except Exception as e:
            err(f"  Enrich failed: {e}")
            continue

        info("  Transforming to metric_data...")
        try:
            prepared = prepare_flattened_for_schema(
                df, as_of_date=as_of, source_code="longequity",
            )
        except Exception as e:
            err(f"  Transform failed: {e}")
            continue
        info(f"  {len(prepared.company)} companies, {len(prepared.metric_data)} metric rows")

        info("  Loading into Supabase...")
        try:
            load_result = load_prepared_into_supabase(
                prepared, sb, universe_label="LongEquity",
            )
        except Exception as e:
            err(f"  Load failed: {e}")
            continue

        result["files_processed"] += 1
        result["months_loaded"].append(f"{as_of.year:04d}-{as_of.month:02d}")
        result["companies_inserted"] += load_result.company_inserted
        result["metric_rows_inserted"] += load_result.metric_data_inserted
        info(
            f"  Inserted: {load_result.company_inserted} companies, "
            f"{load_result.metric_data_inserted} metric rows"
        )

    # Fix company rows from prior runs that loaded with exchange_id=NULL
    # (before ticker resolution existed).
    if all_new_resolutions:
        info("")
        info("Fixing company records from previous runs with UNKNOWN exchange...")
        try:
            fixed = fix_company_primary_keys(sb, all_new_resolutions)
            info(f"  Fixed {fixed} company record(s)." if fixed else "  No records needed fixing.")
        except Exception as e:
            info(f"  Fix step failed (non-critical): {e}")

    # Merge duplicates (same name + exchange, different ticker — happens when
    # a later run resolves a previously-UNKNOWN row to its real ticker).
    info("")
    info("Checking for duplicate companies...")
    try:
        merge_logs = merge_duplicate_companies(sb)
        if merge_logs:
            for msg in merge_logs:
                info(f"  {msg}")
        else:
            info("  No duplicates found.")
    except Exception as e:
        info(f"  Dedup step failed (non-critical): {e}")

    # Always rebuild the cumulative `longequity` universe so the momentum
    # backtester sees every-ever-seen company on every month from 2002-01
    # onward, and the new month immediately.
    info("")
    info("Rebuilding cumulative LongEquity universe...")
    try:
        from ingest.longequity_universe import (  # noqa: PLC0415
            rebuild_cumulative_longequity_universe,
        )
        res = rebuild_cumulative_longequity_universe(
            sb, on_progress=lambda m: info(f"  {m}"),
        )
        info(
            f"  Done: {res.companies} companies x {res.months} months = "
            f"{res.rows_written} rows. legacy cumulative dropped: "
            f"{res.deleted_old_cumulative}"
        )
    except Exception as e:
        info(f"  Cumulative rebuild failed (non-critical): {e}")

    return result


def run_longequity_ingest_sync(
    supabase_client=None,
    *,
    on_progress: Callable[[str], None] | None = None,
) -> dict:
    """Synchronous LongEquity ingest, driven by the scheduled pipeline.

    Thin wrapper over `run_longequity_ingest_core`: adds the upstream-
    freshness fast-path (skip the whole acquire when upstream has no month
    newer than what's loaded — the dominant happy-path on a weekly tick) and
    flattens the core's `(level, message)` progress into the caller's
    single-line `on_progress`. Returns the core's result dict (see its
    docstring for the shape)."""
    sb = supabase_client or supabase

    def progress(_level: str, msg: str) -> None:
        # The blank-line spacers are SSE cosmetics — skip them for the
        # pipeline's single-line `current_message`.
        if on_progress is not None and msg:
            try:
                on_progress(msg)
            except Exception:
                pass

    # Fast-path: ask upstream if there's anything newer than what we have.
    try:
        latest = check_latest_available_month(supabase=sb)
    except Exception as e:
        progress("info", f"latest-available probe failed (continuing with full acquire): {e}")
        latest = None
    if latest is not None:
        existing_months = _get_db_longequity_months()
        latest_ym = f"{latest.year:04d}-{latest.month:02d}"
        progress("info", f"Upstream latest: {latest_ym}; already loaded: {len(existing_months)} month(s)")
        if latest_ym in existing_months:
            progress("info", "No new LongEquity months upstream — skipping ingest.")
            return {
                "status": "no_new_data",
                "files_processed": 0,
                "months_loaded": [],
                "companies_inserted": 0,
                "metric_rows_inserted": 0,
                "error": None,
            }

    return run_longequity_ingest_core(sb, progress=progress)


async def _ingest_long_equity_stream():
    """Full LongEquity ingest pipeline as SSE. Runs the shared
    `run_longequity_ingest_core` in a worker thread (so the blocking
    Supabase/OpenFIGI calls don't stall the event loop) and relays its
    progress callbacks as typed `info`/`error` events, then a final `done`
    summary built from the result dict."""
    q: _queue.Queue = _queue.Queue()
    holder: dict = {}

    def _run() -> None:
        try:
            holder["result"] = run_longequity_ingest_core(
                supabase, progress=lambda level, msg: q.put((level, msg)),
            )
        except Exception as e:
            q.put(("error", f"Pipeline failed: {e}"))
        finally:
            q.put(None)

    threading.Thread(target=_run, daemon=True).start()

    yield sse_keepalive()
    while True:
        item = await asyncio.to_thread(q.get)
        if item is None:
            break
        level, msg = item
        yield event(level, msg)

    res = holder.get("result") or {}
    if res.get("status") == "no_new_data":
        yield event("done", "Pipeline finished — all months already loaded.")
    elif res.get("status") == "error":
        yield event("done", f"Pipeline stopped: {res.get('error')}")
    else:
        yield event("done", (
            f"Pipeline complete. {res.get('files_processed', 0)} file(s) processed. "
            f"Total new rows — companies: {res.get('companies_inserted', 0)}, "
            f"metric data: {res.get('metric_rows_inserted', 0)}."
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
    # Retained for backwards-compat with any pinned curls; the
    # endpoint now ignores `name`/`description`/dates and always
    # rebuilds the canonical `longequity` cumulative universe.
    name: str | None = None
    description: str | None = None
    start_date: str | None = None
    end_date: str | None = None


@router.post("/api/longequity/save-universe")
async def longequity_save_universe(req: LongEquitySaveUniverseRequest):
    """SSE: rebuild the cumulative `longequity` universe.

    Replaced the old "save a custom universe" flow — the cumulative
    rebuild now runs automatically at the end of each LongEquity
    ingest, and the universe is always one canonical row labeled
    `longequity`. This endpoint stays as a manual trigger for cases
    where the user wants to force a rebuild without re-ingesting
    (e.g. after a manual metric_data fix).
    """
    def _run(q: _queue.Queue):
        def emit(step: str, status: str, message: str):
            q.put(json.dumps({
                "type": "progress", "step": step, "status": status, "message": message,
            }))

        try:
            from ingest.longequity_universe import (  # noqa: PLC0415
                rebuild_cumulative_longequity_universe,
            )
            emit('rebuild', 'in_progress', 'Rebuilding cumulative LongEquity universe...')
            res = rebuild_cumulative_longequity_universe(
                supabase,
                on_progress=lambda m: emit('rebuild', 'in_progress', m),
            )
            emit(
                'rebuild', 'done',
                f'Done: {res.companies} companies x {res.months} months = '
                f'{res.rows_written} rows. legacy cumulative dropped: '
                f'{res.deleted_old_cumulative}',
            )
            q.put(json.dumps({
                'type': 'done',
                'message': (
                    f'longequity universe: {res.rows_written:,} rows across '
                    f'{res.months} months ({res.companies} companies).'
                ),
                'data': {
                    'universe_id': res.universe_id,
                    'label': 'LongEquity',
                    'months': res.months,
                    'companies': res.companies,
                    'rows_inserted': res.rows_written,
                    'deleted_old_cumulative': res.deleted_old_cumulative,
                },
            }))
        except Exception as e:
            q.put(json.dumps({'type': 'error', 'message': f'{e}\n{traceback.format_exc()}'}))
            return
        finally:
            q.put(None)

    q: _queue.Queue = _queue.Queue()
    threading.Thread(target=_run, args=(q,), daemon=True).start()

    async def generate():
        yield sse_keepalive()
        while True:
            msg = await asyncio.to_thread(q.get)
            if msg is None:
                break
            yield sse_raw(msg)

    return StreamingResponse(generate(), media_type='text/event-stream')
