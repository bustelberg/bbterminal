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


def run_longequity_ingest_sync(
    supabase_client=None,
    *,
    on_progress: Callable[[str], None] | None = None,
) -> dict:
    """Synchronous LongEquity ingest, driven by the scheduled pipeline.

    Same flow as the SSE generator (acquire → flatten → resolve →
    enrich → transform → load → cumulative-universe rebuild), but
    returns a result dict instead of yielding events. `on_progress`
    receives one line per major step so the caller can surface it in
    the pipeline's `current_message`.

    Short-circuits when upstream has no newer month than what's
    already loaded — the dominant happy-path on a weekly tick.
    Returns:
        {
            "status": "no_new_data" | "ok" | "error",
            "files_processed": int,
            "months_loaded": [YYYY-MM],
            "companies_inserted": int,
            "metric_rows_inserted": int,
            "error": Optional[str],
        }
    """
    sb = supabase_client or supabase

    def emit(msg: str) -> None:
        if on_progress is not None:
            try:
                on_progress(msg)
            except Exception:
                pass

    result = {
        "status": "ok",
        "files_processed": 0,
        "months_loaded": [],
        "companies_inserted": 0,
        "metric_rows_inserted": 0,
        "error": None,
    }

    # Fast-path: ask upstream if there's anything newer than what we have.
    # If `check_latest_available_month` returns None or matches an existing
    # month, we have nothing to do.
    try:
        latest = check_latest_available_month(supabase=sb)
    except Exception as e:
        emit(f"latest-available probe failed (continuing with full acquire): {e}")
        latest = None

    existing_months = _get_db_longequity_months()
    if latest is not None:
        latest_ym = f"{latest.year:04d}-{latest.month:02d}"
        emit(f"Upstream latest: {latest_ym}; already loaded: {len(existing_months)} month(s)")
        if latest_ym in existing_months:
            result["status"] = "no_new_data"
            emit("No new LongEquity months upstream — skipping ingest.")
            return result

    emit("Acquiring Long Equity files (Storage → remote URL)…")
    try:
        all_files = acquire_raw_longequity_backfill(sb)
    except Exception as e:
        result["status"] = "error"
        result["error"] = f"acquire failed: {e}"
        emit(result["error"])
        return result

    # Filter to only the months we don't already have.
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
        emit(f"All {len(all_files)} month(s) already loaded.")
        return result

    emit(f"{len(files)} new month(s) to ingest.")

    try:
        db_overrides: list[dict] = get_ticker_overrides(sb)
    except Exception:
        db_overrides = []

    all_new_resolutions: list[dict] = []
    for i, (filename, content) in enumerate(files, 1):
        try:
            as_of = _as_of_date_from_filename(filename)
        except ValueError as e:
            emit(f"  [{i}/{len(files)}] {filename}: bad filename ({e}); skipping")
            continue
        emit(f"  [{i}/{len(files)}] {filename} (as_of {as_of}) — flatten + load")

        try:
            df = flatten_excel(content)
            try:
                unknowns = detect_unknown_tickers(df, db_overrides=db_overrides)
            except Exception:
                unknowns = []
            if unknowns:
                try:
                    resolved = resolve_via_openfigi(unknowns)
                except Exception:
                    resolved = []
                if resolved:
                    try:
                        save_ticker_overrides(sb, resolved)
                    except Exception:
                        pass
                    db_overrides = db_overrides + resolved
                    all_new_resolutions.extend(resolved)
            df = enrich_flattened_df_with_primary_listing(
                df, extra_overrides=db_overrides if db_overrides else None,
            )
            prepared = prepare_flattened_for_schema(
                df, as_of_date=as_of, source_code="longequity",
            )
            load_result = load_prepared_into_supabase(
                prepared, sb, universe_label="LongEquity",
            )
        except Exception as e:
            emit(f"    failed: {e}")
            continue

        result["files_processed"] += 1
        result["months_loaded"].append(f"{as_of.year:04d}-{as_of.month:02d}")
        result["companies_inserted"] += load_result.company_inserted
        result["metric_rows_inserted"] += load_result.metric_data_inserted

    # Fix any UNKNOWN-exchange rows from prior runs.
    if all_new_resolutions:
        try:
            fix_company_primary_keys(sb, all_new_resolutions)
        except Exception:
            pass

    # Merge same-name-different-ticker duplicates that this run may have
    # introduced (a previously-UNKNOWN row gaining its real ticker).
    try:
        merge_duplicate_companies(sb)
    except Exception:
        pass

    # Always rebuild the cumulative LongEquity universe at the end so
    # the momentum backtester sees the new month immediately.
    emit("Rebuilding cumulative LongEquity universe…")
    try:
        from ingest.longequity_universe import (  # noqa: PLC0415
            rebuild_cumulative_longequity_universe,
        )
        rebuild_result = rebuild_cumulative_longequity_universe(
            sb, on_progress=lambda m: emit(f"  {m}"),
        )
        emit(
            f"  rebuilt: {rebuild_result.companies} companies × "
            f"{rebuild_result.months} months = {rebuild_result.rows_written} rows"
        )
    except Exception as e:
        emit(f"  cumulative rebuild failed (non-critical): {e}")

    return result


async def _ingest_long_equity_stream():
    """Full LongEquity ingest pipeline as SSE: acquire → flatten → resolve
    tickers → enrich → transform → load → de-dup. Each step yields its
    own `info`/`error` events; only new months (not already in DB) are
    processed."""
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
                universe_label="LongEquity",
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

    # Rebuild the cumulative `longequity` universe so the momentum
    # backtester sees every-ever-seen company on every month from
    # 2002-01 onward. Replaces what used to be a manual button on the
    # LongEquity page — keeps the universe in sync with metric_data
    # automatically. Also drops the legacy `longequity_cumulative`
    # universe (kept as a one-shot user button before this change).
    yield event("info", "")
    yield event("info", "Rebuilding cumulative LongEquity universe...")
    try:
        from ingest.longequity_universe import (  # noqa: PLC0415
            rebuild_cumulative_longequity_universe,
        )
        emit_log: list[str] = []
        res = await asyncio.to_thread(
            rebuild_cumulative_longequity_universe, supabase,
            on_progress=emit_log.append,
        )
        for msg in emit_log:
            yield event("info", f"  {msg}")
        yield event(
            "info",
            f"  Done: {res.companies} companies x {res.months} months = "
            f"{res.rows_written} rows. "
            f"legacy cumulative dropped: {res.deleted_old_cumulative}",
        )
    except Exception as e:
        yield event("info", f"  Cumulative rebuild failed (non-critical): {e}")

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


