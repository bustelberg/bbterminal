"""HTTP endpoints for the template-managed universe model.

Each registered `UniverseTemplate` (`backend/index_universe/templates/`)
gets a uniform set of endpoints:

  GET    /api/universe-templates                  — list all templates
  GET    /api/universe-templates/{key}            — one template summary
  GET    /api/universe-templates/{key}/months     — captured months list
  GET    /api/universe-templates/{key}/membership — holdings on a date
  POST   /api/universe-templates/{key}/refresh    — SSE: trigger refresh

The `/api/acwi/save-universe` endpoint is superseded by
`POST /api/universe-templates/ACWI/refresh` and removed in the same
rebuild that added this module.
"""
from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import queue as _queue
import threading
import traceback
from datetime import date

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from concurrent.futures import ThreadPoolExecutor

from deps import supabase
from index_universe.templates import all_templates, get_template
from index_universe.templates import _refresh_status
from index_universe.templates._cache import (
    list_summary_get,
    list_summary_set,
)
from routers._cache_headers import CACHE_PIPELINE
from routers.index_universe._helpers import drain_thread_queue

router = APIRouter(tags=["universe-templates"])


def _summary(template) -> dict:
    """Compact list-row payload: hard-stop, latest captured month,
    membership count at latest month. Used by both the list endpoint
    and the single-template detail endpoint (the detail one adds the
    full months list)."""
    uid = template.universe_id(supabase)
    months = template.available_months(supabase) if uid is not None else []
    latest_month = months[-1] if months else None
    latest_count = 0
    if uid is not None and latest_month:
        # Cheap row count rather than fetching all holdings — the
        # detailed membership endpoint is used when the UI actually
        # wants names.
        c_resp = (
            supabase.table("universe_membership")
            .select("company_id", count="exact")
            .eq("universe_id", uid)
            .eq("target_month", latest_month)
            .limit(0)
            .execute()
        )
        latest_count = getattr(c_resp, "count", 0) or 0
    # `last_refreshed_at` lets the /schedule UI flag templates that have
    # never been refreshed in this env (typically a fresh prod deploy where
    # the universe row exists from migrations but no pipeline has run yet).
    last_refreshed_at = template.last_refreshed_at(supabase) if uid is not None else None
    return {
        "template_key": template.template_key,
        "label": template.label,
        "description": template.description,
        "earliest_date": template.earliest_date.isoformat(),
        "universe_id": uid,
        "months_captured": len(months),
        "earliest_captured_month": months[0] if months else None,
        "latest_captured_month": latest_month,
        "latest_membership_count": latest_count,
        "last_refreshed_at": last_refreshed_at,
    }


@router.get("/api/universe-templates")
async def list_universe_templates(response: Response):
    """Every registered template with its current state.

    Three performance moves vs the naive `[_summary(t) for t in all_templates()]`:

    1. Result is cached in-process for 5 min via `list_summary_get/set`. The
       only thing that changes the payload is a template refresh, which
       explicitly invalidates the cache via `invalidate_template`.
    2. On a cache miss, the per-template summaries run in parallel through
       a small thread pool. Each `_summary` is ~4 sequential Supabase round
       trips and templates are independent, so parallelism turns N*latency
       into ~1*latency. Matters most on the /backtest page where the dropdown
       blocks on this endpoint.
    3. Cache-Control on the response so the browser caches per-tab too;
       repeat opens of the dropdown within 60s don't even round-trip.
    """
    response.headers["Cache-Control"] = CACHE_PIPELINE
    def _q():
        cached = list_summary_get()
        if cached is not None:
            return cached
        templates = list(all_templates())
        with ThreadPoolExecutor(max_workers=max(1, len(templates))) as pool:
            data = list(pool.map(_summary, templates))
        list_summary_set(data)
        return data
    return await asyncio.to_thread(_q)


@router.get("/api/universe-templates/refresh-status")
async def universe_template_refresh_status():
    """Live per-template refresh status from the in-process registry.

    Cheap (no DB) so the frontend can poll it every couple seconds to drive
    the busy spinner + progress bar. Returns only templates touched since
    process start; absent keys mean "idle". Shape: `{template_key:
    {status, message, pct, started_at, finished_at, error}}`.

    NOTE: declared BEFORE `/{template_key}` so this static path isn't
    swallowed by the path-param route."""
    return _refresh_status.get_all()


@router.get("/api/universe-templates/{template_key}")
async def get_universe_template(template_key: str, response: Response):
    """Single template summary + the full list of captured months. The
    list lets the UI build a date scrubber without a second round-trip."""
    try:
        t = get_template(template_key)
    except KeyError:
        raise HTTPException(404, f"Unknown template_key: {template_key}")
    response.headers["Cache-Control"] = CACHE_PIPELINE
    def _q() -> dict:
        s = _summary(t)
        s["months"] = t.available_months(supabase)
        return s
    return await asyncio.to_thread(_q)


@router.get("/api/universe-templates/{template_key}/recent-changes")
async def get_universe_template_recent_changes(
    template_key: str, limit: int = 5,
):
    """Latest N additions/removals/renames for a template, sourced from
    `ingest_run.templates_summary` array entries on the most recent
    successful runs. The /schedule per-template expand uses this to
    show "last week's diff" without re-loading the full membership
    table."""
    try:
        get_template(template_key)  # validate key
    except KeyError:
        raise HTTPException(404, f"Unknown template_key: {template_key}")
    limit = max(1, min(20, limit))

    def _q() -> list[dict]:
        # Pull recent ingest_runs and pluck out THIS template's diff
        # entry. We over-fetch (limit * 4) on the runs query because
        # not every run includes every template (e.g., a daily
        # template-refresh might fail mid-phase and leave gaps).
        resp = (
            supabase.table("ingest_run")
            .select("run_id, started_at, finished_at, status, templates_summary")
            .order("started_at", desc=True)
            .limit(limit * 4)
            .execute()
        )
        rows = resp.data or []
        out: list[dict] = []
        for r in rows:
            summary = r.get("templates_summary") or []
            for entry in summary:
                if entry.get("template_key") != template_key:
                    continue
                # Skip entries that errored — those carry no diff.
                if entry.get("error"):
                    continue
                out.append({
                    "run_id": r["run_id"],
                    "started_at": r["started_at"],
                    "finished_at": r.get("finished_at"),
                    "status": r["status"],
                    "this_month": entry.get("this_month"),
                    "prev_month": entry.get("prev_month"),
                    "additions_count": entry.get("additions_count", 0),
                    "removals_count": entry.get("removals_count", 0),
                    "renames_count": entry.get("renames_count", 0),
                    "additions": entry.get("additions") or [],
                    "removals": entry.get("removals") or [],
                    "renames": entry.get("renames") or [],
                })
                if len(out) >= limit:
                    return out
                break  # one entry per run for this template
        return out

    return await asyncio.to_thread(_q)


@router.get("/api/universe-templates/{template_key}/months")
async def list_universe_template_months(template_key: str, response: Response):
    """Just the captured-months list. Cheaper than the full summary
    when the UI is only re-fetching after a refresh."""
    try:
        t = get_template(template_key)
    except KeyError:
        raise HTTPException(404, f"Unknown template_key: {template_key}")
    response.headers["Cache-Control"] = CACHE_PIPELINE
    months = await asyncio.to_thread(t.available_months, supabase)
    return {"template_key": template_key, "months": months}


@router.get("/api/universe-templates/{template_key}/membership")
async def get_universe_template_membership(
    template_key: str, date: str, request: Request,
):
    """Holdings active in the given month (`date` = 'YYYY-MM' or
    'YYYY-MM-DD'; we use only the year-month portion to look up
    `universe_membership.target_month`).

    Responds with an `ETag` derived from `(template_key, target_month,
    last_refreshed_at)`. Browsers that resend with `If-None-Match` get a
    304 with no body — repeat scrubs through the same months cost zero
    bandwidth + a sub-millisecond server check."""
    try:
        t = get_template(template_key)
    except KeyError:
        raise HTTPException(404, f"Unknown template_key: {template_key}")
    target_month = (date or "")[:7]
    if len(target_month) != 7 or target_month[4] != "-":
        raise HTTPException(400, "date must be 'YYYY-MM' or 'YYYY-MM-DD'")

    def _q() -> tuple[str | None, list[dict]]:
        return t.membership_at_with_meta(supabase, target_month)
    last_refreshed, rows = await asyncio.to_thread(_q)

    # Quoted strong ETag — `W/"..."` would be a weak one (semantically
    # equivalent but not byte-identical), and we ARE byte-identical
    # until the underlying refresh changes anything, so strong is
    # correct.
    etag_seed = f"{template_key}:{target_month}:{last_refreshed or 'never'}"
    etag = '"' + hashlib.sha256(etag_seed.encode()).hexdigest()[:16] + '"'

    if request.headers.get("if-none-match") == etag:
        return Response(
            status_code=304,
            headers={"ETag": etag, "Cache-Control": CACHE_PIPELINE},
        )

    return JSONResponse(
        content={
            "template_key": template_key,
            "target_month": target_month,
            "count": len(rows),
            "membership": rows,
            "last_refreshed_at": last_refreshed,
        },
        headers={"ETag": etag, "Cache-Control": CACHE_PIPELINE},
    )


@router.get("/api/universe-templates/{template_key}/all-companies.csv")
async def export_all_companies_csv(template_key: str):
    """CSV of every company that has ever appeared in this template's
    universe. One row per unique company. Aggregated server-side via
    the `universe_all_companies_ever` SQL function (single round-trip).

    Columns: exchange_code, gurufocus_ticker, company_name,
    exchange_name, sector, gurufocus_url.
    """
    try:
        t = get_template(template_key)
    except KeyError:
        raise HTTPException(404, f"Unknown template_key: {template_key}")

    def _build() -> str:
        rows = t.all_companies_ever(supabase)
        buf = io.StringIO()
        writer = csv.writer(buf)
        # Header matches the user-requested column order:
        # exchange ticker / company ticker / company name / exchange name / sector / gurufocus link.
        writer.writerow([
            "exchange_code",
            "gurufocus_ticker",
            "company_name",
            "exchange_name",
            "sector",
            "gurufocus_url",
        ])
        for r in rows:
            writer.writerow([
                r.get("exchange_code") or "",
                r.get("gurufocus_ticker") or "",
                r.get("company_name") or "",
                r.get("exchange_name") or "",
                r.get("sector") or "",
                r.get("gurufocus_url") or "",
            ])
        return buf.getvalue()

    body = await asyncio.to_thread(_build)
    filename = f"{template_key}-all-companies-{date.today().isoformat()}.csv"
    return Response(
        content=body,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/api/universe-templates/{template_key}/refresh")
async def refresh_universe_template(template_key: str):
    """Trigger a refresh: SSE stream of progress events from the
    template's reconstruction pipeline. Same event shape the old
    `/api/acwi/save-universe` produced (`progress`, `done`, `error`)
    so the /acwi frontend's progress timeline keeps working."""
    try:
        t = get_template(template_key)
    except KeyError:
        raise HTTPException(404, f"Unknown template_key: {template_key}")

    q: _queue.Queue = _queue.Queue()

    # Already refreshing (e.g. a scheduled tick, or another tab triggered
    # it)? Don't kick a duplicate heavy reconstruction — tell the client to
    # watch the shared progress instead. The registry-backed status endpoint
    # + the UI's poll keep the spinner/progress bar live either way.
    if _refresh_status.is_running(template_key):
        async def _busy_gen():
            yield "data: " + json.dumps({
                "type": "progress",
                "message": f"'{t.label}' is already refreshing — watching shared progress.",
            }) + "\n\n"
        return StreamingResponse(_busy_gen(), media_type="text/event-stream")

    def _worker():
        def push(message: str, pct: int | None = None):
            payload: dict = {"type": "progress", "message": message}
            if pct is not None:
                payload["pct"] = pct
            q.put(json.dumps(payload))
        try:
            # tracked_refresh updates the in-process registry (busy + live
            # progress) AND forwards each event to `push` for this client's
            # SSE stream.
            result = _refresh_status.tracked_refresh(
                t, supabase, extra_on_progress=push,
            )
            q.put(json.dumps({
                "type": "done",
                "message": (
                    f"Refreshed '{t.label}': {result.months_written} months, "
                    f"current diff +{result.diff.additions_count}/"
                    f"-{result.diff.removals_count}/r{result.diff.renames_count}"
                ),
                "stats": {
                    "template_key": result.template_key,
                    "universe_id": result.universe_id,
                    "months_written": result.months_written,
                    "diff": result.diff.to_summary_entry(),
                },
            }))
        except Exception as e:
            q.put(json.dumps({"type": "error", "message": f"{e}\n{traceback.format_exc()}"}))
        q.put(None)

    threading.Thread(target=_worker, daemon=True).start()

    async def _gen():
        async for chunk in drain_thread_queue(q):
            yield chunk
    return StreamingResponse(_gen(), media_type="text/event-stream")
