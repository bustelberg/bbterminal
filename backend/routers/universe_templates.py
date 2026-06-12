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
from datetime import date, datetime, timezone

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
from routers._sse import sse_event
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


# ─── Static (frozen) universe snapshots ─────────────────────────────


def _frozen_summary(u: dict) -> dict:
    """`_summary`-shaped payload for a frozen (static) universe row so the
    /backtest dropdown can render it next to the live templates.

    `template_key` deliberately carries the universe's LABEL (not a real
    template_key — a frozen universe's is NULL): the dropdown sends
    `index_name = template_key` as the backtest's `index_universe`, and
    `_load_index_universe` resolves a frozen universe via its label
    fallback. So the label IS the selector."""
    uid = u["universe_id"]
    try:
        m_resp = supabase.rpc("universe_available_months", {"p_universe_id": uid}).execute()
        months = [r["target_month"] for r in (m_resp.data or []) if r.get("target_month")]
    except Exception:
        months = []
    earliest_month = months[0] if months else None
    latest_month = months[-1] if months else None
    latest_count = 0
    if latest_month:
        c_resp = (
            supabase.table("universe_membership")
            .select("company_id", count="exact")
            .eq("universe_id", uid)
            .eq("target_month", latest_month)
            .limit(0)
            .execute()
        )
        latest_count = getattr(c_resp, "count", 0) or 0
    return {
        "template_key": u["label"],
        "label": u["label"],
        "description": u.get("description"),
        "earliest_date": f"{earliest_month}-01" if earliest_month else None,
        "universe_id": uid,
        "months_captured": len(months),
        "earliest_captured_month": earliest_month,
        "latest_captured_month": latest_month,
        "latest_membership_count": latest_count,
        "last_refreshed_at": u.get("frozen_at"),
        "frozen_at": u.get("frozen_at"),
        "frozen_from": u.get("frozen_from"),
    }


def _copy_memberships_via_supabase(src_id: int, dst_id: int) -> int:
    """PostgREST fall-back for the membership copy when direct Postgres
    (SUPABASE_DB_URL) isn't available: page through the source rows and bulk
    insert them under the new universe_id."""
    from deps import chunked, paginate  # noqa: PLC0415

    cols = ("company_id", "target_month", "universe_ticker", "sector", "industry")
    rows = list(paginate(
        lambda lo, hi: supabase.table("universe_membership")
        .select(",".join(cols))
        .eq("universe_id", src_id)
        .order("company_id")
        .order("target_month")
        .range(lo, hi)
        .execute()
    ))
    payload = [{"universe_id": dst_id, **{c: r.get(c) for c in cols}} for r in rows]
    for chunk in chunked(payload, 500):
        supabase.table("universe_membership").insert(chunk).execute()
    return len(payload)


def _freeze_month_snapshot(src_id: int, dst_id: int, month: str, on_progress=None) -> int:
    """Copy ONE month's constituents (`month` = YYYY-MM) of `src` into `dst`,
    stored under that single month. A universe with a lone month is treated
    as a CONSTANT basket by the backtest loader (`broadcast_constant`) — its
    set is applied to every rebalance — so there's no need to materialize a
    row per (member × month). ~one membership-sized insert.

    `on_progress(message)` (optional) is called as the copy proceeds so an SSE
    caller can surface real per-chunk progress instead of a single spinner."""
    from deps import chunked, paginate  # noqa: PLC0415

    def _note(msg: str) -> None:
        if on_progress:
            on_progress(msg)

    _note(f"Reading {month} membership…")
    cols = ("company_id", "universe_ticker", "sector", "industry")
    members = list(paginate(
        lambda lo, hi: supabase.table("universe_membership")
        .select(",".join(cols))
        .eq("universe_id", src_id)
        .like("target_month", f"{month}%")
        .order("company_id")
        .range(lo, hi)
        .execute()
    ))
    if not members:
        return 0
    payload = [
        {"universe_id": dst_id, "target_month": month, **{c: m.get(c) for c in cols}}
        for m in members
    ]
    total = len(payload)
    _note(f"Copying {total} constituents…")
    done = 0
    for chunk in chunked(payload, 500):
        supabase.table("universe_membership").insert(chunk).execute()
        done += len(chunk)
        _note(f"Inserted {done}/{total} constituents…")
    return total


@router.get("/api/static-universes")
async def list_static_universes(response: Response):
    """Frozen universe snapshots (`frozen_at` set, `template_key` NULL) — the
    reproducible, pipeline-immune universes the /backtest dropdown lists
    alongside the live templates. Newest snapshot first."""
    response.headers["Cache-Control"] = CACHE_PIPELINE

    def _q():
        resp = (
            supabase.table("universe")
            .select("universe_id, label, description, frozen_at, frozen_from")
            .not_.is_("frozen_at", "null")
            .order("frozen_at", desc=True)
            .execute()
        )
        return [_frozen_summary(u) for u in (resp.data or [])]

    return await asyncio.to_thread(_q)


def _freeze_core(key: str, as_of: str | None, on_progress=None, *, src_universe_id: int | None = None) -> dict:
    """The actual freeze work, with optional `on_progress(message)` callbacks so
    an SSE caller can stream real progress. Raises HTTPException on bad input
    (propagated as an HTTP error by the JSON path, mapped to an `error` SSE
    event by the streaming path). Returns the frozen-snapshot summary dict.

    `src_universe_id` lets a NON-template universe (e.g. the SP500 universe,
    which lives in the `universe` table but isn't a registered template) be
    frozen through the same path: when given, the template lookup is skipped and
    that universe is the copy source. `key` is still used to label the snapshot
    (`"<key> (as of …)"`)."""
    def _note(msg: str) -> None:
        if on_progress:
            on_progress(msg)

    if src_universe_id is not None:
        src_id = src_universe_id
    else:
        _note(f"Resolving {key} template…")
        try:
            template = get_template(key)
        except KeyError as e:
            raise HTTPException(status_code=404, detail=f"Unknown template '{key}'") from e
        src_id = template.universe_id(supabase)
        if src_id is None:
            raise HTTPException(status_code=409, detail=f"Template '{key}' has no universe yet — refresh it first.")

    if as_of:
        month = as_of[:7]
        label = f"{key} (as of {month})"
        description = (
            f"Fixed basket: {key} constituents as of {month}, held constant through "
            f"time (a single snapshot the backtest applies to every month). The "
            f"pipeline never refreshes it."
        )
    else:
        today = date.today().isoformat()
        label = f"{key} (as of {today})"
        description = f"Static snapshot of {key} frozen {today}. Membership is fixed — the pipeline never refreshes it."

    _note("Checking for an existing snapshot…")
    existing = (
        supabase.table("universe")
        .select("universe_id, label, description, frozen_at, frozen_from")
        .eq("label", label)
        .limit(1)
        .execute()
    )
    if existing.data:
        return {"created": False, **_frozen_summary(existing.data[0])}

    _note(f"Creating snapshot universe \"{label}\"…")
    ins = (
        supabase.table("universe")
        .insert({
            "label": label,
            "template_key": None,
            "frozen_at": datetime.now(timezone.utc).isoformat(),
            "frozen_from": key,
            "description": description,
        })
        .execute()
    )
    dst_id = ins.data[0]["universe_id"]

    if as_of:
        copied = _freeze_month_snapshot(src_id, dst_id, month, on_progress=on_progress)
        if not copied:
            # No constituents at that month — don't leave an empty
            # universe lying around.
            supabase.table("universe").delete().eq("universe_id", dst_id).execute()
            raise HTTPException(
                status_code=409,
                detail=f"{key} has no membership for {month} — pick a captured month.",
            )
    else:
        _note("Copying full membership history…")
        from momentum.data._pg import copy_universe_memberships_via_pg  # noqa: PLC0415
        copied = copy_universe_memberships_via_pg(src_id, dst_id)
        if copied is None:
            copied = _copy_memberships_via_supabase(src_id, dst_id)

    _note("Finalizing…")
    u = (
        supabase.table("universe")
        .select("universe_id, label, description, frozen_at, frozen_from")
        .eq("universe_id", dst_id)
        .limit(1)
        .execute()
    ).data[0]
    return {"created": True, "members_copied": copied, **_frozen_summary(u)}


def _freeze_done_message(result: dict) -> str:
    """Human-readable `done`-event summary mirroring the message the /acwi UI
    used to compose client-side."""
    label = result.get("label", "snapshot")
    if result.get("created") is False:
        return f'Already frozen: "{label}" — selectable in /backtest + /regime-detector.'
    copied = result.get("members_copied", 0)
    return (
        f'Froze "{label}" — {copied} constituents held constant. '
        f"Now selectable in /backtest + /regime-detector."
    )


@router.post("/api/universe-templates/{key}/freeze")
async def freeze_template(key: str, request: Request, as_of: str | None = None):
    """Snapshot a live template universe into a static, NON-template universe
    (`template_key = NULL`) so the pipeline never re-reconstructs it and
    backtests against it are reproducible.

    Two modes:
      - `as_of=YYYY-MM[-DD]` → FIXED BASKET: the constituents on that month,
        replicated across every month so a backtest holds them constant.
        Label `"<KEY> (as of YYYY-MM)"`.
      - no `as_of` → full-history copy labelled with today's date.
    Idempotent on the resulting label — a repeat call returns the existing
    snapshot instead of duplicating it.

    Content-negotiated: clients sending `Accept: text/event-stream` (the /acwi
    page) get an SSE progress stream (`progress`/`done`/`error` events); all
    other callers (the /leonteq page, external scripts) get the JSON summary."""
    wants_sse = "text/event-stream" in (request.headers.get("accept") or "")

    if not wants_sse:
        return await asyncio.to_thread(_freeze_core, key, as_of, None)

    q: _queue.Queue = _queue.Queue()

    def _worker():
        def push(message: str):
            q.put(json.dumps({"type": "progress", "message": message}))
        try:
            result = _freeze_core(key, as_of, push)
            q.put(json.dumps({
                "type": "done",
                "message": _freeze_done_message(result),
                "result": result,
            }))
        except HTTPException as he:
            q.put(json.dumps({"type": "error", "message": str(he.detail)}))
        except Exception as e:  # noqa: BLE001
            q.put(json.dumps({"type": "error", "message": f"{e}\n{traceback.format_exc()}"}))
        q.put(None)

    threading.Thread(target=_worker, daemon=True).start()

    async def _gen():
        async for chunk in drain_thread_queue(q):
            yield chunk
    return StreamingResponse(_gen(), media_type="text/event-stream")


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
            yield sse_event({
                "type": "progress",
                "message": f"'{t.label}' is already refreshing — watching shared progress.",
            })
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
