"""Scheduled refresh pipeline — HTTP layer.

Backs the /schedule page. The pipeline itself (run-row tracking + the
five phases + the three orchestrators) lives in the `ingest.phases`
package, split one module per phase so each is unit-testable in
isolation. This module keeps only the HTTP surface:

  * `kick_off_refresh` / `_spawn_ingest` — job dispatch (by `job_name`)
    onto the right orchestrator in a daemon thread.
  * POST /api/ingest/scheduled-refresh/{cron,trigger} — start a run.
  * GET  /api/ingest/runs[/{run_id}[/templates/...]] — read run rows.
  * GET  /api/schedule/upcoming — the live scheduler activity strip.

Each run executes these phases in order (see `ingest.phases.pipeline`):

  0. Source acquisition  — probe upstream (ACWI XLS staleness).
  1. Template refresh     — reconstruct every `UniverseTemplate`.
  2. Orphan prune         — drop companies no source universe holds.
  2.5. Duplicate merge    — collapse cross-source duplicate rows.
  3. Price + volume        — refresh every surviving company.
  4. Momentum compute      — one current-picks snapshot per strategy.

Phases run independently — a failure in one is captured in
`error_summary` but the next phase still attempts.

Two scheduled triggers (defined in `scheduler.py`):
    weekly_price_volume   Tuesday 02:00 UTC      — captures Monday closes
    monthly_price_volume  2nd of month 02:00 UTC — captures month-start

Both run the SAME pipeline; the names just record cadence on the row.
Manual "Run now" from /schedule uses `triggered_by='manual'`.
"""
from __future__ import annotations

import asyncio
import os
import threading
from datetime import datetime, timezone

from fastapi import APIRouter, Header, HTTPException

from deps import supabase
from ingest.phases import (
    _create_run,
    _run_daily_mtd_pipeline_sync,
    _run_daily_template_refresh_pipeline_sync,
    _run_pipeline_sync,
)

router = APIRouter(tags=["ingest"])

_VALID_JOB_NAMES = {
    "weekly_price_volume",
    "monthly_price_volume",
    "manual",
    # Daily MTD refresh: fires Wed-Sat 02:00 UTC. Refreshes prices for the
    # pooled set of held companies across all enabled scheduled strategies,
    # then recomputes + persists MTD on each strategy's latest snapshot so
    # the /schedule UI shows fresh "to-date" stats every trading day.
    "daily_holdings_refresh",
    # Daily template refresh: fires Mon, Wed-Sun 02:30 UTC (skipping Tue
    # which the weekly full pipeline covers). Lightweight — runs only
    # acquisition + templates + prune + dedupe, no prices/momentum.
    # Picks up MSCI announcement changes for ACWI and Leonteq eligibility
    # list updates on a daily cadence so /schedule's per-template
    # additions/removals view stays current.
    "daily_template_refresh",
    # One-shot bootstrap fired by `scheduler.py` at app start when one or
    # more registered templates have never been refreshed in this env
    # (typically the first request after a deploy that introduced a new
    # template, or a fresh prod environment where migrations created the
    # universe row but no pipeline has run yet). Same pipeline as the
    # weekly/monthly ticks — distinguished by job_name so the run row's
    # `triggered_by` history is clear.
    "bootstrap_template_refresh",
}


def _spawn_ingest(run_id: int, job_name: str) -> None:
    """Dispatch by `job_name`. The full pipeline (acquisition → … →
    momentum) runs for the weekly/manual/bootstrap jobs; the lightweight
    daily orchestrators run for `daily_holdings_refresh` (prices+MTD)
    and `daily_template_refresh` (templates+prune+dedupe)."""
    if job_name == "daily_holdings_refresh":
        target = _run_daily_mtd_pipeline_sync
    elif job_name == "daily_template_refresh":
        target = _run_daily_template_refresh_pipeline_sync
    else:
        target = _run_pipeline_sync
    threading.Thread(
        target=target,
        args=(run_id,),
        daemon=True,
        name=f"pipeline-run-{run_id}",
    ).start()


def kick_off_refresh(job_name: str, triggered_by: str) -> int:
    """Public entry point. Inserts an `ingest_run` row + spawns the daemon
    thread; returns the new `run_id`. Used by both the HTTP endpoints below
    and the in-process APScheduler defined in `scheduler.py`."""
    if job_name not in _VALID_JOB_NAMES:
        raise ValueError(
            f"Unknown job_name {job_name!r}; expected one of {sorted(_VALID_JOB_NAMES)}"
        )
    if triggered_by not in ("auto", "manual"):
        raise ValueError(f"triggered_by must be 'auto' or 'manual', got {triggered_by!r}")
    run_id = _create_run(job_name, triggered_by)
    _spawn_ingest(run_id, job_name)
    return run_id


@router.post("/api/ingest/scheduled-refresh/cron")
async def cron_scheduled_refresh(
    job_name: str = "weekly_price_volume",
    x_cron_secret: str = Header(default=""),
):
    """Cron entry point. Verifies `X-Cron-Secret`, inserts an `ingest_run`
    row tagged `triggered_by='auto'`, spawns the work in a daemon thread,
    and returns the run_id immediately so the cron's curl exits fast.
    The Railway cron just needs the 200 — it doesn't wait for completion."""
    expected = os.environ.get("CRON_SECRET", "")
    if not expected:
        raise HTTPException(500, "CRON_SECRET env var is not set on the server")
    if x_cron_secret != expected:
        raise HTTPException(401, "Invalid cron secret")
    if job_name not in _VALID_JOB_NAMES:
        raise HTTPException(
            400,
            f"Unknown job_name {job_name!r}; expected one of {sorted(_VALID_JOB_NAMES)}",
        )

    run_id = await asyncio.to_thread(_create_run, job_name, "auto")
    _spawn_ingest(run_id, job_name)
    return {"run_id": run_id, "status": "running", "job_name": job_name}


@router.post("/api/ingest/scheduled-refresh/trigger")
async def trigger_scheduled_refresh(job_name: str = "manual"):
    """Manual trigger from the /schedule UI's Run-now button. Same work
    as the cron path, just `triggered_by='manual'`. No cron-secret —
    auth is enforced by the frontend proxy middleware in `frontend/proxy.ts`."""
    if job_name not in _VALID_JOB_NAMES:
        raise HTTPException(
            400,
            f"Unknown job_name {job_name!r}; expected one of {sorted(_VALID_JOB_NAMES)}",
        )
    run_id = await asyncio.to_thread(_create_run, job_name, "manual")
    _spawn_ingest(run_id, job_name)
    return {"run_id": run_id, "status": "running", "job_name": job_name}


@router.get("/api/ingest/runs")
async def list_ingest_runs(limit: int = 25, job_name: str | None = None):
    """Recent ingest runs (newest first). Caps `limit` to 200. Pass
    `job_name=...` to filter to a single job type — the /schedule
    daily-MTD card uses this to fetch only its own runs."""
    limit = max(1, min(200, limit))

    def _query() -> list[dict]:
        q = (
            supabase.table("ingest_run")
            .select("*")
            .order("started_at", desc=True)
            .limit(limit)
        )
        if job_name:
            q = q.eq("job_name", job_name)
        return q.execute().data or []

    return await asyncio.to_thread(_query)


# Human-facing metadata for each known scheduler job id, keyed by the
# APScheduler job id (see `scheduler.py`). `label`/`description` drive the
# /schedule "Pipeline activity" strip; `cadence` is the recurring trigger
# in words. One-shot catch-up jobs are included so a bootstrap/catch-up
# that's still pending (or just fired) reads sensibly instead of as a raw
# id. Unknown ids fall back to a humanized id + the run's own job_name.
_JOB_META: dict[str, dict[str, str]] = {
    "weekly_price_volume": {
        "label": "Weekly full pipeline",
        "description": "acquisition → templates → prune → prices → momentum",
        "cadence": "Tuesday 02:00 UTC",
    },
    "daily_holdings_refresh": {
        "label": "Daily MTD refresh",
        "description": "re-prices held companies + recomputes MTD per strategy",
        "cadence": "Mon, Wed–Sun 02:00 UTC",
    },
    "daily_template_refresh": {
        "label": "Daily template refresh",
        "description": "reconstructs template universes (no prices/momentum)",
        "cadence": "Mon, Wed–Sun 02:30 UTC",
    },
    "monthly_template_refresh": {
        "label": "Monthly template refresh",
        "description": "month-boundary template capture",
        "cadence": "1st of month 00:30 UTC",
    },
    "bootstrap_template_refresh": {
        "label": "Full pipeline (bootstrap)",
        "description": "one-shot first population on app start",
        "cadence": "one-shot",
    },
    "startup_template_catchup": {
        "label": "Template catch-up",
        "description": "one-shot — templates fell behind the month while down",
        "cadence": "one-shot",
    },
    "startup_price_catchup": {
        "label": "Price catch-up",
        "description": "one-shot — held prices fell behind while down",
        "cadence": "one-shot",
    },
    "manual": {
        "label": "Manual run",
        "description": "triggered from the UI",
        "cadence": "on demand",
    },
}


def _job_meta(key: str | None) -> dict[str, str]:
    """Look up display metadata for a scheduler job id / job_name, with a
    humanized fallback for ids we don't have a hard-coded entry for."""
    if key and key in _JOB_META:
        return _JOB_META[key]
    pretty = (key or "job").replace("_", " ").strip().capitalize()
    return {"label": pretty, "description": "", "cadence": ""}


@router.get("/api/schedule/upcoming")
async def schedule_upcoming():
    """Backs the /schedule "Pipeline activity" strip.

    Returns the live scheduler job list (with next-fire times, straight
    from the in-process APScheduler so it's the single source of truth —
    including one-shot catch-up jobs the bootstrap path schedules on app
    start) plus every `ingest_run` currently in `status='running'`. The
    frontend renders the running set as a "Running now" group and the
    rest as a chronological "Upcoming" list, marking a job busy when a
    run that fires it is in flight."""
    from scheduler import list_scheduled_jobs  # noqa: PLC0415 — avoid import cycle

    def _running() -> list[dict]:
        resp = (
            supabase.table("ingest_run")
            .select(
                "run_id, job_name, triggered_by, started_at, "
                "current_phase, current_message"
            )
            .eq("status", "running")
            .order("started_at", desc=False)
            .execute()
        )
        return resp.data or []

    running = await asyncio.to_thread(_running)
    running_job_names = {r.get("job_name") for r in running}

    raw_jobs = list_scheduled_jobs()
    jobs: list[dict] = []
    for j in raw_jobs:
        meta = _job_meta(j.get("id"))
        fires = j.get("fires")
        jobs.append({
            "id": j.get("id"),
            "fires": fires,
            "next_run_at": j.get("next_run_at"),
            "label": meta["label"],
            "description": meta["description"],
            "cadence": meta["cadence"],
            # Busy when a run that fires this job is currently in flight.
            "running": fires in running_job_names or j.get("id") in running_job_names,
        })
    # Chronological — jobs with no next_run_at (paused) sort last.
    jobs.sort(key=lambda x: (x["next_run_at"] is None, x["next_run_at"] or ""))

    running_out = [
        {
            **r,
            "label": _job_meta(r.get("job_name"))["label"],
        }
        for r in running
    ]

    return {
        "now": datetime.now(timezone.utc).isoformat(),
        "scheduler_enabled": bool(raw_jobs),
        "jobs": jobs,
        "running": running_out,
    }


@router.get("/api/ingest/runs/{run_id}")
async def get_ingest_run(run_id: int):
    """Single ingest_run row by id."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("ingest_run")
        .select("*")
        .eq("run_id", run_id)
        .limit(1)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Run not found")
    return resp.data[0]


@router.get("/api/ingest/runs/{run_id}/templates/{template_key}/membership")
async def get_template_membership_for_run(
    run_id: int, template_key: str, q: str = "", limit: int = 500,
):
    """Universe membership captured by this run's templates phase, for
    the given `template_key`. Reads `universe_id` + `this_month` from
    the run's `templates_summary` array entry (set by the templates
    phase as each template's refresh completes). Returns 404 when the
    run didn't include the requested template, or when its diff entry
    failed (no `this_month`)."""
    limit = max(1, min(5000, limit))

    def _query() -> list[dict]:
        run_resp = (
            supabase.table("ingest_run")
            .select("templates_summary")
            .eq("run_id", run_id)
            .limit(1)
            .execute()
        )
        if not run_resp.data:
            raise HTTPException(404, "Run not found")
        summaries = run_resp.data[0].get("templates_summary") or []
        entry = next((s for s in summaries if s.get("template_key") == template_key), None)
        if entry is None:
            raise HTTPException(404, f"Run has no entry for template {template_key}")
        uid = entry.get("universe_id")
        month = entry.get("this_month")
        if uid is None or month is None:
            raise HTTPException(404, f"Run's {template_key} entry has no universe captured")

        mem_resp = (
            supabase.table("universe_membership")
            .select(
                "company_id, universe_ticker, sector, "
                "company:company(company_name, gurufocus_ticker, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code))"
            )
            .eq("universe_id", uid)
            .eq("target_month", month)
            .limit(limit)
            .execute()
        )
        rows = mem_resp.data or []
        ql = q.strip().lower()
        out: list[dict] = []
        for r in rows:
            company = r.get("company") or {}
            exchange = ((company.get("gurufocus_exchange") or {}).get("exchange_code")) or ""
            name = company.get("company_name") or ""
            ticker = r.get("universe_ticker") or company.get("gurufocus_ticker") or ""
            if ql and ql not in name.lower() and ql not in ticker.lower():
                continue
            out.append({
                "company_id": r.get("company_id"),
                "ticker": ticker,
                "company_name": name,
                "exchange": exchange,
                "sector": r.get("sector"),
            })
        out.sort(key=lambda x: (x.get("ticker") or "").upper())
        return out

    return await asyncio.to_thread(_query)
