"""In-process APScheduler for the weekly + daily pipeline ticks.

Two `BackgroundScheduler` cron triggers run inside the FastAPI process:

    weekly_price_volume     Tue 02:00 UTC          — full pipeline
        (acquisition → templates → prune → prices → momentum)
    daily_holdings_refresh  Wed-Sat 02:00 UTC      — lightweight MTD-only
        (prices for held companies → MTD persist per strategy)

The daily tick captures the prior trading day's close (Tue/Wed/Thu/Fri
US close → next-day 02:00 UTC after ~5h GuruFocus settle) so the
/schedule "Daily MTD refresh" card surfaces fresh to-date stats every
trading day. Tuesday is intentionally skipped — the weekly full
pipeline already does everything the daily would, plus more.

(The previous monthly 2nd-of-month tick was retired: per-strategy
`frequency` + `next_due_at` on `scheduled_strategy` now drives whether
a strategy rebalances on a given Tuesday — there's no longer a need
for a separate monthly cron. See `scheduled_strategies.py`.)

Each fired tick calls `kick_off_refresh(job_name, "auto")`, which inserts
an `ingest_run` row tagged `triggered_by='auto'` and starts the daemon
worker thread — identical to what the manual UI "Run now" button does.

Trade-offs vs Railway Cron:
  - Code-managed; deploys with the rest of the backend on git push.
  - Single-instance assumption — if Railway ever scales the backend
    horizontally to N instances, each would fire its own tick. The
    freshness checks downstream would no-op duplicates so it's "wasteful
    but harmless" rather than "broken", but worth knowing.
  - A restart that lands exactly on the tick drops it; next week catches
    up. Acceptable for a recovery cadence in days.

Current-picks is deliberately NOT scheduled here — it's an on-demand
action the user kicks off from the UI's "Current Picks" / "Recompute"
buttons.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from routers.ingest_runs import kick_off_refresh

_log = logging.getLogger(__name__)
_scheduler: BackgroundScheduler | None = None

# A one-shot full pipeline fires this many seconds after process start
# when bootstrap is needed (template never refreshed in this env). The
# delay gives the FastAPI app + Supabase client + DB pool a moment to
# settle before the heavy run kicks off.
_BOOTSTRAP_DELAY_SECONDS = 30
# An `ingest_run` row in `running` state newer than this counts as a
# pipeline currently in flight — guards against a bootstrap firing while
# a manual run started moments earlier is still going. Doubles as the
# "consider this row orphaned" cutoff for `_reap_orphan_runs` below.
_PIPELINE_STALE_AFTER_SECONDS = 3600


def _reap_orphan_runs() -> None:
    """Mark any `ingest_run` row stuck in `status='running'` for longer
    than `_PIPELINE_STALE_AFTER_SECONDS` as errored. Runs once on
    startup so a backend restart that killed mid-run daemon threads
    doesn't leave the /schedule UI showing a perpetually-running job.

    The pipeline workers run as `daemon=True` threads
    (`_spawn_ingest` in `routers/ingest_runs.py`), which means a
    process restart — common during dev with uvicorn --reload, but
    also possible in prod on a Railway deploy that lands while a job
    is in flight — kills them mid-execution. The `ingest_run` row
    keeps the last checkpoint state forever unless something cleans
    it up. The hour-old cutoff is conservative: even the full weekly
    pipeline (acquisition + templates + prune + prices + momentum)
    completes inside an hour, so anything older that's still
    `running` is provably orphaned.

    Best-effort: failures are logged + swallowed so a Supabase blip
    on boot never blocks scheduler startup."""
    from deps import supabase  # noqa: PLC0415

    cutoff_iso = (
        datetime.now(timezone.utc) - timedelta(seconds=_PIPELINE_STALE_AFTER_SECONDS)
    ).isoformat()
    try:
        # 1. Find them so we can log the IDs explicitly. Useful when
        #    triaging a recurring-restart situation — without the log
        #    line you'd never know which run(s) got reaped.
        resp = (
            supabase.table("ingest_run")
            .select("run_id, job_name, started_at, current_phase, current_message")
            .eq("status", "running")
            .lt("started_at", cutoff_iso)
            .order("started_at", desc=False)
            .execute()
        )
        orphans = resp.data or []
        if not orphans:
            return
        _log.warning(
            "[scheduler] reaping %s orphan ingest_run row(s) "
            "(status=running, older than %ss): %s",
            len(orphans), _PIPELINE_STALE_AFTER_SECONDS,
            [
                {
                    "run_id": o["run_id"],
                    "job_name": o.get("job_name"),
                    "current_phase": o.get("current_phase"),
                    "started_at": o.get("started_at"),
                }
                for o in orphans
            ],
        )
        # 2. Mark each as errored. One row at a time so a partial
        #    failure stops on the offending row rather than wiping
        #    all of them with a confusing PostgREST error.
        now_iso = datetime.now(timezone.utc).isoformat()
        for o in orphans:
            try:
                supabase.table("ingest_run").update({
                    "status": "error",
                    "current_phase": "done",
                    "finished_at": now_iso,
                    "error_summary": (
                        f"Orphaned (backend restart while running) — auto-reaped "
                        f"on next startup. Was stuck in phase "
                        f"{o.get('current_phase') or '?'} with message: "
                        f"{(o.get('current_message') or '')[:200]}"
                    ),
                }).eq("run_id", o["run_id"]).execute()
            except Exception as e:
                _log.warning(
                    "[scheduler] failed to reap run_id=%s: %s: %s",
                    o["run_id"], type(e).__name__, e,
                )
    except Exception as e:
        _log.warning(
            "[scheduler] orphan-run probe failed: %s: %s — skipping reap",
            type(e).__name__, e,
        )


def _unrefreshed_templates() -> list[str]:
    """Return `template_key`s for every registered template that's never
    been refreshed in THIS env (universe row missing, or last_refreshed_at
    IS NULL on the existing row). Result drives the bootstrap decision."""
    from deps import supabase  # noqa: PLC0415
    from index_universe.templates import all_templates  # noqa: PLC0415
    unrefreshed: list[str] = []
    for template in all_templates():
        try:
            uid = template.universe_id(supabase)
            if uid is None:
                unrefreshed.append(template.template_key)
                continue
            if template.last_refreshed_at(supabase) is None:
                unrefreshed.append(template.template_key)
        except Exception as e:
            _log.warning(
                "[scheduler] bootstrap check failed for %s: %s: %s",
                template.template_key, type(e).__name__, e,
            )
    return unrefreshed


def _pipeline_already_running() -> bool:
    """True if an `ingest_run` row in `running` state was started in the
    last hour. The bootstrap probe checks this so an in-flight manual run
    doesn't get a second pipeline piled on top of it."""
    from deps import supabase  # noqa: PLC0415
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=_PIPELINE_STALE_AFTER_SECONDS)).isoformat()
    try:
        resp = (
            supabase.table("ingest_run")
            .select("run_id")
            .eq("status", "running")
            .gte("started_at", cutoff)
            .limit(1)
            .execute()
        )
        return bool(resp.data)
    except Exception as e:
        _log.warning(
            "[scheduler] running-pipeline probe failed (%s: %s) — skipping bootstrap to be safe",
            type(e).__name__, e,
        )
        return True  # fail-safe: if we can't query, don't double-fire


def _maybe_bootstrap_templates(sched: BackgroundScheduler) -> None:
    """Schedule a one-shot full pipeline via DateTrigger when a template
    needs an initial refresh in this env. Idempotent: same job_id +
    `replace_existing=True` means re-calling on a hot restart doesn't
    double-book."""
    try:
        unrefreshed = _unrefreshed_templates()
    except Exception as e:
        _log.warning(
            "[scheduler] bootstrap-templates probe failed: %s: %s",
            type(e).__name__, e,
        )
        return
    if not unrefreshed:
        _log.info("[scheduler] bootstrap-templates: all templates refreshed — no-op")
        return
    if _pipeline_already_running():
        _log.info(
            "[scheduler] bootstrap-templates: %s unrefreshed (%s) but a pipeline is already running — skipping",
            len(unrefreshed), unrefreshed,
        )
        return
    run_at = datetime.now(timezone.utc) + timedelta(seconds=_BOOTSTRAP_DELAY_SECONDS)
    _log.warning(
        "[scheduler] bootstrap-templates: %s unrefreshed (%s) — firing full pipeline at %s",
        len(unrefreshed), unrefreshed, run_at.isoformat(),
    )
    sched.add_job(
        _fire_job,
        DateTrigger(run_date=run_at),
        args=["bootstrap_template_refresh"],
        id="bootstrap_template_refresh",
        replace_existing=True,
        coalesce=True,
        misfire_grace_time=600,
    )


def _fire_job(job_name: str) -> None:
    """Wrapper passed to APScheduler — guards against an uncaught exception
    inside the dispatcher killing the scheduler thread."""
    try:
        run_id = kick_off_refresh(job_name, "auto")
        _log.info("[scheduler] fired %s → run_id=%s", job_name, run_id)
    except Exception as e:
        _log.exception(
            "[scheduler] failed to fire %s: %s: %s",
            job_name, type(e).__name__, e,
        )


def register_scheduler(app) -> None:
    """Attach the scheduler to the FastAPI lifecycle. Called once from
    `main.py` after the FastAPI() instance is created."""

    @app.on_event("startup")
    def _start_scheduler() -> None:
        global _scheduler
        if _scheduler is not None:
            return  # already running (multiple startup events on reload)

        # Allow operators to disable the in-process scheduler via env var —
        # useful when running multiple replicas, during a manual ingest test,
        # or in CI where we don't want background jobs touching real data.
        if os.environ.get("DISABLE_SCHEDULER", "").lower() in ("1", "true", "yes"):
            _log.info("[scheduler] DISABLE_SCHEDULER set — in-process jobs not started")
            return

        sched = BackgroundScheduler(timezone="UTC")
        # Weekly: Tuesday 02:00 UTC. Captures the previous Monday's worldwide
        # closes ~5h after the US 21:00 UTC close.
        sched.add_job(
            _fire_job,
            CronTrigger(day_of_week="tue", hour=2, minute=0, timezone="UTC"),
            args=["weekly_price_volume"],
            id="weekly_price_volume",
            replace_existing=True,
            # If a startup happens to coincide with the tick (e.g. a deploy
            # right at 02:00 UTC), `coalesce=True` collapses any backlog
            # into a single run and `misfire_grace_time` gives us 10 min
            # of slack before we declare it skipped.
            coalesce=True,
            misfire_grace_time=600,
        )
        # Daily MTD refresh: Wed-Sat 02:00 UTC. Refreshes prices for the
        # pooled set of held companies (~30-60 vs ~2000 in the weekly
        # tick) and re-persists MTD on each strategy's latest snapshot.
        # Skips Tue (weekly tick covers it) and Sun/Mon (weekend; no
        # fresh closes to capture). Each fire takes minutes, not hours.
        sched.add_job(
            _fire_job,
            CronTrigger(
                day_of_week="wed,thu,fri,sat",
                hour=2, minute=0, timezone="UTC",
            ),
            args=["daily_holdings_refresh"],
            id="daily_holdings_refresh",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=600,
        )
        sched.start()
        _scheduler = sched
        # Reap any orphan `ingest_run` rows left in `status='running'`
        # from a previous process that died mid-job (uvicorn --reload,
        # Railway redeploy, OOM kill, …). Runs BEFORE the bootstrap
        # probe so a stale orphan doesn't fool `_pipeline_already_running`
        # into skipping a bootstrap that should fire.
        try:
            _reap_orphan_runs()
        except Exception as e:
            _log.warning(
                "[scheduler] reap-orphan-runs wrapper failed: %s: %s",
                type(e).__name__, e,
            )
        # Probe for templates that have never been refreshed in this env
        # and schedule a one-shot full pipeline if so. Wrapped so a probe
        # failure can never take down the scheduler startup.
        try:
            _maybe_bootstrap_templates(sched)
        except Exception as e:
            _log.warning(
                "[scheduler] bootstrap-templates wrapper failed: %s: %s",
                type(e).__name__, e,
            )
        next_runs = {j.id: str(j.next_run_time) for j in sched.get_jobs()}
        _log.info("[scheduler] started; next runs: %s", next_runs)

    @app.on_event("shutdown")
    def _stop_scheduler() -> None:
        global _scheduler
        if _scheduler is None:
            return
        try:
            # wait=False so a long-running ingest doesn't block the FastAPI
            # process from terminating — the work is in a daemon thread
            # which dies with the process anyway.
            _scheduler.shutdown(wait=False)
            _log.info("[scheduler] shut down")
        except Exception as e:
            _log.warning("[scheduler] shutdown failed: %s: %s", type(e).__name__, e)
        finally:
            _scheduler = None
