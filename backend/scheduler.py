"""In-process APScheduler for the scheduled price/volume ingest jobs.

Replaces the previous Railway-Cron approach for this codebase. Two
`BackgroundScheduler` cron triggers run inside the FastAPI process:

    weekly_price_volume   Tue 02:00 UTC   (cron: `day_of_week=tue, hour=2`)
    monthly_price_volume  2nd at 02:00 UTC (cron: `day=2, hour=2`)

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

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from routers.ingest_runs import kick_off_refresh

_log = logging.getLogger(__name__)
_scheduler: BackgroundScheduler | None = None


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
        # Monthly: 2nd of every month at 02:00 UTC. Captures the first
        # trading day's closes — if the 1st was a weekend/holiday the run
        # still fires but the freshness check no-ops most companies and
        # the next weekly tick catches up the actual first-trading-day close.
        sched.add_job(
            _fire_job,
            CronTrigger(day=2, hour=2, minute=0, timezone="UTC"),
            args=["monthly_price_volume"],
            id="monthly_price_volume",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=600,
        )
        sched.start()
        _scheduler = sched
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
