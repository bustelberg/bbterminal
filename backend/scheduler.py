"""In-process APScheduler for the single daily smart pipeline tick.

One `BackgroundScheduler` cron trigger runs inside the FastAPI process:

    smart_daily   Daily 02:00 UTC — dependency-driven pipeline

Each tick derives, from the enabled scheduled strategies, exactly what's
needed and runs only that (`ingest.phases.pipeline._run_smart_pipeline_sync`):
refresh only the universes those strategies use, keep every strategy's held
companies priced daily, and rebalance each strategy on the first occurrence
of its baked `rebalance_weekday` in its period. A Monday 02:00 UTC tick
already has Friday's settled close (US close Fri 21:00 UTC + ~5h), so a
first-Monday rebalance decides on Friday's close. Weekend ticks (and any day
with no strategy due + fresh held prices) are cheap no-ops via the
per-company freshness short-circuit.

The previous fixed-calendar jobs (weekly full pipeline, daily MTD refresh,
daily/monthly template refresh) are retired — the single smart tick subsumes
them, scoped to what the strategies actually need.

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
from datetime import date, datetime, timedelta, timezone

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


def _needed_templates_behind(needed_keys: set[str]) -> bool:
    """True when any NEEDED template (one an enabled strategy uses) has
    never been refreshed in this env or has fallen behind the current
    month. Unused templates are intentionally ignored — under the
    dependency-driven model they go stale until a strategy needs them."""
    if not needed_keys:
        return False
    try:
        behind = set(_unrefreshed_templates()) | set(_stale_templates())
    except Exception as e:
        _log.warning("[scheduler] needed-template-behind probe failed: %s: %s", type(e).__name__, e)
        return False
    return bool(behind & needed_keys)


def _maybe_kickstart_smart(sched: BackgroundScheduler) -> None:
    """On startup, schedule a one-shot `smart_daily` when anything the
    enabled strategies need is behind — so an env that was down across the
    02:00 UTC tick catches up immediately rather than waiting a day. The
    smart tick is itself dependency-scoped + idempotent, so firing it is
    always safe. No enabled strategies → no-op (nothing auto-runs, by
    design). Idempotent via the fixed job id + `replace_existing=True`."""
    from ingest.phases.planner import build_plan  # noqa: PLC0415 — avoid import cycle

    try:
        plan = build_plan(datetime.now(timezone.utc))
    except Exception as e:
        _log.warning("[scheduler] smart kickstart: plan build failed: %s: %s", type(e).__name__, e)
        return
    if not plan.strategies:
        _log.info("[scheduler] smart kickstart: no enabled strategies — no-op")
        return

    needed = set(plan.needed_template_keys)
    reasons: list[str] = []
    if plan.due_strategy_ids:
        reasons.append(f"{len(plan.due_strategy_ids)} strategy(ies) due")
    if _needed_templates_behind(needed):
        reasons.append("needed template unrefreshed/behind")
    try:
        if _held_prices_stale():
            reasons.append("held prices stale")
    except Exception as e:
        _log.warning("[scheduler] smart kickstart: price-staleness probe failed: %s: %s", type(e).__name__, e)

    if not reasons:
        _log.info("[scheduler] smart kickstart: everything current — no-op")
        return
    if _pipeline_already_running():
        _log.info("[scheduler] smart kickstart: needed (%s) but a pipeline is already running — skipping", reasons)
        return
    run_at = datetime.now(timezone.utc) + timedelta(seconds=_BOOTSTRAP_DELAY_SECONDS)
    _log.warning(
        "[scheduler] smart kickstart: firing smart_daily at %s — reasons: %s",
        run_at.isoformat(), reasons,
    )
    sched.add_job(
        _fire_job,
        DateTrigger(run_date=run_at),
        args=["smart_daily"],
        id="startup_smart_kickstart",
        replace_existing=True,
        coalesce=True,
        misfire_grace_time=600,
    )


def _stale_templates() -> list[str]:
    """`template_key`s that HAVE data but whose latest captured month is
    behind the current calendar month — i.e. they missed the month rollover
    and need a catch-up refresh. (Never-refreshed templates are excluded
    here; `_unrefreshed_templates` / the bootstrap handle those.)"""
    from deps import supabase  # noqa: PLC0415
    from index_universe.templates import all_templates  # noqa: PLC0415
    current_month = datetime.now(timezone.utc).strftime("%Y-%m")
    stale: list[str] = []
    for template in all_templates():
        try:
            uid = template.universe_id(supabase)
            if uid is None:
                continue  # never refreshed → bootstrap's job, not ours
            months = template.available_months(supabase)
            latest = months[-1] if months else None
            if latest is None or latest < current_month:
                stale.append(template.template_key)
        except Exception as e:
            _log.warning(
                "[scheduler] staleness check failed for %s: %s: %s",
                template.template_key, type(e).__name__, e,
            )
    return stale


def _trading_day_age(latest: "date | None") -> "int | None":
    """Age of `latest` in trading days (Mon-Fri only). 0 when `latest` is
    the most recent trading day (e.g. a Monday call where latest is the
    prior Friday). None when `latest` is missing. Mirrors the admin
    `data-freshness` helper."""
    if latest is None:
        return None
    today = datetime.now(timezone.utc).date()
    if latest >= today:
        return 0
    days = 0
    cursor = today
    while cursor > latest:
        cursor = cursor - timedelta(days=1)
        if cursor.weekday() < 5:  # 0..4 = Mon..Fri
            days += 1
    return days


def _held_prices_stale() -> bool:
    """True when at least one ENABLED scheduled strategy's latest snapshot
    has a price date a trading day or more behind today — i.e. the daily
    MTD refresh missed and the held positions are stale. This is exactly
    the "price data as of <date>" the /schedule UI surfaces. Returns False
    when there are no enabled strategies or no snapshots yet (nothing to
    catch up; the normal pipeline / bootstrap handles first population)."""
    from deps import supabase  # noqa: PLC0415
    strat = (
        supabase.table("scheduled_strategy")
        .select("id")
        .eq("enabled", True)
        .execute()
    )
    ids = [r["id"] for r in (strat.data or [])]
    if not ids:
        return False
    snap = (
        supabase.table("current_picks_snapshot")
        .select("latest_price_date")
        .in_("scheduled_strategy_id", ids)
        .not_.is_("latest_price_date", "null")
        .order("latest_price_date", desc=True)
        .limit(1)
        .execute()
    )
    rows = snap.data or []
    if not rows:
        return False
    raw = rows[0].get("latest_price_date")
    try:
        latest = date.fromisoformat(str(raw)[:10]) if raw else None
    except ValueError:
        return False
    age = _trading_day_age(latest)
    return age is not None and age >= 1


def list_scheduled_jobs() -> list[dict]:
    """Snapshot of every job currently registered on the in-process
    scheduler, for the /schedule "Pipeline activity" strip. Returns one
    dict per job with its id, the underlying job_name it fires
    (`args[0]` — for the one-shot catch-up job this differs from the
    job id: `startup_smart_kickstart` fires `smart_daily`),
    and its next fire time as an ISO string (None if the scheduler paused
    it). Empty list when the scheduler isn't running (DISABLE_SCHEDULER,
    or before startup). Best-effort: never raises."""
    if _scheduler is None:
        return []
    jobs: list[dict] = []
    try:
        for j in _scheduler.get_jobs():
            try:
                fires = j.args[0] if getattr(j, "args", None) else j.id
            except Exception:
                fires = j.id
            nrt = getattr(j, "next_run_time", None)
            jobs.append({
                "id": j.id,
                "fires": fires,
                "next_run_at": nrt.isoformat() if nrt is not None else None,
            })
    except Exception as e:
        _log.warning("[scheduler] list_scheduled_jobs failed: %s: %s", type(e).__name__, e)
        return []
    return jobs


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
        # Single daily "smart" tick: every day 02:00 UTC. It derives, from
        # the enabled scheduled strategies, exactly what's needed and runs
        # only that (see `ingest.phases.pipeline._run_smart_pipeline_sync`):
        # refresh only the universes those strategies use, keep every
        # strategy's held companies priced daily, and rebalance each
        # strategy on the first occurrence of its baked `rebalance_weekday`
        # in its period. Monday 02:00 UTC already has Friday's settled close
        # (US close Fri 21:00 UTC + ~5h), so a first-Monday rebalance decides
        # on Friday's close. Weekend ticks are cheap no-ops via the
        # per-company freshness short-circuit.
        sched.add_job(
            _fire_job,
            CronTrigger(day_of_week="mon-sun", hour=2, minute=0, timezone="UTC"),
            args=["smart_daily"],
            id="smart_daily",
            replace_existing=True,
            # If a startup coincides with the tick (e.g. a deploy right at
            # 02:00 UTC), `coalesce=True` collapses any backlog into a single
            # run and `misfire_grace_time` gives 10 min of slack.
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
        # On startup, fire a one-shot `smart_daily` if anything the enabled
        # strategies need is behind (held prices stale, a strategy never
        # computed, or a NEEDED template unrefreshed/behind the month) — so
        # an env that was down across the 02:00 UTC tick catches up
        # immediately instead of waiting for tomorrow. The smart tick is
        # itself scoped + idempotent, so this is always safe. Wrapped so a
        # probe failure can never take down scheduler startup.
        try:
            _maybe_kickstart_smart(sched)
        except Exception as e:
            _log.warning(
                "[scheduler] smart-kickstart wrapper failed: %s: %s",
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
