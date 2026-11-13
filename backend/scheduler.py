"""APScheduler registration, event listeners, health inspection, and dispatch.

Execution policies live in scheduler_runtime; domain implementations live in
scheduler_jobs. scheduled_jobs.py remains the declarative schedule catalogue.
Manual Run now and timer dispatch use the same recorded execution path.
"""
from __future__ import annotations

import logging
import os
import threading
import time
import traceback
from datetime import date, datetime, timedelta, timezone

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MAX_INSTANCES, EVENT_JOB_MISSED
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from job_runlog import record_run
from routers.ingest_runs import kick_off_refresh
#  The schedule itself lives there, not here — see `_register`. Declaration only: no DB, no
# APScheduler, so it is safe for both this module and the admin router to import.
from scheduled_jobs import BY_ID, ORPHAN_MARKER, SCHEDULED_JOBS
from scheduler_runtime import admission, execution, retries, recovery, readiness
from scheduler_runtime.recovery import _WATCHDOG_MAX_PER_DAY as _WATCHDOG_MAX_PER_DAY
from scheduler_jobs import airs as airs_jobs, market as market_jobs, preparation as preparation_jobs
from scheduler_jobs.runtime import JobRuntime
from scheduler_jobs import price_checks as price_check_jobs
from scheduler_runtime.admission import _scheduled_worker_count as _scheduled_worker_count
from scheduler_runtime.execution import Cancelled as _Cancelled, cancel_requested as _cancel_requested
from scheduler_runtime.context import LogContext

_LogCtx = LogContext

_MAX_LOG_MESSAGE_CHARS = 2000
_LOG_TRUNCATION_SUFFIX = '… [truncated; full receipt is stored with the job]'


class _BoundedSchedulerLog(logging.Filter):
    """Bound the complete event, including exception text, before handlers emit it."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except (TypeError, ValueError):
            # A malformed logging call must not crash the scheduled job itself.
            message = f'{record.msg} [logging format error; args={record.args!r}]'
        if record.exc_info:
            message += '\n' + ''.join(traceback.format_exception(*record.exc_info))
        elif record.exc_text:
            message += '\n' + record.exc_text
        if record.stack_info:
            message += '\n' + record.stack_info
        if len(message) > _MAX_LOG_MESSAGE_CHARS:
            message = message[:_MAX_LOG_MESSAGE_CHARS - len(_LOG_TRUNCATION_SUFFIX)] + _LOG_TRUNCATION_SUFFIX
        record.msg, record.args = message, ()
        # The exception/stack is already included inside the same size budget.
        record.exc_info = record.exc_text = record.stack_info = None
        return True


_log = logging.getLogger(__name__)
_log.addFilter(_BoundedSchedulerLog())
_scheduler: BackgroundScheduler | None = None

# Worker capacity and active-run state are owned by scheduler_runtime.admission.
# The queue intentionally runs one Yahoo/OpenFIGI slice at a time. A slow slice therefore makes
# APScheduler print its own warning every 20 seconds; that is an expected backpressure mechanism,
# not a separate failure per skipped tick. Replace it with the rate-limited summary below.
_QUEUE_OVERRUN_LOG_EVERY_SECONDS = 300.0
_queue_overrun_lock = threading.Lock()
_queue_overruns = 0
_queue_overrun_last_log = 0.0


class _ExpectedQueueOverrunFilter(logging.Filter):
    """Suppress APScheduler's per-tick warning for the deliberately serial queue worker."""

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return not (
            "asset_ingest_queue" in message
            and "maximum number of running instances reached" in message
        )

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

# Held-price retry budget/delay are owned by scheduler_runtime.retries.

# ── PostgREST cold start ───────────────────────────────────────────
#  A startup probe can beat the database to readiness, and the failure looks like a fault.
# PostgREST answers nothing until it has loaded its schema cache; until then EVERY query — a
# one-row `limit(1)` included — comes back as
#
#     APIError: {'code': 'PGRST002', 'message': 'Could not query the database for the
#                schema cache. Retrying.'}
#
# which is PostgREST saying *it* is retrying, not that our query was wrong. The backend and the
# Supabase stack boot in parallel (locally `npx supabase start`, in prod a redeploy restarting
# both), so a catch-up that fires on startup races the schema cache and loses. Swallowed by the
# job's `except`, it logs a traceback and the boot's catch-up is gone until the next daily tick —
# and the startup catch-up exists precisely to repair the gap a daily tick CANNOT. So a warming
# database is waited out, not reported as a failure.


def _is_db_warming(exc: BaseException) -> bool:
    return readiness._is_db_warming(exc)


def _await_db_ready(what: str) -> bool:
    return readiness._await_db_ready(what)


def _reap_orphan_runs() -> None:
    """Mark EVERY `ingest_run` row still in `status='running'` as errored.
    Runs once on startup (before any new run is kicked off), so a backend
    restart that killed mid-run daemon threads doesn't leave the /schedule UI
    showing a perpetually-running job.

    The pipeline workers run as `daemon=True` threads (`_spawn_ingest` in
    `routers/ingest_runs.py`), so a process restart — common during dev with
    uvicorn --reload, but also possible in prod on a Railway deploy that lands
    mid-job — kills them mid-execution, leaving the `ingest_run` row frozen in
    `running` forever. On a FRESH process there are no live pipeline threads,
    so ANY `running` row is provably orphaned by the previous process — reap
    them ALL. (It used to only reap hour-old rows, which left a just-killed job
    showing a frozen "running…" for up to an hour after every restart.) The
    reaper runs before `_maybe_kickstart_smart` kicks off this process's first
    run, so it can never reap a run this process actually owns.

    Best-effort: failures are logged + swallowed so a Supabase blip on boot
    never blocks scheduler startup."""
    from deps import supabase  # noqa: PLC0415

    try:
        # 1. Find them so we can log the IDs explicitly. Useful when
        #    triaging a recurring-restart situation — without the log
        #    line you'd never know which run(s) got reaped.
        resp = (
            supabase.table("ingest_run")
            .select("run_id, job_name, started_at, current_phase, current_message")
            .eq("status", "running")
            .order("started_at", desc=False)
            .execute()
        )
        orphans = resp.data or []
        if not orphans:
            return
        _log.warning(
            "[scheduler] reaping %s orphan ingest_run row(s) (status=running "
            "on a fresh process → provably dead): %s",
            len(orphans),
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
                        #  The prefix is a shared constant — the automatic-jobs overview matches on
                        # it to report this as `interrupted` rather than as a job fault. Reword it
                        # here and the overview silently goes back to calling every dev `--reload`
                        # a failure.
                        f"{ORPHAN_MARKER} — auto-reaped "
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


def _maybe_kickstart_smart(sched: BackgroundScheduler) -> None:
    """On startup, schedule a one-shot daily-sequence run when there's catch-up
    work — so an env that was down across the 05:00 Amsterdam tick (or freshly
    deployed) converges immediately rather than waiting a day. The sequence
    (price-update → rebalance) is itself scoped + idempotent, so firing it is
    always safe. Idempotent via the fixed job id + `replace_existing=True`.

    Catch-up reasons (both strategy-driven — template-universe maintenance was
    dropped with the split pipeline; the rebalance op refreshes the due
    strategy's own universe on demand):
      * an enabled strategy is due to rebalance;
      * an enabled strategy's held prices are stale (missed the daily MTD)."""
    from ingest.phases.planner import build_plan  # noqa: PLC0415 — avoid import cycle

    reasons: list[str] = []
    #  "NO REASONS" AND "WE COULD NOT TELL" ARE NOT THE SAME ANSWER. Both probes below run on
    # startup, racing the Supabase stack (PGRST002 until PostgREST's schema cache loads), and a
    # failed probe leaves `reasons` empty — which used to log "everything current", a claim we
    # had no basis for, about the exact state the kickstart exists to detect.
    probe_failed = False

    try:
        plan = build_plan(datetime.now(timezone.utc))
    except Exception as e:
        probe_failed = True
        _log.warning("[scheduler] kickstart: plan build failed: %s: %s", type(e).__name__, e)
        plan = None
    if plan is not None and plan.strategies:
        if plan.due_strategy_ids:
            reasons.append(f"{len(plan.due_strategy_ids)} strategy(ies) due")
        try:
            if _held_prices_stale():
                reasons.append("held prices stale")
        except Exception as e:
            probe_failed = True
            _log.warning("[scheduler] kickstart: price-staleness probe failed: %s: %s", type(e).__name__, e)

    if not reasons:
        if probe_failed:
            _log.warning(
                "[scheduler] kickstart: could NOT determine whether catch-up is needed — a probe "
                "failed (database still booting?); not firing. The 05:00 Amsterdam tick is the fallback.",
            )
        else:
            _log.info("[scheduler] kickstart: everything current — no-op")
        return
    if _pipeline_already_running():
        _log.info("[scheduler] kickstart: needed (%s) but a pipeline is already running — skipping", reasons)
        return
    run_at = datetime.now(timezone.utc) + timedelta(seconds=_BOOTSTRAP_DELAY_SECONDS)
    _log.warning(
        "[scheduler] kickstart: firing daily sequence at %s — reasons: %s",
        run_at.isoformat(), reasons,
    )
    sched.add_job(
        _fire_daily_sequence,
        DateTrigger(run_date=run_at),
        id="startup_smart_kickstart",
        replace_existing=True,
        coalesce=True,
        misfire_grace_time=600,
    )


def _fire_daily_price_slice() -> None:
    """Refresh the most-stale slice of the company book. Fires every day, unconditionally.

     IT REPLACED A GATED MONTH-END TICK, AND THE GATE IS THE PART THAT IS GONE. The old
    `_fire_month_end_refresh` woke daily and did nothing on ~28 days out of 30, then fired one full
    pass inside a window — with a "did it already run this window" guard, a retry-tomorrow path and
    a fallback that preferred firing when the guard lookup failed. All of that machinery existed to
    make ONE run a month land reliably, which is a hard thing to do; doing a thirtieth of the work
    every day is not, and needs none of it. No window, no guard, no retry logic: if a day is
    missed, the next day's slice is simply more stale and picks up the same names, because
    `_load_all_companies()` is most-stale-first and the database's own staleness is the cursor.

     Never raises into the scheduler thread.
    """
    try:
        _fire_job("price_slice")
    except Exception as e:  # noqa: BLE001
        _log.warning("[scheduler] daily price slice tick failed: %s: %s", type(e).__name__, e)


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


def _body_asset_price_refresh(ctx=None) -> tuple[str, dict]:
    return market_jobs._body_asset_price_refresh(ctx, runtime=_job_runtime())


#: EVERY DECLARED JOB'S BODY, callable twice: by its scheduler tick and by the Run-now button.
#:
#:  ONE BODY, TWO CALLERS — the rule this codebase applies to every scraper and pipeline. A button
#: that ran a second implementation would drift from the thing the schedule actually runs, and the
#: drift would only ever be discovered by the button disagreeing with the nightly result.
#:
#:  A JOB WITH NO ENTRY IS NOT RUNNABLE BY HAND, and that is an answer rather than a gap: the
#: 20-second queue worker has nothing to trigger (it fires three times a minute), and the two
#: pipeline jobs already have their own richer Run-now with a live console tail in their panels.
JOB_BODIES: dict = {}


def _job_runtime() -> JobRuntime:
    """Bind dispatch callbacks at call time, including manual overrides in tests."""
    return JobRuntime(
        reporter=_reporter, run_job=_run_body,
        relative_momentum=_body_relative_momentum_refresh,
        model_prices=_body_airs_model_prices, asset_refresh=_run_asset_price_refresh,
        await_db_ready=_await_db_ready, sources_busy=_analyse_sources_busy,
        wait_for_yahoo_queue=_wait_for_yahoo_queue,
        fundamental_ids=_fundamental_company_ids, record_run=record_run,
        active_jobs=admission.active_jobs,
    )


def _reporter(ctx):
    """Use the shared reporter for durable receipts and the live job stream."""
    from services.job_progress import reporter

    return reporter(ctx)


def _run_body(job_id: str, ctx=None, triggered_by: str = "auto") -> str:
    return execution.run_job(job_id, ctx, triggered_by, bodies=JOB_BODIES, record=record_run)


def _run_recorded_body(job_id: str, ctx=None, triggered_by: str = "auto") -> str:
    return execution.run_recorded(job_id, ctx, triggered_by, bodies=JOB_BODIES, record=record_run)


def _submit_scheduled_work(name: str, work) -> bool:
    return admission.submit(name, work)


def _spawn_body(job_id: str) -> None:
    """The scheduler tick: run a declared body on bounded background capacity."""
    def _go() -> None:
        try:
            _run_body(job_id)
        except Exception as e:  # noqa: BLE001
            _log.exception("[scheduler] %s failed: %s: %s", job_id, type(e).__name__, e)
    if not _submit_scheduled_work(job_id, _go):
        already_running = job_id in admission.active_jobs()
        if not already_running:
            from job_runlog import record_missed
            record_missed(job_id, datetime.now(timezone.utc), "Scheduled worker capacity unavailable; tick did not start")


def start_job_now(job_id: str, *, triggered_by: str = "manual"):
    """Kick a declared job off by hand, as a CANCELLABLE registry job with a progress toast.

     THE SAME BODY THE TICK RUNS, through `_run_body` — so "Run now" cannot come to mean something
    different from what the schedule does.

     `triggered_by` DEFAULTS TO `manual` BECAUSE THAT IS WHO CALLS IT — a button. The watchdog
    passes `watchdog` so its own re-runs are countable in the history, which is where its per-day
    cap now reads its budget from (see `_watchdog_budget_spent`); folded into `manual` a hand-run
    would silently spend the automatic allowance and vice versa.

     CANCELLATION IS COOPERATIVE AND ITS LATENCY DIFFERS PER JOB, which the UI states rather than
    hides: the AIRS scan stops between accounts (seconds), the drift probe between companies, and
    the FX and size jobs are short enough to have no useful boundary at all. "Immediately" is
    not on offer for a scraper mid-download, and claiming it would be the decorative Cancel this
    codebase has already removed once.
    """
    import jobs as _jobs  # noqa: PLC0415

    if job_id not in JOB_BODIES:
        raise KeyError(job_id)
    label = BY_ID[job_id].label if job_id in BY_ID else job_id

    def _work(ctx) -> str:
        return _run_body(job_id, ctx, triggered_by=triggered_by)

    job, _reused = _jobs.start(f"scheduled.{job_id}", label, _work)
    return job


def job_health(now=None) -> dict:
    """The three-way join the automatic-jobs page renders: declared vs registered vs actually ran.

     IT LIVES HERE SO THE WATCHDOG AND THE PAGE CANNOT DISAGREE. This assembly used to be a
    closure inside the admin endpoint. A self-healing tick needs the same verdict, and a second
    copy of "is this job overdue" is the one thing that must not exist: the page would say `ok`
    while the watchdog re-fired, or the reverse, and the surface built to tell you what is wrong
    would be wrong about itself.

     THE PURE PART STAYS PURE. `_scheduled_jobs_status` takes every input and reads no clock and
    no database; this is the impure shell that goes and gets them. `now` is injectable for the
    same reason.

    Returns `{rows, running, now, history_error}` — `history_error` set when the run history could
    not be read, in which case `rows` still carries the registered-vs-declared half, which needs no
    database at all.
    """
    import os  # noqa: PLC0415
    from datetime import datetime as _dt, timezone as _tz  # noqa: PLC0415

    from postgrest.exceptions import APIError  # noqa: PLC0415

    from deps import supabase  # noqa: PLC0415
    from routers._scheduled_jobs_status import build_rows, evidence_names  # noqa: PLC0415
    from scheduled_jobs import registrable  # noqa: PLC0415

    now = now or _dt.now(_tz.utc)
    specs = registrable(dict(os.environ))
    registered = list_scheduled_jobs()
    running = scheduler_running()
    #  Read from the body registry, not assumed per row. A "Run now" rendered for a job with no
    # body is a control that 404s on press — and a watchdog that fires one raises `KeyError`.
    runnable = set(JOB_BODIES)

    def _runs() -> list[dict]:
        #  The newest row **PER JOB**, NEVER A WINDOW FILTERED CLIENT-SIDE. A windowed
        # `.limit(500)` is filled by the noisy jobs and pushes the quiet ones off the end, so it
        # accuses exactly the jobs that are behaving.
        out: list[dict] = []
        for name in evidence_names(specs):
            legacy = (supabase.table("ingest_run")
                    .select("job_name,started_at,finished_at,status,error_summary,current_message,"
                            "companies_total,companies_processed,prices_refreshed,error_count")
                    .eq("job_name", name)
                    .order("started_at", desc=True)
                    .limit(1).execute().data or [])
            for row in legacy:
                row["detail"] = row.get("error_summary") or row.get("current_message")
                row["summary"] = {key: row[key] for key in (
                    "companies_total", "companies_processed", "prices_refreshed", "error_count"
                ) if row.get(key) is not None}
            out.extend(legacy)
        #  Normalised onto `job_name` SO THE JOIN HAS ONE KEY SPACE. `scheduled_job_run` is keyed
        # by the APScheduler job id and `ingest_run` by a pipeline job_name.
        for spec in specs:
            if not spec.records:
                continue
            for row in (supabase.table("scheduled_job_run")
                        .select("job_id,started_at,finished_at,status,detail,summary")
                        .eq("job_id", spec.id)
                        .order("started_at", desc=True)
                        .limit(1).execute().data or []):
                out.append({**row, "job_name": row.pop("job_id"), "scheduled_record": True})
        return out

    err = None
    try:
        runs = _runs()
    except APIError as e:
        runs, err = [], f"{type(e).__name__}: {e}"
    rows = build_rows(specs, registered, runs, now, scheduler_running=running, runnable=runnable,
                      scheduler_booted_at=_booted_at)
    # The queue worker deliberately avoids one durable row per 20-second tick. Its health therefore
    # comes from the queue itself: pending work plus the timestamp of the last resolved item.
    for row in rows:
        if row["id"] != "asset_ingest_queue" or not running or not row["registered"]:
            continue
        try:
            from asset_pipeline import queue as asset_queue  # noqa: PLC0415

            counts = asset_queue.status()
            activity = asset_queue.last_activity()
            pending = int(counts.get("pending") or 0)
            row["observable"] = True
            row["last_detail"] = (
                f"{pending} pending · {int(counts.get('done') or 0)} done · "
                f"{int(counts.get('failed') or 0)} failed"
                + (f" · last activity {activity}" if activity else " · no activity recorded"))
            if pending == 0:
                row["status"] = "ok"
                row["reason"] = "The queue is empty."
            elif asset_queue.is_worker_active():
                row["status"] = "running"
                row["reason"] = "The queue is draining."
            else:
                row["status"] = "error"
                row["reason"] = "Items are pending, but the queue has not moved an item in 10 minutes."
        except Exception as e:  # noqa: BLE001 — job health must not fail because the queue is unreadable
            row["reason"] = f"Could not read queue health: {type(e).__name__}: {e}"
    return {"rows": rows, "running": running, "now": now, "history_error": err}


# Recovery eligibility and retry budgets live in scheduler_runtime.recovery.
def _shutdown_interrupted(row: dict) -> bool:
    return recovery._shutdown_interrupted(row)

#: HOW THE WATCHDOG FIRES A JOB THAT HAS NO `JOB_BODIES` ENTRY.
#:
#:  `JOB_BODIES` MEMBERSHIP WAS DOING TWO JOBS AND THEY HAD QUIETLY DIVERGED. It decides whether
#: the overview renders a generic "Run now" — deliberately NOT for these two, which own a richer
#: button with a live console tail inside their expanded row (`jobPanels.JOB_PANELS`) — and it also
#: decided what the watchdog could re-run. So the two jobs the watchdog was BUILT for (measured
#: 2026-08-18 at 7.1 days for `daily_pipeline`, 18 for the month-end refresh; still 21.0 and 31.8
#: days on 2026-09-01) were the two it skipped, reporting them as `unrunnable` and moving on. The
#: presentation question and the capability question are now separate.
#:
#:  THE VALUE IS THE TICK CALLABLE ITSELF, so an automatic re-run is byte-for-byte what the
#: schedule does. A second path into the pipeline is the one thing that must not exist here.
#:
#:  NO TOAST AND NO CANCEL, unlike `start_job_now` — these fire their own daemon threads and
#: narrate into `ingest_run`, which is where the /schedule panels already watch them. Wrapping them
#: in a registry job would put a second progress surface on a run that already has one.
#:
#:  `asset_ingest_queue` IS ABSENT AND STAYS ABSENT. A 20-second interval worker cannot be
#: "overdue" in any sense worth healing, and it is excluded from the gap scan for the same reason.
#:  FILLED BY `_register_bodies` AT THE BOTTOM OF THE MODULE, exactly as `JOB_BODIES` is and for
#: exactly the same reason: the tick callables are defined throughout this file beside the schedules
#: they belong to, so naming them here would be forward references to functions that do not exist
#: yet. A string-and-`globals()` lookup would dodge that and turn a typo into a runtime KeyError
#: inside the watchdog — the one place a mistake is least likely to be noticed.
_WATCHDOG_STARTERS: dict[str, object] = {}


def _watchdog_budget_spent(job_id: str, today: str) -> bool:
    return recovery._watchdog_budget_spent(job_id, today, starters=_WATCHDOG_STARTERS)


def _fire_job_watchdog() -> None:
    """The watchdog tick — see `_body_job_watchdog`."""
    _spawn_body("job_watchdog")


def _body_job_watchdog(ctx=None) -> tuple[str, dict]:
    return recovery._body_job_watchdog(
        ctx, health=job_health, bodies=JOB_BODIES, starters=_WATCHDOG_STARTERS,
        start_job=start_job_now, reporter=_reporter)


def scheduler_running() -> bool:
    """Whether THIS process has a live in-process scheduler.

     IT IS NOT `bool(list_scheduled_jobs())`, AND THAT IS THE WHOLE REASON IT EXISTS. An empty job
    list has two opposite meanings: on a replica with `DISABLE_SCHEDULER=1` it is correct and
    expected, and on the one instance that is supposed to be running everything it means every
    registration failed. The list cannot tell them apart — a monitor that infers one from the other
    reports the fire as normal.
    """
    return _scheduler is not None


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
    by_id = {row["id"]: row for row in jobs}
    pending = [spec for spec in BY_ID.values() if spec.after_job]
    while pending:
        ready = [spec for spec in pending if spec.after_job in by_id]
        if not ready:
            break
        for spec in ready:
            row = {"id": spec.id, "fires": spec.id,
                   "next_run_at": by_id[spec.after_job]["next_run_at"],
                   "after_job": spec.after_job}
            jobs.append(row)
            by_id[spec.id] = row
            pending.remove(spec)
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


def _fire_daily_sequence() -> None:
    """Run the split pipeline's two operations IN ORDER — price-update, then
    rebalance (a no-op unless a strategy is due) — each as its own
    `ingest_run` row. Both run in a single daemon thread so they execute
    sequentially rather than racing; the global pipeline lock additionally
    guards against any concurrent manual Run-now. Passed to APScheduler (daily
    tick + startup catch-up); never raises into the scheduler thread."""
    def _seq() -> None:
        from ingest.phases import (  # local import to avoid an import cycle
            _create_run,
            _run_price_update_pipeline_sync,
            _run_rebalance_pipeline_sync,
        )
        for job_name, fn in (
            ("price_update", _run_price_update_pipeline_sync),
            ("rebalance", _run_rebalance_pipeline_sync),
        ):
            try:
                run_id = _create_run(job_name, "auto")
                _log.info("[scheduler] daily sequence: %s → run_id=%s", job_name, run_id)
                fn(run_id)
            except Exception as e:
                _log.exception(
                    "[scheduler] daily sequence: %s failed: %s: %s",
                    job_name, type(e).__name__, e,
                )

    _submit_scheduled_work("daily_pipeline", _seq)


def _fire_sunday_rebalance_preflight() -> None:
    """Refresh and rebalance first-Monday strategies at Sunday 10:00 Amsterdam."""
    def _run() -> None:
        from ingest.phases import _create_run, _run_rebalance_pipeline_sync  # noqa: PLC0415

        try:
            run_id = _create_run("rebalance", "auto")
            _log.info("[scheduler] Sunday rebalance preflight → run_id=%s", run_id)
            _run_rebalance_pipeline_sync(run_id, sunday_preflight=True)
        except Exception as e:
            _log.exception(
                "[scheduler] Sunday rebalance preflight failed: %s: %s", type(e).__name__, e,
            )

    _submit_scheduled_work("sunday_rebalance_preflight", _run)


def _fire_price_update_retry() -> None:
    """One-shot stale-held-price retry fired by APScheduler. Runs a fresh
    `price_update` op in its own daemon thread; that op's completion hook
    (`maybe_schedule_price_retry`) chains the NEXT retry if it's STILL stale
    and the day's budget allows. Never raises into the scheduler thread."""
    def _run() -> None:
        from ingest.phases import (  # noqa: PLC0415 — avoid import cycle
            _create_run,
            _run_price_update_pipeline_sync,
        )
        try:
            run_id = _create_run("price_update", "auto")
            _log.warning("[scheduler] stale-price retry → price_update run_id=%s", run_id)
            _run_price_update_pipeline_sync(run_id)
        except Exception as e:
            _log.exception(
                "[scheduler] stale-price retry failed: %s: %s", type(e).__name__, e,
            )

    _submit_scheduled_work("price_retry", _run)


def maybe_schedule_price_retry(*, reason: str = "") -> None:
    retries.schedule_price_retry(_scheduler, _fire_price_update_retry, reason=reason)


def _fire_asset_ingest_queue() -> None:
    market_jobs.process_asset_queue(runtime=_job_runtime())


def _fire_asset_price_refresh() -> None:
    """Daily refresh of STALE `asset_price` series for the HELD instruments.

    `metric_data` (GuruFocus) has had a daily refresh for ever; `asset_price` (Yahoo) never did —
    it was written when a row was added and then aged, silently. A stale series still returns
    prices and still charts; it is just old. Measured 2026-07-14: 197 of the 223 instruments held
    by the AIRS model portfolios were stale, and Meta Platforms — correctly mapped, 3,556 bars —
    rendered as a BLANK row in BUS_2.0_NEU_FX because its last close (2026-07-02) predated that
    portfolio's window (2026-07-09): no price inside the window, so no return over it exists.

     STANDS DOWN WHILE A WORKER IS ACTUALLY DRAINING THE INGEST QUEUE. That queue is *the*
    single Yahoo consumer by design: Yahoo answers an overloaded caller with an EMPTY result
    rather than a 429, and an empty candidate set is how a resolution silently lands on a thin
    foreign listing (the NVDA-on-Stuttgart / Alphabet-on-Vienna class of bug). Our own traffic is
    only chart fetches for symbols we already hold, so it cannot mis-resolve anything ITSELF —
    but it can push Yahoo into that regime while the resolver is mid-search and corrupt ITS work.
    A day-late price is a nuisance; a wrong listing is a wrong price series for ever.

     ...AND THAT MEANS THE WORKER, NOT THE BACKLOG. The first version gated on `pending > 0` and
    never ran once: the queue holds 9,945 pending ISINs last touched 2026-07-07, a week earlier —
    a stalled backlog, not active work (`status()["working"]` is just `pending > 0`, so it says
    "working" about a queue nobody is draining). `is_worker_active()` reads the real heartbeat:
    when a row was last MOVED out of pending.

    Scope is held instruments (~220, not the 16k grid) plus the small set of Yahoo benchmark
    proxies used by AIRS Analyse. Bounded scheduler worker; never raises.

    Also fired ON STARTUP by `_maybe_kickstart_asset_prices` — see there for why a daily tick
    alone is not enough.
    """
    _submit_scheduled_work(
        "asset_price_refresh",
        lambda: _run_asset_price_refresh("daily tick"),
    )


def _run_asset_price_refresh(trigger: str, *, raise_on_failure: bool = False) -> None:
    return market_jobs._run_asset_price_refresh(trigger, raise_on_failure=raise_on_failure, runtime=_job_runtime())


def _maybe_kickstart_asset_prices() -> None:
    """On STARTUP: if the held instruments' prices are behind, fix them now rather than waiting
    for 06:00.

    The daily tick keeps them current going forward; it cannot repair the PAST. A backend that
    was down over the weekend, a fresh deploy, a machine that has not run in a week — all come up
    with stale held prices and, until this, would serve blank rows on /portfolios until the next
    morning. That is the exact state the whole problem was found in: 197 of 223 held instruments
    stale, and Meta Platforms rendering as an empty row in a portfolio that holds it.

    Cheap when there is nothing to do: it DETECTS first (a few queries) and only then fetches, so
    the constant restarts of `uvicorn --reload` cost a couple of round-trips, not 220 Yahoo calls.
    Mirrors `_maybe_kickstart_smart`, which does the same for the GuruFocus pipeline.
    """
    _submit_scheduled_work(
        "asset_price_refresh",
        lambda: _run_asset_price_refresh("startup catch-up"),
    )


def _maybe_kickstart_airs_models() -> None:
    """On STARTUP: if we hold no model-portfolio COMPOSITIONS, scan them now.

     THE DAILY TICK CANNOT FIX A DEPLOYMENT THAT HAS NEVER HAD THEM. `airs_model_portfolio_
    position` was only ever populated by a human pressing "Scan AIRS", so a fresh environment
    starts empty — and `_airs_account_links._models()` keeps only models WITH a composition, so
    every account matches nothing, loses its pairing, and Analyse falls back to an unpaired
    basket. Measured in production 2026-08-03 on `AITopSelectie OFF DYN`: "No valued positions to
    show", beside a portfolios list whose rows expand perfectly.

    Waiting for the next weekday-morning tick would leave that state up for as much as a
    long weekend after the deploy that was supposed to fix it, which is the same reasoning as
    `_maybe_kickstart_asset_prices`.

     IT DETECTS BEFORE IT SCRAPES — ONE COUNT QUERY. The scan is minutes of authenticated
    Playwright, so it must not run on every restart: `uvicorn --reload` restarts constantly, and
    after the first successful scan the count is non-zero and this is a single round-trip no-op.
    Deliberately NOT a staleness check — refreshing an existing composition is the daily tick's
    job. This only fills a hole.

     WARNING level when it acts. uvicorn leaves the root logger at WARNING, so an `info` line is
    invisible in production — and "why did my deploy start scraping AIRS" is a question the log
    has to answer. The healthy no-op stays quiet.
    """
    def _run() -> None:
        try:
            from deps import supabase  # noqa: PLC0415

            n = (supabase.table("airs_model_portfolio_position")
                 .select("portfolio_id", count="exact").limit(1).execute().count or 0)
            if n:
                _log.info("[scheduler] %d model-portfolio position row(s) already stored — "
                          "no startup scan needed", n)
                return
            _log.warning(
                "[scheduler] NO model-portfolio compositions stored — scanning AIRS now. Without "
                "them no account can be paired to a model, so Analyse falls back to an unpaired "
                "basket for every portfolio. This runs once; the weekday morning tick keeps them "
                "current afterwards.")
            from airs_scanner import (  # noqa: PLC0415
                count_model_portfolio_holdings_sync,
                fetch_model_portfolios_sync,
            )
            from routers import _airs_portfolio_store as store  # noqa: PLC0415

            def _quiet(_msg_type: str, **_kw) -> None:
                """No SSE client on a startup run."""

            rows = fetch_model_portfolios_sync(_quiet)
            store.save_portfolios(rows)
            from management_portfolios import dashboard_model_rows
            count_model_portfolio_holdings_sync(
                dashboard_model_rows(rows), _quiet,
                on_positions=store.save_positions,
                on_error=store.save_positions_error,
            )
            _log.warning("[scheduler] startup model-portfolio scan stored %d portfolio(s)",
                         len(rows))
        except Exception as e:
            # Never fatal to boot. A missing AIRS credential or an unavailable AirSPMS must not
            # stop the API from serving; the pairing simply stays unresolved and says so.
            _log.exception("[scheduler] startup model-portfolio scan failed: %s: %s",
                           type(e).__name__, e)

    _submit_scheduled_work("airs_models_kickstart", _run)


def _fire_history_drift_check() -> None:
    """Weekly: probe 1/5th of the universe for a vendor rewrite of PAST bars.

     THE ONE FAILURE THE PIPELINE IS BLIND TO. Prices are only ever appended
    (`d > existing_max`), so a split or a free-share attribution leaves our
    history on the old basis indefinitely — Worldline sat in the live book on a
    +1142% momentum for a stock that had fallen 69%.

    Cheap because of the undocumented `?start_date=&end_date=` filter: a probe is
    ~23 bytes against a 268 KB full series. It is NOT cheap on quota (requests are
    what's metered, and a probe costs the same one as a full fetch), which is why
    normal price gap fetch now compares a one-week overlap for no extra request and immediately
    repairs the common whole-series rescale. This standalone probe is the slower safety net for an
    isolated OLD correction that does not touch the recent tail: one fifth each Monday, so every
    name comes round inside five weeks at ~300 requests/week instead of ~300/day.

    Own daemon thread; never raises into the scheduler."""
    _spawn_body("history_drift_check")


def _body_history_drift(ctx=None) -> tuple[str, dict]:
    return preparation_jobs._body_history_drift(ctx, runtime=_job_runtime())


def _fire_benchmark_price_slice() -> None:
    """Refresh benchmark data used by the Analyse charts."""
    _spawn_body("benchmark_price_slice")


def _body_benchmark_price_slice(ctx=None) -> tuple[str, dict]:
    return market_jobs._body_benchmark_price_slice(ctx, runtime=_job_runtime())


#: The universes whose 12-1 returns are ranked into the seven relative-momentum states.
#:
#:  THE SAME THREE THE ANALYSE MODAL OFFERS AS BENCHMARKS, and that is the requirement, not a
#: coincidence: `_holding_risk` ranks a holding against WHICHEVER benchmark the reader picked, so a
#: label missing here is a picker option whose momentum column silently loses its chip.

#: Coverage below this is reported as a WARNING rather than an info line.
#:
#:  IT IS A PRICE-STALENESS ALARM WEARING A COVERAGE THRESHOLD. The signal engine drops any name
#: whose last close is over 30 days old, so if constituent prices stop being refreshed the first
#: visible symptom is this number falling — not an error, not an empty page. Measured healthy:
#: ACWI 87.6%, SP500 99.2%, AEX 88.0%.


def _body_relative_momentum_refresh(ctx=None) -> tuple[str, dict]:
    return market_jobs._body_relative_momentum_refresh(ctx, runtime=_job_runtime())


# The indices whose constituents get a due-only fundamentals pass.
#
#  All three. Fundamentals are not prices: the ETF's own series answers
# "what did ACWI return", and answers nothing at all about its constituents' margins — which is the
# whole of the Long Equity tab and the fundamentals grid. Those exist only per company.

#  The floor is not zero. Stopping at 0 spends a region's last call and leaves the month-end
# `full_price_refresh` — the job that keeps every price series alive — with nothing. A reserve is
# the cheap way to make this job the one that yields.


def _fire_benchmark_fundamentals() -> None:
    """Quarterly fundamentals pass over every benchmark constituent. Own daemon thread."""
    _spawn_body("benchmark_fundamentals_fill")


def _fire_acwi_weekly_coverage() -> None:
    _spawn_body('acwi_weekly_coverage')


def _body_acwi_weekly_coverage(ctx=None) -> tuple[str, dict]:
    return market_jobs._body_acwi_weekly_coverage(ctx, runtime=_job_runtime())


def _body_acwi_monthly_fundamentals(ctx=None) -> tuple[str, dict]:
    return market_jobs._body_acwi_monthly_fundamentals(ctx, runtime=_job_runtime())


def _bootstrap_acwi_holdings() -> None:
    market_jobs.bootstrap_acwi_holdings()


def _fundamental_company_ids(label: str) -> list[tuple[int, str]]:
    return market_jobs._fundamental_company_ids(label)


def _body_benchmark_fundamentals(ctx=None) -> tuple[str, dict]:
    return market_jobs._body_benchmark_fundamentals(ctx, runtime=_job_runtime())


def _fire_fx_sync() -> None:
    """Daily ECB FX sync — keeps EVERY fetchable currency's `fx_rate` current.
    The daily pipeline only syncs the currencies the held strategies actually
    use (as a side effect of the momentum backtest stream), so unused ACWI
    currencies would otherwise go stale on the /fx-rates page. Idempotent +
    cheap: `sync_fx_rates_to_db` fetches only the gap since each currency's last
    stored date. Own daemon thread; never raises into the scheduler."""
    _spawn_body("fx_sync")


def _body_fx_sync(ctx=None) -> tuple[str, dict]:
    return market_jobs._body_fx_sync(ctx, runtime=_job_runtime())


def _fire_airs_model_prices() -> None:
    """Legacy manual hook: reprice every AIRS model portfolio."""
    _spawn_body("airs_model_prices")


def _body_airs_model_prices(ctx=None, *, stored_composition: bool = True) -> tuple[str, dict]:
    return airs_jobs._body_airs_model_prices(ctx, stored_composition=stored_composition, runtime=_job_runtime())


def _analyse_sources_busy(*, ignore_airs: bool = False) -> bool:
    from asset_pipeline import queue as ingest_queue  # noqa: PLC0415
    from deps import supabase  # noqa: PLC0415
    import jobs  # noqa: PLC0415
    sources = {'airs_weekly_returns', 'dashboard_market', 'airs_vermogen_refresh', 'benchmark_price_slice', 'asset_price_refresh', 'fx_sync', 'airs_price_checks'}
    if ignore_airs:
        sources.discard('airs_vermogen_refresh')
    if admission.active_jobs() & sources:
        return True
    queue_paused_here = ignore_airs and os.environ.get("ASSET_QUEUE_INPROCESS", "1").lower() not in ("0", "false", "no")
    if (not queue_paused_here and ingest_queue.is_worker_active()) or _pipeline_already_running():
        return True
    if any(row.get('status') == 'running' and str(row.get('kind', '')).startswith(('airs.', 'asset.', 'benchmark.'))
           for row in jobs.listing()):
        return True
    try:
        # Scheduled work from an earlier process cannot still be running on
        # this scheduler. A reload may leave its receipt open.
        cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
        if _booted_at is not None:
            cutoff = max(cutoff, _booted_at)
        return bool(supabase.table('scheduled_job_run').select('job_id').eq('status', 'running')
                    .in_('job_id', sorted(sources)).gte('started_at', cutoff.isoformat()).limit(1).execute().data)
    except Exception:
        return True  # no visibility into writers: defer instead of competing with them


def _body_analyse_prepare(ctx=None) -> tuple[str, dict]:
    return preparation_jobs._body_analyse_prepare(ctx, runtime=_job_runtime())


def _fire_analyse_prepare() -> None:
    _spawn_body('analyse_prepare')


def _fire_airs_price_checks() -> None:
    _spawn_body("airs_price_checks")


def _body_airs_price_checks(ctx=None) -> tuple[str, dict]:
    return price_check_jobs.run(ctx, report=_reporter(ctx), wait_for_queue=_wait_for_yahoo_queue)


def _fire_airs_vermogen() -> None:
    """APScheduler callable for the daily AIRS Vermogensoverzicht refresh. Runs
    on its own daemon thread so the long Playwright scrape doesn't block the
    scheduler worker. Re-discovers the live portfolio list + stores each
    portfolio's holdings snapshot (see `airs_vermogen`).

     IT FORCES. The manual button is incremental — it skips an account fully scanned in the last
    `AIRS_FRESH_HOURS` — but this is the once-a-day pass that has to actually pick up the day's
    valuation. Somebody pressing Refresh all before AIRS has valued the books would
    otherwise make this job skip the whole fleet and the new valuation would land a day late.
    The schedule is declared once, in `scheduled_jobs.SCHEDULED_JOBS`. It runs at 11:00 Amsterdam,
    which avoids the earlier stale-valuation race while keeping the once-daily forced scan bounded.
    """
    _spawn_body("airs_vermogen_refresh")


def _wait_for_yahoo_queue(ctx) -> None:
    from asset_pipeline import queue
    deadline = time.monotonic() + 900
    while queue.is_worker_active():
        if _cancel_requested(ctx):
            raise _Cancelled("Refresh cancelled while waiting for the Yahoo queue")
        if time.monotonic() >= deadline:
            raise RuntimeError("Yahoo queue remained active for 15 minutes; refresh could not acquire a quiet window")
        _reporter(ctx)(0, 0, "Waiting for the existing Yahoo queue slice to settle; new slices are paused")
        time.sleep(15)


def _body_dashboard_market(ctx=None) -> tuple[str, dict]:
    return preparation_jobs._body_dashboard_market(ctx, runtime=_job_runtime())


def _body_dashboard_freshness(ctx=None) -> tuple[str, dict]:
    return preparation_jobs._body_dashboard_freshness(ctx, runtime=_job_runtime())


def _body_dashboard_post_airs(ctx=None) -> tuple[str, dict]:
    return preparation_jobs._body_dashboard_post_airs(ctx, runtime=_job_runtime())


def _body_airs_vermogen(ctx=None) -> tuple[str, dict]:
    return airs_jobs._body_airs_vermogen(ctx, runtime=_job_runtime())


def _fire_table_size_sample() -> None:
    """Nightly: record every public table's size on disk.

     BYTES, NOT ROWS WRITTEN — see `db_growth`. Instrumenting the jobs to count their own inserts
    would rank the AIRS model scan (which delete-then-inserts every portfolio'''s positions:
    thousands of rows, zero growth) above the month-end price refresh, and would be blind to
    indexes and bloat.

    Cheap enough to need no gating: one catalog read and ~50 small inserts. Own daemon thread for
    consistency with every other tick; never raises into the scheduler.
    """
    _spawn_body("table_size_sample")


def _body_table_size_sample(ctx=None) -> tuple[str, dict]:
    return preparation_jobs._body_table_size_sample(ctx, runtime=_job_runtime())


def _fire_airs_weekly_returns():
    _spawn_body("airs_weekly_returns")


def _body_airs_weekly_returns(ctx=None):
    from scheduler_jobs.weekly_returns import run
    return run(ctx, report=_reporter(ctx))


def _register_bodies() -> None:
    """Fill `JOB_BODIES` once every body is defined.

     AT THE BOTTOM OF THE MODULE, NOT AT THE DICT. The bodies are defined throughout the file
    beside the ticks they belong to; naming them where the dict is declared would be forward
    references to functions that do not exist yet.
    """
    JOB_BODIES.update({
        "airs_weekly_returns": _body_airs_weekly_returns,
        "dashboard_market": _body_dashboard_market,
        "dashboard_post_airs": _body_dashboard_post_airs,
        "dashboard_freshness": _body_dashboard_freshness,
        "fx_sync": _body_fx_sync,
        "airs_vermogen_refresh": _body_airs_vermogen,
        "analyse_prepare": _body_analyse_prepare,
        "airs_price_checks": _body_airs_price_checks,
        "job_watchdog": _body_job_watchdog,
        "history_drift_check": _body_history_drift,
        "benchmark_price_slice": _body_benchmark_price_slice,
        "asset_price_refresh": _body_asset_price_refresh,
        "benchmark_fundamentals_fill": _body_benchmark_fundamentals,
        'acwi_weekly_coverage': _body_acwi_weekly_coverage,
        'acwi_monthly_fundamentals': _body_acwi_monthly_fundamentals,
        "table_size_sample": _body_table_size_sample,
    })
    #  The same late binding, for the same reason — see `_WATCHDOG_STARTERS`. These two are the
    # tick callables themselves rather than `(ctx) -> (str, dict)` bodies: they spawn their own
    # daemon threads and narrate into `ingest_run`, which is where /schedule already watches them.
    _WATCHDOG_STARTERS.update({
        "daily_pipeline": _fire_daily_sequence,
        "sunday_rebalance_preflight": _fire_sunday_rebalance_preflight,
        "daily_price_slice": _fire_daily_price_slice,
    })


#: When THIS process's scheduler came up.  SET AT START, READ BY THE GAP SCAN — it is the whole
#: evidence that a missed fire time was missed because nothing was alive to fire it.
_booted_at: "datetime | None" = None


def _on_job_missed(event) -> None:
    """APScheduler dropped a fire because it arrived past `misfire_grace_time`.

     NOTHING LISTENED TO THIS EVENT UNTIL NOW, WHICH IS HALF OF WHY THE PRODUCTION FAILURE WAS
    UNDIAGNOSABLE. A dropped fire wrote no row, logged no line and left the job's `next_run_time`
    looking perfect — so `/schedule` could only report `overdue` and shrug. This is the case where
    the process WAS alive and could not get to the job in time (a blocked worker, a saturated pool),
    which is a different fault with a different fix from the process being absent, and the two must
    not arrive as the same row.

     WARNING, NOT INFO. uvicorn leaves the root logger at WARNING, so an INFO line here would be
    invisible in Railway — which is the one place this needs to be readable.
    """
    fire = getattr(event, "scheduled_run_time", None) or datetime.now(timezone.utc)
    detail = (f"the {fire.astimezone(timezone.utc):%Y-%m-%d %H:%M UTC} fire was dropped: it came "
              f"up more than its grace period late while the process was running")
    _log.warning("[scheduler] MISSED %s scheduled for %s — misfire grace exceeded",
                 event.job_id, fire)
    try:
        from job_runlog import record_missed  # noqa: PLC0415

        record_missed(event.job_id, fire, detail, cause="misfire_grace_exceeded")
    except Exception as e:  # noqa: BLE001 — an observer must never raise into the scheduler
        _log.warning("[scheduler] could not record the missed %s tick: %s: %s",
                     event.job_id, type(e).__name__, e)


def _on_job_max_instances(event) -> None:
    """Summarise a serial queue overrun instead of logging one warning per 20-second tick."""
    if event.job_id != "asset_ingest_queue":
        return
    global _queue_overruns, _queue_overrun_last_log
    now = time.monotonic()
    with _queue_overrun_lock:
        _queue_overruns += len(getattr(event, "scheduled_run_times", ()) or (1,))
        # `0.0` is the deliberately-unset initial value.  `time.monotonic()` can
        # itself still be below five minutes on a fresh container, so subtracting
        # it directly would suppress the first useful backpressure signal.
        if (_queue_overrun_last_log
                and now - _queue_overrun_last_log < _QUEUE_OVERRUN_LOG_EVERY_SECONDS):
            return
        skipped = _queue_overruns
        _queue_overruns = 0
        _queue_overrun_last_log = now
    _log.warning(
        "[scheduler] asset ingest queue is still draining its previous slice; "
        "%d 20-second tick(s) skipped (serial Yahoo/OpenFIGI backpressure, not a crash)",
        skipped,
    )


def _on_job_error(event) -> None:
    """A tick raised out of its callable.

     BELT AND BRACES, AND IT COVERS A REAL SEAM. Every `_fire_*` spawns a daemon thread and the
    body records itself through `record_run`, so almost every failure is already durable — but an
    exception raised BEFORE the thread starts (a bad id, a failed import inside `_spawn_body`)
    happened outside every one of those try blocks and vanished. That is the narrow gap this closes;
    it is not the main event, and it must not double-record one the body already owns, which is why
    it writes only to the log unless the body never opened a row.
    """
    _log.exception("[scheduler] job %s raised out of its tick: %s",
                   event.job_id, getattr(event, "exception", None))


def scan_for_missed_ticks(now=None) -> dict:
    """Reconstruct, from each trigger, the ticks that should have fired recently and did not.

     THIS IS THE HALF THAT EXPLAINS THE PRODUCTION SYMPTOM, and it is not a misfire. The scheduler
    here uses APScheduler's DEFAULT IN-MEMORY JOBSTORE, so a boot recomputes every `next_run_time`
    from *now*: a fire time that passed while the process was down never existed, emits no event,
    and leaves `next_run_time` looking healthy. That is precisely how `daily_pipeline` read
    "20.9 days ago" beside "Next run tomorrow, 07:00" — and why no amount of listening to
    APScheduler could ever have caught it. The trigger is a pure function of the calendar, so it can
    be asked what it WOULD have done over a window that reaches back before this process existed.

     RUN AT BOOT, WHICH IS THE ONLY MOMENT IT IS BOTH POSSIBLE AND USEFUL: possible because the
    window now spans a period nobody was watching, useful because a restarting host reaches this
    line often. Idempotent by construction — see `record_missed`.

     IT RECORDS, IT DOES NOT HEAL. What to do about a gap is `_body_job_watchdog`'s decision, with
    its own cap; conflating the two would make the evidence-gatherer a job-firer, and a boot loop
    would then re-fire the fleet on every restart.
    """
    from job_misses import missed_windows, describe, should_scan  # noqa: PLC0415
    from job_runlog import (  # noqa: PLC0415
        ingest_run_stamps, record_missed, started_at_stamps,
    )

    now = now or datetime.now(timezone.utc)
    since = now - timedelta(days=job_misses_lookback())
    found: dict[str, int] = {}
    unreadable: list[str] = []
    for spec in SCHEDULED_JOBS:
        if not should_scan(spec):
            continue
        #  Both tables, because "DID THIS TICK FIRE" HAS TWO ANSWER SHEETS. A `records=False`
        # job (`daily_pipeline`, `daily_price_slice`) writes `ingest_run` rows and NO
        # `scheduled_job_run` row — by design, so one event cannot have two disagreeing records.
        # Scanning only the latter would report every night of a healthy pipeline as a missed tick,
        # which is the loudest possible false alarm on the two jobs nobody can afford to start
        # ignoring. The `missed` rows this writes still go under the job's own id — that is the one
        # thing it CAN say — which is why `build_rows` now always looks a spec up under its id too.
        own = started_at_stamps(spec.id, since)
        via_ingest = ingest_run_stamps(spec.evidence, since)
        if own is None or via_ingest is None:
            #  A failed read is not an empty history. Treated as empty, one Supabase blip at boot
            # would invent a week of misses for every job at once — the monitoring manufacturing
            # the outage it exists to report.
            unreadable.append(spec.id)
            continue
        stamps = own + via_ingest
        try:
            trigger = CronTrigger(**(spec.trigger or {}))
        except Exception as e:  # noqa: BLE001 — a malformed declaration is not a reason to fail boot
            _log.warning("[scheduler] cannot rebuild %s's trigger to scan for gaps: %s: %s",
                         spec.id, type(e).__name__, e)
            continue
        grace = int((spec.options or {}).get("misfire_grace_time") or 3600)
        misses = missed_windows(trigger, stamps, now=now,
                                lookback_days=job_misses_lookback(), grace_seconds=grace)
        written = 0
        for fire in misses:
            if record_missed(spec.id, fire, describe(fire, _booted_at),
                             cause="scheduler_not_running", booted_at=(
                                 _booted_at.isoformat() if _booted_at else None)):
                written += 1
        if written:
            found[spec.id] = written
            #  One loud line per job, at WARNING so Railway shows it. This is the sentence somebody
            # greps for when a page says "overdue" and they want to know since when.
            _log.warning("[scheduler] %s missed %d scheduled tick(s) in the last %dd — the "
                         "scheduler was not running for them (first: %s)",
                         spec.id, written, job_misses_lookback(), misses[0])
    if unreadable:
        _log.warning("[scheduler] gap scan could not read the run history for: %s",
                     ", ".join(unreadable))
    return {"missed": found, "unreadable": unreadable,
            "total": sum(found.values())}


def job_misses_lookback() -> int:
    """How many days back the gap scan looks.  ENV-OVERRIDABLE so a long outage can be
    reconstructed once by hand without a deploy; the default is deliberately short (see
    `job_misses.DEFAULT_LOOKBACK_DAYS`)."""
    from job_misses import DEFAULT_LOOKBACK_DAYS  # noqa: PLC0415

    raw = os.environ.get("JOB_GAP_LOOKBACK_DAYS", "").strip()
    if raw.isdigit() and 1 <= int(raw) <= 90:
        return int(raw)
    return DEFAULT_LOOKBACK_DAYS


def _boot_gap_pass() -> None:
    """At boot: write down which ticks were lost while nothing was running, then heal what can be.

     RECORD FIRST, HEAL SECOND, AND NEVER THE OTHER WAY ROUND. The watchdog's verdict comes from
    `job_health`, which reads the run history — so a heal that ran first would re-fire the jobs and
    the gap scan would then find their fresh rows and conclude nothing had been missed. The outage
    would erase its own evidence, every time, which is the failure mode that made this invisible in
    the first place.

     THE HEAL IS THE WATCHDOG, NOT A SECOND FIRING MECHANISM. `_body_job_watchdog` already owns
    the decision about which states "run it again" actually fixes (`_WATCHDOG_HEALS` — not
    `missing`, `error` or `unknown`, each of which it would paper over) and the per-day cap that
    stops a structurally broken job being retried for ever. Re-firing jobs directly from here would
    be a second copy of that judgement, and the copy is the one that drifts.

     WHICH IS ALSO WHY THE CAP HAD TO BECOME DURABLE FIRST (`watchdog_runs_today`). It lived in a
    process-local dict, and a boot resets that to zero — so a host in a restart loop, which is
    exactly the host that reaches this line, would have re-fired the whole fleet on every restart
    with a guard that could never see it had already done so.

     THE WATCHDOG IS A SCHEDULED JOB AND THAT IS PRECISELY THE PROBLEM THIS SOLVES. It fires at
    11:00 UTC; if the process is not alive at 11:00 UTC it is missed by the same mechanism as
    everything it was meant to heal — measured in production at 44.7h stale, itself reported
    `overdue`. Running it once per boot means any boot heals the backlog, whatever the host does to
    the clock.

     NEVER RAISES. It runs on a daemon thread off the startup hook; an exception here would be an
    unhandled thread exception during a deploy, which is noise on top of the outage it is reporting.
    """
    #  The scan is evidence and its result must not gate the heal — it did, and that made a boot
    # heal only the subset of broken jobs that happened to have a missed FIRE. The two answer
    # different questions: `scan_for_missed_ticks` asks "did a tick fail to fire" (the process was
    # down, or APScheduler dropped it), while the watchdog asks "is this job broken" — which is
    # also true of a job that fired perfectly and then ERRORED, or was `interrupted` by a deploy
    # landing mid-run. Neither of those leaves a missed tick, so `if not result["total"]: return`
    # skipped exactly the failures that have nothing to do with the schedule. A boot heals the
    # backlog or it does not; "heals the backlog, but only when the host was also down" is a rule
    # nobody can hold in their head, and it is not the rule the docstring above claims.
    #
    #  The heal is safe to run unconditionally because the judgement is not here. `_body_job_watchdog`
    # decides what "run it again" actually fixes (`_WATCHDOG_HEALS` — never `missing`, `error` or
    # `unknown`) and `_watchdog_budget_spent` reads a DURABLE per-day cap, so a host in a restart
    # loop cannot re-fire the fleet on every boot. A scan that found nothing now costs one extra
    # `job_health` read per boot, which is the price of the guarantee.
    try:
        result = scan_for_missed_ticks()
    except Exception as e:  # noqa: BLE001
        #  And a failed scan no longer cancels the heal either. Losing the evidence is not a
        # reason to leave the jobs broken; the watchdog reads the run history itself.
        _log.warning("[scheduler] the boot gap scan failed: %s: %s — healing anyway",
                     type(e).__name__, e)
        result = {}
    if result.get("total"):
        _log.warning("[scheduler] boot gap scan recorded %d missed tick(s): %s",
                     result["total"], result["missed"])
    else:
        _log.info("[scheduler] boot gap scan: no missed ticks in the last %dd",
                  job_misses_lookback())
    if os.environ.get("DISABLE_BOOT_HEAL", "").lower() in ("1", "true", "yes"):
        #  An off switch for the healing half alone, because the two halves have very different
        # risk. Recording is a handful of inserts; healing starts real jobs that spend vendor quota.
        # A deployment that wants the evidence without the action can have exactly that.
        _log.warning("[scheduler] DISABLE_BOOT_HEAL set — not re-running the broken jobs")
        return
    # This function already owns the sole maintenance worker as job_gap_scan.
    # Submitting another maintenance task here always loses admission. Run the
    # recorded watchdog on this worker instead, preserving the capacity bound.
    try:
        _run_body("job_watchdog")
    except Exception as e:  # noqa: BLE001 — same isolation as a scheduled body
        _log.exception("[scheduler] startup job_watchdog failed: %s: %s", type(e).__name__, e)


def register_scheduler(app) -> None:
    """Attach the scheduler to the FastAPI lifecycle. Called once from
    `main.py` after the FastAPI() instance is created."""

    @app.on_event("startup")
    def _start_scheduler() -> None:
        global _scheduler, _booted_at
        if _scheduler is not None:
            return  # already running (multiple startup events on reload)
        #  Stamped before anything else. Every missed-tick row this boot writes carries it, and it
        # is the whole argument that the tick was missed because nothing was alive: a fire time
        # before this instant, with no row, on a scheduler that only exists from here.
        _booted_at = datetime.now(timezone.utc)

        # Allow operators to disable the in-process scheduler via env var —
        # useful when running multiple replicas, during a manual ingest test,
        # or in CI where we don't want background jobs touching real data.
        if os.environ.get("DISABLE_SCHEDULER", "").lower() in ("1", "true", "yes"):
            _log.info("[scheduler] DISABLE_SCHEDULER set — in-process jobs not started")
            return

        # These threads only dispatch; long work has separate bounded capacity.
        sched = BackgroundScheduler(timezone="UTC", executors={
            "default": {"type": "threadpool", "max_workers": 2},
        })
        # Max-instance warnings originate on the scheduler logger. Logger
        # filters do not propagate from parents to children.
        for logger_name in ("apscheduler.scheduler", "apscheduler.executors.default"):
            executor_log = logging.getLogger(logger_name)
            if not any(isinstance(f, _ExpectedQueueOverrunFilter) for f in executor_log.filters):
                executor_log.addFilter(_ExpectedQueueOverrunFilter())
        #  The observers go on before any job is added, and before `start()`. Until 2026-09-01
        # nothing listened to either event, which is half of why a production job could sit 20 days
        # stale with a healthy next-run beside it and no explanation anywhere: a dropped fire wrote
        # no row and logged no line. See `_on_job_missed` for what the two events can and cannot
        # tell us — notably that neither of them fires for the commonest case of all, a process that
        # was not running, which is what `scan_for_missed_ticks` is for.
        sched.add_listener(_on_job_missed, EVENT_JOB_MISSED)
        sched.add_listener(_on_job_error, EVENT_JOB_ERROR)
        sched.add_listener(_on_job_max_instances, EVENT_JOB_MAX_INSTANCES)

        def _register(job_id: str, fn) -> None:
            """Register one declared job —  THE SCHEDULE COMES FROM `scheduled_jobs.py`.

            Every cadence below used to be a `CronTrigger(...)` literal at the call site. That was
            fine while nothing else claimed to know the schedule; the moment an admin page shows
            "every day, 05:00 UTC" the literal becomes a SECOND copy, and the copy is the one that
            drifts — leaving a page confidently reporting a cadence nothing runs at. Now there is
            one number, it lives in the declaration, and this reads it.

            The trigger KIND is decided by which field the spec set: cron by default, interval for
            the queue worker. `options` carries the per-job coalesce / grace / max_instances, which
            are behaviour under load rather than schedule — but they belong beside the schedule they
            modify, not here.
            """
            spec = BY_ID[job_id]
            if spec.after_job:
                return  # Parent executes this stage synchronously; never register another clock.
            trigger = (IntervalTrigger(seconds=spec.interval_seconds)
                       if spec.interval_seconds is not None
                       else CronTrigger(**(spec.trigger or {})))
            options = dict(spec.options)
            if job_id == "analyse_prepare":
                options["next_run_time"] = datetime.now(timezone.utc) + timedelta(seconds=15)
            sched.add_job(fn, trigger, id=spec.id, replace_existing=True, **options)
        # Single daily tick at 05:00 Amsterdam that runs the
        # split pipeline's two operations IN ORDER (see `ingest.phases.pipeline`):
        #   1. price-update — re-price the held companies + refresh MTD;
        #   2. rebalance    — rebalance any strategy whose rebalance day has
        #      arrived (a no-op otherwise).
        # They run sequentially in one daemon thread and never overlap (the
        # rebalance also serializes against manual Run-now via the pipeline
        # lock). The Amsterdam-local trigger keeps the requested wall-clock time through DST.
        # Monday's tick still
        # has Friday's settled close, so a first-Monday rebalance decides on
        # Friday's close. Weekend ticks are cheap no-ops via the per-company
        # freshness short-circuit.
        # If a startup coincides with the tick (e.g. a deploy right at 05:00 Amsterdam),
        # `coalesce=True` collapses any backlog into a single run and
        # `misfire_grace_time` gives 10 min of slack — both declared with the schedule.
        _register("dashboard_market", lambda: _spawn_body("dashboard_market"))
        _register("dashboard_post_airs", lambda: _spawn_body("dashboard_post_airs"))
        _register("dashboard_freshness", lambda: _spawn_body("dashboard_freshness"))
        _register("daily_pipeline", _fire_daily_sequence)
        _register("sunday_rebalance_preflight", _fire_sunday_rebalance_preflight)
        # Daily price slice — the most-stale companies, every day, so the whole book cycles in
        # ~19 days and no series ever ages into the signal engine's 30-day staleness drop.
        # Replaced the gated month-end full pass (2026-09-02): one pass a month against a 30-day
        # guard is the same period, so coverage collapsed for the few days before each refresh.
        # 12:00 UTC — the slot the month-end tick already held, well clear of the 05:00 pipeline,
        # and 07:00 EST, safely inside the EST usage month that resets midnight EST on the 1st.
        # Prices-only; serializes against the daily ops via the pipeline lock.
        _register("daily_price_slice", _fire_daily_price_slice)
        # Daily AIRS refresh — working days, morning, Amsterdam-local (the hour is declared in
        # `scheduled_jobs.SCHEDULED_JOBS`, not restated here).
        # The per-job timezone makes APScheduler handle the CET/CEST DST shift;
        # only weekday holidays aren't skipped (a holiday run just re-stores the
        # prior close, harmless). Re-discovers the live AirSPMS portfolio list
        # each run (it changes day-to-day) and stores each portfolio's Rendement
        # + Vermogensoverzicht. Runs on its own thread.
        #
        #  It also scans the model portfolios now (Stamgegevens → Model portefeuilles), which
        # nothing scheduled ever did. Their compositions are what the account↔model PAIRING is
        # guessed from, so on a deployment where nobody pressed "Scan AIRS" by hand every book
        # was unpaired and Analyse fell back to a basket. See `_fire_airs_vermogen`.
        _register("airs_vermogen_refresh", _fire_airs_vermogen)
        _register("analyse_prepare", _fire_analyse_prepare)
        _register("airs_price_checks", _fire_airs_price_checks)
        #  The one job whose subject is the other jobs — see `_body_job_watchdog`.
        _register("job_watchdog", _fire_job_watchdog)
        # Nightly database-size snapshot — one row per public table, so "how fast is this growing
        # and which tables" is a subtraction rather than a guess. Reads the Postgres catalog; it
        # writes ~50 tiny rows and takes milliseconds.
        _register("table_size_sample", _fire_table_size_sample)
        # Daily ECB FX sync — weekdays 16:30 UTC, after the ECB ~16:00 CET
        # reference-rate publication, so the `fx_rate` table (and the /fx-rates
        # page) shows EVERY currency current, not just the held ones the daily
        # pipeline happens to sync. Idempotent + cheap (fetches only the gap).
        _register("fx_sync", _fire_fx_sync)
        # Daily Yahoo price refresh for the HELD instruments — the `asset_price` twin of the
        # 05:00 GuruFocus price update, which `asset_price` never had (it aged silently: 197 of
        # 223 held instruments were stale, and a portfolio whose window opened after its
        # holdings' last close rendered blank rows).
        #
        # 06:00 UTC: every market's previous close is long settled, and it is AFTER the 05:00
        # daily sequence rather than racing it. `max_instances=1` so a slow run (≈220 gap fetches
        # at ~1.5s each) can never overlap the next day's tick, and the job itself stands down
        # entirely while the ingest queue is resolving — see `_fire_asset_price_refresh`.
        _register("asset_price_refresh", _fire_asset_price_refresh)
        # Refresh every benchmark's constituents, prices and caps, then calculate relative
        # momentum in the same worker. The dependency is explicit: a fixed later clock time could
        # start ranking while a long ACWI refresh is still in progress.
        _register("benchmark_price_slice", _fire_benchmark_price_slice)
        # Weekly due-only fundamentals pass. It fetches only companies whose next filing could
        # have arrived, rather than re-reading every constituent.
        _register("benchmark_fundamentals_fill", _fire_benchmark_fundamentals)
        _register("airs_weekly_returns", _fire_airs_weekly_returns)
        _register("acwi_weekly_coverage", _fire_acwi_weekly_coverage)
        _register("acwi_monthly_fundamentals", lambda: _spawn_body("acwi_monthly_fundamentals"))
        # Weekly history-drift safety probe. Normal GuruFocus price updates compare a one-week
        # overlap and immediately repair a rescale; this Monday slice catches isolated old-bar
        # corrections that leave the recent tail untouched.
        _register("history_drift_check", _fire_history_drift_check)
        # Asset-pipeline ingest-queue worker — ON by default. Analyse queues missing ISINs and a
        # normal backend deployment must consume them without requiring a second process. A
        # standalone worker remains supported for deployments that explicitly opt out here.
        if os.environ.get("ASSET_QUEUE_INPROCESS", "1").lower() not in ("0", "false", "no"):
            _register("asset_ingest_queue", _fire_asset_ingest_queue)
            _log.info("[scheduler] in-process asset ingest-queue worker enabled")
        sched.start()
        _scheduler = sched
        _submit_scheduled_work("acwi_holdings_bootstrap", _bootstrap_acwi_holdings)
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
        # an env that was down across the 05:00 UTC tick catches up
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
        # ...and the same catch-up for `asset_price` (Yahoo, the /portfolios holdings). The 06:00
        # tick keeps them current going FORWARD; it cannot repair the past. A backend down over a
        # weekend, or a fresh deploy, comes up with stale held prices and would serve blank rows
        # on /portfolios until the next morning. Detects first (a few queries), so the constant
        # restarts of `uvicorn --reload` are a near-free no-op rather than 220 Yahoo calls.
        try:
            _maybe_kickstart_asset_prices()
        except Exception as e:
            _log.warning(
                "[scheduler] asset-price kickstart wrapper failed: %s: %s",
                type(e).__name__, e,
            )
        # ...and fill the model-portfolio compositions if this deployment has none. Unlike the
        # two above this is not a staleness catch-up — it is the "never had them" case, which the
        # daily tick cannot fix retroactively and which silently unpairs every account until it
        # runs. One count query when they exist; see `_maybe_kickstart_airs_models`.
        next_runs = {j.id: str(j.next_run_time) for j in sched.get_jobs()}
        #  Warning, not info — uvicorn leaves the root logger at WARNING, so the one line that says
        # this process's scheduler exists at all was invisible in Railway. "Did the scheduler even
        # start after that deploy?" is the first question a stale job raises and the log could not
        # answer it.
        _log.warning("[scheduler] started at %s; next runs: %s", _booted_at, next_runs)

        #  The gap scan and the heal run off the startup hook, on their own thread. Both read
        # Supabase (one query per job, then possibly a job start), and a FastAPI startup hook that
        # blocks on the network is a deploy that looks hung — on a host which, per the evidence
        # this was written for, is already restarting more than it should.
        _submit_scheduled_work("job_gap_scan", _boot_gap_pass)

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


#  At import, after every body is defined — see `_register_bodies`. Without this the Run-now
# endpoint would find an empty registry and report every job as not runnable by hand.
_register_bodies()
