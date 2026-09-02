"""In-process APScheduler for the single daily smart pipeline tick.

One `BackgroundScheduler` cron trigger runs inside the FastAPI process:

    smart_daily   Daily 05:00 UTC — dependency-driven pipeline

Each tick derives, from the enabled scheduled strategies, exactly what's
needed and runs only that (`ingest.phases.pipeline._run_smart_pipeline_sync`):
refresh only the universes those strategies use, keep every strategy's held
companies priced daily, and rebalance each strategy on the first occurrence
of its baked `rebalance_weekday` in its period. A Monday 05:00 UTC tick
already has Friday's settled close (US close Fri 21:00 UTC + ~8h), so a
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
import threading
from datetime import date, datetime, timedelta, timezone

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from job_runlog import record_run
from routers.ingest_runs import kick_off_refresh
# ⚠ THE SCHEDULE ITSELF LIVES THERE, NOT HERE — see `_register`. Declaration only: no DB, no
# APScheduler, so it is safe for both this module and the admin router to import.
from scheduled_jobs import BY_ID, ORPHAN_MARKER, SCHEDULED_JOBS

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

# ── Stale held-price retry ─────────────────────────────────────────
# After a price-update, GuruFocus may not yet have published the prior
# session's closes (the slower EU EOD feeds especially). Rather than wait a
# full day for the next 05:00 UTC tick, re-run the held-price refresh a few
# hours later to pick them up. Bounded per UTC day so a genuinely
# unpublishable name (market holiday, illiquid stock) can't loop forever —
# once the day's budget is spent the next daily tick takes over.
_PRICE_RETRY_DELAY_HOURS = float(os.environ.get("PRICE_RETRY_DELAY_HOURS", "3"))
_PRICE_RETRY_MAX_PER_DAY = int(os.environ.get("PRICE_RETRY_MAX_PER_DAY", "3"))
_price_retry_lock = threading.Lock()
# UTC-date ISO string → retries scheduled so far that day. Pruned to a single
# key (today's) on each schedule so it never grows.
_price_retry_counts: dict[str, int] = {}

# ── PostgREST cold start ───────────────────────────────────────────
# ⚠ A STARTUP PROBE CAN BEAT THE DATABASE TO READINESS, AND THE FAILURE LOOKS LIKE A FAULT.
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
_DB_WARMUP_ATTEMPTS = int(os.environ.get("DB_WARMUP_ATTEMPTS", "6"))
_DB_WARMUP_DELAY_SECONDS = float(os.environ.get("DB_WARMUP_DELAY_SECONDS", "5"))


def _is_db_warming(exc: BaseException) -> bool:
    """Is PostgREST still BOOTING (wait for it), or did the query genuinely fail (report it)?

    Matched on the PostgREST error CODE, never on its prose: PGRST002 = the schema cache is not
    loaded yet, PGRST001 = it cannot reach the database at all. Both are states a booting stack
    passes THROUGH. Every other code is a real answer about our query and must surface — a
    substring match on "Retrying." would swallow those too.
    """
    code = str(getattr(exc, "code", "") or "")
    if code in {"PGRST001", "PGRST002"}:
        return True
    # postgrest's APIError carries the raw dict in `str(exc)`; keep a fallback in case the
    # attribute moves, but stay anchored on the codes.
    text = str(exc)
    return "PGRST001" in text or "PGRST002" in text


def _await_db_ready(what: str) -> bool:
    """Block (on a daemon thread) until PostgREST serves a trivial read, or give up saying so.

    ⚠ THIS IS A GATE, NOT A RETRY WRAPPER AROUND THE WORK. It spends one tiny query per attempt;
    wrapping the job itself would re-spend whatever the job had already done (the asset-price
    refresh costs ~1.5s of Yahoo per instrument). Once the gate opens, the job runs exactly once
    and any later error is a real one.

    Returns True when the database answered, False when it never did — the caller then logs one
    clear line instead of a traceback that reads like a bug in the job.
    """
    import time  # noqa: PLC0415

    from deps import supabase  # noqa: PLC0415

    for attempt in range(1, _DB_WARMUP_ATTEMPTS + 1):
        try:
            supabase.table("asset_analysis").select("analysis_id").limit(1).execute()
            return True
        except Exception as e:  # noqa: BLE001
            if not _is_db_warming(e):
                return True  # a real error — let the job hit it and report it properly
            if attempt == _DB_WARMUP_ATTEMPTS:
                _log.warning(
                    "[scheduler] %s: the database was still warming up after %s attempt(s) "
                    "(%s) — standing down; the daily tick is the fallback.",
                    what, _DB_WARMUP_ATTEMPTS, e,
                )
                return False
            _log.info(
                "[scheduler] %s: database still warming up (%s), retrying in %ss [%s/%s]",
                what, getattr(e, "code", type(e).__name__),
                _DB_WARMUP_DELAY_SECONDS, attempt, _DB_WARMUP_ATTEMPTS,
            )
            time.sleep(_DB_WARMUP_DELAY_SECONDS)
    return False


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
                        # ⚠ THE PREFIX IS A SHARED CONSTANT — the automatic-jobs overview matches on
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
    work — so an env that was down across the 05:00 UTC tick (or freshly
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
    # ⚠ "NO REASONS" AND "WE COULD NOT TELL" ARE NOT THE SAME ANSWER. Both probes below run on
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
                "failed (database still booting?); not firing. The 05:00 UTC tick is the fallback.",
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

    ⚠⚠ IT REPLACED A GATED MONTH-END TICK, AND THE GATE IS THE PART THAT IS GONE. The old
    `_fire_month_end_refresh` woke daily and did nothing on ~28 days out of 30, then fired one full
    pass inside a window — with a "did it already run this window" guard, a retry-tomorrow path and
    a fallback that preferred firing when the guard lookup failed. All of that machinery existed to
    make ONE run a month land reliably, which is a hard thing to do; doing a thirtieth of the work
    every day is not, and needs none of it. No window, no guard, no retry logic: if a day is
    missed, the next day's slice is simply more stale and picks up the same names, because
    `_load_all_companies()` is most-stale-first and the database's own staleness is the cursor.

    ⚠ Never raises into the scheduler thread.
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
    """⚠ THE EXISTING BODY, UNCHANGED — it already owns its own `record_run` (it has three distinct
    `skipped` outcomes to report) and its own try/except. Wrapping it again would open a second run
    row for one run, so `_run_body` skips its own record for this one."""
    _run_asset_price_refresh("manual run")
    return "see the run record", {}


#: EVERY DECLARED JOB'S BODY, callable twice: by its scheduler tick and by the Run-now button.
#:
#: ⚠⚠ ONE BODY, TWO CALLERS — the rule this codebase applies to every scraper and pipeline. A button
#: that ran a second implementation would drift from the thing the schedule actually runs, and the
#: drift would only ever be discovered by the button disagreeing with the nightly result.
#:
#: ⚠ A JOB WITH NO ENTRY IS NOT RUNNABLE BY HAND, and that is an answer rather than a gap: the
#: 20-second queue worker has nothing to trigger (it fires three times a minute), and the two
#: pipeline jobs already have their own richer Run-now with a live console tail in their panels.
JOB_BODIES: dict = {}


def _reporter(ctx):
    """`(done, total, message)` → the job's progress stream, or nowhere on a scheduler tick.

    ⚠ A REPORTER MUST NEVER BE THE REASON A SCAN FAILS — the rule `airs_vermogen._step` already
    states. The work is the scan; the line on screen is a courtesy, and a listener that raised
    would lose a refresh that had already downloaded everything.

    ⚠ `ctx is None` ON THE TICK, which is not a degraded mode: nobody is watching a 09:30 cron, and
    the run record carries the outcome either way.
    """
    def step(done, total, message) -> None:
        if ctx is None:
            return
        try:
            ctx.progress(int(done or 0), int(total or 0), str(message)[:300])
        except Exception:  # noqa: BLE001
            _log.debug("[scheduler] progress listener raised", exc_info=True)
    return step


class _Cancelled(Exception):
    """Raised by a body that stopped because Cancel was pressed. Translated to the registry's own
    `JobCancelled` by `_run_body`, so a body never has to import the jobs module."""


def _run_body(job_id: str, ctx=None, triggered_by: str = "auto") -> str:
    """Run one declared job's body inside its run record — the ONE path for both callers."""
    import jobs as _jobs  # noqa: PLC0415

    body = JOB_BODIES[job_id]
    # ⚠ THE ASSET REFRESH RECORDS ITSELF (three `skipped` outcomes of its own), so a second record
    # here would open two rows for one run.
    if job_id == "asset_price_refresh":
        body(ctx)
        return "done"
    stopped: str | None = None
    with record_run(job_id, triggered_by=triggered_by) as rec:
        try:
            detail, summary = body(ctx)
        except _Cancelled as e:
            # ⚠⚠ CAUGHT *INSIDE* THE `with`, NOT OUTSIDE IT. Letting it propagate through
            # `record_run` closes the row as `error` — measured: a cancelled probe wrote
            # `status=error, detail=_Cancelled: stopped at step 19` while the toast correctly said
            # `cancelled`. Two records of one event, disagreeing, and the durable one was the wrong
            # one. Setting the status here and re-raising after the block leaves the row honest.
            rec.status = "cancelled"
            rec.detail = str(e)
            stopped = str(e)
        else:
            rec.done(detail, **{k: v for k, v in (summary or {}).items() if v is not None})
    if stopped is not None:
        raise _jobs.JobCancelled(stopped)
    return detail


def _spawn_body(job_id: str) -> None:
    """The scheduler tick: run the body on a daemon thread, never raising into APScheduler."""
    def _go() -> None:
        try:
            _run_body(job_id)
        except Exception as e:  # noqa: BLE001
            _log.exception("[scheduler] %s failed: %s: %s", job_id, type(e).__name__, e)
    threading.Thread(target=_go, daemon=True, name=job_id.replace("_", "-")).start()


def start_job_now(job_id: str, *, triggered_by: str = "manual"):
    """Kick a declared job off by hand, as a CANCELLABLE registry job with a progress toast.

    ⚠ THE SAME BODY THE TICK RUNS, through `_run_body` — so "Run now" cannot come to mean something
    different from what the schedule does.

    ⚠ `triggered_by` DEFAULTS TO `manual` BECAUSE THAT IS WHO CALLS IT — a button. The watchdog
    passes `watchdog` so its own re-runs are countable in the history, which is where its per-day
    cap now reads its budget from (see `_watchdog_budget_spent`); folded into `manual` a hand-run
    would silently spend the automatic allowance and vice versa.

    ⚠ CANCELLATION IS COOPERATIVE AND ITS LATENCY DIFFERS PER JOB, which the UI states rather than
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

    return _jobs.start(f"scheduled.{job_id}", label, _work)


def job_health(now=None) -> dict:
    """The three-way join the automatic-jobs page renders: declared vs registered vs actually ran.

    ⚠⚠ IT LIVES HERE SO THE WATCHDOG AND THE PAGE CANNOT DISAGREE. This assembly used to be a
    closure inside the admin endpoint. A self-healing tick needs the same verdict, and a second
    copy of "is this job overdue" is the one thing that must not exist: the page would say `ok`
    while the watchdog re-fired, or the reverse, and the surface built to tell you what is wrong
    would be wrong about itself.

    ⚠ THE PURE PART STAYS PURE. `_scheduled_jobs_status` takes every input and reads no clock and
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
    # ⚠ READ FROM THE BODY REGISTRY, NOT ASSUMED PER ROW. A "Run now" rendered for a job with no
    # body is a control that 404s on press — and a watchdog that fires one raises `KeyError`.
    runnable = set(JOB_BODIES)

    def _runs() -> list[dict]:
        # ⚠⚠ THE NEWEST ROW **PER JOB**, NEVER A WINDOW FILTERED CLIENT-SIDE. A windowed
        # `.limit(500)` is filled by the noisy jobs and pushes the quiet ones off the end, so it
        # accuses exactly the jobs that are behaving.
        out: list[dict] = []
        for name in evidence_names(specs):
            out += (supabase.table("ingest_run")
                    .select("job_name,started_at,finished_at,status,error_summary")
                    .eq("job_name", name)
                    .order("started_at", desc=True)
                    .limit(1).execute().data or [])
        # ⚠ NORMALISED ONTO `job_name` SO THE JOIN HAS ONE KEY SPACE. `scheduled_job_run` is keyed
        # by the APScheduler job id and `ingest_run` by a pipeline job_name.
        for spec in specs:
            if not spec.records:
                continue
            for row in (supabase.table("scheduled_job_run")
                        .select("job_id,started_at,finished_at,status,detail,summary")
                        .eq("job_id", spec.id)
                        .order("started_at", desc=True)
                        .limit(1).execute().data or []):
                out.append({**row, "job_name": row.pop("job_id")})
        return out

    err = None
    try:
        runs = _runs()
    except APIError as e:
        runs, err = [], f"{type(e).__name__}: {e}"
    rows = build_rows(specs, registered, runs, now, scheduler_running=running, runnable=runnable)
    return {"rows": rows, "running": running, "now": now, "history_error": err}


#: What the watchdog will re-run by itself, and nothing else.
#:
#: ⚠⚠ `missing` IS DELIBERATELY ABSENT AND IS THE MOST TEMPTING ONE. It means the job is not
#: REGISTERED — `add_job` threw, or the whole scheduler is down — and firing the body by hand makes
#: the page go green while the schedule stays broken. That is the single failure this monitoring
#: surface exists to catch, and auto-healing it would delete the evidence.
#:
#: ⚠ `error` IS ABSENT TOO. It has a recorded reason and a blind re-run is far likelier to repeat
#: it than to fix it; a job that fails every night should be read, not retried. `interrupted` and
#: `overdue` are the two where "run it again" IS the fix — the first is a deploy or an OOM landing
#: mid-run, the second is nothing having completed in the job's own allowance.
#:
#: ⚠ `unknown` IS ABSENT because we cannot tell whether it ran; re-running on no evidence is how a
#: quota gets spent twice.
#:
#: ⚠⚠ `missed` JOINED THEM ON 2026-09-01 AND IT IS THE CLEAREST MEMBER OF THE SET. The other two are
#: inferences from silence; this one is a recorded fact that the tick never ran — either dropped
#: past its grace or never fired because nothing was alive — so "run it again" is not merely the
#: likely fix, it is the only thing that was ever missing. It is also the opposite of `error`: there
#: is no failure to repeat, because nothing was attempted.
_WATCHDOG_HEALS: frozenset[str] = frozenset({"overdue", "interrupted", "missed"})

#: HOW THE WATCHDOG FIRES A JOB THAT HAS NO `JOB_BODIES` ENTRY.
#:
#: ⚠⚠ `JOB_BODIES` MEMBERSHIP WAS DOING TWO JOBS AND THEY HAD QUIETLY DIVERGED. It decides whether
#: the overview renders a generic "Run now" — deliberately NOT for these two, which own a richer
#: button with a live console tail inside their expanded row (`jobPanels.JOB_PANELS`) — and it also
#: decided what the watchdog could re-run. So the two jobs the watchdog was BUILT for (measured
#: 2026-08-18 at 7.1 days for `daily_pipeline`, 18 for the month-end refresh; still 21.0 and 31.8
#: days on 2026-09-01) were the two it skipped, reporting them as `unrunnable` and moving on. The
#: presentation question and the capability question are now separate.
#:
#: ⚠ THE VALUE IS THE TICK CALLABLE ITSELF, so an automatic re-run is byte-for-byte what the
#: schedule does. A second path into the pipeline is the one thing that must not exist here.
#:
#: ⚠ NO TOAST AND NO CANCEL, unlike `start_job_now` — these fire their own daemon threads and
#: narrate into `ingest_run`, which is where the /schedule panels already watch them. Wrapping them
#: in a registry job would put a second progress surface on a run that already has one.
#:
#: ⚠ `asset_ingest_queue` IS ABSENT AND STAYS ABSENT. A 20-second interval worker cannot be
#: "overdue" in any sense worth healing, and it is excluded from the gap scan for the same reason.
#: ⚠ FILLED BY `_register_bodies` AT THE BOTTOM OF THE MODULE, exactly as `JOB_BODIES` is and for
#: exactly the same reason: the tick callables are defined throughout this file beside the schedules
#: they belong to, so naming them here would be forward references to functions that do not exist
#: yet. A string-and-`globals()` lookup would dodge that and turn a typo into a runtime KeyError
#: inside the watchdog — the one place a mistake is least likely to be noticed.
_WATCHDOG_STARTERS: dict[str, object] = {}


#: Auto re-runs allowed per job per UTC day.
#:
#: ⚠⚠ THE CAP IS THE WHOLE SAFETY STORY. Without it a job that fails for a structural reason — no
#: GuruFocus quota left, AIRS credentials rotated — is re-fired on every tick for ever, which turns
#: one broken job into a machine that spends the day retrying it. Two is enough to ride out a
#: transient (a deploy, a blip) and low enough that a genuine fault stays a fault someone reads.
#: Same shape as the price-update retry's own `max 3/UTC-day`.
_WATCHDOG_MAX_PER_DAY = 2

#: ⚠⚠ THE IN-PROCESS HALF OF THE CAP, WHICH ON ITS OWN WAS A GUARD THAT EVAPORATED IN THE ONE
#: SCENARIO IT EXISTED FOR. A dict dies with the process, and the commonest reason the watchdog has
#: work to do is a host that keeps RESTARTING — so every restart reset the budget to zero and the
#: cap could never see what it had already spent. Harmless while the only caller was an 11:00 tick
#: in a long-lived process; not harmless now that a BOOT runs the sweep (`_boot_gap_pass`), which
#: is by definition the moment the counter is empty. It is kept as the cheap first check — a
#: process that has already fired twice this hour needs no query to know it — and the durable count
#: below is the one that binds.
_watchdog_fired: dict[tuple[str, str], int] = {}


def _watchdog_budget_spent(job_id: str, today: str) -> bool:
    """Whether this job has already used its re-run budget today, counting ACROSS processes.

    ⚠ THE DATABASE IS THE AUTHORITY AND A FAILED READ SPENDS NOTHING. `watchdog_runs_today` returns
    None when it could not count, and that is treated as "cap reached": a watchdog which cannot
    verify its own budget must not spend it, or a Supabase blip becomes the trigger for re-firing
    the whole fleet.

    ⚠ THE HIGHER OF THE TWO COUNTS WINS. The in-process tally can lead the database by a moment
    (a re-run started seconds ago may not have its row yet), and the durable one leads after a
    restart. Taking the max means neither blind spot opens the gate.
    """
    from job_runlog import ingest_runs_today, watchdog_runs_today  # noqa: PLC0415

    local = _watchdog_fired.get((job_id, today), 0)
    spec = BY_ID.get(job_id)
    if job_id in _WATCHDOG_STARTERS and spec is not None:
        # ⚠⚠ A JOB WITH NO `scheduled_job_run` ROW NEEDS A DIFFERENT MEASURING STICK, AND WITHOUT
        # ONE THE CAP WOULD SIMPLY NEVER BIND. `watchdog_runs_today` counts rows tagged `watchdog`
        # in a table these two never write to — it would return 0 for ever, and a host in a restart
        # loop would re-fire the pipeline on every boot with a guard that could not see it had.
        # Their own `ingest_run` rows are the countable thing; see `ingest_runs_today` for why
        # counting ALL of them (not just the watchdog's) is the stricter and correct choice here.
        durable = ingest_runs_today(spec.evidence, today)
    else:
        durable = watchdog_runs_today(job_id, today)
    if durable is None:
        return True
    return max(local, durable) >= _WATCHDOG_MAX_PER_DAY


def _fire_job_watchdog() -> None:
    """The watchdog tick — see `_body_job_watchdog`."""
    _spawn_body("job_watchdog")


def _body_job_watchdog(ctx=None) -> tuple[str, dict]:
    """Re-run the jobs the automatic-jobs page is already reporting as broken.

    ⚠⚠ THE PAGE KNEW AND NOTHING ACTED ON IT. `/schedule` has been computing `overdue` and
    `interrupted` per job for months — an honest three-way join between what is declared, what is
    registered and what actually ran — and the only consumer was a human reading it. A daily
    pipeline that dies mid-run therefore stays dead until somebody notices: measured in production
    2026-08-18 at 7.1 days for `daily_pipeline` and 18 for the month-end refresh, both with a
    perfectly healthy `next run` beside them, because the TICK was firing and the WORK was not
    finishing.

    ⚠ IT RE-RUNS, IT DOES NOT DIAGNOSE. The two states it heals are the two where "run it again" is
    genuinely the fix. See `_WATCHDOG_HEALS` for why `missing`, `error` and `unknown` are not on
    that list — each of them would have the watchdog erase the evidence rather than the fault.

    ⚠ IT CANNOT MAKE A JOB THAT LEGITIMATELY DOES NOTHING TODAY REPORT A FRESH SUCCESS. The
    month-end refresh acts only in the last days of the month; re-running it on the 12th is a
    no-op, so it will still read `interrupted` afterwards. That is a true statement about the job
    and not a watchdog failure — which is exactly why the cap exists, so it says it twice and stops
    rather than every hour for a fortnight.

    ⚠ ONE AT A TIME, THROUGH `start_job_now` — the same body the tick runs, as a cancellable
    registry job with a toast, so an automatic re-run is visible in the same place a manual one is
    and can be stopped the same way.

    ⚠⚠ EXCEPT THE TWO PIPELINE JOBS, WHICH IT COULD NOT TOUCH AT ALL UNTIL 2026-09-01 AND WHICH ARE
    THE TWO IT WAS BUILT FOR. They have no `JOB_BODIES` entry — deliberately, because they own a
    richer Run-now with a live console tail in their own expanded row — and that same membership
    was gating what this could re-run, so `daily_pipeline` and `daily_price_slice` landed in
    `unrunnable` every sweep. Measured 2026-09-01: 21.0 and 31.8 days stale, both reported broken
    by the page this reads, both skipped by the code that reads it. `_WATCHDOG_STARTERS` separates
    the presentation question from the capability one; they fire through their own tick callable
    and are capped on their own `ingest_run` rows.
    """
    from datetime import datetime as _dt, timezone as _tz  # noqa: PLC0415

    step = _reporter(ctx)
    health = job_health()
    rows = health["rows"]
    if health["history_error"]:
        # ⚠ NO HISTORY MEANS NO VERDICT, AND A WATCHDOG WITHOUT ONE MUST DO NOTHING. Re-running
        # every job because the database was briefly unreachable is the opposite of self-healing.
        return (f"skipped — could not read the run history ({health['history_error']})",
                {"checked": 0, "restarted": 0})

    today = _dt.now(_tz.utc).date().isoformat()
    broken = [r for r in rows if r.get("status") in _WATCHDOG_HEALS]
    step(0, len(broken) or 1, f"{len(broken)} job(s) to re-run of {len(rows)} checked")

    restarted, capped, unrunnable = [], [], []
    for i, row in enumerate(broken, 1):
        jid = row["id"]
        if jid not in JOB_BODIES and jid not in _WATCHDOG_STARTERS:
            unrunnable.append(jid)
            continue
        if _watchdog_budget_spent(jid, today):
            capped.append(jid)
            continue
        key = (jid, today)
        _watchdog_fired[key] = _watchdog_fired.get(key, 0) + 1
        step(i, len(broken), f"re-running {jid} ({row.get('status')})")
        # ⚠ LOUD. uvicorn leaves the root logger at WARNING, so an INFO line here is invisible in
        # Railway — and "why did my pipeline run at 11:00" is a question the log has to answer.
        _log.warning("[watchdog] re-running %s — %s: %s", jid, row.get("status"),
                     row.get("why") or "")
        try:
            if jid in JOB_BODIES:
                # ⚠ TAGGED `watchdog`, WHICH IS WHAT MAKES THE CAP COUNTABLE. Left as `manual`
                # these rows would be indistinguishable from somebody pressing Run now, so the
                # budget could not be read back out of the history — and a hand-run would spend
                # the automatic budget.
                start_job_now(jid, triggered_by="watchdog")
            else:
                # ⚠ THE TICK CALLABLE ITSELF — see `_WATCHDOG_STARTERS`. It spawns its own daemon
                # thread and writes its own `ingest_run` rows, so there is nothing to await and
                # nothing to record here that the run does not record better.
                _WATCHDOG_STARTERS[jid]()
            restarted.append(jid)
        except Exception as e:  # noqa: BLE001 — one bad job must not stop the sweep
            _log.exception("[watchdog] could not start %s: %s: %s", jid, type(e).__name__, e)

    summary = {"checked": len(rows), "broken": len(broken), "restarted": len(restarted),
               "restarted_ids": restarted, "capped": capped, "unrunnable": unrunnable}
    if not broken:
        return f"all {len(rows)} job(s) healthy", summary
    msg = f"re-ran {len(restarted)}/{len(broken)}"
    if capped:
        msg += f" — {len(capped)} already retried {_WATCHDOG_MAX_PER_DAY}× today: {', '.join(capped)}"
    if unrunnable:
        msg += f" — {len(unrunnable)} have no body: {', '.join(unrunnable)}"
    return msg, summary


def scheduler_running() -> bool:
    """Whether THIS process has a live in-process scheduler.

    ⚠⚠ IT IS NOT `bool(list_scheduled_jobs())`, AND THAT IS THE WHOLE REASON IT EXISTS. An empty job
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

    threading.Thread(target=_seq, daemon=True, name="daily-pipeline").start()


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

    threading.Thread(target=_run, daemon=True, name="price-retry").start()


def maybe_schedule_price_retry(*, reason: str = "") -> None:
    """Schedule a one-shot held-price retry `_PRICE_RETRY_DELAY_HOURS` out when
    the enabled strategies' held prices are STILL stale after a price-update.

    Called at the end of every `price_update` op (daily tick, startup catch-up,
    AND manual Run-now) so any path that leaves held prices behind gets the
    auto-retry. No-op when prices are fresh, the scheduler is disabled
    (CI / DISABLE_SCHEDULER), or today's retry budget (`_PRICE_RETRY_MAX_PER_DAY`)
    is spent — at which point the next 05:00 UTC tick takes over. The retry
    re-runs the whole price-update, but the per-company freshness short-circuit
    in `_run_prices_phase` means only the actually-stale names hit GuruFocus.

    Best-effort — never raises into the caller."""
    try:
        if _scheduler is None or _PRICE_RETRY_MAX_PER_DAY <= 0:
            return
        # Publish-lag check (held name behind the GLOBAL latest close), NOT the
        # calendar-day `_held_prices_stale` the kickstart uses — otherwise the
        # normal "today's close isn't out yet" state would trigger a retry every
        # single day. See `held_prices_lagging`.
        from ingest.phases.prices import held_prices_lagging  # noqa: PLC0415
        if not held_prices_lagging():
            return
        today = datetime.now(timezone.utc).date().isoformat()
        with _price_retry_lock:
            used = _price_retry_counts.get(today, 0)
            if used >= _PRICE_RETRY_MAX_PER_DAY:
                _log.info(
                    "[scheduler] held prices still stale but today's retry budget "
                    "(%s) is spent — waiting for the next daily tick",
                    _PRICE_RETRY_MAX_PER_DAY,
                )
                return
            # Keep only today's key so the dict can't grow unbounded.
            _price_retry_counts.clear()
            attempt = used + 1
            _price_retry_counts[today] = attempt
        run_at = datetime.now(timezone.utc) + timedelta(hours=_PRICE_RETRY_DELAY_HOURS)
        _scheduler.add_job(
            _fire_price_update_retry,
            DateTrigger(run_date=run_at),
            id="price_update_retry",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=3600,
        )
        _log.warning(
            "[scheduler] held prices stale%s — retry %s/%s scheduled at %s",
            f" ({reason})" if reason else "", attempt, _PRICE_RETRY_MAX_PER_DAY,
            run_at.isoformat(),
        )
    except Exception as e:
        _log.warning(
            "[scheduler] maybe_schedule_price_retry failed: %s: %s",
            type(e).__name__, e,
        )


def _fire_asset_ingest_queue() -> None:
    """Drain one slice of the asset-pipeline ingest queue. THE single Yahoo/
    OpenFIGI consumer for uploaded ISINs — runs on a short interval with
    max_instances=1, so slices process back-to-back without ever overlapping (no
    competing Yahoo traffic → no throttle-corrupted resolutions). A no-op when the
    queue is empty. Never raises into the scheduler."""
    try:
        from asset_pipeline import queue as _q  # noqa: PLC0415
        r = _q.process_slice()
        if r.get("processed"):
            _log.info("[scheduler] asset ingest queue: %s processed (%s ok, %s failed, %s remaining)",
                      r.get("processed"), r.get("ok"), r.get("failed"), r.get("remaining"))
    except Exception:  # noqa: BLE001
        _log.exception("[scheduler] asset ingest queue worker failed")


def _fire_asset_price_refresh() -> None:
    """Daily refresh of STALE `asset_price` series for the HELD instruments.

    `metric_data` (GuruFocus) has had a daily refresh for ever; `asset_price` (Yahoo) never did —
    it was written when a row was added and then aged, silently. A stale series still returns
    prices and still charts; it is just old. Measured 2026-07-14: 197 of the 223 instruments held
    by the AIRS model portfolios were stale, and Meta Platforms — correctly mapped, 3,556 bars —
    rendered as a BLANK row in BUS_2.0_NEU_FX because its last close (2026-07-02) predated that
    portfolio's window (2026-07-09): no price inside the window, so no return over it exists.

    ⚠ STANDS DOWN WHILE A WORKER IS ACTUALLY DRAINING THE INGEST QUEUE. That queue is *the*
    single Yahoo consumer by design: Yahoo answers an overloaded caller with an EMPTY result
    rather than a 429, and an empty candidate set is how a resolution silently lands on a thin
    foreign listing (the NVDA-on-Stuttgart / Alphabet-on-Vienna class of bug). Our own traffic is
    only chart fetches for symbols we already hold, so it cannot mis-resolve anything ITSELF —
    but it can push Yahoo into that regime while the resolver is mid-search and corrupt ITS work.
    A day-late price is a nuisance; a wrong listing is a wrong price series for ever.

    ⚠ ...AND THAT MEANS THE WORKER, NOT THE BACKLOG. The first version gated on `pending > 0` and
    never ran once: the queue holds 9,945 pending ISINs last touched 2026-07-07, a week earlier —
    a stalled backlog, not active work (`status()["working"]` is just `pending > 0`, so it says
    "working" about a queue nobody is draining). `is_worker_active()` reads the real heartbeat:
    when a row was last MOVED out of pending.

    Scope is HELD-only (~220 instruments, not the 16k grid). Own daemon thread; never raises.

    Also fired ON STARTUP by `_maybe_kickstart_asset_prices` — see there for why a daily tick
    alone is not enough.
    """
    threading.Thread(
        target=_run_asset_price_refresh, args=("daily tick",),
        name="asset-price-refresh", daemon=True,
    ).start()


def _run_asset_price_refresh(trigger: str) -> None:
    """The body, shared by the 06:00 tick and the startup catch-up. Never raises.

    ⚠ THE THREE EARLY EXITS ARE `skipped`, NOT `ok` AND NOT FAILURES. Standing down for the queue
    worker, finding everything current, and waiting on a database that has not finished booting are
    all correct behaviour — but recording them as `ok` would make a job that stood down every single
    day for a fortnight (because a stalled worker looked live) indistinguishable from one doing its
    work. The overview can then show the reason.
    """
    # ⚠ `startup` IS ITS OWN TRIGGER, so the overview does not read a restart storm under
    # `uvicorn --reload` as the 06:00 tick having fired forty times.
    triggered = "startup" if "startup" in trigger else "auto"
    with record_run("asset_price_refresh", triggered_by=triggered) as rec:
      try:
        from asset_pipeline import price_refresh, queue as _q  # noqa: PLC0415

        # ⚠ FIRST, WAIT FOR THE DATABASE — this fires on startup, in parallel with the Supabase
        # stack coming up, and every query below (the queue heartbeat is simply the first) fails
        # with PGRST002 until PostgREST has loaded its schema cache. Losing the catch-up to a
        # boot race costs a whole day of stale held prices; see `_await_db_ready`.
        if not _await_db_ready(f"asset price refresh ({trigger})"):
            rec.skip("the database was not ready in time")
            return

        if _q.is_worker_active():
            _log.info(
                "[scheduler] asset price refresh (%s) SKIPPED — the ingest queue worker is live "
                "(last activity %s); adding Yahoo load now risks corrupting the listings it is "
                "resolving.", trigger, _q.last_activity(),
            )
            rec.skip(f"the ingest queue worker is live (last activity {_q.last_activity()})")
            return

        # Detect BEFORE fetching. A restart must not cost ~220 Yahoo calls just to discover
        # there was nothing to do — and with `--reload` in dev, restarts are constant. This is
        # a handful of queries (one grouped COPY), so the common case is a near-free no-op.
        stale, latest, considered = price_refresh.find_stale(held_only=True)
        if not stale:
            _log.info(
                "[scheduler] asset price refresh (%s): all %s held instrument(s) current as of "
                "%s — nothing to do", trigger, considered, latest,
            )
            rec.skip(f"all {considered} held instrument(s) current as of {latest}")
            rec.done(considered=considered, latest=str(latest), stale=0)
            return

        # WARNING, not info: uvicorn leaves the ROOT logger at WARNING, so an `info` here is
        # invisible in production — and "we found the held prices stale and are refetching them"
        # is exactly the line you want in the deploy log. `_maybe_kickstart_smart` logs its own
        # firing at warning for the same reason. The no-op case above stays at info: a healthy
        # restart should be quiet.
        _log.warning(
            "[scheduler] asset price refresh (%s): %s of %s held instrument(s) stale vs %s "
            "(oldest %s) — fetching the gap",
            trigger, len(stale), considered, latest, stale[0]["last_close"],
        )
        r = price_refresh.refresh_stale(held_only=True)
        _log.warning(
            "[scheduler] asset price refresh (%s) done — %s moved, %s unchanged, %s failed",
            trigger, r["moved"], r["unchanged"], r["failed"],
        )
        rec.done(f"{r['moved']} moved, {r['unchanged']} unchanged, {r['failed']} failed",
                 considered=considered, stale=len(stale), moved=r["moved"],
                 unchanged=r["unchanged"], failed=r["failed"])
      except Exception as e:  # noqa: BLE001
        # ⚠ CAUGHT HERE, SO THE RECORD MUST BE SET BY HAND. `record_run` marks a run failed off the
        # exception PROPAGATING; swallowing it (which this must, to keep the scheduler thread
        # alive) would otherwise close the row as `ok`.
        rec.status = "error"
        rec.detail = f"{type(e).__name__}: {str(e)[:400]}"
        _log.exception("[scheduler] asset price refresh (%s) failed", trigger)


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
    threading.Thread(
        target=_run_asset_price_refresh, args=("startup catch-up",),
        name="asset-price-kickstart", daemon=True,
    ).start()


def _maybe_kickstart_airs_models() -> None:
    """On STARTUP: if we hold no model-portfolio COMPOSITIONS, scan them now.

    ⚠ THE DAILY TICK CANNOT FIX A DEPLOYMENT THAT HAS NEVER HAD THEM. `airs_model_portfolio_
    position` was only ever populated by a human pressing "Scan AIRS", so a fresh environment
    starts empty — and `_airs_account_links._models()` keeps only models WITH a composition, so
    every account matches nothing, loses its pairing, and Analyse falls back to an unpaired
    basket. Measured in production 2026-08-03 on `AITopSelectie OFF DYN`: "No valued positions to
    show", beside a portfolios list whose rows expand perfectly.

    Waiting for the next weekday-morning tick would leave that state up for as much as a
    long weekend after the deploy that was supposed to fix it, which is the same reasoning as
    `_maybe_kickstart_asset_prices`.

    ⚠ IT DETECTS BEFORE IT SCRAPES — ONE COUNT QUERY. The scan is minutes of authenticated
    Playwright, so it must not run on every restart: `uvicorn --reload` restarts constantly, and
    after the first successful scan the count is non-zero and this is a single round-trip no-op.
    Deliberately NOT a staleness check — refreshing an existing composition is the daily tick's
    job. This only fills a hole.

    ⚠ WARNING level when it acts. uvicorn leaves the root logger at WARNING, so an `info` line is
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
            count_model_portfolio_holdings_sync(
                rows, _quiet,
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

    threading.Thread(target=_run, name="airs-models-kickstart", daemon=True).start()


def _fire_history_drift_check() -> None:
    """Daily: probe 1/5th of the universe for a vendor rewrite of PAST bars.

    ⚠ THE ONE FAILURE THE PIPELINE IS BLIND TO. Prices are only ever appended
    (`d > existing_max`), so a split or a free-share attribution leaves our
    history on the old basis indefinitely — Worldline sat in the live book on a
    +1142% momentum for a stock that had fallen 69%.

    Cheap because of the undocumented `?start_date=&end_date=` filter: a probe is
    ~23 bytes against a 268 KB full series. It is NOT cheap on quota (requests are
    what's metered, and a probe costs the same one as a full fetch), which is why
    it walks a fifth of the universe a day — every name inside a week at ~300
    requests/day — instead of all of it.

    Own daemon thread; never raises into the scheduler."""
    _spawn_body("history_drift_check")


def _body_history_drift(ctx=None) -> tuple[str, dict]:
    """⚠ CANCEL LANDS AT THE NEXT PROBE. `on_step` fires per company, so this is one of the bodies
    that CAN stop quickly — it walks a fifth of the universe and each probe is one request."""
    from ingest.history_drift import daily_drift_check  # noqa: PLC0415

    seen = {"n": 0}
    report = _reporter(ctx)

    def _step(m, lvl) -> None:
        (_log.warning if lvl in ("warn", "error") else _log.info)("[drift] %s", m)
        # ⚠ NO TOTAL — the walk's size is decided inside `daily_drift_check` and never handed out,
        # so the bar stays indeterminate and the LINE carries the progress. A fabricated total
        # would give a bar that reaches 100% and keeps going.
        seen["n"] += 1
        report(seen["n"], 0, m)
        # ⚠ RAISED, NOT RETURNED. `daily_drift_check` has no stop hook, so the only way out is to
        # unwind it from the callback it does have — at a per-company boundary, where the last
        # company is either fully refetched or untouched.
        if ctx is not None and getattr(ctx, "cancelled", False):
            raise _Cancelled(
                f"stopped after {seen['n']} probe(s); everything refetched so far is kept")

    res = daily_drift_check(on_step=_step)
    drifted = res.get("drifted") or []
    if drifted:
        _log.warning("[scheduler] history drift: %s of %s probed companies had rewritten "
                     "history and were refetched", len(drifted), res.get("probed"))
    # ⚠ FINDING NOTHING IS THE JOB WORKING, NOT THE JOB SKIPPING. Most days no vendor rewrote
    # anything, and that is `ok` with a count of zero — the number is the point, because a probe
    # count that quietly falls to 0 means the WALK stopped, which looks identical to a clean week
    # if only the drift count is recorded.
    return (f"{len(drifted)} drifted of {res.get('probed')} probed",
            {"probed": res.get("probed"), "drifted": len(drifted),
             "names": [str(d) for d in drifted][:20]})


# Which rebuilt indices the daily job refreshes.
#
# ⚠⚠ ONLY THE ONES WITHOUT A REACHABLE ETF. ACWI and SP500 take their headline from the index ETF's
# own price series (`routers/_benchmark_etf`), refreshed inside the 05:00 price_update in two calls.
# Rebuilding their 1,684 and 491 constituents daily would be thousands of paced Yahoo calls to
# re-derive a number we already hold — and the rebuild is the LESS accurate of the two (full market
# cap where MSCI float-adjusts, 84% coverage renormalised across the rest). AEX has no reachable
# UCITS line on GuruFocus, so it IS the rebuild, and its 25 constituents are the entire cost.
#
# ⚠ Adding a label here is a real daily Yahoo budget, not a config tweak. Check the constituent
# count first.
_REBUILT_INDICES: tuple[str, ...] = ("AEX",)


def _fire_benchmark_index_refresh() -> None:
    """Daily constituent refresh for the indices we REBUILD rather than read off an ETF.

    ⚠⚠ NOTHING ELSE COVERS THESE. `asset_price_refresh` is scoped to instruments HELD IN A MODEL
    PORTFOLIO, so an index constituent no book holds was refreshed by nothing at all — which is why
    a rebuilt index could sit on months-old closes while every dashboard around it was current, and
    say nothing about it. Own daemon thread; never raises into the scheduler."""
    _spawn_body("benchmark_index_refresh")


def _body_benchmark_index_refresh(ctx=None) -> tuple[str, dict]:
    """Constituents → market caps → two prices each, per label. Returns a one-line summary.

    ⚠ ONE LABEL'S FAILURE IS NOT THE JOB'S. Each is independent — a Yahoo wobble on one index must
    not cost the others their refresh, and the summary names whichever fell over.
    """
    from routers._benchmark_refresh import refresh_benchmark  # noqa: PLC0415

    done: list[str] = []
    failed: list[str] = []
    for label in _REBUILT_INDICES:
        try:
            # ⚠ THE SAME FUNCTION THE BUTTON RUNS. A second "scheduled" path would be a second
            # definition of what refreshing an index means, and the two would drift.
            summary = refresh_benchmark(
                label,
                # `emit` is the SSE sender for the interactive path; here the steps go to the log.
                lambda _t, **kw: _log.info("[benchmark_index_refresh] %s", kw.get("message", "")),
                should_stop=(lambda: bool(ctx and ctx.cancelled())) if ctx else None,
            )
            done.append(f"{label} ({(summary or {}).get('priced', '?')} priced)")
        except Exception as e:  # noqa: BLE001 — one label must not take the others down
            _log.warning("[benchmark_index_refresh] %s failed: %s: %s", label, type(e).__name__, e)
            failed.append(f"{label}: {type(e).__name__}")
    msg = f"refreshed {', '.join(done) or 'nothing'}"
    if failed:
        msg += f" · failed {', '.join(failed)}"
    return msg, {"refreshed": len(done), "failed": len(failed)}


#: The universes whose 12-1 returns are ranked into the seven relative-momentum states.
#:
#: ⚠ THE SAME THREE THE ANALYSE MODAL OFFERS AS BENCHMARKS, and that is the requirement, not a
#: coincidence: `_holding_risk` ranks a holding against WHICHEVER benchmark the reader picked, so a
#: label missing here is a picker option whose momentum column silently loses its chip.
_RANKED_UNIVERSES = ("ACWI", "SP500", "AEX")

#: Coverage below this is reported as a WARNING rather than an info line.
#:
#: ⚠ IT IS A PRICE-STALENESS ALARM WEARING A COVERAGE THRESHOLD. The signal engine drops any name
#: whose last close is over 30 days old, so if constituent prices stop being refreshed the first
#: visible symptom is this number falling — not an error, not an empty page. Measured healthy:
#: ACWI 87.6%, SP500 99.2%, AEX 88.0%.
_RANK_COVERAGE_WARN = 70.0


def _fire_relative_momentum_refresh() -> None:
    """Re-rank each benchmark universe's 12-1 momentum for the newest closes we hold.

    ⚠ 07:00 UTC, AFTER `price_update` (05:00) and `benchmark_index_refresh` (06:30). A rank is a
    statement about a set of prices, so computing it before the day's prices land would stamp
    today's date on yesterday's closes. Own daemon thread; never raises into the scheduler.
    """
    _spawn_body("relative_momentum_refresh")


def _body_relative_momentum_refresh(ctx=None) -> tuple[str, dict]:
    """Compute + persist one slice per universe. Returns a one-line summary.

    ⚠ ONE UNIVERSE'S FAILURE IS NOT THE JOB'S, the same rule as the index refresh beside it: a
    universe with no members or a bad price load must not cost the other two their ranks.

    ⚠⚠ IT RANKS AS OF THE NEWEST CLOSE WE HOLD, NEVER `today`. Today is routinely a date we have no
    prices for — a weekend, a holiday, a pipeline that has not run — and asking for it would rank
    an empty set or drop every name on the staleness rule. This is the same date /backtest uses for
    its default end.
    """
    from momentum import relative  # noqa: PLC0415
    from routers.momentum._helpers import latest_db_price_date  # noqa: PLC0415

    as_of = latest_db_price_date()
    if as_of is None:
        # ⚠ NOT AN ERROR. No closes at all is a statement about the price pipeline, not about this
        # job, and failing here would point an operator at the wrong thing.
        return "no close prices held — nothing to rank", {"ranked": 0, "skipped": True}

    done: list[str] = []
    failed: list[str] = []
    thin: list[str] = []
    for label in _RANKED_UNIVERSES:
        if ctx and ctx.cancelled():
            break
        try:
            result = relative.compute(
                label, as_of,
                on_step=lambda m, _l=label: _log.info("[relative_momentum] %s: %s", _l, m),
            )
            relative.persist(result)
            cov = result.coverage_pct
            done.append(f"{label} {result.universe_n}/{result.members_total}")
            if cov < _RANK_COVERAGE_WARN:
                # ⚠ WARNING, because uvicorn leaves the root logger at WARNING in production and an
                #   `info` here would be invisible exactly when it matters.
                thin.append(f"{label} {cov:.0f}%")
                _log.warning("[relative_momentum] %s coverage %.1f%% (%d of %d) — constituent "
                             "closes may have stopped refreshing", label, cov,
                             result.universe_n, result.members_total)
        except Exception as e:  # noqa: BLE001 — one universe must not take the others down
            _log.warning("[relative_momentum] %s failed: %s: %s", label, type(e).__name__, e)
            failed.append(f"{label}: {type(e).__name__}")

    msg = f"ranked as of {as_of} — {', '.join(done) or 'nothing'}"
    if thin:
        msg += f" · ⚠ thin coverage: {', '.join(thin)}"
    if failed:
        msg += f" · failed {', '.join(failed)}"
    return msg, {"as_of": as_of.isoformat(), "ranked": len(done),
                 "failed": len(failed), "thin": len(thin)}


# The indices whose constituents get a quarterly fundamentals pass.
#
# ⚠ ALL THREE, unlike `_REBUILT_INDICES`. Fundamentals are not prices: the ETF's own series answers
# "what did ACWI return", and answers nothing at all about its constituents' margins — which is the
# whole of the Long Equity tab and the fundamentals grid. Those exist only per company.
_FUNDAMENTAL_INDICES: tuple[str, ...] = ("ACWI", "SP500", "AEX")

# ⚠ THE FLOOR IS NOT ZERO. Stopping at 0 spends a region's last call and leaves the month-end
# `full_price_refresh` — the job that keeps every price series alive — with nothing. A reserve is
# the cheap way to make this job the one that yields.
_FUNDAMENTALS_REGION_FLOOR = 2000


class _LogCtx:
    """The `ctx` `fill_company_ids` expects, for a run with nobody watching.

    ⚠ THE TICK HAS NO JOB CONTEXT. Bodies are handed `ctx=None` on a schedule (see `_reporter`) and
    `fill_company_ids` calls `ctx.emit(...)` throughout — so a tick would die on its first narration
    line having done nothing, and the failure would read as a fundamentals problem rather than a
    plumbing one. This turns those lines into log lines.
    """

    def __init__(self, cancelled=None):
        self._cancelled = cancelled or (lambda: False)
        self.counts: dict[str, int] = {}

    def emit(self, kind: str, message: str = "", **_kw) -> None:
        self.counts[kind] = self.counts.get(kind, 0) + 1
        # ⚠ WARNING for the milestones, not INFO. uvicorn leaves the root logger at WARNING in
        # production, so an info line from a QUARTERLY job is invisible exactly where someone asks
        # "did it run?" — and the next chance to find out is three months away.
        if kind in ("start", "error", "done"):
            _log.warning("[benchmark_fundamentals] %s: %s", kind, message)
        else:
            _log.info("[benchmark_fundamentals] %s: %s", kind, message)

    def cancelled(self) -> bool:
        return bool(self._cancelled())


def _fire_benchmark_fundamentals() -> None:
    """Quarterly fundamentals pass over every benchmark constituent. Own daemon thread."""
    _spawn_body("benchmark_fundamentals_fill")


def _fundamental_company_ids(label: str) -> list[tuple[int, str]]:
    """`[(company_id, gurufocus exchange_code)]` for one index — the GuruFocus side of it.

    ⚠⚠ NOT `_asset_benchmark.members()`. That returns the ASSET world, whose `company_id` slot
    actually carries an `analysis_id` (its own docstring says so) — handing those to a fundamentals
    fill would look up entirely unrelated companies and quietly fill the wrong ones. Fundamentals
    are keyed on the GuruFocus company.

    ⚠ THE EXCHANGE RIDES ALONG because the quota is PER REGION, and the region is a property of the
    listing. Without it the budget gate could only be all-or-nothing.
    """
    from deps import IN_CHUNK_SIZE, supabase  # noqa: PLC0415

    uni = (supabase.table("universe").select("universe_id")
           .eq("label", label).limit(1).execute().data or [])
    if not uni:
        return []
    ids: set[int] = set()
    off = 0
    while True:  # ⚠ PAGED — 1,998 members for ACWI against PostgREST's 1,000-row cloud cap.
        rows = (supabase.table("universe_membership").select("company_id")
                .eq("universe_id", uni[0]["universe_id"]).order("company_id")
                .range(off, off + 999).execute().data or [])
        if not rows:
            break
        ids.update(r["company_id"] for r in rows)
        off += len(rows)
    out: list[tuple[int, str]] = []
    ordered = sorted(ids)
    for i in range(0, len(ordered), IN_CHUNK_SIZE):
        for c in (supabase.table("company")
                  .select("company_id,gurufocus_exchange:gurufocus_exchange(exchange_code)")
                  .in_("company_id", ordered[i:i + IN_CHUNK_SIZE]).execute().data or []):
            out.append((c["company_id"],
                        ((c.get("gurufocus_exchange") or {}) or {}).get("exchange_code") or ""))
    return out


def _body_benchmark_fundamentals(ctx=None) -> tuple[str, dict]:
    """Fill statements for every benchmark constituent, bounded by the monthly GuruFocus quota.

    ⚠⚠ THE BUDGET GATE IS THE POINT, and it is the shape `full_price_refresh` already uses: read
    the per-region remaining and DROP the companies whose region is at the floor, rather than
    calling and failing. An exhausted region does not refuse politely — it returns errors that read
    as data problems, and a job that discovers its quota by exhausting it takes the month-end price
    refresh down with it.

    ⚠ DECIDED BEFORE ANY CALL, PER REGION. A pass that started and stopped halfway would leave an
    index part-filled with no record of where it got to; budgeting up front lets the summary say
    what was deferred, and `only_due=True` means next quarter picks up exactly those.

    ⚠ ONE INDEX'S FAILURE IS NOT THE JOB'S — they are independent, and the summary names whichever
    fell over.
    """
    from deps import supabase  # noqa: PLC0415
    from ingest.api_usage import _region_for_exchange, remaining_budget  # noqa: PLC0415, PLC2701
    from routers._fundamental_fill import fill_company_ids  # noqa: PLC0415

    budget = remaining_budget(supabase)
    room = {r: max(0, n - _FUNDAMENTALS_REGION_FLOOR) for r, n in budget.items()}
    _log.warning("[benchmark_fundamentals] quota left %s — usable above the %s floor: %s",
                 budget, _FUNDAMENTALS_REGION_FLOOR, room)

    log_ctx = _LogCtx(lambda: bool(ctx and ctx.cancelled()))
    filled = 0
    deferred = 0
    failed: list[str] = []
    for label in _FUNDAMENTAL_INDICES:
        try:
            pairs = _fundamental_company_ids(label)
        except Exception as e:  # noqa: BLE001
            _log.warning("[benchmark_fundamentals] %s: could not read membership: %s: %s",
                         label, type(e).__name__, e)
            failed.append(f"{label}: {type(e).__name__}")
            continue
        keep: list[int] = []
        for cid, exch in pairs:
            region = _region_for_exchange(exch)
            if room.get(region, 0) <= 0:
                deferred += 1
                continue
            room[region] -= 1
            keep.append(cid)
        if not keep:
            _log.warning("[benchmark_fundamentals] %s: no quota left for any of its regions", label)
            continue
        try:
            fill_company_ids(log_ctx, label, keep, feeds="statements", only_due=True)
            filled += len(keep)
        except Exception as e:  # noqa: BLE001
            _log.warning("[benchmark_fundamentals] %s failed: %s: %s", label, type(e).__name__, e)
            failed.append(f"{label}: {type(e).__name__}")

    msg = f"{filled} constituents filled"
    if deferred:
        msg += (f" · {deferred} deferred to next quarter "
                f"(region at the {_FUNDAMENTALS_REGION_FLOOR}-call floor)")
    if failed:
        msg += f" · failed {', '.join(failed)}"
    return msg, {"filled": filled, "deferred": deferred, "failed": len(failed)}


def _fire_fx_sync() -> None:
    """Daily ECB FX sync — keeps EVERY fetchable currency's `fx_rate` current.
    The daily pipeline only syncs the currencies the held strategies actually
    use (as a side effect of the momentum backtest stream), so unused ACWI
    currencies would otherwise go stale on the /fx-rates page. Idempotent +
    cheap: `sync_fx_rates_to_db` fetches only the gap since each currency's last
    stored date. Own daemon thread; never raises into the scheduler."""
    _spawn_body("fx_sync")


def _body_fx_sync(ctx=None) -> tuple[str, dict]:
    """⚠ NOT CANCELLABLE MID-RUN, and `ctx` is accepted only so every body has one shape. The whole
    sync is a handful of ECB requests over seconds; there is no boundary worth checking, and a
    partial FX table is worse than a complete one (a missing rate silently drops a holding from its
    portfolio — see `_fx`'s paging note)."""
    from datetime import date as _date  # noqa: PLC0415

    from deps import supabase  # noqa: PLC0415
    from fx_rates import ECB_CURRENCIES, _USD_PEGS  # noqa: PLC0415
    from momentum.data import sync_fx_rates_to_db  # noqa: PLC0415

    currencies = list(ECB_CURRENCIES) + list(_USD_PEGS.keys()) + ["TWD"]
    status = sync_fx_rates_to_db(supabase, currencies, _date(2000, 1, 1), _date.today())
    synced = sum(1 for s in status.values() if s.get("status") == "synced")
    errors = sum(1 for s in status.values() if s.get("status") == "error")
    _log.info("[scheduler] fx sync done: %s/%s currencies updated, %s errors",
              synced, len(status), errors)
    # ⚠ A SYNC THAT UPDATED NOTHING IS THE NORMAL CASE, NOT A SKIP. It is idempotent and fetches
    # only the gap, so on a day the ECB has already been read every currency is correctly up to
    # date — `ok` with a count of 0, which is what makes a run of zeros over a WEEK legible as the
    # ECB feed having died.
    return (f"{synced}/{len(status)} currencies updated, {errors} error(s)",
            {"currencies": len(status), "synced": synced, "errors": errors})


def _fire_airs_model_prices() -> None:
    """The 05:00 tick: reprice every AIRS model portfolio. See `_body_airs_model_prices`."""
    _spawn_body("airs_model_prices")


def _body_airs_model_prices(ctx=None) -> tuple[str, dict]:
    """Bring every paired model portfolio's VALUATION current — composition, instruments, FX,
    prices, recompute — without touching the accounts.

    ⚠⚠ IT RUNS THE MODEL HALF AND ONLY THE MODEL HALF. `halves=("model",)` is not a tuning knob;
    it is what makes 05:00 a safe hour to run at. The account scrape may not run before AIRS has
    valued the books — see the two ⚠⚠ notes on `_fire_airs_vermogen` — and this job exists
    precisely because the half that CAN run early was the one nothing scheduled ever did.

    ⚠ THIS IS THE GAP THE 09:30 JOB LEAVES. That one scrapes the accounts and SCANS the model
    portfolios (their names and compositions); neither pass ever priced them. So a model's YTD
    moved only when a human opened /portfolios and pressed Refresh on that row — which is why the
    figures on the Analyse modal could sit weeks behind the book beside them.

    ⚠ ONE FUNCTION, THE SAME ONE THE BUTTONS CALL. `refresh_many` fans out over
    `refresh_portfolio_fully`; there is no scheduled copy of "refresh a portfolio" to drift from
    the interactive one, which is the mistake `scan_one`'s own docstring records having already
    been made one layer down.

    ⚠ A FAILED PORTFOLIO IS COUNTED, NOT RAISED. One book that will not price must not abandon the
    other forty-four, and the summary names how many fell over rather than reporting the whole
    tick as either fine or broken.
    """
    from routers._airs_account_links import list_account_links  # noqa: PLC0415
    from routers._airs_full_refresh import refresh_many  # noqa: PLC0415

    step = _reporter(ctx)
    stop = (lambda: bool(getattr(ctx, "cancelled", False))) if ctx is not None else None

    # ⚠ ONLY THE PAIRED ONES. A model with no account running it has no valuation to keep current,
    # and an account with no model has no composition to price — `refresh_portfolio_fully` would
    # report `absent` for every one of them and spend a request finding out.
    paired = [a["portefeuille"] for a in list_account_links()["accounts"]
              if a.get("model_portfolio_id") is not None and a.get("portefeuille")]
    if not paired:
        return "no paired model portfolios to price", {"portfolios": 0}

    done = {"n": 0}

    def _on_result(name: str, res: dict) -> None:
        done["n"] += 1
        step(done["n"], len(paired), f"{name} — {res.get('model_status') or res.get('status')}")

    step(0, len(paired), f"pricing {len(paired)} model portfolio(s)")
    results = refresh_many(paired, halves=("model",), on_result=_on_result, should_stop=stop)
    ok = sum(1 for r in results if r.get("model_status") == "ok")
    bad = [r.get("portefeuille") for r in results if r.get("model_status") not in ("ok", "skipped")]
    summary = {"portfolios": len(paired), "priced": ok, "failed": len(bad),
               "failed_names": bad[:10]}
    msg = f"{ok}/{len(paired)} model portfolio(s) repriced"
    if bad:
        msg += f" — {len(bad)} FAILED: {', '.join(str(b) for b in bad[:5])}"
    return msg, summary


def _fire_airs_vermogen() -> None:
    """APScheduler callable for the daily AIRS Vermogensoverzicht refresh. Runs
    on its own daemon thread so the long Playwright scrape doesn't block the
    scheduler worker. Re-discovers the live portfolio list + stores each
    portfolio's holdings snapshot (see `airs_vermogen`).

    ⚠ IT FORCES. The manual button is incremental — it skips an account fully scanned in the last
    `AIRS_FRESH_HOURS` — but this is the once-a-day pass that has to actually pick up the day's
    valuation. Somebody pressing Refresh all at 08:00, before AIRS had valued the books, would
    otherwise make this job skip the whole fleet and the new valuation would land a day late.
    (That sentence said "the 11:00 job" and the tick has been 10:00 for months — a restated time is
    a time that goes stale. The schedule is declared once, in `scheduled_jobs.SCHEDULED_JOBS`.)

    ⚠⚠ AND THAT SAME REASONING NOW CUTS THE OTHER WAY, SINCE THE TICK MOVED TO 09:30 (2026-08-13).
    If AIRS has not valued the books by then, this run stores YESTERDAY's valuation — and because
    it forces and fires once, nothing re-reads it until tomorrow. The symptom is holdings that are
    a full day behind while looking perfectly current. If that appears, add a second attempt later
    in the morning; do not move this one earlier.
    """
    _spawn_body("airs_vermogen_refresh")


def _body_airs_vermogen(ctx=None) -> tuple[str, dict]:
      """⚠⚠ CANCEL LANDS BETWEEN ACCOUNTS, NEVER INSIDE ONE — `run_airs_vermogen_refresh_sync`
      already takes the `should_stop` hook and checks it at exactly that boundary, because an
      account's four reports are downloaded and stored as a unit. Everything already stored is kept.

      ⚠ ONE RESULT FOR BOTH HALVES, because it is one JOB — but each half keeps its own try/except,
      so a failed composition scan still cannot cost the daily valuation. It reports `error` if
      EITHER failed and names which: "not fatal to the accounts refresh" is a statement about
      control flow, not a reason to report a failure as ok."""
      summary: dict = {}
      failures: list[str] = []
      stop = (lambda: bool(getattr(ctx, "cancelled", False))) if ctx is not None else None
      step = _reporter(ctx)
      if True:
        try:
            from airs_vermogen import run_airs_vermogen_refresh_sync  # noqa: PLC0415
            # ⚠ TWO PHASES, TWO COUNTERS, AND THE MESSAGE SAYS WHICH. The accounts pass counts
            # accounts and the model pass counts models; the totals are only known when each starts,
            # so the bar restarts between them. A restarting bar reads as a failure unless the line
            # under it names the phase — hence the prefixes below.
            res = run_airs_vermogen_refresh_sync(
                triggered_by="auto", force=True, should_stop=stop,
                on_step=lambda d, t, m: step(d, t, f"Accounts · {m}")) or {}
            summary.update(accounts=res.get("complete_accounts"),
                           portfolios_found=res.get("portfolios_found"),
                           holdings_rows=res.get("holdings_rows"),
                           cancelled_at=res.get("cancelled_at"))
        except Exception as e:
            failures.append(f"accounts: {type(e).__name__}: {e}")
            _log.exception(
                "[scheduler] airs_vermogen refresh failed: %s: %s", type(e).__name__, e,
            )
        # ⚠ THE MODEL PORTFOLIOS TOO — NOTHING SCHEDULED HAS EVER SCANNED THEM, AND THE PAIRING
        # SILENTLY DEPENDS ON THEM. This tick refreshes the ACCOUNTS (Rendement,
        # Vermogensoverzicht); the model COMPOSITIONS were only ever populated by pressing "Scan
        # AIRS" on the portfolios page by hand. So a deployment where nobody pressed it has an
        # empty `airs_model_portfolio_position` — and `_airs_account_links._models()` keeps only
        # models that HAVE a composition, so every account then matches nothing, loses its
        # pairing, and Analyse falls back to an unpaired basket for books that are perfectly
        # fine. That is the production symptom this exists to end: "No valued positions to show",
        # on a portfolio whose rows expand normally one panel away.
        #
        # ⚠ AFTER the accounts, in the SAME thread, never beside it. Both drive one authenticated
        # AirSPMS session through Playwright; two scrapers at once is a contended login, and the
        # failure mode there is a half-finished scan rather than an error.
        # ⚠ SKIPPED ENTIRELY ON A CANCEL. Running minutes more of scraping after the reader asked
        # to stop is the same mistake the fleet refresh already refuses to make.
        try:
            if stop is not None and stop():
                raise _Cancelled("stopped between accounts; everything stored so far is kept")
            from airs_scanner import (  # noqa: PLC0415
                count_model_portfolio_holdings_sync,
                fetch_model_portfolios_sync,
            )
            from routers import _airs_portfolio_store as store  # noqa: PLC0415

            # ⚠ THE SCANNER ALREADY EMITS PER-ITEM EVENTS — it was being handed a no-op. Both
            # `fetch_model_portfolios_sync` and `count_model_portfolio_holdings_sync` send a
            # `message` per portfolio; forwarding them is the difference between a toast that says
            # "starting…" for four minutes and one that names the book it is on.
            #
            # ⚠ THE PAIR IS READ AS DATA NOW (2026-08-17). It used to say the scanner did not expose
            # `i`/`n`, so this counted `count` events itself and reported a total of 0 — an
            # indeterminate bar for the four minutes this phase runs. `count_model_portfolio_
            # holdings_sync` carries them as fields since the manual button became a job, and both
            # callers read the same pair rather than each keeping a private tally. Still never
            # parsed out of the message: the prose is for the reader, the numbers are for the bar.
            models_at = {"done": 0, "total": 0}

            def _relay(kind: str, **kw) -> None:
                msg = kw.get("message")
                if not msg:
                    return
                if kind == "count":
                    models_at["done"] = int(kw.get("i") or 0)
                if kw.get("n"):
                    models_at["total"] = int(kw["n"])
                step(models_at["done"], models_at["total"], f"Models · {msg}")
                # ⚠ CHECKED HERE TOO. The model scan is the long half (one edit page + one XLS per
                # portfolio, minutes); without this a Cancel pressed during it would be honoured
                # only after every remaining book had been downloaded.
                #
                # ⚠ TWO MECHANISMS REACH THE SAME BOUNDARY, AND THAT IS WORTH KNOWING. This raises
                # out of the event hook; the manual job passes `should_stop=` and the scanner
                # returns. Both stop BETWEEN portfolios — this one because `count` is emitted after
                # the row is downloaded, counted and persisted — so neither can leave a row half
                # written. Named rather than unified: rewriting the scheduler's cancellation is a
                # separate change from making the button's own scan stoppable.
                if stop is not None and stop():
                    raise _Cancelled(
                        f"stopped after {models_at['done']} model(s); everything stored is kept")

            rows = fetch_model_portfolios_sync(_relay)
            store.save_portfolios(rows)
            # ⚠ WRITES AS IT GOES (`on_positions`), so a scan that dies halfway leaves behind
            # what it did reach — the same contract the manual button has.
            count_model_portfolio_holdings_sync(
                rows, _relay,
                on_positions=store.save_positions,
                on_error=store.save_positions_error,
            )
            _log.warning("[scheduler] airs model-portfolio scan: %d portfolio(s) stored", len(rows))
            summary["models"] = len(rows)
        except _Cancelled:
            raise
        except Exception as e:
            # Best effort, and NEVER fatal to the accounts refresh above — that one is the daily
            # valuation and must not be lost to a failure in the composition scan.
            failures.append(f"models: {type(e).__name__}: {e}")
            _log.exception(
                "[scheduler] airs model-portfolio scan failed: %s: %s", type(e).__name__, e,
            )
      if failures:
          raise RuntimeError(" · ".join(failures)[:400])
      return (f"{summary.get('accounts')} account(s), {summary.get('models')} model(s)", summary)


def _fire_table_size_sample() -> None:
    """Nightly: record every public table's size on disk.

    ⚠ BYTES, NOT ROWS WRITTEN — see `db_growth`. Instrumenting the jobs to count their own inserts
    would rank the AIRS model scan (which delete-then-inserts every portfolio'''s positions:
    thousands of rows, zero growth) above the month-end price refresh, and would be blind to
    indexes and bloat.

    Cheap enough to need no gating: one catalog read and ~50 small inserts. Own daemon thread for
    consistency with every other tick; never raises into the scheduler.
    """
    _spawn_body("table_size_sample")


def _body_table_size_sample(ctx=None) -> tuple[str, dict]:
    """⚠ NOT CANCELLABLE, AND NOT WORTH MAKING SO — one catalog read and ~50 small inserts, over in
    milliseconds. A cancel would land after it finished."""
    from db_growth import sample_table_sizes  # noqa: PLC0415

    res = sample_table_sizes()
    _log.info("[scheduler] db size snapshot: %s tables, %s MB total (biggest %s)",
              res.get("tables"), res.get("total_mb"), res.get("biggest"))
    return f"{res.get('tables')} tables, {res.get('total_mb')} MB total", res


def _register_bodies() -> None:
    """Fill `JOB_BODIES` once every body is defined.

    ⚠ AT THE BOTTOM OF THE MODULE, NOT AT THE DICT. The bodies are defined throughout the file
    beside the ticks they belong to; naming them where the dict is declared would be forward
    references to functions that do not exist yet.
    """
    JOB_BODIES.update({
        "fx_sync": _body_fx_sync,
        "airs_vermogen_refresh": _body_airs_vermogen,
        "airs_model_prices": _body_airs_model_prices,
        "job_watchdog": _body_job_watchdog,
        "history_drift_check": _body_history_drift,
        "asset_price_refresh": _body_asset_price_refresh,
        "benchmark_index_refresh": _body_benchmark_index_refresh,
        "relative_momentum_refresh": _body_relative_momentum_refresh,
        "benchmark_fundamentals_fill": _body_benchmark_fundamentals,
        "table_size_sample": _body_table_size_sample,
    })
    # ⚠ THE SAME LATE BINDING, FOR THE SAME REASON — see `_WATCHDOG_STARTERS`. These two are the
    # tick callables themselves rather than `(ctx) -> (str, dict)` bodies: they spawn their own
    # daemon threads and narrate into `ingest_run`, which is where /schedule already watches them.
    _WATCHDOG_STARTERS.update({
        "daily_pipeline": _fire_daily_sequence,
        "daily_price_slice": _fire_daily_price_slice,
    })


#: When THIS process's scheduler came up. ⚠ SET AT START, READ BY THE GAP SCAN — it is the whole
#: evidence that a missed fire time was missed because nothing was alive to fire it.
_booted_at: "datetime | None" = None


def _on_job_missed(event) -> None:
    """APScheduler dropped a fire because it arrived past `misfire_grace_time`.

    ⚠⚠ NOTHING LISTENED TO THIS EVENT UNTIL NOW, WHICH IS HALF OF WHY THE PRODUCTION FAILURE WAS
    UNDIAGNOSABLE. A dropped fire wrote no row, logged no line and left the job's `next_run_time`
    looking perfect — so `/schedule` could only report `overdue` and shrug. This is the case where
    the process WAS alive and could not get to the job in time (a blocked worker, a saturated pool),
    which is a different fault with a different fix from the process being absent, and the two must
    not arrive as the same row.

    ⚠ WARNING, NOT INFO. uvicorn leaves the root logger at WARNING, so an INFO line here would be
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


def _on_job_error(event) -> None:
    """A tick raised out of its callable.

    ⚠ BELT AND BRACES, AND IT COVERS A REAL SEAM. Every `_fire_*` spawns a daemon thread and the
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

    ⚠⚠ THIS IS THE HALF THAT EXPLAINS THE PRODUCTION SYMPTOM, and it is not a misfire. The scheduler
    here uses APScheduler's DEFAULT IN-MEMORY JOBSTORE, so a boot recomputes every `next_run_time`
    from *now*: a fire time that passed while the process was down never existed, emits no event,
    and leaves `next_run_time` looking healthy. That is precisely how `daily_pipeline` read
    "20.9 days ago" beside "Next run tomorrow, 07:00" — and why no amount of listening to
    APScheduler could ever have caught it. The trigger is a pure function of the calendar, so it can
    be asked what it WOULD have done over a window that reaches back before this process existed.

    ⚠ RUN AT BOOT, WHICH IS THE ONLY MOMENT IT IS BOTH POSSIBLE AND USEFUL: possible because the
    window now spans a period nobody was watching, useful because a restarting host reaches this
    line often. Idempotent by construction — see `record_missed`.

    ⚠ IT RECORDS, IT DOES NOT HEAL. What to do about a gap is `_body_job_watchdog`'s decision, with
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
        # ⚠⚠ BOTH TABLES, BECAUSE "DID THIS TICK FIRE" HAS TWO ANSWER SHEETS. A `records=False`
        # job (`daily_pipeline`, `daily_price_slice`) writes `ingest_run` rows and NO
        # `scheduled_job_run` row — by design, so one event cannot have two disagreeing records.
        # Scanning only the latter would report every night of a healthy pipeline as a missed tick,
        # which is the loudest possible false alarm on the two jobs nobody can afford to start
        # ignoring. The `missed` rows this writes still go under the job's own id — that is the one
        # thing it CAN say — which is why `build_rows` now always looks a spec up under its id too.
        own = started_at_stamps(spec.id, since)
        via_ingest = ingest_run_stamps(spec.evidence, since)
        if own is None or via_ingest is None:
            # ⚠ A FAILED READ IS NOT AN EMPTY HISTORY. Treated as empty, one Supabase blip at boot
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
            # ⚠ ONE LOUD LINE PER JOB, at WARNING so Railway shows it. This is the sentence somebody
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
    """How many days back the gap scan looks. ⚠ ENV-OVERRIDABLE so a long outage can be
    reconstructed once by hand without a deploy; the default is deliberately short (see
    `job_misses.DEFAULT_LOOKBACK_DAYS`)."""
    from job_misses import DEFAULT_LOOKBACK_DAYS  # noqa: PLC0415

    raw = os.environ.get("JOB_GAP_LOOKBACK_DAYS", "").strip()
    if raw.isdigit() and 1 <= int(raw) <= 90:
        return int(raw)
    return DEFAULT_LOOKBACK_DAYS


def _boot_gap_pass() -> None:
    """At boot: write down which ticks were lost while nothing was running, then heal what can be.

    ⚠⚠ RECORD FIRST, HEAL SECOND, AND NEVER THE OTHER WAY ROUND. The watchdog's verdict comes from
    `job_health`, which reads the run history — so a heal that ran first would re-fire the jobs and
    the gap scan would then find their fresh rows and conclude nothing had been missed. The outage
    would erase its own evidence, every time, which is the failure mode that made this invisible in
    the first place.

    ⚠⚠ THE HEAL IS THE WATCHDOG, NOT A SECOND FIRING MECHANISM. `_body_job_watchdog` already owns
    the decision about which states "run it again" actually fixes (`_WATCHDOG_HEALS` — not
    `missing`, `error` or `unknown`, each of which it would paper over) and the per-day cap that
    stops a structurally broken job being retried for ever. Re-firing jobs directly from here would
    be a second copy of that judgement, and the copy is the one that drifts.

    ⚠⚠ WHICH IS ALSO WHY THE CAP HAD TO BECOME DURABLE FIRST (`watchdog_runs_today`). It lived in a
    process-local dict, and a boot resets that to zero — so a host in a restart loop, which is
    exactly the host that reaches this line, would have re-fired the whole fleet on every restart
    with a guard that could never see it had already done so.

    ⚠ THE WATCHDOG IS A SCHEDULED JOB AND THAT IS PRECISELY THE PROBLEM THIS SOLVES. It fires at
    11:00 UTC; if the process is not alive at 11:00 UTC it is missed by the same mechanism as
    everything it was meant to heal — measured in production at 44.7h stale, itself reported
    `overdue`. Running it once per boot means any boot heals the backlog, whatever the host does to
    the clock.

    ⚠ NEVER RAISES. It runs on a daemon thread off the startup hook; an exception here would be an
    unhandled thread exception during a deploy, which is noise on top of the outage it is reporting.
    """
    try:
        result = scan_for_missed_ticks()
    except Exception as e:  # noqa: BLE001
        _log.warning("[scheduler] the boot gap scan failed: %s: %s", type(e).__name__, e)
        return
    if not result.get("total"):
        _log.info("[scheduler] boot gap scan: no missed ticks in the last %dd",
                  job_misses_lookback())
        return
    _log.warning("[scheduler] boot gap scan recorded %d missed tick(s): %s",
                 result["total"], result["missed"])
    if os.environ.get("DISABLE_BOOT_HEAL", "").lower() in ("1", "true", "yes"):
        # ⚠ AN OFF SWITCH FOR THE HEALING HALF ALONE, because the two halves have very different
        # risk. Recording is a handful of inserts; healing starts real jobs that spend vendor quota.
        # A deployment that wants the evidence without the action can have exactly that.
        _log.warning("[scheduler] DISABLE_BOOT_HEAL set — not re-running the missed jobs")
        return
    _spawn_body("job_watchdog")


def register_scheduler(app) -> None:
    """Attach the scheduler to the FastAPI lifecycle. Called once from
    `main.py` after the FastAPI() instance is created."""

    @app.on_event("startup")
    def _start_scheduler() -> None:
        global _scheduler, _booted_at
        if _scheduler is not None:
            return  # already running (multiple startup events on reload)
        # ⚠ STAMPED BEFORE ANYTHING ELSE. Every missed-tick row this boot writes carries it, and it
        # is the whole argument that the tick was missed because nothing was alive: a fire time
        # before this instant, with no row, on a scheduler that only exists from here.
        _booted_at = datetime.now(timezone.utc)

        # Allow operators to disable the in-process scheduler via env var —
        # useful when running multiple replicas, during a manual ingest test,
        # or in CI where we don't want background jobs touching real data.
        if os.environ.get("DISABLE_SCHEDULER", "").lower() in ("1", "true", "yes"):
            _log.info("[scheduler] DISABLE_SCHEDULER set — in-process jobs not started")
            return

        sched = BackgroundScheduler(timezone="UTC")
        # ⚠⚠ THE OBSERVERS GO ON BEFORE ANY JOB IS ADDED, AND BEFORE `start()`. Until 2026-09-01
        # nothing listened to either event, which is half of why a production job could sit 20 days
        # stale with a healthy next-run beside it and no explanation anywhere: a dropped fire wrote
        # no row and logged no line. See `_on_job_missed` for what the two events can and cannot
        # tell us — notably that neither of them fires for the commonest case of all, a process that
        # was not running, which is what `scan_for_missed_ticks` is for.
        sched.add_listener(_on_job_missed, EVENT_JOB_MISSED)
        sched.add_listener(_on_job_error, EVENT_JOB_ERROR)

        def _register(job_id: str, fn) -> None:
            """Register one declared job — ⚠ THE SCHEDULE COMES FROM `scheduled_jobs.py`.

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
            trigger = (IntervalTrigger(seconds=spec.interval_seconds)
                       if spec.interval_seconds is not None
                       else CronTrigger(**(spec.trigger or {})))
            sched.add_job(fn, trigger, id=spec.id, replace_existing=True, **spec.options)
        # Single daily tick at 05:00 UTC (~07:00 CEST / 06:00 CET) that runs the
        # split pipeline's two operations IN ORDER (see `ingest.phases.pipeline`):
        #   1. price-update — re-price the held companies + refresh MTD;
        #   2. rebalance    — rebalance any strategy whose rebalance day has
        #      arrived (a no-op otherwise).
        # They run sequentially in one daemon thread and never overlap (the
        # rebalance also serializes against manual Run-now via the pipeline
        # lock). 05:00 UTC (was 02:00) gives GuruFocus a few extra hours to
        # publish the previous day's EUROPEAN EOD closes — at 02:00 UTC the
        # slower-publishing EU exchanges (XPAR/MIL/XAMS) often still lacked the
        # prior close, leaving held EU names a day stale until the next tick.
        # US/Asia closes are long settled by either hour. Monday's tick still
        # has Friday's settled close, so a first-Monday rebalance decides on
        # Friday's close. Weekend ticks are cheap no-ops via the per-company
        # freshness short-circuit.
        # If a startup coincides with the tick (e.g. a deploy right at 05:00 UTC),
        # `coalesce=True` collapses any backlog into a single run and
        # `misfire_grace_time` gives 10 min of slack — both declared with the schedule.
        _register("daily_pipeline", _fire_daily_sequence)
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
        # ⚠ IT ALSO SCANS THE MODEL PORTFOLIOS NOW (Stamgegevens → Model portefeuilles), which
        # nothing scheduled ever did. Their compositions are what the account↔model PAIRING is
        # guessed from, so on a deployment where nobody pressed "Scan AIRS" by hand every book
        # was unpaired and Analyse fell back to a basket. See `_fire_airs_vermogen`.
        _register("airs_vermogen_refresh", _fire_airs_vermogen)
        # ⚠ THE PRICING HALF, AT 05:00 — see `_body_airs_model_prices`. Registered beside the
        # scrape it deliberately does NOT duplicate.
        _register("airs_model_prices", _fire_airs_model_prices)
        # ⚠ THE ONE JOB WHOSE SUBJECT IS THE OTHER JOBS — see `_body_job_watchdog`.
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
        # Daily history-drift probe — the early warning between monthly full
        # refetches. 07:00 UTC: after the 05:00 pipeline sequence and the 06:00
        # asset-price refresh, so it never competes with them for GuruFocus.
        # Daily constituent refresh for the REBUILT indices (AEX). 06:30 UTC: after the
        # 05:00 pipeline and before the 07:00 drift probe, so the three never compete.
        _register("benchmark_index_refresh", _fire_benchmark_index_refresh)
        # Re-rank the benchmark universes' 12-1 momentum. 07:30 UTC: after the 05:00 pipeline, the
        # 06:30 index refresh and the 07:00 drift probe, so it ranks the freshest closes the day
        # has and competes with none of them.
        _register("relative_momentum_refresh", _fire_relative_momentum_refresh)
        # Quarterly fundamentals over every benchmark constituent. The 10th, far from
        # month-end, so it can never drain a region the full price refresh needs.
        _register("benchmark_fundamentals_fill", _fire_benchmark_fundamentals)
        _register("history_drift_check", _fire_history_drift_check)
        # Asset-pipeline ingest-queue worker — OPT-IN (ASSET_QUEUE_INPROCESS=1).
        # By default the worker is the STANDALONE `scripts/asset_queue_worker.py`
        # process, which survives backend restarts (dev --reload / redeploys) and
        # keeps draining. Run EXACTLY ONE worker — this in-process tick OR the
        # standalone script, never both (two would compete for the Yahoo throttle
        # and re-introduce throttle-corrupted resolutions). When enabled: every
        # 20s drain one slice; max_instances=1 + coalesce run slices back-to-back
        # without overlap; empty queue → instant no-op.
        if os.environ.get("ASSET_QUEUE_INPROCESS", "").lower() in ("1", "true", "yes"):
            _register("asset_ingest_queue", _fire_asset_ingest_queue)
            _log.info("[scheduler] ASSET_QUEUE_INPROCESS set — in-process ingest-queue worker enabled")
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
        try:
            _maybe_kickstart_airs_models()
        except Exception as e:
            _log.warning(
                "[scheduler] airs-models kickstart wrapper failed: %s: %s",
                type(e).__name__, e,
            )
        next_runs = {j.id: str(j.next_run_time) for j in sched.get_jobs()}
        # ⚠ WARNING, NOT INFO — uvicorn leaves the root logger at WARNING, so the one line that says
        # this process's scheduler exists at all was invisible in Railway. "Did the scheduler even
        # start after that deploy?" is the first question a stale job raises and the log could not
        # answer it.
        _log.warning("[scheduler] started at %s; next runs: %s", _booted_at, next_runs)

        # ⚠⚠ THE GAP SCAN AND THE HEAL RUN OFF THE STARTUP HOOK, ON THEIR OWN THREAD. Both read
        # Supabase (one query per job, then possibly a job start), and a FastAPI startup hook that
        # blocks on the network is a deploy that looks hung — on a host which, per the evidence
        # this was written for, is already restarting more than it should.
        threading.Thread(target=_boot_gap_pass, daemon=True, name="job-gap-scan").start()

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


# ⚠ AT IMPORT, AFTER EVERY BODY IS DEFINED — see `_register_bodies`. Without this the Run-now
# endpoint would find an empty registry and report every job as not runnable by hand.
_register_bodies()
