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

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

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

# ── Stale held-price retry ─────────────────────────────────────────
# After a price-update, GuruFocus may not yet have published the prior
# session's closes (the slower EU EOD feeds especially). Rather than wait a
# full day for the next 05:00 UTC tick, re-run the held-price refresh a few
# hours later to pick them up. Bounded per UTC day so a genuinely
# unpublishable name (market holiday, illiquid stock) can't loop forever —
# once the day's budget is spent the next daily tick takes over.
_PRICE_RETRY_DELAY_HOURS = float(os.environ.get("PRICE_RETRY_DELAY_HOURS", "3"))
_PRICE_RETRY_MAX_PER_DAY = int(os.environ.get("PRICE_RETRY_MAX_PER_DAY", "3"))
# The month-end full-price refresh gets ALL companies' most-recent prices before
# the monthly GuruFocus quota resets. Rather than a single last-day tick (which a
# deploy/restart in its 1h grace would drop for the whole month), it runs on a
# daily 12:00-UTC tick gated to the last `_MONTH_END_WINDOW_DAYS` days of the
# month: it fires once the window opens (~2 days before month end), and if that
# day's run is missed OR errors, the next day in the window retries — no
# startup/every-deploy auto-repricing. Guarded so it only actually runs once the
# window has a successful full refresh.
_MONTH_END_WINDOW_DAYS = int(os.environ.get("MONTH_END_WINDOW_DAYS", "2"))
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


def _month_end_window_start(today: "date") -> "date":
    """First day of the end-of-month refresh window: `_MONTH_END_WINDOW_DAYS`
    days before the month's last day (inclusive). E.g. a 30-day month with the
    default 2 → the 28th, so the window is the 28th–30th."""
    import calendar  # noqa: PLC0415
    last_day = calendar.monthrange(today.year, today.month)[1]
    start_day = max(1, last_day - _MONTH_END_WINDOW_DAYS)
    return today.replace(day=start_day)


def _fire_month_end_refresh() -> None:
    """Daily 12:00-UTC tick that runs the FULL price refresh once during the
    end-of-month window (the last `_MONTH_END_WINDOW_DAYS`+1 days). Fires on the
    first window day; if that run was missed (deploy/restart) or errored, the next
    day in the window retries — so a single dropped tick no longer loses the whole
    month. No-op outside the window, and once the window already has a successful
    full refresh (so it runs at most once/month). Never raises into the scheduler
    thread."""
    try:
        today = datetime.now(timezone.utc).date()
        window_start = _month_end_window_start(today)
        if today < window_start:
            return  # not yet in the end-of-month window — cheap daily no-op

        from deps import supabase  # noqa: PLC0415 — avoid import cycle
        # Already refreshed in THIS window? (A successful full/manual run whose
        # finished_at is on/after the window start.) An ad-hoc refresh earlier in
        # the month doesn't count — we want fresh prices near month end.
        try:
            resp = (
                supabase.table("ingest_run")
                .select("run_id")
                .in_("job_name", ["full_price_refresh", "manual"])
                .eq("status", "ok")
                .gte("finished_at", window_start.isoformat())
                .limit(1)
                .execute()
            )
            if resp.data:
                _log.info(
                    "[scheduler] month-end refresh: already ran this window (since %s) — no-op",
                    window_start,
                )
                return
        except Exception as e:
            # If the guard lookup fails, prefer firing (a redundant run is cheap —
            # already-fresh companies short-circuit) over silently skipping.
            _log.warning(
                "[scheduler] month-end refresh: window-guard lookup failed (%s: %s) — firing anyway",
                type(e).__name__, e,
            )

        _log.warning(
            "[scheduler] month-end refresh: in window (since %s) with no successful "
            "full refresh yet — firing full_price_refresh", window_start,
        )
        _fire_job("full_price_refresh")
    except Exception as e:
        _log.warning("[scheduler] month-end refresh tick failed: %s: %s", type(e).__name__, e)


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
    """The body, shared by the 06:00 tick and the startup catch-up. Never raises."""
    try:
        from asset_pipeline import price_refresh, queue as _q  # noqa: PLC0415

        # ⚠ FIRST, WAIT FOR THE DATABASE — this fires on startup, in parallel with the Supabase
        # stack coming up, and every query below (the queue heartbeat is simply the first) fails
        # with PGRST002 until PostgREST has loaded its schema cache. Losing the catch-up to a
        # boot race costs a whole day of stale held prices; see `_await_db_ready`.
        if not _await_db_ready(f"asset price refresh ({trigger})"):
            return

        if _q.is_worker_active():
            _log.info(
                "[scheduler] asset price refresh (%s) SKIPPED — the ingest queue worker is live "
                "(last activity %s); adding Yahoo load now risks corrupting the listings it is "
                "resolving.", trigger, _q.last_activity(),
            )
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
    except Exception:  # noqa: BLE001
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
    def _run() -> None:
        try:
            from ingest.history_drift import daily_drift_check  # noqa: PLC0415

            res = daily_drift_check(
                on_step=lambda m, lvl: (
                    _log.warning if lvl in ("warn", "error") else _log.info
                )("[drift] %s", m),
            )
            if res.get("drifted"):
                _log.warning(
                    "[scheduler] history drift: %s of %s probed companies had rewritten "
                    "history and were refetched", len(res["drifted"]), res.get("probed"),
                )
        except Exception as e:
            _log.exception("[scheduler] history drift check failed: %s: %s", type(e).__name__, e)
    threading.Thread(target=_run, daemon=True, name="history-drift").start()


def _fire_fx_sync() -> None:
    """Daily ECB FX sync — keeps EVERY fetchable currency's `fx_rate` current.
    The daily pipeline only syncs the currencies the held strategies actually
    use (as a side effect of the momentum backtest stream), so unused ACWI
    currencies would otherwise go stale on the /fx-rates page. Idempotent +
    cheap: `sync_fx_rates_to_db` fetches only the gap since each currency's last
    stored date. Own daemon thread; never raises into the scheduler."""
    def _run() -> None:
        try:
            from datetime import date as _date  # noqa: PLC0415

            from deps import supabase  # noqa: PLC0415
            from fx_rates import ECB_CURRENCIES, _USD_PEGS  # noqa: PLC0415
            from momentum.data import sync_fx_rates_to_db  # noqa: PLC0415

            currencies = list(ECB_CURRENCIES) + list(_USD_PEGS.keys()) + ["TWD"]
            status = sync_fx_rates_to_db(supabase, currencies, _date(2000, 1, 1), _date.today())
            synced = sum(1 for s in status.values() if s.get("status") == "synced")
            errors = sum(1 for s in status.values() if s.get("status") == "error")
            _log.info(
                "[scheduler] fx sync done: %s/%s currencies updated, %s errors",
                synced, len(status), errors,
            )
        except Exception as e:
            _log.exception("[scheduler] fx sync failed: %s: %s", type(e).__name__, e)
    threading.Thread(target=_run, daemon=True, name="fx-sync").start()


def _fire_airs_vermogen() -> None:
    """APScheduler callable for the daily AIRS Vermogensoverzicht refresh. Runs
    on its own daemon thread so the long Playwright scrape doesn't block the
    scheduler worker. Re-discovers the live portfolio list + stores each
    portfolio's holdings snapshot (see `airs_vermogen`).

    ⚠ IT FORCES. The manual button is incremental — it skips an account fully scanned in the last
    `AIRS_FRESH_HOURS` — but this is the once-a-day pass that has to actually pick up the day's
    valuation. Somebody pressing Refresh all at 08:00, before AIRS had valued the books, would
    otherwise make the 11:00 job skip the whole fleet and the new valuation would land a day late.
    """
    def _run():
        try:
            from airs_vermogen import run_airs_vermogen_refresh_sync  # noqa: PLC0415
            run_airs_vermogen_refresh_sync(triggered_by="auto", force=True)
        except Exception as e:
            _log.exception(
                "[scheduler] airs_vermogen refresh failed: %s: %s", type(e).__name__, e,
            )
    threading.Thread(target=_run, daemon=True, name="airs-vermogen").start()


def _fire_crm_relaties() -> None:
    """APScheduler callable for the daily CRM 'Alle relaties' refresh (11:00
    Amsterdam, every day). Downloads the export + OVERWRITES airs_crm_relatie
    with the latest snapshot. Own daemon thread so the Playwright scrape doesn't
    block the scheduler worker."""
    def _run():
        try:
            from airs_crm import run_crm_relaties_refresh_sync  # noqa: PLC0415
            res = run_crm_relaties_refresh_sync()
            _log.info("[scheduler] CRM relaties refresh — %s relations (%s KB)",
                      res.get("rows"), (res.get("bytes") or 0) // 1024)
        except Exception as e:
            _log.exception(
                "[scheduler] CRM relaties refresh failed: %s: %s", type(e).__name__, e,
            )
    threading.Thread(target=_run, daemon=True, name="crm-relaties").start()


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
        sched.add_job(
            _fire_daily_sequence,
            CronTrigger(day_of_week="mon-sun", hour=5, minute=0, timezone="UTC"),
            id="daily_pipeline",
            replace_existing=True,
            # If a startup coincides with the tick (e.g. a deploy right at
            # 02:00 UTC), `coalesce=True` collapses any backlog into a single
            # run and `misfire_grace_time` gives 10 min of slack.
            coalesce=True,
            misfire_grace_time=600,
        )
        # Month-end FULL price refresh — re-price EVERY company (most-stale
        # first), bounded by the monthly GuruFocus quota that's about to reset.
        # Fires DAILY at 12:00 UTC (07:00 EST, safely inside the EST usage month
        # that resets midnight EST on the 1st), but `_fire_month_end_refresh`
        # gates it to the last `_MONTH_END_WINDOW_DAYS`+1 days AND to "not already
        # done this window" — so it runs ONCE near month end, and a missed/failed
        # day retries the next day in the window (vs the old single last-day tick
        # a deploy could drop for the whole month). Prices-only; serializes
        # against the daily ops via the pipeline lock.
        sched.add_job(
            _fire_month_end_refresh,
            CronTrigger(hour=12, minute=0, timezone="UTC"),
            id="month_end_price_refresh",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=3600,
        )
        # Daily AIRS refresh — working days (Mon–Fri) at 10:00 Amsterdam time.
        # The per-job timezone makes APScheduler handle the CET/CEST DST shift;
        # only weekday holidays aren't skipped (a holiday run just re-stores the
        # prior close, harmless). Re-discovers the live AirSPMS portfolio list
        # each run (it changes day-to-day) and stores each portfolio's Rendement
        # + Vermogensoverzicht. Runs on its own thread.
        sched.add_job(
            _fire_airs_vermogen,
            CronTrigger(day_of_week="mon-fri", hour=10, minute=0, timezone="Europe/Amsterdam"),
            id="airs_vermogen_refresh",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=3600,
        )
        # Daily CRM "Alle relaties" refresh — EVERY day at 11:00 Amsterdam time.
        # Downloads the export and OVERWRITES airs_crm_relatie with the latest
        # snapshot (full table replace, not a per-date accumulation). Dedicated
        # job (separate from the portfolio refresh) so the CRM table is reliably
        # fresh daily; its own thread for the Playwright scrape.
        sched.add_job(
            _fire_crm_relaties,
            CronTrigger(day_of_week="mon-sun", hour=11, minute=0, timezone="Europe/Amsterdam"),
            id="crm_relaties_refresh",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=3600,
        )
        # Daily ECB FX sync — weekdays 16:30 UTC, after the ECB ~16:00 CET
        # reference-rate publication, so the `fx_rate` table (and the /fx-rates
        # page) shows EVERY currency current, not just the held ones the daily
        # pipeline happens to sync. Idempotent + cheap (fetches only the gap).
        sched.add_job(
            _fire_fx_sync,
            CronTrigger(day_of_week="mon-fri", hour=16, minute=30, timezone="UTC"),
            id="fx_sync",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=3600,
        )
        # Daily Yahoo price refresh for the HELD instruments — the `asset_price` twin of the
        # 05:00 GuruFocus price update, which `asset_price` never had (it aged silently: 197 of
        # 223 held instruments were stale, and a portfolio whose window opened after its
        # holdings' last close rendered blank rows).
        #
        # 06:00 UTC: every market's previous close is long settled, and it is AFTER the 05:00
        # daily sequence rather than racing it. `max_instances=1` so a slow run (≈220 gap fetches
        # at ~1.5s each) can never overlap the next day's tick, and the job itself stands down
        # entirely while the ingest queue is resolving — see `_fire_asset_price_refresh`.
        sched.add_job(
            _fire_asset_price_refresh,
            CronTrigger(day_of_week="mon-sun", hour=6, minute=0, timezone="UTC"),
            id="asset_price_refresh",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=3600,
        )
        # Daily history-drift probe — the early warning between monthly full
        # refetches. 07:00 UTC: after the 05:00 pipeline sequence and the 06:00
        # asset-price refresh, so it never competes with them for GuruFocus.
        sched.add_job(
            _fire_history_drift_check,
            CronTrigger(day_of_week="mon-fri", hour=7, minute=0, timezone="UTC"),
            id="history_drift_check",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=3600,
        )
        # Asset-pipeline ingest-queue worker — OPT-IN (ASSET_QUEUE_INPROCESS=1).
        # By default the worker is the STANDALONE `scripts/asset_queue_worker.py`
        # process, which survives backend restarts (dev --reload / redeploys) and
        # keeps draining. Run EXACTLY ONE worker — this in-process tick OR the
        # standalone script, never both (two would compete for the Yahoo throttle
        # and re-introduce throttle-corrupted resolutions). When enabled: every
        # 20s drain one slice; max_instances=1 + coalesce run slices back-to-back
        # without overlap; empty queue → instant no-op.
        if os.environ.get("ASSET_QUEUE_INPROCESS", "").lower() in ("1", "true", "yes"):
            sched.add_job(
                _fire_asset_ingest_queue,
                IntervalTrigger(seconds=20),
                id="asset_ingest_queue",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=30,
            )
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
