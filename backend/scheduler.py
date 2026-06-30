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
_price_retry_lock = threading.Lock()
# UTC-date ISO string → retries scheduled so far that day. Pruned to a single
# key (today's) on each schedule so it never grows.
_price_retry_counts: dict[str, int] = {}


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

    try:
        plan = build_plan(datetime.now(timezone.utc))
    except Exception as e:
        _log.warning("[scheduler] kickstart: plan build failed: %s: %s", type(e).__name__, e)
        plan = None
    if plan is not None and plan.strategies:
        if plan.due_strategy_ids:
            reasons.append(f"{len(plan.due_strategy_ids)} strategy(ies) due")
        try:
            if _held_prices_stale():
                reasons.append("held prices stale")
        except Exception as e:
            _log.warning("[scheduler] kickstart: price-staleness probe failed: %s: %s", type(e).__name__, e)

    if not reasons:
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
    portfolio's holdings snapshot (see `airs_vermogen`)."""
    def _run():
        try:
            from airs_vermogen import run_airs_vermogen_refresh_sync  # noqa: PLC0415
            run_airs_vermogen_refresh_sync(triggered_by="auto")
        except Exception as e:
            _log.exception(
                "[scheduler] airs_vermogen refresh failed: %s: %s", type(e).__name__, e,
            )
    threading.Thread(target=_run, daemon=True, name="airs-vermogen").start()


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
        # `day='last'` = the last calendar day of the month; noon UTC (07:00
        # EST) is safely inside the EST usage month (which resets midnight EST
        # on the 1st = 05:00 UTC) and leaves ~17h of runway. The full refresh is
        # prices-only and serializes against the daily ops via the pipeline lock.
        sched.add_job(
            _fire_job,
            CronTrigger(day="last", hour=12, minute=0, timezone="UTC"),
            args=["full_price_refresh"],
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
