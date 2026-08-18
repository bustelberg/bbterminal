"""WHAT IS SUPPOSED TO RUN BY ITSELF, AND HOW WE WOULD KNOW IF IT STOPPED.

⚠⚠ THE FAILURE THIS EXISTS FOR IS A JOB THAT IS NOT THERE AT ALL, AND IT IS INVISIBLE IN EVERY
    SURFACE WE HAD. `scheduler.list_scheduled_jobs()` reports what APScheduler is currently holding
    — which is exactly nothing under `DISABLE_SCHEDULER=1`, nothing before startup finishes, and
    nothing for a job whose `add_job` threw inside the startup handler. An empty list and a healthy
    idle scheduler render identically. So "what should be running" cannot be read off the scheduler;
    it has to be DECLARED, and the page's job is to show the declaration beside reality and point at
    the disagreement.

⚠⚠ AND THE TRIGGERS LIVE HERE, NOT IN `scheduler.py`, WHICH IS THE WHOLE POINT OF THE MODULE.
    A declaration that merely *describes* the cron is a second copy of it, and the copy is what
    drifts — within a month the page is confidently reporting a cadence nothing runs at, which is
    worse than no page. `scheduler.py` builds its triggers FROM `SCHEDULED_JOBS`, so the schedule on
    screen IS the schedule that fires. There is one number and it is in this file.

⚠ THIS FILE IS DECLARATION ONLY — no imports from `scheduler`, no APScheduler, no `deps`. Both the
    scheduler and the admin router import it, and anything heavier here becomes an import cycle
    (`scheduler` → `ingest.phases` → `deps`) or drags the DB into a module the router reads at
    request time.

⚠ `evidence` IS THE HONEST PART. It names the `ingest_run.job_name` rows a job leaves behind, and
    for six of the eight it is EMPTY — those jobs currently write nothing but a log line that
    scrolls away in Railway. An empty tuple therefore means "we cannot tell whether this ran", which
    the API must report as UNKNOWN and never as ok and never as a failure. Filling those in is the
    next step (`record_run`); until then the page must not pretend.
"""
from __future__ import annotations

from dataclasses import dataclass, field

# ── The two things an operator actually asks ──────────────────────────────────
#   "is it registered?"  -> compare `SCHEDULED_JOBS` against `list_scheduled_jobs()`
#   "did it run?"        -> `evidence`, joined against `ingest_run`
# Everything else on the page is context for those two.


#: How `scheduler._reap_orphan_runs` marks a run whose process died mid-flight.
#:
#: ⚠⚠ ONE STRING, WRITTEN IN ONE PLACE AND READ IN ANOTHER, SO IT LIVES HERE. The reaper stamps a
#: killed run `status='error'` — correct, because it certainly did not finish — but "the process was
#: restarted" and "the job is broken" are different facts with different fixes, and the overview has
#: to tell them apart. Matching on a literal copied into the reader is how that quietly stops
#: working the day someone rewords the message.
ORPHAN_MARKER = "Orphaned (backend restart while running)"


@dataclass(frozen=True)
class JobSpec:
    """One job that is supposed to run on its own."""

    #: APScheduler job id — the join key against `list_scheduled_jobs()`. MUST match `add_job(id=…)`.
    id: str
    #: What a human calls it.
    label: str
    #: What it keeps current — named as TABLES, because "is my data stale" is the question behind
    #: "did the job run", and a job name does not answer it.
    fills: str
    #: The cadence in words, for the page. Derived from `trigger` by eye, never parsed from it —
    #: see `test_scheduled_jobs.py`, which asserts the two agree so this cannot become a lie.
    cadence: str
    #: `CronTrigger(**trigger)` kwargs. ⚠ THE REAL SCHEDULE — `scheduler.py` reads this.
    trigger: dict | None = None
    #: `IntervalTrigger(seconds=…)` instead, for the queue worker.
    interval_seconds: int | None = None
    #: Extra `add_job` kwargs (coalesce / misfire_grace_time / max_instances).
    options: dict = field(default_factory=dict)
    #: How long may pass between successful runs before this is OVERDUE.
    #:
    #: ⚠ IT IS NOT THE CADENCE. A Mon–Fri job is 3 days idle over every weekend and that is healthy,
    #: so a "daily" threshold would cry wolf every Monday and be ignored by Tuesday. Each of these
    #: is the longest HEALTHY gap plus one period of slack.
    #:
    #: ⚠ AND FOR THE MONTH-END REFRESH IT DESCRIBES THE WORK, NOT THE TICK. That job wakes every day
    #: and does nothing on 28 of them by design; an operator who sees "last run 20 days ago" wants
    #: to know whether the REFRESH is late, not whether the tick fired.
    max_age_hours: float | None = None
    #: EXTRA `ingest_run.job_name` values this job writes — the pipeline's detailed per-phase
    #: history. Additive to the `scheduled_job_run` row every recording job writes under its own id.
    evidence: tuple[str, ...] = ()
    #: Whether this job writes a `scheduled_job_run` row (`job_runlog.record_run`).
    #:
    #: ⚠ IT IS NOT "IS THIS JOB OBSERVABLE" — `evidence` is the other half of that. A job with
    #: NEITHER leaves no durable trace at all, and the API must then say `unknown`: never `ok`,
    #: which would be a fabrication, and never `error`, which would cry wolf.
    #:
    #: ⚠ THE TWO PIPELINE JOBS SET IT FALSE ON PURPOSE. They already write a far richer `ingest_run`
    #: row per operation, which `/schedule` renders as their history; a second, thinner row beside
    #: it would be two records of one event, free to disagree. They are observable through
    #: `evidence`. The queue worker sets it false for the opposite reason — it is unobservable and
    #: should stay that way, because a row three times a minute is a write loop, not a history.
    records: bool = True
    #: Env var that gates registration — absent means it is legitimately not registered.
    optional_env: str | None = None
    #: Anything an operator needs before reading the row as a fault.
    note: str = ""


SCHEDULED_JOBS: tuple[JobSpec, ...] = (
    JobSpec(
        id="daily_pipeline",
        label="Daily pipeline (price update → rebalance)",
        fills="metric_data prices/volumes for held companies · current_picks snapshots",
        cadence="Every day, 05:00 UTC",
        trigger={"day_of_week": "mon-sun", "hour": 5, "minute": 0, "timezone": "UTC"},
        options={"coalesce": True, "misfire_grace_time": 600},
        max_age_hours=30,
        evidence=("price_update", "rebalance"),
        records=False,          # already writes ingest_run — see `records`
        note="Two ingest_run rows per fire, in order. 05:00 rather than 02:00 so the slower "
             "European EOD closes are published by run time.",
    ),
    JobSpec(
        id="month_end_price_refresh",
        label="Month-end full price refresh",
        fills="metric_data for EVERY company, most-stale first",
        cadence="Daily tick at 12:00 UTC; acts only in the last days of the month",
        trigger={"hour": 12, "minute": 0, "timezone": "UTC"},
        options={"coalesce": True, "misfire_grace_time": 3600},
        # ⚠ THE WORK IS MONTHLY EVEN THOUGH THE TICK IS DAILY — see `max_age_hours`. 35 days covers
        # a long month plus the retry window; sooner would flag a job that is behaving exactly as
        # designed on 28 days out of 30.
        max_age_hours=35 * 24,
        evidence=("full_price_refresh",),
        records=False,          # already writes ingest_run — see `records`
        note="Bounded by what is left of the monthly GuruFocus quota, which resets midnight EST "
             "on the 1st — it spends what would otherwise be lost.",
    ),
    JobSpec(
        id="asset_price_refresh",
        label="Yahoo price refresh (held instruments)",
        fills="asset_price for every instrument held in a model portfolio",
        cadence="Every day, 06:00 UTC (+ on startup)",
        trigger={"day_of_week": "mon-sun", "hour": 6, "minute": 0, "timezone": "UTC"},
        options={"coalesce": True, "max_instances": 1, "misfire_grace_time": 3600},
        max_age_hours=30,
        note="Stands down while the ingest-queue worker is live — Yahoo answers an overloaded "
             "caller with an EMPTY result rather than a 429.",
    ),
    JobSpec(
        id="history_drift_check",
        label="History-drift probe",
        fills="re-fetches metric_data bars a vendor rewrote (splits, free-share attributions)",
        cadence="Weekdays, 07:00 UTC",
        trigger={"day_of_week": "mon-fri", "hour": 7, "minute": 0, "timezone": "UTC"},
        options={"coalesce": True, "max_instances": 1, "misfire_grace_time": 3600},
        max_age_hours=80,
        note="Walks a fifth of the universe a day — every name within a week. The one failure the "
             "append-only price path is blind to.",
    ),
    JobSpec(
        id="fx_sync",
        label="ECB FX sync",
        fills="fx_rate, for every fetchable currency",
        cadence="Weekdays, 16:30 UTC",
        trigger={"day_of_week": "mon-fri", "hour": 16, "minute": 30, "timezone": "UTC"},
        options={"coalesce": True, "misfire_grace_time": 3600},
        max_age_hours=80,
        note="After the ~16:00 CET publication. The daily pipeline only syncs the HELD "
             "currencies, so without this the unused ones go stale.",
    ),
    JobSpec(
        id="airs_model_prices",
        label="AIRS model portfolios — reprice",
        fills="airs_model_portfolio_position · asset_execution · fx_rate · asset_price",
        cadence="Daily, 05:00 Amsterdam",
        # ⚠⚠ THE PRICING HALF ONLY — `halves=("model",)`, NEVER the accounts scrape. Two ⚠⚠ notes
        # on `airs_vermogen_refresh` below record why nothing that scrapes AIRS may run at this
        # hour: a forcing account pass that lands before AIRS has valued the books stores
        # YESTERDAY's valuation, and since it fires once nothing re-reads it until tomorrow — the
        # symptom is holdings a full day behind that look perfectly current. The MODEL half has no
        # such hazard. A composition is a dated set of weights rather than a daily valuation, and
        # its other four steps talk to OpenFIGI, the ECB and Yahoo, none of which care what time
        # AIRS runs its batch.
        #
        # ⚠ 05:00 AMSTERDAM, NOT UTC, and deliberately unlike the ingest pipeline's 05:00 UTC tick.
        # This one is about a European working morning ("current when I open the page"), so it must
        # hold its wall-clock hour across the DST shift; the pipeline's is anchored to market closes
        # and must not. They are therefore an hour apart for half the year, which is a bonus rather
        # than the reason.
        #
        # ⚠ EVERY PAIRED MODEL, PRICED CONCURRENTLY (`refresh_many`), which is safe because the one
        # serial resource — the AirSPMS session the composition read needs — is a lock inside
        # `refresh_portfolio_fully`. The Yahoo legs are what take the time and they overlap.
        trigger={"hour": 5, "minute": 0, "timezone": "Europe/Amsterdam"},
        options={"coalesce": True, "misfire_grace_time": 3600},
        max_age_hours=30,
        note="Prices every model portfolio: composition, instrument resolve, FX backfill, price "
             "fetch, YTD recompute. ⚠ Does NOT scrape the accounts — that is the 09:30 job, and "
             "05:00 is documented as too early for it.",
    ),
    JobSpec(
        id="airs_vermogen_refresh",
        label="AIRS portfolios + model scan",
        fills="airs_holding · airs_performance · airs_mutatie · airs_model_portfolio*",
        cadence="Weekdays, 09:30 Amsterdam",
        # ⚠ MOVED 10:00 → 09:30 ON REQUEST (2026-08-13). It refreshes the COMPLETE table: the job
        # passes `force=True`, which overrides the incremental `AIRS_FRESH_HOURS` skip, so every
        # discovered account is re-downloaded rather than only the stale ones.
        #
        # ⚠⚠ 09:30 MAY BE BEFORE AIRS HAS VALUED THE BOOKS, AND THERE IS NO SECOND ATTEMPT. The
        # note on `_fire_airs_vermogen` records that a run at 08:00 lands before the valuation is
        # ready; 10:00 was on the safe side of that. This job fires once a day and forces, so a
        # scrape that arrives early stores YESTERDAY's valuation and nothing re-reads it until
        # tomorrow — the failure is a full day of stale holdings that look perfectly current. If
        # that shows up, the fix is a second attempt later in the morning, not an earlier one.
        trigger={"day_of_week": "mon-fri", "hour": 9, "minute": 30,
                 "timezone": "Europe/Amsterdam"},
        options={"coalesce": True, "misfire_grace_time": 3600},
        max_age_hours=80,
        note="Refreshes EVERY account (force=True), not just the stale ones. Amsterdam-local so "
             "APScheduler handles the DST shift. ⚠ 09:30 is early relative to AIRS's own "
             "valuation — if holdings read a day behind, this time is the first thing to check.",
    ),
    JobSpec(
        id="crm_relaties_refresh",
        label="CRM 'Alle relaties' refresh",
        fills="airs_crm_relatie (full table replace, not an accumulation)",
        cadence="Every day, 11:00 Amsterdam",
        trigger={"day_of_week": "mon-sun", "hour": 11, "minute": 0,
                 "timezone": "Europe/Amsterdam"},
        options={"coalesce": True, "misfire_grace_time": 3600},
        max_age_hours=30,
    ),
    JobSpec(
        id="table_size_sample",
        label="Database size snapshot",
        fills="table_size_sample — one row per public table",
        # ⚠ LATE IN THE DAY, AFTER EVERY OTHER JOB HAS WRITTEN. A snapshot is only attributable to a
        # day if it is taken once that day's work is done; sampled at 04:00 it would credit each
        # day's growth to the day before. 22:00 UTC sits after the 16:30 FX sync, which is the last
        # scheduled writer.
        cadence="Every day, 22:00 UTC",
        trigger={"day_of_week": "mon-sun", "hour": 22, "minute": 0, "timezone": "UTC"},
        options={"coalesce": True, "misfire_grace_time": 3600},
        max_age_hours=30,
        note="Bytes on disk per table, read from the Postgres catalog — NOT rows written. Several "
             "jobs overwrite or upsert, so rows written and growth are different questions, and a "
             "row count cannot see indexes or bloat. ⚠ Supabase Storage is not in the database and "
             "is not counted.",
    ),
    JobSpec(
        id="asset_ingest_queue",
        label="Asset ingest-queue worker (in-process)",
        fills="asset_execution mappings + asset_price for newly queued ISINs",
        cadence="Every 20 seconds",
        interval_seconds=20,
        options={"max_instances": 1, "coalesce": True, "misfire_grace_time": 30},
        # ⚠ NO OVERDUE THRESHOLD. It fires three times a minute and no-ops on an empty queue, so
        # "when did it last run" is not a health question — and by default it is not registered at
        # all, because the STANDALONE worker is the default deployment.
        max_age_hours=None,
        # ⚠ THE ONE JOB THAT DOES NOT RECORD — see `records`. Three rows a minute is a write loop,
        # not a history, and its liveness already has a better answer in the queue heartbeat.
        records=False,
        optional_env="ASSET_QUEUE_INPROCESS",
        note="⚠ RUN EXACTLY ONE WORKER — this OR scripts/asset_queue_worker.py, never both. Two "
             "compete for the Yahoo throttle and reintroduce throttle-corrupted resolutions.",
    ),
)

BY_ID: dict[str, JobSpec] = {j.id: j for j in SCHEDULED_JOBS}


def registrable(env: dict[str, str]) -> list[JobSpec]:
    """The specs that SHOULD be registered given this process's environment.

    ⚠ AN OPT-IN JOB THAT IS OFF IS NOT MISSING, and the difference has to survive all the way to
    the page — otherwise the default deployment shows a permanent red row for a worker that is
    correctly running as a separate process.
    """
    def on(spec: JobSpec) -> bool:
        if spec.optional_env is None:
            return True
        return env.get(spec.optional_env, "").lower() in ("1", "true", "yes")

    return [s for s in SCHEDULED_JOBS if on(s)]
