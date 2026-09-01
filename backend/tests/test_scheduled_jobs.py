"""The automatic-jobs overview: declared vs registered vs actually ran.

⚠⚠ THE FAILURE BEING GUARDED IS "A JOB QUIETLY STOPPED EXISTING", and every source on its own reads
as healthy while it happens. `list_scheduled_jobs()` is empty under `DISABLE_SCHEDULER`, empty
before startup, and empty of any job whose `add_job` threw — none of which is distinguishable from
an idle scheduler by looking at the list. So the declaration is the fixed point, and these tests pin
the three ways the join is allowed to disagree with it.

⚠ AND `unknown` IS PINNED AS A FIRST-CLASS OUTCOME. Six of the eight jobs leave no durable record
yet, so "did it run" has no answer for them; reporting green would be a fabrication and red would
cry wolf. Either one teaches the reader to stop reading the page, which is the only way a monitoring
surface really fails.
"""
from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone

import pytest

from routers._scheduled_jobs_status import SEVERITY, build_rows, evidence_names, summarize
from scheduled_jobs import ORPHAN_MARKER, SCHEDULED_JOBS, JobSpec, registrable

NOW = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)


def _spec(**kw) -> JobSpec:
    base = {"id": "j", "label": "J", "fills": "t", "cadence": "daily",
            "trigger": {"hour": 5}, "max_age_hours": 30}
    return JobSpec(**{**base, **kw})


def _reg(job_id: str, next_run: str | None = "2026-08-14T05:00:00+00:00") -> dict:
    return {"id": job_id, "fires": job_id, "next_run_at": next_run}


def _run(name: str, hours_ago: float, status: str = "ok", err: str | None = None) -> dict:
    return {"job_name": name, "status": status, "error_summary": err,
            "started_at": (NOW - timedelta(hours=hours_ago)).isoformat(),
            "finished_at": None}


class TestTheDeclarationIsTheFixedPoint:
    def test_every_spec_declares_exactly_one_kind_of_trigger(self):
        for s in SCHEDULED_JOBS:
            assert (s.trigger is None) != (s.interval_seconds is None), s.id

    def test_ids_are_unique(self):
        ids = [s.id for s in SCHEDULED_JOBS]
        assert len(ids) == len(set(ids))

    def test_the_prose_cadence_agrees_with_the_trigger_that_fires(self):
        """⚠ THE ONE WAY THIS FILE CAN LIE. `cadence` is written by hand for the page while
        `trigger` is what APScheduler runs; nothing else forces them to describe the same schedule,
        and a page confidently reporting a cadence nothing runs at is worse than no page."""
        for s in SCHEDULED_JOBS:
            if not s.trigger:
                continue
            hour = s.trigger.get("hour")
            assert hour is None or f"{hour:02d}:" in s.cadence, f"{s.id}: {s.cadence}"
            tz = str(s.trigger.get("timezone", ""))
            if tz == "UTC":
                assert "UTC" in s.cadence, s.id
            elif "Amsterdam" in tz:
                assert "Amsterdam" in s.cadence, s.id

    def test_the_scheduler_registers_every_declared_id(self):
        """⚠ STATIC, BECAUSE THE RUNTIME CANNOT ANSWER IT. A declared job nothing registers is
        exactly the fault this whole feature exists to surface — and it would be surfaced only on a
        live instance, at whatever hour someone happened to look."""
        import scheduler

        src = inspect.getsource(scheduler)
        for s in SCHEDULED_JOBS:
            assert f'_register("{s.id}"' in src, s.id

    def test_the_reaper_writes_the_marker_the_overview_matches_on(self):
        """⚠ ONE STRING, TWO READERS. `scheduler` stamps it and `_scheduled_jobs_status` matches
        it; a literal copied into either would stop agreeing the day someone reworded the message,
        and the overview would silently go back to calling every restart a job fault."""
        import scheduler

        src = inspect.getsource(scheduler)
        assert "ORPHAN_MARKER" in src
        assert 'f"Orphaned (backend restart' not in src, "the marker was inlined again"

    def test_the_scheduler_no_longer_hardcodes_a_cron(self):
        """The schedule must be read from the declaration, not restated beside it — a second copy
        is the thing that drifts."""
        import scheduler

        body = inspect.getsource(scheduler.register_scheduler)
        assert "CronTrigger(day_of_week=" not in body


class TestOptInJobs:
    def test_an_opt_in_job_is_absent_without_its_env(self):
        assert not [s for s in registrable({}) if s.optional_env]

    def test_and_present_with_it(self):
        opt = [s for s in SCHEDULED_JOBS if s.optional_env]
        assert opt, "the fixture assumes at least one opt-in job"
        env = {s.optional_env: "1" for s in opt}
        assert len(registrable(env)) == len(SCHEDULED_JOBS)

    def test_an_opt_in_job_that_is_off_reads_OFF_not_MISSING(self):
        """⚠ THE DEFAULT DEPLOYMENT RUNS THE STANDALONE WORKER, so a permanent red row here would
        be a false alarm on every page load — and a page with a permanent red row is a page nobody
        reads."""
        s = _spec(id="q", optional_env="ASSET_QUEUE_INPROCESS", max_age_hours=None)
        row = build_rows([s], [], [], NOW, scheduler_running=True)[0]
        assert row["status"] == "off"


class TestTheThreeWayDisagreement:
    def test_a_dead_scheduler_makes_every_job_missing(self):
        rows = build_rows([_spec()], [], [], NOW, scheduler_running=False)
        assert rows[0]["status"] == "missing"

    def test_declared_but_not_registered_is_missing(self):
        rows = build_rows([_spec()], [], [], NOW, scheduler_running=True)
        assert rows[0]["status"] == "missing"
        assert "not registered" in rows[0]["reason"]

    def test_missing_outranks_a_healthy_history(self):
        """⚠ A JOB THAT RAN FINE YESTERDAY AND IS GONE TODAY IS THE WORST ROW ON THE PAGE, not the
        best. Ordering the checks the other way would render it green off its own last success."""
        s = _spec(evidence=("j",))
        rows = build_rows([s], [], [_run("j", 1)], NOW, scheduler_running=True)
        assert rows[0]["status"] == "missing"

    def test_a_job_that_records_NOTHING_is_UNKNOWN_never_ok(self):
        """Neither a `scheduled_job_run` row nor an `ingest_run` name — nothing to read. Green
        would be a fabrication, red would cry wolf."""
        rows = build_rows([_spec(records=False)], [_reg("j")], [], NOW, scheduler_running=True)
        assert rows[0]["status"] == "unknown"
        assert rows[0]["observable"] is False
        assert "only in the logs" in rows[0]["reason"]

    def test_a_recording_job_with_no_row_yet_is_UNKNOWN_too_but_says_something_else(self):
        """⚠ TWO SHADES OF `unknown`, AND THE WORDING CARRIES THE DIFFERENCE. Above we cannot see
        the job at all; here we can — it writes a durable row — and there is nothing to see, which
        is a real gap worth chasing rather than an instrumentation hole."""
        rows = build_rows([_spec(evidence=("j",))], [_reg("j")], [], NOW, scheduler_running=True)
        assert rows[0]["status"] == "unknown"
        assert rows[0]["observable"] is True
        assert "never recorded" in rows[0]["reason"]

    def test_a_process_killed_MID_RUN_is_interrupted_not_an_error(self):
        """⚠⚠ A DEPLOY, AN OOM, OR `uvicorn --reload` IS NOT A BROKEN JOB. The reaper stamps such a
        run `error` — correct, it certainly did not finish — but the fix is "run it again", not
        "debug it", and rendering it identically to a real fault paints a red row on every local
        restart. A reader who learns to discount red rows discounts the real one too."""
        run = _run("j", 2, "error", f"{ORPHAN_MARKER} — auto-reaped on next startup.")
        rows = build_rows([_spec(evidence=("j",))], [_reg("j")], [run], NOW,
                          scheduler_running=True)
        assert rows[0]["status"] == "interrupted"
        assert "run it again" in rows[0]["reason"]

    def test_but_an_interrupted_run_does_NOT_satisfy_the_freshness_check(self):
        """⚠ THE WORK DID NOT HAPPEN. Treating it as a run would let a job interrupted months ago
        sit amber for ever while its data went stale — so past its own allowance it is `overdue`,
        which is what a reader needs to act on."""
        run = _run("j", 40 * 24, "error", f"{ORPHAN_MARKER} — auto-reaped on next startup.")
        rows = build_rows([_spec(evidence=("j",), max_age_hours=35 * 24)], [_reg("j")], [run],
                          NOW, scheduler_running=True)
        assert rows[0]["status"] == "overdue"
        assert "nothing has completed since" in rows[0]["reason"]

    def test_a_CANCELLED_run_did_not_finish_either(self):
        """⚠ THE RUN-NOW BUTTON CAN PRODUCE THIS. Somebody pressed Stop — not a fault, and
        everything written is kept, but the work did not complete, so it must not read as `ok` and
        must not satisfy the freshness check."""
        rows = build_rows([_spec(evidence=("j",))], [_reg("j")], [_run("j", 2, "cancelled")],
                          NOW, scheduler_running=True)
        assert rows[0]["status"] == "interrupted"
        assert "cancelled" in rows[0]["reason"]

    def test_an_old_cancelled_run_escalates_to_overdue(self):
        rows = build_rows([_spec(evidence=("j",), max_age_hours=30)], [_reg("j")],
                          [_run("j", 40, "cancelled")], NOW, scheduler_running=True)
        assert rows[0]["status"] == "overdue"

    def test_interrupted_outranks_unknown_but_not_a_real_error(self):
        assert SEVERITY["error"] < SEVERITY["interrupted"] < SEVERITY["unknown"]

    def test_a_failed_last_run_is_an_error_and_carries_its_summary(self):
        rows = build_rows([_spec(evidence=("j",))], [_reg("j")],
                          [_run("j", 1, "error", "GuruFocus 502")], NOW, scheduler_running=True)
        assert rows[0]["status"] == "error"
        assert "502" in rows[0]["reason"]

    def test_too_old_is_overdue(self):
        rows = build_rows([_spec(evidence=("j",), max_age_hours=30)], [_reg("j")],
                          [_run("j", 40)], NOW, scheduler_running=True)
        assert rows[0]["status"] == "overdue"

    def test_a_weekend_gap_on_a_weekday_job_is_NOT_overdue(self):
        """⚠ `max_age_hours` IS NOT THE CADENCE. A Mon–Fri job is three days idle every weekend by
        design; a threshold set to the cadence would cry wolf every Monday."""
        rows = build_rows([_spec(evidence=("j",), max_age_hours=80)], [_reg("j")],
                          [_run("j", 66)], NOW, scheduler_running=True)
        assert rows[0]["status"] == "ok"

    def test_a_job_with_no_threshold_is_never_overdue(self):
        rows = build_rows([_spec(evidence=("j",), max_age_hours=None)], [_reg("j")],
                          [_run("j", 5000)], NOW, scheduler_running=True)
        assert rows[0]["status"] == "ok"

    def test_a_SKIPPED_run_is_a_success(self):
        """⚠ SEVERAL OF THESE JOBS ARE DESIGNED TO NO-OP — the month-end refresh acts twice a
        month, the asset-price refresh stands down while the ingest queue is live. Scoring that as
        a failure would put a permanent red row on the page for healthy behaviour."""
        rows = build_rows([_spec(evidence=("j",))], [_reg("j")], [_run("j", 2, "skipped")], NOW,
                          scheduler_running=True)
        assert rows[0]["status"] == "ok"

    def test_a_fresh_row_still_RUNNING_is_running_not_a_failure(self):
        rows = build_rows([_spec(evidence=("j",))], [_reg("j")], [_run("j", 1, "running")], NOW,
                          scheduler_running=True)
        assert rows[0]["status"] == "running"

    def test_a_row_stuck_in_RUNNING_past_its_allowance_is_an_error(self):
        """⚠⚠ THE FAILURE THAT USED TO BE INVISIBLE. `record_run` writes on ENTRY precisely so a job
        killed mid-flight — redeploy, OOM, --reload — leaves this behind instead of nothing; a row
        that never closed and is long past its own cadence means the process died and nobody has
        run it since."""
        rows = build_rows([_spec(evidence=("j",), max_age_hours=30)], [_reg("j")],
                          [_run("j", 40, "running")], NOW, scheduler_running=True)
        assert rows[0]["status"] == "error"
        assert "never finished" in rows[0]["reason"]

    def test_a_fresh_successful_run_is_ok(self):
        rows = build_rows([_spec(evidence=("j",))], [_reg("j")], [_run("j", 2)], NOW,
                          scheduler_running=True)
        assert rows[0]["status"] == "ok"
        assert rows[0]["last_age_hours"] == pytest.approx(2.0)


class TestMultiNameJobs:
    def test_the_newest_run_across_ALL_of_a_jobs_names_wins(self):
        """`daily_pipeline` writes price_update THEN rebalance; either can be the last thing that
        happened, and naming one would report the job as last-run at whichever we picked."""
        s = _spec(evidence=("price_update", "rebalance"))
        rows = build_rows([s], [_reg("j")], [_run("price_update", 9), _run("rebalance", 1)],
                          NOW, scheduler_running=True)
        assert rows[0]["last_age_hours"] == pytest.approx(1.0)

    def test_a_failed_second_leg_surfaces_as_the_jobs_status(self):
        s = _spec(evidence=("price_update", "rebalance"))
        rows = build_rows([s], [_reg("j")],
                          [_run("price_update", 9), _run("rebalance", 1, "error", "boom")],
                          NOW, scheduler_running=True)
        assert rows[0]["status"] == "error"


class TestUndeclaredJobs:
    def test_the_two_sources_share_one_key_space(self):
        """`scheduled_job_run` rows arrive keyed by the JOB ID and `ingest_run` rows by a pipeline
        job_name; the router normalises both onto `job_name`, so a job is found under either."""
        s = _spec(id="fx_sync", records=True)
        rows = build_rows([s], [_reg("fx_sync")], [_run("fx_sync", 2)], NOW,
                          scheduler_running=True)
        assert rows[0]["status"] == "ok"

    def test_the_pipeline_jobs_are_observable_WITHOUT_recording(self):
        """⚠ THEY WRITE A RICHER `ingest_run` ROW ALREADY. A second, thinner record beside it would
        be two accounts of one event, free to disagree."""
        s = _spec(records=False, evidence=("price_update",))
        rows = build_rows([s], [_reg("j")], [_run("price_update", 2)], NOW, scheduler_running=True)
        assert rows[0]["observable"] is True
        assert rows[0]["status"] == "ok"

    def test_a_registered_job_nothing_declares_is_REPORTED(self):
        """⚠ NOT DROPPED. It is either a legitimate dynamic one-shot (the startup catch-up, the
        +3h stale-price retry) or the residue of a rename the declaration never learned about —
        and silently filtering it is how the page comes to describe a system it no longer matches.
        """
        rows = build_rows([], [_reg("startup_smart_kickstart")], [], NOW, scheduler_running=True)
        assert [r["id"] for r in rows] == ["startup_smart_kickstart"]
        assert rows[0]["status"] == "unknown"


class TestOrderingAndSummary:
    def test_rows_sort_worst_first(self):
        specs = [_spec(id="a", evidence=("a",)), _spec(id="b"), _spec(id="c", evidence=("c",))]
        rows = build_rows(specs, [_reg("a"), _reg("b")],
                          [_run("a", 1, "error", "x")], NOW, scheduler_running=True)
        assert [r["status"] for r in rows] == ["missing", "error", "unknown"]

    def test_unknown_outranks_ok(self):
        """A job we cannot see is a question to answer, not a state to be content with."""
        assert SEVERITY["unknown"] < SEVERITY["ok"]

    def test_the_summary_names_the_worst_row(self):
        specs = [_spec(id="a", evidence=("a",)), _spec(id="b", evidence=("b",))]
        rows = build_rows(specs, [_reg("a"), _reg("b")],
                          [_run("a", 1), _run("b", 1, "error", "x")], NOW, scheduler_running=True)
        s = summarize(rows)
        assert s["worst"] == "error"
        assert s["counts"] == {"error": 1, "ok": 1}
        assert s["total"] == 2


class TestEvidenceNames:
    def test_distinct_and_sorted(self):
        specs = [_spec(id="a", evidence=("z", "a")), _spec(id="b", evidence=("a",))]
        assert evidence_names(specs) == ["a", "z"]

    def test_a_job_with_no_evidence_contributes_nothing_to_query(self):
        assert evidence_names([_spec()]) == []

    def test_the_real_declaration_asks_for_a_handful_of_names(self):
        """⚠ THE READ IS ONE ROW PER NAME, so this is also the query count. It was a windowed
        `.limit(500)` once, and the window filled with `price_update` rows and pushed the month-end
        refresh — idle for 35 days BY DESIGN — off the end, reporting it as never recorded."""
        assert len(evidence_names(list(SCHEDULED_JOBS))) <= 6


class TestTimestamps:
    def test_a_naive_timestamp_is_read_as_UTC_not_local(self):
        """Anything that dropped the offset would otherwise move by up to two hours — enough to
        flip a job overdue and back twice a year."""
        s = _spec(evidence=("j",), max_age_hours=30)
        naive = {"job_name": "j", "status": "ok", "error_summary": None,
                 "started_at": (NOW - timedelta(hours=2)).replace(tzinfo=None).isoformat(),
                 "finished_at": None}
        rows = build_rows([s], [_reg("j")], [naive], NOW, scheduler_running=True)
        assert rows[0]["last_age_hours"] == pytest.approx(2.0)

    def test_a_paused_job_keeps_its_null_next_run(self):
        """APScheduler nulls `next_run_time` on a PAUSED job — registered, listed, and never going
        to fire again. Folding that into `registered` would hide it."""
        rows = build_rows([_spec()], [_reg("j", None)], [], NOW, scheduler_running=True)
        assert rows[0]["registered"] is True
        assert rows[0]["next_run_at"] is None


class TestATickThatNeverRanIsItsOwnVerdict:
    """⚠⚠ THE STATE THAT DID NOT EXIST UNTIL 2026-09-01, AND ITS ABSENCE IS WHY A PRODUCTION JOB
    COULD SIT 20.9 DAYS STALE WITH NOTHING TO READ. Every other status here reasons about a job
    that STARTED — `record_run` is a context manager around real work, so it cannot speak for work
    that never began. A tick lost to a misfire, or to a process that was not alive at the fire
    time, left no row at all, and this page could only report the silence as `overdue` and offer no
    cause. `missed` is written by an OBSERVER instead (`job_runlog.record_missed`) and carries the
    sentence the page used to have to guess at."""

    def test_it_renders_as_missed_and_repeats_the_recorded_reason(self):
        """⚠ THE ROW'S OWN `detail`, NEVER A STRING BUILT HERE. It names the fire time and the
        moment the process actually started — facts only the writer had. Re-deriving a reason at
        render time would be a second, poorer answer to a question already answered."""
        why = ("no run recorded for the 2026-09-01 05:00 UTC tick — this process did not start "
               "until 2026-09-01 07:12 UTC, so the scheduler was not alive to fire it")
        row = _run("j", 1, "missed")
        row["detail"] = why
        rows = build_rows([_spec(evidence=("j",))], [_reg("j")], [row], NOW,
                          scheduler_running=True)
        assert rows[0]["status"] == "missed"
        assert rows[0]["reason"] == why

    def test_a_missed_tick_cannot_satisfy_the_freshness_check(self):
        """⚠ NO WORK HAPPENED, so past its own allowance the job is genuinely late and says so —
        the same rule `interrupted` and `cancelled` follow. A `missed` row counted as a run would
        turn the evidence of an outage into proof there wasn't one."""
        fresh = build_rows([_spec(evidence=("j",), max_age_hours=30)], [_reg("j")],
                           [_run("j", 5, "missed")], NOW, scheduler_running=True)
        assert fresh[0]["status"] == "missed"      # inside its allowance: named, not yet late
        late = build_rows([_spec(evidence=("j",), max_age_hours=30)], [_reg("j")],
                          [_run("j", 40, "missed")], NOW, scheduler_running=True)
        assert late[0]["status"] == "overdue"
        assert "nothing has completed in 1.7 days" in late[0]["reason"]

    def test_a_real_run_after_a_miss_wins(self):
        """⚠ `_latest_run` TAKES THE NEWEST ACROSS EVERY NAME, which is what makes it safe for a
        job's id to be a lookup name even when it proves itself through `ingest_run`. A run and a
        miss are different events, not two accounts of one."""
        miss = _run("j", 6, "missed")
        rows = build_rows([_spec(evidence=("j",))], [_reg("j")], [miss, _run("j", 1)], NOW,
                          scheduler_running=True)
        assert rows[0]["status"] == "ok"

    def test_a_pipeline_job_is_looked_up_under_its_OWN_id_as_well(self):
        """⚠⚠ THE HALF THAT WOULD HAVE MADE THE WHOLE FEATURE A NO-OP ON THE JOB THAT NEEDED IT.
        `daily_pipeline` is `records=False` — it proves itself through `ingest_run` — so `names`
        used to exclude its id entirely. But a missed tick can only ever be recorded under the id
        (there is no phase history for work that never started), so the gap scan's evidence for the
        very job measured 20.9 days stale would have been written and then never read."""
        s = _spec(records=False, evidence=("price_update",))
        row = _run(s.id, 1, "missed")
        row["detail"] = "the 05:00 tick never ran"
        rows = build_rows([s], [_reg("j")], [row], NOW, scheduler_running=True)
        assert rows[0]["status"] == "missed"
        assert rows[0]["reason"] == "the 05:00 tick never ran"

    def test_but_the_queue_worker_is_still_UNOBSERVABLE_rather_than_accused(self):
        """⚠⚠ THE GATE ON THAT WIDENING. `asset_ingest_queue` is `records=False` with no evidence
        AND an interval trigger — genuinely unobservable, and the gap scan skips it because a job
        that fires every 20 seconds is DESIGNED to be absent whenever the process is. Adding its id
        unconditionally would move it from "leaves no durable record" to "never recorded — a real
        gap": a fabricated accusation against the one job that can never answer it."""
        rows = build_rows([_spec(records=False, trigger=None, interval_seconds=20)], [_reg("j")], [], NOW,
                          scheduler_running=True)
        assert rows[0]["status"] == "unknown"
        assert rows[0]["observable"] is False
        assert "only in the logs" in rows[0]["reason"]
