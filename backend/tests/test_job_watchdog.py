"""The watchdog: the automatic-jobs page's own verdict, acted on.

⚠⚠ THE MEASURED SYMPTOM (prod, 2026-08-18). `daily_pipeline` read `overdue — 7.1d ago` and
`daily_price_slice` read `interrupted — 18.0d ago`, both with a perfectly healthy `next run`
beside them. The TICK was firing; the WORK was not finishing, and nothing tried again. The page had
been computing exactly that verdict for months and its only consumer was a person reading it.

⚠ WHAT THESE TESTS PIN IS THE POLICY, NOT THE HEALTH MATH — that is `_scheduled_jobs_status`, which
is pure and tested on its own. What can only be got wrong HERE is WHICH states get re-run, that a
broken job cannot be retried for ever, and that an unreadable history makes the watchdog do nothing
rather than everything.
"""
from __future__ import annotations

import pytest

import job_runlog
import scheduler as S


@pytest.fixture
def wd(monkeypatch):
    """A canned health report plus a record of every job the watchdog starts.

    ⚠ THE DURABLE HALF OF THE CAP IS STUBBED TO "nothing spent yet", so these tests exercise the
    in-process half exactly as they always did. Left unstubbed it would reach for Supabase, fail
    (as `conftest` guarantees), and be read as "cap reached" — which would turn every test in this
    file green-to-red for a reason that has nothing to do with the policy they pin.

    ⚠ `start_job_now` NOW TAKES A KEYWORD (`triggered_by`), which is the whole mechanism behind the
    durable cap: an automatic re-run has to be distinguishable from somebody pressing Run now.
    """
    started: list[str] = []
    health = {"rows": [], "running": True, "now": None, "history_error": None}

    monkeypatch.setattr(S, "job_health", lambda now=None: health)
    monkeypatch.setattr(S, "start_job_now",
                        lambda jid, **_kw: started.append(jid))
    monkeypatch.setattr(S, "JOB_BODIES", {"a": object(), "b": object(), "c": object()})
    monkeypatch.setattr(S, "_watchdog_fired", {})
    monkeypatch.setattr(S, "_WATCHDOG_STARTERS", {})
    monkeypatch.setattr(job_runlog, "watchdog_runs_today", lambda jid, today: 0)
    monkeypatch.setattr(job_runlog, "ingest_runs_today", lambda names, today: 0)
    return health, started


def _row(jid: str, status: str) -> dict:
    return {"id": jid, "label": jid, "status": status, "why": f"{status} because"}


class TestWhichStatesItHeals:
    def test_overdue_and_interrupted_are_re_run(self, wd):
        health, started = wd
        health["rows"] = [_row("a", "overdue"), _row("b", "interrupted")]

        msg, summary = S._body_job_watchdog()

        assert sorted(started) == ["a", "b"]
        assert summary["restarted"] == 2
        assert "re-ran 2/2" in msg

    @pytest.mark.parametrize("status", ["missing", "error", "unknown", "off", "running", "ok"])
    def test_nothing_else_is_touched(self, wd, status):
        """⚠⚠ `missing` IS THE TEMPTING ONE AND THE MOST IMPORTANT EXCLUSION. It means the job is
        not REGISTERED — `add_job` threw, or the scheduler is down — so running the body by hand
        turns the page green while the SCHEDULE stays broken. That is the single failure this
        monitoring surface exists to catch, and healing it would delete the evidence.

        ⚠ `error` has a recorded reason and a blind re-run repeats it far more often than it fixes
        it. `unknown` means we cannot tell whether it ran, and re-running on no evidence is how a
        vendor quota gets spent twice."""
        health, started = wd
        health["rows"] = [_row("a", status)]

        _msg, summary = S._body_job_watchdog()

        assert started == []
        assert summary["restarted"] == 0

    def test_a_healthy_fleet_says_so_and_does_nothing(self, wd):
        health, started = wd
        health["rows"] = [_row("a", "ok"), _row("b", "running")]
        msg, summary = S._body_job_watchdog()
        assert started == []
        assert "healthy" in msg and summary["broken"] == 0


class TestItCannotRetryForEver:
    def test_the_daily_cap_stops_a_permanently_broken_job(self, wd):
        """⚠⚠ WITHOUT THE CAP, A STRUCTURAL FAULT BECOMES A MACHINE THAT RETRIES IT. No GuruFocus
        quota left, AIRS credentials rotated — the job fails, stays `overdue`, and every tick fires
        it again. Two is enough to ride out a deploy or a blip and low enough that a genuine fault
        stays a fault somebody reads."""
        health, started = wd
        health["rows"] = [_row("a", "overdue")]

        for _ in range(5):
            S._body_job_watchdog()

        assert started == ["a"] * S._WATCHDOG_MAX_PER_DAY
        _msg, summary = S._body_job_watchdog()
        assert summary["capped"] == ["a"]

    def test_the_cap_is_per_job(self, wd):
        health, started = wd
        health["rows"] = [_row("a", "overdue"), _row("b", "overdue")]
        S._body_job_watchdog()
        assert sorted(started) == ["a", "b"], "one job's attempt consumed another's budget"

    def test_a_job_with_no_body_is_reported_not_started(self, wd):
        # ⚠ `start_job_now` raises KeyError on an unregistered body; the sweep must survive it and
        # say which job it could not run rather than dying on the first one.
        health, started = wd
        health["rows"] = [_row("nobody", "overdue"), _row("a", "overdue")]

        msg, summary = S._body_job_watchdog()

        assert started == ["a"]
        assert summary["unrunnable"] == ["nobody"]
        assert "no body" in msg

    def test_one_failing_start_does_not_abandon_the_sweep(self, wd, monkeypatch):
        health, started = wd
        health["rows"] = [_row("a", "overdue"), _row("b", "overdue")]

        def _start(jid: str, **_kw):
            if jid == "a":
                raise RuntimeError("registry full")
            started.append(jid)
        monkeypatch.setattr(S, "start_job_now", _start)

        _msg, summary = S._body_job_watchdog()

        assert started == ["b"]
        assert summary["restarted"] == 1


class TestNoVerdictMeansNoAction:
    def test_an_unreadable_history_does_nothing_at_all(self, wd):
        """⚠⚠ THE OPPOSITE OF SELF-HEALING. With no run history every job looks like it has never
        completed, so a watchdog that acted would re-fire the ENTIRE fleet because Supabase was
        briefly unreachable."""
        health, started = wd
        health["rows"] = [_row("a", "overdue"), _row("b", "interrupted")]
        health["history_error"] = "APIError: connection reset"

        msg, summary = S._body_job_watchdog()

        assert started == []
        assert summary["restarted"] == 0
        assert "could not read the run history" in msg


class TestThePipelineJobsItCouldNotTouch:
    """⚠⚠ THE TWO JOBS THIS WATCHDOG WAS BUILT FOR WERE THE TWO IT SKIPPED, for eleven days after
    it shipped. `daily_pipeline` and `daily_price_slice` have no `JOB_BODIES` entry —
    deliberately, because a generic "Run now" would be a worse button than the one with a live
    console tail in their own expanded row — and that same membership was gating what the watchdog
    could re-run. So they landed in `unrunnable` every sweep while the page beside them reported
    exactly the states this heals. Measured 2026-09-01: 21.0 and 31.8 days stale.

    ⚠ THE FIX SEPARATES TWO QUESTIONS THAT HAD BEEN ONE: "does the overview render a button" and
    "can this be re-run automatically". `_WATCHDOG_STARTERS` answers the second."""

    @pytest.fixture
    def pipeline(self, wd, monkeypatch):
        health, started = wd
        fired: list[str] = []
        monkeypatch.setattr(S, "_WATCHDOG_STARTERS", {
            "daily_pipeline": lambda: fired.append("daily_pipeline"),
            "daily_price_slice": lambda: fired.append("daily_price_slice"),
        })
        return health, started, fired

    def test_a_job_with_a_starter_but_no_body_is_re_run(self, pipeline):
        health, started, fired = pipeline
        health["rows"] = [_row("daily_pipeline", "overdue")]

        _msg, summary = S._body_job_watchdog()

        assert fired == ["daily_pipeline"]
        assert summary["restarted_ids"] == ["daily_pipeline"]
        assert summary["unrunnable"] == []

    def test_it_fires_the_TICK_CALLABLE_not_a_registry_job(self, pipeline):
        """⚠ THE SAME THING THE SCHEDULE DOES, byte for byte. These spawn their own daemon thread
        and narrate into `ingest_run`, which is where /schedule already watches them; wrapping them
        in a registry job would put a second progress surface on a run that has one."""
        health, started, fired = pipeline
        health["rows"] = [_row("daily_price_slice", "interrupted")]

        S._body_job_watchdog()

        assert fired == ["daily_price_slice"]
        assert started == [], "it must not go through start_job_now"

    def test_a_job_with_neither_is_still_reported_unrunnable(self, pipeline):
        """⚠ `asset_ingest_queue` IS THE REAL CASE and it must stay out: a 20-second interval worker
        cannot be overdue in any sense worth healing."""
        health, started, fired = pipeline
        health["rows"] = [_row("asset_ingest_queue", "overdue")]

        msg, summary = S._body_job_watchdog()

        assert fired == [] and started == []
        assert summary["unrunnable"] == ["asset_ingest_queue"]
        assert "no body" in msg

    def test_the_cap_binds_on_the_jobs_OWN_runs(self, pipeline, monkeypatch):
        """⚠⚠ WITHOUT THIS THE CAP WOULD NEVER BIND ON THESE TWO. They write no `scheduled_job_run`
        row at all, so the watchdog-tagged count is 0 for ever — and a host in a restart loop would
        re-fire the pipeline on every boot with a guard that could not see it had. Their own
        `ingest_run` rows are the countable thing."""
        health, started, fired = pipeline
        health["rows"] = [_row("daily_pipeline", "overdue")]
        monkeypatch.setattr(job_runlog, "ingest_runs_today",
                            lambda names, today: S._WATCHDOG_MAX_PER_DAY)

        _msg, summary = S._body_job_watchdog()

        assert fired == []
        assert summary["capped"] == ["daily_pipeline"]

    def test_an_uncountable_budget_spends_nothing(self, pipeline, monkeypatch):
        """⚠ A WATCHDOG THAT CANNOT VERIFY ITS BUDGET MUST NOT SPEND IT — least of all on the job
        that re-prices every held company. Same rule as an unreadable health report."""
        health, started, fired = pipeline
        health["rows"] = [_row("daily_pipeline", "overdue"), _row("a", "overdue")]
        monkeypatch.setattr(job_runlog, "ingest_runs_today", lambda names, today: None)
        monkeypatch.setattr(job_runlog, "watchdog_runs_today", lambda jid, today: None)

        _msg, summary = S._body_job_watchdog()

        assert fired == [] and started == []
        assert sorted(summary["capped"]) == ["a", "daily_pipeline"]

    def test_a_missed_tick_is_healed_like_any_other_break(self, pipeline):
        """⚠ `missed` IS THE CLEAREST MEMBER OF `_WATCHDOG_HEALS`: a recorded fact that nothing was
        attempted, rather than an inference from silence."""
        health, started, fired = pipeline
        health["rows"] = [_row("daily_pipeline", "missed")]

        S._body_job_watchdog()

        assert fired == ["daily_pipeline"]


class TestTheDeclarationAndTheStartersAgree:
    def test_every_starter_names_a_declared_job_that_has_no_body(self):
        """⚠⚠ A STARTER FOR A JOB THAT ALSO HAS A BODY WOULD BE A SECOND PATH INTO THE SAME WORK,
        and the watchdog prefers the body — so the starter would be dead code that looks live. A
        starter for an id nothing declares is a typo that only ever surfaces mid-sweep."""
        from scheduled_jobs import BY_ID  # noqa: PLC0415

        for jid in S._WATCHDOG_STARTERS:
            assert jid in BY_ID, f"{jid} is not a declared job"
            assert jid not in S.JOB_BODIES, f"{jid} has a body; the starter is unreachable"

    def test_every_declared_job_is_either_runnable_or_deliberately_not(self):
        """⚠ THE WHOLE FLEET, SO A NEW JOB CANNOT QUIETLY JOIN THE UNHEALABLE SET. Only the interval
        worker may be in neither map — see `_WATCHDOG_STARTERS`."""
        from scheduled_jobs import SCHEDULED_JOBS  # noqa: PLC0415

        orphans = [s.id for s in SCHEDULED_JOBS
                   if s.id not in S.JOB_BODIES and s.id not in S._WATCHDOG_STARTERS]
        assert orphans == ["asset_ingest_queue"], orphans
