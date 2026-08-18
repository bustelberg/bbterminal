"""The watchdog: the automatic-jobs page's own verdict, acted on.

⚠⚠ THE MEASURED SYMPTOM (prod, 2026-08-18). `daily_pipeline` read `overdue — 7.1d ago` and
`month_end_price_refresh` read `interrupted — 18.0d ago`, both with a perfectly healthy `next run`
beside them. The TICK was firing; the WORK was not finishing, and nothing tried again. The page had
been computing exactly that verdict for months and its only consumer was a person reading it.

⚠ WHAT THESE TESTS PIN IS THE POLICY, NOT THE HEALTH MATH — that is `_scheduled_jobs_status`, which
is pure and tested on its own. What can only be got wrong HERE is WHICH states get re-run, that a
broken job cannot be retried for ever, and that an unreadable history makes the watchdog do nothing
rather than everything.
"""
from __future__ import annotations

import pytest

import scheduler as S


@pytest.fixture
def wd(monkeypatch):
    """A canned health report plus a record of every job the watchdog starts."""
    started: list[str] = []
    health = {"rows": [], "running": True, "now": None, "history_error": None}

    monkeypatch.setattr(S, "job_health", lambda now=None: health)
    monkeypatch.setattr(S, "start_job_now", lambda jid: started.append(jid))
    monkeypatch.setattr(S, "JOB_BODIES", {"a": object(), "b": object(), "c": object()})
    monkeypatch.setattr(S, "_watchdog_fired", {})
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

        def _start(jid: str):
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
