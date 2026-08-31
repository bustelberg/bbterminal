"""A HUNG JOB MUST BE ABLE TO DIE.

⚠⚠ THE PRODUCTION FAILURE, 2026-08-31. `Refresh all portfolios` blocked inside a Playwright call,
so its worker never reached a `ctx.check()`. Cancellation here is cooperative by design, so Cancel
could only set the flag — the card read "cancelling…" with "starting…" as its last line, and three
separate mechanisms then kept it alive forever:

  * `_prune` drops TERMINAL jobs only, so the corpse stayed in the registry;
  * `attachRunningJobs` adopts every `running` job on page load, so the toast reappeared on EVERY
    visit to /management-dashboard — and only a terminal job gets the linger-and-dismiss countdown,
    so there was no way to be rid of it;
  * `find_running` matched it, so pressing the button again ATTACHED TO THE CORPSE. That is why the
    card never got past "starting…": no new run was ever starting.

The reaper writes such a job off after `STALE_SECONDS` of silence. It cannot kill the thread —
nothing can, from outside — and the summary says so.

Unit-only: real threads, a one-second staleness window, no I/O.
"""
from __future__ import annotations

import threading
import time

import pytest

import jobs as J


@pytest.fixture
def fast(monkeypatch):
    """A one-second silence window, and an empty registry either side."""
    monkeypatch.setattr(J, "STALE_SECONDS", 1)
    J._JOBS.clear()  # noqa: SLF001
    yield
    for job in list(J._JOBS.values()):  # noqa: SLF001
        job._cancel.set()  # noqa: SLF001
    J._JOBS.clear()  # noqa: SLF001


def _hang(ctx) -> str:
    """A worker that never asks whether it should stop — the shape of the real one."""
    ctx.emit("info", "starting…")
    while not ctx.job._cancel.is_set():  # noqa: SLF001  (test-only escape hatch, not ctx.check)
        time.sleep(0.02)
    return "never reached in the test"


class TestSilenceIsWrittenOff:
    def test_a_running_job_that_says_nothing_becomes_failed(self, fast):
        J.start("k", "hangs", _hang)
        assert J.listing()[0]["status"] == "running"
        time.sleep(1.1)
        row = J.listing()[0]
        assert row["status"] == "failed"
        assert "No progress" in (row["summary"] or "")

    def test_failed_not_cancelled(self, fast):
        """⚠ NOBODY ASKED FOR THIS TO STOP; it stopped answering. Filing it as a cancellation would
        put a worker's crash in the same column as a reader's decision."""
        J.start("k", "hangs", _hang)
        time.sleep(1.1)
        assert J.listing()[0]["status"] == "failed"

    def test_the_summary_does_not_pretend_the_thread_was_killed(self, fast):
        """⚠ IT IS A STATUS CHANGE, NOT A KILL — a thread blocked in a vendor call cannot be
        interrupted from outside, and a summary claiming otherwise is the kind of thing a reader
        acts on."""
        J.start("k", "hangs", _hang)
        time.sleep(1.1)
        assert "may still be running" in (J.listing()[0]["summary"] or "")

    def test_it_sets_cancel_so_a_waking_worker_unwinds(self, fast):
        job, _ = J.start("k", "hangs", _hang)
        time.sleep(1.1)
        J.listing()
        assert job.cancel_requested

    def test_a_watcher_gets_a_line_saying_why(self, fast):
        """⚠ A CARD THAT FLIPS TO `failed` WITH NOTHING TO READ is the same dead end from the other
        side. The reaper emits, so an open stream ends with an explanation."""
        job, _ = J.start("k", "hangs", _hang)
        time.sleep(1.1)
        J.listing()
        assert [e for e in job.since(0) if e["kind"] == "error"]


class TestTheNextPressStartsARealRun:
    def test_a_reaped_job_no_longer_blocks_its_own_label(self, fast):
        """⚠⚠ THE HALF THAT MATTERS MOST. `start` attaches to `find_running` instead of launching —
        right for a run in flight, fatal for a hung one. Before the reaper, every press adopted the
        corpse and nothing new ever started."""
        J.start("k", "same label", _hang)
        _job2, reused_while_hung = J.start("k", "same label", _hang)
        assert reused_while_hung is True                     # the bug, while it is still 'running'

        time.sleep(1.1)
        ran = threading.Event()
        _job3, reused_after = J.start("k", "same label", lambda _c: ran.set() or "clean")
        assert reused_after is False
        assert ran.wait(2)

    def test_a_LIVE_job_is_still_reused(self, fast):
        """⚠ THE REAPER MUST NOT BREAK IDEMPOTENCE. A job that is narrating is doing its work, and
        a second press has to attach to it — that is what stops two fills over one index."""
        def _chatty(ctx):
            for _ in range(40):
                ctx.emit("info", "still here")
                time.sleep(0.05)
            return "done"

        J.start("k", "chatty", _chatty)
        time.sleep(1.2)                                       # past STALE_SECONDS, but never silent
        _again, reused = J.start("k", "chatty", _chatty)
        assert reused is True


class TestTheClockIsSilenceNotAge:
    def test_a_long_job_that_keeps_talking_is_left_alone(self, fast):
        def _chatty(ctx):
            for _ in range(40):
                ctx.emit("progress", "working", done=1, total=40)
                time.sleep(0.05)
            return "done"

        J.start("k", "long", _chatty)
        time.sleep(1.2)
        assert J.listing()[0]["status"] == "running"

    def test_the_heartbeat_starts_at_creation(self, fast):
        """⚠ A JOB THAT DIES BEFORE ITS FIRST `emit` still has to age out. `last_event_at` starts at
        creation rather than at 0, or such a job is instantly stale — and starts at creation rather
        than never, or it lives forever."""
        job, _ = J.start("k", "silent", lambda _c: time.sleep(5) or "done")
        assert job.last_event_at >= job.created_at
        time.sleep(1.1)
        assert J.listing()[0]["status"] == "failed"
