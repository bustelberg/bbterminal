"""Starting a job is idempotent per (kind, label) — and that is what makes Cancel believable.

⚠⚠ THE BUG THIS PINS PRESENTED AS "CANCEL DOESN'T WORK". Nothing stopped a second press launching a
second identical job, so two fills ran over the same 1,712 constituents, sharing one global rate
limiter — both crawling — and a Cancel stopped exactly one of them. The run visibly kept going,
because a *different* run was still going.

⚠ AND THE SECOND PRESS WAS NOT CARELESSNESS. `PortfolioFundamentalsRefresh` knew it had a job in
flight only from its own React state, so reopening the modal or reloading the page brought the
button back reading "Refresh benchmark" while the work was still running. Pressing it again was the
obvious thing to do.

⚠ ATTACH, DO NOT REFUSE. An error would be correct and useless — the reader wants the thing, and it
is already happening. Handing back the running job lets the second press adopt it, flip the button
to Cancel, and heal the UI.
"""
from __future__ import annotations

import threading
import time

import jobs as reg


def _blocker(release: threading.Event):
    """A worker that parks until told, so a job can be observed mid-flight."""
    def _work(ctx):
        while not release.wait(0.01):
            ctx.check()
        return "done"
    return _work


class TestTheSameWorkTwice:

    def test_a_second_press_attaches_to_the_running_job(self):
        release = threading.Event()
        try:
            a, reused_a = reg.start("test.kind", "ACWI", _blocker(release))
            b, reused_b = reg.start("test.kind", "ACWI", _blocker(release))
            assert reused_a is False
            assert reused_b is True
            assert a.id == b.id, "a second press started a SECOND run over the same work"
        finally:
            release.set()

    def test_the_worker_body_runs_only_once(self):
        """⚠ THE COST IS THE POINT — a duplicate is not a wasted handle, it is a second pass over
        every constituent, spending the quota twice and contending for the same rate limiter."""
        release = threading.Event()
        runs = []

        def _work(ctx):
            runs.append(1)
            while not release.wait(0.01):
                ctx.check()
            return "done"

        try:
            reg.start("test.once", "ACWI", _work)
            time.sleep(0.05)
            reg.start("test.once", "ACWI", _work)
            time.sleep(0.05)
            assert runs == [1]
        finally:
            release.set()

    def test_different_labels_are_different_work(self):
        """⚠ (kind, label) IS WHAT "THE SAME WORK" MEANS. Two indices, two companies, two baskets —
        de-duplicating on `kind` alone would make the AEX press silently adopt the ACWI run."""
        release = threading.Event()
        try:
            a, _ = reg.start("test.kind2", "ACWI", _blocker(release))
            b, reused = reg.start("test.kind2", "AEX", _blocker(release))
            assert reused is False and a.id != b.id
        finally:
            release.set()

    def test_a_FINISHED_job_does_not_block_a_new_one(self):
        """⚠ ONLY A LIVE RUN DE-DUPLICATES. Finished jobs linger in the registry for 15 minutes so a
        reader can still see how they ended — if those blocked too, the button would go dead for a
        quarter of an hour after every successful press."""
        done, _ = reg.start("test.finished", "ACWI", lambda ctx: "immediate")
        for _ in range(200):
            if done.terminal:
                break
            time.sleep(0.01)
        assert done.terminal
        release = threading.Event()
        try:
            again, reused = reg.start("test.finished", "ACWI", _blocker(release))
            assert reused is False and again.id != done.id
        finally:
            release.set()

    def test_a_cancelled_job_does_not_block_a_new_one(self):
        release = threading.Event()
        a, _ = reg.start("test.cancelled", "ACWI", _blocker(release))
        reg.cancel(a.id)
        release.set()
        for _ in range(200):
            if a.terminal:
                break
            time.sleep(0.01)
        assert a.status == "cancelled"
        b, reused = reg.start("test.cancelled", "ACWI", lambda ctx: "next")
        assert reused is False and b.id != a.id


class TestCancelStopsWhatIsActuallyRunning:

    def test_cancelling_one_label_leaves_another_alone(self):
        """The symptom, stated as a test: with duplicates possible, cancelling stopped one of two
        identical runs and the work carried on."""
        release = threading.Event()
        try:
            a, _ = reg.start("test.iso", "ACWI", _blocker(release))
            b, _ = reg.start("test.iso", "AEX", _blocker(release))
            reg.cancel(a.id)
            for _ in range(200):
                if a.terminal:
                    break
                time.sleep(0.01)
            assert a.status == "cancelled"
            assert not b.terminal, "cancelling ACWI stopped the AEX run too"
        finally:
            release.set()

    def test_find_running_only_sees_live_work(self):
        release = threading.Event()
        try:
            a, _ = reg.start("test.find", "ACWI", _blocker(release))
            assert reg.find_running("test.find", "ACWI") is a
            assert reg.find_running("test.find", "AEX") is None
            assert reg.find_running("test.other", "ACWI") is None
        finally:
            release.set()
