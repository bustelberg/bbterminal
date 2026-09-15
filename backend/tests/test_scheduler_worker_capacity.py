"""The scheduler's bounded admission gate must not regress into Thread-per-tick."""
from __future__ import annotations

import threading
from concurrent.futures import Future

import scheduler as S


class _Executor:
    def __init__(self):
        self.submitted: list[Future] = []

    def submit(self, _work):
        future = Future()
        self.submitted.append(future)
        return future


def test_scheduled_work_deduplicates_a_live_job_and_releases_its_slot(monkeypatch):
    """A slow interval run costs one slot, never a new thread per subsequent tick."""
    executor = _Executor()
    monkeypatch.setattr(S, "_scheduled_executor", executor)
    monkeypatch.setattr(S, "_scheduled_futures", {})
    monkeypatch.setattr(S, "_scheduled_slots", threading.BoundedSemaphore(1))

    assert S._submit_scheduled_work("prices", lambda: None) is True
    assert S._submit_scheduled_work("prices", lambda: None) is False
    assert S._submit_scheduled_work("other", lambda: None) is False
    assert len(executor.submitted) == 1

    executor.submitted[0].set_result(None)

    assert S._submit_scheduled_work("other", lambda: None) is True
    assert len(executor.submitted) == 2
