"""The scheduler's bounded admission gate must not regress into Thread-per-tick."""
from __future__ import annotations

import threading
import logging
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


def test_queue_overrun_is_one_rate_limited_backpressure_summary(monkeypatch, caplog):
    monkeypatch.setattr(S, "_queue_overruns", 0)
    monkeypatch.setattr(S, "_queue_overrun_last_log", 0.0)
    event = type("Event", (), {
        "job_id": "asset_ingest_queue",
        "scheduled_run_times": [object()],
    })()
    with caplog.at_level(logging.WARNING):
        S._on_job_max_instances(event)
        S._on_job_max_instances(event)
    assert caplog.text.count("asset ingest queue is still draining") == 1


def test_queue_overrun_filter_only_hides_the_expected_apscheduler_message():
    filt = S._ExpectedQueueOverrunFilter()
    expected = logging.LogRecord("apscheduler.executors.default", logging.WARNING, "", 0,
                                 "Execution of job asset_ingest_queue skipped: maximum number of running instances reached", (), None)
    other = logging.LogRecord("apscheduler.executors.default", logging.WARNING, "", 0,
                              "Execution of job another_job skipped: maximum number of running instances reached", (), None)
    assert filt.filter(expected) is False
    assert filt.filter(other) is True
