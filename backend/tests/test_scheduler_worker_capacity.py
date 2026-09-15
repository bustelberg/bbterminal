"""The scheduler's bounded admission gate must not regress into Thread-per-tick."""
from __future__ import annotations

import threading
import logging
from concurrent.futures import Future
from datetime import date

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


def test_queue_worker_prioritises_shared_beta_trackers(monkeypatch):
    from asset_pipeline import queue
    from routers._asset_financials import _BENCHMARK_RISK_ETF

    calls = []
    monkeypatch.setattr(queue, "process_slice", lambda **kwargs: calls.append(kwargs) or {
        "processed": 0, "ok": 0, "failed": 0, "remaining": 0,
    })

    S._fire_asset_ingest_queue()

    assert calls == [{"priority_isins": list(_BENCHMARK_RISK_ETF.values())}]


def test_cancellation_reader_accepts_the_jobs_boolean_property():
    assert S._cancel_requested(type("Ctx", (), {"cancelled": True})()) is True
    assert S._cancel_requested(type("Ctx", (), {"cancelled": False})()) is False


def test_cancellation_reader_keeps_legacy_callable_contexts_working():
    assert S._cancel_requested(type("Ctx", (), {"cancelled": lambda _self: True})()) is True


def test_relative_momentum_job_accepts_the_jobs_boolean_cancellation(monkeypatch):
    from routers.momentum import _helpers

    monkeypatch.setattr(_helpers, "latest_db_price_date", lambda: date(2026, 9, 15))
    message, detail = S._body_relative_momentum_refresh(
        type("Ctx", (), {"cancelled": True})())

    assert detail["ranked"] == 0
    assert "nothing" in message


def test_benchmark_refresh_ranks_only_after_all_benchmarks_are_refreshed(monkeypatch):
    from routers import _benchmark_refresh

    events: list[str] = []
    monkeypatch.setattr(
        _benchmark_refresh, "refresh_benchmark",
        lambda label, _emit: events.append(label) or {"priceable": 1, "prices_fetched": 1},
    )
    monkeypatch.setattr(
        S, "_body_relative_momentum_refresh",
        lambda _ctx: (events.append("ranks") or "ranked 3 universes", {"ranked": 3}),
    )

    _, detail = S._body_benchmark_price_slice()

    assert events == ["ACWI", "SP500", "AEX", "ranks"]
    assert detail["relative_momentum"] == {"ranked": 3}
