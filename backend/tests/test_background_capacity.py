import threading

import pytest

from common.background import BoundedExecutor
from routers import ingest_runs as I


def test_failed_worker_start_cannot_leave_a_queued_task(monkeypatch):
    pool = BoundedExecutor(1, "unit-background")
    calls = []
    with monkeypatch.context() as patch:
        def fail(_thread):
            raise RuntimeError("can't start new thread")
        patch.setattr(threading.Thread, "start", fail)
        with pytest.raises(RuntimeError, match="can't start"):
            pool.submit(lambda: calls.append("ghost"))
    assert pool.submit(lambda: calls.append("accepted")).result(timeout=2) is None
    assert calls == ["accepted"]


def test_executor_rejects_overload_and_reuses_worker_after_failure():
    pool = BoundedExecutor(1, "unit-background")
    release = threading.Event()
    first = pool.submit(lambda: release.wait(2))
    try:
        with pytest.raises(RuntimeError, match="capacity"):
            pool.submit(lambda: None)
    finally:
        release.set()
    assert first.result(timeout=2)

    def crash():
        raise ValueError("body failed")

    with pytest.raises(ValueError, match="body failed"):
        pool.submit(crash).result(timeout=2)
    assert pool.submit(lambda: 42).result(timeout=2) == 42
    assert len(pool._threads) == 1


def test_pipeline_overload_is_rejected_before_creating_a_run(monkeypatch):
    monkeypatch.setattr(I, "_ingest_slot", threading.BoundedSemaphore(1))
    assert I._ingest_slot.acquire(False)
    monkeypatch.setattr(I, "_create_run", lambda *_: pytest.fail("must reserve before inserting"))
    with pytest.raises(I.HTTPException) as exc:
        I.kick_off_refresh("price_slice", "auto")
    assert exc.value.status_code == 503


def test_failed_pipeline_start_finalizes_run_and_releases_capacity(monkeypatch):
    from ingest.phases import runlog

    monkeypatch.setattr(I, "_ingest_slot", threading.BoundedSemaphore(1))
    monkeypatch.setattr(I, "_create_run", lambda *_: 3713)
    writes = []
    monkeypatch.setattr(runlog, "_update_run", lambda rid, **kw: writes.append((rid, kw)))
    with monkeypatch.context() as patch:
        def fail(_thread):
            raise RuntimeError("can't start new thread")
        patch.setattr(threading.Thread, "start", fail)
        with pytest.raises(RuntimeError, match="can't start"):
            I.kick_off_refresh("price_slice", "auto")
    assert writes[0][0] == 3713
    assert writes[0][1]["status"] == "error"
    assert writes[0][1]["finished_at"]
    assert I._ingest_slot.acquire(False)


def test_successful_pipeline_releases_admission(monkeypatch):
    monkeypatch.setattr(I, "_ingest_slot", threading.BoundedSemaphore(1))
    monkeypatch.setattr(I, "_create_run", lambda *_: 3716)
    ran = threading.Event()
    monkeypatch.setattr(I, "_run_price_slice_pipeline_sync", lambda rid: ran.set())
    # Execute the wrapper synchronously to assert its release without polling.
    class Thread:
        def __init__(self, *, target, **_kw):
            self.target = target
        def start(self):
            self.target()
    monkeypatch.setattr(I.threading, "Thread", Thread)
    assert I.kick_off_refresh("price_slice", "auto") == 3716
    assert ran.is_set()
    assert I._ingest_slot.acquire(False)
