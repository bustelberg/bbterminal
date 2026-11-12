"""Bounded worker admission and per-job duplicate suppression."""
from __future__ import annotations
import logging
import os
import threading
from concurrent.futures import Future
from common.background import BoundedExecutor

_log = logging.getLogger('scheduler')

def _scheduled_worker_count() -> int:
    """Bound scheduled work more tightly on the small production container.

    Each scheduled body can own its own short-lived I/O pool.  On Railway, admitting
    three such bodies alongside the web I/O executor made the total process thread
    count exceed the runtime allowance.  Two jobs still permit a slow ingest and a
    cheap housekeeping task to overlap; additional ticks are safely recorded as
    skipped by the admission gate below.
    """
    production = bool(os.environ.get("RAILWAY_PUBLIC_DOMAIN"))
    default = "2" if production else "3"
    try:
        requested = int(os.environ.get("SCHEDULED_JOB_WORKERS", default))
    except ValueError:
        requested = int(default)
    return max(1, min(2 if production else 8, requested))


_SCHEDULED_WORKERS = _scheduled_worker_count()
_scheduled_executor = BoundedExecutor(
    max_workers=_SCHEDULED_WORKERS, thread_name_prefix="scheduled-job",
)
_scheduled_futures: dict[str, Future] = {}
_scheduled_futures_lock = threading.Lock()
_scheduled_slots = threading.BoundedSemaphore(_SCHEDULED_WORKERS)
# Health checks must still run when market-data or AIRS work occupies every slot.
_maintenance_executor = BoundedExecutor(max_workers=1, thread_name_prefix="scheduled-health")
_maintenance_slots = threading.BoundedSemaphore(1)
_MAINTENANCE_JOBS = frozenset({"job_watchdog", "table_size_sample", "job_gap_scan"})



def submit(name: str, work) -> bool:
    """Admit one scheduled task to the bounded worker pool.

    APScheduler's own threads only dispatch work; long-running bodies must not
    consume one of those threads.  Conversely, each tick must *not* create a
    fresh unbounded worker.  Returning ``False`` means an earlier copy
    is pending/running, or the process could not create a pool worker; either
    way the tick is safely dropped and logged instead of raising from the
    APScheduler callback.
    """
    # Do not leave old ticks queued behind a long-running job.  A delayed price
    # refresh is less useful than tomorrow's on-time one, and an unbounded
    # executor queue is merely unbounded threads with the failure postponed.
    maintenance = name in _MAINTENANCE_JOBS
    slots = _maintenance_slots if maintenance else _scheduled_slots
    executor = _maintenance_executor if maintenance else _scheduled_executor
    if not slots.acquire(blocking=False):
        _log.warning("[scheduler] %s not started: all %d %s workers are busy",
                     name, 1 if maintenance else _SCHEDULED_WORKERS,
                     "maintenance" if maintenance else "scheduled")
        return False

    with _scheduled_futures_lock:
        existing = _scheduled_futures.get(name)
        if existing is not None and not existing.done():
            slots.release()
            _log.warning("[scheduler] %s not started: an earlier run is still pending or running", name)
            return False
        # A completed future is no longer an admission lock.  Pop it before
        # submitting so an exception from submit cannot leave a stale entry.
        _scheduled_futures.pop(name, None)
        try:
            future = executor.submit(work)
        except RuntimeError as e:
            slots.release()
            # Uvicorn reload/interpreter shutdown can race an APScheduler
            # callback. It is an expected no-op: no work must start while the
            # process is leaving, and a traceback makes that normal lifecycle
            # event look like a failed scheduled job.
            if "after shutdown" in str(e).lower():
                _log.info("[scheduler] %s skipped: worker pool is shutting down", name)
            else:
                # A cron callback must survive a genuine worker-pool failure,
                # but retain the exception context for diagnostics.
                _log.exception("[scheduler] %s not started: worker pool unavailable: %s", name, e)
            return False
        _scheduled_futures[name] = future

    def _finished(done: Future) -> None:
        with _scheduled_futures_lock:
            # Do not remove a newer run if this callback arrives after a later
            # submission (possible only with a manually-completed fake Future
            # in tests, but the invariant costs nothing to state here).
            if _scheduled_futures.get(name) is done:
                _scheduled_futures.pop(name, None)
            slots.release()

    future.add_done_callback(_finished)
    return True


def active_jobs() -> set[str]:
    with _scheduled_futures_lock:
        return {name for name, future in _scheduled_futures.items() if not future.done()}
