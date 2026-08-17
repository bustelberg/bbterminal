"""Background jobs a user started, with progress a browser can watch and a cancel that works.

WHY THIS EXISTS
    Every long operation on the Management Dashboard already ran as `threading.Thread` + `Queue`,
    with the SSE endpoint draining the queue directly. That shape has one defect and it is not
    cosmetic: **the client is not attached to the work**. Disconnect — navigate away, close the
    tab, press a Cancel that aborts the fetch — and the generator stops being consumed while the
    worker thread runs happily to completion. On the S&P's fundamentals fill that is 237
    constituents x ~3 GuruFocus calls ~= 711 calls that could not be stopped by anything short of
    a redeploy.

    Putting a JOB between the two fixes that, and three other things fall out of it for free:
      * the run survives navigation and reload — the toast re-attaches instead of the work
        becoming invisible-but-still-running;
      * two tabs can watch the same run;
      * several runs can be in flight at once, each reporting separately.

⚠⚠ CANCELLATION IS COOPERATIVE, AND THAT IS WHAT MAKES IT *SAFE*.
    Nothing here kills a thread. `ctx.check()` raises `JobCancelled` at a point the WORKER chose,
    which is always a boundary where the database is already consistent — between two GuruFocus
    feeds, between two companies. A thread killed mid-write would leave exactly the half-loaded
    state this app spends so much effort refusing to display. The cost is latency: pressing Cancel
    during a feed stops the job after that feed, not instantly. That is the right trade and the UI
    says "cancelling…" rather than pretending otherwise.

⚠ A DISCONNECT IS NOT A CANCEL. Closing the stream leaves the job running, deliberately: the
    reader may just be navigating away, and silently abandoning a half-finished ingest because
    someone clicked a different page is how you get a company with statements and no estimates.
    Cancel is an explicit POST and nothing else.

⚠ IN-PROCESS, NOT A TABLE, and that is a considered limit rather than an oversight. It matches the
    assumption `ingest/phases/pipeline.py::_PIPELINE_LOCK` already makes — one instance, with
    `DISABLE_SCHEDULER=1` on any replica — so there is no migration and no second source of truth.
    A deploy mid-job loses the job, but the work dies with the process anyway, so nothing is
    orphaned; what is lost is the record, and `ingest_run` remains where durable history lives for
    the scheduled pipeline. If this ever needs to survive a restart or span replicas, that is the
    point at which it earns a table — not before.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

_log = logging.getLogger(__name__)

# How long a finished job stays readable before it is pruned. Long enough that a reader who
# stepped away still sees how it ended; short enough that the registry cannot grow without bound.
RETAIN_SECONDS = 15 * 60

TERMINAL = ("done", "failed", "cancelled")


class JobCancelled(Exception):
    """Raised by `JobCtx.check()` when cancellation was requested. The runner catches it and marks
    the job `cancelled` — a worker should let it propagate rather than swallowing it.

    ⚠ ITS MESSAGE, IF IT HAS ONE, BECOMES THE SUMMARY. A worker that stops part-way usually knows
    something the registry cannot — which account it stopped before, what it had already stored,
    what is now stale — and that is the whole reason a reader looks at a cancelled card. Raised
    bare (as `check()` does) it still reads "cancelled", so nothing had to change to keep working.
    """


@dataclass
class Job:
    id: str
    kind: str
    label: str
    status: str = "running"
    created_at: float = field(default_factory=time.time)
    ended_at: float | None = None
    done: int = 0
    total: int = 0
    summary: str | None = None
    # ⚠ EXTERNAL, METERED CALLS — NOT A REQUEST COUNT. Our own database reads are free and
    # unlimited; a GuruFocus call comes out of a finite monthly quota, and a reader deciding
    # whether to press a button again deserves to know which kind they just spent. Jobs that spend
    # nothing metered leave this at 0 and the UI shows nothing, which is why it is worth
    # distinguishing "spent none" from "does not apply" only in the sense that both render blank.
    api_calls: int = 0
    # ⚠ AN APPEND-ONLY LIST, NOT A QUEUE, AND THE DIFFERENCE IS THE WHOLE FEATURE. A Queue can be
    # drained by exactly one consumer, once — so a reader who reloads has missed everything, and a
    # second tab steals events from the first. A list plus a per-subscriber cursor gives replay and
    # any number of watchers for free.
    events: list[dict] = field(default_factory=list)
    _cancel: threading.Event = field(default_factory=threading.Event)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def terminal(self) -> bool:
        return self.status in TERMINAL

    @property
    def cancel_requested(self) -> bool:
        return self._cancel.is_set()

    def since(self, seq: int) -> list[dict]:
        """Every event after `seq`. Cheap, and safe to call from the event loop while the worker
        thread appends — the lock is held only for the slice."""
        with self._lock:
            return [e for e in self.events if e["seq"] > seq]

    def public(self) -> dict:
        return {
            "id": self.id, "kind": self.kind, "label": self.label, "status": self.status,
            "done": self.done, "total": self.total, "summary": self.summary,
            "api_calls": self.api_calls, "cancel_requested": self.cancel_requested,
            "created_at": self.created_at, "ended_at": self.ended_at,
        }


@dataclass
class JobCtx:
    """What a worker is handed. The only surface a job function needs."""

    job: Job

    @property
    def cancelled(self) -> bool:
        return self.job.cancel_requested

    def check(self) -> None:
        """⚠ CALL THIS AT EVERY BOUNDARY WHERE STOPPING IS SAFE, and nowhere else. It is the only
        thing that makes Cancel real: a worker that never checks simply runs to completion and the
        button is a lie."""
        if self.job.cancel_requested:
            raise JobCancelled

    def emit(self, kind: str, message: str, **data: Any) -> None:
        """One progress line. `done`/`total` are lifted onto the job so a re-attaching client gets
        the bar's position without replaying the whole log."""
        j = self.job
        with j._lock:  # noqa: SLF001
            seq = len(j.events) + 1
            if "done" in data:
                j.done = int(data["done"] or 0)
            if "total" in data:
                j.total = int(data["total"] or 0)
            j.events.append({"seq": seq, "kind": kind, "message": message, **data})

    def progress(self, done: int, total: int, message: str, **data: Any) -> None:
        self.emit("progress", message, done=done, total=total, **data)

    def spent(self, calls: int) -> None:
        """Record metered external calls. Additive, so a worker can report per unit of work.

        ⚠ REPORT IT EVEN WHEN THE JOB THEN FAILS. The quota is gone either way, and a failed run
        that says it cost nothing is the one that gets retried until the month's budget is.
        """
        if calls:
            with self.job._lock:  # noqa: SLF001
                self.job.api_calls += int(calls)


_JOBS: dict[str, Job] = {}
_REGISTRY_LOCK = threading.Lock()


def _prune() -> None:
    """Drop finished jobs past `RETAIN_SECONDS`. Called on every start and list, so the registry
    is tidied by use rather than by a timer nobody would remember exists."""
    cutoff = time.time() - RETAIN_SECONDS
    with _REGISTRY_LOCK:
        for jid in [j.id for j in _JOBS.values()
                    if j.terminal and (j.ended_at or 0) < cutoff]:
            _JOBS.pop(jid, None)


def find_running(kind: str, label: str) -> Job | None:
    """The live job for this exact piece of work, if there is one."""
    with _REGISTRY_LOCK:
        for j in _JOBS.values():
            if j.kind == kind and j.label == label and not j.terminal:
                return j
    return None


def start(kind: str, label: str, fn: Callable[[JobCtx], str | None]) -> tuple[Job, bool]:
    """Run `fn(ctx)` on a daemon thread. Returns `(job, reused)`.

    `fn` returns the summary line, or None. It should call `ctx.check()` wherever stopping is safe
    and let `JobCancelled` propagate.

    ⚠⚠ STARTING IS IDEMPOTENT PER (kind, label), AND WITHOUT THAT CANCEL CANNOT WORK. Nothing used
    to stop a second press launching a second identical job: two fills over the same 1,712
    constituents, sharing one global rate limiter so both crawl, and a Cancel that stops exactly one
    of them. From the outside that is a Cancel button that does nothing — the run "keeps going",
    because a different run is still going.

    It is not a hypothetical press. `PortfolioFundamentalsRefresh` only knows it has a job in flight
    from its own React state, so reopening the modal — or reloading the page — brings the button
    back reading "Refresh benchmark" while the work is still running. Pressing it again is the
    obvious thing to do and was the wrong thing to do.

    ⚠ ATTACH, DO NOT REFUSE. Returning an error would be correct and useless: the reader wants the
    thing they asked for, and it is already happening. Handing back the running job means the second
    press adopts it, the button flips to Cancel, and the UI heals itself.

    ⚠ THE KEY IS (kind, label) BECAUSE THAT IS WHAT "THE SAME WORK" MEANS HERE — two different
    companies, baskets or indices differ in `label`; the same one twice is a duplicate. A caller that
    genuinely needs concurrent same-label runs would need a distinguishing label, which is the honest
    way to express it.
    """
    _prune()
    existing = find_running(kind, label)
    if existing is not None:
        _log.info("[job] %s (%s) is already running as %s — attaching", label, kind, existing.id)
        return existing, True
    job = Job(id=uuid.uuid4().hex[:12], kind=kind, label=label)
    with _REGISTRY_LOCK:
        _JOBS[job.id] = job
    ctx = JobCtx(job)

    def _runner() -> None:
        try:
            summary = fn(ctx)
            job.summary = summary
            job.status = "done"
            ctx.emit("done", summary or "finished")
        except JobCancelled as e:
            job.status = "cancelled"
            # The worker's own account of where it stopped when it gave one — see `JobCancelled`.
            job.summary = str(e) or "cancelled — stopped at a safe point"
            # ⚠ NAMED AS AN OUTCOME, NOT AN ERROR. A cancelled job did what it was told; rendering
            # it in red beside a genuine failure teaches the reader to ignore both.
            ctx.emit("cancelled", job.summary)
        except Exception as e:  # noqa: BLE001
            _log.warning("[job] %s (%s) failed — %s: %s", label, kind, type(e).__name__, e)
            job.status = "failed"
            job.summary = f"{type(e).__name__}: {str(e)[:160]}"
            ctx.emit("error", job.summary)
        finally:
            job.ended_at = time.time()

    threading.Thread(target=_runner, name=f"job-{job.id}", daemon=True).start()
    return job, False


def get(job_id: str) -> Job | None:
    with _REGISTRY_LOCK:
        return _JOBS.get(job_id)


def cancel(job_id: str) -> Job | None:
    """Request cancellation. Idempotent; returns None for an unknown id.

    ⚠ IT ONLY *REQUESTS*. The job stops when its worker next reaches a `ctx.check()`, which is why
    the response reports `cancel_requested` rather than a status of `cancelled`."""
    job = get(job_id)
    if job is None:
        return None
    if not job.terminal:
        job._cancel.set()  # noqa: SLF001
    return job


def listing() -> list[dict]:
    """Everything still in the registry, newest first — running jobs plus recently finished ones,
    which is what lets a page reload re-attach to work already in flight."""
    _prune()
    with _REGISTRY_LOCK:
        jobs = sorted(_JOBS.values(), key=lambda j: j.created_at, reverse=True)
    return [j.public() for j in jobs]
