"""Scheduled workers implement the same context protocol as user jobs."""
import logging
from scheduler_runtime.execution import Cancelled

_log = logging.getLogger('scheduler')

class LogContext:
    """The `ctx` `fill_company_ids` expects, for a run with nobody watching.

     THE TICK HAS NO JOB CONTEXT. Bodies are handed `ctx=None` on a schedule (see `_reporter`) and
    `fill_company_ids` calls `ctx.emit(...)` throughout — so a tick would die on its first narration
    line having done nothing, and the failure would read as a fundamentals problem rather than a
    plumbing one. This turns those lines into log lines.
    """

    def __init__(self, cancelled=None, step=None, label=""):
        self._cancelled = cancelled or (lambda: False)
        #  The same lines now go to the toast as well, when somebody is watching. This class was
        # written for the tick, where `ctx is None` and the log is the only destination — and it
        # was then reused unchanged for the Run-now button, where it swallowed every narration
        # line a person was standing there waiting for. `step` is None on a tick, so the schedule's
        # behaviour is unchanged.
        self._step = step
        self._label = label
        self.counts: dict[str, int] = {}

    def emit(self, kind: str, message: str = "", **_kw) -> None:
        self.counts[kind] = self.counts.get(kind, 0) + 1
        #  WARNING for the milestones, not INFO. uvicorn leaves the root logger at WARNING in
        # production, so an info line from a QUARTERLY job is invisible exactly where someone asks
        # "did it run?" — and the next chance to find out is three months away.
        context = f'{self._label} · {kind}' if self._label else kind
        if kind in ("start", "error", "done"):
            _log.warning("[benchmark_fundamentals] %s: %s", context, message)
        else:
            _log.info("[benchmark_fundamentals] %s: %s", context, message)
        if self._step is not None and message:
            #  NO done/total — this relay has no idea how many lines are coming, and inventing a
            # denominator would draw a progress bar that jumps backwards. `_reporter` takes 0/0
            # happily and the toast shows the message alone.
            self._step(0, 0, f"{self._label} · {message}" if self._label else message)

    @property
    def cancelled(self) -> bool:
        return bool(self._cancelled())

    def spent(self, calls: int) -> None:
        self.counts["calls"] = self.counts.get("calls", 0) + calls

    def progress(self, done: int, total: int, message: str = "", **_data) -> None:
        self.emit("info", message)

    def check(self) -> None:
        """Match the cancellable job-context protocol used by the shared fill.

        Scheduled runs do not have a registry ``JobCtx``, but ``fill_company_ids``
        deliberately calls ``check()`` at every safe company boundary.  Keeping
        that protocol here means the scheduled and Run-now paths exercise the
        same cancellation contract instead of the scheduled path failing before
        it can start a constituent.
        """
        if self.cancelled:
            raise Cancelled("benchmark fundamentals fill cancelled")

