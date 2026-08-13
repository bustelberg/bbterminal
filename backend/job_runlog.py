"""RECORDING THAT AN AUTOMATIC JOB RAN — one row in `scheduled_job_run`, per fire.

⚠⚠ A CONTEXT MANAGER, NOT A "log it at the end" CALL, AND THE DIFFERENCE IS THE WHOLE POINT. The
    interesting failures are the ones where the job does NOT reach its end: an exception, a Railway
    redeploy mid-scrape, an OOM kill. A call at the bottom of the function records only the runs
    that were never in doubt. Writing the row on ENTRY means a crashed job leaves a row stuck in
    `running`, which is exactly the evidence you want and precisely what the logger could never
    give you.

⚠⚠ IT MUST NEVER BE THE REASON A JOB FAILS. This is bookkeeping wrapped around real work — an FX
    sync that fetched every rate and then hit a dead Supabase on the way out has SUCCEEDED, and
    turning that into a failure would be the monitoring breaking the thing it monitors. Every write
    here is best-effort and swallows its own errors (loudly, to the log). The cost is that a
    database outage shows up as a missing row, i.e. as `unknown` on the overview — honest, and the
    right way round.

⚠ `skipped` IS A SUCCESS. Several of these jobs are DESIGNED to no-op — the month-end refresh wakes
    daily and acts twice a month, the asset-price refresh stands down while the ingest queue is
    live. `ok` would hide the difference between "did the work" and "correctly did nothing";
    `error` would cry wolf on healthy behaviour. It is its own status and the overview treats it as
    fine.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

_log = logging.getLogger(__name__)


class RunRecord:
    """The handle a job gets inside the `with`. Everything on it is optional."""

    def __init__(self, job_id: str, row_id: int | None) -> None:
        self.job_id = job_id
        self.row_id = row_id
        self.status: str = "ok"
        self.detail: str | None = None
        self.summary: dict[str, Any] | None = None
        self.ingest_run_id: int | None = None

    def skip(self, why: str) -> None:
        """The job woke, decided there was nothing to do, and that is a healthy outcome."""
        self.status = "skipped"
        self.detail = why

    def done(self, detail: str | None = None, **summary: Any) -> None:
        """What it did. Keyword args become the `summary` JSON — counts in the job's own terms."""
        if detail is not None:
            self.detail = detail
        if summary:
            self.summary = {**(self.summary or {}), **summary}


def _supabase():
    """Imported lazily — this module is pulled in by `scheduler`, and `deps` at import time would
    make a bookkeeping helper part of the startup import graph."""
    from deps import supabase  # noqa: PLC0415

    return supabase


@contextmanager
def record_run(job_id: str, triggered_by: str = "auto") -> Iterator[RunRecord]:
    """Record one run of `job_id`, whatever happens inside.

    ⚠ THE EXCEPTION IS RE-RAISED. This records; it does not handle. Every caller already has its own
    `try/except` that keeps a failure out of the APScheduler thread, and swallowing it here would
    take that decision away from the code that knows what a failure means.
    """
    rec = RunRecord(job_id, None)
    try:
        rows = (_supabase().table("scheduled_job_run")
                .insert({"job_id": job_id, "triggered_by": triggered_by})
                .execute().data or [])
        rec.row_id = rows[0]["id"] if rows else None
    except Exception as e:  # noqa: BLE001 — see the module note: never break the job
        _log.warning("[runlog] could not open a run row for %s: %s: %s",
                     job_id, type(e).__name__, e)

    try:
        yield rec
    except Exception as e:
        rec.status = "error"
        # ⚠ TYPE AND MESSAGE, TRUNCATED. The traceback is already in the log via the caller's
        # `_log.exception`; what the overview needs is one line it can render in a table cell.
        rec.detail = f"{type(e).__name__}: {str(e)[:400]}"
        _close(rec)
        raise
    _close(rec)


def _close(rec: RunRecord) -> None:
    if rec.row_id is None:
        # The row was never opened (see above). Say what happened anyway, so the outcome is at
        # least in the log rather than lost with the failed insert.
        _log.info("[runlog] %s finished %s (no row: the insert failed) — %s",
                  rec.job_id, rec.status, rec.detail or "")
        return
    patch: dict[str, Any] = {
        "status": rec.status,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "detail": rec.detail,
        "summary": rec.summary,
        "ingest_run_id": rec.ingest_run_id,
    }
    try:
        _supabase().table("scheduled_job_run").update(patch).eq("id", rec.row_id).execute()
    except Exception as e:  # noqa: BLE001
        # ⚠ THE ROW IS NOW STUCK IN `running`, AND THAT IS THE HONEST OUTCOME — we genuinely do not
        # know how it ended from the database's point of view. The overview reads a stale `running`
        # as "the process died mid-job", which is one of the two real explanations.
        _log.warning("[runlog] could not close run %s for %s: %s: %s",
                     rec.row_id, rec.job_id, type(e).__name__, e)
