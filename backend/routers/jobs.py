"""The generic transport for background jobs: list, watch, cancel.

⚠ THE TRANSPORT IS GENERIC; STARTING A JOB IS NOT, AND THAT SPLIT IS DELIBERATE. There is no
`POST /api/jobs` taking a "kind" — a job is started by the endpoint that owns the work
(`/api/benchmarks/isin/{isin}/fundamentals/ingest/job`), because only that endpoint knows what to
run, what to validate, and what it costs. A generic starter would need a registry of kinds mapping
strings to callables, which is an open door to running arbitrary work by name.

Admin-only by default: none of these paths are in `_USER_READ_PREFIXES`, and every job that exists
today spends GuruFocus quota, so only an admin can start one and only an admin can watch one.
"""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

import jobs as job_registry
from routers._sse import sse_event

router = APIRouter()

# How often the stream looks for new events. The worker is a thread and the stream is asyncio, so
# the two are bridged by polling rather than by an event the worker would have to schedule onto
# the loop. ⚠ IT IS ALSO THE DISCONNECT-DETECTION INTERVAL: `is_disconnected()` is only checked
# between sleeps, so a longer tick means a longer wait before an abandoned stream closes itself.
_TICK_SECONDS = 0.15


class JobView(BaseModel):
    id: str
    kind: str
    label: str
    status: str                 # running | done | failed | cancelled
    done: int = 0
    total: int = 0
    summary: str | None = None
    # Metered external calls this job spent (GuruFocus quota today). 0 means none were spent —
    # a refusal, or every feed served from cache — and the UI shows nothing rather than "0".
    api_calls: int = 0
    # ⚠ SEPARATE FROM `status`. Cancellation is cooperative — this flips the moment Cancel is
    # pressed, while `status` only becomes "cancelled" when the worker actually reaches a safe
    # stopping point. A UI that read only `status` would look like the button did nothing.
    cancel_requested: bool = False
    created_at: float
    ended_at: float | None = None


@router.get("/api/jobs", response_model=list[JobView])
async def list_jobs():
    """Running jobs plus recently finished ones (15 minutes).

    This is what a page reload re-attaches to: without it, a job started before a refresh keeps
    running with nothing on screen to say so.
    """
    return job_registry.listing()


@router.post("/api/jobs/{job_id}/cancel", response_model=JobView)
async def cancel_job(job_id: str):
    """Ask a job to stop at its next safe point.

    ⚠ 200 WITH `cancel_requested`, NOT A PROMISE THAT IT STOPPED. The worker halts between units of
    work — between two GuruFocus feeds — so a job mid-feed keeps going for a few seconds. Reporting
    it as already cancelled would make the row disappear while its API calls were still in flight.
    """
    job = job_registry.cancel(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="No such job (it may have been pruned)")
    return job.public()


@router.get("/api/jobs/{job_id}/stream")
async def stream_job(job_id: str, request: Request, after: int = 0):
    """SSE: every event after `after`, then the live tail, then close.

    ⚠ `after` IS WHAT MAKES THIS RE-ATTACHABLE. A reconnecting client passes the last sequence it
    saw and gets the gap, so a reload — or a second tab — shows the run's history rather than
    joining mid-sentence with no idea what came before.

    ⚠ A DISCONNECT DOES NOT CANCEL. Closing this stream stops the reporting and nothing else; see
    the module docstring in `jobs.py`. Cancel is an explicit POST.
    """
    job = job_registry.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="No such job (it may have been pruned)")

    async def _gen():
        seen = after
        # The job's own header first, so a client that attaches late knows what it is watching
        # before any progress line arrives.
        yield sse_event({"type": "job", **job.public()})
        while True:
            for e in job.since(seen):
                seen = e["seq"]
                # ⚠ EVERY FRAME CARRIES A `type`, so the client switches on one field. The job
                # header and a progress line are different shapes; letting the consumer infer
                # which it got from whichever keys happen to be present is how a new field
                # silently changes the branch taken.
                yield sse_event({"type": "event", **e})
            if job.terminal:
                # ⚠ THE TERMINAL CHECK COMES *AFTER* THE DRAIN, or the last event — the one that
                # says how it ended — is the one the client never receives.
                yield sse_event({"type": "job", **job.public()})
                return
            if await request.is_disconnected():
                return
            await asyncio.sleep(_TICK_SECONDS)

    return StreamingResponse(_gen(), media_type="text/event-stream")
