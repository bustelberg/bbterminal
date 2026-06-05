"""Run-row tracking primitives shared across pipeline phases.

`ingest_run` is the single audit/progress row for one pipeline
invocation. Every phase reports live status by writing to it via
`_update_run` (best-effort — a transient DB blip on a checkpoint must
not abort the run) and throttles its chatty `current_message` updates
through `_Throttle`. Extracted from the old monolithic
`routers/ingest_runs.py` so each phase module imports just the tracking
helpers it needs and can be unit-tested against a fake Supabase client.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from deps import supabase

# Minimum interval between `current_message` writes for the ACWI and
# momentum phases (which emit many events per second). Prevents
# hammering the DB while still keeping the live status fresh.
_MESSAGE_THROTTLE_SECONDS = 1.0


class _Throttle:
    """Wall-clock throttle for `current_message` writes. Phases create
    one per invocation; the first call always passes, subsequent calls
    skip until `min_interval` has elapsed."""
    def __init__(self, min_interval: float = _MESSAGE_THROTTLE_SECONDS):
        import time as _t
        self._time = _t
        self.min_interval = min_interval
        self.last_at = 0.0

    def should_write(self) -> bool:
        now = self._time.time()
        if now - self.last_at < self.min_interval:
            return False
        self.last_at = now
        return True


def _now_utc_iso() -> str:
    """ISO timestamp matching Supabase's timestamptz format."""
    return datetime.now(timezone.utc).isoformat()


def _create_run(job_name: str, triggered_by: str) -> int:
    resp = supabase.table("ingest_run").insert({
        "job_name": job_name,
        "triggered_by": triggered_by,
        "status": "running",
    }).execute()
    if not resp.data:
        raise RuntimeError("Failed to insert ingest_run row")
    return int(resp.data[0]["run_id"])


def _update_run(run_id: int, **fields) -> None:
    """Best-effort update. Checkpoint writes shouldn't abort the whole run
    on a transient DB blip, so we swallow + log rather than raise."""
    try:
        supabase.table("ingest_run").update(fields).eq("run_id", run_id).execute()
    except Exception as e:
        logging.getLogger(__name__).warning(
            "[ingest_run] update failed for run_id=%s: %s: %s",
            run_id, type(e).__name__, e,
        )
