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
import threading
from collections import OrderedDict, deque
from datetime import datetime, timezone

from deps import supabase

# Minimum interval between `current_message` writes for the ACWI and
# momentum phases (which emit many events per second). Prevents
# hammering the DB while still keeping the live status fresh.
_MESSAGE_THROTTLE_SECONDS = 1.0

# ── Step log ───────────────────────────────────────────────────────
# `current_message` is ONE line, throttled to ~1/s, and each write overwrites the
# last — it is a status, not a record. It cannot answer "which companies did the
# refresh actually touch, and what did each one return", because 1,400 of those
# lines were written over the same field in the time it takes to poll it once.
# The step log is the other half: an append-only, cursor-readable transcript of a
# run that the /schedule Run-now buttons tail into the browser console.
#
# IN MEMORY, ON PURPOSE. Per-company detail is thousands of rows per run and it
# is interesting for minutes — persisting it would put a write on the hot path of
# every fetch to keep a transcript nobody reads twice. It dies with the process
# (single-instance, same assumption as `_PIPELINE_LOCK`); the durable record of
# what a run DID is still `ingest_run` + the snapshots it wrote.
_LOG_MAX_ENTRIES = 20_000     # per run — a full-price refresh is ~16k companies
_LOG_MAX_RUNS = 4             # buffers retained; oldest evicted
_log_lock = threading.Lock()
_log_buffers: "OrderedDict[int, deque[dict]]" = OrderedDict()
_log_state: dict[int, dict] = {}   # run_id → {"seq": int, "dropped": int}


# The transcript is ALSO mirrored here, so a run has a console trail wherever it
# executes — not only in the process that happens to hold the ring buffer.
# ⚠ AND THAT MATTERS MORE THAN IT SOUNDS: the buffer is per-process, so a job run
# from a script (or a backend that has since restarted) leaves NOTHING behind for
# the /log endpoint to serve. Mirroring at INFO costs nothing in production —
# uvicorn leaves the root logger at WARNING, so these are invisible until you ask
# for them (`LOG_LEVEL=INFO`, or `basicConfig(level=INFO)` in a script) — while
# warn/error mirror at WARNING and therefore DO reach the deploy log.
_step_log = logging.getLogger("ingest.steps")


def log_step(run_id: int, message: str, *, level: str = "info", phase: str | None = None) -> None:
    """Append one step to this run's transcript. Never raises — a log line must
    not be able to fail the work it describes.

    `level` is a COLOUR for the console (`info` / `warn` / `error`), not a story:
    the message carries the meaning. Cheap enough to call per company."""
    try:
        _step_log.log(
            logging.WARNING if level in ("warn", "error") else logging.INFO,
            "[run %s] %s%s", run_id, f"{phase}: " if phase else "", message,
        )
    except Exception:  # noqa: BLE001
        pass
    try:
        with _log_lock:
            buf = _log_buffers.get(run_id)
            if buf is None:
                buf = deque(maxlen=_LOG_MAX_ENTRIES)
                _log_buffers[run_id] = buf
                _log_state[run_id] = {"seq": 0, "dropped": 0}
                while len(_log_buffers) > _LOG_MAX_RUNS:
                    old, _ = _log_buffers.popitem(last=False)
                    _log_state.pop(old, None)
            _log_buffers.move_to_end(run_id)
            st = _log_state[run_id]
            st["seq"] += 1
            # A bounded deque drops its head silently. Count what fell off so the
            # reader can SAY a gap happened rather than present a truncated
            # transcript as a complete one.
            if len(buf) == buf.maxlen:
                st["dropped"] += 1
            buf.append({
                "seq": st["seq"],
                "at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "level": level,
                "phase": phase,
                "message": message,
            })
    except Exception:  # noqa: BLE001 — logging must never break the pipeline
        pass


def read_log(run_id: int, after: int = 0, limit: int = 2000) -> dict:
    """Entries with `seq > after`, oldest first, plus the cursor to ask next.

    `gap` is non-zero when `after` points at entries already evicted from the
    ring — the caller has MISSED lines, and saying so beats a silent jump."""
    with _log_lock:
        buf = _log_buffers.get(run_id)
        entries = [] if buf is None else [e for e in buf if e["seq"] > after]
        state = _log_state.get(run_id) or {"seq": 0, "dropped": 0}
        oldest = buf[0]["seq"] if buf else 0
    gap = max(0, (oldest - 1) - after) if (entries and after) else 0
    truncated = len(entries) > limit
    if truncated:
        entries = entries[:limit]
    return {
        "entries": entries,
        "next": entries[-1]["seq"] if entries else after,
        "latest": state["seq"],
        "dropped": state["dropped"],
        "gap": gap,
        "more": truncated,
    }


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
