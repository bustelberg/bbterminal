"""In-process per-template refresh status registry.

Tracks whether each `UniverseTemplate` is currently refreshing and its live
progress (message + optional pct), so the frontend can show a busy spinner
and a live progress bar regardless of WHO triggered the refresh: a user
clicking "Refresh", the scheduled month-end / daily tick, or the weekly
pipeline.

Why in-memory (not a DB table): the backend runs single-instance (see the
Deployment notes in CLAUDE.md) and EVERY refresh path — the standalone SSE
endpoint and the in-process APScheduler pipeline — executes in THIS process,
so a module-level dict guarded by a lock is shared by all of them. State
resets on restart; a refresh interrupted by a restart simply shows idle
again and the next tick re-runs it (refresh is idempotent). The frontend
polls a cheap endpoint backed by this registry rather than the heavier
`GET /api/universe-templates` summary.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone

_lock = threading.Lock()
# template_key -> {status, message, pct, started_at, finished_at, error}
_status: dict[str, dict] = {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def mark_running(template_key: str, message: str = "Starting…") -> None:
    with _lock:
        _status[template_key] = {
            "status": "running",
            "message": message,
            "pct": None,
            "started_at": _now_iso(),
            "finished_at": None,
            "error": None,
        }


def update_progress(template_key: str, message: str, pct: int | None = None) -> None:
    with _lock:
        s = _status.get(template_key)
        if s is None or s.get("status") != "running":
            s = {
                "status": "running",
                "started_at": _now_iso(),
                "finished_at": None,
                "error": None,
                "pct": None,
            }
            _status[template_key] = s
        if message:
            s["message"] = message
        if pct is not None:
            s["pct"] = pct


def mark_done(template_key: str, message: str = "Done") -> None:
    with _lock:
        s = _status.get(template_key) or {"started_at": _now_iso()}
        s.update({
            "status": "done",
            "message": message,
            "pct": 100,
            "finished_at": _now_iso(),
            "error": None,
        })
        _status[template_key] = s


def mark_error(template_key: str, error: str) -> None:
    with _lock:
        s = _status.get(template_key) or {"started_at": _now_iso()}
        s.update({
            "status": "error",
            "message": "Refresh failed",
            "finished_at": _now_iso(),
            "error": error,
        })
        _status[template_key] = s


def is_running(template_key: str) -> bool:
    with _lock:
        s = _status.get(template_key)
        return bool(s and s.get("status") == "running")


def get_all() -> dict[str, dict]:
    with _lock:
        return {k: dict(v) for k, v in _status.items()}


def tracked_refresh(template, supabase, *, extra_on_progress=None):
    """Run `template.refresh()` while keeping the registry up to date so the
    UI sees a live busy/progress state. `extra_on_progress(message, pct)` is
    also invoked per progress event (e.g. to push SSE to a connected client).
    Re-raises on failure after recording the error — callers handle reporting.
    """
    key = template.template_key
    mark_running(key, "Starting…")

    def on_progress(message: str, pct: int | None = None) -> None:
        update_progress(key, message, pct)
        if extra_on_progress is not None:
            extra_on_progress(message, pct)

    try:
        result = template.refresh(supabase, on_progress=on_progress)
        mark_done(
            key,
            f"Refreshed: {result.months_written} months "
            f"(+{result.diff.additions_count}/-{result.diff.removals_count}"
            f"/r{result.diff.renames_count})",
        )
        return result
    except Exception as e:
        mark_error(key, f"{type(e).__name__}: {e}")
        raise
