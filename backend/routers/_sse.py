"""Server-Sent Events frame formatting.

Single source of truth for the SSE wire format shared across every
streaming endpoint (ingest, backtest, broker scan, template refresh, …).
Previously each stream redefined its own `_emit` / `event` / `_keepalive`
helper inline, which let the format drift subtly between endpoints.

The frame grammar (see the SSE spec):
  - a data frame is `data: <payload>\\n\\n`
  - a comment/keepalive frame is `: <text>\\n\\n` (ignored by EventSource,
    but keeps proxies from timing the connection out mid-compute)
"""
from __future__ import annotations

import json
from typing import Any


def sse_event(data: dict[str, Any]) -> str:
    """Serialize a dict as an SSE `data:` frame."""
    return f"data: {json.dumps(data)}\n\n"


def sse_message(msg_type: str, message: str, **extra: Any) -> str:
    """Convenience for the common `{type, message, ...}` progress payload."""
    return sse_event({"type": msg_type, "message": message, **extra})


def sse_raw(payload: str) -> str:
    """Wrap an already-serialized JSON string as an SSE `data:` frame.

    For queue-drain loops that carry pre-serialized payloads (the producer
    json-dumped before pushing), so we don't re-encode.
    """
    return f"data: {payload}\n\n"


def sse_keepalive() -> str:
    """Keepalive comment frame — emit before long work to beat proxy timeouts."""
    return ": keepalive\n\n"
