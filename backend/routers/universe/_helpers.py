"""Shared helpers for the universe routers.

Three derived-metric utilities (cutoff date / metric loader / merged-view
applier) plus one shared SSE drain pattern. The drain pattern is repeated
in /screen, /build, /derived-metrics/recompute, and /derive — they all
push JSON-string events from a background thread through a Queue and
need the same drain-with-keepalive loop on the async side."""
from __future__ import annotations

import asyncio
import queue as _queue
from datetime import date

from deps import supabase, IN_CHUNK_SIZE


def _cutoff_for_target_month(target_month: str) -> date:
    """Latest fiscal-year-end date to consider for a given target month.
    Matches the convention in screen.py: target month 'YYYY-MM' uses the
    previous calendar year's FY data → cutoff (YYYY-1)-12-31."""
    year = int(target_month[:4])
    return date(year - 1, 12, 31)


def _load_derived_metrics(
    company_ids: list[int],
    metric_codes: list[str],
) -> dict[int, list[tuple[date, dict[str, float]]]]:
    """Fetch derived metric rows for the given companies + codes.

    Returns {company_id -> [(fy_end_date, {code: value}), …]}, sorted by
    date asc. Batched in IN_CHUNK_SIZE chunks (Cloudflare 502 avoidance)."""
    out: dict[int, dict[str, dict[str, float]]] = {}  # cid -> {iso_date -> {code -> value}}
    for i in range(0, len(company_ids), IN_CHUNK_SIZE):
        batch = company_ids[i:i + IN_CHUNK_SIZE]
        resp = (
            supabase.table("metric_data")
            .select("company_id, metric_code, target_date, numeric_value")
            .in_("company_id", batch)
            .eq("source_code", "derived")
            .in_("metric_code", metric_codes)
            .limit(100000)
            .execute()
        )
        for row in (resp.data or []):
            cid = row["company_id"]
            d = row["target_date"]
            code = row["metric_code"]
            v = row["numeric_value"]
            if v is None:
                continue
            out.setdefault(cid, {}).setdefault(d, {})[code] = float(v)

    result: dict[int, list[tuple[date, dict[str, float]]]] = {}
    for cid, by_date in out.items():
        rows: list[tuple[date, dict[str, float]]] = []
        for iso, metrics in by_date.items():
            try:
                rows.append((date.fromisoformat(iso), metrics))
            except ValueError:
                continue
        rows.sort(key=lambda x: x[0])
        result[cid] = rows
    return result


def _applicable_metrics(
    rows: list[tuple[date, dict[str, float]]],
    cutoff: date,
) -> dict[str, float]:
    """Merged view of all derived metric values as of `cutoff`.

    Walks FYs in ascending order and overlays each, so later FYs overwrite
    earlier ones. Any code seen up to the cutoff is returned — needed
    because a given FY entry may not include every metric."""
    merged: dict[str, float] = {}
    for d, metrics in rows:
        if d > cutoff:
            break
        merged.update(metrics)
    return merged


async def drain_sse_queue(q: _queue.Queue, task):
    """Drain a queue of JSON-string events emitted by a background thread.

    The background `_run` pushes either a JSON string or None (sentinel).
    This async generator yields each as a `data:` SSE frame and finishes
    when the sentinel arrives or the background task is done and the
    queue is empty. A leading keepalive is yielded so the proxy doesn't
    close the connection during the worker's startup."""
    yield ": keepalive\n\n"
    while True:
        try:
            msg = await asyncio.to_thread(q.get, timeout=0.15)
        except Exception:
            if task.done():
                while not q.empty():
                    m = q.get_nowait()
                    if m is not None:
                        yield f"data: {m}\n\n"
                break
            continue
        if msg is None:
            break
        yield f"data: {msg}\n\n"
