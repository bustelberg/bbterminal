"""HOW FAST THE DATABASE IS GROWING, AND WHICH TABLES ARE DOING IT.

⚠⚠ IT MEASURES BYTES ON DISK, NOT ROWS WRITTEN, AND THAT CHOICE INVERTS THE ANSWER. The intuitive
    instrumentation is "have each job count what it inserts", and it would rank `crm_relaties_refresh`
    — which OVERWRITES its whole table, thousands of rows written and zero growth — above the
    month-end price refresh. Several of these jobs are delete-then-insert snapshots or upserts, so
    rows written and disk used are different quantities. A row count also cannot see INDEXES or
    BLOAT, which on an 18 GB table are most of the cost.

⚠ MEASURED FROM OUTSIDE EVERY JOB. Nothing here asks a job to report anything, so no job can forget
    to, none can drift out of step, and a job added next month is covered the day it ships. The
    trade is that this says WHAT grew, never WHO grew it — per-job attribution is a separate and
    lossier measurement.

⚠ POSTGRES ONLY. Supabase STORAGE (the `gurufocus-raw` bucket of cached vendor JSON) is not in the
    database. Anyone reconciling this against the hosting's disk figure will find a gap, and that
    is the gap.
"""
from __future__ import annotations

import logging

from deps import supabase

_log = logging.getLogger(__name__)

#: Rows inserted per call, chunked — one row per public table. Comfortably under any limit today
#: (~50 tables), but the chunk keeps a schema that grows from ever hitting a request cap.
_CHUNK = 200


def sample_table_sizes() -> dict:
    """Take one size snapshot of every public table. Returns a summary for the run record.

    ⚠ THE SAMPLE IS ONE `now()` FOR THE WHOLE SET — the default on `sampled_at` fires per row, so
    a slow insert would stamp the same snapshot across two timestamps and `distinct on (table_name)`
    in `table_growth` would then compare a table against a different moment than its neighbour.
    Stamped once, here, so a snapshot is a snapshot.
    """
    rows = (supabase.rpc("table_sizes").execute().data) or []
    if not rows:
        # ⚠ AN ANSWER, NOT AN ERROR — but a strange one: a database with no public tables. Recorded
        # rather than raised so the job's own row shows it happened and found nothing.
        return {"tables": 0, "total_bytes": 0, "note": "table_sizes() returned nothing"}

    from datetime import datetime, timezone  # noqa: PLC0415

    stamp = datetime.now(timezone.utc).isoformat()
    payload = [{
        "sampled_at": stamp,
        "table_name": r["table_name"],
        "total_bytes": r["total_bytes"],
        "table_bytes": r.get("table_bytes"),
        "index_bytes": r.get("index_bytes"),
        "rows_estimate": r.get("rows_estimate"),
    } for r in rows]
    for i in range(0, len(payload), _CHUNK):
        supabase.table("table_size_sample").insert(payload[i:i + _CHUNK]).execute()

    total = sum(int(r["total_bytes"] or 0) for r in rows)
    biggest = max(rows, key=lambda r: int(r["total_bytes"] or 0))
    return {
        "tables": len(rows),
        "total_bytes": total,
        "total_mb": round(total / 1_048_576, 1),
        "biggest": biggest["table_name"],
        "biggest_mb": round(int(biggest["total_bytes"] or 0) / 1_048_576, 1),
    }


def growth(days: int = 7) -> dict:
    """Per-table growth over `days`, newest-first by bytes added.

    ⚠ ONE ROW PER TABLE, COMPUTED IN POSTGRES (`table_growth`). Reading the raw samples and reducing
    them here would be ~50 tables x N days of rows — past PostgREST's silent 1,000-row cap within a
    fortnight — and the growth figure would quietly start being computed over a partial window.

    ⚠ A TABLE WITH NO BASELINE REPORTS `None`, NEVER 0. Until the history reaches back `days`, there
    is nothing to subtract from; a 0 there would present a database that has not been measured yet
    as one that has not grown.
    """
    rows = (supabase.rpc("table_growth", {"days": days}).execute().data) or []
    out = []
    for r in rows:
        latest = int(r["latest_bytes"] or 0)
        earlier = r.get("earlier_bytes")
        delta = (latest - int(earlier)) if earlier is not None else None
        out.append({
            "table": r["table_name"],
            "bytes": latest,
            "mb": round(latest / 1_048_576, 2),
            "delta_bytes": delta,
            "delta_mb": round(delta / 1_048_576, 2) if delta is not None else None,
            # ⚠ PER DAY IS THE FIGURE THAT EXTRAPOLATES, and it is divided by the window ACTUALLY
            # measured rather than by `days` — the baseline is the newest sample at-or-before the
            # cutoff, which on a sparse history can be older than asked for. Dividing by the
            # requested window would understate growth by however far off the baseline sits.
            "per_day_mb": _per_day_mb(delta, r.get("earlier_at"), r.get("latest_at")),
            "rows_estimate": r.get("rows_estimate"),
            "measured_from": r.get("earlier_at"),
            "measured_to": r.get("latest_at"),
        })
    out.sort(key=lambda x: (x["delta_bytes"] is None, -(x["delta_bytes"] or 0), -x["bytes"]))
    total = sum(x["bytes"] for x in out)
    measured = [x for x in out if x["delta_bytes"] is not None]
    return {
        "days": days,
        "tables": len(out),
        "total_bytes": total,
        "total_mb": round(total / 1_048_576, 1),
        "total_delta_mb": (round(sum(x["delta_bytes"] for x in measured) / 1_048_576, 2)
                           if measured else None),
        # ⚠ SAID OUT LOUD. With one sample the whole page is sizes and no growth, and a reader who
        # does not know that reads every "—" as "this table is not growing".
        "has_baseline": bool(measured),
        "rows": out,
    }


def _per_day_mb(delta: int | None, since: str | None, until: str | None) -> float | None:
    if delta is None or not since or not until:
        return None
    from datetime import datetime  # noqa: PLC0415

    try:
        a = datetime.fromisoformat(str(since).replace("Z", "+00:00"))
        b = datetime.fromisoformat(str(until).replace("Z", "+00:00"))
    except ValueError:
        return None
    span_days = (b - a).total_seconds() / 86400
    if span_days <= 0:
        return None
    return round((delta / 1_048_576) / span_days, 3)
