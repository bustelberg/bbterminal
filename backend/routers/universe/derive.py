"""Derived universe creation: tighten a base universe via quality filters.

Endpoints:
    POST   /api/universe/derive/preview   dry-run: row counts per month
    POST   /api/universe/derive           SSE: create a derived (tightened) universe

The preview endpoint runs the filter against an in-memory copy of the
base's memberships and returns per-month pass counts. The create
endpoint additionally precomputes any missing derived metrics, inserts
the new `universe` row, and bulk-inserts memberships in batches of 500.
"""
from __future__ import annotations

import asyncio
import json
import logging
import queue as _queue
import time

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from deps import supabase, IN_CHUNK_SIZE

from ._helpers import (
    _applicable_metrics,
    _cutoff_for_target_month,
    _load_derived_metrics,
    drain_sse_queue,
)
from ._models import DeriveUniverseRequest

router = APIRouter(tags=["universe"])


@router.post("/api/universe/derive/preview")
async def universe_derive_preview(body: DeriveUniverseRequest):
    """Count how many companies per month would survive the filter. No writes."""
    from universe.derived_metrics import company_passes, required_metric_codes

    def _run():
        base_resp = supabase.table("universe").select(
            "universe_id, label"
        ).eq("universe_id", body.base_universe_id).limit(1).execute()
        if not base_resp.data:
            raise HTTPException(status_code=404, detail="base universe not found")

        rows: list[dict] = []
        offset = 0
        page = 1000
        while True:
            r = (
                supabase.table("universe_membership")
                .select("target_month, company_id")
                .eq("universe_id", body.base_universe_id)
                .range(offset, offset + page - 1)
                .execute()
            )
            batch = r.data or []
            rows.extend(batch)
            if len(batch) < page:
                break
            offset += page

        if not rows:
            return {"monthly_counts": [], "base_rows": 0, "passed_rows": 0, "missing_metrics": 0}

        codes = required_metric_codes(body.filter_config)
        cids = sorted({r["company_id"] for r in rows})

        if not codes:
            # Nothing enabled → filter is a no-op, every base row passes.
            by_month: dict[str, int] = {}
            for r in rows:
                by_month[r["target_month"]] = by_month.get(r["target_month"], 0) + 1
            return {
                "monthly_counts": [
                    {"month": m, "count": c} for m, c in sorted(by_month.items())
                ],
                "base_rows": len(rows),
                "passed_rows": len(rows),
                "missing_metrics": 0,
                "base_label": base_resp.data[0]["label"],
            }

        metrics_by_cid = _load_derived_metrics(cids, codes)

        missing = 0
        pass_by_month: dict[str, int] = {}
        base_by_month: dict[str, int] = {}
        for row in rows:
            m = row["target_month"]
            cid = row["company_id"]
            base_by_month[m] = base_by_month.get(m, 0) + 1
            fy_rows = metrics_by_cid.get(cid, [])
            if not fy_rows:
                missing += 1
                continue
            cutoff = _cutoff_for_target_month(m)
            applicable = _applicable_metrics(fy_rows, cutoff)
            if company_passes(body.filter_config, applicable):
                pass_by_month[m] = pass_by_month.get(m, 0) + 1

        months_sorted = sorted(base_by_month.keys())
        return {
            "monthly_counts": [
                {"month": m, "count": pass_by_month.get(m, 0), "base_count": base_by_month[m]}
                for m in months_sorted
            ],
            "base_rows": len(rows),
            "passed_rows": sum(pass_by_month.values()),
            "missing_metrics": missing,
            "base_label": base_resp.data[0]["label"],
        }

    return await asyncio.to_thread(_run)


@router.post("/api/universe/derive")
async def universe_derive_create(body: DeriveUniverseRequest):
    """Create a derived (tightened) universe. SSE: precompute → filter → insert."""
    from universe.derived_metrics import (
        _fmt_duration as _fmt_dur,
        company_passes,
        precompute_for_companies,
        required_metric_codes,
    )

    def _run(q: _queue.Queue):
        def emit(step: str, status: str, message: str, **extra):
            q.put(json.dumps({
                "type": "progress", "step": step, "status": status, "message": message, **extra,
            }))

        try:
            label = (body.label or "").strip()
            if not label:
                q.put(json.dumps({"type": "error", "message": "label is required"}))
                return

            emit("validate", "in_progress", "Validating inputs...")
            base_resp = supabase.table("universe").select(
                "universe_id, label"
            ).eq("universe_id", body.base_universe_id).limit(1).execute()
            if not base_resp.data:
                q.put(json.dumps({"type": "error", "message": "base universe not found"}))
                return
            base_label = base_resp.data[0]["label"]

            dup = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
            if dup.data:
                q.put(json.dumps({"type": "error", "message": f"label '{label}' already exists"}))
                return
            emit("validate", "done", f"Base: {base_label} → new label: {label}")

            emit("load_base", "in_progress", f"Loading memberships from {base_label}...")
            rows: list[dict] = []
            offset = 0
            page = 1000
            while True:
                r = (
                    supabase.table("universe_membership")
                    .select("target_month, company_id, universe_ticker, sector")
                    .eq("universe_id", body.base_universe_id)
                    .range(offset, offset + page - 1)
                    .execute()
                )
                batch = r.data or []
                rows.extend(batch)
                if len(batch) < page:
                    break
                offset += page
            months = sorted({r["target_month"] for r in rows if r.get("target_month")})
            cids = sorted({r["company_id"] for r in rows})
            emit(
                "load_base", "done",
                f"Loaded {len(rows):,} memberships across {len(months)} months, {len(cids)} companies.",
            )

            codes = required_metric_codes(body.filter_config)
            if codes and cids:
                emit(
                    "precompute", "in_progress",
                    f"Precomputing derived metrics for {len(cids)} companies...",
                )
                companies: list[dict] = []
                for i in range(0, len(cids), IN_CHUNK_SIZE):
                    batch = cids[i:i + IN_CHUNK_SIZE]
                    r = supabase.table("company").select(
                        "company_id, gurufocus_ticker, company_name, "
                        "gurufocus_exchange:gurufocus_exchange(exchange_code)"
                    ).in_("company_id", batch).execute()
                    for c in (r.data or []):
                        exch = c.pop("gurufocus_exchange", None) or {}
                        c["gurufocus_exchange"] = exch.get("exchange_code")
                        companies.append(c)

                last_done_summary = ""
                for ev in precompute_for_companies(supabase, companies):
                    etype = ev.get("type")
                    msg = ev.get("message", "")
                    if etype == "progress_update":
                        emit("precompute", "in_progress", msg)
                    elif etype == "done":
                        last_done_summary = msg
                emit("precompute", "done", last_done_summary or "Derived metrics up to date.")
            else:
                emit("precompute", "done", "No filters enabled; skipping precompute.")

            emit("filter", "in_progress", f"Applying filter to {len(rows):,} rows...")
            metrics_by_cid = _load_derived_metrics(cids, codes) if codes else {}

            kept: list[dict] = []
            missing = 0
            for row in rows:
                cid = row["company_id"]
                if not codes:
                    kept.append(row)
                    continue
                fy_rows = metrics_by_cid.get(cid, [])
                if not fy_rows:
                    missing += 1
                    continue
                cutoff = _cutoff_for_target_month(row["target_month"])
                applicable = _applicable_metrics(fy_rows, cutoff)
                if company_passes(body.filter_config, applicable):
                    kept.append(row)
            emit(
                "filter", "done",
                f"{len(kept):,} / {len(rows):,} rows pass"
                + (f" ({missing:,} excluded for missing metrics)." if missing else "."),
            )

            if not kept:
                q.put(json.dumps({
                    "type": "error",
                    "message": "Filter matches zero rows — adjust thresholds or precompute metrics.",
                }))
                return

            emit("create", "in_progress", "Creating universe row...")
            created = supabase.table("universe").insert({
                "label": label,
                "description": body.description,
                "parent_universe_id": body.base_universe_id,
                "filter_config": body.filter_config,
            }).execute()
            new_id = created.data[0]["universe_id"]
            emit("create", "done", f"Universe created (id={new_id}).")

            # Dedup defensively on (company_id, target_month). The base may
            # carry stale duplicate rows from prior runs; the universe_membership
            # PK would reject them mid-insert otherwise.
            seen_keys: set[tuple] = set()
            payload: list[dict] = []
            dropped_dupes = 0
            for r in kept:
                key = (r["company_id"], r["target_month"])
                if key in seen_keys:
                    dropped_dupes += 1
                    continue
                seen_keys.add(key)
                payload.append({
                    "universe_id": new_id,
                    "company_id": r["company_id"],
                    "target_month": r["target_month"],
                    "universe_ticker": r.get("universe_ticker"),
                    "sector": r.get("sector"),
                })
            if dropped_dupes:
                emit(
                    "filter", "done",
                    f"{len(kept):,} rows passed filter; dropped {dropped_dupes:,} duplicate "
                    f"(company_id, target_month) row(s) before insert.",
                )
            batch_size = 500
            total_inserted = 0
            total_batches = (len(payload) + batch_size - 1) // batch_size
            insert_started = time.monotonic()
            emit(
                "insert", "in_progress",
                f"Inserting {len(payload):,} rows in {total_batches} batches...",
            )
            for bi, i in enumerate(range(0, len(payload), batch_size), start=1):
                chunk = payload[i:i + batch_size]
                elapsed = time.monotonic() - insert_started
                rate = (bi - 1) / elapsed if elapsed > 0 and bi > 1 else 0
                remaining = (total_batches - bi + 1) / rate if rate > 0 else 0
                emit(
                    "insert", "in_progress",
                    f"Starting batch {bi}/{total_batches} ({len(chunk):,} rows) · "
                    f"{_fmt_dur(elapsed)} elapsed · ~{_fmt_dur(remaining)} left",
                )
                try:
                    resp = supabase.table("universe_membership").insert(chunk).execute()
                    total_inserted += len(resp.data or [])
                except Exception as batch_exc:
                    emit(
                        "insert", "in_progress",
                        f"Batch {bi}/{total_batches} failed: {batch_exc}. Retrying once...",
                    )
                    resp = supabase.table("universe_membership").insert(chunk).execute()
                    total_inserted += len(resp.data or [])
                elapsed = time.monotonic() - insert_started
                rate = bi / elapsed if elapsed > 0 else 0
                remaining = (total_batches - bi) / rate if rate > 0 else 0
                emit(
                    "insert", "in_progress",
                    f"Batch {bi}/{total_batches} done · {total_inserted:,}/{len(payload):,} rows · "
                    f"{_fmt_dur(elapsed)} elapsed · ~{_fmt_dur(remaining)} left",
                )
            emit("insert", "done", f"Inserted {total_inserted:,} rows in {_fmt_dur(time.monotonic() - insert_started)}.")

            q.put(json.dumps({
                "type": "done",
                "message": f"Created '{label}' from {base_label} with {total_inserted:,} rows.",
                "data": {
                    "universe_id": new_id,
                    "label": label,
                    "rows_inserted": total_inserted,
                    "base_universe_id": body.base_universe_id,
                    "base_label": base_label,
                },
            }))
        except Exception as exc:
            logging.getLogger(__name__).exception("universe/derive failed")
            q.put(json.dumps({"type": "error", "message": str(exc)}))
        finally:
            q.put(None)

    async def generate():
        qq: _queue.Queue = _queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, qq)
        async for chunk in drain_sse_queue(qq, task):
            yield chunk

    return StreamingResponse(generate(), media_type="text/event-stream")
