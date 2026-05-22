"""Derived-metric introspection + recompute.

Endpoints:
    GET    /api/universe/derived-metrics/criteria   spec + default filter_config
    GET    /api/universe/derived-metrics/status     "how many companies have metrics"
    POST   /api/universe/derived-metrics/recompute  SSE: recompute derived metrics

Derived metrics are the quality numbers (ROIC, FCF growth, …) that the
`derive` step uses to tighten a base universe. They live in `metric_data`
under `source_code = 'derived'` and are recomputed from cached GuruFocus
annuals when this endpoint is hit."""
from __future__ import annotations

import asyncio
import json
import queue as _queue

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from deps import supabase, IN_CHUNK_SIZE

from ._helpers import drain_sse_queue
from ._models import RecomputeRequest

router = APIRouter(tags=["universe"])


@router.get("/api/universe/derived-metrics/criteria")
async def universe_derived_criteria():
    """Criterion specs + default filter_config for the /universe UI."""
    from universe.derived_metrics import CRITERIA_SPECS, default_filter_config

    specs = []
    for s in CRITERIA_SPECS:
        entry: dict = {
            "key": s.key,
            "label": s.label,
            "default_threshold": s.default_threshold,
            "default_enabled": s.default_enabled,
        }
        if s.components is not None:
            entry["components"] = [
                {"label": label, "code": code, "default": default}
                for label, code, default in s.components
            ]
        else:
            entry["metric"] = s.metric
            entry["op"] = s.op
        specs.append(entry)
    return {"specs": specs, "default_filter_config": default_filter_config()}


@router.get("/api/universe/derived-metrics/status")
async def universe_derived_status():
    """How many companies have at least one derived metric row stored."""
    def _run():
        resp = (
            supabase.table("metric_data")
            .select("company_id", count="exact")
            .eq("source_code", "derived")
            .limit(1)
            .execute()
        )
        total_rows = resp.count or 0

        # Distinct companies — pull in pages (Supabase has no DISTINCT here).
        seen: set[int] = set()
        offset = 0
        page = 1000
        while True:
            r = (
                supabase.table("metric_data")
                .select("company_id")
                .eq("source_code", "derived")
                .range(offset, offset + page - 1)
                .execute()
            )
            batch = r.data or []
            for row in batch:
                seen.add(row["company_id"])
            if len(batch) < page:
                break
            offset += page
        return {"companies_with_derived_metrics": len(seen), "total_rows": total_rows}

    return await asyncio.to_thread(_run)


@router.post("/api/universe/derived-metrics/recompute")
async def universe_derived_recompute(body: RecomputeRequest = RecomputeRequest()):
    """Recompute derived metric values from cached GuruFocus annuals. SSE."""
    from universe.derived_metrics import precompute_for_companies

    def _collect_companies() -> list[dict]:
        cid_set: set[int] = set()
        offset = 0
        page = 1000
        q = supabase.table("universe_membership").select("company_id")
        if body.universe_ids:
            q = q.in_("universe_id", body.universe_ids)
        while True:
            r = q.range(offset, offset + page - 1).execute()
            batch = r.data or []
            for row in batch:
                if row.get("company_id"):
                    cid_set.add(row["company_id"])
            if len(batch) < page:
                break
            offset += page

        ids = sorted(cid_set)
        if not ids:
            return []

        companies: list[dict] = []
        for i in range(0, len(ids), IN_CHUNK_SIZE):
            batch = ids[i:i + IN_CHUNK_SIZE]
            r = supabase.table("company").select(
                "company_id, gurufocus_ticker, company_name, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code)"
            ).in_("company_id", batch).execute()
            for c in (r.data or []):
                exch = c.pop("gurufocus_exchange", None) or {}
                c["gurufocus_exchange"] = exch.get("exchange_code")
                companies.append(c)
        return companies

    def _run(q: _queue.Queue):
        companies = _collect_companies()
        q.put(json.dumps({
            "type": "progress",
            "message": f"Found {len(companies)} companies across selected universes.",
        }))
        for event in precompute_for_companies(supabase, companies):
            q.put(json.dumps(event))
        q.put(None)

    async def generate():
        qq: _queue.Queue = _queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, qq)
        async for chunk in drain_sse_queue(qq, task):
            yield chunk

    return StreamingResponse(generate(), media_type="text/event-stream")
