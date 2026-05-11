"""Criteria-driven screening endpoints.

Endpoints:
    GET    /api/universe/criteria          LongEquity criteria spec for the UI
    POST   /api/universe/screen            SSE: screen all companies
    POST   /api/universe/build             SSE: build per-month universes for a range
    GET    /api/universe/validate          compare screen results vs LongEquity

The two SSE endpoints (`screen`, `build`) share the worker-queue pattern:
push JSON-string events onto a queue from a background thread and drain
them on the async side via `drain_sse_queue`."""
from __future__ import annotations

import asyncio
import json
import queue as _queue

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from deps import supabase
from universe.criteria import CRITERIA_DESCRIPTIONS, CRITERIA_MIN_YEARS, CRITERIA_NAMES
from universe.screen import (
    build_and_store_universes,
    screen_universe,
    validate_vs_longequity,
)

from ._helpers import drain_sse_queue
from ._models import BuildUniverseRequest, ScreenRequest

router = APIRouter(tags=["universe"])


@router.get("/api/universe/criteria")
async def universe_criteria():
    """LongEquity quality criteria list with descriptions + min_years."""
    return [
        {
            "key": key,
            "label": label,
            "description": CRITERIA_DESCRIPTIONS.get(key, ""),
            "min_years": CRITERIA_MIN_YEARS.get(key, 1),
        }
        for key, label in CRITERIA_NAMES
    ]


@router.post("/api/universe/screen")
async def universe_screen(body: ScreenRequest = ScreenRequest()):
    """Screen all companies against LongEquity criteria. SSE stream."""
    as_of = body.as_of_year
    force = body.force_refresh

    def _run(q: _queue.Queue):
        resp = supabase.table("company").select(
            "company_id, gurufocus_ticker, exchange_id, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))"
        ).limit(10000).execute()
        companies = []
        for c in (resp.data or []):
            exch_info = c.pop("gurufocus_exchange", None) or {}
            country_info = exch_info.pop("country", None) or {}
            c["gurufocus_exchange"] = exch_info.get("exchange_code")
            c["country"] = country_info.get("country_name")
            companies.append(c)
        label = f" as of {as_of}" if as_of else ""
        q.put(json.dumps({"type": "progress", "message": f"Found {len(companies)} companies to screen{label}."}))

        for event in screen_universe(supabase, companies, as_of_year=as_of, force_refresh=force):
            q.put(json.dumps(event))
        q.put(None)

    async def generate():
        q: _queue.Queue = _queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, q)
        async for chunk in drain_sse_queue(q, task):
            yield chunk

    return StreamingResponse(generate(), media_type="text/event-stream")


@router.post("/api/universe/build")
async def universe_build(body: BuildUniverseRequest):
    """Build monthly universes for a date range and persist them. SSE stream."""
    start = body.start_month
    end = body.end_month
    lbl = body.label
    max_co = body.max_companies

    def _run(q: _queue.Queue):
        resp = supabase.table("company").select(
            "company_id, gurufocus_ticker, exchange_id, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))"
        ).limit(10000).execute()
        companies = []
        for c in (resp.data or []):
            exch_info = c.pop("gurufocus_exchange", None) or {}
            country_info = exch_info.pop("country", None) or {}
            c["gurufocus_exchange"] = exch_info.get("exchange_code")
            c["country"] = country_info.get("country_name")
            companies.append(c)

        for event in build_and_store_universes(supabase, companies, start, end, label=lbl, max_companies=max_co):
            q.put(json.dumps(event))
        q.put(None)

    async def generate():
        q: _queue.Queue = _queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, q)
        async for chunk in drain_sse_queue(q, task):
            yield chunk

    return StreamingResponse(generate(), media_type="text/event-stream")


@router.get("/api/universe/validate")
async def universe_validate():
    """Compare current screening results against LongEquity snapshots."""
    def _run():
        resp = supabase.table("company").select(
            "company_id, gurufocus_ticker, exchange_id, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))"
        ).limit(10000).execute()
        companies = []
        for c in (resp.data or []):
            exch_info = c.pop("gurufocus_exchange", None) or {}
            country_info = exch_info.pop("country", None) or {}
            c["gurufocus_exchange"] = exch_info.get("exchange_code")
            c["country"] = country_info.get("country_name")
            companies.append(c)

        results = []
        for event in screen_universe(supabase, companies):
            if event["type"] == "done":
                results = event["data"]["results"]
                break

        return validate_vs_longequity(supabase, results)

    return await asyncio.to_thread(_run)
