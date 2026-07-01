"""Snapshot-diff SSE streaming.

Turns a set of "compute the current payload" builders into ONE server-held push
stream that re-emits a topic only when its payload actually changes — replacing
per-topic client polling. Each topic carries its own recompute interval (fast
while something's running, slow when idle) so a cheap status topic and a heavy
aggregation topic can share one connection without the heavy one being recomputed
on every tick.

The frame carries the topic INSIDE the data payload (`{"topic","payload"}`)
because the frontend `runSSE` only parses `data:` frames, not named `event:`
lines. `: keepalive` comments flush every tick to beat proxy idle-timeouts.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable

from fastapi import Request
from fastapi.responses import StreamingResponse

from routers._sse import sse_event, sse_keepalive

Builder = Callable[[], Awaitable[object]]
# Given the map of the latest payloads, return the min seconds between recomputes
# for a topic (so it can go fast while a run is active, slow when idle).
IntervalFn = Callable[[dict[str, object]], float]

_SSE_HEADERS = {
    "Cache-Control": "no-store",
    "Connection": "keep-alive",
    # Disable proxy response buffering (nginx / Railway) so frames flush live.
    "X-Accel-Buffering": "no",
}


def _digest(payload: object) -> str:
    return hashlib.sha1(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()


def snapshot_stream_response(
    request: Request,
    topics: dict[str, tuple[Builder, IntervalFn]],
    *,
    base_tick: float = 2.0,
    max_seconds: float = 3600.0,
) -> StreamingResponse:
    """Stream each topic's payload, re-emitting only on change. The loop wakes
    every `base_tick`s and recomputes a topic only once its own interval has
    elapsed. Ends on client disconnect or after `max_seconds` (the client
    reconnects) so a connection can't leak forever."""
    async def gen():
        last_digest: dict[str, str] = {}
        last_time: dict[str, float] = {}
        latest: dict[str, object] = {}
        loop = asyncio.get_event_loop()
        started = loop.time()
        # Establish the connection before the first (possibly slow) round.
        yield sse_keepalive()
        while True:
            if await request.is_disconnected():
                break
            now = loop.time()
            if now - started > max_seconds:
                break
            for topic, (build, interval_fn) in topics.items():
                due = topic not in last_time or (now - last_time[topic]) >= interval_fn(latest)
                if not due:
                    continue
                last_time[topic] = now
                try:
                    payload = await build()
                except Exception as e:  # noqa: BLE001 — surface, don't kill the stream
                    payload = {"error": f"{type(e).__name__}: {e}"}
                latest[topic] = payload
                d = _digest(payload)
                if last_digest.get(topic) != d:
                    last_digest[topic] = d
                    yield sse_event({"topic": topic, "payload": payload})
            yield sse_keepalive()
            await asyncio.sleep(base_tick)

    return StreamingResponse(gen(), media_type="text/event-stream", headers=_SSE_HEADERS)


def status_stream_response(
    request: Request,
    build: Builder,
    *,
    running_key: str = "running",
    topic: str = "status",
    interval: float = 1.0,
    max_seconds: float = 3600.0,
) -> StreamingResponse:
    """Stream a `{running: bool, ...}`-shaped status payload until `running`
    goes false (then close) — for the long-job maintenance buttons (market-cap /
    OpenFIGI) whose status endpoints aren't `ingest_run` rows. Emits on change."""
    async def gen():
        last: str | None = None
        loop = asyncio.get_event_loop()
        started = loop.time()
        yield sse_keepalive()
        while True:
            if await request.is_disconnected():
                break
            if loop.time() - started > max_seconds:
                break
            try:
                payload = await build()
            except Exception as e:  # noqa: BLE001
                payload = {running_key: False, "error": f"{type(e).__name__}: {e}"}
            d = _digest(payload)
            if d != last:
                last = d
                yield sse_event({"topic": topic, "payload": payload})
            if isinstance(payload, dict) and not payload.get(running_key):
                break  # job finished — close the stream
            yield sse_keepalive()
            await asyncio.sleep(interval)

    return StreamingResponse(gen(), media_type="text/event-stream", headers=_SSE_HEADERS)


def run_detail_stream_response(
    request: Request,
    build: Builder,
    *,
    interval: float = 2.0,
    max_seconds: float = 3600.0,
) -> StreamingResponse:
    """Stream a single `ingest_run`-shaped payload until it reaches a terminal
    `status` (anything other than 'running'), then close — for the transient
    'watch this job to completion' UIs. Emits only on change + a keepalive/tick."""
    async def gen():
        last: str | None = None
        loop = asyncio.get_event_loop()
        started = loop.time()
        yield sse_keepalive()
        while True:
            if await request.is_disconnected():
                break
            if loop.time() - started > max_seconds:
                break
            try:
                payload = await build()
            except Exception as e:  # noqa: BLE001
                payload = {"status": "error", "current_message": f"{type(e).__name__}: {e}"}
            d = _digest(payload)
            if d != last:
                last = d
                yield sse_event({"topic": "run", "payload": payload})
            status = payload.get("status") if isinstance(payload, dict) else None
            if status is not None and status != "running":
                break  # terminal — the caller's job is done
            yield sse_keepalive()
            await asyncio.sleep(interval)

    return StreamingResponse(gen(), media_type="text/event-stream", headers=_SSE_HEADERS)
