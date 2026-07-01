"""Unit tests for the snapshot-diff SSE streamers (`routers._sse_stream`).

Drive the async generators directly (via `asyncio.run`) and assert the wire
behaviour: a topic is re-emitted only when its payload changes, and the status /
run streams close themselves when the job reaches a terminal state.
"""
from __future__ import annotations

import asyncio
import json

from routers._sse_stream import (
    run_detail_stream_response,
    snapshot_stream_response,
    status_stream_response,
)


class _Req:
    """Fake Starlette request: reports disconnected after N `is_disconnected`
    polls so a stream that wouldn't otherwise stop still terminates the test."""
    def __init__(self, disconnect_after: int = 1_000):
        self.n = 0
        self.disconnect_after = disconnect_after

    async def is_disconnected(self) -> bool:
        self.n += 1
        return self.n > self.disconnect_after


async def _collect(resp, limit: int = 200) -> list[dict]:
    frames: list[dict] = []
    async for chunk in resp.body_iterator:
        s = chunk if isinstance(chunk, str) else chunk.decode()
        for line in s.split("\n"):
            if line.startswith("data: "):
                frames.append(json.loads(line[6:]))
        if len(frames) >= limit:
            break
    return frames


def test_snapshot_reemits_only_on_change():
    async def run():
        counter = {"n": 0}

        async def changing():
            counter["n"] += 1
            return {"v": counter["n"]}

        async def const():
            return {"x": 1}

        topics = {
            "a": (changing, lambda _l: 0.0),
            "b": (const, lambda _l: 0.0),
        }
        req = _Req(disconnect_after=3)  # ~3 rounds
        resp = snapshot_stream_response(req, topics, base_tick=0.001, max_seconds=5)
        return await _collect(resp)

    frames = asyncio.run(run())
    a = [f for f in frames if f["topic"] == "a"]
    b = [f for f in frames if f["topic"] == "b"]
    assert len(b) == 1              # constant payload → emitted exactly once
    assert len(a) >= 2             # changing payload → re-emitted each round


def test_status_stream_stops_when_not_running():
    async def run():
        seq = [
            {"running": True, "message": "a"},
            {"running": True, "message": "b"},
            {"running": False, "message": "done"},
        ]
        i = {"n": 0}

        async def build():
            v = seq[min(i["n"], len(seq) - 1)]
            i["n"] += 1
            return v

        resp = status_stream_response(_Req(), build, interval=0.001, max_seconds=5)
        return await _collect(resp)

    frames = asyncio.run(run())
    assert frames[-1]["payload"]["running"] is False   # closed on the terminal frame
    assert not frames[-1]["payload"].get("running")


def test_run_detail_stream_stops_on_terminal_status():
    async def run():
        seq = [
            {"status": "running", "current_message": "working"},
            {"status": "ok", "prices_refreshed": 3},
        ]
        i = {"n": 0}

        async def build():
            v = seq[min(i["n"], len(seq) - 1)]
            i["n"] += 1
            return v

        resp = run_detail_stream_response(_Req(), build, interval=0.001, max_seconds=5)
        return await _collect(resp)

    frames = asyncio.run(run())
    assert frames[-1]["topic"] == "run"
    assert frames[-1]["payload"]["status"] == "ok"   # emitted then closed
