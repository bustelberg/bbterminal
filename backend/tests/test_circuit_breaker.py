"""The GuruFocus HTTP circuit breaker (`ingest._gurufocus_http._CircuitBreaker`).

Consolidated from three module globals + two free functions into one class —
which made it unit-testable for the first time. A controllable clock replaces
`time.time` so the cooldown math is deterministic.
"""
from __future__ import annotations

import ingest._gurufocus_http as g
from ingest._gurufocus_http import _CircuitBreaker


class _Clock:
    def __init__(self, t: float = 1000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t


def _cb(monkeypatch, clock, **kw) -> _CircuitBreaker:
    monkeypatch.setattr(g.time, "time", clock)
    return _CircuitBreaker(threshold=kw.get("threshold", 3),
                           cooldown_s=kw.get("cooldown_s", 600),
                           proxy_env_var="PROXY")


def test_stays_closed_below_threshold(monkeypatch):
    cb = _cb(monkeypatch, _Clock(), threshold=5)
    for _ in range(4):
        assert cb.note_block() is False
    assert cb.seconds_remaining() == 0.0


def test_trips_open_at_threshold(monkeypatch):
    clock = _Clock()
    cb = _cb(monkeypatch, clock, threshold=3, cooldown_s=600)
    assert cb.note_block() is False  # 1
    assert cb.note_block() is False  # 2
    assert cb.note_block() is True   # 3 → OPEN
    assert cb.seconds_remaining() == 600.0


def test_cooldown_counts_down_then_auto_resets(monkeypatch):
    clock = _Clock()
    cb = _cb(monkeypatch, clock, threshold=1, cooldown_s=600)
    assert cb.note_block() is True
    clock.t += 100
    assert cb.seconds_remaining() == 500.0
    clock.t += 600  # past the cooldown window
    assert cb.seconds_remaining() == 0.0


def test_note_success_closes_and_resets_counter(monkeypatch):
    clock = _Clock()
    cb = _cb(monkeypatch, clock, threshold=2, cooldown_s=600)
    cb.note_block()
    assert cb.note_block() is True
    assert cb.seconds_remaining() > 0

    cb.note_success()
    assert cb.seconds_remaining() == 0.0
    # Counter reset → a single fresh block doesn't re-open a threshold-2 breaker.
    assert cb.note_block() is False
