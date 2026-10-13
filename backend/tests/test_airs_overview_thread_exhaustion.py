"""The portfolios overview must remain available while refresh work uses all threads."""
from __future__ import annotations

import asyncio

from routers import _airs_overview as overview


def test_overview_falls_back_inline_when_a_worker_cannot_start(monkeypatch):
    def no_worker(*_args, **_kwargs):
        raise RuntimeError("can't start new thread")

    expected = [{"name": "Bustelberg"}]
    monkeypatch.setattr(overview.asyncio, "to_thread", no_worker)
    monkeypatch.setattr(overview, "list_overview", lambda: expected)

    assert asyncio.run(overview.list_overview_async()) == expected


def test_overview_does_not_hide_an_unrelated_worker_failure(monkeypatch):
    def broken_worker(*_args, **_kwargs):
        raise RuntimeError("database worker failed")

    monkeypatch.setattr(overview.asyncio, "to_thread", broken_worker)

    try:
        asyncio.run(overview.list_overview_async())
    except RuntimeError as exc:
        assert str(exc) == "database worker failed"
    else:
        raise AssertionError("an unrelated RuntimeError must be visible")
