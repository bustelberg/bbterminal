"""Regression guard for the universe lookup (audit finding M2).

`_find_universe_row` resolves a universe by `template_key` then `label`
using parameterized `.eq()` calls. It replaced an interpolated
`.or_(f"template_key.eq.{label},label.eq.{label}")` filter that let a
user-supplied `index_universe` inject PostgREST filter grammar.

The key security assertion: a crafted label containing PostgREST filter
syntax is treated as a LITERAL universe key (matches nothing), not as
filter grammar — so it can't widen the match to other rows.
"""
from __future__ import annotations

from routers.momentum.backtest_stream import universe_loader
from tests._fake_supabase import FakeSupabase


def _patch(monkeypatch, rows):
    fake = FakeSupabase(tables={"universe": rows})
    monkeypatch.setattr(universe_loader, "supabase", fake)
    return fake


def test_resolves_by_template_key_first(monkeypatch):
    _patch(monkeypatch, [
        {"universe_id": 7, "template_key": "ACWI", "label": "acwi-2026", "last_refreshed_at": "2026-06-01"},
    ])
    row = universe_loader._find_universe_row("ACWI")
    assert row is not None and row["universe_id"] == 7


def test_falls_back_to_label(monkeypatch):
    # No template_key on this row → must be found via the `label` lookup.
    _patch(monkeypatch, [
        {"universe_id": 11, "label": "SP500", "last_refreshed_at": "2026-05-01"},
    ])
    row = universe_loader._find_universe_row("SP500")
    assert row is not None and row["universe_id"] == 11


def test_missing_returns_none(monkeypatch):
    _patch(monkeypatch, [{"universe_id": 7, "template_key": "ACWI"}])
    assert universe_loader._find_universe_row("DOES_NOT_EXIST") is None


def test_injection_string_is_treated_as_literal(monkeypatch):
    """The crux of the fix: a label carrying PostgREST `.or_` grammar must
    NOT match the real ACWI row. With the old interpolated filter, a
    payload like this rewrote the boolean tree to match everything; with
    `.eq()` it's a literal key that matches nothing."""
    _patch(monkeypatch, [
        {"universe_id": 7, "template_key": "ACWI", "label": "acwi-2026"},
    ])
    malicious = "ACWI),label.not.is.null,(label.eq.x"
    assert universe_loader._find_universe_row(malicious) is None
