"""Single-snapshot universe model (2026-06): a universe is a frozen set as of
a date, not a per-month series. The backtest universe loader collapses any
stored membership to its latest month (`_latest_month_only`) and broadcasts
that one snapshot across every rebalance month (`broadcast_constant`) — so a
collapsed ACWI/SP500/Leonteq universe still produces a full-history backtest
from a single set. These pure functions are the core of that behavior.
"""
from __future__ import annotations

from datetime import date

from routers.momentum.backtest_stream.universe_loader import (
    _latest_month_only,
    broadcast_constant,
)


# ── _latest_month_only ──────────────────────────────────────────────────

def test_latest_month_only_keeps_newest_month():
    panel = {
        "2024-01": {1: "Tech", 2: "Energy"},
        "2026-06": {3: "Health"},
        "2025-03": {4: "Materials"},
    }
    out = _latest_month_only(panel)
    assert out == {"2026-06": {3: "Health"}}


def test_latest_month_only_passthrough_single_and_empty():
    single = {"2026-06": {1: "Tech"}}
    assert _latest_month_only(single) == single
    assert _latest_month_only({}) == {}
    assert _latest_month_only(None) is None


# ── broadcast_constant ──────────────────────────────────────────────────

def test_broadcast_constant_spreads_single_month_across_range():
    panel = {"2026-06": {1: "Tech", 2: "Energy"}}
    out = broadcast_constant(panel, date(2002, 1, 1), date(2026, 6, 1))
    months = sorted(out.keys())
    # One entry per calendar month across the whole window.
    assert months[0] == "2002-01"
    assert months[-1] == "2026-06"
    assert len(months) == (2026 - 2002) * 12 + 6
    # Every month aliases the same constituent set.
    assert out["2002-01"] == {1: "Tech", 2: "Energy"}
    assert out["2002-01"] is out["2026-06"]


def test_broadcast_constant_leaves_multi_month_unchanged():
    panel = {
        "2024-01": {1: "Tech"},
        "2024-02": {2: "Energy"},
    }
    out = broadcast_constant(panel, date(2024, 1, 1), date(2024, 12, 1))
    # >1 captured month → returned untouched (no broadcast).
    assert out == panel


def test_broadcast_constant_handles_empty_and_none():
    assert broadcast_constant(None, date(2024, 1, 1), date(2024, 2, 1)) is None
    assert broadcast_constant({}, date(2024, 1, 1), date(2024, 2, 1)) == {}
    # A single month whose set is empty is left as-is (nothing to broadcast).
    empty_set = {"2026-06": {}}
    assert broadcast_constant(empty_set, date(2024, 1, 1), date(2024, 2, 1)) == empty_set
