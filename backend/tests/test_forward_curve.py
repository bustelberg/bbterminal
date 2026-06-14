"""Unit tests for the snapshot-derived live extension of a scheduled
strategy's frozen backtest curve — the single mechanism that keeps
/schedule's monthly-returns + since-go-live views current with the latest
priced day.

`_walk_snapshot_curve` turns the current-picks snapshot history (which the
price-update job marks to market) into a relative equity curve;
`_splice_snapshot_tail` grafts the part past the backtest curve's end onto
it, on the backtest's cumulative-return scale.
"""
from __future__ import annotations

from routers._schedule_hydration import (
    _splice_snapshot_tail,
    _walk_snapshot_curve,
)


def _snap(kind, lpd, pct, as_of="2026-01-01", created="2026-01-01T00:00:00Z"):
    return {
        "kind": kind,
        "latest_price_date": lpd,
        "as_of_date": as_of,
        "period_return_pct": pct,
        "created_at": created,
    }


# ── _walk_snapshot_curve ────────────────────────────────────────────────

def test_walk_empty():
    assert _walk_snapshot_curve([]) == ([], None, 1.0)


def test_walk_rebalance_then_price_updates():
    # Rebalance opens a period; price_updates refresh its running return.
    snaps = [
        _snap("rebalance", "2026-05-01", 0.0, as_of="2026-05-01"),
        _snap("price_update", "2026-05-15", 3.0, as_of="2026-05-01"),
        _snap("price_update", "2026-05-29", 5.0, as_of="2026-05-01"),
    ]
    curve, last_rebal, open_start = _walk_snapshot_curve(snaps)
    assert [d for d, _ in curve] == ["2026-05-01", "2026-05-15", "2026-05-29"]
    assert abs(curve[-1][1] - 1.05) < 1e-9       # +5% running
    assert last_rebal == "2026-05-01"
    assert abs(open_start - 1.0) < 1e-9          # only one period opened


def test_walk_compounds_across_rebalances():
    # First period closes +5%, second opens and runs +10% → 1.05 * 1.10.
    snaps = [
        _snap("rebalance", "2026-04-01", 0.0, as_of="2026-04-01"),
        _snap("price_update", "2026-04-30", 5.0, as_of="2026-04-01"),
        _snap("rebalance", "2026-05-01", 0.0, as_of="2026-05-01"),
        _snap("price_update", "2026-05-29", 10.0, as_of="2026-05-01"),
    ]
    curve, _, open_start = _walk_snapshot_curve(snaps)
    assert abs(curve[-1][1] - 1.05 * 1.10) < 1e-9
    assert abs(open_start - 1.05) < 1e-9         # second period's start equity


# ── _splice_snapshot_tail ───────────────────────────────────────────────

def test_splice_empty_inputs():
    assert _splice_snapshot_tail([], []) == (None, [])
    assert _splice_snapshot_tail([("2026-05-29", 10.0)], []) == (None, [])
    assert _splice_snapshot_tail([], [("2026-06-12", 1.1)]) == (None, [])


def test_splice_no_forward_points():
    # Snapshot curve no fresher than the backtest end → nothing to splice.
    bt = [("2026-06-02", 10.0)]
    snap = [("2026-05-01", 1.0), ("2026-06-02", 1.05)]
    assert _splice_snapshot_tail(bt, snap) == (None, [])


def test_splice_grafts_relative_move_onto_curve_end():
    # Backtest ends 2026-06-02 at +10%. Snapshot curve is anchored at 06-02
    # (equity 1.05) and runs to 06-12 (equity 1.05 * 1.04 = 1.092). The
    # spliced tail should reflect the +4% RELATIVE move on top of +10%.
    bt = [("2026-05-01", 5.0), ("2026-06-02", 10.0)]
    snap = [
        ("2026-05-01", 1.00),
        ("2026-06-02", 1.05),    # anchor (level differs from backtest — ignored)
        ("2026-06-12", 1.092),   # +4% vs the anchor
    ]
    cutover, tail = _splice_snapshot_tail(bt, snap)
    assert cutover == "2026-06-12"
    assert len(tail) == 1
    assert tail[0]["date"] == "2026-06-12"
    # 1.10 * (1.092 / 1.05) - 1 = 1.10 * 1.04 - 1 = 0.144 → 14.4%
    assert abs(tail[0]["cumulative_return_pct"] - 14.4) < 1e-6


def test_splice_anchors_at_first_point_when_curve_starts_after_backtest():
    # No snapshot point on/before the backtest end → anchor at the first
    # snapshot point; only strictly-later points form the tail.
    bt = [("2026-06-02", 10.0)]
    snap = [("2026-06-05", 1.00), ("2026-06-12", 1.02)]
    cutover, tail = _splice_snapshot_tail(bt, snap)
    assert cutover == "2026-06-05"
    assert [p["date"] for p in tail] == ["2026-06-05", "2026-06-12"]
    # First tail point sits exactly at the backtest end level (+10%).
    assert abs(tail[0]["cumulative_return_pct"] - 10.0) < 1e-6
    # Second: 1.10 * (1.02 / 1.00) - 1 = 0.122 → 12.2%
    assert abs(tail[1]["cumulative_return_pct"] - 12.2) < 1e-6


def test_splice_end_to_end_from_snapshots():
    # The realistic path: walk snapshots → splice onto the backtest curve.
    bt = [("2026-05-01", 5.0), ("2026-06-02", 10.0)]
    snaps = [
        _snap("rebalance", "2026-06-01", 0.0, as_of="2026-06-01"),
        _snap("price_update", "2026-06-12", 4.0, as_of="2026-06-01"),
    ]
    snap_curve, _, _ = _walk_snapshot_curve(snaps)
    cutover, tail = _splice_snapshot_tail(bt, snap_curve)
    assert cutover == "2026-06-12"
    # Anchor = snapshot equity at/<= 06-02 → the 06-01 rebalance point (1.0).
    # 06-12 equity = 1.04 → spliced cum = 1.10 * 1.04 - 1 = 14.4%.
    assert abs(tail[-1]["cumulative_return_pct"] - 14.4) < 1e-6
