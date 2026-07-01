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

from datetime import date

import pandas as pd

import routers._schedule_hydration as _H
import routers.momentum.backtest_crud as _bc
from routers._schedule_hydration import (
    _open_basket_live_curve,
    _returns_from_backtest,
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


def test_splice_grafts_whole_live_curve_from_its_start():
    # The live curve begins ON 2026-06-02 (its first day = the cutover) and runs
    # to 06-12 with a +4% move. The live basket ENTERS on the cutover day with no
    # return yet, so it rebases to the last backtest point STRICTLY BEFORE the
    # cutover (05-01, +5%) — the cutover-day backtest point is superseded by the
    # live curve, so its move must NOT be folded in (that's what would make MTD
    # disagree with the holdings open-period return).
    bt = [("2026-05-01", 5.0), ("2026-06-02", 10.0)]
    snap = [
        ("2026-06-02", 1.00),    # cutover; rebased to the prior backtest close (+5%)
        ("2026-06-12", 1.04),    # +4% vs the cutover
    ]
    cutover, tail = _splice_snapshot_tail(bt, snap)
    assert cutover == "2026-06-02"
    assert [p["date"] for p in tail] == ["2026-06-02", "2026-06-12"]
    assert abs(tail[0]["cumulative_return_pct"] - 5.0) < 1e-6
    # 1.05 * (1.04 / 1.00) - 1 = 0.092 → +9.2%
    assert abs(tail[1]["cumulative_return_pct"] - 9.2) < 1e-6


def test_splice_anchors_at_first_point_when_curve_starts_after_backtest():
    # Live curve starts strictly after the backtest end — the common case
    # (backtest saved before go-live). Cutover = the live curve's first day;
    # it's rebased to the backtest's last level.
    bt = [("2026-06-02", 10.0)]
    snap = [("2026-06-05", 1.00), ("2026-06-12", 1.02)]
    cutover, tail = _splice_snapshot_tail(bt, snap)
    assert cutover == "2026-06-05"
    assert [p["date"] for p in tail] == ["2026-06-05", "2026-06-12"]
    # First tail point sits exactly at the backtest end level (+10%).
    assert abs(tail[0]["cumulative_return_pct"] - 10.0) < 1e-6
    # Second: 1.10 * (1.02 / 1.00) - 1 = 0.122 → 12.2%
    assert abs(tail[1]["cumulative_return_pct"] - 12.2) < 1e-6


def test_splice_live_replaces_overlapping_backtest_tail():
    # REGRESSION: the saved backtest's horizon ran PAST go-live (through
    # 06-15), so its curve overlaps the live month. The live held basket
    # (go-live 06-01, +4% by 06-12) must take precedence from go-live — its
    # month reads as the basket's real return, not the backtest's +12%.
    bt = [("2026-05-01", 5.0), ("2026-06-15", 12.0)]
    snaps = [
        _snap("rebalance", "2026-06-01", 0.0, as_of="2026-06-01"),
        _snap("price_update", "2026-06-12", 4.0, as_of="2026-06-01"),
    ]
    snap_curve, _, _ = _walk_snapshot_curve(snaps)
    cutover, tail = _splice_snapshot_tail(bt, snap_curve)
    # Cut over at go-live, rebased to the backtest level on 06-01 (the 05-01
    # point, +5% — the 06-15 backtest point is superseded by the live curve).
    assert cutover == "2026-06-01"
    assert abs(tail[0]["cumulative_return_pct"] - 5.0) < 1e-6
    # 1.05 * 1.04 - 1 = 0.092 → +9.2%. The June calendar move off this curve is
    # 1.092 / 1.05 - 1 = +4% — exactly the held basket's period return.
    assert abs(tail[-1]["cumulative_return_pct"] - 9.2) < 1e-6


def test_splice_anchors_before_cutover_with_dense_backtest():
    # REGRESSION (MTD/YTD vs holdings open-period mismatch): a DENSE daily
    # backtest curve has a point ON the cutover (go-live) day. The live basket
    # ENTERS that day with no return yet, so it must continue from the level the
    # DAY BEFORE — not the cutover-day backtest close (which would fold that
    # day's backtest move into MTD/YTD).
    bt = [
        ("2026-05-29", 10.0),
        ("2026-05-30", 11.0),
        ("2026-06-01", 13.0),   # backtest's go-live-day point (a +~1.8% day)
        ("2026-06-02", 14.0),
    ]
    snap = [("2026-06-01", 1.00), ("2026-06-25", 0.9902)]   # basket -0.98% over June
    cutover, tail = _splice_snapshot_tail(bt, snap)
    assert cutover == "2026-06-01"
    t = {p["date"]: p["cumulative_return_pct"] for p in tail}
    # Anchored at 05-30 (+11%), NOT the cutover-day 06-01 (+13%).
    assert abs(t["2026-06-01"] - 11.0) < 1e-6
    # 06-25 = 1.11 * 0.9902 - 1 → +9.91%.
    assert abs(t["2026-06-25"] - ((1.11 * 0.9902 - 1) * 100)) < 1e-4


def test_mtd_matches_open_period_return_with_dense_backtest(monkeypatch):
    # End-to-end: with a dense backtest, the MTD off `_returns_from_backtest`
    # must equal the live basket's June return (the holdings open-period figure,
    # -0.98%) — NOT inflated by the backtest's go-live-day move.
    bt = [("2026-05-29", 10.0), ("2026-05-30", 11.0), ("2026-06-01", 13.0), ("2026-06-25", 14.0)]
    snap = [("2026-06-01", 1.00), ("2026-06-25", 0.9902)]
    cutover, tail = _splice_snapshot_tail(bt, snap)
    kept = [(d, c) for d, c in bt if d < cutover]
    curve = kept + [(p["date"], p["cumulative_return_pct"]) for p in tail]
    monkeypatch.setattr(_H, "_extended_curve", lambda rid, snaps, cash=0.0: curve)
    r = _returns_from_backtest(1, "2026-06-01", date(2026, 6, 25), [])
    assert abs(r["mtd_return_pct"] - (-0.98)) < 0.01
    # YTD anchors before Jan 1 → curve start (+10%): 1.0991/1.10 - 1 = -0.08%.
    assert r["ytd_return_pct"] is not None


def test_splice_end_to_end_from_snapshots():
    # The realistic path: walk snapshots → splice onto the backtest curve.
    # Backtest ends 06-02; the live curve opens 06-01 and supersedes it.
    bt = [("2026-05-01", 5.0), ("2026-06-02", 10.0)]
    snaps = [
        _snap("rebalance", "2026-06-01", 0.0, as_of="2026-06-01"),
        _snap("price_update", "2026-06-12", 4.0, as_of="2026-06-01"),
    ]
    snap_curve, _, _ = _walk_snapshot_curve(snaps)
    cutover, tail = _splice_snapshot_tail(bt, snap_curve)
    assert cutover == "2026-06-01"
    # Rebased to the backtest level at go-live (05-01 = +5%); the basket's +4%
    # over June → 1.05 * 1.04 - 1 = +9.2%. (Old behaviour kept the backtest's
    # 06-02 +10% and stacked +4% on top → a too-high 14.4%.)
    assert abs(tail[-1]["cumulative_return_pct"] - 9.2) < 1e-6


# ── _open_basket_live_curve (dense daily open-basket mark) ──────────────────

def test_open_basket_live_curve_dense_and_matches_reprice(monkeypatch):
    # Two equal-weight long holdings entered 2026-05-29; priced every trading
    # day. The curve must have a point PER trading day (fills the gap the sparse
    # snapshot tail left) and its endpoint must equal the weighted mean of
    # price_eur/entry_price_eur — the SAME basis as the holdings table's reprice.
    from datetime import date as _date
    import momentum.data as md

    holdings = [
        {"company_id": 1, "entry_price_eur": 100.0, "weight": 0.5, "side": "long", "entry_date": "2026-05-29"},
        {"company_id": 2, "entry_price_eur": 50.0, "weight": 0.5, "side": "long", "entry_date": "2026-05-29"},
    ]
    monkeypatch.setattr(_bc, "load_backtest_result_sync",
                        lambda rid: {"monthly_records": [{"date": "2026-06-01", "holdings": holdings}]})
    eur = pd.DataFrame([
        {"company_id": 1, "target_date": _date(2026, 5, 29), "price": 100.0},
        {"company_id": 2, "target_date": _date(2026, 5, 29), "price": 50.0},
        {"company_id": 1, "target_date": _date(2026, 6, 1), "price": 110.0},
        {"company_id": 2, "target_date": _date(2026, 6, 1), "price": 50.0},
        {"company_id": 1, "target_date": _date(2026, 6, 2), "price": 121.0},
        {"company_id": 2, "target_date": _date(2026, 6, 2), "price": 55.0},
    ])
    monkeypatch.setattr(md, "load_all_prices", lambda *a, **k: eur)
    monkeypatch.setattr(md, "load_company_currency", lambda *a, **k: {})
    monkeypatch.setattr(md, "load_fx_rates", lambda *a, **k: {})
    monkeypatch.setattr(md, "convert_prices_to_eur", lambda *a, **k: (eur, None))

    curve = _open_basket_live_curve(99)
    assert [d for d, _ in curve] == ["2026-05-29", "2026-06-01", "2026-06-02"]
    assert abs(curve[0][1] - 1.0) < 1e-9
    assert abs(curve[1][1] - 1.05) < 1e-9      # mean(1.10, 1.00)
    assert abs(curve[-1][1] - 1.155) < 1e-9    # mean(1.21, 1.10) → +15.5% endpoint


def test_open_basket_live_curve_empty_without_run(monkeypatch):
    monkeypatch.setattr(_bc, "load_backtest_result_sync", lambda rid: None)
    assert _open_basket_live_curve(1) == []


# ── cash sleeve on the backtest curve ───────────────────────────────────────

def test_scale_curve_returns_halves_daily_returns():
    # 50% cash halves every period's return then recompounds.
    pts = [("2026-01-01", 10.0), ("2026-01-02", 21.0)]  # day-2 daily = +10%
    scaled = _H._scale_curve_returns(pts, 0.5, as_pct=True)
    assert abs(scaled[0][1] - 5.0) < 1e-6
    assert abs(scaled[1][1] - 10.25) < 1e-6           # 1.05 * 1.05 − 1


def test_scale_curve_returns_noop_at_zero_cash():
    pts = [("2026-01-01", 10.0), ("2026-01-02", 21.0)]
    assert _H._scale_curve_returns(pts, 0.0, as_pct=True) == pts
    assert _H._scale_curve_returns(pts, None, as_pct=True) == pts


def test_scale_curve_equity_space():
    # as_pct=False: levels are equity (base 1). 50% cash halves the return.
    pts = [("2026-01-01", 1.0), ("2026-01-02", 1.10)]   # +10% day 2
    scaled = _H._scale_curve_returns(pts, 0.5, as_pct=False)
    assert abs(scaled[-1][1] - 1.05) < 1e-9


def test_curve_stats_annualized_and_maxdd():
    ann, mdd = _H._curve_stats([("2026-01-01", 0.0), ("2027-01-01", 20.0)])
    assert abs(ann - 20.0) < 0.2 and mdd == 0.0
    # up to +20% then down to +8% → peak 1.20, trough 1.08 → drawdown 10%.
    _, mdd2 = _H._curve_stats([("2026-01-01", 0.0), ("2026-06-01", 20.0), ("2026-12-01", 8.0)])
    assert abs(mdd2 - 10.0) < 1e-6
