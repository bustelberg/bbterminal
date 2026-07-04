"""`live_period_records` — the strategy's live rebalance baskets as PeriodRecord
rows the /schedule holdings table appends to the frozen backtest periods.

Pins: backfill (seed) rows are skipped; one row per open period keeping the
freshest mark; periods sorted by rebalance date; each period's return chained
onto the backtest curve's end cumulative; the last period flagged open.
"""
from __future__ import annotations

import routers._schedule_hydration as hyd


def _snaps():
    return [
        # Period 2026-06-01: a rebalance then a fresher price_update (kept).
        {"kind": "rebalance", "as_of_date": "2026-06-01", "latest_price_date": "2026-06-05",
         "holdings": [{"company_id": 1}], "period_return_pct": 2.0, "is_backfill": False},
        {"kind": "price_update", "as_of_date": "2026-06-01", "latest_price_date": "2026-06-20",
         "holdings": [{"company_id": 1}, {"company_id": 2}], "period_return_pct": 3.5, "is_backfill": False},
        # Period 2026-07-06: the current open rebalance (0% at creation).
        {"kind": "rebalance", "as_of_date": "2026-07-06", "latest_price_date": "2026-07-06",
         "holdings": [{"company_id": 3}], "period_return_pct": 0.0, "is_backfill": False},
        # A backfill seed of the backtest's last period — must be skipped.
        {"kind": "rebalance", "as_of_date": "2026-05-01", "latest_price_date": "2026-05-06",
         "holdings": [], "period_return_pct": 9.9, "is_backfill": True},
    ]


def test_records_chain_onto_backtest_end(monkeypatch):
    # Backtest ended at +10% cumulative.
    monkeypatch.setattr(hyd, "_load_backtest_pts", lambda _r, _c=0.0: [("2026-05-31", 10.0)])
    recs = hyd.live_period_records(_snaps(), backtest_run_id=1)

    assert [r["date"] for r in recs] == ["2026-06-01", "2026-07-06"]
    # Period 06-01 kept the freshest snapshot (3.5%, 2 holdings), not the 2.0% one.
    assert recs[0]["portfolio_return_pct"] == 3.5
    assert len(recs[0]["holdings"]) == 2
    assert recs[0]["as_of_date"] == "2026-06-20"
    assert recs[0]["is_open"] is False
    # Cumulative chains from +10%: 1.10 * 1.035 - 1 = 13.85%.
    assert recs[0]["cumulative_return_pct"] == 13.85
    # Open period unchanged (0% this period) and flagged open.
    assert recs[1]["cumulative_return_pct"] == 13.85
    assert recs[1]["is_open"] is True


def test_no_live_rebalances_is_empty(monkeypatch):
    monkeypatch.setattr(hyd, "_load_backtest_pts", lambda _r, _c=0.0: [("2026-05-31", 10.0)])
    # Only a backfill seed → no genuine live periods.
    only_seed = [{"kind": "rebalance", "as_of_date": "2026-05-01",
                  "latest_price_date": "2026-05-06", "holdings": [],
                  "period_return_pct": 1.0, "is_backfill": True}]
    assert hyd.live_period_records(only_seed, backtest_run_id=1) == []


def test_no_backtest_curve_still_chains_from_flat(monkeypatch):
    monkeypatch.setattr(hyd, "_load_backtest_pts", lambda _r, _c=0.0: [])
    recs = hyd.live_period_records(_snaps(), backtest_run_id=1)
    # Chains from 1.0 (no backtest cum): 1.035 - 1 = 3.5%.
    assert recs[0]["cumulative_return_pct"] == 3.5
