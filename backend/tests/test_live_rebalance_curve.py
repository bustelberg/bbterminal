"""`_live_rebalance_dense_curve` — the dense equity tail that FOLLOWS live
rebalances (each rebalance's real basket re-priced daily over its window,
chained), rather than marking the backtest's last basket forward forever.

The price loaders are mocked so the test pins the chaining/boundary math: the
curve switches baskets at each rebalance date and compounds period-over-period.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

import momentum.data as mdata
import routers._schedule_hydration as H


def _install_prices(monkeypatch, df: pd.DataFrame):
    monkeypatch.setattr(mdata, "load_all_prices", lambda *_a, **_k: df)
    monkeypatch.setattr(mdata, "load_company_currency", lambda *_a, **_k: {1: "EUR", 2: "EUR"})
    monkeypatch.setattr(mdata, "load_fx_rates", lambda *_a, **_k: {})
    monkeypatch.setattr(mdata, "convert_prices_to_eur", lambda df_, _cur, _fx: (df_, None))


def test_chains_baskets_across_rebalances(monkeypatch):
    # Two live rebalances into DIFFERENT companies:
    #   period 1 (06-01): company 1 @100 → 120 on 06-15  (+20%)
    #   period 2 (07-06): company 2 @200 → 210 on 07-20  (+5%)
    df = pd.DataFrame([
        {"company_id": 1, "target_date": date(2026, 6, 1), "price": 100.0},
        {"company_id": 1, "target_date": date(2026, 6, 15), "price": 120.0},
        {"company_id": 2, "target_date": date(2026, 7, 6), "price": 200.0},
        {"company_id": 2, "target_date": date(2026, 7, 20), "price": 210.0},
    ])
    _install_prices(monkeypatch, df)

    snaps = [
        {"kind": "rebalance", "as_of_date": "2026-06-01", "latest_price_date": "2026-06-15",
         "is_backfill": False,
         "holdings": [{"company_id": 1, "weight": 1.0, "entry_price_eur": 100.0}]},
        {"kind": "rebalance", "as_of_date": "2026-07-06", "latest_price_date": "2026-07-20",
         "is_backfill": False,
         "holdings": [{"company_id": 2, "weight": 1.0, "entry_price_eur": 200.0}]},
    ]

    curve = H._live_rebalance_dense_curve(snaps)
    d = dict(curve)
    # Period 1 marks: entry flat, then +20%.
    assert round(d["2026-06-01"], 4) == 1.0
    assert round(d["2026-06-15"], 4) == 1.2
    # Rebalance boundary: period 2 opens at the CHAINED equity (1.20), not 1.0.
    assert round(d["2026-07-06"], 4) == 1.2
    # Period 2 compounds +5% on top → 1.20 * 1.05 = 1.26.
    assert round(d["2026-07-20"], 4) == 1.26


def test_cash_sleeve_drags_without_renormalizing(monkeypatch):
    # One rebalance into company 1 at 60% weight + 40% cash. Company doubles.
    # Un-renormalized weighted return = 0.6 * (200/100 - 1) = +60% (cash's 40%
    # simply doesn't participate). Renormalizing would wrongly give +100%.
    df = pd.DataFrame([
        {"company_id": 1, "target_date": date(2026, 6, 1), "price": 100.0},
        {"company_id": 1, "target_date": date(2026, 6, 20), "price": 200.0},
    ])
    _install_prices(monkeypatch, df)
    snaps = [{
        "kind": "rebalance", "as_of_date": "2026-06-01", "latest_price_date": "2026-06-20",
        "is_backfill": False,
        "holdings": [
            {"company_id": 1, "weight": 0.6, "entry_price_eur": 100.0},
            {"company_id": 0, "weight": 0.4, "is_cash": True, "entry_price_eur": 1.0},
        ],
    }]
    curve = H._live_rebalance_dense_curve(snaps)
    assert round(dict(curve)["2026-06-20"], 4) == 1.6


def test_no_live_rebalances_returns_empty(monkeypatch):
    _install_prices(monkeypatch, pd.DataFrame(columns=["company_id", "target_date", "price"]))
    # Only a backfill seed → no live periods.
    assert H._live_rebalance_dense_curve([
        {"kind": "rebalance", "as_of_date": "2026-05-01", "latest_price_date": "2026-05-06",
         "is_backfill": True, "holdings": [{"company_id": 1, "weight": 1.0, "entry_price_eur": 100.0}]},
    ]) == []
