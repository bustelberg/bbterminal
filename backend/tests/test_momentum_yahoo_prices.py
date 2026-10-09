"""Momentum's company-shaped loaders must read Yahoo's asset-domain series."""
from __future__ import annotations

from datetime import date

import pandas as pd

from momentum.data import prices
from timeseries import SeriesUnavailable


def test_price_loader_maps_yahoo_analysis_ids_back_to_company_ids(monkeypatch):
    monkeypatch.setattr(
        prices, "load_company_asset_map",
        lambda _sb, _ids: {10: (101, "USD"), 20: (202, "EUR")},
    )
    seen = {}

    def _load(ids, series, start, end):
        seen.update(ids=ids, series=series, start=start, end=end)
        return pd.DataFrame({
            "entity_id": [202, 101],
            "date": pd.to_datetime(["2025-01-03", "2025-01-02"]),
            "close": [20.0, 10.0],
        })

    monkeypatch.setattr(prices, "load_series", _load)
    actual = prices.load_all_prices(object(), [10, 20], date(2025, 1, 1), date(2025, 1, 5))

    assert seen["series"] == "yf.close"
    assert seen["ids"] == [101, 202]
    assert actual.to_dict("records") == [
        {"company_id": 10, "target_date": pd.Timestamp("2025-01-02"), "price": 10.0},
        {"company_id": 20, "target_date": pd.Timestamp("2025-01-03"), "price": 20.0},
    ]


def test_volume_loader_uses_yahoo_volume_and_drops_missing_values(monkeypatch):
    monkeypatch.setattr(prices, "load_company_asset_map", lambda _sb, _ids: {10: (101, "USD")})
    monkeypatch.setattr(
        prices, "load_series",
        lambda *_args: pd.DataFrame({
            "entity_id": [101, 101],
            "date": pd.to_datetime(["2025-01-02", "2025-01-03"]),
            "volume": [1000.0, None],
        }),
    )

    actual = prices.load_all_volumes(object(), [10], date(2025, 1, 1), date(2025, 1, 5))

    assert actual.to_dict("records") == [
        {"company_id": 10, "target_date": pd.Timestamp("2025-01-02"), "volume": 1000.0},
    ]


def test_price_loader_uses_paged_yahoo_fallback_when_copy_is_unavailable(monkeypatch):
    monkeypatch.setattr(prices, "load_company_asset_map", lambda _sb, _ids: {10: (101, "USD")})
    monkeypatch.setattr(prices, "load_series", lambda *_args: (_ for _ in ()).throw(SeriesUnavailable("no COPY")))
    monkeypatch.setattr(
        prices, "_yahoo_via_postgrest",
        lambda *_args: pd.DataFrame({
            "entity_id": [101], "date": pd.to_datetime(["2025-01-02"]), "close": [10.0],
        }),
    )

    actual = prices.load_all_prices(object(), [10], date(2025, 1, 1), date(2025, 1, 5))

    assert actual.to_dict("records") == [
        {"company_id": 10, "target_date": pd.Timestamp("2025-01-02"), "price": 10.0},
    ]
