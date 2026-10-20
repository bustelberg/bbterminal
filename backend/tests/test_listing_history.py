"""Listing events must not turn vendor placeholders into universe profits."""
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from common.price_history import filter_listing_rows
from momentum.data import prices
from momentum.backtest.equity_curve import _compute_universe_period_daily
from momentum.backtest.indices import _build_price_index
from tests._fake_supabase import FakeSupabase


@pytest.mark.parametrize("symbol,start,before,placeholder,first_close", [
    ("6030.HK", "2011-10-06", "2011-10-04", 0.01, 13.007741928100586),
    ("9201.T", "2012-09-19", "2012-09-18", 1.0, 1915.0),
])
@pytest.mark.parametrize("fallback", [False, True])
def test_existing_yahoo_rows_cannot_create_a_pre_listing_position(
    monkeypatch, symbol, start, before, placeholder, first_close, fallback,
):
    """Exercise both transports, signals' volume history, and the real EW helper."""
    sb = FakeSupabase({"asset_analysis": [{"analysis_id": 101, "symbol": symbol}]})
    monkeypatch.setattr(prices, "load_company_asset_map", lambda *_: {10: (101, "EUR"), 20: (202, "EUR")})
    dates = pd.to_datetime([before, start, before, start])

    def frame(_ids, series, *_args):
        field = series.split(".")[-1]
        return pd.DataFrame({
            "entity_id": [101, 101, 202, 202], "date": dates,
            field: [placeholder, first_close, 100.0, 100.0] if field == "close" else [0, 1000, 1000, 1000],
        })

    if fallback:
        def unavailable(*_args):
            raise prices.SeriesUnavailable("no COPY")
        monkeypatch.setattr(prices, "load_series", unavailable)
        monkeypatch.setattr(prices, "_yahoo_via_postgrest", lambda _sb, ids, begin, end, field: frame(ids, f"yf.{field}"))
    else:
        monkeypatch.setattr(prices, "load_series", frame)

    begin, end = date.fromisoformat(before), date.fromisoformat(start)
    loaded = prices.load_all_prices(sb, [10, 20], begin, end)
    assert loaded[loaded.company_id == 10].price.tolist() == [first_close]
    volumes = prices.load_all_volumes(sb, [10, 20], begin, end)
    assert volumes[volumes.company_id == 10].volume.tolist() == [1000]
    curve = _compute_universe_period_daily(
        {10, 20}, _build_price_index(loaded), entry_ts=pd.Timestamp(before), exit_ts=pd.Timestamp(start),
    )
    # No entry for the new listing: the universe stays flat rather than gaining
    # 650x / 958x from averaging the placeholder-to-listing price ratio.
    assert [factor for _, factor in curve] == [1.0, 1.0]
    assert sb.writes == []


def test_reviewed_boundaries_preserve_other_series_and_interior_zero_volume_days():
    rows = [
        {"target_date": "2012-09-18", "close": 1, "volume": 0},
        {"target_date": "2012-09-19", "close": 1915, "volume": 1000},
        {"target_date": "2012-09-20", "close": 1915, "volume": 0},
    ]
    assert filter_listing_rows(rows, "9201.T") == rows[1:]
    assert filter_listing_rows(rows, "OTHER") == rows


def test_new_ingests_and_parquet_do_not_reintroduce_pre_relisting_bars(monkeypatch):
    from asset_pipeline import parquet, store

    def ts(day):
        return int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())

    window = {
        "meta": {"symbol": "9201.T"},
        "timestamp": [ts("2009-01-05"), ts("2012-09-18"), ts("2012-09-19"), ts("2012-09-20")],
        "indicators": {"quote": [{"close": [100, 1, 1915, 1922.5], "volume": [1000, 0, 80931200, 0]}]},
    }
    sb = FakeSupabase()
    monkeypatch.setattr(store, "supabase", sb)
    monkeypatch.setattr(store.yahoo, "chart_window", lambda *_: window)
    monkeypatch.setattr(parquet, "write", lambda *_: None)
    assert store.store_series(101, "9201.T", ts("2000-01-01")) == 2
    assert [row["target_date"] for row in sb.tables["asset_price"]] == ["2012-09-19", "2012-09-20"]
    archive = parquet.build_frame(window)
    assert [day.isoformat() for day in archive.date] == ["2012-09-19", "2012-09-20"]


def test_price_history_revision_invalidates_backtest_replay_cache(monkeypatch):
    from routers.momentum import _helpers
    from routers.momentum.backtest_stream.models import BacktestRequest

    request = BacktestRequest(start_date="2002-01-01", end_date="2026-10-01")
    current = _helpers.backtest_strategy_hash(request)
    monkeypatch.setattr(_helpers, "PRICE_HISTORY_VERSION", "previous-price-history")
    assert _helpers.backtest_strategy_hash(request) != current
