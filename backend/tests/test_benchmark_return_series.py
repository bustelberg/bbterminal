from __future__ import annotations

import pytest

from tests._fake_supabase import FakeSupabase


def test_proxy_curve_uses_the_portfolios_exact_dates_and_prior_market_closes(monkeypatch):
    from routers import _benchmark_etf as etf
    from routers import _benchmark_index as index

    monkeypatch.setattr(etf, "ensure_fresh", lambda _label: (7, ("2026-02-02", 120.0)))
    monkeypatch.setattr(etf, "_at_or_before", lambda _bid, _date: ("2025-12-31", 100.0))
    monkeypatch.setattr(etf, "supabase", FakeSupabase(tables={"benchmark_price": [
        {"benchmark_id": 7, "target_date": "2025-12-31", "price": 100.0},
        {"benchmark_id": 7, "target_date": "2026-01-30", "price": 110.0},
        {"benchmark_id": 7, "target_date": "2026-02-02", "price": 120.0},
    ]}))
    monkeypatch.setattr(index, "_fx_to_eur", lambda *_args: {})
    monkeypatch.setattr(index, "_rate", lambda *_args: 1.0)

    got = etf.etf_return_series(
        "SP500", "2026-01-01", ["2026-01-01", "2026-01-31", "2026-02-02"])

    assert [p["date"] for p in got["points"]] == [
        "2026-01-01", "2026-01-31", "2026-02-02"]
    # 31 January is a weekend: the point stays on the portfolio date but uses Friday's close.
    assert got["points"][1]["price_date"] == "2026-01-30"
    assert [p["cum_pct"] for p in got["points"]] == pytest.approx([0.0, 10.0, 20.0])
    assert got["return_pct"] == pytest.approx(20.0)
