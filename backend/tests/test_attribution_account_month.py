"""Monthly attribution must compare AIRS's monthly return with that same month."""
from datetime import date as real_date

import pytest

import deps
from routers import _airs_account_links as links
from routers import _airs_portfolio_analysis as analysis
from routers import _airs_portfolio_attribution as at
from routers import _airs_portfolio_variant as variant
from routers import _benchmark_etf as etf
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def account(monkeypatch):
    sb = FakeSupabase({"airs_performance": [
        {"portefeuille": "BOOK", "periode": "2026-08-31", "rendement": 90, "fetched_at": "2026-10-20"},
        {"portefeuille": "BOOK", "periode": "2026-09-15", "rendement": -5, "fetched_at": "2026-09-16"},
        {"portefeuille": "BOOK", "periode": "2026-09-30", "rendement": 2, "fetched_at": "2026-10-01"},
        {"portefeuille": "OTHER", "periode": "2026-09-30", "rendement": 99, "fetched_at": "2026-10-01"},
        {"portefeuille": "BOOK", "periode": "2026-10-18", "rendement": -1, "fetched_at": "2026-10-19"},
    ]})
    monkeypatch.setattr(deps, "supabase", sb)
    monkeypatch.setattr(links, "list_account_links", lambda: {"accounts": [
        {"model_portfolio_id": 12, "portefeuille": "BOOK", "ytd_pct": 50},
    ]})
    return sb


def test_monthly_book_return_uses_latest_row_of_the_selected_month(account):
    assert at._monthly_book_return(12, "2026-09-01", "2026-09-30") == {
        "return_pct": 2, "as_of": "2026-09-30",
    }
    assert account.writes == []


def test_missing_or_null_month_does_not_borrow_ytd_or_another_month(account):
    assert at._monthly_book_return(12, "2026-07-01", "2026-07-31") == {}
    assert at._monthly_book_return(99, "2026-09-01", "2026-09-30") == {}
    account.tables["airs_performance"][2]["rendement"] = None
    assert at._monthly_book_return(12, "2026-09-01", "2026-09-30")["return_pct"] is None
    account.tables["airs_performance"][2]["rendement"] = 0
    assert at._monthly_book_return(12, "2026-09-01", "2026-09-30")["return_pct"] == 0


@pytest.fixture
def attribution(monkeypatch, account):
    class Today(real_date):
        @classmethod
        def today(cls):
            return cls(2026, 10, 20)

    monkeypatch.setattr(at, "date", Today)
    monkeypatch.setattr(at, "ref_model", lambda *_: {"name": "Test portfolio", "positions_datum": "2026-01-01"})
    monkeypatch.setattr(analysis, "_weighted_benchmark_profiles", lambda: {})
    monkeypatch.setattr(analysis, "_book_return", lambda *_: {"book_ytd_pct": 50, "book_comparable": True})
    monkeypatch.setattr(variant, "portfolio_variant", lambda *_: None)
    monkeypatch.setattr(etf, "etf_returns", lambda *_: {})
    holding = {"isin": "STOCK", "airs_name": "Stock", "asset_class": "Equity", "weight_pct": 60,
               "return_pct": 10, "current_value_eur": 600}
    monkeypatch.setattr(at, "portfolio_legs", lambda *_a, **_k: [dict(holding)])
    monkeypatch.setattr(at, "split_legs", lambda hs, *_: ([{**h, "bucket": "Tech", "grid_row": {"name": "Stock"}} for h in hs], [], 100))
    monkeypatch.setattr(at, "_grid", lambda *_: {"STOCK": {"name": "Stock"}})
    monkeypatch.setattr(at, "_country_by_code", lambda: {})
    monkeypatch.setattr(at, "_buckets", lambda *_a, **_k: ("Tech", "US", "USD"))
    monkeypatch.setattr(at, "_fund_sector_exposures", lambda *_a, **_k: [])
    endpoints = []

    def benchmark(_label, start, **kwargs):
        endpoints.append(("benchmark", start, kwargs.get("end")))
        return ([{"isin": "STOCK", "company_name": "Stock", "weight_pct": 100,
                  "return_eur_pct": 5, "market_cap_eur": 1000}], {"covered_pct": 100})

    def marks(_ids, start, *, end):
        endpoints.append(("marks", start, end))
        return {"STOCK": {"return_pct": 0 if end == start else 10}}

    monkeypatch.setattr(at, "index_rows", benchmark)
    monkeypatch.setattr(at, "compute_holding_marks", marks)
    return endpoints


def test_monthly_attribution_and_logs_use_the_matching_month(attribution, caplog):
    result = at.compute_attribution(12, "ACWI", month="2026-09-01")
    assert result["account_return_pct"] == 2
    assert result["account_excess_pct"] == pytest.approx(-3)  # 2% monthly book - 5% monthly index
    assert result["unattributed_excess_pct"] == pytest.approx(-6)  # minus 60% * (10% - 5%)
    assert "(sector/2026-09)" in caplog.text
    assert "(sector/ytd)" not in caplog.text


def test_current_month_prices_end_at_the_accounts_latest_observation(attribution):
    result = at.compute_attribution(12, "ACWI", month="2026-10-01")
    assert result["account_return_pct"] == -1
    assert ("benchmark", "2026-10-01", "2026-10-18") in attribution
    assert ("marks", "2026-10-01", "2026-10-18") in attribution


def test_ytd_keeps_the_cumulative_account_return_and_log_label(attribution, caplog):
    result = at.compute_attribution(12, "ACWI")
    assert result["account_return_pct"] == 50
    assert result["account_excess_pct"] == pytest.approx(45)
    assert "(sector/ytd)" in caplog.text


def test_missing_month_leaves_account_reconciliation_unavailable(attribution):
    result = at.compute_attribution(12, "ACWI", month="2026-07-01")
    assert result["account_return_pct"] is None
    assert result["account_excess_pct"] is None
    assert result["unattributed_excess_pct"] is None
