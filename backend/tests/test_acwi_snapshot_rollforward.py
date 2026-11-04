"""ACWI snapshot weights remain fixed between imports."""
import pytest

from routers import _active_share as active, _asset_benchmark as prices, _benchmark_index as pricing
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def snapshot(monkeypatch):
    from index_universe.acwi import asset_membership, holdings
    rows = [{"analysis_id": aid, "name": name, "weight_pct": weight, "currency": "EUR"}
            for aid, name, weight in ((1, "Apple Inc", 60), (2, "Microsoft Corp", 40))]
    monkeypatch.setattr(asset_membership, "resolve", lambda *_: (rows, {"as_of": "2026-10-29"}))
    monkeypatch.setattr(holdings, "load_latest_acwi_holdings", lambda: ([
        {"Name": row["name"], "Weight (%)": row["weight_pct"]} for row in rows], "2026-10-29"))
    monkeypatch.setattr(active, "load_rows_via_copy", lambda *args: [])
    monkeypatch.setattr(active, "supabase", FakeSupabase())
    monkeypatch.setattr(pricing, "_fx_to_eur", lambda *args: {})
    return rows


def test_snapshot_ignores_newer_marks_and_does_not_need_old_marks(snapshot, monkeypatch):
    monkeypatch.setattr(prices, "window_marks", lambda *args: pytest.fail("snapshot weights must not use prices"))
    result = active._load_benchmark_weights("ACWI", None)
    assert result["coverage"]["workbook_as_of"] == "2026-10-29"
    assert {key: row["weight_pct"] for key, row in result["bench"].items()} == {"apple": 60, "microsoft": 40}


def test_return_windows_still_require_two_dates():
    member = {"company_id": 1, "currency": "EUR", "market_cap_eur": 1}
    marks = {1: {"start": ("2026-10-29", 100), "end": ("2026-10-29", 100)}}
    assert pricing._window_rows([member], {}, {}, "2026-10-29", marks=marks)[0] == []
    assert pricing._window_rows([member], {}, {}, "2026-10-29", marks=marks, allow_single_mark=True)[0][0]["return_eur_pct"] == 0


@pytest.mark.parametrize("stock_target", [100, 60, 30])
def test_active_share_and_concentration_work_for_different_profiles(snapshot, monkeypatch, stock_target):
    from routers._portfolio_concentration import compute_concentration
    monkeypatch.setattr(prices, "window_marks", lambda *args: {
        aid: {"start": ("2026-10-29", 100), "end": ("2026-10-29", 100)} for aid in (1, 2)
    })
    benchmark = active._load_benchmark_weights("ACWI", None)
    monkeypatch.setattr(active, "_benchmark_weights", lambda *args: benchmark)
    monkeypatch.setattr(active, "_grid_by_isin", lambda *args: {
        "APPLE": {"analysis_id": 1, "name": "Apple Inc"},
        "MSFT": {"analysis_id": 2, "name": "Microsoft Corp"},
    })
    policy = {"Stocks": stock_target, "Bonds": 100 - stock_target}
    for apple_share in (60, 80):
        holdings = [
            {"isin": "APPLE", "name": "Apple Inc", "bucket": "Equity", "weight_pct": apple_share * stock_target / 100},
            {"isin": "MSFT", "name": "Microsoft Corp", "bucket": "Equity", "weight_pct": (100 - apple_share) * stock_target / 100},
        ]
        if stock_target < 100:
            holdings.append({"isin": "BOND", "name": "AGGH.AS", "bucket": "Bonds", "is_fund": True,
                             "weight_pct": 100 - stock_target})
        share = active.compute_active_share(holdings, "ACWI", benchmark_weights=policy)
        concentration = compute_concentration(holdings, "ACWI", benchmark_weights=policy)
        assert share["available"], share
        assert concentration["available"], concentration
        apple = next(row for row in share["rows"] if row["name"] == "Apple Inc")
        assert apple["benchmark_pct"] == pytest.approx(0.6 * stock_target)
