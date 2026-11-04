import pytest

from routers import _airs_monthly_lookthrough as lookthrough
from tests._fake_supabase import FakeSupabase


def test_toppenberg_bundle_combines_dated_constituents_and_preserves_direct_positions(monkeypatch):
    month = "2026-09"
    ai = "AI-TopSelectie Index"
    brands = "MerkenTopSelectie Index"
    missing = "DividendTopSelectie Index"
    cash = {"name": "Effectenrekening", "opening_weight_pct": 10}
    snapshot = {
        "opening_weight_date": "2026-08-31", "closing_date": "2026-09-30",
        "positions": [{"name": ai, "opening_weight_pct": 30}, {"name": brands, "opening_weight_pct": 40},
                      {"name": missing, "opening_weight_pct": 20}, cash],
    }
    bundle = {"portefeuille": "TOPS_NEU_BEH_DYN", "years": {"2026": {
        "details": {month: {"snapshot": snapshot}},
    }}}
    accounts = {ai: "AI", brands: "BRANDS", missing: "DIV"}
    monkeypatch.setattr(lookthrough, "dynamic_account_for_holding", accounts.get)

    def child(account, opening_value, closing_value):
        holding = {"name": "Shared stock", "isin": "SHARED", "quantity": 10, "currency": "EUR"}
        cash_row = {"name": "Effectenrekening", "value_eur": 100 - opening_value, "quantity": 1}
        return {"portefeuille": account, "month": month + "-01", "report": {"sections": {
            "opening": {"status": "ok", "data": {"as_of": "2026-08-31",
                        "rows": [{**holding, "value_eur": opening_value}, cash_row]}},
            "closing": {"status": "ok", "data": {"as_of": "2026-09-30",
                        "rows": [{**holding, "value_eur": closing_value}, cash_row]}},
            "transactions": {"status": "no_data"},
        }}}

    db = FakeSupabase({"airs_monthly_report": [child("AI", 60, 66), child("BRANDS", 50, 49)]})
    monkeypatch.setattr(lookthrough, "supabase", db)
    import dashboard_daily
    monkeypatch.setattr(dashboard_daily, "prepare_month", lambda *_a, **_kw: {"errors": {"opening": "Missing"}})
    from routers import _airs_monthly_report
    downloads = []
    monkeypatch.setattr(_airs_monthly_report, "collect_report", lambda account, month, ctx: downloads.append((account, month)))
    monkeypatch.setattr(_airs_monthly_report, "_prepare_performance", lambda account: bundle)

    assert _airs_monthly_report.prepare_performance("TOPS_NEU_BEH_DYN") is bundle

    positions = snapshot["lookthrough_positions"]
    shares = [row for row in positions if row.get("isin") == "SHARED"]
    assert [row["certificate"] for row in shares] == [ai, brands]
    assert [row["opening_weight_pct"] for row in shares] == pytest.approx([18, 20])
    assert [row["return_pct"] for row in shares] == pytest.approx([10, -2])
    assert shares[0]["via"] == shares[1]["via"] == [
        {"label": "AITopSelectie", "weight_pct": 60},
        {"label": "MerkenTopSelectie", "weight_pct": 50},
    ]
    assert all("contribution_pp" not in row for row in shares)
    assert cash in positions
    assert {"name": missing, "opening_weight_pct": 20} in positions
    assert snapshot["lookthrough_unavailable"] == [missing]
    assert downloads == [("DIV", month)]
