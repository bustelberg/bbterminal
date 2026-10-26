from datetime import date
from types import SimpleNamespace

from asset_pipeline import price_refresh
from ingest.phases import prices
from routers.momentum import _helpers
from tests._fake_supabase import FakeSupabase


def test_ranking_anchor_reads_asset_metadata_instead_of_sorting_bars(monkeypatch):
    class MetadataOnly(FakeSupabase):
        def table(self, name):
            assert name == "asset_analysis"
            return super().table(name)

    monkeypatch.setattr(price_refresh, "supabase", MetadataOnly({"asset_analysis": [
        {"price_to": None}, {"price_to": "2026-10-22"}, {"price_to": "2026-10-23"},
    ]}))
    assert _helpers.latest_db_price_date() == date(2026, 10, 23)


def test_scoped_date_reads_bound_work_and_keep_companies_without_prices(monkeypatch):
    calls = []

    class ScopedDB:
        def rpc(self, name, params):
            assert name == "company_latest_metric_dates_for"
            assert params["p_metric_code"] == "close_price"
            ids = params["p_company_ids"]
            calls.append(ids)
            return SimpleNamespace(execute=lambda: SimpleNamespace(data=[
                {"company_id": cid, "latest_target_date": "2026-10-23"}
                for cid in ids if cid != 101
            ]))

    monkeypatch.setattr(prices, "supabase", ScopedDB())
    result = prices._latest_close_dates_for(list(range(1, 252)))
    assert [len(batch) for batch in calls] == [100, 100, 51]
    assert len(result) == 250
    assert 101 not in result
    assert result[251] == "2026-10-23"


def test_latest_date_batch_failure_does_not_return_partial_freshness(monkeypatch):
    monkeypatch.setattr(prices, "supabase", FakeSupabase({"company": [{"company_id": 1}]}))

    def fail(_ids):
        raise RuntimeError("statement timeout")

    monkeypatch.setattr(prices, "_latest_close_dates_for", fail)
    assert prices._latest_close_dates_all() == {}
