"""Phase-level tests for the company-list loaders in
`ingest/phases/prices.py` — `_load_all_companies` (full universe, stale-
first) and `_collect_held_companies` (the pooled held set the daily MTD
refresh drives over). Both are DB-shaped; we run them against an in-memory
`FakeSupabase` (no Postgres). This is the first use of the reusable fake —
extend it for the prune / dedupe / momentum phases next.
"""
from __future__ import annotations


import ingest.phases.prices as prices_mod
from ingest.phases.prices import _collect_held_companies, _load_all_companies

from tests._fake_supabase import FakeSupabase


def _co(cid, ticker, exch, **extra):
    return {
        "company_id": cid,
        "gurufocus_ticker": ticker,
        "delisted_at": None,
        "out_of_scope_at": None,
        "gurufocus_exchange": {"exchange_code": exch},
        **extra,
    }


class TestLoadAllCompanies:
    def test_filters_drops_and_stale_first_sort(self, monkeypatch):
        company = [
            _co(1, "AAA", "NYSE"),
            _co(2, "BBB", "HKSE"),
            _co(3, "CCC", "LSE"),
            _co(4, "DEL", "NYSE", delisted_at="2026-01-01"),       # delisted → filtered
            _co(5, None, "NYSE"),                                   # no ticker → dropped
            _co(6, "NOX", ""),                                      # no exchange → dropped
            _co(7, "OOS", "NYSE", out_of_scope_at="2026-01-01"),   # out of scope → filtered
        ]
        # Stale-first: cid 3 has no price row (sorts first), then oldest, then newest.
        rpc = {
            "company_latest_close_price_dates": [
                {"company_id": 1, "latest_target_date": "2026-03-01"},  # newest
                {"company_id": 2, "latest_target_date": "2026-01-01"},  # oldest
                # company 3 absent → "" key sorts before any date
            ]
        }
        fake = FakeSupabase(tables={"company": company}, rpc_results=rpc)
        monkeypatch.setattr(prices_mod, "supabase", fake)

        out = _load_all_companies()
        assert [c["cid"] for c in out] == [3, 2, 1]
        assert out[2] == {"cid": 1, "ticker": "AAA", "exchange": "NYSE"}

    def test_empty_universe(self, monkeypatch):
        fake = FakeSupabase(tables={"company": []}, rpc_results={"company_latest_close_price_dates": []})
        monkeypatch.setattr(prices_mod, "supabase", fake)
        assert _load_all_companies() == []

    def test_survives_missing_rpc_falls_back_to_insertion_order(self, monkeypatch):
        company = [_co(1, "AAA", "NYSE"), _co(2, "BBB", "HKSE")]
        # No rpc result registered → the stale-sort try/except swallows and
        # keeps insertion order.
        fake = FakeSupabase(tables={"company": company})
        monkeypatch.setattr(prices_mod, "supabase", fake)
        out = _load_all_companies()
        assert [c["cid"] for c in out] == [1, 2]


class TestCollectHeldCompanies:
    def _company_table(self):
        return [
            {"company_id": 1, "gurufocus_ticker": "AAA", "gurufocus_exchange": {"exchange_code": "NYSE"}},
            {"company_id": 2, "gurufocus_ticker": "BBB", "gurufocus_exchange": {"exchange_code": "HKSE"}},
            {"company_id": 3, "gurufocus_ticker": None, "gurufocus_exchange": {"exchange_code": "LSE"}},
        ]

    def test_pools_latest_snapshot_per_enabled_strategy_and_dedups(self, monkeypatch):
        tables = {
            "scheduled_strategy": [
                {"id": 10, "enabled": True},
                {"id": 11, "enabled": True},
                {"id": 12, "enabled": False},  # disabled → ignored
            ],
            "current_picks_snapshot": [
                # strategy 10: newer snapshot wins over the older one
                {"scheduled_strategy_id": 10, "created_at": "2026-03-02", "holdings": [{"company_id": 1}, {"company_id": 2}]},
                {"scheduled_strategy_id": 10, "created_at": "2026-02-01", "holdings": [{"company_id": 99}]},
                # strategy 11: shares company 2 (dedup) + adds 3
                {"scheduled_strategy_id": 11, "created_at": "2026-03-01", "holdings": [{"company_id": 2}, {"company_id": 3}]},
                # strategy 12 is disabled, so its snapshot must not contribute
                {"scheduled_strategy_id": 12, "created_at": "2026-03-09", "holdings": [{"company_id": 50}]},
            ],
            "company": self._company_table(),
        }
        fake = FakeSupabase(tables=tables)
        monkeypatch.setattr(prices_mod, "supabase", fake)

        out = _collect_held_companies(run_id=1)
        # Pool {1,2,3}; company 3 dropped (no ticker); 99 came from a stale
        # snapshot; 50 from a disabled strategy.
        assert sorted(c["cid"] for c in out) == [1, 2]
        assert {c["ticker"] for c in out} == {"AAA", "BBB"}

    def test_no_enabled_strategies(self, monkeypatch):
        fake = FakeSupabase(tables={"scheduled_strategy": [{"id": 1, "enabled": False}]})
        monkeypatch.setattr(prices_mod, "supabase", fake)
        assert _collect_held_companies(run_id=1) == []

    def test_no_snapshots(self, monkeypatch):
        tables = {
            "scheduled_strategy": [{"id": 10, "enabled": True}],
            "current_picks_snapshot": [],
            "company": self._company_table(),
        }
        fake = FakeSupabase(tables=tables)
        monkeypatch.setattr(prices_mod, "supabase", fake)
        assert _collect_held_companies(run_id=1) == []
