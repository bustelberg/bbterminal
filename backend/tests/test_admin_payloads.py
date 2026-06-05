"""Admin response-shaping + freshness helpers (`routers/_admin_payloads.py`,
`routers/_admin_health.py`).

These were inline closures/helpers in the 1,250-line `routers.admin` and
couldn't be exercised without standing up FastAPI + the admin auth gate. Now
that they're plain functions, pin the transforms that the external IBKR /
monitoring script depends on: the run + schedule summaries, the portfolio
payload shape, and the trading-day age math.
"""
from __future__ import annotations

from datetime import date, timedelta

import routers._admin_payloads as payloads_mod
from routers._admin_health import _trading_day_age
from routers._admin_payloads import (
    _build_portfolio_payload,
    _summarize_run,
    _summarize_schedule,
)

from tests._fake_supabase import FakeSupabase


class TestSummarizeRun:
    def test_momentum_dict_is_wrapped_to_list(self):
        out = _summarize_run({"run_id": 1, "momentum_summary": {"strategy_id": 7}})
        assert [m["strategy_id"] for m in out["momentum"]] == [7]

    def test_momentum_list_passes_through(self):
        out = _summarize_run({"momentum_summary": [{"strategy_id": 1}, {"strategy_id": 2}]})
        assert [m["strategy_id"] for m in out["momentum"]] == [1, 2]

    def test_non_list_templates_summary_coerced_to_empty(self):
        out = _summarize_run({"templates_summary": None, "momentum_summary": None})
        assert out["templates"] == []
        assert out["momentum"] == []

    def test_template_field_renames(self):
        out = _summarize_run({
            "templates_summary": [{
                "template_key": "ACWI", "universe_id": 5, "this_month": "2026-06",
                "additions_count": 3, "removals_count": 1, "renames_count": 0,
            }],
        })
        t = out["templates"][0]
        assert t["target_month"] == "2026-06"  # this_month → target_month
        assert (t["additions"], t["removals"], t["renames"]) == (3, 1, 0)

    def test_price_counters_default_to_zero(self):
        out = _summarize_run({})
        assert out["prices"] == {
            "companies_processed": 0, "prices_refreshed": 0, "volumes_refreshed": 0,
            "forbidden": 0, "delisted": 0, "errors": 0,
        }


class TestSummarizeSchedule:
    def test_none_snapshot_yields_null_portfolio(self):
        out = _summarize_schedule({"id": 4, "name": "Mine", "frequency": "monthly"}, None)
        assert out["latest_portfolio"] is None
        assert out["name"] == "Mine"

    def test_name_falls_back_to_id(self):
        out = _summarize_schedule({"id": 9}, None)
        assert out["name"] == "Strategy #9"


class TestBuildPortfolioPayload:
    def test_fills_exchange_and_sums_weights(self, monkeypatch):
        fake = FakeSupabase(tables={"company": [
            {"company_id": 1, "gurufocus_exchange": {"exchange_code": "NYSE"}},
            {"company_id": 2, "gurufocus_exchange": {"exchange_code": "OHEL"}},
        ]})
        monkeypatch.setattr(payloads_mod, "supabase", fake)

        snap = {
            "snapshot_id": 100,
            "name": "S",
            "config": {"selection_mode": "momentum", "strategy_type": "long_only"},
            "holdings": [
                {"company_id": 1, "ticker": "AAA", "weight": 0.6},
                {"company_id": 2, "ticker": "BBB", "weight": 0.4, "side": "short"},
            ],
        }
        out = _build_portfolio_payload(snap)
        assert out["holdings_count"] == 2
        assert out["total_weight"] == 1.0
        by_ticker = {h["ticker"]: h for h in out["holdings"]}
        assert by_ticker["AAA"]["exchange"] == "NYSE"
        assert by_ticker["AAA"]["side"] == "long"  # default
        assert by_ticker["BBB"]["exchange"] == "OHEL"
        assert by_ticker["BBB"]["side"] == "short"

    def test_empty_holdings(self, monkeypatch):
        monkeypatch.setattr(payloads_mod, "supabase", FakeSupabase(tables={"company": []}))
        out = _build_portfolio_payload({"snapshot_id": 1, "holdings": []})
        assert out["holdings"] == []
        assert out["total_weight"] == 0.0


class TestTradingDayAge:
    def test_none_is_none(self):
        assert _trading_day_age(None) is None

    def test_today_is_zero(self):
        assert _trading_day_age(date.today()) == 0

    def test_future_is_zero(self):
        assert _trading_day_age(date.today() + timedelta(days=3)) == 0

    def test_two_weeks_back_is_ten_trading_days(self):
        # A 14-calendar-day window always spans exactly two weekends → 10 weekdays.
        assert _trading_day_age(date.today() - timedelta(days=14)) == 10
