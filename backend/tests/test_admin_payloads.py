"""Admin response-shaping + freshness helpers (`routers/_admin_payloads.py`,
`routers/_admin_health.py`).

These were inline closures/helpers in the 1,250-line `routers.admin` and
couldn't be exercised without standing up FastAPI + the admin auth gate. Now
that they're plain functions, pin what the external IBKR script depends on:
the portfolio payload shape and the trading-day age math.
"""
from __future__ import annotations

from datetime import date, timedelta

import routers._admin_payloads as payloads_mod
from routers._admin_health import _trading_day_age
from routers._admin_payloads import _build_portfolio_payload

from tests._fake_supabase import FakeSupabase


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
