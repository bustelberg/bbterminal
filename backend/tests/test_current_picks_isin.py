"""Unit tests for `_enrich_holdings_isin` — the read-time enrichment that
attaches each holding's ISIN to the current-picks snapshot so the /schedule
Current-portfolio card renders ETF + stock ISINs together (no separate
/api/companies + /api/benchmarks fetch to race)."""
from __future__ import annotations

import routers.momentum.current_picks as cp
from tests._fake_supabase import FakeSupabase


def _fake(monkeypatch) -> FakeSupabase:
    fake = FakeSupabase(tables={
        "company": [
            {"company_id": 10, "isin": "US0000000010"},
            {"company_id": 11, "isin": None},          # subscribed but no ISIN
        ],
        "benchmark": [
            {"benchmark_id": 5, "isin": "IE00B4L5Y983"},
        ],
    })
    monkeypatch.setattr(cp, "supabase", fake)
    return fake


def test_stock_and_etf_isin_resolved(monkeypatch):
    _fake(monkeypatch)
    holdings = [
        {"company_id": 10, "ticker": "AAA"},   # stock  → company.isin
        {"company_id": 11, "ticker": "BBB"},   # stock, no isin → stays absent
        {"company_id": -5, "ticker": "ETF"},   # ETF (-benchmark_id) → benchmark.isin
        {"company_id": None, "ticker": "X"},   # no id → skipped
    ]
    out = cp._enrich_holdings_isin(holdings)
    assert out[0]["isin"] == "US0000000010"
    assert out[1].get("isin") is None
    assert out[2]["isin"] == "IE00B4L5Y983"   # the ETF ISIN the card used to race for
    assert "isin" not in out[3]


def test_existing_isin_not_overwritten(monkeypatch):
    _fake(monkeypatch)
    holdings = [{"company_id": 10, "isin": "ALREADY_SET"}]
    out = cp._enrich_holdings_isin(holdings)
    assert out[0]["isin"] == "ALREADY_SET"


def test_empty_holdings_noop():
    assert cp._enrich_holdings_isin([]) == []
