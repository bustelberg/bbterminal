from __future__ import annotations

import json
from types import SimpleNamespace

from asset_pipeline import yahoo


class _ImmediateThrottle:
    @staticmethod
    def run(call):
        return call()


class _Session:
    def __init__(self, payload: dict):
        self.payload = payload

    def get(self, *_args, **_kwargs):
        return SimpleNamespace(status_code=200, text=json.dumps(self.payload))


def _payload(*, legal_type: str | None, quote_type: str | None = None) -> dict:
    return {
        "quoteSummary": {"result": [{
            "fundProfile": {"legalType": legal_type},
            "price": {"quoteType": quote_type},
            "topHoldings": {"sectorWeightings": [
                {"technology": {"raw": 0.6}},
                {"financial_services": {"raw": 0.4}},
            ]},
        }]},
    }


def test_fund_sector_weightings_accepts_only_yahoo_verified_etfs(monkeypatch):
    monkeypatch.setattr(yahoo, "_HAS_CURL", True)
    monkeypatch.setattr(yahoo, "_throttle", _ImmediateThrottle())
    monkeypatch.setattr(yahoo, "_track_request", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        yahoo, "_quote_session", lambda: (_Session(_payload(legal_type="Exchange Traded Fund")), "c")
    )

    assert yahoo.fund_sector_weightings("SPMO") == [
        {"sector": "Technology", "weight_pct": 60.0},
        {"sector": "Financial Services", "weight_pct": 40.0},
    ]

    monkeypatch.setattr(
        yahoo, "_quote_session", lambda: (_Session(_payload(legal_type="Mutual Fund")), "c")
    )
    assert yahoo.fund_sector_weightings("NOT-AN-ETF") == []


def test_mutual_fund_sector_weightings_accepts_only_yahoo_mutual_funds(monkeypatch):
    monkeypatch.setattr(yahoo, "_HAS_CURL", True)
    monkeypatch.setattr(yahoo, "_throttle", _ImmediateThrottle())
    monkeypatch.setattr(yahoo, "_track_request", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        yahoo, "_quote_session",
        lambda: (_Session(_payload(legal_type=None, quote_type="MUTUALFUND")), "c"),
    )

    assert yahoo.mutual_fund_sector_weightings("0P0001TPVP.F") == [
        {"sector": "Technology", "weight_pct": 60.0},
        {"sector": "Financial Services", "weight_pct": 40.0},
    ]

    monkeypatch.setattr(
        yahoo, "_quote_session",
        lambda: (_Session(_payload(
            legal_type="Exchange Traded Fund", quote_type="ETF")), "c"),
    )
    assert yahoo.mutual_fund_sector_weightings("SPMO") == []
