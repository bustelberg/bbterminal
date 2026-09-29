"""Portfolio Fundamental must use the same current-book membership as Analyse."""
from __future__ import annotations

import asyncio


def test_current_book_excludes_funds_cash_and_non_positive_positions(monkeypatch):
    from routers import _airs_holding_isin, earnings

    source_rows = [
        {"isin": "NL0001", "holding_name": "ASML", "bucket": "Equity",
         "is_etf": False, "current_value_eur": 76_200},
        {"isin": "US0002", "holding_name": "Berkshire", "bucket": "Equity",
         "is_etf": False, "current_value_eur": 50_600},
        {"isin": "IE0003", "holding_name": "World ETF", "bucket": "Equity",
         "is_etf": True, "current_value_eur": 20_000},
        {"isin": None, "holding_name": "Cash", "bucket": "Cash",
         "is_etf": False, "current_value_eur": 10_000},
        {"isin": "CH0004", "holding_name": "Short", "bucket": "Equity",
         "is_etf": False, "current_value_eur": -1_000},
    ]
    monkeypatch.setattr(
        _airs_holding_isin,
        "resolve_account_isins",
        lambda portefeuille, freshen=False: {"rows": source_rows},
    )

    body = earnings.FundamentalCoverageRequest(
        portfolio_id=123,
        book_portfolio="BUS_Offensief_Dyn",
    )
    members = asyncio.run(earnings._load_and_expand_members(body))

    assert members == [
        {"isin": "NL0001", "name": "ASML", "weight": 76_200.0},
        {"isin": "US0002", "name": "Berkshire", "weight": 50_600.0},
    ]


def test_company_list_keeps_members_whose_metrics_are_not_ingested_yet():
    from routers import earnings

    rows = earnings._fundamental_company_rows({"rows": [
        {"isin": "NL0001", "company_id": 1, "reason": "covered"},
        {"isin": "CH0002", "company_id": 2, "reason": "no_metrics"},
        {"isin": "IE0003", "company_id": None, "reason": "fund"},
    ]})

    assert [row["company_id"] for row in rows] == [1, 2]


def test_certificate_look_through_is_explicit(monkeypatch):
    from routers import _airs_holding_isin, _airs_portfolio_analysis, earnings

    certificate = {"isin": "CH0001", "holding_name": "Star Selection Index",
                   "bucket": "Equity", "is_etf": True, "current_value_eur": 30_000}
    monkeypatch.setattr(
        _airs_holding_isin,
        "resolve_account_isins",
        lambda portefeuille, freshen=False: {"rows": [certificate]},
    )
    monkeypatch.setattr(
        _airs_portfolio_analysis,
        "_expand_book_rows",
        lambda rows: [
            {"isin": "US0002", "holding_name": "Underlying company", "bucket": "Equity",
             "is_fund": False, "current_value_eur": 30_000},
        ],
    )

    direct = asyncio.run(earnings._load_and_expand_members(
        earnings.FundamentalCoverageRequest(book_portfolio="BUS_Offensief_Dyn")))
    looked_through = asyncio.run(earnings._load_and_expand_members(
        earnings.FundamentalCoverageRequest(
            book_portfolio="BUS_Offensief_Dyn", look_through_certificates=True)))

    assert direct == []
    assert looked_through == [
        {"isin": "US0002", "name": "Underlying company", "weight": 30_000.0},
    ]
