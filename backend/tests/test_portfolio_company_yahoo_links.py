import asyncio
from datetime import date

import pytest

from routers import earnings
from tests._fake_supabase import FakeSupabase


def test_company_rows_use_stored_yahoo_symbols_not_guessed_tickers(monkeypatch):
    fake = FakeSupabase({
        'asset_execution': [{'isin': 'US0846707026', 'yahoo_symbol': 'BRK-B', 'status': 'ok'}],
        'company': [], 'metric_data': [], 'company_yahoo_price': [],
    })
    monkeypatch.setattr(earnings, 'supabase', fake)
    async def members(body):
        return [{'isin': 'US0846707026', 'weight_pct': 50}]
    async def coverage(members):
        return {}
    monkeypatch.setattr(earnings, '_load_and_expand_members', members)
    monkeypatch.setattr('routers._fundamental_coverage.coverage_for_async', coverage)
    monkeypatch.setattr(earnings, '_fundamental_company_rows', lambda coverage: [
        {'company_id': 1, 'isin': 'US0846707026', 'name': 'Berkshire Hathaway', 'weight_pct': 50},
        {'company_id': 2, 'isin': 'UNMAPPED', 'name': 'Unmapped', 'weight_pct': 50},
    ])
    result = asyncio.run(earnings.portfolio_company_metrics(earnings.FundamentalCoverageRequest()))
    assert result['rows'][0]['yahoo_symbol'] == 'BRK-B'
    assert result['rows'][1]['yahoo_symbol'] is None


@pytest.mark.parametrize('fx_available', [True, False])
def test_usd_quote_keeps_its_currency_and_valuation_uses_eur(monkeypatch, fx_available):
    day = date.today().isoformat()
    fake = FakeSupabase({
        'asset_execution': [{'isin': 'ASML_ISIN', 'yahoo_symbol': 'ASML', 'status': 'ok'}],
        'company': [{'company_id': 1, 'gurufocus_exchange': {'currency_code': 'EUR'}}],
        'metric_data': [],
        'company_yahoo_price': [{'company_id': 1, 'target_date': day, 'close': 1647.12, 'currency': 'USD'}],
    })
    monkeypatch.setattr(earnings, 'supabase', fake)
    async def members(body):
        return [{'isin': 'ASML_ISIN', 'weight_pct': 100}]
    async def coverage(members):
        return {}
    monkeypatch.setattr(earnings, '_load_and_expand_members', members)
    monkeypatch.setattr('routers._fundamental_coverage.coverage_for_async', coverage)
    monkeypatch.setattr(earnings, '_fundamental_company_rows', lambda coverage: [
        {'company_id': 1, 'isin': 'ASML_ISIN', 'name': 'ASML', 'weight_pct': 100}])
    monkeypatch.setattr('routers._benchmark_index._fx_to_eur',
                        lambda *args: {'USD': {day: 1.1}} if fx_available else {})
    row = asyncio.run(earnings.portfolio_company_metrics(earnings.FundamentalCoverageRequest()))['rows'][0]
    assert row['stock_price'] == 1647.12
    assert row['stock_price_currency'] == 'USD'
    assert row['currency'] == 'EUR'
    assert row['metrics'][0]['numeric_value'] == (pytest.approx(1647.12 / 1.1) if fx_available else None)
