from datetime import date

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from services import portfolio_valuation as V, valuation_workbook as W
from routers._portfolio_valuation import WorkbookAssumptions, WorkbookCompanyAssumption
from tests._fake_supabase import FakeSupabase


def forecast(code='annual_eps_nri_estimate', year=None, value=10):
    year = year or date.today().year + 1
    return {'company_id': 1, 'metric_code': code, 'target_date': f'{year}-03-31',
            'numeric_value': value, 'source_code': 'gurufocus'}


def test_consensus_preserves_basis_and_does_not_splice_different_eps_series():
    year = date.today().year + 1
    rows = [forecast(year=year), forecast('annual_per_share_eps_estimate', year+1, 20)]
    assert 'reason' in W.consensus(rows, date.today().isoformat())
    rows += [forecast(year=year+1, value=12)]
    assert W.consensus(rows, date.today().isoformat())['fy2'] == 12
    rows += [forecast(year=year, value=11)]
    assert 'Conflicting' in W.consensus(rows, date.today().isoformat())['reason']


def test_tesla_defaults_to_saved_eps_and_repairs_only_the_empty_legacy_template():
    seed = next(row for row in W.defaults()['companies'] if row['ticker'] == 'TSLA')
    assert seed['basis_kind'] == 'forward_eps'
    legacy = {'basis_kind': 'manual', 'basis_label': 'SOTP / optie-scenario vereist',
        'manual_basis': None, 'fair_multiple': 11, 'review_required': True}
    repaired = W.normalize_assumptions('TSLA', legacy)
    assert repaired == {**legacy, 'basis_kind': 'forward_eps', 'basis_label': ''}
    assert W.normalize_assumptions('OTHER', legacy) == legacy
    entered = {**legacy, 'manual_basis': 10, 'basis_currency': 'USD'}
    assert W.normalize_assumptions('TSLA', entered) == entered
    intentional = {**legacy, 'basis_label': 'My manual scenario'}
    assert W.normalize_assumptions('TSLA', intentional) == intentional


def setup(monkeypatch):
    database = FakeSupabase({'company': [{'company_id': 1, 'isin': 'EXACT', 'gurufocus_ticker': 'ADBE',
        'gurufocus_exchange': {'is_us': True, 'exchange_code': 'NAS', 'currency_code': 'USD'}}],
        'metric_data': [forecast(), forecast(year=date.today().year+2, value=12)]})
    monkeypatch.setattr(W, 'supabase', database)
    monkeypatch.setattr(V, 'read', lambda _id: {'rows': [{'isin': 'EXACT', 'price_date': date.today().isoformat()}]})
    monkeypatch.setattr(W, 'read_memberships', lambda: {'memberships': {'EXACT': {'FTS': ['FamilieTopSelectie']}},
        'legend': [{'code': 'FTS', 'status': 'complete'}, {'code': 'STAR', 'status': 'unavailable'}], 'complete': False})
    return database


def test_saved_fx_is_bundled_without_writing_assumption_overrides(monkeypatch):
    database = FakeSupabase({'fx_rate': [
        {'currency_code': 'USD', 'rate_date': '2026-11-13', 'rate': 1.25},
        {'currency_code': 'USD', 'rate_date': '2026-11-17', 'rate': 2},
        {'currency_code': 'GBP', 'rate_date': '2026-11-13', 'rate': 0},
    ]})
    monkeypatch.setattr(W, 'supabase', database)
    workbook = {'rows': [{'quote': {'price_date': '2026-11-16'}, 'assumptions': {}}]}
    row = W.attach_fx(workbook)['rows'][0]
    assert row['fx_rates'] == {'2026-11-13': {'USD': 1.25}}
    assert row['assumptions'] == {}


def test_exact_security_gets_excel_assumptions_and_forecasts_without_market_price_import(monkeypatch):
    database = setup(monkeypatch)
    result = W.read(7)
    row = result['rows'][0]
    assert row['seed_source'] == 'test.xlsx'
    assert row['assumptions']['growth'] == .09
    assert row['assumptions']['fair_multiple'] == 15
    assert row['forecast']['fy2'] == 12
    assert row['selections'] == ['FTS']  # Current holdings replace the Excel BB/MTS flags.
    assert row['membership_profiles']['FTS'] == ['FamilieTopSelectie']
    assert row['membership_complete'] is False
    assert row['membership_unknown_codes'] == ['STAR']
    assert 'price' not in row and 'price' not in row['assumptions']
    assert database.writes == []


def test_confirmed_family_membership_is_not_made_partial_by_another_missing_profile(monkeypatch):
    setup(monkeypatch)
    membership = {'memberships': {'EXACT': {'BB': ['Bustelberg Offensief']}},
        'legend': [{'code': 'BB', 'status': 'partial'}], 'complete': False}
    monkeypatch.setattr(W, 'read_memberships', lambda: membership)
    row = W.read(7)['rows'][0]
    assert row['selections'] == ['BB']
    assert row['membership_complete'] is True
    assert row['membership_unknown_codes'] == []
    membership['memberships'] = {}
    row = W.read(7)['rows'][0]
    assert row['membership_complete'] is False
    assert row['membership_unknown_codes'] == ['BB']


def test_assumptions_persist_and_zero_growth_is_not_replaced_by_seed(monkeypatch):
    database = setup(monkeypatch)
    payload = WorkbookAssumptions(parameters={}, companies={'EXACT': {'growth': 0, 'fair_multiple': 17}}).model_dump()
    monkeypatch.setattr(W, 'read', lambda *_args, **_kwargs: pytest.fail('Saving must not rebuild the workbook'))
    monkeypatch.setattr(W, 'read_memberships', lambda: pytest.fail('Saving must not scan memberships'))
    saved = W.save(7, payload)
    assert saved['companies']['EXACT']['growth'] == 0
    assert saved['companies']['EXACT']['fair_multiple'] == 17
    assert saved['updated_at']
    assert 'rows' not in saved
    assert database.tables[W.TABLE][0]['portfolio_id'] == 7
    with pytest.raises(HTTPException) as error:
        W.save(7, {**payload, 'companies': {'OTHER': {}}})
    assert error.value.status_code == 422


def test_unqualified_foreign_ticker_does_not_receive_us_assumptions(monkeypatch):
    database = setup(monkeypatch)
    database.tables['company'][0]['gurufocus_exchange'] = {'exchange_code': 'FOREIGN', 'currency_code': 'EUR'}
    row = W.read(7)['rows'][0]
    assert row['seed_source'] is None
    assert row['assumptions'] == {}


def test_raw_yahoo_price_and_unit_are_returned_without_normalization_or_reconstruction(monkeypatch):
    database = setup(monkeypatch)
    monkeypatch.setattr(V, 'read', lambda _id: {'rows': [{'isin': 'EXACT',
        'price_date': date.today().isoformat(), 'price': 123.45, 'currency': 'GBP'}]})
    database.tables['company_yahoo_price'] = [{'company_id': 1, 'target_date': date.today().isoformat(),
        'close': 12345, 'currency': 'GBp'}]
    row = W.read(7)['rows'][0]
    assert row['quote']['price'] == 12345
    assert row['quote']['currency'] == 'GBp'
    assert row['quote']['source'] == 'Yahoo Finance'
    database.tables['company_yahoo_price'] = []
    assert W.read(7)['rows'][0]['quote']['price'] is None


@pytest.mark.parametrize('bad', [{'growth': -1}, {'fair_multiple': 0}, {'growth': float('nan')}, {'months_fy1': 13}])
def test_invalid_assumptions_are_rejected(bad):
    with pytest.raises(ValidationError):
        WorkbookCompanyAssumption(**bad)


def test_tsmc_adr_uses_one_consistent_taiwan_listing_for_quote_and_eps(monkeypatch):
    database = setup(monkeypatch)
    database.tables['company'][0].update(isin='TW0002330008', gurufocus_ticker='2330',
        gurufocus_exchange={'is_us': False, 'exchange_code': 'TPE', 'currency_code': 'TWD'})
    database.tables['company_yahoo_price'] = [{'company_id': 1, 'target_date': date.today().isoformat(),
        'close': 1200, 'currency': 'TWD'}]
    monkeypatch.setattr(V, 'read', lambda _id: {'rows': [{'isin': 'US8740391003',
        'price_date': date.today().isoformat()}]})
    row = W.read(7)['rows'][0]
    assert row['isin'] == 'US8740391003'
    assert row['ticker'] == 'TPE:2330'
    assert row['forecast']['currency'] == row['quote']['currency'] == 'TWD'
    assert row['quote']['price'] == 1200
    assert row['forecast']['fy2'] == 12
    assert row['seed_source'] == 'test.xlsx'
    assert V.reference_isin('UNREVIEWED_ADR') == 'UNREVIEWED_ADR'


@pytest.mark.parametrize('receipt,ordinary,ticker,exchange,currency', [
    ('USY5217N1183', 'INE018A01030', 'LT', 'NSE', 'INR'),
    ('US78392B2060', 'KR7000660001', '000660', 'XKRX', 'KRW'),
])
def test_reviewed_receipts_keep_prices_and_eps_on_the_same_ordinary_share(
        monkeypatch, receipt, ordinary, ticker, exchange, currency):
    database = setup(monkeypatch)
    database.tables['company'][0].update(isin=ordinary, gurufocus_ticker=ticker,
        gurufocus_exchange={'is_us': False, 'exchange_code': exchange, 'currency_code': currency})
    database.tables['company_yahoo_price'] = [{'company_id': 1, 'target_date': date.today().isoformat(),
        'close': 1234, 'currency': currency}]
    result = W.read(7, prepared_bundle={'rows': [{'isin': receipt}]}, include_membership=False)['rows'][0]
    assert result['isin'] == receipt
    assert result['quote']['price'] == 1234
    assert result['quote']['currency'] == result['forecast']['currency'] == currency
    assert result['forecast']['fy2'] == 12
    assert 'ordinary-share reference' in result['forecast']['basis_label']
    assert result['assumptions'].get('basis_fx') is None


def test_current_quote_does_not_depend_on_historical_valuation_availability(monkeypatch):
    database = setup(monkeypatch)
    monkeypatch.setattr(V, 'read', lambda _id: {'rows': [{'isin': 'EXACT', 'status': 'unavailable'}]})
    database.tables['company_yahoo_price'] = [{'company_id': 1, 'target_date': date.today().isoformat(),
        'close': 125, 'currency': 'USD'}]
    monkeypatch.setattr(W, 'read_memberships', lambda: pytest.fail('Source row reads must not scan portfolio memberships'))
    row = W.read(7, only_isin='EXACT')['rows'][0]
    assert row['quote']['price'] == 125
    assert row['forecast']['fy2'] == 12
    assert row['membership_complete'] is False


def test_yahoo_link_symbol_comes_from_the_actual_quote_listing(monkeypatch):
    database = setup(monkeypatch)
    database.tables['company_yahoo_price'] = [{'company_id': 1, 'analysis_id': 10,
        'target_date': date.today().isoformat(), 'close': 125, 'currency': 'USD'}]
    database.tables['asset_execution'] = [
        {'isin': 'EXACT', 'analysis_id': 10, 'yahoo_symbol': 'ADBE', 'status': 'ok'},
        {'isin': 'OTHER', 'analysis_id': 20, 'yahoo_symbol': 'WRONG', 'status': 'ok'}]
    assert W.read(7)['rows'][0]['quote']['symbol'] == 'ADBE'
    database.tables['asset_execution'][0]['analysis_id'] = 99
    assert W.read(7)['rows'][0]['quote']['symbol'] is None


def test_row_response_reuses_the_already_authorized_bundle(monkeypatch):
    setup(monkeypatch)
    monkeypatch.setattr(V, 'read', lambda *_: pytest.fail('Refresh response must not repeat portfolio scope/bundle reads'))
    row = W.read(7, only_isin='EXACT', prepared_bundle={'portfolio_id': 7, 'rows': [{'isin': 'EXACT'}]})['rows'][0]
    assert row['ticker'] == 'ADBE'
    assert row['forecast']['fy2'] == 12
