"""The 29 reported dashes: raw membership survives unresolved/unpriced listings."""
from pathlib import Path
import csv

import pytest

from index_universe.acwi import asset_membership as M, holdings as H
from index_universe.acwi.identity import issuer_key, membership_key
from routers import _active_share as A, _asset_benchmark as B, _benchmark_index as P

MEMBERS = ['Shell PLC EUR', 'Diploma', 'RELX', 'Goodman Group', 'Everpure, Inc.',
           'CapitaLand Integr Comm Tr', 'Equity Resident', 'Avalonbay Comm. Inc',
           'Berkshire Hathaway - B', 'Alphabet - C', 'MSCI Inc. A', 'Investor AB - B']
ABSENT = ['Topicus.com', 'Medpace Holdings', 'Kinsale Capital', 'Gaztransport et Technigaz',
          'Revvity, Inc.', 'Vontobel Holding AG', 'PBF Energy', 'Oscar Health Inc.',
          'HF Sinclair Corporation', 'Moderna', 'Teleflex', 'Grail', 'Roko AB', 'EastGroup Properties',
          'Host Hotels Resorts Inc', 'American Homes 4 Rent', 'Aedifica', 'Eurocomm. Prop.',
          'Equity Lifestyle', 'Warehouses De Pauw', 'Wereldhave']


@pytest.fixture
def raw_benchmark(monkeypatch):
    # Only Apple has a resolved/priced listing. Every other source member must still be a member.
    source, stamp = H.load_acwi_holdings()
    apple = next(row for row in source if row['Ticker'] == 'AAPL')
    monkeypatch.setattr(H, 'load_latest_acwi_holdings', lambda: (source, stamp))
    monkeypatch.setattr(M, 'resolve', lambda *_: ([{
        'analysis_id': 1, 'name': apple['Name'], 'weight_pct': float(apple['Weight (%)']),
        'currency': 'USD', 'yahoo_symbol': 'AAPL'}], {'as_of': stamp}))
    monkeypatch.setattr(A, 'load_rows_via_copy', lambda *_args: [])
    monkeypatch.setattr(B, 'window_marks', lambda *_args: {1: {'start': (stamp, 100), 'end': (stamp, 100)}})
    monkeypatch.setattr(P, '_fx_to_eur', lambda *_args: {})
    monkeypatch.setattr(P, '_window_rows', lambda *_args, **_kwargs: ([{
        'company_id': 1, 'company_name': apple['Name'], 'return_eur_pct': 0}], {}))
    snapshot = A._load_benchmark_weights('ACWI', None)
    monkeypatch.setattr(A, '_benchmark_weights', lambda *_args: snapshot)
    monkeypatch.setattr(A, '_grid_by_isin', lambda _ids: {})
    monkeypatch.setattr(A, '_execution_by_isin', lambda _ids: {})
    return snapshot


@pytest.mark.parametrize('name,expected', [(name, True) for name in MEMBERS] + [(name, False) for name in ABSENT])
def test_each_reported_dash_against_the_complete_source(raw_benchmark, name, expected):
    result = A.compute_active_share([{'isin': 'NO_LOCAL_MAPPING', 'name': name, 'weight_pct': 100}],
                                    'ACWI', held_only=True)
    assert result['available']
    assert result['rows'][0]['in_benchmark'] is expected
    assert result['off_benchmark_pct'] == (0 if expected else 100)
    if expected:
        assert result['rows'][0]['benchmark_pct'] > 0
        assert result['rows'][0]['weight_calculation'] is None


def test_successor_folds_predecessors_once_without_copying_benchmark_weight(raw_benchmark):
    result = A.compute_active_share([
        {'isin': 'OLD_EQR', 'name': 'Equity Resident', 'weight_pct': 40},
        {'isin': 'OLD_AVB', 'name': 'Avalonbay Comm. Inc', 'weight_pct': 60}], 'ACWI', held_only=True)
    assert len(result['rows']) == 1
    assert result['rows'][0]['portfolio_pct'] == 100
    assert result['n_in_benchmark'] == 1
    assert membership_key('Equity Resident', '2026-08-17') != membership_key('Avalonbay Comm. Inc', '2026-08-17')
    assert membership_key('Equity Resident', '2026-08-18') == membership_key('Vivmark Residential', '2026-08-18')
    assert issuer_key('Equity Resident') != issuer_key('Vivmark Residential')  # Never a price-history alias.


def test_new_snapshot_retains_small_weights_and_listing_currency():
    rows, stamp = H.load_acwi_holdings()
    assert stamp == '2026-10-29'
    assert len(rows) == 2195
    assert sum(float(row['Weight (%)']) for row in rows) == pytest.approx(100)
    small = [row for row in rows if 0 < float(row['Weight (%)']) < .005]
    assert small  # Published 0.00% weights must not remove real constituents.
    shell = next(row for row in rows if row['Ticker'] == 'SHEL')
    assert shell['Currency'] == 'GBP'
    with Path(H._CSV_FILE).open(encoding='utf-8-sig', newline='') as stream:
        original = list(csv.reader(stream))
    assert original[1] == ['Fund Holdings as of', 'Oct 29, 2026']


@pytest.mark.parametrize('name,ticker,location,execution', [
    ('SHELL', 'SHEL', 'United Kingdom', {'isin': 'GB00BP6MXD84', 'yahoo_symbol': 'SHELL.AS', 'name': 'Shell PLC EUR', 'analysis_id': 1}),
    ('RELX PLC', 'REL', 'United Kingdom', {'isin': 'GB00B2B0DG97', 'yahoo_symbol': 'REN.AS', 'name': 'RELX', 'analysis_id': 2}),
])
def test_different_listing_ticker_resolves_by_exact_issuer_and_country(monkeypatch, name, ticker, location, execution):
    monkeypatch.setattr(M, 'load_acwi_holdings', lambda: ([{'Name': name, 'Ticker': ticker,
        'Location': location, 'Exchange': 'London Stock Exchange', 'Asset Class': 'Equity', 'Weight (%)': '1'}], '2026-10-29'))
    monkeypatch.setattr(M, '_executions', lambda: [execution])
    monkeypatch.setattr(M, '_country_by_code', lambda: {'GB': 'United Kingdom'})
    rows, _ = M.resolve()
    assert rows[0]['analysis_id'] == execution['analysis_id']
    assert rows[0]['yahoo_symbol'] == execution['yahoo_symbol']


def test_reused_ticker_cannot_override_reviewed_everpure_security(monkeypatch):
    monkeypatch.setattr(M, 'load_acwi_holdings', lambda: ([{'Name': 'EVERPURE INC CLASS A', 'Ticker': 'P',
        'Location': 'United States', 'Exchange': 'NYSE', 'Asset Class': 'Equity', 'Weight (%)': '1'}], '2026-10-29'))
    monkeypatch.setattr(M, '_executions', lambda: [
        {'isin': 'US6983541078', 'yahoo_symbol': 'P', 'name': 'Pandora Media', 'analysis_id': 1},
        {'isin': 'US74624M1027', 'yahoo_symbol': 'PSTG', 'name': 'Pure Storage Inc', 'analysis_id': 2}])
    monkeypatch.setattr(M, '_country_by_code', lambda: {'US': 'United States'})
    rows, _ = M.resolve()
    assert rows[0]['analysis_id'] == 2
    assert rows[0]['yahoo_symbol'] == 'PSTG'


def test_name_fallback_refuses_wrong_country_and_share_class(monkeypatch):
    source = {'Name': 'BERKSHIRE HATHAWAY CLASS B', 'Ticker': 'BRKB', 'Location': 'United States',
              'Exchange': 'NYSE', 'Asset Class': 'Equity', 'Weight (%)': '1'}
    monkeypatch.setattr(M, 'load_acwi_holdings', lambda: ([source], '2026-10-29'))
    monkeypatch.setattr(M, '_country_by_code', lambda: {'US': 'United States', 'GB': 'United Kingdom'})
    monkeypatch.setattr(M, '_executions', lambda: [{'isin': 'US0000000001', 'yahoo_symbol': 'OTHER',
        'name': 'Berkshire Hathaway Class A', 'analysis_id': 1}])
    assert M.resolve()[0] == []
    monkeypatch.setattr(M, '_executions', lambda: [{'isin': 'US0000000001', 'yahoo_symbol': 'BRK-A',
        'name': 'Berkshire Hathaway - A', 'analysis_id': 1}])
    assert M.resolve()[0] == []  # Shorthand must not disguise the wrong share class.
    monkeypatch.setattr(M, '_executions', lambda: [{'isin': 'US0000000001', 'yahoo_symbol': 'BRK-A',
        'name': 'Berkshire Hathaway Class A', 'analysis_id': 1}])
    assert M.resolve()[0] == []  # The older same-base-ticker fallback must also reject Class A.
    monkeypatch.setattr(M, '_executions', lambda: [{'isin': 'GB0000000001', 'yahoo_symbol': 'OTHER',
        'name': 'Berkshire Hathaway Class B', 'analysis_id': 1}])
    assert M.resolve()[0] == []


@pytest.mark.parametrize('name', ["L` Oreal", "L'Oreal", "L’ Oréal", "L'Oréal SA", "L'Ore\u0301al", 'LOREAL SA'])
def test_loreal_membership_and_weight_do_not_depend_on_local_asset_mapping(raw_benchmark, name):
    expected = raw_benchmark['bench']['loreal']['weight_pct']
    result = A.compute_active_share([{'isin': 'FR0000120321', 'name': name, 'weight_pct': 100}],
                                    'ACWI', held_only=True)
    row = result['rows'][0]
    assert row['in_benchmark'] is True
    assert row['benchmark_pct'] == pytest.approx(expected)
    assert row['benchmark_pct'] > 0
    from routers._airs_portfolio_analysis import _mark_acwi_sector_overlap
    portfolio = {'Consumer Defensive': [{'name': name, 'isin': 'FR0000120321'}]}
    benchmark = {'Consumer Defensive': [{'name': 'LOREAL SA', 'isin': None, 'weight_pct': expected}]}
    _mark_acwi_sector_overlap(portfolio, benchmark, '2026-11-06')
    assert portfolio['Consumer Defensive'][0]['in_both'] is True
    assert portfolio['Consumer Defensive'][0]['issuer_key'] == 'loreal'


@pytest.mark.parametrize('name', ['Loreal Technology', 'L Oreal Holdings Partners', 'Royal', 'L Oral'])
def test_loreal_alias_does_not_invent_other_issuer_matches(name):
    assert issuer_key(name) != 'loreal'
