from unittest.mock import Mock

import pytest

from leonteq import management as m
from routers import _airs_portfolio_analysis as pa


@pytest.fixture
def source(monkeypatch):
    payload = {'success': True, 'leonteq_name': 'AI-TopSelectie', 'catalogue_retrieved_at': '2026-11-16T10:00:00Z',
        'performance': {'return_window': {'portfolio_name': 'AI-TopSelectie', 'currency': 'EUR',
            'from': '2026-06-01', 'to': '2026-11-12', 'ytd_pct': 8.78,
            'retrieved_at': '2026-11-16T11:00:00Z'}},
        'summary': {'ytd': '8.78%', 'inception_date': '2026-06-01', 'as_of': '2026-11-12'},
        'composition': {'success': True, 'retrieved_at': '2026-11-16T09:00:00Z', 'groups': [
            {'name': 'Equity', 'positions': [{'name': 'Correct stock', 'isin': 'STOCK',
                'weight': '75.00%', 'total_value': '7’500.00 EUR', 'currency': 'USD',
                'performance': '32.92%', 'price_date': '2026-11-12'}]},
            {'name': 'Cash', 'positions': [{'name': 'EUR cash', 'isin': None,
                'weight': '25.00%', 'total_value': '2’500.00 EUR', 'currency': 'EUR'}]},
        ]}}
    monkeypatch.setattr(m, 'detail', lambda _: payload)
    monkeypatch.setattr(pa, '_country_by_code', lambda: {})
    monkeypatch.setattr(pa, '_grid', lambda _: {'STOCK': {'asset_class': 'equity', 'sector': 'Technology', 'currency': 'USD'}})
    monkeypatch.setattr(pa, '_bench_members', lambda _: ([], {}))
    monkeypatch.setattr(pa, '_basket_returns', lambda *_: pytest.fail('Must never reconstruct Leonteq returns'))
    monkeypatch.setattr(pa, '_index_returns', Mock(return_value={'2026-06-01': {'eur_pct': 5, 'as_of': '2026-11-12'}}))
    return payload


def test_full_source_values_and_official_return_reach_shared_analyse(source):
    result = m.analysis('AITopSelectie', 'TEST')
    assert result['returns']['source'] == 'leonteq'
    assert result['returns']['portfolio_ytd_pct'] == 8.78
    assert result['returns']['ytd_excess_pct'] == pytest.approx(3.78)
    pa._index_returns.assert_called_once_with('TEST', ['2026-06-01'], end='2026-11-12')
    assert result['weight_basis'] == 'book'
    assert len(result['book_holdings']) == 2
    stock, cash = result['book_holdings']
    assert stock['current_value_eur'] == 7500
    assert stock['weight_now_pct'] == 75
    assert stock['currency'] == 'USD'
    assert stock['own_return_pct'] == 32.92  # Display source AVG; never substitute it for portfolio YTD.
    assert stock['own_return_source'] == 'leonteq_average'
    assert stock['return_pct'] is None
    assert stock['contribution_pct'] is None  # No matching-period Leonteq attribution in snapshot.
    assert stock['start_value_eur'] is None
    assert cash['current_value_eur'] == 2500
    assert cash['weight_now_pct'] == 25
    assert cash['bucket'] == 'Cash'
    assert 'pair' not in result['book_note'].lower()


def test_return_history_aligns_benchmark_without_opening_holdings_or_catalogue_dates(source):
    source['summary'] = {}
    result = m.analysis('AITopSelectie', 'TEST')
    assert result['returns']['portfolio_ytd_pct'] == 8.78
    assert result['returns']['benchmark_ytd_pct'] == 5
    assert result['returns']['ytd_excess_pct'] == pytest.approx(3.78)
    pa._index_returns.assert_called_once_with('TEST', ['2026-06-01'], end='2026-11-12')
    assert result['returns']['book_reason'] is None
    assert result['book_holdings'][0]['current_value_eur'] == 7500


def test_unknown_catalogue_date_uses_verified_history_boundaries(source):
    source['summary'].pop('as_of')
    result = m.analysis('AITopSelectie', 'TEST')
    assert result['returns']['portfolio_ytd_pct'] == 8.78
    assert result['returns']['benchmark_ytd_pct'] == 5
    assert result['returns']['ytd_excess_pct'] == pytest.approx(3.78)
    assert result['returns']['portfolio_as_of'] == '2026-11-12'


def test_missing_opening_composition_does_not_prevent_exact_ai_benchmark_alignment(source):
    source['performance'] = {}
    source['accounting'] = {'success': False, 'reason': 'No opening holdings on 8 June',
        'retrieved_at': '2026-11-16T11:00:00Z',
        'index_details': {'currency': 'EUR', 'issueDate': '2026-05-19'},
        'history': {'series': [['2026-06-08', 100], ['2026-11-12', 98.21]],
            'performances': [{'nameOrIsin': 'AI-TopSelectie', 'performanceYTD': {'value': -1.79}}]}}
    pa._index_returns.return_value = {'2026-06-08': {'eur_pct': 8, 'as_of': '2026-11-12'}}
    result = m.analysis('AITopSelectie', 'TEST')
    pa._index_returns.assert_called_once_with('TEST', ['2026-06-08'], end='2026-11-12')
    assert result['returns']['portfolio_ytd_pct'] == -1.79
    assert result['returns']['benchmark_ytd_pct'] == 8
    assert result['returns']['ytd_excess_pct'] == pytest.approx(-9.79)
    assert result['book_holdings'][0]['contribution_pct'] is None


def test_no_calendar_ytd_substitute_when_authoritative_return_dates_cannot_be_read(source, monkeypatch):
    source['performance'] = {}
    monkeypatch.setattr(m.return_windows, 'read', Mock(side_effect=m.return_windows.ApiUnavailable('Source dates missing')))
    result = m.analysis('AITopSelectie', 'TEST')
    assert result['returns']['portfolio_ytd_pct'] == 8.78
    assert result['returns']['benchmark_ytd_pct'] is None
    assert result['returns']['ytd_excess_pct'] is None
    assert result['returns']['book_reason'] == 'Source dates missing'
    pa._index_returns.assert_not_called()


def test_zero_and_negative_source_position_returns_do_not_depend_on_opening_ledger(source):
    stock = source['composition']['groups'][0]['positions'][0]
    for value, expected in [('0.00%', 0), (0, 0), ('−12.34%', -12.34), ('—', None)]:
        stock['performance'] = value
        row = m.analysis('AITopSelectie', 'TEST')['book_holdings'][0]
        assert row['own_return_pct'] == expected
        assert row['contribution_pct'] is None


def test_missing_holdings_fail_closed(source):
    source['success'] = False
    with pytest.raises(m.HTTPException, match='Leonteq holdings are unavailable'):
        m.analysis('AITopSelectie', 'TEST')


def test_non_euro_values_are_not_mislabelled_and_risk_uses_market_inputs(source, monkeypatch):
    source['composition']['groups'][0]['positions'][0]['total_value'] = '7’500.00 USD'
    risk = Mock(return_value={'STOCK': {'vol_5y_pct': 12}})
    monkeypatch.setattr(pa, '_holding_risk', risk)
    result = m.analysis('AITopSelectie', 'TEST', include_holding_risk=True)
    assert result['book_holdings'][0]['current_value_eur'] is None
    assert result['book_holdings'][0]['vol_5y_pct'] == 12
    risk.assert_called_once_with(['STOCK'], 'TEST')


def test_reconciled_source_ledger_populates_existing_holdings_sold_and_fee_contract(source):
    source['accounting'] = {'success': True, 'result': {
        'available': True, 'from': '2025-12-31', 'to': '2026-11-12',
        'basis_eur': 10000, 'reported_ytd_pct': 8.78,
        'cash': {'opening': 2500, 'closing': 2500},
        'positions': {
            'STOCK': {'name': 'Correct stock', 'isin': 'STOCK', 'opening': 7500, 'closing': 8378,
                      'profit': 878, 'buys': 0, 'dividends': 100, 'return_pct': 11.7067, 'contribution_pct': 8.78},
            'SOLD': {'name': 'Sold stock', 'isin': 'SOLD', 'opening': 0, 'closing': 0,
                     'profit': 10, 'buys': 100, 'dividends': 0, 'return_pct': 10, 'contribution_pct': .1},
            'fee:AMC': {'name': 'Leonteq AMC fees', 'isin': None, 'opening': 0, 'closing': 0,
                        'profit': -10, 'buys': 0, 'dividends': 0, 'return_pct': None, 'contribution_pct': -.1},
        }}}
    result = m.analysis('AITopSelectie', 'TEST')
    stock, cash = result['book_holdings']
    assert stock['contribution_pct'] == 8.78
    assert stock['income_eur'] == 100
    assert stock['own_return_source'] == 'leonteq_transactions'
    assert stock['own_return_pct'] != 32.92
    assert cash['start_value_eur'] == 2500
    assert cash['weight_start_pct'] == 25
    assert result['realised']['positions'][0]['closed_out'] is True
    assert result['realised']['certificate_adjustments'][0]['contribution_pct'] == -.1
    assert result['realised']['reconciles'] is True
