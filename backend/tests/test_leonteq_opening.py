from decimal import Decimal as D

import pytest

from leonteq import opening as o
from tests.test_leonteq_accounting import boundary, records, transaction


def order(**fields):
    return {'id': 'fill', 'status': 'BOOKED', 'orderDate': '2026-01-02', 'side': 'BUY',
        'instrumentName': 'Stock', 'instrumentRic': 'RIC', 'quantity': {'value': 5},
        'filledQuantity': {'value': 5}, 'netAmount': {'value': 500},
        'netPrice': {'value': 100}, 'fxRate': {'value': 1},
        'conversionCurrency': 'EUR', 'amcCurrency': 'EUR', 'underlyingCurrency': 'EUR', **fields}


def initial(existing=(), orders=None):
    return o.initial_records(list(existing), orders or [order()],
        {'rows': [{'instrumentRic': 'RIC', 'instrumentIsin': 'STOCK'}]}, '2026-01-01', '2026-01-02')


def test_booked_initial_instruction_supplies_missing_trade_using_source_ric_mapping():
    result = initial()
    assert result[0]['isin'] == 'STOCK'
    assert result[0]['cash_eur'] == '-500'
    assert result[0]['source'] == 'leonteq_booked_instruction'
    duplicate = records(transaction(quantity=5, amount=500))
    assert initial(duplicate) == duplicate


def test_unbooked_instructions_do_not_become_fills_and_conflicts_fail_closed():
    assert initial(orders=[order(status='CANCELLED')]) == []
    with pytest.raises(o.ApiUnavailable, match='conflict'):
        initial(records(transaction(quantity=5, amount=490)))
    with pytest.raises(o.ApiUnavailable, match='Incomplete'):
        initial(orders=[order(filledQuantity={'value': 4})])


def test_all_cash_opening_is_reconstructed_and_checked_against_independent_index():
    first = boundary('2026-01-02', 495, 500)
    marks = [('2026-01-01', D(100)), ('2026-01-02', D('99.5'))]
    result = o.reconstruct(first, initial(), '2026-01-01', marks)
    assert result['nav'] == 1000
    assert set(result['positions']) == {'cash:EUR'}
    with pytest.raises(o.ApiUnavailable, match='first return'):
        o.reconstruct(first, initial(), '2026-01-01', [(marks[0]), ('2026-01-02', D(101))])
    with pytest.raises(o.ApiUnavailable, match='every opening holding'):
        o.reconstruct(boundary('2026-01-02', 495, 500, quantity=6), initial(), '2026-01-01', marks)


def test_instructions_reader_is_get_only_and_paginates_without_overlap():
    from unittest.mock import Mock
    from leonteq.portal_api import read
    page = Mock()
    page.request.get.side_effect = [Mock(ok=True, json=lambda: {'totalRows': 2, 'rows': [order()]}),
        Mock(ok=True, json=lambda: {'totalRows': 2, 'rows': [order(id='second')]})]
    assert len(read(page, {'Fund': 'id'}, 'Fund', instructions=True)['instructions']) == 2
    assert 'offset=1' in page.request.get.call_args.args[0]
    page.request.post.assert_not_called()


def test_transport_failure_does_not_expose_browser_headers():
    from unittest.mock import Mock
    from leonteq.portal_api import read
    page = Mock()
    page.request.get.side_effect = RuntimeError('Cookie: private-authentication')
    with pytest.raises(o.ApiUnavailable) as error:
        read(page, {'Fund': 'id'}, 'Fund', instructions=True)
    assert 'private-authentication' not in str(error.value)
    assert error.value.__suppress_context__


def test_collector_recovers_only_actual_inception_and_retains_execution_evidence(monkeypatch):
    from leonteq import accounting, portal_api
    first = {'closingPriceDate': '2026-01-02', 'rows': [
        {'name': 'Stock', 'instrumentIsin': 'STOCK', 'instrumentRic': 'RIC',
         'instrumentCategory': 'Share', 'amcCurrency': 'EUR', 'currency': 'EUR',
         'totalValueAmcCcy': 495, 'totalUnits': 5},
        {'name': 'EUR', 'instrumentCategory': 'Cash', 'amcCurrency': 'EUR',
         'currency': 'EUR', 'totalValueAmcCcy': 500, 'totalUnits': 500}],
        'totals': {'totalValueAmcCcy': 995}}
    def read(_page, _ids, name, **options):
        if options.get('accounting'):
            return {'transactions': []}
        if options.get('cash_history'):
            return {'complete': False}
        if options.get('instructions'):
            return {'instructions': [order()]}
        if options.get('history'):
            return {'details': {'issueDate': '2026-01-01', 'currency': 'EUR'},
                'history': {'series': [['2026-01-01', 100], ['2026-01-02', 99.5]],
                            'performances': [{'nameOrIsin': name, 'performanceYTD': {'value': -.5}}]}}
        return {'accounting_valuation': first if options['valuation_date']=='2026-01-02' else
                {'closingPriceDate': '2026-01-01', 'rows': [], 'totals': {}}}
    monkeypatch.setattr(portal_api, 'read', read)
    result = accounting._collect(None, {}, 'Fund', '2026-01-02', lambda *_: None)
    assert result['success']
    assert result['opening_reconstruction']['opening_cash_eur'] == '1000'
    assert result['transactions'][0]['source'] == 'leonteq_booked_instruction'
    assert result['result']['positions']['STOCK']['contribution_pct'] == -.5
    legacy_month = {key: value for key, value in result['monthly_attribution']['2026-01'].items()
                    if key != 'unreconciled_cash'}
    assert accounting.monthly_cash_verified(result, legacy_month)
