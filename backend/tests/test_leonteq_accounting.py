from decimal import Decimal as D
from unittest.mock import Mock

import pytest

from leonteq import accounting as a, portal_api


def transaction(event='Buy', quantity=1, amount=100, number=1, **fields):
    return {'reportId': 1, 'transactionNumber': number, 'date': '2026-01-02',
            'name': 'Stock', 'instrumentIsin': 'STOCK', 'event': event,
            'instrumentCategory': 'Share', 'instrumentCurrency': 'EUR', 'bookingCurrency': 'EUR',
            'totalQuantity': {'value': quantity}, 'grossPrice': {'value': 98},
            'grossAmount': {'value': 98}, 'netPrice': {'value': 100},
            'netAmount': {'value': amount}, 'bookingFx': {'value': 1},
            'amcBaseCcyFx': {'value': 1}, **fields}


def records(*rows):
    return a.transactions({'rows': list(rows), 'totalRows': len(rows)})


def boundary(stamp, stock, cash, quantity=5):
    return {'date': stamp, 'nav': D(str(stock + cash)), 'positions': {
        'STOCK': {'name': 'Stock', 'isin': 'STOCK', 'cash': False, 'value': D(str(stock)), 'quantity': D(str(quantity))},
        'cash:EUR': {'name': 'EUR', 'isin': None, 'cash': True, 'value': D(str(cash)), 'quantity': D(str(cash))},
    }}


def test_reader_preserves_dividends_net_amounts_and_execution_fx_without_double_tax():
    buy = transaction(instrumentCurrency='USD', netAmount={'value': 100}, bookingFx={'value': .9}, amcBaseCcyFx={'value': .95})
    dividend = transaction('Dividend', quantity=5, amount=-9, number=2,
        instrumentCurrency='USD', bookingFx={'value': .9}, grossAmount={'value': -10})
    fee = transaction('AMC Fees', quantity=0, amount=1, number=3, instrumentIsin=None)
    parsed = records(buy, dividend, fee, dividend)
    assert len(parsed) == 3
    assert parsed[0]['cash_eur'] == '-90.0'
    assert parsed[0]['net_price'] == '100'
    assert parsed[0]['gross_amount'] == '98'
    assert parsed[1]['cash_eur'] == '8.1'
    assert parsed[2]['cash_eur'] == '-1'


def test_negative_eur_cash_is_a_cash_balance_not_a_short_security():
    raw = {'closingPriceDate': '2026-01-02', 'rows': [
        {'name': 'Stock', 'instrumentIsin': 'STOCK', 'instrumentCategory': 'Share',
         'currency': 'EUR', 'amcCurrency': 'EUR', 'totalValueAmcCcy': 1100, 'totalUnits': 10},
        {'name': 'EURO', 'instrumentCategory': 'Cash', 'currency': 'EUR',
         'amcCurrency': 'EUR', 'totalValueAmcCcy': -100, 'totalUnits': -100}],
         'totals': {'totalValueAmcCcy': 1000}}
    assert a.valuation(raw, '2026-01-02')['positions']['cash:EUR']['value'] == -100
    raw['rows'][0]['totalUnits'] = -10
    with pytest.raises(a.ApiUnavailable, match='Short holdings'):
        a.valuation(raw, '2026-01-02')


def test_native_currency_settlements_use_the_source_amc_fx():
    sell = transaction('Sell', quantity=-10, amount=-100, instrumentCurrency='USD',
        bookingCurrency='USD', bookingFx={'value': 1}, amcBaseCcyFx={'value': .9})
    buy = transaction('Buy', quantity=100, amount=100, number=2, instrumentCurrency='USD',
        bookingCurrency='USD', bookingFx={'value': 1}, amcBaseCcyFx={'value': .9})
    parsed = records(sell, buy)
    assert [r['cash_eur'] for r in parsed] == ['90.0', '-90.0']
    assert sum(D(r['cash_eur']) for r in parsed) == 0


def test_foreign_cash_dividend_and_fx_return_are_separate_from_external_funding():
    opening = boundary('2025-12-31', 500, 500)
    closing = boundary('2026-01-02', 500, 500)
    closing['positions']['cash:USD'] = {'name': 'US dollar', 'isin': None, 'cash': True,
        'value': D(9), 'quantity': D(10)}
    closing['nav'] += D(9)
    ledger = records(transaction('Dividend', quantity=5, amount=-10,
        instrumentCurrency='USD', bookingCurrency='USD', amcBaseCcyFx={'value': .9}))
    result = a.calculate([opening, closing], ledger,
        [('2025-12-31', D(100)), ('2026-01-02', D('100.9'))], .9)
    assert result['positions']['STOCK']['profit'] == 9
    assert result['positions']['cash:USD']['profit'] == 0
    assert result['has_external_flows'] is False


def test_conflicting_duplicates_or_truncated_feeds_fail_closed():
    with pytest.raises(a.ApiUnavailable, match='Incomplete'):
        a.transactions({'rows': [transaction()], 'totalRows': 2})
    with pytest.raises(a.ApiUnavailable, match='Conflicting'):
        records(transaction(), transaction(amount=110))


def test_net_purchases_dividends_and_fees_reconcile_and_link_across_days():
    opening = boundary('2025-12-31', 500, 500)
    day1 = boundary('2026-01-02', 660, 405, quantity=6)
    day2 = boundary('2026-01-05', 720, 405, quantity=6)
    ledger = records(transaction(), transaction('Dividend', amount=-10, number=2),
                     transaction('AMC Fees', amount=5, number=3, instrumentIsin=None))
    result = a.calculate([opening, day1, day2], ledger,
                         [('2025-12-31', D(100)), ('2026-01-02', D('106.5')), ('2026-01-05', D('112.5'))], 12.5)
    stock = result['positions']['STOCK']
    assert stock['profit'] == 130
    assert stock['buys'] == 100
    assert stock['dividends'] == 10
    assert stock['return_pct'] == pytest.approx(130 / 600 * 100)
    assert result['positions']['fee:AMC']['profit'] == -5
    assert sum(r['contribution_pct'] for r in result['positions'].values()) == pytest.approx(12.5)
    assert stock['contribution_pct'] != pytest.approx(130 / 1000 * 100)  # linked, not naive period P&L weighting


def test_fully_sold_positions_remain_in_attribution():
    opening = boundary('2025-12-31', 500, 500)
    closing = boundary('2026-01-02', 0, 1050, quantity=0)
    result = a.calculate([opening, closing], records(transaction('Sell', quantity=-5, amount=-550)),
                         [('2025-12-31', D(100)), ('2026-01-02', D(105))], 5)
    assert result['positions']['STOCK']['closing'] == 0
    assert result['positions']['STOCK']['profit'] == 50
    assert result['positions']['STOCK']['contribution_pct'] == 5


def test_external_funding_requires_a_source_reconciled_timing_basis():
    before = boundary('2025-12-31', 500, 500)
    after = boundary('2026-01-02', 660, 510, quantity=6)  # EUR100 funding at start, EUR70 investment gain
    rate = D(70) / D(1100)
    result = a.calculate([before, after], records(transaction(), transaction('Dividend', amount=-10, number=2)),
        [('2025-12-31', D(100)), ('2026-01-02', 100*(1+rate))], float(rate*100))
    assert result['positions']['STOCK']['contribution_pct'] == pytest.approx(float(rate*100))
    with pytest.raises(a.ApiUnavailable, match='flow timing'):
        a.calculate([before, after], records(transaction(), transaction('Dividend', amount=-10, number=2)),
            [('2025-12-31', D(100)), ('2026-01-02', D('106.5'))], 6.5)


@pytest.mark.parametrize('bad', [
    transaction(netAmount=None), transaction(bookingFx=None),
    transaction(event='Transfer'), transaction(bookingCurrency='USD'),
])
def test_missing_net_cash_or_unsupported_events_never_become_contributions(bad):
    with pytest.raises(a.ApiUnavailable):
        a.calculate([boundary('2025-12-31', 500, 500), boundary('2026-01-02', 600, 400, 6)],
                    records(bad), [('2025-12-31', D(100)), ('2026-01-02', D(100))], 0)


def test_splits_or_missing_trades_are_not_assumed_to_be_profit():
    with pytest.raises(a.ApiUnavailable, match='quantities'):
        a.calculate([boundary('2025-12-31', 500, 500), boundary('2026-01-02', 500, 500, 10)],
                    [], [('2025-12-31', D(100)), ('2026-01-02', D(100))], 0)


def test_historical_request_uses_confirmed_date_parameter_and_never_current_fallback():
    page = Mock()
    raw = {'rows': [], 'totals': {}, 'closingPriceDate': '2025-12-31'}
    page.request.get.return_value = Mock(ok=True, json=lambda: raw)
    result = portal_api.read(page, {'Fund': 'id'}, 'Fund', valuation_date='2025-12-31')
    assert page.request.get.call_args.args[0].endswith('/composition?sort=%2Bname&date=2025-12-31')
    assert result['accounting_valuation'] == raw
    with pytest.raises(a.ApiUnavailable, match='opening valuation is unavailable'):
        a.valuation(raw, '2025-12-31')
    with pytest.raises(a.ApiUnavailable, match='date does not match'):
        a.valuation(raw, '2026-01-02')


def test_missing_opening_snapshot_is_recorded_without_substituting_later_date(monkeypatch):
    calls = []
    def read(_page, _ids, name, **options):
        calls.append(options)
        if options.get('accounting'):
            return {'transactions': []}
        if options.get('cash_history'):
            return {'complete': False}
        if options.get('history'):
            return {'details': {'issueDate': '2025-01-01', 'currency': 'EUR'}, 'history': {
                'series': [['2025-12-31', 100], ['2026-01-02', 101]],
                'performances': [{'nameOrIsin': name, 'performanceYTD': {'value': 1}}]}}
        return {'accounting_valuation': {'rows': [], 'totals': {}, 'closingPriceDate': options['valuation_date']}}
    monkeypatch.setattr(portal_api, 'read', read)
    result = a._collect(None, {}, 'Fund', '2026-01-02', lambda *_: None)
    assert result['success'] is False
    assert '2025-12-31' in result['reason']
    assert list(result['valuations']) == ['2025-12-31']
    assert not result['result']['available']


def test_complete_source_acquisition_reads_every_boundary_before_publishing(monkeypatch):
    opening = boundary('2025-12-31', 500, 500)
    closing = boundary('2026-01-02', 660, 405, 6)
    def raw(snapshot):
        return {'closingPriceDate': snapshot['date'],
            'rows': [{'name': p['name'], 'instrumentIsin': p['isin'],
                'instrumentCategory': 'Cash' if p['cash'] else 'Share',
                'amcCurrency': 'EUR', 'currency': 'EUR',
                'totalValueAmcCcy': {'value': float(p['value'])},
                'totalUnits': {'value': float(p['quantity'])}} for p in snapshot['positions'].values()],
            'totals': {'totalValueAmcCcy': {'value': float(snapshot['nav'])}}}
    valuations = {opening['date']: raw(opening), closing['date']: raw(closing)}
    source = Mock(side_effect=lambda _page, _ids, name, **options:
        {'transactions': records(transaction(), transaction('Dividend', amount=-10, number=2),
                                  transaction('AMC Fees', amount=5, number=3, instrumentIsin=None))} if options.get('accounting') else
        {'details': {'issueDate': '2025-01-01', 'currency': 'EUR'}, 'history': {
            'series': [['2025-12-31', 100], ['2026-01-02', 106.5]],
            'performances': [{'nameOrIsin': name, 'performanceYTD': {'value': 6.5}}]}} if options.get('history') else
        {'complete': True, 'certificates': []} if options.get('cash_history') else
        {'accounting_valuation': valuations[options['valuation_date']]})
    monkeypatch.setattr(portal_api, 'read', source)
    progress = Mock()
    result = a._collect(None, {}, 'Fund', '2026-01-02', progress)
    assert result['success'] is True
    assert result['status'] == 'reconciled'
    assert len(result['transactions']) == 3
    assert list(result['valuations']) == ['2025-12-31', '2026-01-02']
    assert result['result']['reported_ytd_pct'] == 6.5
    assert result['result']['positions']['STOCK']['contribution_pct'] == 7
    assert result['result']['positions']['fee:AMC']['contribution_pct'] == -.5
    assert source.call_args_list[1].kwargs == {'history': True}
    assert progress.call_count == 2
    # If ?to= is ignored, collect the same dates as the published source YTD.
    ignored_end = a._collect(None, {}, 'Fund', '2026-01-01', Mock())
    assert ignored_end['success'] is True
    assert ignored_end['result']['to'] == '2026-01-02'
    source.reset_mock()
    incremental = a._collect(None, {}, 'Fund', '2026-01-02', Mock(), previous=result)
    assert incremental['success'] is True
    assert incremental['cached_valuations'] == 1
    assert [call.kwargs['valuation_date'] for call in source.call_args_list if 'valuation_date' in call.kwargs] == ['2026-01-02']


def test_estimates_keep_unexplained_difference_separate_from_company_profit():
    opening = boundary('2025-12-31', 500, 500)
    closing = boundary('2026-01-02', 550, 600)
    points = [('2025-12-31', D(100)), ('2026-01-02', D(104))]
    with pytest.raises(a.ApiUnavailable, match='does not reconcile'):
        a.calculate([opening, closing], [], points, 4)
    result = a.calculate([opening, closing], [], points, 4, allow_estimates=True)
    assert result['estimated'] is True
    assert result['positions']['STOCK']['profit'] == 50
    assert result['positions']['STOCK']['contribution_pct'] == 5
    assert 'adjustment:unallocated' not in result['positions']
    assert sum(row['contribution_pct'] for row in result['positions'].values()) == 5
    assert result['calculated_contribution_pct'] == 5
    assert result['difference_pct'] == -1


def test_source_cash_movements_are_compared_without_hiding_unexplained_cash():
    opening = boundary('2025-12-31', 500, 500)
    closing = boundary('2026-01-02', 550, 600)
    points = [('2025-12-31', D(100)), ('2026-01-02', D(105))]
    source_flows = [{'date': '2026-01-02', 'cash_eur': '90'}]
    with pytest.raises(a.ApiUnavailable, match='cash movements and position ledger differ'):
        a.calculate([opening, closing], [], points, 5, cash_flows=source_flows)
    result = a.calculate([opening, closing], [], points, 5, cash_flows=source_flows, allow_estimates=True)
    assert result['estimated'] is True
    assert result['source_cash_flows_available'] is True
    assert result['cash_differences'] == [{'from': '2025-12-31', 'to': '2026-01-02', 'difference_eur': 10}]
    assert result['positions']['STOCK']['profit'] == 50


def test_snapshot_is_durable_account_scoped_and_reads_never_start_source_sessions(monkeypatch):
    import deps
    from leonteq import portfolio_cache, portal_login, session
    from tests._fake_supabase import FakeSupabase
    database = FakeSupabase({})
    monkeypatch.setattr(deps, 'supabase', database)
    monkeypatch.setattr(portfolio_cache, '_account_key', lambda: 'account-A')
    login = Mock(return_value={'success': True})
    monkeypatch.setattr(portal_login, 'check_login', login)
    assert a.saved('Fund') is None
    login.assert_not_called()
    monkeypatch.setattr(session.session, 'resources', (None, None, None, Mock()))
    monkeypatch.setattr(session, 'on_session_thread', lambda fn: fn())
    payload = {'version': a.VERSION, 'success': False, 'source': 'Leonteq',
               'status': 'accounting_unavailable', 'reason': 'Missing opening source date',
               'retrieved_at': '2026-11-16T10:00:00Z', 'result': {'available': False}}
    monkeypatch.setattr(a, '_collect', lambda *_: payload)
    assert a.refresh('Fund', '2026-11-12') == payload
    assert a.saved('Fund') == payload
    assert login.call_count == 1
    monkeypatch.setattr(portfolio_cache, '_account_key', lambda: 'account-B')
    assert a.saved('Fund') is None
