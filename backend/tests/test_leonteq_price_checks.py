from unittest.mock import Mock
from datetime import datetime, timezone

import pytest

from jobs import Job, JobCtx
from leonteq import price_checks as prices
from tests._fake_supabase import FakeSupabase


def row(price=100, **extra):
    return {'source': 'Leonteq', 'name': 'Example', 'isin': 'ONE', 'source_price': price,
            'source_currency': 'USD', 'source_date': '2026-11-06', **extra}


def quote(price=100, **extra):
    return {'symbol': 'EXAMPLE', 'currency': 'USD', 'bars': [('2026-11-06', price)], **extra}


@pytest.mark.parametrize('value,alert', [(105, False), (95, False), (105.0001, True), (94.9999, True)])
def test_alert_threshold_is_strictly_greater_than_five_percent(value, alert):
    result = prices.compare(row(value), quote())
    assert result['alert'] is alert
    assert result['difference_pct'] == pytest.approx(value - 100)


def test_pence_are_normalized_before_comparing_prices():
    result = prices.compare(row(100, source_currency='GBP'), quote(10000, currency='GBp'))
    assert result['status'] == 'ok'
    assert result['difference_pct'] == 0
    assert result['yahoo_price'] == 100
    assert result['yahoo_currency'] == 'GBP'


@pytest.mark.parametrize('source,reference', [
    (row(source_date=None), quote()),
    (row(source_date='2026-11-09'), quote()),
    (row(source_currency='EUR'), quote()),
    (row(0), quote()), (row('NaN'), quote()), (row(), None),
])
def test_missing_data_dates_and_currencies_are_never_reported_as_passing(source, reference):
    result = prices.compare(source, reference)
    assert result['status'] == 'unavailable'
    assert result['difference_pct'] is None
    assert result['alert'] is False


def test_newer_yahoo_closes_cannot_enter_an_older_source_comparison():
    result = prices.compare(row(), quote(bars=[('2026-11-06', 100), ('2026-11-09', 200)]))
    assert result['status'] == 'ok'
    assert result['yahoo_date'] == '2026-11-06'
    assert result['yahoo_price'] == 100


def test_cash_does_not_require_a_yahoo_mapping():
    assert prices.compare(row(None, cash=True), None)['status'] == 'cash'


@pytest.mark.parametrize('source', ['Leonteq', 'AIRS'])
@pytest.mark.parametrize('isin', [None, 'CASH-IDENTIFIER'])
def test_effectenrekening_is_cash_even_with_an_identifier(monkeypatch, source, isin):
    from leonteq import airs_models, composition_cache
    monkeypatch.setattr(composition_cache, 'composition', lambda *args, **kwargs: {
        'success': True, 'groups': [{'name': 'Other', 'positions': [
            {'name': ' Effectenrekening ', 'isin': isin}]}]})
    monkeypatch.setattr(airs_models, 'composition', lambda *args, **kwargs: {
        'rows': [{'Fonds': ' EFFECTENREKENING ', 'ISINCode': isin}]})
    holding = prices._read_source({'leonteq_name': 'Fund', 'airs_portfolio_id': 1}, source)[0]
    result = prices.compare(holding, quote())
    assert result['status'] == 'cash'
    assert result['difference_pct'] is None
    assert result['alert'] is False


def test_yahoo_uses_unadjusted_closes_and_excludes_the_forming_session(monkeypatch):
    from asset_pipeline import yahoo
    now = int(datetime(2026, 11, 9, 12, tzinfo=timezone.utc).timestamp())
    prior = int(datetime(2026, 11, 6, 12, tzinfo=timezone.utc).timestamp())
    monkeypatch.setattr(prices.time, 'time', lambda: now)
    monkeypatch.setattr(yahoo, 'is_closed_bar', lambda _: True)
    chart = Mock(return_value={'meta': {'symbol': 'EXAMPLE', 'currency': 'USD',
        'exchangeTimezoneName': 'UTC', 'currentTradingPeriod': {'regular': {'end': now + 3600}}},
        'timestamp': [prior, now], 'indicators': {'quote': [{'close': [100, 999]}], 'adjclose': [{'adjclose': [50, 50]}]}})
    monkeypatch.setattr(yahoo, 'chart', chart)
    assert prices.fetch_yahoo('EXAMPLE')['bars'] == [('2026-11-06', 100)]
    chart.assert_called_once_with('EXAMPLE', rng='3mo', interval='1d')


def test_batch_checks_all_links_and_reuses_sources_and_yahoo_symbols(monkeypatch):
    from leonteq import portfolio_links
    links = [{'leonteq_name': 'Same fund', 'airs_portfolio_id': i, 'airs_name': f'Dynamic {i}'} for i in (1, 2)]
    monkeypatch.setattr(portfolio_links, 'options', lambda kind: {'links': links})
    source = Mock(side_effect=lambda link, name: [row(106, source=name)])
    monkeypatch.setattr(prices, '_read_source', source)
    monkeypatch.setattr(prices, '_executions', lambda _: {'ONE': 'EXAMPLE'})
    fetch = Mock(return_value=quote())
    monkeypatch.setattr(prices, 'fetch_yahoo', fetch)
    result = prices.run(JobCtx(Job(id='test', kind='prices', label='Prices')))
    assert source.call_count == 3  # One Leonteq fund, two Dynamic portfolios.
    fetch.assert_called_once_with('EXAMPLE')
    assert result['summary'] == {'portfolios': 2, 'yahoo_symbols': 1, 'checked': 4, 'alerts': 4, 'unavailable': 0, 'cash': 0}
    assert result['refresh_error'] is None  # An alert is a completed check, not a failed refresh.


def test_batch_retains_other_sources_when_one_portfolio_read_fails(monkeypatch):
    from leonteq import portfolio_links
    monkeypatch.setattr(portfolio_links, 'options', lambda _: {'links': [
        {'leonteq_name': 'Fund', 'airs_portfolio_id': 1, 'airs_name': 'Dynamic'}]})
    def source(link, name):
        if name == 'Leonteq':
            raise ValueError('Source unavailable')
        return [row(source=name)]
    monkeypatch.setattr(prices, '_read_source', source)
    monkeypatch.setattr(prices, '_executions', lambda _: {'ONE': 'EXAMPLE'})
    monkeypatch.setattr(prices, 'fetch_yahoo', lambda _: quote())
    result = prices.run(JobCtx(Job(id='test', kind='prices', label='Prices')))
    assert result['summary']['checked'] == 1
    assert result['errors']
    assert result['refresh_error'] == 'incomplete_price_checks'


def test_reviewed_symbol_overrides_are_used_without_repointing_or_guessing(monkeypatch):
    from asset_pipeline import symbol_override
    monkeypatch.setattr(prices, 'supabase', FakeSupabase({'asset_execution': [
        {'isin': 'ONE', 'yahoo_symbol': 'OLD', 'status': 'ok'}]}))
    monkeypatch.setattr(symbol_override, 'load_symbol_overrides', lambda: {'ONE': 'RIGHT'})
    assert prices._executions(['ONE', 'MISSING']) == {'ONE': 'RIGHT'}


def test_recheck_uses_fresh_server_holding_and_resolves_currency_mismatch(monkeypatch):
    from leonteq import portfolio_links, price_symbols
    monkeypatch.setattr(portfolio_links, 'options', lambda _: {'links': [
        {'leonteq_name': 'Fund', 'airs_portfolio_id': 1, 'airs_name': 'Dynamic'}]})
    source = Mock(return_value=[row(106)])
    monkeypatch.setattr(prices, '_read_source', source)
    monkeypatch.setattr(prices, '_executions', lambda _: {'ONE': 'WRONG'})
    resolve = Mock(return_value='RIGHT')
    monkeypatch.setattr(price_symbols, 'resolve', resolve)
    monkeypatch.setattr(prices, 'fetch_yahoo', lambda symbol: quote(symbol=symbol, currency='EUR' if symbol == 'WRONG' else 'USD'))
    result = prices.run(JobCtx(Job(id='test', kind='prices', label='Prices')), name='Fund', portfolio_id=1,
                        source='Leonteq', isin='ONE', holding_name='Example')
    source.assert_called_once_with({'leonteq_name': 'Fund', 'airs_portfolio_id': 1, 'airs_name': 'Dynamic'}, 'Leonteq')
    assert resolve.call_args.kwargs['force'] is True
    assert result['portfolios'][0]['rows'][0]['symbol'] == 'RIGHT'
    assert result['summary']['alerts'] == 1


def test_batch_resolves_missing_symbol_once_per_isin_currency(monkeypatch):
    from leonteq import portfolio_links, price_symbols
    monkeypatch.setattr(portfolio_links, 'options', lambda _: {'links': [
        {'leonteq_name': 'Fund', 'airs_portfolio_id': 1, 'airs_name': 'Dynamic'}]})
    monkeypatch.setattr(prices, '_read_source', lambda link, source: [row(source=source)])
    monkeypatch.setattr(prices, '_executions', lambda _: {})
    resolve = Mock(return_value='RIGHT')
    monkeypatch.setattr(price_symbols, 'resolve', resolve)
    monkeypatch.setattr(prices, 'fetch_yahoo', lambda symbol: quote(symbol=symbol))
    result = prices.run(JobCtx(Job(id='test', kind='prices', label='Prices')))
    assert resolve.call_count == 1
    assert result['summary']['checked'] == 2


def test_resolution_failures_stay_on_the_affected_rows(monkeypatch):
    from leonteq import portfolio_links, price_symbols
    monkeypatch.setattr(portfolio_links, 'options', lambda _: {'links': [
        {'leonteq_name': 'Fund', 'airs_portfolio_id': 1, 'airs_name': 'Dynamic'}]})
    monkeypatch.setattr(prices, '_read_source', lambda link, source: [row(source=source)])
    monkeypatch.setattr(prices, '_executions', lambda _: {})
    monkeypatch.setattr(price_symbols, 'resolve', Mock(side_effect=RuntimeError('provider unavailable')))
    result = prices.run(JobCtx(Job(id='test', kind='prices', label='Prices')))
    assert result['errors'] == []
    assert result['summary']['unavailable'] == 2
    assert all('Recheck this row' in r['reason'] for r in result['portfolios'][0]['rows'])
