from unittest.mock import Mock

import pytest

from jobs import Job, JobCtx
from leonteq import recent_transactions as recent
from leonteq.portal_api import ApiUnavailable


def trade(**kwargs):
    return {'date': '2026-11-10', 'event': 'Buy', 'name': 'Example', 'instrumentCategory': 'Share',
            'reportId': 100, 'transactionNumber': 2, 'instrumentIsin': 'US0378331005',
            'totalQuantity': {'value': 10}, 'grossPrice': {'value': 100}, 'instrumentCurrency': 'USD', **kwargs}


def test_only_booked_security_trades_are_kept_and_deduplicated():
    rows = [trade(), trade(), trade(event='AMC Fees'), trade(event='Currency'),
            trade(status='Cancelled'), trade(status='Pending'), trade(event='Buy', instrumentCategory='Cash')]
    result = recent.parse_transactions({'totalRows': len(rows), 'rows': rows})
    assert len(result) == 1
    assert result[0] == {'id': '100:2', 'date': '2026-11-10', 'event': 'Buy', 'name': 'Example',
                         'isin': 'US0378331005', 'quantity': 10, 'price': 100, 'currency': 'USD'}


@pytest.mark.parametrize('payload', [
    {'rows': [trade()], 'totalRows': 2},
    {'rows': [trade(date='not a date')], 'totalRows': 1},
    {'rows': [trade(totalQuantity={'value': 'NaN'})], 'totalRows': 1},
    {'rows': [trade(reportId=None)], 'totalRows': 1},
    {'rows': [trade(), trade(name='Conflicting')], 'totalRows': 2},
])
def test_incomplete_or_conflicting_feed_never_becomes_an_action_queue(payload):
    with pytest.raises(ApiUnavailable):
        recent.parse_transactions(payload)


def test_queue_uses_fixed_connection_and_fresh_source_inputs(monkeypatch):
    from leonteq import portfolio_links, composition_cache, airs_models, holding_connections
    monkeypatch.setattr(portfolio_links, 'options', lambda kind: {'links': [
        {'leonteq_name': 'Fund', 'airs_portfolio_id': 1}] if kind == 'fixed' else []})
    monkeypatch.setattr(recent, 'read', lambda name: {'transactions': [], 'retrieved_at': 'now'})
    left = Mock(return_value={'success': True, 'groups': [{'positions': []}]})
    right = Mock(return_value={'rows': [{'Fonds': 'Example'}]})
    monkeypatch.setattr(composition_cache, 'composition', left)
    monkeypatch.setattr(airs_models, 'composition', right)
    monkeypatch.setattr(holding_connections, 'read', lambda name, portfolio_id: {'rules': []})
    ctx = JobCtx(Job(id='test', kind='transactions', label='Recent transactions'))
    result = recent.run(ctx, 'Fund', 1)
    assert result['success'] is True
    left.assert_called_once_with('Fund', refresh=True)
    right.assert_called_once_with(1, refresh=True)


def test_queue_rejects_saved_fallback_after_failed_source_refresh(monkeypatch):
    from fastapi import HTTPException
    from leonteq import portfolio_links, composition_cache
    monkeypatch.setattr(portfolio_links, 'options', lambda kind: {'links': [{'leonteq_name': 'Fund', 'airs_portfolio_id': 1}]})
    monkeypatch.setattr(recent, 'read', lambda name: {'transactions': [], 'retrieved_at': 'now'})
    monkeypatch.setattr(composition_cache, 'composition', lambda *args, **kwargs: {'success': True, 'groups': [{}], 'refresh_error': 'timeout'})
    with pytest.raises(HTTPException, match='Fresh Leonteq holdings'):
        recent.run(JobCtx(Job(id='test', kind='transactions', label='Recent')), 'Fund', 1)


def test_transaction_reader_uses_observed_pagination_and_loads_every_page():
    from leonteq.portal_api import read
    page = Mock()
    page.request.get.side_effect = [Mock(ok=True, json=Mock(return_value={'totalRows': 102, 'rows': [trade(transactionNumber=i) for i in range(100)]})),
                                    Mock(ok=True, json=Mock(return_value={'totalRows': 102, 'rows': [trade(transactionNumber=i) for i in range(100, 102)]}))]
    result = read(page, {'Fund': 'ID'}, 'Fund', transactions=True)
    assert len(result['transactions']) == 102
    assert page.request.get.call_args_list[0].args[0].endswith('index-transactions?offset=0&limit=100&sort=-transactionTime')
    assert page.request.get.call_args_list[1].args[0].endswith('index-transactions?offset=100&limit=100&sort=-transactionTime')


@pytest.mark.parametrize('second', [
    {'totalRows': 2, 'rows': []},
    {'totalRows': 3, 'rows': [trade(transactionNumber=3)]},
    {'totalRows': 2, 'rows': [trade()]},
])
def test_transaction_reader_rejects_missing_changed_or_overlapping_pages(second):
    from leonteq.portal_api import read
    page = Mock()
    page.request.get.side_effect = [Mock(ok=True, json=Mock(return_value={'totalRows': 2, 'rows': [trade()]})),
                                    Mock(ok=True, json=Mock(return_value=second))]
    with pytest.raises(ApiUnavailable):
        read(page, {'Fund': 'ID'}, 'Fund', transactions=True)
