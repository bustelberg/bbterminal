from unittest.mock import Mock

import pytest

from leonteq import portfolio_cache as cache
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def store(monkeypatch):
    store = FakeSupabase({})
    monkeypatch.setattr(cache, 'supabase', store)
    monkeypatch.setenv('LEONTEQ_USER', 'test-user')
    monkeypatch.setenv('LEONTEQ_PASSWORD', 'test-password')
    return store


def payload(name='AI-TopSelectie'):
    return {'success': True, 'status': 'success', 'portfolios': [{'name': name}],
            'retrieved_at': '2026-11-05T10:00:00+00:00'}


def test_opening_saved_list_never_logs_in_even_on_a_miss(store, monkeypatch):
    login = Mock(return_value=payload())
    monkeypatch.setattr(cache, 'check_login', login)
    assert cache.saved_portfolios()['status'] == 'not_cached'
    login.assert_not_called()
    assert cache.portfolios()['snapshot_saved'] is True
    assert cache.saved_portfolios()['cached'] is True
    assert cache.portfolios()['cached'] is True
    login.assert_called_once_with()


def test_explicit_refresh_updates_durable_list(store, monkeypatch):
    login = Mock(side_effect=[payload(), payload('Updated portfolio')])
    monkeypatch.setattr(cache, 'check_login', login)
    cache.portfolios()
    assert cache.portfolios(refresh=True)['cached'] is False
    assert cache.saved_portfolios()['portfolios'] == [{'name': 'Updated portfolio'}]
    assert login.call_count == 2
    assert len(store.tables[cache.TABLE]) == 1


def test_failed_refresh_retains_list_and_timestamp(store, monkeypatch):
    monkeypatch.setattr(cache, 'check_login', Mock(side_effect=[payload(), {'success': False, 'status': 'timeout'}]))
    first = cache.portfolios()
    result = cache.portfolios(refresh=True)
    assert result['success'] is True
    assert result['cached'] is True
    assert result['retrieved_at'] == first['retrieved_at']
    assert result['refresh_error'] == 'timeout'


def test_saved_list_is_scoped_to_lynqs_account(store, monkeypatch):
    login = Mock(return_value=payload())
    monkeypatch.setattr(cache, 'check_login', login)
    cache.portfolios()
    monkeypatch.setenv('LEONTEQ_USER', 'another-user')
    assert cache.saved_portfolios()['status'] == 'not_cached'
    monkeypatch.setenv('LEONTEQ_PASSWORD', '')
    monkeypatch.delenv('LYNQS_PASS', raising=False)
    assert cache.saved_portfolios()['status'] == 'missing_credentials'
