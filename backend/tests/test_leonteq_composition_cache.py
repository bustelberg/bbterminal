from unittest.mock import Mock

import pytest

from leonteq import composition_cache as cache
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def store(monkeypatch):
    store = FakeSupabase({})
    monkeypatch.setattr(cache, 'supabase', store)
    monkeypatch.setenv('LEONTEQ_USER', 'test-user')
    monkeypatch.setenv('LEONTEQ_PASSWORD', 'test-password')
    return store


def payload(value='100 EUR'):
    return {'success': True, 'status': 'success', 'portfolio_name': 'AI-TopSelectie',
            'groups': [{'name': 'Cash', 'positions': [{'name': 'EURO'}]}],
            'total_value': value, 'retrieved_at': '2026-11-05T10:00:00+00:00'}


def test_reopening_reads_durable_snapshot_without_logging_in(store, monkeypatch):
    login = Mock(return_value=payload())
    monkeypatch.setattr(cache, 'check_login', login)
    first = cache.composition('AI-TopSelectie')
    second = cache.composition('AI-TopSelectie')
    assert first['snapshot_saved'] is True
    assert second['cached'] is True
    assert second['retrieved_at'] == first['retrieved_at']
    login.assert_called_once_with('AI-TopSelectie')
    assert len(store.tables[cache.TABLE]) == 1


def test_saved_only_cache_miss_never_logs_in(store, monkeypatch):
    login = Mock(side_effect=AssertionError('preview must not log in'))
    monkeypatch.setattr(cache, 'check_login', login)
    assert cache.composition('Uncached', saved_only=True)['status'] == 'not_cached'
    login.assert_not_called()


def test_explicit_refresh_replaces_snapshot(store, monkeypatch):
    login = Mock(side_effect=[payload(), payload('200 EUR')])
    monkeypatch.setattr(cache, 'check_login', login)
    cache.composition('AI-TopSelectie')
    assert cache.composition('AI-TopSelectie', refresh=True)['cached'] is False
    assert cache.composition('AI-TopSelectie')['total_value'] == '200 EUR'
    assert login.call_count == 2
    assert len(store.tables[cache.TABLE]) == 1


def test_failed_refresh_preserves_last_successful_snapshot(store, monkeypatch):
    monkeypatch.setattr(cache, 'check_login', Mock(side_effect=[payload(), {'success': False, 'status': 'timeout'}]))
    cache.composition('AI-TopSelectie')
    result = cache.composition('AI-TopSelectie', refresh=True)
    assert result['cached'] is True
    assert result['refresh_error'] == 'timeout'
    assert cache.composition('AI-TopSelectie')['total_value'] == '100 EUR'


def test_saved_compositions_are_isolated_by_account(store, monkeypatch):
    login = Mock(side_effect=[payload(), payload('200 EUR')])
    monkeypatch.setattr(cache, 'check_login', login)
    cache.composition('AI-TopSelectie')
    monkeypatch.setenv('LEONTEQ_USER', 'another-user')
    assert cache.composition('AI-TopSelectie')['total_value'] == '200 EUR'
    assert login.call_count == 2
    assert len(store.tables[cache.TABLE]) == 2
