from unittest.mock import Mock

import pytest

from leonteq import return_windows as w


def history():
    return {'series': [['2026-06-08', 100], ['2026-11-12', 98.21]],
            'performances': [{'nameOrIsin': 'Fund', 'performanceYTD': {'value': -1.79}}]}


def test_later_inception_uses_actual_index_start_not_january_or_commercial_issue_date():
    result = w.parse(history(), {'currency': 'EUR', 'issueDate': '2026-05-19'}, 'Fund', '2026-11-16T10:00:00Z')
    assert result['from'] == '2026-06-08'
    assert result['to'] == '2026-11-12'
    assert result['ytd_pct'] == -1.79


def test_existing_portfolio_uses_year_opening_mark():
    data = {'series': [['2025-11-01', 100], ['2025-12-31', 120], ['2026-11-12', 132]],
            'performances': [{'nameOrIsin': 'Fund', 'performanceYTD': {'value': 10}}]}
    assert w.parse(data, {'currency': 'EUR', 'issueDate': '2025-01-01'}, 'Fund', 'now')['from'] == '2025-12-31'


def test_truncated_or_inconsistent_history_never_becomes_a_comparison():
    with pytest.raises(w.ApiUnavailable, match='year-opening'):
        w.parse(history(), {'currency': 'EUR', 'issueDate': '2025-01-01'}, 'Fund', 'now')
    wrong = history()
    wrong['performances'][0]['performanceYTD']['value'] = 18.06
    with pytest.raises(w.ApiUnavailable, match='reconcile'):
        w.parse(wrong, {'currency': 'EUR', 'issueDate': '2026-05-19'}, 'Fund', 'now')


def test_failed_position_accounting_still_supplies_verified_return_window():
    source = {'leonteq_name': 'Fund', 'catalogue_retrieved_at': '2026-11-16T09:00:00Z',
              'accounting': {'success': False, 'reason': 'Opening holdings missing',
                  'history': history(), 'index_details': {'currency': 'EUR', 'issueDate': '2026-05-19'},
                  'retrieved_at': '2026-11-16T10:00:00Z'}}
    assert w.cached(source)['from'] == '2026-06-08'
    source['catalogue_retrieved_at'] = '2026-11-16T11:00:00Z'
    assert w.cached(source) is None


def test_lightweight_acquisition_reads_only_return_history_and_preserves_monthly_dates(monkeypatch):
    import deps
    from leonteq import portal_login, session, portal_api, portfolio_cache
    from tests._fake_supabase import FakeSupabase
    database = FakeSupabase({})
    monkeypatch.setattr(deps, 'supabase', database)
    monkeypatch.setattr(portfolio_cache, '_account_key', lambda: 'account')
    monkeypatch.setattr(portal_login, 'check_login', lambda: {'success': True})
    monkeypatch.setattr(session.session, 'resources', (None, None, None, Mock()))
    monkeypatch.setattr(session, 'on_session_thread', lambda fn: fn())
    read = Mock(return_value={'history': history(), 'details': {'currency': 'EUR', 'issueDate': '2026-05-19'}})
    monkeypatch.setattr(portal_api, 'read', read)
    source = {'leonteq_name': 'Fund', 'performance': {'months': [{'month': '2026-10', 'return_pct': 1}],
              'retrieved_at': '2026-11-15T09:00:00Z'}}
    result, error = w.ensure(source)
    assert error is None
    assert result['from'] == '2026-06-08'
    assert read.call_args.kwargs == {'history': True}
    saved = database.tables['leonteq_performance_snapshot'][0]['payload']
    assert saved['months'] == source['performance']['months']
    assert saved['retrieved_at'] == '2026-11-15T09:00:00Z'
    source['performance'] = saved
    assert w.ensure(source)[0] == result
    assert read.call_count == 1
