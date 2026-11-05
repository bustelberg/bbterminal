"""Credential handling and truthful portal-session detection."""
from types import SimpleNamespace
from unittest.mock import Mock

from leonteq import portal_login as login


def test_missing_credentials_never_opens_browser(monkeypatch):
    for key in ('LEONTEQ_USER', 'LEONTEQ_PASSWORD', 'LYNQS_USER', 'LYNQS_PASS'):
        monkeypatch.delenv(key, raising=False)
    attempt = Mock()
    monkeypatch.setattr(login, '_attempt_login', attempt)
    assert login.check_login() == {'success': False, 'status': 'missing_credentials'}
    attempt.assert_not_called()


def test_performance_retries_read_only_browser_failure_once(monkeypatch):
    monkeypatch.setenv('LEONTEQ_USER', 'test-user')
    monkeypatch.setenv('LEONTEQ_PASSWORD', 'test-password')
    attempt = Mock(side_effect=[{'success': False, 'status': 'browser_error'}, {'success': True, 'months': []}])
    monkeypatch.setattr(login, '_attempt_login', attempt)
    monkeypatch.setattr(login.time, 'sleep', lambda _: None)
    assert login.check_performance('AI-TopSelectie')['success'] is True
    assert attempt.call_count == 2
    assert not login._login_lock.locked()


def test_performance_failure_retains_stages_without_retrying_rejected_credentials(monkeypatch):
    monkeypatch.setenv('LEONTEQ_USER', 'test-user')
    monkeypatch.setenv('LEONTEQ_PASSWORD', 'test-password')
    attempt = Mock(return_value={'success': False, 'status': 'login_rejected'})
    monkeypatch.setattr(login, '_attempt_login', attempt)
    assert len(login.check_performance('AI-TopSelectie')['attempts']) == 1
    attempt.assert_called_once()


def test_browser_diagnostics_do_not_include_form_values_or_tokens():
    diagnostic = login.browser_diagnostic(RuntimeError('net::ERR_CONNECTION_RESET secret-password https://login/?token=private'), 'login_navigation')
    assert diagnostic == {'stage': 'login_navigation', 'error_type': 'RuntimeError', 'reason': 'ERR_CONNECTION_RESET'}


def test_credentials_stay_server_side_and_concurrent_checks_are_refused(monkeypatch):
    monkeypatch.setenv('LEONTEQ_USER', 'test-user')
    monkeypatch.setenv('LEONTEQ_PASSWORD', 'test-password')

    def attempt(user, password):
        assert (user, password) == ('test-user', 'test-password')
        assert login.check_login() == {'success': False, 'status': 'busy'}
        return {'success': True, 'status': 'success'}

    monkeypatch.setattr(login, '_attempt_login', attempt)
    assert login.check_login() == {'success': True, 'status': 'success'}
    assert not login._login_lock.locked()


def test_browser_failure_releases_lock(monkeypatch):
    monkeypatch.setenv('LEONTEQ_USER', 'test-user')
    monkeypatch.setenv('LEONTEQ_PASSWORD', 'test-password')
    monkeypatch.setattr(login, '_attempt_login', Mock(side_effect=ImportError))
    assert login.check_login() == {'success': False, 'status': 'browser_unavailable'}
    assert not login._login_lock.locked()


def test_login_requires_a_session_and_a_return_to_the_portal():
    context = Mock()
    page = SimpleNamespace(url=login.INDICES_URL)
    context.cookies.return_value = []
    assert not login._authenticated(context, page)
    context.cookies.return_value = [{'name': 'lynqs-auth', 'value': ''}]
    assert not login._authenticated(context, page)
    context.cookies.return_value = [{'name': 'lynqs-auth', 'value': 'test-session'}]
    assert login._authenticated(context, page)
    page.url = 'https://www.lynqs.com/login'
    assert not login._authenticated(context, page)


def test_portfolio_read_keeps_live_fields_and_reports_retrieval_time():
    page = Mock()
    rows = [{'name': 'AI-TopSelectie', 'sponsor': 'Bustelberg B.V.', 'value': '982.09',
             'aum': '834’611', 'itd': '-1.79%', 'ytd': '-1.79%'}]
    page.locator.return_value.evaluate_all.return_value = rows
    result = login._read_portfolios(page)
    assert result['success'] is True
    assert result['portfolios'] == rows
    assert result['retrieved_at']


def test_unreadable_table_is_not_reported_as_success():
    page = Mock()
    for rows in ([], [{'name': None}]):
        page.locator.return_value.evaluate_all.return_value = rows
        assert login._read_portfolios(page) == {'success': False, 'status': 'portfolios_failed'}


def test_composition_refuses_unknown_or_ambiguous_portfolio_names():
    page = Mock()
    for count in (0, 2):
        page.locator.return_value.filter.return_value.count.return_value = count
        assert login._read_composition(page, 'Unknown') == {'success': False, 'status': 'portfolio_not_found'}
    page.get_by_text.assert_not_called()


def test_composition_preserves_groups_and_portal_totals(monkeypatch):
    monkeypatch.setattr(login, '_read_composition_export', Mock(return_value=None))
    page = Mock()
    page.locator.return_value.filter.return_value.count.return_value = 1
    groups = [{'name': 'Cash', 'weight': '100%', 'positions': [{'name': 'EURO', 'isin': None}]}]
    page.locator.return_value.filter.return_value.first.evaluate.return_value = groups
    page.locator.return_value.filter.return_value.locator.return_value.inner_text.side_effect = ['834’610.77 EUR', '100%']
    result = login._read_composition(page, 'AI-TopSelectie')
    assert result['success'] is True
    assert result['portfolio_name'] == 'AI-TopSelectie'
    assert result['groups'] == groups
    assert result['total_value'] == '834’610.77 EUR'
    assert result['total_weight'] == '100%'
    assert result['source'] == 'composition_table'
