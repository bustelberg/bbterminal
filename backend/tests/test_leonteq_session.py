from contextvars import ContextVar
from unittest.mock import Mock
import threading

from leonteq.session import PortalSession, on_session_thread


def resources():
    pw, browser, context, page = Mock(), Mock(), Mock(), Mock()
    browser.is_connected.return_value = True
    page.is_closed.return_value = False
    return pw, browser, context, page


def test_repeated_source_reads_reuse_one_browser_without_reusing_portfolio_data(monkeypatch):
    from leonteq import portal_login as login
    import leonteq.session as module

    scope = resources()
    scope[3].url = login.INDICES_URL
    factory = Mock(return_value=scope)
    session = PortalSession(factory=factory)
    monkeypatch.setattr(module, 'session', session)
    # Initial request signs in; the next request uses the authenticated session.
    monkeypatch.setattr(login, '_authenticated', Mock(side_effect=[False, True, True, True, True, True]))
    reads = Mock(side_effect=[{'success': True, 'version': 1}, {'success': True, 'version': 2}])
    monkeypatch.setattr(login, '_read_portfolios', reads)
    try:
        assert login._attempt_login('user', 'password')['version'] == 1
        assert login._attempt_login('user', 'password')['version'] == 2
        factory.assert_called_once()
        assert reads.call_count == 2
        assert scope[3].goto.call_count == 2
        scope[3].reload.assert_called_once_with(wait_until='domcontentloaded', timeout=30000)
        # Only the first operation fills the username/password form.
        assert scope[3].locator.return_value.first.fill.call_count == 2
    finally:
        on_session_thread(session.close)


def test_password_change_expiry_and_browser_loss_discard_the_previous_session():
    clock = [0]
    scopes = [resources() for _ in range(4)]
    factory = Mock(side_effect=scopes)
    session = PortalSession(factory=factory, clock=lambda: clock[0], max_age=60)
    assert session.get('user', 'old')[2] is False
    assert session.get('user', 'old')[2] is True
    assert session.get('user', 'new')[2] is False
    scopes[0][1].close.assert_called_once()
    clock[0] = 61
    assert session.get('user', 'new')[2] is False
    scopes[1][1].close.assert_called_once()
    scopes[2][1].is_connected.return_value = False
    assert session.get('user', 'new')[2] is False
    session.close()
    assert factory.call_count == 4


def test_browser_failure_discards_the_session_before_another_refresh(monkeypatch):
    from playwright.sync_api import TimeoutError
    from leonteq import portal_login as login
    import leonteq.session as module

    scope = resources()
    scope[3].goto.side_effect = TimeoutError('timeout')
    session = PortalSession(factory=lambda: scope)
    monkeypatch.setattr(module, 'session', session)
    result = login._attempt_login('user', 'password')
    assert result['status'] == 'timeout'
    assert session.resources is None
    scope[1].close.assert_called_once()
    scope[0].stop.assert_called_once()


def test_session_worker_keeps_one_owner_thread_and_preserves_progress_context():
    marker = ContextVar('refresh-test', default='missing')
    token = marker.set('current job')
    try:
        first = on_session_thread(lambda: (threading.get_ident(), marker.get()))
        second = on_session_thread(lambda: (threading.get_ident(), marker.get()))
        assert first == second
        assert first[0] != threading.get_ident()
        assert first[1] == 'current job'
    finally:
        marker.reset(token)
