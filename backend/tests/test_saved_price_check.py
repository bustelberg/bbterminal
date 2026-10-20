from routers import _airs_price_check as checks
from tests._fake_supabase import FakeSupabase


def test_saved_reads_do_not_resolve_or_refresh(monkeypatch):
    fake = FakeSupabase({'airs_price_check': [
        {'portefeuille': 'A', 'checked_at': '2026-10-20T10:00:00Z', 'result': {'rows': []}}]})
    monkeypatch.setattr(checks, 'supabase', fake)
    def forbidden(*args, **kwargs):
        raise AssertionError('Dashboard read must not calculate or fetch prices')
    monkeypatch.setattr('routers._airs_holding_isin.resolve_account_isins', forbidden)
    assert checks.saved_price_checks(['A'])['A']['result'] == {'rows': []}
    assert checks.saved_price_checks(['B']) == {}


def test_refresh_saves_only_after_resolution_and_retains_previous_on_failure(monkeypatch):
    fake = FakeSupabase({'airs_price_check': []})
    monkeypatch.setattr(checks, 'supabase', fake)
    def resolve(account, *, freshen):
        assert freshen is True
        if account == 'B':
            raise RuntimeError('Yahoo unavailable')
        return {'portefeuille': account, 'rows': [{'holding_name': 'Stock'}]}
    monkeypatch.setattr('routers._airs_holding_isin.resolve_account_isins', resolve)
    result = checks.refresh_price_checks(['A', 'A', 'B'])
    assert result['saved'] == 1
    assert 'B' in result['errors']
    saved = checks.saved_price_checks(['A'])['A']
    assert saved['checked_at']
    assert saved['result']['rows'][0]['holding_name'] == 'Stock'


def test_cancel_does_not_start_more_checks(monkeypatch):
    monkeypatch.setattr(checks, 'save_price_check', lambda account: (_ for _ in ()).throw(AssertionError()))
    assert checks.refresh_price_checks(['A'], should_stop=lambda: True)['saved'] == 0
