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
    applied: list[bool] = []
    monkeypatch.setattr(checks, '_apply_symbol_overrides', lambda: applied.append(True))
    def resolve(account, *, freshen):
        assert freshen is True
        if account == 'B':
            raise RuntimeError('Yahoo unavailable')
        return {'portefeuille': account, 'rows': [{'holding_name': 'Stock', 'yahoo_symbol': 'STOCK'}]}
    monkeypatch.setattr('routers._airs_holding_isin.resolve_account_isins', resolve)
    result = checks.refresh_price_checks(['A', 'A', 'B'])
    assert result['saved'] == 1
    assert 'B' in result['errors']
    saved = checks.saved_price_checks(['A'])['A']
    assert saved['checked_at']
    assert saved['result']['rows'][0]['holding_name'] == 'Stock'
    assert saved['result']['rows'][0]['yahoo_symbol'] == 'STOCK'
    assert applied == [True], 'a batch applies ticker pins once before comparisons'


def test_cancel_does_not_start_more_checks(monkeypatch):
    monkeypatch.setattr(checks, '_apply_symbol_overrides', lambda: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr(checks, 'save_price_check', lambda account: (_ for _ in ()).throw(AssertionError()))
    assert checks.refresh_price_checks(['A'], should_stop=lambda: True)['saved'] == 0


def test_toppenberg_price_check_replaces_certificates_with_child_strategy_holdings(monkeypatch):
    fake = FakeSupabase({'airs_price_check': []})
    monkeypatch.setattr(checks, 'supabase', fake)
    monkeypatch.setattr(checks, '_apply_symbol_overrides', lambda: 0)
    responses = {
        'TOPS_NEU_BEH_DYN': {'portefeuille': 'TOPS_NEU_BEH_DYN', 'as_of': '2026-10-21', 'rows': [
            {'holding_name': 'Star Selection Index', 'isin': 'CH1593776334'},
            {'holding_name': 'Direct bond', 'isin': 'IE00DIRECT'},
        ]},
        'StarTopSelectie OFF DYN': {'portefeuille': 'StarTopSelectie OFF DYN', 'as_of': '2026-10-20', 'rows': [
            {'holding_name': 'Actual listed stock', 'isin': 'US0000000001'},
        ]},
    }
    monkeypatch.setattr('routers._airs_holding_isin.resolve_account_isins',
                        lambda account, *, freshen: responses[account])

    saved = checks.save_price_check('TOPS_NEU_BEH_DYN')['result']

    assert saved['looked_through_certificates'] is True
    assert saved['looked_through_accounts'] == ['StarTopSelectie OFF DYN']
    assert [row['isin'] for row in saved['rows']] == ['US0000000001', 'IE00DIRECT']
    assert saved['rows'][0]['airs_price_date'] == '2026-10-20'
    assert saved['rows'][0]['certificate_holding'] == 'Star Selection Index'
