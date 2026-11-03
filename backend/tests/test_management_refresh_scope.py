"""Only dashboard portfolios and their model dependencies incur AIRS downloads."""
from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

import airs_vermogen as V
import management_portfolios as scope
from tests._fake_supabase import FakeSupabase


def configured_accounts():
    root = Path(__file__).resolve().parent.parent / 'config'
    return [*scope.MANAGEMENT_GROUPS,
            *(r['dynamic_portefeuille'] for r in json.loads((root / 'management_topselecties.json').read_text())['topselecties']),
            *(r['dynamic_portefeuille'] for r in json.loads((root / 'airs_portfolio_exclusions.json').read_text())['test_topselecties'])]


def test_dashboard_scope_contains_the_22_reviewed_accounts():
    accounts = configured_accounts()
    assert len(accounts) == 22
    assert all(scope.is_dashboard_account(name) for name in accounts)
    for name in ('BUS_BM_AAN_kw_EUR_2026_d', 'BUS_Defensief_Kl_MV', 'WTS test 1 FX',
                 'BUS_WTS_SterkeMerken_Fx', 'TOPS_VTS_L', 'BUS_FTS_DEF_DYN'):
        assert not scope.is_dashboard_account(name)


def test_refresh_order_matches_dashboard_groups_and_descending_risk_profiles():
    ordered = [scope.key(name) for name in sorted(reversed(configured_accounts()), key=scope.dashboard_refresh_key)]
    assert ordered[:9] == [
        'bus20offdyn', 'busoffensiefdyn', 'busbepoffensiefdyn', 'busneutraaldyn', 'busdefensiefdyn',
        'topsoffbehdyn', 'topsbeoffbehdyn', 'topsneubehdyn', 'topsdefbehdyn',
    ]
    assert ordered[9:19] == [
        'aitopselectieoffdyn', 'aztopselectiedyn', 'ditopselectieoffdyn', 'europatopselectoffdyn',
        'busftsoffdyn', 'busmtsoffafsdyn', 'motopselectiedyn', 'obtopselectiedyn',
        'startopselectieoffdyn', 'vtopselectieoffdy',
    ]
    assert ordered[19:] == ['busifdyn', 'dealmakerstopseloffdyn', 'tolpoortenselectoffdyn']
    assert scope.dashboard_progress_label('BUS_IF_DYN') == 'Imagine Fund'


@pytest.mark.parametrize('cancelled', [False, True])
def test_noop_fleet_uses_visible_cached_price_checks_and_honours_cancel(monkeypatch, cancelled):
    accounts = configured_accounts()
    monkeypatch.setattr(V, '_discover_portfolios', lambda: accounts)
    monkeypatch.setattr(V, '_roster_verdicts', lambda: {name: {
        'reports_at': datetime.now(timezone.utc).isoformat(), 'reports_ok': list(V.REPORTS)} for name in accounts})
    monkeypatch.setattr(V, 'scan_one', lambda *a, **kw: pytest.fail('AIRS reports already current'))
    progress = []
    def compare(names, **options):
        assert options['freshen'] is False
        assert options['only_if_changed'] is True
        options['on_step'](0, 0, 'All 22 saved price checks are already current')
        return {'saved': 0, 'skipped': 22, 'errors': {}, 'cancelled': cancelled}
    monkeypatch.setattr('routers._airs_price_check.refresh_price_checks', compare)
    result = V.run_airs_vermogen_refresh_sync(dashboard_only=True,
        on_step=lambda *step: progress.append(step))
    assert result['status'] == ('cancelled' if cancelled else 'ok')
    assert bool(result['cancelled_at']) == cancelled
    assert result['price_checks']['skipped'] == 22
    assert any('already current' in step[2] for step in progress)


@pytest.mark.parametrize('force', [False, True])
def test_refresh_downloads_only_selected_stale_accounts_and_counts_actual_work(monkeypatch, force):
    accounts = configured_accounts()
    discovered = [*accounts, *(f'OUTSIDE_{i}' for i in range(26))]
    scans, checks, progress, completed = [], [], [], []
    saved = set()
    monkeypatch.setattr(V, '_discover_portfolios', lambda: discovered)
    monkeypatch.setattr(V, '_roster_verdicts', lambda: {accounts[0].lower(): {
        'reports_at': datetime.now(timezone.utc).isoformat(), 'reports_ok': list(V.REPORTS)}})
    monkeypatch.setattr(V, '_record_reports', lambda outcomes, stamp: saved.update(outcomes))
    monkeypatch.setattr('routers._airs_accounts._holding_counts', lambda: pytest.fail('configured portfolios must not be rejected by size'))
    def scan(name, *a, **kw):
        assert completed == scans, 'publish the previous row before starting the next account'
        scans.append(name)
        kw['on_report'](name, 'Rendement', 'ok', 'stored')
        return dict(reports_ok=list(V.REPORTS), as_of='2026-10-31', holdings=1,
                    mutaties=0, model_weights=1, errors=[])
    monkeypatch.setattr(V, 'scan_one', scan)
    monkeypatch.setattr('routers._airs_price_check.refresh_price_checks',
                        lambda names, **kw: checks.extend(names) or {'errors': {}})
    def account_done(done, total, name):
        assert name in saved, 'the row must not update before its report receipt is saved'
        assert done == len(scans)
        completed.append(name)
    result = V.run_airs_vermogen_refresh_sync(force=force, dashboard_only=True,
        on_step=lambda done, total, message: progress.append((done, total, message)),
        on_account_done=account_done)
    assert result['status'] == 'ok'
    assert result['portfolios_found'] == result['complete_accounts'] == 22
    assert result['portfolios_discovered'] == 48
    assert result['outside_dashboard'] == 26
    assert len(scans) == (22 if force else 21)
    assert set(scans) <= set(accounts)
    assert scans == sorted(scans, key=scope.dashboard_refresh_key)
    assert completed == scans
    assert scope.key(scans[0]) == 'bus20offdyn'
    assert set(checks) == set(accounts)
    starts = [step for step in result['log'] if step['kind'] == 'account_start']
    assert len(starts) == len(scans)
    assert {step['n'] for step in starts} == {len(scans)}
    assert not any('EXPECTED 44' in message for _, _, message in progress)
    assert any('Bustelberg portfolios · Bustelberg 2.0 Offensief · Rendement: ok' in message
               for _, _, message in progress)


def test_model_scan_keeps_reviewed_paired_and_recursive_child_models_only(monkeypatch):
    import deps
    from routers import _airs_account_links as links, _airs_ref as ref
    monkeypatch.setattr(deps, 'supabase', FakeSupabase({'airs_model_portfolio_link': [
        {'isin': 'CHILD', 'linked_portfolio_id': 8}, {'isin': 'LOOP', 'linked_portfolio_id': 7},
        {'isin': 'UNRELATED', 'linked_portfolio_id': 99}]}))
    monkeypatch.setattr(links, 'list_account_links', lambda: {'accounts': [
        {'portefeuille': 'BUS_Offensief_Dyn', 'model_portfolio_id': 7},
        {'portefeuille': 'OUTSIDE', 'model_portfolio_id': 99}]})
    monkeypatch.setattr(ref, 'positions', lambda: [
        {'portfolio_id': 7, 'isin': 'CHILD'}, {'portfolio_id': 8, 'isin': 'LOOP'},
        {'portfolio_id': 99, 'isin': 'UNRELATED'}])
    rows = [dict(id=pid, name=name, fixed=kind) for pid, name, kind in [
        (1, 'BUS_Offensief_FX', 'fixed'), (2, 'BUS_Offensief_Dyn', 'normaal'),
        (7, 'Manually linked', 'fixed'), (8, 'Required child', 'fixed'),
        (10, 'StarTopSelectie OFF FX', 'fixed'), (99, 'Unrelated', 'fixed')]]
    assert {row['id'] for row in scope.dashboard_model_rows(rows)} == {1, 7, 8, 10}


def test_failed_composition_is_repaired_even_after_an_earlier_success(monkeypatch):
    from routers import _airs_portfolio_store as store
    monkeypatch.setattr(store, 'supabase', FakeSupabase({'airs_model_portfolio': [
        {'id': 1, 'positions_scanned_at': '2026-10-30', 'positions_error': 'failed download'},
        {'id': 2, 'positions_scanned_at': '2026-10-30', 'positions_error': None}]}))
    assert store.unscanned_position_ids([1, 2]) == {1}


def test_paired_pricing_does_not_reprice_unrelated_accounts(monkeypatch):
    import scheduler
    from routers import _airs_account_links as links, _airs_portfolio_refresh as refresh
    monkeypatch.setattr(links, 'list_account_links', lambda: {'accounts': [
        {'portefeuille': 'BUS_Offensief_Dyn', 'model_portfolio_id': 7},
        {'portefeuille': 'BUS_BM_AAN_kw_EUR_2026_d', 'model_portfolio_id': 99}]})
    calls = []
    monkeypatch.setattr(refresh, 'refresh_portfolio', lambda pid, *a, **kw: calls.append(pid) or {})
    _, result = scheduler._body_airs_model_prices()
    assert calls == [7]
    assert result['portfolios'] == 1


def test_dashboard_audit_ignores_unselected_accounts_and_their_old_archives(monkeypatch):
    import dashboard_daily
    from routers import _airs_account_links as links
    names = [*configured_accounts(), 'OUTSIDE']
    monkeypatch.setattr(dashboard_daily, 'supabase', FakeSupabase({
        'airs_account_roster': [{'portefeuille': name, 'last_seen_at': '2026-11-02'} for name in names],
        'airs_year_archive': [{'portefeuille': 'OUTSIDE', 'year': 2025, 'kind': 'year_end', 'payload': {'complete': False}}],
    }))
    monkeypatch.setattr(links, 'list_account_links', lambda: {'accounts': []})
    result = dashboard_daily.freshness(dashboard_only=True)
    assert result['checked'] == 22
    assert 'OUTSIDE' not in {row['portefeuille'] for row in result['accounts']}
    assert not any('OUTSIDE' in issue for issue in result['global_issues'])


def test_year_end_dashboard_pass_does_not_download_unselected_accounts(monkeypatch):
    import deps
    import airs_history
    monkeypatch.setattr(deps, 'supabase', FakeSupabase({'airs_performance': [
        {'portefeuille': 'OUTSIDE', 'periode': '2025-12-31'}]}))
    monkeypatch.setattr('airs_scanner.download_transacties_sync', lambda *a: pytest.fail('out-of-scope export'))
    assert airs_history._finalise_year(None, 2025, dashboard_only=True) == []
