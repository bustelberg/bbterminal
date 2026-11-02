from routers import _airs_price_check as checks
from tests._fake_supabase import FakeSupabase
import pytest


def test_unchanged_price_checks_finish_without_resolution_or_yahoo(monkeypatch):
    from datetime import datetime, timezone
    fake = FakeSupabase({'airs_price_check': [{'portefeuille': 'A',
        'checked_at': datetime.now(timezone.utc).isoformat(), 'result': {'input_version': 'v1', 'rows': []}}]})
    monkeypatch.setattr(checks, 'supabase', fake)
    monkeypatch.setattr(checks, '_price_check_version', lambda: 'v1')
    monkeypatch.setattr(checks, '_checked_rows', lambda *a, **kw: (_ for _ in ()).throw(AssertionError('unchanged comparison')))
    monkeypatch.setattr(checks, '_apply_symbol_overrides', lambda: (_ for _ in ()).throw(AssertionError('no Yahoo work')))
    progress = []
    result = checks.refresh_price_checks(['A'], freshen=False, only_if_changed=True,
        on_step=lambda *step: progress.append(step))
    assert result == {'saved': 0, 'skipped': 1, 'errors': {}}
    assert 'already current' in progress[-1][2]
    assert fake.writes == []


def test_outdated_or_legacy_check_rebuilds_from_stored_prices_and_stamps_inputs(monkeypatch):
    from datetime import datetime, timezone
    fake = FakeSupabase({'airs_price_check': [{'portefeuille': 'A',
        'checked_at': datetime.now(timezone.utc).isoformat(), 'result': {'rows': []}}]})
    monkeypatch.setattr(checks, 'supabase', fake)
    monkeypatch.setattr(checks, '_price_check_version', lambda: 'v2')
    calls = []
    monkeypatch.setattr(checks, '_checked_rows', lambda account, *, freshen: (
        calls.append((account, freshen)) or {'as_of': '2026-10-31'}, [{'isin': 'TEST'}], []))
    result = checks.refresh_price_checks(['A'], freshen=False, only_if_changed=True)
    assert calls == [('A', False)]
    assert result['saved'] == 1
    assert fake.tables['airs_price_check'][0]['result']['input_version'] == 'v2'
    # The next unchanged press reuses the just-created snapshot.
    assert checks.refresh_price_checks(['A'], freshen=False, only_if_changed=True)['skipped'] == 1
    assert calls == [('A', False)]


@pytest.mark.parametrize('version,checked_at', [('v2', None), (None, None), ('v1', '2000-01-01T00:00:00Z')])
def test_changed_unverified_or_old_sources_do_not_reuse_a_comparison(monkeypatch, version, checked_at):
    from datetime import datetime, timezone
    fake = FakeSupabase({'airs_price_check': [{'portefeuille': 'A',
        'checked_at': checked_at or datetime.now(timezone.utc).isoformat(),
        'result': {'input_version': 'v1', 'rows': []}}]})
    monkeypatch.setattr(checks, 'supabase', fake)
    monkeypatch.setattr(checks, '_price_check_version', lambda: version)
    calls = []
    monkeypatch.setattr(checks, '_checked_rows', lambda account, *, freshen: (
        calls.append(freshen) or {}, [], []))
    result = checks.refresh_price_checks(['A'], freshen=False, only_if_changed=True)
    assert calls == [False]
    assert result['saved'] == 1 and result['skipped'] == 0


def test_source_fingerprint_includes_prices_and_children_but_not_discovery(monkeypatch):
    from io import BytesIO
    from common import pg
    from routers import _prepared_analyse
    from asset_pipeline import symbol_override, isin_alias
    tables = []
    state = {'stamp': b'v1', 'pins': {}}
    monkeypatch.setattr(pg, '_run_copy', lambda sql, args: tables.extend(args[0]) or BytesIO(state['stamp']))
    monkeypatch.setattr(_prepared_analyse, 'code_version', lambda: 'code')
    monkeypatch.setattr(symbol_override, 'load_symbol_overrides', lambda: state['pins'])
    monkeypatch.setattr(symbol_override, 'alias_price_override_isins', lambda: set())
    monkeypatch.setattr(isin_alias, 'load_aliases', lambda: {})
    first = checks._price_check_version()
    assert first and first == checks._price_check_version()
    assert 'airs_account_roster' not in tables
    assert {'airs_holding', 'asset_price', 'asset_execution', 'airs_model_portfolio_link', 'fx_rate'} <= set(tables)
    state['stamp'] = b'v2'
    assert checks._price_check_version() != first
    second = checks._price_check_version()
    state['pins'] = {'TEST': 'NEW'}
    assert checks._price_check_version() != second


def test_batch_reuses_toppenberg_children_before_saving(monkeypatch):
    from routers import _airs_holding_isin
    fake = FakeSupabase({})
    monkeypatch.setattr(checks, 'supabase', fake)
    calls = []
    def resolve(account, *, freshen, price_checks):
        assert not freshen
        # No comparisons have been written while shared source data is being read.
        assert not fake.writes
        calls.append(account)
        rows = ([{'holding_name': 'Star Selection Index', 'isin': 'CERT'}] if account.startswith('TOPS_')
                else [{'holding_name': 'Stock', 'isin': 'TEST'}])
        return {'as_of': '2026-10-31', 'rows': rows}
    monkeypatch.setattr(_airs_holding_isin, '_resolve_account_isins', resolve)
    result = checks.refresh_price_checks(['TOPS_OFF_BEH_DYN', 'TOPS_NEU_BEH_DYN', 'StarTopSelectie OFF DYN'], freshen=False)
    assert result['saved'] == 3
    assert calls.count('StarTopSelectie OFF DYN') == 1


def test_cancellation_saves_completed_comparison_and_stops_next_account(monkeypatch):
    fake = FakeSupabase({})
    monkeypatch.setattr(checks, 'supabase', fake)
    calls, progress = [], []
    monkeypatch.setattr(checks, '_checked_rows', lambda account, *, freshen: (
        calls.append(account) or {'as_of': '2026-10-31'}, [], []))
    result = checks.refresh_price_checks(['A', 'B'], freshen=False,
        should_stop=lambda: bool(calls), on_step=lambda *step: progress.append(step))
    assert calls == ['A']
    assert result['cancelled']
    assert result['saved'] == 1
    assert fake.tables['airs_price_check'][0]['portefeuille'] == 'A'
    assert progress[-1][0] < progress[-1][1]


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


def test_row_recheck_fetches_only_the_selected_resolved_series(monkeypatch):
    fake = FakeSupabase({'airs_price_check': [], 'asset_execution': [
        {'isin': 'JP3837800006', 'analysis_id': 77, 'yahoo_symbol': '7741.T'},
    ]})
    monkeypatch.setattr(checks, 'supabase', fake)
    monkeypatch.setattr(checks, '_apply_symbol_overrides', lambda: 0)
    freshens = []
    monkeypatch.setattr(checks, '_checked_rows', lambda account, *, freshen: (
        freshens.append(freshen) or {'portefeuille': account, 'as_of': '2026-10-21'},
        [{'holding_name': 'HOYA Corporation', 'isin': 'JP3837800006', 'yahoo_symbol': '7741.T'}], []))
    fetched = []
    monkeypatch.setattr('asset_pipeline.store.store_series',
                        lambda analysis_id, symbol, _from: fetched.append((analysis_id, symbol)))

    saved = checks.recheck_price_check_row('TOPS_OFF_BEH_DYN', ' jp3837800006 ')

    assert fetched == [(77, '7741.T')]
    assert freshens == [False, False]
    assert saved['result']['rows'][0]['isin'] == 'JP3837800006'


def test_row_recheck_prefers_a_small_incremental_window_when_history_exists(monkeypatch):
    fake = FakeSupabase({'airs_price_check': [], 'asset_execution': [
        {'isin': 'JP3837800006', 'analysis_id': 77, 'yahoo_symbol': '7741.T'},
    ], 'asset_price': [
        {'analysis_id': 77, 'target_date': '2026-10-20', 'close': 27_000},
    ]})
    monkeypatch.setattr(checks, 'supabase', fake)
    monkeypatch.setattr(checks, '_apply_symbol_overrides', lambda: 0)
    monkeypatch.setattr(checks, '_checked_rows', lambda account, *, freshen: (
        {'portefeuille': account}, [{'holding_name': 'HOYA Corporation', 'isin': 'JP3837800006'}], []))
    extended = []
    monkeypatch.setattr('asset_pipeline.store.extend_series',
                        lambda analysis_id, symbol, since: extended.append((analysis_id, symbol, since)) or 3)
    monkeypatch.setattr('asset_pipeline.store.store_series',
                        lambda *_args: (_ for _ in ()).throw(AssertionError('must not reload full history')))

    checks.recheck_price_check_row('TOPS_OFF_BEH_DYN', 'JP3837800006')

    assert extended == [(77, '7741.T', '2026-10-20')]


def test_row_recheck_refuses_an_isin_not_displayed_by_the_account(monkeypatch):
    monkeypatch.setattr(checks, '_apply_symbol_overrides', lambda: 0)
    monkeypatch.setattr(checks, '_checked_rows', lambda account, *, freshen: (
        {'portefeuille': account}, [{'isin': 'US0000000001'}], []))

    try:
        checks.recheck_price_check_row('BOOK', 'JP3837800006')
    except ValueError as exc:
        assert 'not a displayed holding' in str(exc)
    else:
        raise AssertionError('row recheck must not refresh an arbitrary ISIN')
