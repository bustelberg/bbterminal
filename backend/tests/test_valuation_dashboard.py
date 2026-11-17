"""One payload and one forced stream; authoritative source and partial-save boundaries."""
import asyncio
from copy import deepcopy

import pytest
from fastapi import HTTPException
from jobs import JobCancelled
from services import valuation_dashboard as D
from services import portfolio_valuation as V
from leonteq import composition_cache, portfolio_cache, management
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def selected(monkeypatch):
    row = {'id': 1, 'name': 'One', 'management_group': 'bustelberg'}
    monkeypatch.setattr(V, 'portfolios', lambda: [row])
    return row


def test_single_cached_payload_contains_all_tab_inputs_without_source_refresh(selected, monkeypatch):
    monkeypatch.setattr(D, 'holdings', lambda _selected: ([{'isin': 'CURRENT', 'name': 'Current', 'weight_pct': 5}], {'composition_date': '2026-11-13'}))
    monkeypatch.setattr(D, 'supabase', FakeSupabase({V.TABLE: [{'portfolio_id': 1, 'prepared_at': 'stamp',
        'payload': {'method': V.METHOD, 'rows': [{'isin': 'OLD_AIRS', 'name': 'Old', 'status': 'ok'}]}}]}))
    calls = []
    def workbook(pid, **kwargs):
        calls.append((pid, kwargs))
        assert kwargs['include_membership'] is True
        assert [row['isin'] for row in kwargs['prepared_bundle']['rows']] == ['CURRENT']
        return {'parameters': {'years': 3}, 'rows': [{'isin': 'CURRENT'}], 'membership_legend': [{'code': 'BB'}]}
    monkeypatch.setattr(D.W, 'read', workbook)
    monkeypatch.setattr(D, '_refresh_holdings', lambda *_: pytest.fail('GET must not force providers'))
    result = D.read(1)
    assert len(calls) == 1
    assert result['workbook']['parameters'] == {'years': 3}
    assert result['workbook']['membership_legend'] == [{'code': 'BB'}]
    assert result['rows'][0]['isin'] == 'CURRENT'


def test_cached_missing_forecasts_use_repaired_saved_feed(selected, monkeypatch):
    payload = {'method': V.METHOD, 'evidence': {'source': 'AIRS'}, 'rows': [{'isin': 'HITACHI'}],
        'workbook': {'parameters': {}, 'rows': [{'isin': 'HITACHI', 'assumptions': {},
            'selections': ['BB'], 'forecast': {'reason': 'Missing forecasts'}}]}}
    monkeypatch.setattr(D, 'supabase', FakeSupabase({V.TABLE: [
        {'portfolio_id': 1, 'prepared_at': 'stamp', 'payload': payload}]}))
    def saved_inputs(pid, **kwargs):
        assert kwargs['include_membership'] is False
        assert kwargs['prepared_bundle'] == payload
        return {'rows': [{'isin': 'HITACHI', 'forecast': {'fy1': 216.89, 'fy2': 261.26}}]}
    monkeypatch.setattr(D.W, 'read', saved_inputs)
    monkeypatch.setattr(D, '_refresh_holdings', lambda *_: pytest.fail('GET must not fetch providers'))
    row = D.read(1)['workbook']['rows'][0]
    assert row['forecast']['fy1'] == 216.89
    assert row['selections'] == ['BB']


@pytest.mark.parametrize('price_date', [None, '2020-01-01'])
def test_cached_quotes_recheck_saved_inputs_without_fetching_providers(selected, monkeypatch, price_date):
    from datetime import date
    today = date.today().isoformat()
    payload = {'method': V.METHOD, 'evidence': {'source': 'AIRS'}, 'rows': [{'isin': 'RECEIPT'}],
        'workbook': {'as_of': '2020-01-01', 'parameters': {}, 'rows': [{'isin': 'RECEIPT',
            'assumptions': {}, 'selections': ['BB'], 'quote': {'price': 12, 'price_date': price_date}}]}}
    monkeypatch.setattr(D, 'supabase', FakeSupabase({V.TABLE: [
        {'portfolio_id': 1, 'prepared_at': 'stamp', 'payload': payload}]}))
    monkeypatch.setattr(D.W, 'read', lambda *args, **kwargs: {'as_of': today, 'rows': [
        {'isin': 'RECEIPT', 'ticker': 'NSE:LT', 'quote': {'price': 4155.5, 'price_date': today}}]})
    monkeypatch.setattr(D.W, 'attach_fx', lambda workbook: workbook)
    workbook = D.read(1)['workbook']
    assert workbook['rows'][0]['quote']['price_date'] == today
    assert workbook['rows'][0]['selections'] == ['BB']
    assert workbook['as_of'] == today


def test_topselectie_holdings_never_use_an_airs_model(selected, monkeypatch):
    selected.update(management_group='topselecties', name='FamilieTopSelectie')
    monkeypatch.setattr(D, '_leonteq_identity', lambda *_args, **_kwargs: 'Family Index')
    monkeypatch.setattr(V, 'company_holdings', lambda *_: pytest.fail('No AIRS Topselecties fallback'))
    monkeypatch.setattr(composition_cache, 'composition', lambda *_args, **_kwargs: {'success': True,
        'as_of': '2026-11-13', 'retrieved_at': '2026-11-16T12:00:00Z', 'groups': [
            {'name': 'Share', 'positions': [{'isin': 'LEONTEQ', 'name': 'Company', 'weight': '5%'}]},
            {'name': 'Cash', 'positions': [{'isin': None, 'weight': '95%'}]}]})
    rows, evidence = D.holdings(selected)
    assert rows == [{'isin': 'LEONTEQ', 'name': 'Company', 'weight_pct': 5}]
    assert evidence['source'] == 'Leonteq'
    monkeypatch.setattr(composition_cache, 'composition', lambda *_args, **_kwargs: {'success': False})
    with pytest.raises(HTTPException, match='No AIRS|no AIRS'):
        D.holdings(selected)


def test_missing_leonteq_snapshot_returns_a_valid_unprepared_bundle(selected, monkeypatch):
    selected.update(management_group='topselecties', name='AITopSelectie')
    monkeypatch.setattr(D, 'supabase', FakeSupabase({V.TABLE: []}))
    monkeypatch.setattr(D, '_leonteq_identity', lambda *_: 'AI-TopSelectie')
    monkeypatch.setattr(composition_cache, 'composition', lambda *args, **kwargs: {'success': False, 'status': 'not_cached'})
    monkeypatch.setattr(D.W, 'read', lambda *_args, **_kwargs: pytest.fail('No source-input reads before first preparation'))
    result = D.read(1)
    assert result['ready'] is False and result['stale'] is True
    assert result['rows'] == result['workbook']['rows'] == []
    assert result['workbook']['parameters']
    assert 'Press Refresh' in result['detail']


def test_unprepared_read_does_not_hide_real_holdings_failures(selected, monkeypatch):
    monkeypatch.setattr(D, 'supabase', FakeSupabase({V.TABLE: []}))
    def failed(_selected):
        raise HTTPException(503, 'Storage unavailable')
    monkeypatch.setattr(D, 'holdings', failed)
    with pytest.raises(HTTPException, match='Storage unavailable'):
        D.read(1)


def test_leonteq_timeout_explains_stage_impact_and_retry_without_browser_secrets():
    message = D._leonteq_failure('holdings', {
        'status': 'timeout', 'diagnostics': {'stage': 'authentication', 'error': 'secret token'}}, 'AITopSelectie')
    assert 'AITopSelectie: Leonteq timed out while waiting for Leonteq sign-in to finish.' in message
    assert 'Company price and forecast updates have not started.' in message
    assert 'previously saved valuation remains unchanged' in message
    assert 'Try Refresh again in a minute.' in message
    assert 'secret token' not in message
    assert 'AIRS fallback' not in message


def test_forced_airs_refresh_scopes_only_the_selected_model(selected, monkeypatch):
    from services import airs_refresh
    calls = []
    def collect(**kwargs):
        calls.append(kwargs)
        return {'selected': [{'id': 1, 'holdings': 5}], 'errors': []}
    monkeypatch.setattr(airs_refresh, 'collect_model_compositions', collect)
    monkeypatch.setattr(portfolio_cache, 'portfolios', lambda **_: pytest.fail('Selected AIRS source must not refresh Topselecties'))
    D._refresh_holdings(selected, lambda: None)
    assert len(calls) == 1
    assert calls[0]['portfolio_ids'] == {1}
    assert calls[0]['force'] is True


def test_forced_leonteq_refresh_is_one_selected_composition_and_never_airs(selected, monkeypatch):
    from services import airs_refresh
    selected.update(management_group='topselecties', name='Family')
    calls = []
    monkeypatch.setattr(management, 'entries', lambda: [{'display_name': 'Family', 'leonteq_name': 'Family Index'}])
    monkeypatch.setattr(portfolio_cache, 'saved_portfolios', lambda: {'success': False, 'portfolios': []})
    monkeypatch.setattr(portfolio_cache, 'portfolios', lambda **kwargs: calls.append(('catalogue', kwargs)) or {
        'success': True, 'snapshot_saved': True, 'portfolios': [{'name': 'Family Index'}, {'name': 'Other Index'}]})
    monkeypatch.setattr(composition_cache, 'composition', lambda name, **kwargs: calls.append((name, kwargs)) or {'success': True, 'snapshot_saved': True})
    monkeypatch.setattr(airs_refresh, 'collect_model_compositions', lambda **_: pytest.fail('No AIRS fallback'))
    D._refresh_holdings(selected, lambda: None)
    assert calls == [('catalogue', {'refresh': True}), ('Family Index', {'refresh': True})]


def test_known_leonteq_identity_refreshes_holdings_without_catalogue_login(selected, monkeypatch):
    selected.update(management_group='topselecties', name='AITopSelectie')
    monkeypatch.setattr(portfolio_cache, 'saved_portfolios', lambda: {
        'success': True, 'portfolios': [{'name': 'AI-TopSelectie Index'}]})
    monkeypatch.setattr(portfolio_cache, 'portfolios', lambda **_: pytest.fail('Redundant catalogue login'))
    calls = []
    monkeypatch.setattr(composition_cache, 'composition', lambda name, **kwargs:
        calls.append((name, kwargs)) or {'success': True, 'snapshot_saved': True})
    D._refresh_holdings(selected, lambda: None)
    assert calls == [('AI-TopSelectie Index', {'refresh': True})]


@pytest.mark.parametrize('result,reason', [
    ({'success': False, 'status': 'busy'}, 'another Leonteq operation'),
    ({'success': False, 'status': 'missing_credentials'}, 'credentials are not configured'),
    ({'success': True, 'refresh_error': 'timeout'}, 'timed out'),
    ({'success': True, 'snapshot_saved': False}, 'could not be saved'),
])
def test_leonteq_refresh_reports_underlying_failure_without_accepting_saved_holdings(selected, monkeypatch, result, reason):
    selected.update(management_group='topselecties')
    monkeypatch.setattr(D, '_leonteq_identity', lambda *_args, **_kwargs: 'AI-TopSelectie Index')
    monkeypatch.setattr(composition_cache, 'composition', lambda *_args, **_kwargs: result)
    with pytest.raises(HTTPException) as caught:
        D._refresh_holdings(selected, lambda: None)
    assert caught.value.status_code == 502
    assert reason.casefold() in caught.value.detail.casefold()


@pytest.fixture
def refresh_env(selected, monkeypatch):
    rows = [{'isin': 'A', 'name': 'Alpha', 'weight_pct': 5, 'status': 'ok', 'price': 10},
            {'isin': 'B', 'name': 'Beta', 'weight_pct': 6, 'status': 'ok', 'price': 20}]
    bundle = {'portfolio_id': 1, 'rows': rows, 'method': V.METHOD, 'evidence': {}, 'prepared_at': 'stamp', 'ready': True}
    monkeypatch.setattr(D, '_refresh_holdings', lambda *_: None)
    monkeypatch.setattr(D, 'holdings', lambda *_: (rows, {}))
    monkeypatch.setattr(D, '_saved_bundle', lambda *_: deepcopy(bundle))
    database = FakeSupabase({V.TABLE: []})
    monkeypatch.setattr(D, 'supabase', database)
    reads = []
    monkeypatch.setattr(D.W, 'read', lambda pid, **kwargs: reads.append((pid, kwargs)) or {'parameters': {}, 'rows': [], 'membership_legend': []})
    return rows, reads, database


def test_refresh_stream_retains_failures_and_returns_all_tabs_once(refresh_env, monkeypatch):
    rows, reads, database = refresh_env
    refreshed = []
    def refresh_row(pid, isin, **kwargs):
        refreshed.append(isin)
        assert kwargs['current_holdings'][0] == rows
        if isin == 'A':
            raise HTTPException(502, 'GuruFocus returned empty statements.')
        return {**kwargs['existing'], 'rows': [{**row, 'price': 30 if row['isin'] == 'B' else row['price']} for row in rows]}
    monkeypatch.setattr(V, 'refresh_row', refresh_row)
    events = []
    result = D.refresh(1, events.append, lambda: None)
    assert refreshed == ['A', 'B']
    assert len(reads) == 2
    assert reads[0][1]['only_isin'] == 'B'
    assert reads[0][1]['include_membership'] is False
    assert next(event for event in events if event['type'] == 'row')['workbook']
    assert result['rows'][0]['price'] == 10
    assert result['rows'][1]['price'] == 30
    assert result['stale'] is True
    assert 'empty statements' in result['errors']['A']
    assert events[-1]['type'] == 'complete' and events[-1]['data']['workbook'] == result['workbook']
    assert events[-1]['counts'] == {'succeeded': 1, 'failed': 1, 'total': 2}
    assert database.tables[V.TABLE][0]['payload']['errors'] == result['errors']


def test_cancellation_leaves_completed_rows_saved_and_releases_lock(refresh_env, monkeypatch):
    saved = []
    def refresh_row(pid, isin, **kwargs):
        saved.append(isin)
        return kwargs['existing']
    def check():
        if saved:
            raise JobCancelled()
    monkeypatch.setattr(V, 'refresh_row', refresh_row)
    with pytest.raises(JobCancelled):
        D.refresh(1, lambda _: None, check)
    assert saved == ['A']
    lock = V._publication_lock(1)
    assert lock.acquire(blocking=False)
    lock.release()


def test_one_http_response_streams_progress_and_final_payload(monkeypatch):
    from routers._portfolio_valuation import refresh_valuation_dashboard
    def refresh(pid, emit, check):
        assert pid == 1
        check()
        emit({'type': 'progress', 'done': 0, 'total': 1, 'message': 'Updating'})
        emit({'type': 'complete', 'data': {'workbook': {'parameters': {}, 'membership_legend': []}}, 'failures': []})
    monkeypatch.setattr(D, 'refresh', refresh)
    async def collect():
        response = await refresh_valuation_dashboard(1)
        assert response.media_type == 'text/event-stream'
        return ''.join([chunk async for chunk in response.body_iterator])
    body = asyncio.run(collect())
    assert '"type": "progress"' in body
    assert '"type": "complete"' in body


def test_daily_valuation_uses_amsterdam_eight_am_schedule():
    from scheduled_jobs import SCHEDULED_JOBS
    spec = next(job for job in SCHEDULED_JOBS if job.id == 'portfolio_valuation_daily')
    assert spec.trigger == {'hour': 8, 'minute': 0, 'timezone': 'Europe/Amsterdam'}


def test_empty_database_preparation_discovers_sources_before_publishing(monkeypatch):
    from services import airs_refresh, portfolio_membership
    from scheduler_runtime.context import LogContext
    roster, calls = [], []
    monkeypatch.setattr(V, 'portfolios', lambda: list(roster))
    def accounts(ctx, *, force):
        assert force
        calls.append('accounts')
    def models(ctx, *, force):
        assert force
        calls.append('models')
        roster.extend([{'id': 7, 'name': 'AIRS'}, {'id': 8, 'name': 'Leonteq'}])
    monkeypatch.setattr(airs_refresh, 'refresh_accounts', accounts)
    monkeypatch.setattr(airs_refresh, 'scan_models', models)
    def holdings(ctx, **kwargs):
        assert kwargs == {'source': 'all', 'all_portfolios': True}
        calls.append('holdings')
    monkeypatch.setattr(portfolio_membership, 'refresh_holdings', holdings)
    caches = []
    def publish(pid, emit, check, **kwargs):
        calls.append(pid)
        assert kwargs['refresh_holdings'] is False
        caches.append(kwargs['source_cache'])
        return {'errors': {}}
    monkeypatch.setattr(D, 'refresh', publish)
    summary = D.prepare_all(LogContext())
    assert calls == ['accounts', 'models', 'holdings', 7, 8]
    assert summary == {'saved': [7, 8], 'errors': {}, 'unavailable_portfolios': []}
    assert caches[0] is caches[1]


def test_preparation_cancellation_releases_the_global_lock(monkeypatch):
    from scheduler_runtime.context import LogContext
    monkeypatch.setattr(V, 'portfolios', lambda: [])
    with pytest.raises(JobCancelled):
        class Context(LogContext):
            def check(self):
                raise JobCancelled()
        D.prepare_all(Context())
    assert D._preparation_lock.acquire(blocking=False)
    D._preparation_lock.release()


def test_refresh_publishes_a_complete_bundle_without_prior_valuation_rows(refresh_env, monkeypatch):
    rows, _reads, database = refresh_env
    monkeypatch.setattr(V, 'supabase', database)
    monkeypatch.setattr(D, '_saved_bundle', lambda *_: {
        'portfolio_id': 1, 'rows': [{**row, 'status': 'pending'} for row in rows],
        'method': V.METHOD, 'evidence': {}, 'ready': False, 'prepared_at': None})
    fetched = []
    monkeypatch.setattr(V, 'refresh_row_sources', lambda isin, name: fetched.append(isin))
    monkeypatch.setattr(V, 'load_inputs', lambda *_: ({}, [], [], 'No historical EPS available.'))
    result = D.refresh(1, lambda _: None, lambda: None)
    assert fetched == ['A', 'B']
    assert result['ready'] is True
    assert result['stale'] is False
    assert database.tables[V.TABLE][0]['payload']['workbook'] == result['workbook']


def test_cash_only_portfolio_also_publishes_a_workbook(refresh_env, monkeypatch):
    _rows, _reads, database = refresh_env
    monkeypatch.setattr(D, 'holdings', lambda *_: ([], {}))
    monkeypatch.setattr(D, '_saved_bundle', lambda *_: {
        'portfolio_id': 1, 'rows': [], 'method': V.METHOD, 'evidence': {}, 'prepared_at': None})
    result = D.refresh(1, lambda _: None, lambda: None)
    assert result['ready'] and not result['stale']
    assert database.tables[V.TABLE][0]['payload']['workbook'] == result['workbook']
