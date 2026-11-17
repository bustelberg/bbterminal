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
    monkeypatch.setattr(portfolio_cache, 'portfolios', lambda **kwargs: calls.append(('catalogue', kwargs)) or {
        'success': True, 'snapshot_saved': True, 'portfolios': [{'name': 'Family Index'}, {'name': 'Other Index'}]})
    monkeypatch.setattr(composition_cache, 'composition', lambda name, **kwargs: calls.append((name, kwargs)) or {'success': True, 'snapshot_saved': True})
    monkeypatch.setattr(airs_refresh, 'collect_model_compositions', lambda **_: pytest.fail('No AIRS fallback'))
    D._refresh_holdings(selected, lambda: None)
    assert calls == [('catalogue', {'refresh': True}), ('Family Index', {'refresh': True})]


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
        if isin == 'A': raise HTTPException(502, 'GuruFocus returned empty statements.')
        return {**kwargs['existing'], 'rows': [{**row, 'price': 30 if row['isin'] == 'B' else row['price']} for row in rows]}
    monkeypatch.setattr(V, 'refresh_row', refresh_row)
    events = []
    result = D.refresh(1, events.append, lambda: None)
    assert refreshed == ['A', 'B']
    assert len(reads) == 1
    assert result['rows'][0]['price'] == 10
    assert result['rows'][1]['price'] == 30
    assert result['stale'] is True
    assert 'empty statements' in result['errors']['A']
    assert events[-1]['type'] == 'complete' and events[-1]['data']['workbook'] == result['workbook']
    assert database.tables[V.TABLE][0]['payload']['errors'] == result['errors']


def test_cancellation_leaves_completed_rows_saved_and_releases_lock(refresh_env, monkeypatch):
    saved = []
    def refresh_row(pid, isin, **kwargs):
        saved.append(isin)
        return kwargs['existing']
    def check():
        if saved: raise JobCancelled()
    monkeypatch.setattr(V, 'refresh_row', refresh_row)
    with pytest.raises(JobCancelled): D.refresh(1, lambda _: None, check)
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
