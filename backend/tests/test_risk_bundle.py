import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from routers import _risk_bundle as B, _risk_read as R, _prepared_analyse as P
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def store(monkeypatch):
    db = FakeSupabase({'analyse_source_revision': [{'scope': 'holdings', 'version': 1}]})
    monkeypatch.setattr(P, 'supabase', db)
    monkeypatch.setattr(P, 'code_version', lambda: 'bundle-test')
    return db


def test_prepared_bundle_is_atomic_read_only_and_independent_of_holding_order(store, monkeypatch):
    calculate = Mock(side_effect=lambda fn, *a, **kw: {'available': True, 'measure': fn.__name__})
    monkeypatch.setattr(R, 'read', calculate)
    holdings = [{'isin': 'A', 'name': 'One', 'weight_pct': 20}, {'isin': 'B', 'name': 'Two', 'weight_pct': 30}]
    assert B.read(holdings, None) is None
    B.prepare(holdings, None)
    assert calculate.call_count == 6
    calculate.side_effect = AssertionError('interactive reads must not calculate')
    result = B.read(list(reversed(holdings)), None)
    assert set(result['views']) == {'active-share', 'concentration', 'tracking-error', 'risk-correlation', 'volatility', 'drawdown'}
    assert B.read([{**holdings[0], 'weight_pct': 21}, holdings[1]], None) is None
    assert B.read(holdings, {'Stocks': 60, 'Bonds': 40}) is None
    assert calculate.call_count == 6


def test_source_change_restarts_automatically_and_publishes_only_a_complete_generation(store, monkeypatch):
    monkeypatch.setattr(B, '_settle', lambda ctx: None)
    calls = []
    def calculate(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            store.tables['analyse_source_revision'][0]['version'] = 2
            return {'available': True, 'generation': 1}
        return {'available': True, 'generation': 2}
    monkeypatch.setattr(R, 'read', calculate)
    B.prepare([], None)
    assert len(calls) == 7  # Stop the invalidated pass immediately, then prepare six fresh views.
    assert {view['generation'] for view in B.read([], None)['views'].values()} == {2}


def test_continuous_updates_are_bounded_and_never_publish_mixed_data(store, monkeypatch):
    from jobs import UserFacingJobError
    monkeypatch.setattr(B, '_settle', lambda ctx: None)
    def calculate(*args, **kwargs):
        store.tables['analyse_source_revision'][0]['version'] += 1
        return {'available': True}
    monkeypatch.setattr(R, 'read', calculate)
    with pytest.raises(UserFacingJobError, match='still updating'):
        B.prepare([], None)
    assert B.read([], None) is None


def test_revision_change_at_publication_retries_instead_of_failing(store, monkeypatch):
    monkeypatch.setattr(B, '_settle', lambda ctx: None)
    monkeypatch.setattr(R, 'read', lambda *a, **kw: {'available': True})
    save = P.save
    attempts = []
    def racing_save(*args):
        attempts.append(1)
        if len(attempts) == 1:
            store.tables['analyse_source_revision'][0]['version'] = 2
            return False
        return save(*args)
    monkeypatch.setattr(P, 'save', racing_save)
    B.prepare([], None)
    assert len(attempts) == 2
    assert len(B.read([], None)['views']) == 6


def test_monthly_volatility_keeps_the_existing_daily_preliminary_fallback(store, monkeypatch):
    def calculate(fn, *args, **kwargs):
        return {'available': args[2] == 'daily', 'preliminary': kwargs.get('preliminary', False)} if fn.__name__ == 'compute_volatility' else {'available': True}
    monkeypatch.setattr(R, 'read', calculate)
    assert B.prepare([], None)['volatility'] == {'available': True, 'preliminary': True}


def test_other_accounts_airs_updates_do_not_invalidate_market_risk(store, monkeypatch):
    store.tables['analyse_source_revision'].append({'scope': 'risk', 'version': 10})
    monkeypatch.setattr(R, 'read', lambda *a, **kw: {'available': True})
    B.prepare([], None)
    store.tables['analyse_source_revision'][0]['version'] += 1
    assert B.read([], None) is not None
    store.tables['analyse_source_revision'][1]['version'] += 1
    assert B.read([], None) is None


def test_cold_http_read_enqueues_work_and_returns_without_calculating(monkeypatch):
    import jobs
    from routers.airs import RiskBundleRequest, airs_portfolio_risk_bundle
    monkeypatch.setattr(B, 'read', lambda *args: None)
    prepare = Mock(side_effect=AssertionError('calculation on HTTP path'))
    monkeypatch.setattr(B, 'prepare', prepare)
    start = Mock(return_value=(SimpleNamespace(id='job-id'), False))
    monkeypatch.setattr(jobs, 'start', start)
    response = asyncio.run(airs_portfolio_risk_bundle(RiskBundleRequest(holdings=[])))
    assert response.status_code == 202
    start.assert_called_once()
    prepare.assert_not_called()


def test_warm_http_read_returns_all_views_without_starting_a_job(monkeypatch):
    import jobs
    from routers.airs import RiskBundleRequest, airs_portfolio_risk_bundle
    monkeypatch.setattr(B, 'read', lambda *args: {'views': {'volatility': {'available': True}}})
    start = Mock(side_effect=AssertionError('warm reads must not start work'))
    monkeypatch.setattr(jobs, 'start', start)
    result = asyncio.run(airs_portfolio_risk_bundle(RiskBundleRequest(holdings=[])))
    assert result['ready'] and result['views']['volatility']['available']
    start.assert_not_called()


def test_read_only_poll_does_not_start_an_unwatched_job(monkeypatch):
    import jobs
    from routers.airs import RiskBundleRequest, airs_portfolio_risk_bundle
    monkeypatch.setattr(B, 'read', lambda *args: None)
    start = Mock(side_effect=AssertionError('poll must not start work'))
    monkeypatch.setattr(jobs, 'start', start)
    result = asyncio.run(airs_portfolio_risk_bundle(RiskBundleRequest(holdings=[]), prepare_if_missing=False))
    assert result.status_code == 202
    start.assert_not_called()


def test_waiting_for_quiet_sources_remains_cancellable(store):
    from jobs import JobCancelled
    ctx = SimpleNamespace(progress=Mock(), check=Mock(side_effect=JobCancelled('stopped')))
    with pytest.raises(JobCancelled):
        B._settle(ctx)
