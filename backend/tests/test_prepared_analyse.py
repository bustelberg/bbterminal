import asyncio
from unittest.mock import Mock

import pytest

from routers import _prepared_analyse as P
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def store(monkeypatch):
    db = FakeSupabase({'analyse_source_revision': [
        {'scope': 'holdings', 'version': 10}, {'scope': 'acwi', 'version': 20}]})
    monkeypatch.setattr(P, 'supabase', db)
    monkeypatch.setattr(P, 'code_version', lambda: 'code-v1')
    return db


def test_current_snapshot_round_trips_without_sharing_mutable_rows(store):
    assert P.save('holdings', 'test', {'rows': [{'weight': 2}]}, 10, 'code-v1')
    version, result = P.read('holdings', 'test')
    assert version == 10
    result['rows'][0]['weight'] = 99
    assert P.read('holdings', 'test')[1]['rows'][0]['weight'] == 2


@pytest.mark.parametrize('field,value', [('source_version', 9), ('code_version', 'old'),
                                       ('calculation_day', '2000-01-01'), ('scope', 'acwi')])
def test_mismatched_snapshots_are_not_served(store, field, value):
    P.save('holdings', 'test', {'value': 1}, 10, 'code-v1')
    store.table('analyse_prepared_snapshot').update({field: value}).eq('cache_key', 'test').execute()
    assert P.read('holdings', 'test')[1] is None


def test_changes_during_calculation_are_not_saved(store, monkeypatch):
    assert not P.save('holdings', 'test', {}, 9, 'code-v1')
    assert not P.save('holdings', 'test', {}, 10, 'old-code')
    versions = iter([10, 11])
    P.save('holdings', 'test', {'value': 1}, 10, 'code-v1')
    monkeypatch.setattr(P, 'source_version', lambda _: next(versions))
    assert P.read('holdings', 'test')[1] is None


def test_batch_is_bounded_and_next_batch_skips_current_rows(store, monkeypatch):
    from routers import _airs_portfolio_analysis as A, _active_share as S
    calls = []
    def calculate(pid, *_args):
        calls.append(pid)
        assert not A._INCLUDE_HOLDING_RISK.get()
        return {'portfolio_id': pid, 'book_holdings': []}
    monkeypatch.setattr(A, 'compute_portfolio_analysis', calculate)
    monkeypatch.setattr(S, '_load_benchmark_weights', lambda *_: {'issuer_by_asset': {1: 'one'}})
    first = P.prepare(portfolio_ids=[1, 2, 3])
    assert first['holdings'] == 4
    assert first['benchmarks'] == 1
    assert first['batch_exhausted']
    assert calls == [1, 1, 2, 2]
    second = P.prepare(portfolio_ids=[1, 2, 3])
    assert second['holdings'] == 2
    assert second['already_current'] == 5
    assert A._INCLUDE_HOLDING_RISK.get() is True


def test_busy_sources_do_not_start_a_preparation_connection(monkeypatch):
    import psycopg
    connect = Mock(side_effect=AssertionError('must not connect'))
    monkeypatch.setattr(psycopg, 'connect', connect)
    assert P.run_controlled(busy=lambda: True)['skipped']
    connect.assert_not_called()


def test_cancelled_and_exhausted_batches_do_not_calculate(store, monkeypatch):
    from routers import _active_share as A
    calculate = Mock(side_effect=AssertionError('should not calculate'))
    monkeypatch.setattr(A, '_load_benchmark_weights', calculate)
    assert P.prepare(portfolio_ids=[1], should_stop=lambda: True)['cancelled']
    assert P.prepare(portfolio_ids=[1], max_seconds=0)['budget_exhausted']
    calculate.assert_not_called()


def test_replica_lock_excludes_a_second_worker(monkeypatch):
    import common.pg as pg
    import psycopg
    monkeypatch.setattr(pg, '_db_url', lambda: 'unit-test')
    conn = Mock()
    conn.execute.return_value.fetchone.return_value = (False,)
    connection = Mock()
    connection.__enter__ = Mock(return_value=conn)
    connection.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(psycopg, 'connect', lambda *_args, **_kwargs: connection)
    monkeypatch.setattr(P, 'prepare', Mock(side_effect=AssertionError('lock is held')))
    assert 'another preparation' in P.run_controlled(busy=lambda: False)['skipped']


def test_prepared_overview_is_small_and_detail_does_not_recompute(store, monkeypatch):
    from routers import _airs_portfolio_analysis as A
    payload = {'portfolio_id': 1, 'name': 'Book', 'benchmark': 'ACWI', 'returns': {'portfolio_ytd_pct': 3},
               'allocation': [], 'book_holdings': [{'name': 'Stock'}], 'details_pending': False}
    assert P.save('holdings', P.holdings_key(1, 'ACWI', False), payload, 10, 'code-v1')
    monkeypatch.setattr(A, 'compute_portfolio_analysis', Mock(side_effect=AssertionError('recomputed')))
    async def run():
        overview = await A.compute_portfolio_analysis_async(1, 'ACWI', 'book', 'book', include_axes=False, overview_only=True)
        assert overview['returns']['portfolio_ytd_pct'] == 3
        assert overview['book_holdings'] == []
        assert overview['details_pending']
        detail = await A.compute_portfolio_analysis_async(1, 'ACWI', 'book', 'book', include_axes=False, include_holding_risk=False)
        assert detail['book_holdings'] == [{'name': 'Stock'}]
    asyncio.run(run())


def test_benchmark_restores_numeric_asset_identity_keys(store, monkeypatch):
    from routers import _active_share as A
    P.save('acwi', 'acwi:ACWI', {'issuer_by_asset': {'42': 'issuer'}, 'bench': {}, 'coverage': {}}, 20, 'code-v1')
    monkeypatch.setattr(A, '_load_benchmark_weights', Mock(side_effect=AssertionError('recomputed')))
    assert A._benchmark_weights('ACWI', None)['issuer_by_asset'] == {42: 'issuer'}


def test_opening_serves_complete_prepared_holdings_without_another_detail_request(store, monkeypatch):
    from routers import _airs_portfolio_analysis as A, _analysis_cache as cache
    payload = {'portfolio_id': 1, 'benchmark': 'ACWI', 'returns': {'portfolio_ytd_pct': 3},
               'book_holdings': [{'name': 'Stock', 'weight_now_pct': 70}],
               'realised': {'total': 2}, 'details_pending': False}
    assert P.save('holdings', P.holdings_key(1, 'ACWI', False), payload, 10, 'code-v1')
    monkeypatch.setattr(A, 'compute_portfolio_analysis', Mock(side_effect=AssertionError('recomputed')))
    monkeypatch.setattr(cache, 'fingerprint', Mock(side_effect=AssertionError('unnecessary fingerprint')))
    result = asyncio.run(A.compute_portfolio_analysis_async(
        1, 'ACWI', 'book', 'book', include_axes=False,
        overview_only=True, include_holding_risk=False))
    assert result['book_holdings'] == payload['book_holdings']
    assert result['realised'] == payload['realised']
    assert result['details_pending'] is False
    assert 'prepared_lookup' in result['timings_ms']


def test_process_code_is_frozen_but_live_config_changes_invalidate(monkeypatch, tmp_path):
    code = tmp_path / 'calculation.py'
    config = tmp_path / 'aliases.json'
    code.write_text('old code')
    config.write_text('{"old":true}')
    monkeypatch.setattr(P, '_FILES', (code, config))
    before = P.code_version()
    code.write_text('newly deployed code for the next process')
    assert P.code_version() == before
    config.write_text('{"changed_config":true}')
    assert P.code_version() != before
