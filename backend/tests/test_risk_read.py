from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import Mock

import pytest

from routers import _risk_read as R, _prepared_analyse as P, _analysis_cache as C
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def cache(monkeypatch):
    db = FakeSupabase({'analyse_source_revision': [{'scope': 'holdings', 'version': 1}]})
    monkeypatch.setattr(P, 'supabase', db)
    monkeypatch.setattr(P, 'code_version', lambda: 'risk-test')
    monkeypatch.setattr(C, 'leg', lambda _key, calculate: calculate())
    return db


def test_persisted_results_reuse_across_workers_and_invalidate_on_writes(cache):
    calls = []
    def compute(holdings, benchmark='ACWI'):
        calls.append(1)
        assert not R.ALLOW_REPAIR.get()
        return {'available': True, 'rows': [{'weight': holdings[0]['weight_pct']}]}
    first = R.read(compute, [{'weight_pct': 40.0}])
    first['rows'][0]['weight'] = -1
    assert R.read(compute, [{'weight_pct': 40.0}], benchmark='ACWI')['rows'][0]['weight'] == 40
    assert len(calls) == 1
    cache.table('analyse_source_revision').update({'version': 2}).eq('scope', 'holdings').execute()
    R.read(compute, [{'weight_pct': 40.0}])
    assert len(calls) == 2
    assert R.ALLOW_REPAIR.get()


def test_unavailable_result_is_replaced_when_inputs_change(cache):
    answer = {'available': False, 'reason': 'Missing history'}
    def compute():
        return dict(answer)
    assert not R.read(compute)['available']
    answer['available'] = True
    cache.table('analyse_source_revision').update({'version': 2}).eq('scope', 'holdings').execute()
    assert R.read(compute)['available']


def test_cache_identity_distinguishes_funds_weights_and_benchmark(cache):
    calls = []
    def compute(holdings, benchmark='ACWI'):
        calls.append(1)
        return {'available': True}
    row = {'isin': 'A', 'name': 'One', 'weight_pct': 40}
    R.read(compute, [row])
    R.read(compute, [{**row, 'value_eur': 100}])
    assert len(calls) == 1
    R.read(compute, [{**row, 'is_fund': True}])
    R.read(compute, [{**row, 'weight_pct': 50}])
    R.read(compute, [row], 'SP500')
    assert len(calls) == 4


def test_prepared_integer_policy_matches_api_float_policy(cache):
    calls = []
    def compute(benchmark_weights):
        calls.append(1)
        return {'available': True}
    R.read(compute, {'Stocks': 100, 'Bonds': 0})
    R.read(compute, {'Stocks': 100.0, 'Bonds': 0.0})
    assert len(calls) == 1


def test_concurrent_cold_reads_compute_once(cache):
    entered, release = Event(), Event()
    calls = []
    def compute():
        calls.append(1)
        entered.set()
        assert release.wait(5)
        return {'available': True}
    with ThreadPoolExecutor(2) as pool:
        first = pool.submit(R.read, compute)
        assert entered.wait(5)
        second = pool.submit(R.read, compute)
        release.set()
        assert first.result() == second.result()
    assert len(calls) == 1


def test_interactive_benchmark_read_never_downloads(monkeypatch):
    from routers._risk_benchmark_history import ensure_benchmark_history
    from asset_pipeline import price_refresh
    repair = Mock(side_effect=AssertionError('vendor call during a read'))
    monkeypatch.setattr(price_refresh, 'repair_price_histories', repair)
    token = R.ALLOW_REPAIR.set(False)
    try:
        assert ensure_benchmark_history({}, {'missing'}, '2021-01-01', 6, Mock()) == {}
    finally:
        R.ALLOW_REPAIR.reset(token)
    repair.assert_not_called()


def test_shared_history_reuses_work_and_invalidates_on_source_changes(monkeypatch):
    calls, revision = [], ['shared-series-test-1']
    monkeypatch.setattr(C, 'fingerprint', lambda: revision[0])
    @R.shared_series
    def build(holdings, frequency='monthly', preloaded=None):
        calls.append(1)
        return {'portfolio': [len(calls)]}
    token = R.ALLOW_REPAIR.set(False)
    try:
        assert build([{'weight_pct': 40}]) == build([{'weight_pct': 40}], frequency='monthly')
        assert len(calls) == 1
        revision[0] = 'shared-series-test-2'
        build([{'weight_pct': 40}])
        assert len(calls) == 2
    finally:
        R.ALLOW_REPAIR.reset(token)


def test_preparation_preserves_whole_book_weights_and_normalises_only_correlation(monkeypatch):
    calls = []
    monkeypatch.setattr(R, 'read', lambda fn, *args, **kwargs: calls.append((fn.__name__, args, kwargs)))
    payload = {'book_holdings': [
        {'isin': 'DIRECT', 'name': 'Microsoft', 'bucket': 'Equity', 'current_value_eur': 30},
        {'isin': 'MIXED', 'name': 'Visa', 'bucket': 'Equity', 'current_value_eur': 40,
         'sources': [{'label': None, 'start_value_eur': 10, 'value_eur': 10},
                     {'label': 'Certificate', 'start_value_eur': 30, 'value_eur': 30}]},
        {'name': 'Cash', 'bucket': 'Cash', 'current_value_eur': 30}],
        'risk_profile_policy': {'Stocks': 60, 'Bonds': 40}}
    assert R.prepare_views(payload, False) == 6
    active = calls[0][1][0]
    assert [row['weight_pct'] for row in active] == [30, 10]
    correlation = next(args[0] for name, args, _ in calls if name == 'compute_risk_correlation')
    assert [row['weight_pct'] for row in correlation] == [75, 25]
    assert calls[-1][2]['start_date'] == '2021-01-01'


def test_preparation_moves_past_already_prepared_risk_books(cache, monkeypatch):
    from routers import _airs_portfolio_analysis as A
    calls = []
    monkeypatch.setattr(A, 'compute_portfolio_analysis', lambda pid, *_args: {'portfolio_id': pid, 'book_holdings': []})
    monkeypatch.setattr(R, 'prepare_views', lambda payload, look: calls.append((payload['portfolio_id'], look)) or 6)
    first = P.prepare(portfolio_ids=[1, 2, 3], benchmark=False, risk=True, max_seconds=120)
    assert first['risk_views'] == 24
    assert first['batch_exhausted']
    second = P.prepare(portfolio_ids=[1, 2, 3], benchmark=False, risk=True, max_seconds=120)
    assert second['risk_views'] == 12
    assert calls == [(1, False), (1, True), (2, False), (2, True), (3, False), (3, True)]
