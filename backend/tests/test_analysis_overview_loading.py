import asyncio
import threading

import pytest

from routers import _airs_portfolio_analysis as analysis, _analysis_cache as cache


def test_identical_requests_share_work_and_a_cancelled_waiter_does_not_cancel_others(monkeypatch):
    monkeypatch.setattr(cache, 'fingerprint', lambda: 'test-overview-sharing')
    monkeypatch.setattr(cache, 'get', lambda *_: None)
    monkeypatch.setattr(cache, 'put', lambda *_: None)
    started, release = threading.Event(), threading.Event()
    calls = []

    def compute(*_args):
        calls.append(analysis._OVERVIEW_ONLY.get())
        started.set()
        assert release.wait(5)
        return {'returns': {'portfolio_ytd_pct': 3.75}, 'timings_ms': {'work': 10}}

    monkeypatch.setattr(analysis, 'compute_portfolio_analysis', compute)

    async def run():
        first = asyncio.create_task(analysis.compute_portfolio_analysis_async(1, overview_only=True))
        assert await asyncio.to_thread(started.wait, 5)
        second = asyncio.create_task(analysis.compute_portfolio_analysis_async(1, overview_only=True))
        await asyncio.sleep(.02)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        release.set()
        result = await second
        assert result['details_pending'] is True
        assert result['returns']['portfolio_ytd_pct'] == 3.75
        await asyncio.sleep(0)

    asyncio.run(run())
    assert calls == [True]
    assert not analysis._ANALYSIS_PENDING
    assert analysis._OVERVIEW_ONLY.get() is False


def test_cached_response_reports_current_lookup_time_not_historic_compute_time(monkeypatch):
    monkeypatch.setattr(cache, 'fingerprint', lambda: 'cached')
    original = {'timings_ms': {'old_work': 5000}, 'returns': {'portfolio_ytd_pct': 3.75}}
    monkeypatch.setattr(cache, 'get', lambda *_: original)
    result = asyncio.run(analysis.compute_portfolio_analysis_async(1))
    assert 'old_work' not in result['timings_ms']
    assert 'cache_lookup' in result['timings_ms']
    assert original['timings_ms'] == {'old_work': 5000}


def test_failed_inflight_request_is_removed_and_can_be_retried(monkeypatch):
    monkeypatch.setattr(cache, 'fingerprint', lambda: 'retry')
    monkeypatch.setattr(cache, 'get', lambda *_: None)
    monkeypatch.setattr(cache, 'put', lambda *_: None)
    calls = []
    def compute(*_):
        calls.append(1)
        if len(calls) == 1:
            raise ValueError('temporary failure')
        return {'returns': {}}
    monkeypatch.setattr(analysis, 'compute_portfolio_analysis', compute)
    async def run():
        with pytest.raises(ValueError, match='temporary failure'):
            await analysis.compute_portfolio_analysis_async(2)
        await asyncio.sleep(0)
        assert not analysis._ANALYSIS_PENDING
        assert not (await analysis.compute_portfolio_analysis_async(2))['details_pending']
    asyncio.run(run())
    assert len(calls) == 2


def test_lightweight_and_full_requests_have_separate_scope_and_cache_keys(monkeypatch):
    monkeypatch.setattr(cache, 'fingerprint', lambda: 'scope')
    monkeypatch.setattr(cache, 'get', lambda *_: None)
    keys, modes = [], []
    monkeypatch.setattr(cache, 'put', lambda key, *_: keys.append(key))
    def compute(*_):
        modes.append(analysis._OVERVIEW_ONLY.get())
        return {'returns': {}}
    monkeypatch.setattr(analysis, 'compute_portfolio_analysis', compute)
    async def run():
        await asyncio.gather(analysis.compute_portfolio_analysis_async(3, overview_only=True),
                             analysis.compute_portfolio_analysis_async(3, overview_only=False))
    asyncio.run(run())
    assert set(modes) == {True, False}
    assert len(set(keys)) == 2


def test_overview_skips_risk_history_without_filling_cache_with_missing_risk():
    token = analysis._OVERVIEW_ONLY.set(True)
    try:
        assert analysis._holding_risk(['TEST'], 'ACWI') == {}
    finally:
        analysis._OVERVIEW_ONLY.reset(token)


@pytest.mark.parametrize('overview_only', [True, False])
def test_book_overview_does_not_calculate_yahoo_strategy(monkeypatch, overview_only):
    import routers._airs_portfolio_perf as perf
    monkeypatch.setattr(perf, 'compute_portfolio_performance', lambda **_: pytest.fail('strategy load on overview'))
    monkeypatch.setattr(analysis, '_book_return', lambda *_: {'book_portefeuille': 'BOOK', 'book_ytd_pct': 3.75})
    monkeypatch.setattr(analysis, '_apply_book_source', lambda value, _: value.update(portfolio_ytd_pct=3.75))
    token = analysis._OVERVIEW_ONLY.set(overview_only)
    try:
        result = analysis._returns(1, '2025-12-30', 'ACWI', 'book')
    finally:
        analysis._OVERVIEW_ONLY.reset(token)
    assert result['portfolio_ytd_pct'] == 3.75
    assert result['strategy_ytd_pct'] is None


def test_optional_risk_history_is_skipped_without_caching_empty_results(monkeypatch):
    monkeypatch.setattr(analysis, '_leg', lambda *_: pytest.fail('risk cache accessed for disabled columns'))
    token = analysis._INCLUDE_HOLDING_RISK.set(False)
    try:
        assert analysis._holding_risk(['TEST'], 'ACWI') == {}
    finally:
        analysis._INCLUDE_HOLDING_RISK.reset(token)
    assert analysis._INCLUDE_HOLDING_RISK.get() is True


def test_first_page_never_loads_child_ledgers_prices_or_risk(monkeypatch):
    from routers import _airs_account_links as links, _airs_holding_isin as resolver
    monkeypatch.setattr(analysis, 'ref_model', lambda _: {'name': 'Test', 'positions_datum': '2025-12-31'})
    monkeypatch.setattr(links, 'list_account_links', lambda: {'accounts': [
        {'model_portfolio_id': 1, 'portefeuille': 'BOOK'}]})
    calls = []
    def resolve(book, **kwargs):
        calls.append((book, kwargs))
        return {'as_of': '2026-10-23', 'rows': [
            {'isin': 'TEST', 'bucket': 'Equity', 'current_value_eur': 80},
            {'bucket': 'Cash', 'current_value_eur': 20}]}
    monkeypatch.setattr(resolver, 'resolve_account_isins', resolve)
    monkeypatch.setattr(analysis, 'ref_positions_for', lambda _: [])
    monkeypatch.setattr(analysis, '_expand_book_rows', lambda rows: rows)
    monkeypatch.setattr(analysis, '_book_fetched_at', lambda _: None)
    monkeypatch.setattr(analysis, '_variant_bands', lambda *_: {})
    monkeypatch.setattr(analysis, '_returns', lambda *_: {'portfolio_ytd_pct': 3.75, 'ytd_from': '2026-01-01'})
    monkeypatch.setattr(analysis, '_scorecard_block_returns', lambda *_: {
        'block_returns': [], 'benchmark_ytd_pct': 9.61})
    for name in ('_book_port_items', '_with_results', '_realised_block', '_holding_risk', '_grid'):
        monkeypatch.setattr(analysis, name, lambda *_: pytest.fail('detail loader on opening path'))
    token = analysis._OVERVIEW_ONLY.set(True)
    try:
        result = analysis.compute_portfolio_analysis(1, 'ACWI', 'book', 'book', include_axes=False)
    finally:
        analysis._OVERVIEW_ONLY.reset(token)
    assert calls == [('BOOK', {'freshen': False, 'price_checks': False})]
    assert result['book_holdings'] == []
    assert result['realised'] is None
    assert result['returns']['benchmark_ytd_pct'] == 9.61
    assert result['returns']['ytd_excess_pct'] == pytest.approx(-5.86)
    assert next(r['pct'] for r in result['allocation'] if r['bucket'] == 'Equity') == 80


def test_stocks_charts_skip_returns_ledgers_and_holding_enrichment(monkeypatch):
    monkeypatch.setattr(analysis, 'ref_model', lambda _: {'name': 'Test', 'positions_datum': '2025-12-31'})
    monkeypatch.setattr(analysis, 'ref_positions_for', lambda _: [])
    monkeypatch.setattr(analysis, '_country_by_code', lambda: {})
    monkeypatch.setattr(analysis, '_grid', lambda _: {})
    monkeypatch.setattr(analysis, '_bench_members', lambda _: ([], {}))
    from routers import _airs_holding_isin as resolver
    monkeypatch.setattr(resolver, '_load_bucket_overrides', lambda _: {})
    items = [(45, ('Technology', 'United States', 'USD'))]
    labels = [{'isin': 'TEST', 'name': 'Test stock', 'asset_class': 'Equity'}]
    monkeypatch.setattr(analysis, '_book_port_items', lambda *_: {
        'items': items, 'direct_items': items, 'labels': labels, 'direct_labels': labels,
        'alloc_items': [(45, 'Equity')], 'direct_alloc_items': [(45, 'Equity')],
        'classified_w': 45, 'total_w': 100, 'current_book_total': 100,
        'foreign': 0, 'holdings': 1, 'holdings_detail': [], 'portefeuille': 'BOOK',
    })
    for name in ('_with_results', '_realised_block', '_returns_timed', '_with_sold_risk'):
        monkeypatch.setattr(analysis, name, lambda *_: pytest.fail('unrelated detail work in chart request'))
    token = analysis._OVERVIEW_ONLY.set(True)
    try:
        result = analysis.compute_portfolio_analysis(1, 'ACWI', 'book', 'book', 'Equity', False, True)
    finally:
        analysis._OVERVIEW_ONLY.reset(token)
    assert len(result['axes']) == 3
    assert result['axes'][0]['rows'][0]['portfolio_pct'] == 45
    assert 'returns' not in result


def test_resolver_is_reused_per_request_without_sharing_mutable_rows(monkeypatch):
    from common.read_cache import read_cache
    from routers import _airs_holding_isin as resolver
    calls = []
    def resolve(book, **kwargs):
        calls.append((book, kwargs))
        return {'rows': [{'isin': 'TEST'}]}
    monkeypatch.setattr(resolver, '_resolve_account_isins', resolve)
    with read_cache('resolver-test'):
        first = resolver.resolve_account_isins('BOOK', freshen=False, price_checks=False)
        first['rows'][0]['isin'] = 'MUTATED'
        second = resolver.resolve_account_isins('BOOK', freshen=False, price_checks=False)
        assert second['rows'][0]['isin'] == 'TEST'
        resolver.resolve_account_isins('BOOK', freshen=False)
    assert len(calls) == 2  # price-check mode has its own answer
    with read_cache('new-request'):
        resolver.resolve_account_isins('BOOK', freshen=False, price_checks=False)
    assert len(calls) == 3
