import pytest


def test_opening_response_bundles_curve_with_overview(monkeypatch):
    from routers import airs, _airs_portfolio_analysis as analysis, _airs_value_series as curves

    calls = []
    overview = {'portfolio_id': 1, 'book_portefeuille': 'BUS', 'details_pending': True,
                'book_holdings': [], 'returns': {'portfolio_ytd_pct': 4.86, 'benchmark_ytd_pct': 9.27,
                                                 'block_returns': [{'bucket': 'Stocks', 'benchmark_return_pct': 10}]},
                'risk_profile_policy': {'Equity': 70}}

    async def compute(*args, **kwargs):
        calls.append(kwargs)
        return overview

    def series(account, benchmark, weights, *, returns_only):
        assert (account, benchmark, weights, returns_only) == ('BUS', 'ACWI', {'Equity': 70}, True)
        return {'returns': [{'date': '2026-11-02', 'cum_pct': 4.86}],
                'benchmark_source': 'weighted_etf', 'benchmark_return_pct': 9.25,
                'benchmark_as_of': '2026-11-02',
                'benchmark_components': [{'name': 'Stocks', 'return_pct': 9.5}]}

    async def to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(analysis, 'compute_portfolio_analysis_async', compute)
    monkeypatch.setattr(curves, 'value_series', series)
    monkeypatch.setattr(airs.asyncio, 'to_thread', to_thread)
    request = airs.airs_model_portfolio_analysis(
        1, benchmark='ACWI', source='book', overview_only=True, include_holding_risk=False)
    with pytest.raises(StopIteration) as complete:
        request.send(None)
    result = complete.value.value
    assert result['returns']['benchmark_ytd_pct'] == 9.25
    assert result['returns']['ytd_excess_pct'] == pytest.approx(-4.39)
    assert result['returns']['block_returns'][0]['benchmark_return_pct'] == 9.5
    assert 'benchmark_return_pct' not in result['return_series']
    assert overview['returns']['benchmark_ytd_pct'] == 9.27
    assert result['return_series']['returns'][0]['cum_pct'] == 4.86
    assert result['details_pending'] and result['book_holdings'] == []
    assert calls == [{'overview_only': True, 'include_holding_risk': False}]
    assert 'return_series' not in overview
