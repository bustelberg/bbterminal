import asyncio


def test_opening_response_bundles_curve_with_overview(monkeypatch):
    from routers import airs, _airs_portfolio_analysis as analysis, _airs_value_series as curves

    calls = []
    overview = {'portfolio_id': 1, 'book_portefeuille': 'BUS', 'details_pending': True,
                'book_holdings': [], 'returns': {'portfolio_ytd_pct': 4.86},
                'risk_profile_policy': {'Equity': 70}}

    async def compute(*args, **kwargs):
        calls.append(kwargs)
        return overview

    def series(account, benchmark, weights, *, returns_only):
        assert (account, benchmark, weights, returns_only) == ('BUS', 'ACWI', {'Equity': 70}, True)
        return {'returns': [{'date': '2026-11-02', 'cum_pct': 4.86}]}

    monkeypatch.setattr(analysis, 'compute_portfolio_analysis_async', compute)
    monkeypatch.setattr(curves, 'value_series', series)
    result = asyncio.run(airs.airs_model_portfolio_analysis(
        1, benchmark='ACWI', source='book', overview_only=True, include_holding_risk=False))
    assert result['returns'] == overview['returns']
    assert result['return_series']['returns'][0]['cum_pct'] == 4.86
    assert result['details_pending'] and result['book_holdings'] == []
    assert calls == [{'overview_only': True, 'include_holding_risk': False}]
    assert 'return_series' not in overview
