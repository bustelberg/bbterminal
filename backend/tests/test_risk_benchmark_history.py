"""Every fixed-window risk entry point repairs a short benchmark before rejecting it."""
from datetime import date, timedelta
from functools import lru_cache

import pytest

from asset_pipeline import price_refresh
from routers import _risk_benchmark_history as H
from routers import _tracking_error as T
from routers._asset_financials import _BENCHMARK_RISK_ETF
from routers._portfolio_correlation_risk import compute_risk_correlation
from routers._portfolio_volatility import compute_volatility
from routers._portfolio_drawdown import compute_drawdown


def prices():
    rows, day = [], date(2020, 12, 30)
    while day < date.today():
        if day.weekday() < 5:
            rows.append((day.isoformat(), 100 + len(rows) * .1))
        day += timedelta(days=1)
    return rows


@pytest.mark.parametrize('compute,frequency', [
    (compute_risk_correlation, 'weekly'), (compute_volatility, 'monthly'),
    (compute_drawdown, 'daily'),
])
def test_short_benchmark_is_fetched_and_calculated_without_a_manual_refresh(monkeypatch, compute, frequency):
    monkeypatch.setattr(H, '_attempts', {})
    benchmark = _BENCHMARK_RISK_ETF['ACWI']
    full = prices()
    stored = {'STOCK': full, benchmark: [row for row in full if row[0] >= '2023-07-27']}
    calls = []

    @lru_cache(maxsize=4)
    def load(*_args):
        return dict(stored)

    def repair(isins, **kwargs):
        calls.append((isins, kwargs))
        stored[benchmark] = full

    monkeypatch.setattr(T, '_risk_prices', load)
    monkeypatch.setattr(price_refresh, 'repair_price_histories', repair)
    result = compute([{'isin': 'STOCK', 'name': 'Stock', 'weight_pct': 100, 'bucket': 'Equity'}],
                     'ACWI', frequency=frequency, start_date='2021-01-01', require_complete_history=True)
    assert result['available'], result
    assert result['window_from'] == '2021-01-01'
    assert len(calls) == 1
    assert calls[0][0] == {benchmark}
    assert calls[0][1]['full_history'] and calls[0][1]['yahoo_only']


def test_failed_vendor_fetch_has_a_bounded_retry_cooldown_and_rereads_the_store(monkeypatch):
    monkeypatch.setattr(H, '_attempts', {})
    clock, calls, reads = [100], [], []
    monkeypatch.setattr(H.time, 'monotonic', lambda: clock[0])
    def repair(*_args, **_kwargs):
        calls.append(1)
        raise RuntimeError('Yahoo temporarily unavailable')
    monkeypatch.setattr(price_refresh, 'repair_price_histories', repair)
    def reload():
        reads.append(1)
        return {}
    for _ in range(2):
        assert H.ensure_benchmark_history({}, {'ACWI'}, '2021-01-01', 7, reload) == {}
    assert len(calls) == 1 and len(reads) == 2
    clock[0] += 61
    H.ensure_benchmark_history({}, {'ACWI'}, '2021-01-01', 7, reload)
    assert len(calls) == 2


def test_cagr_bootstraps_an_absent_benchmark_mapping_before_returning_no_data(monkeypatch):
    from routers import _annual_portfolio_simulation as S
    monkeypatch.setattr(H, '_attempts', {})
    benchmark = _BENCHMARK_RISK_ETF['ACWI']
    mappings = {'STOCK': {'name': 'Stock', 'asset_class': 'equity', 'analysis_id': 1, 'yahoo_symbol': 'STOCK'}}
    calls = []
    def repair(isins, **kwargs):
        calls.append(isins)
        mappings[benchmark] = {'analysis_id': 2, 'yahoo_symbol': 'ISAC.L'}
    monkeypatch.setattr(price_refresh, 'repair_price_histories', repair)
    monkeypatch.setattr(S, '_yahoo_mappings', lambda _isins: dict(mappings))
    monkeypatch.setattr(S, '_risk_prices', lambda *_args: {'STOCK': prices(), benchmark: prices()})
    result = S.compute_simulation([{'isin': 'STOCK', 'name': 'Stock', 'weight_pct': 100}],
                                  'ACWI', start_date='2021-01-01')
    assert result['available'], result
    assert result['historical_comparison']['portfolio_cagr_pct'] is not None
    assert result['benchmark_history']['cagr_pct'] is not None
    assert calls == [{benchmark}]


def test_complete_benchmark_does_not_fetch_or_reload(monkeypatch):
    monkeypatch.setattr(price_refresh, 'repair_price_histories', lambda *_a, **_k: pytest.fail('unexpected fetch'))
    series = {'ACWI': prices()}
    assert H.ensure_benchmark_history(series, {'ACWI'}, '2021-01-01', 7,
                                      lambda: pytest.fail('unexpected reload')) is series
