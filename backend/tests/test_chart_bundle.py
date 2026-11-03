import asyncio
import math

import pytest
from fastapi import HTTPException

from routers import earnings, _benchmark_snapshots as snapshots, _chart_statistics as stats

CODE = 'annuals__Income Statement__Revenue'


def rows(values):
    return [{'metric_code': CODE, 'target_date': f'{year}-12-31', 'numeric_value': value}
            for year, value in values]


def test_bundle_waits_for_both_lines_and_returns_statistics_on_one_shared_window(monkeypatch):
    own = rows([(2017, 100), (2019, 144), (2025, 500)])
    bench = rows([(2019, 100), (2025, 200)])
    saved_stats = stats.precalculate(bench, 'annual')
    monkeypatch.setattr(snapshots, 'read_comparison', lambda *a: {'built_at': 'revision-one',
        'growth': {'metrics': bench, 'chart_statistics': saved_stats}})
    async def exercise():
        entered, release = asyncio.Event(), asyncio.Event()
        async def own_read(*a):
            entered.set()
            await release.wait()
            return {'metrics': own}
        monkeypatch.setattr(earnings, 'get_earnings_metrics_by_isin', own_read)
        task = asyncio.create_task(earnings.chart_bundle(earnings.ChartBundleRequest(
            isin='US0000000001', benchmark={'universe': 'ACWI'})))
        await entered.wait()
        assert not task.done()
        release.set()
        return await task
    result = asyncio.run(exercise())
    assert result['ready'] is True
    assert result['benchmark_built_at'] == 'revision-one'
    a, b = result['own']['chart_statistics']['revenue'], result['benchmark']['chart_statistics']['revenue']
    assert (a['fromX'], a['toX']) == (b['fromX'], b['toX']) == (2019, 2025)
    assert a['fit']['n'] == b['fit']['n'] == 2
    assert math.isclose(a['cagr']['pct'], 100 * ((500 / 144) ** (1 / 6) - 1))
    assert b == saved_stats['revenue']['windows']['2019:2025']


def test_missing_benchmark_returns_no_partial_company_payload(monkeypatch):
    async def own(*a):
        return {'metrics': rows([(2025, 100)])}
    monkeypatch.setattr(earnings, 'get_earnings_metrics_by_isin', own)
    monkeypatch.setattr(snapshots, 'read_comparison', lambda *a: None)
    with pytest.raises(HTTPException) as error:
        asyncio.run(earnings.chart_bundle(earnings.ChartBundleRequest(isin='X', benchmark={'universe': 'ACWI'})))
    assert error.value.status_code == 503


def test_ltm_and_forecasts_are_not_in_reported_statistics():
    data = rows([(2024, 100), (2025, 121)])
    data += [{'metric_code': 'ltm__Income Statement__Revenue', 'target_date': '2026-06-30', 'numeric_value': 1000},
             {'metric_code': 'annual_eps_nri_estimate', 'target_date': '2030-12-31', 'numeric_value': 10000}]
    result = stats.precalculate(data, 'annual')['revenue']['full']
    assert result['fit']['n'] == 2
    assert math.isclose(result['cagr']['pct'], 21)


def test_missing_saved_window_does_not_regress_the_benchmark_in_the_chart_request():
    with pytest.raises(ValueError, match='statistics are missing'):
        stats.paired_statistics(rows([(2024, 100), (2025, 121)]), rows([(2024, 100), (2025, 121)]), 'annual', {})


def test_quarterly_company_without_a_benchmark_keeps_its_requested_cadence(monkeypatch):
    async def own(isin, cadence, keys):
        assert cadence == 'quarterly'
        return {'metrics': [{'metric_code': CODE, 'target_date': '2024-03-31', 'numeric_value': 100},
                            {'metric_code': CODE, 'target_date': '2024-06-30', 'numeric_value': 105}]}
    monkeypatch.setattr(earnings, 'get_earnings_metrics_by_isin', own)
    result = asyncio.run(earnings.chart_bundle(earnings.ChartBundleRequest(isin='X', cadence='quarterly')))
    assert result['benchmark'] is None
    own_stats = result['own']['chart_statistics']['revenue']
    assert own_stats['cagr']['years'] == .25
    assert math.isclose(own_stats['cagr']['pct'], 100 * (1.05 ** 4 - 1))
