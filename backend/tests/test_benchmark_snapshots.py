import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from routers import _benchmark_snapshots as snapshots, _blend_prewarm as warm


@pytest.mark.parametrize('cadence', ['annual', 'quarterly'])
def test_compact_result_matches_cross_language_fixture_and_contains_no_company_rows(cadence):
    fixture = json.loads((Path(__file__).parent / 'fixtures/benchmark_snapshot_parity.json').read_text())
    if cadence == 'quarterly':
        fixture = fixture['quarterly']
    payloads = {path: {'rows': fixture['rows']} for path in snapshots.INPUTS}
    payloads['universe-period-caps'] = {'caps': fixture['caps']}
    payloads['fundamental-blend-metrics'] = {'metrics': [], 'coverage': {'rows': fixture['rows'], 'covered_pct': 100}}
    result = snapshots.compact('AEX', cadence, payloads)
    assert result == fixture['snapshot']
    assert 'rows' not in result['growth']['coverage']
    assert len(json.dumps(result)) < len(json.dumps(payloads)) / 3


def test_incomplete_rebuild_never_replaces_saved_snapshot(monkeypatch):
    monkeypatch.setattr(snapshots, 'supabase', SimpleNamespace(rpc=lambda *a, **kw: pytest.fail('must not publish')))
    with pytest.raises(ValueError, match='keeping the previous snapshot'):
        snapshots.publish('ACWI', 'annual', {}, '2026-11-03T00:00:00Z')


def test_acwi_500_boundary_uses_reported_values_and_keeps_zero():
    rows = [{'isin': str(i), 'weight_pct': 1, 'fcf': {'2024': 0, **({'2025': 10} if i < 499 else {})}}
            for i in range(500)]
    result = snapshots.weighted_series(rows, {}, 'ACWI', 'fcf', lambda r, p: r['fcf'].get(p))
    assert result['points'] == [[2024.0, 0.0]]
    assert result['coverage'][1]['companies'] == 499


def test_acwi_backfill_prefers_a_missing_input_in_a_year_below_the_floor():
    from routers._acwi_weekly import annual_priorities

    rows = [{'isin': str(i), 'weight_pct': 1, 'fcf': {'2023': 1, '2024': 1},
             'total_equity': {'2023': 2, '2024': 2},
             'noncurrent_liabilities': {year: 1 for year in ('2023', '2024')
                                       if (i, year) != (0, '2023') and (i, year) != (1, '2024')}}
            for i in range(501)]
    payloads = {path: {'rows': []} for path in snapshots.INPUTS}
    payloads['cash-return-inputs']['rows'] = rows
    payloads['universe-period-caps'] = {'caps': {}}
    payloads['fundamental-blend-metrics'] = {'metrics': []}
    snapshot = snapshots.compact('ACWI', 'annual', payloads)
    snapshot['series']['cash-return-inputs']['croic_reported']['coverage'][0]['companies'] = 499
    priority, coverage = annual_priorities(payloads, snapshot, {i: {'isin': str(i)} for i in range(501)})
    assert coverage['cash-return-inputs/croic_reported']['2023'] == 499
    assert priority[0] > priority[1]


def test_background_warm_publishes_once_after_every_endpoint_not_during_critical_warm(monkeypatch):
    async def endpoint(body, request):
        return {'rows': []}
    monkeypatch.setattr(warm, '_endpoints', lambda: [(p, endpoint) for p in ['universe-period-caps', 'fundamental-blend-metrics', *snapshots.INPUTS]])
    monkeypatch.setattr(warm, '_generation', 7)
    published = []
    monkeypatch.setattr(snapshots, 'publish', lambda *args: published.append(args))
    asyncio.run(warm._warm_one('ACWI', 'annual', 7, endpoints=warm._boot_endpoints()))
    assert not published
    asyncio.run(warm._warm_one('ACWI', 'annual', 7))
    assert len(published) == 1
    assert set(published[0][2]) == {'universe-period-caps', 'fundamental-blend-metrics', *snapshots.INPUTS}


def test_chart_route_reads_only_saved_payload_and_keeps_available_results(monkeypatch):
    from routers import benchmarks
    from starlette.requests import Request
    expected = {'built_at': '2026-11-03T10:00:00Z', 'growth': {'chart_statistics': {}},
                'series': {'margin-inputs': {'corrected': {'points': [[2025, 10]]}}}}
    monkeypatch.setattr(snapshots, 'read', lambda label, cadence, **kw: expected)
    monkeypatch.setattr(snapshots, 'compact', lambda *a: pytest.fail('chart reads must not calculate'))
    request = Request({'type': 'http', 'headers': []})
    response = asyncio.run(benchmarks.benchmark_chart_snapshot('ACWI', request, 'annual'))
    assert json.loads(response.body) == expected
    assert response.headers['X-BB-Benchmark-Built-At'] == expected['built_at']


def test_missing_snapshot_is_seeded_from_stored_inputs_before_boot_warm(monkeypatch):
    saved = {}
    events = []
    monkeypatch.setattr(snapshots, 'read', lambda label, cadence, **kw: saved.get(cadence))
    async def build(label, cadence, gen, endpoints=None):
        events.append(('publish' if endpoints is None else 'inputs', cadence))
        if endpoints is None:
            saved[cadence] = {'built_at': 'ready'}
        return 1
    monkeypatch.setattr(warm, '_warm_one', build)
    monkeypatch.setattr(warm, '_targets', lambda: [('ACWI', 'annual'), ('ACWI', 'quarterly')])
    asyncio.run(warm._warm_boot_critical(warm._generation))
    assert events == [('publish', 'annual'), ('inputs', 'annual'), ('publish', 'quarterly'), ('inputs', 'quarterly')]


def test_existing_snapshot_stays_readable_without_rebuilding_or_vendor_requests(monkeypatch):
    saved = {'built_at': 'previous-complete-revision'}
    monkeypatch.setattr(snapshots, 'read', lambda *a, **kw: saved)
    async def unexpected(*a, **kw):
        pytest.fail('An available saved chart needs no acquisition or rebuild')
    monkeypatch.setattr(warm, '_warm_one', unexpected)
    assert asyncio.run(warm.ensure_saved('ACWI', 'annual')) is saved
