from datetime import datetime, timedelta, timezone

from routers import _acwi_weekly as weekly, _benchmark_index as index, _fundamental_backfill as backfill
from routers import _fundamental_fill as fill, _benchmark_repair as repair, _blend_prewarm as warm, _benchmark_snapshots as snapshots
from ingest import api_usage
from tests.test_fill_cancel import FakeCtx


def test_weekly_retries_available_companies_preserves_quota_and_publishes_both_cadences(monkeypatch):
    now = datetime.now(timezone.utc).isoformat()
    monkeypatch.setattr(index, '_members', lambda *a, **kw: [{'company_id': i} for i in range(1, 6)])
    companies = {i: {'company_id': i, 'company_name': f'Co {i}', 'gurufocus_ticker': f'T{i}',
                    'gurufocus_exchange': {'exchange_code': 'NYSE'}} for i in range(1, 5)}
    companies[2].update(financials_fetched_at=now, estimates_fetched_at=now)
    last_week_later = (datetime.now(timezone.utc) - timedelta(days=7) + timedelta(hours=1)).isoformat()
    companies[1].update(financials_fetched_at=last_week_later, estimates_fetched_at=last_week_later)
    monkeypatch.setattr(backfill, 'company_rows', lambda ids: companies)
    monkeypatch.setattr(backfill, 'eligible', lambda c: 'Outside subscription' if c['company_id'] == 3 else None)
    monkeypatch.setattr(fill, 'order_work', lambda companies: sorted(companies, key=lambda c: c['company_id']))
    monkeypatch.setattr(api_usage, 'remaining_budget', lambda *a: {'usa': 2004})
    monkeypatch.setattr(api_usage, '_region_for_exchange', lambda *a: 'usa')
    monkeypatch.setattr(repair, 'audit', lambda *a: {'present_companies_by_metric': {'revenue': 577}})
    monkeypatch.setattr(weekly, '_annual_growth_rows', lambda *a: [])
    monkeypatch.setattr(weekly, 'annual_priorities', lambda *a: ({1: 10}, {'revenue': {'2024': 499}}))
    requested = []
    def fetch(ctx, label, ids, **kw):
        requested.extend(ids)
        assert kw['best_effort'] and kw['force'] and kw['key_ratios']
        assert kw['feeds'] == 'statements_estimates'
        kw['receipt'].update(attempted=1, failed=1, loaded=0, calls=1)
    monkeypatch.setattr(fill, 'fill_company_ids', fetch)
    published = []
    async def build(label, cadence, gen, *, on_publish=None):
        published.append(cadence)
        if on_publish:
            on_publish({}, {})
    monkeypatch.setattr(warm, '_warm_one', build)
    async def seed(label, cadence):
        assert not requested
        return {'built_at': now}
    monkeypatch.setattr(warm, 'ensure_saved', seed)
    monkeypatch.setattr(snapshots, 'read', lambda *a: {'built_at': datetime.now(timezone.utc).isoformat(),
                                                    'growth': {'member_counts': {}}})
    message, result = weekly.run(FakeCtx(), reserve=2000)
    assert requested == [1]
    assert result['members'] == 5
    assert result['unavailable'] == 3  # Empty answer, unsupported venue, missing identity.
    assert result['recently_checked'] == 1
    assert result['quota_deferred'] == 1
    assert published == ['annual', 'annual', 'quarterly']
    assert result['annual_chart_coverage_before'] == {'revenue': {'2024': 499}}
    assert 'charts saved' in message


def test_declared_weekly_job_has_local_time_trigger_and_monitoring():
    from scheduled_jobs import SCHEDULED_JOBS
    spec = next(s for s in SCHEDULED_JOBS if s.id == 'acwi_weekly_coverage')
    assert spec.trigger == {'day_of_week': 'sat', 'hour': 3, 'minute': 0, 'timezone': 'Europe/Amsterdam'}
    assert spec.records and spec.max_age_hours == 24 * 8
