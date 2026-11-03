from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import jobs
from routers import _benchmark_repair as repair, _benchmark_index as index, _fundamental_backfill as backfill
from routers import _fundamental_fill as fill
from routers.earnings import _metric_codes


@pytest.fixture
def audit_env(monkeypatch):
    repair._recent.clear()
    repair._failed.clear()
    monkeypatch.setattr(index, '_members', lambda *a, **kw: [{'company_id': cid} for cid in range(1, 5)])
    today = datetime.now(timezone.utc).isoformat()
    companies = {cid: {'company_id': cid, 'company_name': f'Company {cid}'} for cid in range(1, 5)}
    companies[4].update(financials_fetched_at=today, estimates_fetched_at=today)
    monkeypatch.setattr(backfill, 'company_rows', lambda ids: companies)
    monkeypatch.setattr(backfill, 'eligible', lambda c: 'unsubscribed exchange' if c['company_id'] == 3 else None)
    core = {_metric_codes(key)[-1] for key in repair.CORE}
    estimate = set(_metric_codes('eps_nri_estimate'))
    presence = {
        '1': list(core | estimate),
        '2': list((core | estimate) - {_metric_codes('fcf_ps')[-1]}),
        '3': [], '4': [],
    }
    monkeypatch.setattr(repair, 'supabase', SimpleNamespace(rpc=lambda *a, **kw:
        SimpleNamespace(execute=lambda: SimpleNamespace(data={'presence': presence, 'periods': []}))))
    return presence


def test_audit_checks_real_alias_values_and_only_selects_eligible_unchecked_gaps(audit_env):
    result = repair.audit('ACWI', 'annual')
    assert result['statements_to_fetch'] == [2]
    assert result['estimates_to_fetch'] == []
    assert [(c['company_id'], c['reason']) for c in result['unavailable']] == [(3, 'unsubscribed exchange')]
    assert result['present_companies_by_metric']['revenue'] == 2
    assert set(result['profile']) >= {'membership_ms', 'metric_presence_rpc_ms', 'company_metadata_ms', 'metric_batches'}


def test_failed_audit_never_starts_a_vendor_fetch(audit_env, monkeypatch):
    monkeypatch.setattr(repair, 'supabase', SimpleNamespace(rpc=lambda *a, **kw:
        SimpleNamespace(execute=lambda: SimpleNamespace(data=None))))
    monkeypatch.setattr(fill, 'fill_company_ids', lambda *a, **kw: pytest.fail('must not fetch unverified gaps'))
    job = jobs.Job('verify', 'test', 'ACWI')
    with pytest.raises(RuntimeError, match='no automatic fetch'):
        repair.repair(jobs.JobCtx(job), 'ACWI', 'annual')
    assert job.events[-1]['profile']['total'] >= 0


def test_verified_missing_feed_is_filled_then_rechecked_and_not_repeated(audit_env, monkeypatch):
    calls = []
    def fetch(ctx, label, ids, **kw):
        ctx.check()
        assert not ctx.cancelled
        ctx.spent(1)
        calls.append((label, ids, kw))
        ctx.emit('info', 'Technical feed detail', company_id=2)
        audit_env['2'].append(_metric_codes('fcf_ps')[-1])
    monkeypatch.setattr(fill, 'fill_company_ids', fetch)
    job = jobs.Job('repair', 'test', 'ACWI')
    summary = repair.repair(jobs.JobCtx(job), 'ACWI', 'annual')
    assert calls == [('ACWI', [2], {'feeds': 'statements', 'force': True, 'only_due': False,
                                  'receipt': {}, 'best_effort': True})]
    assert 'still unavailable' in summary
    assert any(event.get('details', {}).get('original_kind') == 'info' for event in job.events)
    assert all('elapsed_ms' in event for event in job.events)
    assert job.api_calls == 1
    assert set(job.events[-1]['profile']) >= {'stored_data_audit', 'fetch_statements', 'verification', 'total'}
    repair.repair(jobs.JobCtx(job), 'ACWI', 'annual')
    assert len(calls) == 1


def test_full_membership_survives_postgrest_page_cap(monkeypatch):
    from tests._fake_supabase import FakeSupabase
    fake = FakeSupabase({'universe': [{'label': 'ACWI', 'universe_id': 1}],
        'universe_membership': [{'universe_id': 1, 'company_id': cid} for cid in range(1, 1002)],
        'company': [{'company_id': cid, 'company_name': f'Company {cid}', 'market_cap_eur': 1,
                     'delisted_at': None, 'out_of_scope_at': None} for cid in range(1, 1002)],
        'gurufocus_exchange': []})
    monkeypatch.setattr(index, 'supabase', fake)
    assert len(index._members('ACWI', require_market_cap=False)) == 1001


def test_empty_vendor_batch_keeps_data_and_cools_down_across_cadences(audit_env, monkeypatch):
    calls = []
    def fetch(ctx, label, ids, **kw):
        calls.append(ids)
        kw['receipt'].update(rows=0, attempted=12, failed=12)
        raise jobs.UserFacingJobError('GuruFocus did not provide financial statements for 12 companies')
    monkeypatch.setattr(fill, 'fill_company_ids', fetch)
    job = jobs.Job('empty', 'test', 'ACWI')
    with pytest.raises(jobs.UserFacingJobError, match='provider returned no financial statements'):
        repair.repair(jobs.JobCtx(job), 'ACWI', 'annual')
    assert any(e.get('data_changed') is False for e in job.events)
    assert any(e.get('details', {}).get('receipts') for e in job.events)
    with pytest.raises(jobs.UserFacingJobError, match='15 minutes'):
        repair.repair(jobs.JobCtx(jobs.Job('retry', 'test', 'ACWI')), 'ACWI', 'quarterly')
    assert len(calls) == 1


def test_expected_empty_statements_do_not_prevent_estimates_or_verification(audit_env, monkeypatch):
    audit_env['2'] = [code for code in audit_env['2'] if code not in _metric_codes('eps_nri_estimate')]
    calls = []
    def fetch(ctx, label, ids, **kw):
        assert kw['best_effort'] is True
        calls.append(kw['feeds'])
        kw['receipt'].update(rows=0, attempted=1, failed=1, calls=1, deferred=0)
    monkeypatch.setattr(fill, 'fill_company_ids', fetch)
    job = jobs.Job('partial', 'test', 'ACWI')
    result = repair.repair(jobs.JobCtx(job), 'ACWI', 'annual')
    assert calls == ['statements', 'estimates']
    assert 'still unavailable' in result
    assert any(e['message'] == 'Coverage after checking/fetching' for e in job.events)
    assert sum(e['message'].endswith('request results') for e in job.events) == 2
    assert any(e.get('data_changed') is False for e in job.events)
    repair.repair(jobs.JobCtx(job), 'ACWI', 'annual')
    assert len(calls) == 2
