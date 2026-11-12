import pytest
from fastapi import HTTPException

from jobs import Job, JobCtx, JobCancelled, UserFacingJobError
from leonteq import refresh_jobs as refresh
from leonteq.progress import report


@pytest.fixture
def ctx():
    refresh._results.clear()
    return JobCtx(Job(id='refresh-unit', kind='leonteq.refresh.list', label='Portfolios'))


@pytest.mark.parametrize('operation,module,function,payload', [
    ('list', 'portfolio_cache', 'portfolios', {'success': True, 'portfolios': [{'name': 'AI'}]}),
    ('composition', 'composition_cache', 'composition', {'success': True, 'groups': [{'positions': [{}]}]}),
    ('airs', 'airs_models', 'composition', {'portfolio': 'AI', 'rows': [{}]}),
    ('monthly', 'monthly_performance', 'comparison', {'success': True, 'months': []}),
])
def test_source_refresh_reports_real_stages_without_putting_holdings_on_the_job_stream(ctx, monkeypatch, operation, module, function, payload):
    import importlib
    calls = []

    def source(*args, **kwargs):
        calls.append((args, kwargs))
        report('Reading holdings', 'Posities lezen')
        return payload

    monkeypatch.setattr(importlib.import_module(f'leonteq.{module}'), function, source)
    summary = refresh.run(ctx, operation, name='AI', portfolio_id=1)
    assert summary
    assert calls[0][1]['refresh'] is True
    assert refresh.result_for(ctx.job.id) == {'status': 200, 'payload': payload}
    assert 'Reading holdings' in [event['message'] for event in ctx.job.events]
    assert ctx.job.events[-1]['done'] == ctx.job.events[-1]['total'] == 1
    assert all('response' not in event and 'payload' not in event for event in ctx.job.events)
    before = len(ctx.job.events)
    report('Outside the refresh')
    assert len(ctx.job.events) == before


def test_saved_fallback_is_returned_but_the_popup_reports_failure(ctx, monkeypatch):
    from leonteq import portfolio_cache
    payload = {'success': True, 'portfolios': [{'name': 'Saved portfolio'}], 'refresh_error': 'busy'}
    monkeypatch.setattr(portfolio_cache, 'portfolios', lambda **_: payload)
    with pytest.raises(UserFacingJobError, match='Another refresh'):
        refresh.run(ctx, 'list')
    assert refresh.result_for(ctx.job.id)['payload'] == payload
    assert not any(event.get('done') == 1 for event in ctx.job.events)


def test_airs_error_keeps_http_status_and_a_readable_failure(ctx, monkeypatch):
    from leonteq import airs_models
    def fail(*_, **kwargs):
        raise HTTPException(409, 'AIRS is busy with another refresh')
    monkeypatch.setattr(airs_models, 'composition', fail)
    with pytest.raises(UserFacingJobError, match='AIRS is busy'):
        refresh.run(ctx, 'airs', portfolio_id=1)
    assert refresh.result_for(ctx.job.id) == {'status': 409, 'payload': {'detail': 'AIRS is busy with another refresh'}}


def test_cancellation_before_the_first_stage_never_calls_the_source(ctx, monkeypatch):
    from leonteq import portfolio_cache
    monkeypatch.setattr(portfolio_cache, 'portfolios', lambda **_: pytest.fail('cancelled refresh contacted source'))
    ctx.job._cancel.set()
    with pytest.raises(JobCancelled):
        refresh.run(ctx, 'list')


def test_dutch_progress_is_reported_in_the_same_popup(ctx, monkeypatch):
    from leonteq import portfolio_cache
    monkeypatch.setattr(portfolio_cache, 'portfolios', lambda **_: {'success': True, 'portfolios': []})
    refresh.run(ctx, 'list', lang='nl')
    assert ctx.job.events[0]['message'].startswith('Verbinding maken')


def test_price_batch_completes_with_incomplete_coverage_and_keeps_results(ctx, monkeypatch):
    from leonteq import price_checks
    payload = {'success': True, 'refresh_error': 'incomplete_price_checks',
               'summary': {'portfolios': 2, 'checked': 3, 'alerts': 1, 'unavailable': 2},
               'portfolios': [], 'errors': []}
    monkeypatch.setattr(price_checks, 'run', lambda _: payload)
    summary = refresh.run(ctx, 'prices')
    assert summary.startswith('Completed with incomplete coverage:')
    assert '2 uncheckable' in summary
    assert refresh.result_for(ctx.job.id)['payload'] == payload
    assert ctx.job.events[-1]['done'] == 1


@pytest.mark.parametrize('refresh_error', [None, 'incomplete_price_checks'])
def test_price_batch_source_refresh_failure_still_fails_with_partial_results(ctx, monkeypatch, refresh_error):
    from leonteq import price_checks
    payload = {'success': True, 'refresh_error': refresh_error,
               'summary': {'portfolios': 2, 'checked': 3, 'alerts': 0, 'unavailable': 1},
               'portfolios': [], 'errors': ['AIRS could not refresh: TimeoutError.']}
    monkeypatch.setattr(price_checks, 'run', lambda _: payload)
    with pytest.raises(UserFacingJobError, match='1 uncheckable'):
        refresh.run(ctx, 'prices')
    assert refresh.result_for(ctx.job.id)['payload'] == payload
