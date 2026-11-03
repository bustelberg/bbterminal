import jobs
from routers import jobs as routes


def result(coroutine):
    try:
        coroutine.send(None)
    except StopIteration as done:
        return done.value
    raise AssertionError('Details must be a non-blocking registry read')


def test_details_pages_keep_all_diagnostics_and_profiling(monkeypatch):
    job = jobs.Job('log', 'test', 'ACWI')
    ctx = jobs.JobCtx(job)
    for i in range(1100):
        ctx.emit('diagnostic', f'Company {i}', profile={'database_ms': i}, details={'metric': 'revenue'})
    monkeypatch.setattr(routes.job_registry, 'get', lambda _id: job)
    first = result(routes.job_details('log', after=0))
    assert len(first['events']) == 500 and first['next_cursor'] == 500 and first['has_more']
    second = result(routes.job_details('log', after=500))
    third = result(routes.job_details('log', after=second['next_cursor']))
    all_events = first['events'] + second['events'] + third['events']
    assert len(all_events) == 1100
    assert all_events[-1]['profile'] == {'database_ms': 1099}
    assert all_events[-1]['elapsed_ms'] >= 0
    assert not third['has_more']
