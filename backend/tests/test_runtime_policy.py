"""Local startup must never start automatic work, even without opt-out flags."""
import inspect

import pytest

from runtime_policy import production_automation_enabled


@pytest.fixture
def environment(monkeypatch):
    for key in ('APP_ENV', 'RAILWAY_ENVIRONMENT_NAME', 'RAILWAY_PUBLIC_DOMAIN', 'DISABLE_SCHEDULER'):
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


@pytest.mark.parametrize('values,expected', [
    ({}, False),
    ({'DISABLE_SCHEDULER': '0'}, False),
    ({'APP_ENV': 'production'}, True),
    ({'RAILWAY_ENVIRONMENT_NAME': 'production'}, True),
    ({'RAILWAY_PUBLIC_DOMAIN': 'example.up.railway.app'}, True),
    ({'APP_ENV': 'local', 'RAILWAY_PUBLIC_DOMAIN': 'example.up.railway.app'}, False),
    ({'RAILWAY_ENVIRONMENT_NAME': 'staging', 'RAILWAY_PUBLIC_DOMAIN': 'example.up.railway.app'}, False),
    ({'APP_ENV': 'production', 'DISABLE_SCHEDULER': 'true'}, False),
])
def test_automatic_work_environment_boundary(environment, values, expected):
    for key, value in values.items():
        environment.setenv(key, value)
    assert production_automation_enabled() is expected


def test_local_startup_without_disable_flags_does_not_touch_database_or_start_threads(environment):
    import main
    import scheduler

    # The suite rejects real database access. Make thread creation fail too,
    # so adding a new unguarded warmup cannot silently slip through this test.
    def reject_thread(*args, **kwargs):
        pytest.fail('local startup attempted to create a background thread')

    environment.setattr('threading.Thread.start', reject_thread)
    environment.setattr(scheduler, '_scheduler', None)

    for hook in main.app.router.on_startup:
        if inspect.iscoroutinefunction(hook):
            # Executor sizing is server configuration, independent of data work.
            assert hook.__name__ == '_size_io_thread_pool'
        else:
            hook()
    assert scheduler._scheduler is None


def test_local_ticks_retries_and_prewarming_do_nothing(environment):
    import scheduler
    from routers import _blend_prewarm

    def unexpected(*args, **kwargs):
        pytest.fail('automatic local work reached dispatch')

    environment.setattr(scheduler.admission, 'submit', unexpected)
    environment.setattr(scheduler.retries, 'schedule_price_retry', unexpected)
    environment.setattr(scheduler, 'kick_off_refresh', unexpected)
    environment.setattr(scheduler, 'record_missed', unexpected, raising=False)
    environment.setattr('threading.Thread.start', unexpected)
    assert scheduler._submit_scheduled_work('test', unexpected) is False
    scheduler._spawn_body('asset_price_refresh')
    scheduler._fire_job('price_slice')
    scheduler.maybe_schedule_price_retry()
    _blend_prewarm.arm()


def test_manual_job_remains_available_locally(environment):
    import jobs
    import scheduler

    calls = []
    context = object()
    environment.setattr(scheduler, 'JOB_BODIES', {'test': object()})
    environment.setattr(scheduler, '_run_body', lambda job_id, ctx, triggered_by:
                        calls.append((job_id, ctx, triggered_by)))
    def start(key, label, work):
        work(context)
        return 'manual job', False
    environment.setattr(jobs, 'start', start)
    assert scheduler.start_job_now('test') == 'manual job'
    assert calls == [('test', context, 'manual')]


def test_local_read_does_not_lazily_reprice(environment):
    from routers._schedule_snapshots import ensure_snapshot_fresh
    assert ensure_snapshot_fresh(123) is None
