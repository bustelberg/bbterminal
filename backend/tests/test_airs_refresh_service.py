"""Service boundaries: locking, saved partial work and caller cancellation."""
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

import airs_scanner
import airs_vermogen
from jobs import JobCancelled
from services import airs_refresh as service
from routers import _airs_portfolio_store as store


@pytest.fixture
def models(monkeypatch):
    import management_portfolios
    lock = threading.Lock()
    monkeypatch.setattr(airs_vermogen, '_LOCK', lock)
    monkeypatch.setattr(airs_vermogen, '_acquire_session', lambda wait: lock.acquire(False))
    rows = [{'id': 1}, {'id': 2}]
    saved = []
    monkeypatch.setattr(airs_scanner, 'fetch_model_portfolios_sync', lambda emit: rows)
    monkeypatch.setattr(management_portfolios, 'dashboard_model_rows', lambda rows: rows)
    monkeypatch.setattr(store, 'save_portfolios', lambda rows: saved.append(('roster', list(rows))))
    monkeypatch.setattr(store, 'unscanned_position_ids', lambda ids: {2})
    monkeypatch.setattr(store, 'save_positions', lambda *args: saved.append(('positions', args)))
    monkeypatch.setattr(store, 'save_positions_error', lambda *args: saved.append(('error', args)))
    return lock, saved


@pytest.mark.parametrize('force,expected', [(False, [2]), (True, [1, 2])])
def test_shared_discovery_preserves_scope_and_partial_failure(models, monkeypatch, force, expected):
    lock, saved = models
    def count(rows, emit, *, on_positions, on_error, **kwargs):
        assert lock.locked()
        assert saved[0][0] == 'roster'
        assert [row['id'] for row in rows] == expected
        on_error(2, 'bad export')
    monkeypatch.setattr(airs_scanner, 'count_model_portfolio_holdings_sync', count)
    result = service.collect_model_compositions(on_event=lambda *_: None, force=force)
    assert result['errors'] == [{'id': 2, 'error': 'bad export'}]
    assert saved[-1] == ('error', (2, 'bad export'))
    assert not lock.locked()


def test_busy_service_does_not_read_or_persist(models):
    lock, saved = models
    lock.acquire()
    try:
        assert service.collect_model_compositions(on_event=lambda *_: None, force=True) is None
        assert saved == []
    finally:
        lock.release()


def test_failure_releases_session_and_keeps_completed_discovery(models, monkeypatch):
    lock, saved = models
    def count(*args, **kwargs):
        raise RuntimeError('download failed')
    monkeypatch.setattr(airs_scanner, 'count_model_portfolio_holdings_sync', count)
    with pytest.raises(RuntimeError, match='download failed'):
        service.collect_model_compositions(on_event=lambda *_: None, force=True, wait_seconds=60)
    assert saved[0][0] == 'roster'
    assert not lock.locked()


def test_model_cancel_is_a_cancelled_job_and_releases_session(models, monkeypatch):
    lock, saved = models
    ctx = SimpleNamespace(cancelled=False, progress=lambda *args: None)
    def count(rows, emit, *, should_stop, **kwargs):
        kwargs['on_positions'](2, '2026-11-11', [{'isin': 'TEST'}], None)
        ctx.cancelled = True
        assert should_stop()
    monkeypatch.setattr(airs_scanner, 'count_model_portfolio_holdings_sync', count)
    with pytest.raises(JobCancelled, match='portfolios stored'):
        service.scan_models(ctx)
    assert saved[0][0] == 'roster'
    assert saved[-1] == ('positions', (2, '2026-11-11', [{'isin': 'TEST'}], None))
    assert not lock.locked()


def test_overlapping_model_scans_share_the_session_and_retry_after_release(models, monkeypatch):
    lock, saved = models
    entered, finish = threading.Event(), threading.Event()
    counted = []
    def count(rows, emit, **kwargs):
        counted.append([row['id'] for row in rows])
        entered.set()
        assert finish.wait(5), 'test did not release the model scan'
    monkeypatch.setattr(airs_scanner, 'count_model_portfolio_holdings_sync', count)
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(service.collect_model_compositions, on_event=lambda *_: None, force=True)
        try:
            assert entered.wait(5)
            assert service.collect_model_compositions(on_event=lambda *_: None, force=False) is None
            assert len(saved) == 1 and saved[0][0] == 'roster'
            assert counted == [[1, 2]]
        finally:
            finish.set()
        assert first.result(timeout=5)['errors'] == []
    assert not lock.locked()
    assert service.collect_model_compositions(on_event=lambda *_: None, force=False)['errors'] == []
    assert counted == [[1, 2], [2]]


def test_manual_pricing_translates_service_cancel(monkeypatch):
    def price(**kwargs):
        assert kwargs['stored_composition'] is False
        raise service.RefreshCancelled('stopped before model pricing')
    monkeypatch.setattr(service, 'price_models', price)
    with pytest.raises(JobCancelled, match='stopped before model pricing'):
        service.refresh_models(SimpleNamespace(cancelled=False, progress=lambda *args: None))


def test_scheduled_pricing_translates_service_cancel(monkeypatch):
    import scheduler
    def price(**kwargs):
        assert kwargs['stored_composition'] is True
        raise service.RefreshCancelled('stopped before model pricing')
    monkeypatch.setattr(service, 'price_models', price)
    with pytest.raises(scheduler._Cancelled, match='stopped before model pricing'):
        scheduler._body_airs_model_prices()


def test_market_job_does_not_start_provider_work_after_cancel(monkeypatch):
    from services import airs_market_jobs
    from routers import _airs_price_check
    def check():
        raise JobCancelled('cancelled')
    def refresh(*args):
        pytest.fail('cancelled job reached provider work')
    monkeypatch.setattr(_airs_price_check, 'recheck_price_check_row', refresh)
    with pytest.raises(JobCancelled):
        airs_market_jobs.refresh_holding_price(
            SimpleNamespace(check=check, progress=lambda *args: None),
            portefeuille='BOOK', normalized='TEST')


def test_progress_listener_failure_does_not_fail_completed_work(monkeypatch):
    import job_runlog
    from services.job_progress import reporter
    recorded = []
    monkeypatch.setattr(job_runlog, 'record_progress', lambda *event: recorded.append(event))
    def broken(*args):
        raise RuntimeError('stream disconnected')
    reporter(SimpleNamespace(progress=broken))(1, 2, 'x' * 400)
    assert recorded == [(1, 2, 'x' * 300)]
