"""Overlapping workers with real thread coordination and in-memory dependencies."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar
from types import SimpleNamespace
import threading

import pytest

import common.pg
import dashboard_daily
from jobs import JobCancelled
from scheduler_runtime import admission, execution
from scheduler_jobs import price_checks
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def receipts():
    rows = []
    @contextmanager
    def record(job_id, **kwargs):
        row = SimpleNamespace(job_id=job_id, status='running', detail=None, summary=None)
        def done(detail, **summary):
            row.status, row.detail, row.summary = 'ok', detail, summary
        row.done = done
        rows.append(row)
        try:
            yield row
        except Exception:
            if row.status == 'running':
                row.status = 'error'
            raise
    return record, rows


@pytest.fixture
def advisory_lock(monkeypatch):
    import psycopg
    lock, commands = threading.Lock(), []
    monkeypatch.setattr(common.pg, '_db_url', lambda: 'unit-test')
    monkeypatch.setattr(dashboard_daily, '_lease_held', ContextVar('test_daily_lease', default=False))
    class Connection:
        owned = False
        def execute(self, command):
            commands.append(command)
            if 'pg_try_advisory_lock' in command:
                self.owned = lock.acquire(False)
                return SimpleNamespace(fetchone=lambda: (self.owned,))
            assert 'pg_advisory_unlock' in command and self.owned
            self.owned = False
            lock.release()
        def __enter__(self):
            return self
        def __exit__(self, *args):
            if self.owned:
                self.owned = False
                lock.release()
    monkeypatch.setattr(psycopg, 'connect', lambda *args, **kwargs: Connection())
    return lock, commands


@pytest.mark.parametrize('cancel', [False, True])
def test_daily_lease_rejects_overlap_and_releases_after_partial_failure_or_cancel(advisory_lock, receipts, cancel):
    lock, commands = advisory_lock
    record, recorded = receipts
    entered, finish = threading.Event(), threading.Event()
    saved, context_after = {}, []
    evidence = {'saved': 1, 'errors': {'BOOK-B': 'missing FX'}, 'source': {'symbol': 'TEST', 'stored': '2026-11-09'}}
    def body(ctx):
        saved['BOOK-A'] = {'complete': True}
        # A nested preparation stage must reuse the owner's lease.
        with dashboard_daily.daily_lease('dashboard_post_airs', 'manual') as nested:
            assert nested
        entered.set()
        assert finish.wait(5), 'test did not release the worker'
        if cancel:
            raise execution.Cancelled('stopped after BOOK-A; completed work saved')
        raise dashboard_daily.IncompleteRefresh('partial preparation', evidence)
    def first_run():
        try:
            return execution.run_job('dashboard_market', None, 'manual',
                                     bodies={'dashboard_market': body}, record=record)
        finally:
            context_after.append(dashboard_daily._lease_held.get())
    def forbidden(ctx):
        pytest.fail('overlapping job entered its body')
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(first_run)
        try:
            assert entered.wait(5), 'first worker did not acquire the lease'
            assert lock.locked()
            with pytest.raises(RuntimeError, match='holds the cross-replica lock'):
                execution.run_job('dashboard_post_airs', None, 'manual',
                                  bodies={'dashboard_post_airs': forbidden}, record=record)
            assert len([sql for sql in commands if 'pg_try_advisory_lock' in sql]) == 2
        finally:
            finish.set()
        expected = JobCancelled if cancel else dashboard_daily.IncompleteRefresh
        with pytest.raises(expected):
            first.result(timeout=5)
    assert saved == {'BOOK-A': {'complete': True}}
    assert context_after == [False]
    assert not lock.locked()
    assert recorded[0].status == ('cancelled' if cancel else 'error')
    if not cancel:
        assert recorded[0].summary == evidence
    assert recorded[1].status == 'error'
    assert 'could not acquire its lease' in recorded[1].detail
    assert execution.run_job('dashboard_market', None, 'manual',
                             bodies={'dashboard_market': lambda ctx: ('retry completed', {'saved': 1})},
                             record=record) == 'retry completed'
    assert not lock.locked() and dashboard_daily._lease_held.get() is False
    assert len([sql for sql in commands if 'pg_advisory_unlock' in sql]) == 2


@pytest.mark.parametrize('cancel', [False, True])
def test_worker_capacity_is_reusable_after_failure_or_cancel(monkeypatch, receipts, cancel):
    record, recorded = receipts
    entered, finish, released = threading.Event(), threading.Event(), threading.Event()
    semaphore = threading.BoundedSemaphore(1)
    class Capacity:
        def acquire(self, **kwargs):
            return semaphore.acquire(**kwargs)
        def release(self):
            semaphore.release()
            released.set()
    class Pool:
        def __init__(self, executor):
            self.executor, self.futures = executor, []
        def submit(self, work):
            future = self.executor.submit(work)
            self.futures.append(future)
            return future
    saved = []
    def body(ctx):
        saved.append('completed account')
        entered.set()
        assert finish.wait(5), 'test did not release the worker'
        if cancel:
            raise execution.Cancelled('safe stopping point')
        raise dashboard_daily.IncompleteRefresh('provider unavailable', {'saved': 1, 'error': 'HTTP 503'})
    with ThreadPoolExecutor(max_workers=1) as executor:
        pool = Pool(executor)
        monkeypatch.setattr(admission, '_scheduled_executor', pool)
        monkeypatch.setattr(admission, '_scheduled_slots', Capacity())
        monkeypatch.setattr(admission, '_scheduled_futures', {})
        monkeypatch.setattr(admission, '_scheduled_futures_lock', threading.Lock())
        def work():
            return execution.run_recorded('prices', None, 'auto', bodies={'prices': body}, record=record)
        assert admission.submit('prices', work)
        try:
            assert entered.wait(5)
            assert not admission.submit('prices', lambda: pytest.fail('duplicate ran'))
            assert not admission.submit('other', lambda: pytest.fail('capacity exceeded'))
            assert len(pool.futures) == 1
        finally:
            finish.set()
        with pytest.raises(JobCancelled if cancel else dashboard_daily.IncompleteRefresh):
            pool.futures[0].result(timeout=5)
        assert released.wait(5), 'completion did not return worker capacity'
        assert admission.active_jobs() == set()
        assert saved == ['completed account']
        assert recorded[0].status == ('cancelled' if cancel else 'error')
        if not cancel:
            assert recorded[0].summary == {'saved': 1, 'error': 'HTTP 503'}
        assert admission.submit('prices', lambda: 'next run completed')
        assert pool.futures[1].result(timeout=5) == 'next run completed'


def test_same_job_cannot_overlap_even_when_another_worker_is_free(monkeypatch):
    entered, finish, cleaned = threading.Event(), threading.Event(), threading.Event()
    calls, futures = [], []
    class Pool:
        def __init__(self, executor):
            self.executor = executor
        def submit(self, work):
            future = self.executor.submit(work)
            futures.append(future)
            return future
    def first_work():
        calls.append('first')
        entered.set()
        assert finish.wait(5), 'test did not release the first worker'
        return 'first completed'
    with ThreadPoolExecutor(max_workers=2) as executor:
        monkeypatch.setattr(admission, '_scheduled_executor', Pool(executor))
        monkeypatch.setattr(admission, '_scheduled_slots', threading.BoundedSemaphore(2))
        monkeypatch.setattr(admission, '_scheduled_futures', {})
        monkeypatch.setattr(admission, '_scheduled_futures_lock', threading.Lock())
        assert admission.submit('prices', first_work)
        futures[0].add_done_callback(lambda future: cleaned.set())
        try:
            assert entered.wait(5)
            assert not admission.submit('prices', lambda: calls.append('duplicate'))
            # Capacity exists: rejecting the duplicate must be the per-job guard.
            assert admission.submit('different-job', lambda: 'other worker completed')
            assert futures[1].result(timeout=5) == 'other worker completed'
            assert calls == ['first']
        finally:
            finish.set()
        assert futures[0].result(timeout=5) == 'first completed'
        assert cleaned.wait(5)
        assert admission.submit('prices', lambda: 'next prices run completed')
        assert futures[2].result(timeout=5) == 'next prices run completed'


@pytest.mark.parametrize('cancel', [False, True])
def test_real_recorder_finalizes_partial_price_flow_without_losing_saved_data(monkeypatch, cancel):
    import job_runlog
    db = FakeSupabase()
    original_table = db.table
    def table(name):
        query = original_table(name)
        if name == 'scheduled_job_run':
            insert = query.insert
            # Supply the database-generated ID without using a live database.
            query.insert = lambda row: insert({'id': 1, 'status': 'running', **row})
        return query
    monkeypatch.setattr(db, 'table', table)
    monkeypatch.setattr(job_runlog, '_supabase', lambda: db)
    monkeypatch.setattr(job_runlog, '_register_heartbeat', lambda row_id: None)
    parent_before = job_runlog._active_run.get()
    ctx = SimpleNamespace(cancelled=False)
    full_error = 'Provider failure evidence. ' * 200
    prices = {'failed': 1, 'results': [{'isin': 'TEST', 'symbol': 'TEST', 'status': 'error', 'error': full_error}]}
    def save(**kwargs):
        assert kwargs['freshen'] is False
        db.table('airs_price_check').upsert({'portefeuille': 'BOOK-A', 'result': {'rows': ['completed']}},
                                          on_conflict='portefeuille').execute()
        ctx.cancelled = cancel
        return {'saved': 1, 'errors': {}}
    providers = price_checks.PriceCheckProviders(lambda: {'TEST'}, lambda **kwargs: prices, save)
    def body(ctx):
        return price_checks.run(ctx, report=lambda *args: None, wait_for_queue=lambda ctx: None, providers=providers)
    with pytest.raises(JobCancelled if cancel else dashboard_daily.IncompleteRefresh):
        execution.run_job('airs_price_checks', ctx, 'manual',
                          bodies={'airs_price_checks': body}, record=job_runlog.record_run)
    row, = db.tables['scheduled_job_run']
    assert row['status'] == ('cancelled' if cancel else 'error')
    assert row['finished_at']
    assert 'profile' in row['summary']
    assert db.tables['airs_price_check'][0]['result'] == {'rows': ['completed']}
    assert job_runlog._active_run.get() is parent_before
    if not cancel:
        assert row['summary']['saved'] == 1
        assert row['summary']['prices']['results'][0]['error'] == full_error
