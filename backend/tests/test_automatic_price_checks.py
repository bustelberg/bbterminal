"""Automatic checks as one isolated flow: ordering, cancellation and saved results."""
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import dashboard_daily
from jobs import JobCancelled
from routers import _airs_price_check as checks
from scheduler_jobs import price_checks
from scheduler_runtime.execution import Cancelled, run_job
from tests._fake_supabase import FakeSupabase


def test_wait_then_one_refresh_then_save_from_stored_prices():
    calls = []
    prices = {'moved': 2, 'failed': 0}
    def refresh(**kwargs):
        calls.append(('refresh', kwargs))
        return prices
    def save(**kwargs):
        assert kwargs['freshen'] is False
        assert kwargs['only_if_changed'] is True
        assert kwargs['should_stop'] is None
        calls.append(('save',))
        return {'saved': 2, 'skipped': 1, 'errors': {}}
    providers = price_checks.PriceCheckProviders(lambda: {'TEST'}, refresh, save)
    message, result = price_checks.run(
        report=lambda *args: None, wait_for_queue=lambda ctx: calls.append(('wait',)), providers=providers)
    assert calls == [('wait',), ('refresh', {'isins': {'TEST'}, 'stale_days': 1}), ('save',)]
    assert result['prices'] is prices
    assert result['saved'] == 2 and result['skipped'] == 1
    assert message == '2 account price checks refreshed'


@pytest.mark.parametrize('error', [Cancelled('cancelled while waiting'), RuntimeError('queue timeout')])
def test_queue_wait_failure_starts_no_provider_or_save_work(error):
    def forbidden(*args, **kwargs):
        pytest.fail('provider work started before a quiet queue window')
    def wait(ctx):
        raise error
    with pytest.raises(type(error), match=str(error)):
        price_checks.run(report=lambda *args: None, wait_for_queue=wait,
                         providers=price_checks.PriceCheckProviders(forbidden, forbidden, forbidden))


def test_cancel_before_start_does_not_even_wait_for_queue():
    def forbidden(*args, **kwargs):
        pytest.fail('cancelled job started work')
    with pytest.raises(Cancelled, match='before automatic price checks'):
        price_checks.run(SimpleNamespace(cancelled=True), report=lambda *args: None,
                         wait_for_queue=forbidden,
                         providers=price_checks.PriceCheckProviders(forbidden, forbidden, forbidden))


@pytest.mark.parametrize('cancel_after_first', [False, True])
def test_partial_comparisons_survive_failure_or_cancellation(monkeypatch, cancel_after_first):
    db = FakeSupabase()
    monkeypatch.setattr(checks, 'supabase', db)
    monkeypatch.setattr(checks, '_price_check_version', lambda: 'v1')
    monkeypatch.setattr(checks, 'saved_price_checks', lambda accounts: {})
    ctx = SimpleNamespace(cancelled=False)
    built = []
    def build(account, *, freshen):
        assert freshen is False
        built.append(account)
        if account == 'B':
            raise RuntimeError('missing FX')
        return {'portefeuille': account, 'result': {'rows': [{'isin': 'TEST'}]}}
    monkeypatch.setattr(checks, '_build_price_check', build)
    def progress(done, total, message):
        if cancel_after_first and 'comparison ready to save' in message:
            ctx.cancelled = True
    prices = {'failed': 0, 'moved': 1}
    providers = price_checks.PriceCheckProviders(
        lambda: {'TEST'}, lambda **kwargs: prices,
        lambda **kwargs: checks.refresh_price_checks(['A', 'B'], **kwargs))
    expected = Cancelled if cancel_after_first else dashboard_daily.IncompleteRefresh
    with pytest.raises(expected) as caught:
        price_checks.run(ctx, report=progress, wait_for_queue=lambda ctx: None, providers=providers)
    assert built == (['A'] if cancel_after_first else ['A', 'B'])
    assert [row['portefeuille'] for row in db.tables['airs_price_check']] == ['A']
    assert db.tables['airs_price_check'][0]['result']['input_version'] == 'v1'
    if cancel_after_first:
        assert 'saved checks are kept' in str(caught.value)
    else:
        assert caught.value.summary['saved'] == 1
        assert caught.value.summary['errors'] == {'B': 'RuntimeError: missing FX'}
        assert caught.value.summary['prices'] is prices


def test_same_job_id_records_failure_without_adding_a_daily_lease(monkeypatch):
    def no_lease(*args, **kwargs):
        pytest.fail('automatic checks unexpectedly acquired the dashboard daily lease')
    monkeypatch.setattr(dashboard_daily, 'daily_lease', no_lease)
    receipts = []
    @contextmanager
    def record(job_id, **kwargs):
        assert job_id == 'airs_price_checks'
        rec = SimpleNamespace(summary=None)
        receipts.append(rec)
        yield rec
    prices = {'failed': 1, 'results': [{'symbol': 'BMW3.DE', 'isin': 'TEST', 'status': 'error', 'error': 'HTTP 503'}]}
    providers = price_checks.PriceCheckProviders(lambda: {'TEST'}, lambda **kwargs: prices,
                                                lambda **kwargs: {'saved': 3, 'errors': {}})
    def body(ctx):
        return price_checks.run(ctx, report=lambda *args: None, wait_for_queue=lambda ctx: None, providers=providers)
    with pytest.raises(dashboard_daily.IncompleteRefresh, match='BMW3.DE'):
        run_job('airs_price_checks', None, 'auto', bodies={'airs_price_checks': body}, record=record)
    assert len(receipts) == 1
    assert receipts[0].summary['saved'] == 3
    assert receipts[0].summary['prices'] is prices


def test_cancelled_flow_remains_a_cancelled_job():
    receipts = []
    @contextmanager
    def record(job_id, **kwargs):
        rec = SimpleNamespace(status='running', detail=None)
        receipts.append(rec)
        yield rec
    def body(ctx):
        raise Cancelled('saved checks are kept')
    with pytest.raises(JobCancelled, match='saved checks are kept'):
        run_job('airs_price_checks', None, 'manual', bodies={'airs_price_checks': body}, record=record)
    assert receipts[0].status == 'cancelled'
