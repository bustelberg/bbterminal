"""Recording semantics without importing the scheduler or running a real job."""
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from jobs import JobCancelled
from scheduler_runtime import execution


@pytest.fixture
def recorder():
    receipts = []
    @contextmanager
    def record(name, **kwargs):
        rec = SimpleNamespace(status='running', detail=None, summary=None)
        def done(detail, **summary):
            rec.status, rec.detail, rec.summary = 'ok', detail, summary
        rec.done = done
        receipts.append(rec)
        yield rec
    return record, receipts


def test_cancellation_is_recorded_before_transport_translation(recorder):
    record, receipts = recorder
    def body(ctx):
        raise execution.Cancelled('stopped after saved account')
    with pytest.raises(JobCancelled, match='stopped after saved account'):
        execution.run_recorded('test', None, 'manual', bodies={'test': body}, record=record)
    assert receipts[0].status == 'cancelled'
    assert receipts[0].detail == 'stopped after saved account'


def test_incomplete_job_retains_exact_structured_receipt(recorder):
    record, receipts = recorder
    class PartialResult(RuntimeError):
        summary = {'saved': 3, 'errors': {'BOOK': 'missing FX'}}
    def body(ctx):
        raise PartialResult('partial work saved')
    with pytest.raises(PartialResult):
        execution.run_recorded('test', None, 'auto', bodies={'test': body}, record=record)
    assert receipts[0].summary == PartialResult.summary


def test_self_recording_asset_job_has_no_duplicate_receipt(recorder):
    record, receipts = recorder
    calls = []
    assert execution.run_recorded('asset_price_refresh', None, 'auto',
                                  bodies={'asset_price_refresh': lambda ctx: calls.append(ctx)},
                                  record=record) == 'done'
    assert calls == [None]
    assert receipts == []


def test_success_filters_only_absent_summary_fields(recorder):
    record, receipts = recorder
    execution.run_recorded('test', None, 'auto',
                           bodies={'test': lambda ctx: ('saved', {'saved': 0, 'errors': [], 'extra': None})},
                           record=record)
    assert receipts[0].status == 'ok'
    assert receipts[0].summary == {'saved': 0, 'errors': []}
