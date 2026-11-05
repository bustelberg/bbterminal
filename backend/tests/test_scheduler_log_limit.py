import logging

import scheduler


def test_scheduler_receipt_logging_has_a_hard_cap_without_mutating_the_receipt(caplog):
    receipt = {'message': 'Dashboard incomplete', 'receipt': {'accounts': ['missing close ' * 20000]}}
    with caplog.at_level(logging.ERROR, logger='scheduler'):
        scheduler._log.error('[scheduler] Post-AIRS %s failed: %s', 'audit', receipt)
    record = caplog.records[-1]
    assert len(record.getMessage()) == scheduler._MAX_LOG_MESSAGE_CHARS
    assert record.getMessage().endswith(scheduler._LOG_TRUNCATION_SUFFIX)
    assert record.getMessage().startswith('[scheduler] Post-AIRS audit failed:')
    assert len(receipt['receipt']['accounts'][0]) > scheduler._MAX_LOG_MESSAGE_CHARS


def test_exception_text_and_traceback_share_the_same_budget(caplog):
    with caplog.at_level(logging.ERROR, logger='scheduler'):
        try:
            raise RuntimeError('unavailable ' * 20000)
        except RuntimeError:
            scheduler._log.exception('[scheduler] refresh failed')
    record = caplog.records[-1]
    assert len(record.getMessage()) == scheduler._MAX_LOG_MESSAGE_CHARS
    assert record.exc_info is None
    assert record.exc_text is None
    assert record.stack_info is None
    assert record.getMessage().endswith(scheduler._LOG_TRUNCATION_SUFFIX)


def test_short_logs_and_percent_values_remain_readable(caplog):
    with caplog.at_level(logging.ERROR, logger='scheduler'):
        scheduler._log.error('[scheduler] audit: %s', {'weight': '4.95%', 'status': 'stale'})
    assert caplog.records[-1].getMessage() == "[scheduler] audit: {'weight': '4.95%', 'status': 'stale'}"


def test_stack_info_cannot_bypass_the_cap():
    record = logging.LogRecord('scheduler', logging.ERROR, __file__, 1, 'failed', (), None)
    record.stack_info = 'stack ' * 10000
    scheduler._BoundedSchedulerLog().filter(record)
    assert len(record.getMessage()) == scheduler._MAX_LOG_MESSAGE_CHARS
    assert record.stack_info is None


def test_malformed_format_arguments_cannot_crash_the_job_or_bypass_the_cap(caplog):
    with caplog.at_level(logging.ERROR, logger='scheduler'):
        scheduler._log.error('Failed %s', 'audit', {'accounts': ['unavailable ' * 10000]})
    message = caplog.records[-1].getMessage()
    assert len(message) == scheduler._MAX_LOG_MESSAGE_CHARS
    assert 'logging format error' in message


def test_fundamentals_batches_name_their_scope_in_production_logs(caplog):
    with caplog.at_level(logging.WARNING, logger='scheduler'):
        scheduler._LogCtx(label='Dashboard holdings').emit('start', '61 companies')
        scheduler._LogCtx(label='ACWI').emit('start', '1286 companies')
    assert '[benchmark_fundamentals] Dashboard holdings · start: 61 companies' in caplog.text
    assert '[benchmark_fundamentals] ACWI · start: 1286 companies' in caplog.text
