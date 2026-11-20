"""A completed Yahoo request must not disguise an old or only partly repaired series."""
from contextlib import contextmanager

import pytest

from asset_pipeline import price_refresh, store
from tests._fake_supabase import FakeSupabase


def test_price_slice_receipt_identifies_failed_instruments(monkeypatch):
    from asset_pipeline import queue
    from ingest.phases import prices

    monkeypatch.setattr(prices, 'supabase', FakeSupabase({'company': [{'company_id': 1, 'isin': 'TEST'}]}))
    monkeypatch.setattr(prices, '_update_run', lambda *_a, **_kw: None)
    monkeypatch.setattr(prices, 'log_step', lambda *_a, **_kw: None)
    monkeypatch.setattr(queue, 'enqueue', lambda *_a, **_kw: None)
    monkeypatch.setattr(queue, 'process_slice', lambda **_kw: {})
    monkeypatch.setattr(price_refresh, 'backfill_missing', lambda *_: {})
    monkeypatch.setattr(price_refresh, 'refresh_stale', lambda **_kw: {
        'failed': 1, 'results': [{'isin': 'TEST', 'symbol': 'FUND', 'status': 'error', 'error': 'rate limited'}],
    })
    errors = []
    prices._run_prices_phase(1, errors, companies_override=[{'cid': 1}])
    assert len(errors) == 1
    assert errors[0].startswith('Yahoo price refresh had 1 failed instrument(s).\nFUND / TEST: rate limited')
    assert 'provider_latest=unconfirmed' in errors[0]
    assert 'Next: retry daily_price_slice' in errors[0]
    assert 'Saved prices are retained' in errors[0]


@pytest.mark.parametrize("last,expected_moved,expected_remaining", [
    ("2026-08-21", 0, 1), ("2026-10-29", 1, 1), ("2026-10-30", 1, 0),
])
def test_refresh_receipt_records_symbols_dates_and_remaining_gap(monkeypatch, last, expected_moved, expected_remaining):
    monkeypatch.setattr(price_refresh, '_unavailable_close', lambda *a: {'status': 'unavailable'})
    monkeypatch.setattr(price_refresh, "find_stale", lambda *a, **kw: ([{
        "isin": "TEST", "analysis_id": 1, "yahoo_symbol": "FUND", "last_close": "2026-08-21",
    }], "2026-10-30", 1))
    monkeypatch.setattr(price_refresh, "supabase", FakeSupabase({"asset_price": [
        {"analysis_id": 1, "target_date": last, "close": 100},
        {"analysis_id": 1, "target_date": "2026-11-02", "close": None},
    ]}))
    monkeypatch.setattr(store, "extend_series", lambda *a: 0)
    result = price_refresh.refresh_stale(stale_days=1, sleep_s=0)
    assert result["moved"] == expected_moved
    assert result["remaining_stale"] == expected_remaining
    row = result["results"][0]
    assert (row["symbol"], row["isin"], row["latest_close"]) == ("FUND", "TEST", last)
    assert row["expected_close"] == "2026-10-30"


def test_limited_refresh_counts_unattempted_instruments_as_remaining(monkeypatch):
    monkeypatch.setattr(price_refresh, '_unavailable_close', lambda *a: {'status': 'unavailable'})
    monkeypatch.setattr(price_refresh, "find_stale", lambda *a, **kw: ([{
        "isin": str(i), "analysis_id": i, "yahoo_symbol": str(i), "last_close": "2026-08-21",
    } for i in range(3)], "2026-10-30", 3))
    monkeypatch.setattr(price_refresh, "supabase", FakeSupabase({"asset_price": []}))
    monkeypatch.setattr(store, "extend_series", lambda *a: 0)
    result = price_refresh.refresh_stale(limit=1, stale_days=1, sleep_s=0)
    assert result["remaining_stale"] == 3
    assert result["skipped"] == 2
    assert len(result["results"]) == 1


@pytest.mark.parametrize("strict", [False, True])
def test_scheduler_keeps_unrepaired_price_receipt_without_crashing_startup(monkeypatch, caplog, strict):
    import scheduler
    from asset_pipeline import queue
    from dashboard_daily import IncompleteRefresh
    from job_runlog import RunRecord
    from routers import _benchmark_etf
    rec = RunRecord("asset_price_refresh", None)
    @contextmanager
    def recording(*a, **kw):
        yield rec
    monkeypatch.setattr(scheduler, "record_run", recording)
    monkeypatch.setattr(scheduler, "_await_db_ready", lambda *a: True)
    monkeypatch.setattr(queue, "is_worker_active", lambda: False)
    monkeypatch.setattr(price_refresh, "watched_isins", lambda: {"TEST"})
    monkeypatch.setattr(price_refresh, "find_stale", lambda **kw: ([{"last_close": "2026-08-21"}], "2026-10-30", 1))
    monkeypatch.setattr(price_refresh, "backfill_missing", lambda *a: {"backfilled": 0})
    monkeypatch.setattr(price_refresh, "backfill_nav_fallbacks", lambda *a: {"stored": 0})
    monkeypatch.setattr(_benchmark_etf, "refresh_index_proxies", lambda **kw: 1)
    receipt = {"moved": 0, "unchanged": 1, "failed": 0, "remaining_stale": 1, "results": [{
        "symbol": "FUND", "isin": "TEST", "latest_close": "2026-08-21",
        "expected_close": "2026-10-30", "still_stale": True, "reason": "No newer close",
    }]}
    monkeypatch.setattr(price_refresh, "refresh_stale", lambda **kw: receipt)
    if strict:
        with pytest.raises(IncompleteRefresh, match="1 still stale"):
            scheduler._run_asset_price_refresh("daily", raise_on_failure=True)
    else:
        scheduler._run_asset_price_refresh("startup catch-up")
    assert rec.status == "error"
    assert rec.summary == receipt
    assert "FUND (TEST) still dated 2026-08-21" in caplog.text


def test_nav_failure_keeps_the_exact_reviewed_source_and_exception(monkeypatch):
    def unavailable(issue_id):
        raise RuntimeError('provider returned HTTP 503')
    monkeypatch.setattr(price_refresh, '_iex_nav_rows', unavailable)
    result = price_refresh.backfill_nav_fallbacks({'XS1002121454'})
    assert result['failed'] == 1
    assert result['stored'] == 0
    assert result['results'] == [{'isin': 'XS1002121454', 'provider': 'IEX', 'issue_id': '600015811',
                                  'status': 'error', 'error': 'RuntimeError: provider returned HTTP 503'}]


@pytest.mark.parametrize('isin,symbol,last', [
    ('DE0005190037', 'BMW3.DE', '2026-06-30'),
    ('US2855121099', 'EA', '2026-08-10'),
    ('US0534841012', 'AVB', '2026-08-24'),
    ('US49177J1025', 'KVUE', '2026-10-30'),
])
def test_reviewed_ended_listing_keeps_dated_history_without_requesting_impossible_close(monkeypatch, isin, symbol, last):
    from unittest.mock import Mock
    monkeypatch.setattr(price_refresh, 'find_stale', lambda *a, **kw: ([{
        'isin': isin, 'analysis_id': 1, 'yahoo_symbol': symbol, 'last_close': last,
    }], '2026-11-19', 1))
    fetch = Mock(side_effect=AssertionError('Ended listings must not be fetched'))
    monkeypatch.setattr(store, 'extend_series', fetch)
    result = price_refresh.refresh_stale(sleep_s=0)
    assert result['failed'] == 0
    assert result['remaining_stale'] == 1
    row = result['results'][0]
    assert row['status'] == 'unavailable' and row['listing_ended']
    assert row['latest_close'] == last
    assert 'vendor_latest_close' not in row
    fetch.assert_not_called()


def test_listing_end_requires_exact_identity_and_does_not_hide_missing_history():
    from asset_pipeline.listing_lifecycle import ended_listing
    assert ended_listing('WRONG', 'KVUE', '2026-11-19') is None
    assert ended_listing('US49177J1025', 'WRONG', '2026-11-19') is None
    assert ended_listing('US49177J1025', 'KVUE', '2026-10-30') is None


@pytest.mark.parametrize('failure', ['nav', 'backfill', 'proxy'])
def test_source_failure_does_not_prevent_independent_holdings_refresh(monkeypatch, failure):
    import scheduler
    from asset_pipeline import queue
    from dashboard_daily import IncompleteRefresh
    from job_runlog import RunRecord
    from routers import _benchmark_etf
    from unittest.mock import Mock

    rec = RunRecord('asset_price_refresh', None)
    @contextmanager
    def recording(*a, **kw):
        yield rec
    monkeypatch.setattr(scheduler, 'record_run', recording)
    monkeypatch.setattr(scheduler, '_await_db_ready', lambda *a: True)
    monkeypatch.setattr(queue, 'is_worker_active', lambda: False)
    monkeypatch.setattr(price_refresh, 'watched_isins', lambda: {'TEST'})
    monkeypatch.setattr(price_refresh, 'find_stale', lambda **kw: (
        [{'last_close': '2026-11-17'}], '2026-11-18', 1))
    monkeypatch.setattr(price_refresh, 'backfill_missing', lambda *a: {
        'backfilled': 0, 'failed': int(failure == 'backfill')})
    nav = {'stored': 0, 'failed': int(failure == 'nav')}
    monkeypatch.setattr(price_refresh, 'backfill_nav_fallbacks', lambda *a: nav)
    monkeypatch.setattr(_benchmark_etf, 'refresh_index_proxies', lambda **kw: int(failure != 'proxy'))
    refresh = Mock(return_value={'moved': 1, 'unchanged': 0, 'failed': 0,
                                'remaining_stale': 0, 'results': [{'symbol': 'TEST', 'status': 'moved'}]})
    monkeypatch.setattr(price_refresh, 'refresh_stale', refresh)
    with pytest.raises(IncompleteRefresh, match='Price source preparation incomplete') as caught:
        scheduler._run_asset_price_refresh('daily', raise_on_failure=True)
    refresh.assert_called_once_with(isins={'TEST'}, stale_days=1)
    assert caught.value.summary['moved'] == 1
    assert caught.value.summary['nav_fallback'] == nav
    assert rec.status == 'error'
    assert rec.summary == caught.value.summary
