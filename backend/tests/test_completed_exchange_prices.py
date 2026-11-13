from datetime import datetime
from io import BytesIO
from unittest.mock import Mock

import pytest

from asset_pipeline import price_refresh, store, yahoo, parquet
from tests._fake_supabase import FakeSupabase


def stamp(value):
    return int(datetime.fromisoformat(value).timestamp())


def asx_window():
    return {'meta': {'symbol': 'PME.AX', 'exchangeTimezoneName': 'Australia/Sydney',
                     'currentTradingPeriod': {'regular': {'end': stamp('2026-11-12T05:00:00+00:00')}}},
            'timestamp': [stamp('2026-11-10T23:00:00+00:00'), stamp('2026-11-11T23:00:00+00:00')],
            'indicators': {'quote': [{'close': [140, 145], 'volume': [100, 200]}]}}


@pytest.mark.parametrize('mode', ['full', 'incremental'])
@pytest.mark.parametrize('hour,latest', [(2, '2026-11-11'), (8, '2026-11-12')])
def test_asx_completed_close_survives_storage_but_forming_and_future_bars_do_not(monkeypatch, mode, hour, latest):
    now = stamp(f'2026-11-12T{hour:02}:00:00+00:00')
    monkeypatch.setattr(yahoo.time, 'time', lambda: now)
    window = asx_window()
    db = FakeSupabase({'asset_price': [
        {'analysis_id': 8542, 'target_date': '2026-11-11', 'close': 140},
        {'analysis_id': 8542, 'target_date': '2026-11-12', 'close': 666},  # previous partial
        {'analysis_id': 8542, 'target_date': '2026-11-13', 'close': 777},
        {'analysis_id': 99, 'target_date': '2026-11-12', 'close': 999},
    ], 'asset_analysis': [{'analysis_id': 8542}]})
    monkeypatch.setattr(store, 'supabase', db)
    monkeypatch.setattr(yahoo, 'chart_window', lambda *_, **__: window)
    monkeypatch.setattr(parquet, 'build_frame', lambda *_, **__: None)
    monkeypatch.setattr(parquet, 'write', lambda *_, **__: None)
    monkeypatch.setattr('common.pg._run_copy', lambda *_, **__: BytesIO(f'2026-11-11,{latest},2,2026-11-11,{latest},0'.encode()))
    if mode == 'full':
        store.store_series(8542, 'PME.AX', window['timestamp'][0])
    else:
        store.extend_series(8542, 'PME.AX', '2026-11-11')
    rows = [row for row in db.tables['asset_price'] if row['analysis_id'] == 8542]
    assert max(row['target_date'] for row in rows) == latest
    assert next(row for row in rows if row['target_date'] == latest)['close'] == (145 if hour == 8 else 140)
    assert not any(row['target_date'] == '2026-11-13' for row in rows)
    assert db.tables['asset_analysis'][0]['price_to'] == latest
    assert any(row['analysis_id'] == 99 and row['close'] == 999 for row in db.tables['asset_price'])
    if hour == 8:
        with pytest.raises(price_refresh.NewerCloseAvailable):
            price_refresh._unavailable_close('PME.AX', '2026-11-11', '2026-11-12')
    else:
        assert price_refresh._unavailable_close('PME.AX', '2026-11-11', '2026-11-12')['vendor_latest_close'] == '2026-11-11'


def test_current_session_requires_a_matching_confirmed_end():
    window = asx_window()
    bar = window['timestamp'][-1]
    now = stamp('2026-11-12T08:00:00+00:00')
    assert yahoo.is_completed_daily_bar(bar, window['meta'], now=now)
    assert not yahoo.is_completed_daily_bar(bar, {'exchangeTimezoneName': 'Australia/Sydney'}, now=now)
    tomorrow = {**window['meta'], 'currentTradingPeriod': {'regular': {'end': stamp('2026-11-13T05:00:00+00:00')}}}
    assert not yahoo.is_completed_daily_bar(bar, tomorrow, now=now)
    assert not yahoo.is_completed_daily_bar(now + 86400, window['meta'], now=now)


def test_refresh_recovery_saves_the_exact_available_asx_close_and_reports_success(monkeypatch):
    now = stamp('2026-11-12T08:00:00+00:00')
    monkeypatch.setattr(yahoo.time, 'time', lambda: now)
    db = FakeSupabase({'asset_price': [{'analysis_id': 8542, 'target_date': '2026-11-11', 'close': 140}],
                       'asset_analysis': [{'analysis_id': 8542}]})
    monkeypatch.setattr(store, 'supabase', db)
    monkeypatch.setattr(price_refresh, 'supabase', db)
    monkeypatch.setattr(price_refresh, 'find_stale', lambda *_, **__: ([{
        'analysis_id': 8542, 'isin': 'AU000000PME8', 'yahoo_symbol': 'PME.AX', 'last_close': '2026-11-11',
    }], '2026-11-12', 1))
    fetch = Mock(side_effect=[None, asx_window()])
    monkeypatch.setattr(yahoo, 'chart_window', fetch)
    monkeypatch.setattr('common.pg._run_copy', lambda *_, **__: BytesIO(b'2026-11-11,2026-11-12,2,2026-11-11,2026-11-12,0'))
    result = price_refresh.refresh_stale(stale_days=1, sleep_s=0)
    assert fetch.call_count == 2  # availability response is reused for recovery
    assert result['failed'] == 0
    assert result['moved'] == 1
    assert result['remaining_stale'] == 0
    assert result['results'][0]['latest_close'] == '2026-11-12'
    assert result['results'][0]['vendor_latest_close'] == '2026-11-12'


@pytest.mark.parametrize('hour,expected', [(2, ['2026-11-11']), (8, ['2026-11-11', '2026-11-12'])])
def test_archive_uses_the_same_completed_sessions_and_exchange_dates(monkeypatch, hour, expected):
    now = stamp(f'2026-11-12T{hour:02}:00:00+00:00')
    monkeypatch.setattr(yahoo.time, 'time', lambda: now)
    frame = parquet.build_frame(asx_window())
    assert [day.isoformat() for day in frame.date] == expected
