from datetime import date
from unittest.mock import Mock

import pytest

from leonteq import benchmark_performance as B
from tests._fake_supabase import FakeSupabase


def series():
    return {'ticker': 'ACWI', 'points': [
        {'date': '2025-12-31', 'price_date': '2025-12-31', 'cum_pct': 0},
        {'date': '2026-01-31', 'price_date': '2026-01-30', 'cum_pct': 10},
        {'date': '2026-02-28', 'price_date': '2026-02-27', 'cum_pct': 21}]}


def source():
    return {'retrieved_at': '2026-03-15T12:00:00Z', 'months': [{'month': '2026-01', 'return_pct': 5}, {'month': '2026-02', 'return_pct': -3},
                       {'month': '2026-03', 'return_pct': 2}]}


def test_monthly_returns_unchain_cumulative_benchmark_and_keep_missing_values():
    result = B.monthly_rows(source(), 2026, series(), date(2026, 3, 15))
    assert len(result) == 12
    assert result[0]['benchmark_pct'] == pytest.approx(10)
    assert result[1]['benchmark_pct'] == pytest.approx(10)
    assert result[1]['difference_pp'] == pytest.approx(-13)
    assert result[2]['leonteq_pct'] == 2
    assert result[2]['benchmark_pct'] is None
    assert result[2]['completed'] is False
    assert result[2]['mtd'] is True
    assert result[3]['leonteq_pct'] is None


@pytest.mark.parametrize('problem', ['missing_open', 'stale_end', 'missing_fx', 'invalid_price'])
def test_missing_or_stale_benchmark_boundaries_never_become_zero_returns(problem):
    data = series()
    if problem == 'missing_open':
        data['points'] = data['points'][1:]
    elif problem == 'stale_end':
        data['points'][1]['price_date'] = '2026-01-15'
    elif problem == 'missing_fx':
        data['points'] = data['points'][:1]
    else:
        data['points'][1]['cum_pct'] = float('nan')
    row = B.monthly_rows(source(), 2026, data, date(2026, 3, 15))[0]
    assert row['leonteq_pct'] == 5
    assert row['benchmark_pct'] is None
    assert row['difference_pp'] is None


def test_reads_only_the_saved_portfolio_and_existing_eur_benchmark_series(monkeypatch):
    from routers import _benchmark_etf
    db = FakeSupabase({B.TABLE: [{'account_key': 'account', 'portfolio_name': 'Fund', 'payload': source()}]})
    monkeypatch.setattr(B, 'supabase', db)
    monkeypatch.setattr(B, '_account_key', lambda: 'account')
    benchmark = Mock(return_value=series())
    monkeypatch.setattr(_benchmark_etf, 'etf_return_series', benchmark)
    result = B.comparison('Fund', 2026)
    assert result['years'] == [2026]
    assert result['months'][0]['leonteq_pct'] == 5
    assert result['currency'] == 'EUR'
    assert benchmark.call_args.args[:2] == ('ACWI', '2025-12-31')
    assert benchmark.call_args.kwargs == {'read_only': True}
    assert db.writes == []


def test_missing_saved_leonteq_history_is_reported_without_scraping(monkeypatch):
    from routers import _benchmark_etf
    monkeypatch.setattr(B, 'supabase', FakeSupabase({}))
    monkeypatch.setattr(B, '_account_key', lambda: 'account')
    monkeypatch.setattr(_benchmark_etf, 'etf_return_series', lambda *args, **kwargs: {})
    result = B.comparison('Fund', 2025)
    assert result['saved_returns'] is False
    assert result['benchmark_available'] is False
    assert all(row['leonteq_pct'] is None for row in result['months'])


def test_saved_mtd_never_turns_into_a_full_month_return_as_the_calendar_advances():
    saved = {**source(), 'retrieved_at': '2026-02-15T12:00:00Z'}
    rows = B.monthly_rows(saved, 2026, series(), date(2026, 3, 15))
    assert rows[0]['leonteq_pct'] == 5
    assert rows[1]['leonteq_pct'] is None
    assert rows[1]['benchmark_pct'] == pytest.approx(10)
    assert rows[1]['difference_pp'] is None


def test_november_mtd_uses_latest_stored_close_and_exposes_both_fetch_dates(monkeypatch):
    from routers import _benchmark_etf
    from datetime import datetime, timezone
    class Clock:
        @staticmethod
        def now(_zone):
            return datetime(2026, 11, 11, 12, tzinfo=timezone.utc)
    saved = {'retrieved_at': '2026-11-05T10:00:00Z', 'months': [{'month': '2026-11', 'return_pct': 3.5}]}
    db = FakeSupabase({B.TABLE: [{'account_key': 'account', 'portfolio_name': 'Fund', 'payload': saved}]})
    monkeypatch.setattr(B, 'supabase', db)
    monkeypatch.setattr(B, '_account_key', lambda: 'account')
    # Preserve real datetime parsing in monthly_rows while fixing comparison's clock.
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, zone):
            return Clock.now(zone)
    monkeypatch.setattr(B, 'datetime', FrozenDateTime)
    benchmark = Mock(return_value={'ticker': 'ACWI', 'fetched_at': '2026-11-11T10:00:00Z', 'points': [
        {'date': '2026-10-31', 'price_date': '2026-10-30', 'cum_pct': 10},
        {'date': '2026-11-11', 'price_date': '2026-11-10', 'cum_pct': 12.2}]})
    monkeypatch.setattr(_benchmark_etf, 'etf_return_series', benchmark)
    result = B.comparison('Fund', 2026)
    november = result['months'][10]
    assert november['mtd'] is True
    assert november['leonteq_pct'] == 3.5
    assert november['benchmark_pct'] == pytest.approx(2)
    assert november['benchmark_price_date'] == '2026-11-10'
    assert result['months'][11]['benchmark_pct'] is None
    assert benchmark.call_args.args[2][-1] == '2026-11-11'
    assert result['benchmark_fetched_at'] == '2026-11-11T10:00:00Z'
    assert result['retrieved_at'] == saved['retrieved_at']


def test_manual_benchmark_refresh_bypasses_an_earlier_same_day_check(monkeypatch):
    import asyncio
    from routers import benchmarks, _benchmark_etf
    refresh = Mock(return_value=4)
    ensure = Mock(return_value=(1, ('2026-11-10', 100)))
    monkeypatch.setattr(_benchmark_etf, 'refresh_index_proxies', refresh)
    monkeypatch.setattr(_benchmark_etf, 'ensure_fresh', ensure)
    async def in_thread(function, *args, **kwargs):
        return function(*args, **kwargs)
    monkeypatch.setattr(benchmarks.asyncio, 'to_thread', in_thread)
    result = asyncio.run(benchmarks.refresh_index_proxy('ACWI'))
    ensure.assert_called_once_with('ACWI', force=True, bypass_daily_guard=True, strict=True)
    assert result['as_of'] == '2026-11-10'
