"""No network/database: validate return basis, FX, windows and publication safety."""
from datetime import date, datetime, timedelta, timezone
from math import sqrt
from statistics import stdev
from types import SimpleNamespace

import pytest

from services import benchmark_factsheet as F


def series(start, end, growth=1.0001):
    result = {}
    value = 100
    day = start
    while day <= end:
        if day.weekday() < 5:
            result[day.isoformat()] = value
            value *= growth
        day += timedelta(days=1)
    return result


def test_cagr_total_return_and_sample_daily_volatility_are_distinct():
    prices = {'2026-11-09': 100, '2026-11-10': 110, '2026-11-11': 99}
    result = F.statistics(prices, min(prices), max(prices))
    assert result['total_return_pct'] == pytest.approx(-1)
    assert result['cagr_pct'] == pytest.approx((.99 ** (365.2425 / 2) - 1) * 100)
    assert result['volatility_pct'] == pytest.approx(stdev([.1, -.1]) * sqrt(252) * 100)
    assert result['observations'] == 2


def test_short_history_is_not_presented_as_fifteen_years():
    end = date(2026, 11, 12)
    stock = series(date(2011, 10, 1), end)
    bonds = series(date(2017, 11, 22), end)
    rows = F.calculate({'ACWI': stock, 'AGG': bonds})
    assert rows[0]['period']['start'] == '2011-11-11'  # Anniversary is a Saturday.
    assert rows[0]['period']['end'] == '2026-11-12'
    assert rows[1]['period'] is None
    assert 'Fifteen years' in rows[1]['reason']
    assert rows[1]['available_history']['start'] == '2017-11-22'
    assert rows[1]['available_history']['years'] < 15


def test_both_series_use_shared_end_and_shared_open_when_full_history_exists():
    stock = series(date(2011, 10, 1), date(2026, 11, 12))
    bonds = series(date(2011, 10, 1), date(2026, 11, 13))
    rows = F.calculate({'ACWI': stock, 'AGG': bonds})
    assert {row['period']['end'] for row in rows} == {'2026-11-12'}
    assert {row['period']['start'] for row in rows} == {'2011-11-11'}


def test_prices_convert_at_each_dates_historical_fx_not_current_fx():
    prices = {'2026-11-10': 100, '2026-11-11': 125}
    fx = {'USD': {'2026-11-10': 2, '2026-11-11': 2.5}}
    assert F.eur_prices(prices, 'USD', fx) == {'2026-11-10': 50, '2026-11-11': 50}
    assert F.eur_prices(prices, 'EUR', {}) == prices


@pytest.mark.parametrize('fx', [{}, {'USD': {'2025-01-01': 1.1}}, {'USD': {'2026-11-10': 0}}])
def test_missing_or_stale_fx_is_not_assumed_to_be_parity(fx):
    with pytest.raises(ValueError, match='historical USD/EUR'):
        F.eur_prices({'2026-11-10': 100}, 'USD', fx)


def test_weekly_or_gapped_history_cannot_look_like_daily_volatility():
    prices = series(date(2011, 10, 1), date(2026, 11, 12))
    with pytest.raises(ValueError, match='gaps or too few'):
        F.statistics(dict(list(prices.items())[::5]), min(prices), max(prices))
    broken = {day: value for day, value in prices.items() if not '2020-01-01' <= day <= '2020-04-01'}
    with pytest.raises(ValueError, match='gaps'):
        F.statistics(broken, min(broken), max(broken))


def test_calendar_anniversary_handles_leap_day():
    assert F.years_before(date(2024, 2, 29)) == date(2009, 2, 28)


def test_raw_closes_are_never_substituted_for_dividend_adjusted_data(monkeypatch):
    import asset_pipeline.yahoo as Y
    monkeypatch.setattr(Y, 'is_completed_daily_bar', lambda *_args: True)
    stamp = int(datetime(2026, 11, 12, tzinfo=timezone.utc).timestamp())
    chart = {'meta': {'currency': 'USD'}, 'timestamp': [stamp],
             'indicators': {'quote': [{'close': [100]}]}}
    with pytest.raises(ValueError, match='dividend-adjusted'):
        F.adjusted_prices(chart, 'ACWI', 'USD', date(2026, 11, 13))
    chart['indicators']['adjclose'] = [{'adjclose': [95]}]
    assert F.adjusted_prices(chart, 'ACWI', 'USD', date(2026, 11, 13)) == {'2026-11-12': 95}
    chart['meta']['currency'] = 'EUR'
    with pytest.raises(ValueError, match='expected USD'):
        F.adjusted_prices(chart, 'AGG', 'USD', date(2026, 11, 13))


def test_provider_failure_does_not_overwrite_previous_factsheet(monkeypatch):
    import asset_pipeline.yahoo as Y
    import common.fx_load as FX
    monkeypatch.setattr(F, 'read', lambda: {'ready': True, 'stale': True, 'rows': [{'ticker': 'old'}]})
    monkeypatch.setattr(Y, 'chart_window', lambda *_args: None)
    monkeypatch.setattr(FX, 'load_fx_to_eur', lambda *_args: {})
    writes = []
    monkeypatch.setattr(F, 'supabase', SimpleNamespace(table=lambda *_args: writes.append('unexpected write')))
    with pytest.raises(ValueError, match='ACWI.*previous Factsheet remains saved'):
        F.prepare(force=True, today=date(2026, 11, 13))
    assert writes == []


def test_fresh_factsheet_is_reused_without_provider_requests(monkeypatch):
    import asset_pipeline.yahoo as Y
    saved = {'ready': True, 'stale': False, 'rows': []}
    monkeypatch.setattr(F, 'read', lambda: saved)
    monkeypatch.setattr(Y, 'chart_window', lambda *_args: pytest.fail('Unexpected provider request'))
    assert F.prepare() == saved


def test_read_is_database_only_and_new_store_has_no_placeholder_metrics(monkeypatch):
    import asset_pipeline.yahoo as Y
    class Query:
        def __getattr__(self, _key):
            return lambda *_args, **_kwargs: self

        def execute(self):
            return SimpleNamespace(data=[])
    monkeypatch.setattr(F, 'supabase', SimpleNamespace(table=lambda _name: Query()))
    monkeypatch.setattr(Y, 'chart_window', lambda *_args: pytest.fail('GET must not fetch'))
    assert F.read() == {'ready': False, 'rows': [], 'currency': 'EUR', 'period_years': 15}


def test_http_schema_does_not_return_thousands_of_source_prices():
    from routers._benchmark_factsheet import FactsheetResponse
    rows = F.calculate({'ACWI': series(date(2011, 10, 1), date(2026, 11, 12)),
                        'AGG': series(date(2017, 11, 22), date(2026, 11, 12))})
    response = FactsheetResponse.model_validate({'ready': True, 'currency': 'EUR', 'rows': rows})
    assert 'source_prices_eur' not in response.model_dump()['rows'][0]


def test_first_preparation_publishes_auditable_eur_metrics_after_both_sources_succeed(monkeypatch):
    import asset_pipeline.yahoo as Y
    import common.fx_load as FX
    source = {'ACWI': series(date(2011, 10, 1), date(2026, 11, 12)),
              'AGG': series(date(2011, 10, 1), date(2026, 11, 12))}
    requested_starts = []
    def chart(ticker, *_args):
        requested_starts.append(datetime.fromtimestamp(_args[0], timezone.utc).date())
        rows = source[ticker]
        return {'meta': {'currency': 'USD'},
                'timestamp': [int(datetime.combine(date.fromisoformat(day), datetime.min.time(), timezone.utc).timestamp()) for day in rows],
                'indicators': {'adjclose': [{'adjclose': list(rows.values())}]}}
    monkeypatch.setattr(Y, 'chart_window', chart)
    monkeypatch.setattr(Y, 'is_completed_daily_bar', lambda *_args: True)
    monkeypatch.setattr(FX, 'load_fx_to_eur', lambda *_args: {'USD': {day: 2 for day in source['ACWI']}})
    monkeypatch.setattr(F, 'read', lambda: {'ready': False})
    saved = []
    class Query:
        def upsert(self, value, **_kwargs):
            saved.append(value)
            return self

        def execute(self):
            return SimpleNamespace(data=[])
    monkeypatch.setattr(F, 'supabase', SimpleNamespace(table=lambda _name: Query()))
    result = F.prepare(today=date(2026, 11, 13))
    assert result['ready'] is True
    assert result['period_years'] == 15
    assert all(day.year == 2011 for day in requested_starts)
    assert len(saved) == 1
    assert result['rows'][0]['currency'] == 'EUR'
    assert result['rows'][0]['source_prices_eur']['2011-10-03'] == 50
    assert result['rows'][0]['period'] is not None
    assert result['rows'][1]['ticker'] == 'AGG'
    assert result['rows'][1]['source_prices_eur']['2011-10-03'] == 50
    assert result['rows'][1]['period'] is not None
    assert result['rows'][1]['available_history'] is None
    assert result['rows'][1]['period']['years'] > 14.99
    assert result['rows'][1]['period']['observations'] > 3000
    assert saved[0]['key'] != 'eur-total-return-10y-acwi-agg-v2'  # Do not reuse ten-year results.


def test_daily_receipt_retains_fifteen_year_statistics_without_raw_prices(monkeypatch):
    from scheduler_jobs.factsheet import run
    rows = F.calculate({'ACWI': series(date(2011, 10, 1), date(2026, 11, 12)),
                        'AGG': series(date(2011, 10, 1), date(2026, 11, 12))})
    def prepare(*, force):
        assert force is True
        return {'period_years': 15, 'prepared_at': '2026-11-13T23:45:00Z', 'rows': rows}
    monkeypatch.setattr(F, 'prepare', prepare)
    _, summary = run()
    assert summary['period_years'] == 15
    assert summary['rows'][0]['period'] == rows[0]['period']
    assert 'source_prices_eur' not in summary['rows'][0]
