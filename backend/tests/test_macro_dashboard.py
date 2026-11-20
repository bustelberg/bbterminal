from datetime import date

import pytest

from routers import macro_dashboard as macro


def test_scheduled_refresh_keeps_working_after_one_source_fails(monkeypatch):
    from dashboard_daily import IncompleteRefresh
    from scheduler_jobs import macro as job

    attempted = []
    def refresh(key):
        attempted.append(key)
        if key == 'oecd-cli':
            raise ValueError('empty source')
        return {'points': [{'date': '2026-11-19'}], 'fetched_at': '2026-11-20T08:00:00Z'}

    monkeypatch.setattr(macro, 'refresh_snapshot', refresh)
    with pytest.raises(IncompleteRefresh) as error:
        job.run(report=lambda *_: None)
    assert attempted == list(macro.SERIES)
    assert set(error.value.summary['saved']) == set(macro.SERIES) - {'oecd-cli'}
    assert 'empty source' in error.value.summary['errors']['oecd-cli']


def test_core_cpi_uses_same_month_prior_year_across_full_history(monkeypatch):
    monkeypatch.setattr(macro, 'fred_csv', lambda *args: {
        date(2024, 1, 1): 100, date(2025, 1, 1): 100, date(2025, 3, 1): 104,
        date(2026, 1, 1): 103, date(2026, 2, 1): 120, date(2026, 3, 1): 109.2,
    })
    result = macro.fetch_points('core-cpi', date(2026, 4, 1))
    assert [point['date'] for point in result] == ['2025-01-01', '2026-01-01', '2026-03-01']
    assert [point['value'] for point in result] == pytest.approx([0, 3, 5])


def test_buffett_matches_quarters_and_converts_gdp_to_millions(monkeypatch):
    quarter = date(2026, 1, 1)
    monkeypatch.setattr(macro, 'fred_csv', lambda series, *_: (
        {quarter: 30_000} if series == 'GDP' else {quarter: 75_000_000, date(2026, 4, 1): 80_000_000}))
    assert macro.fetch_points('buffett', date(2026, 6, 1)) == [{'date': '2026-01-01', 'value': 2.5}]


def test_source_csv_supports_oecd_months_and_skips_missing_values():
    source = 'TIME_PERIOD,OBS_VALUE\n2025-12,98\n2026-01,100.1\n2026-02,.\n2026-03,NaN\n'
    assert macro.csv_points(source, ('TIME_PERIOD',), 'OBS_VALUE', date(2026, 3, 31)) == {
        date(2025, 12, 1): 98,
        date(2026, 1, 1): 100.1,
    }
