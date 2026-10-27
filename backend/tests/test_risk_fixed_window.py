from datetime import date, timedelta
import pytest
from routers import _tracking_error as T
from routers import _risk_history_window as W
from routers._asset_financials import _BENCHMARK_RISK_ETF


class Today(date):
    @classmethod
    def today(cls):
        return cls(2026, 10, 27)


def prices():
    day, value, rows = date(2020, 12, 30), 100, []
    while day <= date(2026, 10, 28):
        if day.weekday() < 5:
            rows.append((day.isoformat(), value))
            value *= 1.001
        day += timedelta(days=1)
    return rows


@pytest.mark.parametrize('frequency', ['daily', 'weekly', 'monthly'])
def test_fixed_window_does_not_shrink_to_a_new_holdings_listing(monkeypatch, frequency):
    monkeypatch.setattr(T, 'date', Today)
    monkeypatch.setattr(W, 'date', Today)
    series = prices()
    built = T.build_paired_series([
        {'isin': 'OLD', 'name': 'Old stock', 'bucket': 'Equity', 'weight_pct': 40},
        {'isin': 'NEW', 'name': 'New stock', 'bucket': 'Equity', 'weight_pct': 10},
    ], 'ACWI', frequency, 1,
        preloaded={'OLD': series, 'NEW': [row for row in series if row[0] >= '2023-01-01'],
                   _BENCHMARK_RISK_ETF['ACWI']: series},
        benchmark_weights={'Stocks': 60, 'Bonds': 40, 'EquityOnly': 1},
        require_complete_holdings=True, start_date='2021-01-01')
    assert built['requested_start'] == '2021-01-01'
    assert built['excluded_holdings'] == ['New stock']
    assert built['priced'] == 1
    assert built['weight'] == {'OLD': .4}
    assert min(built['obs_dates']) < '2021-02-01'
    assert max(built['obs_dates']) == '2026-10-27'


def test_history_marks_include_the_opening_anchor_and_latest_close():
    marks = W.history_marks('2021-01-01', '2026-10-26')
    assert marks[0] == '2020-12-31'
    assert marks[-2:] == ['2026-09-30', '2026-10-26']
    assert W.history_years('2021-01-01', 1, date(2026, 10, 27)) == 7
