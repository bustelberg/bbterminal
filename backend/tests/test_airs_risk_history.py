from datetime import date

import pytest

from routers._airs_risk_history import calculate_history


def row(day, value, opening=100, closing=100):
    return {'periode': day, 'cumulatief_rendement': value,
            'beginvermogen': opening, 'eindvermogen': closing}


def test_observed_drawdown_and_recovery_use_airs_returns_not_account_values():
    result = calculate_history([row('2026-01-31', 10), row('2026-02-28', -1, closing=10000),
                                row('2026-03-31', 12)], 'BOOK')
    assert result['max_drawdown_pct'] == pytest.approx(-10)
    assert result['current_drawdown_pct'] == 0
    assert result['peak_date'] == '2026-01-31'
    assert result['trough_date'] == '2026-02-28'
    assert result['recovery_date'] == '2026-03-31'
    assert result['worst_month_pct'] == pytest.approx(-10)
    assert result['negative_months'] == 1
    assert result['observations'] == 3  # the synthetic January reference is not an observation


def test_latest_year_only_and_missing_months_are_not_treated_as_monthly_returns():
    result = calculate_history([row('2025-12-31', 40), row('2026-03-31', 10),
                                row('2026-05-31', 5), row('2026-06-30', 6)], 'BOOK')
    assert result['year'] == 2026
    assert result['points'][0]['date'] == '2026-03-31'
    assert result['points'][0]['drawdown_pct'] == 0
    assert [m['month'] for m in result['months']] == ['2026-06']
    assert any('January is missing' in warning for warning in result['warnings'])
    assert any('gaps longer' in warning for warning in result['warnings'])


def test_partial_month_does_not_enter_worst_complete_month_statistics():
    result = calculate_history([row('2026-01-31', 1), row('2026-02-15', -20)], 'BOOK')
    assert result['max_drawdown_pct'] < -20
    assert result['worst_month_pct'] == pytest.approx(1)
    assert len(result['months']) == 1
    assert any('partial month' in warning for warning in result['warnings'])


def test_empty_invalid_and_unfunded_history_is_unknown_not_zero():
    result = calculate_history([row('2026-01-31', 0, 0, 0), row('2026-02-28', None),
                                row('2026-03-31', float('nan')), row('bad-date', 1)], 'BOOK')
    assert result['observations'] == 0
    assert result['max_drawdown_pct'] is None
    assert result['worst_month_pct'] is None
    assert result['points'] == []


def test_one_observation_does_not_claim_measured_risk():
    result = calculate_history([row('2026-01-31', -5)], 'BOOK')
    assert result['max_drawdown_pct'] is None
    assert result['current_drawdown_pct'] is None
    assert result['observations'] == 1


def test_unrecovered_loss_and_total_loss_are_supported():
    result = calculate_history([row('2026-01-31', 0), row('2026-02-28', -100)], 'BOOK')
    assert result['max_drawdown_pct'] == -100
    assert result['recovery_date'] is None
    assert result['negative_months'] == 1


def test_unsorted_rows_and_duplicate_dates_do_not_create_fake_observations():
    result = calculate_history([row('2026-02-28', 5), row('2026-01-31', 10),
                                row('2026-02-28', 6)], 'BOOK')
    assert result['observations'] == 2
    assert result['points'][-1]['cumulative_pct'] == 6
    assert result['current_drawdown_pct'] == pytest.approx(-3.636364)


def test_future_placeholders_are_not_used_as_actual_losses():
    result = calculate_history([row('2026-09-30', 5), row('2026-10-23', 6),
                                row('2026-10-31', -90), row('2027-01-31', -99)],
                               'BOOK', as_of=date(2026, 10, 27))
    assert result['year'] == 2026
    assert result['last_date'] == '2026-10-23'
    assert result['observations'] == 2
    assert result['max_drawdown_pct'] == 0
