import pytest

from routers._airs_monthly_positions import position_performance
from routers._airs_monthly_reconciliation import reconcile_positions


def reconcile(*, buy=0, close=1100, income=0, flow=0, closing_quantity=None):
    opening = [dict(name='Stock', isin='TEST', currency='EUR', value_eur=1000, quantity=10)]
    closing = [dict(name='Stock', isin='TEST', currency='EUR', value_eur=close,
                    quantity=closing_quantity if closing_quantity is not None else 10 + (10 if buy else 0))]
    activity = [dict(name='Stock', kind='buy', date='2026-10-10', quantity=10, amount_eur=buy)] if buy else []
    sections = {
        'opening': {'status': 'ok', 'data': {'as_of': '2026-09-30', 'rows': opening}},
        'closing': {'status': 'ok', 'data': {'as_of': '2026-10-23', 'rows': closing}},
        'transactions': {'status': 'ok', 'data': {'readable': True, 'activity': activity}},
    }
    pnl = close - 1000 - buy + income
    perf = dict(beginvermogen=1000, eindvermogen=close, rendement=pnl / 10,
                beleggingsresultaat=pnl, opbrengsten=income, kosten=0,
                mutatie_opgelopen_rente=0, stortingen=flow, onttrekkingen=0)
    positions = position_performance(sections, '2026-10-01', '2026-10-23')
    return positions, reconcile_positions(sections, perf, positions), sections, perf


def test_untraded_weight_times_return_equals_contribution():
    positions, check, *_ = reconcile()
    row = positions[0]
    assert row['opening_weight_pct'] == 100
    assert row['return_pct'] == 10
    assert row['contribution_pp'] == 10
    assert check['status'] == 'reconciled'
    assert check['difference_pp'] == pytest.approx(0)


def test_trades_are_cash_adjusted_not_opening_weight_times_price_return():
    positions, check, *_ = reconcile(buy=1050, close=2200)
    assert positions[0]['return_pct'] == 10
    assert positions[0]['contribution_pp'] == 15
    calculation = positions[0]['contribution_calculation']
    assert calculation['opening_value_eur'] == 1000
    assert calculation['closing_value_eur'] == 2200
    assert calculation['purchases_eur'] == 1050
    assert calculation['sales_eur'] == 0
    assert calculation['result_eur'] == 150
    assert calculation['portfolio_opening_capital_eur'] == 1000
    assert calculation['opening_date'] == '2026-09-30'
    assert calculation['trades'] == [dict(date='2026-10-10', kind='buy', amount_eur=1050)]
    assert check['status'] == 'reconciled'


def test_income_is_a_reported_component_not_assigned_by_weight():
    positions, check, *_ = reconcile(income=20)
    assert positions[0]['contribution_pp'] == 10
    assert check['adjustments'][0] == dict(kind='opbrengsten', contribution_pp=2)
    assert check['contribution_sum_pp'] == 12
    assert check['status'] == 'reconciled'


def test_missing_trades_and_external_flows_never_claim_a_reconciliation():
    positions, check, *_ = reconcile(closing_quantity=20)
    assert positions[0]['contribution_pp'] is None
    assert check['status'] == 'not_reconciled'
    assert 'position_results_incomplete' in check['issues']
    positions, check, *_ = reconcile(flow=100)
    assert check['status'] == 'unavailable'
    assert check['contribution_sum_pp'] is None
    assert positions[0]['contribution_pp'] is None


def test_mismatched_opening_and_nonzero_cost_sign_do_not_get_balanced_away():
    positions, _, sections, perf = reconcile()
    perf['beginvermogen'] = 998
    perf['kosten'] = 1
    check = reconcile_positions(sections, perf, positions)
    assert check['status'] == 'not_reconciled'
    assert 'opening_capital_mismatch' in check['issues']
    assert 'cost_treatment_unverified' in check['issues']
    assert all(a['kind'] != 'balancing' for a in check['adjustments'])


def test_cash_trading_movements_are_not_counted_twice():
    positions, _, sections, perf = reconcile()
    cash = dict(name='Effectenrekening', isin=None, currency='EUR', quantity=100, value_eur=100)
    sections['opening']['data']['rows'].append(cash)
    sections['closing']['data']['rows'].append(cash.copy())
    perf.update(beginvermogen=1100, eindvermogen=1200, rendement=100 / 1100 * 100)
    positions = position_performance(sections, '2026-10-01', '2026-10-23')
    check = reconcile_positions(sections, perf, positions)
    row = next(r for r in positions if r['name'] == 'Effectenrekening')
    assert row['contribution_pp'] == 0
    assert row['return_pct'] is None
    assert check['status'] == 'reconciled'
