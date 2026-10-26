import pytest

from routers._airs_monthly_positions import position_performance


def holding(value, quantity=10, name='Stock', isin='TEST'):
    return {'name': name, 'isin': isin, 'currency': 'EUR', 'value_eur': value, 'quantity': quantity}


def sections(before, after, activity=None, **extra):
    return {
        'opening': {'status': 'ok', 'data': {'as_of': '2026-09-30', 'rows': before}},
        'closing': {'status': 'ok', 'data': {'as_of': '2026-10-23', 'rows': after}},
        'transactions': {'status': 'ok', 'data': {'readable': True, 'activity': activity or [], **extra}},
    }


def result(data):
    return position_performance(data, '2026-10-01', '2026-10-23')[0]


def test_reports_monthly_unit_price_change_not_ytd_or_portfolio_capital_ratio():
    row = result(sections([holding(1000)], [holding(1100)]))
    assert row['return_pct'] == pytest.approx(10)
    assert row['start_date'] == '2026-09-30'
    assert row['end_date'] == '2026-10-23'


def test_purchase_does_not_turn_more_shares_into_a_gain():
    buy = {'name': 'Stock', 'kind': 'buy', 'date': '2026-10-10', 'quantity': 10, 'amount_eur': 1050}
    row = result(sections([holding(1000, 10)], [holding(2200, 20)], [buy]))
    assert row['return_pct'] == pytest.approx(10)  # not 120% value growth


def test_fractional_units_are_not_rounded_into_a_false_return():
    row = result(sections([holding(100, .5)], [holding(110, .5)]))
    assert row['return_pct'] == pytest.approx(10)


def test_zero_movement_and_a_known_zero_closing_value_are_real_results():
    assert result(sections([holding(1000)], [holding(1000)]))['return_pct'] == 0
    assert result(sections([holding(1000)], [holding(0)]))['return_pct'] == -100


def test_new_and_sold_positions_use_the_actual_execution_boundary():
    buy = {'name': 'Stock', 'kind': 'buy', 'date': '2026-10-10', 'quantity': 10, 'amount_eur': 1000}
    row = result(sections([], [holding(1100)], [buy]))
    assert row['return_pct'] == pytest.approx(10)
    assert row['start_date'] == '2026-10-10'
    assert row['status'] == 'bought'
    sell = {**buy, 'kind': 'sell', 'amount_eur': 950}
    row = result(sections([holding(1000)], [], [sell]))
    assert row['return_pct'] == pytest.approx(-5)
    assert row['end_date'] == '2026-10-10'
    assert row['status'] == 'sold'


def test_split_or_unknown_transfer_never_produces_a_false_loss():
    row = result(sections([holding(1000, 10)], [holding(1000, 100)]))
    assert row['return_pct'] is None
    assert row['reason'] == 'quantity_not_reconciled'
    row = result(sections([holding(1000)], [holding(1100)], unclassified_names=['Stock']))
    assert row['return_pct'] is None


def test_missing_reports_are_unknown_even_when_two_valuations_look_comparable():
    data = sections([holding(1000)], [holding(1100)])
    data['transactions'] = {'status': 'error', 'data': None}
    assert result(data)['return_pct'] is None
    data = sections([holding(1000)], [holding(1100)])
    data['opening'] = {'status': 'error', 'data': None}
    assert result(data)['return_pct'] is None


def test_sale_and_repurchase_do_not_claim_the_unheld_interval_as_a_return():
    trades = [
        {'name': 'Stock', 'kind': 'sell', 'date': '2026-10-05', 'quantity': 10, 'amount_eur': 1100},
        {'name': 'Stock', 'kind': 'buy', 'date': '2026-10-20', 'quantity': 10, 'amount_eur': 1050},
    ]
    row = result(sections([holding(1000)], [holding(1200)], trades))
    assert row['return_pct'] is None
    assert row['reason'] == 'holding_period_discontinuous'
