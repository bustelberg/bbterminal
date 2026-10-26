import pytest

from routers import _airs_portfolio_analysis as analysis


@pytest.fixture
def run(monkeypatch):
    monkeypatch.setattr(analysis, '_holding_risk', lambda *_: {})
    monkeypatch.setattr(analysis, '_child_book_ledgers', lambda *_: {})
    monkeypatch.setattr(analysis, '_via_capital', lambda *_: {})
    return analysis._with_results


def book():
    return {'available': True, 'basis_eur': 1000, 'positions': [
        {'name': 'Certificate', 'held': True, 'realised_result_eur': 4, 'income_eur': 2}]}


def holdings(cash_rate=0):
    return [
        {'name': name, 'bucket': bucket, 'start_value_eur': 50, 'current_value_eur': 55,
         'own_return_pct': cash_rate if bucket == 'Cash' else 10,
         'via_names': ['Strategy'], 'via_holding_names': ['Certificate'],
         'sources': [{'label': 'Strategy', 'wrapper_name': 'Certificate',
                      'start_value_eur': 50, 'value_eur': 55,
                      'return_pct': cash_rate if bucket == 'Cash' else 10}]}
        for name, bucket in [('Liquiditeiten', 'Cash'), ('Stock', 'Equity')]]


def test_wrapper_profit_is_not_cash_earnings_and_total_is_conserved(run):
    realised = book()
    cash, equity = run(holdings(), realised)
    assert cash['result_eur'] == 0
    assert cash['return_pct'] == 0
    assert cash['own_return_pct'] == 0
    assert cash['contribution_pct'] == 0
    adjustment, = realised['certificate_adjustments']
    assert adjustment['result_eur'] == 8  # 5 value gain + half of 4 sale and 2 income
    assert adjustment['contribution_pct'] == pytest.approx(.8)
    assert equity['result_eur'] + cash['result_eur'] + adjustment['result_eur'] == 16
    assert equity['contribution_pct'] + adjustment['contribution_pct'] == pytest.approx(1.6)


def test_genuine_measured_cash_fx_is_retained(run):
    realised = book()
    cash, equity = run(holdings(cash_rate=2), realised)
    assert cash['result_eur'] == 1  # actual 2% cash move on its 50 opening amount
    assert cash['return_pct'] == 2
    adjustment, = realised['certificate_adjustments']
    assert adjustment['result_eur'] == 7
    assert cash['result_eur'] + equity['result_eur'] + adjustment['result_eur'] == 16


def test_direct_cash_income_and_fx_are_not_erased(run):
    realised = {'available': True, 'basis_eur': 1000, 'positions': []}
    cash, = run([{'name': 'Cash USD', 'bucket': 'Cash', 'start_value_eur': 100,
                  'current_value_eur': 102, 'income_eur': 1, 'own_return_pct': 3}], realised)
    assert cash['unrealised_eur'] == 2
    assert cash['result_eur'] == 3
    assert cash['own_return_pct'] == 3
    assert realised['certificate_adjustments'] == []


def test_adjustments_use_same_flow_aware_contribution_basis(run):
    realised = {**book(), 'has_external_flows': True, 'book_ytd_pct': 8, 'ledger_result_eur': 16}
    rows = run(holdings(), realised)
    total = sum(r['contribution_pct'] for r in rows)
    total += sum(r['contribution_pct'] for r in realised['certificate_adjustments'])
    assert total == pytest.approx(8)


def test_net_zero_adjustment_still_keeps_unrealised_and_realised_operands(run):
    realised = book()
    realised['positions'][0].update(realised_result_eur=-10, income_eur=0)
    cash, _ = run(holdings(), realised)
    adjustment, = realised['certificate_adjustments']
    assert cash['result_eur'] == 0
    assert adjustment['unrealised_eur'] == 5
    assert adjustment['realised_result_eur'] == -5
    assert adjustment['result_eur'] == 0
