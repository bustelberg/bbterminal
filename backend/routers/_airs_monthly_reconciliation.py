"""Cash-adjusted position contributions, checked against AIRS, never scaled to fit.

Only zero-external-flow months are supported: dividing a EUR result by opening
capital is not an exact time-weighted contribution when capital enters/leaves.
"""
from __future__ import annotations

from math import isfinite


def _number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value)


def reconcile_positions(sections: dict, performance: dict | None, positions: list[dict]) -> dict:
    def data(key):
        section = sections.get(key) or {}
        return section.get('data') if section.get('status') == 'ok' else None

    perf = performance or {}
    capital = perf.get('beginvermogen')
    target = perf.get('rendement')
    result = {'status': 'unavailable', 'monthly_return_pct': target, 'contribution_sum_pp': None,
              'difference_pp': None, 'adjustments': [], 'issues': []}
    for row in positions:
        row.update(opening_weight_pct=None, contribution_pp=None, contribution_calculation=None)
    if not _number(capital) or capital <= 0 or not _number(target):
        result['issues'].append('performance_missing')
        return result
    opening, closing, transactions = data('opening'), data('closing'), data('transactions')
    if not opening or not closing or not transactions or not transactions.get('readable'):
        result['issues'].append('reports_missing')
        return result
    before, after = opening.get('rows', []), closing.get('rows', [])
    if any(not _number(r.get('value_eur')) for r in before + after):
        result['issues'].append('values_missing')
        return result
    if abs(sum(r['value_eur'] for r in before) - capital) > 1:
        result['issues'].append('opening_capital_mismatch')
    end_capital = perf.get('eindvermogen')
    if not _number(end_capital) or abs(sum(r['value_eur'] for r in after) - end_capital) > 1:
        result['issues'].append('closing_capital_mismatch')
    if any(not _number(perf.get(k)) for k in ('stortingen', 'onttrekkingen')):
        result['issues'].append('external_flows_unknown')
        return result
    if perf['stortingen'] != 0 or perf['onttrekkingen'] != 0:
        result['issues'].append('external_flows_require_daily_attribution')
        return result

    activity = transactions.get('activity', [])
    unknown = False
    for row in positions:
        a = [r for r in before if r['name'] == row['name']]
        b = [r for r in after if r['name'] == row['name']]
        # Cash balance changes include the other side of purchases/sales and
        # distributions; never count them a second time as investment gains.
        cash = row['name'] == 'Effectenrekening' and not row.get('isin')
        if cash:
            row['is_cash'] = True
            row['return_pct'] = None
        if len(a) > 1 or len(b) > 1:
            unknown = True
            continue
        v0 = a[0]['value_eur'] if a else 0
        v1 = b[0]['value_eur'] if b else 0
        row['opening_weight_pct'] = 100 * v0 / capital
        if cash:
            row['contribution_pp'] = 0.0
            continue
        trades = [t for t in activity if t['name'] == row['name']]
        if row.get('return_pct') is None or any(not _number(t.get('amount_eur')) or t['amount_eur'] < 0 for t in trades):
            unknown = True
            continue
        purchases = sum(t['amount_eur'] for t in trades if t['kind'] == 'buy')
        sales = sum(t['amount_eur'] for t in trades if t['kind'] == 'sell')
        pnl = v1 - v0 + sales - purchases
        row['contribution_pp'] = 100 * pnl / capital
        row['contribution_calculation'] = {
            'opening_value_eur': v0, 'closing_value_eur': v1,
            'purchases_eur': purchases, 'sales_eur': sales, 'result_eur': pnl,
            'portfolio_opening_capital_eur': capital,
            'opening_date': opening.get('as_of'), 'closing_date': closing.get('as_of'),
            'trades': [{'date': t['date'], 'kind': t['kind'], 'amount_eur': t['amount_eur']} for t in trades],
        }

    # These are reported accounting components, not an invented balancing item.
    for key in ('opbrengsten', 'mutatie_opgelopen_rente'):
        value = perf.get(key)
        if not _number(value):
            result['issues'].append('accounting_components_missing')
        else:
            result['adjustments'].append({'kind': key, 'contribution_pp': 100 * value / capital})
    # AIRS cost sign is unverified for nonzero costs. Never guess +/- to close a gap.
    if not _number(perf.get('kosten')) or perf['kosten'] != 0:
        result['issues'].append('cost_treatment_unverified')
    if unknown:
        result['issues'].append('position_results_incomplete')
    known_sum = sum(r['contribution_pp'] for r in positions if r['contribution_pp'] is not None)
    known_sum += sum(r['contribution_pp'] for r in result['adjustments'])
    result['contribution_sum_pp'] = known_sum
    result['difference_pp'] = target - known_sum
    # Compare unrounded money to cents, allowing only the source report's
    # cent rounding, not a visually rounded 0.01pp (~EUR 118 on this book).
    investment = perf.get('beleggingsresultaat')
    if not _number(investment) or abs(known_sum * capital / 100 - investment) > 1:
        result['issues'].append('investment_result_mismatch')
    if abs(result['difference_pp'] * capital / 100) > 1:
        result['issues'].append('return_mismatch')
    result['status'] = 'reconciled' if not result['issues'] else 'not_reconciled'
    return result
