"""Parse the official LYNQS composition CSV, refusing incomplete exports."""
from __future__ import annotations

import csv
import io
import re
from decimal import Decimal, InvalidOperation

_REQUIRED = {'Name', 'ISIN', 'Instrument category', 'Total value base CCY',
             'Total units', 'Current price', 'AVG Performance', 'CCY', 'Weight (%)'}


def _number(value: str) -> Decimal:
    try:
        result = Decimal(value.replace('’', '').replace("'", '').strip())
    except (InvalidOperation, AttributeError):
        raise ValueError('Invalid export number') from None
    if not result.is_finite():
        raise ValueError('Non-finite export number')
    return result


def _display(value: str) -> str:
    return format(_number(value), ',f').replace(',', '’')


def parse_composition_csv(blob: bytes, *, total_value: str, total_weight: str,
                          summaries: list[dict], expected_count: int) -> list[dict]:
    """Reconcile exported base-currency values and weights to the portal totals.

    Exported values/weights are rounded per position, so reconciliation allows
    up to one cent / 0.01 percentage point per row, never a missing position.
    """
    reader = csv.DictReader(io.StringIO(blob.decode('utf-8-sig')))
    if not _REQUIRED.issubset(reader.fieldnames or []):
        raise ValueError('Missing composition export columns')
    rows = list(reader)
    if not rows or len(rows) != expected_count:
        raise ValueError('Incomplete composition export')
    total = re.fullmatch(r"([\d’\s.,'\-]+)\s+([A-Z]{3})", total_value.strip())
    if not total:
        raise ValueError('Unknown portal total currency')
    amount, currency = total.groups()
    groups = {summary['name']: {**summary, 'positions': []} for summary in summaries}
    value_sum, weight_sum = Decimal(0), Decimal(0)
    for row in rows:
        name, category = row.get('Name'), row.get('Instrument category')
        if not name or category not in groups or not row.get('CCY'):
            raise ValueError('Unknown or incomplete export position')
        value_sum += _number(row['Total value base CCY'])
        weight_sum += _number(row['Weight (%)'])
        groups[category]['positions'].append({
            'name': name, 'isin': row['ISIN'] or None,
            'total_value': f"{_display(row['Total value base CCY'])} {currency}",
            'units': _display(row['Total units']), 'price': _display(row['Current price']),
            'currency': row['CCY'], 'performance': f"{_display(row['AVG Performance'])}%" if row['AVG Performance'] else '-',
            'weight': f"{_display(row['Weight (%)'])}%",
            'identifier': row.get('Identifier') or None,
            'entry_price': row.get('AVG Entry price') or None, 'fx': row.get('FX') or None,
        })
    tolerance = Decimal('0.01') * len(rows)
    if abs(value_sum - _number(amount)) > tolerance:
        raise ValueError('Composition export value does not reconcile')
    if abs(weight_sum - _number(total_weight.removesuffix('%'))) > tolerance:
        raise ValueError('Composition export weights do not reconcile')
    for group in groups.values():
        group_weight = sum((_number(position['weight'].removesuffix('%')) for position in group['positions']), Decimal(0))
        if abs(group_weight - _number(group['weight'].removesuffix('%'))) > tolerance:
            raise ValueError('Composition export group does not reconcile')
    return list(groups.values())
