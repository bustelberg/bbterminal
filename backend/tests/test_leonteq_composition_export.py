import csv
import io

import pytest

from leonteq.composition_export import parse_composition_csv


def export_blob(**overrides):
    rows = [
        {'Name': 'EURO', 'ISIN': '', 'Instrument category': 'Cash', 'Total value base CCY': '25.00',
         'Total units': '25', 'Current price': '1', 'AVG Performance': '', 'CCY': 'EUR', 'Weight (%)': '25.00'},
        {'Name': 'EXAMPLE SHARE', 'ISIN': 'US0079031078', 'Instrument category': 'Share', 'Total value base CCY': '75.00',
         'Total units': '3', 'Current price': '30', 'AVG Performance': '0', 'CCY': 'USD', 'Weight (%)': '75.00'},
    ]
    rows[1].update(overrides)
    text = io.StringIO()
    writer = csv.DictWriter(text, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    return ('\ufeff' + text.getvalue()).encode('utf-8')


def parse(blob, **kwargs):
    return parse_composition_csv(blob, total_value='100.00 EUR', total_weight='100%',
        summaries=[{'name': 'Cash', 'weight': '25%'}, {'name': 'Share', 'weight': '75%'}],
        expected_count=2, **kwargs)


def test_export_preserves_cash_identifiers_and_base_currency_valuation():
    cash, shares = parse(export_blob())
    assert cash['positions'][0]['isin'] is None
    assert cash['positions'][0]['performance'] == '-'
    share = shares['positions'][0]
    assert share['isin'] == 'US0079031078'
    assert share['total_value'] == '75.00 EUR'
    assert share['currency'] == 'USD'
    assert share['price'] == '30'
    assert share['performance'] == '0%'


@pytest.mark.parametrize('blob', [b'Name\n', b'Name,ISIN\nExample,US0079031078\n',
    export_blob(**{'Total value base CCY': '75000'}),
    export_blob(**{'Instrument category': 'Unknown'}),
    export_blob(**{'Weight (%)': '7.5'}),
    export_blob(**{'Current price': 'NaN'})])
def test_incomplete_or_inconsistent_exports_are_rejected(blob):
    with pytest.raises(ValueError):
        parse(blob)


def test_rounded_values_can_reconcile_within_per_position_precision():
    shares = parse(export_blob(**{'Total value base CCY': '74.99'}))[1]
    assert shares['positions'][0]['total_value'] == '74.99 EUR'
