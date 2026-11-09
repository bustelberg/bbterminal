from unittest.mock import Mock

import pytest

from leonteq import portal_api as api


def metric(value, precision=2):
    return {'value': value, 'precision': precision}


def catalogue():
    return {'totalRows': 1, 'rows': [{
        'ruleBookId': 'fund-id', 'name': ' Fund ', 'indexSponsor': ' Sponsor ',
        'inceptionDate': '2026-05-19', 'currentValuation': 100, 'assetsUnderManagement': 1000,
        'performanceYTD': metric(0), 'performanceInception': metric(-1.2),
        'performance1D': metric(0), 'performance1DAbsolute': metric(0),
        'recentValuations': [{'date': '2026-11-06', 'value': 100}],
    }]}


def cash():
    return {'name': 'EURO', 'instrumentCategory': 'Cash', 'amcCurrency': 'EUR', 'currency': 'EUR',
            'totalValueAmcCcy': metric(100), 'weightPercentAmcCcy': metric(100),
            'totalUnits': metric(100), 'currentPrice': {'data': metric(1)},
            'averagePerformance': metric(None), 'instrumentIsin': None}


def test_catalogue_preserves_zero_and_source_names_are_trimmed_like_the_portal():
    row = api.catalogue(catalogue())[0]
    assert row['name'] == 'Fund'
    assert row['sponsor'] == 'Sponsor'
    assert row['ytd'] == '0.00%'
    assert row['itd'] == '-1.20%'
    assert row['value'] == '100.00'
    assert row['last_7_days_path'] == 'M 0.00 20.00'


def test_catalogue_rejects_partial_results_and_ambiguous_names():
    with pytest.raises(api.ApiUnavailable):
        api.catalogue([])
    payload = catalogue()
    payload['totalRows'] = 2
    with pytest.raises(api.ApiUnavailable):
        api.catalogue(payload)
    payload['rows'].append({**payload['rows'][0], 'name': 'Fund'})
    with pytest.raises(api.ApiUnavailable):
        api.catalogue(payload)


def test_cash_without_entry_price_is_valid_and_totals_must_reconcile():
    with pytest.raises(api.ApiUnavailable):
        api.composition(None, 'Fund')
    payload = {'rows': [cash()], 'totals': {'totalValueAmcCcy': 100, 'weightPercentAmcCcy': 100}}
    result = api.composition(payload, 'Fund')
    position = result['groups'][0]['positions'][0]
    assert position['price'] == '1.00'
    assert position['entry_price'] is None
    assert position['isin'] is None
    assert position['performance'] == '-'
    payload['rows'][0]['currentPrice']['date'] = '2026-11-06'
    payload['closingPriceDate'] = '2026-11-09'
    assert api.composition(payload, 'Fund')['groups'][0]['positions'][0]['price_date'] == '2026-11-06'
    payload['rows'][0]['currentPrice'] = {'data': metric(1.12345, 5)}
    assert api.composition(payload, 'Fund')['groups'][0]['positions'][0]['price'] == '1.12345'
    payload['totals']['totalValueAmcCcy'] = 200
    with pytest.raises(api.ApiUnavailable):
        api.composition(payload, 'Fund')


def test_monthly_values_are_percentages_not_fractions_and_missing_is_not_zero():
    result = api.monthly([{'year': 2026, 'january': metric(0), 'february': metric(-1.2), 'total': metric(4.5)}], 'Fund')
    assert [row['return_pct'] for row in result['months'][:3]] == [0, -1.2, None]
    assert result['yearly_totals'] == {'2026': 4.5}


def test_every_explicit_read_requests_new_data_without_page_navigation():
    page = Mock()
    first, second = catalogue(), catalogue()
    second['rows'][0]['currentValuation'] = 101
    page.request.get.side_effect = [Mock(ok=True, json=lambda: first), Mock(ok=True, json=lambda: second)]
    ids = {}
    assert api.read(page, ids)['portfolios'][0]['value'] == '100.00'
    assert api.read(page, ids)['portfolios'][0]['value'] == '101.00'
    assert ids == {'Fund': 'fund-id'}
    assert page.request.get.call_count == 2
    assert all(call.kwargs['headers'] == {'Cache-Control': 'no-cache'} for call in page.request.get.call_args_list)
    page.goto.assert_not_called()
