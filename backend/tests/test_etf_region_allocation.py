import html
import json
from unittest.mock import Mock

import pytest

from routers import _etf_region_allocation as R

ISIN = 'IE000OEF25S1'
ISHARES = 'IE00BP3QZ825'


def page():
    return '<span data-testid="tl_etf-holdings_reference-date">As of 28/09/2026</span>'


def country_rows(values):
    return ''.join(f'<td data-testid="tl_etf-holdings_countries_value_name">{name}</td>'
                   f'<td data-testid="tl_etf-holdings_countries_value_percentage">{weight}%</td>' for name, weight in values)


def test_country_mapping_aggregates_existing_regions_and_keeps_other_unknown():
    result = R.parse_justetf(page(), country_rows([
        ('United States', 40), ('Canada', 5), ('United Kingdom', 20), ('Japan', 25), ('Other', 10)]), ISIN)
    rows = {row.region: row for row in result.regions}
    assert rows['North America'].weight_pct == 45
    assert [row.country for row in rows['North America'].provider_weights] == ['United States', 'Canada']
    assert rows['Europe'].weight_pct == 20
    assert rows['Pacific'].weight_pct == 25
    assert rows['Unclassified'].weight_pct == 10
    assert rows['Unclassified'].provider_weights[0].country == 'Other'
    assert result.as_of == '2026-09-28'
    assert sum(row.weight_pct for row in result.regions) == 100


def test_reuses_financial_region_definitions_and_preserves_unknown_country_labels():
    result = R.validated(ISIN, [('Israel', 20), ('South Korea', 30), ('Unknown country', 50)], '2026-11-09', 'test', 'test')
    assert {row.region: row.weight_pct for row in result.regions} == {'Europe': 20, 'Emerging Markets': 30, 'Unclassified': 50}


def test_reads_official_ishares_geography_payload_and_cash_without_changing_sector_parser():
    payload = {'name': 'countries', 'fullName': 'exposureBreakdowns.geography.countries',
               'dataPointsByNameMap': {'type': {'value': ['United States', 'Other', 'Cash and/or Derivatives']},
                                     'fund': {'value': [95, 4.5, 0.5]}, 'asOf': {'value': 20261109}}}
    embedded = '"countries":' + json.dumps(payload, separators=(',', ':'))
    result = R.parse_ishares(html.escape(embedded, quote=True), ISHARES)
    assert result.source == 'iShares'
    assert result.as_of == '2026-11-09'
    assert {row.region: row.weight_pct for row in result.regions} == {'North America': 95, 'Unclassified': 5}
    assert len(result.regions[1].provider_weights) == 2


@pytest.mark.parametrize('rows', [[('United States', 80)], [('Other', 101)], [('Other', -1), ('United States', 101)],
                                  [('Other', float('nan'))], [('Other', 50), ('Other', 50)]])
def test_invalid_or_partial_country_data_is_rejected(rows):
    with pytest.raises(ValueError):
        R.validated(ISIN, rows, '2026-11-09', 'test', 'test')


def test_expands_justetf_country_table_with_same_session_and_caches_success(monkeypatch):
    R._CACHE.clear()
    get = Mock(side_effect=[Mock(text=page() + 'Wicket.Ajax.ajax({"u":"/expanded?loadMoreCountries=1&amp;x=2"'),
                            Mock(text=country_rows([('United States', 75), ('Other', 25)]))])
    monkeypatch.setattr(R, '_tracked_get', get)
    result = R.fetch_region_allocation(ISIN)
    assert R.fetch_region_allocation(ISIN) is result
    assert get.call_count == 2
    assert get.call_args_list[0].args[0] is get.call_args_list[1].args[0]
    assert get.call_args_list[1].args[1].endswith('/expanded?loadMoreCountries=1&x=2')
    assert get.call_args.kwargs['job'] == 'etf_region_allocation'
    R._CACHE.clear()


def test_ishares_failure_falls_back_to_justetf(monkeypatch):
    R._CACHE.clear()
    get = Mock(side_effect=ValueError('upstream unavailable'))
    monkeypatch.setattr(R, '_tracked_get', get)
    result = R.parse_justetf(page(), country_rows([('United States', 100)]), ISHARES)
    fallback = Mock(return_value=result)
    monkeypatch.setattr(R, '_justetf', fallback)
    assert R.fetch_region_allocation(ISHARES) == result
    fallback.assert_called_once_with(ISHARES)
    R._CACHE.clear()


def test_failures_are_not_cached_and_unknown_funds_are_not_claimed_supported(monkeypatch):
    R._CACHE.clear()
    monkeypatch.setattr(R, '_justetf', Mock(side_effect=ValueError('incomplete countries')))
    with pytest.raises(ValueError):
        R.fetch_region_allocation(ISIN)
    assert ISIN not in R._CACHE
    assert not R.supported('CH1593776334')
    assert not R.supported('IE000MEQP5U8')
