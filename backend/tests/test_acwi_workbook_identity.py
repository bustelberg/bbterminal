from index_universe.acwi.holdings import load_acwi_holdings
from routers._airs_portfolio_attribution import _overlaps


def test_sp_global_is_in_authoritative_excel_even_without_pricing_or_cap_coverage():
    rows, _ = load_acwi_holdings()
    member = next(row for row in rows if row['Ticker'] == 'SPGI')
    assert member['Name'] == 'S&P GLOBAL'
    assert _overlaps({'isin': 'US78409V1044', 'name': 'S&P Global Inc.'}, set(), [member['Name']])
    assert not _overlaps({'name': 'Coinbase Global Inc'}, set(), [member['Name']])
    assert not _overlaps({'name': 'WSP Global Inc'}, set(), [member['Name']])
