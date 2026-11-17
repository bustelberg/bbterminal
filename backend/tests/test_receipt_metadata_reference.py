import pytest

from routers import _airs_portfolio_analysis as analysis
from routers._airs_holding_isin import classify_bucket
from tests._fake_supabase import FakeSupabase


@pytest.mark.parametrize('receipt,ordinary,currency', [
    ('USY5217N1183', 'INE018A01030', 'INR'),
    ('US78392B2060', 'KR7000660001', 'KRW'),
])
def test_reviewed_receipts_share_issuer_metadata_but_not_prices_or_quote_currency(
        monkeypatch, receipt, ordinary, currency):
    def load(table, columns, key, isins):
        assert ordinary in isins and receipt in isins
        return [{'isin': ordinary, 'company_id': 123, 'name': 'Internal company name',
            'asset_class': 'equity', 'sector': 'Technology', 'country': 'Korea',
            'currency': currency, 'market_cap_currency': currency, 'status': 'ok'}]
    monkeypatch.setattr(analysis, 'load_rows_via_copy', load)
    monkeypatch.setattr(analysis, 'supabase', FakeSupabase({}))
    rows = analysis._grid_uncached([receipt, 'UNREVIEWED'])
    assert set(rows) == {receipt}
    row = rows[receipt]
    assert row['isin'] == receipt
    assert row['company_id'] == 123
    assert row['metadata_reference_isin'] == ordinary
    assert row['sector'] == 'Technology'
    assert 'currency' not in row and 'market_cap_currency' not in row
    assert classify_bucket(None, False, receipt, 'Receipt', row) == 'Equity'
    assert analysis._buckets(row, False, receipt,
        {'IN': 'India', 'KR': 'South Korea', 'US': 'United States'})[1] == 'Emerging Markets'


def test_direct_receipt_metadata_is_preferred(monkeypatch):
    receipt = 'US78392B2060'
    monkeypatch.setattr(analysis, 'load_rows_via_copy', lambda *_: [{
        'isin': receipt, 'company_id': 123, 'name': 'Direct ADR', 'status': 'ok',
        'asset_class': 'equity', 'currency': 'USD'}])
    monkeypatch.setattr(analysis, 'supabase', FakeSupabase({}))
    row = analysis._grid_uncached([receipt])[receipt]
    assert row['name'] == 'Direct ADR' and row['currency'] == 'USD'
    assert 'metadata_reference_isin' not in row
