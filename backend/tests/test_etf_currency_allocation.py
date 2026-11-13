import pytest

from routers import _etf_currency_allocation as currency
from asset_pipeline import yahoo


def test_partial_holdings_preserve_weights_and_unknown_currencies():
    rows = currency.map_holdings([
        {'name': 'US stock', 'symbol': 'US', 'holdingPercent': {'raw': .12}},
        {'name': 'UK stock', 'symbol': 'UK.L', 'holdingPercent': .08},
        {'name': 'Unknown', 'symbol': 'X', 'holdingPercent': .05},
    ], {'US': {'currency': 'USD'}, 'UK.L': {'currency': 'GBp'}})
    by_name = {row.name: row for row in rows}
    assert by_name['US stock'].weight_pct == 12
    assert by_name['UK stock'].currency == 'GBP'
    assert by_name['Unknown'].currency == 'Unclassified'
    assert by_name['Unknown'].reason
    assert by_name['Unreported holdings'].weight_pct == 75
    assert sum(row.weight_pct for row in rows) == 100


@pytest.mark.parametrize('rows', [
    [{'name': 'Bad', 'holdingPercent': float('nan')}],
    [{'name': 'Bad', 'holdingPercent': -.1}],
    [{'name': 'A', 'holdingPercent': .7}, {'name': 'B', 'holdingPercent': .4}],
    [{'symbol': 'A', 'holdingPercent': .1}, {'symbol': 'A', 'holdingPercent': .2}],
])
def test_invalid_provider_weights_are_not_cached_as_exposure(rows):
    with pytest.raises(ValueError):
        currency.map_holdings(rows, {})


def test_letko_uses_holdings_currencies_not_eur_share_class_and_caches(monkeypatch):
    monkeypatch.setattr(currency, '_CURRENCY_TABLE_ISINS', set())
    currency._CACHE.clear()
    calls = []
    def holdings(symbol, **kwargs):
        calls.append((symbol, kwargs))
        return [{'name': 'Stock', 'symbol': 'STOCK', 'holdingPercent': .2}]
    monkeypatch.setattr(yahoo, 'fund_holdings', holdings)
    monkeypatch.setattr(yahoo, 'quote', lambda symbols: {'STOCK': {'currency': 'USD'}})
    result = currency.fetch_currency_allocation('IE000MEQP5U8')
    assert result.as_of is None
    assert result.holdings[1].currency == 'USD'
    assert currency.fetch_currency_allocation('IE000MEQP5U8') == result
    assert calls == [('0P0001TPVP.F', {'vehicle': 'mutual_fund'})]
    currency._CACHE.clear()


def test_unavailable_holdings_explain_listing_and_are_retryable(monkeypatch):
    monkeypatch.setattr(currency, '_CURRENCY_TABLE_ISINS', set())
    currency._CACHE.clear()
    monkeypatch.setattr(yahoo, 'fund_holdings', lambda *args, **kwargs: [])
    with pytest.raises(ValueError, match='IE000MEQP5U8 / 0P0001TPVP.F'):
        currency.fetch_currency_allocation('IE000MEQP5U8')
    assert not currency._CACHE


@pytest.mark.parametrize('isin', sorted(currency._CURRENCY_TABLE_ISINS))
def test_full_currency_tables_cover_each_reported_fund(isin):
    import json
    from pathlib import Path
    pages = json.loads((Path(__file__).parent / 'fixtures/comdirect_fund_currencies.json').read_text(encoding='utf-8'))
    result = currency.parse_comdirect_currencies(pages[isin], isin)
    assert result.basis == 'published_currency_allocation'
    assert result.as_of is None  # The NAV timestamp is not an allocation date.
    assert sum(row.weight_pct for row in result.holdings) == pytest.approx(100)
    assert sum(row.weight_pct for row in result.holdings if row.currency == 'Unclassified') < 1
    assert sum(row.weight_pct for row in result.holdings if row.currency == 'USD') > 5
    with pytest.raises(ValueError, match='requested ISIN'):
        currency.parse_comdirect_currencies(pages[isin], 'WRONG')


def test_published_table_is_used_before_partial_yahoo_holdings(monkeypatch):
    import json
    from pathlib import Path
    from types import SimpleNamespace
    isin = 'IE000MEQP5U8'
    pages = json.loads((Path(__file__).parent / 'fixtures/comdirect_fund_currencies.json').read_text(encoding='utf-8'))
    currency._CACHE.clear()
    monkeypatch.setattr(currency, '_tracked_get', lambda *a, **k: SimpleNamespace(text=pages[isin]))
    monkeypatch.setattr(yahoo, 'fund_holdings', lambda *a, **k: pytest.fail('Full table must precede Yahoo top holdings'))
    result = currency.fetch_currency_allocation(isin)
    assert result.source == 'comdirect'
    assert currency.fetch_currency_allocation(isin) == result
    currency._CACHE.clear()


def test_missing_batch_currency_uses_only_exact_chart_listing(monkeypatch):
    monkeypatch.setattr(yahoo, 'quote', lambda _: {})
    monkeypatch.setattr(yahoo, 'chart', lambda symbol, **k: {'meta': {'symbol': symbol if symbol == 'GOOD' else 'OTHER', 'currency': 'USD'}})
    assert currency._holding_quotes(['GOOD', 'BAD']) == {'GOOD': {'currency': 'USD'}}
