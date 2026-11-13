"""Published fund holdings mapped to their exact listing's quote currency.

This describes traded holdings, not economic FX exposure or hedge effectiveness.
Partial holdings lists are never rebased to 100%.
"""
import math
import re
import threading
import time
from typing import Literal

import requests
from lxml import html as lxml_html
from pydantic import BaseModel

from asset_pipeline import yahoo
from asset_pipeline.fx import SUBUNIT
from routers._etf_sector_allocation import _PRODUCTS, _CACHE_TTL_SECONDS, _tracked_get


class FundCurrencyHolding(BaseModel):
    name: str
    symbol: str | None = None
    currency: str
    weight_pct: float
    reason: str | None = None


class EtfCurrencyAllocationResponse(BaseModel):
    isin: str
    name: str
    as_of: str | None = None
    source: str
    source_url: str
    basis: Literal['holdings', 'published_currency_allocation'] = 'holdings'
    holdings: list[FundCurrencyHolding]


_CACHE = {}
_LOCK = threading.Lock()
# Public tables checked against these exact share-class ISINs. These are currency
# allocations, not countries or the currency in which the fund's NAV is quoted.
_CURRENCY_TABLE_ISINS = {
    'IE000OEF25S1', 'IE00BP3QZ825', 'IE000MEQP5U8',
    'IE000R4ZNTN3', 'IE00BF4RFH31', 'IE00BM67HQ30',
}
_CURRENCY_NAMES = {
    'US-Dollar': 'USD', 'Euro': 'EUR', 'Japanische Yen': 'JPY',
    'Kanadische Dollar': 'CAD', 'Pfund Sterling': 'GBP', 'Schweizer Franken': 'CHF',
    'Australische Dollar': 'AUD', 'Schwedische Krone': 'SEK', 'Hongkong Dollar': 'HKD',
    'Dänische Kronen': 'DKK', 'Israelischer Neuer Shekel': 'ILS', 'Norwegische Krone': 'NOK',
    'Singapur-Dollar': 'SGD', 'Neuseeland-Dollar': 'NZD', 'Mexikanische Peso Nuevo': 'MXN',
    'Indische Rupie': 'INR', 'Brasilianische Real': 'BRL', 'Chinesischer Renminbi Yuan': 'CNY',
    'Indonesische Rupiah': 'IDR', 'Südkoreanischer Won': 'KRW', 'Philippinischer Peso': 'PHP',
    'Polnischer Zloty': 'PLN', 'Thailändischer Baht': 'THB',
}


def parse_comdirect_currencies(page, isin):
    root = lxml_html.fromstring(page)
    if not re.search(r'\bISIN:\s*' + re.escape(isin) + r'\b', ' '.join(root.itertext())):
        raise ValueError('Currency allocation page does not identify the requested ISIN')
    tables = [table for table in root.xpath('//table')
              if any(cell.text_content().strip() == 'Währung' for cell in table.xpath('./thead/tr/th'))]
    if len(tables) != 1:
        raise ValueError('Expected one published currency allocation table')
    rows, seen = [], set()
    for row in tables[0].xpath('./tbody/tr'):
        cells = row.xpath('./td')
        if len(cells) != 3:
            raise ValueError('Currency allocation row is incomplete')
        name = ' '.join(cells[2].text_content().split())
        match = re.fullmatch(r'([0-9]+(?:,[0-9]+)?)\s*%', cells[1].text_content().strip())
        if not match or not name or name in seen:
            raise ValueError('Currency allocation contains an invalid or duplicate row')
        seen.add(name)
        currency = _CURRENCY_NAMES.get(name, 'Unclassified')
        rows.append(FundCurrencyHolding(name=f'Published allocation · {name}', currency=currency,
            weight_pct=float(match.group(1).replace(',', '.')),
            reason=None if currency != 'Unclassified' else 'Provider reports other or unknown currencies'))
    total = sum(row.weight_pct for row in rows)
    if not rows or not 0 < total <= 100.01 + 1e-8:
        raise ValueError(f'Published currency weights total {total:.2f}%; expected at most 100%')
    if abs(total - 100) <= .01 + 1e-8:
        for row in rows:
            row.weight_pct *= 100 / total
    else:
        rows.append(FundCurrencyHolding(name='Unreported currency allocation', currency='Unclassified',
            weight_pct=100 - total, reason='Currency allocation coverage is incomplete'))
    return EtfCurrencyAllocationResponse(isin=isin, name=_PRODUCTS[isin]['name'],
        source='comdirect', source_url=f'https://www.comdirect.de/inf/fonds/{isin}',
        basis='published_currency_allocation', holdings=rows)


def supported(isin):
    return isin.strip().upper() in _PRODUCTS


def _symbol(isin, product):
    if product.get('symbol'):
        return product['symbol']
    # Exact stored ISIN mapping only; never guess a symbol from a company name.
    from deps import supabase
    rows = (supabase.table('asset_execution').select('yahoo_symbol').eq('status', 'ok')
            .eq('isin', isin).execute()).data or []
    symbols = {row.get('yahoo_symbol') for row in rows if row.get('yahoo_symbol')}
    if len(symbols) != 1:
        raise ValueError(f'{isin}: no unique verified Yahoo listing; configure its symbol before retrying')
    return symbols.pop()


def map_holdings(rows, quotes):
    holdings = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError('Yahoo returned an invalid holding')
        raw = row.get('holdingPercent')
        raw = raw.get('raw') if isinstance(raw, dict) else raw
        if isinstance(raw, bool) or not isinstance(raw, (int, float)) or not math.isfinite(raw) or not 0 <= raw <= 1:
            raise ValueError(f"Invalid holding weight for {row.get('name') or row.get('symbol')}")
        if raw == 0:
            continue
        symbol = str(row.get('symbol') or '').strip()
        name = str(row.get('name') or symbol or 'Unnamed holding')
        identity = symbol or name
        if identity in seen:
            raise ValueError(f'Duplicate published holding: {identity}')
        seen.add(identity)
        currency = (quotes.get(symbol) or {}).get('currency') or ''
        currency = SUBUNIT.get(currency, (currency, 1))[0]
        known = bool(re.fullmatch(r'[A-Z]{3}', currency))
        holdings.append(FundCurrencyHolding(name=name, symbol=symbol or None,
            currency=currency if known else 'Unclassified', weight_pct=raw * 100,
            reason=None if known else 'No quote currency returned for this exact holding symbol'))
    total = sum(row.weight_pct for row in holdings)
    if total > 100.01:
        raise ValueError(f'Published holdings total {total:.2f}%; expected at most 100%')
    # Only absorb publication rounding. Never inflate a partial list.
    if total > 100:
        for row in holdings:
            row.weight_pct *= 100 / total
    elif total < 100:
        holdings.append(FundCurrencyHolding(name='Unreported holdings', currency='Unclassified',
            weight_pct=100 - total, reason='Yahoo does not publish the remaining holdings'))
    return sorted(holdings, key=lambda row: -row.weight_pct)


def fetch_currency_allocation(isin):
    isin = isin.strip().upper()
    with _LOCK:
        cached = _CACHE.get(isin)
        if cached and time.monotonic() - cached[0] < _CACHE_TTL_SECONDS:
            return cached[1]
    product = _PRODUCTS[isin]
    if isin in _CURRENCY_TABLE_ISINS:
        # Prefer the full published currency table to Yahoo's top-ten-only list.
        # A failed acquisition remains retryable; never cache an empty allocation.
        with requests.Session() as session:
            response = _tracked_get(session, f'https://www.comdirect.de/inf/fonds/{isin}',
                source='comdirect', job='etf_currency_allocation')
            response.encoding = 'utf-8'
            result = parse_comdirect_currencies(response.text, isin)
        with _LOCK:
            _CACHE[isin] = (time.monotonic(), result)
        return result
    symbol = _symbol(isin, product)
    rows = yahoo.fund_holdings(symbol, vehicle='mutual_fund' if product['provider'] == 'yahoo_mutual_fund' else 'etf')
    if not rows:
        raise ValueError(f'{isin} / {symbol}: Yahoo returned no verified fund holdings; no currency weights were inferred')
    symbols = list(dict.fromkeys(row.get('symbol') for row in rows if isinstance(row, dict) and row.get('symbol')))
    result = EtfCurrencyAllocationResponse(isin=isin, name=product['name'], source='Yahoo Finance',
        source_url=f'https://finance.yahoo.com/quote/{symbol}/holdings/',
        holdings=map_holdings(rows, _holding_quotes(symbols)))
    with _LOCK:
        _CACHE[isin] = (time.monotonic(), result)
    return result


def _holding_quotes(symbols):
    quotes = yahoo.quote(symbols)
    for symbol in symbols:
        if (quotes.get(symbol) or {}).get('currency'):
            continue
        chart = yahoo.chart(symbol, rng='5d') or {}
        meta = chart.get('meta') or {}
        # A chart for a substituted listing is not evidence for this holding.
        if meta.get('symbol') == symbol and meta.get('currency'):
            quotes[symbol] = {'currency': meta['currency']}
    return quotes
