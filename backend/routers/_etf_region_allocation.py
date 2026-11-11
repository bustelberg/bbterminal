"""Issuer/justETF country exposure mapped to the portfolio's existing MSCI regions."""
import html
import json
import math
import re
import threading
import time
from datetime import datetime
from urllib.parse import urljoin

import requests
from pydantic import BaseModel

from asset_pipeline.geo import msci_region_of
from routers._etf_sector_allocation import _PRODUCTS, _tracked_get, _CACHE_TTL_SECONDS, _JUSTETF_ROOT


class EtfProviderCountryWeight(BaseModel):
    country: str
    weight_pct: float


class EtfRegionWeight(BaseModel):
    region: str
    weight_pct: float
    provider_weights: list[EtfProviderCountryWeight]


class EtfRegionAllocationResponse(BaseModel):
    isin: str
    name: str
    as_of: str
    source: str
    source_url: str
    regions: list[EtfRegionWeight]


_CACHE = {}
_LOCK = threading.Lock()


def supported(isin):
    product = _PRODUCTS.get(isin.strip().upper())
    # Yahoo sector data does not prove mutual-fund country coverage.
    return bool(product and product['provider'] != 'yahoo_mutual_fund')


def validated(isin, rows, as_of, source, source_url):
    countries = {}
    for name, raw in rows:
        name, weight = str(name).strip(), float(raw)
        if not name or not math.isfinite(weight) or not 0 <= weight <= 100:
            raise ValueError('Country allocation contains an invalid weight')
        if name in countries:
            raise ValueError('Country allocation contains duplicate countries')
        countries[name] = weight
    total = sum(countries.values())
    if not countries or not 99 <= total <= 101:
        raise ValueError(f'Country weights total {total:.2f}%, expected about 100%')
    groups = {}
    for country, weight in countries.items():
        # Unknown countries and provider "Other" remain explicit; never spread them over known regions.
        region = msci_region_of(country) or 'Unclassified'
        groups.setdefault(region, []).append(EtfProviderCountryWeight(country=country, weight_pct=weight))
    return EtfRegionAllocationResponse(isin=isin, name=_PRODUCTS[isin]['name'], as_of=as_of,
        source=source, source_url=source_url, regions=[
            EtfRegionWeight(region=region, weight_pct=round(sum(row.weight_pct for row in weights), 8), provider_weights=weights)
            for region, weights in sorted(groups.items(), key=lambda item: -sum(row.weight_pct for row in item[1]))])


def parse_ishares(page, isin):
    decoded = html.unescape(page)
    marker = re.search(r'"countries":\{"name":"countries","fullName":"exposureBreakdowns.geography.countries"', decoded)
    if not marker:
        raise ValueError('iShares country exposure is unavailable')
    payload, _ = json.JSONDecoder().raw_decode(decoded[decoded.find('{', marker.start()):])
    points = payload.get('dataPointsByNameMap') or {}
    names = (points.get('type') or {}).get('value') or []
    weights = (points.get('fund') or {}).get('value') or []
    if not names or len(names) != len(weights):
        raise ValueError('iShares country names and weights are incomplete')
    as_of = datetime.strptime(str((points.get('asOf') or {}).get('value')), '%Y%m%d').date().isoformat()
    return validated(isin, list(zip(names, weights)), as_of, 'iShares', _PRODUCTS[isin]['url'])


def parse_justetf(page, expanded, isin):
    rows = re.findall(r'data-testid="tl_etf-holdings_countries_value_name">\s*([^<]+?)\s*</td>.*?'
                      r'data-testid="tl_etf-holdings_countries_value_percentage">\s*([0-9.,]+)%', expanded, re.S)
    dated = re.search(r'tl_etf-holdings_reference-date">\s*As of\s+([^<]+)', page)
    if not dated:
        raise ValueError('Country allocation has no source date')
    as_of = datetime.strptime(html.unescape(dated.group(1)).strip(), '%d/%m/%Y').date().isoformat()
    return validated(isin, [(html.unescape(name).strip(), weight.replace(',', '.')) for name, weight in rows],
                     as_of, 'justETF', f'{_JUSTETF_ROOT}/en/etf-profile.html?isin={isin}')


def _justetf(isin):
    url = f'{_JUSTETF_ROOT}/en/etf-profile.html?isin={isin}'
    last = None
    for _ in range(2):
        try:
            with requests.Session() as session:
                page = _tracked_get(session, url, source='justetf', job='etf_region_allocation').text
                callback = re.search(r'Wicket\.Ajax\.ajax\(\{"u":"([^"]*loadMoreCountries[^"]*)"', page)
                expanded = (_tracked_get(session, urljoin(_JUSTETF_ROOT, html.unescape(callback.group(1))),
                                         source='justetf', job='etf_region_allocation').text if callback else page)
            return parse_justetf(page, expanded, isin)
        except Exception as error:
            last = error
    raise last


def fetch_region_allocation(isin):
    isin = isin.strip().upper()
    with _LOCK:
        cached = _CACHE.get(isin)
        if cached and time.monotonic() - cached[0] < _CACHE_TTL_SECONDS:
            return cached[1]
    product = _PRODUCTS[isin]
    if product['provider'] == 'ishares':
        try:
            with requests.Session() as session:
                page = _tracked_get(session, product['url'], source='ishares', job='etf_region_allocation').text
            result = parse_ishares(page, isin)
        except Exception:
            result = _justetf(isin)
    else:
        result = _justetf(isin)
    with _LOCK:
        _CACHE[isin] = (time.monotonic(), result)
    return result
