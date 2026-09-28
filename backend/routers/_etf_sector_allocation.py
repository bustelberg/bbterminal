"""Look-through sector weights for explicitly verified external funds.

``is_fund`` is deliberately too broad for this job: it also covers mutual funds and the Leonteq
certificates that wrap our own TopSelectie portfolios. This module therefore owns an exact ISIN
allow-list of external ETFs and mutual-fund share classes for which a sector feed has been
verified. No name heuristic can create a button.

The preferred source is the issuer. Verified physical iShares funds use their daily official
breakdown, with justETF as a fallback; other European ETFs use justETF's ISIN-keyed profile and
its full "show more" sector table when published. The remaining listings use Yahoo's ETF-only
``topHoldings`` feed. Successful results are cached for six hours.
"""
from __future__ import annotations

import html
import json
import logging
import math
import re
import threading
import time
from datetime import datetime
from urllib.parse import urljoin

import requests
from pydantic import BaseModel

from asset_pipeline.sector_override import normalize_gics_sector
from deps import supabase
from ingest.api_usage import api_usage_job, classify_outcome, track_api_call

MOMENTUM_ISIN = "IE00BP3QZ825"

_log = logging.getLogger(__name__)

_ISHARES_PRODUCTS = {
    MOMENTUM_ISIN: (
        "iShares Edge MSCI World Momentum Factor UCITS ETF",
        "270051/ishares-edge-msci-world-momentum-factor-ucits-etf",
    ),
    "IE00B4K48X80": (
        "iShares Core MSCI Europe UCITS ETF EUR (Acc)",
        "251861/ishares-msci-europe-ucits-etf-acc-fund",
    ),
    "IE00B6R52259": (
        "iShares MSCI All Country World UCITS ETF USD (Acc)",
        "251850/ishares-msci-acwi-ucits-etf-acc-fund",
    ),
    "IE00BP3QZ601": (
        "iShares Edge MSCI World Quality Factor UCITS ETF (Acc)",
        "270054/ishares-msci-world-quality-factor-ucits-etf",
    ),
    "IE00BF4RFH31": (
        "iShares MSCI World Small Cap UCITS ETF",
        "296576/ishares-msci-world-small-cap-ucits-etf",
    ),
    "IE000R4ZNTN3": (
        "iShares MSCI World ex-USA UCITS ETF USD (Acc)",
        "340748/ishares-msci-world-ex-usa-ucits-etf",
    ),
    "IE00BZ0PKT83": (
        "iShares STOXX World Equity Multifactor UCITS ETF USD (Acc)",
        "277246/ishares-factorselect-msci-world-ucits-etf",
    ),
}

_JUSTETF_NAMES = {
    "LU1681044480": "Amundi MSCI Emerging Markets Asia UCITS ETF EUR (C)",
    "IE000OEF25S1": "Invesco MSCI World Equal Weight UCITS ETF Acc",
    "IE00BL0BMZ89": "VanEck Morningstar Global Wide Moat UCITS ETF",
    "IE00BFMXYX26": "Vanguard FTSE Japan UCITS ETF (USD) Accumulating",
    "IE00BM67HQ30": "Xtrackers MSCI World Utilities UCITS ETF 1C",
    # Synthetic fund: iShares' sector table describes its swap collateral (currently almost all
    # "Other"), not NASDAQ-100 exposure. justETF's index-facing split is the honest chart input.
    "IE0001ZFMLN7": "iShares NASDAQ 100 Swap UCITS ETF USD (Acc)",
}

_YAHOO_PRODUCTS = {
    "US46138E3392": ("Invesco S&P 500 Momentum ETF", "SPMO"),
    "IE000PS0J481": ("Global X European Infrastructure Development UCITS ETF", "BRIJ.L"),
    "IE00BM8R0J59": ("Global X Nasdaq 100 Covered Call UCITS ETF", "QYLD.L"),
    "IE0002L5QB31": ("Global X S&P 500 Covered Call UCITS ETF", "XYLU.L"),
    "IE00077FRP95": ("Global X SuperDividend UCITS ETF", "SDIV.L"),
    "IE00B8CJW150": ("Invesco Morningstar US Energy Infrastructure MLP UCITS ETF", "MLPD.L"),
    "IE000LCKJ888": ("WisdomTree Physical AI, Humanoids and Drones UCITS ETF", "WPAI.L"),
}

_YAHOO_MUTUAL_FUNDS = {
    # Yahoo search resolves this exact ISIN to the Frankfurt mutual-fund share class below. Its
    # quoteSummary publishes all eleven sectors and identifies the instrument as MUTUALFUND.
    "IE000MEQP5U8": (
        "Letko Brosseau Global Emerging Markets Equity Fund - Class Launch EUR Acc",
        "0P0001TPVP.F",
    ),
}

_PRODUCTS = {
    **{isin: {"name": name, "provider": "justetf"}
       for isin, name in _JUSTETF_NAMES.items()},
    **{isin: {"name": name, "provider": "yahoo", "symbol": symbol}
       for isin, (name, symbol) in _YAHOO_PRODUCTS.items()},
    **{isin: {"name": name, "provider": "yahoo_mutual_fund", "symbol": symbol}
       for isin, (name, symbol) in _YAHOO_MUTUAL_FUNDS.items()},
    **{
        isin: {
            "name": name,
            "provider": "ishares",
            "fallback": "justetf",
            "url": f"https://www.ishares.com/uk/individual/en/products/{path}",
        }
        for isin, (name, path) in _ISHARES_PRODUCTS.items()
    },
}

_ISHARES_MARKER = '"sector":{"name":"sector","fullName":"exposureBreakdowns.sector"'
_JUSTETF_ROOT = "https://www.justetf.com"
_CACHE_TTL_SECONDS = 6 * 60 * 60
_CACHE: dict[str, tuple[float, "EtfSectorAllocationResponse"]] = {}
_CACHE_LOCK = threading.Lock()
_NON_SECTOR_BUCKETS = {
    "cash and/or derivatives": "Cash",
    "non-corporate": "Unclassified",
    "other": "Unclassified",
}


class EtfProviderSectorWeight(BaseModel):
    sector: str
    weight_pct: float


class EtfSectorWeight(BaseModel):
    provider_sectors: list[str]
    provider_weights: list[EtfProviderSectorWeight]
    sector: str
    weight_pct: float


class EtfSectorAllocationResponse(BaseModel):
    isin: str
    name: str
    as_of: str | None = None
    source: str
    source_url: str
    sectors: list[EtfSectorWeight]


def supported(isin: str) -> bool:
    return isin.strip().upper() in _PRODUCTS


def _validated_response(
    *, isin: str, name: str, as_of: str | None, source: str, source_url: str,
    rows: list[tuple[str, float]] | list[dict],
) -> EtfSectorAllocationResponse:
    weights_by_sector: dict[str, float] = {}
    provider_weights_by_sector: dict[str, dict[str, float]] = {}
    for raw in rows:
        raw_name, raw_weight = ((raw["sector"], raw["weight_pct"])
                                if isinstance(raw, dict) else raw)
        provider_sector = str(raw_name).strip()
        normalized_name = " ".join(provider_sector.lower().split())
        sector = (normalize_gics_sector(provider_sector)
                  or _NON_SECTOR_BUCKETS.get(normalized_name))
        weight = float(raw_weight)
        if not sector:
            raise ValueError(f"unknown provider sector {provider_sector!r}")
        if not math.isfinite(weight) or weight < 0 or weight > 100:
            raise ValueError("sector allocation contains an invalid row")
        # Different provider categories can converge on one of our sectors (for example justETF's
        # Consumer Cyclicals and Consumer Services both belong to Consumer Discretionary).
        weights_by_sector[sector] = weights_by_sector.get(sector, 0.0) + weight
        provider_weights = provider_weights_by_sector.setdefault(sector, {})
        provider_weights[provider_sector] = provider_weights.get(provider_sector, 0.0) + weight
    sectors = [
        EtfSectorWeight(
            provider_sectors=list(provider_weights_by_sector[sector]),
            provider_weights=[
                EtfProviderSectorWeight(sector=name, weight_pct=round(provider_weight, 8))
                for name, provider_weight in provider_weights_by_sector[sector].items()
            ],
            sector=sector,
            weight_pct=round(weight, 8),
        )
        for sector, weight in weights_by_sector.items()
    ]
    total = sum(row.weight_pct for row in sectors)
    if not sectors or not 99 <= total <= 101:
        raise ValueError(f"sector weights total {total:.2f}%, expected about 100%")
    return EtfSectorAllocationResponse(
        isin=isin, name=name, as_of=as_of, source=source,
        source_url=source_url, sectors=sectors,
    )


def parse_ishares_sector_payload(page: str, *, isin: str) -> EtfSectorAllocationResponse:
    """Extract and validate the official sector object embedded in an iShares page."""
    isin = isin.strip().upper()
    product = _PRODUCTS[isin]
    decoded = html.unescape(page)
    marker_at = decoded.find(_ISHARES_MARKER)
    if marker_at < 0:
        raise ValueError("iShares page did not contain its sector allocation payload")
    object_at = decoded.find("{", marker_at)
    sector_data, _end = json.JSONDecoder().raw_decode(decoded[object_at:])
    points = sector_data.get("dataPointsByNameMap") or {}
    names = (points.get("type") or {}).get("value") or []
    weights = (points.get("fund") or {}).get("value") or []
    raw_as_of = (points.get("asOf") or {}).get("value")
    if not names or len(names) != len(weights):
        raise ValueError("iShares sector names and weights are missing or misaligned")
    try:
        as_of = datetime.strptime(str(raw_as_of), "%Y%m%d").date().isoformat()
    except (TypeError, ValueError) as exc:
        raise ValueError("iShares sector allocation has no valid as-of date") from exc
    return _validated_response(
        isin=isin, name=product["name"], as_of=as_of, source="iShares",
        source_url=product["url"], rows=list(zip(names, weights, strict=True)),
    )


_JUSTETF_ROWS = re.compile(
    r'data-testid="tl_etf-holdings_sectors_value_name">\s*([^<]+?)\s*</td>.*?'
    r'data-testid="tl_etf-holdings_sectors_value_percentage">\s*([0-9.,]+)%',
    re.S,
)


def parse_justetf_sector_payload(
    page: str, expanded: str, *, isin: str,
) -> EtfSectorAllocationResponse:
    """Parse justETF's expanded sector table and the dated profile around it."""
    isin = isin.strip().upper()
    product = _PRODUCTS[isin]
    matches = _JUSTETF_ROWS.findall(expanded)
    if not matches:
        raise ValueError("justETF did not publish sector weights for this ETF")
    date_match = re.search(
        r'tl_etf-holdings_reference-date">\s*As of\s+([^<]+)', page)
    if not date_match:
        raise ValueError("justETF sector allocation has no as-of date")
    try:
        as_of = datetime.strptime(
            html.unescape(date_match.group(1)).strip(), "%d/%m/%Y"
        ).date().isoformat()
    except ValueError as exc:
        raise ValueError("justETF sector allocation has an invalid as-of date") from exc
    rows = [(html.unescape(name).strip(), float(weight.replace(",", ".")))
            for name, weight in matches]
    source_url = f"{_JUSTETF_ROOT}/en/etf-profile.html?isin={isin}"
    return _validated_response(
        isin=isin, name=product["name"], as_of=as_of, source="justETF",
        source_url=source_url, rows=rows,
    )


def _tracked_get(session: requests.Session, url: str, *, source: str) -> requests.Response:
    status: int | None = None
    body = ""
    try:
        response = session.get(
            url, headers={"User-Agent": "Mozilla/5.0 bbterminal/1.0"}, timeout=20)
        status, body = response.status_code, response.text or ""
        response.raise_for_status()
        return response
    finally:
        track_api_call(
            supabase, source=source, region="global", job="etf_sector_allocation",
            outcome=classify_outcome(status, has_data=bool(body)),
        )


def _fetch_ishares(isin: str, product: dict) -> EtfSectorAllocationResponse:
    with requests.Session() as session:
        response = _tracked_get(session, product["url"], source="ishares")
    return parse_ishares_sector_payload(response.text, isin=isin)


def _fetch_justetf(isin: str) -> EtfSectorAllocationResponse:
    source_url = f"{_JUSTETF_ROOT}/en/etf-profile.html?isin={isin}"
    with requests.Session() as session:
        page = _tracked_get(session, source_url, source="justetf").text
        callback = re.search(
            r'Wicket\.Ajax\.ajax\(\{"u":"([^"]*loadMoreSectors[^"]*)"', page)
        # Short sector lists are already complete in the server-rendered profile and deliberately
        # have no "show more" callback. Xtrackers World Utilities is the concrete case: four rows
        # summing to 100%, all inline. Only make the second request when justETF advertises it;
        # `_validated_response` still rejects an inline table that is merely a partial preview.
        expanded = page
        if callback:
            expanded_url = urljoin(_JUSTETF_ROOT, html.unescape(callback.group(1)))
            expanded = _tracked_get(session, expanded_url, source="justetf").text
    return parse_justetf_sector_payload(page, expanded, isin=isin)


def _fetch_yahoo(isin: str, product: dict) -> EtfSectorAllocationResponse:
    from asset_pipeline.yahoo import (  # noqa: PLC0415
        fund_sector_weightings, mutual_fund_sector_weightings)

    with api_usage_job("etf_sector_allocation"):
        rows = (mutual_fund_sector_weightings(product["symbol"])
                if product["provider"] == "yahoo_mutual_fund"
                else fund_sector_weightings(product["symbol"]))
    return _validated_response(
        isin=isin, name=product["name"], as_of=None, source="Yahoo Finance",
        source_url=f"https://finance.yahoo.com/quote/{product['symbol']}/holdings/",
        rows=rows,
    )


def fetch_sector_allocation(isin: str) -> EtfSectorAllocationResponse:
    """Fetch one supported external fund, caching successful answers for six hours."""
    isin = isin.strip().upper()
    product = _PRODUCTS[isin]
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _CACHE.get(isin)
        if cached and now - cached[0] < _CACHE_TTL_SECONDS:
            return cached[1]

    provider = product["provider"]
    try:
        if provider == "ishares":
            allocation = _fetch_ishares(isin, product)
        elif provider == "justetf":
            allocation = _fetch_justetf(isin)
        elif provider in ("yahoo", "yahoo_mutual_fund"):
            allocation = _fetch_yahoo(isin, product)
        else:  # pragma: no cover - registry construction makes this impossible
            raise ValueError(f"unknown fund sector provider {provider}")
    except Exception as exc:
        if product.get("fallback") != "justetf":
            raise
        _log.warning("[etf sectors] iShares failed for %s (%s: %s); trying justETF",
                     isin, type(exc).__name__, exc)
        allocation = _fetch_justetf(isin)

    with _CACHE_LOCK:
        _CACHE[isin] = (now, allocation)
    return allocation
