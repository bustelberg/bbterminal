"""Fetch daily FX rates from the ECB Statistical Data Warehouse.

ECB provides free daily rates for ~30 currencies against EUR.
API docs: https://data-api.ecb.europa.eu/

Also handles:
- Pegged currencies (AED, SAR, QAR, KWD) derived via USD peg + ECB USD/EUR
- TWD via Yahoo Finance
"""

from __future__ import annotations

import csv
import io
import json
import logging
from datetime import date, datetime, timedelta

import requests

log = logging.getLogger(__name__)

_ECB_BASE = "https://data-api.ecb.europa.eu/service/data/EXR"

# All active ECB currencies (as of Apr 2026)
ECB_CURRENCIES = [
    "AUD", "BRL", "CAD", "CHF", "CNY", "CZK", "DKK", "GBP", "HKD", "HUF",
    "IDR", "ILS", "INR", "ISK", "JPY", "KRW", "MXN", "MYR", "NOK", "NZD",
    "PHP", "PLN", "RON", "SEK", "SGD", "THB", "TRY", "USD", "ZAR",
]


def fetch_ecb_latest() -> list[dict]:
    """Fetch the latest daily rate for every active ECB currency.

    Returns a list of dicts with keys: currency, rate, date.
    Rate = how many units of <currency> per 1 EUR.
    """
    keys = "+".join(ECB_CURRENCIES)
    url = f"{_ECB_BASE}/D.{keys}.EUR.SP00.A?lastNObservations=1&format=csvdata"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(resp.text))
    results = []
    for row in reader:
        try:
            code = row["CURRENCY"]
            info = CURRENCY_INFO.get(code)
            results.append({
                "currency": code,
                "name": info[0] if info else code,
                "country": info[1] if info else "",
                "rate": float(row["OBS_VALUE"]),
                "date": row["TIME_PERIOD"],
            })
        except (KeyError, ValueError) as e:
            log.warning("Skipping ECB row: %s", e)
    return sorted(results, key=lambda r: r["currency"])


def fetch_ecb_history(currency: str, start_date: str | None = None) -> list[dict]:
    """Fetch daily historical rates for a single currency vs EUR.

    Args:
        currency: ISO currency code (e.g. 'USD')
        start_date: Optional ISO date string (e.g. '2015-01-01') to limit history

    Returns list of dicts with keys: date, rate.
    """
    url = f"{_ECB_BASE}/D.{currency}.EUR.SP00.A?format=csvdata"
    if start_date:
        url += f"&startPeriod={start_date}"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(resp.text))
    results = []
    for row in reader:
        try:
            results.append({
                "date": row["TIME_PERIOD"],
                "rate": float(row["OBS_VALUE"]),
            })
        except (KeyError, ValueError):
            continue
    return sorted(results, key=lambda r: r["date"])


# ---------------------------------------------------------------------------
# Pegged currencies — fixed rate to USD
# ---------------------------------------------------------------------------

_USD_PEGS: dict[str, float] = {
    "AED": 3.6725,   # since 1997
    "SAR": 3.75,     # since 1986
    "QAR": 3.64,     # since 2001
    "KWD": 0.306,    # basket peg, ~stable vs USD
}


def _derive_pegged_latest(ecb_rates: list[dict]) -> list[dict]:
    """Derive latest EUR rates for pegged currencies using USD/EUR rate."""
    usd_rate = next((r for r in ecb_rates if r["currency"] == "USD"), None)
    if not usd_rate:
        return []
    usd_eur = usd_rate["rate"]  # USD per 1 EUR
    results = []
    for code, peg in sorted(_USD_PEGS.items()):
        # peg = units of local currency per 1 USD
        # rate = units of local currency per 1 EUR = peg * usd_eur
        info = CURRENCY_INFO.get(code)
        results.append({
            "currency": code,
            "name": info[0] if info else code,
            "country": info[1] if info else "",
            "rate": round(peg * usd_eur, 4),
            "date": usd_rate["date"],
            "source": "pegged",
        })
    return results


def _derive_pegged_history(currency: str, start_date: str | None = None) -> list[dict]:
    """Derive historical EUR rates for a pegged currency from ECB USD/EUR history."""
    peg = _USD_PEGS.get(currency)
    if peg is None:
        return []
    usd_history = fetch_ecb_history("USD", start_date)
    return [{"date": h["date"], "rate": round(peg * h["rate"], 4)} for h in usd_history]


# ---------------------------------------------------------------------------
# TWD via Yahoo Finance
# ---------------------------------------------------------------------------

_YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"


def _fetch_twd_latest() -> dict | None:
    """Fetch latest TWD/EUR rate from Yahoo Finance."""
    url = f"{_YAHOO_BASE}/TWDEUR=X?range=5d&interval=1d"
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        data = resp.json()
        result = data["chart"]["result"][0]
        closes = result["indicators"]["quote"][0]["close"]
        timestamps = result["timestamp"]
        # Find last valid close (TWD per 1 EUR = 1 / close since Yahoo gives EUR per 1 TWD)
        for i in range(len(closes) - 1, -1, -1):
            if closes[i] is not None:
                eur_per_twd = closes[i]
                twd_per_eur = round(1.0 / eur_per_twd, 4)
                dt = datetime.utcfromtimestamp(timestamps[i]).strftime("%Y-%m-%d")
                info = CURRENCY_INFO["TWD"]
                return {
                    "currency": "TWD",
                    "name": info[0],
                    "country": info[1],
                    "rate": twd_per_eur,
                    "date": dt,
                    "source": "yahoo",
                }
    except Exception as e:
        log.warning("Failed to fetch TWD from Yahoo: %s", e)
    return None


def _fetch_twd_history(start_date: str | None = None) -> list[dict]:
    """Fetch TWD/EUR daily history from Yahoo Finance."""
    # Yahoo max range for daily data
    start = start_date or "2000-01-01"
    period1 = int(datetime.strptime(start, "%Y-%m-%d").timestamp())
    period2 = int(datetime.now().timestamp())
    url = f"{_YAHOO_BASE}/TWDEUR=X?period1={period1}&period2={period2}&interval=1d"
    try:
        resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        data = resp.json()
        result = data["chart"]["result"][0]
        closes = result["indicators"]["quote"][0]["close"]
        timestamps = result["timestamp"]
        results = []
        for ts, close in zip(timestamps, closes):
            if close is not None and close > 0:
                dt = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                results.append({"date": dt, "rate": round(1.0 / close, 4)})
        return sorted(results, key=lambda r: r["date"])
    except Exception as e:
        log.warning("Failed to fetch TWD history from Yahoo: %s", e)
        return []


# ---------------------------------------------------------------------------
# Unified fetch functions (ECB + pegged + TWD)
# ---------------------------------------------------------------------------

def fetch_all_latest() -> list[dict]:
    """Fetch latest rates for all currencies: ECB + pegged + TWD."""
    ecb = fetch_ecb_latest()
    pegged = _derive_pegged_latest(ecb)
    twd = _fetch_twd_latest()
    all_rates = ecb + pegged
    if twd:
        all_rates.append(twd)
    # Tag ECB rates with source
    for r in ecb:
        r.setdefault("source", "ecb")
    return sorted(all_rates, key=lambda r: r["currency"])


def fetch_history(currency: str, start_date: str | None = None) -> list[dict]:
    """Fetch daily history for any supported currency vs EUR."""
    if currency in _USD_PEGS:
        return _derive_pegged_history(currency, start_date)
    if currency == "TWD":
        return _fetch_twd_history(start_date)
    return fetch_ecb_history(currency, start_date)


# ---------------------------------------------------------------------------
# Currency metadata: (name, country/region)
CURRENCY_INFO: dict[str, tuple[str, str]] = {
    "AED": ("UAE Dirham", "United Arab Emirates"),
    "AUD": ("Australian Dollar", "Australia"),
    "BRL": ("Brazilian Real", "Brazil"),
    "CAD": ("Canadian Dollar", "Canada"),
    "CHF": ("Swiss Franc", "Switzerland"),
    "CLP": ("Chilean Peso", "Chile"),
    "CNY": ("Chinese Yuan", "China"),
    "COP": ("Colombian Peso", "Colombia"),
    "CZK": ("Czech Koruna", "Czech Republic"),
    "DKK": ("Danish Krone", "Denmark"),
    "EGP": ("Egyptian Pound", "Egypt"),
    "EUR": ("Euro", "Eurozone"),
    "GBP": ("British Pound", "United Kingdom"),
    "HKD": ("Hong Kong Dollar", "Hong Kong"),
    "HUF": ("Hungarian Forint", "Hungary"),
    "IDR": ("Indonesian Rupiah", "Indonesia"),
    "ILS": ("Israeli Shekel", "Israel"),
    "INR": ("Indian Rupee", "India"),
    "ISK": ("Icelandic Krona", "Iceland"),
    "JPY": ("Japanese Yen", "Japan"),
    "KRW": ("South Korean Won", "South Korea"),
    "KWD": ("Kuwaiti Dinar", "Kuwait"),
    "MXN": ("Mexican Peso", "Mexico"),
    "MYR": ("Malaysian Ringgit", "Malaysia"),
    "NOK": ("Norwegian Krone", "Norway"),
    "NZD": ("New Zealand Dollar", "New Zealand"),
    "PHP": ("Philippine Peso", "Philippines"),
    "PLN": ("Polish Zloty", "Poland"),
    "QAR": ("Qatari Riyal", "Qatar"),
    "RON": ("Romanian Leu", "Romania"),
    "RUB": ("Russian Ruble", "Russia"),
    "SAR": ("Saudi Riyal", "Saudi Arabia"),
    "SEK": ("Swedish Krona", "Sweden"),
    "SGD": ("Singapore Dollar", "Singapore"),
    "THB": ("Thai Baht", "Thailand"),
    "TRY": ("Turkish Lira", "Turkey"),
    "TWD": ("New Taiwan Dollar", "Taiwan"),
    "USD": ("US Dollar", "United States"),
    "ZAR": ("South African Rand", "South Africa"),
}


# iShares exchange name → local trading currency
_EXCHANGE_TO_CURRENCY: dict[str, str] = {
    "Abu Dhabi Securities Exchange": "AED",
    "Asx - All Markets": "AUD",
    "Athens Exchange S.A. Cash Market": "EUR",
    "Bolsa De Madrid": "EUR",
    "Bolsa De Valores De Colombia": "COP",
    "Bolsa Mexicana De Valores": "MXN",
    "Borsa Italiana": "EUR",
    "Bse Ltd": "INR",
    "Budapest Stock Exchange": "HUF",
    "Bursa Malaysia": "MYR",
    "Cboe BZX": "USD",
    "Dubai Financial Market": "AED",
    "Egyptian Exchange": "EGP",
    "Euronext Amsterdam": "EUR",
    "Gretai Securities Market": "TWD",
    "Hong Kong Exchanges And Clearing Ltd": "HKD",
    "Indonesia Stock Exchange": "IDR",
    "Irish Stock Exchange - All Market": "EUR",
    "Istanbul Stock Exchange": "TRY",
    "Johannesburg Stock Exchange": "ZAR",
    "Korea Exchange (Kosdaq)": "KRW",
    "Korea Exchange (Stock Market)": "KRW",
    "Kuwait Stock Exchange": "KWD",
    "London Stock Exchange": "GBP",
    "NASDAQ": "USD",
    "NO MARKET (E.G. UNLISTED)": "USD",
    "NYSE": "USD",
    "Nasdaq Omx Helsinki Ltd.": "EUR",
    "Nasdaq Omx Nordic": "SEK",
    "National Stock Exchange Of India": "INR",
    "New Zealand Exchange Ltd": "NZD",
    "Nyse Euronext - Euronext Brussels": "EUR",
    "Nyse Euronext - Euronext Lisbon": "EUR",
    "Nyse Euronext - Euronext Paris": "EUR",
    "Omx Nordic Exchange Copenhagen A/S": "DKK",
    "Oslo Bors Asa": "NOK",
    "Philippine Stock Exchange Inc.": "PHP",
    "Prague Stock Exchange": "CZK",
    "Qatar Exchange": "QAR",
    "SIX Swiss Exchange": "CHF",
    "Santiago Stock Exchange": "CLP",
    "Saudi Stock Exchange": "SAR",
    "Shanghai Stock Exchange": "CNY",
    "Shenzhen Stock Exchange": "CNY",
    "Singapore Exchange": "SGD",
    "Standard-Classica-Forts": "RUB",
    "Stock Exchange Of Thailand": "THB",
    "Taiwan Stock Exchange": "TWD",
    "Tel Aviv Stock Exchange": "ILS",
    "Tokyo Stock Exchange": "JPY",
    "Toronto Stock Exchange": "CAD",
    "Warsaw Stock Exchange/Equities/Main Market": "PLN",
    "Wiener Boerse Ag": "EUR",
    "XBSP": "BRL",
    "Xetra": "EUR",
}


def exchange_to_currency(exchange: str) -> str | None:
    """Map an iShares exchange name to its local trading currency."""
    return _EXCHANGE_TO_CURRENCY.get(exchange)


def get_coverage_info() -> dict:
    """Compare ACWI local currencies against ECB availability.

    Derives the local trading currency from each holding's exchange,
    since the iShares file reports all prices in USD.
    """
    from index_universe.acwi import load_acwi_holdings
    holdings, _ = load_acwi_holdings()

    # Count holdings per local currency (derived from exchange)
    currency_counts: dict[str, int] = {}
    unmapped_exchanges: dict[str, int] = {}
    for h in holdings:
        exch = h.get("Exchange", "")
        c = _EXCHANGE_TO_CURRENCY.get(exch)
        if c:
            currency_counts[c] = currency_counts.get(c, 0) + 1
        else:
            unmapped_exchanges[exch] = unmapped_exchanges.get(exch, 0) + 1

    acwi_currencies = sorted(currency_counts.keys())
    all_available = set(ECB_CURRENCIES) | set(_USD_PEGS.keys()) | {"TWD"}

    covered = sorted(c for c in acwi_currencies if c in all_available and c != "EUR")
    missing = sorted(c for c in acwi_currencies if c not in all_available and c != "EUR")

    # Build currency info lookup for frontend
    currency_info = {
        code: {"name": info[0], "country": info[1]}
        for code, info in CURRENCY_INFO.items()
    }

    return {
        "ecb_currencies": ECB_CURRENCIES,
        "acwi_currencies": acwi_currencies,
        "currency_counts": currency_counts,
        "currency_info": currency_info,
        "covered": covered,
        "missing": missing,
        "eur_count": currency_counts.get("EUR", 0),
        "unmapped_exchanges": unmapped_exchanges,
    }
