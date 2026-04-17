from __future__ import annotations

import json
import os
from pathlib import Path

import requests

_OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
_BATCH_SIZE = 100

# OpenFIGI exchCode → our gurufocus_exchange code.
# Unknown codes are kept as-is (still better than UNKNOWN).
_EXCHCODE_MAP: dict[str, str] = {
    # OpenFIGI exchCode → GuruFocus exchange name
    # Verified against GuruFocus API (404 = wrong name, 200/403 = correct)
    #
    # US exchanges
    "US": "NYSE",
    "UW": "NASDAQ",
    "UN": "NYSE",
    "UA": "NYSE",       # NYSE American (AMEX)
    "UP": "NYSE",       # NYSE Arca
    "UR": "NYSE",       # NYSE Arca
    "UQ": "NASDAQ",
    # Europe — GuruFocus uses MIC codes for most European exchanges
    "LN": "LSE",
    "GY": "XTER",       # Germany (XETRA) → GuruFocus uses XTER
    "GF": "FRA",        # Frankfurt
    "GR": "XTER",       # German regional → XTER
    "NA": "XAMS",       # Amsterdam
    "FP": "XPAR",       # Paris
    "BB": "XBRU",       # Brussels
    "SM": "XMAD",       # Madrid
    "IM": "MIL",        # Milan
    "DC": "OCSE",       # Copenhagen
    "NO": "OSL",        # Oslo
    "ST": "OSTO",       # Stockholm
    "HB": "OSTO",       # Stockholm (alt)
    "FH": "OHEL",       # Helsinki
    "VX": "XSWX",       # Swiss
    "PW": "WAR",        # Warsaw
    "AV": "XPRA",       # Vienna → GuruFocus uses XPRA
    # Americas
    "CN": "TSX",        # Toronto
    "CT": "TSX",        # Toronto (alt)
    "MF": "BMV",        # Mexico
    "MX": "BMV",
    # Asia-Pacific
    "TT": "TSE",        # Tokyo
    "HK": "HKSE",       # Hong Kong — GuruFocus uses HKSE
    "AU": "ASX",
    "NZ": "NZSE",       # New Zealand
    "SS": "SSE",        # Shanghai
    "SZ": "SZSE",       # Shenzhen
    "KS": "XKRX",       # Korea — GuruFocus uses XKRX
    "TW": "TWSE",       # Taiwan
    "IN": "NSE",        # India
    "JT": "JSE",        # Johannesburg
}


def _exchcode_to_exchange(code: str | None) -> str:
    if not code:
        return "UNKNOWN"
    return _EXCHCODE_MAP.get(code, code)


# Nordic exchanges where share classes use a space (e.g., "NOVO B", "ATCO A")
_NORDIC_EXCHANGES = {"OSTO", "OCSE", "OHEL", "OSL"}


def _normalize_ticker_for_gurufocus(ticker: str, exchange: str) -> str:
    """Normalize ticker for GuruFocus API compatibility.

    Nordic share classes: dots/dashes before a single letter become spaces.
    E.g., "NOVO.B" -> "NOVO B", "ATCO-A" -> "ATCO A".
    """
    import re
    if exchange in _NORDIC_EXCHANGES:
        # "NOVO.B" or "NOVO-B" -> "NOVO B"
        m = re.match(r'^(.+)[.\-]([A-Z])$', ticker)
        if m:
            return f"{m.group(1)} {m.group(2)}"
    return ticker


def detect_unknown_tickers(
    df,
    *,
    fill_path: Path | None = None,
    db_overrides: list[dict] | None = None,
) -> list[dict]:
    """
    Return tickers in df not covered by fill_ticker.json or db_overrides.
    Each entry: {ticker, country, exchange}.
    """
    import pandas as pd

    if fill_path is None:
        fill_path = Path(__file__).resolve().parent / "fill_ticker.json"

    def _norm(t: str) -> str:
        return t.upper().replace("-", ".").replace(" ", ".")

    known: set[str] = set()
    if fill_path.exists():
        data = json.loads(fill_path.read_text(encoding="utf-8"))
        known |= {_norm(row["ticker"]) for row in data if "ticker" in row}
    if db_overrides:
        known |= {_norm(row["ticker"]) for row in db_overrides}

    unknowns: list[dict] = []
    seen: set[str] = set()
    for _, row in df.iterrows():
        ticker = str(row.get("ticker", "") or "").strip()
        if not ticker or _norm(ticker) in known or _norm(ticker) in seen:
            continue
        seen.add(_norm(ticker))
        unknowns.append({
            "ticker": ticker,
            "country": str(row.get("country", "") or "").strip(),
            "exchange": str(row.get("exchange", "") or "").strip(),
        })
    return unknowns


def _best_match(results: list[dict]) -> dict | None:
    """Pick the best equity match from an OpenFIGI data array."""
    if not results:
        return None
    for r in results:
        if r.get("securityType") in ("Common Stock", "Ordinary Shares"):
            return r
    return results[0]


# Map country names from LongEquity to OpenFIGI exchCode hints.
# This helps OpenFIGI return the correct exchange for ambiguous tickers.
_COUNTRY_TO_EXCHCODE: dict[str, str] = {
    "USA": "US",
    "United States": "US",
    "Canada": "CN",
    "UK": "LN",
    "United Kingdom": "LN",
    "Germany": "GY",
    "France": "FP",
    "Netherlands": "NA",
    "Belgium": "BB",
    "Spain": "SM",
    "Italy": "IM",
    "Denmark": "DC",
    "Norway": "NO",
    "Sweden": "ST",
    "Finland": "FH",
    "Switzerland": "VX",
    "Poland": "PW",
    "Austria": "AV",
    "Japan": "TT",
    "Hong Kong": "HK",
    "Australia": "AU",
    "New Zealand": "NZ",
    "South Korea": "KS",
    "Taiwan": "TW",
    "India": "IN",
    "Mexico": "MF",
    "Brazil": "BZ",
    "South Africa": "JT",
}


def resolve_via_openfigi(unknowns: list[dict]) -> list[dict]:
    """
    Resolve unknown tickers via the OpenFIGI API.
    Returns list of {ticker, gurufocus_ticker, gurufocus_exchange, source} for each resolved ticker.
    Unresolvable tickers are silently skipped.
    Set OPENFIGI_API_KEY env var for higher rate limits (optional).
    """
    if not unknowns:
        return []

    api_key = os.environ.get("OPENFIGI_API_KEY")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    resolved: list[dict] = []

    for i in range(0, len(unknowns), _BATCH_SIZE):
        batch = unknowns[i : i + _BATCH_SIZE]
        jobs = []
        for u in batch:
            job: dict = {"idType": "TICKER", "idValue": u["ticker"].replace("-", " ")}
            # Use country hint to guide OpenFIGI to the right exchange
            country = u.get("country", "").strip()
            exchcode_hint = _COUNTRY_TO_EXCHCODE.get(country)
            if exchcode_hint:
                job["exchCode"] = exchcode_hint
            jobs.append(job)

        resp = requests.post(_OPENFIGI_URL, json=jobs, headers=headers, timeout=30)
        resp.raise_for_status()
        items = resp.json()

        for u, item in zip(batch, items):
            if "data" not in item or not item["data"]:
                # If country-scoped search returned nothing, retry without exchCode
                country = u.get("country", "").strip()
                if _COUNTRY_TO_EXCHCODE.get(country):
                    retry_job = [{"idType": "TICKER", "idValue": u["ticker"].replace("-", " ")}]
                    try:
                        retry_resp = requests.post(_OPENFIGI_URL, json=retry_job, headers=headers, timeout=30)
                        retry_resp.raise_for_status()
                        retry_items = retry_resp.json()
                        if retry_items and "data" in retry_items[0] and retry_items[0]["data"]:
                            item = retry_items[0]
                        else:
                            continue
                    except Exception:
                        continue
                else:
                    continue
            match = _best_match(item["data"])
            if not match:
                continue
            exchange = _exchcode_to_exchange(match.get("exchCode"))
            raw_ticker = match.get("ticker") or u["ticker"]
            gf_ticker = _normalize_ticker_for_gurufocus(raw_ticker, exchange)
            resolved.append({
                "ticker": u["ticker"],
                "gurufocus_ticker": gf_ticker,
                "gurufocus_exchange": exchange,
                "source": "openfigi",
            })

    return resolved
