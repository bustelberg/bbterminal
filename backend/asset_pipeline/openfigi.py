"""OpenFIGI ISIN -> identity fallback.

When Yahoo's ISIN search returns nothing (common for some UK/EU ETFs and for
bonds/gilts), OpenFIGI still resolves the ISIN to ticker + name + securityType.
We use the name/ticker to re-search Yahoo, and the securityType to classify
things Yahoo can't price (individual bonds). curl_cffi POST (urllib fallback);
optional OPENFIGI_API_KEY raises the rate limit."""
from __future__ import annotations

import json
import os
import time

_URL = "https://api.openfigi.com/v3/mapping"

try:
    from curl_cffi import requests as _creq
    _HAS_CURL = True
except Exception:  # noqa: BLE001
    _HAS_CURL = False


def lookup_isin(isin: str) -> list[dict]:
    """Return OpenFIGI's `data` rows for one ISIN (`{ticker, exchCode, name,
    securityType, ...}`), or [] on miss/error. Best-effort."""
    headers = {"Content-Type": "application/json"}
    key = os.environ.get("OPENFIGI_API_KEY")
    if key:
        headers["X-OPENFIGI-APIKEY"] = key
    jobs = [{"idType": "ID_ISIN", "idValue": isin}]
    try:
        if _HAS_CURL:
            r = _creq.post(_URL, json=jobs, headers=headers, impersonate="chrome", timeout=30)
            item = r.json()[0]
        else:
            from urllib.request import Request, urlopen  # noqa: PLC0415
            req = Request(_URL, data=json.dumps(jobs).encode(), headers=headers, method="POST")
            with urlopen(req, timeout=30) as resp:  # noqa: S310
                item = json.loads(resp.read())[0]
        return (item.get("data") if isinstance(item, dict) else None) or []
    except Exception:  # noqa: BLE001
        return []


def lookup_isins(isins: list[str]) -> dict[str, list[dict]]:
    """Batch ISIN -> OpenFIGI `data` rows. `/v3/mapping` accepts up to 10 jobs per
    request anonymously (100 with `OPENFIGI_API_KEY`), so we chunk accordingly and
    pace between chunks to respect the rate limit (25 req/min anon, 250 with key).
    Best-effort: a failed chunk yields [] for its ISINs. Returns {ISIN: rows} with
    ISINs upper-cased."""
    uniq = list(dict.fromkeys(x.strip().upper() for x in isins if x and x.strip()))
    if not uniq:
        return {}
    headers = {"Content-Type": "application/json"}
    key = os.environ.get("OPENFIGI_API_KEY")
    if key:
        headers["X-OPENFIGI-APIKEY"] = key
    chunk = 100 if key else 10
    pace = 0.3 if key else 2.5  # seconds between chunks — rate-limit guard
    out: dict[str, list[dict]] = {}
    for start in range(0, len(uniq), chunk):
        batch = uniq[start:start + chunk]
        jobs = [{"idType": "ID_ISIN", "idValue": isin} for isin in batch]
        try:
            if _HAS_CURL:
                r = _creq.post(_URL, json=jobs, headers=headers, impersonate="chrome", timeout=30)
                items = r.json()
            else:
                from urllib.request import Request, urlopen  # noqa: PLC0415
                req = Request(_URL, data=json.dumps(jobs).encode(), headers=headers, method="POST")
                with urlopen(req, timeout=30) as resp:  # noqa: S310
                    items = json.loads(resp.read())
        except Exception:  # noqa: BLE001
            items = []
        if isinstance(items, list):
            for isin, item in zip(batch, items):
                out[isin] = (item.get("data") if isinstance(item, dict) else None) or []
        if start + chunk < len(uniq):
            time.sleep(pace)
    return out


def extract_columns(rows: list[dict]) -> dict:
    """Flatten OpenFIGI `data` rows for one ISIN into the 5 grid columns: the top
    row's figi/name/securityType + the unique tickers/exchange-codes joined
    (matches the reference openfigi_check shape). Always returns all 5 keys (None when
    absent) so it can be merged straight into an execution upsert."""
    if not rows:
        return {k: None for k in ("openfigi_figi", "openfigi_name", "openfigi_ticker",
                                  "openfigi_exch", "openfigi_type")}
    top = rows[0]
    tickers = list(dict.fromkeys(r.get("ticker") for r in rows if r.get("ticker")))
    exchs = list(dict.fromkeys(r.get("exchCode") for r in rows if r.get("exchCode")))
    return {
        "openfigi_figi": top.get("figi"),
        "openfigi_name": top.get("name"),
        "openfigi_ticker": ", ".join(tickers) or None,
        "openfigi_exch": ", ".join(exchs) or None,
        "openfigi_type": top.get("securityType"),
    }
