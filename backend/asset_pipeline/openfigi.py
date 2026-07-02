"""OpenFIGI ISIN -> identity fallback.

When Yahoo's ISIN search returns nothing (common for some UK/EU ETFs and for
bonds/gilts), OpenFIGI still resolves the ISIN to ticker + name + securityType.
We use the name/ticker to re-search Yahoo, and the securityType to classify
things Yahoo can't price (individual bonds). curl_cffi POST (urllib fallback);
optional OPENFIGI_API_KEY raises the rate limit."""
from __future__ import annotations

import json
import os

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
