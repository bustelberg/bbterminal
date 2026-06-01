"""Forward-walk: resolve MSCI ADDED events that landed AFTER the XLS
snapshot date and synthesize feasible holdings for them.

The XLS holdings file is the only source of (ticker, exchange, sector)
metadata for any holding. For securities added to ACWI after the XLS
was downloaded, the announcement carries only `(country, name)` —
this module fills the gap by:

  1. OpenFIGI `/v3/search` lookup by `(name, country)` to get a
     candidate `(ticker, exchange)`.
  2. GuruFocus API probe (`/stock/{EXCH}:{TICK}/price`) to verify the
     candidate actually points to a real, covered security — the
     OpenFIGI match could otherwise resolve to a delisted sibling or
     the wrong issuer.
  3. Bucket: `resolved` (will be injected into the reconstruction) vs
     `unresolved` (surfaced on /schedule for manual review).

Resolutions are cached to JSON keyed by the MSCI announcement href so
re-runs of the same pipeline tick don't burn API quota.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from urllib.parse import quote

import requests

from ingest._gurufocus_http import cf_get, is_available as _cf_is_available

from .net_additions import compute_constituent_changes
from .reconstruction import _parse_effective_date, _parse_xls_as_of


log = logging.getLogger(__name__)


_DATA_DIR = os.path.dirname(os.path.dirname(__file__))
_RESOLUTION_CACHE_FILE = os.path.join(_DATA_DIR, "msci_forward_resolution.json")

_OPENFIGI_SEARCH_URL = "https://api.openfigi.com/v3/search"
_OPENFIGI_KEY = os.environ.get("OPENFIGI_API_KEY", "")

_GOOD_EQUITY_TYPES = {
    "Common Stock", "Ordinary Shares", "REIT", "Stapled Security",
    "ADR", "GDR", "Depositary Receipt", "Preferred Stock",
}


# MSCI country code → (OpenFIGI exchCode hints, GF exchange prefix).
# "" GF prefix = US (NYSE/NASDAQ go bare in GuruFocus's URL scheme).
_COUNTRY_MAP: dict[str, tuple[list[str], str | None]] = {
    "US": (["UN", "UW", "UR", "UA", "UQ", "UP"], ""),
    "GB": (["LN"], "LSE"),
    "DE": (["GY", "GR", "GF"], "XTER"),
    "FR": (["FP"], "XPAR"),
    "NL": (["NA"], "XAMS"),
    "BE": (["BB"], "XBRU"),
    "ES": (["SM"], "XMAD"),
    "IT": (["IM"], "MIL"),
    "DK": (["DC"], "OCSE"),
    "NO": (["NO"], "OSL"),
    "SE": (["SS", "HB"], "OSTO"),
    "FI": (["FH"], "OHEL"),
    "CH": (["SW", "SE", "VX"], "XSWX"),
    "AT": (["AV"], "XPRA"),
    "IE": (["ID"], "DUB"),
    "PT": (["PL"], "XLIS"),
    "GR": (["GA"], "ATH"),
    "PL": (["PW"], "WAR"),
    "CZ": (["CP"], "XPRA"),
    "HU": (["HB"], "BUD"),
    "TR": (["TI"], "IST"),
    "JP": (["JT", "JP"], "TSE"),
    "HK": (["HK"], "HKSE"),
    "KR": (["KS", "KQ"], "XKRX"),
    "TW": (["TT"], "TPE"),
    "IN": (["IN", "IB"], "NSE"),
    "SG": (["SP"], "SGX"),
    "MY": (["MK"], "XKLS"),
    "TH": (["TB"], "BKK"),
    "ID": (["IJ"], "ISX"),
    "PH": (["PM"], "PHS"),
}


_SUFFIX_CLEAN = re.compile(
    r"\s+("
    r"CORP(?:ORATION)?|INC|LTD|LIMITED|LLC|LLP|PLC|SE|AG|SA|NV|OYJ|AB|ASA|"
    r"HLDG(?:S)?|HOLDINGS?|GROUP|GRP|CO|COMPANY|CIE|SPA|S\.?A\.?|N\.?V\.?|"
    r"BV|GMBH|KGAA|KK"
    r")\.?\s*$",
    re.IGNORECASE,
)
_SHARECLASS = re.compile(r"\s+[A-Z]$")
_PAREN = re.compile(r"\s*\([^)]+\)\s*$")


def _normalize_name(name: str) -> str:
    s = re.sub(r"\s+", " ", name.strip())
    while True:
        new = _PAREN.sub("", s).strip()
        if new == s:
            break
        s = new
    s = _SHARECLASS.sub("", s).strip()
    s = _SUFFIX_CLEAN.sub("", s).strip()
    return s


def _openfigi_search(query: str, exch_code: str | None) -> list[dict]:
    headers = {"Content-Type": "application/json"}
    if _OPENFIGI_KEY:
        headers["X-OPENFIGI-APIKEY"] = _OPENFIGI_KEY
    body: dict = {"query": query}
    if exch_code:
        body["exchCode"] = exch_code
    try:
        r = requests.post(_OPENFIGI_SEARCH_URL, json=body, headers=headers, timeout=15)
        if r.status_code == 429:
            time.sleep(5)
            r = requests.post(_OPENFIGI_SEARCH_URL, json=body, headers=headers, timeout=15)
        if r.status_code != 200:
            return []
        return r.json().get("data", []) or []
    except Exception:
        return []


def _pick_best(results: list[dict], wanted_exch_codes: list[str]) -> dict | None:
    if not results:
        return None
    for r in results:
        if r.get("securityType") in _GOOD_EQUITY_TYPES and r.get("exchCode") in wanted_exch_codes:
            return r
    for r in results:
        if r.get("securityType") in _GOOD_EQUITY_TYPES:
            return r
    return None


def _build_gf_symbol(ticker: str, gf_prefix: str | None) -> str:
    """GuruFocus URL convention: US-listed names go bare, others get an
    exchange prefix. Mirrors `ingest.gurufocus_url`."""
    t = ticker.strip()
    if not gf_prefix:
        return t
    return f"{gf_prefix}:{t}"


def _probe_gurufocus(symbol: str, *, timeout: int = 15) -> tuple[str, str]:
    """Call GuruFocus `/stock/{symbol}/price`. Returns (verdict, detail) where
    verdict is one of:
        valid          200 with parseable price history
        not_found      404 / 200 with empty / non-equity payload — wrong symbol
        paywalled      403 — symbol may exist but GF subscription doesn't cover it
        unreachable    network / Cloudflare / timeout — undetermined, treat as unresolved
    """
    base = os.environ.get("GURUFOCUS_BASE_URL", "").strip().rstrip("/")
    api_key = os.environ.get("GURUFOCUS_API_KEY", "").strip()
    if not base or not api_key:
        return "unreachable", "GURUFOCUS_BASE_URL / GURUFOCUS_API_KEY not set"
    if base.endswith("/data"):
        base = base[: -len("/data")]
    url = f"{base}/public/user/{api_key}/stock/{quote(symbol, safe=':')}/price"

    if not _cf_is_available():
        return "unreachable", "curl_cffi not installed"

    resp = cf_get(url, headers={"Accept": "application/json"}, timeout=timeout)
    if resp.error is not None:
        return "unreachable", resp.error
    status = resp.status_code or 0
    if status == 404:
        return "not_found", f"GuruFocus 404 for {symbol}"
    if status == 403:
        return "paywalled", f"GuruFocus 403 for {symbol} (subscription doesn't cover)"
    if status >= 400:
        return "unreachable", f"GuruFocus {status} for {symbol}"
    body = resp.text or ""
    if not body:
        return "not_found", f"GuruFocus 200 empty body for {symbol}"
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return "not_found", f"GuruFocus 200 non-JSON for {symbol}"
    items = data if isinstance(data, list) else next(
        (v for v in data.values() if isinstance(v, list)), []
    )
    if not items:
        return "not_found", f"GuruFocus 200 empty price array for {symbol}"
    return "valid", f"GuruFocus has {len(items)} price points for {symbol}"


def _load_cache() -> dict:
    try:
        with open(_RESOLUTION_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _save_cache(cache: dict) -> None:
    try:
        with open(_RESOLUTION_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, sort_keys=True)
    except Exception as e:
        log.warning("[forward_additions] cache write failed: %s", e)


def resolve_post_xls_additions(
    xls_as_of_str: str,
) -> tuple[list[dict], list[dict]]:
    """Resolve MSCI ADDED announcements with effective_date > XLS_as_of
    that don't match any existing XLS holding. Returns (resolved, unresolved).

    resolved entries (will be injected into reconstruction):
        {synthetic_ticker, symbol, gf_exchange, gf_ticker, name, country,
         eff_date (date), gf_url, sector, openfigi_match}

    unresolved entries (surfaced to /schedule for manual review):
        {name, country, eff_date (ISO str), reason, gf_url (best guess
         or None), openfigi_candidate (dict or None), msci_href}
    """
    xls_as_of = _parse_xls_as_of(xls_as_of_str)
    if xls_as_of is None:
        log.warning(
            "[forward_additions] could not parse XLS as-of %r — "
            "skipping forward additions",
            xls_as_of_str,
        )
        return [], []

    changes = compute_constituent_changes()
    candidates = []
    for ch in changes:
        if ch.get("action") != "ADDED":
            continue
        if ch.get("matched"):
            continue  # already in XLS; backward walk handles it
        eff_d = _parse_effective_date(ch.get("effective_date") or "")
        if eff_d is None or eff_d <= xls_as_of:
            continue
        country = (ch.get("country") or "").strip().upper()
        if country not in _COUNTRY_MAP:
            continue  # out-of-scope country (Russia, Africa ex-ZA, LatAm, …)
        candidates.append(ch)

    if not candidates:
        return [], []

    cache = _load_cache()
    resolved: list[dict] = []
    unresolved: list[dict] = []
    cache_dirty = False

    for ch in candidates:
        href = ch.get("href") or ""
        name = ch.get("company_name") or ""
        country = (ch.get("country") or "").strip().upper()
        eff_d = _parse_effective_date(ch.get("effective_date") or "")
        eff_iso = eff_d.isoformat() if eff_d else None
        exch_codes, gf_prefix = _COUNTRY_MAP[country]

        cached = cache.get(href)
        if cached and cached.get("eff_date") == eff_iso:
            bucket = cached.get("bucket")
            if bucket == "resolved":
                resolved.append(cached["resolved_entry"])
                continue
            if bucket == "unresolved":
                unresolved.append(cached["unresolved_entry"])
                continue

        query = _normalize_name(name)
        best: dict | None = None
        for hint in exch_codes:
            results = _openfigi_search(query, hint)
            best = _pick_best(results, [hint])
            if best:
                break
        if best is None:
            # Last-ditch: try without exchange hint
            results = _openfigi_search(query, None)
            best = _pick_best(results, exch_codes)

        if best is None or not best.get("ticker"):
            entry = {
                "name": name,
                "country": country,
                "eff_date": eff_iso,
                "reason": "openfigi_no_match",
                "gf_url": None,
                "openfigi_candidate": None,
                "msci_href": href,
            }
            unresolved.append(entry)
            cache[href] = {"eff_date": eff_iso, "bucket": "unresolved", "unresolved_entry": entry}
            cache_dirty = True
            continue

        gf_ticker = best["ticker"]
        symbol = _build_gf_symbol(gf_ticker, gf_prefix)
        gf_url = f"https://www.gurufocus.com/stock/{symbol}/summary"
        verdict, detail = _probe_gurufocus(symbol)

        if verdict == "valid":
            entry = {
                "synthetic_ticker": f"__forward__:{href}",
                "symbol": symbol,
                "gf_exchange": gf_prefix or "NYSE",
                "gf_ticker": gf_ticker,
                "name": name,
                "country": country,
                "eff_date": eff_iso,
                "gf_url": gf_url,
                "sector": None,
                "openfigi_match": {
                    "exch_code": best.get("exchCode"),
                    "security_type": best.get("securityType"),
                    "name": best.get("name"),
                },
            }
            resolved.append(entry)
            cache[href] = {"eff_date": eff_iso, "bucket": "resolved", "resolved_entry": entry}
            cache_dirty = True
        else:
            entry = {
                "name": name,
                "country": country,
                "eff_date": eff_iso,
                "reason": f"gf_{verdict}",
                "gf_url": gf_url,
                "openfigi_candidate": {
                    "exch_code": best.get("exchCode"),
                    "ticker": best.get("ticker"),
                    "name": best.get("name"),
                },
                "msci_href": href,
                "detail": detail,
            }
            unresolved.append(entry)
            cache[href] = {"eff_date": eff_iso, "bucket": "unresolved", "unresolved_entry": entry}
            cache_dirty = True

    if cache_dirty:
        _save_cache(cache)

    log.info(
        "[forward_additions] xls_as_of=%s: %s candidates → %s resolved, %s unresolved",
        xls_as_of, len(candidates), len(resolved), len(unresolved),
    )
    return resolved, unresolved
