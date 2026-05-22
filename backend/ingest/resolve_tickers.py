from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Callable

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
    "UW": "NASDAQ",     # NASDAQ Global Select Market
    "UN": "NYSE",
    "UA": "NYSE",       # NYSE American (AMEX)
    "UP": "NYSE",       # NYSE Arca
    "UR": "NYSE",       # NYSE Arca
    "UQ": "NASDAQ",     # NASDAQ Global Market
    "MM": "NASDAQ",     # NASDAQ tier (Bloomberg/OpenFIGI returns this for
                        # some ADRs and sub-classes — e.g. Baidu's BIDU ADR
                        # ISIN comes back on "MM" rather than "UW").
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
    "VX": "XSWX",       # Swiss (legacy Bloomberg code; OpenFIGI now returns "SW")
    "SW": "XSWX",       # Swiss (OpenFIGI primary code)
    "PW": "WAR",        # Warsaw
    "AV": "XPRA",       # Vienna → GuruFocus uses XPRA
    # Americas
    "CN": "TSX",        # Toronto
    "CT": "TSX",        # Toronto (alt)
    "MF": "BMV",        # Mexico
    "MX": "BMV",
    # Asia-Pacific
    "TT": "TSE",        # Tokyo (legacy Bloomberg code; OpenFIGI now returns "JP")
    "JP": "TSE",        # Tokyo (OpenFIGI primary code)
    "HK": "HKSE",       # Hong Kong — GuruFocus uses HKSE
    "AU": "ASX",
    "NZ": "NZSE",       # New Zealand
    "SS": "OSTO",       # Stockholm (OpenFIGI primary code — NOT Shanghai;
                        # Bloomberg "SS" = Stockholm, Shanghai is out of
                        # scope per FEASIBLE_GF_EXCHANGES so we don't map it).
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
    """Pick the best equity match from an OpenFIGI data array.

    Preference order:
      1. Common Stock / Ordinary Shares on an exchCode we map in
         `_EXCHCODE_MAP` (primary listings — e.g. NASDAQ "UW",
         SIX "SW", HKEX "HK").
      2. Common Stock / Ordinary Shares on any exchange (fallback for
         exchanges we haven't mapped yet).
      3. First result (last resort).

    This bias is what stops Baidu's ISIN from resolving to the OTC
    sub-class "BIDUN" instead of NASDAQ's "BIDU", or Baloise's ISIN
    from resolving to the German "BLON" instead of SIX's "BALN" —
    OpenFIGI returns all listings per ISIN and we want the primary."""
    if not results:
        return None
    for r in results:
        if r.get("securityType") in ("Common Stock", "Ordinary Shares"):
            if r.get("exchCode") in _EXCHCODE_MAP:
                return r
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
    "Sweden": "SS",     # OpenFIGI primary code (older "ST" returns empty)
    "Finland": "FH",
    "Switzerland": "SW",  # OpenFIGI primary code (older "VX" returns empty)
    "Poland": "PW",
    "Austria": "AV",
    "Japan": "JP",      # OpenFIGI primary code (older "TT" returns empty)
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


def resolve_isins_via_openfigi(
    items: list[dict],
    *,
    on_progress: Callable[[str, int | None], None] | None = None,
    progress_start: int = 65,
    progress_end: int = 72,
) -> list[dict]:
    """
    Resolve ISINs to (gurufocus_ticker, gurufocus_exchange) via OpenFIGI.

    Input items:  [{isin: str, country: str | None}]
    Output items: [{isin, gurufocus_ticker, gurufocus_exchange, source}]

    ISIN is globally unique to an issuer but can map to multiple listings
    (primary + ADRs / secondary venues). The optional `country` hint
    biases OpenFIGI towards the issuer's home listing — without it,
    ASML's ISIN can resolve to the NYSE ADR even though Leonteq tracks
    the Amsterdam primary.

    Emits per-batch progress so the caller's SSE bar doesn't appear
    stalled on hundreds of ISINs (each batch + retry pass can take
    several seconds at OpenFIGI's free-tier rate limits).

    Unresolvable / errored entries are silently skipped (same contract as
    `resolve_via_openfigi`). Caller is expected to handle the gap.
    """
    if not items:
        return []

    api_key = os.environ.get("OPENFIGI_API_KEY")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    def emit(msg: str, pct: int | None = None) -> None:
        if on_progress is None:
            return
        try:
            on_progress(msg, pct)
        except Exception:
            pass

    resolved: list[dict] = []
    total_batches = max(1, (len(items) + _BATCH_SIZE - 1) // _BATCH_SIZE)
    span = max(1, progress_end - progress_start)

    for batch_idx, i in enumerate(range(0, len(items), _BATCH_SIZE)):
        batch = items[i : i + _BATCH_SIZE]
        jobs: list[tuple[dict, dict]] = []
        for u in batch:
            isin = (u.get("isin") or "").strip()
            if not isin:
                continue
            job: dict = {"idType": "ID_ISIN", "idValue": isin}
            country = (u.get("country") or "").strip()
            exchcode_hint = _COUNTRY_TO_EXCHCODE.get(country)
            if exchcode_hint:
                job["exchCode"] = exchcode_hint
            jobs.append((u, job))

        pct = progress_start + min(span, int((batch_idx + 1) * span / total_batches))
        emit(
            f"OpenFIGI ISIN batch {batch_idx + 1}/{total_batches} "
            f"({len(jobs)} ISINs, {len(resolved)} resolved so far)",
            pct,
        )

        if not jobs:
            continue

        try:
            resp = requests.post(
                _OPENFIGI_URL,
                json=[j for (_, j) in jobs],
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            items_resp = resp.json()
        except Exception as e:
            emit(
                f"  batch {batch_idx + 1}/{total_batches} failed "
                f"({type(e).__name__}); continuing.",
                None,
            )
            continue

        retry_needed: list[tuple[dict, int]] = []  # (item, index in items_resp)
        for j_idx, ((u, _job), item) in enumerate(zip(jobs, items_resp)):
            if "data" not in item or not item["data"]:
                country = (u.get("country") or "").strip()
                if _COUNTRY_TO_EXCHCODE.get(country):
                    retry_needed.append((u, j_idx))
                    continue
                continue
            _record_resolution(resolved, u, item)

        # Bundle the country-hint retries into ONE batch call instead of
        # one HTTP request per row. Cuts ~60% off the wall clock when a
        # lot of country hints overfilter.
        if retry_needed:
            emit(
                f"  retrying {len(retry_needed)} without country hint",
                None,
            )
            retry_body = [
                {"idType": "ID_ISIN", "idValue": u["isin"]}
                for (u, _) in retry_needed
            ]
            try:
                retry_resp = requests.post(
                    _OPENFIGI_URL, json=retry_body, headers=headers, timeout=30,
                )
                retry_resp.raise_for_status()
                retry_items = retry_resp.json()
            except Exception as e:
                emit(
                    f"  retry pass failed ({type(e).__name__}); continuing.",
                    None,
                )
                retry_items = []
            for (u, _), retry_item in zip(retry_needed, retry_items):
                if "data" not in retry_item or not retry_item["data"]:
                    continue
                _record_resolution(resolved, u, retry_item)

    emit(
        f"OpenFIGI done: {len(resolved)}/{len(items)} ISINs resolved.",
        progress_end,
    )
    return resolved


def _record_resolution(resolved: list[dict], u: dict, item: dict) -> None:
    """Push a single OpenFIGI match into the `resolved` list."""
    match = _best_match(item["data"])
    if not match:
        return
    raw_ticker = (match.get("ticker") or "").strip()
    if not raw_ticker:
        return
    raw_exch_code = match.get("exchCode") or ""
    exchange = _exchcode_to_exchange(raw_exch_code)
    gf_ticker = _normalize_ticker_for_gurufocus(raw_ticker, exchange)
    resolved.append({
        "isin": u["isin"],
        "gurufocus_ticker": gf_ticker,
        "gurufocus_exchange": exchange,
        # Raw OpenFIGI exchCode preserved for diagnostic purposes —
        # lets the caller distinguish "this code was kept as-is" from
        # "this code came out of _EXCHCODE_MAP" when investigating
        # unmapped exchanges.
        "openfigi_exch_code": raw_exch_code,
        "source": "openfigi-isin",
    })
