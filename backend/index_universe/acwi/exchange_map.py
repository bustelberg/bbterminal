"""iShares ↔ GuruFocus exchange mapping + ticker normalization.

The iShares fund file labels each listing by its iShares exchange name
("Tokyo Stock Exchange", "Xetra", …). GuruFocus uses short prefix codes
("TSE", "XTER", …) that prefix the symbol in URLs and DB rows. This
module owns the translation in both directions plus per-exchange ticker
quirks (Hong Kong zero-padding, Istanbul .E suffix, etc.) and the
cross-exchange override table at `gf_ticker_overrides.json`.
"""
from __future__ import annotations

import json
import os


_US_EXCHANGES = {"NYSE", "NASDAQ", "AMEX", "CBOE BZX"}

# Map iShares exchange names → GuruFocus exchange prefixes
_ISHARES_TO_GF: dict[str, str] = {
    # US — no prefix needed
    "NYSE": "",
    "NASDAQ": "",
    "Cboe BZX": "",
    # Europe
    "London Stock Exchange": "LSE",
    "Xetra": "XTER",
    "Nyse Euronext - Euronext Paris": "XPAR",
    "Nyse Euronext - Euronext Amsterdam": "XAMS",
    "Euronext Amsterdam": "XAMS",
    "Nyse Euronext - Euronext Brussels": "XBRU",
    "Nyse Euronext - Euronext Lisbon": "XLIS",
    "Borsa Italiana": "MIL",
    "Bolsa De Madrid": "XMAD",
    "SIX Swiss Exchange": "XSWX",
    "Nasdaq Omx Nordic": "OSTO",
    "Omx Nordic Exchange Copenhagen A/S": "OCSE",
    "Oslo Bors Asa": "OSL",
    "Nasdaq Omx Helsinki Ltd.": "OHEL",
    "Warsaw Stock Exchange/Equities/Main Market": "WAR",
    "Wiener Boerse Ag": "WBO",
    "Athens Exchange S.A. Cash Market": "ATH",
    "Irish Stock Exchange - All Market": "DUB",
    "Budapest Stock Exchange": "BUD",
    "Prague Stock Exchange": "XPRA",
    "Istanbul Stock Exchange": "IST",
    # Americas
    "Toronto Stock Exchange": "TSX",
    "Bolsa Mexicana De Valores": "MEX",
    "XBSP": "BSP",
    "Santiago Stock Exchange": "XSGO",
    "Bolsa De Valores De Colombia": "BOG",
    # Asia-Pacific
    "Tokyo Stock Exchange": "TSE",
    "Hong Kong Exchanges And Clearing Ltd": "HKSE",
    "Shanghai Stock Exchange": "SHSE",
    "Shenzhen Stock Exchange": "SZSE",
    "Taiwan Stock Exchange": "TPE",
    "Gretai Securities Market": "ROCO",
    "Korea Exchange (Stock Market)": "XKRX",
    "Korea Exchange (Kosdaq)": "XKRX",
    "National Stock Exchange Of India": "NSE",
    "Bse Ltd": "BSE",
    "Asx - All Markets": "ASX",
    "New Zealand Exchange Ltd": "NZSE",
    "Singapore Exchange": "SGX",
    "Bursa Malaysia": "XKLS",
    "Indonesia Stock Exchange": "ISX",
    "Stock Exchange Of Thailand": "BKK",
    "Philippine Stock Exchange Inc.": "PHS",
    # Middle East / Africa
    "Saudi Stock Exchange": "SAU",
    "Abu Dhabi Securities Exchange": "ADX",
    "Dubai Financial Market": "DFM",
    "Qatar Exchange": "DSMD",
    "Kuwait Stock Exchange": "KUW",
    "Tel Aviv Stock Exchange": "TASE",
    "Johannesburg Stock Exchange": "JSE",
    "Egyptian Exchange": "CAI",
    # Russia
    "Standard-Classica-Forts": "MCX",
}

# Map URL-style GF codes to exchange_list API codes (for DB currency lookup)
_GF_URL_TO_API: dict[str, str] = {
    "MCX": "MIC",
    "TASE": "XTAE",
}


def gurufocus_exchange(exchange: str) -> str | None:
    """Return the GuruFocus exchange code for an iShares exchange name.

    Returns empty string for US exchanges (no prefix needed), None if unknown.
    """
    return _ISHARES_TO_GF.get(exchange)


def gurufocus_exchange_for_db(exchange: str) -> str | None:
    """Return the exchange_currency DB code for an iShares exchange name.

    Maps through _ISHARES_TO_GF first, then converts URL codes to API codes.
    """
    gf = _ISHARES_TO_GF.get(exchange)
    if gf is None:
        return None
    if gf == "":
        # US exchanges — match the codes seeded in `gurufocus_exchange`
        # (see supabase/migrations/20260418000000_normalized_schema.sql:128).
        us_map = {"NYSE": "NYSE", "NASDAQ": "NASDAQ", "Cboe BZX": "CBOE"}
        return us_map.get(exchange, "NYSE")
    return _GF_URL_TO_API.get(gf, gf)


def expected_db_exchange_codes() -> set[str]:
    """Every db-side exchange_code acwi can emit for an iShares holding.

    Used by main.py at startup to diff against `gurufocus_exchange.exchange_code`
    so silent skips like the MSFT/NASDAQ regression fail loudly next time.
    """
    codes: set[str] = set()
    for ishares_name in _ISHARES_TO_GF.keys():
        c = gurufocus_exchange_for_db(ishares_name)
        if c:
            codes.add(c)
    return codes


# JSON config / cache files stay in index_universe/ (parent of this package).
_DATA_DIR = os.path.dirname(os.path.dirname(__file__))
_GF_TICKER_OVERRIDES_FILE = os.path.join(_DATA_DIR, "gf_ticker_overrides.json")
_GF_TICKER_OVERRIDES_CACHE: dict[str, dict[str, str]] | None = None


def _load_gf_ticker_overrides() -> dict[str, dict[str, str]]:
    """Load {gf_exchange_prefix: {ishares_ticker: gurufocus_ticker}} map."""
    global _GF_TICKER_OVERRIDES_CACHE
    if _GF_TICKER_OVERRIDES_CACHE is not None:
        return _GF_TICKER_OVERRIDES_CACHE
    try:
        if os.path.exists(_GF_TICKER_OVERRIDES_FILE):
            with open(_GF_TICKER_OVERRIDES_FILE, "r", encoding="utf-8") as f:
                _GF_TICKER_OVERRIDES_CACHE = json.load(f)
        else:
            _GF_TICKER_OVERRIDES_CACHE = {}
    except Exception:
        _GF_TICKER_OVERRIDES_CACHE = {}
    return _GF_TICKER_OVERRIDES_CACHE


def _resolve_ticker_override(ticker: str, gf_prefix: str) -> tuple[str | None, str]:
    """Look up an override for (ticker, gf_prefix) in gf_ticker_overrides.json.

    Returns (override_gf_prefix, ticker). `override_gf_prefix` is None if no
    cross-exchange remap (the original gf_prefix should be used). Override
    values may be either:
      - a plain string (ticker rename, same exchange), or
      - a dict {"exchange": "FRA", "ticker": "6R9"} (cross-exchange remap),
        where missing fields fall back to the originals.
    """
    override = _load_gf_ticker_overrides().get(gf_prefix, {}).get(ticker)
    if isinstance(override, dict):
        return (override.get("exchange") or gf_prefix, override.get("ticker") or ticker)
    if isinstance(override, str):
        return (None, override)
    return (None, ticker)


def _normalize_gf_ticker(ticker: str, gf_prefix: str) -> tuple[str, str]:
    """Apply ticker overrides + exchange-specific normalizations.

    Returns (final_gf_prefix, normalized_ticker). The prefix may differ from
    the input when an override remaps the listing to a different exchange.
    """
    new_prefix, t = _resolve_ticker_override(ticker, gf_prefix)
    final_prefix = new_prefix if new_prefix is not None else gf_prefix
    if final_prefix == "HKSE" and t.isdigit():
        t = t.zfill(5)
    if final_prefix == "IST" and t.endswith(".E"):
        t = t[:-2]
    if final_prefix == "BKK" and t.endswith(".R"):
        t = t[:-2]
    if final_prefix == "XSGO":
        t = t.replace(".", "-")
    return (final_prefix, t)


def gurufocus_url(ticker: str, exchange: str) -> str | None:
    """Build a GuruFocus summary URL for a holding.

    Returns None if the exchange is unknown or the ticker is empty.
    """
    if not ticker or ticker == "--":
        return None

    gf_prefix = _ISHARES_TO_GF.get(exchange)
    if gf_prefix is None:
        return None

    final_prefix, t = _normalize_gf_ticker(ticker, gf_prefix)
    symbol = t if final_prefix == "" else f"{final_prefix}:{t}"
    return f"https://www.gurufocus.com/stock/{symbol}/summary"


_SKIP_LISTINGS: frozenset[tuple[str, str]] = frozenset({
    ("HKSE", "3750"),
    ("XSWX", "LISN"),
})


def gurufocus_ticker_normalized(ticker: str, exchange: str) -> tuple[str, str] | None:
    """Return (db_exchange_code, gf_ticker) for an iShares (ticker, exchange).

    Uses the DB-API exchange code (e.g. NYSE/NAS for US, HKSE/TSE/etc abroad)
    so it can be matched against the `company` table. Honors cross-exchange
    overrides — when a stock's GF listing lives on a different exchange than
    iShares reports (e.g. Verisure: OSTO → FRA), the returned db_exchange is
    the override target, not the iShares-derived one.
    Returns None if the exchange is unknown or ticker is empty.
    """
    if not ticker or ticker == "--":
        return None
    gf_prefix = _ISHARES_TO_GF.get(exchange)
    if gf_prefix is None:
        return None
    if (gf_prefix, ticker) in _SKIP_LISTINGS:
        return None
    final_prefix, t = _normalize_gf_ticker(ticker, gf_prefix)
    if final_prefix != gf_prefix:
        # Override remapped to a different exchange — use the override prefix
        # directly as the DB code (no _GF_URL_TO_API conversion since the
        # override author chose the prefix deliberately).
        db_exchange = _GF_URL_TO_API.get(final_prefix, final_prefix)
    else:
        db_exchange = gurufocus_exchange_for_db(exchange)
    if db_exchange is None:
        return None
    return (db_exchange, t)


# GuruFocus exchange prefixes considered "feasible" — the regions covered by
# the current GuruFocus subscription: USA + Europe + Asia (incl. Middle East),
# excluding Russia / AU / NZ / Africa / LatAm. Mirror of the frontend's
# FEASIBLE_GF_EXCHANGES set. Empty string = US.
FEASIBLE_GF_EXCHANGES = frozenset([
    "",  # US (NYSE, NASDAQ, Cboe BZX)
    # Europe
    "LSE", "XTER", "XPAR", "XAMS", "XBRU", "XLIS", "MIL", "XMAD", "XSWX",
    "OSTO", "OCSE", "OSL", "OHEL", "WAR", "XPRA", "ATH", "DUB", "BUD", "IST",
    # Asia (East / SE / South)
    "TSE", "HKSE", "SHSE", "SZSE", "TPE", "ROCO", "XKRX",
    "NSE", "BSE", "SGX", "XKLS", "ISX", "BKK", "PHS",
    # Middle East
    "SAU", "DSMD", "KUW", "XTAE", "ADX", "DFM",
])
