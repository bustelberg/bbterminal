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
from dataclasses import dataclass


# ⚠ `CBOE` AND `CBOE BZX` ARE BOTH HERE BECAUSE BOTH SPELLINGS REACH THIS MODULE. `_build_symbol`
# in `ingest/earnings/_common.py` and `_gf_listing`'s own US set both say `CBOE`; this one said
# only `CBOE BZX`, so a Cboe-listed company was US enough to be addressed as a bare ticker and not
# US enough to be considered covered. No company in the database carries either code today, which
# is the only reason that never showed.
_US_EXCHANGES = {"NYSE", "NASDAQ", "AMEX", "CBOE", "CBOE BZX"}

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
    "Bse Ltd": "BOM",  # Bombay Stock Exchange — GuruFocus/IBKR code is BOM, not BSE
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
      - a dict {"unavailable": true, "reason": "..."} (out-of-scope marker;
        the company still lands in `company` under the iShares-derived
        exchange + ticker so it shows up in /companies, but the ingest
        downstream tags it `out_of_scope_at` and skips it from
        universe_membership + the price phase). See `unavailable_reason`.
    """
    override = _load_gf_ticker_overrides().get(gf_prefix, {}).get(ticker)
    if isinstance(override, dict):
        # Unavailable flag short-circuits the remap path. The company
        # keeps its iShares-derived (gf_prefix, ticker) so the row in
        # `company` is human-readable; out-of-scope status is queried
        # separately via `unavailable_reason`.
        if override.get("unavailable"):
            return (None, ticker)
        return (override.get("exchange") or gf_prefix, override.get("ticker") or ticker)
    if isinstance(override, str):
        return (None, override)
    return (None, ticker)


def unavailable_reason(ticker: str, ishares_exchange: str) -> str | None:
    """Return the out-of-scope reason string for an iShares (ticker, exchange)
    pair, or None if the override doesn't mark it as unavailable.

    Used by the ACWI ingest path to stamp `company.out_of_scope_at` +
    `company.out_of_scope_reason` instead of slotting the company into
    `universe_membership`. The DB columns surface in /companies as an
    amber OUT OF SCOPE badge."""
    gf_prefix = _ISHARES_TO_GF.get(ishares_exchange)
    if gf_prefix is None:
        return None
    override = _load_gf_ticker_overrides().get(gf_prefix, {}).get(ticker)
    if isinstance(override, dict) and override.get("unavailable"):
        return override.get("reason") or "(no reason given)"
    return None


# DB-side exchange codes that share the empty-string outer key in
# `gf_ticker_overrides.json` (US exchanges have no prefix in
# GuruFocus URLs and historically used "" as the iShares-mapped form).
_US_DB_EXCHANGE_CODES = {"NYSE", "NASDAQ", "AMEX", "CBOE"}


@dataclass(frozen=True)
class CompanyOverrideResult:
    """Outcome of applying `gf_ticker_overrides.json` to a (db_exchange,
    ticker) pair already in DB form. Idempotent: when no override
    matches, `target_exchange` and `target_ticker` equal the input and
    `unavailable_reason` is None — callers can no-op on that case.

    Used by both the in-flight ingest paths (ACWI, Leonteq auto-create)
    and the one-shot retroactive sweep that fixes historical rows."""
    target_exchange: str        # may equal input, or be remapped to a different exchange code
    target_ticker: str          # may equal input, or be renamed via the override
    unavailable_reason: str | None  # non-None = mark row out-of-scope (no remap applied in this case)


def apply_company_override(exchange_code: str, ticker: str) -> CompanyOverrideResult:
    """Resolve an override entry for a (db_exchange_code, ticker) pair
    in the form they live in `company` table rows. The single entry
    point any ingest path (ACWI / Leonteq / LongEquity / …) should
    call before inserting a `company` row, AND the one the retroactive
    sweep uses to fix historical bad rows.

    Three possible outcomes:
      1. No override → result equals input, unavailable_reason=None.
      2. Remap → result carries the override's target exchange + ticker
         (either field may equal the input when the override touches
         only one of them).
      3. Unavailable → result equals input + unavailable_reason set.
         The caller should insert the company under the input
         (exchange, ticker) and stamp `out_of_scope_at` +
         `out_of_scope_reason` — DO NOT slot into universe_membership
         + skip from the price phase.

    Note on the US empty-string convention: the override JSON uses ""
    as the outer key for US listings (matches the iShares-mapping
    convention). DB rows use real exchange codes (NYSE/NASDAQ/AMEX/
    CBOE), so we normalize before lookup."""
    if not ticker:
        return CompanyOverrideResult(exchange_code, ticker, None)
    lookup_prefix = "" if exchange_code.upper() in _US_DB_EXCHANGE_CODES else exchange_code
    entry = _load_gf_ticker_overrides().get(lookup_prefix, {}).get(ticker)
    if entry is None:
        return CompanyOverrideResult(exchange_code, ticker, None)
    if isinstance(entry, dict):
        if entry.get("unavailable"):
            return CompanyOverrideResult(
                exchange_code, ticker,
                entry.get("reason") or "(no reason given)",
            )
        # `or` on the strings is intentional for ticker (an empty/missing
        # string falls back to the input). For exchange we mirrored the
        # NASDAQ:GLNG case — explicit "NASDAQ" wins over the input prefix.
        new_exch = entry.get("exchange") if entry.get("exchange") else exchange_code
        new_tick = entry.get("ticker") if entry.get("ticker") else ticker
        return CompanyOverrideResult(new_exch, new_tick, None)
    if isinstance(entry, str):
        return CompanyOverrideResult(exchange_code, entry, None)
    return CompanyOverrideResult(exchange_code, ticker, None)


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
    # US class-share separator: iShares (and Bloomberg) writes
    # "BRK/B" / "BF/B"; GuruFocus uses the dot form. Same rule applies
    # to TSX (Canadian dual-class shares: "RCI/B" → "RCI.B") even
    # though TSX is unsubscribed today — keep the normalization
    # consistent so the company row matches when we get coverage.
    if final_prefix in ("", "NYSE", "NASDAQ", "AMEX", "CBOE", "TSX") and "/" in t:
        t = t.replace("/", ".")
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
    "OTCPK",  # US OTC Pink — GuruFocus prices these (e.g. QinetiQ QNTQF), USD

    # US ETF venues. GuruFocus's own `exchange_list` puts these in region USA:
    #   NAS  NYSE  OTCPK  OTCBB  AMEX  ARCA  IEXG  BATS  GREY
    # ARCA (NYSE Arca) is where most US ETFs actually list — of SPY / IWM / VOO /
    # XLU / EDV / GLD / QQQ, SIX are ARCA and only QQQ is NAS. Omitting it doesn't
    # merely lose coverage, it MIS-RESOLVES: SPY and GLD also list on SGX, which IS
    # in this set, so an unknown ARCA silently hands you the SINGAPORE line of SPY.
    # (OTCBB and GREY are left out deliberately — defunct / grey-market venues we
    # have no reason to price.)
    "ARCA", "BATS", "IEXG",

    # Europe — NOTE: UK (LSE) and Ireland (DUB) are NOT here; GuruFocus
    # returns "403 unsubscribed region" for them (confirmed by probing every
    # exchange), same as India. Continental Europe is covered.
    "XTER", "XPAR", "XAMS", "XBRU", "XLIS", "MIL", "XMAD", "XSWX",
    "OSTO", "OCSE", "OSL", "OHEL", "WAR", "XPRA", "ATH", "BUD", "IST",
    # ⚠⚠ VIENNA AND FRANKFURT WERE MISSING, AND THE OMISSION COST DATA (added 2026-09-01).
    # Every other continental venue was here, so these two read as "unsubscribed" — which meant
    # three companies holding a total of **53,879** `metric_data` rows were treated as outside the
    # subscription: VERBUND AG (39,523), Erste Group Bank (13,894) and Verisure (462). The
    # `refuse_unsubscribed` gate added the same day would have stopped refreshing all three.
    # ⚠ THE EVIDENCE IS VERBUND: its GuruFocus price history and our own independent yfinance
    # series **agree on 75 of 75 periods** (`ingest.earnings.price_sanity`). A region GuruFocus does
    # not sell us cannot produce that — Diploma, genuinely unsubscribed on the LSE, disagreed on 10
    # of 25. So the vendor does cover these; this map did not.
    # ⚠ NOT PROBED LIKE THE ORIGINALS. The rest of this set was built by calling every exchange and
    # reading the 403s; these two are inferred from data we already hold, which costs no quota but
    # is one step weaker. If either ever starts returning "unsubscribed", that is the thing to
    # re-check first.
    "WBO", "FRA",
    # Asia (East / SE / South)
    # NOTE: India (NSE, BOM) is NOT here — GuruFocus returns
    # "403 unsubscribed region [India]" for it, so we have no price/mktcap/ISIN
    # data for Indian listings (they get the UNSUBSCRIBED badge + are excluded
    # from feasible ACWI holdings). Confirmed by probing every exchange below.
    "TSE", "HKSE", "SHSE", "SZSE", "TPE", "ROCO", "XKRX",
    "SGX", "XKLS", "ISX", "BKK", "PHS",
    # Middle East
    "SAU", "DSMD", "KUW", "XTAE", "ADX", "DFM",
])


#: Exchange codes GuruFocus's own `isin/{ISIN}` endpoint returns that differ from the codes used
#: everywhere else here (`gurufocus_exchange.exchange_code`, `_build_symbol`, this file's own set).
#:
#: ⚠⚠ IT LIVES HERE NOW, WITH THE COVERAGE SET IT HAS TO AGREE WITH (moved 2026-09-01). It used to
#: sit in `routers/_gf_listing.py`, whose docstring already warned it "is the first place to look
#: when a US listing mysteriously reads as out-of-coverage" — and then
#: `is_gf_subscribed_exchange`, one module away, did not apply it. `NAS` therefore answered False:
#: harmless while only the listing picker asked (it normalises first), and a live hazard the moment
#: `ingest.earnings.refuse_unsubscribed` started gating vendor calls on this function, because a
#: NASDAQ company addressed by the vendor's own spelling would have been refused as unsubscribed.
GF_EXCHANGE_ALIASES = {"NAS": "NASDAQ"}


def is_gf_subscribed_exchange(exchange_code: str | None) -> bool:
    """True if our GuruFocus subscription covers this exchange — so a missing
    market cap (or price) is a data gap, NOT a coverage gap. US exchanges are
    represented as '' inside FEASIBLE_GF_EXCHANGES, so they're checked against
    `_US_EXCHANGES`; every other exchange is matched by its code directly.
    Unknown/empty exchange → False (we can't claim coverage).

    ⚠ IT NORMALISES FIRST. The same venue reaches this by several spellings — `NAS` from the
    vendor's `isin/` payload, `NASDAQ` from our own `gurufocus_exchange` row — and a coverage
    answer that depends on which caller spelled it is not an answer. See `GF_EXCHANGE_ALIASES`.
    """
    if not exchange_code:
        return False
    code = GF_EXCHANGE_ALIASES.get(exchange_code.strip().upper(), exchange_code.strip().upper())
    if code in _US_EXCHANGES:
        return True
    return code in FEASIBLE_GF_EXCHANGES
