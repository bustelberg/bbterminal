"""ISIN/symbol -> longest + most-liquid Yahoo listing, with a decision record.

The core of the asset-pipeline prototype. For an ISIN we enumerate candidate
listings (Yahoo search on the ISIN + on the resolved name), score each by
liquidity (median daily traded value in EUR) and history depth, then pick the
most-liquid among those with enough history — the ANALYSIS instrument to
backtest on. Native symbols (crypto/fx/commodity/index) resolve to themselves.
The IBKR step (EXECUTION instrument) is delegated to `ibkr` (stubbed)."""
from __future__ import annotations

import re
import statistics as st
import time
from datetime import date, datetime, timezone

from . import ibkr, openfigi, yahoo

ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
MIN_YEARS = 3.0
_EQUITY_TYPES = {"EQUITY", "ETF"}
# Corporate-form / share-class noise stripped before name-matching candidates.
_STOP = {
    "the", "inc", "plc", "ag", "sa", "co", "ltd", "group", "holdings", "corp",
    "nv", "oyj", "asa", "ab", "spa", "class", "ord", "shares", "limited", "company",
}


def detect_id_type(identifier: str) -> str:
    return "isin" if ISIN_RE.match(identifier.strip().upper()) else "yahoo"


def _tokens(name: str | None) -> set[str]:
    return {w for w in re.split(r"[^a-z0-9]+", (name or "").lower()) if w and w not in _STOP}


def _asset_class(quote_type: str | None, symbol: str) -> str:
    qt = (quote_type or "").upper()
    su = symbol.upper()
    if qt == "CRYPTOCURRENCY" or su.endswith("-USD") or su.endswith("-EUR"):
        return "crypto"
    if qt == "CURRENCY" or su.endswith("=X"):
        return "fx"
    if qt == "FUTURE" or su.endswith("=F"):
        return "commodity"
    if qt == "INDEX" or su.startswith("^"):
        return "index"
    if qt == "ETF":
        return "etf"
    return "equity"


# Single-underlying wrappers (crypto / commodity ETPs): keyword (word-boundary)
# -> (label, asset_class, [candidate analysis symbols]). The candidate list is
# PREFERENCE-ORDERED: a clean liquid ETF (real daily volume, e.g. GLD since 2004)
# BEFORE the continuous futures (GC=F — long but settlement-only / zero-volume
# early history). `_pick_analysis` walks the list and takes the first candidate
# with real volume + enough history, so a 0-volume series is skipped for the next
# best. Only consulted when the resolved instrument is an ETF, so an equity
# literally named "Barrick Gold" is never mis-mapped.
_UNDERLYING: list[tuple[tuple[str, ...], str, str, list[str]]] = [
    (("bitcoin cash",), "Bitcoin Cash", "crypto", ["BCH-USD"]),
    (("bitcoin", "btc", "xbt"), "Bitcoin", "crypto", ["BTC-USD"]),
    (("ethereum", "ether"), "Ethereum", "crypto", ["ETH-USD"]),
    (("solana",), "Solana", "crypto", ["SOL-USD"]),
    (("ripple", "xrp"), "XRP", "crypto", ["XRP-USD"]),
    (("litecoin",), "Litecoin", "crypto", ["LTC-USD"]),
    (("gold",), "Gold", "commodity", ["GLD", "IAU", "GC=F"]),
    (("silver",), "Silver", "commodity", ["SLV", "SI=F"]),
    (("platinum",), "Platinum", "commodity", ["PPLT", "PL=F"]),
    (("palladium",), "Palladium", "commodity", ["PALL", "PA=F"]),
    (("natural gas",), "Natural Gas", "commodity", ["UNG", "NG=F"]),
    (("brent",), "Brent Crude", "commodity", ["BNO", "BZ=F"]),
    (("crude", "wti"), "Crude Oil (WTI)", "commodity", ["USO", "CL=F"]),
    (("copper",), "Copper", "commodity", ["CPER", "HG=F"]),
]


# Leveraged / inverse products must NOT be mapped to the plain underlying (their
# return profile differs) — they become their own analysis asset instead.
_LEVERAGED_RE = re.compile(r"(\b\d+x\b|-\d+x\b|leverag|inverse|\bshort\b|\bultra\b|\bbear\b)", re.I)


def _is_leveraged(name: str | None) -> bool:
    return bool(_LEVERAGED_RE.search(name or ""))


# A physically-backed commodity ETP is sometimes typed EQUITY by Yahoo (not ETF),
# so gate the underlying swap on NAME hints too — but exclude real equities.
_WRAPPER_HINT = re.compile(r"\b(physical|etc|etp|etn|etf|trust|fund|shares|bullion)\b", re.I)
_EQUITY_HINT = re.compile(
    r"\b(mining|miner|miners|producers|resources|corp|corporation|company|companies|holdings|ltd|plc|inc|ag|sa|nv)\b",
    re.I,
)
# Baskets of commodity EQUITIES (miners/producers) are NOT the commodity itself.
_BASKET_RE = re.compile(r"\b(miner|miners|mining|producers|equities|companies)\b", re.I)


def _looks_like_wrapper(name: str | None, asset_class: str) -> bool:
    """True when this looks like a single-underlying commodity/crypto wrapper —
    either Yahoo typed it ETF, or the name has wrapper hints (physical/ETC/…) and
    no equity/miner hints. Keeps 'Barrick Gold Corp' and 'Gold Miners' out."""
    if asset_class == "etf":
        return True
    nm = name or ""
    return bool(_WRAPPER_HINT.search(nm)) and not _EQUITY_HINT.search(nm)


def _detect_underlying(name: str | None) -> tuple[str, str, list[str]] | None:
    """If an ETF/ETP name names a single underlying asset, return
    ``(label, asset_class, [candidate symbols])`` — the candidates to backtest
    on, preference-ordered. Word-boundary match so 'Goldman' doesn't hit 'gold'.
    A miners/producers basket returns None (it's equities, not the commodity)."""
    low = (name or "").lower()
    if _BASKET_RE.search(low):
        return None
    for keywords, label, aclass, candidates in _UNDERLYING:
        for kw in keywords:
            if re.search(rf"\b{re.escape(kw)}\b", low):
                return label, aclass, candidates
    return None


def _pick_analysis(symbols: list[str]) -> dict | None:
    """Walk preference-ordered candidate analysis symbols; return the first with
    REAL volume (median daily traded value > 0) AND enough history — so a
    zero-volume / settlement-only series (e.g. GC=F's early tail) is dropped for
    the next best (e.g. GLD). Falls back to the first that scored at all."""
    fallback: dict | None = None
    for sym in symbols:
        sc = _score(sym)
        if not sc:
            continue
        if fallback is None:
            fallback = sc
        if (sc.get("med_adv_eur") or 0) > 0 and sc.get("years", 0) >= MIN_YEARS:
            return sc
    return fallback


def _years(iso: str | None) -> float:
    if not iso:
        return 0.0
    try:
        return (date.today() - date.fromisoformat(iso)).days / 365.25
    except ValueError:
        return 0.0


def _score(symbol: str) -> dict | None:
    """One 3mo daily fetch -> liquidity (median daily traded value, EUR) +
    history start (from meta.firstTradeDate, present regardless of range)."""
    r = yahoo.chart(symbol, rng="3mo")
    if not r:
        return None
    meta = r.get("meta") or {}
    q = ((r.get("indicators") or {}).get("quote") or [{}])[0]
    cl, vl = q.get("close") or [], q.get("volume") or []
    ts = r.get("timestamp") or []
    ccy = meta.get("currency")
    fx = yahoo.fx_to_eur(ccy) or 0.0
    vals = [
        cl[i] * vl[i] * fx
        for i in range(min(len(cl), len(vl), len(ts)))
        if cl[i] and vl[i] and yahoo.is_closed_bar(ts[i])  # skip today's partial bar
    ]
    med = st.median(vals) if vals else 0.0
    ft = meta.get("firstTradeDate")
    start = datetime.fromtimestamp(ft, timezone.utc).date().isoformat() if ft else None
    return {
        "symbol": symbol,
        "currency": ccy,
        "exchange": meta.get("fullExchangeName"),
        "med_adv_eur": med,
        "first_date": start,
        "first_ts": ft,
        "years": round(_years(start), 1),
        "quote_type": meta.get("instrumentType"),
        "name": meta.get("longName") or meta.get("shortName"),
    }


def _bars(result: dict | None) -> list[dict]:
    """Every OHLCV bar in a chart result, dropping null (non-trading/padded)
    rows so slices are real candles."""
    if not result:
        return []
    ts = result.get("timestamp") or []
    q = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    o, h, low, c, v = (q.get(k) or [] for k in ("open", "high", "low", "close", "volume"))

    def g(a: list, i: int):
        return a[i] if i < len(a) else None

    out = []
    for i in range(len(ts)):
        if g(c, i) is None or not yahoo.is_closed_bar(ts[i]):  # skip null + today's partial
            continue
        out.append({
            "date": datetime.fromtimestamp(ts[i], timezone.utc).date().isoformat(),
            "open": g(o, i), "high": g(h, i), "low": g(low, i), "close": g(c, i), "volume": g(v, i),
        })
    return out


def _fetch_candles(symbol: str, first_ts: int | None, n: int = 5) -> dict:
    """True DAILY oldest/newest candles from one full-history fetch (period
    window → real 1d bars; `range=max` would coarsen to quarterly), with the
    leading zero-volume backfill run trimmed so the oldest candles show real
    trading data rather than settlement-only flat rows."""
    if not first_ts:
        return {"oldest": [], "newest": _bars(yahoo.chart(symbol, rng="1mo", interval="1d"))[-n:]}
    full = yahoo.trim_leading_no_volume(
        _bars(yahoo.chart_window(symbol, int(first_ts), int(time.time()), "1d"))
    )
    return {"oldest": full[:n], "newest": full[-n:]}


def _reason(chosen: dict, runner: dict | None, scored: list[dict]) -> str:
    parts = [
        f"Picked {chosen['symbol']} ({chosen.get('exchange')}, {chosen.get('currency')}): "
        f"highest liquidity (€{chosen['med_adv_eur'] / 1e6:.1f}M median daily traded value) "
        f"with {chosen['years']}y history."
    ]
    if runner and runner["med_adv_eur"] > 0 and chosen["med_adv_eur"] > 0:
        diff = (1 - runner["med_adv_eur"] / chosen["med_adv_eur"]) * 100
        parts.append(f"Runner-up {runner['symbol']} had {diff:.0f}% less volume.")
    thin = sum(1 for s in scored if s["med_adv_eur"] < chosen["med_adv_eur"] * 0.01)
    if thin:
        parts.append(f"{thin} near-dead cross-listing(s) ranked out.")
    return " ".join(parts)


_BOND_RE = re.compile(r"\b(gilt|bond|note|bill|treasury|govt|government|debenture)\b", re.I)


def _is_bond(sec: str | None) -> bool:
    return bool(_BOND_RE.search(sec or ""))


def _class_from_sec(sec: str | None) -> str:
    s = (sec or "").lower()
    if _is_bond(s):
        return "bond"
    if "etf" in s or "etp" in s or "fund" in s:
        return "etf"
    return "equity"


def _rank_candidates(quotes: list[dict], name_hint: str | None) -> tuple[list[dict], str | None]:
    """Filter Yahoo search quotes to equity/ETF listings that name-match the
    hint, score each by liquidity; return (scored, sector)."""
    want = _tokens(name_hint) if name_hint else set()
    seen: dict[str, dict] = {}
    sector: str | None = None
    for q in quotes:
        sym = q.get("symbol")
        if not sym or sym in seen or q.get("quoteType") not in _EQUITY_TYPES:
            continue
        if want and not (_tokens(q.get("shortname") or q.get("longname")) & want):
            continue  # name guard — kills wrong-ISIN / unrelated hits
        seen[sym] = q
        if not sector and (q.get("sectorDisp") or q.get("sector")):
            sector = q.get("sectorDisp") or q.get("sector")
    scored = [s for s in (_score(sym) for sym in seen) if s and s["med_adv_eur"] > 0]
    return scored, sector


def _identity_result(identifier: str, idt: str, name: str | None, asset_class: str, reason: str) -> dict:
    """Resolved identity but NO usable price series (a bond, or an ISIN Yahoo
    can't price). analysis=None → the UI shows the identity + reason, not a bare
    failure."""
    return {
        "input": identifier, "id_type": idt, "asset_class": asset_class, "wrapper": None,
        "is_leveraged": False, "candidates": [], "execution": None, "analysis": None,
        "underlying": None, "reason": reason, "analysis_note": None,
        "sector": name or asset_class, "candles": None,
        "ibkr": ibkr.resolve_tradeable_eu(identifier.upper()) if idt == "isin" else None,
    }


def resolve(identifier: str, id_type: str | None = None, with_candles: bool = True) -> dict:
    identifier = identifier.strip()
    idt = id_type or detect_id_type(identifier)
    sector: str | None = None

    if idt == "isin":
        isin = identifier.upper()
        quotes = yahoo.search(isin)
        name_hint = next((q.get("shortname") or q.get("longname") for q in quotes
                          if q.get("shortname") or q.get("longname")), None)
        if name_hint:  # broaden with a name search — catches cross-listings ISIN search misses
            quotes = quotes + yahoo.search(name_hint)
        scored, sector = _rank_candidates(quotes, name_hint)

        if not scored:
            # Yahoo's ISIN search missed it — fall back to OpenFIGI identity.
            rows = openfigi.lookup_isin(isin)
            o = rows[0] if rows else None
            if not o:
                return _identity_result(identifier, idt, None, "equity",
                                        "Not found on Yahoo or OpenFIGI for this ISIN.")
            sec, oname, oticker = o.get("securityType") or "", o.get("name"), o.get("ticker")
            if _is_bond(sec):
                return _identity_result(
                    identifier, idt, oname, "bond",
                    f"Identified as {oname or isin} ({sec}) via OpenFIGI — Yahoo has no "
                    "daily price series for individual bonds/gilts.")
            # Re-search Yahoo by the OpenFIGI name + ticker to recover the listing.
            extra = yahoo.search(oname or "")
            if oticker:
                extra = extra + yahoo.search(oticker)
            name_hint = oname or name_hint
            scored, sector = _rank_candidates(extra, name_hint)
            if not scored:
                return _identity_result(
                    identifier, idt, oname, _class_from_sec(sec),
                    f"Identified as {oname or isin} ({sec}) via OpenFIGI, but no Yahoo price "
                    "series resolved.")

        elig = [s for s in scored if s["years"] >= MIN_YEARS] or scored
        elig.sort(key=lambda s: (-s["med_adv_eur"], s["first_date"] or "9999"))
        chosen = elig[0]
        for s in scored:
            s["eligible"] = s["years"] >= MIN_YEARS
        scored.sort(key=lambda s: -s["med_adv_eur"])
        runner = next((s for s in scored if s["symbol"] != chosen["symbol"]), None)
        reason = _reason(chosen, runner, scored)
        asset_class = _asset_class(chosen.get("quote_type"), chosen["symbol"])
        ibkr_res = ibkr.resolve_tradeable_eu(isin, analysis=chosen)
    else:
        sc = _score(identifier)
        if not sc:
            return _identity_result(identifier, idt, None, "equity",
                                    f"Yahoo returned no data for '{identifier}'.")
        sc["eligible"] = True
        scored, chosen = [sc], sc
        asset_class = _asset_class(sc.get("quote_type"), identifier)
        reason = f"Single native {asset_class} listing — no cross-listing to rank."
        ibkr_res = None

    # Analysis vs execution split. `chosen` is the resolved tradeable
    # (EXECUTION) listing. If it's a single-underlying WRAPPER (a crypto/
    # commodity ETP), the thing to BACKTEST is the underlying's long series —
    # e.g. a Bitcoin ETF (short history) → BTC-USD (since 2014). Swap the
    # ANALYSIS instrument to it; keep the ETF as execution.
    execution = chosen
    analysis_note: str | None = None
    is_leveraged = False
    underlying: tuple[str, str, list[str]] | None = None
    if _looks_like_wrapper(chosen.get("name"), asset_class):
        if _is_leveraged(chosen.get("name")):
            is_leveraged = True  # leveraged/inverse — backtest on ITSELF, not the underlying
            analysis_note = (
                f"{execution['symbol']} looks leveraged/inverse — NOT mapped to a plain "
                "underlying (different return profile). Backtested on itself (short history)."
            )
        else:
            underlying = _detect_underlying(chosen.get("name"))
    picked = _pick_analysis(underlying[2]) if underlying else None
    if underlying and picked:
        u_label, u_aclass, _cands = underlying
        analysis = picked
        analysis_asset_class = u_aclass  # the TRUE asset (commodity/crypto), not the proxy's 'etf'
        analysis_note = (
            f"Input resolves to a {u_label} ETP ({execution['symbol']}); backtest on "
            f"{picked['symbol']} (since {picked.get('first_date')}, {picked.get('years')}y, "
            "real daily volume) — chosen over lower-quality alternatives (e.g. continuous "
            "futures with settlement-only / zero-volume early history). The ETP is the "
            "execution instrument."
        )
    else:
        analysis = chosen
        analysis_asset_class = asset_class

    candles = _fetch_candles(analysis["symbol"], analysis.get("first_ts")) if with_candles else None

    return {
        "input": identifier,
        "id_type": idt,
        "asset_class": analysis_asset_class,   # the TRUE asset (crypto for a BTC ETF)
        "wrapper": "etf" if (underlying and picked) else None,
        "is_leveraged": is_leveraged,
        "candidates": scored,
        "execution": execution,                # what you trade (resolved from the ISIN)
        "analysis": analysis,                  # what you backtest (underlying if wrapped)
        "underlying": {"symbol": underlying[0], "label": underlying[1]} if underlying else None,
        "chosen": analysis,                    # back-compat
        "reason": reason,                      # why this LISTING among candidates
        "analysis_note": analysis_note,        # why analysis != execution (or None)
        "sector": sector or analysis_asset_class,
        "candles": candles,                    # candles OF THE ANALYSIS instrument
        "ibkr": ibkr_res,
    }
