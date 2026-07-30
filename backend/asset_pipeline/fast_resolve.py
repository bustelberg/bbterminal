"""Fast ISIN -> yfinance resolution without Yahoo SEARCH.

The slow `resolve()` fans out several Yahoo search calls + a chart probe per
candidate (dozens of requests/ISIN) and trips Yahoo's rate-limit on a bulk run.
This path builds the Yahoo symbol DIRECTLY from a clean identity and validates it
with a SINGLE chart call whose name must match — ~1-3 Yahoo calls/ISIN.

Primary signal: the LEONTEQ (lynqs) row — its Bloomberg ticker (`MU UQ Equity`
-> ticker `MU`, exchange `UQ`) and RIC (`MU.OQ`) pin the exact PRIMARY listing +
exchange, which map cleanly to a Yahoo symbol (`MU`; `PUM GY`/`PUMG.DE` -> `PUM.DE`;
`GSK LN`/`GSK.L` -> `GSK.L`). Fallback signal: OpenFIGI listings (works when they
carry a clean composite code like `US`/`GR`). Rows with neither return None (the
caller may use the search resolver).
"""
from __future__ import annotations

from . import yahoo
from .resolve import (
    _asset_class,
    _score,
    resolve_analysis_instrument,
    same_company,
)


def _m(codes: str, suffix: str) -> dict[str, str]:
    return {c: suffix for c in codes.split()}


# Bloomberg 2-letter exchange code (2nd token of "TICKER XX Equity") -> Yahoo suffix.
_BBG: dict[str, str] = {
    **_m("UN UW UQ UR UA UP UF UV UO US UD UC UB UM UX UT UL", ""),   # United States
    **_m("CN CT", ".TO"), **_m("CV", ".V"),                            # Canada
    **_m("LN", ".L"),                                                  # London
    **_m("GY GR GF", ".DE"),                                           # Germany (Xetra)
    **_m("FP", ".PA"), **_m("NA", ".AS"), **_m("BB", ".BR"),
    **_m("IM", ".MI"), **_m("SM SQ", ".MC"),
    **_m("SW SE VX", ".SW"),                                           # Switzerland
    **_m("SS", ".ST"), **_m("NO", ".OL"), **_m("DC", ".CO"),
    **_m("FH", ".HE"), **_m("PL", ".LS"), **_m("AV", ".VI"), **_m("ID", ".IR"),
    **_m("JT JP", ".T"), **_m("HK", ".HK"),
    **_m("C1 CH", ".SS"), **_m("C2 CS", ".SZ"),
    **_m("AT AU", ".AX"), **_m("NZ", ".NZ"), **_m("SP", ".SI"),
    **_m("KS KP", ".KS"), **_m("KQ", ".KQ"), **_m("TT", ".TW"),
    **_m("IN IB", ".NS"), **_m("IS", ".BO"),
    **_m("BZ BS", ".SA"), **_m("MM MF", ".MX"), **_m("SJ", ".JO"),
}

# RIC exchange suffix (after the dot in `MU.OQ`) -> Yahoo suffix.
_RIC: dict[str, str] = {
    **_m("O OQ N K A P Z", ""),        # US (Nasdaq / NYSE / Arca / American)
    **_m("TO", ".TO"), **_m("V", ".V"),
    **_m("L", ".L"), **_m("DE F", ".DE"), **_m("PA", ".PA"), **_m("AS", ".AS"),
    **_m("BR", ".BR"), **_m("MI", ".MI"), **_m("MC MA", ".MC"),
    **_m("S SW VX", ".SW"), **_m("ST", ".ST"), **_m("OL", ".OL"),
    **_m("CO", ".CO"), **_m("HE", ".HE"), **_m("LS", ".LS"), **_m("VI", ".VI"),
    **_m("I IR", ".IR"), **_m("T", ".T"), **_m("HK", ".HK"),
    **_m("SS", ".SS"), **_m("SZ", ".SZ"), **_m("AX", ".AX"), **_m("NZ", ".NZ"),
    **_m("SI", ".SI"), **_m("KS", ".KS"), **_m("KQ", ".KQ"),
    **_m("TW TWO", ".TW"), **_m("NS", ".NS"), **_m("BO", ".BO"),
    **_m("SA", ".SA"), **_m("MX", ".MX"), **_m("J", ".JO"),
}

# OpenFIGI composite codes (a MINORITY of ISINs carry a clean one) -> Yahoo suffix.
_FIGI_EXCH: dict[str, str] = {
    **_m("US", ""), **_m("CN CT", ".TO"), **_m("LN", ".L"), **_m("GR GY GF", ".DE"),
    **_m("FP", ".PA"), **_m("NA", ".AS"), **_m("BB", ".BR"), **_m("IM", ".MI"),
    **_m("SM", ".MC"), **_m("SW SE", ".SW"), **_m("SS", ".ST"), **_m("NO", ".OL"),
    **_m("DC", ".CO"), **_m("FH", ".HE"), **_m("AV", ".VI"), **_m("JT JP", ".T"),
    **_m("HK", ".HK"), **_m("AT AU", ".AX"), **_m("SP", ".SI"), **_m("TT", ".TW"),
}

_SKIP_TYPES = ("Bond", "Bill", "Note", "Debenture", "Warrant", "Right", "Option",
               "Future", "Index", "Curncy", "Repo")


def _clean(ticker: str, suffix: str) -> str:
    """Bloomberg/RIC ticker -> Yahoo local ticker."""
    t = (ticker or "").strip().upper().replace(" ", "-")
    if suffix == "":
        t = t.replace("/", "-")           # US share class BRK/A -> BRK-A
    else:
        t = t.split("/")[0]               # drop class markers on foreign lines
    if suffix == ".HK" and t.isdigit():
        t = t.zfill(4)                    # HK 700 -> 0700.HK
    return t


def _from_leonteq(bbg_ticker: str | None, ric: str | None) -> list[str]:
    """Yahoo symbol candidates from a Leonteq row's Bloomberg ticker + RIC."""
    out: list[str] = []

    def _push(sym: str) -> None:
        if sym and sym not in out:
            out.append(sym)

    tok = (bbg_ticker or "").split()
    tkr, bexch = (tok[0], tok[1].upper()) if len(tok) >= 2 else (None, None)
    ric_root, ric_sfx = (None, None)
    if ric and "." in ric:
        ric_root, ric_sfx = ric.rsplit(".", 1)
        ric_sfx = ric_sfx.upper()
    # Best: clean Bloomberg ticker + the exchange suffix (from bbg code, else RIC).
    if tkr:
        if bexch in _BBG:
            _push(_clean(tkr, _BBG[bexch]) + _BBG[bexch])
        if ric_sfx in _RIC:
            _push(_clean(tkr, _RIC[ric_sfx]) + _RIC[ric_sfx])
    # Then the RIC root (sometimes carries an exchange letter, e.g. PUMG) as a
    # secondary guess.
    if ric_root and ric_sfx in _RIC:
        _push(_clean(ric_root, _RIC[ric_sfx]) + _RIC[ric_sfx])
    return out


def _from_openfigi(isin: str, rows: list[dict]) -> list[str]:
    """Yahoo symbol candidates from OpenFIGI listings that carry a clean composite
    exchange code (home-country first)."""
    home = (isin or "")[:2].upper()
    scored: list[tuple[tuple, str]] = []
    seen: set[str] = set()
    _country = {"": "US", ".TO": "CA", ".L": "GB", ".DE": "DE", ".PA": "FR",
                ".AS": "NL", ".MI": "IT", ".MC": "ES", ".SW": "CH", ".T": "JP",
                ".HK": "HK", ".AX": "AU"}
    for r in rows:
        t = (r.get("ticker") or "").strip()
        ex = (r.get("exchCode") or "").strip().upper()
        st = r.get("securityType") or ""
        if not t or ex not in _FIGI_EXCH or any(b in st for b in _SKIP_TYPES):
            continue
        sfx = _FIGI_EXCH[ex]
        sym = _clean(t, sfx) + sfx
        if not sym or sym in seen:
            continue
        seen.add(sym)
        pr = (0 if _country.get(sfx) == home else 1, len(sym))
        scored.append((pr, sym))
    scored.sort(key=lambda x: x[0])
    return [s for _, s in scored]


def _from_yahoo_isin(isin: str) -> list[str]:
    """Yahoo's OWN resolution of the ISIN — one search call, ISIN as the query.

    WHY THIS EXISTS (the ISPA.DE hole)
        Every other source here CONSTRUCTS a symbol as `ticker + venue suffix`, and that
        silently assumes OpenFIGI and Yahoo agree on the ticker. On a German venue they often
        do not. iShares STOXX Global Select Dividend 100 (`DE000A0F5UH1`) is `SDGPEX` on
        OpenFIGI's Xetra/Frankfurt lines and `ISPA` on Yahoo's — so we built `SDGPEX.DE`,
        which does not exist, and the Xetra listing was UNREACHABLE by construction. The row
        sat queued while the candidates on offer were Vienna (EUR 31,824/day) and Zurich
        (EUR 94,776/day). Asking Yahoo to resolve the ISIN returns `ISPA.DE` directly, with no
        ticker guess in the middle.

    ⚠ IT IS A CANDIDATE SOURCE, NEVER AN ANSWER. Yahoo resolves an ISIN to *a* listing, not to
    the *liquid* one, and its pick is regularly the wrong end of that:

        US02079K3059  Alphabet Class A            -> 1GOOGL.MI   (Milan. Not GOOGL.)
        IE00BP3QZ825  iShares Edge World Momentum -> IWFM.L      (EUR 1.45m/day; IS3R.DE does 5.63m)

    Trusting it outright rebuilds the exact wrong-listing bug this module exists to prevent.
    So it only ever ADDS to the pool, and the caller's liquidity ranking + name gate still
    decide. That also makes it safe under load: Yahoo answers an overloaded caller with an
    EMPTY list rather than a 429, and an empty list here costs us the improvement, never the
    correctness — we simply fall back to the constructed candidates.

    One filter: Yahoo hands back a PSEUDO-SYMBOL for some funds — literally the ISIN plus a
    venue suffix (`IE00BNDS1P30.SG`), a Stuttgart placeholder rather than a listing. It is not
    a symbol we can price, so it is dropped here rather than left to fail a probe.
    """
    try:
        hits = yahoo.search(isin, count=10)
    except Exception:
        return []
    key = (isin or "").strip().upper()
    out: list[str] = []
    for h in hits:
        sym = (h.get("symbol") or "").strip().upper()
        if not sym or key in sym:
            continue
        if (h.get("quoteType") or "").upper() in {"CURRENCY", "INDEX", "FUTURE", "OPTION"}:
            continue
        if sym not in out:
            out.append(sym)
    return out


def build_candidates(isin: str, rows: list[dict], leonteq: tuple | None = None,
                     limit: int = 5, yahoo_isin: bool = False) -> list[str]:
    """Ordered candidate Yahoo symbols: Leonteq-derived first (most reliable),
    then OpenFIGI-derived. `leonteq` is `(bbg_ticker, ric)` or None.

    `yahoo_isin` adds Yahoo's own ISIN resolution (see `_from_yahoo_isin`). It costs ONE extra
    search call per ISIN, which is why it is off by default: the whole point of this module is
    ~1 call/ISIN on a bulk run, and doubling that is how you get throttled into the empty-list
    failure mode. Deliberate, per-row tools (the repointers) turn it on.
    """
    cands: list[str] = []
    if leonteq:
        cands += _from_leonteq(leonteq[0], leonteq[1])
    # Ahead of the constructed symbols: Yahoo resolved the ISIN itself, so this candidate
    # involves no ticker guess. It must not be the one that falls off the `limit` cliff.
    if yahoo_isin:
        for s in _from_yahoo_isin(isin):
            if s not in cands:
                cands.append(s)
    for s in _from_openfigi(isin, rows):
        if s not in cands:
            cands.append(s)
    return cands[:limit]


# A listing at least this liquid IS the primary — accept it without scoring the
# thinner cross-listings. Below it, we score every candidate and take the most
# liquid, so a transient empty on the primary can't strand us on a thin line
# (the NVIDIA -> NVD.SG / NVDA.SW bug: €1.6M/€6.5k instead of NVDA's €28B).
_FAST_ACCEPT_ADV_EUR = 5_000_000.0


def _score_retry(sym: str) -> dict | None:
    """One chart/liquidity probe, retried once — Yahoo returns transient empties
    under load, and a miss on the PRIMARY listing is what strands a name on a thin
    cross-listing."""
    sc = _score(sym)
    if sc is None:
        sc = _score(sym)
    return sc


def fast_resolve(isin: str, rows: list[dict], figi_name: str | None,
                 leonteq: tuple | None = None) -> dict | None:
    """Resolve to the MOST-LIQUID name-validated candidate listing. Scores the
    home-first candidates; early-accepts the primary if it's clearly liquid, else
    scores them all and takes the max-ADV one (so a transient empty on the primary
    can't drop us onto a thin cross-listing). None when nothing validates."""
    cands = build_candidates(isin, rows, leonteq)
    picked: dict | None = None
    for i, sym in enumerate(cands):
        sc = _score_retry(sym)
        if not sc or (sc.get("med_adv_eur") or 0) <= 0:
            continue
        # The candidates are ISIN-anchored, so this cannot swap in a different company —
        # but `_clean(ticker) + suffix` CONSTRUCTS a Yahoo symbol, and a ticker is reused
        # across venues, so the constructed symbol can land on an unrelated instrument.
        # Hence the name check. It must be `same_company` (which strips corporate forms),
        # NOT a raw `_name_score` floor: token_set_ratio scores "NVIDIA Corporation" vs
        # OpenFIGI's "NVIDIA CORP" at 75.9, under the 80 floor. Here a false reject doesn't
        # corrupt anything — it just drops the LIQUID listing and leaves us on the thin
        # cross-listing, which is the very bug this function exists to prevent.
        if figi_name and not same_company(sc.get("name"), figi_name):
            continue
        if picked is None or (sc.get("med_adv_eur") or 0) > (picked.get("med_adv_eur") or 0):
            picked = sc
        if i == 0 and (sc.get("med_adv_eur") or 0) >= _FAST_ACCEPT_ADV_EUR:
            break  # the home primary is clearly liquid — done
    if picked is None:
        return None
    sym = picked["symbol"]
    asset_class = _asset_class(picked.get("quote_type"), sym)
    picked["eligible"] = True
    # Same analysis-vs-execution split as the search path: a single-underlying
    # crypto/commodity ETP (BTC/ETH/gold) backtests on the underlying's long
    # series (ETH-USD…), keeping THIS listing as execution.
    ai = resolve_analysis_instrument(picked, asset_class)
    u = ai["underlying"]
    return {
        "input": isin, "id_type": "isin", "asset_class": ai["analysis_asset_class"],
        "wrapper": ai["wrapper"], "is_leveraged": ai["is_leveraged"], "candidates": [picked],
        "execution": ai["execution"], "analysis": ai["analysis"], "chosen": ai["analysis"],
        "underlying": {"symbol": u[0], "label": u[1]} if u else None,
        "reason": f"Fast path: identity -> {sym} (most-liquid validated listing).",
        "analysis_note": ai["analysis_note"], "sector": ai["analysis_asset_class"],
        "candles": None, "ibkr": None,
    }
