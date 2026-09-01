"""ISIN -> GuruFocus listing, without a `company` row.

WHY
    The Div/share column reaches GuruFocus through `company` (ISIN -> company.isin
    -> gurufocus_ticker). No ETF is ever ingested into `company`, so ~87% of the
    /asset-pipeline grid can never reach a dividend — even though GuruFocus has
    the data (QQQ returns 89 per-share distributions). GuruFocus's undocumented
    `isin/{ISIN}` endpoint resolves an ISIN straight to [{symbol, exchange}].

WHICH LISTING — AND WHY IT IS *NOT* ABOUT CURRENCY
    `isin/{ISIN}` returns EVERY listing worldwide. Apple comes back with 19:

        WBO:AAPL  XBUL:APC  XSWX:AAPL  XSGO:AAPL  FRA:APC   HAM:APC   STU:APC
        XTER:APC  LTS:0R2V  CHIX:APCd  MIL:1AAPL  XKAZ:AAPL_KZ  MEX:AAPL
        LIM:AAPL  WAR:AAPL  BSE:AAPL   MIC:AAPL-RM  UKEX:AAPL  NAS:AAPL

    The obvious fear — "a Xetra line reports Apple's dividend in EUR, so we'd show
    the wrong number" — is FALSE, and it was measured (2026-07-13). GuruFocus reports
    a dividend in its DECLARATION currency on every listing of the ISIN:

        AAPL (Nasdaq) 2026-05-11 -> 0.27 USD     91 records
        XTER:APC      2026-05-11 -> 0.27 USD     82 records
        XSWX:AAPL     2026-05-11 -> 0.27 USD     63 records
        MIL:1AAPL     2026-05-11 -> 0.27 USD     35 records

    Ratio 1.0000 everywhere. Same ISIN = same share class = one economic dividend,
    and the per-record `currency` field says so. There is no double-FX hazard, so
    the EUR panel is just the usual pay-date conversion, exactly as for a company.

    What the same measurement DID expose is the real defect: 91 -> 82 -> 63 -> 35.
    The foreign feeds are not wrong, they are INCOMPLETE. Zurich lists 2026-05-11
    and 2026-02-09 and then jumps to 2021-02-05 — about sixteen quarterly payments
    missing from the middle. That is dangerous precisely because it is invisible:
    `_trailing_12m` infers the payment frequency from the median ex-date gap and
    sums the last k payments, so a holed series yields a confident, plausible,
    completely wrong "trailing 12m".

    So the picker PREFERS the home listing (the fullest feed) but no longer REFUSES
    when it can't have one — a foreign listing's data is real, just possibly partial.
    It returns `is_home=False` instead, and the caller says so out loud.

    Filtering to GF-subscribed exchanges alone is NOT enough: Apple survives that
    filter on XSWX, XTER, MIL, WAR *and* NAS.

TWO CODE SPACES
    The `isin/` endpoint says `NAS`; our `gurufocus_exchange` table and
    `_build_symbol` say `NASDAQ` (the pipeline logs `NASDAQ:WDC`). Unmapped, every
    US listing would fail `is_gf_subscribed_exchange` and be badged UNSUBSCRIBED.
    `GF_EXCHANGE_ALIASES` is that bridge, and it is the first place to look when a
    US listing mysteriously reads as out-of-coverage.
"""
from __future__ import annotations

from typing import NamedTuple

# `isin/{ISIN}` exchange codes that differ from the codes used everywhere else in
# this codebase (`gurufocus_exchange.exchange_code`, `_build_symbol`, and
# `FEASIBLE_GF_EXCHANGES`). Confirmed by probing: the endpoint returns NAS for
# Nasdaq, while symbols are built as bare tickers for NASDAQ and the exchange row
# is keyed NASDAQ. NYSE / AMEX / OTCPK / XTER / MIL / TSE / XSWX already agree.
GF_EXCHANGE_ALIASES = {"NAS": "NASDAQ"}

# GuruFocus's USA region, straight from its own `exchange_list`:
#     NAS  NYSE  OTCPK  OTCBB  AMEX  ARCA  IEXG  BATS  GREY
#
# ARCA (NYSE Arca) matters far more than it looks: it is where most US ETFs list.
# Of SPY / IWM / VOO / XLU / EDV / GLD / QQQ, SIX resolve to ARCA and only QQQ to NAS.
#
# These venues have no `gurufocus_exchange` row (that table only holds the exchanges
# our EQUITY universes touch), so `exchange_currency` can't price them — and a missing
# currency silently costs a candidate its currency point, which would flag a perfectly
# good home listing as "not home". They are all USD; say so.
_US_EXCHANGES = frozenset({
    "NASDAQ", "NYSE", "AMEX", "CBOE", "OTCPK", "ARCA", "BATS", "IEXG",
})
_US_CURRENCY = "USD"

#: Venues that are reachable but are not where a security really trades — they lose any tie.
#:
#: ⚠ `FRA` IS THE FRANKFURT FLOOR, NOT XETRA. Both are German, both quote EUR, and a company often
#: carries the same ticker on each, so they score identically and only the tie-break separates
#: them. Xetra (`XTER`) is the electronic venue where the volume and the history are; the floor is
#: the thin one. Without this set the tie went alphabetically, and `FRA` sorts first.
#:
#: ⚠ IT IS ABOUT PICKING BETWEEN LISTINGS, NOT ABOUT COVERAGE. `FRA` stays in
#: `FEASIBLE_GF_EXCHANGES` — GuruFocus answers for it, and a company we hold ONLY on the floor
#: (Verisure) must still resolve. This only says: given a choice, prefer the primary venue.
_SECONDARY_VENUES = frozenset({"FRA"})


class Listing(NamedTuple):
    ticker: str
    exchange: str


class Resolution(NamedTuple):
    """status:
        ok           we have a listing to query
        not_found    GuruFocus does not know this ISIN (empty list / null)
        unsubscribed listings exist, but none on an exchange we subscribe to

    Both non-ok statuses are NEGATIVE-CACHED — the API call is spent, and no retry
    would find anything.

    is_home:
        True   the listing matched BOTH this row's ticker and its currency, so it is
               the row's own listing and carries the fullest payment feed.
        False  we fell back to another listing of the same ISIN. Its amounts are the
               same declaration-currency numbers (measured — see the module header),
               but its history may be PARTIAL: Milan holds 35 of Apple's 91 payments,
               Zurich 63 with a five-year hole. Real data, incomplete data. The UI
               must say so, and `_trailing_12m` must not sum across the holes.
    """
    listing: Listing | None
    status: str
    is_home: bool = False


def normalize_gf_exchange(code: str | None) -> str:
    """Map an `isin/` endpoint exchange code into OUR code space."""
    c = (code or "").strip().upper()
    return GF_EXCHANGE_ALIASES.get(c, c)


def base_symbol(symbol: str | None) -> str:
    """A Yahoo symbol stripped to the part GuruFocus would recognise.

    Yahoo suffixes the venue (`EXS1.DE`, `BAYN.DE`); GuruFocus carries the bare
    ticker plus a separate exchange. Crypto/FX pairs (`BTC-USD`) never resolve by
    ISIN anyway, so only the dot matters here.
    """
    return (symbol or "").strip().upper().split(".")[0]


def pick_listing(
    candidates: list[dict],
    *,
    symbol_hint: str | None,
    currency_hint: str | None,
    exchange_currency: dict[str, str],
) -> Resolution:
    """Choose the GuruFocus listing that IS the asset row, or choose nothing.

    `exchange_currency` maps our exchange codes -> trading currency (from
    `gurufocus_exchange`); it is how a candidate's currency is known without a
    second API call.

    Scoring, deliberately blunt — two independent signals, each worth one point:
      +1  the listing trades in the same currency as the asset row
      +1  the listing's ticker equals the asset row's (Yahoo) base symbol

    Score 2 = this IS the row's own listing (`is_home`). Apple's NAS:AAPL scores 2
    (USD + AAPL); XSWX:AAPL scores 1 (ticker only — it trades in CHF); the
    Frankfurt / Vienna / Milan lines score 0. QQQ's lone NAS:QQQ scores 2.

    We take the best candidate even when nothing scores 2, because a foreign listing
    of the same ISIN reports the SAME declaration-currency amounts (measured — see
    the module header). It is not the wrong number; it may just be a shorter history.
    Refusing it would blank the cell on data we actually have.

    Ties break toward a US listing: GuruFocus is a US product and its US feeds are
    the fullest (Nasdaq 91 payments vs Milan's 35), so when the row itself gives us
    nothing to go on, the US line is the best bet for a complete history. Then by
    exchange/ticker, so the choice is deterministic and a re-resolve is stable.
    """
    from index_universe.acwi.exchange_map import (  # noqa: PLC0415
        is_gf_subscribed_exchange,
    )

    if not candidates:
        return Resolution(None, "not_found")

    want_sym = base_symbol(symbol_hint)
    want_ccy = (currency_hint or "").strip().upper()

    scored: list[tuple[int, Listing]] = []
    for c in candidates:
        ticker = (c.get("symbol") or "").strip()
        exch = normalize_gf_exchange(c.get("exchange"))
        if not ticker or not exch:
            continue
        # Out-of-subscription listings can never return data — drop before scoring
        # so they can't win a tie or mask a real candidate.
        if not is_gf_subscribed_exchange(exch):
            continue
        # US ETF venues (ARCA/BATS/IEXG) have no `gurufocus_exchange` row, so fall
        # back to USD rather than scoring them as currency-unknown.
        ccy = (exchange_currency.get(exch)
               or (_US_CURRENCY if exch in _US_EXCHANGES else "")).upper()
        score = 0
        if want_ccy and ccy == want_ccy:
            score += 1
        if want_sym and ticker.upper() == want_sym:
            score += 1
        scored.append((score, Listing(ticker, exch)))

    if not scored:
        return Resolution(None, "unsubscribed")

    # Highest score wins; US listing breaks a tie; then exchange/ticker so the
    # result is deterministic across re-resolves.
    #
    # ⚠⚠ `_SECONDARY_VENUES` LOSES A TIE, AND IT EXISTS BECAUSE THE ALPHABET NEARLY DECIDED THIS.
    # The final tie-break is the exchange code ascending, which is arbitrary but deterministic —
    # fine while every candidate was a primary venue. Adding `FRA` to `FEASIBLE_GF_EXCHANGES`
    # (2026-09-01) broke that assumption: a German company can list on both Xetra and the Frankfurt
    # floor with the same ticker and the same currency, so the two score identically, and `"FRA" <
    # "XTER"` would have handed every such tie to the thinner floor. That is the Stuttgart failure
    # this codebase already carries a ⚠⚠ about (NVDA on `LLY.SG`-class venues, EUR 1.6M/day against
    # Nasdaq's 28,076M), arrived at through a sort key rather than a bad match.
    best_score, best = max(
        scored,
        key=lambda sl: (sl[0], sl[1].exchange in _US_EXCHANGES,
                        sl[1].exchange not in _SECONDARY_VENUES,
                        sl[1].exchange, sl[1].ticker),
    )
    return Resolution(best, "ok", is_home=best_score >= 2)
