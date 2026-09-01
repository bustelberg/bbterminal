"""The ISIN -> GuruFocus listing picker.

Every candidate list below is the REAL response from `isin/{ISIN}`, captured by
probing the live API — not a hand-written fixture. The Apple case is the whole
reason the module exists: 19 listings, five of which survive the
GF-subscription filter, in four different currencies.
"""
from __future__ import annotations

import pytest

from routers._gf_listing import (
    base_symbol,
    normalize_gf_exchange,
    pick_listing,
)

# `gurufocus_exchange` code -> trading currency (the subset these tests touch).
EXCH_CCY = {
    "NASDAQ": "USD", "NYSE": "USD", "OTCPK": "USD",
    "XSWX": "CHF", "XTER": "EUR", "MIL": "EUR", "XMAD": "EUR",
    "WAR": "PLN", "TSE": "JPY", "XPAR": "EUR",
}

# Real: GET isin/US0378331005
APPLE = [
    {"symbol": "AAPL", "exchange": "WBO"}, {"symbol": "APC", "exchange": "XBUL"},
    {"symbol": "AAPL", "exchange": "XSWX"}, {"symbol": "AAPL", "exchange": "XSGO"},
    {"symbol": "APC", "exchange": "FRA"}, {"symbol": "APC", "exchange": "HAM"},
    {"symbol": "APC", "exchange": "STU"}, {"symbol": "APC", "exchange": "XTER"},
    {"symbol": "0R2V", "exchange": "LTS"}, {"symbol": "APCd", "exchange": "CHIX"},
    {"symbol": "1AAPL", "exchange": "MIL"}, {"symbol": "AAPL_KZ", "exchange": "XKAZ"},
    {"symbol": "AAPL", "exchange": "MEX"}, {"symbol": "AAPL", "exchange": "LIM"},
    {"symbol": "AAPL", "exchange": "WAR"}, {"symbol": "AAPL", "exchange": "BSE"},
    {"symbol": "AAPL-RM", "exchange": "MIC"}, {"symbol": "AAPL", "exchange": "UKEX"},
    {"symbol": "AAPL", "exchange": "NAS"},
]

# Real: GET isin/US46090E1038 — the ETF that started this. One listing, no drama.
QQQ = [{"symbol": "QQQ", "exchange": "NAS"}]

# Real: GET isin/DE000BAY0017
BAYER = [
    {"symbol": "BAYN", "exchange": "WBO"}, {"symbol": "BAYN", "exchange": "XBUL"},
    {"symbol": "BAYN", "exchange": "XSWX"}, {"symbol": "BAYN", "exchange": "FRA"},
    {"symbol": "BAYN", "exchange": "HAM"}, {"symbol": "BAYN", "exchange": "STU"},
    {"symbol": "BAYN", "exchange": "XTER"}, {"symbol": "BAY", "exchange": "XMAD"},
    {"symbol": "BAYNd", "exchange": "CHIX"}, {"symbol": "0P6S", "exchange": "LTS"},
    {"symbol": "BAYER", "exchange": "BUD"}, {"symbol": "1BAYN", "exchange": "MIL"},
    {"symbol": "BAYNN", "exchange": "MEX"}, {"symbol": "BAYNd", "exchange": "CEUX"},
    {"symbol": "BAY", "exchange": "WAR"}, {"symbol": "BAYN", "exchange": "BSE"},
    {"symbol": "BAYZF", "exchange": "OTCPK"},
]


def pick(candidates, symbol, currency):
    return pick_listing(
        candidates, symbol_hint=symbol, currency_hint=currency,
        exchange_currency=EXCH_CCY,
    )


class TestExchangeCodeSpace:
    def test_nas_is_normalized_to_nasdaq(self):
        # The isin/ endpoint says NAS; the rest of the codebase says NASDAQ. Miss
        # this and every US listing reads as UNSUBSCRIBED.
        assert normalize_gf_exchange("NAS") == "NASDAQ"

    def test_codes_that_already_agree_pass_through(self):
        for code in ("NYSE", "XTER", "MIL", "TSE", "OTCPK"):
            assert normalize_gf_exchange(code) == code

    def test_base_symbol_strips_the_yahoo_venue_suffix(self):
        assert base_symbol("BAYN.DE") == "BAYN"
        assert base_symbol("QQQ") == "QQQ"
        assert base_symbol(None) == ""


class TestUsEtfVenues:
    """ARCA is where US ETFs actually live, and we didn't know the code.

    GuruFocus's `exchange_list` region USA is:
        NAS  NYSE  OTCPK  OTCBB  AMEX  ARCA  IEXG  BATS  GREY
    Of SPY / IWM / VOO / XLU / EDV / GLD / QQQ, SIX resolve to ARCA — only QQQ is NAS.
    With ARCA unknown, `is_gf_subscribed_exchange` said False and every one of them
    cached as 'unsubscribed' (a blank cell), despite GuruFocus holding 106 dividend
    payments for IWM.
    """

    def test_arca_is_subscribed(self):
        from index_universe.acwi.exchange_map import is_gf_subscribed_exchange
        for code in ("ARCA", "BATS", "IEXG"):
            assert is_gf_subscribed_exchange(code), f"{code} must be reachable"

    def test_iwm_resolves_to_arca(self):
        # Real: GET isin/US4642876555 -> [{'symbol': 'IWM', 'exchange': 'ARCA'}]
        r = pick([{"symbol": "IWM", "exchange": "ARCA"}], "IWM", "USD")
        assert r.status == "ok"
        assert r.listing == ("IWM", "ARCA")
        # USD must come from the US-venue fallback: ARCA has no gurufocus_exchange row,
        # and without it the ticker-only score would flag this perfect match "not home".
        assert r.is_home is True

    def test_spy_takes_arca_not_singapore(self):
        # THE mis-resolution this bug was hiding. Real: isin/US78462F1030 ->
        # [{'symbol': 'SPY', 'exchange': 'SGX'}, {'symbol': 'SPY', 'exchange': 'ARCA'}].
        # SGX *is* in FEASIBLE_GF_EXCHANGES, so with ARCA unknown the picker would have
        # silently handed back the SINGAPORE line of SPY.
        cands = [{"symbol": "SPY", "exchange": "SGX"}, {"symbol": "SPY", "exchange": "ARCA"}]
        r = pick(cands, "SPY", "USD")
        assert r.listing == ("SPY", "ARCA")
        assert r.is_home is True


class TestPickListing:
    def test_qqq_resolves(self):
        # The case the user asked for: ETF, one listing, USD, ticker matches.
        r = pick(QQQ, "QQQ", "USD")
        assert r.status == "ok"
        assert r.listing.ticker == "QQQ"
        assert r.listing.exchange == "NASDAQ"     # normalized, NOT 'NAS'

    def test_apple_resolves_to_nasdaq_not_zurich(self):
        # THE regression case. AAPL survives the subscription filter on XSWX (CHF),
        # XTER, MIL, WAR and NAS. Only NASDAQ matches BOTH the ticker and the USD
        # currency of the asset row, so it must win outright.
        r = pick(APPLE, "AAPL", "USD")
        assert r.status == "ok"
        assert r.listing == ("AAPL", "NASDAQ")

    def test_bayer_resolves_to_xetra(self):
        # BAYN is on XSWX (CHF) and XTER (EUR); the EUR row picks Xetra.
        r = pick(BAYER, "BAYN.DE", "EUR")
        assert r.status == "ok"
        assert r.listing == ("BAYN", "XTER")

    def test_ticker_alone_wins_when_currency_is_unknown(self):
        # No currency hint (score 1 from the ticker) is still the top scorer — but
        # it isn't proof this is the row's own listing, so is_home stays False.
        r = pick([{"symbol": "SAP", "exchange": "XTER"}], "SAP", None)
        assert r.status == "ok"
        assert r.listing == ("SAP", "XTER")
        assert r.is_home is False

    def test_lone_foreign_candidate_is_taken_but_flagged_not_home(self):
        # iShares Core MSCI World (IE00B4L5Y983) is EUNL.DE in our grid (Xetra, EUR);
        # GuruFocus's only listing for that ISIN is OTCPK:IRRRF (US OTC).
        #
        # We TAKE it. The original fear — that a foreign line reports the payout in
        # the wrong currency — was measured and is FALSE: GuruFocus reports a dividend
        # in its DECLARATION currency on every listing of the ISIN (Apple = 0.27 USD
        # on Nasdaq, Xetra, Zurich and Milan alike, ratio 1.0000). Refusing blanked a
        # cell over data we actually have.
        #
        # What IS true is that a non-home feed can be SHORT (Milan holds 35 of Apple's
        # 91 payments; Zurich 63, with a five-year hole), so it comes back is_home=False
        # and the UI says the history may be partial.
        r = pick([{"symbol": "IRRRF", "exchange": "OTCPK"}], "EUNL.DE", "EUR")
        assert r.status == "ok"
        assert r.listing == ("IRRRF", "OTCPK")
        assert r.is_home is False

    def test_ties_break_toward_the_us_listing(self):
        # Nothing discriminates -> prefer the US line, whose GuruFocus feed is deepest.
        cands = [{"symbol": "1XYZ", "exchange": "MIL"}, {"symbol": "XYZF", "exchange": "OTCPK"}]
        r = pick(cands, "XYZ", "JPY")
        assert r.status == "ok"
        assert r.listing.exchange == "OTCPK"

    def test_the_pick_is_deterministic(self):
        # A re-resolve must land on the same listing, or the cached symbol and the
        # freshly-computed one silently disagree.
        cands = [{"symbol": "1XYZ", "exchange": "MIL"}, {"symbol": "XYZW", "exchange": "XPAR"}]
        assert pick(list(reversed(cands)), "XYZ", "EUR").listing == pick(cands, "XYZ", "EUR").listing

    def test_only_unsubscribed_listings(self):
        # ⚠ WAS `LSE + WBO` UNTIL 2026-09-01, on the belief GuruFocus would 403 both. Vienna turned
        # out to be covered — VERBUND's vendor price history agrees with our own independent series
        # on 75 of 75 periods — so `WBO` joined `FEASIBLE_GF_EXCHANGES` and this pair stopped being
        # an example of the thing it was testing. LSE and India are the genuinely refused ones.
        cands = [{"symbol": "TYT", "exchange": "LSE"}, {"symbol": "TOM", "exchange": "NSE"}]
        assert pick(cands, "TYT", "GBP").status == "unsubscribed"

    def test_empty_is_not_found(self):
        # Roche (CH0012032048) really does return [].
        assert pick([], "ROG", "CHF").status == "not_found"

    @pytest.mark.parametrize("junk", [{"symbol": "", "exchange": "NAS"}, {"symbol": "X"}])
    def test_malformed_candidates_are_skipped(self, junk):
        assert pick([junk], "X", "USD").status in ("not_found", "unsubscribed")

    def test_listing_present_exactly_when_status_is_ok(self):
        # The invariant: a caller never gets a listing back from a non-ok status, and
        # never gets an ok status without one.
        for r in (
            pick([], "X", "USD"),                                        # not_found
            pick([{"symbol": "TYT", "exchange": "LSE"}], "TYT", "GBP"),  # unsubscribed
            pick(QQQ, "QQQ", "USD"),                                     # ok, home
            pick([{"symbol": "IRRRF", "exchange": "OTCPK"}], "EUNL.DE", "EUR"),  # ok, foreign
        ):
            assert (r.listing is not None) == (r.status == "ok")

    def test_only_a_full_match_counts_as_home(self):
        # is_home drives a user-visible "history may be partial" caveat, so it must
        # mean "this is the row's own listing", not merely "we found something".
        assert pick(QQQ, "QQQ", "USD").is_home is True
        assert pick(APPLE, "AAPL", "USD").is_home is True
        # Ticker matches, currency doesn't (no Apple listing trades in JPY).
        assert pick(APPLE, "AAPL", "JPY").is_home is False
        # Currency matches, ticker doesn't.
        assert pick(APPLE, "NOPE", "USD").is_home is False

    def test_a_chf_row_is_home_on_zurich(self):
        # The flip side, and why is_home is about the ROW rather than about "US wins":
        # for a CHF-quoted Apple row, XSWX:AAPL genuinely IS the home listing.
        r = pick(APPLE, "AAPL", "CHF")
        assert r.listing == ("AAPL", "XSWX")
        assert r.is_home is True


class TestViennaAndFrankfurtAreCovered:
    """⚠⚠ THEY WERE MISSING FROM `FEASIBLE_GF_EXCHANGES` UNTIL 2026-09-01, and the omission was
    silent because nothing asks a coverage map whether it is complete. Every other continental
    venue was in it, so `WBO` and `FRA` read as "outside the subscription" and three companies
    holding 53,879 `metric_data` rows between them — VERBUND AG (39,523), Erste Group Bank (13,894)
    and Verisure (462) — were classed as unfetchable.

    ⚠ THE EVIDENCE IS CROSS-SOURCE, not a probe: VERBUND's GuruFocus price history and our own
    independent yfinance series agree on 75 of 75 periods (`ingest.earnings.price_sanity`). A
    region the vendor does not sell us cannot produce that — Diploma, genuinely unsubscribed on the
    LSE, disagreed on 10 of 25."""

    def test_both_are_subscribed(self):
        from index_universe.acwi.exchange_map import is_gf_subscribed_exchange
        for code in ("WBO", "FRA"):
            assert is_gf_subscribed_exchange(code), f"{code} must be reachable"

    def test_the_genuinely_unsubscribed_are_untouched(self):
        """⚠ THE OTHER HALF OF THE CHANGE. Widening a coverage map is only safe if it widens by
        exactly what was intended — LSE is the reason `refuse_unsubscribed` exists."""
        from index_universe.acwi.exchange_map import is_gf_subscribed_exchange
        for code in ("LSE", "DUB", "NSE", "BOM", "TSX", "TSXV", "ASX", "NZSE"):
            assert not is_gf_subscribed_exchange(code), f"{code} must stay refused"


class TestTheFrankfurtFloorLosesATie:
    """⚠⚠ ADDING `FRA` TO THE COVERAGE MAP NEARLY LET THE ALPHABET PICK A LISTING. A German company
    lists on both Xetra (`XTER`, where the volume and the history are) and the Frankfurt floor
    (`FRA`, thin) under the SAME ticker in the SAME currency — so the two score identically and only
    the tie-break separates them. That tie-break was the exchange code ascending, and
    `"FRA" < "XTER"`. This is the Stuttgart failure (`LLY.SG`, EUR 1.6M/day against Nasdaq's
    28,076M) reached through a sort key instead of a bad match.

    ⚠⚠ THE `BAYER` FIXTURE ABOVE CANNOT SHOW IT, WHICH IS WHY THIS PASSES ITS OWN CURRENCY MAP.
    `EXCH_CCY` has no `FRA` entry, so in that fixture Frankfurt scores one lower than Xetra on the
    currency match and loses on score rather than on the tie-break. In PRODUCTION the currency comes
    from `gurufocus_exchange`, where `FRA` is EUR — so the tie is real there and invisible here."""

    def test_xetra_beats_the_floor_when_both_quote_eur(self):
        ccy = {**EXCH_CCY, "FRA": "EUR"}
        r = pick_listing([{"symbol": "BAYN", "exchange": "FRA"},
                          {"symbol": "BAYN", "exchange": "XTER"}],
                         symbol_hint="BAYN.DE", currency_hint="EUR", exchange_currency=ccy)
        assert r.listing == ("BAYN", "XTER")

    def test_and_the_order_of_the_candidates_does_not_matter(self):
        """⚠ THE VENDOR'S LIST ORDER IS NOT A PREFERENCE. `max` keeps the first of equal keys, so a
        tie-break that did not actually separate these would silently follow whichever the vendor
        happened to list first."""
        ccy = {**EXCH_CCY, "FRA": "EUR"}
        for cands in ([{"symbol": "BAYN", "exchange": "XTER"},
                       {"symbol": "BAYN", "exchange": "FRA"}],
                      [{"symbol": "BAYN", "exchange": "FRA"},
                       {"symbol": "BAYN", "exchange": "XTER"}]):
            r = pick_listing(cands, symbol_hint="BAYN", currency_hint="EUR",
                             exchange_currency=ccy)
            assert r.listing == ("BAYN", "XTER")

    def test_but_a_company_held_ONLY_on_the_floor_still_resolves(self):
        """⚠⚠ THE POINT OF A TIE-BREAK IS THAT IT ONLY BREAKS TIES. Verisure is on `FRA` and nowhere
        else we hold; demoting the venue must not refuse it, or this change would have taken data
        away from the very company it was made to unblock."""
        ccy = {**EXCH_CCY, "FRA": "EUR"}
        r = pick_listing([{"symbol": "6R9", "exchange": "FRA"}],
                         symbol_hint="6R9", currency_hint="EUR", exchange_currency=ccy)
        assert r.status == "ok"
        assert r.listing == ("6R9", "FRA")

    def test_a_us_listing_still_outranks_both(self):
        """⚠ THE NEW KEY SITS AFTER THE US ONE, so it cannot disturb the existing preference."""
        ccy = {**EXCH_CCY, "FRA": "EUR"}
        r = pick_listing([{"symbol": "BAYZF", "exchange": "OTCPK"},
                          {"symbol": "BAYZF", "exchange": "FRA"}],
                         symbol_hint="BAYZF", currency_hint="USD", exchange_currency=ccy)
        assert r.listing.exchange == "OTCPK"
