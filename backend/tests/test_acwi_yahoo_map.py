"""iShares ticker + exchange -> the Yahoo symbol the asset grid is keyed by.

⚠⚠ THE ALTERNATIVE TO THIS MAP IS A NAME MATCH, AND ON THIS DATA A NAME MATCH IS WRONG. Measured
with `scripts/measure_acwi_asset_gap.py`: `BERKSHIRE HATHAWAY CLASS B` matched Berkshire **A**,
`NEWMONT` (United States) matched the Australian CDI line, and `MIZUHO FINANCIAL GROUP` (Japan)
matched MAGELLAN FINANCIAL GROUP (Australia) — a different company on a different continent, which
is this codebase's WisdomTree Coffee -> Luckin Coffee failure reproduced. A ticker plus an exchange
is a deterministic address, and these tests pin the three places that determinism is not obvious.

Unit-only: a pure function, no database, no network.
"""
from __future__ import annotations

from index_universe.acwi.yahoo_map import EXCHANGE_SUFFIX, yahoo_symbol


class TestTheOrdinaryCase:
    def test_a_suffixed_venue(self):
        assert yahoo_symbol("CSU", "Toronto Stock Exchange") == "CSU.TO"
        assert yahoo_symbol("BHP", "Asx - All Markets") == "BHP.AX"
        assert yahoo_symbol("SHEL", "London Stock Exchange") == "SHEL.L"

    def test_a_us_venue_has_no_suffix(self):
        assert yahoo_symbol("XOM", "NYSE") == "XOM"
        assert yahoo_symbol("NVDA", "NASDAQ") == "NVDA"


class TestTheThreeThatAreNotObvious:
    def test_hong_kong_is_zero_padded_to_four_digits(self):
        """⚠ iShares files `700`, Yahoo wants `0700.HK`. Without the pad the symbol is simply
        absent from the grid and the constituent goes missing for a formatting reason that reads
        as a data gap."""
        assert yahoo_symbol("700", "Hong Kong Exchanges And Clearing Ltd") == "0700.HK"
        assert yahoo_symbol("1299", "Hong Kong Exchanges And Clearing Ltd") == "1299.HK"

    def test_a_dot_in_a_us_ticker_becomes_a_hyphen(self):
        """⚠ Yahoo's own convention, and what `asset_execution` stores: `BRK.B` -> `BRK-B`."""
        assert yahoo_symbol("BRK.B", "NYSE") == "BRK-B"

    def test_the_nordics_are_one_label_over_three_markets(self):
        """⚠⚠ `Nasdaq Omx Nordic` covers Stockholm, Helsinki and Copenhagen — three Yahoo suffixes
        behind one exchange string. The row's country is what separates them, and guessing would
        put a Swedish bank on a Danish ticker that may well exist."""
        # ⚠⚠ THE SHARE CLASS IS A HYPHEN, AND THIS TEST ASSERTED A SPACE UNTIL 2026-09-01. It
        # expected `VOLV B.ST` and `NOVO B.CO` — symbols that exist nowhere. Checked against
        # `asset_execution`: `VOLV-B.ST` is AB Volvo and `NOVO-B.CO` is Novo Nordisk, both held and
        # priced, while the spaced spellings match no row at all. The test was green because it
        # pinned the builder's output rather than anything the output has to join to.
        assert yahoo_symbol("VOLV B", "Nasdaq Omx Nordic", "Sweden") == "VOLV-B.ST"
        assert yahoo_symbol("NOKIA", "Nasdaq Omx Nordic", "Finland") == "NOKIA.HE"
        assert yahoo_symbol("NOVO B", "Nasdaq Omx Nordic", "Denmark") == "NOVO-B.CO"

    def test_a_nordic_row_with_no_country_is_refused(self):
        assert yahoo_symbol("NOKIA", "Nasdaq Omx Nordic", "") is None


class TestItRefusesRatherThanGuesses:
    """⚠⚠ THE WHOLE SAFETY ARGUMENT. A venue this map cannot place must produce NO row — pointing
    at a plausible wrong listing is exactly the failure the name match made, and it is invisible:
    the constituent appears, priced, in the wrong currency, under the wrong company."""

    def test_an_unknown_exchange(self):
        assert yahoo_symbol("XYZ", "Some Exchange We Have Never Seen") is None

    def test_an_empty_ticker(self):
        assert yahoo_symbol("", "NYSE") is None
        assert yahoo_symbol("   ", "Toronto Stock Exchange") is None

    def test_a_hong_kong_row_with_no_digits(self):
        assert yahoo_symbol("ABC", "Hong Kong Exchanges And Clearing Ltd") is None


class TestTheMapItself:
    def test_the_us_venues_map_to_a_bare_symbol_not_to_none(self):
        """⚠ `''` AND `None` ARE DIFFERENT ANSWERS HERE and the code branches on it: an empty
        suffix means "a US symbol, no suffix", absence means "I cannot place this venue". A US
        venue accidentally left out of the map would silently drop 559 constituents."""
        for venue in ("NYSE", "NASDAQ"):
            assert EXCHANGE_SUFFIX[venue] == ""

    def test_every_suffix_starts_with_a_dot_or_is_empty(self):
        for venue, suffix in EXCHANGE_SUFFIX.items():
            assert suffix == "" or suffix.startswith("."), venue


class TestTheTickerSpellingsTheFileUses:
    """⚠⚠ THREE TRANSFORMS, EACH ADDED ONLY AFTER PROBING THE SPELLING AGAINST `asset_execution`
    AND FINDING THE ROW. Before them, Novo Nordisk, Nordea, BP, BAE, National Grid, Rolls-Royce and
    all twelve Turkish constituents resolved to symbols that exist nowhere — and the failure was
    SILENT, because they were then picked up by the country-gated interlisting fallback in
    `asset_membership`, which is a rule meant to be a last resort rather than the primary path for
    a whole venue."""

    def test_a_trailing_dot_is_the_lses_padding_and_goes(self):
        """⚠ `BP..L` MATCHES NOTHING. iShares pads London tickers to a fixed width with dots."""
        assert yahoo_symbol("BP.", "London Stock Exchange") == "BP.L"
        assert yahoo_symbol("BA.", "London Stock Exchange") == "BA.L"
        assert yahoo_symbol("NG.", "London Stock Exchange") == "NG.L"
        assert yahoo_symbol("RR.", "London Stock Exchange") == "RR.L"

    def test_a_space_becomes_a_hyphen(self):
        """⚠ THE NORDIC SHARE-CLASS CONVENTION. `NOVO B` is Yahoo's `NOVO-B.CO`."""
        assert yahoo_symbol("NOVO B", "Omx Nordic Exchange Copenhagen A/S") == "NOVO-B.CO"
        assert yahoo_symbol("NDA FI", "Nasdaq Omx Helsinki Ltd.") == "NDA-FI.HE"

    def test_istanbuls_board_code_is_not_part_of_the_ticker(self):
        assert yahoo_symbol("ASELS.E", "Istanbul Stock Exchange") == "ASELS.IS"
        assert yahoo_symbol("THYAO.E", "Istanbul Stock Exchange") == "THYAO.IS"

    def test_a_share_class_is_a_hyphen_on_every_venue(self):
        """⚠⚠ THE SUFFIXED BRANCH USED TO SKIP THIS AND THE US BRANCH DID NOT, so `BBD.B` became
        `BBD.B.TO`, a symbol that exists nowhere. Probed over all 32 non-US rows carrying an
        internal dot: 8 join as a hyphen, 0 as a dot."""
        assert yahoo_symbol("BBD.B", "Toronto Stock Exchange") == "BBD-B.TO"
        assert yahoo_symbol("TECK.B", "Toronto Stock Exchange") == "TECK-B.TO"
        assert yahoo_symbol("BT.A", "London Stock Exchange") == "BT-A.L"
        assert yahoo_symbol("BRK.B", "NYSE") == "BRK-B"

    def test_the_class_marker_is_KEPT_not_dropped(self):
        """⚠⚠ THE TWO CLASSES ARE DIFFERENT SECURITIES. Bombardier B and Bombardier A are not
        interchangeable; a "drop everything after the dot" rule would merge them into whichever
        line we happen to hold — the Berkshire B -> Berkshire A failure this module exists to
        prevent. Istanbul's `.E` is stripped because it is a BOARD, not a class."""
        assert yahoo_symbol("BBD.A", "Toronto Stock Exchange") == "BBD-A.TO"
        assert yahoo_symbol("BBD.B", "Toronto Stock Exchange") != yahoo_symbol(
            "BBD.A", "Toronto Stock Exchange")


class TestTheKeysAreTheFilesOwnSpellings:
    """⚠⚠ TWELVE KEYS ONCE NAMED A VENUE THE FILE NEVER WRITES (fixed 2026-09-01). The map had been
    built from the asset grid's exchange names rather than the export's, so `Euronext Brussels` sat
    in it while the file said `Nyse Euronext - Euronext Brussels`. A dead key is silent: the venue
    resolves to None, the row is counted as "venue unknown", and the map LOOKS complete. Measured:
    137 of 2,270 equity rows unplaced, 78 of them instruments we already hold and price."""

    def test_the_export_spellings_resolve(self):
        for ticker, venue, expected in (
            ("ABI", "Nyse Euronext - Euronext Brussels", "ABI.BR"),
            ("EDP", "Nyse Euronext - Euronext Lisbon", "EDP.LS"),
            ("PKO", "Warsaw Stock Exchange/Equities/Main Market", "PKO.WA"),
            ("ETE", "Athens Exchange S.A. Cash Market", "ETE.AT"),
            ("RYA", "Irish Stock Exchange - All Market", "RYA.IR"),
            ("5274", "Gretai Securities Market", "5274.TWO"),
            ("CBOE", "Cboe BZX", "CBOE"),
            ("532483", "Bse Ltd", "532483.BO"),
        ):
            assert yahoo_symbol(ticker, venue) == expected, venue

    def test_russia_stays_refused(self):
        """⚠ NOT AN OVERSIGHT. Yahoo delisted Russian equities, so there is no series to point at
        under any suffix — the honest answer is no row."""
        assert yahoo_symbol("PLZL", "Standard-Classica-Forts", "Russian Federation") is None

    def test_an_unlisted_placeholder_stays_refused(self):
        assert yahoo_symbol("--", "NO MARKET (E.G. UNLISTED)") is None
