"""One AIRS account's holdings: identity, Class, and the price check.

⚠ THIS FILE USED TO PIN A FUZZY NAME MATCHER — ~200 lines of scoring, 1:1 assignment and refusal
logic that recovered each holding's ISIN from a PAIRED model portfolio. All of it was deleted
2026-07-23, when the Vermogensoverzicht started carrying `ISIN-code`, and its tests went with it.
The four mechanisms and their measured failures are recorded in the module docstring rather than
here, because a test for code that no longer exists is a test nobody can run.

What remains is what the pairing never provided: deduping the account, classifying the instrument
from the asset grid, and checking OUR price against AIRS's implied one.
"""
from __future__ import annotations

import pytest

from asset_pipeline.price_refresh import DEFAULT_STALE_DAYS
from routers._airs_holding_isin import (
    BUCKET_ALTS,
    BUCKET_BONDS,
    BUCKET_CASH,
    BUCKET_EQUITY,
    BUCKET_ORDER,
    _CHECK_STALE_DAYS,
    _MAX_CLOSE_LAG_DAYS,
    _close_lag_days,
    _dedupe,
    classify_bucket,
)


class TestAStalePriceIsNotAWrongListing:
    """⚠ THE CHECK IS ONLY AS GOOD AS THE PRICE UNDER IT. AIRS's implied price is compared with our
    own close at a 15% tolerance — a band meant to catch share-class errors of 19x and 20x. A series
    that merely STOPPED UPDATING drifts past 15% on any ordinary mover, and the row then reads
    `price_mismatch`: "our listing is wrong" about a listing that is perfect. Measured 2026-07-29:
    AMD went 552.33 -> 430.05 (-22%) with every stored bar matching Yahoo to the cent.

    The lag is what separates the two, and it is measured against the day AIRS VALUED THE BOOK, not
    against today — an account whose snapshot is a fortnight old is correctly compared with a
    fortnight-old close.
    """

    def test_a_close_from_the_valuation_day_has_no_lag(self):
        assert _close_lag_days({"date": "2026-07-29"}, "2026-07-29") == 0

    def test_the_lag_is_against_the_valuation_date_not_today(self):
        """Both sides a fortnight old is a MATCHED pair, not a stale one."""
        assert _close_lag_days({"date": "2026-07-14"}, "2026-07-15") == 1

    def test_a_weekend_gap_stays_inside_the_tolerance(self):
        """Friday's close against a Monday valuation is the normal case, not a finding."""
        assert _close_lag_days({"date": "2026-07-24"}, "2026-07-27") <= _MAX_CLOSE_LAG_DAYS

    def test_the_measured_amd_gap_is_outside_it(self):
        """Six days behind is what produced the false alarm this rule exists to stop."""
        assert _close_lag_days({"date": "2026-07-22"}, "2026-07-29") == 7
        assert _close_lag_days({"date": "2026-07-21"}, "2026-07-29") > _MAX_CLOSE_LAG_DAYS

    def test_the_measured_amat_gap_is_inside_it_which_is_why_the_refresh_must_lead(self):
        """⚠ THE LAG GUARD ALONE CANNOT SAVE THIS — AND THAT IS THE POINT OF `_CHECK_STALE_DAYS`.

        Applied Materials, 2026-07-30: a flawless mapping (AMAT, NasdaqGS, USD) whose stored close
        was 07-27 against a 07-30 valuation — THREE days, inside the guard, so the row was judged.
        On that close the ratio was 0.846 and it read as a wrong listing; on 07-29's close it is
        1.002. The only thing that prevents it is fetching the gap first, and the fleet's own
        `stale_days=3` did not consider a two-day-old series worth fetching.
        """
        assert _close_lag_days({"date": "2026-07-27"}, "2026-07-30") <= _MAX_CLOSE_LAG_DAYS
        assert _CHECK_STALE_DAYS < DEFAULT_STALE_DAYS

    def test_a_missing_close_or_date_yields_no_lag(self):
        """None must mean "no opinion" — those rows are already `unpriced`, and inventing a lag
        would move them into a verdict about a price we do not have."""
        assert _close_lag_days(None, "2026-07-29") is None
        assert _close_lag_days({}, "2026-07-29") is None
        assert _close_lag_days({"date": None}, "2026-07-29") is None

    def test_an_unparseable_date_yields_no_lag_rather_than_raising(self):
        assert _close_lag_days({"date": "not-a-date"}, "2026-07-29") is None


class TestTheAccountBillsOneInstrumentOnSeveralLines:
    """⚠ AIRS lists one instrument on several rows — `6,5% Rabobank Certificaten 14-perp.` at
    2.60% AND 0.01%. Two rows for one instrument is two a reader has to reconcile, and every
    per-holding figure (weight, value, income) would be split across them."""

    def test_lines_are_merged_and_counted(self):
        rows = _dedupe([
            {"holding_name": "Rabobank", "quantity": 100, "weight": 0.026,
             "current_value_eur": 2600.0, "start_value_eur": 2500.0},
            {"holding_name": "Rabobank", "quantity": 1, "weight": 0.0001,
             "current_value_eur": 10.0, "start_value_eur": 9.0},
        ])
        assert len(rows) == 1
        assert rows[0]["lines"] == 2
        assert rows[0]["quantity"] == 101
        assert rows[0]["current_value_eur"] == 2610.0
        assert rows[0]["weight"] == pytest.approx(0.0261)   # float sum, not a fixed-point one

    def test_a_single_line_is_untouched_and_still_says_so(self):
        rows = _dedupe([{"holding_name": "ASML Holding", "quantity": 27,
                         "current_value_eur": 41834.0}])
        assert len(rows) == 1 and rows[0]["lines"] == 1

    def test_a_nameless_row_is_dropped(self):
        assert _dedupe([{"holding_name": "  ", "quantity": 1}]) == []


class TestTheClassComesFromTheGridAndTheName:
    """⚠ AIRS's `categorie` WAS DROPPED with the pairing (2026-07-23), so the asset grid and the
    holding's name are all that is left. Measured across all 668 holdings, removing it moved 58
    rows; two whole groups were real regressions and are recovered here from yfinance — the same
    source the Sector column already shows.
    """

    def _grid(self, **kw):
        return {"asset_class": kw.get("asset_class"), "sector": kw.get("sector"),
                "name": kw.get("name"), "leonteq_name": None, "leonteq_product_type": None}

    def test_a_reit_is_alternatives_not_an_ordinary_equity(self):
        """40 holdings — Simon Property, Prologis, Welltower, Aedifica, Vonovia… AIRS called them
        `VAS`; yfinance calls the sector `Real Estate`, which is the same fact."""
        g = self._grid(asset_class="equity", sector="Real Estate", name="Simon Property Group")
        assert classify_bucket(None, False, "US8288061091", "Simon Property Group", g) == BUCKET_ALTS

    def test_the_real_estate_test_runs_BEFORE_the_equity_test(self):
        """⚠ Order decides it: a REIT's grid asset_class IS `equity`, so an equity-first branch
        returns Equity and the sector is never consulted."""
        g = self._grid(asset_class="equity", sector="Real Estate", name="Prologis")
        assert classify_bucket(None, False, "US74340W1036", "Prologis", g) != BUCKET_EQUITY

    def test_a_bond_etf_missing_from_the_grid_is_still_bonds(self):
        """⚠ `iShares iBonds 2032 Term Corp UCITS ETF USD` is not in asset_grid at all, and
        `\\bbond` does NOT match "iBonds" — there is no word boundary before the b. It classified
        as an equity ETF until `ibond` was added."""
        assert classify_bucket(None, True, "IE000XYZ12345",
                               "iShares iBonds 2032 Term Corp UCITS ETF USD", {}) == BUCKET_BONDS

    def test_a_bond_etf_the_grid_calls_equity_is_still_bonds(self):
        """⚠ `iShares Euro HY Corp Bd ETF EUR` sits in the grid as asset_class 'equity',
        sector 'equity' — simply wrong for a bond fund. "Bd" is the only bond tell on the row."""
        g = self._grid(asset_class="equity", sector="equity", name="iShares Euro HY Corp Bd ETF")
        assert classify_bucket(None, True, "IE00B66F4759",
                               "iShares Euro HY Corp Bd ETF EUR", g) == BUCKET_BONDS

    def test_fixed_income_in_the_name_is_a_bond_tell(self):
        assert classify_bucket(None, False, "NL000FRESH01",
                               "Fresh Fixed Income Fund", {}) == BUCKET_BONDS

    def test_bd_is_word_bounded_so_it_cannot_fire_inside_a_word(self):
        g = self._grid(asset_class="equity", sector="Healthcare", name="Abbott Laboratories")
        assert classify_bucket(None, False, "US0028241000", "Abbott Laboratories", g) == BUCKET_EQUITY

    def test_an_ordinary_equity_is_untouched(self):
        g = self._grid(asset_class="equity", sector="Technology", name="ASML Holding NV")
        assert classify_bucket(None, False, "NL0010273215", "ASML Holding", g) == BUCKET_EQUITY

    def test_a_fund_with_no_bond_tell_is_equity_because_that_is_what_it_holds(self):
        """⚠ NOT ITS OWN BUCKET. `Equity ETF` was retired 2026-08-18: every bucket names what a
        holding INVESTS IN, and an equity fund invests in equity — a bond ETF has always been
        `Bonds` rather than "ETF Bonds", which is what made the equity split the inconsistent one.
        """
        g = self._grid(asset_class="etf", sector="etf", name="iShares Core MSCI World UCITS ETF")
        assert classify_bucket(None, True, "IE00B4L5Y983",
                               "iShares Core MSCI World", g) == BUCKET_EQUITY

    def test_the_etf_wrapper_never_changes_the_bucket(self):
        """⚠⚠ THE INVARIANT THE MERGE CREATED, AND THE ONE A REVERT WOULD BREAK FIRST. `is_etf` may
        still decide Equity-vs-Unclassified in the fallback, but it may never move a holding
        between two REAL classes — that is what "the wrapper is not the asset class" means.
        """
        for ac, isin, name, grid in (
            ("Equity", "NL0010273215", "ASML Holding",
             self._grid(asset_class="equity", sector="Technology", name="ASML Holding NV")),
            (None, "IE00B4L5Y983", "iShares Core MSCI World",
             self._grid(asset_class="equity", sector="Technology", name="iShares Core MSCI World")),
            (None, "IE00B4613386", "iShares Euro Corp Bond",
             self._grid(asset_class="bond", sector="bonds", name="iShares Euro Corp Bond UCITS")),
        ):
            assert (classify_bucket(ac, True, isin, name, grid)
                    == classify_bucket(ac, False, isin, name, grid)), name

    def test_the_retired_bucket_is_gone_from_the_order(self):
        """A key nothing renders must not be offerable by the manual-override picker either — the
        picker is built from `BUCKET_ORDER`, and the DB now rejects the value outright."""
        assert "Equity ETF" not in BUCKET_ORDER
        assert BUCKET_ORDER == ["Equity", "Bonds", "Alternatives", "Cash", "Unclassified"]

    def test_cash_resolves_with_no_isin_and_no_grid_row(self):
        """The cash line has no ISIN at all, so its name is the only thing that can identify it."""
        assert classify_bucket(None, False, None, "Effectenrekening", {}) == BUCKET_CASH
        assert classify_bucket(None, False, None, "Liquiditeiten", {}) == BUCKET_CASH

    def test_nothing_decides_means_unclassified_not_a_guess(self):
        """⚠ An honest "unsure". Folding it into Equity — the bucket a reader would least
        question — is how an unknown instrument becomes a confident wrong answer."""
        assert classify_bucket(None, False, "XX0000000000", "House Product", {}) == "Unclassified"
