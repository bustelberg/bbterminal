"""Invested capital for a stock this book reaches through a certificate.

THE PROBLEM. Bustelberg Offensief bought ONE instrument — the "Star Selection Index" certificate.
Looked through, it shows 21 stocks (Shopify, ServiceNow, Topicus...). None of them has a purchase
in this book's transactions, because this book never bought them, so "what did the money put into
Shopify return" has no denominator here.

THREE ANSWERS, IN ORDER OF HOW TRUE THEY ARE

  1. LOOK THROUGH to the child book. `StarTopSelectie OFF DYN` is the book that actually bought
     Shopify, and it has the dates and the sizes. This is a real per-stock measurement.
  2. THE WRAPPER'S OWN FIGURE, attributed as the wrapper's — one number for the whole certificate,
     shipped under a separate key so it is never mistaken for the leg's.
  3. NOTHING.

⚠ THE SHORTCUT THAT LOOKS LIKE (1) AND IS ACTUALLY WORSE THAN (3): split the certificate's capital
across its legs by today's weights. Both the result and the capital get the SAME share, so the rate
cancels and all 21 legs report the identical number — measured, -3.86% on every one. That is one
measurement copied 21 times wearing 21 different names, and it is indistinguishable on screen from
21 real ones. Never do it.

⚠ IT IS THE STRATEGY'S RETURN, NOT THIS BOOK'S. Bustelberg's own experience depends on when IT
bought the certificate; the strategy's depends on when IT bought Shopify. Only the second is
answerable from stored flows. `own_return_pct` (the Return column) already makes exactly this
compromise, so the two columns agree with each other rather than each being wrong differently.
"""
from __future__ import annotations

import pytest

from routers._airs_portfolio_analysis import _via_capital

# The certificate as the parent book sees it: one route in, labelled, naming the child book.
CHILD = "StarTopSelectie OFF DYN"


def _leg(name="Shopify", *, value=509.32, book_value=11243.16, book=CHILD, extra_sources=()):
    return {
        "name": name,
        "via_names": ["StarTopSelectie Offensief"],
        "via_holding_names": ["Star Selection Index"],
        "sources": [{"label": "StarTopSelectie Offensief", "model_id": 2094, "book": book,
                     "value_eur": value, "book_current_value_eur": book_value}, *extra_sources],
    }


# The child book's OWN ledger position for Shopify — real purchases, real dates.
CHILD_LEDGER = {CHILD: {"Shopify": {"name": "Shopify", "return_pct": 11.54,
                                    "avg_capital_eur": 10_000.0}}}
# The parent's ledger, which knows only the certificate.
WRAPPER = {"Star Selection Index": {"name": "Star Selection Index", "return_pct": -3.86,
                                    "avg_capital_eur": 52_974.24}}


class TestTheLookThrough:
    def test_it_uses_the_child_books_own_measurement(self):
        got = _via_capital(_leg(), WRAPPER, CHILD_LEDGER)
        assert got["capital_source"] == "lookthrough"
        assert got["capital_book"] == CHILD
        assert got["money_weighted_return_pct"] == 11.54

    def test_the_rate_transfers_and_the_euros_are_scaled_to_this_books_slice(self):
        """⚠ THE CHILD'S BALANCE SHEET IS NOT THIS BOOK'S. It put EUR 10,000 into Shopify; this
        book owns EUR 509 of an EUR 11,243 position — 4.53% of it. Reporting the unscaled figure
        would put the strategy's capital inside someone else's portfolio, and it would not tie to
        anything else in the row."""
        got = _via_capital(_leg(), WRAPPER, CHILD_LEDGER)
        assert got["avg_capital_eur"] == pytest.approx(10_000.0 * (509.32 / 11243.16), abs=0.01)
        # Scaling both sides leaves the rate untouched — that is the point of scaling at all.
        assert got["money_weighted_return_pct"] == 11.54
        # ...and the child's own figure rides along unscaled, so the card can say whose it is.
        assert got["via_avg_capital_eur"] == 10_000.0

    def test_it_beats_the_wrapper_figure_when_both_exist(self):
        got = _via_capital(_leg(), WRAPPER, CHILD_LEDGER)
        assert got["money_weighted_return_pct"] == 11.54       # the stock's
        assert "via_money_weighted_return_pct" not in got      # not the certificate's -3.86%

    def test_two_legs_of_one_certificate_get_DIFFERENT_numbers(self):
        """⚠ THE WHOLE TEST OF WHETHER THIS IS A MEASUREMENT. The weight-split shortcut gives every
        leg the same figure; a real look-through cannot, because the child bought them on different
        days at different sizes."""
        led = {CHILD: {"Shopify": {"return_pct": 11.54, "avg_capital_eur": 10_000.0},
                       "ServiceNow": {"return_pct": -4.20, "avg_capital_eur": 7_500.0}}}
        a = _via_capital(_leg("Shopify"), WRAPPER, led)
        b = _via_capital(_leg("ServiceNow"), WRAPPER, led)
        assert a["money_weighted_return_pct"] != b["money_weighted_return_pct"]


class TestTheFallback:
    def test_no_child_flows_leaves_the_wrapper_figure_under_its_OWN_key(self):
        """Measured today: not one child book has transactions, so this is the live path. The
        certificate's -3.86% is real and worth showing — as the certificate's."""
        got = _via_capital(_leg(), WRAPPER, {CHILD: {}})
        assert got["via_money_weighted_return_pct"] == -3.86
        assert got["via_avg_capital_eur"] == 52_974.24
        # ⚠ AND NEVER IN THE LEG'S OWN COLUMN. Shopify did not return -3.86% on the money.
        assert "money_weighted_return_pct" not in got
        assert "capital_source" not in got

    def test_a_child_that_lacks_THIS_instrument_falls_back_too(self):
        got = _via_capital(_leg("Shopify"), WRAPPER, {CHILD: {"ServiceNow": {"return_pct": 1.0}}})
        assert got["via_money_weighted_return_pct"] == -3.86

    def test_nothing_at_all_yields_nothing(self):
        assert _via_capital(_leg(), {}, {CHILD: {}}) == {}

    def test_a_child_position_with_no_rate_is_not_used(self):
        led = {CHILD: {"Shopify": {"return_pct": None, "avg_capital_eur": 10_000.0}}}
        got = _via_capital(_leg(), WRAPPER, led)
        assert got["via_money_weighted_return_pct"] == -3.86


class TestWhenThereIsNoSingleAnswer:
    def test_two_certificates_means_no_one_wrapper_figure(self):
        """A stock reached through two certificates has two invested-capital experiences; naming
        one picks a winner at random."""
        h = _leg()
        h["via_names"] = ["StarTopSelectie Offensief", "AITopSelectie Offensief"]
        h["via_holding_names"] = ["Star Selection Index", "AI Selection Index"]
        assert _via_capital(h, WRAPPER, CHILD_LEDGER) == {}

    def test_also_held_directly_means_the_row_is_only_partly_the_certificates(self):
        """⚠ `label: None` is the book's OWN shares. The row is then part its own position and part
        the wrapper's, and a single figure would describe only some of it."""
        h = _leg(extra_sources=({"label": None, "model_id": None, "value_eur": 50_489.0},))
        assert _via_capital(h, WRAPPER, CHILD_LEDGER) == {}

    def test_a_directly_held_row_is_never_touched(self):
        assert _via_capital({"name": "ASML", "via_names": [], "via_holding_names": [],
                             "sources": [{"label": None}]}, WRAPPER, CHILD_LEDGER) == {}


class TestScalingEdges:
    def test_an_unknown_child_position_size_withholds_the_euros_but_keeps_the_rate(self):
        """The rate is measured; the slice is not. A capital figure guessed at 100% would overstate
        it by whatever share this book does not own — better a blank beside a real rate."""
        got = _via_capital(_leg(book_value=0), WRAPPER, CHILD_LEDGER)
        assert got["money_weighted_return_pct"] == 11.54
        assert got["avg_capital_eur"] is None

    def test_a_child_with_a_rate_but_no_capital_still_reports_the_rate(self):
        led = {CHILD: {"Shopify": {"return_pct": 11.54, "avg_capital_eur": None}}}
        got = _via_capital(_leg(), WRAPPER, led)
        assert got["money_weighted_return_pct"] == 11.54
        assert got["avg_capital_eur"] is None

    def test_an_unnamed_child_book_cannot_be_looked_through(self):
        got = _via_capital(_leg(book=None), WRAPPER, CHILD_LEDGER)
        assert got["via_money_weighted_return_pct"] == -3.86
