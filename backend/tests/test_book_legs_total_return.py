"""Both sides of a Brinson subtraction must price the same instrument the same way.

Selection effect is `w_b × (R_p,bucket − R_b,bucket)` — it subtracts our return from the INDEX's
for the same names. The benchmark is rebuilt from `asset_price`, so pricing our side off AIRS put
one instrument on the two sides of that subtraction at two different numbers, and Brinson booked
the difference as skill. Measured 2026-08-05, direct holdings only, one window:

    ASML Holding              AIRS +49.68%   yfinance +63.70%   -14.03pp
    Lam Research              AIRS +74.16%   yfinance +89.90%   -15.74pp
    AITopSelectie   median |gap| 3.39pp, 18 of 20 legs over 1pp
    BUS_Offensief   median |gap| 2.20pp, 20 of 25 legs over 1pp

Hold ASML at exactly its index weight and the old basis still reported a 14pp selection effect on
it. The gaps were also ONE-DIRECTIONAL (the holdings snapshot trails the latest close), so they
biased rather than cancelled.

⚠ IT FIXED A SECOND FAULT THAT WAS WORSE. `_expand_book_rows` splits a certificate's start AND
current value by each holding's share, so every instrument inside one came out with the WRAPPER's
return — BUS_Offensief's 23 wrapped legs carried FOUR distinct returns between them. Pricing the
instrument closes both faults with one source: 48 distinct returns across 48 priced legs.

⚠ WHAT THIS DELIBERATELY GAVE UP. These legs no longer reproduce AIRS's `cumulatief_rendement` —
they are not AIRS's numbers. That is correct for a RELATIVE decomposition (a difference between two
vendors is not alpha), and `airs_return_pct` rides along on every leg so the gap can be shown
rather than discovered. The income is still loaded and still reported per leg, but is OUT of the
comparison: both sides are price returns now, so a dividend can no longer read as selection skill
(~1.1pp/yr, measured against ISAC).
"""
from __future__ import annotations

from dataclasses import dataclass

import pytest

import routers._airs_attribution_basis as basis

START = "2026-01-01"


@dataclass
class _Income:
    gross_eur: float
    tax_eur: float          # ⚠ NEGATIVE, as AIRS books it
    payments: int = 1


@pytest.fixture
def book(monkeypatch):
    """One paired book: a dividend payer with withholding, a payer with none, a holding the
    journal has no line for, and cash.

    ⚠ THE AIRS VALUES AND THE PRICE MARKS DISAGREE ON PURPOSE — that disagreement IS the subject.
    `US Payer` is +10% on AIRS's own valuation, +18.5% once its dividend is added, and +25% on the
    price series, so a test that reads one where it means another cannot pass by accident.
    """
    rows = [
        {"holding_name": "US Payer", "isin": "US0000000001", "start_value_eur": 1000.0,
         "current_value_eur": 1100.0, "asset_class": "Equity", "bucket": "Stocks"},
        {"holding_name": "NL Payer", "isin": "NL0000000002", "start_value_eur": 1000.0,
         "current_value_eur": 1000.0, "asset_class": "Equity", "bucket": "Stocks"},
        {"holding_name": "Silent", "isin": "US0000000003", "start_value_eur": 1000.0,
         "current_value_eur": 900.0, "asset_class": "Equity", "bucket": "Stocks"},
        {"holding_name": "Cash", "isin": None, "start_value_eur": 500.0,
         "current_value_eur": 500.0, "asset_class": "Cash", "bucket": "Cash"},
    ]
    income = {
        # 100 gross, 15 withheld -> 85 net. The sign trap lives here.
        "US Payer": _Income(gross_eur=100.0, tax_eur=-15.0),
        "NL Payer": _Income(gross_eur=50.0, tax_eur=0.0),
    }
    marks = {"US0000000001": {"return_pct": 25.0},
             "NL0000000002": {"return_pct": -4.0},
             "US0000000003": {"return_pct": 7.5}}
    monkeypatch.setattr(basis, "list_account_links",
                        lambda: {"accounts": [{"portefeuille": "BOOK_A",
                                               "model_portfolio_id": 7}]},
                        raising=False)
    monkeypatch.setattr("routers._airs_account_links.list_account_links",
                        lambda: {"accounts": [{"portefeuille": "BOOK_A",
                                               "model_portfolio_id": 7}]})
    monkeypatch.setattr("routers._airs_holding_isin.resolve_account_isins",
                        lambda _p, **_k: {"rows": rows})
    monkeypatch.setattr("routers._airs_portfolio_analysis._expand_book_rows", lambda r: r)
    monkeypatch.setattr("routers._airs_accounts._direct_result",
                        lambda _p, _n: (income, {"gross": None, "tax": None, "funds": None}))
    monkeypatch.setattr(basis, "compute_holding_marks", lambda _i, _s: marks)
    return rows


def _by_name(legs):
    return {leg["airs_name"]: leg for leg in legs}


class TestTheReturnComesFromThePriceSeries:
    """⚠ The benchmark is built from `asset_price`. Our side must be too, or the same instrument
    carries two different numbers into one subtraction and the difference reads as skill."""

    def test_the_leg_uses_the_mark_not_the_books_own_valuation(self, book):
        leg = _by_name(basis.book_legs(7, START))["US Payer"]
        assert leg["return_pct"] == pytest.approx(25.0)      # the price series
        assert leg["return_pct"] != pytest.approx(18.5)      # AIRS, income included
        assert leg["return_pct"] != pytest.approx(10.0)      # AIRS, price only

    def test_every_leg_declares_a_PRICE_basis(self, book):
        # The old basis said "total": income in the numerator against a price-return benchmark,
        # which read every dividend as selection skill.
        assert {leg["return_basis"] for leg in basis.book_legs(7, START)} == {"price"}

    def test_a_holding_the_price_series_cannot_reach_has_no_return(self, book, monkeypatch):
        # ⚠ None, so `split_legs` reports it as `unpriced` — the one exclusion that is a genuine
        # gap rather than an answer, and the one the panel already warns about loudly.
        monkeypatch.setattr(basis, "compute_holding_marks", lambda _i, _s: {})
        assert _by_name(basis.book_legs(7, START))["US Payer"]["return_pct"] is None


class TestAirsIsCarriedButNotUsed:
    """The book's own figure is what makes this panel's divergence from `cumulatief_rendement`
    explainable rather than mysterious."""

    def test_the_airs_figure_rides_along(self, book):
        """`holdingTotalReturn`'s definition, to the digit: (1100 + 100 − 15) / 1000 − 1."""
        leg = _by_name(basis.book_legs(7, START))["US Payer"]
        assert leg["airs_return_pct"] == pytest.approx(18.5)

    def test_the_withholding_is_ADDED_because_it_is_already_negative(self, book):
        """The trap: `- tax_eur` would give (1100 + 100 + 15)/1000 − 1 = 21.5% — plausible, and
        wrong by twice the withholding on every foreign holding."""
        leg = _by_name(basis.book_legs(7, START))["US Payer"]
        assert leg["airs_return_pct"] != pytest.approx(21.5)
        assert leg["income_eur"] == pytest.approx(85.0)

    def test_income_is_reported_but_is_NOT_in_the_compared_return(self, book):
        # Both sides are price returns now. The income still has to be visible — a reader is owed
        # the fact that it exists and sits outside the comparison.
        leg = _by_name(basis.book_legs(7, START))["NL Payer"]
        assert leg["income_eur"] == pytest.approx(50.0)
        assert leg["return_pct"] == pytest.approx(-4.0)


class TestAbsencesStayApart:
    def test_no_journal_line_is_None_income_not_zero(self, book):
        """"paid nothing" and "we have not read this book's journal" are different claims and only
        one of them is safe to make — same rule the row's own column follows."""
        leg = _by_name(basis.book_legs(7, START))["Silent"]
        assert leg["income_eur"] is None
        assert leg["return_pct"] == pytest.approx(7.5)

    def test_cash_has_no_return_at_all(self, book):
        leg = _by_name(basis.book_legs(7, START))["Cash"]
        assert leg["return_pct"] is None
        assert leg["is_cash"] is True

    def test_cash_still_carries_its_weight(self, book):
        """It has no return, but it is real exposure — dropping its weight would renormalise the
        book over its non-cash part and overstate every other holding's share."""
        legs = _by_name(basis.book_legs(7, START))
        assert legs["Cash"]["weight_pct"] == pytest.approx(500 / 3500 * 100)


class TestTheWeightIsStillTheOpeningValue:
    """⚠ UNCHANGED, AND DELIBERATELY. A weight does not need the two sides to share a vendor —
    Brinson compares OUR weight against the INDEX's by construction. Only the return had to be
    unified. Weighting by the CURRENT value overweights the winners: measured on AITopSelectie,
    +58.75% against the book's true +44.99%."""

    def test_weights_are_beginwaarde_shares(self, book):
        legs = _by_name(basis.book_legs(7, START))
        assert legs["US Payer"]["weight_pct"] == pytest.approx(1000 / 3500 * 100)
        assert sum(leg["weight_pct"] for leg in basis.book_legs(7, START)) == pytest.approx(100.0)


class TestBothPathsShareOneBasis:
    def test_the_book_path_takes_the_window_like_the_model_path(self):
        """⚠ It used to ignore `start` entirely, so switching `source` changed the VENDOR as well
        as the weights — two variables at once, on a control the reader thinks moves one."""
        import inspect

        assert "start" in inspect.signature(basis.book_legs).parameters
        assert "compute_holding_marks" in inspect.getsource(basis.book_legs)
