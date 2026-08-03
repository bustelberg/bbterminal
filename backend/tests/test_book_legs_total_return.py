"""The Analyse modal and the row that opens it must report the SAME return for the same holding.

Both surfaces are AIRS-sourced, and both were right about something — they just answered different
questions and looked identical doing it:

    the expanded row   `(current + gross dividend + dividend_tax) ÷ Beginwaarde − 1`   TOTAL
    the Analyse modal  `current ÷ Beginwaarde − 1`                                     price only

So a holding that paid a dividend read one way in the table and another in the modal, on the same
page, for the same book, with nothing on either surface saying which was which. `book_legs` now
uses the row's numerator, and takes the income from the row's own loader (`_direct_result`, keyed
on `holding_name`) rather than reading the Mutaties journal a second time.

⚠ THE TAX IS ADDED, NOT SUBTRACTED. `tax_eur` is already negative — AIRS books withholding as a
debit — so `gross + tax` IS the net. The intuitive `- tax` adds the withholding back and overstates
every foreign holding by twice it, silently, because the result is still a plausible number.

⚠ AND IT MAKES THE PORTFOLIO SIDE A TOTAL RETURN AGAINST A PRICE-RETURN BENCHMARK. The rebuilt
index carries no dividends (~1.1pp/yr, measured against ISAC), so attributed naively a dividend
reads as selection skill. That is surfaced per leg (`return_basis`), not absorbed — the alternative
is two different numbers for one holding on one screen, which is the worse of the two lies.
"""
from __future__ import annotations

from dataclasses import dataclass

import pytest

import routers._airs_attribution_basis as basis


@dataclass
class _Income:
    gross_eur: float
    tax_eur: float          # ⚠ NEGATIVE, as AIRS books it
    payments: int = 1


@pytest.fixture
def book(monkeypatch):
    """One paired book with three holdings: a dividend payer with withholding, a payer with
    none, and a holding the journal has no line for at all."""
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
    monkeypatch.setattr(basis, "list_account_links",
                        lambda: {"accounts": [{"portefeuille": "BOOK_A",
                                               "model_portfolio_id": 7}]},
                        raising=False)
    monkeypatch.setattr("routers._airs_account_links.list_account_links",
                        lambda: {"accounts": [{"portefeuille": "BOOK_A",
                                               "model_portfolio_id": 7}]})
    monkeypatch.setattr("routers._airs_holding_isin.resolve_account_isins",
                        lambda _p: {"rows": rows})
    monkeypatch.setattr("routers._airs_portfolio_analysis._expand_book_rows", lambda r: r)
    monkeypatch.setattr("routers._airs_accounts._direct_result",
                        lambda _p, _n: (income, {"gross": None, "tax": None, "funds": None}))
    return rows


def _by_name(legs):
    return {leg["airs_name"]: leg for leg in legs}


class TestTheLegMatchesTheRowsReturnColumn:
    def test_the_income_is_in_the_numerator(self, book):
        """`holdingTotalReturn`'s definition, to the digit: (1100 + 100 − 15) / 1000 − 1."""
        leg = _by_name(basis.book_legs(7))["US Payer"]
        assert leg["return_pct"] == pytest.approx(18.5)

    def test_the_withholding_is_ADDED_because_it_is_already_negative(self, book):
        """The trap: `- tax_eur` would give (1100 + 100 + 15)/1000 − 1 = 21.5% — plausible, and
        wrong by twice the withholding on every foreign holding."""
        leg = _by_name(basis.book_legs(7))["US Payer"]
        assert leg["return_pct"] != pytest.approx(21.5)
        assert leg["income_eur"] == pytest.approx(85.0)

    def test_a_holding_with_no_withholding_is_unaffected(self, book):
        leg = _by_name(basis.book_legs(7))["NL Payer"]
        assert leg["return_pct"] == pytest.approx(5.0)     # (1000 + 50) / 1000 − 1

    def test_a_price_only_return_is_no_longer_what_is_reported(self, book):
        """Guards the regression directly: the old formula gave +10.00% for the US payer."""
        assert _by_name(basis.book_legs(7))["US Payer"]["return_pct"] != pytest.approx(10.0)


class TestAbsencesStayApart:
    def test_no_journal_line_is_None_income_not_zero(self, book):
        """"paid nothing" and "we have not read this book's journal" are different claims and
        only one of them is safe to make — same rule the row's own column follows."""
        leg = _by_name(basis.book_legs(7))["Silent"]
        assert leg["income_eur"] is None
        # ...and with no income to add, the return is the price return, which is correct here.
        assert leg["return_pct"] == pytest.approx(-10.0)

    def test_cash_has_no_return_at_all(self, book):
        leg = _by_name(basis.book_legs(7))["Cash"]
        assert leg["return_pct"] is None
        assert leg["is_cash"] is True

    def test_cash_still_carries_its_weight(self, book):
        """It has no return, but it is real exposure — dropping its weight would renormalise the
        book over its non-cash part and overstate every other holding's share."""
        legs = _by_name(basis.book_legs(7))
        assert legs["Cash"]["weight_pct"] == pytest.approx(500 / 3500 * 100)


class TestTheBasisIsDeclared:
    """The benchmark this gets compared against is a PRICE return. Saying so per leg is what
    keeps a dividend from reading as selection skill without anyone noticing."""

    def test_every_leg_says_what_it_includes(self, book):
        assert {leg["return_basis"] for leg in basis.book_legs(7)} == {"total"}


class TestTheWeightIsStillTheOpeningValue:
    """Unchanged by this — pinned because the income change touches the same rows. Weighting by
    the CURRENT value overweights the winners: measured on AITopSelectie, +58.75% against the
    book's true +44.99%."""

    def test_weights_are_beginwaarde_shares(self, book):
        legs = _by_name(basis.book_legs(7))
        assert legs["US Payer"]["weight_pct"] == pytest.approx(1000 / 3500 * 100)
        assert sum(leg["weight_pct"] for leg in basis.book_legs(7)) == pytest.approx(100.0)
