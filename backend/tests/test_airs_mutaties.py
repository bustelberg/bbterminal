"""The AIRS Mutaties journal: the income a price return cannot see.

Every fixture below is a REAL row from `rapport_types=MUT` on BUS_Neutraal_Dyn, 2026-01-01 to
2026-07-23 (91 rows, 54 `Dividend` + 37 `Dividendbelasting`).
"""
from __future__ import annotations

from datetime import date
from io import BytesIO

import pandas as pd
import pytest

from airs_mutaties import (
    LEDGER_DIVIDEND,
    LEDGER_DIVIDEND_TAX,
    attach,
    direct_result,
    parse_mutaties,
)

COLS = ["Grootboek", "Boekdatum", "Omschrijving", "Fonds", "Rekening", "Debet", "Credit",
        "Valuta", "Rekvaluta", "Valutakoers", "Bedrag eur", "Bedrag vv", "Bedrag"]


def _row(grootboek, datum, fonds, debet, credit, ccy, koers, eur, vv):
    return {"Grootboek": grootboek, "Boekdatum": datum, "Omschrijving": f"Dividend {fonds}",
            "Fonds": fonds, "Rekening": "1234", "Debet": debet, "Credit": credit,
            "Valuta": ccy, "Rekvaluta": "EUR", "Valutakoers": koers,
            "Bedrag eur": eur, "Bedrag vv": vv, "Bedrag": round(eur, 2)}


# ASML pays in EUR and loses 15% Dutch withholding; Microsoft pays in USD.
ASML = [
    _row(LEDGER_DIVIDEND, "2026-02-18", "ASML Holding", 0.00, 62.40, "EUR", 1.0, 62.40, 62.40),
    _row(LEDGER_DIVIDEND, "2026-05-05", "ASML Holding", 0.00, 105.30, "EUR", 1.0, 105.30, 105.30),
    _row(LEDGER_DIVIDEND_TAX, "2026-02-18", "ASML Holding", 9.36, 0.0, "EUR", 1.0, -9.36, -9.36),
    _row(LEDGER_DIVIDEND_TAX, "2026-05-05", "ASML Holding", 15.80, 0.0, "EUR", 1.0, -15.80, -15.80),
]
MSFT = [
    _row(LEDGER_DIVIDEND, "2026-03-12", "Microsoft", 0.0, 49.14, "USD", 0.866026, 42.556508, 49.14),
    _row(LEDGER_DIVIDEND, "2026-06-11", "Microsoft", 0.0, 49.14, "USD", 0.866776, 42.593395, 49.14),
    _row(LEDGER_DIVIDEND_TAX, "2026-03-12", "Microsoft", 7.37, 0.0, "USD", 0.866026, -6.382610, -7.37),
    _row(LEDGER_DIVIDEND_TAX, "2026-06-11", "Microsoft", 7.37, 0.0, "USD", 0.866776, -6.388143, -7.37),
]


def _xls(rows: list[dict]) -> bytes:
    buf = BytesIO()
    pd.DataFrame(rows, columns=COLS).to_excel(buf, index=False)
    return buf.getvalue()


def _summary(rows: list[dict]):
    return direct_result(parse_mutaties(_xls(rows)))


class TestTheAmountIsAirssOwnSignedEuroFigure:
    """⚠ `Bedrag eur` is ALREADY signed and ALREADY converted. Re-deriving it from Debet/Credit
    or re-applying `Valutakoers` is how you double-count or flip a sign."""

    def test_the_tax_row_is_negative_so_net_is_a_plain_sum(self):
        d = _summary(ASML).by_fonds["ASML Holding"]
        assert d.gross_eur == pytest.approx(167.70)     # 62.40 + 105.30
        assert d.tax_eur == pytest.approx(-25.16)       # -9.36 + -15.80
        assert d.net_eur == pytest.approx(142.54)

    def test_a_foreign_payment_is_taken_in_eur_not_reconverted(self):
        """Microsoft pays 49.14 USD at 0.866026 = 42.556508 EUR, and AIRS has already done it."""
        d = _summary(MSFT).by_fonds["Microsoft"]
        assert d.gross_eur == pytest.approx(85.149903, abs=1e-6)
        assert d.net_eur == pytest.approx(72.38)   # rounded to cents: it is money
        # The local amount rides along but is NOT what the sum is built from.
        m = [x for x in parse_mutaties(_xls(MSFT)) if x.grootboek == LEDGER_DIVIDEND][0]
        assert m.amount_local == pytest.approx(49.14) and m.currency == "USD"
        assert m.amount_eur == pytest.approx(42.556508, abs=1e-6)

    def test_gross_and_net_are_kept_apart(self):
        """A US name loses 15% and a Dutch one keeps more; collapsing them hides that."""
        d = _summary(MSFT).by_fonds["Microsoft"]
        assert d.tax_eur < 0
        assert d.net_eur < d.gross_eur

    def test_payments_counts_dividends_not_journal_lines(self):
        d = _summary(ASML).by_fonds["ASML Holding"]
        assert d.payments == 2          # 4 rows, 2 of them tax
        assert d.first == date(2026, 2, 18) and d.last == date(2026, 5, 5)


class TestOnlyTheDividendLedgersAreIncome:
    """⚠ A journal is a journal. The day AIRS adds a movement row, an unfiltered sum turns a
    deposit into investment income."""

    def test_an_unknown_ledger_is_excluded_and_counted_never_summed(self):
        rows = [*ASML, _row("Storting", "2026-03-01", "ASML Holding", 0, 50_000, "EUR", 1.0,
                            50_000, 50_000)]
        s = _summary(rows)
        assert s.by_fonds["ASML Holding"].net_eur == pytest.approx(142.54)   # unchanged
        assert s.ignored == {"Storting": 1}

    def test_income_with_no_instrument_is_not_folded_into_one(self):
        rows = [*ASML, _row(LEDGER_DIVIDEND, "2026-03-01", "", 0, 10, "EUR", 1.0, 10, 10)]
        s = _summary(rows)
        assert s.by_fonds["ASML Holding"].net_eur == pytest.approx(142.54)
        assert s.ignored == {"(no Fonds)": 1}

    def test_a_blank_fonds_does_not_arrive_as_the_string_nan(self):
        """The cash-line trap again: pandas reads a blank as NaN, `str()` makes it "nan", and
        "nan" is truthy — so it would become an instrument called nan."""
        rows = [*ASML, _row(LEDGER_DIVIDEND, "2026-03-01", None, 0, 10, "EUR", 1.0, 10, 10)]
        assert "nan" not in _summary(rows).by_fonds


class TestAttachingToHoldings:
    """⚠ The join is by NAME — the sheet has no ISIN. Both sides are AIRS strings truncated at the
    same 50 chars, so the match is EXACT; nothing fuzzy belongs here."""

    def test_a_name_that_matches_a_holding_is_attached(self):
        s = _summary([*ASML, *MSFT])
        att, un = attach(s, {"ASML Holding", "Microsoft", "Nestle"})
        assert set(att) == {"ASML Holding", "Microsoft"}
        assert un == []

    def test_a_sold_position_keeps_its_income_instead_of_losing_it(self):
        """⚠ Measured: `Automatic Data Proc.`, `Marsh&Mclennan` and an iShares HY fund paid into
        BUS_Neutraal_Dyn and were then sold, so no holding row can carry them. Dropping them
        understates the book with nothing on screen to say so."""
        sold = _row(LEDGER_DIVIDEND, "2026-01-15", "Marsh&Mclennan", 0, 88.0, "USD", 0.9, 79.2, 88.0)
        s = _summary([*ASML, sold])
        att, un = attach(s, {"ASML Holding"})
        assert set(att) == {"ASML Holding"}
        assert [d.fonds for d in un] == ["Marsh&Mclennan"]
        assert un[0].net_eur == pytest.approx(79.2)

    def test_the_match_is_exact_not_a_prefix(self):
        """The 3 unmatched names are NOT truncations — no holding starts with them. A prefix
        match would invent a link to whatever shares an opening substring."""
        s = _summary([_row(LEDGER_DIVIDEND, "2026-01-15", "Automatic Data Proc.", 0, 10, "EUR",
                           1.0, 10, 10)])
        att, un = attach(s, {"Automatic Data Processing Inc"})
        assert att == {} and len(un) == 1


class TestTheSheetShapeIsChecked:
    def test_a_missing_required_column_raises_rather_than_summing_nothing(self):
        """⚠ Zero income and no income look identical downstream. A shape change must be loud."""
        rows = [{k: v for k, v in r.items() if k != "Bedrag eur"} for r in ASML]
        buf = BytesIO()
        pd.DataFrame(rows).to_excel(buf, index=False)   # NOT _xls: it would re-add the column
        with pytest.raises(ValueError, match="missing columns"):
            parse_mutaties(buf.getvalue())

    def test_headers_are_matched_case_and_whitespace_insensitively(self):
        rows = [{f"  {k.upper()}  ": v for k, v in r.items()} for r in ASML]
        buf = BytesIO()
        pd.DataFrame(rows).to_excel(buf, index=False)
        assert direct_result(parse_mutaties(buf.getvalue())).by_fonds["ASML Holding"].payments == 2

    def test_an_empty_journal_is_zero_rows_not_a_crash(self):
        s = direct_result(parse_mutaties(_xls([])))
        assert s.rows == 0 and s.by_fonds == {} and s.ignored == {}
