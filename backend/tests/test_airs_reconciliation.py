"""The book-vs-positions reconciliation, and the realised-results layer it depends on.

Pure: dicts in, dataclasses out. No AIRS, no DB, no network.

⚠ THE ANCHOR IS A REAL BOOK. Every figure in `TestTheMeasuredCase` is AITopSelectie OFF DYN as of
2026-08-05, read off the live tables — so these tests pin the arithmetic that was verified against
AIRS's own `cumulatief_rendement`, not a hand-made example that agrees with the code by
construction.
"""
from __future__ import annotations

import pytest

from airs_reconciliation import OpenSide, contributions, open_side_from_rows, reconcile
from airs_transacties import ParsedSheet, realised_results

# AITopSelectie OFF DYN, 2026-08-05 — the aggregated ATT row.
BOOK = {
    "cumulatief_rendement": 38.729375,
    "beleggingsresultaat": 387293.75,
    "beginvermogen": 1000000.0,
    "eindvermogen": 1387293.75,
    "stortingen": 0, "onttrekkingen": 0, "kosten": 0,
    "reconciles": True,
}
OPEN = OpenSide(start_eur=1006880.70, end_eur=1387867.64, priced=20, unpriced=1)
REALISED_YTD = 6306.85          # Σ `Res. YtD` over the eight sell rows


def _sell(fonds, proceeds, cost, ytd, prior=0.0, qty=1.0, datum="2026-01-22"):
    return {"Tt": "V", "Datum": datum, "Fonds": fonds, "Aantal": qty,
            "Waarde  EUR.1": proceeds, "Kostprijs": cost,
            "Res.  YtD": ytd, "Res.  voorg. jr.": prior, "Waarde  EUR": 0.0}


def _buy(fonds, value_eur, qty=1.0):
    return {"Tt": "A", "Datum": "2026-01-05", "Fonds": fonds, "Aantal": qty,
            "Waarde  EUR": value_eur, "Waarde  EUR.1": 0.0, "Kostprijs": 0.0,
            "Res.  YtD": 0.0, "Res.  voorg. jr.": 0.0}


def _sheet(rows):
    cols = ["Tt", "Datum", "Fonds", "Aantal", "Waarde  EUR", "Waarde  EUR.1",
            "Kostprijs", "Res.  YtD", "Res.  voorg. jr."]
    return ParsedSheet(columns=cols, kinds={c: "text" for c in cols}, rows=rows)


class TestTheMeasuredCase:
    """AITopSelectie OFF DYN — the account the whole panel was validated against."""

    def test_the_year_reconciles_to_the_books_own_result(self):
        r = reconcile(BOOK, OPEN, realised_ytd_eur=REALISED_YTD)
        # held 380,986.94 + realised 6,306.85 + sold income 0 = 387,293.79
        assert r.total_result_eur == pytest.approx(387293.79, abs=0.01)
        assert r.residual_vs_book_eur == pytest.approx(0.04, abs=0.01)
        assert r.reconciles is True

    def test_the_positions_derived_return_reproduces_cumulatief_rendement(self):
        # ⚠ THE POINT OF THE WHOLE PANEL. Five decimal places against AIRS's own figure.
        r = reconcile(BOOK, OPEN, realised_ytd_eur=REALISED_YTD)
        assert r.total_return_pct == pytest.approx(38.729375, abs=1e-4)
        assert r.return_basis == "opening_capital"

    def test_the_open_positions_alone_understate_it(self):
        # The gap this panel exists to explain: -0.89pp, which is exactly the realised leg.
        r = reconcile(BOOK, OPEN, realised_ytd_eur=REALISED_YTD)
        assert r.open.return_pct == pytest.approx(37.8383, abs=1e-3)
        assert r.gap_pp == pytest.approx(-0.891, abs=1e-3)

    def test_the_opening_capitals_disagree_and_the_sign_is_negative_here(self):
        # ⚠ The rows claim MORE opening value than the book had, because AIRS restates
        # `Beginwaarde` to the CURRENT quantity. Calling this "closed positions" would be claiming
        # a negative amount of them.
        r = reconcile(BOOK, OPEN, realised_ytd_eur=REALISED_YTD)
        assert r.start_gap_eur == pytest.approx(-6880.70, abs=0.01)


class TestAnUnfetchedSheetIsNotZero:
    """The failure that would be invisible: no transactions cached, so the realised leg is
    UNKNOWN — and a 0 would publish the open positions' figure as the year's total."""

    def test_no_realised_input_yields_no_total_at_all(self):
        r = reconcile(BOOK, OPEN)                      # realised_ytd_eur omitted -> None
        assert r.realised_ytd_eur is None
        assert r.total_result_eur is None
        assert r.total_return_pct is None
        assert r.return_basis == "unavailable"
        assert r.reconciles is None

    def test_an_explicit_zero_is_honoured_as_a_real_answer(self):
        # A book that genuinely sold nothing. Distinct from "we never looked".
        r = reconcile(BOOK, OPEN, realised_ytd_eur=0.0)
        assert r.total_result_eur == pytest.approx(OPEN.result_eur, abs=0.01)
        assert r.reconciles is False           # the 6,306.85 is genuinely missing here


class TestFlowsRefuseTheDivision:
    """⚠⚠ `result ÷ opening capital` is a return only when nothing was paid in or out."""

    def test_a_book_that_opened_at_zero_and_took_a_deposit_gets_no_percentage(self):
        # AzTopSelectie_DYN: begin 0, stortingen 1,000,000, end 998,784 — it LOST 1,216, and the
        # division is undefined rather than merely awkward.
        book = {**BOOK, "beginvermogen": 0.0, "stortingen": 1000000.0,
                "beleggingsresultaat": -1215.59, "cumulatief_rendement": -0.121559}
        r = reconcile(book, OPEN, realised_ytd_eur=0.0)
        assert r.total_return_pct is None
        assert r.return_basis == "unavailable"

    def test_a_mid_year_deposit_refuses_the_percentage_but_keeps_the_euro_total(self):
        book = {**BOOK, "stortingen": 250000.0}
        r = reconcile(book, OPEN, realised_ytd_eur=REALISED_YTD)
        assert r.return_basis == "flows"
        assert r.total_return_pct is None
        # ⚠ The EUR total survives — it is flow-free by construction (a deposit is not a result).
        assert r.total_result_eur == pytest.approx(387293.79, abs=0.01)

    def test_a_withdrawal_that_exactly_offsets_a_deposit_is_STILL_flows(self):
        # ⚠ GROSS, NOT NET, AND THIS IS THE CASE THAT PROVES IT. EUR 100k in during January and
        # EUR 100k out in December nets to zero, and the extra capital was still invested for
        # eleven months — so `beginvermogen` is not the capital the result was earned on. A net
        # test would wave through exactly the book that most needs the flow-aware figure.
        book = {**BOOK, "stortingen": 100000.0, "onttrekkingen": 100000.0}
        r = reconcile(book, OPEN, realised_ytd_eur=REALISED_YTD)
        assert r.return_basis == "flows"
        assert r.total_return_pct is None


class TestOpenSideFromRows:
    def test_a_row_with_no_opening_value_is_out_not_zero(self):
        side = open_side_from_rows([
            {"start_value_eur": 100.0, "current_value_eur": 110.0},
            {"start_value_eur": 0.0, "current_value_eur": 50.0},      # bought mid-year, or cash
            {"start_value_eur": 100.0, "current_value_eur": None},    # unpriceable
        ])
        assert side.priced == 1
        assert side.unpriced == 2
        assert side.return_pct == pytest.approx(10.0)

    def test_the_dividend_tax_is_added_not_subtracted(self):
        # ⚠ `dividend_tax_eur` is already NEGATIVE. The intuitive minus adds the tax back and
        # overstates every foreign holding by twice the withholding.
        side = open_side_from_rows([{"start_value_eur": 1000.0, "current_value_eur": 1000.0,
                                     "dividend_eur": 100.0, "dividend_tax_eur": -15.0}])
        assert side.end_eur == pytest.approx(1085.0)
        assert side.return_pct == pytest.approx(8.5)

    def test_no_priced_rows_gives_an_undefined_return_never_zero(self):
        side = open_side_from_rows([{"start_value_eur": 0.0, "current_value_eur": 5.0}])
        assert side.return_pct is None


class TestRealisedResults:
    def test_the_realised_ytd_is_airss_own_column_not_proceeds_minus_cost(self):
        # ⚠⚠ THE TRAP. A position carried across a year end realises a gain of which only part is
        # this year's. proceeds − cost = 1,000 here; the year's share is 400.
        s = realised_results(_sheet([_sell("Old Holding", 5000.0, 4000.0, 400.0, prior=600.0)]))
        assert s.realised_ytd_eur == pytest.approx(400.0)
        assert s.legs["Old Holding"].prior_year_eur == pytest.approx(600.0)

    def test_sales_of_one_instrument_are_aggregated_into_one_leg(self):
        s = realised_results(_sheet([
            _sell("Synopsys", 1778.52, 1647.53, 130.99, datum="2026-01-22"),
            _sell("Synopsys", 900.00, 800.00, 100.00, datum="2026-03-10"),
        ]))
        assert len(s.legs) == 1
        leg = s.legs["Synopsys"]
        assert leg.sales == 2
        assert leg.realised_ytd_eur == pytest.approx(230.99)
        assert (leg.first, leg.last) == ("2026-01-22", "2026-03-10")

    def test_buys_are_summed_apart_and_never_enter_the_realised_total(self):
        s = realised_results(_sheet([_buy("Nvidia", 50191.62), _sell("ASML", 5774.0, 4931.5, 842.5)]))
        assert s.buy_count == 1
        assert s.buys_eur == pytest.approx(50191.62)
        assert s.realised_ytd_eur == pytest.approx(842.5)

    def test_an_uninterpreted_type_is_counted_never_silently_dropped(self):
        # Measured: Tt='D', KLA-Tencor, 369 shares, every money column 0.0. Excluded from the sums
        # because it carries no money — and COUNTED, so a `D` that one day carries a value cannot
        # slip in unnoticed.
        s = realised_results(_sheet([
            {"Tt": "D", "Datum": "2026-06-12", "Fonds": "KLA-Tencor", "Aantal": 369.0,
             "Waarde  EUR": 0.0, "Waarde  EUR.1": 0.0, "Kostprijs": 0.0,
             "Res.  YtD": 0.0, "Res.  voorg. jr.": 0.0},
            _sell("ASML", 5774.0, 4931.5, 842.5),
        ]))
        assert s.unknown_types == {"D": 1}
        assert s.realised_ytd_eur == pytest.approx(842.5)

    def test_an_unrecognised_sheet_refuses_rather_than_summing_to_zero(self):
        # ⚠ A missing column means this is not the report that was measured. A confident
        # "EUR 0.00 realised" would be a plausible number, not an error.
        s = realised_results(ParsedSheet(columns=["Datum", "Fonds"], rows=[{"Fonds": "X"}]))
        assert s.unreadable is not None
        assert "Res.  YtD" in s.unreadable
        assert s.realised_ytd_eur == 0.0        # and the caller passes None, never this

    def test_a_sale_with_no_instrument_is_counted_not_folded_into_a_name(self):
        s = realised_results(_sheet([_sell("", 100.0, 90.0, 10.0)]))
        assert s.legs == {}
        assert s.unknown_types == {"(sale with no Fonds)": 1}


class TestTwoClocks:
    """⚠⚠ The held leg is the VOLK holdings snapshot; the book's result is the ATT report. They are
    separate downloads and land a day apart, and one day of market movement on a EUR 1.4m book was
    read as a EUR 57,330 missing position."""

    def test_a_misaligned_residual_is_unknown_not_failed(self):
        # AITopSelectie, measured 2026-08-05: ATT to 08-05, holdings at 08-04.
        book = {**BOOK, "periode": "2026-08-05", "beleggingsresultaat": 444624.08}
        r = reconcile(book, OPEN, realised_ytd_eur=REALISED_YTD, holdings_as_of="2026-08-04")
        assert r.dates_aligned is False
        # ⚠ None, not False. Calling it False accuses the arithmetic of a fault the calendar owns,
        # and sends a reader hunting for a position that is not missing.
        assert r.reconciles is None
        assert r.residual_reason and "2026-08-04" in r.residual_reason

    def test_a_tie_still_counts_even_when_the_dates_differ(self):
        # ⚠ BUS_Offensief_Dyn reconciles to EUR 0.05 with its two sides nominally a day apart. The
        # market plainly did not move it, so suppressing a proven agreement on a calendar
        # technicality would discard the evidence the check exists to produce.
        #
        # ⚠ THE FIRST ASSERTION USED TO READ `abs(...) > 1  # this book genuinely differs`, WHICH
        # CONTRADICTED THE COMMENT TWO LINES ABOVE IT. `BOOK` is BUS_Offensief_Dyn — the residual
        # is EUR 0.04, i.e. it IS the near-tie this test is named for, not a book that differs.
        # Asserting the opposite made the case indistinguishable from the exact-tie one below (both
        # `reconciles is True`), so the pair proved one thing twice and the misaligned-but-agreeing
        # path — the whole point — was never actually checked. The materially-different book is the
        # test above this one, at EUR −57,330.
        book = {**BOOK, "periode": "2026-08-05"}
        r = reconcile(book, OPEN, realised_ytd_eur=REALISED_YTD, holdings_as_of="2026-08-04")
        assert abs(r.residual_vs_book_eur) < 1        # a proven agreement, to four cents
        assert r.dates_aligned is False
        assert r.reconciles is True                   # ...and it still counts as one
        # The exact tie, for the boundary: nothing about a zero residual depends on the fuzz above.
        book_tie = {**book, "beleggingsresultaat": 387293.79}
        r2 = reconcile(book_tie, OPEN, realised_ytd_eur=REALISED_YTD, holdings_as_of="2026-08-04")
        assert r2.residual_vs_book_eur == pytest.approx(0.0, abs=1e-9)
        assert r2.dates_aligned is False
        assert r2.reconciles is True                  # a tie is a tie
        assert r2.residual_reason is None

    def test_aligned_dates_and_a_real_gap_is_a_genuine_failure(self):
        book = {**BOOK, "periode": "2026-08-04", "beleggingsresultaat": 500000.0}
        r = reconcile(book, OPEN, realised_ytd_eur=REALISED_YTD, holdings_as_of="2026-08-04")
        assert r.dates_aligned is True
        assert r.reconciles is False
        assert r.residual_reason and "a leg is missing" in r.residual_reason


class TestContributions:
    """One denominator — the book's own opening capital — so the legs add to its YTD."""

    def _rec(self, **over):
        r = reconcile(BOOK, OPEN, sold_income_eur=0.0, realised_ytd_eur=REALISED_YTD,
                      holdings_as_of="2026-08-04")
        base = {"book_start_eur": r.book_start_eur, "open_result_eur": r.open.result_eur,
                "realised_ytd_eur": r.realised_ytd_eur, "sold_income_eur": r.sold_income_eur,
                "return_basis": r.return_basis, "realised": []}
        return {**base, **over}

    def test_the_three_legs_add_to_the_books_own_ytd(self):
        c = contributions(self._rec())
        assert c["held_pct"] + c["realised_pct"] + c["sold_income_pct"] == pytest.approx(
            c["total_pct"], abs=1e-9)
        assert c["total_pct"] == pytest.approx(BOOK["cumulatief_rendement"], abs=1e-4)

    def test_the_denominator_is_the_books_opening_capital_not_the_held_positions(self):
        # ⚠ THE WHOLE POINT. On the held book's own opening value (1,006,880.70) the held leg
        # would read 37.84% — the positions table's figure — and the sold leg could not be
        # expressed at all, because a sold position is not in that denominator.
        c = contributions(self._rec())
        assert c["basis_eur"] == pytest.approx(1000000.0)
        assert c["held_pct"] == pytest.approx(38.0987, abs=1e-3)   # NOT 37.84
        assert OPEN.return_pct == pytest.approx(37.8383, abs=1e-3)

    def test_a_flow_book_gets_no_percentages_at_all(self):
        # Three contributions that do not add to the figure they decompose each look reasonable
        # alone, which is worse than showing none.
        r = reconcile({**BOOK, "stortingen": 250000.0}, OPEN, realised_ytd_eur=REALISED_YTD)
        c = contributions({"book_start_eur": r.book_start_eur,
                           "open_result_eur": r.open.result_eur,
                           "realised_ytd_eur": r.realised_ytd_eur,
                           "sold_income_eur": 0.0, "return_basis": r.return_basis,
                           "realised": [{"fonds": "X", "realised_ytd_eur": 1.0}]})
        assert c["comparable"] is False
        assert c["held_pct"] is None and c["total_pct"] is None
        assert c["legs"] == []

    def test_the_coverage_share_is_of_the_ABSOLUTE_movement(self):
        # ⚠ A realised LOSS beside a held GAIN is not "negative coverage" — the question is how
        # much of the movement happened outside the holdings table, and a loss counts as much.
        c = contributions(self._rec(open_result_eur=75164.23, realised_ytd_eur=-28656.46,
                                    sold_income_eur=695.50))
        assert c["realised_share_of_result_pct"] == pytest.approx(
            28656.46 / (75164.23 + 28656.46 + 695.50) * 100, abs=1e-6)
        assert c["realised_share_of_result_pct"] > 0

    def test_no_realised_input_yields_no_contributions(self):
        c = contributions(self._rec(realised_ytd_eur=None))
        assert c["held_pct"] is None
        assert c["realised_share_of_result_pct"] is None
