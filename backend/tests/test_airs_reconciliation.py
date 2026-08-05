"""The book-vs-positions reconciliation, and the realised-results layer it depends on.

Pure: dicts in, dataclasses out. No AIRS, no DB, no network.

⚠ THE ANCHOR IS A REAL BOOK. Every figure in `TestTheMeasuredCase` is AITopSelectie OFF DYN as of
2026-08-05, read off the live tables — so these tests pin the arithmetic that was verified against
AIRS's own `cumulatief_rendement`, not a hand-made example that agrees with the code by
construction.
"""
from __future__ import annotations

import pytest

from airs_reconciliation import OpenSide, open_side_from_rows, reconcile
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
