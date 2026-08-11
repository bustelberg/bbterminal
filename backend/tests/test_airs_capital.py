"""Average invested capital + the one-list position ledger. Pure: dicts in, dataclasses out.

⚠ THE TWO BOOKS BEHIND THESE NUMBERS ARE REAL, and they disagree in the way that decided the
design. BUS_Offensief_Dyn was fully invested all year; AITopSelectie OFF DYN opened the year in
CASH and deployed EUR 1m on 5 January. A 1-January weight describes the first and lies about the
second (it would call it 96% cash), which is why the weight here is average invested capital.
"""
from __future__ import annotations

from datetime import date

import pytest

from airs_capital import build_ledger, contribution_pct, money_weighted_return_pct
from airs_transacties import Trade

Y0 = date(2026, 1, 1)
END = date(2026, 8, 5)
DAYS = (END - Y0).days          # 216


def _buy(fonds, eur, qty, datum):
    return Trade(fonds=fonds, kind="buy", datum=datum, eur=eur, quantity=qty)


def _sell(fonds, eur, qty, datum, ytd=0.0, prior=0.0):
    return Trade(fonds=fonds, kind="sell", datum=datum, eur=eur, quantity=qty,
                 realised_ytd_eur=ytd, prior_year_eur=prior)


def _held(name, qty, start, cur):
    return {"holding_name": name, "quantity": qty,
            "start_value_eur": start, "current_value_eur": cur}


class TestAverageInvestedCapital:
    def test_a_position_held_all_year_carries_its_full_opening_value(self):
        led = build_ledger([_held("A", 100, 10000.0, 12000.0)], [], {}, 10000.0, Y0, END)
        p = led.positions[0]
        assert p.opening_eur == pytest.approx(10000.0)
        assert p.avg_capital_eur == pytest.approx(10000.0)

    def test_a_buy_early_in_the_year_counts_for_almost_all_of_it(self):
        # ⚠ THE AITopSelectie CASE. Bought 5 January, so the money was invested for ~97% of the
        # period — a 1-January weight would say this position was 0% of the book.
        w = (END - date(2026, 1, 5)).days / DAYS
        led = build_ledger([_held("A", 100, 0.0, 12000.0)],
                           [_buy("A", 10000.0, 100, "2026-01-05")], {}, 1000000.0, Y0, END)
        p = led.positions[0]
        assert p.opening_eur == 0.0                       # genuinely not held on 1 January
        assert p.avg_capital_eur == pytest.approx(10000.0 * w, abs=0.01)
        assert w > 0.97

    def test_a_sale_stops_counting_from_its_own_date(self):
        w = (END - date(2026, 2, 1)).days / DAYS
        led = build_ledger([], [_sell("A", 4000.0, 40, "2026-02-01", ytd=500.0)], {},
                           100000.0, Y0, END)
        p = led.positions[0]
        # Opening = proceeds − Res. YtD (nothing was bought this year, so all of it was held).
        assert p.opening_eur == pytest.approx(3500.0)
        assert p.avg_capital_eur == pytest.approx(3500.0 - 4000.0 * w, abs=0.01)

    def test_a_flow_dated_outside_the_window_is_clamped(self):
        # ⚠ AIRS occasionally books to a settlement date past the report's end. Unclamped, the
        # weight goes negative and the position reports LESS capital than it ever held.
        led = build_ledger([], [_buy("A", 1000.0, 10, "2026-12-31")], {}, 10000.0, Y0, END)
        assert led.positions[0].avg_capital_eur == pytest.approx(0.0)


class TestDeRestatement:
    """⚠ `Beginwaarde` is qty_now × the 1-Jan price, NOT the 1-Jan value."""

    def test_a_position_bought_into_has_its_opening_value_scaled_back(self):
        # Owned 100 on 1 Jan, bought 50 in March -> AIRS reports Beginwaarde for 150 shares.
        # The true opening value is 100 shares' worth.
        led = build_ledger([_held("A", 150, 15000.0, 18000.0)],
                           [_buy("A", 5200.0, 50, "2026-03-01")], {}, 20000.0, Y0, END)
        assert led.positions[0].opening_eur == pytest.approx(10000.0)

    def test_a_position_trimmed_has_the_sold_shares_added_back(self):
        # Owned 150 on 1 Jan, sold 50 -> current qty 100, so Beginwaarde covers only 100.
        led = build_ledger([_held("A", 100, 10000.0, 11000.0)],
                           [_sell("A", 5500.0, 50, "2026-03-01", ytd=500.0)], {}, 20000.0, Y0, END)
        assert led.positions[0].opening_eur == pytest.approx(15000.0)

    def test_shares_bought_beyond_the_current_quantity_cannot_make_the_opening_negative(self):
        led = build_ledger([_held("A", 50, 5000.0, 6000.0)],
                           [_buy("A", 9000.0, 90, "2026-02-01")], {}, 10000.0, Y0, END)
        assert led.positions[0].opening_eur == 0.0


class TestASoldOutPositionsOpeningValue:
    def test_it_is_proceeds_minus_res_ytd_when_nothing_was_bought_this_year(self):
        # ⚠ proceeds − Res. YtD == Kostprijs + Res. voorg. jr. == last year's closing value.
        led = build_ledger([], [_sell("A", 12000.0, 100, "2026-04-01", ytd=2000.0, prior=3000.0)],
                           {}, 50000.0, Y0, END)
        assert led.positions[0].opening_eur == pytest.approx(10000.0)
        assert led.positions[0].closed_out is True

    def test_a_name_bought_AND_sold_within_the_year_contributes_no_opening_capital(self):
        # ⚠ THE BUG THIS RULE EXISTS FOR. Counting `proceeds − Res. YtD` here would invent opening
        # capital that did not exist on 1 January — done naively across a real book it moved the
        # gap from EUR 55,427 to EUR 377,776.
        led = build_ledger([], [_buy("A", 9000.0, 100, "2026-02-01"),
                                _sell("A", 10000.0, 100, "2026-05-01", ytd=1000.0)],
                           {}, 50000.0, Y0, END)
        assert led.positions[0].opening_eur == pytest.approx(0.0)

    def test_a_partly_pre_owned_name_is_split_proportionally(self):
        # Held 50 at the open, bought 50 in February, sold all 100 in May. Half the opening claim
        # is real. ⚠ Proportional because AIRS does not publish its parcel matching — an
        # approximation, and `capital_coverage_ratio` is where it shows.
        led = build_ledger([], [_buy("A", 5000.0, 50, "2026-02-01"),
                                _sell("A", 12000.0, 100, "2026-05-01", ytd=2000.0)],
                           {}, 50000.0, Y0, END)
        assert led.positions[0].opening_eur == pytest.approx(5000.0)   # (12000-2000) x 50/100


class TestTheLedgerAddsUp:
    def test_contributions_sum_to_the_books_own_return(self):
        # ⚠ THE IDENTITY THE WHOLE TABLE ASSERTS. Held P&L + realised + income, over the book's
        # opening capital.
        led = build_ledger(
            [_held("A", 100, 10000.0, 11000.0), _held("B", 50, 5000.0, 5500.0)],
            [_sell("C", 6000.0, 60, "2026-03-01", ytd=1000.0)],
            {"A": 200.0}, 20000.0, Y0, END)
        total = sum(contribution_pct(p, led.basis_eur) for p in led.positions)
        # 1000 + 500 + 1000 + 200 = 2700 on 20000 = 13.5%
        assert total == pytest.approx(13.5, abs=1e-9)
        assert led.total_result_eur == pytest.approx(2700.0)

    def test_income_attaches_to_a_name_the_book_no_longer_holds(self):
        # ⚠ A sold position's dividend is real and belongs on its own row, not in a leftover
        # bucket — which is why the journal is read per name rather than via the rolled-up orphans.
        led = build_ledger([], [_sell("Gone", 5000.0, 50, "2026-03-01", ytd=500.0)],
                           {"Gone": 120.0}, 10000.0, Y0, END)
        p = led.positions[0]
        assert p.income_eur == pytest.approx(120.0)
        assert p.result_eur == pytest.approx(620.0)

    def test_weights_sum_to_100_and_a_negative_capital_cannot_inflate_them(self):
        # A sale weighted more heavily than the buy that supplied it can leave a position with
        # negative average capital; letting it shrink the denominator would inflate every other
        # row.
        led = build_ledger([_held("A", 100, 10000.0, 11000.0)],
                           [_buy("B", 100.0, 1, "2026-08-01"),
                            _sell("B", 5000.0, 1, "2026-01-02", ytd=0.0)],
                           {}, 20000.0, Y0, END)
        assert sum(p.weight_pct for p in led.positions) == pytest.approx(100.0)
        assert all(p.weight_pct >= 0 for p in led.positions)

    def test_the_capital_coverage_ratio_is_reported_not_forced_to_one(self):
        # Modified Dietz ignores the price path and the opening values are reconstructed, so the
        # sum is near — not equal to — the book's opening capital. Measured 0.98 and 1.02.
        led = build_ledger([_held("A", 100, 9000.0, 11000.0)], [], {}, 10000.0, Y0, END)
        assert led.capital_coverage_ratio == pytest.approx(0.9)


class TestTheTwoReturnsAreDifferentQuestions:
    def test_the_money_weighted_return_divides_by_capital_actually_tied_up(self):
        # ⚠ A name bought late shows a LARGER percentage on the same euros than one held all year.
        # That is the intended reading — "how hard did this money work" — and it is why this column
        # will not match the Holdings table's Return.
        led = build_ledger([_held("A", 100, 0.0, 11000.0)],
                           [_buy("A", 10000.0, 100, "2026-06-01")], {}, 100000.0, Y0, END)
        p = led.positions[0]
        assert p.avg_capital_eur < 10000.0                     # invested for part of the year
        assert money_weighted_return_pct(p) is not None

    def test_a_position_with_no_capital_has_no_return_rather_than_zero(self):
        led = build_ledger([], [_buy("A", 0.0, 0, "2026-03-01")], {}, 10000.0, Y0, END)
        assert money_weighted_return_pct(led.positions[0]) is None

    def test_no_basis_means_no_contribution_rather_than_zero(self):
        led = build_ledger([_held("A", 100, 10000.0, 11000.0)], [], {}, None, Y0, END)
        assert contribution_pct(led.positions[0], led.basis_eur) is None
