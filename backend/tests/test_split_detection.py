"""Telling a SPLIT apart from a TRANSFER, when AIRS calls both of them `D` (Deponering).

⚠ THE TWO NEED OPPOSITE HANDLING, WHICH IS WHY GUESSING IS NOT AN OPTION. A split multiplies every
earlier quantity (the shares you already held became more shares); a transfer in leaves them alone
(new shares arrived beside them). AIRS books both as a deposit of securities with every money
column zero, so the row itself cannot say which.

⚠ THE ANSWER IS THAT TWO INDEPENDENT COLUMNS MUST AGREE:

  1. the QUANTITY ratio — `qty_now / (qty_now − deposited)` — must be a whitelisted split ratio;
  2. every pre-event trade's PRICE ratio, divided by that same quantity ratio, must land in a
     plausible price move.

On a transfer the second gate fails hard rather than marginally: the prices share one basis while
the quantity ratio is 10, so the quotient is ~0.1 — the stock would have had to fall 90%.

MEASURED 2026-08-05 on KLA-Tencor, in two books with different share counts:

    BUS_Offensief   310 / (310 − 279) = 10.0000   implied move 1.074
    AITopSelectie   410 / (410 − 369) = 10.0000   implied moves 1.000, 1.185

The 1.000 is the decisive one — a 5 January purchase at EXACTLY 10.0000x the 1 January price. A
stock does not move 0.00% in four days AND independently happen to be 10x; that is one price in two
unit bases, and the quantity column reached 10.0000 on its own.

⚠ WHAT IT COST TO GET THIS WRONG, BEFORE THE DETECTION EXISTED: `qty_now − bought` mixed the bases
and gave 296 where the truth is 170, so KLA-Tencor's opening value read EUR 32,605 instead of
EUR 18,725 and its money-weighted return read +39.81% instead of +56.67%. Seventeen points, and
entirely plausible-looking.
"""
from __future__ import annotations

import pytest

from airs_capital import SPLIT_RATIOS, build_ledger, detect_split, money_weighted_return_pct
from airs_transacties import Trade

from datetime import date

Y0, END = date(2026, 1, 1), date(2026, 8, 5)


class TestTheMeasuredCase:
    """KLA-Tencor, both books, real numbers."""

    def test_bus_offensief(self):
        # 310 held, 279 deposited, 1-Jan price EUR 110.15, a 3 Feb buy at EUR 1,183.36
        assert detect_split(310, 279, 34146.96 / 310, [16567.08 / 14]) == pytest.approx(10.0)

    def test_aitopselectie(self):
        # ⚠ The decisive one: the 5 Jan buy is EXACTLY 10.0000x the 1 Jan price.
        assert detect_split(410, 369, 44799.0 / 410,
                            [50262.02 / 46, 6474.15 / 5]) == pytest.approx(10.0)

    def test_the_quantity_ratio_alone_is_not_enough(self):
        """Same 10:1 quantity ratio, but the prices share ONE basis — a transfer in, not a split.
        This is the case the whole detector exists to refuse."""
        assert detect_split(310, 279, 110.15, [118.34]) is None


class TestBothGatesMustPass:
    def test_a_ratio_off_the_whitelist_is_refused(self):
        # 7:1 is not a split ratio anyone issues; a quantity change of that shape is something else.
        assert detect_split(700, 600, 100.0, [700.0]) is None

    def test_a_ratio_near_but_not_on_the_whitelist_is_refused(self):
        # ⚠ 1% tolerance, deliberately tight. "Any small rational" is dense enough to sit near
        # anything, which is how a real event gets "corrected" into nothing.
        assert detect_split(1050, 950, 100.0, [1000.0]) is None

    def test_a_price_move_beyond_the_band_is_refused(self):
        # Quantity says 10:1 and the price says the stock also quadrupled — one of the two is wrong,
        # and we do not get to choose which.
        assert detect_split(310, 279, 110.15, [110.15 * 10 * 4]) is None

    def test_no_pre_event_trade_means_nothing_to_cross_check(self):
        """⚠ ONE GATE IS NOT TWO. With no trade before the deposit the price test cannot run, so
        the quantity ratio stands alone — and a lone ratio is exactly what a transfer also has."""
        assert detect_split(310, 279, 110.15, []) is None

    @pytest.mark.parametrize("ratio", SPLIT_RATIOS)
    def test_every_whitelisted_ratio_is_reachable(self, ratio):
        before, px = 100.0, 50.0
        assert detect_split(before * ratio, before * (ratio - 1), px,
                            [px * ratio]) == pytest.approx(ratio)


class TestDegenerateInputs:
    def test_a_deposit_of_everything_is_refused(self):
        # qty_before would be 0 — the position did not exist before, so there is no ratio.
        assert detect_split(310, 310, 110.15, [1183.36]) is None

    def test_a_deposit_larger_than_the_holding_is_refused(self):
        assert detect_split(310, 400, 110.15, [1183.36]) is None

    def test_no_opening_price_is_refused(self):
        assert detect_split(310, 279, 0.0, [1183.36]) is None


class TestItFlowsThroughTheLedger:
    """The point of detecting it: the position stops being refused and gets a real figure."""

    def _rows(self):
        return [{"holding_name": "KLA", "quantity": 310.0,
                 "start_value_eur": 34146.96, "current_value_eur": 52617.89}]

    def _trades(self):
        return [Trade(fonds="KLA", kind="buy", datum="2026-02-03", eur=16567.08, quantity=14.0)]

    def _income(self):
        """⚠⚠ THE DIVIDEND IS NOT OPTIONAL FURNITURE — IT IS PART OF THE FIGURE THIS TEST ASSERTS.

        `result_eur` is `held + realised + INCOME`, so a fixture passing `{}` here computes a
        return on price alone while asserting a percentage measured on the real book, which
        carried its dividend. That fixture reproduced NEITHER documented number: 56.38% against
        the +56.67% above, and 39.60% against the +39.81%. Both short by the same 0.52% — a
        numerator shortfall, since the two denominators differ (EUR 32,762 and EUR 46,641) and no
        error in the day-weighting could move both by one ratio. Restoring the income puts both
        back on the cent.

        MEASURED from `airs_mutatie`, BUS_Offensief_Dyn / KLA-Tencor, through the same
        `direct_result` production feeds to `build_ledger` as `income_by_name`:

            2026-03-03   Dividend  +50.75   Dividendbelasting   -7.61
            2026-06-02   Dividend  +61.21   Dividendbelasting   -9.18
            gross 111.96   tax -16.78   NET 95.17

        ⚠ NET, AND THE TAX IS ALREADY NEGATIVE — AIRS books withholding as a negative amount, so
        the net is `gross + tax`. Subtracting it instead would take the withholding off twice and
        understate every foreign holding by exactly that much.
        """
        return {"KLA": 95.17}

    def test_without_the_ratio_the_position_is_refused(self):
        led = build_ledger(self._rows(), self._trades(), self._income(), 1197811.04, Y0, END,
                           unknown_names={"KLA"})
        p = led.positions[0]
        assert p.capital_unknown is True
        assert money_weighted_return_pct(p) is None

    def test_with_the_ratio_it_is_computed(self):
        led = build_ledger(self._rows(), self._trades(), self._income(), 1197811.04, Y0, END,
                           unknown_names={"KLA"}, splits={"KLA": 10.0})
        p = led.positions[0]
        assert p.capital_unknown is False
        # 14 pre-split shares are 140 in today's basis, so 310 − 140 = 170 were held at the open.
        assert p.opening_eur == pytest.approx(34146.96 * 170 / 310, abs=1.0)
        assert money_weighted_return_pct(p) == pytest.approx(56.67, abs=0.1)

    def test_the_income_is_inside_the_return_not_beside_it(self):
        """⚠ THE GUARD THAT KEEPS THE FIXTURE HONEST. Without it, someone restoring `{}` here
        turns `test_with_the_ratio_it_is_computed` red by 0.29pp — a gap small enough to look like
        a rounding tolerance and be "fixed" by widening `abs=`, which would silently drop the
        dividend out of a money-weighted RETURN. Naming the dependency makes that impossible."""
        with_income = build_ledger(self._rows(), self._trades(), self._income(), 1197811.04,
                                   Y0, END, unknown_names={"KLA"}, splits={"KLA": 10.0})
        without = build_ledger(self._rows(), self._trades(), {}, 1197811.04,
                               Y0, END, unknown_names={"KLA"}, splits={"KLA": 10.0})
        p, q = with_income.positions[0], without.positions[0]
        assert p.income_eur == pytest.approx(95.17)
        assert p.result_eur - q.result_eur == pytest.approx(95.17, abs=0.01)
        # Same capital either way — a dividend is a RESULT, never a further investment.
        assert p.avg_capital_eur == pytest.approx(q.avg_capital_eur)
        assert money_weighted_return_pct(q) == pytest.approx(56.38, abs=0.01)

    def test_the_euro_result_is_identical_either_way(self):
        """⚠ ONLY QUANTITIES WERE EVER AMBIGUOUS. A split moves no money, so nothing in the euro
        columns may shift when one is detected — if it did, the rescale would be touching the
        result rather than the basis."""
        a = build_ledger(self._rows(), self._trades(), self._income(), 1197811.04, Y0, END,
                         unknown_names={"KLA"})
        b = build_ledger(self._rows(), self._trades(), self._income(), 1197811.04, Y0, END,
                         unknown_names={"KLA"}, splits={"KLA": 10.0})
        assert a.positions[0].result_eur == pytest.approx(b.positions[0].result_eur)
