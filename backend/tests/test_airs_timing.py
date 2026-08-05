"""Was the trading worth it? — buy-and-hold plus the effect of each decision.

THE IDENTITY, WHICH IS EXACT AND NOT APPROXIMATE

    actual = buy-and-hold + SUM(effect of each trade)

    buy-and-hold  = qty_open x (price_now - price_open)
    a BUY  of q at p  ->  q x (price_now - p)     it rose after you bought -> gain
    a SELL of q at p  ->  q x (p - price_now)     it fell after you sold   -> gain

It falls straight out of the algebra rather than being fitted:

    actual - buyhold = (qty_now - qty_open) x price_now - SUM(buys) + SUM(sells)
                     = SUM q_buy x (price_now - p_buy) + SUM q_sell x (p_sell - price_now)

MEASURED 2026-08-05 on BUS_Offensief_Dyn, residual 0.00 on every position:

    KLA-Tencor      buyhold +10,129   trades +7,196   = +17,325   actual +17,325
    Adobe Systems   buyhold  -2,615   trades +3,028   =    +413   actual    +413
    ASML Holding    buyhold +34,174   trades      0   = +34,174   actual +34,174

Adobe is the case worth keeping: doing nothing LOST money, and two correctly-timed decisions
turned it positive. That is the whole reason this exists — the holdings table can say the trading
helped, and cannot say which trade or by how much.

⚠ AGAINST DOING NOTHING, NEVER AGAINST A PERFECT DECISION. A lucky call and a good one produce
the same number. Nothing here is a skill claim.
"""
from __future__ import annotations

import pytest

from airs_timing import analyse_timing
from airs_transacties import Trade


def _buy(q, eur, datum="2026-03-01"):
    return Trade(fonds="X", kind="buy", datum=datum, eur=eur, quantity=q)


def _sell(q, eur, datum="2026-03-01"):
    return Trade(fonds="X", kind="sell", datum=datum, eur=eur, quantity=q)


class TestTheIdentityHolds:
    def test_no_trades_means_the_result_is_all_buy_and_hold(self):
        # 62 shares, EUR 921.40 -> EUR 1,472.60 (ASML, measured).
        a = analyse_timing("ASML", 62, 921.40 * 62, 1472.60 * 62, [])
        assert a.timing_eur == 0
        assert a.buy_hold_eur == pytest.approx(a.actual_eur, abs=0.01)
        assert a.reconciles

    def test_a_buy_that_rose_afterwards_is_a_gain(self):
        # KLA measured, already in today's share basis: 170 held at the open, 140 bought at
        # EUR 118.34, now EUR 169.74.
        a = analyse_timing("KLA", 310, 110.152 * 310, 169.735 * 310, [_buy(140, 16567.08)])
        assert a.qty_open == pytest.approx(170)
        assert a.trades[0].effect_eur == pytest.approx(140 * (169.735 - 118.336), abs=1.0)
        assert a.trades[0].effect_eur > 0
        assert a.reconciles

    def test_a_sell_before_a_fall_is_a_gain(self):
        # Sold at 246.14 into a market that is now 223.61 — the loss avoided IS the gain.
        a = analyse_timing("ADBE", 115, 244.20 * 115, 223.61 * 115,
                           [_sell(78, 78 * 246.14), _buy(66, 66 * 204.36)])
        assert a.trades[0].effect_eur == pytest.approx(78 * (246.14 - 223.61), abs=1.0)
        assert a.trades[0].effect_eur > 0
        assert a.reconciles

    def test_trading_can_rescue_a_position_that_buy_and_hold_would_have_lost(self):
        # ⚠ THE CASE THE PANEL EXISTS FOR. Adobe: doing nothing loses, the decisions win.
        a = analyse_timing("ADBE", 115, 244.20 * 115, 223.61 * 115,
                           [_sell(78, 78 * 246.14), _buy(66, 66 * 204.36)])
        assert a.buy_hold_eur < 0
        assert a.timing_eur > 0
        assert a.actual_eur > 0

    def test_a_badly_timed_buy_is_a_cost(self):
        # Bought at 200 into a market now at 100: the money would have been better left alone.
        a = analyse_timing("X", 200, 100.0 * 200, 100.0 * 200, [_buy(100, 100 * 200.0)])
        assert a.trades[0].effect_eur == pytest.approx(-10000.0, abs=1.0)
        assert a.timing_eur < 0
        assert a.reconciles


class TestSplits:
    def test_a_pre_split_trade_is_converted_before_it_is_compared(self):
        """⚠ WITHOUT THIS THE COMPARISON IS NONSENSE — `q x (price_now − p)` against a quantity ten
        times too small and a price ten times too large. It is the same defect that put 17 points
        on KLA's money-weighted return."""
        a = analyse_timing("KLA", 310, 110.152 * 310, 169.735 * 310,
                           [_buy(14, 16567.08, datum="2026-02-03")],
                           split_ratio=10.0, split_date="2026-06-12")
        t = a.trades[0]
        assert t.rescaled is True
        assert t.quantity == pytest.approx(140)          # 14 pre-split shares
        assert t.price_eur == pytest.approx(118.336, abs=0.01)
        assert a.qty_open == pytest.approx(170)
        assert a.reconciles

    def test_a_trade_after_the_split_is_left_alone(self):
        a = analyse_timing("KLA", 310, 110.152 * 310, 169.735 * 310,
                           [_buy(140, 16567.08, datum="2026-07-01")],
                           split_ratio=10.0, split_date="2026-06-12")
        assert a.trades[0].rescaled is False
        assert a.trades[0].quantity == pytest.approx(140)

    def test_the_euro_amount_never_moves(self):
        """Money has no share basis. If a rescale changes an amount, it is touching the result."""
        a = analyse_timing("KLA", 310, 110.152 * 310, 169.735 * 310,
                           [_buy(14, 16567.08, datum="2026-02-03")],
                           split_ratio=10.0, split_date="2026-06-12")
        assert a.trades[0].amount_eur == pytest.approx(16567.08)


class TestTheRestatementGap:
    def test_it_names_the_difference_from_the_tables_own_result(self):
        """⚠ AIRS prices shares bought later at JANUARY's price, so its result exceeds the economic
        one by q_bought x (p_buy − price_open) — EUR 1,146 on KLA. Two correct answers to different
        questions, and the modal must not leave a reader to find the second one."""
        airs = 169.735 * 310 - 110.152 * 310          # Huidige waarde − restated Beginwaarde
        a = analyse_timing("KLA", 310, 110.152 * 310, 169.735 * 310,
                           [_buy(140, 16567.08)], airs_result_eur=airs)
        assert a.restatement_eur == pytest.approx(140 * (118.336 - 110.152), abs=2.0)

    def test_no_gap_where_nothing_was_traded(self):
        airs = 1472.60 * 62 - 921.40 * 62
        a = analyse_timing("ASML", 62, 921.40 * 62, 1472.60 * 62, [], airs_result_eur=airs)
        assert a.restatement_eur == pytest.approx(0.0, abs=0.01)


class TestTheTwoNormalisations:
    """`move_pct` says how GOOD a decision was; `effect_pp` says how MUCH it mattered. A reader
    given one immediately wants the other, and conflating them is how a brilliant call on three
    shares gets read as having carried the year."""

    def test_a_buys_percent_is_what_it_has_made_since(self):
        a = analyse_timing("KLA", 310, 110.152 * 310, 169.735 * 310, [_buy(140, 140 * 118.336)])
        # 169.735 / 118.336 - 1
        assert a.trades[0].move_pct == pytest.approx(43.43, abs=0.05)

    def test_a_sells_percent_is_what_it_avoided_since(self):
        # Sold at 246.14, now 223.61 — the price fell 9.15% of the proceeds after the decision.
        a = analyse_timing("ADBE", 115, 244.20 * 115, 223.61 * 115, [_sell(78, 78 * 246.14)])
        assert a.trades[0].move_pct == pytest.approx(9.15, abs=0.05)

    def test_a_sell_before_a_RISE_scores_negative(self):
        # ⚠ SIGNED SO FAVOURABLE IS POSITIVE IN BOTH DIRECTIONS. Measured on AITopSelectie's KLA:
        # the price rose 31% after the January sale, so the sale cost 31% of its proceeds.
        a = analyse_timing("X", 100, 100.0 * 100, 200.0 * 100, [_sell(50, 50 * 100.0)])
        assert a.trades[0].move_pct == pytest.approx(-100.0, abs=0.01)

    def test_the_percent_is_blind_to_size(self):
        """Three shares and three thousand, same call, same %."""
        small = analyse_timing("X", 1000, 100.0 * 1000, 150.0 * 1000, [_buy(3, 3 * 100.0)])
        large = analyse_timing("X", 1000, 100.0 * 1000, 150.0 * 1000, [_buy(300, 300 * 100.0)])
        assert small.trades[0].move_pct == pytest.approx(large.trades[0].move_pct)
        # ...and pp is precisely what tells them apart.
        assert abs(small.trades[0].effect_pp) < abs(large.trades[0].effect_pp)

    def test_every_line_divides_by_the_SAME_base_so_the_identity_carries_through(self):
        """⚠ One denominator is the whole reason the percentages add up. Give each line its own
        and the 'decomposition' silently stops being one."""
        a = analyse_timing("ADBE", 115, 244.20 * 115, 223.61 * 115,
                           [_sell(78, 78 * 246.14), _buy(66, 66 * 204.36)])
        assert a.open_value_eur == pytest.approx(a.qty_open * a.price_open_eur, abs=0.01)
        assert a.buy_hold_pct + a.timing_pp == pytest.approx(a.actual_pct, abs=0.02)
        assert a.timing_pp == pytest.approx(sum(t.effect_pp for t in a.trades), abs=0.01)

    def test_the_base_is_what_was_HELD_not_AIRSs_restated_opening_value(self):
        """⚠ THE RESTATEMENT BUG WEARING A PERCENT SIGN. `start_value_eur` prices TODAY's 310
        shares at January's price; only 170 were held. Dividing by it would report a buy-and-hold
        return on shares that were never there."""
        a = analyse_timing("KLA", 310, 110.152 * 310, 169.735 * 310, [_buy(140, 140 * 118.336)])
        assert a.open_value_eur == pytest.approx(170 * 110.152, abs=1.0)
        assert a.open_value_eur < 110.152 * 310
        # buy-and-hold in percent IS the instrument's move, by construction.
        assert a.buy_hold_pct == pytest.approx((169.735 / 110.152 - 1) * 100, abs=0.05)

    def test_percentages_are_withheld_where_nothing_was_held_at_the_open(self):
        """⚠ MEASURED: AITopSelectie bought its whole KLA position on 5 January. A 0.00pp there
        reads as 'this decision did not matter' when the decisions were the entire result."""
        a = analyse_timing("KLA", 410, 100.0 * 410, 155.0 * 410,
                           [_buy(460, 460 * 100.0), _sell(50, 50 * 100.0)])
        assert a.qty_open == 0.0
        assert a.open_value_eur is None
        assert a.buy_hold_pct is None and a.actual_pct is None and a.timing_pp is None
        assert all(t.effect_pp is None for t in a.trades)
        # ⚠ ...but the per-trade % survives, because it needs no opening position — only the trade.
        assert all(t.move_pct is not None for t in a.trades)

    def test_the_euro_reconcile_flag_is_never_asserted_on_the_percentages(self):
        """Rounding three figures to 2dp can miss by a hundredth (Adobe: −8.43 + 9.77 = 1.34 against
        1.33). A tolerance loose enough to absorb that is loose enough to hide a real break, so
        `reconciles` stays on the exact EUR line."""
        a = analyse_timing("ADBE", 115, 244.20 * 115, 223.61 * 115,
                           [_sell(78, 78 * 246.14), _buy(66, 66 * 204.36)])
        assert a.reconciles
        assert abs(a.residual_eur) < 0.5


class TestRefusals:
    def test_no_opening_value_means_no_counterfactual(self):
        # Not held when the year opened — "what would doing nothing have made" has no subject.
        assert analyse_timing("X", 100, 0.0, 5000.0, []) is None

    def test_no_quantity_is_refused(self):
        assert analyse_timing("X", 0, 1000.0, 1000.0, []) is None

    def test_buys_beyond_the_holding_cannot_make_the_opening_negative(self):
        """A negative counterfactual would render as a confident number. Clamped to zero, which
        reads as 'nothing was held at the open' — true, and the honest floor."""
        a = analyse_timing("X", 50, 100.0 * 50, 120.0 * 50, [_buy(90, 90 * 110.0)])
        assert a.qty_open == 0.0
        assert a.buy_hold_eur == 0.0

    def test_a_zero_quantity_trade_is_skipped_not_divided_by(self):
        a = analyse_timing("X", 100, 100.0 * 100, 120.0 * 100, [_buy(0, 0.0)])
        assert a.trades == []
        assert a.reconciles
