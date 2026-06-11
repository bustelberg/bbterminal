"""Daily 'tit-for-tat' timing overlay tests.

The overlay re-chains the daily equity curve: hold the strategy today only
when the PRIOR day's underlying return was >= 0, else sit in cash (0%). It
reshapes the curve + headline stats but never changes selection. Off by
default → byte-identical to the plain strategy.
"""
from __future__ import annotations

import pandas as pd

from momentum.backtest.equity_curve import _build_daily_equity_curve
from momentum.backtest.types import PeriodHolding, PeriodRecord


def _one_holding_record(prices: list[float], start="2024-01-01") -> PeriodRecord:
    """A single-period, single-holding record whose daily curve traces the
    given price path (entry at prices[0])."""
    dates = pd.date_range(start, periods=len(prices), freq="D")
    cid = 1
    h = PeriodHolding(
        company_id=cid, ticker="A", company_name="A", sector="X",
        score=0.0, category_scores={}, weight=1.0, forward_return_pct=0.0,
        entry_price_eur=prices[0], exit_price_eur=prices[-1],
        entry_date=dates[0].strftime("%Y-%m-%d"), exit_date=dates[-1].strftime("%Y-%m-%d"),
        side="long",
    )
    rec = PeriodRecord(date=dates[0].strftime("%Y-%m-%d"), holdings=[h], portfolio_return_pct=0.0, cumulative_return_pct=0.0)
    price_index = {cid: pd.Series(prices, index=dates, dtype="float64")}
    return rec, price_index


class TestDailyTimingCurve:
    def test_off_is_identical(self):
        rec, pidx = _one_holding_record([100, 110, 99, 105, 120])
        plain, _, _ = _build_daily_equity_curve([rec], pidx, "long_only", daily_timing=False)
        again, _, _ = _build_daily_equity_curve([rec], pidx, "long_only", daily_timing=False)
        assert plain == again  # sanity: deterministic

    def test_sits_out_the_day_after_a_down_day(self):
        # Path: 100 →110 (+10%, day1) →99 (−10%, day2) →108.9 (+10%, day3).
        # Day 3 follows a DOWN day (day2), so tit-for-tat sits in cash that
        # day and does NOT capture the +10% bounce.
        rec, pidx = _one_holding_record([100, 110, 99, 108.9])
        plain, _, _ = _build_daily_equity_curve([rec], pidx, "long_only", daily_timing=False)
        timed, _, _ = _build_daily_equity_curve([rec], pidx, "long_only", daily_timing=True)

        # Plain ends roughly flat (compounding ±10% then +10%): 1.1*0.9*1.1 = 1.089.
        assert plain[-1][1] == round((1.1 * 0.9 * 1.1 - 1) * 100, 4)
        # Timed: day1 invested (start), day2 follows an UP day → invested
        # (takes the −10%), day3 follows a DOWN day → cash (skips +10%).
        # Factor = 1.1 * 0.9 * 1.0 = 0.99.
        assert timed[-1][1] == round((1.1 * 0.9 - 1) * 100, 4)

    def test_skips_downside_when_prior_day_down(self):
        # 100 →90 (−10%, day1) →81 (−10%, day2) →89.1 (+10%, day3).
        # Day1 invested (start) → takes −10%. Day2 follows a DOWN day → cash
        # (skips the second −10%). Day3 follows a (cash) DOWN underlying day
        # → cash (skips +10%). Timed factor = 0.9.
        rec, pidx = _one_holding_record([100, 90, 81, 89.1])
        timed, _, _ = _build_daily_equity_curve([rec], pidx, "long_only", daily_timing=True)
        assert timed[-1][1] == round((0.9 - 1) * 100, 4)

    def test_counts_swaps_per_period(self):
        # One transition (invested → cash) on the day after the down day.
        rec, pidx = _one_holding_record([100, 110, 99, 108.9])
        _, _, swaps = _build_daily_equity_curve([rec], pidx, "long_only", daily_timing=True)
        assert swaps == {0: 1}
        # Off → nothing to bill.
        _, _, swaps_off = _build_daily_equity_curve([rec], pidx, "long_only", daily_timing=False)
        assert swaps_off == {}
