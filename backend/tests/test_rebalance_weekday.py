"""Rebalance-weekday semantics for the backtest engine.

Two things this pins, both central to the "sweep different rebalance days"
feature:

1. `_generate_rebalance_dates(weekday=W)` lands every rebalance on the
   first weekday-W of each period (first Monday by default, first
   Wednesday for W=2), and different weekdays produce different grids.

2. The signal cutoff is strict-`<` the rebalance date. So a first-
   Wednesday rebalance decides on data through the PRIOR trading day's
   (Tuesday's) close — Wednesday's own close is never seen at decision
   time. We prove this by showing the signal at a Wednesday cutoff is
   unchanged whether or not the Wednesday bar exists in the price series:
   if Wednesday's close were used, dropping it would move the signal.
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from momentum.backtest import BacktestConfig, run_backtest
from momentum.backtest.dates import _generate_rebalance_dates
from momentum.signals import compute_price_signals, compute_signals_panel

from tests._backtest_helpers import (
    build_prices_df,
    build_universe_df,
    equal_signal_weights,
)


class TestGenerateRebalanceDatesWeekday:
    def test_monthly_first_monday_default(self):
        dates = _generate_rebalance_dates(date(2024, 1, 1), date(2024, 3, 31), "monthly")
        assert dates == [date(2024, 1, 1), date(2024, 2, 5), date(2024, 3, 4)]
        assert all(d.weekday() == 0 for d in dates)  # Mondays

    def test_monthly_first_wednesday(self):
        dates = _generate_rebalance_dates(date(2024, 1, 1), date(2024, 3, 31), "monthly", weekday=2)
        assert dates == [date(2024, 1, 3), date(2024, 2, 7), date(2024, 3, 6)]
        assert all(d.weekday() == 2 for d in dates)  # Wednesdays

    def test_different_weekdays_differ(self):
        mon = _generate_rebalance_dates(date(2024, 1, 1), date(2024, 6, 30), "monthly", weekday=0)
        wed = _generate_rebalance_dates(date(2024, 1, 1), date(2024, 6, 30), "monthly", weekday=2)
        assert mon != wed
        assert len(mon) == len(wed)  # same number of periods, shifted by 2 days

    def test_weekly_lands_on_chosen_weekday(self):
        dates = _generate_rebalance_dates(date(2024, 1, 1), date(2024, 1, 31), "weekly", weekday=2)
        assert all(d.weekday() == 2 for d in dates)
        assert dates[0] == date(2024, 1, 3)


class TestSignalCutoffIsStrictlyBeforeRebalanceDay:
    """A Wednesday rebalance must compute signals from data strictly before
    Wednesday (i.e. through Tuesday's close)."""

    def _series(self, n: int = 60, end: str = "2024-03-06") -> pd.Series:
        # `end` = Wed 2024-03-06. Daily business-day series of rising prices.
        idx = pd.bdate_range(end=pd.Timestamp(end), periods=n)
        prices = np.linspace(100.0, 160.0, n)
        return pd.Series(prices, index=idx, dtype="float64")

    def test_wednesday_close_not_used_in_signal(self):
        cutoff = date(2024, 3, 6)  # Wednesday — a bar that exists in the series
        full = self._series()
        assert pd.Timestamp(cutoff) in full.index

        universe = pd.DataFrame({
            "company_id": [1],
            "sector": ["Tech"],
            "company_name": ["Co"],
            "gurufocus_ticker": ["CO"],
        })

        # Signal with the Wednesday bar present in the index.
        with_wed = compute_signals_panel(
            universe, [cutoff], price_index={1: full},
        )[cutoff]

        # Drop the Wednesday bar entirely and recompute at the SAME cutoff.
        # If the engine used Wednesday's close, removing it would change the
        # signal. Strict-`<` anchors both to Tuesday, so they must match.
        without_wed = compute_signals_panel(
            universe, [cutoff], price_index={1: full.iloc[:-1]},
        )[cutoff]

        assert not with_wed.empty and not without_wed.empty
        for col in ("mom_12_1", "mom_6m", "above_200ma", "drawdown_from_recent_high_pct"):
            a = with_wed.iloc[0][col]
            b = without_wed.iloc[0][col]
            assert (pd.isna(a) and pd.isna(b)) or a == b, f"{col} differs: {a} vs {b}"

    def test_panel_matches_single_cutoff_path(self):
        # The strict-`<` single-cutoff path and the vectorized panel must
        # agree at a Wednesday cutoff (parity guard for the cutoff edge).
        cutoff = date(2024, 3, 6)
        full = self._series()
        universe = pd.DataFrame({
            "company_id": [1],
            "sector": ["Tech"],
            "company_name": ["Co"],
            "gurufocus_ticker": ["CO"],
        })
        single = compute_price_signals(pd.DataFrame(), universe, cutoff, price_index={1: full})
        panel = compute_signals_panel(universe, [cutoff], price_index={1: full})[cutoff]
        for col in ("mom_12_1", "mom_6m", "drawdown_from_recent_high_pct"):
            a = single.iloc[0][col]
            b = panel.iloc[0][col]
            assert (pd.isna(a) and pd.isna(b)) or abs(a - b) < 1e-9


class TestEntryPricedAtPriorTradingDay:
    """The execution/entry price is the close of the last trading day
    STRICTLY BEFORE the rebalance date — the same bar the signals saw. On a
    real (business-day) calendar that means a first-Monday rebalance enters
    at the prior Friday's close, and a first-Wednesday rebalance at Tuesday's
    close. (The synthetic exponential-price suite can't catch this: a uniform
    one-day shift of both entry and exit leaves the return ratio unchanged.)
    """

    def _run(self, *, weekday: int):
        # Business-day calendar → weekends are NOT trading days, so "prior
        # trading day" before a Monday is the preceding Friday.
        dates = pd.bdate_range(start="2023-01-02", end="2025-03-15")
        companies = {10: 1.0012, 11: 1.0010, 20: 1.0011, 21: 1.0009}
        prices = build_prices_df(companies, dates)
        universe = build_universe_df([
            (10, "A1", "Alpha", "Alpha-1"),
            (11, "A2", "Alpha", "Alpha-2"),
            (20, "B1", "Beta", "Beta-1"),
            (21, "B2", "Beta", "Beta-2"),
        ])
        config = BacktestConfig(
            start_date=date(2024, 12, 1),
            end_date=date(2025, 2, 1),
            signal_weights=equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=2,
            rebalance_frequency="monthly",
            rebalance_weekday=weekday,
        )
        return run_backtest(config, prices, universe)

    def test_monday_rebalance_enters_at_prior_friday(self):
        result = self._run(weekday=0)
        first = result.monthly_records[0]
        # Period label is the rebalance Monday (the signal anchor)…
        assert first.date == "2024-12-02"
        assert first.holdings
        # …but every holding is PRICED at the prior trading day = Fri Nov 29.
        for h in first.holdings:
            assert h.entry_date == "2024-11-29", f"{h.company_id} entered {h.entry_date}"

    def test_wednesday_rebalance_enters_at_prior_tuesday(self):
        result = self._run(weekday=2)
        first = result.monthly_records[0]
        # First Wednesday of Dec 2024 is the 4th; prior trading day = Tue 3rd.
        assert first.date == "2024-12-04"
        assert first.holdings
        for h in first.holdings:
            assert h.entry_date == "2024-12-03", f"{h.company_id} entered {h.entry_date}"

    def test_company_with_gap_on_prior_day_never_uses_rebalance_day(self):
        # Regression for the "some holdings still use the rebalance Monday"
        # bug: a company that DIDN'T trade the prior trading day (a data gap)
        # must fall BACK to its last available close, never forward onto the
        # rebalance day. Punch a one-day hole in company 10 on Fri 2024-11-29
        # (the prior trading day before the Mon 2024-12-02 rebalance).
        dates = pd.bdate_range(start="2023-01-02", end="2025-03-15")
        companies = {10: 1.0012, 11: 1.0010, 20: 1.0011, 21: 1.0009}
        prices = build_prices_df(companies, dates)
        prices = prices[
            ~((prices["company_id"] == 10) & (prices["target_date"] == date(2024, 11, 29)))
        ].reset_index(drop=True)
        universe = build_universe_df([
            (10, "A1", "Alpha", "Alpha-1"),
            (11, "A2", "Alpha", "Alpha-2"),
            (20, "B1", "Beta", "Beta-1"),
            (21, "B2", "Beta", "Beta-2"),
        ])
        config = BacktestConfig(
            start_date=date(2024, 12, 1),
            end_date=date(2025, 2, 1),
            signal_weights=equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=2,
            rebalance_frequency="monthly",
            rebalance_weekday=0,
        )
        result = run_backtest(config, prices, universe)
        first = result.monthly_records[0]
        by_cid = {h.company_id: h.entry_date for h in first.holdings}
        # The gap company falls back to its prior close (Thu Nov 28)…
        assert by_cid[10] == "2024-11-28", by_cid
        # …a company that traded Friday uses Friday…
        assert by_cid[11] == "2024-11-29", by_cid
        # …and NOTHING enters on the rebalance Monday.
        assert all(d != "2024-12-02" for d in by_cid.values()), by_cid
