"""Rebalance-frequency variants: cadence generation + end-to-end loops.

Pins the per-frequency annualization constants (`_periods_per_year`),
the rebalance-date generator across monthly / weekly / daily variants,
and full end-to-end loops at `every_2_months` and `weekly` so the wider
strides don't silently break selection or forward-return wiring."""
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from momentum.backtest import (
    BacktestConfig,
    _build_price_index,
    _generate_rebalance_dates,
    _periods_per_year,
    run_backtest,
)

from tests._backtest_helpers import (
    build_prices_df,
    build_universe_df,
    calendar_daily,
    equal_signal_weights,
    expected_forward_return,
)


class TestPeriodsPerYear:
    """Pin the per-frequency annualization constants. These feed into the
    Sharpe sqrt-scaling and the annualized-return calculation, so silently
    changing one would shift all reported stats."""

    def test_constants(self):
        assert _periods_per_year("daily") == 252.0
        assert _periods_per_year("weekly") == 52.0
        assert _periods_per_year("monthly") == 12.0
        assert _periods_per_year("every_2_months") == 6.0
        assert _periods_per_year("every_3_months") == 4.0


class TestGenerateRebalanceDates:
    """Cadence: each frequency emits the right stride between start and end.
    Boundary inclusivity matters — the monthly variants emit the end-of-range
    month-start (forward-return needs the *next* date, so the last entry
    is an exit-only sentinel)."""

    def test_monthly(self):
        dates = _generate_rebalance_dates(date(2024, 1, 1), date(2024, 6, 1), "monthly")
        assert dates == [
            date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1),
            date(2024, 4, 1), date(2024, 5, 1), date(2024, 6, 1),
        ]

    def test_every_2_months_strides_by_two(self):
        dates = _generate_rebalance_dates(date(2024, 1, 1), date(2024, 7, 1), "every_2_months")
        assert dates == [date(2024, 1, 1), date(2024, 3, 1), date(2024, 5, 1), date(2024, 7, 1)]

    def test_every_3_months_strides_by_three(self):
        dates = _generate_rebalance_dates(date(2024, 1, 1), date(2024, 12, 1), "every_3_months")
        assert dates == [date(2024, 1, 1), date(2024, 4, 1), date(2024, 7, 1), date(2024, 10, 1)]

    def test_weekly_emits_mondays(self):
        # 2024-01-01 IS a Monday — first emitted date should equal start.
        dates = _generate_rebalance_dates(date(2024, 1, 1), date(2024, 1, 22), "weekly")
        assert dates == [
            date(2024, 1, 1), date(2024, 1, 8), date(2024, 1, 15), date(2024, 1, 22),
        ]
        assert all(d.weekday() == 0 for d in dates)

    def test_weekly_skips_to_first_monday_when_start_isnt_monday(self):
        # 2024-01-03 is a Wednesday → first emitted date is the next Monday.
        dates = _generate_rebalance_dates(date(2024, 1, 3), date(2024, 1, 22), "weekly")
        assert dates[0] == date(2024, 1, 8)

    def test_daily_uses_prices_df_trading_days(self):
        # Synthetic prices with explicit gaps — generator must not invent
        # trading days that aren't in the data.
        rows = [
            {"company_id": 1, "target_date": d, "price": 100.0}
            for d in [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 5), date(2024, 1, 8)]
        ]
        prices = pd.DataFrame(rows)
        dates = _generate_rebalance_dates(date(2024, 1, 1), date(2024, 1, 10), "daily", prices)
        assert dates == [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 5), date(2024, 1, 8)]

    def test_daily_requires_prices(self):
        with pytest.raises(ValueError, match="daily frequency requires prices_df"):
            _generate_rebalance_dates(date(2024, 1, 1), date(2024, 1, 10), "daily", None)


class TestEvery2MonthsBacktest:
    """End-to-end: same universe, every_2_months cadence emits half as many
    rebalance periods as monthly across the same date range. Selection +
    forward-return wiring must still work with the wider stride."""

    def test_full_loop(self):
        # 6-month window so we get 3 every-2-month entries (Jan, Mar, May)
        # plus a May→Jul exit pin (May has no forward → 2 records: Jan, Mar).
        history_start = "2023-09-01"
        prices_end = "2024-08-15"
        bt_start = date(2024, 1, 1)
        bt_end = date(2024, 7, 1)

        dates = calendar_daily(history_start, prices_end)
        companies = {10: 1.0012, 11: 1.0010, 20: 1.0011, 21: 1.0009}
        prices = build_prices_df(companies, dates)
        universe = build_universe_df([
            (10, "A1", "Alpha", "Alpha-1"),
            (11, "A2", "Alpha", "Alpha-2"),
            (20, "B1", "Beta",  "Beta-1"),
            (21, "B2", "Beta",  "Beta-2"),
        ])

        config = BacktestConfig(
            start_date=bt_start,
            end_date=bt_end,
            signal_weights=equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=2,
            rebalance_frequency="every_2_months",
        )
        result = run_backtest(config, prices, universe)

        # Periods: [Jan, Mar, May, Jul]; iterates [:-1] → 3 records (Jan, Mar, May).
        assert [r.date for r in result.monthly_records] == ["2024-01", "2024-03", "2024-05"]

        # All 4 picked each period (top_n_sectors=2 × top_n_per_sector=2 = 4).
        for rec in result.monthly_records:
            assert {h.company_id for h in rec.holdings} == {10, 11, 20, 21}

        # Forward returns derived against entry/exit pairs from periods[i] → periods[i+1].
        idx = _build_price_index(prices)
        period_starts = [date(2024, 1, 1), date(2024, 3, 1), date(2024, 5, 1), date(2024, 7, 1)]
        for i, rec in enumerate(result.monthly_records):
            entry_ts = pd.Timestamp(period_starts[i])
            exit_ts = pd.Timestamp(period_starts[i + 1])
            for h in rec.holdings:
                expected = expected_forward_return(idx[h.company_id], entry_ts, exit_ts)
                assert h.forward_return_pct == pytest.approx(expected)


class TestWeeklyBacktest:
    """Weekly cadence inside a 6-week window. Same universe shape; the
    rebalance period is now 7 days, and entry/exit prices come from the
    next available trading day on/after each Monday."""

    def test_full_loop(self):
        history_start = "2023-09-01"
        prices_end = "2024-03-15"
        bt_start = date(2024, 1, 1)  # Monday
        bt_end = date(2024, 2, 12)   # Monday — 6 weekly entries inclusive

        dates = calendar_daily(history_start, prices_end)
        companies = {10: 1.0010, 20: 1.0008}
        prices = build_prices_df(companies, dates)
        universe = build_universe_df([
            (10, "A1", "Alpha", "Alpha-1"),
            (20, "B1", "Beta",  "Beta-1"),
        ])

        config = BacktestConfig(
            start_date=bt_start,
            end_date=bt_end,
            signal_weights=equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=1,
            rebalance_frequency="weekly",
        )
        result = run_backtest(config, prices, universe)

        # Mondays from Jan 1 to Feb 12 inclusive: 7 dates → 6 records (last is exit).
        assert len(result.monthly_records) == 6
        # Sub-monthly variant emits full YYYY-MM-DD.
        assert all(len(r.date) == 10 and r.date[4] == "-" and r.date[7] == "-"
                   for r in result.monthly_records)
        assert result.monthly_records[0].date == "2024-01-01"
        assert result.monthly_records[-1].date == "2024-02-05"
