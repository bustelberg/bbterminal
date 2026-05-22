"""Basic 3-month backtest + top-N-per-sector selection.

Pins the holdings shape, equal weights, forward returns, cumulative
compounding, avg_holdings, and turnover for two canonical universes:
(1) every company picked every month (4 names, 2 per sector);
(2) the weakest in each sector dropped (6 names, 2 of 3 picked)."""
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from momentum.backtest import BacktestConfig, _build_price_index, run_backtest

from tests._backtest_helpers import (
    BACKTEST_END,
    BACKTEST_START,
    EXPECTED_MONTHS,
    PRICE_HISTORY_START,
    PRICES_END,
    build_prices_df,
    build_universe_df,
    calendar_daily,
    equal_signal_weights,
    expected_forward_return,
)


class TestBasicThreeMonthBacktest:
    """4 companies in 2 sectors, top_n_sectors=2, top_n_per_sector=2 →
    every company is picked every month. Pins the holdings shape, equal
    weights, forward returns, and cumulative compounding."""

    def test_full_loop(self):
        dates = calendar_daily(PRICE_HISTORY_START, PRICES_END)
        companies = {
            10: 1.0010,  # Alpha
            20: 1.0008,  # Alpha
            30: 1.0009,  # Beta
            40: 1.0007,  # Beta
        }
        prices = build_prices_df(companies, dates)
        universe = build_universe_df([
            (10, "A1", "Alpha", "Alpha-1"),
            (20, "A2", "Alpha", "Alpha-2"),
            (30, "B1", "Beta",  "Beta-1"),
            (40, "B2", "Beta",  "Beta-2"),
        ])

        config = BacktestConfig(
            start_date=BACKTEST_START,
            end_date=BACKTEST_END,
            signal_weights=equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=2,
        )

        result = run_backtest(config, prices, universe)

        assert [r.date for r in result.monthly_records] == EXPECTED_MONTHS

        # All 4 picked each month, equal weights.
        for rec in result.monthly_records:
            ids = {h.company_id for h in rec.holdings}
            assert ids == {10, 20, 30, 40}, f"month={rec.date} got={ids}"
            assert all(h.weight == pytest.approx(0.25) for h in rec.holdings)
            assert sum(h.weight for h in rec.holdings) == pytest.approx(1.0)

        # Forward returns match what _price_on_or_after derives. Re-derive
        # from the same price index so the test confirms wiring, not maths.
        idx = _build_price_index(prices)
        # Engine aligns to first-Monday-of-month now (matches /schedule).
        # Dec 1 2024 Sun → Dec 2; Jan 1 2025 Wed → Jan 6; Feb 1 2025 Sat → Feb 3; Mar 1 2025 Sat → Mar 3.
        month_starts = [date(2024, 12, 2), date(2025, 1, 6), date(2025, 2, 3), date(2025, 3, 3)]
        for i, rec in enumerate(result.monthly_records):
            entry_ts = pd.Timestamp(month_starts[i])
            exit_ts = pd.Timestamp(month_starts[i + 1])
            for h in rec.holdings:
                expected = expected_forward_return(idx[h.company_id], entry_ts, exit_ts)
                assert h.forward_return_pct == pytest.approx(expected), (
                    f"month={rec.date} cid={h.company_id} expected={expected} got={h.forward_return_pct}"
                )

        # Cumulative compounds across the equity curve. All companies have
        # positive daily factors → every monthly portfolio_return_pct is
        # positive → cumulative_return_pct strictly increases.
        cumulatives = [r.cumulative_return_pct for r in result.monthly_records]
        assert cumulatives == sorted(cumulatives)
        # `summary.total_return_pct` equals the final equity-curve value
        # (both are `round(cumulative, 2)` after the last month).
        assert result.summary.total_return_pct == pytest.approx(
            result.monthly_records[-1].cumulative_return_pct
        )
        # Sanity: chain monthly returns and confirm the same final value.
        chained = 1.0
        for r in result.monthly_records:
            if r.portfolio_return_pct is not None:
                chained *= 1 + r.portfolio_return_pct / 100
        assert result.summary.total_return_pct == pytest.approx(round((chained - 1) * 100, 2))

        # `avg_holdings` reflects the 4-per-month constant.
        assert result.summary.avg_holdings == pytest.approx(4.0)
        # No turnover — same 4 names every month.
        assert result.summary.avg_monthly_turnover_pct == pytest.approx(0.0)


class TestTopNPerSectorSelection:
    """6 companies, 3 per sector, top_n_per_sector=2. The weakest in each
    sector should never be picked; the four strongest always are."""

    def test_weakest_dropped_each_month(self):
        dates = calendar_daily(PRICE_HISTORY_START, PRICES_END)
        companies = {
            # Alpha sector (strict ordering: 10 > 11 > 12)
            10: 1.0012,
            11: 1.0010,
            12: 1.0001,  # weakest — should never be picked
            # Beta sector (strict ordering: 20 > 21 > 22)
            20: 1.0011,
            21: 1.0009,
            22: 1.0002,  # weakest — should never be picked
        }
        prices = build_prices_df(companies, dates)
        universe = build_universe_df([
            (10, "A1", "Alpha", "Alpha-1"),
            (11, "A2", "Alpha", "Alpha-2"),
            (12, "A3", "Alpha", "Alpha-3"),
            (20, "B1", "Beta",  "Beta-1"),
            (21, "B2", "Beta",  "Beta-2"),
            (22, "B3", "Beta",  "Beta-3"),
        ])

        config = BacktestConfig(
            start_date=BACKTEST_START,
            end_date=BACKTEST_END,
            signal_weights=equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=2,
        )
        result = run_backtest(config, prices, universe)

        for rec in result.monthly_records:
            picked = {h.company_id for h in rec.holdings}
            assert picked == {10, 11, 20, 21}, f"month={rec.date} got={picked}"
