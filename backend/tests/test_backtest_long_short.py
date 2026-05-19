"""Long-short variants: top + bottom selection, side flags, collision
dedup, and the random-mode guard.

Covers both the scoring layer (`score_and_select(direction="bottom")`)
and the runner's wiring — long + short bucket builds, mean(long) −
mean(short) return math, and the long_short × random rejection."""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from momentum.backtest import BacktestConfig, _build_price_index, run_backtest
from momentum.scoring import score_and_select

from tests._backtest_helpers import (
    BACKTEST_END,
    BACKTEST_START,
    EXPECTED_MONTHS,
    PRICE_HISTORY_START,
    PRICES_END,
    build_prices_df,
    build_six_company_signals_df,
    build_universe_df,
    calendar_daily,
    equal_signal_weights,
    expected_forward_return,
)


class TestScoreAndSelectDirection:
    """`direction="bottom"` mirrors the top-side ranking: worst sectors,
    worst names within each. Same shape, opposite end."""

    def test_bottom_picks_worst_sector_and_worst_names(self):
        df = build_six_company_signals_df()
        # Both sectors have 3 names; with top_n_sectors=1 the picker chooses
        # one sector — Alpha (10,11,12) outranks Beta on every signal here
        # (since alpha values include 5,3,1 vs beta's 4,2,0; mean is higher).
        # direction="top" picks Alpha; direction="bottom" picks Beta.
        top = score_and_select(df, equal_signal_weights(),
                               top_n_sectors=1, top_n_per_sector=2, direction="top")
        bottom = score_and_select(df, equal_signal_weights(),
                                  top_n_sectors=1, top_n_per_sector=2, direction="bottom")

        assert set(top["sector"].unique()) == {"Alpha"}
        assert set(bottom["sector"].unique()) == {"Beta"}
        # Top: best 2 in Alpha → ids 10, 11.
        assert sorted(top["company_id"].tolist()) == [10, 11]
        # Bottom: worst 2 in Beta → ids 21, 22.
        assert sorted(bottom["company_id"].tolist()) == [21, 22]


class TestLongShortBacktest:
    """End-to-end long-short: select top + bottom, confirm holdings carry
    side flags, and confirm portfolio_return_pct = mean(long) − mean(short)."""

    def test_long_short_full_loop(self):
        dates = calendar_daily(PRICE_HISTORY_START, PRICES_END)
        # Two sectors of three each. Within each sector, ranks are:
        # Alpha: 10 > 11 > 12 (10 strongest, 12 weakest)
        # Beta:  20 > 21 > 22
        companies = {
            10: 1.0012, 11: 1.0010, 12: 1.0001,
            20: 1.0011, 21: 1.0009, 22: 1.0002,
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
            top_n_sectors=2,        # both sectors land on each side
            top_n_per_sector=1,     # top 1 per sector → 2 longs; bottom 1 → 2 shorts
            strategy_type="long_short",
        )
        result = run_backtest(config, prices, universe)

        assert [r.date for r in result.monthly_records] == EXPECTED_MONTHS

        # Each period: 2 longs (best per sector) + 2 shorts (worst per sector).
        for rec in result.monthly_records:
            longs = [h for h in rec.holdings if h.side == "long"]
            shorts = [h for h in rec.holdings if h.side == "short"]
            assert {h.company_id for h in longs} == {10, 20}
            assert {h.company_id for h in shorts} == {12, 22}
            # Long weight = 1/2; short weight = 1/2 (each side sums to 1.0
            # → 200% gross, 0% net).
            assert all(h.weight == pytest.approx(0.5) for h in longs)
            assert all(h.weight == pytest.approx(0.5) for h in shorts)

        # Portfolio return per period = mean(long_returns) - mean(short_returns).
        # Re-derive from the same price index so the test pins the math, not
        # the magnitudes.
        idx = _build_price_index(prices)
        # Engine aligns to first-Monday-of-month now (matches /schedule).
        month_starts = [date(2024, 12, 2), date(2025, 1, 6), date(2025, 2, 3), date(2025, 3, 3)]
        for i, rec in enumerate(result.monthly_records):
            entry_ts = pd.Timestamp(month_starts[i])
            exit_ts = pd.Timestamp(month_starts[i + 1])
            long_rets = [expected_forward_return(idx[h.company_id], entry_ts, exit_ts)
                         for h in rec.holdings if h.side == "long"]
            short_rets = [expected_forward_return(idx[h.company_id], entry_ts, exit_ts)
                          for h in rec.holdings if h.side == "short"]
            expected = round(float(np.mean(long_rets)) - float(np.mean(short_rets)), 2)
            assert rec.portfolio_return_pct == pytest.approx(expected), (
                f"period={rec.date} expected={expected} got={rec.portfolio_return_pct}"
            )

        # Long-short on this synthetic universe should be strictly profitable
        # every period — the longs (best names) outgrow the shorts (weakest
        # names) by construction. So cumulative return is monotonically up.
        cumulatives = [r.cumulative_return_pct for r in result.monthly_records]
        assert cumulatives == sorted(cumulatives)
        # And every period's return should be positive (longs beat shorts).
        for r in result.monthly_records:
            assert r.portfolio_return_pct is not None
            assert r.portfolio_return_pct > 0

    def test_long_short_dedupes_overlap(self):
        """When the same company would land on both books (small universe
        with overlapping sector lists), drop it from both. Here the universe
        has only one sector so top + bottom both pick from it."""
        dates = calendar_daily(PRICE_HISTORY_START, PRICES_END)
        # 3 names in a single sector. With top_n_sectors=1 both top and
        # bottom select from the same sector, and with top_n_per_sector=2
        # the top 2 (10,11) and bottom 2 (11,12) collide on 11.
        companies = {10: 1.0012, 11: 1.0010, 12: 1.0001}
        prices = build_prices_df(companies, dates)
        universe = build_universe_df([
            (10, "S1", "Solo", "Solo-1"),
            (11, "S2", "Solo", "Solo-2"),
            (12, "S3", "Solo", "Solo-3"),
        ])
        config = BacktestConfig(
            start_date=BACKTEST_START,
            end_date=BACKTEST_END,
            signal_weights=equal_signal_weights(),
            top_n_sectors=1,
            top_n_per_sector=2,
            strategy_type="long_short",
        )
        result = run_backtest(config, prices, universe)

        for rec in result.monthly_records:
            ids = [h.company_id for h in rec.holdings]
            # 11 collided → dropped from both sides. 10 stays long, 12 stays short.
            assert 11 not in ids
            longs = {h.company_id for h in rec.holdings if h.side == "long"}
            shorts = {h.company_id for h in rec.holdings if h.side == "short"}
            assert longs == {10}
            assert shorts == {12}

    def test_long_short_forbids_random(self):
        """Random + long-short produces meaningless results, so the engine
        rejects the combination at config validation."""
        dates = calendar_daily(PRICE_HISTORY_START, PRICES_END)
        prices = build_prices_df({10: 1.0010, 20: 1.0010}, dates)
        universe = build_universe_df([
            (10, "A1", "Alpha", "Alpha-1"),
            (20, "B1", "Beta",  "Beta-1"),
        ])
        config = BacktestConfig(
            start_date=BACKTEST_START,
            end_date=BACKTEST_END,
            signal_weights=equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=1,
            selection_mode="random",
            random_seed=42,
            strategy_type="long_short",
        )
        with pytest.raises(ValueError, match="long_short"):
            run_backtest(config, prices, universe)

    def test_long_only_default_unchanged(self):
        """Smoke check: omitting strategy_type produces side="long" on every
        holding and the same return math as before this change shipped."""
        dates = calendar_daily(PRICE_HISTORY_START, PRICES_END)
        companies = {10: 1.0010, 20: 1.0008}
        prices = build_prices_df(companies, dates)
        universe = build_universe_df([
            (10, "A1", "Alpha", "Alpha-1"),
            (20, "B1", "Beta",  "Beta-1"),
        ])
        config = BacktestConfig(
            start_date=BACKTEST_START,
            end_date=BACKTEST_END,
            signal_weights=equal_signal_weights(),
            top_n_sectors=2,
            top_n_per_sector=1,
        )
        result = run_backtest(config, prices, universe)
        for rec in result.monthly_records:
            assert all(h.side == "long" for h in rec.holdings)
