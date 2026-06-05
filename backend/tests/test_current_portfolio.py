"""Integration coverage for `run_current_portfolio` — the "current picks"
month-to-date engine that backs /schedule's daily snapshots.

This path had NO unit coverage (only HTTP-level CI smoke), which made
refactoring its 430-line body risky. These tests drive it on the same
synthetic monotonic-price universe the backtest suite uses, pinning the
month-start holdings (selection, weights, MTD sign, the per-holding fields the
holding-builder populates) and the daily-picks panel — so the holding-builder
extraction below them is verified end-to-end, not just structurally.
"""
from __future__ import annotations

from datetime import date

import pytest

import pandas as pd

from momentum.backtest import BacktestConfig
from momentum.backtest.current_portfolio import _build_holding, run_current_portfolio

from tests._backtest_helpers import (
    PRICE_HISTORY_START,
    PRICES_END,
    build_prices_df,
    build_universe_df,
    calendar_daily,
    equal_signal_weights,
)

# A mid-month "today" inside the synthetic price range → current month is
# March 2025, whose first-Monday rebalance is 2025-03-03.
_TODAY = date(2025, 3, 15)
_REBALANCE = "2025-03-03"


def _run(top_n_per_sector: int = 2):
    dates = calendar_daily(PRICE_HISTORY_START, PRICES_END)
    companies = {
        10: 1.0012,  # Alpha — strongest
        11: 1.0010,  # Alpha
        12: 1.0001,  # Alpha — weakest
        20: 1.0011,  # Beta — strongest
        21: 1.0009,  # Beta
        22: 1.0002,  # Beta — weakest
    }
    prices = build_prices_df(companies, dates)
    universe = build_universe_df([
        (10, "A1", "Alpha", "Alpha-1"),
        (11, "A2", "Alpha", "Alpha-2"),
        (12, "A3", "Alpha", "Alpha-3"),
        (20, "B1", "Beta", "Beta-1"),
        (21, "B2", "Beta", "Beta-2"),
        (22, "B3", "Beta", "Beta-3"),
    ])
    config = BacktestConfig(
        start_date=date(2025, 1, 1),
        end_date=_TODAY,
        signal_weights=equal_signal_weights(),
        top_n_sectors=2,
        top_n_per_sector=top_n_per_sector,
    )
    return run_current_portfolio(config, prices, universe, today=_TODAY)


class TestMonthStartHoldings:
    def test_anchors_to_first_monday_rebalance(self):
        cp = _run()
        assert cp.as_of_date == _REBALANCE
        assert cp.latest_price_date is not None

    def test_selects_top_two_per_sector_equal_weight(self):
        cp = _run(top_n_per_sector=2)
        ids = {h.company_id for h in cp.holdings}
        assert ids == {10, 11, 20, 21}  # the weakest in each sector dropped
        assert all(h.weight == pytest.approx(0.25) for h in cp.holdings)
        assert sum(h.weight for h in cp.holdings) == pytest.approx(1.0)

    def test_holding_fields_populated(self):
        cp = _run()
        by_id = {h.company_id: h for h in cp.holdings}
        h = by_id[10]
        # Identity + score/rank fields the holding-builder fills.
        assert h.ticker == "A1"
        assert h.company_name == "Alpha-1"
        assert h.sector == "Alpha"
        assert h.score >= 0.0
        assert set(h.category_scores.keys()) >= {"price"}
        assert h.sector_rank == 1  # strongest in Alpha
        # EUR prices come from the (only) price series; exit is the latest close.
        assert h.entry_price_eur is not None
        assert h.exit_price_eur is not None
        # Monotonic-up prices → positive MTD from the rebalance entry.
        assert h.forward_return_pct is not None and h.forward_return_pct > 0

    def test_top_one_per_sector_picks_only_strongest(self):
        cp = _run(top_n_per_sector=1)
        assert {h.company_id for h in cp.holdings} == {10, 20}
        assert all(h.weight == pytest.approx(0.5) for h in cp.holdings)


class TestDailyPicks:
    def test_daily_picks_cover_the_month_to_date(self):
        cp = _run()
        assert cp.daily_picks, "expected at least one daily pick"
        # Every daily snapshot lands on/after the rebalance and on/before today.
        for dp in cp.daily_picks:
            assert _REBALANCE <= dp.date <= _TODAY.isoformat()
        # Each day's picks are the same equal-weighted selection shape.
        last = cp.daily_picks[-1]
        assert {h.company_id for h in last.holdings} == {10, 11, 20, 21}
        assert all(h.entry_price_eur is not None for h in last.holdings)

    def test_daily_picks_are_chronological(self):
        cp = _run()
        dates = [dp.date for dp in cp.daily_picks]
        assert dates == sorted(dates)


class TestBuildHolding:
    """Pure holding-builder shared by the month-start + daily loops."""

    def _row(self, **over):
        base = {
            "company_id": 7, "gurufocus_ticker": "XYZ", "company_name": "Xyz Co",
            "sector": "Tech", "momentum_score": 88.312,
            "sector_rank": 2.0, "company_rank": 5.0, "score_price": 71.27,
        }
        base.update(over)
        return pd.Series(base)

    def test_rounds_and_maps_fields(self):
        h = _build_holding(
            self._row(), weight=0.25, currency="USD",
            entry_price_eur=1.234567, exit_price_eur=2.0,
            entry_price_local=9.87654, exit_price_local=None,
            entry_date="2025-03-03", exit_date=None, forward_return_pct=4.2,
        )
        assert h.company_id == 7 and h.ticker == "XYZ" and h.sector == "Tech"
        assert h.score == 88.31              # momentum_score rounded to 2dp
        assert h.weight == 0.25 and h.currency == "USD"
        assert h.entry_price_eur == 1.2346   # rounded to 4dp
        assert h.exit_price_local is None    # None passes through
        assert h.entry_price_local == 9.8765
        assert h.sector_rank == 2 and h.company_rank == 5  # float → int
        assert h.forward_return_pct == 4.2
        assert h.category_scores.get("price") == 71.3

    def test_nan_ranks_and_score_become_none_or_zero(self):
        h = _build_holding(
            self._row(momentum_score=float("nan"), sector_rank=float("nan"),
                      company_rank=float("nan")),
            weight=0.5, currency=None,
            entry_price_eur=None, exit_price_eur=None,
            entry_price_local=None, exit_price_local=None,
            entry_date=None, exit_date=None, forward_return_pct=None,
        )
        assert h.score == 0.0            # NaN momentum_score → 0.0
        assert h.sector_rank is None     # NaN rank → None
        assert h.company_rank is None
        assert h.entry_price_eur is None
