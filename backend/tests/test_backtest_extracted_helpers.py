"""Pure helpers extracted from the backtest god-functions during the
runner/_period/scoring decomposition.

The full loops are covered end-to-end by the `test_backtest_*` suite; these
pin the edge cases the extraction made independently testable — the
closed-vs-open chain-link accounting, the one-sided long-short return
fallbacks, and the NaN/missing handling in the category-score builder.
"""
from __future__ import annotations

import pandas as pd

from momentum.backtest._period import _aggregate_portfolio_return
from momentum.backtest._summary import _PeriodAccumulators
from momentum.backtest.runner import (
    _chain_strategy_return,
    _chain_universe_baseline,
    _early_empty_record,
)
from momentum.scoring import extract_category_scores


class TestChainStrategyReturn:
    def test_closed_period_advances_accumulator(self):
        a = _PeriodAccumulators()
        cum = _chain_strategy_return(a, 10.0, is_open_iter=False)
        assert a.cumulative_factor == 1.1
        assert a.all_period_returns == [10.0]
        assert cum == a.cumulative

    def test_open_period_does_not_mutate(self):
        a = _PeriodAccumulators()
        a.cumulative_factor = 1.2
        a.cumulative = 20.0
        cum = _chain_strategy_return(a, 5.0, is_open_iter=True)
        assert a.cumulative_factor == 1.2  # untouched
        assert a.all_period_returns == []
        assert round(cum, 4) == round((1.2 * 1.05 - 1) * 100, 4)

    def test_none_return_leaves_curve_flat(self):
        a = _PeriodAccumulators()
        a.cumulative = 7.5
        assert _chain_strategy_return(a, None, is_open_iter=False) == 7.5
        assert a.all_period_returns == []


class TestChainUniverseBaseline:
    def test_none_returns_none(self):
        a = _PeriodAccumulators()
        assert _chain_universe_baseline(a, None, is_open_iter=False) is None

    def test_closed_bumps_factor_and_records(self):
        a = _PeriodAccumulators()
        out = _chain_universe_baseline(a, 25.0, is_open_iter=False)
        assert a.universe_cumulative_factor == 1.25
        assert a.universe_period_returns == [25.0]
        assert round(out, 4) == 25.0

    def test_open_is_display_only(self):
        a = _PeriodAccumulators()
        a.universe_cumulative_factor = 1.1
        out = _chain_universe_baseline(a, 10.0, is_open_iter=True)
        assert a.universe_cumulative_factor == 1.1  # untouched
        assert a.universe_period_returns == []
        assert round(out, 4) == round((1.1 * 1.1 - 1) * 100, 4)


class TestEarlyEmptyRecord:
    def test_shape(self):
        from datetime import date
        rec = _early_empty_record(date(2024, 5, 6), 3.14159, "no data", is_open=True)
        assert rec.date == "2024-05-06"
        assert rec.holdings == []
        assert rec.portfolio_return_pct is None
        assert rec.cumulative_return_pct == 3.14  # rounded to 2dp
        assert rec.empty_reason == "no data"
        assert rec.is_open is True


class TestAggregatePortfolioReturn:
    def test_long_only_is_mean(self):
        assert _aggregate_portfolio_return([10.0, 20.0], [], "long_only") == 15.0

    def test_long_only_empty_is_none(self):
        assert _aggregate_portfolio_return([], [], "long_only") is None

    def test_long_short_is_long_minus_short(self):
        # mean(long)=15, mean(short)=-5 → 15 − (−5) = 20
        assert _aggregate_portfolio_return([10.0, 20.0], [-10.0, 0.0], "long_short") == 20.0

    def test_long_short_missing_short_falls_back_to_long(self):
        assert _aggregate_portfolio_return([4.0, 6.0], [], "long_short") == 5.0

    def test_long_short_missing_long_falls_back_to_negated_short(self):
        assert _aggregate_portfolio_return([], [8.0, 2.0], "long_short") == -5.0

    def test_long_short_both_empty_is_none(self):
        assert _aggregate_portfolio_return([], [], "long_short") is None


class TestExtractCategoryScores:
    def test_rounds_present_and_nones_missing(self):
        row = pd.Series({"score_price": 87.36, "score_volume": float("nan")})
        out = extract_category_scores(row)
        assert out["price"] == 87.4   # rounded to 1dp
        assert out["volume"] is None  # NaN → None

    def test_absent_column_is_none(self):
        row = pd.Series({"score_price": 50.0})  # no score_volume column
        out = extract_category_scores(row)
        assert out["price"] == 50.0
        assert out["volume"] is None
