"""Momentum backtest engine — split into submodules for readability.

External callers (`routers.momentum.backtest_stream`,
`routers.momentum.signals`, `scripts.profile_current_portfolio`,
`tests.test_backtest`) keep importing from `momentum.backtest`, so this
`__init__.py` re-exports every name those modules touched when the engine
lived in one file.

Layout:
  types.py             — type aliases, dataclasses, sector aliases, _periods_per_year
  dates.py             — _generate_rebalance_dates + helpers
  indices.py           — price/volume index builders + as-of price lookups
  equity_curve.py      — daily curve reconstruction + drawdown detection
  preparation.py       — _prepare_backtest + shared-inputs sweep optimization
  runner.py            — run_backtest + run_multi_trial_backtest
  current_portfolio.py — run_current_portfolio
"""
from __future__ import annotations

from .types import (
    BacktestConfig,
    BacktestResult,
    BacktestSummary,
    CurrentPortfolio,
    DailyPick,
    DrawdownPeriod,
    HoldingSide,
    PeriodHolding,
    PeriodRecord,
    RebalanceFrequency,
    StrategyType,
    _DEFAULT_FREQUENCY,
    _DEFAULT_STRATEGY,
    _norm_sector,
    _periods_per_year,
    _SECTOR_ALIASES,
)
from .dates import _generate_month_starts, _generate_rebalance_dates
from .indices import (
    _build_price_index,
    _build_volume_index,
    _date_on_or_after,
    _price_on_or_after,
    _price_on_or_before,
)
from .equity_curve import (
    _build_daily_equity_curve,
    _find_drawdown_periods,
    _pick_top_n_non_overlapping,
)
from .preparation import (
    _BacktestPrepared,
    _SharedBacktestInputs,
    _prepare_backtest,
    build_shared_backtest_inputs,
    prepare_variant_from_shared,
)
from .runner import run_backtest, run_multi_trial_backtest
from .current_portfolio import run_current_portfolio

__all__ = [
    # types
    "BacktestConfig", "BacktestResult", "BacktestSummary", "CurrentPortfolio",
    "DailyPick", "DrawdownPeriod", "HoldingSide", "PeriodHolding", "PeriodRecord",
    "RebalanceFrequency", "StrategyType",
    # dates
    "_generate_rebalance_dates",
    # indices
    "_build_price_index", "_build_volume_index",
    "_price_on_or_after", "_date_on_or_after", "_price_on_or_before",
    # equity curve
    "_build_daily_equity_curve", "_find_drawdown_periods", "_pick_top_n_non_overlapping",
    # preparation
    "_BacktestPrepared", "_SharedBacktestInputs",
    "_prepare_backtest", "build_shared_backtest_inputs", "prepare_variant_from_shared",
    # runner
    "run_backtest", "run_multi_trial_backtest",
    # current portfolio
    "run_current_portfolio",
    # helpers / constants
    "_periods_per_year", "_norm_sector", "_SECTOR_ALIASES",
    "_DEFAULT_FREQUENCY", "_DEFAULT_STRATEGY",
]
