"""Headline-stats computation for `run_backtest`.

After the per-period loop finishes, we have `period_records` plus a few
accumulators (`cumulative`, `cumulative_factor`, `all_period_returns`,
`turnover_values`, `holdings_counts`). This module folds those into the
final `BacktestResult` — daily equity curve, max drawdown (peak-to-
trough-to-recovery), annualized return, Sharpe, and the summary
dataclass.

Headline stats use CLOSED periods only so an open period's partial-
window data doesn't bias the numbers. The daily curve published for the
chart still includes the open period — that's the line the user sees
through "today". `closed_curve` is the same chain truncated at the last
closed period; it's what feeds Sharpe + Max DD."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from .equity_curve import (
    _build_daily_equity_curve,
    _find_drawdown_periods,
    _pick_top_n_non_overlapping,
)
from .types import (
    BacktestResult,
    BacktestSummary,
    PeriodRecord,
    StrategyType,
    _periods_per_year,
)


@dataclass
class _PeriodAccumulators:
    """Mutable state the main loop threads through each period iteration."""
    cumulative: float = 0.0
    cumulative_factor: float = 1.0
    all_period_returns: list[float] = None
    turnover_values: list[float] = None
    holdings_counts: list[int] = None

    def __post_init__(self):
        if self.all_period_returns is None:
            self.all_period_returns = []
        if self.turnover_values is None:
            self.turnover_values = []
        if self.holdings_counts is None:
            self.holdings_counts = []


def build_backtest_result(
    period_records: list[PeriodRecord],
    accumulators: _PeriodAccumulators,
    *,
    price_index: dict[int, pd.Series],
    strategy_type: StrategyType,
    benchmark_price_index: dict[int, pd.Series] | None,
    rebalance_frequency,
) -> BacktestResult:
    """Build the final `BacktestResult` from the per-period state."""
    closed_records = [r for r in period_records if not r.is_open]
    daily_curve, _daily_returns_full = _build_daily_equity_curve(
        period_records, price_index, strategy_type,
        benchmark_price_index=benchmark_price_index,
    )
    closed_curve, closed_daily_returns = _build_daily_equity_curve(
        closed_records, price_index, strategy_type,
        benchmark_price_index=benchmark_price_index,
    )
    # `total_return` and `annualized_return_pct` are intentionally both
    # derived from the period-chain `cumulative_factor`. The daily curve
    # shadows the same growth path on average but diverges on the margin
    # — period chain excludes holdings whose forward_return_pct is None
    # (missing exit price), while the daily curve carries them through
    # with a stale `asof()` price. Over thousands of daily rebalances
    # those edge-case ratios accumulate, and the two end-of-backtest
    # values disagree by more than rounding (the symptom: 37% annualized
    # next to a 66,000% total return — math says one of those is wrong).
    # Pinning both to the period chain keeps the headline numbers
    # internally consistent. The daily curve is still the source for
    # max-drawdown + Sharpe (those need intra-period detail) and for
    # the chart line.
    total_return = round(accumulators.cumulative, 2)
    if closed_curve:
        first_date = date.fromisoformat(closed_curve[0][0])
        last_date = date.fromisoformat(closed_curve[-1][0])
        n_years = max(0.0, (last_date - first_date).days / 365.25)
    else:
        n_years = (
            len(accumulators.all_period_returns) / _periods_per_year(rebalance_frequency)
            if accumulators.all_period_returns else 0
        )
    annualized = (
        round((accumulators.cumulative_factor ** (1 / n_years) - 1) * 100, 2)
        if n_years > 0 else 0
    )

    # Identify all drawdown periods (peak-to-trough-to-recovery) on the
    # closed-period daily curve. Open period is excluded so a still-running
    # mid-drawdown doesn't fold into the historical max DD.
    if closed_curve:
        values = [(d, 1 + cum / 100) for d, cum in closed_curve]
    else:
        values = [(r.date, 1 + r.cumulative_return_pct / 100) for r in closed_records]
    all_drawdown_periods = _find_drawdown_periods(values)
    top_drawdowns = _pick_top_n_non_overlapping(all_drawdown_periods, 3)
    max_dd = top_drawdowns[0].drawdown_pct if top_drawdowns else 0.0

    # Sharpe — annualized from daily returns × √252 when we have at least a
    # month of trading days. Falls back to the old period-frequency formula
    # when the daily curve is unavailable (e.g. degenerate runs). Uses
    # closed-period daily returns only (open period's partial-window
    # samples would understate volatility).
    sharpe = None
    if len(closed_daily_returns) >= 21:
        arr = np.array(closed_daily_returns)
        d_mean = float(arr.mean())
        d_std = float(arr.std())
        if d_std > 0:
            sharpe = round((d_mean / d_std) * (252 ** 0.5), 2)
    elif len(accumulators.all_period_returns) >= int(_periods_per_year(rebalance_frequency)):
        arr = np.array(accumulators.all_period_returns)
        period_mean = float(arr.mean())
        period_std = float(arr.std())
        if period_std > 0:
            sharpe = round((period_mean / period_std) * (_periods_per_year(rebalance_frequency) ** 0.5), 2)

    summary = BacktestSummary(
        total_return_pct=total_return,
        annualized_return_pct=annualized,
        max_drawdown_pct=round(max_dd, 2),
        sharpe_ratio=sharpe,
        avg_monthly_turnover_pct=(
            round(float(np.mean(accumulators.turnover_values)), 2)
            if accumulators.turnover_values else 0
        ),
        total_months=len(accumulators.all_period_returns),
        avg_holdings=(
            round(float(np.mean(accumulators.holdings_counts)), 1)
            if accumulators.holdings_counts else 0
        ),
        top_drawdowns=top_drawdowns,
    )

    return BacktestResult(
        monthly_records=period_records,
        summary=summary,
        daily_records=daily_curve,
    )
