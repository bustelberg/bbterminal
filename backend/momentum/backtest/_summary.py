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
    # Universe (equal-weight-everything) baseline. Chain-linked the
    # same way as the strategy's cumulative_factor, over closed
    # periods only. Drives `universe_total_return_pct` on the summary
    # so the user can compare strategy vs. universe at a glance.
    universe_cumulative_factor: float = 1.0
    universe_period_returns: list[float] = None
    # Daily universe baseline curve — same shape as the strategy's
    # `daily_records`. The runner appends one entry per trading day
    # inside each period using `_compute_universe_period_daily`,
    # chain-linked via `universe_daily_factor`. Open periods feed the
    # display tail but don't bump the cumulative factor (matching the
    # per-period chain so headline universe stats stay closed-only).
    universe_daily_records: list[tuple[str, float]] = None
    universe_daily_factor: float = 1.0

    def __post_init__(self):
        if self.all_period_returns is None:
            self.all_period_returns = []
        if self.turnover_values is None:
            self.turnover_values = []
        if self.holdings_counts is None:
            self.holdings_counts = []
        if self.universe_period_returns is None:
            self.universe_period_returns = []
        if self.universe_daily_records is None:
            self.universe_daily_records = []


def build_backtest_result(
    period_records: list[PeriodRecord],
    accumulators: _PeriodAccumulators,
    *,
    price_index: dict[int, pd.Series],
    strategy_type: StrategyType,
    benchmark_price_index: dict[int, pd.Series] | None,
    rebalance_frequency,
    monthly_baseline: dict | None = None,
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
    sortino = None
    if len(closed_daily_returns) >= 21:
        arr = np.array(closed_daily_returns)
        d_mean = float(arr.mean())
        d_std = float(arr.std())
        if d_std > 0:
            sharpe = round((d_mean / d_std) * (252 ** 0.5), 2)
        # Sortino — same period mean over downside-only std.
        downside = arr[arr < 0]
        if len(downside) > 1:
            downside_std = float(downside.std())
            if downside_std > 0:
                sortino = round((d_mean / downside_std) * (252 ** 0.5), 2)
    elif len(accumulators.all_period_returns) >= int(_periods_per_year(rebalance_frequency)):
        arr = np.array(accumulators.all_period_returns)
        period_mean = float(arr.mean())
        period_std = float(arr.std())
        if period_std > 0:
            sharpe = round((period_mean / period_std) * (_periods_per_year(rebalance_frequency) ** 0.5), 2)
        downside = arr[arr < 0]
        if len(downside) > 1:
            downside_std = float(downside.std())
            if downside_std > 0:
                sortino = round((period_mean / downside_std) * (_periods_per_year(rebalance_frequency) ** 0.5), 2)

    # Win rate + median month return are BOTH computed on calendar-month
    # returns regardless of rebalance cadence. A daily-rebalance variant
    # otherwise reported "win rate = 51%" (per-day) next to a yearly
    # variant's "win rate = 80%" (per-year), and a sweep comparing the
    # two looked like the yearly was crushing the daily even when their
    # daily curves were identical. Resampling the closed daily equity
    # curve to month-end and chaining month-over-month puts every
    # variant on the same per-calendar-month scale.
    monthly_returns_pct: list[float] = []
    if closed_curve:
        # Last cum value seen in each calendar month is the month-end
        # equity. Preserves insertion order so the chain stays
        # chronological.
        month_last_factor: dict[str, float] = {}
        month_order: list[str] = []
        for d, cum in closed_curve:
            m = d[:7]  # "YYYY-MM"
            if m not in month_last_factor:
                month_order.append(m)
            month_last_factor[m] = 1.0 + cum / 100.0
        if len(month_order) >= 2:
            prev_factor = month_last_factor[month_order[0]]
            for m in month_order[1:]:
                cur = month_last_factor[m]
                if prev_factor > 0:
                    monthly_returns_pct.append((cur / prev_factor - 1.0) * 100.0)
                prev_factor = cur

    win_rate_pct: float | None = None
    median_period_return_pct: float | None = None
    if monthly_returns_pct:
        m_arr = np.array(monthly_returns_pct)
        wins = int((m_arr > 0).sum())
        total = int(m_arr.size)
        if total > 0:
            win_rate_pct = round(100.0 * wins / total, 2)
        median_period_return_pct = round(float(np.median(m_arr)), 2)
    elif accumulators.all_period_returns:
        # Degenerate-curve fallback: a daily curve wasn't available
        # (closed_curve empty) but per-period returns exist. Use those
        # so the fields aren't nulled out for legacy paths that never
        # built a daily curve. Tracks rebalance cadence in this branch
        # — the user-facing tooltip on the column makes that explicit.
        pr_arr = np.array(accumulators.all_period_returns)
        wins = int((pr_arr > 0).sum())
        total = int(pr_arr.size)
        if total > 0:
            win_rate_pct = round(100.0 * wins / total, 2)
        median_period_return_pct = round(float(np.median(pr_arr)), 2)

    # Universe headline. Prefer the cadence-INDEPENDENT monthly
    # baseline when one was supplied by `run_backtest` (the typical
    # path when a universe is selected) so two variants on the same
    # universe + window get IDENTICAL headline universe stats — the
    # column is then a property of the universe, not of the variant's
    # rebalance cadence. Falls back to the per-period chain
    # (`universe_cumulative_factor` walking closed strategy periods)
    # when the monthly baseline is unavailable — e.g. no universe
    # selected, or degenerate-window runs where no calendar month
    # produced usable returns.
    if monthly_baseline is not None:
        universe_total = monthly_baseline.get("total_pct")
        universe_annualized = monthly_baseline.get("annualized_pct")
    elif accumulators.universe_period_returns:
        universe_total = round((accumulators.universe_cumulative_factor - 1) * 100, 2)
        universe_annualized = (
            round((accumulators.universe_cumulative_factor ** (1 / n_years) - 1) * 100, 2)
            if n_years > 0 else 0.0
        )
    else:
        universe_total = None
        universe_annualized = None

    summary = BacktestSummary(
        total_return_pct=total_return,
        annualized_return_pct=annualized,
        max_drawdown_pct=round(max_dd, 2),
        sharpe_ratio=sharpe,
        sortino_ratio=sortino,
        win_rate_pct=win_rate_pct,
        median_period_return_pct=median_period_return_pct,
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
        universe_total_return_pct=universe_total,
        universe_annualized_return_pct=universe_annualized,
    )

    # Apples-to-apples date alignment for the universe baseline. The
    # strategy's daily curve unions trading days across its handful of
    # holdings; the universe's daily curve unions across every eligible
    # cid in the panel, which spans more exchanges → more union dates.
    # Without resampling, two side effects show up in the chart:
    #   1. The "Periods" column on the universe row exceeds the
    #      strategy's by 10-15% (more dates seen).
    #   2. `periodsPerYear` in the frontend's points-derived stats
    #      (`alignSeries`) scales up correspondingly, inflating the
    #      universe's Sharpe by √(period_count_ratio).
    # Filtering universe entries to the strategy's date set fixes both
    # at once. Falls back to the native universe dates when the
    # strategy has no curve (degenerate runs) so the user still sees a
    # baseline line.
    strategy_date_set = {d for d, _ in daily_curve}
    universe_daily_aligned = (
        [(d, v) for d, v in accumulators.universe_daily_records if d in strategy_date_set]
        if strategy_date_set
        else list(accumulators.universe_daily_records)
    )

    return BacktestResult(
        monthly_records=period_records,
        summary=summary,
        daily_records=daily_curve,
        universe_daily_records=universe_daily_aligned,
    )
