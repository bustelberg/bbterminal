"""Momentum backtest engine. Supports multiple rebalance frequencies."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Callable, Literal

import numpy as np
import pandas as pd

from .signals import PRICE_SIGNAL_DEFS, compute_signals_panel
from .scoring import score_and_select, random_select, _get_category_keys

_logger = logging.getLogger(__name__)


# Rebalance cadence. New variants get added here + to _periods_per_year +
# _generate_rebalance_dates. The default "monthly" preserves the original
# month-start, hold-1-month behavior; the others stride differently.
RebalanceFrequency = Literal[
    "daily", "weekly", "monthly",
    "every_2_months", "every_3_months", "every_4_months", "every_5_months",
    "every_6_months", "every_7_months", "every_8_months", "every_9_months",
    "every_10_months", "every_11_months", "every_12_months",
]

_DEFAULT_FREQUENCY: RebalanceFrequency = "monthly"


# Long-only = pick top sectors / top names per sector and equal-weight long them.
# Long-short = also pick bottom sectors / bottom names and short them at 100%
# gross on each side (200% gross, 0% net). Period return is then
# mean(long_returns) − mean(short_returns).
StrategyType = Literal["long_only", "long_short"]

_DEFAULT_STRATEGY: StrategyType = "long_only"

# A holding's `side` decides whether its forward return contributes positively
# or negatively to the period return. Long-only backtests emit only "long"
# rows; long-short emits both, dedupe-collisions removed.
HoldingSide = Literal["long", "short"]


def _periods_per_year(freq: RebalanceFrequency) -> float:
    """Approximate number of rebalance periods per calendar year.

    Used to (a) annualize total return, (b) scale Sharpe by √(periods/yr).
    Daily uses 252 trading days; weekly uses 52; calendar-month variants
    use 12 / 6 / 4. These are nominal — actual generated dates may differ
    slightly when prices_df is short of trading days at boundaries.
    """
    return {
        "daily": 252.0,
        "weekly": 52.0,
        "monthly": 12.0,
        "every_2_months": 6.0,
        "every_3_months": 4.0,
        "every_4_months": 3.0,
        "every_5_months": 12.0 / 5.0,
        "every_6_months": 2.0,
        "every_7_months": 12.0 / 7.0,
        "every_8_months": 12.0 / 8.0,
        "every_9_months": 12.0 / 9.0,
        "every_10_months": 12.0 / 10.0,
        "every_11_months": 12.0 / 11.0,
        "every_12_months": 1.0,
    }[freq]


@dataclass
class BacktestConfig:
    start_date: date
    end_date: date
    signal_weights: dict[str, float]
    top_n_sectors: int = 4
    top_n_per_sector: int = 6
    category_weights: dict[str, float] | None = None  # e.g. {"price": 0.5, "volume": 0.5}
    selection_mode: str = "momentum"  # "momentum" or "random"
    random_seed: int | None = None  # only used when selection_mode == "random"
    rebalance_frequency: RebalanceFrequency = _DEFAULT_FREQUENCY
    strategy_type: StrategyType = _DEFAULT_STRATEGY

    @classmethod
    def from_dict(cls, d: dict) -> BacktestConfig:
        return cls(
            start_date=date.fromisoformat(d["start_date"]),
            end_date=date.fromisoformat(d["end_date"]),
            signal_weights=d.get("signal_weights", {s["key"]: s["default_weight"] for s in PRICE_SIGNAL_DEFS}),
            top_n_sectors=d.get("top_n_sectors", 4),
            top_n_per_sector=d.get("top_n_per_sector", 6),
            category_weights=d.get("category_weights"),
            selection_mode=d.get("selection_mode", "momentum"),
            random_seed=d.get("random_seed"),
            rebalance_frequency=d.get("rebalance_frequency", _DEFAULT_FREQUENCY),
            strategy_type=d.get("strategy_type", _DEFAULT_STRATEGY),
        )


@dataclass
class PeriodHolding:
    company_id: int
    ticker: str
    company_name: str
    sector: str
    score: float
    category_scores: dict[str, float | None]  # e.g. {"price": 72.5, "volume": 45.1}
    weight: float
    forward_return_pct: float | None
    currency: str | None = None
    entry_price_local: float | None = None
    exit_price_local: float | None = None
    entry_price_eur: float | None = None
    exit_price_eur: float | None = None
    entry_date: str | None = None
    exit_date: str | None = None
    # "long" or "short". forward_return_pct is always the underlying price
    # return; the period-level aggregator uses `side` to decide the sign of
    # the contribution. Long-only backtests emit "long" everywhere.
    side: HoldingSide = "long"


@dataclass
class PeriodRecord:
    date: str  # YYYY-MM
    holdings: list[PeriodHolding]
    portfolio_return_pct: float | None
    cumulative_return_pct: float
    empty_reason: str | None = None


@dataclass
class DrawdownPeriod:
    """A peak-to-trough drawdown period."""
    drawdown_pct: float
    peak_date: str      # YYYY-MM
    trough_date: str    # YYYY-MM
    recovery_date: str | None  # YYYY-MM or None if not yet recovered


@dataclass
class BacktestSummary:
    total_return_pct: float
    annualized_return_pct: float
    max_drawdown_pct: float
    sharpe_ratio: float | None
    avg_monthly_turnover_pct: float
    total_months: int
    avg_holdings: float
    top_drawdowns: list[DrawdownPeriod] = field(default_factory=list)
    # Populated only when the backtest aggregates across multiple trials
    # (random-baseline mode with n_trials > 1). All headline stats above
    # are means in that case; these fields hold the cross-trial std-dev.
    n_trials: int | None = None
    total_return_pct_std: float | None = None
    annualized_return_pct_std: float | None = None
    max_drawdown_pct_std: float | None = None
    sharpe_ratio_std: float | None = None
    avg_monthly_turnover_pct_std: float | None = None


@dataclass
class DailyPick:
    """One trading day's worth of hypothetical picks + turnover vs the previous
    day. Two return numbers per day:
      - portfolio_return_pct: chain-linked cumulative MTD through this day,
        formed by multiplying each day's equal-weighted close-to-close return
        across rebalances.
      - next_day_return_pct: equal-weighted close-to-close return of *this*
        day's portfolio held until the next trading day. NULL on the latest
        day in the panel (no next day yet)."""
    date: str                              # YYYY-MM-DD
    holdings: list[PeriodHolding]
    turnover_abs: int                      # number of holdings that differ from previous day
    turnover_pct: float                    # turnover_abs / max(len(today), len(prev)) * 100
    portfolio_return_pct: float | None = None
    next_day_return_pct: float | None = None


@dataclass
class CurrentPortfolio:
    """Snapshot of what the strategy would hold right now (rebalance on
    the first of the current month, MTD return through the latest price)."""
    as_of_date: str       # YYYY-MM-01 — start of current month
    latest_price_date: str | None  # most recent price date observed across the portfolio
    holdings: list[PeriodHolding]
    daily_picks: list[DailyPick] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "as_of_date": self.as_of_date,
            "latest_price_date": self.latest_price_date,
            "holdings": [
                {
                    "company_id": h.company_id,
                    "ticker": h.ticker,
                    "company_name": h.company_name,
                    "sector": h.sector,
                    "score": h.score,
                    "category_scores": h.category_scores,
                    "weight": round(h.weight, 4),
                    "forward_return_pct": h.forward_return_pct,
                    "currency": h.currency,
                    "entry_price_local": h.entry_price_local,
                    "exit_price_local": h.exit_price_local,
                    "entry_price_eur": h.entry_price_eur,
                    "exit_price_eur": h.exit_price_eur,
                    "entry_date": h.entry_date,
                    "exit_date": h.exit_date,
                }
                for h in self.holdings
            ],
            "daily_picks": [
                {
                    "date": d.date,
                    "turnover_abs": d.turnover_abs,
                    "turnover_pct": d.turnover_pct,
                    "portfolio_return_pct": d.portfolio_return_pct,
                    "next_day_return_pct": d.next_day_return_pct,
                    "holdings": [
                        {
                            "company_id": h.company_id,
                            "ticker": h.ticker,
                            "company_name": h.company_name,
                            "sector": h.sector,
                            "score": h.score,
                            "category_scores": h.category_scores,
                            "weight": round(h.weight, 4),
                            "forward_return_pct": h.forward_return_pct,
                            "currency": h.currency,
                            "entry_price_local": h.entry_price_local,
                            "exit_price_local": h.exit_price_local,
                            "entry_price_eur": h.entry_price_eur,
                            "exit_price_eur": h.exit_price_eur,
                            "entry_date": h.entry_date,
                            "exit_date": h.exit_date,
                        }
                        for h in d.holdings
                    ],
                }
                for d in self.daily_picks
            ],
        }


@dataclass
class BacktestResult:
    monthly_records: list[PeriodRecord]
    summary: BacktestSummary
    # Daily portfolio equity curve, chain-linked across rebalance periods.
    # Each entry: (YYYY-MM-DD, cumulative_return_pct). Empty for degenerate
    # runs with no holdings; the frontend falls back to the period curve in
    # that case.
    daily_records: list[tuple[str, float]] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "daily_records": [
                {"date": d, "cumulative_return_pct": cum}
                for d, cum in self.daily_records
            ],
            "monthly_records": [
                {
                    "date": r.date,
                    "holdings": [
                        {
                            "company_id": h.company_id,
                            "ticker": h.ticker,
                            "company_name": h.company_name,
                            "sector": h.sector,
                            "score": h.score,
                            "category_scores": h.category_scores,
                            "weight": round(h.weight, 4),
                            "forward_return_pct": h.forward_return_pct,
                            "currency": h.currency,
                            "entry_price_local": h.entry_price_local,
                            "exit_price_local": h.exit_price_local,
                            "entry_price_eur": h.entry_price_eur,
                            "exit_price_eur": h.exit_price_eur,
                            "entry_date": h.entry_date,
                            "exit_date": h.exit_date,
                            "side": h.side,
                        }
                        for h in r.holdings
                    ],
                    "portfolio_return_pct": r.portfolio_return_pct,
                    "cumulative_return_pct": r.cumulative_return_pct,
                    **({"empty_reason": r.empty_reason} if r.empty_reason else {}),
                }
                for r in self.monthly_records
            ],
            "summary": {
                "total_return_pct": self.summary.total_return_pct,
                "annualized_return_pct": self.summary.annualized_return_pct,
                "max_drawdown_pct": self.summary.max_drawdown_pct,
                "sharpe_ratio": self.summary.sharpe_ratio,
                "avg_monthly_turnover_pct": self.summary.avg_monthly_turnover_pct,
                "total_months": self.summary.total_months,
                "avg_holdings": self.summary.avg_holdings,
                "top_drawdowns": [
                    {
                        "drawdown_pct": dd.drawdown_pct,
                        "peak_date": dd.peak_date,
                        "trough_date": dd.trough_date,
                        "recovery_date": dd.recovery_date,
                    }
                    for dd in self.summary.top_drawdowns
                ],
                "n_trials": self.summary.n_trials,
                "total_return_pct_std": self.summary.total_return_pct_std,
                "annualized_return_pct_std": self.summary.annualized_return_pct_std,
                "max_drawdown_pct_std": self.summary.max_drawdown_pct_std,
                "sharpe_ratio_std": self.summary.sharpe_ratio_std,
                "avg_monthly_turnover_pct_std": self.summary.avg_monthly_turnover_pct_std,
            },
        }


def _generate_month_starts(start: date, end: date) -> list[date]:
    """Generate first-of-month dates between start and end."""
    months = []
    current = date(start.year, start.month, 1)
    end_limit = date(end.year, end.month, 1)
    while current <= end_limit:
        months.append(current)
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return months


def _generate_rebalance_dates(
    start: date,
    end: date,
    freq: RebalanceFrequency,
    prices_df: pd.DataFrame | None = None,
) -> list[date]:
    """Generate rebalance dates for `freq` between [start, end].

    For calendar-stride variants (monthly / 2m / 3m), produces every Nth
    first-of-month date and is independent of prices_df — _price_on_or_after
    walks the company's series to the next available trading day at entry.

    For weekly, produces every Monday in range — actual entry still falls
    on the first available trading day on/after that Monday via
    _price_on_or_after.

    For daily, requires prices_df to identify the actual set of trading days
    in range (the union across all companies). Without prices_df we have no
    calendar to use, so we'd produce Mon-Fri sequences that include
    market holidays.
    """
    if freq == "monthly":
        return _generate_month_starts(start, end)
    # every_N_months → take every N-th month-start. Adding new strides only
    # needs an entry in the map below + the Literal at the top.
    _MONTH_STRIDES = {
        "every_2_months": 2,
        "every_3_months": 3,
        "every_4_months": 4,
        "every_5_months": 5,
        "every_6_months": 6,
        "every_7_months": 7,
        "every_8_months": 8,
        "every_9_months": 9,
        "every_10_months": 10,
        "every_11_months": 11,
        "every_12_months": 12,
    }
    if freq in _MONTH_STRIDES:
        return _generate_month_starts(start, end)[::_MONTH_STRIDES[freq]]
    if freq == "weekly":
        # Every Monday in range. weekday(): Mon=0..Sun=6.
        days_until_mon = (-start.weekday()) % 7
        first_mon = start + timedelta(days=days_until_mon)
        out: list[date] = []
        d = first_mon
        while d <= end:
            out.append(d)
            d += timedelta(days=7)
        return out
    if freq == "daily":
        if prices_df is None or prices_df.empty:
            raise ValueError("daily frequency requires prices_df to identify trading days")
        all_dates = pd.to_datetime(prices_df["target_date"]).dt.date.unique()
        return sorted(d for d in all_dates if start <= d <= end)
    raise ValueError(f"Unknown rebalance frequency: {freq}")


def _build_price_index(prices_df: pd.DataFrame) -> dict[int, pd.Series]:
    """Pre-index prices into a dict of {company_id: Series(price, DatetimeIndex)}.

    One-time cost that eliminates repeated DataFrame filtering.
    """
    result: dict[int, pd.Series] = {}
    for cid, group in prices_df.groupby("company_id"):
        s = pd.Series(
            group["price"].values,
            index=pd.DatetimeIndex(group["target_date"]),
            dtype="float64",
        ).sort_index()
        result[int(cid)] = s
    return result


def _price_on_or_after(series: pd.Series, target: pd.Timestamp) -> float | None:
    """Get the first available price on or after target date from a pre-indexed Series."""
    subset = series[series.index >= target]
    if subset.empty:
        return None
    return float(subset.iloc[0])


def _build_daily_equity_curve(
    period_records: list["PeriodRecord"],
    price_index: dict[int, pd.Series],
    strategy_type: "StrategyType",
) -> tuple[list[tuple[str, float]], list[float]]:
    """Reconstruct a daily portfolio equity curve from the period-level
    holdings.

    Within each period the portfolio's daily relative value is
        long-only:    mean over long-holdings of (price[t] / entry_price)
        long-short:   1 + mean(long_price/entry) − mean(short_price/entry)
    where each holding's `entry_price_eur` is the price it actually entered
    at and `price[t]` is the latest-available EUR close on or before day t.
    Periods are chain-linked so the curve is continuous across rebalances.

    This produces stats (max DD, Sharpe) that respect intra-period moves —
    a monthly strategy that's flat at month-end after a -15% mid-month
    drawdown now reports that drawdown, where the period-level curve
    masked it.

    Returns:
        daily_records: [(YYYY-MM-DD, cumulative_return_pct), …]
        daily_returns: day-over-day arithmetic returns (for Sharpe).
    """
    daily_dates: list[date] = []
    daily_factors: list[float] = []
    cumulative_factor = 1.0  # carries across periods

    for pr in period_records:
        if not pr.holdings:
            continue
        long_h = [h for h in pr.holdings if h.side == "long" and h.entry_price_eur not in (None, 0)]
        short_h = [h for h in pr.holdings if h.side == "short" and h.entry_price_eur not in (None, 0)]
        if not long_h and not short_h:
            continue

        # Period window from the holdings' actual trading dates.
        entry_iso = [h.entry_date for h in pr.holdings if h.entry_date]
        exit_iso = [h.exit_date for h in pr.holdings if h.exit_date]
        if not entry_iso or not exit_iso:
            continue
        period_start = pd.Timestamp(min(entry_iso))
        period_end = pd.Timestamp(max(exit_iso))

        # Union of trading days across all holdings inside the window.
        # exit_date is exclusive — it's the next period's entry day, the
        # close that day belongs to the next period's run.
        all_days_set: set[pd.Timestamp] = set()
        for h in pr.holdings:
            s = price_index.get(h.company_id)
            if s is None:
                continue
            # Inclusive on both bounds: the exit day is the day we sell at,
            # and its price IS the period's realized close. Excluding it
            # left daily-rebalance periods with zero days inside the
            # window (each period was [T, T+1), so only T survived; that
            # day's factor is always 1.0 since entry_price == price[T],
            # producing the flat-line bug). Inclusive bounds duplicate the
            # boundary date in adjacent periods, but the values are equal
            # by chain-link construction so the chart is unaffected.
            mask = (s.index >= period_start) & (s.index <= period_end)
            all_days_set.update(s.index[mask].tolist())
        if not all_days_set:
            continue
        sorted_days = sorted(all_days_set)

        for day in sorted_days:
            long_vals: list[float] = []
            short_vals: list[float] = []
            for h in long_h:
                s = price_index.get(h.company_id)
                if s is None:
                    continue
                # `asof` is O(log n) vs the O(n) boolean-mask slice; on a
                # 6-year monthly run this is the difference between ~22s
                # of curve construction and well under a second.
                v = s.asof(day)
                if pd.isna(v):
                    continue
                long_vals.append(float(v) / h.entry_price_eur)
            for h in short_h:
                s = price_index.get(h.company_id)
                if s is None:
                    continue
                v = s.asof(day)
                if pd.isna(v):
                    continue
                short_vals.append(float(v) / h.entry_price_eur)

            long_avg = sum(long_vals) / len(long_vals) if long_vals else 1.0
            short_avg = sum(short_vals) / len(short_vals) if short_vals else 1.0
            if strategy_type == "long_short":
                period_relative = 1.0 + (long_avg - 1.0) - (short_avg - 1.0)
            else:
                period_relative = long_avg

            daily_factor = cumulative_factor * period_relative
            daily_dates.append(day.date())
            daily_factors.append(daily_factor)

        if daily_factors:
            # Chain-link: next period starts where this one finishes.
            cumulative_factor = daily_factors[-1]

    daily_records = [
        (d.isoformat(), round((f - 1) * 100, 4))
        for d, f in zip(daily_dates, daily_factors)
    ]
    daily_returns: list[float] = []
    for i in range(1, len(daily_factors)):
        prev = daily_factors[i - 1]
        if prev > 0:
            daily_returns.append(daily_factors[i] / prev - 1)
    return daily_records, daily_returns


def _date_on_or_after(series: pd.Series, target: pd.Timestamp) -> str | None:
    """Get the actual trading date of the first price on or after target."""
    idx = series.index[series.index >= target]
    if len(idx) == 0:
        return None
    return idx[0].strftime("%Y-%m-%d")


def _price_on_or_before(series: pd.Series, target: pd.Timestamp) -> tuple[float, pd.Timestamp] | None:
    """Get (price, trading-date) of the latest available price on or before target."""
    subset = series[series.index <= target]
    if subset.empty:
        return None
    return float(subset.iloc[-1]), subset.index[-1]


def _build_volume_index(volumes_df: pd.DataFrame) -> dict[int, pd.Series]:
    """Build a dict of {company_id: Series} from the volumes DataFrame."""
    if volumes_df.empty:
        return {}
    result: dict[int, pd.Series] = {}
    for cid, group in volumes_df.groupby("company_id"):
        s = pd.Series(
            group["volume"].values,
            index=pd.DatetimeIndex(group["target_date"]),
            dtype="float64",
        ).sort_index()
        result[int(cid)] = s
    return result


def _find_drawdown_periods(values: list[tuple[str, float]]) -> list[DrawdownPeriod]:
    """Find all drawdown periods from a list of (date, portfolio_value) tuples.

    A drawdown starts when value drops below a peak and ends when the value
    recovers back to the peak level (or at the end of the series).
    """
    if len(values) < 2:
        return []

    periods: list[DrawdownPeriod] = []
    peak_val = values[0][1]
    peak_date = values[0][0]
    trough_val = peak_val
    trough_date = peak_date
    in_drawdown = False

    for dt, val in values[1:]:
        if val >= peak_val:
            # Recovered or new high
            if in_drawdown:
                dd_pct = round((trough_val / peak_val - 1) * 100, 2)
                periods.append(DrawdownPeriod(
                    drawdown_pct=dd_pct,
                    peak_date=peak_date,
                    trough_date=trough_date,
                    recovery_date=dt,
                ))
                in_drawdown = False
            peak_val = val
            peak_date = dt
            trough_val = val
            trough_date = dt
        else:
            in_drawdown = True
            if val < trough_val:
                trough_val = val
                trough_date = dt

    # Handle ongoing drawdown at end of series
    if in_drawdown:
        dd_pct = round((trough_val / peak_val - 1) * 100, 2)
        periods.append(DrawdownPeriod(
            drawdown_pct=dd_pct,
            peak_date=peak_date,
            trough_date=trough_date,
            recovery_date=None,
        ))

    return periods


def _pick_top_n_non_overlapping(periods: list[DrawdownPeriod], n: int) -> list[DrawdownPeriod]:
    """Pick the top N drawdowns by magnitude, excluding overlapping periods.

    A period overlaps if its peak-to-recovery range intersects with any
    already-selected period's peak-to-recovery range.
    """
    # Sort by drawdown magnitude (most negative first)
    sorted_periods = sorted(periods, key=lambda p: p.drawdown_pct)
    selected: list[DrawdownPeriod] = []

    for p in sorted_periods:
        if len(selected) >= n:
            break
        # Check overlap with already selected
        p_end = p.recovery_date or "9999-99"
        overlaps = False
        for s in selected:
            s_end = s.recovery_date or "9999-99"
            # Two ranges [p.peak, p_end] and [s.peak, s_end] overlap if:
            if p.peak_date <= s_end and p_end >= s.peak_date:
                overlaps = True
                break
        if not overlaps:
            selected.append(p)

    return selected


@dataclass
class _BacktestPrepared:
    """Precomputed inputs that depend only on dates / prices / universe — not
    on the selection RNG. Cached and reused across trials by
    `run_multi_trial_backtest` so the (expensive) signal panel is built once
    rather than N times.

    `periods` is the rebalance-date list (was named `months` when only
    monthly was supported). Length must be ≥ 2 — first N-1 are entry dates,
    last is the final exit date.
    """
    periods: list[date]
    price_index: dict[int, pd.Series]
    local_price_index: dict[int, pd.Series] | None
    volume_index: dict[int, pd.Series] | None
    panel: dict[date, pd.DataFrame]
    frequency: RebalanceFrequency


def _prepare_backtest(
    *,
    start_date: date,
    end_date: date,
    prices_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    volumes_df: pd.DataFrame | None,
    prices_local_df: pd.DataFrame | None,
    monthly_eligible: dict[str, dict[int, str | None]] | None,
    frequency: RebalanceFrequency = _DEFAULT_FREQUENCY,
) -> _BacktestPrepared:
    periods = _generate_rebalance_dates(start_date, end_date, frequency, prices_df)
    if len(periods) < 2:
        raise ValueError(f"Need at least 2 rebalance periods for a {frequency} backtest (got {len(periods)})")

    price_index = _build_price_index(prices_df)
    local_price_index = (
        _build_price_index(prices_local_df)
        if prices_local_df is not None and not prices_local_df.empty
        else None
    )
    volume_index = (
        _build_volume_index(volumes_df)
        if volumes_df is not None and not volumes_df.empty
        else None
    )

    cutoff_dates = periods[:-1]  # last period has no forward return → no signals needed
    if monthly_eligible is not None:
        panel_universe_ids: set[int] = set()
        for month_dict in monthly_eligible.values():
            panel_universe_ids.update(month_dict.keys())
        panel_universe_df = (
            universe_df[universe_df["company_id"].isin(panel_universe_ids)]
            .reset_index(drop=True)
        )
    else:
        panel_universe_df = universe_df

    panel = compute_signals_panel(
        panel_universe_df,
        cutoff_dates,
        price_index=price_index,
        volume_index=volume_index,
    )

    return _BacktestPrepared(
        periods=periods,
        price_index=price_index,
        local_price_index=local_price_index,
        volume_index=volume_index,
        panel=panel,
        frequency=frequency,
    )


@dataclass
class _SharedBacktestInputs:
    """The portion of a backtest's setup that's identical across every
    variant in a sweep — price/volume indices and a single signal panel
    built over the *union* of all variants' cutoff dates. Each variant
    then takes a sliced view of this panel via `_prepare_variant_from_shared`.

    Building the per-company rolling signal panels is the dominant cost
    in `_prepare_backtest` (it scans each company's full price history
    once); per-cutoff lookups are cheap searchsorted ops. Rolling those
    builds into a single call cuts ~N-1 redundant scans for an N-variant
    sweep, which is the difference between a 14-variant sweep paying
    ~14×10s = 140s of panel construction vs ~10s.
    """
    price_index: dict[int, pd.Series]
    local_price_index: dict[int, pd.Series] | None
    volume_index: dict[int, pd.Series] | None
    panel_universe_df: pd.DataFrame
    union_panel: dict[date, pd.DataFrame]


def build_shared_backtest_inputs(
    *,
    prices_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    volumes_df: pd.DataFrame | None,
    prices_local_df: pd.DataFrame | None,
    monthly_eligible: dict[str, dict[int, str | None]] | None,
    union_cutoffs: list[date],
) -> _SharedBacktestInputs:
    """Build the shared portion of `_BacktestPrepared` once for a sweep.
    `union_cutoffs` should contain every cutoff date any variant in the
    sweep will need (i.e. union of `_generate_rebalance_dates(...)[:-1]`
    across variants). Cutoff order doesn't matter for the panel; the
    function dedupes + sorts internally."""
    price_index = _build_price_index(prices_df)
    local_price_index = (
        _build_price_index(prices_local_df)
        if prices_local_df is not None and not prices_local_df.empty
        else None
    )
    volume_index = (
        _build_volume_index(volumes_df)
        if volumes_df is not None and not volumes_df.empty
        else None
    )
    if monthly_eligible is not None:
        panel_universe_ids: set[int] = set()
        for month_dict in monthly_eligible.values():
            panel_universe_ids.update(month_dict.keys())
        panel_universe_df = (
            universe_df[universe_df["company_id"].isin(panel_universe_ids)]
            .reset_index(drop=True)
        )
    else:
        panel_universe_df = universe_df

    deduped_cutoffs = sorted(set(union_cutoffs))
    union_panel = compute_signals_panel(
        panel_universe_df,
        deduped_cutoffs,
        price_index=price_index,
        volume_index=volume_index,
    )
    return _SharedBacktestInputs(
        price_index=price_index,
        local_price_index=local_price_index,
        volume_index=volume_index,
        panel_universe_df=panel_universe_df,
        union_panel=union_panel,
    )


def prepare_variant_from_shared(
    *,
    shared: _SharedBacktestInputs,
    start_date: date,
    end_date: date,
    frequency: RebalanceFrequency,
    prices_df: pd.DataFrame,
) -> _BacktestPrepared:
    """Build a `_BacktestPrepared` for one variant from sweep-shared
    inputs. The signal panel is filtered to just this variant's cutoffs;
    indices are reused as-is. Use this in place of `_prepare_backtest`
    when you've already called `build_shared_backtest_inputs` for the
    sweep — gives byte-identical results, just without re-doing the
    expensive per-company rolling panel scan."""
    periods = _generate_rebalance_dates(start_date, end_date, frequency, prices_df)
    if len(periods) < 2:
        raise ValueError(f"Need at least 2 rebalance periods for a {frequency} backtest (got {len(periods)})")
    cutoff_set = set(periods[:-1])
    sliced_panel = {d: df for d, df in shared.union_panel.items() if d in cutoff_set}
    return _BacktestPrepared(
        periods=periods,
        price_index=shared.price_index,
        local_price_index=shared.local_price_index,
        volume_index=shared.volume_index,
        panel=sliced_panel,
        frequency=frequency,
    )


def run_backtest(
    config: BacktestConfig,
    prices_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    send_event: Callable[..., Any] | None = None,
    *,
    volumes_df: pd.DataFrame | None = None,
    monthly_eligible: dict[str, dict[int, str | None]] | None = None,
    prices_local_df: pd.DataFrame | None = None,
    company_currency: dict[int, str | None] | None = None,
    prepared: _BacktestPrepared | None = None,
) -> BacktestResult:
    """Run a momentum backtest at the configured rebalance cadence.

    For each rebalance period:
    1. Compute price and volume signals using data strictly before the period
    2. Score and select top companies
    3. Compute forward return through the next rebalance date
    4. Track cumulative portfolio return

    If monthly_eligible is provided (from universe_snapshot), only companies
    in the eligible set for that month are considered. The eligibility table
    is keyed by YYYY-MM regardless of cadence — sub-monthly periods
    inherit the snapshot of the month they fall in.

    `prepared` is an internal optimization for `run_multi_trial_backtest`:
    when supplied, the periods / indices / signal panel are reused instead
    of being recomputed. None means compute fresh.
    """
    # Random selection has no meaningful interpretation for long-short — a
    # randomly-picked short bucket is just noise on top of a randomly-picked
    # long bucket, with no signal-driven structure. Catch it loudly here
    # instead of silently producing nonsense.
    if config.strategy_type == "long_short" and config.selection_mode == "random":
        raise ValueError("long_short strategy is incompatible with random selection mode")
    # `all` selection holds every eligible name in the universe — there's
    # no top/bottom split to drive a long-short book either.
    if config.strategy_type == "long_short" and config.selection_mode == "all":
        raise ValueError("long_short strategy is incompatible with 'all' selection mode")

    if prepared is None:
        prepared = _prepare_backtest(
            start_date=config.start_date,
            end_date=config.end_date,
            prices_df=prices_df,
            universe_df=universe_df,
            volumes_df=volumes_df,
            prices_local_df=prices_local_df,
            monthly_eligible=monthly_eligible,
            frequency=config.rebalance_frequency,
        )
    periods = prepared.periods
    price_index = prepared.price_index
    local_price_index = prepared.local_price_index
    # volume_index isn't used directly here — it was already incorporated
    # into the precomputed signal panel during _prepare_backtest.
    panel = prepared.panel
    # Sub-monthly variants need full YYYY-MM-DD on each record so the UI can
    # disambiguate same-month rows. Monthly/2m/3m keep "YYYY-MM" so saved
    # results, the cache, and existing frontend charts stay backward-compatible.
    sub_monthly = prepared.frequency in ("daily", "weekly")

    def _record_date(d: date) -> str:
        return d.isoformat() if sub_monthly else d.isoformat()[:7]

    def _record_label(d: date) -> str:
        return d.isoformat() if sub_monthly else d.strftime("%b %Y")

    period_records: list[PeriodRecord] = []
    cumulative = 0.0  # cumulative return in %
    cumulative_factor = 1.0  # multiplicative
    prev_holdings_set: set[int] = set()
    all_period_returns: list[float] = []
    turnover_values: list[float] = []
    holdings_counts: list[int] = []

    # Random selector RNG: seeded once per backtest so re-runs with the same
    # seed produce identical picks across all periods.
    rng = (
        np.random.default_rng(config.random_seed)
        if config.selection_mode == "random"
        else None
    )

    for i, period_date in enumerate(periods[:-1]):  # last period has no forward return
        next_period = periods[i + 1]

        if send_event:
            pct = round((i / (len(periods) - 1)) * 100)
            send_event(
                "progress",
                month=_record_date(period_date),
                pct=pct,
                message=f"Computing signals for {_record_label(period_date)}...",
            )

        # Resolve this period's eligible set + sector map (snapshot-based universes only).
        sector_map: dict[int, str | None] = {}
        eligible_ids: set[int] | None = None
        if monthly_eligible is not None:
            month_key = period_date.isoformat()[:7]
            sector_map = monthly_eligible.get(month_key) or {}
            eligible_ids = set(sector_map.keys())
            if not eligible_ids:
                snap_min = min(monthly_eligible.keys())
                snap_max = max(monthly_eligible.keys())
                if month_key < snap_min or month_key > snap_max:
                    reason = f"Month is outside universe snapshot range ({snap_min} to {snap_max})"
                else:
                    reason = "All companies in the universe snapshot failed screening criteria for this month (0 passing)"
                if send_event:
                    send_event(
                        "warning",
                        scope="universe",
                        message=f"{_record_label(period_date)}: {reason}",
                    )
                period_records.append(PeriodRecord(
                    date=_record_date(period_date),
                    holdings=[],
                    portfolio_return_pct=None,
                    cumulative_return_pct=round(cumulative, 2),
                    empty_reason=reason,
                ))
                continue

        # Look up signals for this period from the precomputed panel, then
        # apply the per-month universe filter + sector remap when using a
        # snapshot-based universe (the panel was built from the base
        # `universe_df` whose sector is None for snapshot universes).
        signals_df = panel.get(period_date, pd.DataFrame())
        if not signals_df.empty and eligible_ids is not None:
            signals_df = signals_df[signals_df["company_id"].isin(eligible_ids)].copy()
            signals_df["sector"] = signals_df["company_id"].map(sector_map)
        if signals_df.empty:
            reason = f"No companies had enough price data (need >= 20 data points before {_record_label(period_date)})"
            if send_event:
                send_event(
                    "progress",
                    month=_record_date(period_date),
                    pct=pct,
                    message=f"{_record_label(period_date)}: 0 holdings — {reason}",
                )
                send_event(
                    "warning",
                    scope="backtest",
                    message=f"{_record_label(period_date)}: {reason}",
                )
            period_records.append(PeriodRecord(
                date=_record_date(period_date),
                holdings=[],
                portfolio_return_pct=None,
                cumulative_return_pct=round(cumulative, 2),
                empty_reason=reason,
            ))
            continue

        # Select longs (always) and shorts (long-short only). For random
        # mode there's only ever one bucket — `selected_top` — and shorts
        # stay empty.
        if config.selection_mode == "all":
            # "Hold the whole universe" baseline — every eligible name in
            # the period equally weighted. Useful as a market-cap-naive
            # index proxy and as a control when comparing against
            # signal-driven selections. top_n_sectors / top_n_per_sector
            # are deliberately ignored: the whole point is no filtering.
            selected_top = signals_df.copy().reset_index(drop=True)
            selected_bottom = pd.DataFrame()
        elif rng is not None:
            selected_top = random_select(
                signals_df,
                top_n_sectors=config.top_n_sectors,
                top_n_per_sector=config.top_n_per_sector,
                rng=rng,
            )
            selected_bottom = pd.DataFrame()
        else:
            selected_top = score_and_select(
                signals_df,
                config.signal_weights,
                top_n_sectors=config.top_n_sectors,
                top_n_per_sector=config.top_n_per_sector,
                category_weights=config.category_weights,
                direction="top",
            )
            if config.strategy_type == "long_short":
                selected_bottom = score_and_select(
                    signals_df,
                    config.signal_weights,
                    top_n_sectors=config.top_n_sectors,
                    top_n_per_sector=config.top_n_per_sector,
                    category_weights=config.category_weights,
                    direction="bottom",
                )
                # If a name lands in both books (small universe / overlapping
                # sector sets), drop it from both. The intent is "go long the
                # best and short the worst" — keeping a name on both sides
                # is just self-cancellation that distorts the gross-200%
                # weight math.
                if not selected_bottom.empty and not selected_top.empty:
                    top_ids = set(selected_top["company_id"].astype(int))
                    bot_ids = set(selected_bottom["company_id"].astype(int))
                    collisions = top_ids & bot_ids
                    if collisions:
                        selected_top = selected_top[
                            ~selected_top["company_id"].isin(collisions)
                        ].reset_index(drop=True)
                        selected_bottom = selected_bottom[
                            ~selected_bottom["company_id"].isin(collisions)
                        ].reset_index(drop=True)
                        if send_event:
                            send_event(
                                "warning",
                                scope="backtest",
                                message=(
                                    f"{_record_label(period_date)}: dropped "
                                    f"{len(collisions)} name(s) appearing on both long and short books"
                                ),
                            )
            else:
                selected_bottom = pd.DataFrame()

        if selected_top.empty and selected_bottom.empty:
            n_signals = len(signals_df)
            sectors = signals_df["sector"].nunique() if "sector" in signals_df.columns else 0
            reason = f"{n_signals} companies had signals across {sectors} sectors but none passed selection (top_n_sectors={config.top_n_sectors}, top_n_per_sector={config.top_n_per_sector})"
            if send_event:
                send_event(
                    "progress",
                    month=_record_date(period_date),
                    pct=pct,
                    message=f"{_record_label(period_date)}: 0 holdings — {reason}",
                )
                send_event(
                    "warning",
                    scope="backtest",
                    message=f"{_record_label(period_date)}: {reason}",
                )
            period_records.append(PeriodRecord(
                date=_record_date(period_date),
                holdings=[],
                portfolio_return_pct=None,
                cumulative_return_pct=round(cumulative, 2),
                empty_reason=reason,
            ))
            continue

        # Equal weight per side. For long-only the short bucket is empty so
        # the long book sums to 1.0 (100% gross long). For long-short each
        # side sums to 1.0 independently → 200% gross, 0% net.
        n_long = len(selected_top)
        n_short = len(selected_bottom)
        long_weight = 1.0 / n_long if n_long > 0 else 0.0
        short_weight = 1.0 / n_short if n_short > 0 else 0.0
        holdings_counts.append(n_long + n_short)

        entry_ts = pd.Timestamp(period_date)
        exit_ts = pd.Timestamp(next_period)

        # Closure that fills in price lookups + per-category scores for one
        # selected row. Returns the holding plus the price return so the
        # caller can route it into the long or short bucket.
        def _make_holding(row: pd.Series, side: HoldingSide, w: float) -> tuple[PeriodHolding, float | None]:
            cid = int(row["company_id"])
            series = price_index.get(cid)
            entry_price = _price_on_or_after(series, entry_ts) if series is not None else None
            exit_price = _price_on_or_after(series, exit_ts) if series is not None else None
            fwd_return: float | None = None
            if entry_price and exit_price and entry_price > 0:
                fwd_return = round((exit_price / entry_price - 1) * 100, 2)

            local_series = local_price_index.get(cid) if local_price_index is not None else None
            entry_local = _price_on_or_after(local_series, entry_ts) if local_series is not None else None
            exit_local = _price_on_or_after(local_series, exit_ts) if local_series is not None else None

            # Actual trading dates (prefer local series, fall back to EUR series).
            date_series = local_series if local_series is not None else series
            entry_dt = _date_on_or_after(date_series, entry_ts) if date_series is not None else None
            exit_dt = _date_on_or_after(date_series, exit_ts) if date_series is not None else None

            cat_scores: dict[str, float | None] = {}
            for cat in _get_category_keys():
                col = f"score_{cat}"
                if col in row.index and pd.notna(row[col]):
                    cat_scores[cat] = round(float(row[col]), 1)
                else:
                    cat_scores[cat] = None

            score_val = row.get("momentum_score")
            return PeriodHolding(
                company_id=cid,
                ticker=str(row.get("gurufocus_ticker", "")),
                company_name=str(row.get("company_name", "")),
                sector=str(row["sector"]),
                score=round(float(score_val), 2) if pd.notna(score_val) else 0.0,
                category_scores=cat_scores,
                weight=w,
                forward_return_pct=fwd_return,
                currency=(company_currency or {}).get(cid),
                entry_price_local=round(entry_local, 4) if entry_local is not None else None,
                exit_price_local=round(exit_local, 4) if exit_local is not None else None,
                entry_price_eur=round(entry_price, 4) if entry_price is not None else None,
                exit_price_eur=round(exit_price, 4) if exit_price is not None else None,
                entry_date=entry_dt,
                exit_date=exit_dt,
                side=side,
            ), fwd_return

        holdings: list[PeriodHolding] = []
        long_returns: list[float] = []
        short_returns: list[float] = []
        for _, row in selected_top.iterrows():
            h, ret = _make_holding(row, "long", long_weight)
            holdings.append(h)
            if ret is not None:
                long_returns.append(ret)
        for _, row in selected_bottom.iterrows():
            h, ret = _make_holding(row, "short", short_weight)
            holdings.append(h)
            if ret is not None:
                short_returns.append(ret)

        # Portfolio return:
        #   long-only: equal-weighted mean of long returns.
        #   long-short (gross 100% long + 100% short): mean(long) − mean(short).
        # If a side is empty (degenerate period), fall back to whatever is
        # available — the strategy temporarily becomes one-sided.
        if config.strategy_type == "long_short":
            long_avg = float(np.mean(long_returns)) if long_returns else None
            short_avg = float(np.mean(short_returns)) if short_returns else None
            if long_avg is not None and short_avg is not None:
                port_return = round(long_avg - short_avg, 2)
            elif long_avg is not None:
                port_return = round(long_avg, 2)
            elif short_avg is not None:
                port_return = round(-short_avg, 2)
            else:
                port_return = None
        else:
            port_return = round(float(np.mean(long_returns)), 2) if long_returns else None

        if port_return is not None:
            cumulative_factor *= (1 + port_return / 100)
            cumulative = (cumulative_factor - 1) * 100
            all_period_returns.append(port_return)

        # Turnover
        current_set = {h.company_id for h in holdings}
        if prev_holdings_set:
            overlap = len(current_set & prev_holdings_set)
            total = max(len(current_set), len(prev_holdings_set))
            turnover = round((1 - overlap / total) * 100, 2) if total > 0 else 0
            turnover_values.append(turnover)
        prev_holdings_set = current_set

        period_records.append(PeriodRecord(
            date=_record_date(period_date),
            holdings=holdings,
            portfolio_return_pct=port_return,
            cumulative_return_pct=round(cumulative, 2),
        ))

    # Build a daily equity curve from the period holdings. Drawdown +
    # Sharpe are computed against this rather than the period-end curve so
    # intra-period moves (a -15% week mid-month that recovers by month-end)
    # are visible. Annualization uses the daily-based curve too — the
    # period-level total_return is preserved as a sanity check but the
    # headline stats now reflect actual daily volatility.
    daily_curve, daily_returns = _build_daily_equity_curve(
        period_records, price_index, config.strategy_type,
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
    total_return = round(cumulative, 2)
    if daily_curve:
        first_date = date.fromisoformat(daily_curve[0][0])
        last_date = date.fromisoformat(daily_curve[-1][0])
        n_years = max(0.0, (last_date - first_date).days / 365.25)
    else:
        n_years = len(all_period_returns) / _periods_per_year(prepared.frequency) if all_period_returns else 0
    annualized = round((cumulative_factor ** (1 / n_years) - 1) * 100, 2) if n_years > 0 else 0

    # Identify all drawdown periods (peak-to-trough-to-recovery) on the
    # daily curve when available; fall back to the period curve otherwise.
    if daily_curve:
        values = [(d, 1 + cum / 100) for d, cum in daily_curve]
    else:
        values = [(r.date, 1 + r.cumulative_return_pct / 100) for r in period_records]
    all_drawdown_periods = _find_drawdown_periods(values)
    top_drawdowns = _pick_top_n_non_overlapping(all_drawdown_periods, 3)
    max_dd = top_drawdowns[0].drawdown_pct if top_drawdowns else 0.0

    # Sharpe — annualized from daily returns × √252 when we have at least a
    # month of trading days. Falls back to the old period-frequency formula
    # when the daily curve is unavailable (e.g. degenerate runs).
    sharpe = None
    if len(daily_returns) >= 21:
        arr = np.array(daily_returns)
        d_mean = float(arr.mean())
        d_std = float(arr.std())
        if d_std > 0:
            sharpe = round((d_mean / d_std) * (252 ** 0.5), 2)
    elif len(all_period_returns) >= int(_periods_per_year(prepared.frequency)):
        arr = np.array(all_period_returns)
        period_mean = float(arr.mean())
        period_std = float(arr.std())
        if period_std > 0:
            sharpe = round((period_mean / period_std) * (_periods_per_year(prepared.frequency) ** 0.5), 2)

    summary = BacktestSummary(
        total_return_pct=total_return,
        annualized_return_pct=annualized,
        max_drawdown_pct=round(max_dd, 2),
        sharpe_ratio=sharpe,
        avg_monthly_turnover_pct=round(float(np.mean(turnover_values)), 2) if turnover_values else 0,
        total_months=len(all_period_returns),
        avg_holdings=round(float(np.mean(holdings_counts)), 1) if holdings_counts else 0,
        top_drawdowns=top_drawdowns,
    )

    if send_event:
        send_event("progress", month="done", pct=100, message="Backtest complete")

    return BacktestResult(
        monthly_records=period_records,
        summary=summary,
        daily_records=daily_curve,
    )


def run_multi_trial_backtest(
    config: BacktestConfig,
    prices_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    n_trials: int,
    send_event: Callable[..., Any] | None = None,
    *,
    volumes_df: pd.DataFrame | None = None,
    monthly_eligible: dict[str, dict[int, str | None]] | None = None,
    prices_local_df: pd.DataFrame | None = None,
    company_currency: dict[int, str | None] | None = None,
) -> BacktestResult:
    """Run `n_trials` independent backtests with sequential seeds and return
    an aggregated BacktestResult.

    Headline summary stats are means across trials; *_std fields hold the
    cross-trial standard deviation. The equity curve (cumulative_return_pct
    on each PeriodRecord) is the per-month mean across trials. Holdings
    on each PeriodRecord come from the first trial — they're random
    anyway, so aggregating them isn't meaningful.

    Forces selection_mode="random". Caller controls the base seed via
    config.random_seed; trial seeds are base, base+1, ..., base+N-1.
    """
    if n_trials < 1:
        raise ValueError("n_trials must be >= 1")
    if config.selection_mode != "random":
        raise ValueError("run_multi_trial_backtest requires selection_mode='random'")

    base_seed = config.random_seed if config.random_seed is not None else 0

    # Build the price/volume indices and signal panel once — they only depend
    # on dates / prices / universe, none of which change across random trials.
    # This turns N-trial wall time from O(N × panel) into O(panel + N × select).
    if send_event and n_trials > 1:
        send_event(
            "progress",
            month="prepare",
            pct=0,
            message=f"Precomputing signals for {n_trials} trials...",
        )
    prepared = _prepare_backtest(
        start_date=config.start_date,
        end_date=config.end_date,
        prices_df=prices_df,
        universe_df=universe_df,
        volumes_df=volumes_df,
        prices_local_df=prices_local_df,
        monthly_eligible=monthly_eligible,
        frequency=config.rebalance_frequency,
    )

    trial_results: list[BacktestResult] = []
    for i in range(n_trials):
        if send_event:
            pct = round((i / n_trials) * 100)
            send_event(
                "progress",
                month=f"trial-{i + 1}",
                pct=pct,
                message=f"Trial {i + 1}/{n_trials} (seed={base_seed + i})...",
            )
        trial_config = BacktestConfig(
            start_date=config.start_date,
            end_date=config.end_date,
            signal_weights=config.signal_weights,
            top_n_sectors=config.top_n_sectors,
            top_n_per_sector=config.top_n_per_sector,
            category_weights=config.category_weights,
            selection_mode="random",
            random_seed=base_seed + i,
            rebalance_frequency=config.rebalance_frequency,
            strategy_type=config.strategy_type,
        )
        # No per-month progress for individual trials — too noisy.
        result = run_backtest(
            trial_config,
            prices_df,
            universe_df,
            send_event=None,
            volumes_df=volumes_df,
            monthly_eligible=monthly_eligible,
            prices_local_df=prices_local_df,
            company_currency=company_currency,
            prepared=prepared,
        )
        trial_results.append(result)

    # Aggregate: per-month mean cumulative return across trials. All trials
    # iterate the same month grid so records align by index.
    n_months = max(len(r.monthly_records) for r in trial_results)
    aggregated_records: list[PeriodRecord] = []
    base_records = trial_results[0].monthly_records  # holdings + dates from trial 0
    for m_idx in range(n_months):
        cum_values = []
        port_returns = []
        for tr in trial_results:
            if m_idx >= len(tr.monthly_records):
                continue
            rec = tr.monthly_records[m_idx]
            cum_values.append(rec.cumulative_return_pct)
            if rec.portfolio_return_pct is not None:
                port_returns.append(rec.portfolio_return_pct)
        if not cum_values:
            continue
        base = base_records[m_idx] if m_idx < len(base_records) else None
        aggregated_records.append(PeriodRecord(
            date=base.date if base else "",
            holdings=base.holdings if base else [],
            portfolio_return_pct=round(float(np.mean(port_returns)), 2) if port_returns else None,
            cumulative_return_pct=round(float(np.mean(cum_values)), 2),
            empty_reason=base.empty_reason if base else None,
        ))

    # Aggregate summary stats across trials.
    def _arr(field: str) -> np.ndarray:
        vals = [getattr(r.summary, field) for r in trial_results if getattr(r.summary, field) is not None]
        return np.array(vals, dtype=float) if vals else np.array([])

    def _mean_std(field: str) -> tuple[float | None, float | None]:
        a = _arr(field)
        if a.size == 0:
            return None, None
        return round(float(a.mean()), 2), round(float(a.std()), 2)

    tr_mean, tr_std = _mean_std("total_return_pct")
    ann_mean, ann_std = _mean_std("annualized_return_pct")
    dd_mean, dd_std = _mean_std("max_drawdown_pct")
    sharpe_mean, sharpe_std = _mean_std("sharpe_ratio")
    turn_mean, turn_std = _mean_std("avg_monthly_turnover_pct")

    # Use trial 0's drawdown periods + total_months + avg_holdings as
    # representative; per-trial std for drawdown periods isn't meaningful.
    base_summary = trial_results[0].summary
    summary = BacktestSummary(
        total_return_pct=tr_mean if tr_mean is not None else 0.0,
        annualized_return_pct=ann_mean if ann_mean is not None else 0.0,
        max_drawdown_pct=dd_mean if dd_mean is not None else 0.0,
        sharpe_ratio=sharpe_mean,
        avg_monthly_turnover_pct=turn_mean if turn_mean is not None else 0.0,
        total_months=base_summary.total_months,
        avg_holdings=base_summary.avg_holdings,
        top_drawdowns=base_summary.top_drawdowns,
        n_trials=n_trials,
        total_return_pct_std=tr_std,
        annualized_return_pct_std=ann_std,
        max_drawdown_pct_std=dd_std,
        sharpe_ratio_std=sharpe_std,
        avg_monthly_turnover_pct_std=turn_std,
    )

    if send_event:
        send_event("progress", month="done", pct=100, message=f"{n_trials} trials complete")

    # Daily curve: use trial 0's, same convention as `holdings` (random
    # trials would otherwise need per-date alignment, and the curves
    # themselves are random anyway). Cross-trial daily mean would be more
    # principled but the multi-trial path is rarely used.
    daily_curve = trial_results[0].daily_records if trial_results else []

    return BacktestResult(
        monthly_records=aggregated_records,
        summary=summary,
        daily_records=daily_curve,
    )


def run_current_portfolio(
    config: BacktestConfig,
    prices_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    send_event: Callable[..., Any] | None = None,
    *,
    volumes_df: pd.DataFrame | None = None,
    monthly_eligible: dict[str, dict[int, str | None]] | None = None,
    prices_local_df: pd.DataFrame | None = None,
    company_currency: dict[int, str | None] | None = None,
    today: date | None = None,
) -> CurrentPortfolio:
    """Compute the strategy's portfolio for the current month with MTD returns.

    Mirrors a single iteration of run_backtest:
      * as_of_date = first of current month
      * signals computed using prices strictly before as_of_date
      * entry_price = first price on/after as_of_date
      * exit_price = LATEST available price (vs run_backtest's next-month price)
      * forward_return_pct field carries the MTD return

    Random selection mode is not supported here — picking randomly for "what
    should I hold today" has no useful interpretation.
    """
    if config.selection_mode == "random":
        raise ValueError("run_current_portfolio does not support random selection mode")

    t_total_start = time.perf_counter()
    today_d = today or date.today()
    month_start = date(today_d.year, today_d.month, 1)
    month_key = month_start.isoformat()[:7]

    if send_event:
        send_event("progress", month=month_key, pct=10, message=f"Computing signals as of {month_start.isoformat()}...")

    # Filter universe for this month if snapshot-based — same logic as the
    # backtest loop, just for one month.
    month_universe_df = universe_df
    if monthly_eligible is not None:
        sector_map = monthly_eligible.get(month_key) or {}
        eligible_ids = set(sector_map.keys())
        if not eligible_ids:
            # Fall back to the most recent snapshot we have, since the
            # current month may not yet be populated in universe_membership.
            available_keys = sorted(monthly_eligible.keys())
            if available_keys:
                fallback_key = available_keys[-1]
                sector_map = monthly_eligible.get(fallback_key) or {}
                eligible_ids = set(sector_map.keys())
                if send_event:
                    send_event(
                        "warning",
                        scope="universe",
                        message=f"No universe snapshot for {month_key}; using latest available ({fallback_key})",
                    )
        if eligible_ids:
            month_universe_df = universe_df[
                universe_df["company_id"].isin(eligible_ids)
            ].copy().reset_index(drop=True)
            month_universe_df["sector"] = month_universe_df["company_id"].map(sector_map)

    # Build price/volume indices once
    price_index = _build_price_index(prices_df)
    local_price_index = (
        _build_price_index(prices_local_df)
        if prices_local_df is not None and not prices_local_df.empty
        else None
    )
    volume_index = _build_volume_index(volumes_df) if volumes_df is not None and not volumes_df.empty else None

    # Trading dates that fall inside the current month, derived from prices_df.
    # Built up front so the signal panel can compute every cutoff in one pass.
    trading_dates_set: set[date] = set()
    for raw_d in prices_df["target_date"].unique():
        if isinstance(raw_d, date) and not isinstance(raw_d, pd.Timestamp):
            dd = raw_d
        elif isinstance(raw_d, str):
            try:
                dd = date.fromisoformat(raw_d[:10])
            except ValueError:
                continue
        else:
            try:
                dd = pd.Timestamp(raw_d).date()
            except Exception:
                continue
        if month_start <= dd <= today_d:
            trading_dates_set.add(dd)
    trading_dates = sorted(trading_dates_set)

    # Single vectorized pass — computes every (company, cutoff) cell up front
    # so the daily loop below is a cheap dict lookup. Includes month_start so
    # the locked-at-start holdings use the same code path.
    t_panel = time.perf_counter()
    panel_cutoffs: list[date] = sorted({month_start, *trading_dates})
    panel = compute_signals_panel(
        month_universe_df, panel_cutoffs,
        price_index=price_index,
        volume_index=volume_index,
    )
    t_panel_elapsed = time.perf_counter() - t_panel

    t_month_start_signals = time.perf_counter()
    signals_df = panel.get(month_start, pd.DataFrame())
    t_month_start_signals_elapsed = time.perf_counter() - t_month_start_signals
    if signals_df.empty:
        if send_event:
            send_event("progress", month=month_key, pct=100, message="No companies had enough data for signals")
        return CurrentPortfolio(as_of_date=month_start.isoformat(), latest_price_date=None, holdings=[])

    if send_event:
        send_event("progress", month=month_key, pct=60, message=f"Scoring {len(signals_df)} companies...")

    # Score and select — same path as backtest momentum mode
    t_month_start_select = time.perf_counter()
    selected = score_and_select(
        signals_df,
        config.signal_weights,
        top_n_sectors=config.top_n_sectors,
        top_n_per_sector=config.top_n_per_sector,
        category_weights=config.category_weights,
    )
    t_month_start_select_elapsed = time.perf_counter() - t_month_start_select

    if selected.empty:
        if send_event:
            send_event("progress", month=month_key, pct=100, message="No companies passed selection")
        return CurrentPortfolio(as_of_date=month_start.isoformat(), latest_price_date=None, holdings=[])

    if send_event:
        send_event("progress", month=month_key, pct=85, message="Computing MTD returns...")

    n_holdings = len(selected)
    weight = 1.0 / n_holdings
    entry_ts = pd.Timestamp(month_start)

    holdings: list[PeriodHolding] = []
    latest_observed: pd.Timestamp | None = None

    for _, row in selected.iterrows():
        cid = int(row["company_id"])
        series = price_index.get(cid)

        entry_price = _price_on_or_after(series, entry_ts) if series is not None else None
        # Exit = latest available price in the EUR series.
        exit_price = float(series.iloc[-1]) if series is not None and len(series) > 0 else None
        exit_dt_ts = series.index[-1] if series is not None and len(series) > 0 else None
        if exit_dt_ts is not None and (latest_observed is None or exit_dt_ts > latest_observed):
            latest_observed = exit_dt_ts

        mtd_return = None
        if entry_price and exit_price and entry_price > 0:
            mtd_return = round((exit_price / entry_price - 1) * 100, 2)

        local_series = local_price_index.get(cid) if local_price_index is not None else None
        entry_local = _price_on_or_after(local_series, entry_ts) if local_series is not None else None
        exit_local = float(local_series.iloc[-1]) if local_series is not None and len(local_series) > 0 else None

        date_series = local_series if local_series is not None else series
        entry_dt = _date_on_or_after(date_series, entry_ts) if date_series is not None else None
        exit_dt = (
            date_series.index[-1].strftime("%Y-%m-%d")
            if date_series is not None and len(date_series) > 0
            else None
        )

        cat_scores: dict[str, float | None] = {}
        for cat in _get_category_keys():
            col = f"score_{cat}"
            if col in row.index and pd.notna(row[col]):
                cat_scores[cat] = round(float(row[col]), 1)
            else:
                cat_scores[cat] = None

        score_val = row.get("momentum_score")
        holdings.append(PeriodHolding(
            company_id=cid,
            ticker=str(row.get("gurufocus_ticker", "")),
            company_name=str(row.get("company_name", "")),
            sector=str(row["sector"]),
            score=round(float(score_val), 2) if pd.notna(score_val) else 0.0,
            category_scores=cat_scores,
            weight=weight,
            forward_return_pct=mtd_return,
            currency=(company_currency or {}).get(cid),
            entry_price_local=round(entry_local, 4) if entry_local is not None else None,
            exit_price_local=round(exit_local, 4) if exit_local is not None else None,
            entry_price_eur=round(entry_price, 4) if entry_price is not None else None,
            exit_price_eur=round(exit_price, 4) if exit_price is not None else None,
            entry_date=entry_dt,
            exit_date=exit_dt,
        ))

    if send_event:
        send_event("progress", month=month_key, pct=85, message=f"{len(holdings)} holdings selected; computing daily picks…")

    # Daily picks: each cutoff already has its signals in `panel` from the
    # single vectorized pass above, so this loop is just per-day score+select
    # and holdings construction.
    daily_picks: list[DailyPick] = []
    prev_ids: set[int] = set()
    # Chain-linked cumulative MTD under the standard pre-rebalance convention:
    # day d's contribution to cum return = the previous day's (pre-rebalance)
    # portfolio held one trading day forward. Day 0 contributes 0% (we just
    # entered). Concretely: today's chain contribution == previous day's
    # next_day_return_pct (the same number, before % conversion). We
    # accumulate that into `cum_factor` and expose `(cum_factor − 1) × 100`
    # as `portfolio_return_pct` on each DailyPick.
    cum_factor = 1.0
    prev_d_ts: pd.Timestamp | None = None
    t_daily_loop_start = time.perf_counter()
    t_daily_signals_total = 0.0
    t_daily_select_total = 0.0
    t_daily_holdings_total = 0.0
    for i, d in enumerate(trading_dates):
        if send_event:
            pct = 85 + round(15 * (i + 1) / max(1, len(trading_dates)))
            send_event("progress", month=month_key, pct=pct, message=f"Daily picks {i + 1}/{len(trading_dates)}: {d.isoformat()}")

        t_signals = time.perf_counter()
        daily_signals = panel.get(d, pd.DataFrame())
        t_daily_signals_total += time.perf_counter() - t_signals
        if daily_signals.empty:
            continue
        t_select = time.perf_counter()
        daily_selected = score_and_select(
            daily_signals,
            config.signal_weights,
            top_n_sectors=config.top_n_sectors,
            top_n_per_sector=config.top_n_per_sector,
            category_weights=config.category_weights,
        )
        t_daily_select_total += time.perf_counter() - t_select
        if daily_selected.empty:
            continue
        t_holdings = time.perf_counter()

        day_ts = pd.Timestamp(d)
        day_weight = 1.0 / len(daily_selected)
        day_holdings: list[PeriodHolding] = []
        today_ids: set[int] = set()

        # Each daily pick is its own 1-day portfolio: bought at THAT day's
        # close, sold at the NEXT trading day's close. Per-stock exit prices
        # and forward_return_pct are filled in on the next iteration once we
        # have tomorrow's prices. The same backfill computes the prior day's
        # next_day_return_pct (= chain-link contribution to cumulative MTD).
        prior_one_day_return: float | None = None
        if daily_picks and prev_d_ts is not None:
            prev_pick = daily_picks[-1]
            forward_components: list[float] = []
            for h in prev_pick.holdings:
                series = price_index.get(h.company_id)
                if series is None:
                    continue
                today_eur_pair = _price_on_or_before(series, day_ts)
                if today_eur_pair is None:
                    continue
                today_eur, _ = today_eur_pair

                local_series = local_price_index.get(h.company_id) if local_price_index is not None else None
                local_pair = _price_on_or_before(local_series, day_ts) if local_series is not None else None
                today_local = local_pair[0] if local_pair is not None else None

                date_series = local_series if local_series is not None else series
                today_dt_pair = _price_on_or_before(date_series, day_ts) if date_series is not None else None
                today_dt = today_dt_pair[1].strftime("%Y-%m-%d") if today_dt_pair is not None else None

                # Mutate the previous day's holding object directly: it was
                # appended to daily_picks with exit fields blank.
                h.exit_price_eur = round(float(today_eur), 4)
                h.exit_price_local = round(float(today_local), 4) if today_local is not None else None
                h.exit_date = today_dt
                if h.entry_price_eur and h.entry_price_eur > 0:
                    ret = today_eur / h.entry_price_eur - 1
                    h.forward_return_pct = round(ret * 100.0, 2)
                    forward_components.append(ret)
            if forward_components:
                prior_one_day_return = sum(forward_components) / len(forward_components)
                prev_pick.next_day_return_pct = round(prior_one_day_return * 100.0, 2)

        for _, drow in daily_selected.iterrows():
            cid = int(drow["company_id"])
            today_ids.add(cid)
            score_val = drow.get("momentum_score")

            series = price_index.get(cid)
            entry_pair = _price_on_or_before(series, day_ts) if series is not None else None
            entry_price = entry_pair[0] if entry_pair is not None else None

            local_series = local_price_index.get(cid) if local_price_index is not None else None
            entry_local_pair = _price_on_or_before(local_series, day_ts) if local_series is not None else None
            entry_local = entry_local_pair[0] if entry_local_pair is not None else None

            date_series = local_series if local_series is not None else series
            entry_dt_pair = _price_on_or_before(date_series, day_ts) if date_series is not None else None
            entry_dt = entry_dt_pair[1].strftime("%Y-%m-%d") if entry_dt_pair is not None else None

            cat_scores: dict[str, float | None] = {}
            for cat in _get_category_keys():
                col = f"score_{cat}"
                if col in drow.index and pd.notna(drow[col]):
                    cat_scores[cat] = round(float(drow[col]), 1)
                else:
                    cat_scores[cat] = None

            day_holdings.append(PeriodHolding(
                company_id=cid,
                ticker=str(drow.get("gurufocus_ticker", "")),
                company_name=str(drow.get("company_name", "")),
                sector=str(drow["sector"]),
                score=round(float(score_val), 2) if pd.notna(score_val) else 0.0,
                category_scores=cat_scores,
                weight=day_weight,
                # Exit fields are intentionally None here. The next iteration
                # backfills them once tomorrow's prices are available; the
                # latest day in the panel keeps None (no next trading day yet).
                forward_return_pct=None,
                currency=(company_currency or {}).get(cid),
                entry_price_local=round(entry_local, 4) if entry_local is not None else None,
                exit_price_local=None,
                entry_price_eur=round(entry_price, 4) if entry_price is not None else None,
                exit_price_eur=None,
                entry_date=entry_dt,
                exit_date=None,
            ))

        # Pre-rebalance chain link: today's contribution to cum MTD is the
        # PREVIOUS day's portfolio held one trading day forward (computed
        # above as `prior_one_day_return`). Day 0 contributes 0% — we just
        # entered. After day 0, port_mtd reads (cum_factor − 1) × 100,
        # carrying the running cumulative return through rebalances.
        if i == 0:
            port_mtd = 0.0
        elif prior_one_day_return is not None:
            cum_factor *= (1.0 + prior_one_day_return)
            port_mtd = round((cum_factor - 1.0) * 100.0, 2)
        else:
            # No valid prior-portfolio prices — leave cum unchanged, no return.
            port_mtd = None

        # Turnover: max of (stocks added today, stocks removed today).
        # For a fixed-size portfolio with N swaps, both equal N — so the
        # display reads "N stocks changed" intuitively. With size drift
        # the larger side is the more honest "movement" count.
        if prev_ids:
            adds = len(today_ids - prev_ids)
            removes = len(prev_ids - today_ids)
            turnover_abs = max(adds, removes)
            denom = max(len(today_ids), len(prev_ids), 1)
            turnover_pct = round(turnover_abs / denom * 100, 2)
        else:
            turnover_abs = 0
            turnover_pct = 0.0

        daily_picks.append(DailyPick(
            date=d.isoformat(),
            holdings=day_holdings,
            turnover_abs=turnover_abs,
            turnover_pct=turnover_pct,
            portfolio_return_pct=port_mtd,
        ))
        prev_ids = today_ids
        prev_d_ts = day_ts
        t_daily_holdings_total += time.perf_counter() - t_holdings

    t_daily_loop_elapsed = time.perf_counter() - t_daily_loop_start
    t_total_elapsed = time.perf_counter() - t_total_start
    n_days = len(trading_dates)
    universe_size = int(month_universe_df["company_id"].nunique()) if not month_universe_df.empty else 0
    timing_msg = (
        f"[run_current_portfolio timing] total={t_total_elapsed:.2f}s | "
        f"panel={t_panel_elapsed:.2f}s ({len(panel_cutoffs)} cutoffs) | "
        f"month_start: signals={t_month_start_signals_elapsed * 1000:.1f}ms, "
        f"select={t_month_start_select_elapsed:.2f}s | "
        f"daily_loop={t_daily_loop_elapsed:.2f}s ({n_days} days, "
        f"signals={t_daily_signals_total * 1000:.1f}ms (lookup), "
        f"select={t_daily_select_total:.2f}s avg={t_daily_select_total / max(n_days, 1) * 1000:.0f}ms/day, "
        f"holdings={t_daily_holdings_total:.2f}s) | "
        f"universe_size={universe_size}"
    )
    _logger.info(timing_msg)
    if send_event:
        send_event("timing", message=timing_msg)
        send_event("progress", month=month_key, pct=100, message=f"{len(holdings)} holdings, {len(daily_picks)} daily snapshots")

    return CurrentPortfolio(
        as_of_date=month_start.isoformat(),
        latest_price_date=latest_observed.strftime("%Y-%m-%d") if latest_observed is not None else None,
        holdings=holdings,
        daily_picks=daily_picks,
    )
