"""Monthly momentum backtest engine."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable

import numpy as np
import pandas as pd

from .signals import PRICE_SIGNAL_DEFS, compute_price_signals, compute_signals_panel
from .scoring import score_and_select, random_select, _get_category_keys

_logger = logging.getLogger(__name__)


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
        )


@dataclass
class MonthlyHolding:
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


@dataclass
class MonthlyRecord:
    date: str  # YYYY-MM
    holdings: list[MonthlyHolding]
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
    day. Each holding carries the start-of-month → that-day MTD return, so the
    UI can render this list like the monthly backtest table."""
    date: str                              # YYYY-MM-DD
    holdings: list[MonthlyHolding]
    turnover_abs: int                      # number of holdings that differ from previous day
    turnover_pct: float                    # turnover_abs / max(len(today), len(prev)) * 100
    portfolio_return_pct: float | None = None  # equal-weight mean of holdings' MTD returns


@dataclass
class CurrentPortfolio:
    """Snapshot of what the strategy would hold right now (rebalance on
    the first of the current month, MTD return through the latest price)."""
    as_of_date: str       # YYYY-MM-01 — start of current month
    latest_price_date: str | None  # most recent price date observed across the portfolio
    holdings: list[MonthlyHolding]
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
    monthly_records: list[MonthlyRecord]
    summary: BacktestSummary

    def to_dict(self) -> dict:
        return {
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
) -> BacktestResult:
    """Run the monthly momentum backtest.

    For each month:
    1. Compute price and volume signals using data up to that month
    2. Score and select top companies
    3. Compute forward 1-month return for each holding
    4. Track cumulative portfolio return

    If monthly_eligible is provided (from universe_snapshot), only companies
    in the eligible set for that month are considered.
    """
    months = _generate_month_starts(config.start_date, config.end_date)
    if len(months) < 2:
        raise ValueError("Need at least 2 months for a backtest")

    # Build price index once — eliminates repeated DataFrame filtering
    price_index = _build_price_index(prices_df)
    local_price_index = (
        _build_price_index(prices_local_df)
        if prices_local_df is not None and not prices_local_df.empty
        else None
    )
    volume_index = _build_volume_index(volumes_df) if volumes_df is not None and not volumes_df.empty else None

    monthly_records: list[MonthlyRecord] = []
    cumulative = 0.0  # cumulative return in %
    cumulative_factor = 1.0  # multiplicative
    prev_holdings_set: set[int] = set()
    all_monthly_returns: list[float] = []
    turnover_values: list[float] = []
    holdings_counts: list[int] = []

    # Random selector RNG: seeded once per backtest so re-runs with the same
    # seed produce identical picks across all months.
    rng = (
        np.random.default_rng(config.random_seed)
        if config.selection_mode == "random"
        else None
    )

    for i, month_date in enumerate(months[:-1]):  # last month has no forward return
        next_month = months[i + 1]

        if send_event:
            pct = round((i / (len(months) - 1)) * 100)
            send_event(
                "progress",
                month=month_date.isoformat()[:7],
                pct=pct,
                message=f"Computing signals for {month_date.strftime('%b %Y')}...",
            )

        # Filter universe for this month if snapshot-based
        month_universe_df = universe_df
        if monthly_eligible is not None:
            month_key = month_date.isoformat()[:7]
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
                        message=f"{month_date.strftime('%b %Y')}: {reason}",
                    )
                monthly_records.append(MonthlyRecord(
                    date=month_date.isoformat()[:7],
                    holdings=[],
                    portfolio_return_pct=None,
                    cumulative_return_pct=round(cumulative, 2),
                    empty_reason=reason,
                ))
                continue
            month_universe_df = universe_df[
                universe_df["company_id"].isin(eligible_ids)
            ].copy().reset_index(drop=True)
            # Attach per-month sector from the universe_membership snapshot.
            # `load_universe` left sector=None on the base df; without this
            # merge, sector-based selection would find 0 sectors.
            month_universe_df["sector"] = month_universe_df["company_id"].map(sector_map)

        # Compute signals as of this month
        signals_df = compute_price_signals(
            prices_df, month_universe_df, as_of_date=month_date,
            price_index=price_index,
            volume_index=volume_index,
        )
        if signals_df.empty:
            reason = f"No companies had enough price data (need >= 20 data points before {month_date.strftime('%b %Y')})"
            if send_event:
                send_event(
                    "progress",
                    month=month_date.isoformat()[:7],
                    pct=pct,
                    message=f"{month_date.strftime('%b %Y')}: 0 holdings — {reason}",
                )
                send_event(
                    "warning",
                    scope="backtest",
                    message=f"{month_date.strftime('%b %Y')}: {reason}",
                )
            monthly_records.append(MonthlyRecord(
                date=month_date.isoformat()[:7],
                holdings=[],
                portfolio_return_pct=None,
                cumulative_return_pct=round(cumulative, 2),
                empty_reason=reason,
            ))
            continue

        # Score and select (or pick at random as a noise-floor baseline)
        if rng is not None:
            selected = random_select(
                signals_df,
                top_n_sectors=config.top_n_sectors,
                top_n_per_sector=config.top_n_per_sector,
                rng=rng,
            )
        else:
            selected = score_and_select(
                signals_df,
                config.signal_weights,
                top_n_sectors=config.top_n_sectors,
                top_n_per_sector=config.top_n_per_sector,
                category_weights=config.category_weights,
            )

        if selected.empty:
            n_signals = len(signals_df)
            sectors = signals_df["sector"].nunique() if "sector" in signals_df.columns else 0
            reason = f"{n_signals} companies had signals across {sectors} sectors but none passed selection (top_n_sectors={config.top_n_sectors}, top_n_per_sector={config.top_n_per_sector})"
            if send_event:
                send_event(
                    "progress",
                    month=month_date.isoformat()[:7],
                    pct=pct,
                    message=f"{month_date.strftime('%b %Y')}: 0 holdings — {reason}",
                )
                send_event(
                    "warning",
                    scope="backtest",
                    message=f"{month_date.strftime('%b %Y')}: {reason}",
                )
            monthly_records.append(MonthlyRecord(
                date=month_date.isoformat()[:7],
                holdings=[],
                portfolio_return_pct=None,
                cumulative_return_pct=round(cumulative, 2),
                empty_reason=reason,
            ))
            continue

        # Equal weight
        n_holdings = len(selected)
        weight = 1.0 / n_holdings
        holdings_counts.append(n_holdings)

        # Compute forward returns using pre-indexed series
        holdings: list[MonthlyHolding] = []
        returns: list[float] = []
        entry_ts = pd.Timestamp(month_date)
        exit_ts = pd.Timestamp(next_month)

        for _, row in selected.iterrows():
            cid = int(row["company_id"])
            series = price_index.get(cid)

            entry_price = _price_on_or_after(series, entry_ts) if series is not None else None
            exit_price = _price_on_or_after(series, exit_ts) if series is not None else None

            fwd_return = None
            if entry_price and exit_price and entry_price > 0:
                fwd_return = round((exit_price / entry_price - 1) * 100, 2)
                returns.append(fwd_return)

            local_series = local_price_index.get(cid) if local_price_index is not None else None
            entry_local = _price_on_or_after(local_series, entry_ts) if local_series is not None else None
            exit_local = _price_on_or_after(local_series, exit_ts) if local_series is not None else None

            # Actual trading dates (prefer local series, fall back to EUR series).
            date_series = local_series if local_series is not None else series
            entry_dt = _date_on_or_after(date_series, entry_ts) if date_series is not None else None
            exit_dt = _date_on_or_after(date_series, exit_ts) if date_series is not None else None

            # Extract per-category scores
            cat_scores: dict[str, float | None] = {}
            for cat in _get_category_keys():
                col = f"score_{cat}"
                if col in row.index and pd.notna(row[col]):
                    cat_scores[cat] = round(float(row[col]), 1)
                else:
                    cat_scores[cat] = None

            score_val = row.get("momentum_score")
            holdings.append(MonthlyHolding(
                company_id=cid,
                ticker=str(row.get("gurufocus_ticker", "")),
                company_name=str(row.get("company_name", "")),
                sector=str(row["sector"]),
                score=round(float(score_val), 2) if pd.notna(score_val) else 0.0,
                category_scores=cat_scores,
                weight=weight,
                forward_return_pct=fwd_return,
                currency=(company_currency or {}).get(cid),
                entry_price_local=round(entry_local, 4) if entry_local is not None else None,
                exit_price_local=round(exit_local, 4) if exit_local is not None else None,
                entry_price_eur=round(entry_price, 4) if entry_price is not None else None,
                exit_price_eur=round(exit_price, 4) if exit_price is not None else None,
                entry_date=entry_dt,
                exit_date=exit_dt,
            ))

        # Portfolio return = equal-weighted average of individual returns
        port_return = round(float(np.mean(returns)), 2) if returns else None

        if port_return is not None:
            cumulative_factor *= (1 + port_return / 100)
            cumulative = (cumulative_factor - 1) * 100
            all_monthly_returns.append(port_return)

        # Turnover
        current_set = {h.company_id for h in holdings}
        if prev_holdings_set:
            overlap = len(current_set & prev_holdings_set)
            total = max(len(current_set), len(prev_holdings_set))
            turnover = round((1 - overlap / total) * 100, 2) if total > 0 else 0
            turnover_values.append(turnover)
        prev_holdings_set = current_set

        monthly_records.append(MonthlyRecord(
            date=month_date.isoformat()[:7],
            holdings=holdings,
            portfolio_return_pct=port_return,
            cumulative_return_pct=round(cumulative, 2),
        ))

    # Summary stats
    total_return = round(cumulative, 2)
    n_years = len(all_monthly_returns) / 12 if all_monthly_returns else 0
    annualized = round((cumulative_factor ** (1 / n_years) - 1) * 100, 2) if n_years > 0 else 0

    # Identify all drawdown periods (peak-to-trough-to-recovery)
    values = [(r.date, 1 + r.cumulative_return_pct / 100) for r in monthly_records]
    all_drawdown_periods = _find_drawdown_periods(values)
    # Pick top 3 non-overlapping
    top_drawdowns = _pick_top_n_non_overlapping(all_drawdown_periods, 3)
    max_dd = top_drawdowns[0].drawdown_pct if top_drawdowns else 0.0

    # Sharpe (annualized, using monthly returns)
    sharpe = None
    if len(all_monthly_returns) >= 12:
        arr = np.array(all_monthly_returns)
        monthly_mean = float(arr.mean())
        monthly_std = float(arr.std())
        if monthly_std > 0:
            sharpe = round((monthly_mean / monthly_std) * (12 ** 0.5), 2)

    summary = BacktestSummary(
        total_return_pct=total_return,
        annualized_return_pct=annualized,
        max_drawdown_pct=round(max_dd, 2),
        sharpe_ratio=sharpe,
        avg_monthly_turnover_pct=round(float(np.mean(turnover_values)), 2) if turnover_values else 0,
        total_months=len(all_monthly_returns),
        avg_holdings=round(float(np.mean(holdings_counts)), 1) if holdings_counts else 0,
        top_drawdowns=top_drawdowns,
    )

    if send_event:
        send_event("progress", month="done", pct=100, message="Backtest complete")

    return BacktestResult(monthly_records=monthly_records, summary=summary)


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
    on each MonthlyRecord) is the per-month mean across trials. Holdings
    on each MonthlyRecord come from the first trial — they're random
    anyway, so aggregating them isn't meaningful.

    Forces selection_mode="random". Caller controls the base seed via
    config.random_seed; trial seeds are base, base+1, ..., base+N-1.
    """
    if n_trials < 1:
        raise ValueError("n_trials must be >= 1")
    if config.selection_mode != "random":
        raise ValueError("run_multi_trial_backtest requires selection_mode='random'")

    base_seed = config.random_seed if config.random_seed is not None else 0

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
        )
        trial_results.append(result)

    # Aggregate: per-month mean cumulative return across trials. All trials
    # iterate the same month grid so records align by index.
    n_months = max(len(r.monthly_records) for r in trial_results)
    aggregated_records: list[MonthlyRecord] = []
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
        aggregated_records.append(MonthlyRecord(
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

    return BacktestResult(monthly_records=aggregated_records, summary=summary)


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

    holdings: list[MonthlyHolding] = []
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
        holdings.append(MonthlyHolding(
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
        day_holdings: list[MonthlyHolding] = []
        today_ids: set[int] = set()
        day_returns: list[float] = []
        for _, drow in daily_selected.iterrows():
            cid = int(drow["company_id"])
            today_ids.add(cid)
            score_val = drow.get("momentum_score")

            series = price_index.get(cid)
            entry_price = _price_on_or_after(series, entry_ts) if series is not None else None
            exit_pair = _price_on_or_before(series, day_ts) if series is not None else None
            exit_price = exit_pair[0] if exit_pair is not None else None

            mtd_return = None
            if entry_price and exit_price and entry_price > 0:
                mtd_return = round((exit_price / entry_price - 1) * 100, 2)
                day_returns.append(mtd_return)

            local_series = local_price_index.get(cid) if local_price_index is not None else None
            entry_local = _price_on_or_after(local_series, entry_ts) if local_series is not None else None
            exit_local_pair = _price_on_or_before(local_series, day_ts) if local_series is not None else None
            exit_local = exit_local_pair[0] if exit_local_pair is not None else None

            date_series = local_series if local_series is not None else series
            entry_dt = _date_on_or_after(date_series, entry_ts) if date_series is not None else None
            exit_dt_pair = _price_on_or_before(date_series, day_ts) if date_series is not None else None
            exit_dt = exit_dt_pair[1].strftime("%Y-%m-%d") if exit_dt_pair is not None else None

            cat_scores: dict[str, float | None] = {}
            for cat in _get_category_keys():
                col = f"score_{cat}"
                if col in drow.index and pd.notna(drow[col]):
                    cat_scores[cat] = round(float(drow[col]), 1)
                else:
                    cat_scores[cat] = None

            day_holdings.append(MonthlyHolding(
                company_id=cid,
                ticker=str(drow.get("gurufocus_ticker", "")),
                company_name=str(drow.get("company_name", "")),
                sector=str(drow["sector"]),
                score=round(float(score_val), 2) if pd.notna(score_val) else 0.0,
                category_scores=cat_scores,
                weight=day_weight,
                forward_return_pct=mtd_return,
                currency=(company_currency or {}).get(cid),
                entry_price_local=round(entry_local, 4) if entry_local is not None else None,
                exit_price_local=round(exit_local, 4) if exit_local is not None else None,
                entry_price_eur=round(entry_price, 4) if entry_price is not None else None,
                exit_price_eur=round(exit_price, 4) if exit_price is not None else None,
                entry_date=entry_dt,
                exit_date=exit_dt,
            ))

        port_mtd = round(sum(day_returns) / len(day_returns), 2) if day_returns else None

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
