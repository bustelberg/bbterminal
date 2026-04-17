"""Monthly momentum backtest engine."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable

import numpy as np
import pandas as pd

from .signals import PRICE_SIGNAL_DEFS, compute_price_signals
from .scoring import score_and_select, _get_category_keys


@dataclass
class BacktestConfig:
    start_date: date
    end_date: date
    signal_weights: dict[str, float]
    top_n_sectors: int = 4
    top_n_per_sector: int = 6
    category_weights: dict[str, float] | None = None  # e.g. {"price": 0.5, "volume": 0.5}

    @classmethod
    def from_dict(cls, d: dict) -> BacktestConfig:
        return cls(
            start_date=date.fromisoformat(d["start_date"]),
            end_date=date.fromisoformat(d["end_date"]),
            signal_weights=d.get("signal_weights", {s["key"]: s["default_weight"] for s in PRICE_SIGNAL_DEFS}),
            top_n_sectors=d.get("top_n_sectors", 4),
            top_n_per_sector=d.get("top_n_per_sector", 6),
            category_weights=d.get("category_weights"),
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
    monthly_eligible: dict[str, set[int]] | None = None,
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
    volume_index = _build_volume_index(volumes_df) if volumes_df is not None and not volumes_df.empty else None

    monthly_records: list[MonthlyRecord] = []
    cumulative = 0.0  # cumulative return in %
    cumulative_factor = 1.0  # multiplicative
    prev_holdings_set: set[int] = set()
    all_monthly_returns: list[float] = []
    turnover_values: list[float] = []
    holdings_counts: list[int] = []

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
            eligible_ids = monthly_eligible.get(month_key, set())
            if not eligible_ids:
                snap_min = min(monthly_eligible.keys())
                snap_max = max(monthly_eligible.keys())
                if month_key < snap_min or month_key > snap_max:
                    reason = f"Month is outside universe snapshot range ({snap_min} to {snap_max})"
                else:
                    reason = "All companies in the universe snapshot failed screening criteria for this month (0 passing)"
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
            ].reset_index(drop=True)

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
            monthly_records.append(MonthlyRecord(
                date=month_date.isoformat()[:7],
                holdings=[],
                portfolio_return_pct=None,
                cumulative_return_pct=round(cumulative, 2),
                empty_reason=reason,
            ))
            continue

        # Score and select
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

            # Extract per-category scores
            cat_scores: dict[str, float | None] = {}
            for cat in _get_category_keys():
                col = f"score_{cat}"
                if col in row.index and pd.notna(row[col]):
                    cat_scores[cat] = round(float(row[col]), 1)
                else:
                    cat_scores[cat] = None

            holdings.append(MonthlyHolding(
                company_id=cid,
                ticker=str(row.get("gurufocus_ticker", "")),
                company_name=str(row.get("company_name", "")),
                sector=str(row["sector"]),
                score=round(float(row["momentum_score"]), 2),
                category_scores=cat_scores,
                weight=weight,
                forward_return_pct=fwd_return,
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
