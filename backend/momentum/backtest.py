"""Monthly momentum backtest engine."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable

import numpy as np
import pandas as pd

from .signals import PRICE_SIGNAL_DEFS, compute_price_signals
from .scoring import score_and_select


@dataclass
class BacktestConfig:
    start_date: date
    end_date: date
    signal_weights: dict[str, float]
    top_n_sectors: int = 4
    top_n_per_sector: int = 6

    @classmethod
    def from_dict(cls, d: dict) -> BacktestConfig:
        return cls(
            start_date=date.fromisoformat(d["start_date"]),
            end_date=date.fromisoformat(d["end_date"]),
            signal_weights=d.get("signal_weights", {s["key"]: s["default_weight"] for s in PRICE_SIGNAL_DEFS}),
            top_n_sectors=d.get("top_n_sectors", 4),
            top_n_per_sector=d.get("top_n_per_sector", 6),
        )


@dataclass
class MonthlyHolding:
    company_id: int
    ticker: str
    company_name: str
    sector: str
    score: float
    weight: float
    forward_return_pct: float | None


@dataclass
class MonthlyRecord:
    date: str  # YYYY-MM
    holdings: list[MonthlyHolding]
    portfolio_return_pct: float | None
    cumulative_return_pct: float


@dataclass
class BacktestSummary:
    total_return_pct: float
    annualized_return_pct: float
    max_drawdown_pct: float
    sharpe_ratio: float | None
    avg_monthly_turnover_pct: float
    total_months: int
    avg_holdings: float


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
                            "weight": round(h.weight, 4),
                            "forward_return_pct": h.forward_return_pct,
                        }
                        for h in r.holdings
                    ],
                    "portfolio_return_pct": r.portfolio_return_pct,
                    "cumulative_return_pct": r.cumulative_return_pct,
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


def run_backtest(
    config: BacktestConfig,
    prices_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    send_event: Callable[..., Any] | None = None,
) -> BacktestResult:
    """Run the monthly momentum backtest.

    For each month:
    1. Compute price signals using data up to that month
    2. Score and select top companies
    3. Compute forward 1-month return for each holding
    4. Track cumulative portfolio return
    """
    months = _generate_month_starts(config.start_date, config.end_date)
    if len(months) < 2:
        raise ValueError("Need at least 2 months for a backtest")

    # Build price index once — eliminates repeated DataFrame filtering
    price_index = _build_price_index(prices_df)

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

        # Compute signals as of this month
        signals_df = compute_price_signals(
            prices_df, universe_df, as_of_date=month_date,
            price_index=price_index,
        )
        if signals_df.empty:
            monthly_records.append(MonthlyRecord(
                date=month_date.isoformat()[:7],
                holdings=[],
                portfolio_return_pct=None,
                cumulative_return_pct=round(cumulative, 2),
            ))
            continue

        # Score and select
        selected = score_and_select(
            signals_df,
            config.signal_weights,
            top_n_sectors=config.top_n_sectors,
            top_n_per_sector=config.top_n_per_sector,
        )

        if selected.empty:
            monthly_records.append(MonthlyRecord(
                date=month_date.isoformat()[:7],
                holdings=[],
                portfolio_return_pct=None,
                cumulative_return_pct=round(cumulative, 2),
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

            holdings.append(MonthlyHolding(
                company_id=cid,
                ticker=str(row.get("primary_ticker", "")),
                company_name=str(row.get("company_name", "")),
                sector=str(row["sector"]),
                score=round(float(row["momentum_score"]), 2),
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

    # Max drawdown (from portfolio value, not percentage points)
    peak_value = 1.0
    max_dd = 0.0
    for r in monthly_records:
        current_value = 1 + r.cumulative_return_pct / 100
        peak_value = max(peak_value, current_value)
        dd = (current_value / peak_value - 1) * 100 if peak_value > 0 else 0
        max_dd = min(max_dd, dd)

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
    )

    if send_event:
        send_event("progress", month="done", pct=100, message="Backtest complete")

    return BacktestResult(monthly_records=monthly_records, summary=summary)
