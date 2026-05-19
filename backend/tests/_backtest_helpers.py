"""Shared fixture helpers for the test_backtest_*.py suite.

The leading underscore keeps pytest from collecting this as a test
module. Every test file in the backtest group imports from here so they
share one source of truth for synthetic prices, the universe shape, and
the equal-signal-weights bag.
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd


# ~15 months of pre-backtest history — enough for the 200-day MA window
# and the 12-1 momentum signal to compute on the first cutoff.
PRICE_HISTORY_START = "2023-09-01"
PRICES_END = "2025-04-15"

BACKTEST_START = date(2024, 12, 1)
BACKTEST_END = date(2025, 3, 1)
# Engine aligns to first-Monday-of-month → entry dates are Dec 2,
# Jan 6, Feb 3, Mar 3. `run_backtest` iterates the first 3 with the
# 4th as the closing exit, so monthly_records has 3 entries — each
# now labeled with the exact rebalance Monday (YYYY-MM-DD) so the
# UI's period column shows the actual entry day.
EXPECTED_MONTHS = ["2024-12-02", "2025-01-06", "2025-02-03"]


def calendar_daily(start: str, end: str) -> pd.DatetimeIndex:
    return pd.date_range(start=start, end=end, freq="D")


def exp_prices(daily_factor: float, *, dates: pd.DatetimeIndex, start_price: float = 100.0) -> np.ndarray:
    """Pure exponential growth: price[i] = start * factor**i."""
    return start_price * (daily_factor ** np.arange(len(dates)))


def build_prices_df(companies: dict[int, float], dates: pd.DatetimeIndex) -> pd.DataFrame:
    """Long-format prices DataFrame. `companies` maps company_id → daily growth factor."""
    rows: list[dict] = []
    for cid, factor in companies.items():
        values = exp_prices(factor, dates=dates)
        for d, v in zip(dates, values):
            rows.append({
                "company_id": cid,
                "target_date": d.date(),
                "price": float(v),
            })
    return pd.DataFrame(rows)


def build_universe_df(rows: list[tuple[int, str, str | None, str]]) -> pd.DataFrame:
    """rows: list of (company_id, ticker, sector, company_name).
    `sector=None` mirrors the shape produced by `load_universe` for a
    snapshot-based universe (sector comes from `monthly_eligible` instead)."""
    return pd.DataFrame(
        [
            {
                "company_id": cid,
                "gurufocus_ticker": ticker,
                "company_name": name,
                "sector": sector,
                "gurufocus_exchange": "NYSE",
            }
            for (cid, ticker, sector, name) in rows
        ]
    )


def equal_signal_weights() -> dict[str, float]:
    """Equal weight across every price signal — the synthetic series are
    monotonic, so the relative ordering is the same on every signal."""
    return {
        "mom_12_1": 1,
        "mom_6m": 1,
        "volatility_adjusted_return_6m": 1,
        "drawdown_from_recent_high_pct": 1,
        "above_200ma": 1,
    }


def expected_forward_return(price_series: pd.Series, entry: pd.Timestamp, exit_: pd.Timestamp) -> float:
    """Re-derive the forward return the same way `_price_on_or_after` does
    in production, so the test asserts loop wiring rather than re-deriving
    growth maths."""
    e = float(price_series[price_series.index >= entry].iloc[0])
    x = float(price_series[price_series.index >= exit_].iloc[0])
    return round((x / e - 1) * 100, 2)


def build_six_company_signals_df() -> pd.DataFrame:
    """Synthetic signals DataFrame with strict ordering across two sectors.

    Each company has scalar signal values that the scoring engine
    normalizes, but the relative ordering is fixed: the higher the
    integer, the higher the rank.
    """
    return pd.DataFrame([
        {"company_id": 10, "sector": "Alpha", "gurufocus_ticker": "A1", "company_name": "Alpha-1",
         "mom_12_1": 5, "mom_6m": 5, "volatility_adjusted_return_6m": 5,
         "drawdown_from_recent_high_pct": 5, "above_200ma": 5},
        {"company_id": 11, "sector": "Alpha", "gurufocus_ticker": "A2", "company_name": "Alpha-2",
         "mom_12_1": 3, "mom_6m": 3, "volatility_adjusted_return_6m": 3,
         "drawdown_from_recent_high_pct": 3, "above_200ma": 3},
        {"company_id": 12, "sector": "Alpha", "gurufocus_ticker": "A3", "company_name": "Alpha-3",
         "mom_12_1": 1, "mom_6m": 1, "volatility_adjusted_return_6m": 1,
         "drawdown_from_recent_high_pct": 1, "above_200ma": 1},
        {"company_id": 20, "sector": "Beta", "gurufocus_ticker": "B1", "company_name": "Beta-1",
         "mom_12_1": 4, "mom_6m": 4, "volatility_adjusted_return_6m": 4,
         "drawdown_from_recent_high_pct": 4, "above_200ma": 4},
        {"company_id": 21, "sector": "Beta", "gurufocus_ticker": "B2", "company_name": "Beta-2",
         "mom_12_1": 2, "mom_6m": 2, "volatility_adjusted_return_6m": 2,
         "drawdown_from_recent_high_pct": 2, "above_200ma": 2},
        {"company_id": 22, "sector": "Beta", "gurufocus_ticker": "B3", "company_name": "Beta-3",
         "mom_12_1": 0, "mom_6m": 0, "volatility_adjusted_return_6m": 0,
         "drawdown_from_recent_high_pct": 0, "above_200ma": 0},
    ])
