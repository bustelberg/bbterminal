"""Price momentum signal computation.

Ported from old_src/quick_insight/ai_momentum/signals/price.py but adapted
to work on a pre-loaded DataFrame (no per-company DB queries) and to accept
an as_of_date for look-ahead bias prevention.
"""
from __future__ import annotations

from datetime import date

import pandas as pd


# ---------------------------------------------------------------------------
# Per-company signal helpers (operate on a single price Series)
# ---------------------------------------------------------------------------

def _mom_return(series: pd.Series, n_months: int) -> float | None:
    if series.empty:
        return None
    cutoff = series.index[-1] - pd.DateOffset(months=n_months)
    past = series[series.index <= cutoff]
    if past.empty:
        return None
    return round((float(series.iloc[-1]) / float(past.iloc[-1]) - 1) * 100, 2)


def _distance_from_ma_pct(series: pd.Series, window: int) -> float | None:
    if series.empty:
        return None
    ma = float(series.tail(window).mean()) if len(series) >= window else float(series.mean())
    if ma == 0:
        return None
    return round((float(series.iloc[-1]) / ma - 1) * 100, 2)


def _sma_slope_pct(series: pd.Series, *, window: int = 50, lookback_days: int = 20) -> float | None:
    if series.empty:
        return None
    sma = series.rolling(window=window, min_periods=max(5, min(window, 20))).mean().dropna()
    if len(sma) <= lookback_days:
        return None
    current = float(sma.iloc[-1])
    past = float(sma.iloc[-(lookback_days + 1)])
    if past == 0:
        return None
    return round((current / past - 1) * 100, 2)


def _drawdown_from_recent_high_pct(series: pd.Series, lookback_days: int = 252) -> float | None:
    if series.empty:
        return None
    window = series.tail(lookback_days)
    if window.empty:
        return None
    recent_high = float(window.max())
    if recent_high == 0:
        return None
    return round((float(series.iloc[-1]) / recent_high - 1) * 100, 2)


def _annualized_volatility_pct(series: pd.Series, lookback_days: int = 126) -> float | None:
    if len(series) < 3:
        return None
    daily_returns = series.pct_change().dropna().tail(lookback_days)
    if len(daily_returns) < 2:
        return None
    vol = float(daily_returns.std())
    if pd.isna(vol) or vol == 0:
        return None
    return round(vol * (252 ** 0.5) * 100, 2)


def _volatility_adjusted_return(series: pd.Series, *, n_months: int = 6, vol_lookback_days: int = 126) -> float | None:
    ret = _mom_return(series, n_months)
    vol = _annualized_volatility_pct(series, lookback_days=vol_lookback_days)
    if ret is None or vol in (None, 0):
        return None
    return round(ret / vol, 4)


def _compute_single_company_signals(series: pd.Series) -> dict:
    """Compute all price signals for a single company's price series."""
    if series.empty:
        return {}

    price_now = float(series.iloc[-1])
    ma_200 = float(series.tail(200).mean()) if len(series) >= 200 else float(series.mean())
    ma_50 = float(series.tail(50).mean()) if len(series) >= 50 else float(series.mean())

    # 12-1 momentum: 12-month return excluding the most recent month
    skip_last_month_cutoff = series.index[-1] - pd.DateOffset(months=1)
    series_skip_last = series[series.index <= skip_last_month_cutoff]
    cutoff_12m = series.index[-1] - pd.DateOffset(months=12)
    past_12m = series[series.index <= cutoff_12m]

    mom_12_1 = None
    if not past_12m.empty and not series_skip_last.empty:
        mom_12_1 = round((float(series_skip_last.iloc[-1]) / float(past_12m.iloc[-1]) - 1) * 100, 2)

    return {
        "mom_1m": _mom_return(series, 1),
        "mom_3m": _mom_return(series, 3),
        "mom_6m": _mom_return(series, 6),
        "mom_12m": _mom_return(series, 12),
        "mom_12_1": mom_12_1,
        "above_200ma": 1 if price_now > ma_200 else 0,
        "ma_50_above_200": 1 if ma_50 > ma_200 else 0,
        "ma_50_slope_20d_pct": _sma_slope_pct(series, window=50, lookback_days=20),
        "drawdown_from_recent_high_pct": _drawdown_from_recent_high_pct(series, lookback_days=252),
        "volatility_adjusted_return_6m": _volatility_adjusted_return(series, n_months=6, vol_lookback_days=126),
    }


# ---------------------------------------------------------------------------
# Signal definitions (for the frontend)
# ---------------------------------------------------------------------------

PRICE_SIGNAL_DEFS: list[dict] = [
    {"key": "mom_6m", "label": "6M Return", "description": "Total price return over 6 months", "default_weight": 1},
    {"key": "mom_12_1", "label": "12-1M Return", "description": "12-month return excluding last month (classic momentum)", "default_weight": 1},
    {"key": "mom_12m", "label": "12M Return", "description": "Total price return over 12 months", "default_weight": 1},
    {"key": "mom_3m", "label": "3M Return", "description": "Total price return over 3 months", "default_weight": 1},
    {"key": "mom_1m", "label": "1M Return", "description": "Total price return over 1 month", "default_weight": 0},
    {"key": "above_200ma", "label": "Above 200 MA", "description": "Price above 200-day moving average (1 or 0)", "default_weight": 1},
    {"key": "ma_50_above_200", "label": "50 MA > 200 MA", "description": "50-day MA above 200-day MA (1 or 0)", "default_weight": 1},
    {"key": "ma_50_slope_20d_pct", "label": "50 MA Slope", "description": "% change in 50-day MA over last 20 days", "default_weight": 1},
    {"key": "drawdown_from_recent_high_pct", "label": "Drawdown", "description": "% below 52-week high (less negative = better)", "default_weight": 1},
    {"key": "volatility_adjusted_return_6m", "label": "Vol-Adj Return", "description": "6M return divided by annualized volatility", "default_weight": 1},
]


# ---------------------------------------------------------------------------
# Universe-level signal computation
# ---------------------------------------------------------------------------

def compute_price_signals(
    prices_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    as_of_date: date,
    *,
    price_index: dict[int, pd.Series] | None = None,
) -> pd.DataFrame:
    """Compute price signals for all companies as of a given date.

    Args:
        prices_df: Full price DataFrame (unused if price_index is provided).
        universe_df: Company DataFrame with at least [company_id, sector].
        as_of_date: Only use prices on or before this date (no look-ahead).
        price_index: Pre-indexed dict of {company_id: Series}. If provided,
                     avoids repeated DataFrame filtering (much faster).

    Returns:
        DataFrame with company_id, sector, and all signal columns.
        Companies with insufficient data are excluded.
    """
    cutoff = pd.Timestamp(as_of_date)

    results = []

    if price_index is not None:
        for cid in universe_df["company_id"].unique():
            series = price_index.get(int(cid))
            if series is None or len(series) < 20:
                continue
            trimmed = series[series.index <= cutoff]
            if len(trimmed) < 20:
                continue
            signals = _compute_single_company_signals(trimmed)
            signals["company_id"] = cid
            results.append(signals)
    else:
        available = prices_df[prices_df["target_date"] <= cutoff]
        for cid in universe_df["company_id"].unique():
            company_prices = available[available["company_id"] == cid]
            if company_prices.empty or len(company_prices) < 20:
                continue
            series = pd.Series(
                company_prices["price"].values,
                index=pd.DatetimeIndex(company_prices["target_date"]),
                dtype="float64",
            ).sort_index()
            signals = _compute_single_company_signals(series)
            signals["company_id"] = cid
            results.append(signals)

    if not results:
        return pd.DataFrame()

    signals_df = pd.DataFrame(results)
    # Merge sector from universe
    signals_df = signals_df.merge(
        universe_df[["company_id", "sector", "company_name", "primary_ticker"]],
        on="company_id",
        how="left",
    )
    return signals_df
