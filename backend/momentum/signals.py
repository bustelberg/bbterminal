"""Price momentum signal computation.

Ported from old_src/quick_insight/ai_momentum/signals/price.py but adapted
to work on a pre-loaded DataFrame (no per-company DB queries) and to accept
an as_of_date for look-ahead bias prevention.
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd


# Reject companies whose last available price is older than this many days
# before the as-of cutoff — otherwise signals get anchored to stale prices
# (delisted / halted / data-gap names) instead of current state. Applied
# identically by the single-cutoff and multi-cutoff signal panels below.
MAX_STALENESS_DAYS = 30


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
    past_price = float(past.iloc[-1])
    if past_price == 0:
        return None
    return round((float(series.iloc[-1]) / past_price - 1) * 100, 2)


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


# ---------------------------------------------------------------------------
# Volume signal helpers
# ---------------------------------------------------------------------------

def _volume_ratio(vol_series: pd.Series, short_window: int, long_window: int) -> float | None:
    """Ratio of short-term avg volume to long-term avg volume."""
    if len(vol_series) < long_window:
        return None
    short_avg = float(vol_series.tail(short_window).mean())
    long_avg = float(vol_series.tail(long_window).mean())
    if long_avg == 0:
        return None
    return round(short_avg / long_avg, 4)


def _volume_trend(vol_series: pd.Series, n_months: int) -> float | None:
    """% change in average daily volume: recent month vs n_months ago."""
    if vol_series.empty:
        return None
    recent_cutoff = vol_series.index[-1] - pd.DateOffset(days=21)
    past_cutoff = vol_series.index[-1] - pd.DateOffset(months=n_months)
    past_end = past_cutoff + pd.DateOffset(days=21)

    recent = vol_series[vol_series.index > recent_cutoff]
    past = vol_series[(vol_series.index >= past_cutoff) & (vol_series.index <= past_end)]
    if recent.empty or past.empty:
        return None
    recent_avg = float(recent.mean())
    past_avg = float(past.mean())
    if past_avg == 0:
        return None
    return round((recent_avg / past_avg - 1) * 100, 2)


def _compute_volume_signals(vol_series: pd.Series) -> dict:
    """Compute volume signals for a single company."""
    if vol_series.empty or len(vol_series) < 20:
        return {}
    return {
        "vol_20d_vs_60d": _volume_ratio(vol_series, 20, 60),
        "vol_trend_3m": _volume_trend(vol_series, 3),
    }


def _compute_single_company_signals(series: pd.Series) -> dict:
    """Compute all price signals for a single company's price series."""
    if series.empty:
        return {}

    price_now = float(series.iloc[-1])
    ma_200 = float(series.tail(200).mean()) if len(series) >= 200 else float(series.mean())

    # 12-1 momentum: 12-month return excluding the most recent month
    skip_last_month_cutoff = series.index[-1] - pd.DateOffset(months=1)
    series_skip_last = series[series.index <= skip_last_month_cutoff]
    cutoff_12m = series.index[-1] - pd.DateOffset(months=12)
    past_12m = series[series.index <= cutoff_12m]

    mom_12_1 = None
    if not past_12m.empty and not series_skip_last.empty:
        past_12m_price = float(past_12m.iloc[-1])
        if past_12m_price != 0:
            mom_12_1 = round((float(series_skip_last.iloc[-1]) / past_12m_price - 1) * 100, 2)

    return {
        "mom_12_1": mom_12_1,
        "mom_6m": _mom_return(series, 6),
        "volatility_adjusted_return_6m": _volatility_adjusted_return(series, n_months=6, vol_lookback_days=126),
        "drawdown_from_recent_high_pct": _drawdown_from_recent_high_pct(series, lookback_days=252),
        "above_200ma": 1 if price_now > ma_200 else 0,
    }


# ---------------------------------------------------------------------------
# Signal definitions (for the frontend)
# ---------------------------------------------------------------------------

PRICE_SIGNAL_DEFS: list[dict] = [
    # Price momentum signals
    {"key": "mom_12_1", "label": "12-1M Return", "description": "Price return from 12 months ago to 1 month ago, skipping the most recent month. The classic Jegadeesh-Titman momentum factor — avoids short-term mean reversion.", "default_weight": 3, "group": "price"},
    {"key": "mom_6m", "label": "6M Return", "description": "Total price return over the last 6 months. Captures medium-term trend strength.", "default_weight": 2, "group": "price"},
    {"key": "volatility_adjusted_return_6m", "label": "Vol-Adj Return", "description": "6-month return divided by annualized 6-month volatility. Rewards consistent uptrends over volatile spikes. Similar to a Sharpe ratio per stock.", "default_weight": 2, "group": "price"},
    {"key": "drawdown_from_recent_high_pct", "label": "Drawdown", "description": "Current price vs. 52-week high, expressed as a negative %. Closer to 0% = near highs (stronger). Favors stocks holding up well.", "default_weight": 1, "group": "price"},
    {"key": "above_200ma", "label": "Above 200 MA", "description": "Binary: 1 if current price is above the 200-day moving average, 0 otherwise. Classic long-term trend filter — stocks below 200 MA are in a downtrend.", "default_weight": 1, "group": "price"},
    # Volume signals
    {"key": "vol_20d_vs_60d", "label": "Volume Surge", "description": "Ratio of 20-day average volume to 60-day average volume. Values above 1.0 indicate rising interest and conviction behind price moves. Confirms momentum rather than low-volume drift.", "default_weight": 1, "group": "volume"},
    {"key": "vol_trend_3m", "label": "Volume Trend 3M", "description": "Percentage change in average daily volume: current month vs 3 months ago. Positive = growing institutional attention. Stocks with rising volume alongside price momentum tend to sustain their trends.", "default_weight": 1, "group": "volume"},
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
    volume_index: dict[int, pd.Series] | None = None,
) -> pd.DataFrame:
    """Compute price and volume signals for all companies as of a given date.

    Args:
        prices_df: Full price DataFrame (unused if price_index is provided).
        universe_df: Company DataFrame with at least [company_id, sector].
        as_of_date: Only use prices on or before this date (no look-ahead).
        price_index: Pre-indexed dict of {company_id: Series}. If provided,
                     avoids repeated DataFrame filtering (much faster).
        volume_index: Pre-indexed dict of {company_id: Series} for volume data.

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
            # Strict `<` so signals never see the close at which we'll enter the trade.
            trimmed = series[series.index < cutoff]
            if len(trimmed) < 20:
                continue
            if (cutoff - trimmed.index[-1]).days > MAX_STALENESS_DAYS:
                continue
            signals = _compute_single_company_signals(trimmed)
            # Volume signals
            if volume_index is not None:
                vol_series = volume_index.get(int(cid))
                if vol_series is not None and len(vol_series) > 0:
                    vol_trimmed = vol_series[vol_series.index < cutoff]
                    vol_signals = _compute_volume_signals(vol_trimmed)
                    signals.update(vol_signals)
            signals["company_id"] = cid
            results.append(signals)
    else:
        available = prices_df[prices_df["target_date"] < cutoff]
        for cid in universe_df["company_id"].unique():
            company_prices = available[available["company_id"] == cid]
            if company_prices.empty or len(company_prices) < 20:
                continue
            series = pd.Series(
                company_prices["price"].values,
                index=pd.DatetimeIndex(company_prices["target_date"]),
                dtype="float64",
            ).sort_index()
            if (cutoff - series.index[-1]).days > MAX_STALENESS_DAYS:
                continue
            signals = _compute_single_company_signals(series)
            signals["company_id"] = cid
            results.append(signals)

    if not results:
        return pd.DataFrame()

    signals_df = pd.DataFrame(results)
    # Merge sector from universe
    signals_df = signals_df.merge(
        universe_df[["company_id", "sector", "company_name", "gurufocus_ticker"]],
        on="company_id",
        how="left",
    )
    return signals_df


# ---------------------------------------------------------------------------
# Vectorized panel computation
# ---------------------------------------------------------------------------
#
# `compute_price_signals` recomputes signals from scratch for one cutoff. When
# the caller needs signals at many cutoffs (e.g. one per trading day in
# `run_current_portfolio`), that's wasteful — the underlying lookback windows
# are identical, only the anchor date moves. The functions below compute each
# signal as a rolling time series per company in a single pass, then expose
# cheap per-cutoff lookups.
#
# Parity with the per-cutoff path is validated by tests in test_signals.py.

_PRICE_SIGNAL_COLUMNS = (
    "mom_12_1",
    "mom_6m",
    "volatility_adjusted_return_6m",
    "drawdown_from_recent_high_pct",
    "above_200ma",
)
_VOLUME_SIGNAL_COLUMNS = ("vol_20d_vs_60d", "vol_trend_3m")


def _asof_values(series: pd.Series, targets: pd.DatetimeIndex) -> np.ndarray:
    """For each `t` in `targets`, return the value of `series` at the last
    index <= t (NaN if no such index). Vectorized via searchsorted; the input
    series must be sorted ascending."""
    if len(series) == 0:
        return np.full(len(targets), np.nan)
    positions = series.index.searchsorted(targets, side="right") - 1
    out = np.full(len(targets), np.nan)
    valid = positions >= 0
    if valid.any():
        out[valid] = series.values[positions[valid]]
    return out


def _build_price_signal_panel(series: pd.Series) -> pd.DataFrame:
    """Compute all price signals as time series.

    Returned DataFrame has the same index as `series` and one column per
    signal. Each row [d] holds the signal values that `_compute_single_company_signals`
    would return if called on `series[series.index <= d]`.
    """
    if len(series) < 20:
        return pd.DataFrame(index=series.index, columns=list(_PRICE_SIGNAL_COLUMNS), dtype="float64")

    idx = series.index

    # mom_12_1: at each t, (asof(t-1m)) / (asof(t-12m)) - 1, in %.
    targets_1m = idx - pd.DateOffset(months=1)
    targets_12m = idx - pd.DateOffset(months=12)
    num_1m = _asof_values(series, targets_1m)
    num_12m = _asof_values(series, targets_12m)
    with np.errstate(divide="ignore", invalid="ignore"):
        mom_12_1 = np.where(
            (num_12m > 0) & ~np.isnan(num_1m) & ~np.isnan(num_12m),
            (num_1m / num_12m - 1.0) * 100.0,
            np.nan,
        )
    mom_12_1 = np.round(mom_12_1, 2)

    # mom_6m: at each t, series[t] / asof(t-6m) - 1.
    targets_6m = idx - pd.DateOffset(months=6)
    num_6m = _asof_values(series, targets_6m)
    series_vals = series.values
    with np.errstate(divide="ignore", invalid="ignore"):
        mom_6m = np.where(
            (num_6m > 0) & ~np.isnan(num_6m),
            (series_vals / num_6m - 1.0) * 100.0,
            np.nan,
        )
    mom_6m = np.round(mom_6m, 2)

    # volatility_adjusted_return_6m: round(mom_6m / annualized_vol, 4)
    # where annualized_vol = round(daily_returns.std() * sqrt(252) * 100, 2)
    # over a 126-day window. The original calls .dropna().tail(126) on
    # pct_change(), which drops the leading NaN at index 0; rolling(126,
    # min_periods=2) on pct_change() matches once the window contains
    # >=2 non-NaN returns.
    daily_returns = series.pct_change()
    rolling_std = daily_returns.rolling(126, min_periods=2).std().values
    annualized_vol = np.round(rolling_std * (252 ** 0.5) * 100.0, 2)
    with np.errstate(divide="ignore", invalid="ignore"):
        vol_adj = np.where(
            (annualized_vol > 0) & ~np.isnan(annualized_vol) & ~np.isnan(mom_6m),
            mom_6m / annualized_vol,
            np.nan,
        )
    vol_adj = np.round(vol_adj, 4)

    # drawdown_from_recent_high_pct: series[t] / max(series in last 252 bars) - 1, %.
    rolling_max = series.rolling(252, min_periods=1).max().values
    with np.errstate(divide="ignore", invalid="ignore"):
        drawdown = np.where(
            rolling_max > 0,
            (series_vals / rolling_max - 1.0) * 100.0,
            np.nan,
        )
    drawdown = np.round(drawdown, 2)

    # above_200ma: 1 if price > MA200 else 0. Original returns 0/1 (int);
    # we keep the same semantics. ma==0 returns NaN (matches the original's
    # `if ma == 0: return None`, though in practice ma is always positive).
    ma_200 = series.rolling(200, min_periods=1).mean().values
    above = np.where(
        ma_200 == 0,
        np.nan,
        (series_vals > ma_200).astype(float),
    )

    return pd.DataFrame(
        {
            "mom_12_1": mom_12_1,
            "mom_6m": mom_6m,
            "volatility_adjusted_return_6m": vol_adj,
            "drawdown_from_recent_high_pct": drawdown,
            "above_200ma": above,
        },
        index=idx,
    )


def _build_volume_signal_panel(vol_series: pd.Series) -> pd.DataFrame:
    """Compute volume signals as time series — parity target is
    `_compute_volume_signals`. Returns a DataFrame indexed by `vol_series.index`."""
    if vol_series.empty or len(vol_series) < 20:
        return pd.DataFrame(index=vol_series.index, columns=list(_VOLUME_SIGNAL_COLUMNS), dtype="float64")

    idx = vol_series.index

    # vol_20d_vs_60d = mean(last 20) / mean(last 60). Original requires
    # len >= 60 to return a value (otherwise None). min_periods=20/60 here.
    short_avg = vol_series.rolling(20, min_periods=20).mean().values
    long_avg = vol_series.rolling(60, min_periods=60).mean().values
    with np.errstate(divide="ignore", invalid="ignore"):
        vol_20d_vs_60d = np.where(
            (long_avg > 0) & ~np.isnan(long_avg) & ~np.isnan(short_avg),
            short_avg / long_avg,
            np.nan,
        )
    vol_20d_vs_60d = np.round(vol_20d_vs_60d, 4)

    # vol_trend_3m: at each t,
    #   recent = mean(vol in (t - 21d, t])           — left-open, right-closed
    #   past   = mean(vol in [t - 3m, t - 3m + 21d]) — closed-closed
    # The asymmetric boundary semantics in the original come from its use of
    # `index > recent_cutoff` (strict) for recent vs `index >= past_cutoff` (non-
    # strict) for past.
    #
    # Recent: closed='right' time-based rolling on the original (business-day)
    # index — at each bar t, window = (t - 21D, t]. Direct match.
    #
    # Past: the right edge of the window is `t - 3m + 21d`, which is a calendar
    # date that often falls on a non-trading day. To look up at that exact
    # calendar date (instead of falling back to the prior trading day, which
    # would trim the right edge), we compute the past rolling on a daily
    # calendar reindex — NaNs at non-trading days are excluded by mean().
    recent_avg = vol_series.rolling("21D", closed="right").mean()
    daily_idx = pd.date_range(vol_series.index[0], vol_series.index[-1], freq="D")
    vol_daily = vol_series.reindex(daily_idx)
    past_window_daily = vol_daily.rolling("21D", closed="both").mean()
    past_targets = idx - pd.DateOffset(months=3) + pd.DateOffset(days=21)
    past_avg = past_window_daily.reindex(past_targets).values
    recent_vals = recent_avg.values
    with np.errstate(divide="ignore", invalid="ignore"):
        vol_trend_3m = np.where(
            (past_avg > 0) & ~np.isnan(past_avg) & ~np.isnan(recent_vals),
            (recent_vals / past_avg - 1.0) * 100.0,
            np.nan,
        )
    vol_trend_3m = np.round(vol_trend_3m, 2)

    return pd.DataFrame(
        {
            "vol_20d_vs_60d": vol_20d_vs_60d,
            "vol_trend_3m": vol_trend_3m,
        },
        index=idx,
    )


def compute_signals_panel(
    universe_df: pd.DataFrame,
    cutoffs: list[date],
    *,
    price_index: dict[int, pd.Series],
    volume_index: dict[int, pd.Series] | None = None,
) -> dict[date, pd.DataFrame]:
    """Compute price+volume signals for every cutoff in `cutoffs` in one pass.

    For each company in `universe_df`, builds the rolling signal time series
    once over the company's full price history, then indexes into it per
    cutoff. The returned dict maps each cutoff to a DataFrame with the same
    shape and semantics as `compute_price_signals(..., as_of_date=cutoff)`.

    The strict `<` cutoff and 30-day staleness guard from `compute_price_signals`
    are preserved: a company appears in `result[c]` only if it had >= 20
    price bars strictly before `c` and its latest such bar is within 30 days
    of `c`.
    """
    if not cutoffs:
        return {}

    cutoff_ts = [pd.Timestamp(c) for c in cutoffs]
    cutoff_ts_index = pd.DatetimeIndex(cutoff_ts)

    cids = list(universe_df["company_id"].unique())

    def _per_cid(cid_: int) -> dict[pd.Timestamp, dict]:
        """Per-cid worker — independent of every other cid, so a thread
        pool gives near-linear speedup on this loop. Returns the
        per-cutoff row dict; the main thread aggregates."""
        local: dict[pd.Timestamp, dict] = {}
        series = price_index.get(int(cid_))
        if series is None or len(series) < 20:
            return local

        price_panel = _build_price_signal_panel(series)
        if price_panel.empty:
            return local

        vol_series = volume_index.get(int(cid_)) if volume_index is not None else None
        vol_panel = (
            _build_volume_signal_panel(vol_series)
            if vol_series is not None and len(vol_series) > 0
            else None
        )

        price_idx = series.index
        positions = price_idx.searchsorted(cutoff_ts_index, side="left") - 1

        for c, c_ts, pos in zip(cutoffs, cutoff_ts, positions):
            if pos < 0:
                continue
            anchor = price_idx[pos]
            if pos + 1 < 20:
                continue
            if (c_ts - anchor).days > MAX_STALENESS_DAYS:
                continue

            row = {"company_id": int(cid_)}
            row.update(price_panel.iloc[pos].to_dict())

            if vol_panel is not None:
                vol_pos = vol_panel.index.searchsorted(anchor, side="right") - 1
                if vol_pos >= 0 and vol_pos + 1 >= 20:
                    vol_row = vol_panel.iloc[vol_pos]
                    for k in _VOLUME_SIGNAL_COLUMNS:
                        v = vol_row.get(k)
                        if pd.notna(v):
                            row[k] = float(v) if k == "vol_20d_vs_60d" else float(v)

            local[c_ts] = row
        return local

    # Serial loop across cids. The previous parallel version (Win #7)
    # gave modest wins on synthetic data and was contention-bound on
    # real sweeps — see variants.py for the rationale. pandas
    # operations under the hood are numpy, which is fast serially.
    per_cutoff_rows: dict[pd.Timestamp, list[dict]] = {c: [] for c in cutoff_ts}
    for cid in cids:
        local_rows = _per_cid(cid)
        for c_ts, row in local_rows.items():
            per_cutoff_rows[c_ts].append(row)

    # Assemble per-cutoff DataFrames with the same shape compute_price_signals
    # returns (signal columns + sector + company_name + gurufocus_ticker).
    sector_cols = universe_df[["company_id", "sector", "company_name", "gurufocus_ticker"]]
    result: dict[date, pd.DataFrame] = {}
    for c, c_ts in zip(cutoffs, cutoff_ts):
        rows = per_cutoff_rows[c_ts]
        if not rows:
            result[c] = pd.DataFrame()
            continue
        df = pd.DataFrame(rows)
        df = df.merge(sector_cols, on="company_id", how="left")
        result[c] = df

    return result
