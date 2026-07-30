"""The daily as-of signal engine.

Ported verbatim from `momentum/signals.py` (Phase 2). This module owns the MATH
and the as-of DISCIPLINE for the `daily_asof` cadence; `momentum/signals.py` is
now a thin adapter that joins the result to a universe. `asset_pipeline` can
adopt `evaluate_panel` to inherit the strict cutoff and staleness guard its own
month-end cadence lacks.

TWO PATHS, ONE ANSWER
    `compute_single_company_signals(series)` computes every signal at the LAST bar
    of a series — the readable definition.
    `price_panel(series)` / `volume_panel(vol)` compute the same signals as rolling
    time series in one pass, so a caller needing many cutoffs pays once.
    The two must agree bar-for-bar; `tests/test_signals.py::TestSignalsPanelParity`
    pins that, and it is the test that catches a lookahead regression in either.

THE AS-OF DISCIPLINE (`evaluate_panel`)
    * Strict `<` cutoff. `searchsorted(side="left") - 1` anchors on the last bar
      STRICTLY BEFORE the cutoff, so a signal can never see the close at which the
      trade is entered.
    * `MIN_BARS` history. Fewer than 20 bars before the cutoff and the entity is
      dropped rather than scored on noise.
    * `MAX_STALENESS_DAYS`. An entity whose newest bar is more than 30 days before
      the cutoff is dropped — delisted, halted, or a data gap. Without this, a
      signal anchors to a stale price and reads as a confident flat line.

ROUNDING IS PART OF THE DEFINITION
    Every signal rounds (2dp for percents, 4dp for ratios) and the rounding happens
    at specific points — e.g. `volatility_adjusted_return_6m` divides an ALREADY-
    ROUNDED `mom_6m` by an ALREADY-ROUNDED annualized vol. Reordering those rounds
    changes live holdings. See `signal_engine.registry` for each signal's units and
    `round_dp`.
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from .registry import by_cadence

# Reject entities whose last available price is older than this many days before
# the as-of cutoff — otherwise signals get anchored to stale prices (delisted /
# halted / data-gap names) instead of current state.
MAX_STALENESS_DAYS = 30

# Minimum bars strictly before the cutoff for an entity to be scored at all.
MIN_BARS = 20


def _names(group: str) -> tuple[str, ...]:
    return tuple(s.name for s in by_cadence("daily_asof") if s.group == group)


# Column order is the registry's declaration order, and it is load-bearing:
# `price_panel` returns price columns then trend columns, and the scoring engine
# reads them by name.
PRICE_COLUMNS = _names("price")
TREND_COLUMNS = _names("trend")
VOLUME_COLUMNS = _names("volume")


# ---------------------------------------------------------------------------
# Scalar helpers — operate on a single entity's Series, read at its last bar.
# ---------------------------------------------------------------------------

def mom_return(series: pd.Series, n_months: int) -> float | None:
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


def drawdown_from_recent_high_pct(series: pd.Series, lookback_days: int = 252) -> float | None:
    if series.empty:
        return None
    window = series.tail(lookback_days)
    if window.empty:
        return None
    recent_high = float(window.max())
    if recent_high == 0:
        return None
    return round((float(series.iloc[-1]) / recent_high - 1) * 100, 2)


def annualized_volatility_pct(series: pd.Series, lookback_days: int = 126) -> float | None:
    if len(series) < 3:
        return None
    daily_returns = series.pct_change().dropna().tail(lookback_days)
    if len(daily_returns) < 2:
        return None
    vol = float(daily_returns.std())
    if pd.isna(vol) or vol == 0:
        return None
    return round(vol * (252 ** 0.5) * 100, 2)


def volatility_adjusted_return(
    series: pd.Series, *, n_months: int = 6, vol_lookback_days: int = 126,
) -> float | None:
    ret = mom_return(series, n_months)
    vol = annualized_volatility_pct(series, lookback_days=vol_lookback_days)
    if ret is None or vol in (None, 0):
        return None
    return round(ret / vol, 4)


def volume_ratio(vol_series: pd.Series, short_window: int, long_window: int) -> float | None:
    """Ratio of short-term avg volume to long-term avg volume."""
    if len(vol_series) < long_window:
        return None
    short_avg = float(vol_series.tail(short_window).mean())
    long_avg = float(vol_series.tail(long_window).mean())
    if long_avg == 0:
        return None
    return round(short_avg / long_avg, 4)


def volume_trend(vol_series: pd.Series, n_months: int) -> float | None:
    """% change in average daily volume: recent month vs n_months ago.

    NOTE this is NOT the asset pipeline's `vol_trend_3m` (which compares month-end
    averages). Spearman between them is 0.58 — see `registry.PARITY`.
    """
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


def compute_volume_signals(vol_series: pd.Series) -> dict:
    """Volume signals for a single entity, at the series' last bar."""
    if vol_series.empty or len(vol_series) < MIN_BARS:
        return {}
    return {
        "vol_20d_vs_60d": volume_ratio(vol_series, 20, 60),
        "vol_trend_3m": volume_trend(vol_series, 3),
    }


def compute_single_company_signals(series: pd.Series) -> dict:
    """All price + trend signals for a single entity's price series."""
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

    out: dict = {
        "mom_12_1": mom_12_1,
        "mom_6m": mom_return(series, 6),
        "volatility_adjusted_return_6m": volatility_adjusted_return(series, n_months=6, vol_lookback_days=126),
        "drawdown_from_recent_high_pct": drawdown_from_recent_high_pct(series, lookback_days=252),
        "above_200ma": 1 if price_now > ma_200 else 0,
    }
    # Trend-quality signals (group="trend"). Computed via the SAME rolling
    # builder the vectorized panel uses, then read at the last bar — so the
    # per-cutoff and panel paths are byte-identical (validated in test_signals).
    trend = trend_panel(series)
    if not trend.empty:
        last = trend.iloc[-1]
        for k in TREND_COLUMNS:
            v = last.get(k)
            out[k] = float(v) if pd.notna(v) else None
    return out


# ---------------------------------------------------------------------------
# Vectorized panels — each row [d] equals what the scalar path would return on
# `series[series.index <= d]`. Parity pinned by tests/test_signals.py.
# ---------------------------------------------------------------------------

def asof_values(series: pd.Series, targets: pd.DatetimeIndex) -> np.ndarray:
    """For each `t` in `targets`, the value of `series` at the last index <= t
    (NaN if none). Vectorized via searchsorted; `series` must be sorted ascending."""
    if len(series) == 0:
        return np.full(len(targets), np.nan)
    positions = series.index.searchsorted(targets, side="right") - 1
    out = np.full(len(targets), np.nan)
    valid = positions >= 0
    if valid.any():
        out[valid] = series.values[positions[valid]]
    return out


def trend_panel(series: pd.Series) -> pd.DataFrame:
    """Trend-quality signals as rolling time series — the shared source of truth
    for both the scalar (`compute_single_company_signals`) and vectorized
    (`price_panel`) paths, so they stay byte-identical. All windows are trailing
    and the 6M anchor is an as-of lookback, so the value at bar `t` depends only
    on data up to and including `t`.

      - trend_continuity: sign(6M return) × (%up − %down) over 126 trading days.
      - pct_up_days_6m:    %up-days over 126 trading days × 100.
      - rsi_headroom:      −max(0, RSI(14) − 50)  (overbought guard).
    """
    idx = series.index
    if len(series) < MIN_BARS:
        return pd.DataFrame(index=idx, columns=list(TREND_COLUMNS), dtype="float64")

    rets = series.pct_change()
    pos_frac = (rets > 0).astype(float).rolling(126, min_periods=20).mean()
    neg_frac = (rets < 0).astype(float).rolling(126, min_periods=20).mean()
    pct_up = np.round(pos_frac.values * 100.0, 2)

    # 6-month cumulative-return sign via asof(t-6m) — matches mom_6m's anchor.
    targets_6m = idx - pd.DateOffset(months=6)
    num_6m = asof_values(series, targets_6m)
    with np.errstate(divide="ignore", invalid="ignore"):
        cum6 = np.where((num_6m > 0) & ~np.isnan(num_6m), series.values / num_6m - 1.0, np.nan)
    sign6 = np.sign(cum6)
    pf, nf = pos_frac.values, neg_frac.values
    continuity = np.where(
        (~np.isnan(sign6)) & (sign6 != 0) & ~np.isnan(pf) & ~np.isnan(nf),
        sign6 * (pf - nf),
        np.nan,
    )
    continuity = np.round(continuity, 4)

    # RSI(14), simple-average gains/losses. min_periods=14 → NaN until a full
    # 14-bar window clear of the leading pct_change NaN (the scalar guard
    # mirrors this), so the two paths agree.
    up = rets.clip(lower=0)
    down = (-rets).clip(lower=0)
    avg_up = up.rolling(14, min_periods=14).mean().values
    avg_down = down.rolling(14, min_periods=14).mean().values
    with np.errstate(divide="ignore", invalid="ignore"):
        rs = np.where(avg_down > 0, avg_up / avg_down, np.nan)
        rsi = np.where(avg_down == 0, 100.0, 100.0 - 100.0 / (1.0 + rs))
    rsi = np.where(np.isnan(avg_up) | np.isnan(avg_down), np.nan, rsi)
    rsi_headroom = np.where(np.isnan(rsi), np.nan, -np.maximum(0.0, rsi - 50.0))
    rsi_headroom = np.round(rsi_headroom, 2)

    return pd.DataFrame(
        {
            "trend_continuity": continuity,
            "pct_up_days_6m": pct_up,
            "rsi_headroom": rsi_headroom,
        },
        index=idx,
    )


def price_panel(series: pd.Series) -> pd.DataFrame:
    """All price + trend signals as time series, same index as `series`."""
    if len(series) < MIN_BARS:
        return pd.DataFrame(
            index=series.index,
            columns=list(PRICE_COLUMNS) + list(TREND_COLUMNS),
            dtype="float64",
        )

    idx = series.index

    # mom_12_1: at each t, (asof(t-1m)) / (asof(t-12m)) - 1, in %.
    targets_1m = idx - pd.DateOffset(months=1)
    targets_12m = idx - pd.DateOffset(months=12)
    num_1m = asof_values(series, targets_1m)
    num_12m = asof_values(series, targets_12m)
    with np.errstate(divide="ignore", invalid="ignore"):
        mom_12_1 = np.where(
            (num_12m > 0) & ~np.isnan(num_1m) & ~np.isnan(num_12m),
            (num_1m / num_12m - 1.0) * 100.0,
            np.nan,
        )
    mom_12_1 = np.round(mom_12_1, 2)

    # mom_6m: at each t, series[t] / asof(t-6m) - 1.
    targets_6m = idx - pd.DateOffset(months=6)
    num_6m = asof_values(series, targets_6m)
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
    # over a 126-day window. The scalar path calls .dropna().tail(126) on
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

    # above_200ma: 1 if price > MA200 else 0. The scalar path returns 0/1 (int);
    # same semantics here. ma==0 returns NaN (matches its `if ma == 0: return None`,
    # though in practice ma is always positive).
    ma_200 = series.rolling(200, min_periods=1).mean().values
    above = np.where(
        ma_200 == 0,
        np.nan,
        (series_vals > ma_200).astype(float),
    )

    panel = pd.DataFrame(
        {
            "mom_12_1": mom_12_1,
            "mom_6m": mom_6m,
            "volatility_adjusted_return_6m": vol_adj,
            "drawdown_from_recent_high_pct": drawdown,
            "above_200ma": above,
        },
        index=idx,
    )
    # Append the trend-quality columns so the panel row carries every signal
    # (the scoring engine only activates them for the MomentumExtra strategy).
    return pd.concat([panel, trend_panel(series)], axis=1)


def volume_panel(vol_series: pd.Series) -> pd.DataFrame:
    """Volume signals as time series — parity target is `compute_volume_signals`."""
    if vol_series.empty or len(vol_series) < MIN_BARS:
        return pd.DataFrame(index=vol_series.index, columns=list(VOLUME_COLUMNS), dtype="float64")

    idx = vol_series.index

    # vol_20d_vs_60d = mean(last 20) / mean(last 60). The scalar path requires
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
    # The asymmetric boundary semantics come from the scalar path's use of
    # `index > recent_cutoff` (strict) for recent vs `index >= past_cutoff`
    # (non-strict) for past.
    #
    # Recent: closed='right' time-based rolling on the original (business-day)
    # index — at each bar t, window = (t - 21D, t]. Direct match.
    #
    # Past: the right edge of the window is `t - 3m + 21d`, a calendar date that
    # often falls on a non-trading day. To look up at that exact calendar date
    # (instead of falling back to the prior trading day, which would trim the
    # right edge), compute the past rolling on a daily calendar reindex — NaNs at
    # non-trading days are excluded by mean().
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


# ---------------------------------------------------------------------------
# The evaluator — strict `<` cutoff, MIN_BARS, staleness. Entity-agnostic.
# ---------------------------------------------------------------------------

def evaluate_panel(
    entity_ids: list[int],
    cutoffs: list[date],
    *,
    price_index: dict[int, pd.Series],
    volume_index: dict[int, pd.Series] | None = None,
    id_col: str = "entity_id",
) -> dict[pd.Timestamp, list[dict]]:
    """Signals for every (entity, cutoff), honoring the as-of discipline.

    Builds each entity's rolling signal panel once, then indexes into it per
    cutoff — the lookback windows are identical, only the anchor moves.

    Returns `{cutoff_timestamp: [row, ...]}` where a row is `{id_col: id, **signals}`.
    An entity is absent from a cutoff when it has <20 bars strictly before it, or
    its newest such bar is more than `MAX_STALENESS_DAYS` old.
    """
    if not cutoffs:
        return {}

    cutoff_ts = [pd.Timestamp(c) for c in cutoffs]
    cutoff_ts_index = pd.DatetimeIndex(cutoff_ts)
    per_cutoff: dict[pd.Timestamp, list[dict]] = {c: [] for c in cutoff_ts}

    for eid in entity_ids:
        eid = int(eid)
        series = price_index.get(eid)
        if series is None or len(series) < MIN_BARS:
            continue

        panel = price_panel(series)
        if panel.empty:
            continue

        vol_series = volume_index.get(eid) if volume_index is not None else None
        vol_panel = (
            volume_panel(vol_series)
            if vol_series is not None and len(vol_series) > 0
            else None
        )

        price_idx = series.index
        # Strict `<`: the last bar BEFORE the cutoff, never the cutoff's own bar.
        positions = price_idx.searchsorted(cutoff_ts_index, side="left") - 1

        for c_ts, pos in zip(cutoff_ts, positions):
            if pos < 0:
                continue
            anchor = price_idx[pos]
            if pos + 1 < MIN_BARS:
                continue
            if (c_ts - anchor).days > MAX_STALENESS_DAYS:
                continue

            row = {id_col: eid}
            row.update(panel.iloc[pos].to_dict())

            if vol_panel is not None:
                vol_pos = vol_panel.index.searchsorted(anchor, side="right") - 1
                if vol_pos >= 0 and vol_pos + 1 >= MIN_BARS:
                    vol_row = vol_panel.iloc[vol_pos]
                    for k in VOLUME_COLUMNS:
                        v = vol_row.get(k)
                        if pd.notna(v):
                            row[k] = float(v)

            per_cutoff[c_ts].append(row)

    return per_cutoff
