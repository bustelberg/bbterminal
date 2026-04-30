"""Explainability helpers for momentum signals.

Each `explain_*` function mirrors the math of its `signals.py` counterpart
but also returns the intermediate basis numbers (anchor dates + prices,
windowed averages, etc.) so the UI can show "this number came from these
ones". The trimmed price/volume series passed in must already have the
strict-< as_of_date filter applied — same shape `compute_signals_panel`
sees per company.
"""
from __future__ import annotations

from typing import TypedDict

import pandas as pd


class Component(TypedDict, total=False):
    label: str
    value_str: str


class SignalExplain(TypedDict, total=False):
    value: float | None
    components: list[Component]


def _date_str(d) -> str:
    return pd.Timestamp(d).date().isoformat()


def _fmt_price(v: float) -> str:
    if abs(v) >= 1000:
        return f"{v:,.2f}"
    if abs(v) >= 10:
        return f"{v:.2f}"
    return f"{v:.4f}"


def _fmt_volume(v: float) -> str:
    return f"{v:,.0f}"


def explain_mom_12_1(series: pd.Series) -> SignalExplain:
    """12-1 momentum: (price 1m back / price 12m back) − 1, in %."""
    if series.empty:
        return {"value": None, "components": [{"label": "No price history available."}]}
    skip_last_cutoff = series.index[-1] - pd.DateOffset(months=1)
    series_skip = series[series.index <= skip_last_cutoff]
    cutoff_12m = series.index[-1] - pd.DateOffset(months=12)
    past_12m = series[series.index <= cutoff_12m]
    if past_12m.empty or series_skip.empty:
        return {"value": None, "components": [{"label": "Insufficient history (need ≥12 months before the as-of date)."}]}
    p_12m = float(past_12m.iloc[-1])
    d_12m = past_12m.index[-1]
    p_1m = float(series_skip.iloc[-1])
    d_1m = series_skip.index[-1]
    if p_12m == 0:
        return {"value": None, "components": [{"label": f"Price 12 months back ({_date_str(d_12m)}) was 0 — undefined ratio."}]}
    value = round((p_1m / p_12m - 1) * 100, 2)
    return {
        "value": value,
        "components": [
            {"label": f"Price 12 months back (latest close ≤ {_date_str(cutoff_12m)})", "value_str": f"{_fmt_price(p_12m)} on {_date_str(d_12m)}"},
            {"label": f"Price 1 month back (latest close ≤ {_date_str(skip_last_cutoff)})", "value_str": f"{_fmt_price(p_1m)} on {_date_str(d_1m)}"},
            {"label": "Formula", "value_str": f"({_fmt_price(p_1m)} / {_fmt_price(p_12m)} − 1) × 100 = {value:.2f}%"},
        ],
    }


def explain_mom_6m(series: pd.Series) -> SignalExplain:
    """6-month momentum: (latest price / price 6m back) − 1, in %."""
    if series.empty:
        return {"value": None, "components": [{"label": "No price history."}]}
    cutoff = series.index[-1] - pd.DateOffset(months=6)
    past = series[series.index <= cutoff]
    if past.empty:
        return {"value": None, "components": [{"label": "Insufficient history (need ≥6 months)."}]}
    p_now = float(series.iloc[-1])
    d_now = series.index[-1]
    p_past = float(past.iloc[-1])
    d_past = past.index[-1]
    if p_past == 0:
        return {"value": None, "components": [{"label": "Price 6 months back was 0 — undefined ratio."}]}
    value = round((p_now / p_past - 1) * 100, 2)
    return {
        "value": value,
        "components": [
            {"label": "Anchor price (latest close before as-of)", "value_str": f"{_fmt_price(p_now)} on {_date_str(d_now)}"},
            {"label": f"Price 6 months back (latest close ≤ {_date_str(cutoff)})", "value_str": f"{_fmt_price(p_past)} on {_date_str(d_past)}"},
            {"label": "Formula", "value_str": f"({_fmt_price(p_now)} / {_fmt_price(p_past)} − 1) × 100 = {value:.2f}%"},
        ],
    }


def explain_volatility_adjusted_return_6m(series: pd.Series) -> SignalExplain:
    """6-month return ÷ annualized volatility (last 126 daily returns)."""
    mom6 = explain_mom_6m(series)
    if mom6["value"] is None:
        return {"value": None, "components": mom6["components"]}
    if len(series) < 3:
        return {"value": None, "components": [{"label": "Insufficient history for volatility."}]}
    daily_returns = series.pct_change().dropna().tail(126)
    if len(daily_returns) < 2:
        return {"value": None, "components": [{"label": "Need ≥2 daily returns for volatility."}]}
    daily_std = float(daily_returns.std())
    if daily_std == 0:
        return {"value": None, "components": [{"label": "Daily-return std dev is 0 — undefined ratio."}]}
    annualized_vol_pct = round(daily_std * (252 ** 0.5) * 100, 2)
    if annualized_vol_pct == 0:
        return {"value": None, "components": [{"label": "Annualized volatility rounds to 0% — undefined ratio."}]}
    value = round(mom6["value"] / annualized_vol_pct, 4)
    return {
        "value": value,
        "components": [
            {"label": "6-month return (from mom_6m)", "value_str": f"{mom6['value']:.2f}%"},
            {"label": f"Daily-return std dev (last {len(daily_returns)} days)", "value_str": f"{daily_std:.6f}"},
            {"label": "Annualized vol = std × √252 × 100", "value_str": f"{annualized_vol_pct:.2f}%"},
            {"label": "Formula", "value_str": f"{mom6['value']:.2f}% ÷ {annualized_vol_pct:.2f}% = {value:.4f}"},
        ],
    }


def explain_drawdown_from_recent_high_pct(series: pd.Series) -> SignalExplain:
    """Drawdown from 252-day rolling high: (price / max_252d) − 1, in %."""
    if series.empty:
        return {"value": None, "components": [{"label": "No price history."}]}
    window = series.tail(252)
    if window.empty:
        return {"value": None, "components": [{"label": "Insufficient history."}]}
    recent_high = float(window.max())
    high_date = window.idxmax()
    p_now = float(series.iloc[-1])
    d_now = series.index[-1]
    if recent_high == 0:
        return {"value": None, "components": [{"label": "Recent high was 0 — undefined ratio."}]}
    value = round((p_now / recent_high - 1) * 100, 2)
    return {
        "value": value,
        "components": [
            {"label": "Anchor price (latest close)", "value_str": f"{_fmt_price(p_now)} on {_date_str(d_now)}"},
            {"label": f"52-week high (last {len(window)} trading days)", "value_str": f"{_fmt_price(recent_high)} on {_date_str(high_date)}"},
            {"label": "Formula", "value_str": f"({_fmt_price(p_now)} / {_fmt_price(recent_high)} − 1) × 100 = {value:.2f}%"},
        ],
    }


def explain_above_200ma(series: pd.Series) -> SignalExplain:
    """1 if latest price > 200-day MA else 0 (uses available history if <200)."""
    if series.empty:
        return {"value": None, "components": [{"label": "No price history."}]}
    if len(series) >= 200:
        ma = float(series.tail(200).mean())
        ma_label = "200-day moving average"
    else:
        ma = float(series.mean())
        ma_label = f"Average over the {len(series)} available days (less than 200)"
    p_now = float(series.iloc[-1])
    d_now = series.index[-1]
    value = 1 if p_now > ma else 0
    return {
        "value": value,
        "components": [
            {"label": "Anchor price (latest close)", "value_str": f"{_fmt_price(p_now)} on {_date_str(d_now)}"},
            {"label": ma_label, "value_str": _fmt_price(ma)},
            {"label": "Formula", "value_str": f"price > MA → {value} ({_fmt_price(p_now)} {'>' if p_now > ma else '≤'} {_fmt_price(ma)})"},
        ],
    }


def explain_vol_20d_vs_60d(vol_series: pd.Series) -> SignalExplain:
    """Ratio of 20-day to 60-day average volume."""
    if len(vol_series) < 60:
        return {"value": None, "components": [{"label": f"Insufficient volume history (need ≥60 days, have {len(vol_series)})."}]}
    short_avg = float(vol_series.tail(20).mean())
    long_avg = float(vol_series.tail(60).mean())
    if long_avg == 0:
        return {"value": None, "components": [{"label": "60-day avg volume is 0 — undefined ratio."}]}
    value = round(short_avg / long_avg, 4)
    return {
        "value": value,
        "components": [
            {"label": "Avg volume last 20 days", "value_str": _fmt_volume(short_avg)},
            {"label": "Avg volume last 60 days", "value_str": _fmt_volume(long_avg)},
            {"label": "Formula", "value_str": f"{_fmt_volume(short_avg)} / {_fmt_volume(long_avg)} = {value:.4f}"},
        ],
    }


def explain_vol_trend_3m(vol_series: pd.Series) -> SignalExplain:
    """% change in avg daily volume: recent month vs 3 months ago."""
    if vol_series.empty:
        return {"value": None, "components": [{"label": "No volume history."}]}
    recent_cutoff = vol_series.index[-1] - pd.DateOffset(days=21)
    past_cutoff = vol_series.index[-1] - pd.DateOffset(months=3)
    past_end = past_cutoff + pd.DateOffset(days=21)
    recent = vol_series[vol_series.index > recent_cutoff]
    past = vol_series[(vol_series.index >= past_cutoff) & (vol_series.index <= past_end)]
    if recent.empty or past.empty:
        return {"value": None, "components": [{"label": "Insufficient volume history for either window."}]}
    recent_avg = float(recent.mean())
    past_avg = float(past.mean())
    if past_avg == 0:
        return {"value": None, "components": [{"label": "Past-window avg is 0 — undefined ratio."}]}
    value = round((recent_avg / past_avg - 1) * 100, 2)
    return {
        "value": value,
        "components": [
            {"label": f"Recent: avg volume after {_date_str(recent_cutoff)} ({len(recent)} days)", "value_str": _fmt_volume(recent_avg)},
            {"label": f"Past: avg volume {_date_str(past_cutoff)}…{_date_str(past_end)} ({len(past)} days)", "value_str": _fmt_volume(past_avg)},
            {"label": "Formula", "value_str": f"({_fmt_volume(recent_avg)} / {_fmt_volume(past_avg)} − 1) × 100 = {value:.2f}%"},
        ],
    }


def explain_all_signals(price_series: pd.Series, volume_series: pd.Series | None) -> dict[str, SignalExplain]:
    """Run every signal's explainer in order. Caller is responsible for
    pre-trimming both series with the strict-< as_of_date filter."""
    out: dict[str, SignalExplain] = {
        "mom_12_1": explain_mom_12_1(price_series),
        "mom_6m": explain_mom_6m(price_series),
        "volatility_adjusted_return_6m": explain_volatility_adjusted_return_6m(price_series),
        "drawdown_from_recent_high_pct": explain_drawdown_from_recent_high_pct(price_series),
        "above_200ma": explain_above_200ma(price_series),
    }
    if volume_series is not None and not volume_series.empty:
        out["vol_20d_vs_60d"] = explain_vol_20d_vs_60d(volume_series)
        out["vol_trend_3m"] = explain_vol_trend_3m(volume_series)
    return out
