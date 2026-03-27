# src/quick_insight/web/pages/earnings_dashboard_helpers/transforms.py
from __future__ import annotations

import numpy as np
import pandas as pd

from quick_insight.web.helpers.earnings_dashboard_helpers.constants import METRIC, SERIES

# ----------------------------
# Math helpers
# ----------------------------
def compute_cagr(values, dates, *, require_positive: bool = True, min_points: int = 2) -> float | None:
    v = pd.to_numeric(pd.Series(values), errors="coerce").replace([np.inf, -np.inf], np.nan).to_numpy()
    d = pd.to_datetime(pd.Series(dates), errors="coerce").to_numpy()

    n = min(len(v), len(d))
    v, d = v[:n], d[:n]

    mask = (~np.isnan(v)) & (~pd.isna(d))
    if require_positive:
        mask &= v > 0

    idx = np.where(mask)[0]
    if idx.size < int(min_points):
        return None

    i0, i1 = int(idx[0]), int(idx[-1])
    start_val, end_val = float(v[i0]), float(v[i1])
    if not (np.isfinite(start_val) and np.isfinite(end_val)):
        return None
    if require_positive and (start_val <= 0 or end_val <= 0):
        return None

    start_date, end_date = pd.Timestamp(d[i0]), pd.Timestamp(d[i1])
    years = (end_date - start_date).days / 365.25
    if years <= 0:
        return None
    return (end_val / start_val) ** (1.0 / years) - 1.0


def compute_cagr_window(values, dates, *, years: float, require_positive: bool = True, min_points: int = 2) -> float | None:
    s = pd.DataFrame({"d": pd.to_datetime(dates, errors="coerce"), "v": pd.to_numeric(values, errors="coerce")})
    s = s.replace([np.inf, -np.inf], np.nan).dropna(subset=["d", "v"]).sort_values("d")
    if s.empty:
        return None

    end = s["d"].max()
    start = end - pd.Timedelta(days=int(years * 365.25))
    s = s[s["d"] >= start]
    if s.empty:
        return None

    if require_positive:
        s = s[s["v"] > 0]
    if len(s) < int(min_points):
        return None

    return compute_cagr(s["v"].to_numpy(), s["d"].to_numpy(), require_positive=require_positive, min_points=min_points)


def index_to_100(values: pd.Series, base_value: float | None) -> pd.Series:
    if base_value is None or not np.isfinite(base_value) or base_value == 0:
        return pd.Series(index=values.index, data=np.nan, dtype="float64")
    v = pd.to_numeric(values, errors="coerce")
    return 100.0 * (v / float(base_value))


# ----------------------------
# Data transforms for chart 2
# ----------------------------
def build_relative_growth(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    raw must have: as_of_date, metric_code, metric_value (already clean/parsed dates encouraged).
    Returns: (long df for plot), (wide plot df), (cagr dict)
    """
    wide = raw.pivot_table(index="as_of_date", columns="metric_code", values="metric_value", aggfunc="last").sort_index()

    wide["price"] = wide.get(METRIC["PRICE"], np.nan)

    wide["oe_actual"] = np.nan
    if METRIC["EPS_WO_NRI"] in wide.columns and METRIC["DIV_PS"] in wide.columns:
        wide["oe_actual"] = wide[METRIC["EPS_WO_NRI"]] + wide[METRIC["DIV_PS"]]

    wide["oe_est"] = np.nan
    if METRIC["EPS_EST"] in wide.columns and METRIC["DIV_EST"] in wide.columns:
        wide["oe_est"] = wide[METRIC["EPS_EST"]] + wide[METRIC["DIV_EST"]]

    mask_base = wide["price"].notna() & wide["oe_actual"].notna() & (wide["price"] > 0) & (wide["oe_actual"] > 0)
    if not mask_base.any():
        return pd.DataFrame(), pd.DataFrame(), {}

    base_date = wide.index[mask_base].min()
    base_price = float(wide.loc[base_date, "price"])
    base_oe = float(wide.loc[base_date, "oe_actual"])

    wide["price_idx"] = index_to_100(wide["price"], base_price)
    wide["oe_actual_idx"] = index_to_100(wide["oe_actual"], base_oe)
    wide["oe_est_idx"] = index_to_100(wide["oe_est"], base_oe)

    plot = wide.loc[wide.index >= base_date].copy()

    last_actual = plot.index[plot["oe_actual_idx"].notna()].max() if plot["oe_actual_idx"].notna().any() else None
    first_est = plot.index[plot["oe_est_idx"].notna()].min() if plot["oe_est_idx"].notna().any() else None

    frames: list[pd.DataFrame] = [
        plot[["price", "price_idx"]]
        .rename(columns={"price": "raw_value", "price_idx": "index_value"})
        .assign(series_name=SERIES["PRICE"])
    ]

    if last_actual is not None:
        frames.append(
            plot.loc[:last_actual, ["oe_actual", "oe_actual_idx"]]
            .rename(columns={"oe_actual": "raw_value", "oe_actual_idx": "index_value"})
            .assign(series_name=SERIES["OE_ACT"])
        )

    if first_est is not None:
        df_est = (
            plot.loc[first_est:, ["oe_est", "oe_est_idx"]]
            .rename(columns={"oe_est": "raw_value", "oe_est_idx": "index_value"})
            .assign(series_name=SERIES["OE_EST"])
        )
        if last_actual is not None and last_actual < first_est:
            bridge = pd.DataFrame(
                {
                    "raw_value": [float(plot.loc[last_actual, "oe_actual"])],
                    "index_value": [float(plot.loc[last_actual, "oe_actual_idx"])],
                    "series_name": [SERIES["OE_EST"]],
                },
                index=pd.DatetimeIndex([last_actual]),
            )
            df_est = pd.concat([bridge, df_est], axis=0)
        frames.append(df_est)

    df_rel = (
        pd.concat(frames, axis=0)
        .reset_index()
        .rename(columns={"index": "as_of_date"})
        .dropna(subset=["as_of_date", "series_name", "raw_value", "index_value"])
    )
    df_rel = df_rel[df_rel["index_value"] > 0].copy()

    cagr = {
        "price": compute_cagr(plot["price"].to_numpy(), plot.index, require_positive=True),
        "oe_act": compute_cagr(plot["oe_actual"].to_numpy(), plot.index, require_positive=True),
        "oe_est": compute_cagr(plot["oe_est"].to_numpy(), plot.index, require_positive=True),
    }
    return df_rel, plot, cagr
