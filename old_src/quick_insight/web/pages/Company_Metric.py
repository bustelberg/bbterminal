# pages/1_company_metric.py
from __future__ import annotations

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from quick_insight.web.db import load_companies
from quick_insight.web.metrics import fetch_metrics, get_numeric_metric_codes
from quick_insight.web.ui import setup_page


# ============================================================
# Chart helpers (self-contained)
# ============================================================
def _axis_groups_by_scale(ts: pd.DataFrame, *, ratio_threshold: float = 50.0) -> dict[str, int]:
    g = ts.dropna(subset=["metric_value"]).groupby("metric_code")["metric_value"]
    stats = g.quantile([0.05, 0.95]).unstack(level=1)
    if stats.empty:
        return {m: 0 for m in ts["metric_code"].astype(str).unique()}

    stats.columns = ["p05", "p95"]
    stats["mag"] = np.maximum(stats["p05"].abs(), stats["p95"].abs())
    stats = stats.replace([np.inf, -np.inf], np.nan).dropna(subset=["mag"])

    metrics = ts["metric_code"].astype(str).unique()
    if len(stats) <= 1:
        return {m: 0 for m in metrics}

    eps = 1e-9
    mags = stats["mag"].clip(lower=eps)
    max_mag = float(mags.max())
    min_mag = float(mags.min())

    if min_mag <= eps or (max_mag / min_mag) < ratio_threshold:
        return {m: 0 for m in metrics}

    cutoff = float(np.sqrt(max_mag * min_mag))
    groups = {metric: (1 if float(mag) >= cutoff else 0) for metric, mag in mags.items()}

    if len(set(groups.values())) == 1:
        return {m: 0 for m in metrics}

    return groups


def company_metric_timeseries_auto_dual_axis_chart(
    ts: pd.DataFrame,
    *,
    height: int = 520,
    ratio_threshold: float = 50.0,
    log_scale: bool = False,
) -> alt.Chart:
    required = {"as_of_date", "metric_value", "metric_code"}
    missing = required - set(ts.columns)
    if missing:
        raise KeyError(f"Missing columns: {missing}. Got={list(ts.columns)}")

    axis_map = _axis_groups_by_scale(ts, ratio_threshold=ratio_threshold)
    df = ts.copy()
    df["axis_group"] = df["metric_code"].astype(str).map(axis_map).fillna(0).astype(int)

    sel = alt.selection_point(fields=["metric_code"], bind="legend")

    base = (
        alt.Chart(df)
        .add_params(sel)
        .transform_filter(sel)
        .encode(
            x=alt.X("as_of_date:T", title="Snapshot date"),
            color=alt.Color(
                "metric_code:N",
                title="Metric",
                legend=alt.Legend(orient="right", labelLimit=400, symbolLimit=0),
            ),
            tooltip=[
                alt.Tooltip("as_of_date:T", title="Date"),
                alt.Tooltip("metric_code:N", title="Metric"),
                alt.Tooltip("metric_value:Q", title="Value"),
            ],
        )
    )

    y_scale = alt.Scale(type="log") if log_scale else alt.Scale()

    left = (
        base.transform_filter(alt.datum.axis_group == 0)
        .mark_line(point=True)
        .encode(y=alt.Y("metric_value:Q", title="Value (group 1)", scale=y_scale))
    )

    if int(df["axis_group"].max()) == 0:
        return left.properties(height=height)

    right = (
        base.transform_filter(alt.datum.axis_group == 1)
        .mark_line(point=True)
        .encode(
            y=alt.Y(
                "metric_value:Q",
                title="Value (group 2)",
                axis=alt.Axis(orient="right"),
                scale=y_scale,
            )
        )
    )

    return alt.layer(left, right).resolve_scale(y="independent").properties(height=height)


# ============================================================
# Page setup + shared data
# ============================================================
companies = load_companies()
metric_options = get_numeric_metric_codes()

if companies.empty:
    setup_page()
    st.warning("No companies found in the DB yet.")
    st.stop()

if not metric_options:
    setup_page()
    st.warning("No numeric metrics found (metric.value_type='number').")
    st.stop()

companies = companies.fillna("")
labels = (
    companies["company_name"].astype(str)
    + " (Exchange: "
    + companies["primary_exchange"].astype(str)
    + ") (Sector: "
    + companies["sector"].astype(str)
    + ") (Country: "
    + companies["country"].astype(str)
    + ")"
).tolist()

label_to_ticker = dict(zip(labels, companies["primary_ticker"].astype(str).tolist()))
label_to_name = dict(zip(labels, companies["company_name"].astype(str).tolist()))

# -----------------------------
# Defaults
# -----------------------------
DEFAULT_TICKER = "MSFT"
DEFAULT_METRIC = "quarterly__Per Share Data__Month End Stock Price"

default_company_label = next(
    (lbl for lbl in labels if label_to_ticker.get(lbl) == DEFAULT_TICKER),
    labels[0],
)
default_company_index = labels.index(default_company_label) if default_company_label in labels else 0

default_metric_codes = [DEFAULT_METRIC] if DEFAULT_METRIC in metric_options else []


def controls() -> None:
    st.header("Settings")

    st.selectbox("Company", options=labels, index=default_company_index, key="cmot_company_label")

    st.multiselect(
        "Metrics",
        options=metric_options,
        default=default_metric_codes,
        key="cmot_metric_codes",
    )

    st.markdown("### Chart options")
    st.toggle("Log scale (Y-axis)", value=False, key="cmot_use_log_scale")


setup_page(controls=controls)


# ============================================================
# UI (main)
# ============================================================
st.title("Company Metric Over Time")

picked_label: str = st.session_state.get("cmot_company_label", default_company_label)
metric_codes: list[str] = st.session_state.get("cmot_metric_codes", default_metric_codes)
use_log_scale: bool = bool(st.session_state.get("cmot_use_log_scale", False))

ticker = label_to_ticker.get(picked_label, "")
company_name = label_to_name.get(picked_label, "")

if not ticker:
    st.warning("No company selected.")
    st.stop()

if not metric_codes:
    st.info("Select one or more metrics in the sidebar.")
    st.stop()

if len(metric_codes) > 10:
    st.warning("This gets messy beyond ~6–10 metrics. Consider selecting fewer.")

# Fetch long series via shared fetch_metrics and then filter/shape
raw = fetch_metrics(ticker, metric_codes)
ts = raw.loc[:, ["as_of_date", "metric_code", "metric_value"]].copy()

if ts.empty:
    st.warning(f"No data found for {ticker} / selected metrics.")
    st.stop()

st.subheader(f"{ticker} — {company_name}")
st.caption("Tip: click legend items to show/hide metrics.")

chart = company_metric_timeseries_auto_dual_axis_chart(
    ts,
    ratio_threshold=50.0,
    log_scale=use_log_scale,
)
st.altair_chart(chart, width="stretch")

with st.expander("Show data", expanded=False):
    st.dataframe(ts, width="stretch")
