# src/quick_insight/web/pages/3_mynewchart.py
from __future__ import annotations

import pandas as pd
import streamlit as st

from quick_insight.web.db import (
    fetch_metric_distribution_snapshot,
    get_company_lookup,
    get_numeric_metrics_for_snapshot,
    get_snapshot_dates,
    
)
from quick_insight.web.charts import (
    metric_distribution_chart,  # interactive (selection + grey-out)
    metric_distribution_chart_with_highlight,  # visual (vertical highlight line)
)

st.title("Metric Distribution")
st.info("Histogram of one numeric metric across all companies in a single snapshot.")

# -----------------------
# Snapshot + metric setup
# -----------------------
snapshots = get_snapshot_dates()
if not snapshots:
    st.warning("No snapshots found in the DB yet.")
    st.stop()

with st.sidebar:
    st.header("Settings")
    as_of_date = st.selectbox(
        "Snapshot date",
        options=snapshots,
        index=len(snapshots) - 1,
    )

metric_choices = get_numeric_metrics_for_snapshot(as_of_date)
if not metric_choices:
    st.warning(f"No numeric metrics found for snapshot {as_of_date}.")
    st.stop()

with st.sidebar:
    metric_code = st.selectbox("Metric", options=metric_choices, index=0)
    bins = st.slider("Bins", min_value=10, max_value=80, value=30, step=5)
    clip_outliers = st.toggle("Clip outliers (1%–99%)", value=False)

# -----------------------
# Load distribution data
# -----------------------
df = fetch_metric_distribution_snapshot(as_of_date, metric_code=metric_code)
if df.empty:
    st.warning(f"No values found for metric '{metric_code}' in snapshot {as_of_date}.")
    st.stop()

df = df.copy()
df["metric_value"] = pd.to_numeric(df["metric_value"], errors="coerce")
df = df.dropna(subset=["metric_value"]).reset_index(drop=True)

# Optional: clip outliers (OFF by default)
clip_lo = clip_hi = None
if clip_outliers and df["metric_value"].notna().any():
    clip_lo = float(df["metric_value"].quantile(0.01))
    clip_hi = float(df["metric_value"].quantile(0.99))
    df = df[df["metric_value"].between(clip_lo, clip_hi)].copy()

# Optional filters
with st.sidebar:
    st.divider()
    st.subheader("Filters")

    sectors = sorted([s for s in df["sector"].dropna().unique().tolist() if str(s).strip()])
    countries = sorted([c for c in df["country"].dropna().unique().tolist() if str(c).strip()])

    selected_sectors = st.multiselect("Sector", options=sectors, default=[])
    selected_countries = st.multiselect("Country", options=countries, default=[])

if selected_sectors:
    df = df[df["sector"].isin(selected_sectors)].copy()
if selected_countries:
    df = df[df["country"].isin(selected_countries)].copy()

# -----------------------
# Highlight company (optional)
# -----------------------
# Your get_company_lookup() returns primary_ticker (not ticker)
companies = get_company_lookup()
highlight_ticker: str | None = None
if not companies.empty:
    companies = companies.copy()
    companies["label"] = (
        companies["primary_ticker"].astype(str)
        + " — "
        + companies["company_name"].astype(str)
    )
    with st.sidebar:
        st.divider()
        st.subheader("Highlight")
        pick = st.selectbox("Company", ["(none)"] + companies["label"].tolist(), index=0)
    if pick != "(none)":
        highlight_ticker = companies.loc[companies["label"] == pick, "primary_ticker"].iloc[0]

st.caption(
    f"Snapshot: **{as_of_date}** · Metric: **{metric_code}** · Companies: **{len(df)}**"
    + (f" · Clipped to **[{clip_lo:,.4f}, {clip_hi:,.4f}]**" if clip_outliers else "")
)

# Compute highlight value (for vertical line)
highlight_value: float | None = None
if highlight_ticker is not None and "ticker" in df.columns:
    row = df.loc[df["ticker"] == highlight_ticker, "metric_value"]
    if not row.empty and pd.notna(row.iloc[0]):
        highlight_value = float(row.iloc[0])

# -----------------------
# 1) Visual chart (layered) with vertical highlight line
# -----------------------
st.caption("Chart (visual) — shows highlight line (if selected)")
chart_visual = metric_distribution_chart_with_highlight(
    df,
    metric_title=metric_code,
    bins=bins,
    highlight_value=highlight_value,
    height=420,
)
st.altair_chart(chart_visual, width="stretch")

# -----------------------
# 2) Interactive chart (single-view) for selecting a bin
# -----------------------
st.caption("Chart (interactive) — click a bar to filter tables below (click again to clear)")
chart_interactive = metric_distribution_chart(
    df,
    metric_title=metric_code,
    bins=bins,
    height=260,
)

selection = st.altair_chart(chart_interactive, width="stretch", on_select="rerun")


def _extract_bin(sel_obj: object) -> tuple[float | None, float | None]:
    """
    Streamlit selection payload varies by version. Handle common shapes.
    """
    if not isinstance(sel_obj, dict):
        return (None, None)

    s = sel_obj.get("selection")
    if isinstance(s, dict):
        b = s.get("bar_sel")
        if isinstance(b, dict) and "bin_start" in b and "bin_end" in b:
            try:
                return (float(b["bin_start"]), float(b["bin_end"]))
            except Exception:
                return (None, None)

    # fallback shapes
    b2 = sel_obj.get("bar_sel")
    if isinstance(b2, dict) and "bin_start" in b2 and "bin_end" in b2:
        try:
            return (float(b2["bin_start"]), float(b2["bin_end"]))
        except Exception:
            return (None, None)

    if isinstance(s, dict):
        bl = s.get("bar_sel")
        if isinstance(bl, list) and bl:
            first = bl[0]
            if isinstance(first, dict) and "bin_start" in first and "bin_end" in first:
                try:
                    return (float(first["bin_start"]), float(first["bin_end"]))
                except Exception:
                    return (None, None)

    return (None, None)


bin_lo, bin_hi = _extract_bin(selection)

# Filter raw rows if a bin is selected
selected_df = df
if bin_lo is not None and bin_hi is not None:
    selected_df = df[df["metric_value"].between(bin_lo, bin_hi, inclusive="left")].copy()

# Reliable clear button
if bin_lo is not None and bin_hi is not None:
    if st.button("Clear selection"):
        st.rerun()

# Existing table (full dataset)
with st.expander("Show data", expanded=False):
    st.dataframe(
        df.sort_values(["sector", "ticker"]).reset_index(drop=True),
        width="stretch",
    )

# New table: raw rows in selected bin
st.subheader("Selected bin — raw rows")
if bin_lo is None or bin_hi is None:
    st.caption("No bin selected. Click a bar in the interactive histogram to see the underlying rows here.")
else:
    st.caption(f"Selected bin: **{bin_lo:,.6f} → {bin_hi:,.6f}** · Rows: **{len(selected_df)}**")
    st.dataframe(
        selected_df.sort_values(["sector", "ticker"]).reset_index(drop=True),
        width="stretch",
    )
