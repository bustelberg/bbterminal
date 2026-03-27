# src/quick_insight/web/pages/4_top_correlations.py
from __future__ import annotations

import pandas as pd
import streamlit as st

from quick_insight.web.db import fetch_snapshot_numeric_matrix, get_snapshot_dates
from quick_insight.web.charts import top_correlations_chart


# -----------------------------
# Helper: metric family by first token
# -----------------------------
def metric_family(metric: str) -> str:
    """
    Returns the first token before '_' as the metric family.

    Examples:
      rank               -> rank
      rank_roc           -> rank
      rank_5yr_roc       -> rank
      cash_roc_current   -> cash
      revenue_growth_5yr -> revenue
      fcf_margin_avg     -> fcf
    """
    return metric.split("_", 1)[0].lower().strip()


# -----------------------------
# Page
# -----------------------------
st.title("Strongest Metric Relationships")

st.info(
    "For one snapshot, this computes correlations between numeric metrics across companies and shows the "
    "strongest relationships ranked by absolute correlation (positive and negative).\n\n"
    "By default, correlations between metrics from the same *metric family* "
    "(same first word before `_`) are excluded to avoid obvious or mechanical relationships."
)

snapshots = get_snapshot_dates()
if not snapshots:
    st.warning("No snapshots found in the DB yet.")
    st.stop()

with st.sidebar:
    st.header("Settings")

    as_of_date = st.selectbox("Snapshot date", options=snapshots, index=len(snapshots) - 1)
    method = st.selectbox("Correlation method", options=["pearson", "spearman"], index=0)
    top_n = st.slider("Top N pairs", min_value=10, max_value=200, value=50, step=10)
    min_obs = st.slider("Min overlapping observations per pair", min_value=10, max_value=200, value=30, step=5)

    exclude_same_family = st.toggle(
        "Exclude same metric family (same first word)",
        value=True,
    )

    st.divider()
    st.subheader("Filters")

# Load wide matrix first WITHOUT filters to populate selector choices
wide_all = fetch_snapshot_numeric_matrix(as_of_date)
if wide_all.empty:
    st.warning(f"No numeric data found for snapshot {as_of_date}.")
    st.stop()

sector_choices = sorted([s for s in wide_all["sector"].dropna().unique().tolist() if str(s).strip()])
country_choices = sorted([c for c in wide_all["country"].dropna().unique().tolist() if str(c).strip()])

with st.sidebar:
    sector = st.selectbox("Sector (optional)", options=["(all)"] + sector_choices, index=0)
    country = st.selectbox("Country (optional)", options=["(all)"] + country_choices, index=0)

sector_filter = None if sector == "(all)" else sector
country_filter = None if country == "(all)" else country

wide = fetch_snapshot_numeric_matrix(as_of_date, sector=sector_filter, country=country_filter)
if wide.empty:
    st.warning("No data after applying filters.")
    st.stop()

# Identify numeric metric columns (exclude metadata)
meta_cols = {"ticker", "company_name", "sector", "country", "exchange"}
metric_cols = [c for c in wide.columns if c not in meta_cols]

# Keep only numeric columns
X = wide[metric_cols].apply(pd.to_numeric, errors="coerce")

# Drop metrics that are too sparse overall
non_null_counts = X.notna().sum(axis=0)
usable_cols = non_null_counts[non_null_counts >= min_obs].index.tolist()
X = X[usable_cols]

if X.shape[1] < 2:
    st.warning("Not enough usable metrics (after min_obs filtering) to compute correlations.")
    st.stop()

# Compute correlation matrix
corr = X.corr(method=method, min_periods=min_obs)

# Pairwise overlap counts (N per pair)
mask = X.notna().astype("int64")
n_overlap = mask.T @ mask

# Extract upper triangle
pairs: list[tuple[str, str, float, float, int]] = []
cols = corr.columns.tolist()

for i in range(len(cols)):
    for j in range(i + 1, len(cols)):
        a, b = cols[i], cols[j]
        val = corr.loc[a, b]
        n = int(n_overlap.loc[a, b])
        if pd.isna(val):
            continue
        if n < min_obs:
            continue
        v = float(val)
        pairs.append((a, b, v, abs(v), n))

df_pairs = pd.DataFrame(
    pairs,
    columns=["metric_a", "metric_b", "corr", "abs_corr", "n_obs"],
)

if df_pairs.empty:
    st.warning("No metric pairs met the min_obs threshold.")
    st.stop()

# -----------------------------
# Filter: same metric family
# -----------------------------
df_pairs["family_a"] = df_pairs["metric_a"].apply(metric_family)
df_pairs["family_b"] = df_pairs["metric_b"].apply(metric_family)

if exclude_same_family:
    df_pairs = df_pairs[df_pairs["family_a"] != df_pairs["family_b"]].copy()

if df_pairs.empty:
    st.warning(
        "After filtering same-family metrics, no pairs remain. "
        "Try lowering min_obs or disabling the filter."
    )
    st.stop()

# Top N by |corr|, tie-breaker by n_obs
df_top = (
    df_pairs.sort_values(["abs_corr", "n_obs"], ascending=[False, False])
    .head(top_n)
    .reset_index(drop=True)
)

st.caption(
    f"Snapshot: **{as_of_date}** · Method: **{method}** · Ranked by **|corr|** · "
    f"Companies: **{len(wide)}** · Metrics used: **{X.shape[1]}** · "
    f"Showing: **Top {len(df_top)}** pairs"
)

st.altair_chart(top_correlations_chart(df_top), width="stretch")

with st.expander("Show top pairs table", expanded=True):
    st.dataframe(
        df_top.drop(columns=["family_a", "family_b"], errors="ignore"),
        width="stretch",
    )
