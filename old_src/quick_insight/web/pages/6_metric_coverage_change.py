from __future__ import annotations

import streamlit as st

from quick_insight.web.db import (
    fetch_metric_coverage_change,
    get_previous_snapshot_date,
    get_snapshot_dates,
)

st.title("Metric Coverage Change")

st.info(
    "Compares numeric metric coverage between two consecutive snapshots. "
    "Useful to detect newly added metrics, dropped metrics, or big changes in completeness."
)

snapshots = get_snapshot_dates()
if not snapshots:
    st.warning("No snapshots found in the DB yet.")
    st.stop()

with st.sidebar:
    st.header("Settings")

    current_date = st.selectbox("Current snapshot", options=snapshots, index=len(snapshots) - 1)
    prev_date_auto = get_previous_snapshot_date(current_date)

    if prev_date_auto is None:
        st.warning("No previous snapshot exists for the selected snapshot.")
        st.stop()

    previous_date = st.selectbox(
        "Previous snapshot",
        options=[prev_date_auto] + [d for d in snapshots if d < current_date and d != prev_date_auto],
        index=0,
    )

    show_unchanged = st.toggle("Show unchanged metrics", value=False)

df = fetch_metric_coverage_change(current_date, previous_target_date=previous_date)

if df.empty:
    st.warning("No coverage data available.")
    st.stop()

# Optionally hide unchanged
if not show_unchanged:
    df_view = df[df["status"] != "unchanged"].copy()
else:
    df_view = df.copy()

st.caption(
    f"Current: **{current_date}** · Previous: **{previous_date}** · "
    f"Metrics compared: **{len(df)}** · Showing: **{len(df_view)}**"
)

# Summary counts
added = int((df["status"] == "added").sum())
dropped = int((df["status"] == "dropped").sum())
inc = int((df["status"] == "increased").sum())
dec = int((df["status"] == "decreased").sum())

c1, c2, c3, c4 = st.columns(4)
c1.metric("Added", added)
c2.metric("Dropped", dropped)
c3.metric("Increased", inc)
c4.metric("Decreased", dec)

# Tables
st.subheader("Added / Dropped")
df_ad = df_view[df_view["status"].isin(["added", "dropped"])].copy()
df_ad = df_ad.sort_values(["status", "curr_pct"], ascending=[True, False])
st.dataframe(df_ad, width="stretch")

st.subheader("Biggest coverage changes")
df_chg = df_view[df_view["status"].isin(["increased", "decreased"])].copy()
df_chg = df_chg.sort_values(["delta_pct", "delta_n"], ascending=[False, False])
st.dataframe(df_chg, width="stretch")
