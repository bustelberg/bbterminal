# src/quick_insight/web/pages/5_new_companies.py
from __future__ import annotations

import streamlit as st

from quick_insight.web.db import get_snapshot_dates_with_new_companies, fetch_new_companies_in_snapshot


st.title("New Companies Detector")

st.info(
    "Shows companies that appear in the selected snapshot for the first time (not present in any earlier snapshot)."
)

snapshots = get_snapshot_dates_with_new_companies()

if not snapshots:
    st.warning("No snapshots found in the DB yet.")
    st.stop()

with st.sidebar:
    st.header("Settings")
    as_of_date = st.selectbox(
        "Snapshot date",
        options=snapshots,
        index=len(snapshots) - 1,  # latest
    )

df_new = fetch_new_companies_in_snapshot(as_of_date)

st.caption(f"Snapshot: **{as_of_date}** · New companies: **{len(df_new)}**")

if df_new.empty:
    st.success("No new companies found for this snapshot (compared to all earlier snapshots).")
    st.stop()

# Optional quick filters (pure UI; no DB changes)
with st.sidebar:
    st.divider()
    st.subheader("Filters")

    sectors = sorted([s for s in df_new["sector"].dropna().unique().tolist() if str(s).strip()])
    countries = sorted([c for c in df_new["country"].dropna().unique().tolist() if str(c).strip()])

    selected_sectors = st.multiselect("Sector", options=sectors, default=[])
    selected_countries = st.multiselect("Country", options=countries, default=[])

filtered = df_new
if selected_sectors:
    filtered = filtered[filtered["sector"].isin(selected_sectors)]
if selected_countries:
    filtered = filtered[filtered["country"].isin(selected_countries)]

st.dataframe(filtered, width="stretch")
