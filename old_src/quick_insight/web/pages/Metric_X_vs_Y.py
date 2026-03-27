# src/quick_insight/web/pages/2_quality_map.py
from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from quick_insight.web.db import q
from quick_insight.web.ui import setup_page


# ============================================================
# Chart (local, self-contained)
# ============================================================
def _scatter_quality_map(
    df: pd.DataFrame,
    *,
    x_title: str,
    y_title: str,
) -> alt.Chart:

    base = alt.Chart(df).mark_circle(opacity=0.8).encode(
        x=alt.X("x_value:Q", title=x_title),
        y=alt.Y("y_value:Q", title=y_title),

        color=alt.Color(
            "sector:N",
            title="Sector",
            legend=alt.Legend(orient="right"),
        ),

        size=alt.value(80),

        tooltip=[
            alt.Tooltip("ticker:N", title="Ticker"),
            alt.Tooltip("company_name:N", title="Company"),
            alt.Tooltip("sector:N", title="Sector"),
            alt.Tooltip("x_value:Q", title=x_title, format=",.6g"),
            alt.Tooltip("y_value:Q", title=y_title, format=",.6g"),
            alt.Tooltip("market_cap:Q", title="Market cap (bn)", format=",.4s"),
        ],
    )

    return base.interactive().properties(
        width="container",
        height=600,
    )


# ============================================================
# Data access (cached)
# ============================================================
@st.cache_data(show_spinner=False)
def get_numeric_metric_codes() -> list[str]:
    df = q(
        """
        SELECT metric_code
        FROM metric
        WHERE value_type = 'number'
        ORDER BY metric_code
        """
    )
    return df["metric_code"].astype(str).tolist() if not df.empty else []


@st.cache_data(show_spinner=False)
def get_snapshot_dates_for_metric(metric_code: str) -> list[str]:
    if not metric_code:
        return []

    df = q(
        """
        SELECT DISTINCT CAST(s.target_date AS DATE) AS d
        FROM facts_number fn
        JOIN metric   m ON m.metric_id   = fn.metric_id
        JOIN snapshot s ON s.snapshot_id = fn.snapshot_id
        WHERE m.metric_code = ?
        ORDER BY d DESC
        """,
        [metric_code],
    )
    if df.empty:
        return []
    return pd.to_datetime(df["d"], errors="coerce").dt.date.astype(str).tolist()


@st.cache_data(show_spinner=False)
def get_numeric_metrics_for_snapshot(as_of_date: str) -> list[str]:
    if not as_of_date:
        return []

    df = q(
        """
        SELECT DISTINCT CAST(m.metric_code AS VARCHAR) AS metric_code
        FROM facts_number fn
        JOIN metric   m ON m.metric_id   = fn.metric_id
        JOIN snapshot s ON s.snapshot_id = fn.snapshot_id
        WHERE CAST(s.target_date AS DATE) = CAST(? AS DATE)
          AND m.value_type = 'number'
        ORDER BY metric_code
        """,
        [as_of_date],
    )
    return df["metric_code"].astype(str).tolist() if not df.empty else []


@st.cache_data(show_spinner=False)
def fetch_quality_map_snapshot(as_of_date: str, *, x_metric: str, y_metric: str) -> pd.DataFrame:
    if not (as_of_date and x_metric and y_metric):
        return pd.DataFrame(columns=["ticker", "company_name", "sector", "market_cap", "x_value", "y_value"])

    df = q(
        """
        WITH snap AS (
            SELECT snapshot_id
            FROM snapshot
            WHERE CAST(target_date AS DATE) = CAST(? AS DATE)
        ),
        m AS (
            SELECT metric_id, metric_code
            FROM metric
            WHERE metric_code IN (?, ?)
        ),
        vals AS (
            SELECT
                fn.company_id,
                m.metric_code,
                CAST(fn.metric_value AS DOUBLE) AS metric_value
            FROM facts_number fn
            JOIN snap s ON s.snapshot_id = fn.snapshot_id
            JOIN m      ON m.metric_id   = fn.metric_id
        ),
        pivoted AS (
            SELECT
                company_id,
                MAX(CASE WHEN metric_code = ? THEN metric_value END) AS x_value,
                MAX(CASE WHEN metric_code = ? THEN metric_value END) AS y_value
            FROM vals
            GROUP BY company_id
        ),
        mcap AS (
            SELECT
                fn.company_id,
                MAX(CAST(fn.metric_value AS DOUBLE)) AS market_cap
            FROM facts_number fn
            JOIN snap s   ON s.snapshot_id = fn.snapshot_id
            JOIN metric m ON m.metric_id   = fn.metric_id
            WHERE m.metric_code = 'market_cap_bn'
            GROUP BY fn.company_id
        )
        SELECT
            CAST(c.primary_ticker AS VARCHAR) AS ticker,
            CAST(c.company_name   AS VARCHAR) AS company_name,
            CAST(c.sector         AS VARCHAR) AS sector,
            mc.market_cap                     AS market_cap,
            p.x_value                         AS x_value,
            p.y_value                         AS y_value
        FROM pivoted p
        JOIN company c ON c.company_id = p.company_id
        LEFT JOIN mcap mc ON mc.company_id = p.company_id
        ORDER BY sector, ticker
        """,
        [as_of_date, x_metric, y_metric, x_metric, y_metric],
    )

    if df.empty:
        return df

    for col in ["ticker", "company_name", "sector"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    df["x_value"] = pd.to_numeric(df["x_value"], errors="coerce")
    df["y_value"] = pd.to_numeric(df["y_value"], errors="coerce")
    if "market_cap" in df.columns:
        df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")

    return df


# ============================================================
# Sidebar controls
# ============================================================
def controls() -> None:
    st.header("Settings")

    all_metrics = get_numeric_metric_codes()

    DEFAULT_X = "capital_and_asset_light_avg"
    DEFAULT_Y = "fcf_growth_avg"

    # -----------------------------
    # Session state initialization
    # -----------------------------
    if "qm_x" not in st.session_state:
        st.session_state["qm_x"] = DEFAULT_X if DEFAULT_X in all_metrics else None

    if "qm_date" not in st.session_state:
        if st.session_state["qm_x"]:
            dates = get_snapshot_dates_for_metric(st.session_state["qm_x"])
            st.session_state["qm_date"] = dates[0] if dates else None
        else:
            st.session_state["qm_date"] = None

    if "qm_y" not in st.session_state:
        st.session_state["qm_y"] = DEFAULT_Y

    # -----------------------------
    # X Metric
    # -----------------------------
    x_opts = ["— choose X metric —"] + all_metrics
    x_idx = x_opts.index(st.session_state["qm_x"]) if st.session_state["qm_x"] in all_metrics else 0
    pick_x = st.selectbox("X-axis metric", options=x_opts, index=x_idx, key="qm_x_pick")

    if pick_x == "— choose X metric —":
        st.session_state["qm_x"] = None
        st.session_state["qm_date"] = None
        st.session_state["qm_y"] = None
        return

    if st.session_state["qm_x"] != pick_x:
        st.session_state["qm_x"] = pick_x
        dates = get_snapshot_dates_for_metric(pick_x)
        st.session_state["qm_date"] = dates[0] if dates else None
        st.session_state["qm_y"] = None

    # -----------------------------
    # Date (most recent first)
    # -----------------------------
    date_choices = get_snapshot_dates_for_metric(st.session_state["qm_x"])
    date_opts = ["— choose date —"] + date_choices
    date_idx = date_opts.index(st.session_state["qm_date"]) if st.session_state["qm_date"] in date_choices else 0

    pick_date = st.selectbox("Snapshot date", options=date_opts, index=date_idx, key="qm_date_pick")

    if pick_date == "— choose date —":
        st.session_state["qm_date"] = None
        st.session_state["qm_y"] = None
        return

    if st.session_state["qm_date"] != pick_date:
        st.session_state["qm_date"] = pick_date
        st.session_state["qm_y"] = None

    # -----------------------------
    # Y Metric (default applied here)
    # -----------------------------
    y_pool = get_numeric_metrics_for_snapshot(st.session_state["qm_date"])
    y_choices = [m for m in y_pool if m != st.session_state["qm_x"]]

    if st.session_state.get("qm_y") not in y_choices:
        st.session_state["qm_y"] = DEFAULT_Y if DEFAULT_Y in y_choices else None

    y_opts = ["— choose Y metric —"] + y_choices
    y_idx = y_opts.index(st.session_state["qm_y"]) if st.session_state["qm_y"] in y_choices else 0

    pick_y = st.selectbox("Y-axis metric", options=y_opts, index=y_idx, key="qm_y_pick")

    st.session_state["qm_y"] = None if pick_y == "— choose Y metric —" else pick_y



setup_page(controls=controls)


# ============================================================
# Page
# ============================================================
st.title("Metric Scatter Plot (X vs Y)")
st.info("Flow: 1) pick X metric → 2) pick a date → 3) pick Y metric.")

x_metric = st.session_state.get("qm_x")
as_of_date = st.session_state.get("qm_date")
y_metric = st.session_state.get("qm_y")

if not (x_metric and as_of_date and y_metric):
    st.stop()

df_plot = fetch_quality_map_snapshot(as_of_date, x_metric=x_metric, y_metric=y_metric)
df_plot = df_plot.dropna(subset=["x_value", "y_value"])

chart = _scatter_quality_map(df_plot, x_title=x_metric, y_title=y_metric)
st.altair_chart(chart, width="stretch")
