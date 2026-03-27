# src/quick_insight/web/metrics.py
from __future__ import annotations

import pandas as pd
import streamlit as st

from quick_insight.web.db import q


def _is_annual_code(code: str) -> bool:
    c = (code or "").strip()
    return c.startswith("annuals__") or c.startswith("annual_")


def _collapse_to_annual_last(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["as_of_date", "metric_code", "metric_value"])

    out = df.copy()
    out["as_of_date"] = pd.to_datetime(out["as_of_date"], errors="coerce")
    out["metric_code"] = out["metric_code"].astype("string")
    out["metric_value"] = pd.to_numeric(out["metric_value"], errors="coerce")

    out = out.dropna(subset=["as_of_date", "metric_code"])
    if out.empty:
        return pd.DataFrame(columns=["as_of_date", "metric_code", "metric_value"])

    out["year"] = out["as_of_date"].dt.year
    out = (
        out.sort_values(["metric_code", "as_of_date"])
        .groupby(["metric_code", "year"], as_index=False)
        .tail(1)
        .drop(columns=["year"])
        .reset_index(drop=True)
    )
    return out


@st.cache_data(show_spinner=False)
def fetch_metrics(ticker: str, metric_codes: list[str], *, exchange: str | None = None) -> pd.DataFrame:
    """
    Fetch metrics for a single ticker (optionally scoped to an exchange).

    - Annual codes are collapsed to 1 point per year (last date in that year).
    """
    codes = [str(x) for x in metric_codes if x]
    if not codes:
        return pd.DataFrame(columns=["as_of_date", "metric_code", "metric_value"])

    ph = ",".join(["?"] * len(codes))

    where_exchange = ""
    params: list = [ticker]
    if exchange:
        where_exchange = " AND c.primary_exchange = ?"
        params.append(exchange)

    params += codes

    df = q(
        f"""
        SELECT
            CAST(s.target_date AS TIMESTAMP) AS as_of_date,
            CAST(m.metric_code AS VARCHAR)   AS metric_code,
            CAST(fn.metric_value AS DOUBLE)  AS metric_value
        FROM facts_number fn
        JOIN company  c ON c.company_id  = fn.company_id
        JOIN metric   m ON m.metric_id   = fn.metric_id
        JOIN snapshot s ON s.snapshot_id = fn.snapshot_id
        WHERE c.primary_ticker = ?
          {where_exchange}
          AND m.metric_code IN ({ph})
        ORDER BY s.target_date, m.metric_code
        """,
        params,
    )

    if df.empty:
        return pd.DataFrame(columns=["as_of_date", "metric_code", "metric_value"])

    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
    df["metric_code"] = df["metric_code"].astype("string")
    df["metric_value"] = pd.to_numeric(df["metric_value"], errors="coerce")
    df = df.dropna(subset=["as_of_date", "metric_code"])
    if df.empty:
        return pd.DataFrame(columns=["as_of_date", "metric_code", "metric_value"])

    annual_mask = df["metric_code"].astype(str).map(_is_annual_code)
    df_annual = df.loc[annual_mask].copy()
    df_other = df.loc[~annual_mask].copy()

    if not df_annual.empty:
        df_annual = _collapse_to_annual_last(df_annual)

    out = pd.concat([df_other, df_annual], ignore_index=True)
    out = out.sort_values(["metric_code", "as_of_date"]).reset_index(drop=True)
    return out
