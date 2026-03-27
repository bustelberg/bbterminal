# src/quick_insight/web/helpers/earnings_dashboard_helpers/data.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import streamlit as st

from quick_insight.config.config import settings
from quick_insight.web.metrics import fetch_metrics
from quick_insight.web.db import q

from quick_insight.web.helpers.earnings_dashboard_helpers.constants import (
    DEFAULT_TICKER,
    METRIC,
    metrics_for_page,
)


# ----------------------------
# View models
# ----------------------------
@dataclass(frozen=True)
class EarningsQuery:
    ticker: str | None = None
    portfolio_id: int | None = None
    lookback_years: int = 10


@dataclass(frozen=True)
class SnapshotInputs:
    raw_multi: pd.DataFrame
    ts_fcf_ps: pd.DataFrame
    # metric_code -> (min_asof, max_asof) for the latest datapoint per holding
    asof_range_by_metric: dict[str, tuple[pd.Timestamp | None, pd.Timestamp | None]]



@dataclass(frozen=True)
class ChartInputs:
    ts_fcf_yield: pd.DataFrame
    raw2_relative: pd.DataFrame
    ts_fcf_ps: pd.DataFrame


@dataclass(frozen=True)
class EarningsData:
    meta: dict[str, Any]
    selection_label: str
    lookback_years: int
    raw_multi: pd.DataFrame
    snapshot: SnapshotInputs
    charts: ChartInputs

@dataclass(frozen=True)
class EarningsCompareData:
    """Holds primary (A) and compare (B) earnings data."""
    a: EarningsData
    b: EarningsData


# ----------------------------
# Column hygiene
# ----------------------------
def _ensure_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    """If duplicate column labels exist, keep the first occurrence."""
    if df is None or df.empty:
        return df
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    return df


def _first_col(df: pd.DataFrame, name: str) -> pd.Series:
    """Return the FIRST column with this label even if duplicates exist."""
    if name not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index, name=name)
    s = df.loc[:, name]
    if isinstance(s, pd.DataFrame):  # duplicate labels -> DataFrame
        s = s.iloc[:, 0]
    s = s.copy()
    s.name = name
    return s


def _clean_long3(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a clean long frame with exactly:
      as_of_date (datetime), metric_code (str), metric_value (float)
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["as_of_date", "metric_code", "metric_value"])

    df = df.copy()

    # If as_of_date is index, reset safely
    if df.index.name == "as_of_date":
        if "as_of_date" in df.columns:
            df = df.rename(columns={"as_of_date": "as_of_date_col"})
        df = df.reset_index()

    df = _ensure_unique_columns(df)

    out = pd.DataFrame(
        {
            "as_of_date": _first_col(df, "as_of_date"),
            "metric_code": _first_col(df, "metric_code"),
            "metric_value": _first_col(df, "metric_value"),
        }
    )

    out["as_of_date"] = pd.to_datetime(out["as_of_date"], errors="coerce")
    out["metric_value"] = pd.to_numeric(out["metric_value"], errors="coerce")
    out["metric_code"] = out["metric_code"].astype(str)

    out = out.dropna(subset=["as_of_date", "metric_code"])
    out = out.sort_values(["metric_code", "as_of_date"]).reset_index(drop=True)
    return out


def latest_value(raw_multi: pd.DataFrame, metric_code: str) -> tuple[pd.Timestamp | None, float | None]:
    """Return (as_of_date, value) for the latest non-null observation of a metric in a long3 frame."""
    if raw_multi is None or raw_multi.empty:
        return (None, None)

    df = _clean_long3(raw_multi)
    if df.empty:
        return (None, None)

    m = df[df["metric_code"] == str(metric_code)]
    if m.empty:
        return (None, None)

    m = m.dropna(subset=["as_of_date", "metric_value"]).sort_values("as_of_date")
    if m.empty:
        return (None, None)

    row = m.iloc[-1]
    dt = pd.to_datetime(row["as_of_date"], errors="coerce")
    try:
        val = float(row["metric_value"])
    except Exception:
        val = None
    return (dt, val)


def _asof_ranges_from_company_raw(
    raw_multi: pd.DataFrame,
    *,
    metric_codes: list[str],
) -> dict[str, tuple[pd.Timestamp | None, pd.Timestamp | None]]:
    """
    Single-company mode: for each metric_code, return (dt, dt) where dt is the latest as_of_date
    with a non-null metric_value. If missing, (None, None).
    """
    out: dict[str, tuple[pd.Timestamp | None, pd.Timestamp | None]] = {}
    df = _clean_long3(raw_multi)
    if df.empty:
        return {str(c): (None, None) for c in metric_codes}

    for code in metric_codes:
        m = df[df["metric_code"] == str(code)].dropna(subset=["as_of_date", "metric_value"])
        if m.empty:
            out[str(code)] = (None, None)
        else:
            dt = pd.to_datetime(m["as_of_date"].max(), errors="coerce")
            out[str(code)] = (dt, dt)
    return out


def _asof_ranges_from_portfolio_frames(
    frames: list[pd.DataFrame],
    *,
    metric_codes: list[str],
    ticker_col: str = "__ticker",
) -> dict[str, tuple[pd.Timestamp | None, pd.Timestamp | None]]:
    """
    Portfolio mode: frames contain per-holding data + a ticker identifier column.
    For each metric_code:
      - take each holding's latest as_of_date where metric_value is not null
      - return (min_dt, max_dt) across holdings
    If no holding has a datapoint: (None, None).
    """
    out: dict[str, tuple[pd.Timestamp | None, pd.Timestamp | None]] = {str(c): (None, None) for c in metric_codes}
    if not frames:
        return out

    df = pd.concat(frames, ignore_index=True)
    df = _ensure_unique_columns(df)
    if df.empty or ticker_col not in df.columns:
        return out

    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
    df["metric_code"] = df["metric_code"].astype(str)
    df["metric_value"] = pd.to_numeric(df["metric_value"], errors="coerce")

    df = df.dropna(subset=[ticker_col, "metric_code", "as_of_date"])
    if df.empty:
        return out

    # latest per holding per metric
    latest = (
        df.dropna(subset=["metric_value"])
        .sort_values(["metric_code", ticker_col, "as_of_date"])
        .groupby(["metric_code", ticker_col], as_index=False)
        .tail(1)
    )

    for code in metric_codes:
        m = latest[latest["metric_code"] == str(code)]
        if m.empty:
            out[str(code)] = (None, None)
            continue
        dts = pd.to_datetime(m["as_of_date"], errors="coerce").dropna()
        if dts.empty:
            out[str(code)] = (None, None)
        else:
            out[str(code)] = (dts.min(), dts.max())

    return out


# ----------------------------
# DB helpers (portfolio)
# ----------------------------
def _db_path() -> str:
    p = getattr(settings, "db_path", None) or getattr(settings, "duckdb_path", None)
    if not p:
        raise RuntimeError("No DuckDB path configured. Expected settings.db_path or settings.duckdb_path.")
    return str(p)


def _connect(*, read_only: bool = True) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(_db_path(), read_only=read_only)



def list_portfolios_for_dropdown() -> pd.DataFrame:
    sql = """
    SELECT
        p.portfolio_id,
        p.portfolio_name,
        s.target_date,
        s.published_at
    FROM portfolio p
    JOIN snapshot s ON s.snapshot_id = p.snapshot_id
    ORDER BY p.portfolio_name, s.target_date DESC
    """
    return q(sql)  # or however you run queries



@st.cache_data(show_spinner=False)
def load_portfolio_holdings(portfolio_id: int) -> pd.DataFrame:
    """
    Schema-aligned (your schema):
      portfolio_weight(portfolio_id, company_id, weight_value)
      company(company_id, company_name, primary_ticker, primary_exchange, ...)

    Returns: portfolio_id, company_id, weight, company_name, primary_ticker, primary_exchange
    """
    sql = """
    SELECT
      pw.portfolio_id,
      pw.company_id,
      pw.weight_value AS weight,
      c.company_name,
      c.primary_ticker,
      c.primary_exchange
    FROM portfolio_weight pw
    JOIN company c ON c.company_id = pw.company_id
    WHERE pw.portfolio_id = ?
    ORDER BY pw.weight_value DESC
    """
    con = _connect(read_only=True)
    try:
        df = con.execute(sql, [int(portfolio_id)]).df()
    finally:
        con.close()

    df = _ensure_unique_columns(df)
    df["weight"] = pd.to_numeric(df.get("weight"), errors="coerce")
    df = df.dropna(subset=["primary_ticker", "weight"])
    df = df[df["weight"] > 0]
    return df.reset_index(drop=True)


def _normalize_weights(df: pd.DataFrame, weight_col: str = "weight") -> pd.DataFrame:
    out = df.copy()
    w = pd.to_numeric(out[weight_col], errors="coerce").fillna(0.0)
    s = float(w.sum())
    out[weight_col] = (w / s) if s > 0 else w
    return out


# ----------------------------
# Lookback
# ----------------------------
def filter_recent_years(df: pd.DataFrame, *, years: int) -> pd.DataFrame:
    """Keep points with as_of_date >= (today - years)."""
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame(columns=["as_of_date", "metric_code", "metric_value"])

    out = _clean_long3(df)
    if out.empty:
        return out

    today = pd.Timestamp.today().normalize()
    cutoff = today - pd.DateOffset(years=int(years))
    return out[out["as_of_date"] >= cutoff].copy()


# ----------------------------
# Portfolio blending
# ----------------------------
def _is_annual_code(code: str) -> bool:
    c = (code or "").strip()
    return c.startswith("annuals__") or c.startswith("annual_")


def _empty_blend_df() -> pd.DataFrame:
    """
    Typed empty frame for blend results.
    Using explicit dtypes prevents the pandas FutureWarning:
      'The behavior of DataFrame concatenation with empty or all-NA entries is deprecated'
    which fires when pd.concat sees a column-only DataFrame with untyped object columns.
    """
    return pd.DataFrame({
        "as_of_date": pd.Series([], dtype="datetime64[ns]"),
        "metric_code": pd.Series([], dtype="str"),
        "metric_value": pd.Series([], dtype="float64"),
    })


def _weighted_blend_metric_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Each frame must contain: as_of_date, metric_code, metric_value, weight

    IMPORTANT:
    - For annual metric codes (annuals__/annual_), blend by CALENDAR YEAR (not exact date),
      so the portfolio returns 1 datapoint per year even when holdings have different fiscal year-ends.
    - For non-annual metrics, blend by exact as_of_date.
    """
    if not frames:
        return _empty_blend_df()

    df = pd.concat(frames, axis=0, ignore_index=True)
    df = _ensure_unique_columns(df)
    if df.empty:
        return _empty_blend_df()

    # Coerce
    df["as_of_date"] = pd.to_datetime(_first_col(df, "as_of_date"), errors="coerce")
    df["metric_code"] = _first_col(df, "metric_code").astype(str)
    df["metric_value"] = pd.to_numeric(_first_col(df, "metric_value"), errors="coerce")
    df["weight"] = pd.to_numeric(_first_col(df, "weight"), errors="coerce")

    df = df.dropna(subset=["as_of_date", "metric_code", "weight"])
    if df.empty:
        return _empty_blend_df()

    annual_mask = df["metric_code"].map(_is_annual_code)

    # Annual metrics: blend by YEAR
    df_a = df.loc[annual_mask].copy()
    if not df_a.empty:
        df_a["year"] = df_a["as_of_date"].dt.year
        df_a["w_eff"] = np.where(df_a["metric_value"].notna(), df_a["weight"], np.nan)
        df_a["wx"] = df_a["weight"] * df_a["metric_value"]

        g = df_a.groupby(["year", "metric_code"], as_index=False)
        num = g["wx"].sum(min_count=1).rename(columns={"wx": "num"})
        den = g["w_eff"].sum(min_count=1).rename(columns={"w_eff": "den"})
        out_a = num.merge(den, on=["year", "metric_code"], how="outer")
        out_a["metric_value"] = out_a["num"] / out_a["den"]
        out_a = out_a.drop(columns=["num", "den"]).dropna(subset=["metric_value"])

        # use year-end date for chart x-axis consistency
        out_a["as_of_date"] = pd.to_datetime(out_a["year"].astype(str) + "-12-31", errors="coerce")
        out_a = out_a.drop(columns=["year"])
    else:
        out_a = _empty_blend_df()

    # Non-annual metrics: blend by exact DATE
    df_q = df.loc[~annual_mask].copy()
    if not df_q.empty:
        df_q["w_eff"] = np.where(df_q["metric_value"].notna(), df_q["weight"], np.nan)
        df_q["wx"] = df_q["weight"] * df_q["metric_value"]

        g = df_q.groupby(["as_of_date", "metric_code"], as_index=False)
        num = g["wx"].sum(min_count=1).rename(columns={"wx": "num"})
        den = g["w_eff"].sum(min_count=1).rename(columns={"w_eff": "den"})
        out_q = num.merge(den, on=["as_of_date", "metric_code"], how="outer")
        out_q["metric_value"] = out_q["num"] / out_q["den"]
        out_q = out_q.drop(columns=["num", "den"]).dropna(subset=["metric_value"])
    else:
        out_q = _empty_blend_df()

    out = pd.concat([out_q, out_a], ignore_index=True)
    out = _clean_long3(out)
    return out


def _fetch_metrics_safe(ticker: str, metric_list: list[str], *, exchange: str | None) -> pd.DataFrame | None:
    """
    Supports both signatures:
      fetch_metrics(ticker, metric_list)
      fetch_metrics(ticker, metric_list, exchange=...)
    """
    try:
        return fetch_metrics(ticker, metric_list, exchange=exchange)  # type: ignore[arg-type]
    except TypeError:
        return fetch_metrics(ticker, metric_list)  # legacy signature


# ----------------------------
# Main builder
# ----------------------------
@st.cache_data(show_spinner=False)
def build_earnings_data(*, query: EarningsQuery, meta: dict[str, Any]) -> EarningsData:
    metric_list = metrics_for_page()
    snapshot_metric_codes = [
        METRIC["INTEREST_COVERAGE"],
        METRIC["DEBT_TO_EQUITY"],
        METRIC["CAPEX_TO_REV"],
        METRIC["CAPEX_TO_OCF"],
        METRIC["ROE"],
        METRIC["ROIC"],
        METRIC["GROSS_MARGIN"],
        METRIC["NET_MARGIN"],
        METRIC["REV_GROWTH_5Y"],
        METRIC["REV_GROWTH_EST_3_5Y"],
        METRIC["EPS_LT_GROWTH_EST"],
        METRIC["FWD_PE"],
        METRIC["PEG"],
        METRIC["FCF"],
        METRIC["NET_INCOME"],
        METRIC["EPS_DILUTED"],
        METRIC["EPS_FY1_EST"],
    ]


    # ----------------------------
    # Company mode
    # ----------------------------
    if query.portfolio_id is None:
        ticker = (query.ticker or "").strip()
        if not ticker:
            raw_multi = pd.DataFrame(columns=["as_of_date", "metric_code", "metric_value"])
            selection_label = ""
        else:
            tmp = fetch_metrics(ticker, metric_list)

            if tmp is None or (isinstance(tmp, pd.DataFrame) and tmp.empty):
                raw_multi = pd.DataFrame(columns=["as_of_date", "metric_code", "metric_value"])
            else:
                raw_multi = tmp

            raw_multi = _clean_long3(raw_multi)
            raw_multi = filter_recent_years(raw_multi, years=query.lookback_years)
            selection_label = ticker
            asof_range_by_metric = _asof_ranges_from_company_raw(raw_multi, metric_codes=snapshot_metric_codes)


    # ----------------------------
    # Portfolio mode (weighted blend)
    # ----------------------------
    else:
        holdings = load_portfolio_holdings(int(query.portfolio_id))
        holdings = _normalize_weights(holdings, "weight")

        frames: list[pd.DataFrame] = []
        for r in holdings.itertuples(index=False):
            t = str(getattr(r, "primary_ticker", "") or "").strip()
            ex = str(getattr(r, "primary_exchange", "") or "").strip() or None
            w = float(getattr(r, "weight", 0.0) or 0.0)
            if not t or not np.isfinite(w) or w <= 0:
                continue

            tmp = _fetch_metrics_safe(t, metric_list, exchange=ex)
            if tmp is None:
                continue
            if isinstance(tmp, pd.DataFrame) and tmp.empty:
                continue

            tmp = _clean_long3(tmp)
            tmp = filter_recent_years(tmp, years=query.lookback_years)
            if tmp.empty:
                continue

            tmp = tmp.loc[:, ["as_of_date", "metric_code", "metric_value"]].copy()
            tmp["weight"] = w
            tmp["__ticker"] = t  # for per-holding as_of range computation
            frames.append(tmp)


        raw_multi = _weighted_blend_metric_frames(frames)
        selection_label = f"PORTFOLIO:{int(query.portfolio_id)}"
        asof_range_by_metric = _asof_ranges_from_portfolio_frames(
                                    frames,
                                    metric_codes=snapshot_metric_codes,
                                    ticker_col="__ticker",
                                )


    # ----------------------------
    # Build subsets for visuals
    # ----------------------------
    raw_multi = _clean_long3(raw_multi)

    ts_fcf_yield = raw_multi.loc[
        raw_multi["metric_code"] == METRIC["FCF_YIELD"],
        ["as_of_date", "metric_value"],
    ].copy()

    ts_fcf_ps = raw_multi.loc[
        raw_multi["metric_code"] == METRIC["FCF_PS"],
        ["as_of_date", "metric_value"],
    ].copy()

    raw2 = raw_multi.loc[
        raw_multi["metric_code"].isin(
            [METRIC["PRICE"], METRIC["EPS_WO_NRI"], METRIC["DIV_PS"], METRIC["EPS_EST"], METRIC["DIV_EST"]]
        ),
        ["as_of_date", "metric_code", "metric_value"],
    ].copy()

    return EarningsData(
        meta=meta,
        selection_label=selection_label,
        lookback_years=int(query.lookback_years),
        raw_multi=raw_multi,
        snapshot=SnapshotInputs(
                        raw_multi=raw_multi,
                        ts_fcf_ps=ts_fcf_ps,
                        asof_range_by_metric=asof_range_by_metric,
                    ),
        charts=ChartInputs(ts_fcf_yield=ts_fcf_yield, raw2_relative=raw2, ts_fcf_ps=ts_fcf_ps),
    )


@st.cache_data(show_spinner=False)
def build_earnings_compare_data(
    *,
    query_a: EarningsQuery,
    meta_a: dict[str, Any],
    query_b: EarningsQuery,
    meta_b: dict[str, Any],
) -> EarningsCompareData:
    """
    Build A and B separately using the existing single-entity builder.
    This avoids touching any of the current logic and keeps everything stable.
    """
    a = build_earnings_data(query=query_a, meta=meta_a)
    b = build_earnings_data(query=query_b, meta=meta_b)
    return EarningsCompareData(a=a, b=b)