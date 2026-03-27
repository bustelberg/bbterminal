from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import streamlit as st

from quick_insight.web.db import load_companies, q
from quick_insight.web.helpers.earnings_dashboard_helpers.charts import chart_dims
from quick_insight.web.helpers.earnings_dashboard_helpers.data import (
    DEFAULT_TICKER,
    EarningsQuery,
    build_earnings_data,
    list_portfolios_for_dropdown,
)
from quick_insight.web.helpers.earnings_dashboard_helpers.renderers import (
    render_chart_fcf_share,
    render_chart_fcf_yield,
    render_chart_relative_growth,
    render_snapshot_stats,
)
from quick_insight.web.helpers.portfolio_cache import portfolio_cache
from quick_insight.web.ui import setup_page


# ============================================================
# Dropdown sources
# ============================================================
@portfolio_cache.register
@st.cache_data(show_spinner=False)
def _companies_for_dropdown() -> pd.DataFrame:
    return load_companies().fillna("")


@portfolio_cache.register
@st.cache_data(show_spinner=False)
def _portfolios_for_dropdown() -> pd.DataFrame:
    df = list_portfolios_for_dropdown()
    return df.fillna("") if df is not None else pd.DataFrame()


@portfolio_cache.register
@st.cache_data(show_spinner=False)
def _load_portfolio_holdings(portfolio_id: int) -> pd.DataFrame:
    sql = """
    SELECT
        pw.weight_value                         AS weight,
        c.company_name                          AS company_name,
        c.primary_ticker                        AS ticker,
        c.primary_exchange                      AS exchange,
        c.sector                                AS sector
    FROM portfolio_weight pw
    JOIN company c ON c.company_id = pw.company_id
    WHERE pw.portfolio_id = ?
    ORDER BY pw.weight_value DESC, c.company_name
    """
    df = q(sql, [portfolio_id])
    if df is None or df.empty:
        return pd.DataFrame(columns=["weight", "company_name", "ticker", "exchange", "sector"])

    out = df.copy()
    out["weight_pct"] = (out["weight"].astype(float) * 100.0).round(2)
    return out


def _company_labels(df: pd.DataFrame):
    labels = df["company_name"].astype(str) + " (Exchange: " + df["primary_exchange"].astype(str) + ")"
    return labels.tolist(), dict(zip(labels, df["primary_ticker"])), dict(zip(labels, df.to_dict("records")))


def _portfolio_labels(df: pd.DataFrame) -> list[str]:
    if df is None or df.empty:
        return ["(no portfolios)"]

    d = df.copy()
    if "target_date" in d.columns:
        d["_target_date_str"] = pd.to_datetime(d["target_date"], errors="coerce").dt.date.astype(str)
        d["_dropdown_label"] = d["portfolio_name"].astype(str) + " — " + d["_target_date_str"].astype(str)
    else:
        d["_dropdown_label"] = d["portfolio_name"].astype(str)

    return d["_dropdown_label"].tolist()



companies = _companies_for_dropdown()
labels, label_to_ticker, label_to_meta = _company_labels(companies)
default_label = next((l for l in labels if label_to_ticker.get(l) == DEFAULT_TICKER), labels[0] if labels else "")


# ============================================================
# Sidebar controls
# ============================================================
def controls():
    st.header("Settings")

    mode = st.toggle("Portfolio mode", key="earn_portfolio_mode")

    if mode:
        ports = _portfolios_for_dropdown()
        port_labels = _portfolio_labels(ports)
        st.selectbox("Portfolio", port_labels, key="earn_portfolio_label")
    else:
        if labels:
            st.selectbox("Company", labels, index=labels.index(default_label), key="earn_company_label")
        else:
            st.selectbox("Company", ["(no companies)"], key="earn_company_label")

    st.markdown("---")

    cmp_on = st.toggle("Enable comparison", key="earn_compare_enabled")

    if cmp_on:
        cmp_mode = st.toggle("Portfolio mode (compare)", key="earn_compare_portfolio_mode")

        if cmp_mode:
            ports = _portfolios_for_dropdown()
            port_labels = _portfolio_labels(ports)
            st.selectbox("Portfolio (compare)", port_labels, key="earn_compare_portfolio_label")
        else:
            if labels:
                st.selectbox("Company (compare)", labels, key="earn_compare_company_label")
            else:
                st.selectbox("Company (compare)", ["(no companies)"], key="earn_compare_company_label")

    st.radio("Charts per row", [3, 2, 1], index=1, horizontal=True, key="earn_charts_per_row")
    st.number_input("Lookback (years)", 1, 50, 10, key="earn_lookback_years")


setup_page(controls=controls)


# ============================================================
# Helpers
# ============================================================
@dataclass(frozen=True)
class Selection:
    ticker: str | None
    label: str


def _get_company_selection(label_key: str) -> Selection:
    label = st.session_state.get(label_key, default_label)
    ticker = label_to_ticker.get(label)
    return Selection(ticker=ticker, label=label)


def _get_portfolio_selection(label_key: str) -> tuple[int | None, str]:
    chosen_label = str(st.session_state.get(label_key, "") or "")
    ports = _portfolios_for_dropdown()

    if ports is None or ports.empty:
        return None, (chosen_label or "(no portfolios)")

    d = ports.copy()
    if "target_date" in d.columns:
        d["_target_date_str"] = pd.to_datetime(d["target_date"], errors="coerce").dt.date.astype(str)
        d["_dropdown_label"] = d["portfolio_name"].astype(str) + " — " + d["_target_date_str"].astype(str)
    else:
        d["_dropdown_label"] = d["portfolio_name"].astype(str)

    row = d.loc[d["_dropdown_label"] == chosen_label]
    if row.empty:
        first = d.iloc[0]
        return int(first["portfolio_id"]), str(first["_dropdown_label"])

    r0 = row.iloc[0]
    return int(r0["portfolio_id"]), str(r0["_dropdown_label"])


def _safe_label(s: str | None) -> str:
    return str(s) if s else ""


# ============================================================
# Page
# ============================================================
st.title("Earnings Dashboard")

if companies.empty:
    st.warning("No companies in DB.")
    st.stop()

lookback = int(st.session_state.get("earn_lookback_years", 10))
charts_per_row = int(st.session_state.get("earn_charts_per_row", 2))

# ------------------------------------------------------------
# Primary selection
# ------------------------------------------------------------
mode = bool(st.session_state.get("earn_portfolio_mode"))

portfolio_id: int | None = None
portfolio_id_b: int | None = None

if mode:
    portfolio_id, primary_label = _get_portfolio_selection("earn_portfolio_label")
    q_primary = EarningsQuery(ticker=None, portfolio_id=portfolio_id, lookback_years=lookback)
    data = build_earnings_data(query=q_primary, meta={"label": primary_label, "mode": "portfolio"})
else:
    sel = _get_company_selection("earn_company_label")
    primary_label = sel.label
    q_primary = EarningsQuery(ticker=sel.ticker, portfolio_id=None, lookback_years=lookback)
    data = build_earnings_data(query=q_primary, meta=label_to_meta.get(sel.label, {}))

# ------------------------------------------------------------
# Compare selection
# ------------------------------------------------------------
if st.session_state.get("earn_compare_enabled"):
    cmp_mode = bool(st.session_state.get("earn_compare_portfolio_mode"))

    if cmp_mode:
        portfolio_id_b, compare_label = _get_portfolio_selection("earn_compare_portfolio_label")
        q_compare = EarningsQuery(ticker=None, portfolio_id=portfolio_id_b, lookback_years=lookback)
        data2 = build_earnings_data(query=q_compare, meta={"label": compare_label, "mode": "portfolio"})
    else:
        sel2 = _get_company_selection("earn_compare_company_label")
        compare_label = sel2.label
        q_compare = EarningsQuery(ticker=sel2.ticker, portfolio_id=None, lookback_years=lookback)
        data2 = build_earnings_data(query=q_compare, meta=label_to_meta.get(sel2.label, {}))
else:
    data2 = None
    compare_label = None

# ------------------------------------------------------------
# Snapshot
# ------------------------------------------------------------
if data2 is None:
    render_snapshot_stats([(primary_label, data.snapshot.raw_multi, data.snapshot.ts_fcf_ps)])
else:
    render_snapshot_stats(
        [
            (primary_label, data.snapshot.raw_multi, data.snapshot.ts_fcf_ps),
            (_safe_label(compare_label), data2.snapshot.raw_multi, data2.snapshot.ts_fcf_ps),
        ]
    )

st.divider()

# ------------------------------------------------------------
# Holdings expander (portfolio mode only)
# ------------------------------------------------------------
show_holdings_a = bool(st.session_state.get("earn_portfolio_mode")) and portfolio_id is not None
show_holdings_b = (
    bool(st.session_state.get("earn_compare_enabled"))
    and bool(st.session_state.get("earn_compare_portfolio_mode"))
    and portfolio_id_b is not None
)

if show_holdings_a or show_holdings_b:
    with st.expander("Data", expanded=False):
        if show_holdings_a and portfolio_id is not None:
            st.markdown(f"**{primary_label} — holdings**")
            h = _load_portfolio_holdings(int(portfolio_id))
            st.dataframe(
                h[["weight_pct", "company_name", "ticker", "exchange", "sector"]],
                width="stretch",
                hide_index=True,
            )

        if show_holdings_b and portfolio_id_b is not None:
            st.markdown(f"**{_safe_label(compare_label)} — holdings**")
            h2 = _load_portfolio_holdings(int(portfolio_id_b))
            st.dataframe(
                h2[["weight_pct", "company_name", "ticker", "exchange", "sector"]],
                width="stretch",
                hide_index=True,
            )

# ------------------------------------------------------------
# Charts
# ------------------------------------------------------------
W, H = chart_dims(per_row=charts_per_row, aspect_w_over_h=1.6)
cols = st.columns(charts_per_row)

if data2 is None:
    with cols[0]:
        render_chart_fcf_yield(data.charts.ts_fcf_yield, W=W, H=H)
    with cols[1]:
        render_chart_relative_growth(data.charts.raw2_relative, W=W, H=H)
    with cols[2 % charts_per_row]:
        render_chart_fcf_share(data.charts.ts_fcf_ps, W=W, H=H)
else:
    with cols[0]:
        render_chart_fcf_yield(
            data.charts.ts_fcf_yield,
            ts_fcf_yield_b=data2.charts.ts_fcf_yield,
            W=W, H=H,
            label_a=primary_label,
            label_b=_safe_label(compare_label),
        )
    with cols[1]:
        render_chart_relative_growth(
            data.charts.raw2_relative,
            raw2_b=data2.charts.raw2_relative,
            W=W, H=H,
            label_a=primary_label,
            label_b=_safe_label(compare_label),
        )
    with cols[2 % charts_per_row]:
        render_chart_fcf_share(
            data.charts.ts_fcf_ps,
            ts_fcf_ps_b=data2.charts.ts_fcf_ps,
            W=W, H=H,
            label_a=primary_label,
            label_b=_safe_label(compare_label),
        )