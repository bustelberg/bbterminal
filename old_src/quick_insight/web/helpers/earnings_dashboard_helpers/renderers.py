# src/quick_insight/web/helpers/earnings_dashboard_helpers/renderers.py
from __future__ import annotations

from typing import Any

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from quick_insight.web.helpers.earnings_dashboard_helpers.charts import (
    indexed_log_chart,
    legend_only,
    line_with_mean,
)
from quick_insight.web.helpers.earnings_dashboard_helpers.constants import METRIC, METRIC_UNIT, SERIES
from quick_insight.web.helpers.earnings_dashboard_helpers.formatters import (
    fmt_num,
    fmt_pct,
    format_value_by_unit,
)
from quick_insight.web.helpers.earnings_dashboard_helpers.transforms import (
    build_relative_growth,
    compute_cagr,
    compute_cagr_window,
    index_to_100,
)


# ----------------------------
# Numeric safety helpers
# ----------------------------
def _to_float(x) -> float | None:
    """
    Coerce x to a finite float, else None.
    Handles: None, pd.NA, strings, numpy scalars, and (date, value) tuples.
    """
    if x is None:
        return None

    if isinstance(x, tuple) and len(x) >= 2:
        x = x[1]

    try:
        y = pd.to_numeric(x, errors="coerce")
    except Exception:
        return None

    if isinstance(y, (pd.Series, np.ndarray, list, tuple)):
        if len(y) == 0:
            return None
        try:
            y = y[0]
        except Exception:
            return None

    try:
        yf = float(y)
    except Exception:
        return None

    return yf if np.isfinite(yf) else None


def _asof_date(x) -> pd.Timestamp | None:
    """Extract as_of_date from latest_value output: (date, value)."""
    if isinstance(x, tuple) and len(x) >= 1:
        try:
            return pd.to_datetime(x[0], errors="coerce")
        except Exception:
            return None
    return None


def _fmt_asof(dt: pd.Timestamp | None) -> str | None:
    if dt is None or pd.isna(dt):
        return None
    try:
        return str(pd.to_datetime(dt).date())
    except Exception:
        return None


def _fmt_asof_range(rng) -> str | None:
    """rng: (a,b) dates; returns 'As of a → b' or 'As of a'."""
    if not rng:
        return None
    a, b = rng
    a = pd.to_datetime(a, errors="coerce")
    b = pd.to_datetime(b, errors="coerce")
    a_s = _fmt_asof(a)
    b_s = _fmt_asof(b)
    if not a_s and not b_s:
        return None
    if a_s and b_s and a_s != b_s:
        return f"As of {a_s} → {b_s}"
    return f"As of {a_s or b_s}"


def _html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


# ----------------------------
# Snapshot (compute once per entity)
# ----------------------------
def _compute_snapshot_stats(raw: pd.DataFrame, *, ts_fcf_ps: pd.DataFrame) -> dict[str, Any]:
    """
    Compute snapshot numbers for ONE entity.
    Returns dict with:
      - want: metric_key -> metric_code
      - v: latest float values
      - dt: latest as_of_date per key
      - derived: egm, eps_5y, price cagrs, fcf_over_ni, fcf_sh_5y/10y
      - series as-of: eps_asof, price_asof, fcfps_asof
    """
    want = {
        "interest_cov": METRIC["INTEREST_COVERAGE"],
        "debt_to_equity": METRIC["DEBT_TO_EQUITY"],
        "capex_rev": METRIC["CAPEX_TO_REV"],
        "capex_ocf": METRIC["CAPEX_TO_OCF"],
        "roe": METRIC["ROE"],
        "roic": METRIC["ROIC"],
        "gross_m": METRIC["GROSS_MARGIN"],
        "net_m": METRIC["NET_MARGIN"],
        "rev_5y": METRIC["REV_GROWTH_5Y"],
        "rev_est": METRIC["REV_GROWTH_EST_3_5Y"],
        "eps_lt": METRIC["EPS_LT_GROWTH_EST"],
        "fwd_pe": METRIC["FWD_PE"],
        "peg": METRIC["PEG"],
        "fcf": METRIC["FCF"],
        "ni": METRIC["NET_INCOME"],
        "eps": METRIC["EPS_DILUTED"],
        "eps_fy1": METRIC["EPS_FY1_EST"],
    }

    # local import to avoid circulars
    from quick_insight.web.helpers.earnings_dashboard_helpers.data import latest_value

    v: dict[str, float | None] = {}
    dt: dict[str, pd.Timestamp | None] = {}
    for k, code in want.items():
        try:
            lv = latest_value(raw, code)  # (date,value)
        except Exception:
            lv = None
        v[k] = _to_float(lv)
        dt[k] = _asof_date(lv)

    # EGM
    egm = None
    eps_dil = v.get("eps")
    eps_fy1 = v.get("eps_fy1")
    if eps_dil is not None and eps_fy1 is not None and eps_dil != 0:
        egm = (eps_fy1 - eps_dil) / eps_dil

    # EPS 5y CAGR (uses EPS_WO_NRI series)
    eps_ts = raw.loc[raw["metric_code"] == METRIC["EPS_WO_NRI"], ["as_of_date", "metric_value"]].copy()
    if not eps_ts.empty:
        eps_ts["as_of_date"] = pd.to_datetime(eps_ts["as_of_date"], errors="coerce")
        eps_ts["metric_value"] = pd.to_numeric(eps_ts["metric_value"], errors="coerce")
    eps_5y = (
        compute_cagr_window(eps_ts["metric_value"], eps_ts["as_of_date"], years=5, require_positive=True)
        if not eps_ts.empty
        else None
    )
    eps_asof = (
        pd.to_datetime(eps_ts["as_of_date"], errors="coerce").max()
        if not eps_ts.empty and eps_ts["as_of_date"].notna().any()
        else None
    )

    # Price CAGRs
    price_ts = raw.loc[raw["metric_code"] == METRIC["PRICE"], ["as_of_date", "metric_value"]].copy()
    if not price_ts.empty:
        price_ts["as_of_date"] = pd.to_datetime(price_ts["as_of_date"], errors="coerce")
        price_ts["metric_value"] = pd.to_numeric(price_ts["metric_value"], errors="coerce")

    if price_ts.empty:
        price_3y = price_5y = price_all = None
        price_asof = None
    else:
        price_3y = compute_cagr_window(price_ts["metric_value"], price_ts["as_of_date"], years=3, require_positive=True)
        price_5y = compute_cagr_window(price_ts["metric_value"], price_ts["as_of_date"], years=5, require_positive=True)
        price_all = compute_cagr(price_ts["metric_value"], price_ts["as_of_date"], require_positive=True)
        price_asof = (
            pd.to_datetime(price_ts["as_of_date"], errors="coerce").max() if price_ts["as_of_date"].notna().any() else None
        )

    # FCF / Net income
    fcf_over_ni = None
    fcf_latest = v.get("fcf")
    ni_latest = v.get("ni")
    if fcf_latest is not None and ni_latest is not None and ni_latest != 0:
        fcf_over_ni = fcf_latest / ni_latest

    # FCF/share growth (CAGR windows)
    fcf_sh_5y = fcf_sh_10y = None
    fcfps_asof = None
    if ts_fcf_ps is not None and not ts_fcf_ps.empty:
        ts2 = ts_fcf_ps.copy()
        ts2["as_of_date"] = pd.to_datetime(ts2["as_of_date"], errors="coerce")
        ts2["metric_value"] = pd.to_numeric(ts2["metric_value"], errors="coerce")

        fcf_sh_5y = compute_cagr_window(ts2["metric_value"], ts2["as_of_date"], years=5, require_positive=True)
        fcf_sh_10y = compute_cagr_window(ts2["metric_value"], ts2["as_of_date"], years=10, require_positive=True)
        if ts2["as_of_date"].notna().any():
            fcfps_asof = pd.to_datetime(ts2["as_of_date"], errors="coerce").max()

    return {
        "want": want,
        "v": v,
        "dt": dt,
        "egm": egm,
        "eps_5y": eps_5y,
        "eps_asof": eps_asof,
        "price_3y": price_3y,
        "price_5y": price_5y,
        "price_all": price_all,
        "price_asof": price_asof,
        "fcf_over_ni": fcf_over_ni,
        "fcf_sh_5y": fcf_sh_5y,
        "fcf_sh_10y": fcf_sh_10y,
        "fcfps_asof": fcfps_asof,
    }


# ----------------------------
# Snapshot renderer (two panes + per-stat hover tooltip)
# ----------------------------
def render_snapshot_stats(items) -> None:
    """
    items:
      - (label, raw_df, ts_fcf_ps_df) OR
      - (label, raw_df, ts_fcf_ps_df, asof_range_by_metric)

    Tooltip:
      - Portfolio: uses asof_range_by_metric[metric_code] if provided
      - Company: uses dt[metric_key]
      - Derived: uses series as-of keys on S (eps_asof / price_asof / fcfps_asof)
    """
    st.markdown("## Snapshot stats")

    # normalize inputs
    norm: list[tuple[str, pd.DataFrame, pd.DataFrame, dict[str, Any] | None]] = []
    for it in items:
        if len(it) == 3:
            label, raw, ts = it
            asof_rng = None
        else:
            label, raw, ts, asof_rng = it
        norm.append((label, raw, ts, asof_rng))

    stats = [(label, _compute_snapshot_stats(raw, ts_fcf_ps=ts), asof_rng) for label, raw, ts, asof_rng in norm]
    n = len(stats)
    if n == 0:
        st.info("No snapshot items.")
        return

    # -------- format helpers --------
    def _fmt_unit(metric_code: str, x: float | None) -> str:
        return format_value_by_unit(x, METRIC_UNIT.get(metric_code), digits=2)

    def _tooltip_for(
        S: dict[str, Any],
        asof_rng: dict[str, Any] | None,
        *,
        metric_key: str | None = None,
        metric_code: str | None = None,
        derived_asof_key: str | None = None,
    ) -> str | None:
        # portfolio range wins
        if asof_rng is not None and metric_code is not None:
            s = _fmt_asof_range(asof_rng.get(str(metric_code)))
            if s:
                return s

        # company point-in-time
        if metric_key is not None:
            s = _fmt_asof(S["dt"].get(metric_key))
            if s:
                return f"As of {s}"

        # derived series as-of (eps_asof / price_asof / fcfps_asof)
        if derived_asof_key is not None:
            s = _fmt_asof(S.get(derived_asof_key))
            if s:
                return f"As of {s}"

        return None

    def _tooltip_for_multi(
        S: dict[str, Any],
        asof_rng: dict[str, Any] | None,
        *,
        parts: list[tuple[str, str, str]],  # (label, metric_key, metric_code)
    ) -> str | None:
        """
        Build tooltip like: 'As of EPS 2028-06-30 • FY1 2028-09-30'
        If portfolio ranges exist for a metric_code, use that (range) for that part.
        """
        out: list[str] = []
        for part_label, metric_key, metric_code in parts:
            if asof_rng is not None:
                s_rng = _fmt_asof_range(asof_rng.get(str(metric_code)))
                if s_rng:
                    out.append(f"{part_label} {s_rng.replace('As of ', '')}")
                    continue
            s = _fmt_asof(S["dt"].get(metric_key))
            if s:
                out.append(f"{part_label} {s}")

        if not out:
            return None
        return "As of " + " • ".join(out)

    def _value_with_qmark(val: str, tip: str | None) -> str:
        if not tip:
            return val
        tip = _html_escape(tip)
        return f'{val} <span title="{tip}" style="cursor: help; opacity: .75;">?</span>'

    # -------- SPEC --------
    ROWS_LEFT = [
        (
            "Balance Sheet",
            [
                ("Interest coverage", "num", "interest_cov", METRIC["INTEREST_COVERAGE"], None),
                ("Debt-to-Equity", "num", "debt_to_equity", METRIC["DEBT_TO_EQUITY"], None),
            ],
        ),
        (
            "Capital Intensity",
            [
                ("CAPEX / Revenue", "unit", "capex_rev", METRIC["CAPEX_TO_REV"], None),
                ("CAPEX / OCF", "unit", "capex_ocf", METRIC["CAPEX_TO_OCF"], None),
            ],
        ),
        (
            "Capital Allocation",
            [
                ("ROE", "unit", "roe", METRIC["ROE"], None),
                ("ROIC", "unit", "roic", METRIC["ROIC"], None),
            ],
        ),
        (
            "Profitability",
            [
                ("Gross margin", "unit", "gross_m", METRIC["GROSS_MARGIN"], None),
                ("Net margin", "unit", "net_m", METRIC["NET_MARGIN"], None),
            ],
        ),
        (
            "Historical Growth",
            [
                ("Revenue (5Y CAGR)", "unit", "rev_5y", METRIC["REV_GROWTH_5Y"], None),
                ("EPS (5Y CAGR)", "pct", "eps_5y", None, "eps_asof"),
            ],
        ),
    ]

    ROWS_RIGHT = [
        (
            "Outlook",
            [
                ("Revenue 3–5Y EST", "unit", "rev_est", METRIC["REV_GROWTH_EST_3_5Y"], None),
                ("EPS LT Growth EST", "unit", "eps_lt", METRIC["EPS_LT_GROWTH_EST"], None),
            ],
        ),
        (
            "Valuation",
            [
                ("Forward P/E", "num", "fwd_pe", METRIC["FWD_PE"], None),
                ("PEG", "num", "peg", METRIC["PEG"], None),
            ],
        ),
        (
            "Value Creation",
            [
                ("CAGR 3Y", "pct", "price_3y", None, "price_asof"),
                ("CAGR 5Y", "pct", "price_5y", None, "price_asof"),
                ("CAGR since start", "pct", "price_all", None, "price_asof"),
            ],
        ),
        ("Expected Return", [("EGM", "pct_multi", "egm", None, None)]),
        (
            "Cashflow",
            [
                ("FCF / Net Income", "pct_multi", "fcf_over_ni", None, None),
                ("FCF/share 5Y", "pct", "fcf_sh_5y", None, "fcfps_asof"),
                ("FCF/share 10Y", "pct", "fcf_sh_10y", None, "fcfps_asof"),
            ],
        ),
    ]

    def _render_block(container, spec):
        # entity label row per half (only when compare)
        if n >= 2:
            cols = container.columns([1.5] + [1.0] * n, gap="large")
            cols[0].markdown(" ")
            for i, (lab, _, _) in enumerate(stats, start=1):
                cols[i].markdown(f"**{lab}**")
        else:
            container.markdown(f"### {stats[0][0]}")

        def _render_row(row_label, kind, metric_key, metric_code, derived_asof_key):
            cols = container.columns([1.5] + [1.0] * n, gap="large")
            cols[0].markdown(row_label)

            for i, (_, S, asof_rng) in enumerate(stats, start=1):
                if kind == "num":
                    val = fmt_num(S["v"].get(metric_key), digits=2)
                    tip = _tooltip_for(S, asof_rng, metric_key=metric_key, metric_code=metric_code)
                elif kind == "unit":
                    val = _fmt_unit(metric_code, S["v"].get(metric_key))
                    tip = _tooltip_for(S, asof_rng, metric_key=metric_key, metric_code=metric_code)
                elif kind == "pct":
                    val = fmt_pct(S.get(metric_key), digits=2)
                    tip = _tooltip_for(S, asof_rng, derived_asof_key=derived_asof_key)
                elif kind == "pct_multi":
                    val = fmt_pct(S.get(metric_key), digits=2)
                    if metric_key == "egm":
                        tip = _tooltip_for_multi(
                            S,
                            asof_rng,
                            parts=[
                                ("EPS", "eps", METRIC["EPS_DILUTED"]),
                                ("FY1", "eps_fy1", METRIC["EPS_FY1_EST"]),
                            ],
                        )
                    elif metric_key == "fcf_over_ni":
                        tip = _tooltip_for_multi(
                            S,
                            asof_rng,
                            parts=[
                                ("FCF", "fcf", METRIC["FCF"]),
                                ("NI", "ni", METRIC["NET_INCOME"]),
                            ],
                        )
                    else:
                        tip = None
                else:
                    val = "—"
                    tip = None

                cols[i].markdown(_value_with_qmark(val, tip), unsafe_allow_html=True)

        for section_title, rows in spec:
            container.markdown(f"**{section_title}**")
            for row in rows:
                _render_row(*row)
            container.markdown("")

    left, right = st.columns(2, gap="large")
    _render_block(left, ROWS_LEFT)
    _render_block(right, ROWS_RIGHT)


# ----------------------------
# Charts
# ----------------------------
def render_chart_fcf_yield(
    ts_fcf_yield: pd.DataFrame,
    *,
    W: int,
    H: int,
    ts_fcf_yield_b: pd.DataFrame | None = None,
    label_a: str = "A",
    label_b: str = "B",
) -> None:
    st.subheader("FCF Yield %")
    if ts_fcf_yield is None or ts_fcf_yield.empty:
        st.warning("No FCF Yield % data.")
        return

    df_a = ts_fcf_yield.copy()
    df_a["entity"] = label_a

    has_b = ts_fcf_yield_b is not None and not ts_fcf_yield_b.empty
    if has_b:
        df_b = ts_fcf_yield_b.copy()
        df_b["entity"] = label_b
        df = pd.concat([df_a, df_b], ignore_index=True)
    else:
        df = df_a

    avg = float(df_a.dropna(subset=["metric_value"])["metric_value"].mean())
    st.caption(f"All-time avg: **{avg:,.4g}** (red dotted)")

    if has_b:
        base = alt.Chart(df.dropna(subset=["as_of_date", "metric_value"])).mark_line().encode(
            x=alt.X("as_of_date:T", title=""),
            y=alt.Y("metric_value:Q", title=METRIC["FCF_YIELD"]),
            color=alt.Color(
                "entity:N",
                legend=alt.Legend(orient="bottom", direction="horizontal", title=None, labelLimit=0, columns=1),
            ),
            tooltip=["entity:N", "as_of_date:T", "metric_value:Q"],
        )
        st.altair_chart(alt.vconcat(base.properties(width=W, height=H)), width="stretch")
    else:
        c1 = line_with_mean(df_a, y_title=METRIC["FCF_YIELD"], width=W, height=H)
        leg = legend_only(label="FCF Yield % (line) + All-time avg (red dotted)", width=W)
        st.altair_chart(alt.vconcat(c1, leg), width="stretch")

    with st.expander("Data", expanded=False):
        st.dataframe(df, width="stretch")


def render_chart_relative_growth(
    raw2: pd.DataFrame,
    *,
    W: int,
    H: int,
    raw2_b: pd.DataFrame | None = None,
    label_a: str = "A",
    label_b: str = "B",
) -> None:
    st.subheader("Relative Growth (log)")
    st.caption("Price vs Owner Earnings (Actual → Estimate)")

    if raw2 is None or raw2.empty:
        st.warning("No data for Price / OE / estimates.")
        return

    df_rel_a, plot_a, cagr_a = build_relative_growth(raw2)
    if df_rel_a.empty:
        st.warning("Not enough overlapping positive data to align Price and Owner Earnings.")
        return

    has_b = raw2_b is not None and not raw2_b.empty

    # KPIs
    if has_b:
        st.markdown(f"**{label_a}**")

    k1, k2, k3 = st.columns(3)
    k1.metric("CAGR Price", "—" if cagr_a.get("price") is None else f"{cagr_a['price']*100:,.2f}%")
    k2.metric("CAGR OE", "—" if cagr_a.get("oe_act") is None else f"{cagr_a['oe_act']*100:,.2f}%")
    k3.metric("CAGR OE est", "—" if cagr_a.get("oe_est") is None else f"{cagr_a['oe_est']*100:,.2f}%")

    if has_b:
        df_rel_b, plot_b, cagr_b = build_relative_growth(raw2_b)  # type: ignore[arg-type]
        st.markdown(f"**{label_b}**")
        k1b, k2b, k3b = st.columns(3)
        k1b.metric("CAGR Price", "—" if cagr_b.get("price") is None else f"{cagr_b['price']*100:,.2f}%")
        k2b.metric("CAGR OE", "—" if cagr_b.get("oe_act") is None else f"{cagr_b['oe_act']*100:,.2f}%")
        k3b.metric("CAGR OE est", "—" if cagr_b.get("oe_est") is None else f"{cagr_b['oe_est']*100:,.2f}%")
    else:
        df_rel_b = plot_b = None

    has_b_plot = has_b and isinstance(df_rel_b, pd.DataFrame) and not df_rel_b.empty

    def _selname(s: str) -> str:
        return "".join(ch if ch.isalnum() else "_" for ch in str(s))

    # Charts
    cA = indexed_log_chart(
        df_rel_a,
        width=W,
        height=int(H * 0.75) if has_b_plot else H,
        title=label_a if has_b_plot else "",
        color_domain=[SERIES["PRICE"], SERIES["OE_ACT"], SERIES["OE_EST"]],
        color_range=["#1f77b4", "#2ca02c", "#d62728"],
        legend_orient="bottom",
        selection_name=f"legend_{_selname(label_a)}",  # NEW
    )

    if has_b_plot:
        cB = indexed_log_chart(
            df_rel_b,  # type: ignore[arg-type]
            width=W,
            height=int(H * 0.75),
            title=label_b,
            color_domain=[SERIES["PRICE"], SERIES["OE_ACT"], SERIES["OE_EST"]],
            color_range=["#1f77b4", "#2ca02c", "#d62728"],
            legend_orient="bottom",
            selection_name=f"legend_{_selname(label_b)}",  # NEW
        )
        st.altair_chart(alt.vconcat(cA, cB, spacing=14), width="stretch")
    else:
        if has_b and not has_b_plot:
            st.warning(f"{label_b}: not enough overlapping positive data to plot Relative Growth (log).")
        st.altair_chart(cA, width="stretch")

    with st.expander("Data", expanded=False):
        show = ["price", "oe_actual", "oe_est", "price_idx", "oe_actual_idx", "oe_est_idx"]
        st.markdown(f"**{label_a}**")
        st.dataframe(plot_a[show].reset_index().rename(columns={"index": "as_of_date"}), width="stretch")

        if has_b and plot_b is not None and isinstance(plot_b, pd.DataFrame) and not plot_b.empty:
            st.markdown(f"**{label_b}**")
            st.dataframe(plot_b[show].reset_index().rename(columns={"index": "as_of_date"}), width="stretch")



# ---- shared helper for FCF/share chart ----
def _prep_indexed_positive_series(ts: pd.DataFrame) -> tuple[pd.DataFrame, pd.Timestamp, float, float | None]:
    """
    Returns:
      - ts3: filtered ts from first positive point onward with index_value
      - base_date
      - base_val
      - cagr (raw series CAGR)
    Raises ValueError if no positive points.
    """
    ts2 = ts.dropna(subset=["as_of_date", "metric_value"]).sort_values("as_of_date").copy()
    ts2["as_of_date"] = pd.to_datetime(ts2["as_of_date"], errors="coerce")
    ts2["metric_value"] = pd.to_numeric(ts2["metric_value"], errors="coerce")

    pos = ts2["metric_value"] > 0
    if not bool(pos.any()):
        raise ValueError("No positive datapoints")

    base_date = pd.to_datetime(ts2.loc[pos, "as_of_date"]).min()
    base_val = float(ts2.loc[ts2["as_of_date"] == base_date, "metric_value"].iloc[0])

    ts3 = ts2.loc[ts2["as_of_date"] >= base_date].copy()
    ts3["index_value"] = index_to_100(ts3["metric_value"], base_val)

    cagr = compute_cagr(ts3["metric_value"].to_numpy(), ts3["as_of_date"], require_positive=True)
    return ts3, base_date, base_val, cagr


def render_chart_fcf_share(
    ts_fcf_ps: pd.DataFrame,
    *,
    W: int,
    H: int,
    ts_fcf_ps_b: pd.DataFrame | None = None,
    label_a: str = "A",
    label_b: str = "B",
) -> None:
    st.subheader("FCF/share Growth (log)")
    st.caption("Indexed to 100 at first positive point")

    if ts_fcf_ps is None or ts_fcf_ps.empty:
        st.warning("No FCF/share data.")
        return

    has_b = ts_fcf_ps_b is not None and not ts_fcf_ps_b.empty

    # ---- A ----
    try:
        ts3, base_date, base_val, cagr3 = _prep_indexed_positive_series(ts_fcf_ps)
    except ValueError:
        st.warning("No positive FCF/share datapoints (log view needs > 0).")
        return

    if not has_b:
        k1, k2, k3 = st.columns(3)
        k1.metric("CAGR FCF/sh", "—" if cagr3 is None else f"{cagr3*100:,.2f}%")
        k2.metric("Start", str(pd.to_datetime(base_date).date()))
        k3.metric("Base", f"{base_val:,.4g}")
    else:
        st.markdown(f"**{label_a}**")
        k1, k2, k3 = st.columns(3)
        k1.metric("CAGR FCF/sh", "—" if cagr3 is None else f"{cagr3*100:,.2f}%")
        k2.metric("Start", str(pd.to_datetime(base_date).date()))
        k3.metric("Base", f"{base_val:,.4g}")

    dfA = (
        ts3.assign(series_name=SERIES["FCF_PS"], raw_value=ts3["metric_value"])
        .loc[:, ["as_of_date", "series_name", "raw_value", "index_value"]]
        .dropna()
    )
    dfA = dfA[dfA["index_value"] > 0].copy()
    dfA["entity"] = label_a

    # ---- B (optional) ----
    df_all = dfA
    if has_b and ts_fcf_ps_b is not None:
        try:
            tsb3, base_date_b, base_val_b, cagr_b = _prep_indexed_positive_series(ts_fcf_ps_b)
            st.markdown(f"**{label_b}**")
            k1b, k2b, k3b = st.columns(3)
            k1b.metric("CAGR FCF/sh", "—" if cagr_b is None else f"{cagr_b*100:,.2f}%")
            k2b.metric("Start", str(pd.to_datetime(base_date_b).date()))
            k3b.metric("Base", f"{base_val_b:,.4g}")

            dfB = (
                tsb3.assign(series_name=SERIES["FCF_PS"], raw_value=tsb3["metric_value"])
                .loc[:, ["as_of_date", "series_name", "raw_value", "index_value"]]
                .dropna()
            )
            dfB = dfB[dfB["index_value"] > 0].copy()
            dfB["entity"] = label_b

            df_all = pd.concat([dfA, dfB], ignore_index=True)
        except ValueError:
            st.warning(f"{label_b}: No positive FCF/share datapoints (log view needs > 0).")

    # ---- Chart ----
    if not has_b:
        c3 = indexed_log_chart(
            dfA.drop(columns=["entity"], errors="ignore"),
            width=W,
            height=H,
            title="",
            color_domain=[SERIES["FCF_PS"]],
            color_range=["#1f77b4"],
            legend_orient="bottom",
        )
        st.altair_chart(c3, width="stretch")
    else:
        c3 = (
            alt.Chart(df_all)
            .mark_line()
            .encode(
                x=alt.X("as_of_date:T", title=""),
                y=alt.Y("index_value:Q", title="Indexed (base=100)"),
                color=alt.Color(
                    "entity:N",
                    legend=alt.Legend(orient="bottom", direction="horizontal", title=None, labelLimit=0, columns=1),
                ),
                tooltip=["entity:N", "as_of_date:T", "raw_value:Q", "index_value:Q"],
            )
            .properties(width=W, height=H)
        )
        st.altair_chart(c3, width="stretch")

    with st.expander("Data", expanded=False):
        if not has_b:
            st.dataframe(ts3.reset_index(drop=True), width="stretch")
        else:
            st.dataframe(df_all.reset_index(drop=True), width="stretch")
