"""
GuruFocus Data page
─────────────────────────────────────────────────────────────────────────────
Two sections:
  1. Coverage table — all companies, searchable, showing row counts & last date
  2. Ingest panel  — pick companies (or "all missing"), run live, see results
Plus:
  3. Inspect panel — pick one company and view analyst estimate + financials + indicators date coverage

Change:
  - The "Run ingest" button is replaced by a single "Get ALL data…" button for the selected inspect company.
  - That button runs: Analyst Estimates delta + Financials delta + ALL indicator deltas (allowlist),
    with a progress bar + live status + summary.
"""
from __future__ import annotations

import time
import warnings

import pandas as pd
import streamlit as st

from quick_insight.web.ui import setup_page
from quick_insight.web.helpers.gurufocus_data_helpers.repo import GFDataRepo

from quick_insight.web.helpers.gurufocus_data_helpers.ingest_runner import (
    S_OK,
    S_SKIP,
    S_BLOCKED,
    S_ERROR,
)

from quick_insight.web.helpers.gurufocus_data_helpers.analyst_estimate_dates_view import (
    render_analyst_estimate_date_range_ui,
    get_analyst_estimate_date_range,
)
from quick_insight.web.helpers.gurufocus_data_helpers.financials_dates_view import (
    render_financials_date_range_ui,
    get_financials_date_range,
)
from quick_insight.web.helpers.gurufocus_data_helpers.indicators_dates_view import (
    render_indicators_date_range_ui,
)
from quick_insight.web.helpers.gurufocus_data_helpers.indicators_dates_view import (
    get_indicator_date_range,  # type: ignore
)

from quick_insight.ingest.gurufocus.analyst_estimates.load_delta_analyst_estimate import (
    run_apply_analyst_estimate_delta,
)
from quick_insight.ingest.gurufocus.financials.load_delta_financials import (
    run_apply_financials_delta_insert_only,
)
from quick_insight.ingest.gurufocus.stock_indicator.load_delta_indicator import (
    run_apply_indicator_delta_insert_only,
)

from quick_insight.config.indicators import INDICATOR_ALLOWLIST

setup_page()

_repo = GFDataRepo()


@st.cache_data(show_spinner=False)
def _cached_coverage() -> pd.DataFrame:
    cov = _repo.company_metric_coverage()
    snap = _repo.latest_snapshot_per_company()
    df = cov.merge(snap, on="company_id", how="left")
    df["latest_snapshot_date"] = df["latest_snapshot_date"].astype(str).replace("NaT", "—")
    return df


def _bust_cache() -> None:
    _cached_coverage.clear()


def _status_badge(has_data: bool) -> str:
    return "✅ has data" if has_data else "❌ missing"


def _build_company_labels(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["company_name"] = out["company_name"].fillna("").astype(str)
    out["primary_ticker"] = out["primary_ticker"].fillna("").astype(str)
    out["primary_exchange"] = out["primary_exchange"].fillna("").astype(str)
    out["label"] = (
        out["company_name"].str.strip()
        + " — "
        + out["primary_ticker"].str.strip()
        + " ("
        + out["primary_exchange"].str.strip()
        + ")"
    )
    return out


def _run_step(fn, **kwargs) -> dict:
    """
    Run one delta step with very defensive result normalization.

    Returns dict with keys:
      - overall: S_OK|S_SKIP|S_ERROR|S_BLOCKED|...
      - note: str
      - raw: original result object (optional)
    """
    try:
        res = fn(**kwargs)
    except Exception as e:
        return {"overall": S_ERROR, "note": str(e), "raw": None}

    did_nothing = bool(getattr(res, "did_nothing", False))
    if did_nothing:
        reason = getattr(res, "reason", "") or "no changes"
        return {"overall": S_SKIP, "note": reason, "raw": res}

    return {"overall": S_OK, "note": "applied", "raw": res}


def _api_cost_for_selected_company() -> int:
    # 1 for analyst estimates + 1 for financials + 1 per indicator (allowlist)
    return 2 + len(list(INDICATOR_ALLOWLIST))


st.title("GuruFocus Data")

# ═════════════════════════════════════════════════════════════════════════════
# Section 1 — Coverage overview
# ═════════════════════════════════════════════════════════════════════════════
st.subheader("Coverage overview")

with st.spinner("Loading coverage…"):
    coverage = _cached_coverage()

total = len(coverage)
has_data = int(coverage["has_data"].sum()) if total else 0
missing = total - has_data

m1, m2, m3 = st.columns(3)
m1.metric("Total companies", total)
m2.metric("Have data", has_data)
m3.metric("Missing data", missing)

st.divider()

search = st.text_input(
    "Search companies",
    placeholder="Name, ticker, exchange, sector…",
    key="gf_search",
)
show_filter = st.radio(
    "Show",
    ["All", "Missing data only", "Has data only"],
    horizontal=True,
    key="gf_filter",
)

df_view = coverage.copy()

if search.strip():
    q = search.strip().lower()
    mask = (
        df_view["company_name"].astype(str).str.lower().str.contains(q, na=False)
        | df_view["primary_ticker"].astype(str).str.lower().str.contains(q, na=False)
        | df_view["primary_exchange"].astype(str).str.lower().str.contains(q, na=False)
        | df_view["sector"].astype(str).str.lower().str.contains(q, na=False)
    )
    df_view = df_view[mask]

if show_filter == "Missing data only":
    df_view = df_view[~df_view["has_data"]]
elif show_filter == "Has data only":
    df_view = df_view[df_view["has_data"]]

st.caption(f"{len(df_view)} of {total} companies shown.")

if df_view.empty:
    st.info("No companies match your filter.")
else:
    display = df_view[
        [
            "company_name",
            "primary_ticker",
            "primary_exchange",
            "sector",
            "has_data",
            "facts_number_rows",
            "facts_text_rows",
            "distinct_metrics",
            "latest_snapshot_date",
        ]
    ].copy()
    display["has_data"] = display["has_data"].map(_status_badge)

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        st.dataframe(
            display,
            width="stretch",
            hide_index=True,
            column_config={
                "company_name": st.column_config.TextColumn("Company"),
                "primary_ticker": st.column_config.TextColumn("Ticker"),
                "primary_exchange": st.column_config.TextColumn("Exchange"),
                "sector": st.column_config.TextColumn("Sector"),
                "has_data": st.column_config.TextColumn("Status"),
                "facts_number_rows": st.column_config.NumberColumn("Facts (num)", format="%d"),
                "facts_text_rows": st.column_config.NumberColumn("Facts (text)", format="%d"),
                "distinct_metrics": st.column_config.NumberColumn("Metrics", format="%d"),
                "latest_snapshot_date": st.column_config.TextColumn("Latest snapshot"),
            },
        )

# ═════════════════════════════════════════════════════════════════════════════
# Section 2 — Ingest panel (selection only; button repurposed)
# ═════════════════════════════════════════════════════════════════════════════
st.divider()
st.subheader("Run GuruFocus ingest")

all_companies = _build_company_labels(
    coverage[["company_id", "company_name", "primary_ticker", "primary_exchange", "has_data"]]
)

missing_companies = all_companies[~all_companies["has_data"]]

target_mode = st.radio(
    "Which companies to ingest?",
    ["All missing data", "Specific companies"],
    horizontal=True,
    key="gf_target_mode",
)

if target_mode == "Specific companies":
    selected_labels = st.multiselect(
        "Select companies",
        options=all_companies["label"].tolist(),
        key="gf_selected_companies",
    )
    target_df = all_companies[all_companies["label"].isin(selected_labels)]
else:
    target_df = missing_companies
    if target_df.empty:
        st.success("🎉 All companies already have data. Nothing to ingest.")
    else:
        st.info(f"{len(target_df)} companies have no data and will be ingested.")

use_cache = st.checkbox(
    "Use cached API responses (skip re-fetching from GuruFocus)",
    value=True,
    key="gf_use_cache",
)

# ═════════════════════════════════════════════════════════════════════════════
# Section 3 — Inspect single company
# ═════════════════════════════════════════════════════════════════════════════
st.divider()
st.subheader("Inspect one company")

inspect_ticker = ""
inspect_exchange = ""
inspect_label = ""

if all_companies.empty:
    st.info("No companies available to inspect.")
else:
    inspect_label = st.selectbox(
        "Company to inspect",
        options=all_companies["label"].tolist(),
        key="gf_inspect_company",
    )
    inspect_row = all_companies.loc[all_companies["label"] == inspect_label].head(1)
    inspect_ticker = inspect_row["primary_ticker"].iloc[0] if not inspect_row.empty else ""
    inspect_exchange = inspect_row["primary_exchange"].iloc[0] if not inspect_row.empty else ""

    st.markdown("### Data coverage per source")

    col_a, col_b = st.columns(2)

    with col_a:
        with st.expander("Analyst estimate dates", expanded=False):
            render_analyst_estimate_date_range_ui(
                ticker=inspect_ticker,
                exchange=inspect_exchange,
                title="Analyst estimates — date coverage",
            )

    with col_b:
        with st.expander("Financials dates", expanded=False):
            render_financials_date_range_ui(
                ticker=inspect_ticker,
                exchange=inspect_exchange,
                title="Financials — date coverage",
            )

    with st.expander("Indicators dates", expanded=False):
        render_indicators_date_range_ui(
            ticker=inspect_ticker,
            exchange=inspect_exchange,
            title="Indicators — date coverage",
        )

# ═════════════════════════════════════════════════════════════════════════════
# Bulk button (for the selected inspect company)
# ═════════════════════════════════════════════════════════════════════════════
st.divider()
st.subheader("Get all GuruFocus data for selected company")

api_cost = _api_cost_for_selected_company()
btn_text = f"Get ALL data from GuruFocus for this stock {inspect_label} — this costs {api_cost} API requests"

can_run_all = bool(inspect_ticker and inspect_exchange) and not st.session_state.get("gf_all_running", False)

if st.button(btn_text, type="primary", disabled=not can_run_all, key="gf_run_all_btn"):
    st.session_state["gf_all_running"] = True
    st.session_state["gf_all_company"] = (inspect_ticker, inspect_exchange, inspect_label)
    st.session_state["gf_all_results"] = []
    st.rerun()

if st.session_state.get("gf_all_running", False):
    t, x, lbl = st.session_state.get("gf_all_company", ("", "", ""))

    steps: list[tuple[str, callable, dict]] = []

    steps.append(
        (
            "Analyst estimates",
            run_apply_analyst_estimate_delta,
            dict(
                primary_ticker=t,
                primary_exchange=x,
                source_code="gurufocus_api",
                require_company_exists=True,
                lookback_periods=0,
                verbose=True,
            ),
        )
    )
    steps.append(
        (
            "Financials",
            run_apply_financials_delta_insert_only,
            dict(
                primary_ticker=t,
                primary_exchange=x,
                source_code="gurufocus_api",
                require_company_exists=True,
                lookback_periods=0,
                verbose=True,
            ),
        )
    )

    for key in INDICATOR_ALLOWLIST:
        steps.append(
            (
                f"Indicator: {key}",
                run_apply_indicator_delta_insert_only,
                dict(
                    primary_ticker=t,
                    primary_exchange=x,
                    indicator_key=key,
                    type_="auto",
                    lookback_days=30,
                    timeout=60,
                    require_company_exists=True,
                    source_code="gurufocus_api",
                    verbose=True,
                ),
            )
        )

    total_steps = len(steps)
    st.markdown(f"**Running {total_steps} steps for {lbl}…**")

    progress = st.progress(0)
    status = st.empty()
    table_slot = st.empty()

    results: list[dict] = []
    for i, (name, fn, kwargs) in enumerate(steps, start=1):
        status.markdown(f"⏳ **{name}** ({i}/{total_steps})")

        step_res = _run_step(fn, **kwargs)
        results.append(
            {
                "step": name,
                "overall": step_res["overall"],
                "note": step_res["note"],
            }
        )

        progress.progress(i / total_steps)
        table_slot.dataframe(pd.DataFrame(results), width="stretch", hide_index=True)

    # Clear relevant UI caches so panels reflect freshest date ranges
    try:
        get_analyst_estimate_date_range.clear()
    except Exception:
        pass
    try:
        get_financials_date_range.clear()
    except Exception:
        pass
    try:
        get_indicator_date_range.clear()
    except Exception:
        pass

    st.session_state["gf_all_running"] = False
    st.session_state["gf_all_results"] = results

    ok = sum(1 for r in results if r["overall"] == S_OK)
    skip = sum(1 for r in results if r["overall"] == S_SKIP)
    err = sum(1 for r in results if r["overall"] == S_ERROR)
    blk = sum(1 for r in results if r["overall"] == S_BLOCKED)

    if err > 0:
        st.error(f"Done with errors — ✅ {ok} | ⏭ {skip} | 🚫 {blk} | ❌ {err}")
    elif blk > 0:
        st.warning(f"Done (some blocked) — ✅ {ok} | ⏭ {skip} | 🚫 {blk} | ❌ {err}")
    else:
        st.success(f"Done — ✅ {ok} | ⏭ {skip} | 🚫 {blk} | ❌ {err}")

    st.rerun()

elif st.session_state.get("gf_all_results"):
    st.markdown("**Last 'Get ALL data' run:**")
    st.dataframe(pd.DataFrame(st.session_state["gf_all_results"]), width="stretch", hide_index=True)
    if st.button("Clear last run", key="gf_all_clear_last"):
        st.session_state.pop("gf_all_results", None)
        st.rerun()