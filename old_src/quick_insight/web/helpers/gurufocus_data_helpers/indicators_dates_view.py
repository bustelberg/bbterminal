from __future__ import annotations

from dataclasses import asdict
from typing import Any
import io
from contextlib import redirect_stdout, redirect_stderr

import streamlit as st

from quick_insight.ingest.gurufocus.stock_indicator.read_indicator import (
    IndicatorDateRange,
    read_indicator_dates,
)
from quick_insight.ingest.gurufocus.stock_indicator.load_delta_indicator import (
    run_apply_indicator_delta_insert_only,
)
from quick_insight.config.indicators import INDICATOR_ALLOWLIST

from quick_insight.web.helpers.gurufocus_data_helpers.ingest_runner import (
    S_OK,
    S_ERROR,
    S_SKIP,
    S_BLOCKED,
)


# ── cached fetch (dates) ──────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=60 * 30)  # 30 min
def get_indicator_date_range(
    ticker: str,
    exchange: str,
    indicator_key: str,
    *,
    type_: str | None = "auto",
) -> IndicatorDateRange:
    t = (ticker or "").strip().upper()
    x = (exchange or "").strip().upper()
    k = (indicator_key or "").strip()
    if not t or not x or not k:
        raise ValueError("ticker, exchange, and indicator_key are required")
    return read_indicator_dates(t, x, k, type_=type_)


def _run_with_print_capture(fn, *args, **kwargs) -> tuple[Any, str]:
    buf = io.StringIO()
    with redirect_stdout(buf), redirect_stderr(buf):
        result = fn(*args, **kwargs)
    return result, buf.getvalue()


def _badge_for_result(res: Any) -> str:
    try:
        if bool(getattr(res, "did_nothing", False)):
            return f"{S_SKIP} no changes"
        return f"{S_OK} applied"
    except Exception:
        return f"{S_OK} done"


def _fmt_range(r: IndicatorDateRange) -> str:
    type_label = r.type_ if r.type_ is not None else "none"
    return f"{r.first_date or '—'} → {r.last_date or '—'} ({r.count:,})  •  type={type_label}"


def _delta_popup_from_result(res: Any) -> tuple[str, str]:
    """
    Use ApplyIndicatorDeltaInsertOnlyResult fields to show a clean from->to message.
    Returns (level, message) where level is: 'success'|'info'|'warning'
    """
    did_nothing = bool(getattr(res, "did_nothing", False))
    reason = getattr(res, "reason", "") or ""

    before_last = getattr(res, "before_last_date", None)
    after_last = getattr(res, "after_last_date", None)

    rows_selected_new = getattr(res, "rows_selected_new", None)
    indicator_key = getattr(res, "indicator_key", None)

    if did_nothing:
        msg = f"No new data for **{indicator_key}** (already up to date)."
        if reason:
            msg += f"\n\nReason: {reason}"
        return ("info", msg)

    moved = (after_last is not None) and (after_last != before_last)
    if moved:
        msg = f"**{indicator_key}**: {before_last or '—'} ➜ {after_last or '—'}"
    else:
        msg = f"**{indicator_key}** delta applied, but last date did not change."

    if rows_selected_new is not None:
        msg += f"\n\nNew rows inserted: **{rows_selected_new:,}**"

    return ("success" if moved else "warning", msg)


def _clear_indicator_cache_for_company(ticker: str, exchange: str) -> None:
    """
    Clear Streamlit cache for indicator date reads (all keys/types) for this company.
    (Brute-force clear is OK because it's local UI cache.)
    """
    get_indicator_date_range.clear()


# ── UI renderer ───────────────────────────────────────────────────────────────

def render_indicators_date_range_ui(
    *,
    ticker: str,
    exchange: str,
    title: str = "Indicators — date coverage",
    type_: str | None = "auto",
    lookback_days: int = 30,
    timeout: int = 60,
) -> None:
    """
    Shows all indicators in INDICATOR_ALLOWLIST with:
      - current cached date range (if present)
      - a per-indicator fetch/apply delta button
      - per-indicator popup + logs from the runner

    Notes:
      - source_code fixed to gurufocus_api
      - require_company_exists fixed to True
      - type_ defaults to 'auto' (mirrors fetcher logic)
    """
    t = (ticker or "").strip().upper()
    x = (exchange or "").strip().upper()

    st.markdown(f"**{title}**")
    if not t or not x:
        st.info("Pick a ticker + exchange to inspect indicator coverage.")
        return

    # Compact controls (optional, but helpful)
    with st.expander("Settings", expanded=False):
        c1, c2, c3 = st.columns([1, 1, 1])
        type_ = c1.selectbox(
            "Type",
            options=["auto", "q", "a", "ttm", "none"],
            index=0,
            help="Matches read_indicator_dates semantics. 'none' means market-only.",
            key=f"gf_ind_type_{x}_{t}",
        )
        type_resolved: str | None
        if type_ == "none":
            type_resolved = None
        else:
            type_resolved = type_  # "auto"|"q"|"a"|"ttm"

        lookback_days = c2.number_input(
            "Lookback days",
            min_value=0,
            max_value=3650,
            value=int(lookback_days),
            step=1,
            help="Passed to delta runner. Re-check recent window for changes.",
            key=f"gf_ind_lookback_{x}_{t}",
        )
        timeout = c3.number_input(
            "Timeout (s)",
            min_value=5,
            max_value=600,
            value=int(timeout),
            step=5,
            help="Passed to delta runner.",
            key=f"gf_ind_timeout_{x}_{t}",
        )

        if st.button(
            "Clear indicator UI cache",
            key=f"gf_ind_clear_cache_{x}_{t}",
            use_container_width=True,
        ):
            _clear_indicator_cache_for_company(t, x)
            st.success("Cleared cached indicator date ranges.")

    st.caption("Each row shows the cached date range (if present) and lets you fetch updates per indicator.")

    # Render a row per indicator
    for indicator_key in INDICATOR_ALLOWLIST:
        row_key = indicator_key.replace("/", "_")
        ss_key_popup = f"gf_ind_popup_{x}_{t}_{row_key}"
        ss_key_log = f"gf_ind_log_{x}_{t}_{row_key}"

        with st.container(border=True):
            left, right = st.columns([3, 2])

            # Left: indicator name + current range (auto)
            with left:
                st.markdown(f"**{indicator_key}**")

                try:
                    r = get_indicator_date_range(
                        t,
                        x,
                        indicator_key,
                        type_=type_resolved,  # type: ignore[arg-type]
                    )
                except FileNotFoundError:
                    st.caption("— (no cached file yet)")
                    r = None
                except Exception as e:
                    st.caption("—")
                    st.error(str(e))
                    r = None
                else:
                    st.caption(_fmt_range(r))

            # Right: fetch button
            with right:
                btn = st.button(
                    f"Fetch {indicator_key}",
                    type="primary",
                    key=f"gf_ind_fetch_{x}_{t}_{row_key}",
                    use_container_width=True,
                )

            if btn:
                # Force fresh re-read after run
                get_indicator_date_range.clear()

                with st.status(f"Fetching {indicator_key} for {x}:{t}…", expanded=False) as status:
                    try:
                        res, logs = _run_with_print_capture(
                            run_apply_indicator_delta_insert_only,
                            primary_ticker=t,
                            primary_exchange=x,
                            indicator_key=indicator_key,
                            type_=type_resolved,  # "auto"|"q"|"a"|"ttm"|None
                            lookback_days=int(lookback_days),
                            timeout=int(timeout),
                            require_company_exists=True,
                            source_code="gurufocus_api",
                            verbose=True,
                        )
                    except Exception as e:
                        status.update(label=f"{S_ERROR} Fetch failed: {indicator_key}", state="error")
                        st.error(f"{S_ERROR} Indicator delta apply failed for {x}:{t} [{indicator_key}]\n\n{e}")
                    else:
                        badge = _badge_for_result(res)
                        status.update(label=f"{badge} — finished: {indicator_key}", state="complete")

                        level, msg = _delta_popup_from_result(res)
                        st.session_state[ss_key_popup] = (level, msg)
                        st.session_state[ss_key_log] = logs

                        # Re-show updated range (best effort)
                        try:
                            r2 = get_indicator_date_range(
                                t,
                                x,
                                indicator_key,
                                type_=type_resolved,  # type: ignore[arg-type]
                            )
                            st.caption(_fmt_range(r2))
                        except Exception:
                            pass

            # Popup + logs for this indicator
            if st.session_state.get(ss_key_popup):
                level, msg = st.session_state[ss_key_popup]
                if level == "success":
                    st.success(msg)
                elif level == "warning":
                    st.warning(msg)
                else:
                    st.info(msg)

            logs = st.session_state.get(ss_key_log, "")
            if logs.strip():
                with st.expander("Logs (captured from runner)", expanded=False):
                    st.text_area(
                        "stdout/stderr",
                        value=logs,
                        height=220,
                        key=f"gf_ind_logs_area_{x}_{t}_{row_key}",
                    )