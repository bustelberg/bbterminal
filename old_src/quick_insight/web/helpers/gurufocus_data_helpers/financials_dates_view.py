from __future__ import annotations

from dataclasses import asdict
from typing import Any
import io
from contextlib import redirect_stdout, redirect_stderr

import streamlit as st

from quick_insight.ingest.gurufocus.financials.read_financials import (
    FinancialsDateRange,
    read_financials_dates,
)
from quick_insight.ingest.gurufocus.financials.load_delta_financials import (
    run_apply_financials_delta_insert_only,
)
from quick_insight.web.helpers.gurufocus_data_helpers.ingest_runner import (
    S_OK,
    S_ERROR,
    S_SKIP,
    S_BLOCKED,
)


# ── cached fetch (dates) ──────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=60 * 30)  # 30 min
def get_financials_date_range(ticker: str, exchange: str) -> FinancialsDateRange:
    t = (ticker or "").strip().upper()
    x = (exchange or "").strip().upper()
    if not t or not x:
        raise ValueError("ticker and exchange are required")
    return read_financials_dates(t, x)


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


def _fmt_range(r: FinancialsDateRange) -> str:
    def dash(a: str | None, b: str | None) -> str:
        if not a and not b:
            return "—"
        if a and b:
            return f"{a} → {b}"
        return a or b or "—"

    freq = getattr(r, "financial_report_frequency", None) or "—"
    return (
        f"Frequency: {freq}  |  "
        f"Annual: {dash(r.annual_first_raw, r.annual_last_raw)} ({r.annual_count})  |  "
        f"Quarterly: {dash(r.quarterly_first_raw, r.quarterly_last_raw)} ({r.quarterly_count})"
    )


def _delta_popup_from_result(res: Any) -> tuple[str, str]:
    """
    Financials delta has explicit old_last and after_last fields.
    We use those to show a clean 'from -> to' message.

    Returns (level, message) where level is: 'success'|'info'|'warning'
    """
    did_nothing = bool(getattr(res, "did_nothing", False))
    reason = getattr(res, "reason", "") or ""

    # Values from ApplyFinancialsDeltaInsertOnlyResult
    a_old = getattr(res, "annual_old_last_raw", None)
    q_old = getattr(res, "quarterly_old_last_raw", None)
    a_new = getattr(res, "annual_last_after_raw", None)
    q_new = getattr(res, "quarterly_last_after_raw", None)

    # If runner says nothing changed, keep it simple
    if did_nothing:
        msg = "No new financial periods found (database already up to date)."
        if reason:
            msg += f"\n\nReason: {reason}"
        return ("info", msg)

    # Detect which tracks actually moved
    moved_annual = (a_new is not None) and (a_new != a_old)
    moved_quarterly = (q_new is not None) and (q_new != q_old)

    lines: list[str] = []
    if moved_annual:
        lines.append(f"**Annual**: {a_old or '—'} ➜ {a_new or '—'}")
    if moved_quarterly:
        lines.append(f"**Quarterly**: {q_old or '—'} ➜ {q_new or '—'}")

    if not lines:
        # Applied *something* but last dates didn't move (could happen with backfill/revisions)
        # Still show counts if present
        rows_selected_new = getattr(res, "rows_selected_new", None)
        msg = "Financials delta applied, but the latest period did not change."
        if rows_selected_new is not None:
            msg += f"\n\nNew rows inserted: {rows_selected_new}"
        return ("warning", msg)

    rows_selected_new = getattr(res, "rows_selected_new", None)
    if rows_selected_new is not None:
        lines.append(f"\nNew rows inserted: **{rows_selected_new:,}**")

    return ("success", "\n".join(lines))


# ── UI renderer ───────────────────────────────────────────────────────────────

def render_financials_date_range_ui(
    *,
    ticker: str,
    exchange: str,
    title: str = "Financials — date coverage",
) -> None:
    """
    Mirrors the analyst-estimates UI:
      - Always show current financials coverage (cached) on selection
      - One button to fetch/apply delta into DuckDB (source_code fixed to gurufocus_api)
      - Only show: friendly from->to popup + captured logs
    """
    t = (ticker or "").strip().upper()
    x = (exchange or "").strip().upper()

    st.markdown(f"**{title}**")
    if not t or not x:
        st.info("Pick a ticker + exchange to inspect financials coverage.")
        return

    # 1) Always show current coverage (cached)
    try:
        current = get_financials_date_range(t, x)
    except Exception as e:
        st.error(f"Failed to read financials dates for {x}:{t}\n\n{e}")
        current = None

    if current is not None:
        st.caption(_fmt_range(current))
        # Keep extra details hidden (optional debug)
        with st.expander("Details", expanded=False):
            st.code(str(current))
            with st.expander("Raw (debug)", expanded=False):
                st.json(asdict(current))

    # 2) One button to fetch/apply delta
    st.divider()
    btn_label = f"Fetch financials from GuruFocus for {x}:{t}"
    run = st.button(
        btn_label,
        type="primary",
        key=f"gf_fin_delta_apply_{x}_{t}",
        use_container_width=True,
    )

    ss_key_log = f"gf_fin_delta_log_{x}_{t}"
    ss_key_popup = f"gf_fin_delta_popup_{x}_{t}"

    if run:
        # Ensure we re-read after run (but still show 'current' above in this run)
        get_financials_date_range.clear()

        with st.status(f"Fetching + applying financials delta for {x}:{t}…", expanded=True) as status:
            try:
                res, logs = _run_with_print_capture(
                    run_apply_financials_delta_insert_only,
                    primary_ticker=t,
                    primary_exchange=x,
                    source_code="gurufocus_api",
                    require_company_exists=True,
                    lookback_periods=0,  # informational only (per your docstring)
                    verbose=True,
                )
            except Exception as e:
                status.update(label=f"{S_ERROR} Fetch failed for {x}:{t}", state="error")
                st.error(f"{S_ERROR} Financials delta apply failed for {x}:{t}\n\n{e}")
            else:
                badge = _badge_for_result(res)
                status.update(label=f"{badge} — finished for {x}:{t}", state="complete")

                level, msg = _delta_popup_from_result(res)
                st.session_state[ss_key_popup] = (level, msg)
                st.session_state[ss_key_log] = logs

                # Refresh coverage caption to reflect the newest cached financials.json
                try:
                    after = get_financials_date_range(t, x)
                except Exception:
                    after = None
                if after is not None:
                    st.caption(_fmt_range(after))

    # 3) Show popup + logs from the last run (persistent across reruns)
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
                height=260,
                key=f"gf_fin_delta_logs_area_{x}_{t}",
            )