from __future__ import annotations

from dataclasses import asdict
from typing import Any
import io
from contextlib import redirect_stdout, redirect_stderr

import streamlit as st

from quick_insight.ingest.gurufocus.analyst_estimates.read_analyst_estimate import (
    AnalystEstimateDateRange,
    read_analyst_estimate_dates,
)
from quick_insight.ingest.gurufocus.analyst_estimates.load_delta_analyst_estimate import (
    run_apply_analyst_estimate_delta,
)
from quick_insight.web.helpers.gurufocus_data_helpers.ingest_runner import (
    S_OK,
    S_ERROR,
    S_SKIP,
    S_BLOCKED,
)


# ── cached fetch (dates) ──────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=60 * 30)  # 30 min
def get_analyst_estimate_date_range(ticker: str, exchange: str) -> AnalystEstimateDateRange:
    t = (ticker or "").strip().upper()
    x = (exchange or "").strip().upper()
    if not t or not x:
        raise ValueError("ticker and exchange are required")
    return read_analyst_estimate_dates(t, x)


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


def _fmt_range(r: AnalystEstimateDateRange) -> str:
    def dash(a: str | None, b: str | None) -> str:
        if not a and not b:
            return "—"
        if a and b:
            return f"{a} → {b}"
        return a or b or "—"

    return (
        f"Annual: {dash(r.annual_first, r.annual_last)} ({r.annual_count})  |  "
        f"Quarterly: {dash(r.quarterly_first, r.quarterly_last)} ({r.quarterly_count})"
    )


def _delta_popup(before: AnalystEstimateDateRange, after: AnalystEstimateDateRange) -> tuple[str, str]:
    """
    Returns (level, message) where level is: 'success'|'info'|'warning'
    """
    changed_annual = (before.annual_first_raw, before.annual_last_raw, before.annual_count) != (
        after.annual_first_raw,
        after.annual_last_raw,
        after.annual_count,
    )
    changed_quarterly = (before.quarterly_first_raw, before.quarterly_last_raw, before.quarterly_count) != (
        after.quarterly_first_raw,
        after.quarterly_last_raw,
        after.quarterly_count,
    )

    if not changed_annual and not changed_quarterly:
        return (
            "info",
            "No new analyst estimate periods found (coverage unchanged).",
        )

    lines: list[str] = []
    if changed_annual:
        lines.append(
            f"**Annual**: {before.annual_first or '—'} → {before.annual_last or '—'} "
            f"({before.annual_count})  ➜  {after.annual_first or '—'} → {after.annual_last or '—'} "
            f"({after.annual_count})"
        )
    if changed_quarterly:
        lines.append(
            f"**Quarterly**: {before.quarterly_first or '—'} → {before.quarterly_last or '—'} "
            f"({before.quarterly_count})  ➜  {after.quarterly_first or '—'} → {after.quarterly_last or '—'} "
            f"({after.quarterly_count})"
        )

    return ("success", "\n\n".join(lines))


# ── UI renderer ───────────────────────────────────────────────────────────────

def render_analyst_estimate_date_range_ui(
    *,
    ticker: str,
    exchange: str,
    title: str = "Analyst estimates — date coverage",
) -> None:
    """
    Simplified UI:
      - Automatically shows current estimate date coverage when a company is selected
      - One button to fetch/apply delta (source_code fixed to gurufocus_api)
      - Shows captured logs + a friendly message about coverage changes (from/to dates)
    """
    t = (ticker or "").strip().upper()
    x = (exchange or "").strip().upper()

    st.markdown(f"**{title}**")
    if not t or not x:
        st.info("Pick a ticker + exchange to inspect analyst estimate coverage.")
        return

    # 1) Always show current coverage (cached)
    try:
        current = get_analyst_estimate_date_range(t, x)
    except Exception as e:
        st.error(f"Failed to read analyst estimate dates for {x}:{t}\n\n{e}")
        current = None

    if current is not None:
        st.caption(_fmt_range(current))
        with st.expander("Details", expanded=False):
            st.code(str(current))
            st.dataframe(
                [
                    {
                        "annual_first": current.annual_first,
                        "annual_last": current.annual_last,
                        "annual_count": current.annual_count,
                        "quarterly_first": current.quarterly_first,
                        "quarterly_last": current.quarterly_last,
                        "quarterly_count": current.quarterly_count,
                    }
                ],
                width="stretch",
                hide_index=True,
            )
            with st.expander("Raw (debug)", expanded=False):
                st.json(asdict(current))

    # 2) One button to fetch/apply delta
    st.divider()

    btn_label = f"Fetch analyst estimates from GuruFocus for {x}:{t}"
    apply_delta = st.button(
        btn_label,
        type="primary",
        key=f"gf_est_delta_apply_{x}_{t}",
        use_container_width=True,
    )

    ss_key_log = f"gf_est_delta_log_{x}_{t}"
    ss_key_popup = f"gf_est_delta_popup_{x}_{t}"

    if apply_delta:
        before = current  # may be None
        get_analyst_estimate_date_range.clear()  # force a fresh re-read after apply

        with st.status(f"Fetching + applying analyst estimate delta for {x}:{t}…", expanded=True) as status:
            try:
                res, logs = _run_with_print_capture(
                    run_apply_analyst_estimate_delta,
                    primary_ticker=t,
                    primary_exchange=x,
                    source_code="gurufocus_api",
                    require_company_exists=True,
                    lookback_periods=0,
                    verbose=True,
                )
            except Exception as e:
                status.update(label=f"{S_ERROR} Fetch failed for {x}:{t}", state="error")
                st.error(f"{S_ERROR} Delta apply failed for {x}:{t}\n\n{e}")
            else:
                badge = _badge_for_result(res)
                status.update(label=f"{badge} — finished for {x}:{t}", state="complete")

                # Read coverage again (fresh) to show "from -> to"
                after = None
                try:
                    after = get_analyst_estimate_date_range(t, x)
                except Exception:
                    after = None

                # Friendly popup about coverage movement
                if before is not None and after is not None:
                    level, msg = _delta_popup(before, after)
                    st.session_state[ss_key_popup] = (level, msg)
                else:
                    st.session_state[ss_key_popup] = (
                        "warning",
                        "Applied delta, but could not re-read coverage to determine the new date range.",
                    )

                st.session_state[ss_key_log] = logs

                # Also update the small caption in this run
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
                key=f"gf_est_delta_logs_area_{x}_{t}",
            )