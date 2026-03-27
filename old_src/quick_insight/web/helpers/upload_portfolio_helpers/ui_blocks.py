"""
ui_blocks.py
Reusable Streamlit UI sections for the Portfolio Manager page.
"""
from __future__ import annotations

from datetime import date
import warnings

import pandas as pd
import streamlit as st

from .matching import suggest_matches
from .repo import PortfolioRepo
from .services import (
    clean_empty_rows_for_save,
    default_portfolio_name_from_upload,
    run_save_flow,
    validate_editor_rows,
)
from .viewmodels import UI_COLS, editor_to_internal_df, internal_to_editor_df
from quick_insight.web.helpers.upload_portfolio_helpers.excel_parser import parse_holdings_excel

def _data_editor(*args, **kwargs):
    """
    Thin wrapper around st.data_editor that suppresses the pandas FutureWarning
    triggered by Streamlit's own row-append code (df.loc[row_label] = ...).
    This is a known Streamlit bug; the warning originates in their internals,
    not in user code.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="The behavior of DataFrame concatenation with empty or all-NA entries",
            category=FutureWarning,
        )
        return st.data_editor(*args, **kwargs)


def _set_feedback(key: str, level: str, msg: str) -> None:
    st.session_state[f"_fb_{key}"] = (level, msg)


def _show_feedback(key: str) -> None:
    entry = st.session_state.pop(f"_fb_{key}", None)
    if entry is None:
        return
    level, msg = entry
    {"success": st.success, "error": st.error, "warning": st.warning, "info": st.info}[level](msg)




# ============================================================
# Weight unit helpers  (UI shows %, internals store 0..1)
# ============================================================

def _pct_to_fraction(series: pd.Series) -> pd.Series:
    """5.0 -> 0.05"""
    return pd.to_numeric(series, errors="coerce") / 100.0


def _fraction_to_pct(series: pd.Series) -> pd.Series:
    """0.05 -> 5.0"""
    return pd.to_numeric(series, errors="coerce") * 100.0


# ============================================================
# Existing portfolios
# ============================================================

# Column names used in the existing-portfolio editor
_EP_COMPANY = "Company"
_EP_WEIGHT_PCT = "Weight (%)"
_EP_EDITOR_KEY = "pm_edit_weights_editor"
_EP_DATA_KEY = "pm_edit_weights_data"       # stores the seeded DF for the current portfolio
_EP_ACTIVE_PID = "pm_edit_weights_pid"      # tracks which portfolio is loaded


def _ep_blank_row_df() -> pd.DataFrame:
    """Typed blank row — explicit dtypes prevent Streamlit FutureWarning on row append."""
    return pd.DataFrame(
        {_EP_COMPANY: pd.array([None], dtype=pd.StringDtype()), _EP_WEIGHT_PCT: pd.array([None], dtype=pd.Float64Dtype())}
    )


def _ep_seed_df(wdf_raw: pd.DataFrame, company_labels: list[str]) -> pd.DataFrame:
    """
    Convert a raw portfolio_weight dataframe into the 2-col editor shape.
    Rows with a company label that is no longer in the DB are kept as-is so
    the user can see and optionally remove them.
    """
    if wdf_raw.empty:
        return _ep_blank_row_df()

    df = wdf_raw.copy()
    df["company_label"] = (
        df["company_name"].astype(str)
        + " — "
        + df["primary_ticker"].astype(str)
        + " ("
        + df["primary_exchange"].astype(str)
        + ")"
    )
    df = df.rename(columns={"weight_value": "weight"})
    df = df.sort_values("weight", ascending=False)
    df["weight_%"] = _fraction_to_pct(df["weight"]).round(4)

    out = pd.DataFrame({
        _EP_COMPANY: pd.array(df["company_label"].tolist(), dtype=pd.StringDtype()),
        _EP_WEIGHT_PCT: pd.array(df["weight_%"].tolist(), dtype=pd.Float64Dtype()),
    })
    # trailing blank row — concat two fully-typed DFs avoids the FutureWarning
    # that loc-assignment triggers when writing NA into typed-array columns
    blank = _ep_blank_row_df()
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        out = pd.concat([out, blank], ignore_index=True)
    return out


def render_existing_portfolios(
    *,
    ports: pd.DataFrame,
    repo: PortfolioRepo,
    clear_caches_fn,
    cached_load_portfolio_weights,
    company_labels: list[str],
) -> None:
    """Renders the 'Existing portfolios' section."""
    st.subheader("Existing portfolios")

    if ports.empty:
        st.info("No portfolios yet.")
        return

    port_labels = [
        f"{row.portfolio_name} · target={pd.to_datetime(row.target_date).date().isoformat()} · "
        f"pub={pd.to_datetime(row.published_at).date().isoformat()} (id={int(row.portfolio_id)})"
        for row in ports.itertuples(index=False)
    ]
    label_to_pid = dict(zip(port_labels, ports["portfolio_id"].astype(int).tolist()))

    colA, colB = st.columns([1.25, 0.75], gap="large")

    with colA:
        pick = st.selectbox("Select portfolio", port_labels, key="pm_selected_portfolio")
        pid = int(label_to_pid[pick])

        # Re-seed editor whenever user switches to a different portfolio
        if st.session_state.get(_EP_ACTIVE_PID) != pid:
            wdf_raw = cached_load_portfolio_weights(pid)
            st.session_state[_EP_DATA_KEY] = _ep_seed_df(wdf_raw, company_labels)
            st.session_state[_EP_ACTIVE_PID] = pid
            # Clear the old widget state so the editor picks up the new seed
            st.session_state.pop(_EP_EDITOR_KEY, None)

        options = [""] + company_labels

        st.markdown("**Holdings** — edit weights, add or remove rows")
        edited = _data_editor(
            st.session_state[_EP_DATA_KEY],   # seed only; widget owns state after first render
            width="stretch",
            num_rows="dynamic",
            column_config={
                _EP_COMPANY: st.column_config.SelectboxColumn(
                    "Company",
                    options=options,
                    required=False,
                    help="Pick a company. New rows: select here to add a holding.",
                ),
                _EP_WEIGHT_PCT: st.column_config.NumberColumn(
                    "Weight (%)",
                    min_value=0.0,
                    max_value=100.0,
                    step=0.1,
                    format="%.2f %%",
                ),
            },
            key=_EP_EDITOR_KEY,
        )

        # ---- live weight sum ----
        valid_rows = edited[
            edited[_EP_COMPANY].notna()
            & (edited[_EP_COMPANY].astype(str).str.strip() != "")
            & edited[_EP_WEIGHT_PCT].notna()
        ].copy()
        valid_rows["_w"] = pd.to_numeric(valid_rows[_EP_WEIGHT_PCT], errors="coerce")
        wsum_pct = float(valid_rows["_w"].sum()) if not valid_rows.empty else 0.0

        mcol1, mcol2, mcol3 = st.columns([1, 1, 1])
        with mcol1:
            st.metric("Rows", len(valid_rows))
        with mcol2:
            st.metric("Weight sum", f"{wsum_pct:.2f} %")
        with mcol3:
            if st.button("Normalize to 100 %", key="pm_ep_normalize"):
                if wsum_pct <= 0:
                    _set_feedback("pm_ep_normalize", "warning", "Nothing to normalize — no valid weights found.")
                else:
                    valid_rows["_w_norm"] = (valid_rows["_w"] / wsum_pct * 100.0).round(4)
                    normed = edited.copy()
                    normed.loc[valid_rows.index, _EP_WEIGHT_PCT] = valid_rows["_w_norm"].values
                    st.session_state[_EP_DATA_KEY] = normed
                    st.session_state.pop(_EP_EDITOR_KEY, None)
                    _set_feedback("pm_ep_normalize", "success", "✅ Weights normalized to 100 %.")
                    st.rerun()
            _show_feedback("pm_ep_normalize")

        # ---- action buttons ----
        b1, b2, b3 = st.columns([1, 1, 1])
        with b1:
            normalize_on_save = st.checkbox("Normalize on save", value=True, key="pm_ep_norm_save")
        with b2:
            if st.button("Save changes", type="primary", key="pm_ep_save"):
                _handle_save_ep_edits(
                    edited=edited,
                    pid=pid,
                    repo=repo,
                    clear_caches_fn=clear_caches_fn,
                    company_labels=company_labels,
                    label_to_company_id=_build_label_to_cid(repo),
                    normalize=normalize_on_save,
                )
            _show_feedback("pm_ep_save")
        with b3:
            if st.button("Delete portfolio", type="secondary", key="pm_ep_delete"):
                try:
                    repo.delete_portfolio(pid)
                    clear_caches_fn()
                    st.session_state.pop(_EP_ACTIVE_PID, None)
                    _set_feedback("pm_ep_delete", "success", "🗑️ Portfolio deleted.")
                    st.rerun()
                except Exception as e:
                    _set_feedback("pm_ep_delete", "error", f"Could not delete portfolio: {e}")
            _show_feedback("pm_ep_delete")

    with colB:
        st.markdown("**All portfolios** — select to delete")

        _BULK_KEY = "pm_bulk_select"

        # Build selectable view
        sel_df = ports[["portfolio_id", "portfolio_name", "target_date", "published_at"]].copy()
        sel_df.insert(0, "delete", False)
        sel_df["target_date"] = pd.to_datetime(sel_df["target_date"], errors="coerce").dt.date.astype(str)
        sel_df["published_at"] = pd.to_datetime(sel_df["published_at"], errors="coerce").dt.date.astype(str)

        # Select-all toggle — use a separate state key from the button key
        if st.button("Select all", key="pm_bulk_select_all_btn"):
            new_state = not st.session_state.get("pm_bulk_all_checked", False)
            st.session_state["pm_bulk_all_checked"] = new_state
            _set_feedback("pm_bulk_select_all_btn", "info", "All selected." if new_state else "Selection cleared.")
        _show_feedback("pm_bulk_select_all_btn")

        if st.session_state.get("pm_bulk_all_checked", False):
            sel_df["delete"] = True

        edited_sel = _data_editor(
            sel_df,
            width="stretch",
            hide_index=True,
            column_config={
                "delete": st.column_config.CheckboxColumn("✓", width="small"),
                "portfolio_id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "portfolio_name": st.column_config.TextColumn("Name", disabled=True),
                "target_date": st.column_config.TextColumn("Target", disabled=True, width="small"),
                "published_at": st.column_config.TextColumn("Published", disabled=True, width="small"),
            },
            key=_BULK_KEY,
        )

        to_delete = edited_sel[edited_sel["delete"] == True]["portfolio_id"].astype(int).tolist()  # noqa: E712

        if to_delete:
            st.warning(f"{len(to_delete)} portfolio(s) selected for deletion.")
            if st.button(
                f"Delete {len(to_delete)} selected",
                type="primary",
                key="pm_bulk_delete_confirm",
            ):
                errors = []
                deleted = 0
                for del_pid in to_delete:
                    try:
                        repo.delete_portfolio(del_pid)
                        deleted += 1
                    except Exception as e:
                        errors.append(f"ID {del_pid}: {e}")
                clear_caches_fn()
                st.session_state.pop(_EP_ACTIVE_PID, None)
                st.session_state.pop("pm_bulk_all_checked", None)
                if errors and deleted == 0:
                    _set_feedback("pm_bulk_delete_confirm", "error", "All deletions failed:\n" + "\n".join(errors))
                elif errors:
                    _set_feedback("pm_bulk_delete_confirm", "warning", f"Deleted {deleted}, {len(errors)} failed:\n" + "\n".join(errors))
                else:
                    _set_feedback("pm_bulk_delete_confirm", "success", f"🗑️ Deleted {deleted} portfolio(s).")
                st.rerun()
        _show_feedback("pm_bulk_delete_confirm")


def _build_label_to_cid(repo: PortfolioRepo) -> dict[str, int]:
    """Re-use the cached company lookup to build label->company_id map."""
    lu = repo.load_company_lookup()
    return dict(zip(lu["label"], lu["company_id"].astype(int)))


def _handle_save_ep_edits(
    *,
    edited: pd.DataFrame,
    pid: int,
    repo: PortfolioRepo,
    clear_caches_fn,
    company_labels: list[str],
    label_to_company_id: dict[str, int],
    normalize: bool,
) -> None:
    rows = edited.copy()
    rows = rows[
        rows[_EP_COMPANY].notna()
        & (rows[_EP_COMPANY].astype(str).str.strip() != "")
        & rows[_EP_WEIGHT_PCT].notna()
    ]
    rows["weight_frac"] = _pct_to_fraction(rows[_EP_WEIGHT_PCT])
    rows = rows.dropna(subset=["weight_frac"])
    rows = rows[rows["weight_frac"] > 0]

    if rows.empty:
        st.error("No valid rows to save.")
        return

    wsum = float(rows["weight_frac"].sum())
    if wsum <= 0:
        st.error("Weights sum to 0. Nothing to save.")
        return

    if normalize and abs(wsum - 1.0) > 1e-6:
        rows["weight_frac"] = rows["weight_frac"] / wsum

    weights: list[tuple[int, float]] = []
    unmapped: list[str] = []
    for r in rows.itertuples(index=False):
        lbl = str(getattr(r, _EP_COMPANY))
        cid = label_to_company_id.get(lbl)
        if cid is None:
            unmapped.append(lbl)
            continue
        weights.append((int(cid), float(r.weight_frac)))

    if not weights:
        st.error("None of the companies could be mapped to a company ID.")
        return

    repo.replace_portfolio_weights(portfolio_id=pid, weights=weights)
    clear_caches_fn()

    if unmapped:
        st.warning(f"Skipped {len(unmapped)} rows — company not found in DB: {', '.join(unmapped[:5])}")

    _set_feedback("pm_ep_save", "success", f"✅ Saved {len(weights)} holdings.")
    st.session_state.pop(_EP_ACTIVE_PID, None)
    st.rerun()


# ============================================================
# Manual / from-scratch builder
# ============================================================

_M_COMPANY = "Company"
_M_WEIGHT_PCT = "Weight (%)"
_MANUAL_INIT_KEY = "pm_manual_initialized"
_MANUAL_WIDGET_KEY = "pm_manual_widget"


def _blank_manual_df() -> pd.DataFrame:
    return pd.DataFrame(
        {_M_COMPANY: pd.array([None], dtype=pd.StringDtype()), _M_WEIGHT_PCT: pd.array([None], dtype=pd.Float64Dtype())}
    )


def render_manual_builder(
    *,
    company_labels: list[str],
    label_to_company_id: dict[str, int],
    repo: PortfolioRepo,
    clear_caches_fn,
) -> None:
    """Renders the 'Create portfolio from scratch' expander (company + weight % only)."""
    st.subheader("Create portfolio from scratch")

    with st.expander("Build holdings manually", expanded=False):
        st.caption(
            "Pick a company and enter its weight as a percentage (e.g. **5** for 5 %). "
            "Add as many rows as you like — empty rows are ignored on save."
        )

        options = [""] + company_labels

        if _MANUAL_INIT_KEY not in st.session_state:
            st.session_state[_MANUAL_INIT_KEY] = _blank_manual_df()

        edited = _data_editor(
            st.session_state[_MANUAL_INIT_KEY],
            width="stretch",
            num_rows="dynamic",
            column_config={
                _M_COMPANY: st.column_config.SelectboxColumn(
                    "Company",
                    options=options,
                    required=False,
                    help="Select the company to add.",
                ),
                _M_WEIGHT_PCT: st.column_config.NumberColumn(
                    "Weight (%)",
                    min_value=0.0,
                    max_value=100.0,
                    step=0.1,
                    format="%.2f %%",
                    help="Enter weight as a percentage, e.g. 5 for 5 %.",
                ),
            },
            key=_MANUAL_WIDGET_KEY,
        )

        rows = edited.copy()
        rows = rows[
            rows[_M_COMPANY].notna()
            & (rows[_M_COMPANY].astype(str).str.strip() != "")
            & rows[_M_WEIGHT_PCT].notna()
            & (pd.to_numeric(rows[_M_WEIGHT_PCT], errors="coerce") > 0)
        ]

        if rows.empty:
            internal = pd.DataFrame()
        else:
            internal = pd.DataFrame({
                "include": True,
                "holding_name": rows[_M_COMPANY].astype(str).values,
                "weight": _pct_to_fraction(rows[_M_WEIGHT_PCT]).values,
                "match_label": rows[_M_COMPANY].astype(str).values,
                "match_score": 1.0,
                "mv_eur": 0.0,
                "currency": "",
            })

        row_count = len(internal)
        weight_sum_pct = float(_fraction_to_pct(internal["weight"]).sum()) if row_count > 0 else 0.0

        mcol1, mcol2 = st.columns(2)
        with mcol1:
            st.metric("Rows", row_count)
        with mcol2:
            st.metric("Weight sum", f"{weight_sum_pct:.2f} %")

        if row_count > 0 and abs(weight_sum_pct - 100.0) > 0.1:
            st.warning(f"Weights sum to {weight_sum_pct:.2f} %. Enable 'Normalize' below to fix automatically.")

        st.markdown("### Save portfolio (manual)")
        c1, c2, c3, c4 = st.columns([1.2, 0.8, 0.8, 1.0], gap="large")

        with c1:
            manual_name = st.text_input("Portfolio name", key="pm_manual_port_name")
        with c2:
            manual_target = st.date_input("Target date", value=date.today(), key="pm_manual_target_date")
        with c3:
            manual_published = st.date_input("Published at", value=date.today(), key="pm_manual_published_at")
        with c4:
            manual_normalize = st.checkbox("Normalize to 100 %", value=True, key="pm_manual_norm")

        if st.button(
            "Save manual portfolio",
            type="primary",
            key="pm_save_manual",
            disabled=(not manual_name.strip()) or internal.empty,
        ):
            st.session_state.pop(_MANUAL_INIT_KEY, None)
            st.session_state.pop(_MANUAL_WIDGET_KEY, None)
            run_save_flow(
                portfolio_name=manual_name,
                target=manual_target,
                published=manual_published,
                normalize=manual_normalize,
                ed_internal=internal,
                label_to_company_id=label_to_company_id,
                repo=repo,
                clear_caches_fn=clear_caches_fn,
                feedback_key="pm_save_manual",
            )
        _show_feedback("pm_save_manual")


# ============================================================
# Excel upload + map + save
# ============================================================

_UPLOAD_WEIGHT_PCT_COL = "Weight (%)"


def render_upload_section(
    *,
    company_labels: list[str],
    label_to_company_id: dict[str, int],
    company_lu: pd.DataFrame,
    repo: PortfolioRepo,
    clear_caches_fn,
) -> None:
    """Renders the 'Upload Excel -> create portfolio' expander block."""
    st.subheader("Upload Excel -> create portfolio")

    with st.expander("Upload & map holdings", expanded=True):
        st.caption(
            "Expected export columns: **Fondsomschrijving**, **Huidige waarde  EUR**, "
            "**Weging** (optional), **Valuta** (optional). "
            "Effectenrekening is ignored by default."
        )

        up = st.file_uploader(
            "Drag & drop Excel (.xls / .xlsx)",
            type=["xls", "xlsx"],
            key="pm_upload_excel",
        )

        if up is None:
            st.info("Upload an Excel to begin.")
            return

        if "pm_last_uploaded_name" not in st.session_state:
            st.session_state.pm_last_uploaded_name = ""

        if up.name != st.session_state.pm_last_uploaded_name:
            st.session_state.pm_last_uploaded_name = up.name
            st.session_state.pm_port_name = default_portfolio_name_from_upload(up.name)

        try:
            parsed = parse_holdings_excel(up.getvalue())
        except Exception as e:
            st.error(f"Could not parse Excel: {e}")
            return

        hold = parsed.df.copy()

        st.info(
            "Effectenrekening wordt standaard genegeerd. "
            "Deze rij telt ook niet mee in de automatische weging-berekening."
        )

        if not hold.empty:
            src = str(hold["weight_source"].iloc[0])
            if src == "computed":
                st.success("Weging is automatisch berekend op basis van Huidige waarde EUR / totaal.")
            else:
                st.success("Weging is ingelezen uit de Excel-kolom Weging (auto pct->0..1).")

        hold = suggest_matches(hold, company_lu, min_confidence_include=0.55)
        options = ["(ignore)"] + company_labels

        view = internal_to_editor_df(hold)
        view[_UPLOAD_WEIGHT_PCT_COL] = _fraction_to_pct(view[UI_COLS["weight"]]).round(4)
        view = view.drop(columns=[UI_COLS["weight"]])

        st.caption("Tip: zet include = False voor fondsen/ETFs/unknown rows die je wil overslaan.")

        edited_ui = _data_editor(
            view,
            width="stretch",
            num_rows="dynamic",
            column_config={
                "include": st.column_config.CheckboxColumn("Include"),
                UI_COLS["holding_name"]: st.column_config.TextColumn(UI_COLS["holding_name"], disabled=True),
                _UPLOAD_WEIGHT_PCT_COL: st.column_config.NumberColumn(
                    "Weight (%)",
                    min_value=0.0,
                    max_value=100.0,
                    step=0.01,
                    format="%.2f %%",
                    help="Weight as a percentage, e.g. 5 for 5 %.",
                ),
                UI_COLS["match_label"]: st.column_config.SelectboxColumn(
                    UI_COLS["match_label"], options=options
                ),
                UI_COLS["match_score"]: st.column_config.NumberColumn(
                    UI_COLS["match_score"], format="%.2f", disabled=True
                ),
                UI_COLS["mv_eur"]: st.column_config.NumberColumn(UI_COLS["mv_eur"], format="%.2f", disabled=True),
                UI_COLS["currency"]: st.column_config.TextColumn(UI_COLS["currency"], disabled=True),
            },
            key="pm_map_editor",
        )

        edited_ui[UI_COLS["weight"]] = _pct_to_fraction(edited_ui[_UPLOAD_WEIGHT_PCT_COL])
        edited_ui = edited_ui.drop(columns=[_UPLOAD_WEIGHT_PCT_COL])

        ed_internal = editor_to_internal_df(edited_ui)
        ed_internal = clean_empty_rows_for_save(ed_internal)

        vr = validate_editor_rows(ed_internal)
        weight_sum_pct = vr.weight_sum * 100.0

        mcol1, mcol2 = st.columns(2)
        with mcol1:
            st.metric("Included rows", vr.included_count)
        with mcol2:
            if not vr.missing_w:
                st.metric("Weight sum (included)", f"{weight_sum_pct:.2f} %")

        if vr.missing_w:
            st.warning("Some included rows have missing weights. Fill them in before saving.")
        elif abs(weight_sum_pct - 100.0) > 0.1 and vr.included_count > 0:
            st.warning(
                f"Let op: included weights sommen naar {weight_sum_pct:.2f} %. "
                "Je kunt normaliseren bij opslaan."
            )

        st.markdown("### Save portfolio")
        c1, c2, c3, c4 = st.columns([1.2, 0.8, 0.8, 1.0], gap="large")

        with c1:
            portfolio_name = st.text_input("Portfolio name", key="pm_port_name")
        with c2:
            target = st.date_input("Target date", value=date.today(), key="pm_target_date")
        with c3:
            published = st.date_input("Published at", value=date.today(), key="pm_published_at")
        with c4:
            normalize = st.checkbox("Normalize to 100 %", value=True, key="pm_norm")

        if st.button(
            "Save as portfolio",
            type="primary",
            key="pm_save_excel",
            disabled=(not portfolio_name.strip()) or vr.final.empty or vr.missing_w,
        ):
            run_save_flow(
                portfolio_name=portfolio_name,
                target=target,
                published=published,
                normalize=normalize,
                ed_internal=ed_internal,
                label_to_company_id=label_to_company_id,
                repo=repo,
                clear_caches_fn=clear_caches_fn,
                feedback_key="pm_save_excel",
            )
        _show_feedback("pm_save_excel")