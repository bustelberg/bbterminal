"""
ui_blocks.py  —  Company Manager
Reusable Streamlit UI sections. No business logic here; all DB access goes
through CompanyRepo.
"""
from __future__ import annotations

import warnings

import pandas as pd
import streamlit as st

from .repo import CompanyRepo


# ── helpers ──────────────────────────────────────────────────────────────────

_OTHER = "Other (type below)…"


def _exchange_options(repo: CompanyRepo) -> list[str]:
    """Distinct exchanges from DB + escape hatch for custom input."""
    db_vals = repo.list_distinct_exchanges()
    return [""] + db_vals + ([_OTHER] if _OTHER not in db_vals else [])


def _sector_options(repo: CompanyRepo) -> list[str]:
    """Distinct sectors from DB + escape hatch for custom input."""
    db_vals = repo.list_distinct_sectors()
    return [""] + db_vals + ([_OTHER] if _OTHER not in db_vals else [])


def _country_options(repo: CompanyRepo) -> list[str]:
    """Distinct countries from DB + escape hatch for custom input."""
    db_vals = repo.list_distinct_countries()
    return [""] + db_vals + ([_OTHER] if _OTHER not in db_vals else [])


def _resolve_freetext(selectbox_val: str, freetext_val: str) -> str:
    """Return freetext if user chose the escape-hatch option, else the selectbox value."""
    if selectbox_val == _OTHER:
        return freetext_val.strip()
    return selectbox_val


def _data_editor(*args, **kwargs):
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="The behavior of DataFrame concatenation with empty or all-NA entries",
            category=FutureWarning,
        )
        return st.data_editor(*args, **kwargs)


def _set_feedback(key: str, level: str, msg: str) -> None:
    """Store a feedback message keyed by button key for in-place display after rerun."""
    st.session_state[f"_fb_{key}"] = (level, msg)


def _show_feedback(key: str) -> None:
    """Render and clear a stored feedback message immediately below a button."""
    entry = st.session_state.pop(f"_fb_{key}", None)
    if entry is None:
        return
    level, msg = entry
    {"success": st.success, "error": st.error, "warning": st.warning, "info": st.info}[level](msg)


# ── Company list + search ─────────────────────────────────────────────────────

def render_company_list(*, repo: CompanyRepo, clear_caches_fn, cached_list_companies) -> None:
    st.subheader("Companies existing in the database")

    exchange_opts = _exchange_options(repo)
    sector_opts = _sector_options(repo)
    country_opts = _country_options(repo)

    companies = cached_list_companies()

    if companies.empty:
        st.info("No companies in the database yet.")
        return

    # Search bar
    search = st.text_input(
        "Search",
        placeholder="Filter by name, ticker, exchange, sector…",
        key="cm_search",
    )

    df = companies.copy()
    if search.strip():
        q = search.strip().lower()
        mask = (
            df["company_name"].str.lower().str.contains(q, na=False)
            | df["primary_ticker"].str.lower().str.contains(q, na=False)
            | df["primary_exchange"].str.lower().str.contains(q, na=False)
            | df["sector"].str.lower().str.contains(q, na=False)
            | df["country"].str.lower().str.contains(q, na=False)
        )
        df = df[mask]

    st.caption(f"{len(df)} of {len(companies)} companies shown.")

    if df.empty:
        st.info("No companies match your search.")
        return

    # Make company_id the index so it's read-only implicitly
    display = df.set_index("company_id")[
        ["company_name", "primary_ticker", "primary_exchange",
         "longequity_ticker", "country", "sector"]
    ]

    edited = _data_editor(
        display,
        width="stretch",
        num_rows="fixed",
        column_config={
            "company_name":      st.column_config.TextColumn("Name"),
            "primary_ticker":    st.column_config.TextColumn("Ticker"),
            "primary_exchange":  st.column_config.SelectboxColumn("Exchange", options=exchange_opts),
            "longequity_ticker": st.column_config.TextColumn("Longequity ticker"),
            "country":           st.column_config.SelectboxColumn("Country", options=country_opts),
            "sector":            st.column_config.SelectboxColumn("Sector", options=sector_opts),
        },
        key="cm_company_editor",
    )

    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("Save edits", type="primary", key="cm_save_edits"):
            _handle_bulk_save(
                original=display,
                edited=edited,
                repo=repo,
                clear_caches_fn=clear_caches_fn,
            )
        _show_feedback("cm_save_edits")


def _handle_bulk_save(
    *,
    original: pd.DataFrame,
    edited: pd.DataFrame,
    repo: CompanyRepo,
    clear_caches_fn,
) -> None:
    changed = edited.compare(original, keep_shape=False)
    if changed.empty:
        _set_feedback("cm_save_edits", "info", "No changes detected.")
        return

    changed_ids = changed.index.tolist()
    errors: list[str] = []
    saved = 0

    for cid in changed_ids:
        row = edited.loc[cid]
        try:
            repo.update_company(
                company_id=int(cid),
                company_name=str(row.get("company_name", "") or ""),
                primary_ticker=str(row.get("primary_ticker", "") or ""),
                primary_exchange=str(row.get("primary_exchange", "") or ""),
                longequity_ticker=str(row.get("longequity_ticker", "") or ""),
                country=str(row.get("country", "") or ""),
                sector=str(row.get("sector", "") or ""),
            )
            saved += 1
        except Exception as e:
            errors.append(f"ID {cid}: {e}")

    clear_caches_fn()

    if errors and saved == 0:
        _set_feedback("cm_save_edits", "error", "All updates failed:\n" + "\n".join(errors))
    elif errors:
        _set_feedback("cm_save_edits", "warning", f"Saved {saved}, {len(errors)} failed:\n" + "\n".join(errors))
    else:
        _set_feedback("cm_save_edits", "success", f"✅ Saved {saved} company/companies.")
    st.rerun()


# ── Add company form ──────────────────────────────────────────────────────────

def render_add_company(*, repo: CompanyRepo, clear_caches_fn) -> None:
    st.subheader("Add company")

    with st.expander("New company form", expanded=False):
        exchange_opts = _exchange_options(repo)
        sector_opts = _sector_options(repo)
        country_opts = _country_options(repo)

        c1, c2, c3 = st.columns(3)
        with c1:
            name = st.text_input("Company name", key="cm_add_name")
        with c2:
            ticker = st.text_input("Primary ticker *", key="cm_add_ticker")
        with c3:
            exchange_sel = st.selectbox("Primary exchange *", exchange_opts, key="cm_add_exchange")
            if exchange_sel == _OTHER:
                exchange_custom = st.text_input("Type exchange", key="cm_add_exchange_custom")
            else:
                exchange_custom = ""
            exchange = _resolve_freetext(exchange_sel, exchange_custom)

        c4, c5 = st.columns(2)
        with c4:
            country_sel = st.selectbox("Country", country_opts, key="cm_add_country")
            if country_sel == _OTHER:
                country_custom = st.text_input("Type country", key="cm_add_country_custom")
            else:
                country_custom = ""
            country = _resolve_freetext(country_sel, country_custom)
        with c5:
            sector_sel = st.selectbox("Sector", sector_opts, key="cm_add_sector")
            if sector_sel == _OTHER:
                sector_custom = st.text_input("Type sector", key="cm_add_sector_custom")
            else:
                sector_custom = ""
            sector = _resolve_freetext(sector_sel, sector_custom)

        ticker_ok = bool(ticker.strip())
        exchange_ok = bool(exchange.strip())

        if not ticker_ok or not exchange_ok:
            st.caption("* Primary ticker and exchange are required.")

        # GuruFocus verification link — updates live as ticker/exchange are typed
        if ticker_ok and exchange_ok:
            gf_url = f"https://www.gurufocus.com/stock/{exchange.strip().upper()}:{ticker.strip().upper()}/summary"
            st.markdown(
                f"🔍 Verify this is the right company: [**{exchange.strip().upper()}:{ticker.strip().upper()} on GuruFocus**]({gf_url})",
                help="Opens GuruFocus in a new tab. Check that the company shown matches what you intend to add.",
            )

        if st.button(
            "Add company",
            type="primary",
            key="cm_add_submit",
            disabled=not (ticker_ok and exchange_ok),
        ):
            # Duplicate check
            if repo.company_exists(
                primary_ticker=ticker.strip(),
                primary_exchange=exchange.strip(),
            ):
                _set_feedback("cm_add_submit", "error",
                    f"A company with ticker **{ticker.strip().upper()}** on "
                    f"**{exchange.strip().upper()}** already exists.")
                return

            try:
                new_id = repo.add_company(
                    company_name=name,
                    primary_ticker=ticker,
                    primary_exchange=exchange,
                    country=country,
                    sector=sector,
                )
                clear_caches_fn()
                _set_feedback("cm_add_submit", "success", f"✅ '{name.strip() or ticker.strip().upper()}' added (ID {new_id}).")
                st.rerun()
            except Exception as e:
                _set_feedback("cm_add_submit", "error", f"Could not add company: {e}")
        _show_feedback("cm_add_submit")


# ── Delete companies ──────────────────────────────────────────────────────────

_DELETE_INIT_KEY = "cm_delete_df"
_DELETE_WIDGET_KEY = "cm_delete_editor"


def render_delete_companies(*, repo: CompanyRepo, clear_caches_fn, cached_list_companies) -> None:
    st.subheader("Delete companies")

    with st.expander("Select companies to delete", expanded=False):
        companies = cached_list_companies()

        if companies.empty:
            st.info("No companies to delete.")
            return

        search_del = st.text_input(
            "Filter",
            placeholder="Filter by name, ticker…",
            key="cm_del_search",
        )

        df = companies.copy()
        if search_del.strip():
            q = search_del.strip().lower()
            mask = (
                df["company_name"].str.lower().str.contains(q, na=False)
                | df["primary_ticker"].str.lower().str.contains(q, na=False)
                | df["primary_exchange"].str.lower().str.contains(q, na=False)
            )
            df = df[mask]

        sel_df = df[["company_id", "company_name", "primary_ticker", "primary_exchange", "sector"]].copy()
        sel_df.insert(0, "delete", pd.array([False] * len(sel_df), dtype=pd.BooleanDtype()))

        # Select-all toggle
        col_sa, _ = st.columns([1, 5])
        with col_sa:
            if st.button("Select all", key="cm_del_select_all_btn"):
                new_val = not st.session_state.get("cm_del_all_checked", False)
                st.session_state["cm_del_all_checked"] = new_val
                _set_feedback("cm_del_select_all_btn", "info", "All selected." if new_val else "Selection cleared.")
        _show_feedback("cm_del_select_all_btn")

        if st.session_state.get("cm_del_all_checked", False):
            sel_df["delete"] = True

        edited_sel = _data_editor(
            sel_df,
            width="stretch",
            hide_index=True,
            column_config={
                "delete":           st.column_config.CheckboxColumn("✓", width="small"),
                "company_id":       st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "company_name":     st.column_config.TextColumn("Name", disabled=True),
                "primary_ticker":   st.column_config.TextColumn("Ticker", disabled=True, width="small"),
                "primary_exchange": st.column_config.TextColumn("Exchange", disabled=True, width="small"),
                "sector":           st.column_config.TextColumn("Sector", disabled=True),
            },
            key=_DELETE_WIDGET_KEY,
        )

        to_delete = (
            edited_sel[edited_sel["delete"] == True]["company_id"]  # noqa: E712
            .astype(int)
            .tolist()
        )

        if not to_delete:
            return

        # Show usage summary so user knows what they're deleting
        st.warning(f"**{len(to_delete)} company/companies selected.** Check references below before deleting.")

        with st.expander("Show usage in portfolios / facts", expanded=False):
            rows = []
            for cid in to_delete:
                row = companies[companies["company_id"] == cid].iloc[0]
                counts = repo.usage_counts(cid)
                rows.append({
                    "ID": cid,
                    "Name": row["company_name"],
                    "Ticker": row["primary_ticker"],
                    "portfolio_weight rows": counts.get("portfolio_weight", 0),
                    "facts_number rows": counts.get("facts_number", 0),
                    "facts_text rows": counts.get("facts_text", 0),
                })
            st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

        if st.button(
            f"Delete {len(to_delete)} selected",
            type="primary",
            key="cm_del_confirm",
        ):
            errors: list[str] = []
            deleted = 0
            for cid in to_delete:
                try:
                    repo.delete_company(cid)
                    deleted += 1
                except Exception as e:
                    errors.append(f"ID {cid}: {e}")

            clear_caches_fn()
            st.session_state.pop("cm_del_all_checked", None)

            if errors and deleted == 0:
                _set_feedback("cm_del_confirm", "error", "All deletions failed:\n" + "\n".join(errors))
            elif errors:
                _set_feedback("cm_del_confirm", "warning", f"Deleted {deleted}, {len(errors)} failed:\n" + "\n".join(errors))
            else:
                _set_feedback("cm_del_confirm", "success", f"🗑️ Deleted {deleted} company/companies.")
            st.rerun()
        _show_feedback("cm_del_confirm")