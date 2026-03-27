from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import re

import pandas as pd
import streamlit as st

from .repo import PortfolioRepo
from .viewmodels import UI_COLS


@dataclass(frozen=True)
class ValidationResult:
    final: pd.DataFrame          # filtered included+mapped rows
    included_count: int
    missing_w: bool
    weight_sum: float


def validate_editor_rows(ed: pd.DataFrame) -> ValidationResult:
    """
    Takes internal df (after editor_to_internal_df), returns:
      - final: rows where include=True and match_label != "(ignore)"
      - missing_w, included_count, weight_sum
    """
    df = ed.copy()

    df["weight"] = pd.to_numeric(df["weight"], errors="coerce")
    df["include"] = df["include"].fillna(False).astype(bool)
    df["match_label"] = df["match_label"].fillna("(ignore)")

    final = df[(df["include"] == True) & (df["match_label"] != "(ignore)")]  # noqa: E712

    missing_w = final["weight"].isna().any() if not final.empty else False
    included_count = int(final.shape[0])

    if missing_w or final.empty:
        weight_sum = 0.0
    else:
        weight_sum = float(final["weight"].sum())

    return ValidationResult(
        final=final,
        included_count=included_count,
        missing_w=missing_w,
        weight_sum=weight_sum,
    )


def save_portfolio_from_editor(
    *,
    repo: PortfolioRepo,
    portfolio_name: str,
    target: date,
    published: date,
    normalize: bool,
    ed_internal: pd.DataFrame,
    label_to_company_id: dict[str, int],
) -> tuple[int, int, list[str]]:
    """
    Saves portfolio + weights based on the editor dataframe (internal schema).
    Returns (portfolio_id, snapshot_id, unmapped_holdings).
    Raises ValueError for invalid inputs.
    """
    name = (portfolio_name or "").strip()
    if not name:
        raise ValueError("Portfolio name is empty.")

    vr = validate_editor_rows(ed_internal)
    final = vr.final

    if final.empty:
        raise ValueError("No included/mapped rows to save.")
    if vr.missing_w:
        raise ValueError("Some included rows have missing weights.")
    if vr.weight_sum <= 0:
        raise ValueError("Weights sum to 0. Cannot save.")

    final2 = final.copy()
    weights_sum = float(final2["weight"].sum())

    if normalize and abs(weights_sum - 1.0) > 1e-6:
        final2["weight"] = final2["weight"] / weights_sum

    weights: list[tuple[int, float]] = []
    unmapped: list[str] = []

    for r in final2.itertuples(index=False):
        lbl = str(r.match_label)
        cid = label_to_company_id.get(lbl)
        if cid is None:
            unmapped.append(str(r.holding_name))
            continue
        weights.append((int(cid), float(r.weight)))

    if not weights:
        raise ValueError("No valid matched companies to save (all mappings missing).")

    snapshot_id = repo.resolve_snapshot_id(target_date=target, published_at=published)
    pid = repo.upsert_portfolio(portfolio_name=name, snapshot_id=snapshot_id)
    repo.replace_portfolio_weights(portfolio_id=pid, weights=weights)

    return pid, snapshot_id, unmapped


# ============================================================
# Editor / row helpers (moved from Upload_Portfolio.py)
# ============================================================

def default_portfolio_name_from_upload(filename: str) -> str:
    """
    Example:
      18-02-2026_14-36-43__BUS_MTS_DEF_AFS_DYN_VOLK.xls -> BUS_MTS_DEF_AFS_DYN_VOLK
    Fallback:
      <stem>.xls -> <stem>
    """
    stem = Path(filename).stem
    m = re.match(r"^\d{2}-\d{2}-\d{4}_\d{2}-\d{2}-\d{2}__(.+)$", stem)
    if m:
        return m.group(1).strip()
    return stem.strip()


def manual_blank_row() -> dict:
    """Row shape matches the upload editor view (so we can reuse editor_to_internal_df pipeline)."""
    return {
        "include": True,
        UI_COLS["holding_name"]: "",
        UI_COLS["weight"]: 0.0,
        UI_COLS["match_label"]: "",
        UI_COLS["match_score"]: 1.0,
        UI_COLS["mv_eur"]: 0.0,
        UI_COLS["currency"]: "",
    }


def ensure_trailing_blank_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    For 'from scratch': always keep 1 trailing empty row so the user can keep adding.
    Streamlit reruns on edits, so this will feel like "auto-creates a new empty one".
    """
    if df is None or df.empty:
        return pd.DataFrame([manual_blank_row()])

    d = df.copy()

    cols = [
        "include",
        UI_COLS["holding_name"],
        UI_COLS["weight"],
        UI_COLS["match_label"],
        UI_COLS["match_score"],
        UI_COLS["mv_eur"],
        UI_COLS["currency"],
    ]
    for c in cols:
        if c not in d.columns:
            d[c] = "" if c != "include" else True

    d = d[cols]

    last = d.iloc[-1]
    last_label = str(last.get(UI_COLS["match_label"], "") or "").strip()
    last_w = last.get(UI_COLS["weight"], None)

    last_weight_empty = pd.isna(last_w) or (float(last_w) == 0.0)
    last_is_blank = (last_label == "") and last_weight_empty

    if not last_is_blank:
        d = pd.concat([d, pd.DataFrame([manual_blank_row()])], ignore_index=True)

    return d


def clean_empty_rows_for_save(ed_internal: pd.DataFrame) -> pd.DataFrame:
    """
    Drop "empty" rows so they won't be saved:
      - include != True
      - match_label missing / blank / "(ignore)"
      - weight missing
    """
    if ed_internal is None or ed_internal.empty:
        return pd.DataFrame()

    d = ed_internal.copy()

    if "include" in d.columns:
        d["include"] = d["include"].fillna(False).astype(bool)
        d = d[d["include"]]

    if "match_label" in d.columns:
        ml = d["match_label"].fillna("").astype(str).str.strip()
        d = d[(ml != "") & (ml != "(ignore)")]

    if "weight" in d.columns:
        d["weight"] = pd.to_numeric(d["weight"], errors="coerce")
        d = d.dropna(subset=["weight"])

    return d


def run_save_flow(
    *,
    portfolio_name: str,
    target: date,
    published: date,
    normalize: bool,
    ed_internal: pd.DataFrame,
    label_to_company_id: dict[str, int],
    repo: PortfolioRepo,
    clear_caches_fn,
    feedback_key: str = "pm_save",
) -> None:
    """
    Shared save logic for BOTH upload-from-excel and build-from-scratch.
    `clear_caches_fn` is a zero-arg callable (e.g. the Streamlit cache-clearing helper).
    """
    ed_internal = clean_empty_rows_for_save(ed_internal)
    vr = validate_editor_rows(ed_internal)

    save_disabled = (not portfolio_name.strip()) or vr.final.empty or vr.missing_w
    if save_disabled:
        st.error("Cannot save yet. Make sure you have at least one included row with Company + Weight.")
        return

    try:
        pid, snapshot_id, unmapped = save_portfolio_from_editor(
            repo=repo,
            portfolio_name=portfolio_name,
            target=target,
            published=published,
            normalize=normalize,
            ed_internal=ed_internal,
            label_to_company_id=label_to_company_id,
        )
    except Exception as e:
        st.error(str(e))
        st.stop()

    clear_caches_fn()

    if unmapped:
        st.warning(f"Skipped {len(unmapped)} rows because mapping was missing.")

    st.session_state[f"_fb_{feedback_key}"] = ("success", f"✅ Portfolio '{portfolio_name.strip()}' saved successfully!")
    st.rerun()