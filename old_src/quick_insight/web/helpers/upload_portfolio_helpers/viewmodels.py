# viewmodels.py
from __future__ import annotations

import pandas as pd

# ---- UI column naming (raw vs derived) ----
UI_COLS = {
    "holding_name": "Fondsomschrijving (Excel)",
    "mv_eur": "Huidige waarde EUR (Excel)",
    "currency": "Valuta (Excel)",
    "weight": "Weight (derived, 0..1)",
    "match_label": "Auto-matched to company in database",
    "match_score": "Auto-match confidence",
}


def internal_to_editor_df(hold: pd.DataFrame) -> pd.DataFrame:
    """
    Internal holdings df -> editor df with renamed columns + proper types.
    Expects internal columns:
      include, holding_name, weight, match_label, match_score, mv_eur, currency
    """
    view = hold[
        ["include", "holding_name", "weight", "match_label", "match_score", "mv_eur", "currency"]
    ].copy()

    view = view.rename(
        columns={
            "holding_name": UI_COLS["holding_name"],
            "mv_eur": UI_COLS["mv_eur"],
            "currency": UI_COLS["currency"],
            "weight": UI_COLS["weight"],
            "match_label": UI_COLS["match_label"],
            "match_score": UI_COLS["match_score"],
        }
    )

    view[UI_COLS["match_label"]] = view[UI_COLS["match_label"]].fillna("").replace({"": "(ignore)"})
    view[UI_COLS["match_score"]] = pd.to_numeric(view[UI_COLS["match_score"]], errors="coerce").fillna(0.0)
    view[UI_COLS["weight"]] = pd.to_numeric(view[UI_COLS["weight"]], errors="coerce")
    view[UI_COLS["mv_eur"]] = pd.to_numeric(view[UI_COLS["mv_eur"]], errors="coerce")

    view["include"] = view["include"].fillna(False).astype(bool)
    return view


def editor_to_internal_df(edited_ui: pd.DataFrame) -> pd.DataFrame:
    """
    Editor df -> internal df (renames back).
    """
    ed = edited_ui.copy().rename(
        columns={
            UI_COLS["holding_name"]: "holding_name",
            UI_COLS["mv_eur"]: "mv_eur",
            UI_COLS["currency"]: "currency",
            UI_COLS["weight"]: "weight",
            UI_COLS["match_label"]: "match_label",
            UI_COLS["match_score"]: "match_score",
        }
    )

    ed["weight"] = pd.to_numeric(ed["weight"], errors="coerce")
    ed["match_score"] = pd.to_numeric(ed["match_score"], errors="coerce")
    ed["include"] = ed["include"].fillna(False).astype(bool)
    ed["match_label"] = ed["match_label"].fillna("(ignore)")
    return ed
