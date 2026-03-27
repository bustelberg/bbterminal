# upload_portfolio_helpers/matching.py
from __future__ import annotations

import pandas as pd


def _normalize_name(s: str) -> str:
    return (
        (s or "")
        .lower()
        .replace("&", "and")
        .replace("-", " ")
        .replace(".", " ")
        .replace(",", " ")
        .replace("  ", " ")
        .strip()
    )


def _best_match(name: str, candidates: pd.Series) -> tuple[str | None, float]:
    """
    Return (best_candidate_value, score 0..1). Uses rapidfuzz if available, else difflib.
    candidates: Series of strings (company_name).
    """
    q = _normalize_name(name)
    if not q:
        return None, 0.0

    try:
        from rapidfuzz import fuzz, process  # type: ignore

        res = process.extractOne(q, candidates.tolist(), scorer=fuzz.token_sort_ratio)
        if not res:
            return None, 0.0
        best, score, _idx = res
        return str(best), float(score) / 100.0
    except Exception:
        import difflib

        cand_norm = [_normalize_name(x) for x in candidates.tolist()]
        matches = difflib.get_close_matches(q, cand_norm, n=1, cutoff=0.0)
        if not matches:
            return None, 0.0
        best_norm = matches[0]
        score = difflib.SequenceMatcher(None, q, best_norm).ratio()
        idx = cand_norm.index(best_norm)
        return str(candidates.iloc[idx]), float(score)


def suggest_matches(
    holdings: pd.DataFrame,
    company_lu: pd.DataFrame,
    *,
    min_confidence_include: float = 0.55,
) -> pd.DataFrame:
    """
    Adds:
      - match_label (company_lu.label)
      - match_score (0..1)
    Also sets include=False by default for low-confidence rows.
    """
    hold = holdings.copy()

    candidates = company_lu["company_name"].astype(str)

    suggested_label: list[str] = []
    suggested_score: list[float] = []

    for nm in hold["holding_name"].astype(str).tolist():
        best_name, score = _best_match(nm, candidates)
        if best_name is None:
            suggested_label.append("")
            suggested_score.append(0.0)
            continue

        row = company_lu.loc[company_lu["company_name"].astype(str) == str(best_name)].head(1)
        if row.empty:
            suggested_label.append("")
            suggested_score.append(float(score))
        else:
            suggested_label.append(str(row["label"].iloc[0]))
            suggested_score.append(float(score))

    hold["match_label"] = suggested_label
    hold["match_score"] = suggested_score

    # Default exclude low-confidence matches
    hold.loc[hold["match_score"] < float(min_confidence_include), "include"] = False

    return hold
