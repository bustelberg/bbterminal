"""Scoring engine: normalize signals, weight, select top sectors & companies.

Supports multiple signal categories (e.g. price, volume) each scored 0-100
independently, then combined via category weights into a final score.
"""
from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

from .signals import PRICE_SIGNAL_DEFS

# "top" = best sectors / best names per sector (long bucket).
# "bottom" = worst sectors / worst names per sector (short bucket for
# long-short strategies). Default is "top" so existing call sites — which
# expect long-only behavior — are unchanged.
SelectionDirection = Literal["top", "bottom"]


def _score_category(
    df: pd.DataFrame,
    signal_weights: dict[str, float],
    signal_keys: list[str],
    score_col: str,
) -> pd.DataFrame:
    """Min-max normalize signals within a category and compute a 0-100 score.

    Only signals present in both signal_keys and df.columns are used.
    """
    df = df.copy()
    active = {k: signal_weights.get(k, 0) for k in signal_keys if k in df.columns and signal_weights.get(k, 0) != 0}

    if not active:
        df[score_col] = np.nan
        return df

    weight_sum = sum(abs(w) for w in active.values())
    normed = {k: v / weight_sum for k, v in active.items()}

    score = np.zeros(len(df))
    for col, weight in normed.items():
        series = pd.to_numeric(df[col], errors="coerce").astype(float)
        min_val = series.min()
        max_val = series.max()
        if pd.isna(min_val) or pd.isna(max_val) or min_val == max_val:
            norm = pd.Series(0.5, index=df.index)
        else:
            norm = (series - min_val) / (max_val - min_val)
        norm = norm.fillna(0.5)
        score += norm.values * weight

    df[score_col] = (score * 100).round(2)
    return df


def _get_category_keys() -> dict[str, list[str]]:
    """Build {category: [signal_keys]} from PRICE_SIGNAL_DEFS."""
    cats: dict[str, list[str]] = {}
    for s in PRICE_SIGNAL_DEFS:
        group = s.get("group", "price")
        cats.setdefault(group, []).append(s["key"])
    return cats


def compute_category_scores(
    df: pd.DataFrame,
    signal_weights: dict[str, float],
    category_weights: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Score each company per category (0-100), then compute a weighted final score.

    Adds columns: score_price, score_volume, ..., momentum_score (final).
    """
    cats = _get_category_keys()

    if category_weights is None:
        # Default: equal weight per category
        n = len(cats)
        category_weights = {c: 1.0 / n for c in cats}

    # Normalize category weights
    cw_sum = sum(abs(v) for v in category_weights.values())
    if cw_sum == 0:
        cw_sum = 1.0
    cw_normed = {c: v / cw_sum for c, v in category_weights.items()}

    # Score each category independently
    for cat, keys in cats.items():
        col = f"score_{cat}"
        df = _score_category(df, signal_weights, keys, col)

    # Compute weighted final score
    final = np.zeros(len(df))
    has_any = np.zeros(len(df), dtype=bool)
    for cat in cats:
        col = f"score_{cat}"
        if col in df.columns:
            valid = df[col].notna()
            has_any |= valid
            values = df[col].fillna(0).values
            final += values * cw_normed.get(cat, 0)

    df["momentum_score"] = np.where(has_any, np.round(final, 2), 50.0)
    return df


def aggregate_to_sector(
    df: pd.DataFrame,
    score_col: str = "momentum_score",
    group_col: str = "sector",
) -> pd.DataFrame:
    """Average company scores to sector level."""
    return (
        df.groupby(group_col)[score_col]
        .mean()
        .reset_index()
        .sort_values(score_col, ascending=False)
        .reset_index(drop=True)
    )


def score_and_select(
    signals_df: pd.DataFrame,
    signal_weights: dict[str, float],
    *,
    top_n_sectors: int = 4,
    top_n_per_sector: int = 6,
    category_weights: dict[str, float] | None = None,
    direction: SelectionDirection = "top",
    min_price_score: float | None = None,
) -> pd.DataFrame:
    """Full pipeline: score companies → pick N sectors → pick M companies / sector.

    `direction="top"` (default) picks the highest-scoring sectors and the
    highest-scoring companies within each — long-only momentum.

    `direction="bottom"` picks the lowest-scoring sectors and the lowest-
    scoring companies within each — used as the short bucket of a long-short
    strategy.

    `min_price_score` is an optional gate that drops any company whose
    `score_price` is at or below the threshold BEFORE sector aggregation.
    Only applied to the long bucket (`direction="top"`) — for a long-short
    strategy the short side wants low-score names by design. Filter
    happens pre-aggregation so a sector full of below-threshold names
    doesn't get its average pulled up by the few survivors.

    Returns a DataFrame of selected companies with their scores and sector.
    """
    if signals_df.empty:
        return pd.DataFrame()

    # Score each company with per-category scores
    scored = compute_category_scores(signals_df, signal_weights, category_weights)

    # Optional price-score floor — applied only to long selection (the
    # short bucket explicitly targets low scores, so a min there would
    # be self-defeating). Comparison uses strict `>` so a threshold of
    # 30 means "must beat 30", matching how the UI label reads. NaN
    # scores are dropped — a company that couldn't be scored on the
    # price category isn't a candidate for a price-floor strategy.
    if direction == "top" and min_price_score is not None and "score_price" in scored.columns:
        before = len(scored)
        scored = scored[
            scored["score_price"].notna() & (scored["score_price"] > min_price_score)
        ].copy()
        # No-op safety: if every company was excluded the rest of the
        # function still produces an empty DataFrame cleanly.
        _ = before  # kept for future logging / event emission

    # Aggregate to sector and pick from the right end of the ranking. The
    # aggregator returns sectors descending by mean score, so .head() →
    # top, .tail() → bottom.
    sector_scores = aggregate_to_sector(scored)
    if direction == "top":
        chosen_sectors = sector_scores.head(top_n_sectors)["sector"].tolist()
        ascending_within = False
    else:
        chosen_sectors = sector_scores.tail(top_n_sectors)["sector"].tolist()
        ascending_within = True

    in_chosen_sectors = scored[scored["sector"].isin(chosen_sectors)].copy()

    # Within each chosen sector, pick the N best (top) or N worst (bottom)
    # by final score.
    selected = (
        in_chosen_sectors
        .sort_values(["sector", "momentum_score"], ascending=[True, ascending_within])
        .groupby("sector")
        .head(top_n_per_sector)
        .reset_index(drop=True)
    )

    return selected


def random_select(
    signals_df: pd.DataFrame,
    *,
    top_n_sectors: int = 4,
    top_n_per_sector: int = 6,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Random-baseline selector: pick random sectors and random companies.

    Same shape and column contract as score_and_select, but momentum_score
    and category scores are NaN — selection ignores signals entirely. Used
    as a noise floor to isolate signal-driven alpha from structural effects
    (sector diversification, equal-weight, monthly rebalance).
    """
    if signals_df.empty:
        return pd.DataFrame()

    df = signals_df.copy()
    sectors = [s for s in df["sector"].dropna().unique().tolist() if s]
    if not sectors:
        return pd.DataFrame()

    n_sectors = min(top_n_sectors, len(sectors))
    chosen_sectors = rng.choice(sectors, size=n_sectors, replace=False).tolist()

    parts = []
    for sec in chosen_sectors:
        in_sec = df[df["sector"] == sec]
        n = min(top_n_per_sector, len(in_sec))
        if n == 0:
            continue
        idx = rng.choice(in_sec.index.to_numpy(), size=n, replace=False)
        parts.append(in_sec.loc[idx])

    if not parts:
        return pd.DataFrame()

    selected = pd.concat(parts, ignore_index=True)

    # Match score_and_select's output columns with NaN sentinels — the
    # consumer in backtest.py guards on pd.notna for these.
    for cat in _get_category_keys():
        col = f"score_{cat}"
        if col not in selected.columns:
            selected[col] = np.nan
    if "momentum_score" not in selected.columns:
        selected["momentum_score"] = np.nan

    return selected
