"""Scoring engine: normalize signals, weight, select top sectors & companies.

Supports multiple signal categories (e.g. price, volume) each scored 0-100
independently, then combined via category weights into a final score.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .signals import PRICE_SIGNAL_DEFS


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
) -> pd.DataFrame:
    """Full pipeline: score companies -> pick top sectors -> pick top companies.

    Returns a DataFrame of selected companies with their scores and sector.
    """
    if signals_df.empty:
        return pd.DataFrame()

    # Score each company with per-category scores
    scored = compute_category_scores(signals_df, signal_weights, category_weights)

    # Aggregate to sector and pick top sectors
    sector_scores = aggregate_to_sector(scored)
    top_sectors = sector_scores.head(top_n_sectors)["sector"].tolist()

    # Filter to top sectors only
    in_top_sectors = scored[scored["sector"].isin(top_sectors)].copy()

    # Within each top sector, pick top N companies by final score
    selected = (
        in_top_sectors
        .sort_values(["sector", "momentum_score"], ascending=[True, False])
        .groupby("sector")
        .head(top_n_per_sector)
        .reset_index(drop=True)
    )

    return selected
