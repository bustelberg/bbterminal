"""Scoring engine: normalize signals, weight, select top sectors & companies.

Ported from old_src/quick_insight/ai_momentum/scoring/scorer.py and utils.py.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def compute_weighted_score(
    df: pd.DataFrame,
    weights: dict[str, float],
    *,
    score_col: str = "momentum_score",
) -> pd.DataFrame:
    """Min-max normalize signals and compute a weighted score (0-100).

    Columns not present in df are silently skipped.
    """
    df = df.copy()
    active_weights = {k: v for k, v in weights.items() if k in df.columns and v != 0}

    if not active_weights:
        df[score_col] = 50.0
        return df

    weight_sum = sum(abs(w) for w in active_weights.values())
    normed_weights = {k: v / weight_sum for k, v in active_weights.items()}

    score = np.zeros(len(df))

    for col, weight in normed_weights.items():
        series = pd.to_numeric(df[col], errors="coerce").astype(float)
        min_val = series.min()
        max_val = series.max()

        if pd.isna(min_val) or pd.isna(max_val) or min_val == max_val:
            norm = pd.Series(0.5, index=df.index)
        else:
            norm = (series - min_val) / (max_val - min_val)

        # Fill NaN with 0.5 (neutral) so missing signals don't skew results
        norm = norm.fillna(0.5)
        score += norm.values * weight

    df[score_col] = (score * 100).round(2)
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
) -> pd.DataFrame:
    """Full pipeline: score companies -> pick top sectors -> pick top companies.

    Returns a DataFrame of selected companies with their scores and sector.
    """
    if signals_df.empty:
        return pd.DataFrame()

    # Score each company
    scored = compute_weighted_score(signals_df, signal_weights)

    # Aggregate to sector and pick top sectors
    sector_scores = aggregate_to_sector(scored)
    top_sectors = sector_scores.head(top_n_sectors)["sector"].tolist()

    # Filter to top sectors only
    in_top_sectors = scored[scored["sector"].isin(top_sectors)].copy()

    # Within each top sector, pick top N companies
    selected = (
        in_top_sectors
        .sort_values(["sector", "momentum_score"], ascending=[True, False])
        .groupby("sector")
        .head(top_n_per_sector)
        .reset_index(drop=True)
    )

    return selected
