# src\quick_insight\ai_momentum\scoring\scorer.py

import pandas as pd
import numpy as np


def compute_weighted_score(
    df: pd.DataFrame,
    weights: dict[str, float],
    *,
    score_col: str = "momentum_score",
) -> pd.DataFrame:
    """
    Normalize signals and compute a weighted score (0–100).

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe containing raw signals.

    weights : dict[str, float]
        Mapping {column_name: weight}. Columns will be normalized and
        combined using these weights. Negative weights are allowed.

    score_col : str
        Name of the resulting score column.

    Returns
    -------
    pd.DataFrame
        Copy of df with normalized columns and a final score column.
    """

    df = df.copy()

    cols = list(weights.keys())

    # normalize weights to sum to 1 in absolute terms
    weight_sum = sum(abs(w) for w in weights.values())
    weights = {k: v / weight_sum for k, v in weights.items()}

    for col in cols:

        series = df[col].astype(float)

        min_val = series.min()
        max_val = series.max()

        if pd.isna(min_val) or pd.isna(max_val) or min_val == max_val:
            norm = pd.Series(0.5, index=df.index)
        else:
            norm = (series - min_val) / (max_val - min_val)

        df[f"{col}_norm"] = norm

    score = np.zeros(len(df))

    for col, weight in weights.items():
        score += df[f"{col}_norm"] * weight

    df[score_col] = (score * 100).round(2)

    return df