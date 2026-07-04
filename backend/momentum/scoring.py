"""Scoring engine: normalize signals, weight, select top sectors & companies.

Supports multiple signal categories (e.g. price, volume) each scored 0-100
independently, then combined via category weights into a final score.
"""
from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

from .signals import EXTRA_SIGNAL_DEFS, PRICE_SIGNAL_DEFS


def signal_defs_for_mode(selection_mode: str) -> list[dict]:
    """The signal-def list whose `group` tags define the scoring pillars for a
    strategy. MomentumExtra activates the third "trend" pillar; every other mode
    uses the classic price+volume defs, so their scoring is byte-identical."""
    return EXTRA_SIGNAL_DEFS if selection_mode == "momentum_extra" else PRICE_SIGNAL_DEFS


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


def _get_category_keys(signal_defs: list[dict] | None = None) -> dict[str, list[str]]:
    """Build {category: [signal_keys]} from the given signal defs (default:
    the classic price+volume PRICE_SIGNAL_DEFS)."""
    cats: dict[str, list[str]] = {}
    for s in (signal_defs if signal_defs is not None else PRICE_SIGNAL_DEFS):
        group = s.get("group", "price")
        cats.setdefault(group, []).append(s["key"])
    return cats


def extract_category_scores(row: pd.Series) -> dict[str, float | None]:
    """Per-category 0-100 scores off a scored row, rounded to 1dp. Always
    reports the classic price+volume pillars (None when their `score_<cat>`
    column is absent or NaN), plus any extra `score_<cat>` columns the row
    carries — so MomentumExtra rows surface `trend` automatically. Shared by the
    period + current-portfolio holding builders."""
    cats = list(_get_category_keys().keys())  # price, volume — always reported
    for col in row.index:
        if isinstance(col, str) and col.startswith("score_"):
            cat = col[len("score_"):]
            if cat not in cats:
                cats.append(cat)
    out: dict[str, float | None] = {}
    for cat in cats:
        col = f"score_{cat}"
        val = row[col] if col in row.index else None
        out[cat] = round(float(val), 1) if (val is not None and pd.notna(val)) else None
    return out


def compute_category_scores(
    df: pd.DataFrame,
    signal_weights: dict[str, float],
    category_weights: dict[str, float] | None = None,
    signal_defs: list[dict] | None = None,
    *,
    exclude_incomplete: bool = True,
) -> pd.DataFrame:
    """Score each company per category (0-100), then compute a weighted final score.

    Adds columns: score_price, score_volume, ..., momentum_score (final).

    When `exclude_incomplete` (default), a company is DROPPED from scoring if any
    signal that actually carries weight (a weighted signal in a weighted
    category) can't be computed — e.g. < 12 months of price history means no
    12-1M return. Previously such gaps defaulted to a neutral 50; now the name is
    excluded until it has enough history to compute every weighted stat. Pass
    False to keep the old default-50 behavior.
    """
    cats = _get_category_keys(signal_defs)

    if category_weights is None:
        # Default: equal weight per category
        n = len(cats)
        category_weights = {c: 1.0 / n for c in cats}

    # Normalize category weights
    cw_sum = sum(abs(v) for v in category_weights.values())
    if cw_sum == 0:
        cw_sum = 1.0
    cw_normed = {c: v / cw_sum for c, v in category_weights.items()}

    # Exclusion: require EVERY weighted signal (in a weighted category) to be
    # computable; drop names that can't (they re-qualify once they have the
    # history). Applied before scoring so selection never sees them.
    if exclude_incomplete and len(df):
        required = [
            k
            for cat, cat_keys in cats.items() if cw_normed.get(cat, 0) != 0
            for k in cat_keys
            if k in df.columns and signal_weights.get(k, 0) != 0
        ]
        if required:
            complete = df[required].notna().all(axis=1)
            df = df[complete].copy()

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


def score_universe(
    signals_df: pd.DataFrame,
    signal_weights: dict[str, float],
    category_weights: dict[str, float] | None = None,
    signal_defs: list[dict] | None = None,
) -> pd.DataFrame:
    """The score-half of `score_and_select`: append per-category scores
    + `momentum_score` to every row of `signals_df`. Pure function of
    (signals_df, signal_weights, category_weights, signal_defs) — the result
    is safe to cache across any variants that share those inputs (which,
    in practice, is every variant in a sweep, since `signal_weights`
    and `category_weights` come from the base request and don't vary
    per variant).

    `signal_defs` selects which pillars score (default price+volume; pass
    EXTRA_SIGNAL_DEFS for MomentumExtra's trend pillar).

    For long-short strategies the same scored frame feeds both the
    top and bottom selections — no need to rescore between them."""
    if signals_df.empty:
        return signals_df
    return compute_category_scores(signals_df, signal_weights, category_weights, signal_defs)


def select_from_scored(
    scored: pd.DataFrame,
    *,
    top_n_sectors: int = 4,
    top_n_per_sector: int = 6,
    direction: SelectionDirection = "top",
    min_price_score: float | None = None,
    backfill_below_min_score: bool = False,
) -> pd.DataFrame:
    """The select-half of `score_and_select`: applies `min_price_score`,
    aggregates to sector, picks `top_n_sectors` × `top_n_per_sector`,
    attaches `sector_rank` + `company_rank`. Takes the pre-scored
    DataFrame produced by `score_universe` so callers can cache the
    score pass and only pay this per-variant selection cost.

    Behavior is byte-identical to the equivalent path inside
    `score_and_select` — that function now composes these two halves."""
    if scored.empty:
        return pd.DataFrame()

    # Reverted to the Win #B pandas version. The numpy variant produced
    # silent-empty selections in real-data periods where the sector
    # column had non-string values (NaN/pd.NA — `s in set` doesn't
    # match what pandas's `.isin()` does). The pandas implementation
    # handles those gracefully via Series.isin(). The numpy version's
    # speedup (~1.18× on bench) wasn't worth the correctness risk.
    # min_price_score handling. Default (backfill=False) is a HARD FLOOR: names
    # with a price score below `min_price_score` are DROPPED entirely, so every
    # pick satisfies the floor (a sector short on eligible names ends up with
    # fewer than top_n_per_sector — the floor is never violated to pad it). The
    # threshold is inclusive (`>=`): min_price_score=30 keeps a score of exactly
    # 30. With backfill=True it's instead a soft within-sector PREFERENCE that
    # keeps the full pool and pads under-filled sectors with below-floor names.
    has_min = (
        direction == "top" and min_price_score is not None and "score_price" in scored.columns
    )
    pool = scored
    if has_min and not backfill_below_min_score:
        mask = scored["score_price"].notna() & (scored["score_price"] >= min_price_score)
        if not mask.all():
            pool = scored[mask]

    sector_scores = aggregate_to_sector(pool)
    if direction == "top":
        chosen_sectors = sector_scores.head(top_n_sectors)["sector"].tolist()
        ascending_within = False
    else:
        # Bottom: reverse the tail so chosen_sectors[0] is the worst
        # (rank 1 in the "worst sector" sense), matching the "top"
        # convention where rank 1 is the best of what was picked.
        chosen_sectors = list(reversed(sector_scores.tail(top_n_sectors)["sector"].tolist()))
        ascending_within = True

    if not chosen_sectors:
        return pd.DataFrame()

    sector_rank_map = {sec: i + 1 for i, sec in enumerate(chosen_sectors)}
    in_chosen = pool[pool["sector"].isin(chosen_sectors)]
    if in_chosen.empty:
        return pd.DataFrame()

    in_chosen = in_chosen.assign(sector_rank=in_chosen["sector"].map(sector_rank_map))
    sort_cols = ["sector_rank", "momentum_score"]
    sort_asc = [True, ascending_within]
    if has_min and backfill_below_min_score:
        # Above-threshold names first, then by momentum → the top_n_per_sector
        # head takes eligible names first and only dips below the threshold to
        # fill the sector up (the "pick the next eligible company" backfill).
        in_chosen = in_chosen.assign(
            _above_min=(
                in_chosen["score_price"].notna()
                & (in_chosen["score_price"] >= min_price_score)
            ).astype(int),
        )
        sort_cols = ["sector_rank", "_above_min", "momentum_score"]
        sort_asc = [True, False, ascending_within]
    in_chosen = in_chosen.sort_values(sort_cols, ascending=sort_asc)
    selected = (
        in_chosen
        .groupby("sector_rank", sort=False)
        .head(top_n_per_sector)
        .reset_index(drop=True)
    )
    if "_above_min" in selected.columns:
        selected = selected.drop(columns=["_above_min"])

    if not selected.empty:
        selected["sector_rank"] = selected["sector_rank"].astype("Int64")
        selected["company_rank"] = (
            selected.groupby("sector_rank", sort=False).cumcount().astype("int64") + 1
        )

    return selected


def score_and_select(
    signals_df: pd.DataFrame,
    signal_weights: dict[str, float],
    *,
    top_n_sectors: int = 4,
    top_n_per_sector: int = 6,
    category_weights: dict[str, float] | None = None,
    direction: SelectionDirection = "top",
    min_price_score: float | None = None,
    backfill_below_min_score: bool = False,
    signal_defs: list[dict] | None = None,
) -> pd.DataFrame:
    """Convenience wrapper combining `score_universe` + `select_from_scored`
    for callers that don't manage a score cache (single-run path, tests,
    legacy callers). Behavior unchanged from the pre-split version.

    Variant sweeps should use the split form directly so the score pass
    is cached across variants — see `_period.compute_selection_period`
    for the runner's cache-aware call site."""
    scored = score_universe(signals_df, signal_weights, category_weights, signal_defs)
    return select_from_scored(
        scored,
        top_n_sectors=top_n_sectors,
        top_n_per_sector=top_n_per_sector,
        direction=direction,
        min_price_score=min_price_score,
        backfill_below_min_score=backfill_below_min_score,
    )


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
