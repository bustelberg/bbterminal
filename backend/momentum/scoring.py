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


# ⚠⚠ THE NORMALIZATION IS A STRATEGY PARAMETER, NOT AN IMPLEMENTATION DETAIL, AND `minmax` IS THE
#    DEFAULT ONLY BECAUSE CHANGING IT SILENTLY WOULD REWRITE THREE LIVE STRATEGIES. See
#    `_normalize` for what each one does and why `minmax` is the wrong one for a blend.
SCORE_NORMALIZATIONS = ("minmax", "rank", "robust_z")
DEFAULT_SCORE_NORMALIZATION = "minmax"

# ⚠ Robust z is clipped at ±2 MADs BEFORE the 0-1 mapping. Uncapped, one name at +1638% is ~10 MADs
#   out and re-flattens everyone else — the exact defect this mode exists to remove, reintroduced
#   one step later. ±2 keeps ~95% of a normal-ish cross-section un-clipped.
_ROBUST_Z_CAP = 2.0
# The constant that makes MAD a consistent estimator of σ for a normal distribution.
_MAD_TO_SIGMA = 1.4826


def _normalize(series: pd.Series, method: str) -> pd.Series:
    """One signal → [0, 1], so a weighted sum of signals means what its weights say.

    ⚠⚠ `minmax` DOES NOT, AND THAT IS THE DEFECT THIS EXISTS FOR. Min-max is monotonic, so on a
    SINGLE signal it ranks identically to `rank` and nothing is wrong. The damage is in the BLEND:
    the divisor is `max - min`, so one extreme name collapses everyone else into a sliver of the
    0-1 range, and a signal that occupies a sliver contributes almost nothing to a weighted sum
    however heavily it is weighted. Measured on ACWI (1,747 names, three equally-weighted price
    signals, all asked for 33.3%):

        signal      raw max    realized spread    EFFECTIVE weight
        mom_12_1     +1638%             0.0417              16.6%
        mom_6_1       +174%             0.1018              40.6%
        mom_3_1        +73%             0.1073              42.8%

    The longest-horizon signal — the one the strategy is named for — silently gets half the
    influence it was given, and the short-horizon signals absorb the difference. Top-20 selection
    overlap against `rank` on the same weights: **6 of 20**. So this is not cosmetic; it changes
    which companies are bought.

    ⚠ AND IT DOES NOT NEED A DATA BUG. The extremes here are Kioxia (+1638%), SK Hynix, Micron,
    Western Digital, SK Square — a real memory/AI supercycle, and a whole correlated CLUSTER rather
    than one rogue tick. Winsorizing a single outlier would not have fixed it.

    The three modes:

      `minmax`    (x - min) / (max - min). The legacy behaviour, kept as the DEFAULT so saved runs
                  and the three live scheduled strategies keep buying what they bought. Do not
                  "fix" it in place — see `min_price_score` below.
      `rank`      percentile rank, ties averaged. Uniform by construction, so EVERY signal has the
                  same spread and the weights are exact. Also the only mode where the score has a
                  plain-language meaning ("60" = better than 60% of the universe), which is what
                  makes `min_price_score` legible.
      `robust_z`  (x - median) / (1.4826 · MAD), clipped to ±2, mapped to [0, 1]. Outlier-resistant
                  like `rank` but keeps MAGNITUDE: two names in the same decile stay apart if one
                  is genuinely far ahead. Spread is only approximately equal across signals, so
                  weights are near-exact rather than exact.

    ⚠⚠ THE 0-100 SCORE MEANS SOMETHING DIFFERENT UNDER EACH, AND `min_price_score` IS READ AGAINST
    IT. On ACWI the median stock scores **5.0/100** under `minmax` and **50/100** under `rank`, so
    the floor of 30 that all three live strategies carry goes from "roughly the top few percent" to
    "the top 70%". That is why this is a per-strategy parameter that defaults to the old value and
    is folded into `_strategy_hash`, rather than a correction applied everywhere at once.

    ⚠ NaN IS LEFT AS NaN here and neutralized by the caller (`fillna(0.5)`), so the three modes
    agree about missing data rather than each inventing a convention.
    """
    s = pd.to_numeric(series, errors="coerce").astype(float)
    # ⚠ Fewer than two distinct observations is not a cross-section: there is no "relative to the
    #   others" to express, so every mode returns neutral rather than 0, 1, or a rank of 1.0.
    if s.notna().sum() < 2 or s.nunique(dropna=True) < 2:
        return pd.Series(np.nan, index=s.index).where(s.isna(), 0.5)

    if method == "rank":
        return s.rank(pct=True, method="average")

    if method == "robust_z":
        med = s.median()
        mad = (s - med).abs().median()
        if not mad or pd.isna(mad):
            # ⚠ A ZERO MAD MEANS THE MIDDLE HALF IS IDENTICAL, NOT THAT THE SIGNAL IS FLAT — the
            #   tails can still differ. Dividing would be ±inf, so fall back to `rank`, which is
            #   defined for any distribution, rather than to neutral (which would silently delete
            #   a signal that does carry information in its tails).
            return s.rank(pct=True, method="average")
        z = ((s - med) / (_MAD_TO_SIGMA * mad)).clip(-_ROBUST_Z_CAP, _ROBUST_Z_CAP)
        return (z + _ROBUST_Z_CAP) / (2 * _ROBUST_Z_CAP)

    lo, hi = s.min(), s.max()
    return (s - lo) / (hi - lo)


def _score_category(
    df: pd.DataFrame,
    signal_weights: dict[str, float],
    signal_keys: list[str],
    score_col: str,
    *,
    normalization: str = DEFAULT_SCORE_NORMALIZATION,
) -> pd.DataFrame:
    """Normalize signals within a category and compute a 0-100 score.

    Only signals present in both signal_keys and df.columns are used.
    `normalization` picks how each signal is mapped to [0, 1] — see `_normalize`, where the choice
    is explained and measured. It is NOT a free implementation choice: it changes which companies
    are selected and what `min_price_score` means.
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
        norm = _normalize(df[col], normalization).fillna(0.5)
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
    normalization: str = DEFAULT_SCORE_NORMALIZATION,
) -> pd.DataFrame:
    """Score each company per category (0-100), then compute a weighted final score.

    `normalization` selects how each signal is mapped to [0, 1] before the weighted sum —
    see `_normalize`. It changes selection and the meaning of `min_price_score`, so it defaults to
    the legacy `minmax` and travels in `_strategy_hash`.

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
        df = _score_category(df, signal_weights, keys, col, normalization=normalization)

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
    *,
    normalization: str = DEFAULT_SCORE_NORMALIZATION,
) -> pd.DataFrame:
    """The score-half of `score_and_select`: append per-category scores
    + `momentum_score` to every row of `signals_df`. Pure function of
    (signals_df, signal_weights, category_weights, signal_defs) — the result
    is safe to cache across any variants that share those inputs (which,
    in practice, is every variant in a sweep, since `signal_weights`
    and `category_weights` come from the base request and don't vary
    per variant).

    ⚠ `normalization` IS PART OF THAT CACHE KEY WHEREVER THIS RESULT IS MEMOIZED. Two runs that
    differ only in it produce different scores from identical signals, so a key blind to it would
    serve one strategy's scores to another.

    `signal_defs` selects which pillars score (default price+volume; pass
    EXTRA_SIGNAL_DEFS for MomentumExtra's trend pillar).

    For long-short strategies the same scored frame feeds both the
    top and bottom selections — no need to rescore between them."""
    if signals_df.empty:
        return signals_df
    return compute_category_scores(signals_df, signal_weights, category_weights, signal_defs,
                                   normalization=normalization)


def selection_pool(
    scored: pd.DataFrame,
    *,
    direction: SelectionDirection = "top",
    min_price_score: float | None = None,
    backfill_below_min_score: bool = False,
) -> pd.DataFrame:
    """The rows the sector ranking is computed over — `scored` after the
    `min_price_score` hard floor, when that floor applies.

    ⚠ EXTRACTED SO THE SECTOR SCORES SHOWN TO A READER ARE AGGREGATED OVER THE
    SAME ROWS THE SELECTION RANKED. Recomputing "the pool" beside the selector
    is a second definition of it: the floor is skipped for `direction="bottom"`
    and softened to a preference under `backfill_below_min_score`, so a copy
    drifts the moment either is touched, and the sector table would then explain
    a choice that was made over a different set of companies.
    """
    has_min = (
        direction == "top" and min_price_score is not None and "score_price" in scored.columns
    )
    if has_min and not backfill_below_min_score:
        mask = scored["score_price"].notna() & (scored["score_price"] >= min_price_score)
        if not mask.all():
            return scored[mask]
    return scored


def sector_pool_scores(pool: pd.DataFrame) -> list[dict]:
    """Per-sector momentum / price / volume score over `pool`, best momentum first.

    ⚠ EVERY PILLAR GOES THROUGH `aggregate_to_sector`, THE FUNCTION THE SELECTION
    RANKS WITH. It averages (a `mean()`), and that choice is load-bearing — the
    golden-master test exists partly because switching it to `median()` changes
    which sectors get picked and nothing else fails. A second aggregation here
    would let the table disagree with the ranking it is meant to explain.

    ⚠ EVERY SECTOR IN THE POOL, NOT ONLY THE CHOSEN ONES. The sector that just
    missed the cut is the most informative row on the table; showing only the
    picked ones answers "what did we hold" a second time instead of "why".
    """
    if pool.empty or "sector" not in pool.columns:
        return []
    base = aggregate_to_sector(pool)                      # momentum_score, ranked
    counts = pool.groupby("sector").size()
    by_cat: dict[str, pd.Series] = {}
    for col in pool.columns:
        if isinstance(col, str) and col.startswith("score_"):
            agg = aggregate_to_sector(pool, score_col=col)
            by_cat[col[len("score_"):]] = agg.set_index("sector")[col]
    out: list[dict] = []
    for rank, row in enumerate(base.itertuples(index=False), start=1):
        sector = row.sector
        scores = {
            cat: (round(float(s.get(sector)), 1) if pd.notna(s.get(sector)) else None)
            for cat, s in by_cat.items()
        }
        out.append({
            "sector": sector,
            "rank": rank,
            "momentum_score": round(float(row.momentum_score), 2)
            if pd.notna(row.momentum_score) else None,
            "category_scores": scores,
            "companies": int(counts.get(sector, 0)),
        })
    return out


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
    # The COMPANY pool — the floor applies here, and only here.
    pool = selection_pool(
        scored,
        direction=direction,
        min_price_score=min_price_score,
        backfill_below_min_score=backfill_below_min_score,
    )

    # ⚠ SECTORS ARE RANKED OVER EVERY SCORED COMPANY, NOT OVER THE FLOOR-FILTERED POOL
    # (2026-07-31, deliberate change). `min_price_score` is a rule about which COMPANIES are worth
    # buying; ranking sectors on the survivors made it a rule about which SECTORS exist. The
    # difference is not subtle: a sector whose names all sit below the floor did not rank badly, it
    # VANISHED — measured on the live strategies (floor 30), Consumer Cyclical ranked 3rd on
    # 11 June, disappeared entirely on the 12th and 15th, and came back 8th on the 16th. Judging a
    # sector on its best few survivors also flatters exactly the sectors with the fewest of them.
    #
    # The floor still decides every company that gets bought, three lines down.
    #
    # ⚠ CONSEQUENCE, ACCEPTED AND NOT PAPERED OVER: a sector can now be chosen and then contribute
    # NOTHING, because none of its companies clear the floor. The portfolio is smaller that period
    # rather than silently sliding to the next-best sector — substituting one would be a different
    # strategy, chosen here by accident.
    sector_scores = aggregate_to_sector(scored)
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
    normalization: str = DEFAULT_SCORE_NORMALIZATION,
) -> pd.DataFrame:
    """Convenience wrapper combining `score_universe` + `select_from_scored`
    for callers that don't manage a score cache (single-run path, tests,
    legacy callers). Behavior unchanged from the pre-split version.

    Variant sweeps should use the split form directly so the score pass
    is cached across variants — see `_period.compute_selection_period`
    for the runner's cache-aware call site."""
    scored = score_universe(signals_df, signal_weights, category_weights, signal_defs,
                            normalization=normalization)
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
