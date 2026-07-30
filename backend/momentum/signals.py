"""Universe-level momentum signal computation.

The signal MATH and the as-of discipline (strict `<` cutoff, 20-bar minimum,
30-day staleness guard) moved to `signal_engine.daily` in Phase 2, alongside the
asset pipeline's month-end battery, so the two engines can't drift apart. What
remains here is the universe layer: joining signal rows to `universe_df`'s
sector / name / ticker and returning the per-cutoff DataFrames the backtester
consumes.

`MAX_STALENESS_DAYS` is re-exported because callers and tests refer to it by
this path; the signal helpers now live in `signal_engine.daily` and are imported
from there directly.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

from signal_engine import legacy_defs
from signal_engine.daily import (
    MAX_STALENESS_DAYS,
    compute_single_company_signals as _compute_single_company_signals,
    compute_volume_signals as _compute_volume_signals,
    evaluate_panel,
)

__all__ = [
    "EXTRA_SIGNAL_DEFS",
    "MAX_STALENESS_DAYS",
    "PRICE_SIGNAL_DEFS",
    "TREND_SIGNAL_DEFS",
    "compute_price_signals",
    "compute_signals_panel",
]


# Signal DEFINITIONS live in `signal_engine.registry` (Phase 2), alongside the
# asset pipeline's month-end battery, so the two can't drift apart unnoticed
# again.
#
# `key` in these dicts is the legacy NAME (`mom_12_1`), which is what a saved
# `scheduled_strategy.config`'s `signal_weights` is keyed by — not the registry's
# cadence-namespaced key (`daily.mom_12_1`). Changing it would orphan every saved
# strategy.
PRICE_SIGNAL_DEFS: list[dict] = legacy_defs("daily_asof", ("price", "volume"))


# Trend-quality pillar — price-derived "how the return was earned" signals.
# NOT part of PRICE_SIGNAL_DEFS, so scoring only sees the "trend" category when
# a caller explicitly passes EXTRA_SIGNAL_DEFS (the MomentumExtra strategy).
# That keeps the classic Momentum strategy mathematically untouched.
TREND_SIGNAL_DEFS: list[dict] = legacy_defs("daily_asof", ("trend",))

# Price + volume + trend — passed to the scoring engine for the MomentumExtra
# strategy so its `_get_category_keys` discovers the third "trend" pillar.
EXTRA_SIGNAL_DEFS: list[dict] = PRICE_SIGNAL_DEFS + TREND_SIGNAL_DEFS


# ---------------------------------------------------------------------------
# Universe-level signal computation
# ---------------------------------------------------------------------------

def compute_price_signals(
    prices_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    as_of_date: date,
    *,
    price_index: dict[int, pd.Series] | None = None,
    volume_index: dict[int, pd.Series] | None = None,
) -> pd.DataFrame:
    """Compute price and volume signals for all companies as of a given date.

    Args:
        prices_df: Full price DataFrame (unused if price_index is provided).
        universe_df: Company DataFrame with at least [company_id, sector].
        as_of_date: Only use prices on or before this date (no look-ahead).
        price_index: Pre-indexed dict of {company_id: Series}. If provided,
                     avoids repeated DataFrame filtering (much faster).
        volume_index: Pre-indexed dict of {company_id: Series} for volume data.

    Returns:
        DataFrame with company_id, sector, and all signal columns.
        Companies with insufficient data are excluded.
    """
    cutoff = pd.Timestamp(as_of_date)

    results = []

    if price_index is not None:
        for cid in universe_df["company_id"].unique():
            series = price_index.get(int(cid))
            if series is None or len(series) < 20:
                continue
            # Strict `<` so signals never see the close at which we'll enter the trade.
            trimmed = series[series.index < cutoff]
            if len(trimmed) < 20:
                continue
            if (cutoff - trimmed.index[-1]).days > MAX_STALENESS_DAYS:
                continue
            signals = _compute_single_company_signals(trimmed)
            # Volume signals
            if volume_index is not None:
                vol_series = volume_index.get(int(cid))
                if vol_series is not None and len(vol_series) > 0:
                    vol_trimmed = vol_series[vol_series.index < cutoff]
                    vol_signals = _compute_volume_signals(vol_trimmed)
                    signals.update(vol_signals)
            signals["company_id"] = cid
            results.append(signals)
    else:
        available = prices_df[prices_df["target_date"] < cutoff]
        for cid in universe_df["company_id"].unique():
            company_prices = available[available["company_id"] == cid]
            if company_prices.empty or len(company_prices) < 20:
                continue
            series = pd.Series(
                company_prices["price"].values,
                index=pd.DatetimeIndex(company_prices["target_date"]),
                dtype="float64",
            ).sort_index()
            if (cutoff - series.index[-1]).days > MAX_STALENESS_DAYS:
                continue
            signals = _compute_single_company_signals(series)
            signals["company_id"] = cid
            results.append(signals)

    if not results:
        return pd.DataFrame()

    signals_df = pd.DataFrame(results)
    # Merge sector from universe
    signals_df = signals_df.merge(
        universe_df[["company_id", "sector", "company_name", "gurufocus_ticker"]],
        on="company_id",
        how="left",
    )
    return signals_df


def compute_signals_panel(
    universe_df: pd.DataFrame,
    cutoffs: list[date],
    *,
    price_index: dict[int, pd.Series],
    volume_index: dict[int, pd.Series] | None = None,
) -> dict[date, pd.DataFrame]:
    """Compute price+volume signals for every cutoff in `cutoffs` in one pass.

    Thin wrapper over `signal_engine.daily.evaluate_panel`: it owns the strict `<`
    cutoff, the 20-bar minimum and the 30-day staleness guard. This function adds
    the universe join, so each cutoff's DataFrame has the same shape and semantics
    as `compute_price_signals(..., as_of_date=cutoff)`.
    """
    if not cutoffs:
        return {}

    per_cutoff = evaluate_panel(
        list(universe_df["company_id"].unique()),
        cutoffs,
        price_index=price_index,
        volume_index=volume_index,
        id_col="company_id",
    )

    sector_cols = universe_df[["company_id", "sector", "company_name", "gurufocus_ticker"]]
    result: dict[date, pd.DataFrame] = {}
    for c in cutoffs:
        rows = per_cutoff[pd.Timestamp(c)]
        if not rows:
            result[c] = pd.DataFrame()
            continue
        result[c] = pd.DataFrame(rows).merge(sector_cols, on="company_id", how="left")
    return result
