"""Pre-compute the inputs every backtest variant needs: rebalance dates,
price/volume indices, and the signal panel.

The runner can either call `_prepare_backtest` (one-off — builds the full
panel itself) or, for a variant sweep, call `build_shared_backtest_inputs`
once with the union of all variants' cutoffs and then
`prepare_variant_from_shared` per variant. The shared path turns an N-variant
sweep from O(N × panel-build) into O(panel-build + N × slice)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from ..signals import compute_signals_panel
from .dates import _generate_rebalance_dates
from .indices import _build_price_index, _build_volume_index
from .types import RebalanceFrequency, _DEFAULT_FREQUENCY


@dataclass
class _BacktestPrepared:
    """Precomputed inputs that depend only on dates / prices / universe — not
    on the selection RNG. Cached and reused across trials by
    `run_multi_trial_backtest` so the (expensive) signal panel is built once
    rather than N times.

    `periods` is the rebalance-date list (was named `months` when only
    monthly was supported). Length must be ≥ 2 — first N-1 are entry dates,
    last is the final exit date.
    """
    periods: list[date]
    price_index: dict[int, pd.Series]
    local_price_index: dict[int, pd.Series] | None
    volume_index: dict[int, pd.Series] | None
    panel: dict[date, pd.DataFrame]
    frequency: RebalanceFrequency


def _prepare_backtest(
    *,
    start_date: date,
    end_date: date,
    prices_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    volumes_df: pd.DataFrame | None,
    prices_local_df: pd.DataFrame | None,
    monthly_eligible: dict[str, dict[int, str | None]] | None,
    frequency: RebalanceFrequency = _DEFAULT_FREQUENCY,
) -> _BacktestPrepared:
    periods = _generate_rebalance_dates(start_date, end_date, frequency, prices_df)
    if len(periods) < 2:
        raise ValueError(f"Need at least 2 rebalance periods for a {frequency} backtest (got {len(periods)})")

    price_index = _build_price_index(prices_df)
    local_price_index = (
        _build_price_index(prices_local_df)
        if prices_local_df is not None and not prices_local_df.empty
        else None
    )
    volume_index = (
        _build_volume_index(volumes_df)
        if volumes_df is not None and not volumes_df.empty
        else None
    )

    # Include ALL periods as cutoffs (not just periods[:-1]). The last entry
    # is normally just an exit anchor — but when run_backtest decides to
    # append an "open" period it becomes the entry of that final partial
    # period and needs signals computed at that cutoff. Including it
    # unconditionally costs one extra signal column per company; the
    # alternative (recomputing on demand) is more code for the same work.
    cutoff_dates = periods
    if monthly_eligible is not None:
        panel_universe_ids: set[int] = set()
        for month_dict in monthly_eligible.values():
            panel_universe_ids.update(month_dict.keys())
        panel_universe_df = (
            universe_df[universe_df["company_id"].isin(panel_universe_ids)]
            .reset_index(drop=True)
        )
    else:
        panel_universe_df = universe_df

    panel = compute_signals_panel(
        panel_universe_df,
        cutoff_dates,
        price_index=price_index,
        volume_index=volume_index,
    )

    return _BacktestPrepared(
        periods=periods,
        price_index=price_index,
        local_price_index=local_price_index,
        volume_index=volume_index,
        panel=panel,
        frequency=frequency,
    )


@dataclass
class _SharedBacktestInputs:
    """The portion of a backtest's setup that's identical across every
    variant in a sweep — price/volume indices and a single signal panel
    built over the *union* of all variants' cutoff dates. Each variant
    then takes a sliced view of this panel via `_prepare_variant_from_shared`.

    Building the per-company rolling signal panels is the dominant cost
    in `_prepare_backtest` (it scans each company's full price history
    once); per-cutoff lookups are cheap searchsorted ops. Rolling those
    builds into a single call cuts ~N-1 redundant scans for an N-variant
    sweep, which is the difference between a 14-variant sweep paying
    ~14×10s = 140s of panel construction vs ~10s.
    """
    price_index: dict[int, pd.Series]
    local_price_index: dict[int, pd.Series] | None
    volume_index: dict[int, pd.Series] | None
    panel_universe_df: pd.DataFrame
    union_panel: dict[date, pd.DataFrame]


def build_shared_backtest_inputs(
    *,
    prices_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    volumes_df: pd.DataFrame | None,
    prices_local_df: pd.DataFrame | None,
    monthly_eligible: dict[str, dict[int, str | None]] | None,
    union_cutoffs: list[date],
) -> _SharedBacktestInputs:
    """Build the shared portion of `_BacktestPrepared` once for a sweep.
    `union_cutoffs` should contain every cutoff date any variant in the
    sweep will need (i.e. union of `_generate_rebalance_dates(...)[:-1]`
    across variants). Cutoff order doesn't matter for the panel; the
    function dedupes + sorts internally."""
    price_index = _build_price_index(prices_df)
    local_price_index = (
        _build_price_index(prices_local_df)
        if prices_local_df is not None and not prices_local_df.empty
        else None
    )
    volume_index = (
        _build_volume_index(volumes_df)
        if volumes_df is not None and not volumes_df.empty
        else None
    )
    if monthly_eligible is not None:
        panel_universe_ids: set[int] = set()
        for month_dict in monthly_eligible.values():
            panel_universe_ids.update(month_dict.keys())
        panel_universe_df = (
            universe_df[universe_df["company_id"].isin(panel_universe_ids)]
            .reset_index(drop=True)
        )
    else:
        panel_universe_df = universe_df

    deduped_cutoffs = sorted(set(union_cutoffs))
    union_panel = compute_signals_panel(
        panel_universe_df,
        deduped_cutoffs,
        price_index=price_index,
        volume_index=volume_index,
    )
    return _SharedBacktestInputs(
        price_index=price_index,
        local_price_index=local_price_index,
        volume_index=volume_index,
        panel_universe_df=panel_universe_df,
        union_panel=union_panel,
    )


def prepare_variant_from_shared(
    *,
    shared: _SharedBacktestInputs,
    start_date: date,
    end_date: date,
    frequency: RebalanceFrequency,
    prices_df: pd.DataFrame,
) -> _BacktestPrepared:
    """Build a `_BacktestPrepared` for one variant from sweep-shared
    inputs. The signal panel is filtered to just this variant's cutoffs;
    indices are reused as-is. Use this in place of `_prepare_backtest`
    when you've already called `build_shared_backtest_inputs` for the
    sweep — gives byte-identical results, just without re-doing the
    expensive per-company rolling panel scan."""
    periods = _generate_rebalance_dates(start_date, end_date, frequency, prices_df)
    if len(periods) < 2:
        raise ValueError(f"Need at least 2 rebalance periods for a {frequency} backtest (got {len(periods)})")
    # Include all periods as cutoffs (see _prepare_backtest above for the
    # full justification — the last entry becomes the open-period entry).
    cutoff_set = set(periods)
    sliced_panel = {d: df for d, df in shared.union_panel.items() if d in cutoff_set}
    return _BacktestPrepared(
        periods=periods,
        price_index=shared.price_index,
        local_price_index=shared.local_price_index,
        volume_index=shared.volume_index,
        panel=sliced_panel,
        frequency=frequency,
    )
