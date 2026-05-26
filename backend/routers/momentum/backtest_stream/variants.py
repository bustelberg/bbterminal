"""Variants sweep: run a single data load against N (frequency × strategy_type)
backtest configurations.

The data pipeline (universe load → ensure → bulk-load prices/volumes
→ FX) ran ONCE in the orchestrator; this loop runs just the backtest
computation per variant against the same in-memory frames. Each variant
emits its own `variant_start` / `variant_result` / `variant_error`
event identified by `{frequency}__{strategy_type}` so the frontend can
update the variants table row-by-row.

The per-company rolling signal scan is the dominant cost of
`_prepare_backtest`, and it's identical across every variant in the
sweep. Running it N times for N variants was wasting ~(N-1) full passes
over the universe's price history. This file pre-builds the union
signal panel once via `build_shared_backtest_inputs` and the variant
loop calls `prepare_variant_from_shared` to slice it per variant."""
from __future__ import annotations

import asyncio
import json
import logging
import queue as _queue
import time
from datetime import date

import pandas as pd

from momentum.backtest import (
    BacktestConfig,
    _generate_rebalance_dates,
    build_shared_backtest_inputs,
    prepare_variant_from_shared,
    run_backtest,
    run_multi_trial_backtest,
)
from momentum.signals import PRICE_SIGNAL_DEFS

from .benchmarks import fetch_benchmark_price_index


def _emit(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"


def _keepalive() -> str:
    return ": keepalive\n\n"


async def run_variants_sweep(
    req,
    prices_df: pd.DataFrame,
    prices_local_df: pd.DataFrame,
    volumes_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    monthly_eligible: dict[str, dict[int, str | None]] | None,
    company_currency: dict[int, str | None],
    universe_snapshot: list[dict],
    *,
    # When the orchestrator pre-loaded multiple (universe, grouping)
    # combos for a cross-product sweep, these carry the per-combo
    # eligible dict and the union (used for the shared signal panel).
    # Legacy callers omit them and the loop falls back to `monthly_eligible`
    # for every variant — same behavior as before the cross-product feature.
    monthly_eligible_by_combo: dict[tuple, dict] | None = None,
    union_monthly_eligible: dict | None = None,
):
    """Async generator: drive the variants sweep loop, yielding SSE
    events. Returns nothing — the orchestrator only needs the streamed
    events."""
    # Pre-build the sweep-shared inputs ONCE: price/volume indices
    # plus a single signal panel covering the *union* of every
    # variant's cutoff dates. Skip the precompute when
    # `selection_mode == 'all'` since the all-universe path doesn't
    # consult signals.
    shared_backtest = None
    if req.selection_mode != "all":
        _start_d = date.fromisoformat(req.start_date)
        _end_d = date.fromisoformat(req.end_date)
        _union_cutoffs: set[date] = set()
        for _v in req.variants:
            try:
                _periods = _generate_rebalance_dates(
                    _start_d, _end_d, _v.frequency, prices_df,
                )
            except Exception:
                # If a variant can't even produce dates, skip it
                # here — the per-variant loop below will surface
                # the error properly.
                continue
            if len(_periods) >= 2:
                # Include every rebalance date (not just periods[:-1])
                # — the last entry becomes the open-period entry in
                # run_backtest and needs signals at that cutoff too.
                _union_cutoffs.update(_periods)
        if _union_cutoffs:
            yield _emit({
                "type": "progress",
                "pct": 68,
                "message": f"Precomputing signal panel over {len(_union_cutoffs)} union cutoffs (shared by all {len(req.variants)} variants)...",
            })
            yield _keepalive()
            # The shared signal panel covers the UNION of every variant's
            # eligible companies — same idea as the union the orchestrator
            # used to filter `universe_df`. If no per-combo dict was passed
            # in (legacy single-universe sweep), fall back to the request's
            # `monthly_eligible`.
            _shared_me = union_monthly_eligible if union_monthly_eligible else monthly_eligible
            shared_backtest = await asyncio.to_thread(
                build_shared_backtest_inputs,
                prices_df=prices_df,
                universe_df=universe_df,
                volumes_df=volumes_df,
                prices_local_df=prices_local_df,
                monthly_eligible=_shared_me,
                union_cutoffs=sorted(_union_cutoffs),
            )

    # Sector-ETF mode: prefetch benchmark prices once for the whole
    # sweep so every variant shares them.
    variant_benchmark_price_index: dict[int, pd.Series] | None = None
    variant_benchmark_meta: dict[int, tuple[str, str]] | None = None
    if req.selection_mode == "sector_etf" and req.sector_etfs:
        variant_benchmark_price_index, variant_benchmark_meta = await fetch_benchmark_price_index(
            req.sector_etfs
        )

    base_grouping = getattr(req, "grouping", "sector") or "sector"

    for v_idx, vspec in enumerate(req.variants):
        # Effective per-variant dials. `None` on the spec means "inherit
        # base", which keeps legacy 2-axis sweeps (frequency × strategy)
        # behaving identically.
        v_top_sectors = vspec.top_n_sectors if vspec.top_n_sectors is not None else req.top_n_sectors
        v_top_per_sector = vspec.top_n_per_sector if vspec.top_n_per_sector is not None else req.top_n_per_sector
        v_min_score = vspec.min_price_score if vspec.min_price_score is not None else req.min_price_score
        v_universe_label = vspec.universe_label if vspec.universe_label is not None else req.universe_label
        v_index_universe = vspec.index_universe if vspec.index_universe is not None else req.index_universe
        v_grouping_field = vspec.grouping if vspec.grouping is not None else base_grouping

        # Pick the per-combo monthly_eligible if available, else fall back
        # to the shared one (legacy single-universe sweep).
        v_monthly_eligible = monthly_eligible
        if monthly_eligible_by_combo is not None:
            v_combo = (v_universe_label, v_index_universe, v_grouping_field)
            v_monthly_eligible = monthly_eligible_by_combo.get(v_combo, monthly_eligible)

        # Variant key: legacy 2-segment form (`monthly__long_only`) when
        # no axes were overridden, longer form otherwise. The frontend's
        # `parseVariantKey` round-trips both shapes.
        key_parts = [vspec.frequency, vspec.strategy_type]
        if vspec.top_n_sectors is not None:
            key_parts.append(f"s{vspec.top_n_sectors}")
        if vspec.top_n_per_sector is not None:
            key_parts.append(f"p{vspec.top_n_per_sector}")
        if vspec.min_price_score is not None:
            score_str = f"{vspec.min_price_score:g}" if isinstance(vspec.min_price_score, float) else str(vspec.min_price_score)
            key_parts.append(f"m{score_str}")
        if vspec.index_universe is not None:
            key_parts.append(f"u{vspec.index_universe}")
        elif vspec.universe_label is not None:
            key_parts.append(f"u{vspec.universe_label}")
        if vspec.grouping is not None:
            key_parts.append(f"g{vspec.grouping}")
        variant_key = "__".join(key_parts)

        yield _emit({"type": "variant_start", "variant_key": variant_key})
        yield _keepalive()

        # Per-variant config: same base, overridden frequency + strategy.
        # Reject long_short + random/all at the variant level — the same
        # combination check the single-run path applies, but per-row.
        if vspec.strategy_type == "long_short" and req.selection_mode in ("random", "all"):
            yield _emit({
                "type": "variant_error",
                "variant_key": variant_key,
                "message": f"long_short is not supported with selection_mode='{req.selection_mode}'",
            })
            continue

        v_config = BacktestConfig.from_dict({
            "start_date": req.start_date,
            "end_date": req.end_date,
            "signal_weights": req.signal_weights or {s["key"]: s["default_weight"] for s in PRICE_SIGNAL_DEFS},
            "category_weights": req.category_weights,
            "top_n_sectors": v_top_sectors,
            "top_n_per_sector": v_top_per_sector,
            "selection_mode": req.selection_mode,
            "random_seed": req.random_seed,
            "rebalance_frequency": vspec.frequency,
            "strategy_type": vspec.strategy_type,
            "sector_etfs": req.sector_etfs,
            "min_price_score": v_min_score,
        })

        v_progress_queue: _queue.Queue = _queue.Queue()
        v_result_holder: list = []
        v_error_holder: list = []

        def _v_send_event(event_type: str, **kwargs):
            v_progress_queue.put({"type": event_type, **kwargs})

        # Per-variant `_BacktestPrepared` built from the sweep-shared
        # signal panel + indices. `prepared` already carries the variant's
        # frequency, periods, and the filtered panel; `run_backtest`
        # short-circuits its own `_prepare_backtest` call when `prepared`
        # is supplied.
        v_prepared = None
        if shared_backtest is not None:
            try:
                v_prepared = prepare_variant_from_shared(
                    shared=shared_backtest,
                    start_date=date.fromisoformat(req.start_date),
                    end_date=date.fromisoformat(req.end_date),
                    frequency=vspec.frequency,
                    prices_df=prices_df,
                )
            except Exception as _prep_err:
                # Fall through to the regular path which will raise the
                # same error inside the variant thread so it surfaces
                # as a per-variant error event.
                v_prepared = None
                logging.getLogger(__name__).debug(
                    "[variants] prepare_variant_from_shared failed for %s: %s",
                    variant_key, _prep_err,
                )

        def _v_run(cfg=v_config, prepared=v_prepared, me=v_monthly_eligible):
            try:
                if req.selection_mode == "random" and req.n_trials > 1:
                    r = run_multi_trial_backtest(
                        cfg, prices_df, universe_df, req.n_trials, _v_send_event,
                        volumes_df=volumes_df,
                        monthly_eligible=me,
                        prices_local_df=prices_local_df,
                        company_currency=company_currency,
                    )
                else:
                    r = run_backtest(
                        cfg, prices_df, universe_df, _v_send_event,
                        volumes_df=volumes_df,
                        monthly_eligible=me,
                        prices_local_df=prices_local_df,
                        company_currency=company_currency,
                        prepared=prepared,
                        benchmark_price_index=variant_benchmark_price_index,
                        benchmark_meta=variant_benchmark_meta,
                    )
                v_result_holder.append(r)
            except Exception as e:
                v_error_holder.append(e)
            finally:
                v_progress_queue.put(None)

        yield _emit({
            "type": "progress",
            "pct": 68 + round((v_idx / max(1, len(req.variants))) * 32),
            "message": f"[{variant_key}] running backtest computation ({v_idx + 1}/{len(req.variants)})...",
        })
        yield _keepalive()

        loop = asyncio.get_event_loop()
        loop.run_in_executor(None, _v_run)

        # Same keepalive-15s pattern as the single-run path so the proxy
        # doesn't kill the connection during long signal computation
        # phases.
        v_last_yield = time.monotonic()
        while True:
            try:
                evt = await asyncio.to_thread(v_progress_queue.get, timeout=0.2)
            except Exception:
                if time.monotonic() - v_last_yield >= 15.0:
                    yield _keepalive()
                    v_last_yield = time.monotonic()
                continue
            if evt is None:
                break
            if evt["type"] == "progress":
                # Scale this variant's internal pct (0-100) into the
                # overall sweep progress: each variant owns a 32/N slice
                # of the [68, 100] band, and within the slice we advance
                # proportionally to the variant's own progress event.
                # Without this the bar was locked at the variant's start
                # pct throughout its run, then frozen at
                # `68 + ((N-1)/N)*32` after the last variant finished —
                # e.g. 84% for N=8.
                local_pct = float(evt.get("pct") or 0)
                sweep_fraction = (v_idx + max(0.0, min(100.0, local_pct)) / 100.0) / max(1, len(req.variants))
                yield _emit({
                    "type": "progress",
                    "pct": 68 + round(sweep_fraction * 32),
                    "message": f"[{variant_key}] {evt.get('message', '')}",
                })
            elif evt["type"] == "warning":
                yield _emit({
                    "type": "warning",
                    "scope": evt.get("scope", "backtest"),
                    "message": f"[{variant_key}] {evt.get('message', '')}",
                })
            v_last_yield = time.monotonic()

        if v_error_holder:
            yield _emit({
                "type": "variant_error",
                "variant_key": variant_key,
                "message": f"{type(v_error_holder[0]).__name__}: {v_error_holder[0]}",
            })
            continue

        v_result_dict = v_result_holder[0].to_dict()
        yield _emit({
            "type": "variant_result",
            "variant_key": variant_key,
            "data": v_result_dict,
            "universe": universe_snapshot,
        })

    # Belt-and-suspenders 100% emit: even if the final variant didn't
    # deliver a closing progress event (e.g. it errored, or was skipped
    # because long_short+random is forbidden), the user-facing progress
    # bar lands at 100 before `done`.
    yield _emit({"type": "progress", "pct": 100, "message": f"Variants sweep complete ({len(req.variants)})"})
    yield _emit({"type": "done", "message": f"Variants sweep complete ({len(req.variants)})"})
