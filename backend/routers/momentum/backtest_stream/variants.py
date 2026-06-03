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
from routers._sse import sse_event as _emit, sse_keepalive as _keepalive
import logging
import os
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

from ..signals import warm_breakdown_panel_cache
from .benchmarks import fetch_benchmark_price_index


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
    # Hoisted up so the precompute pools (Win #6, #7) can use the same
    # worker count as the variant-execution pool below.
    try:
        _env_workers = int(os.getenv("VARIANT_MAX_WORKERS", "4"))
    except ValueError:
        _env_workers = 4

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
                    weekday=req.rebalance_weekday,
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

    # Cadence-independent universe baseline cache. Pure function of
    # (universe combo, window) — the same value for every variant
    # that runs on that combo. Without this cache each variant re-
    # walks the calendar months and re-equal-weights every eligible
    # company; for a 32-variant sweep on 2 universes that's 30
    # redundant computes. Precomputing once per distinct combo here
    # then passing through `run_backtest`'s `monthly_baseline_override`
    # collapses that to 2 computes. The cache key uses the same combo
    # tuple shape as `monthly_eligible_by_combo`.
    monthly_baseline_cache: dict[tuple, dict | None] = {}
    if shared_backtest is not None and monthly_eligible_by_combo:
        from momentum.backtest._period import compute_monthly_universe_baseline  # noqa: PLC0415
        start_d = date.fromisoformat(req.start_date)
        end_d = date.fromisoformat(req.end_date)
        for combo, me in monthly_eligible_by_combo.items():
            try:
                monthly_baseline_cache[combo] = await asyncio.to_thread(
                    compute_monthly_universe_baseline,
                    shared_backtest.price_index, me,
                    start_date=start_d,
                    end_date=end_d,
                )
            except Exception as _bl_err:
                logging.getLogger(__name__).warning(
                    "[variants] monthly-baseline precompute failed for combo=%s: %s",
                    combo, _bl_err,
                )
                monthly_baseline_cache[combo] = None
        n_combos = len([v for v in monthly_baseline_cache.values() if v is not None])
        if n_combos > 1:
            yield _emit({
                "type": "progress",
                "pct": 69,
                "message": f"Precomputed universe baseline for {n_combos} (universe, grouping) combos -> reused across {len(req.variants)} variants.",
            })

    # Per-period universe baseline cache (Win 2). The runner's period
    # loop calls `compute_universe_period_return(signals_df, ...)` once
    # per CLOSED rebalance period — pure function of (eligible cids at
    # that month, prices, entry_ts, exit_ts). Two variants with the
    # same (combo, frequency) have identical rebalance dates AND
    # identical eligible sets per date → identical per-period
    # baselines. Without caching, a 32-variant sweep over 4 distinct
    # (combo, freq) tuples × ~200 periods recomputes the same 800
    # values 8 times each (5600+ wasted calls). Precomputing once per
    # (combo, freq) and passing through `run_backtest`'s
    # `period_baseline_lookup` kwarg eliminates that.
    #
    # The OPEN period stays in-line — its exit (`open_as_of`) depends
    # on the variant's actual holdings (most recent date common to
    # every held company), which isn't knowable until selection has
    # run. One in-line call per variant is cheap and unavoidable.
    period_baseline_cache: dict[tuple, dict[date, tuple[float | None, int]]] = {}
    if shared_backtest is not None and monthly_eligible_by_combo:
        from momentum.backtest._period import compute_universe_period_return  # noqa: PLC0415

        # Win #6: parallelize the per-period inner loop. Each call is
        # read-only against shared frames (panel_df, price_index) and
        # independent of every other period, so a thread pool sized to
        # match the variants pool gives near-linear speedup on the
        # setup phase. The outer (combo, freq) loop stays sequential —
        # parallelizing both axes risks oversubscribing the CPU.
        def _compute_one_period(
            period_date_inner, next_period_inner, panel_df_inner, eligible_ids_inner,
        ):
            if panel_df_inner is None or panel_df_inner.empty or not eligible_ids_inner:
                return period_date_inner, (None, 0)
            signals_filtered = panel_df_inner[
                panel_df_inner["company_id"].isin(eligible_ids_inner)
            ]
            ret, n = compute_universe_period_return(
                signals_filtered, shared_backtest.price_index,
                entry_ts=pd.Timestamp(period_date_inner),
                exit_ts=pd.Timestamp(next_period_inner),
            )
            return period_date_inner, (ret, n)

        seen_combo_freq: set[tuple] = set()
        for _v in req.variants:
            _v_universe_label = _v.universe_label if _v.universe_label is not None else req.universe_label
            _v_index_universe = _v.index_universe if _v.index_universe is not None else req.index_universe
            _v_grouping = _v.grouping if _v.grouping is not None else base_grouping
            _v_combo = (_v_universe_label, _v_index_universe, _v_grouping)
            cf_key = (_v_combo, _v.frequency)
            if cf_key in seen_combo_freq:
                continue
            seen_combo_freq.add(cf_key)
            me = monthly_eligible_by_combo.get(_v_combo)
            if me is None:
                continue
            try:
                periods = _generate_rebalance_dates(
                    date.fromisoformat(req.start_date),
                    date.fromisoformat(req.end_date),
                    _v.frequency,
                    prices_df,
                    weekday=req.rebalance_weekday,
                )
            except Exception as _gen_err:
                logging.getLogger(__name__).warning(
                    "[variants] period-baseline date generation failed for %s: %s",
                    cf_key, _gen_err,
                )
                continue
            if len(periods) < 2:
                continue

            # Serial execution (parallel pool removed — see variant loop
            # comment below for the rationale). Period count is small
            # (~300) and each call is mostly numpy under the hood, so
            # serial finishes in a few seconds without thread overhead.
            # Yield throttled progress so the user sees which (universe,
            # freq, year-month) is being computed live, instead of
            # waiting silently for the summary at the end.
            _combo_label = f"{_v_universe_label or _v_index_universe or '?'} · {_v.frequency} · {_v_grouping}"
            per_period: dict[date, tuple[float | None, int]] = {}
            _last_emit_ts = time.monotonic()
            total_periods = len(periods) - 1
            for i in range(total_periods):
                period_date = periods[i]
                next_period = periods[i + 1]
                panel_df = shared_backtest.union_panel.get(period_date)
                eligible_ids = set(
                    (me.get(period_date.isoformat()[:7]) or {}).keys()
                )
                pd_date, val = _compute_one_period(
                    period_date, next_period, panel_df, eligible_ids,
                )
                per_period[pd_date] = val
                now = time.monotonic()
                if now - _last_emit_ts >= 0.4 or i == total_periods - 1:
                    yield _emit({
                        "type": "progress",
                        "pct": 69,
                        "message": (
                            f"Universe baseline · {_combo_label} · "
                            f"{period_date.isoformat()[:7]} ({i + 1}/{total_periods})"
                        ),
                    })
                    _last_emit_ts = now
            period_baseline_cache[cf_key] = per_period
        n_cf = len(period_baseline_cache)
        if n_cf > 1:
            yield _emit({
                "type": "progress",
                "pct": 69,
                "message": f"Precomputed per-period universe returns for {n_cf} (universe, freq) combos -> reused across {len(req.variants)} variants.",
            })

    # Cross-variant score cache (Win 3). One empty dict per universe
    # combo, lazily populated by the variant loop — the first variant
    # to score a given (combo, period_date) stores the scored frame,
    # subsequent variants on the same combo short-circuit the score
    # pass via `run_backtest`'s `score_cache` kwarg. Cache scope
    # assumes signal_weights + category_weights are constant across
    # all variants in the sweep (true today — the variant spec varies
    # frequency / strategy / universe / grouping / top_n / min_score,
    # not signal weights). Per-combo keying is required because each
    # combo's eligible cid set differs, producing different
    # signals_df at every period_date. Skipped for random / all
    # selection modes (the runner never calls into scoring) and when
    # there's no per-combo split (single-combo sweeps share their
    # `prepared` panel but each variant has unique selection params,
    # so caching still pays — fall back to a single shared dict).
    score_cache_by_combo: dict[tuple, dict] = {}
    score_cache_single: dict = {}  # fallback for non-cross-product sweeps
    if req.selection_mode not in ("random", "all"):
        if monthly_eligible_by_combo is not None:
            for combo in monthly_eligible_by_combo:
                score_cache_by_combo[combo] = {}

    # Win #A: precompute per-period scores upfront, parallelized across
    # periods, instead of letting the first variant warm them lazily.
    # `score_universe` is a pure function of (filtered_panel,
    # signal_weights, category_weights) — those don't vary across
    # variants in a sweep — so the result for `(combo, period_date)` is
    # safe to materialize once and reuse for every variant. Without
    # this, variant 1 pays ~30-60s of per-period scoring; with it,
    # every variant hits warm cache from period 1.
    #
    # Only meaningful for selection modes that call into scoring
    # (i.e. not 'random' / 'all'). Skipped otherwise.
    if (
        shared_backtest is not None
        and req.selection_mode not in ("random", "all")
        and score_cache_by_combo
    ):
        from momentum.scoring import score_universe  # noqa: PLC0415
        _sig_weights = req.signal_weights or {
            s["key"]: s["default_weight"] for s in PRICE_SIGNAL_DEFS
        }
        _cat_weights = req.category_weights

        def _score_one(
            combo_inner, period_date_inner, panel_df_inner,
            eligible_ids_inner, sector_map_inner,
        ):
            if (
                panel_df_inner is None
                or panel_df_inner.empty
                or not eligible_ids_inner
            ):
                return combo_inner, period_date_inner, None
            # Must apply the SAME `.copy()` + sector remap that the
            # runner does at the top of its per-period loop (see
            # `runner.py` ~L262-264). Without this, the precomputed
            # `scored_df` carries the base universe_df's sector
            # (which is `None` for snapshot universes like ACWI ∩
            # Leonteq), and aggregate_to_sector inside select_from_scored
            # silently drops every row because pandas's groupby
            # default `dropna=True` skips NaN-sector groups → empty
            # selection. The runner's lazy cache path produced the
            # correct sectors; the upfront precompute path didn't.
            filtered = panel_df_inner[
                panel_df_inner["company_id"].isin(eligible_ids_inner)
            ].copy()
            if filtered.empty:
                return combo_inner, period_date_inner, None
            filtered["sector"] = filtered["company_id"].map(sector_map_inner)
            scored = score_universe(filtered, _sig_weights, _cat_weights)
            return combo_inner, period_date_inner, scored

        # Build (combo, period_date, panel, eligible, sector_map) tasks
        # once, then run them serially.
        score_tasks: list[tuple] = []
        for combo, me_inner in (monthly_eligible_by_combo or {}).items():
            for cutoff, panel in shared_backtest.union_panel.items():
                if panel is None or panel.empty:
                    continue
                month_key = cutoff.isoformat()[:7]
                sector_map = me_inner.get(month_key) or {}
                if not sector_map:
                    continue
                eligible_ids = set(sector_map.keys())
                score_tasks.append((combo, cutoff, panel, eligible_ids, sector_map))

        if score_tasks:
            yield _emit({
                "type": "progress",
                "pct": 70,
                "message": (
                    f"Precomputing per-period scores ({len(score_tasks)} "
                    f"(combo, period) cells)…"
                ),
            })
            # Serial. Each call is pandas+numpy on a ~1500-row frame;
            # the GIL contention pattern from the previous parallel
            # version cost more in synchronization than it saved.
            # Yield throttled per-period progress so the user can see
            # the year-month being scored live — instead of one summary
            # at the end of a multi-second loop.
            _last_emit_ts = time.monotonic()
            total_tasks = len(score_tasks)
            for i, t in enumerate(score_tasks):
                combo_done, period_done, scored_df = _score_one(*t)
                if scored_df is not None and combo_done in score_cache_by_combo:
                    score_cache_by_combo[combo_done][period_done] = scored_df
                now = time.monotonic()
                if now - _last_emit_ts >= 0.4 or i == total_tasks - 1:
                    _u_label, _ix_label, _g_label = combo_done
                    _combo_short = f"{_u_label or _ix_label or '?'} · {_g_label}"
                    yield _emit({
                        "type": "progress",
                        "pct": 70,
                        "message": (
                            f"Scoring · {_combo_short} · "
                            f"{period_done.isoformat()[:7]} ({i + 1}/{total_tasks})"
                        ),
                    })
                    _last_emit_ts = now
            warmed = sum(len(d) for d in score_cache_by_combo.values())
            yield _emit({
                "type": "progress",
                "pct": 70,
                "message": (
                    f"Score cache warmed: {warmed} (combo, period) cells "
                    f"across {len(score_cache_by_combo)} combo(s) — every "
                    f"variant on these combos skips per-period scoring."
                ),
            })

    # Win #3: per-sweep cache for `make_period_holding`'s price math.
    # Single dict shared by every variant — the cache key is
    # `(cid, entry_ts.value, exit_ts.value)` so cross-combo overlap on
    # the same (cid, entry, exit) triple still hits (price_index is
    # already a single shared structure across all variants — see
    # `build_shared_backtest_inputs`). For a 252-variant sweep on the
    # same (combo, freq) most cids in the selection repeat across
    # variants — each (cid, entry, exit) is computed exactly once.
    price_cache: dict = {}

    # ===== Win 4: parallel variant execution =====
    # Worker threads run independently; each pushes its lifecycle events
    # (variant_start, optional warnings, terminal variant_result OR
    # variant_error) to a single shared queue. The async generator drains
    # the queue and forwards events to SSE, counting terminals to know
    # when the sweep is done. Workers never emit per-variant `progress`
    # events — with N variants running concurrently those would be noise.
    # Instead the drain loop emits one completion-based progress event
    # after each terminal so the user sees N/total advancing in real time.
    #
    # Pandas releases the GIL during heavy numeric ops, so multiple
    # variant threads make real progress in parallel even in CPython.
    # On a 4-vCPU box a 32-variant sweep typically lands ~3x faster.
    #
    # Cache safety: the read-only caches (`monthly_baseline_cache`,
    # `period_baseline_cache`) were fully populated before this point.
    # `score_cache_by_combo[combo]` is shared-mutable across siblings
    # on the same combo — but CPython dict item assignment is atomic
    # per-key, and the worst case is "two threads compute the same
    # (combo, period_date) score before either stores it; the second
    # overwrites the first with byte-identical data." Harmless duplicate
    # compute, not a correctness bug. Skipping a lock keeps the hot path
    # lock-free.
    #
    # Concurrency cap defaults to 4 (env-tunable). 1 → fully sequential
    # if needed for debugging. Capped at len(variants) so we don't spin
    # up idle workers.
    n_variants = len(req.variants)

    sweep_queue: _queue.Queue = _queue.Queue()

    def _run_one_variant(vspec_inner, v_idx_inner):
        """Thread worker: computes one variant end-to-end and pushes
        every lifecycle event for it to `sweep_queue`. Always emits
        exactly one terminal (`variant_result` or `variant_error`)
        even on unexpected exceptions, so the drain loop's terminal-
        counter is reliable."""
        variant_key = "?"  # set below before any early-exit branch
        try:
            # Effective per-variant dials. `None` on the spec means
            # "inherit base", preserving legacy 2-axis sweep behavior.
            v_top_sectors = vspec_inner.top_n_sectors if vspec_inner.top_n_sectors is not None else req.top_n_sectors
            v_top_per_sector = vspec_inner.top_n_per_sector if vspec_inner.top_n_per_sector is not None else req.top_n_per_sector
            v_min_score = vspec_inner.min_price_score if vspec_inner.min_price_score is not None else req.min_price_score
            v_universe_label = vspec_inner.universe_label if vspec_inner.universe_label is not None else req.universe_label
            v_index_universe = vspec_inner.index_universe if vspec_inner.index_universe is not None else req.index_universe
            v_grouping_field = vspec_inner.grouping if vspec_inner.grouping is not None else base_grouping

            # Per-combo cache lookups (Wins 1-3).
            v_monthly_eligible = monthly_eligible
            v_monthly_baseline: dict | None = None
            v_period_baselines: dict[date, tuple[float | None, int]] | None = None
            v_score_cache: dict | None = None
            if monthly_eligible_by_combo is not None:
                v_combo = (v_universe_label, v_index_universe, v_grouping_field)
                v_monthly_eligible = monthly_eligible_by_combo.get(v_combo, monthly_eligible)
                v_monthly_baseline = monthly_baseline_cache.get(v_combo)
                v_period_baselines = period_baseline_cache.get((v_combo, vspec_inner.frequency))
                v_score_cache = score_cache_by_combo.get(v_combo)
            elif req.selection_mode not in ("random", "all"):
                v_score_cache = score_cache_single

            # Variant key.
            key_parts = [vspec_inner.frequency, vspec_inner.strategy_type]
            if vspec_inner.top_n_sectors is not None:
                key_parts.append(f"s{vspec_inner.top_n_sectors}")
            if vspec_inner.top_n_per_sector is not None:
                key_parts.append(f"p{vspec_inner.top_n_per_sector}")
            if vspec_inner.min_price_score is not None:
                score_str = f"{vspec_inner.min_price_score:g}" if isinstance(vspec_inner.min_price_score, float) else str(vspec_inner.min_price_score)
                key_parts.append(f"m{score_str}")
            if vspec_inner.index_universe is not None:
                key_parts.append(f"u{vspec_inner.index_universe}")
            elif vspec_inner.universe_label is not None:
                key_parts.append(f"u{vspec_inner.universe_label}")
            if vspec_inner.grouping is not None:
                key_parts.append(f"g{vspec_inner.grouping}")
            variant_key = "__".join(key_parts)

            sweep_queue.put({"type": "variant_start", "variant_key": variant_key})

            # Same long_short + random/all rejection the sequential path
            # applied — now lives in the worker so it produces a
            # variant_error terminal (instead of `continue`).
            if vspec_inner.strategy_type == "long_short" and req.selection_mode in ("random", "all"):
                sweep_queue.put({
                    "type": "variant_error",
                    "variant_key": variant_key,
                    "message": f"long_short is not supported with selection_mode='{req.selection_mode}'",
                })
                return

            v_config = BacktestConfig.from_dict({
                "start_date": req.start_date,
                "end_date": req.end_date,
                "signal_weights": req.signal_weights or {s["key"]: s["default_weight"] for s in PRICE_SIGNAL_DEFS},
                "category_weights": req.category_weights,
                "top_n_sectors": v_top_sectors,
                "top_n_per_sector": v_top_per_sector,
                "selection_mode": req.selection_mode,
                "random_seed": req.random_seed,
                "rebalance_frequency": vspec_inner.frequency,
                "rebalance_weekday": req.rebalance_weekday,
                "strategy_type": vspec_inner.strategy_type,
                "sector_etfs": req.sector_etfs,
                "min_price_score": v_min_score,
            })

            # Per-variant prepared (slices the shared signal panel for
            # this frequency).
            v_prepared = None
            if shared_backtest is not None:
                try:
                    v_prepared = prepare_variant_from_shared(
                        shared=shared_backtest,
                        start_date=date.fromisoformat(req.start_date),
                        end_date=date.fromisoformat(req.end_date),
                        frequency=vspec_inner.frequency,
                        prices_df=prices_df,
                        rebalance_weekday=req.rebalance_weekday,
                    )
                except Exception as _prep_err:
                    v_prepared = None
                    logging.getLogger(__name__).debug(
                        "[variants] prepare_variant_from_shared failed for %s: %s",
                        variant_key, _prep_err,
                    )

            # Forward warnings (with variant_key prefix); ignore the
            # runner's per-period progress events (too noisy to interleave
            # across N concurrent variants).
            def _worker_send_event(event_type: str, **kwargs):
                if event_type == "warning":
                    sweep_queue.put({
                        "type": "warning",
                        "scope": kwargs.get("scope", "backtest"),
                        "message": f"[{variant_key}] {kwargs.get('message', '')}",
                        "variant_key": variant_key,
                    })

            if req.selection_mode == "random" and req.n_trials > 1:
                r = run_multi_trial_backtest(
                    v_config, prices_df, universe_df, req.n_trials, _worker_send_event,
                    volumes_df=volumes_df,
                    monthly_eligible=v_monthly_eligible,
                    prices_local_df=prices_local_df,
                    company_currency=company_currency,
                    monthly_baseline_override=v_monthly_baseline,
                    period_baseline_lookup=v_period_baselines,
                )
            else:
                r = run_backtest(
                    v_config, prices_df, universe_df, _worker_send_event,
                    volumes_df=volumes_df,
                    monthly_eligible=v_monthly_eligible,
                    prices_local_df=prices_local_df,
                    company_currency=company_currency,
                    prepared=v_prepared,
                    benchmark_price_index=variant_benchmark_price_index,
                    benchmark_meta=variant_benchmark_meta,
                    monthly_baseline_override=v_monthly_baseline,
                    period_baseline_lookup=v_period_baselines,
                    score_cache=v_score_cache,
                    price_cache=price_cache,
                )

            sweep_queue.put({
                "type": "variant_result",
                "variant_key": variant_key,
                "data": r.to_dict(),
                "universe": universe_snapshot,
            })
        except Exception as e:
            # Catch-all so the drain loop's terminal counter still
            # advances if the worker fails ANYWHERE — including before
            # variant_start fired (in which case the UI will see a
            # variant_error for a variant it never saw start, which is
            # fine; the variant_key is still meaningful when key
            # construction succeeded, and a "?" placeholder otherwise).
            #
            # Logged at DEBUG (with traceback) rather than ERROR/EXCEPTION
            # because most failures here are foreseeable user-input issues
            # — e.g. "Need at least 2 rebalance periods" when the date
            # window is too short for the cadence, "no eligible
            # companies" when a universe-month combination is empty —
            # and the variant_error event already carries the user-facing
            # message to the UI's variants table. Devs debugging an
            # unexpected crash can enable DEBUG to see the full trace.
            logging.getLogger(__name__).debug(
                "[variants] worker for variant_key=%s failed",
                variant_key, exc_info=True,
            )
            sweep_queue.put({
                "type": "variant_error",
                "variant_key": variant_key,
                "message": f"{type(e).__name__}: {e}",
            })

    yield _emit({
        "type": "progress",
        "pct": 70,
        "message": f"Running {n_variants} variants sequentially (parallel removed; numpy-bound is faster serial than thread-contended)...",
    })
    yield _keepalive()

    # Serial execution. The earlier ThreadPoolExecutor pattern was
    # contention-bound (wave-of-4 finishers with 8 workers configured)
    # because pandas/numpy operations release the GIL only partially
    # and the shared score_cache/selection_cache/price_cache dicts
    # serialized read access anyway. Going serial:
    #   - removes thread-spawn + context-switch overhead
    #   - eliminates cache lock contention (one writer, one reader)
    #   - makes per-period timings reproducible
    # We still drain via the same queue + drain loop pattern so the
    # event interleaving (variant_start → variant_result, progress)
    # stays identical to the parallel version.
    n_completed = 0
    last_yield = time.monotonic()
    for v_idx, vspec in enumerate(req.variants):
        await asyncio.to_thread(_run_one_variant, vspec, v_idx)
        # Drain whatever the worker put on the queue for this variant.
        while True:
            try:
                evt = sweep_queue.get_nowait()
            except _queue.Empty:
                break
            yield _emit(evt)
            last_yield = time.monotonic()
            if evt["type"] in ("variant_result", "variant_error"):
                n_completed += 1
                yield _emit({
                    "type": "progress",
                    "pct": 70 + round((n_completed / max(1, n_variants)) * 30),
                    "message": f"{n_completed}/{n_variants} variants complete",
                })
        # SSE keepalive every ~15s if a single variant takes a long time.
        if time.monotonic() - last_yield >= 15.0:
            yield _keepalive()
            last_yield = time.monotonic()

    # Post-sweep cache warming for /signal-breakdown. The runner's
    # `panel_warm_callback` hook (used by single_run.py) covers the
    # single-backtest path; for sweeps we batch the work here so we
    # warm ONCE per (combo, cutoff) instead of N times across N
    # variants on the same combo. After this, the user's first
    # breakdown click on any stock in any covered (universe, cutoff)
    # hits the LRU in <500ms instead of paying the 10s universe-load.
    if shared_backtest is not None and shared_backtest.union_panel:
        if monthly_eligible_by_combo:
            # Cross-product sweep: each combo's monthly_eligible defines
            # the per-cutoff cid filter that /signal-breakdown would
            # apply fresh. Slice the union panel accordingly so the
            # cached frames match what the breakdown endpoint computes.
            for combo, me in monthly_eligible_by_combo.items():
                v_universe_label, v_index_universe, _v_grouping = combo
                for cutoff, panel in shared_backtest.union_panel.items():
                    if panel is None or panel.empty:
                        continue
                    month_key = cutoff.isoformat()[:7]
                    sector_map = me.get(month_key) or {}
                    eligible_cids = set(sector_map.keys())
                    if not eligible_cids:
                        continue
                    filtered = panel[panel["company_id"].isin(eligible_cids)].copy()
                    if filtered.empty:
                        continue
                    filtered["sector"] = filtered["company_id"].map(sector_map)
                    warm_breakdown_panel_cache(
                        v_universe_label, v_index_universe, cutoff, filtered,
                    )
        else:
            # Single-combo sweep without cross-product per-month
            # eligibility: warm with the union panel as-is. Matches the
            # non-snapshot universe path the runner takes.
            for cutoff, panel in shared_backtest.union_panel.items():
                if panel is None or panel.empty:
                    continue
                warm_breakdown_panel_cache(
                    req.universe_label, req.index_universe, cutoff, panel,
                )

    # Belt-and-suspenders 100% emit: even if the final variant didn't
    # deliver a closing progress event, the user-facing progress bar
    # lands at 100 before `done`.
    yield _emit({"type": "progress", "pct": 100, "message": f"Variants sweep complete ({len(req.variants)})"})
    yield _emit({"type": "done", "message": f"Variants sweep complete ({len(req.variants)})"})
