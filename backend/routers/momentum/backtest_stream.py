"""Backtest SSE stream — the single biggest endpoint by far.

`POST /api/momentum/backtest` dispatches into one of three flows:
  - current_portfolio mode (today's pick + per-day MTD history)
  - variants sweep (one universe load, N variant configs streamed
    separately)
  - single-run backtest (optionally multi-trial random aggregation)

Cache short-circuits live at the top: same strategy + same UTC day →
serve from cache. Replays cost nothing; force_recompute=true (the
Recompute button) bypasses.

Kept in one file because the SSE generator function is intricate enough
(progress events interleaved with cache lookups, eligibility checks,
self-heal, and benchmark-price prefetches) that splitting it across
files just trades one big function for several big imports.

The helper functions used here live in `_helpers.py` and are imported
under their old underscore-prefixed names so the body code stays as it
was in main.py.
"""

from __future__ import annotations

import asyncio
import json
import logging
import queue as _queue
import time
import traceback
from datetime import date, datetime, timedelta
from typing import Literal

import pandas as pd
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from deps import supabase
from ingest.prices import (
    PriceResult,
    _ensure_bucket,
    _fetch_price_from_api,
    _parse_price_series,
    ensure_prices_for_company,
    ensure_volume_for_company,
)
from momentum.backtest import (
    BacktestConfig,
    _generate_rebalance_dates,
    build_shared_backtest_inputs,
    prepare_variant_from_shared,
    run_backtest,
    run_current_portfolio,
    run_multi_trial_backtest,
)
from momentum.data import (
    convert_prices_to_eur,
    load_all_prices,
    load_all_volumes,
    load_company_currency,
    load_fx_rates,
    load_universe,
    self_heal_missing_data,
    sync_fx_rates_to_db,
)
from momentum.signals import PRICE_SIGNAL_DEFS

# Helpers — imported under their old underscore-prefixed names so the
# extracted body code below references them the same way as in main.py.
from ._helpers import (
    backtest_strategy_hash as _backtest_strategy_hash,
    fetch_daily_picks_history as _fetch_daily_picks_history,
    find_cached_backtest as _find_cached_backtest,
    find_cached_snapshot as _find_cached_snapshot,
    latest_db_price_date as _latest_db_price_date,
    persist_daily_picks as _persist_daily_picks,
    save_backtest_cache as _save_backtest_cache,
    save_current_picks_snapshot as _save_current_picks_snapshot,
    strategy_hash as _strategy_hash,
)

router = APIRouter(tags=["momentum"])


_DEFAULT_END = "2026-01-01"
_DEFAULT_START = "2017-01-01"


class VariantSpec(BaseModel):
    frequency: Literal[
        "daily", "weekly", "monthly",
        "every_2_months", "every_3_months", "every_4_months", "every_5_months",
        "every_6_months", "every_7_months", "every_8_months", "every_9_months",
        "every_10_months", "every_11_months", "every_12_months",
    ]
    strategy_type: Literal["long_only", "long_short"]


class BacktestRequest(BaseModel):
    start_date: str = _DEFAULT_START
    end_date: str = _DEFAULT_END  # also used as data cutoff — no data newer than this
    signal_weights: dict[str, float] | None = None
    category_weights: dict[str, float] | None = None  # e.g. {"price": 50, "volume": 50}
    top_n_sectors: int = 4
    top_n_per_sector: int = 6
    max_companies: int = 0  # 0 = all, otherwise limit universe (alphabetical)
    universe_label: str | None = None  # if set, use universe_membership for per-month filtering
    index_universe: str | None = None  # if set, use universe_membership for per-month filtering (e.g. "SP500")
    # Literal values reject typos at the request boundary so a misspelled
    # value never silently routes through a default branch downstream
    # (e.g. an unknown `mode` quietly behaving like "backtest"). New
    # variants need to be added here AND wherever the value is consumed.
    selection_mode: Literal["momentum", "random", "all", "sector_etf"] = "momentum"
    random_seed: int | None = None  # only used when selection_mode == "random"
    n_trials: int = 1  # >1 only valid with selection_mode=="random"; aggregates mean ± std
    # Required when selection_mode == "sector_etf": maps sector name → benchmark_id.
    # The strategy ranks sectors via stock-aggregate momentum then holds the
    # mapped ETF for each picked sector (one per sector). Reuses /benchmarks
    # data for ETF prices; only benchmarks with a non-null `sector` tag are
    # eligible.
    sector_etfs: dict[str, int] | None = None
    mode: Literal["backtest", "current_portfolio"] = "backtest"
    force_recompute: bool = False  # ignore cached result and recompute (applies to backtest + current_portfolio)
    # When true (the default for the user-facing buttons), the compute uses
    # only data already in the DB — no GuruFocus / ECB API calls to fill in
    # gaps. The cron and the explicit "Recompute" button override this so
    # they can refresh stale data.
    db_only: bool = True
    rebalance_frequency: Literal[
        "daily", "weekly", "monthly",
        "every_2_months", "every_3_months", "every_4_months", "every_5_months",
        "every_6_months", "every_7_months", "every_8_months", "every_9_months",
        "every_10_months", "every_11_months", "every_12_months",
    ] = "monthly"
    strategy_type: Literal["long_only", "long_short"] = "long_only"
    # When set (non-empty), the request becomes a variants sweep: the data
    # pipeline (universe load → ensure → bulk-load prices/volumes → FX) runs
    # ONCE, then the backtest computation runs per variant against the same
    # in-memory frames. Each variant emits its own `variant_start` /
    # `variant_result` / `variant_error` events identified by a key of
    # `{frequency}__{strategy_type}`. Sweeps are backtest-only; combining
    # `variants` with `mode="current_portfolio"` is rejected.
    variants: list[VariantSpec] | None = None


async def _momentum_backtest_stream(req: BacktestRequest):
    """SSE generator for the momentum backtest."""
    def _emit(data: dict) -> str:
        return f"data: {json.dumps(data)}\n\n"

    def _keepalive() -> str:
        return ": keepalive\n\n"

    # Variants sweep is backtest-only and not cached as a bundle. Per-variant
    # results are streamed individually; if the user wants caching they can
    # save the bundle from the UI.
    if req.variants and req.mode == "current_portfolio":
        yield _emit({"type": "error", "message": "Variants sweep is not supported with mode='current_portfolio'"})
        return

    # current_portfolio mode runs against today only; coerce the date range
    # so price loading covers ~14 months of history (12m momentum + buffer)
    # without requiring the caller to pick the right window.
    if req.mode == "current_portfolio":
        from datetime import timedelta as _td
        _today = date.today()
        req.start_date = (_today - _td(days=14 * 31)).isoformat()
        req.end_date = _today.isoformat()

        # Cache hit short-circuit. Same strategy clicked twice in the same
        # month → serve the stored snapshot, no recompute. Recompute button
        # passes force_recompute=True to bypass.
        if not req.force_recompute:
            try:
                hash_ = _strategy_hash(req)
                month_start = date(_today.year, _today.month, 1).isoformat()
                cached = await asyncio.to_thread(_find_cached_snapshot, hash_, month_start)
                if cached:
                    history = await asyncio.to_thread(_fetch_daily_picks_history, hash_)
                    payload = {
                        "snapshot_id": cached.get("snapshot_id"),
                        "as_of_date": cached.get("as_of_date"),
                        "latest_price_date": cached.get("latest_price_date"),
                        "holdings": cached.get("holdings") or [],
                        "daily_picks": cached.get("daily_picks") or [],
                        "daily_picks_history": history,
                        "strategy_hash": hash_,
                        "from_cache": True,
                    }
                    yield _emit({"type": "progress", "pct": 100, "message": "Loaded cached current picks"})
                    yield _emit({"type": "current_portfolio", "data": payload, "universe": []})
                    yield _emit({"type": "done", "message": "Served from cache"})
                    return
            except Exception as e:
                # Cache lookup failed — fall through to a fresh compute and
                # surface the issue as a non-fatal warning.
                yield _emit({"type": "warning", "scope": "cache", "message": f"Cache lookup failed: {type(e).__name__}: {e}"})

    # Backtest replay cache. Same config + same UTC day → return the stored
    # payload instead of re-loading prices, re-running signals. Bypassed by
    # force_recompute=true. Skipped entirely for variants sweeps — the
    # per-variant results are streamed and not cached as a bundle. The
    # data_date column on backtest_cache scopes validity to today;
    # tomorrow's first run misses naturally.
    if req.mode != "current_portfolio" and not req.force_recompute and not req.variants:
        try:
            bt_hash = _backtest_strategy_hash(req)
            cached_bt = await asyncio.to_thread(_find_cached_backtest, bt_hash)
            if cached_bt:
                cached_payload = cached_bt.get("payload") or {}
                yield _emit({"type": "progress", "pct": 100, "message": "Loaded cached backtest result"})
                yield _emit({
                    "type": "result",
                    "data": cached_payload.get("result"),
                    "universe": cached_payload.get("universe", []),
                    "from_cache": True,
                    "strategy_hash": bt_hash,
                })
                yield _emit({"type": "done", "message": "Served from cache"})
                return
        except Exception as e:
            yield _emit({"type": "warning", "scope": "cache", "message": f"Backtest cache lookup failed: {type(e).__name__}: {e}"})

    try:
        yield _emit({"type": "progress", "pct": 0, "message": "Loading universe..."})
        universe_df = await asyncio.to_thread(load_universe, supabase)
        if universe_df.empty:
            yield _emit({"type": "error", "message": "No companies found in database"})
            return
        yield _emit({"type": "progress", "pct": 5, "message": f"Found {len(universe_df)} companies"})

        # Pre-flight DB-staleness check. A heavy compute against data that's
        # too old to support the requested window is a waste — surface it
        # before we spin up the fetch loop / load gigabytes of prices.
        # current_portfolio: needs at least one trade ON OR AFTER the start
        # of the current month (otherwise we can't price the entry leg).
        # backtest: only a soft warning — the loop truncates to whatever
        # data exists, but the user should know the requested window won't
        # be honoured in full.
        latest_price_date = await asyncio.to_thread(_latest_db_price_date)
        if latest_price_date is None:
            yield _emit({"type": "error", "message": "DB has no price data — run an ingest first"})
            return
        if req.mode == "current_portfolio":
            _today = date.today()
            month_start = date(_today.year, _today.month, 1)
            if latest_price_date < month_start:
                lag_days = (_today - latest_price_date).days
                yield _emit({
                    "type": "error",
                    "message": (
                        f"Cannot compute current picks for {month_start.isoformat()[:7]}: "
                        f"latest price in DB is {latest_price_date.isoformat()} "
                        f"({lag_days} days behind today). "
                        f"Use 'Recompute' to fetch fresh data, or run an ingest first."
                    ),
                })
                return
        else:
            req_end = date.fromisoformat(req.end_date)
            if latest_price_date < req_end:
                yield _emit({
                    "type": "warning",
                    "scope": "data",
                    "message": (
                        f"Backtest end is {req_end.isoformat()} but DB only has prices "
                        f"through {latest_price_date.isoformat()} — the run will truncate."
                    ),
                })

        # Load universe membership if a label is specified
        monthly_eligible: dict[str, set[int]] | None = None
        if req.universe_label:
            yield _emit({"type": "progress", "pct": 6, "message": f"Loading universe '{req.universe_label}'..."})

            def _load_universe_membership():
                # Get universe_id
                u_resp = supabase.table("universe").select("universe_id").eq("label", req.universe_label).limit(1).execute()
                if not u_resp.data:
                    return {}
                universe_id = u_resp.data[0]["universe_id"]
                rows = []
                offset = 0
                page_size = 1000
                while True:
                    resp = supabase.table("universe_membership").select(
                        "target_month, company_id, sector"
                    ).eq("universe_id", universe_id).order(
                        "target_month"
                    ).order("company_id").range(offset, offset + page_size - 1).execute()
                    batch = resp.data or []
                    rows.extend(batch)
                    if len(batch) < page_size:
                        break
                    offset += page_size
                result: dict[str, dict[int, str | None]] = {}
                for r in rows:
                    # Normalize to "YYYY-MM" — the backtest loop keys on
                    # month_date.isoformat()[:7] so any stored "YYYY-MM-DD"
                    # value (e.g. from an older longequity_cumulative build)
                    # would otherwise never match.
                    m = (r.get("target_month") or "")[:7]
                    if not m:
                        continue
                    if m not in result:
                        result[m] = {}
                    result[m][r["company_id"]] = r.get("sector")
                return result

            monthly_eligible = await asyncio.to_thread(_load_universe_membership)
            n_months = len(monthly_eligible)
            if n_months == 0:
                yield _emit({"type": "error", "message": f"No universe data for label '{req.universe_label}'"})
                return
            avg_pass = sum(len(v) for v in monthly_eligible.values()) // n_months
            yield _emit({"type": "progress", "pct": 7, "message": f"Universe: {n_months} months, ~{avg_pass} companies/month"})

            # Diagnose missing sector data: if no membership row has a sector
            # value, sector-based selection will silently pick zero companies.
            # Fail loudly so the user knows to re-save the universe.
            total_sec = sum(
                1 for month_map in monthly_eligible.values()
                for s in month_map.values() if s
            )
            if total_sec == 0:
                yield _emit({"type": "error", "message": f"Universe '{req.universe_label}' has no sector data in universe_membership — re-save this universe from its source page so sectors are populated."})
                return

        # Load index universe if specified (e.g. SP500 — stored as a universe label)
        if req.index_universe and monthly_eligible is None:
            yield _emit({"type": "progress", "pct": 6, "message": f"Loading index universe '{req.index_universe}'..."})

            def _load_index_universe():
                # Index universes are now stored as regular universes
                u_resp = supabase.table("universe").select("universe_id").eq("label", req.index_universe).limit(1).execute()
                if not u_resp.data:
                    return {}
                universe_id = u_resp.data[0]["universe_id"]
                rows: list[dict] = []
                offset = 0
                page_size = 1000
                while True:
                    resp = (
                        supabase.table("universe_membership")
                        .select("target_month, company_id, sector")
                        .eq("universe_id", universe_id)
                        .order("target_month")
                        .range(offset, offset + page_size - 1)
                        .execute()
                    )
                    batch = resp.data or []
                    rows.extend(batch)
                    if len(batch) < page_size:
                        break
                    offset += page_size
                result: dict[str, dict[int, str | None]] = {}
                for r in rows:
                    # Normalize to "YYYY-MM" — the backtest loop keys on
                    # month_date.isoformat()[:7] so any stored "YYYY-MM-DD"
                    # value (e.g. from an older longequity_cumulative build)
                    # would otherwise never match.
                    m = (r.get("target_month") or "")[:7]
                    if not m:
                        continue
                    if m not in result:
                        result[m] = {}
                    result[m][r["company_id"]] = r.get("sector")
                return result

            monthly_eligible = await asyncio.to_thread(_load_index_universe)
            n_months = len(monthly_eligible)
            if n_months == 0:
                yield _emit({"type": "error", "message": f"No index universe data for '{req.index_universe}'"})
                return
            avg_co = sum(len(v) for v in monthly_eligible.values()) // n_months
            yield _emit({"type": "progress", "pct": 7, "message": f"Index universe: {n_months} months, ~{avg_co} companies/month"})

            total_sec = sum(
                1 for month_map in monthly_eligible.values()
                for s in month_map.values() if s
            )
            if total_sec == 0:
                yield _emit({"type": "error", "message": f"Index universe '{req.index_universe}' has no sector data — re-save this universe from its source page so sectors are populated."})
                return

        # Also fail cleanly when no universe was selected at all — the
        # scoring pipeline requires per-company sectors, and `load_universe`
        # leaves them all None in that fallback.
        if monthly_eligible is None and (req.top_n_sectors or 0) > 0:
            yield _emit({"type": "error", "message": "No universe selected. Sector-based selection requires a universe (or index universe) with stored sector data."})
            return

        config = BacktestConfig.from_dict({
            "start_date": req.start_date,
            "end_date": req.end_date,
            "signal_weights": req.signal_weights or {s["key"]: s["default_weight"] for s in PRICE_SIGNAL_DEFS},
            "category_weights": req.category_weights,
            "top_n_sectors": req.top_n_sectors,
            "top_n_per_sector": req.top_n_per_sector,
            "selection_mode": req.selection_mode,
            "random_seed": req.random_seed,
            "rebalance_frequency": req.rebalance_frequency,
            "strategy_type": req.strategy_type,
            "sector_etfs": req.sector_etfs,
        })

        data_cutoff = date.fromisoformat(req.end_date)

        excluded_ids: set[int] = set()

        # When a universe / index_universe is selected, drop every company
        # that doesn't appear in any month of that universe. Otherwise
        # price+volume gets fetched for unrelated companies (LongEquity-only
        # adds, manual /companies entries, members of other saved universes)
        # that the scoring pipeline would discard anyway, wasting GuruFocus
        # API calls and wall-time. The filter is the union across months —
        # per-month membership filtering still runs at scoring time.
        if monthly_eligible is not None:
            eligible_ids: set[int] = set()
            for month_map in monthly_eligible.values():
                eligible_ids.update(month_map.keys())
            before = len(universe_df)
            universe_df = universe_df[universe_df["company_id"].isin(eligible_ids)].reset_index(drop=True)
            dropped = before - len(universe_df)
            if dropped:
                yield _emit({"type": "progress", "pct": 8, "message": f"Trimmed {dropped} companies not in selected universe ({len(universe_df)} remaining)"})

        # If max_companies is set, pre-trim the universe alphabetically so we
        # only fetch what we need. (Parallel fetch makes the old "stop at
        # ok_count" optimization hard to preserve.)
        if req.max_companies > 0 and len(universe_df) > req.max_companies:
            universe_df = universe_df.sort_values("gurufocus_ticker").head(req.max_companies).reset_index(drop=True)

        total_companies = len(universe_df)
        concurrency = int(os.environ.get("BACKTEST_FETCH_CONCURRENCY", "16"))
        blocked_exchanges: set[str] = set()
        skipped_count = 0
        ok_count = 0
        fetch_start_ts = time.monotonic()

        # Companies whose pass-1 ensure call hit a transient API failure
        # (source == "stale_cache" for either price or volume): the DB has
        # older data and the live API call couldn't refresh it. The audit
        # below uses this set as the self-heal retry list instead of
        # re-deriving "stale" from the bulk-loaded frame — the per-company
        # pass-1 outcome is the authoritative signal of "tried and failed".
        pass1_transient: set[int] = set()

        # In db_only mode (the default for the user-facing buttons) we
        # bypass the per-company API ensure-loop and just consume whatever
        # is already in the DB. The pre-flight staleness check above has
        # already errored if the DB isn't current enough; missing-data
        # filtering happens later in signals.py via the 30-day staleness
        # guard. This skips the bucket warmup, the executor, fetch_one,
        # blocked-exchange detection, and delisted-company pruning.
        if req.db_only:
            yield _emit({"type": "progress", "pct": 60, "message": f"DB-only mode: skipping API fetches for {total_companies} companies"})
        else:
            # Warm the storage bucket once before launching tasks — otherwise the
            # first N workers would race and each fire a bucket-create HTTP call.
            await asyncio.to_thread(_ensure_bucket, supabase)

            # Each company task submits 2 blocking HTTP calls in parallel (price +
            # volume), so the executor needs 2 slots per concurrent task or the
            # second call queues behind the first and inflates wall-clock timings.
            from concurrent.futures import ThreadPoolExecutor
            pool_size = concurrency * 2 + 4
            executor = ThreadPoolExecutor(max_workers=pool_size, thread_name_prefix="fetch")
            loop = asyncio.get_event_loop()

            yield _emit({"type": "progress", "pct": 5, "message": f"Ensuring price & volume data for {total_companies} companies (cutoff: {data_cutoff}, concurrency: {concurrency}, pool: {pool_size})..."})
            yield _keepalive()

            result_queue: asyncio.Queue = asyncio.Queue()
            sema = asyncio.Semaphore(concurrency)
            inflight = {"count": 0, "peak": 0}

            async def _fetch_one(row_cid: int, row_ticker: str, row_exchange: str):
                sym = f"{row_exchange}:{row_ticker}"
                if row_exchange in blocked_exchanges:
                    await result_queue.put({"cid": row_cid, "symbol": sym, "exchange": row_exchange, "status": "skipped_blocked"})
                    return
                async with sema:
                    if row_exchange in blocked_exchanges:
                        await result_queue.put({"cid": row_cid, "symbol": sym, "exchange": row_exchange, "status": "skipped_blocked"})
                        return
                    inflight["count"] += 1
                    if inflight["count"] > inflight["peak"]:
                        inflight["peak"] = inflight["count"]
                    task_start = time.monotonic()
                    try:
                        # Run price + volume concurrently inside one company task
                        pr_fut = loop.run_in_executor(
                            executor,
                            functools.partial(
                                ensure_prices_for_company,
                                supabase, row_cid, row_ticker, row_exchange,
                                data_cutoff=data_cutoff,
                            ),
                        )
                        vr_fut = loop.run_in_executor(
                            executor,
                            functools.partial(
                                ensure_volume_for_company,
                                supabase, row_cid, row_ticker, row_exchange,
                                data_cutoff=data_cutoff,
                            ),
                        )
                        pr_res, vr_res = await asyncio.gather(pr_fut, vr_fut, return_exceptions=True)
                        if isinstance(pr_res, BaseException):
                            raise pr_res
                        pr = pr_res
                        if isinstance(vr_res, BaseException):
                            vr = PriceResult()
                            vr.source = "error"
                            vr.error = str(vr_res)
                        else:
                            vr = vr_res
                        elapsed_ms = int((time.monotonic() - task_start) * 1000)
                        await result_queue.put({"cid": row_cid, "symbol": sym, "exchange": row_exchange, "pr": pr, "vr": vr, "status": "ok", "ms": elapsed_ms})
                    except Exception as e:
                        elapsed_ms = int((time.monotonic() - task_start) * 1000)
                        await result_queue.put({"cid": row_cid, "symbol": sym, "exchange": row_exchange, "error": f"{type(e).__name__}: {e}", "status": "error", "ms": elapsed_ms})
                    finally:
                        inflight["count"] -= 1

            tasks = [
                asyncio.create_task(_fetch_one(
                    int(row["company_id"]),
                    row["gurufocus_ticker"],
                    row["gurufocus_exchange"] or "UNKNOWN",
                ))
                for _, row in universe_df.iterrows()
            ]

            async def _sentinel():
                await asyncio.gather(*tasks, return_exceptions=True)
                await result_queue.put(None)

            sentinel_task = asyncio.create_task(_sentinel())

            done_count = 0
            try:
                while True:
                    evt = await result_queue.get()
                    if evt is None:
                        break
                    done_count += 1
                    pct = 5 + round((done_count / max(1, total_companies)) * 55)
                    status = evt["status"]
                    cid = evt["cid"]
                    symbol = evt["symbol"]
                    exchange = evt["exchange"]

                    if status == "skipped_blocked":
                        skipped_count += 1
                        excluded_ids.add(cid)
                        continue
                    if status == "error":
                        excluded_ids.add(cid)
                        yield _emit({"type": "warning", "scope": "fetch", "symbol": symbol, "message": f"{symbol}: fetch failed — {evt['error']}"})
                        continue

                    pr = evt["pr"]
                    vr = evt["vr"]

                    if pr.is_forbidden:
                        blocked_exchanges.add(exchange)
                        excluded_ids.add(cid)
                        yield _emit({"type": "progress", "pct": pct, "message": f"  {symbol}: unsubscribed region — future {exchange} calls will be skipped"})
                    elif pr.is_delisted:
                        excluded_ids.add(cid)
                        await asyncio.to_thread(
                            lambda c=cid: (
                                supabase.table("metric_data").delete().eq("company_id", c).execute(),
                                supabase.table("portfolio_weight").delete().eq("company_id", c).execute(),
                                supabase.table("company").delete().eq("company_id", c).execute(),
                            )
                        )
                        yield _emit({"type": "progress", "pct": pct, "message": f"  {symbol}: DELISTED — removed from database"})
                    else:
                        ok_count += 1
                        if pr.source == "stale_cache" or (vr and vr.source == "stale_cache"):
                            pass1_transient.add(cid)
                        parts: list[str] = []
                        if pr.source == "cache":
                            parts.append(f"price: cache ({pr.rows_loaded})")
                        elif pr.source == "api":
                            parts.append(f"price: API ({pr.rows_loaded})")
                        elif pr.source == "stale_cache":
                            parts.append(f"price: stale cache ({pr.rows_loaded})")
                        elif pr.source == "none":
                            parts.append("price: none")
                        else:
                            parts.append(f"price: {pr.source}")
                        if vr:
                            if vr.source == "cache":
                                parts.append(f"vol: cache ({vr.rows_loaded})")
                            elif vr.source == "api":
                                parts.append(f"vol: API ({vr.rows_loaded})")
                            elif vr.source == "stale_cache":
                                parts.append(f"vol: stale cache ({vr.rows_loaded})")
                            elif vr.source == "error":
                                parts.append(f"vol: error ({vr.error})")
                            else:
                                parts.append(f"vol: none ({vr.error or 'unknown'})")
                        else:
                            parts.append("vol: failed")
                        ms = evt.get("ms", 0)
                        peak = inflight["peak"]
                        yield _emit({"type": "progress", "pct": pct, "message": f"  {symbol} ({done_count}/{total_companies}, {ms}ms, peak:{peak}): {' | '.join(parts)}"})
            finally:
                # Make sure the sentinel task completes before we leave this block
                try:
                    await sentinel_task
                except Exception:
                    pass
                executor.shutdown(wait=False)

            total_elapsed = time.monotonic() - fetch_start_ts
            yield _emit({"type": "progress", "pct": 60, "message": f"Fetch complete in {total_elapsed:.1f}s (peak concurrency: {inflight['peak']}/{concurrency})"})

            if blocked_exchanges:
                yield _emit({"type": "warning", "scope": "fetch", "message": f"Blocked exchanges (unsubscribed): {', '.join(sorted(blocked_exchanges))} — {skipped_count} companies skipped"})

            # Remove excluded companies (blocked exchanges, delisted) from universe
            if excluded_ids:
                universe_df = universe_df[~universe_df["company_id"].isin(excluded_ids)].reset_index(drop=True)
                yield _emit({"type": "progress", "pct": 61, "message": f"Universe after exclusions: {len(universe_df)} companies"})

            # Optionally limit universe size (alphabetical by ticker) — applied after exclusions
            if req.max_companies > 0 and len(universe_df) > req.max_companies:
                universe_df = universe_df.sort_values("gurufocus_ticker").head(req.max_companies).reset_index(drop=True)
                yield _emit({"type": "progress", "pct": 61, "message": f"Limited to {len(universe_df)} companies (alphabetical)"})

        company_ids = universe_df["company_id"].tolist()

        # Load all prices in bulk — capped at data_cutoff
        from datetime import timedelta
        price_start = date.fromisoformat(req.start_date) - timedelta(days=300)
        price_end = date.fromisoformat(req.end_date) + timedelta(days=35)

        yield _emit({"type": "progress", "pct": 62, "message": f"Loading prices from DB ({price_start} to {price_end}, starts early for 200-day MA)..."})
        yield _keepalive()

        # Stream load progress in real time. The loader runs chunks in
        # parallel from worker threads; each thread calls on_progress as it
        # finishes a page. We push those into a queue and drain it from the
        # async generator while awaiting the load task.
        import queue as _queue
        prices_progress_q: _queue.Queue = _queue.Queue()

        def _on_prices_progress(rows_so_far: int, page_num: int, chunks_done: int = 0, chunks_total: int = 0):
            prices_progress_q.put({
                "rows": rows_so_far,
                "page": page_num,
                "chunks_done": chunks_done,
                "chunks_total": chunks_total,
            })

        prices_task = asyncio.create_task(asyncio.to_thread(
            load_all_prices, supabase, company_ids, price_start, price_end,
            on_progress=_on_prices_progress,
        ))

        # Throttle: emit at most every PROGRESS_THROTTLE pages so the SSE
        # stream isn't drowned in updates on very large loads. Percentage is
        # based on chunks-completed (each chunk is a fixed-size company batch
        # — exact denominator known up front), not row count (unknown total).
        PROGRESS_THROTTLE = 25
        last_emitted_page = 0
        def _fmt_progress(p: dict) -> str:
            ct = p.get("chunks_total", 0)
            cd = p.get("chunks_done", 0)
            pct_str = f" ≈ {round(cd / ct * 100)}%" if ct else ""
            return f"  Loaded {p['rows']:,} price rows ({cd}/{ct} chunks{pct_str})..."

        while not prices_task.done():
            drained = []
            while True:
                try:
                    drained.append(prices_progress_q.get_nowait())
                except _queue.Empty:
                    break
            if drained:
                latest = drained[-1]
                if latest["page"] - last_emitted_page >= PROGRESS_THROTTLE:
                    last_emitted_page = latest["page"]
                    yield _emit({"type": "progress", "pct": 63, "message": _fmt_progress(latest)})
            await asyncio.sleep(0.1)
        # Final drain after task completion
        final_total = None
        while True:
            try:
                final_total = prices_progress_q.get_nowait()
            except _queue.Empty:
                break
        if final_total is not None and final_total["page"] != last_emitted_page:
            yield _emit({"type": "progress", "pct": 64, "message": _fmt_progress(final_total)})

        prices_df = await prices_task

        if prices_df.empty:
            yield _emit({"type": "error", "message": "No price data found after ingestion."})
            return

        n_companies_with_prices = prices_df["company_id"].nunique()
        yield _emit({"type": "progress", "pct": 65, "message": f"Loaded {len(prices_df):,} prices for {n_companies_with_prices} companies"})

        # ------------------------------------------------------------------ #
        # FX conversion: convert local-currency prices to EUR so signals and
        # returns are expressed in a single currency for a EUR-based investor.
        # Momentum ratios are scale-invariant so signals are unaffected, but
        # forward returns change with FX drift (e.g. JPY weakness vs EUR).
        # ------------------------------------------------------------------ #
        yield _emit({"type": "progress", "pct": 65, "message": "Resolving trading currency per company..."})
        yield _keepalive()
        company_currency = await asyncio.to_thread(
            load_company_currency, supabase, company_ids,
        )
        currencies_needed = sorted({c for c in company_currency.values() if c})
        yield _emit({"type": "progress", "pct": 65, "message": f"Found {len(currencies_needed)} distinct currencies: {', '.join(currencies_needed)}"})

        # Sync fx_rate table from ECB for every currency in range. This is
        # idempotent and cheap — it only fetches what's missing past the
        # highest existing rate_date per currency. We stream per-currency
        # progress so the user can see the sync isn't stuck.
        # Skipped under db_only — no external ECB calls; rely on whatever
        # FX rows are already in the DB (gaps surface as the existing
        # "no FX history for X" warning during conversion).
        if req.db_only:
            yield _emit({"type": "progress", "pct": 65, "message": "DB-only mode: skipping ECB FX sync, using cached FX rates"})
        else:
            yield _emit({"type": "progress", "pct": 65, "message": f"Syncing FX rates from ECB (through {price_end})..."})
            yield _keepalive()

            fx_progress_q: _queue.Queue = _queue.Queue()
            fx_done = [0]
            fx_total = len(currencies_needed)

            def _on_fx_progress(code: str, status: dict):
                fx_done[0] += 1
                fx_progress_q.put({
                    "code": code,
                    "done": fx_done[0],
                    "total": fx_total,
                    "status": status.get("status"),
                })

            fx_task = asyncio.create_task(asyncio.to_thread(
                sync_fx_rates_to_db, supabase, currencies_needed, price_start, price_end,
                on_progress=_on_fx_progress,
            ))
            while not fx_task.done():
                drained = []
                while True:
                    try:
                        drained.append(fx_progress_q.get_nowait())
                    except _queue.Empty:
                        break
                if drained:
                    latest = drained[-1]
                    pct = round(latest["done"] / max(1, latest["total"]) * 100)
                    yield _emit({
                        "type": "progress",
                        "pct": 65,
                        "message": f"  FX sync {latest['done']}/{latest['total']} ≈ {pct}% (latest: {latest['code']} → {latest['status']})",
                    })
                await asyncio.sleep(0.15)
            fx_sync = await fx_task
            synced_codes = sorted(c for c, s in fx_sync.items() if s.get("status") == "synced")
            cached_codes = sorted(c for c, s in fx_sync.items() if s.get("status") == "cached")
            failed_codes = sorted(c for c, s in fx_sync.items() if s.get("status") == "error")
            nodata_codes = sorted(c for c, s in fx_sync.items() if s.get("status") == "no_data")
            total_rows = sum(s.get("rows", 0) for s in fx_sync.values())
            yield _emit({
                "type": "progress",
                "pct": 65,
                "message": (
                    f"FX sync done: {len(synced_codes)} updated ({total_rows:,} rows), "
                    f"{len(cached_codes)} already current, "
                    f"{len(failed_codes)} failed, {len(nodata_codes)} no_data"
                ),
            })
            if failed_codes:
                for code in failed_codes:
                    err = fx_sync[code].get("error", "unknown")
                    yield _emit({"type": "warning", "scope": "fx", "message": f"FX sync failed for {code}: {err}"})
            if nodata_codes:
                _ccy_names = {
                    "AED": "UAE Dirham", "ARS": "Argentine Peso", "AUD": "Australian Dollar",
                    "BRL": "Brazilian Real", "CAD": "Canadian Dollar", "CHF": "Swiss Franc",
                    "CLP": "Chilean Peso", "CNY": "Chinese Yuan", "COP": "Colombian Peso",
                    "CZK": "Czech Koruna", "DKK": "Danish Krone", "EGP": "Egyptian Pound",
                    "EUR": "Euro", "GBP": "British Pound", "GBX": "British Penny",
                    "HKD": "Hong Kong Dollar", "HUF": "Hungarian Forint", "IDR": "Indonesian Rupiah",
                    "ILS": "Israeli Shekel", "INR": "Indian Rupee", "ISK": "Icelandic Krona",
                    "JPY": "Japanese Yen", "KRW": "South Korean Won", "MXN": "Mexican Peso",
                    "MYR": "Malaysian Ringgit", "NOK": "Norwegian Krone", "NZD": "New Zealand Dollar",
                    "PEN": "Peruvian Sol", "PHP": "Philippine Peso", "PKR": "Pakistani Rupee",
                    "PLN": "Polish Zloty", "QAR": "Qatari Riyal", "RON": "Romanian Leu",
                    "RUB": "Russian Ruble", "SAR": "Saudi Riyal", "SEK": "Swedish Krona",
                    "SGD": "Singapore Dollar", "THB": "Thai Baht", "TRY": "Turkish Lira",
                    "TWD": "Taiwan Dollar", "USD": "US Dollar", "VND": "Vietnamese Dong",
                    "ZAR": "South African Rand",
                }
                labeled = ", ".join(
                    f"{c} ({_ccy_names[c]})" if c in _ccy_names else c for c in nodata_codes
                )
                yield _emit({
                    "type": "warning",
                    "scope": "fx",
                    "message": f"No FX data returned for: {labeled} (ECB may not cover these)",
                })

        yield _emit({"type": "progress", "pct": 65, "message": f"Loading FX rates ({price_start} to {price_end}) for {len(currencies_needed)} currencies..."})
        yield _keepalive()
        fx_rates = await asyncio.to_thread(
            load_fx_rates, supabase, currencies_needed, price_start, price_end,
        )
        loaded_codes = [c for c, s in fx_rates.items() if s is not None and not s.empty]
        missing_codes = sorted(set(currencies_needed) - set(loaded_codes))
        yield _emit({"type": "progress", "pct": 65, "message": f"FX rates loaded for {len(loaded_codes)} currencies"})
        if missing_codes:
            yield _emit({
                "type": "warning",
                "scope": "fx",
                "message": f"No FX history for: {', '.join(missing_codes)} — companies on those currencies will be dropped",
            })

        yield _emit({"type": "progress", "pct": 65, "message": f"Converting {len(prices_df):,} price rows to EUR..."})
        yield _keepalive()
        prices_local_df = prices_df
        prices_df, fx_stats = await asyncio.to_thread(
            convert_prices_to_eur, prices_df, company_currency, fx_rates,
        )
        yield _emit({
            "type": "progress",
            "pct": 65,
            "message": (
                f"FX done: {fx_stats['converted_rows']:,} rows converted "
                f"({', '.join(fx_stats['converted_currencies']) or 'none'}), "
                f"{fx_stats['passthrough_rows']:,} already EUR, "
                f"{fx_stats['dropped_no_currency']:,} dropped (no currency), "
                f"{fx_stats['dropped_no_fx']:,} dropped (no FX rate)"
            ),
        })
        if fx_stats["missing_currencies"]:
            yield _emit({
                "type": "warning",
                "scope": "fx",
                "message": f"Currencies with no FX series in date range: {', '.join(fx_stats['missing_currencies'])}",
            })

        if prices_df.empty:
            yield _emit({"type": "error", "message": "No price data left after FX conversion."})
            return

        # Audit price coverage: flag universe companies with zero or sparse price rows
        _price_counts = prices_df.groupby("company_id").size().to_dict() if not prices_df.empty else {}
        _universe_symbol = {
            int(r["company_id"]): f"{r.get('gurufocus_exchange') or '?'}:{r['gurufocus_ticker']}"
            for _, r in universe_df.iterrows()
        }
        _universe_name = {
            int(r["company_id"]): r.get("company_name") or ""
            for _, r in universe_df.iterrows()
        }
        def _label(cid: int) -> str:
            sym = _universe_symbol.get(int(cid), str(cid))
            name = _universe_name.get(int(cid), "")
            return f"{sym} ({name})" if name else sym
        _no_price = [cid for cid in company_ids if _price_counts.get(int(cid), 0) == 0]
        _sparse_price = [cid for cid in company_ids if 0 < _price_counts.get(int(cid), 0) < 20]

        # Group no-price companies by exchange. An exchange where every
        # universe company has zero price rows is almost certainly
        # unsubscribed on GuruFocus (or fully blocked) — surface it
        # separately from one-off gaps so the user can tell the difference.
        _universe_exchange = {
            int(r["company_id"]): r.get("gurufocus_exchange") or "UNKNOWN"
            for _, r in universe_df.iterrows()
        }
        _exchange_totals: dict[str, int] = {}
        _exchange_no_price: dict[str, int] = {}
        for cid in company_ids:
            exch = _universe_exchange.get(int(cid), "UNKNOWN")
            _exchange_totals[exch] = _exchange_totals.get(exch, 0) + 1
            if _price_counts.get(int(cid), 0) == 0:
                _exchange_no_price[exch] = _exchange_no_price.get(exch, 0) + 1
        _unsubscribed_exchanges = sorted(
            exch for exch, no_price in _exchange_no_price.items()
            if _exchange_totals.get(exch, 0) > 0 and no_price == _exchange_totals[exch]
        )
        if _unsubscribed_exchanges:
            parts = [f"{exch}({_exchange_no_price[exch]})" for exch in _unsubscribed_exchanges]
            total_unsub = sum(_exchange_no_price[e] for e in _unsubscribed_exchanges)
            yield _emit({
                "type": "info",
                "scope": "prices",
                "message": f"Unsubscribed/blocked exchanges (expected to have no data): {', '.join(parts)} — {total_unsub} companies",
            })

        # Remaining no-price cases: exchanges where some companies have
        # data but specific tickers don't — true one-off gaps.
        _no_price_gap = [
            cid for cid in _no_price
            if _universe_exchange.get(int(cid), "UNKNOWN") not in _unsubscribed_exchanges
        ]
        if _no_price_gap:
            sample = ", ".join(_label(int(c)) for c in _no_price_gap[:10])
            more = f" (+{len(_no_price_gap) - 10} more)" if len(_no_price_gap) > 10 else ""
            yield _emit({"type": "warning", "scope": "prices", "message": f"{len(_no_price_gap)} companies on subscribed exchanges have NO price data: {sample}{more}"})
        if _sparse_price:
            sample = ", ".join(
                f"{_label(int(c))}[{_price_counts.get(int(c), 0)} rows]" for c in _sparse_price[:10]
            )
            more = f" (+{len(_sparse_price) - 10} more)" if len(_sparse_price) > 10 else ""
            yield _emit({"type": "warning", "scope": "prices", "message": f"{len(_sparse_price)} companies have < 20 price rows (insufficient for signals): {sample}{more}"})

        # Load volumes from DB — same parallel-load + streamed-progress pattern
        # as prices.
        yield _emit({"type": "progress", "pct": 66, "message": "Loading volumes from DB..."})
        yield _keepalive()

        volumes_progress_q: _queue.Queue = _queue.Queue()

        def _on_volumes_progress(rows_so_far: int, page_num: int, chunks_done: int = 0, chunks_total: int = 0):
            volumes_progress_q.put({
                "rows": rows_so_far,
                "page": page_num,
                "chunks_done": chunks_done,
                "chunks_total": chunks_total,
            })

        volumes_task = asyncio.create_task(asyncio.to_thread(
            load_all_volumes, supabase, company_ids, price_start, price_end,
            on_progress=_on_volumes_progress,
        ))

        def _fmt_v_progress(p: dict) -> str:
            ct = p.get("chunks_total", 0)
            cd = p.get("chunks_done", 0)
            pct_str = f" ≈ {round(cd / ct * 100)}%" if ct else ""
            return f"  Loaded {p['rows']:,} volume rows ({cd}/{ct} chunks{pct_str})..."

        last_emitted_vpage = 0
        while not volumes_task.done():
            drained = []
            while True:
                try:
                    drained.append(volumes_progress_q.get_nowait())
                except _queue.Empty:
                    break
            if drained:
                latest = drained[-1]
                if latest["page"] - last_emitted_vpage >= PROGRESS_THROTTLE:
                    last_emitted_vpage = latest["page"]
                    yield _emit({"type": "progress", "pct": 66, "message": _fmt_v_progress(latest)})
            await asyncio.sleep(0.1)
        final_v = None
        while True:
            try:
                final_v = volumes_progress_q.get_nowait()
            except _queue.Empty:
                break
        if final_v is not None and final_v["page"] != last_emitted_vpage:
            yield _emit({"type": "progress", "pct": 67, "message": _fmt_v_progress(final_v)})

        volumes_df = await volumes_task
        n_vol = volumes_df["company_id"].nunique() if not volumes_df.empty else 0
        yield _emit({"type": "progress", "pct": 67, "message": f"Loaded {len(volumes_df):,} volume records for {n_vol} companies"})

        # Audit volume coverage. Companies on unsubscribed exchanges (already
        # flagged in the prices info message) are expected to have no volume
        # either, so filter them out of the warning set to avoid noise.
        _vol_counts = volumes_df.groupby("company_id").size().to_dict() if not volumes_df.empty else {}
        _no_vol_all = [cid for cid in company_ids if _vol_counts.get(int(cid), 0) == 0]
        _sparse_vol = [cid for cid in company_ids if 0 < _vol_counts.get(int(cid), 0) < 20]
        _no_vol_gap = [
            cid for cid in _no_vol_all
            if _universe_exchange.get(int(cid), "UNKNOWN") not in _unsubscribed_exchanges
        ]
        if _no_vol_gap:
            sample = ", ".join(_label(int(c)) for c in _no_vol_gap[:10])
            more = f" (+{len(_no_vol_gap) - 10} more)" if len(_no_vol_gap) > 10 else ""
            yield _emit({"type": "warning", "scope": "volumes", "message": f"{len(_no_vol_gap)} companies on subscribed exchanges have NO volume data — volume signals will be skipped for them: {sample}{more}"})
        if _sparse_vol:
            sample = ", ".join(
                f"{_label(int(c))}[{_vol_counts.get(int(c), 0)} rows]" for c in _sparse_vol[:10]
            )
            more = f" (+{len(_sparse_vol) - 10} more)" if len(_sparse_vol) > 10 else ""
            yield _emit({"type": "warning", "scope": "volumes", "message": f"{len(_sparse_vol)} companies have < 20 volume rows: {sample}{more}"})

        # Self-heal: for any subscribed-exchange company missing prices or
        # volumes, re-run the ingest pipeline (cache check → API fetch → DB
        # load) and merge the recovered rows back into the in-memory frames.
        # This is a no-op in steady state — the gap sets are empty when
        # everything is already loaded — and only fires for genuinely missing
        # data (empty Storage JSONs, failed prior loads, new tickers, etc.).
        #
        # When the user explicitly asks for fresh data on a current_portfolio
        # run (force_recompute=True), also retry every company whose pass-1
        # ensure call hit a transient API failure (source == "stale_cache").
        # This is the per-company pass-1 outcome, not a re-derivation of
        # "stale" from the bulk frame — pass-1 already runs the same
        # is_daily_data_fresh predicate, so a successful "api" outcome cannot
        # produce a stale row in the audit. Only transient API failures need
        # a second attempt.
        _stale_cids: set[int] = set()
        if req.mode == "current_portfolio" and req.force_recompute:
            company_id_set = {int(c) for c in company_ids}
            _stale_cids = {
                cid for cid in pass1_transient
                if cid in company_id_set
                and _universe_exchange.get(cid, "UNKNOWN") not in _unsubscribed_exchanges
            }
            if _stale_cids:
                yield _emit({
                    "type": "info",
                    "scope": "self-heal",
                    "message": f"Force-recompute: {len(_stale_cids)} companies hit transient API errors during pass 1 — retrying.",
                })

        gap_cids = sorted(set(_no_price_gap) | set(_no_vol_gap) | _stale_cids)
        if req.db_only and gap_cids:
            # Don't refetch under db_only — surface the gap as a non-fatal
            # warning so the user knows some companies will be filtered out
            # of the universe by the staleness guard in signals.py.
            sample = ", ".join(_label(int(c)) for c in gap_cids[:8])
            more = f" (+{len(gap_cids) - 8} more)" if len(gap_cids) > 8 else ""
            yield _emit({
                "type": "warning",
                "scope": "data",
                "message": f"DB-only mode: {len(gap_cids)} companies have missing price/volume data and will be excluded from this run: {sample}{more}",
            })
            gap_cids = []
        if gap_cids:
            yield _emit({"type": "progress", "pct": 67, "message": f"Self-heal: refetching missing data for {len(gap_cids)} companies on subscribed exchanges..."})
            yield _keepalive()

            ticker_lookup = {
                int(r["company_id"]): str(r["gurufocus_ticker"])
                for _, r in universe_df.iterrows()
            }
            exchange_lookup = {
                int(r["company_id"]): str(r.get("gurufocus_exchange") or "")
                for _, r in universe_df.iterrows()
            }

            heal_progress_q: _queue.Queue = _queue.Queue()

            def _on_heal_progress(cid, status, msg):
                heal_progress_q.put({"cid": cid, "status": status, "msg": msg})

            heal_task = asyncio.create_task(asyncio.to_thread(
                self_heal_missing_data,
                supabase, gap_cids, ticker_lookup, exchange_lookup,
                on_progress=_on_heal_progress,
            ))

            done_count = 0
            while not heal_task.done():
                drained = []
                while True:
                    try:
                        drained.append(heal_progress_q.get_nowait())
                    except _queue.Empty:
                        break
                if drained:
                    done_count += len(drained)
                    yield _emit({"type": "progress", "pct": 67, "message": f"  Self-heal: {done_count}/{len(gap_cids)} companies processed..."})
                await asyncio.sleep(0.2)

            heal_result = await heal_task
            healed_cids = heal_result["healed_company_ids"]
            heal_stats = heal_result["stats"]

            heal_msg_parts = [
                f"{heal_stats['prices_fetched']} price fetches",
                f"{heal_stats['volumes_fetched']} volume fetches",
            ]
            if heal_stats["forbidden_exchanges"]:
                heal_msg_parts.append(f"forbidden exchanges (skipped): {', '.join(heal_stats['forbidden_exchanges'])}")
            if heal_stats["errors"]:
                heal_msg_parts.append(f"{heal_stats['errors']} errors")
            yield _emit({
                "type": "info",
                "scope": "self-heal",
                "message": f"Self-heal complete: {' · '.join(heal_msg_parts)}.",
            })

            if healed_cids:
                yield _emit({"type": "progress", "pct": 67, "message": f"Re-loading {len(healed_cids)} healed companies into memory..."})
                yield _keepalive()
                new_local = await asyncio.to_thread(
                    load_all_prices, supabase, healed_cids, price_start, price_end,
                )
                if not new_local.empty:
                    new_eur, _new_fx = await asyncio.to_thread(
                        convert_prices_to_eur, new_local, company_currency, fx_rates,
                    )
                    prices_local_df = pd.concat(
                        [prices_local_df, new_local], ignore_index=True
                    ).sort_values(["company_id", "target_date"]).reset_index(drop=True)
                    if not new_eur.empty:
                        prices_df = pd.concat(
                            [prices_df, new_eur], ignore_index=True
                        ).sort_values(["company_id", "target_date"]).reset_index(drop=True)
                new_volumes = await asyncio.to_thread(
                    load_all_volumes, supabase, healed_cids, price_start, price_end,
                )
                if not new_volumes.empty:
                    volumes_df = pd.concat(
                        [volumes_df, new_volumes], ignore_index=True
                    ).sort_values(["company_id", "target_date"]).reset_index(drop=True)

        # Build universe snapshot once — used by both single-run and variants
        # paths. Variants reuse this for every per-variant `variant_result`
        # event so the client gets the same shape it would from a single run.
        # `_norm_str` handles None / NaN explicitly: pandas Series .get("col",
        # default) only falls through to the default when the COLUMN is
        # missing, not when the cell is None or NaN. Without this normalization
        # an exchange link that's absent in the DB ends up as the literal
        # string "None" or "nan" in the JSON payload, which (a) breaks the
        # frontend's GuruFocus URL helper (US-vs-non-US classifier sees
        # "None" as non-US and produces "/stock/None:TICKER/summary") and
        # (b) renders "(None)" or "(nan)" in the holdings table.
        def _norm_str(val) -> str:
            if val is None:
                return ""
            try:
                if pd.isna(val):
                    return ""
            except (TypeError, ValueError):
                pass
            return str(val)

        universe_snapshot = [
            {
                "company_id": int(row["company_id"]),
                "ticker": _norm_str(row.get("gurufocus_ticker")),
                "exchange": _norm_str(row.get("gurufocus_exchange")),
                "company_name": _norm_str(row.get("company_name")),
                "sector": _norm_str(row.get("sector")),
                "country": _norm_str(row.get("country")),
            }
            for _, row in universe_df.iterrows()
        ]

        # ── Variants sweep path ─────────────────────────────────────────────
        # All data is loaded; iterate variants, running just the backtest
        # computation per (frequency × strategy_type). Each variant emits its
        # own `variant_start` / `variant_result` / `variant_error` event so
        # the frontend can update the variants table row-by-row. The data
        # frames (prices_df, prices_local_df, volumes_df, fx_rates,
        # company_currency, monthly_eligible, universe_df) are reused
        # verbatim — frequency / strategy_type only affect the rebalance
        # date generator and portfolio construction, not the underlying
        # data, so this is the cheap loop the sweep should always have been.
        if req.variants:
            # Pre-build the sweep-shared inputs ONCE: price/volume indices
            # plus a single signal panel covering the *union* of every
            # variant's cutoff dates. The per-company rolling signal scan
            # is the dominant cost of `_prepare_backtest`, and it's
            # identical across every variant in the sweep — running it
            # N times for N variants was wasting ~(N-1) full passes over
            # the universe's price history. Skip the precompute when
            # `selection_mode == 'all'` since the all-universe path
            # doesn't consult signals.
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
                    shared_backtest = await asyncio.to_thread(
                        build_shared_backtest_inputs,
                        prices_df=prices_df,
                        universe_df=universe_df,
                        volumes_df=volumes_df,
                        prices_local_df=prices_local_df,
                        monthly_eligible=monthly_eligible,
                        union_cutoffs=sorted(_union_cutoffs),
                    )

            # Sector-ETF mode: prefetch benchmark prices once for the whole
            # sweep so every variant shares them (same shape as the
            # single-run path above; pulled out to its own block so the
            # variant loop can reference the names).
            variant_benchmark_price_index: dict[int, pd.Series] | None = None
            variant_benchmark_meta: dict[int, tuple[str, str]] | None = None
            if req.selection_mode == "sector_etf" and req.sector_etfs:
                _bm_ids = sorted({int(v) for v in req.sector_etfs.values()})
                if _bm_ids:
                    _meta_resp = await asyncio.to_thread(
                        lambda: supabase.table("benchmark")
                        .select("benchmark_id, ticker, name")
                        .in_("benchmark_id", _bm_ids)
                        .execute()
                    )
                    variant_benchmark_meta = {
                        int(r["benchmark_id"]): (r["ticker"], r["name"])
                        for r in (_meta_resp.data or [])
                    }
                    # Paginate per benchmark_id to bypass Supabase's silent
                    # 1000-row default (see single-run path for the full
                    # story). Without this, all variants share an empty
                    # post-1999 price series and emit 0% returns.
                    _px_rows: list[dict] = []
                    _page_size = 1000
                    for _bid in _bm_ids:
                        _offset = 0
                        while True:
                            _px_resp = await asyncio.to_thread(
                                lambda b=_bid, o=_offset: supabase.table("benchmark_price")
                                .select("benchmark_id, target_date, price")
                                .eq("benchmark_id", b)
                                .order("target_date")
                                .range(o, o + _page_size - 1)
                                .execute()
                            )
                            _batch = _px_resp.data or []
                            _px_rows.extend(_batch)
                            if len(_batch) < _page_size:
                                break
                            _offset += _page_size
                    variant_benchmark_price_index = {}
                    if _px_rows:
                        _df_bm = pd.DataFrame(_px_rows)
                        for _bid, _group in _df_bm.groupby("benchmark_id"):
                            variant_benchmark_price_index[int(_bid)] = pd.Series(
                                _group["price"].values,
                                index=pd.DatetimeIndex(_group["target_date"]),
                                dtype="float64",
                            ).sort_index()

            for v_idx, vspec in enumerate(req.variants):
                variant_key = f"{vspec.frequency}__{vspec.strategy_type}"
                yield _emit({"type": "variant_start", "variant_key": variant_key})
                yield _keepalive()

                # Per-variant config: same base, overridden frequency + strategy.
                # Reject long_short + random/all at the variant level —
                # the same combination check the single-run path applies,
                # but per-row.
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
                    "top_n_sectors": req.top_n_sectors,
                    "top_n_per_sector": req.top_n_per_sector,
                    "selection_mode": req.selection_mode,
                    "random_seed": req.random_seed,
                    "rebalance_frequency": vspec.frequency,
                    "strategy_type": vspec.strategy_type,
                    "sector_etfs": req.sector_etfs,
                })

                v_progress_queue: _queue.Queue = _queue.Queue()
                v_result_holder: list = []
                v_error_holder: list = []

                def _v_send_event(event_type: str, **kwargs):
                    v_progress_queue.put({"type": event_type, **kwargs})

                # Per-variant `_BacktestPrepared` built from the
                # sweep-shared signal panel + indices. `prepared` already
                # carries the variant's frequency, periods, and the
                # filtered panel; `run_backtest` short-circuits its own
                # `_prepare_backtest` call when `prepared` is supplied.
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
                        # Fall through to the regular path which will
                        # raise the same error inside the variant thread
                        # so it surfaces as a per-variant error event.
                        v_prepared = None
                        logging.getLogger(__name__).debug(
                            "[variants] prepare_variant_from_shared failed for %s: %s",
                            variant_key, _prep_err,
                        )

                def _v_run(cfg=v_config, prepared=v_prepared):
                    try:
                        if req.selection_mode == "random" and req.n_trials > 1:
                            # Multi-trial random repeats `run_backtest`
                            # under the hood; it builds its own prepared.
                            # The shared panel still helps if/when we
                            # wire it through here, but for now we just
                            # leave the per-trial path to use its own
                            # cache (which already shares prepared
                            # across trials of the SAME variant).
                            r = run_multi_trial_backtest(
                                cfg, prices_df, universe_df, req.n_trials, _v_send_event,
                                volumes_df=volumes_df,
                                monthly_eligible=monthly_eligible,
                                prices_local_df=prices_local_df,
                                company_currency=company_currency,
                            )
                        else:
                            r = run_backtest(
                                cfg, prices_df, universe_df, _v_send_event,
                                volumes_df=volumes_df,
                                monthly_eligible=monthly_eligible,
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

                # Same keepalive-15s pattern as the single-run path so the
                # proxy doesn't kill the connection during long signal
                # computation phases.
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
                        # Scale this variant's internal pct (0-100) into
                        # the overall sweep progress: each variant owns a
                        # 32/N slice of the [68, 100] band, and within
                        # the slice we advance proportionally to the
                        # variant's own progress event. Without this the
                        # bar was locked at the variant's start pct
                        # throughout its run, then frozen at
                        # `68 + ((N-1)/N)*32` after the last variant
                        # finished — e.g. 84% for N=8.
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

            # Belt-and-suspenders 100% emit: even if the final variant
            # didn't deliver a closing progress event (e.g. it errored,
            # or was skipped because long_short+random is forbidden),
            # the user-facing progress bar lands at 100 before `done`.
            yield _emit({"type": "progress", "pct": 100, "message": f"Variants sweep complete ({len(req.variants)})"})
            yield _emit({"type": "done", "message": f"Variants sweep complete ({len(req.variants)})"})
            return

        # ── Single-run path ─────────────────────────────────────────────────
        # When selection_mode == "sector_etf", pre-fetch the prices for every
        # mapped benchmark ETF once and pass them through to run_backtest /
        # run_current_portfolio. We avoid the (cheap) fetch when the mode
        # doesn't need it.
        benchmark_price_index: dict[int, pd.Series] | None = None
        benchmark_meta: dict[int, tuple[str, str]] | None = None
        if req.selection_mode == "sector_etf" and req.sector_etfs:
            bm_ids = sorted({int(v) for v in req.sector_etfs.values()})
            if bm_ids:
                meta_resp = await asyncio.to_thread(
                    lambda: supabase.table("benchmark")
                    .select("benchmark_id, ticker, name")
                    .in_("benchmark_id", bm_ids)
                    .execute()
                )
                benchmark_meta = {
                    int(r["benchmark_id"]): (r["ticker"], r["name"])
                    for r in (meta_resp.data or [])
                }
                # Pull every price row per benchmark, paginating to defeat
                # Supabase's silent 1000-row limit. A single ETF since
                # 1998 has ~6,886 daily bars and 11 ETFs together exceed
                # 75k rows — a single .in_() query would truncate to the
                # earliest ~90 days per benchmark, and every entry/exit
                # lookup downstream would return None.
                px_rows: list[dict] = []
                page_size = 1000
                for bid in bm_ids:
                    offset = 0
                    while True:
                        px_resp = await asyncio.to_thread(
                            lambda b=bid, o=offset: supabase.table("benchmark_price")
                            .select("benchmark_id, target_date, price")
                            .eq("benchmark_id", b)
                            .order("target_date")
                            .range(o, o + page_size - 1)
                            .execute()
                        )
                        batch = px_resp.data or []
                        px_rows.extend(batch)
                        if len(batch) < page_size:
                            break
                        offset += page_size
                benchmark_price_index = {}
                # Build per-benchmark pd.Series. Same shape as `price_index`
                # so run_backtest's price-lookup helpers (_price_on_or_after,
                # _date_on_or_after) work without modification.
                if px_rows:
                    df_bm = pd.DataFrame(px_rows)
                    for bid, group in df_bm.groupby("benchmark_id"):
                        benchmark_price_index[int(bid)] = pd.Series(
                            group["price"].values,
                            index=pd.DatetimeIndex(group["target_date"]),
                            dtype="float64",
                        ).sort_index()

        # Run backtest with progress callback via queue for real-time streaming
        progress_queue: _queue.Queue = _queue.Queue()
        backtest_result_holder: list = []
        backtest_error_holder: list = []

        def send_event(event_type: str, **kwargs):
            progress_queue.put({"type": event_type, **kwargs})

        def _run_backtest():
            try:
                if req.mode == "current_portfolio":
                    r = run_current_portfolio(
                        config, prices_df, universe_df, send_event,
                        volumes_df=volumes_df,
                        monthly_eligible=monthly_eligible,
                        prices_local_df=prices_local_df,
                        company_currency=company_currency,
                    )
                elif req.selection_mode == "random" and req.n_trials > 1:
                    r = run_multi_trial_backtest(
                        config, prices_df, universe_df, req.n_trials, send_event,
                        volumes_df=volumes_df,
                        monthly_eligible=monthly_eligible,
                        prices_local_df=prices_local_df,
                        company_currency=company_currency,
                    )
                else:
                    r = run_backtest(config, prices_df, universe_df, send_event,
                        volumes_df=volumes_df,
                        monthly_eligible=monthly_eligible,
                        prices_local_df=prices_local_df,
                        company_currency=company_currency,
                        benchmark_price_index=benchmark_price_index,
                        benchmark_meta=benchmark_meta,
                    )
                backtest_result_holder.append(r)
            except Exception as e:
                backtest_error_holder.append(e)
            finally:
                progress_queue.put(None)  # sentinel

        yield _emit({"type": "progress", "pct": 68, "message": "Running backtest computation..."})
        yield _keepalive()

        loop = asyncio.get_event_loop()
        loop.run_in_executor(None, _run_backtest)

        # Stream progress events in real-time as the backtest runs. Emit a
        # keepalive comment every ~15s of silence so the proxy doesn't close
        # the connection during long signal-computation steps that produce no
        # visible events (current_portfolio on a wide universe can sit silent
        # for >30s between emissions).
        last_yield = time.monotonic()
        keepalive_interval = 15.0
        while True:
            try:
                evt = await asyncio.to_thread(progress_queue.get, timeout=0.2)
            except Exception:
                if time.monotonic() - last_yield >= keepalive_interval:
                    yield _keepalive()
                    last_yield = time.monotonic()
                continue
            if evt is None:
                break
            if evt["type"] == "progress":
                scaled_pct = 68 + round(evt.get("pct", 0) * 0.32)
                yield _emit({"type": "progress", "pct": scaled_pct, "message": evt.get("message", "")})
            elif evt["type"] == "warning":
                yield _emit({"type": "warning", "scope": evt.get("scope", "backtest"), "message": evt.get("message", "")})
            last_yield = time.monotonic()

        if backtest_error_holder:
            raise backtest_error_holder[0]
        result = backtest_result_holder[0]

        # universe_snapshot was built above (shared with the variants path).

        if req.mode == "current_portfolio":
            payload = result.to_dict()
            hash_ = _strategy_hash(req)
            payload["strategy_hash"] = hash_
            cfg_dump = req.model_dump()
            # Persist snapshot + per-day rows so subsequent loads are instant.
            # Failures are surfaced as non-fatal warnings; the user still sees
            # the freshly computed result.
            try:
                snapshot_id = await asyncio.to_thread(
                    _save_current_picks_snapshot,
                    payload,
                    cfg_dump,
                    "manual",
                    hash_,
                )
                payload["snapshot_id"] = snapshot_id
            except Exception as e:
                yield _emit({"type": "warning", "scope": "snapshot", "message": f"Could not persist snapshot: {type(e).__name__}: {e}"})
            try:
                await asyncio.to_thread(
                    _persist_daily_picks,
                    hash_,
                    cfg_dump,
                    payload.get("daily_picks") or [],
                )
            except Exception as e:
                yield _emit({"type": "warning", "scope": "daily-picks", "message": f"Could not persist daily picks: {type(e).__name__}: {e}"})
            try:
                payload["daily_picks_history"] = await asyncio.to_thread(_fetch_daily_picks_history, hash_)
            except Exception as e:
                payload["daily_picks_history"] = payload.get("daily_picks") or []
                yield _emit({"type": "warning", "scope": "daily-picks", "message": f"Could not fetch daily picks history: {type(e).__name__}: {e}"})
            yield _emit({"type": "current_portfolio", "data": payload, "universe": universe_snapshot})
            yield _emit({"type": "done", "message": "Current portfolio computed"})
        else:
            result_dict = result.to_dict()
            yield _emit({"type": "result", "data": result_dict, "universe": universe_snapshot})
            # Cache the result for replay. Failures are non-fatal — the user
            # already received their result; we just won't have it cached.
            try:
                await asyncio.to_thread(
                    _save_backtest_cache,
                    _backtest_strategy_hash(req),
                    req.model_dump(),
                    {"result": result_dict, "universe": universe_snapshot},
                )
            except Exception as e:
                yield _emit({"type": "warning", "scope": "cache", "message": f"Could not cache backtest: {type(e).__name__}: {e}"})
            yield _emit({"type": "done", "message": "Backtest complete"})

    except Exception as e:
        yield _emit({"type": "error", "message": str(e)})


@router.post("/api/momentum/backtest")
async def momentum_backtest(req: BacktestRequest):
    return StreamingResponse(
        _momentum_backtest_stream(req),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

