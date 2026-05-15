"""Single-run path: backtest / multi-trial random / current_portfolio.

Runs one BacktestConfig against the loaded data. Dispatches between the
three engine entry points based on `mode` + `selection_mode` + `n_trials`,
threads progress events through a queue with a 15s keepalive, and
persists results (current_portfolio: snapshot + per-day rows; backtest:
replay cache) before emitting the terminal `done` event."""
from __future__ import annotations

import asyncio
import json
import queue as _queue
import time

import pandas as pd

from momentum.backtest import (
    BacktestConfig,
    run_backtest,
    run_current_portfolio,
    run_multi_trial_backtest,
)
from momentum.signals import PRICE_SIGNAL_DEFS

from .._helpers import (
    backtest_strategy_hash as _backtest_strategy_hash,
    fetch_daily_picks_history as _fetch_daily_picks_history,
    persist_daily_picks as _persist_daily_picks,
    save_backtest_cache as _save_backtest_cache,
    save_current_picks_snapshot as _save_current_picks_snapshot,
    strategy_hash as _strategy_hash,
)
from .benchmarks import fetch_benchmark_price_index


def _emit(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"


def _keepalive() -> str:
    return ": keepalive\n\n"


async def run_single(
    req,
    prices_df: pd.DataFrame,
    prices_local_df: pd.DataFrame,
    volumes_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    monthly_eligible: dict[str, dict[int, str | None]] | None,
    company_currency: dict[int, str | None],
    universe_snapshot: list[dict],
):
    """Async generator: runs one backtest (single, multi-trial random, or
    current_portfolio), yields SSE progress events, and emits the
    terminal `result` / `current_portfolio` + `done` events. Persists
    snapshots / cache after a successful run."""
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

    # Sector-ETF mode: prefetch benchmark prices once before launching
    # the backtest. Skipped (returns (None, None)) when not in
    # sector_etf mode.
    benchmark_price_index, benchmark_meta = await fetch_benchmark_price_index(
        req.sector_etfs if req.selection_mode == "sector_etf" else None
    )

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
