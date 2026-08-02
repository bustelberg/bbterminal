"""Single-run path: backtest / multi-trial random / current_portfolio.

Runs one BacktestConfig against the loaded data. Dispatches between the
three engine entry points based on `mode` + `selection_mode` + `n_trials`,
threads progress events through a queue with a 15s keepalive, and
persists results (current_portfolio: snapshot + per-day rows; backtest:
replay cache) before emitting the terminal `done` event."""
from __future__ import annotations

import asyncio
from routers._sse import sse_event as _emit, sse_keepalive as _keepalive
import queue as _queue
import time

import pandas as pd

from momentum.backtest import (
    BacktestConfig,
    run_backtest,
    run_current_portfolio,
    run_multi_trial_backtest,
)
from momentum.scoring import signal_defs_for_mode

from .._helpers import (
    DAILY_HOLDINGS_TAIL_DAYS,
    backtest_strategy_hash as _backtest_strategy_hash,
    fetch_daily_holdings_cache as _fetch_daily_holdings_cache,
    fetch_daily_picks_history as _fetch_daily_picks_history,
    persist_daily_holdings_cache as _persist_daily_holdings_cache,
    persist_daily_picks as _persist_daily_picks,
    save_backtest_cache as _save_backtest_cache,
    save_current_picks_snapshot as _save_current_picks_snapshot,
    strategy_hash as _strategy_hash,
)
from ..signals import warm_breakdown_panel_cache
from .benchmarks import fetch_benchmark_price_index


def _load_cached_selections(hash_: str, start, months_back: int, force: bool):
    """Cached selections as `{date: DataFrame}` for the engine, plus the raw map.

    ⚠ THE MOST RECENT `DAILY_HOLDINGS_TAIL_DAYS` ARE WITHHELD FROM THE CACHE even when
    stored — a late-arriving close can still change a recent day's selection, and a
    cache that never revisits its newest entries is a wrong answer that cannot correct
    itself. Those days are recomputed and re-stored every run; older days are settled.
    """
    import pandas as pd  # noqa: PLC0415
    from datetime import date, timedelta  # noqa: PLC0415

    if force or start is None:
        return {}, {}, {}
    today = date.today()
    raw = _fetch_daily_holdings_cache(hash_, start.isoformat(), today.isoformat())
    # Calendar days, deliberately generous over trading days: erring toward
    # recomputing one extra day costs a few seconds, erring the other way keeps a
    # stale selection on screen.
    cutoff = today - timedelta(days=DAILY_HOLDINGS_TAIL_DAYS * 2)
    # ⚠ A CACHED DAY WITH NO SECTOR SCORES IS STALE, NOT COMPLETE. `sector_scores` was added after
    # the cache shipped, so every day stored by an earlier run carries the column's `'[]'` default.
    # Serving those produces a day whose holdings are right and whose sector ranks are silently
    # empty — measured: 58 of 150 cached days, drawing the rank chart as flat gaps across May, June
    # and most of July while January to April looked fine. Nothing recomputes them on their own:
    # the tail-refresh only reaches the last few days, so a legacy row is served for ever.
    #
    # A day that has holdings ALWAYS has sectors (they come off the same scored frame), so an empty
    # list here can only mean "written before the column existed". Refusing it costs one recompute
    # per legacy day, once, and the cache self-heals.
    usable = {d: v for d, v in raw.items()
              if v.get("holdings") and v.get("sector_scores") and d <= cutoff.isoformat()}
    frames: dict = {}
    sectors: dict = {}
    for d, v in usable.items():
        try:
            dd = date.fromisoformat(d)
        except Exception:  # noqa: BLE001 — a malformed row just costs a recompute
            continue
        frames[dd] = pd.DataFrame(v["holdings"])
        sectors[dd] = v.get("sector_scores") or []
    return frames, sectors, usable


def _selections_to_store(daily_picks: list[dict], already: dict) -> dict[str, dict]:
    """The freshly computed days, in the shape the cache stores.

    ⚠ THE NEWEST DAY IS NOT STORED. Its holdings still have blank exit prices and no
    forward return — the next trading day has not happened — and it is also the day
    most likely to move when a late close lands. Storing it would cache the least
    settled answer in the window.
    """
    if not daily_picks:
        return {}
    newest = max(p.get("date") or "" for p in daily_picks)
    out: dict[str, dict] = {}
    for p in daily_picks:
        d = p.get("date")
        if not d or d in already or d == newest:
            continue
        out[d] = {
            "holdings": [{
                "company_id": h.get("company_id"),
                # The engine rebuilds a day from these column names, so they are the
                # SIGNAL panel's names, not the holding payload's.
                "gurufocus_ticker": h.get("ticker"),
                "company_name": h.get("company_name"),
                "sector": h.get("sector"),
                "momentum_score": h.get("score"),
                # The per-company price/volume pillars the table shows. Stored under
                # the scored frame's column names for the same reason as above.
                "score_price": (h.get("category_scores") or {}).get("price"),
                "score_volume": (h.get("category_scores") or {}).get("volume"),
                "sector_rank": h.get("sector_rank"),
                "company_rank": h.get("company_rank"),
            } for h in (p.get("holdings") or [])],
            "sector_scores": p.get("sector_scores") or [],
        }
    return out


def _attach_exchanges(daily_picks: list[dict]) -> None:
    """Stamp `exchange` onto every holding of every day, in place.

    ⚠ A GURUFOCUS LINK WITHOUT THE EXCHANGE IS A 404 FOR MOST OF THIS UNIVERSE.
    `guruFocusUrl` falls back to a BARE ticker when no exchange is supplied, which
    is correct for a US listing and wrong for everything else — and this universe
    is mostly everything else (Nestle resolves as `XSWX:NESN`, not `NESN`). A link
    that lands on a 404 is worse than no link: it reads as "we do not have this
    company" rather than "we did not tell the URL builder where it trades".

    ⚠ ENRICHED HERE, NOT ADDED TO `PeriodHolding`. That dataclass is what gets
    persisted into `current_picks_snapshot.holdings` and what the golden-master
    test asserts on exactly; widening it to carry a display field would change a
    stored shape for the sake of a hyperlink. Same reasoning — and the same
    best-effort, never-fatal contract — as `_enrich_holdings_isin`.
    """
    ids = sorted({
        int(h["company_id"])
        for day in daily_picks for h in (day.get("holdings") or [])
        if h.get("company_id") is not None and int(h["company_id"]) > 0
    })
    if not ids:
        return
    by_id: dict[int, str] = {}
    try:
        from deps import IN_CHUNK_SIZE, supabase  # noqa: PLC0415

        for i in range(0, len(ids), IN_CHUNK_SIZE):
            r = (supabase.table("company").select("company_id, exchange")
                 .in_("company_id", ids[i:i + IN_CHUNK_SIZE]).execute())
            for c in r.data or []:
                if c.get("exchange"):
                    by_id[int(c["company_id"])] = c["exchange"]
    except Exception as e:  # noqa: BLE001 — a missing link must not fail the walk
        import logging  # noqa: PLC0415

        logging.getLogger(__name__).warning(
            "daily-holdings exchange enrichment failed: %s: %s", type(e).__name__, e)
        return
    for day in daily_picks:
        for h in day.get("holdings") or []:
            cid = h.get("company_id")
            if cid is not None and int(cid) > 0 and not h.get("exchange"):
                ex = by_id.get(int(cid))
                if ex:
                    h["exchange"] = ex


def _daily_from(months_back: int):
    """First day of the month `months_back` months ago, or None for "this period".

    ⚠ WHOLE MONTHS, ANCHORED TO A MONTH START — not `today - 60 days`. The picks
    are a monthly-rebalanced strategy's, so a window that opens mid-month starts
    the chain-linked return partway through a holding period and reads as a
    different strategy. `months_back=2` on 31 July opens 1 June.
    """
    if months_back <= 0:
        return None
    from datetime import date  # noqa: PLC0415

    t = date.today()
    y, m = t.year, t.month - months_back
    while m <= 0:
        m += 12
        y -= 1
    return date(y, m, 1)


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
        "signal_weights": req.signal_weights or {
            s["key"]: s["default_weight"]
            for s in signal_defs_for_mode(req.selection_mode)
        },
        "category_weights": req.category_weights,
        "top_n_sectors": req.top_n_sectors,
        "top_n_per_sector": req.top_n_per_sector,
        "selection_mode": req.selection_mode,
        "random_seed": req.random_seed,
        "rebalance_frequency": req.rebalance_frequency,
        "rebalance_weekday": req.rebalance_weekday,
        "strategy_type": req.strategy_type,
        "sector_etfs": req.sector_etfs,
        # min_price_score MUST be threaded through here — this path builds the
        # config for the single-run backtest AND the live current-portfolio
        # rebalance. Omitting it silently dropped the price-score floor on every
        # live rebalance (the variants path passed it, so a swept backtest looked
        # compliant while the scheduled strategy picked sub-floor names).
        "min_price_score": req.min_price_score,
        "backfill_below_min_score": req.backfill_below_min_score,
        "vol_target": req.vol_target,
        "regime_floor": req.regime_floor,
        "regime_ramp_lo": req.regime_ramp_lo,
        "regime_ramp_hi": req.regime_ramp_hi,
        "daily_timing": req.daily_timing,
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

    # Post-backtest cache warming hook: the runner calls this once per
    # period with that period's eligibility-filtered signals_df. Pipes
    # straight into the /signal-breakdown LRU so the user's first
    # breakdown click after the backtest hits the cache instead of
    # paying the 10s universe-load. Only fired by the regular backtest
    # path — current_portfolio + multi-trial random don't traverse the
    # cutoffs in the shape needed for cache priming.
    def _warm_panel(cutoff, panel):
        warm_breakdown_panel_cache(
            req.universe_label, req.index_universe, cutoff, panel,
        )

    # Reuse previously computed selections for the retrospective walk, so a re-run
    # costs only the days that are genuinely new. Loaded before the executor hands
    # off — the engine needs it up front to drop those cutoffs from the panel.
    _cached_frames: dict = {}
    _cached_sectors: dict = {}
    _cached_raw: dict = {}
    if req.mode == "current_portfolio" and req.daily_months_back > 0:
        _cached_frames, _cached_sectors, _cached_raw = await asyncio.to_thread(
            _load_cached_selections,
            _strategy_hash(req),
            _daily_from(req.daily_months_back),
            req.daily_months_back,
            req.force_recompute,
        )
        if _cached_frames:
            yield _emit({"type": "progress", "pct": 66, "message": (
                f"Reusing {len(_cached_frames)} previously computed day(s)")})

    def _run_backtest():
        try:
            if req.mode == "current_portfolio":
                r = run_current_portfolio(
                    config, prices_df, universe_df, send_event,
                    volumes_df=volumes_df,
                    monthly_eligible=monthly_eligible,
                    prices_local_df=prices_local_df,
                    company_currency=company_currency,
                    daily_from=_daily_from(req.daily_months_back),
                    cached_selections=_cached_frames or None,
                    cached_sector_scores=_cached_sectors or None,
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
                    panel_warm_callback=_warm_panel,
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
        # ⚠ A RETROSPECTIVE WALK IS AN ANSWER, NOT A DECISION — SO IT WRITES NOTHING.
        # `current_picks_day` records what the pipeline decided each day on the data
        # it had at the time; `current_picks_snapshot` records the basket it locked.
        # Recomputing a closed month on today's prices and upserting it would replace
        # both with hindsight, silently and irreversibly (the upsert is keyed on
        # (strategy_hash, target_date), so the original decision is simply gone).
        # `daily_picks_history` still comes back so the UI can show the STORED days
        # beside the computed ones — the comparison is the point of the feature.
        if req.daily_months_back > 0:
            payload["read_only"] = True
            payload["daily_months_back"] = req.daily_months_back
            try:
                await asyncio.to_thread(_attach_exchanges, payload.get("daily_picks") or [])
            except Exception:
                pass  # a missing hyperlink is not worth failing a computed walk over
            # Store the newly computed selections so the next run only pays for the
            # days that are actually new. ⚠ `daily_holdings_cache`, NEVER
            # `current_picks_day` — the two are keyed identically and hold different
            # facts; see the migration.
            fresh = _selections_to_store(payload.get("daily_picks") or [], _cached_raw)
            try:
                await asyncio.to_thread(_persist_daily_holdings_cache, _strategy_hash(req), fresh)
            except Exception as e:
                yield _emit({"type": "warning", "scope": "daily-holdings-cache",
                             "message": f"Could not cache the computed days: {type(e).__name__}: {e}"})
            payload["cache_stats"] = {
                "reused": len(_cached_raw),
                "computed": len(payload.get("daily_picks") or []) - len(_cached_raw),
                "stored": len(fresh),
            }
            try:
                payload["daily_picks_history"] = await asyncio.to_thread(_fetch_daily_picks_history, hash_)
            except Exception:
                payload["daily_picks_history"] = []
            yield _emit({"type": "current_portfolio", "data": payload, "universe": universe_snapshot})
            st = payload["cache_stats"]
            yield _emit({"type": "done", "message": (
                f"{len(payload.get('daily_picks') or [])} trading days "
                f"({st['reused']} reused, {st['computed']} computed) — "
                f"the pipeline's own daily picks were not touched")})
            return
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
