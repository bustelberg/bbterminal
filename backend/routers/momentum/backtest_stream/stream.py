"""Backtest SSE orchestrator + the FastAPI route.

The endpoint is a generator-driven Server-Sent-Events stream. The
orchestrator:
  1. Handles cache short-circuits (current_portfolio + backtest replay).
  2. Loads the universe, runs the pre-flight DB staleness check, and
     resolves `monthly_eligible` (universe / index_universe).
  3. Filters the universe to that snapshot + applies `max_companies`.
  4. Optionally runs the API ensure-fetch loop (db_only=False) and
     prunes the universe of delisted / unsubscribed companies.
  5. Bulk-loads prices in EUR + local, FX rates, and volumes.
  6. Audits coverage and self-heals genuine gaps.
  7. Builds the per-company universe snapshot.
  8. Dispatches to either the variants sweep or the single-run path.

Each phase is its own async generator in a sibling module. The phases
that need to feed values forward yield a sentinel `("__result__", value)`
as their last value; the orchestrator picks that up via a small
`_collect_result` helper.
"""
from __future__ import annotations

import asyncio
import json
from datetime import date, timedelta

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from deps import supabase
from momentum.data import load_universe

from .._helpers import (
    backtest_strategy_hash as _backtest_strategy_hash,
    fetch_daily_picks_history as _fetch_daily_picks_history,
    find_cached_backtest as _find_cached_backtest,
    find_cached_snapshot as _find_cached_snapshot,
    latest_db_price_date as _latest_db_price_date,
    strategy_hash as _strategy_hash,
)
from .audit import audit_price_coverage, audit_volume_coverage, build_universe_snapshot
from .bulk_loaders import (
    load_fx_and_convert,
    load_prices_streamed,
    load_volumes_streamed,
    sync_fx_streamed,
)
from .fetch_loop import run_fetch_loop
from .models import BacktestRequest
from .self_heal import compute_gap_cids, run_self_heal
from .single_run import run_single
from .universe_loader import load_monthly_eligible
from .variants import run_variants_sweep

router = APIRouter(tags=["momentum"])


def _emit(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"


def _keepalive() -> str:
    return ": keepalive\n\n"


async def _collect_result(agen, sink_yield):
    """Drain an async generator, forwarding non-sentinel values to
    `sink_yield` and returning the trailing `("__result__", value)` payload.

    `sink_yield` is a callback the orchestrator passes that appends each
    event to its own outgoing stream. Returns None if the generator ended
    without a sentinel (e.g. an early-error case where the helper yielded
    an error event and returned)."""
    result = None
    async for evt in agen:
        if isinstance(evt, tuple) and len(evt) >= 2 and evt[0] == "__result__":
            result = evt[1:] if len(evt) > 2 else evt[1]
            continue
        await sink_yield(evt)
    return result


async def _momentum_backtest_stream(req: BacktestRequest):
    """SSE generator for the momentum backtest."""

    # Variants sweep is backtest-only and not cached as a bundle. Per-variant
    # results are streamed individually; if the user wants caching they can
    # save the bundle from the UI.
    if req.variants and req.mode == "current_portfolio":
        yield _emit({"type": "error", "message": "Variants sweep is not supported with mode='current_portfolio'"})
        return

    # Backtests are unconditionally DB-only — the canonical refresher is the
    # scheduled-refresh cron (`/api/ingest/scheduled-refresh`, surfaced on
    # the /schedule page). This makes backtest runs predictable, fast, and
    # never side-effecting: no GuruFocus calls, no ECB FX sync, no self-heal
    # round-trips. If the data isn't current, the user triggers a refresh
    # via /schedule and reruns. Current-portfolio mode keeps its own narrow
    # refresh path (current_picks cron self-heals its ~30 holdings before
    # snapshotting) — that's a separate concern from broad data freshness.
    if req.mode != "current_portfolio":
        req.db_only = True

    # current_portfolio mode runs against today only; coerce the date range
    # so price loading covers ~14 months of history (12m momentum + buffer)
    # without requiring the caller to pick the right window.
    if req.mode == "current_portfolio":
        _today = date.today()
        req.start_date = (_today - timedelta(days=14 * 31)).isoformat()
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

        # Drop any company on an exchange the user flagged as
        # broker-unsupported on /fees. Missing rows in `exchange_fee`
        # default to supported, so the user only has to opt OUT of the
        # exchanges their broker can't actually trade. Done after
        # load_universe so the unfiltered count is visible to the
        # progress log first.
        def _unsupported_exchanges() -> set[str]:
            try:
                r = (
                    supabase.table("exchange_fee")
                    .select("exchange_code")
                    .eq("is_broker_supported", False)
                    .execute()
                )
                return {row["exchange_code"] for row in (r.data or []) if row.get("exchange_code")}
            except Exception:
                # Don't fail the backtest if the lookup hiccups — default
                # to "no filter applied" so a transient DB blip can't
                # silently produce an empty universe.
                return set()
        unsupported = await asyncio.to_thread(_unsupported_exchanges)
        if unsupported and "gurufocus_exchange" in universe_df.columns:
            pre = len(universe_df)
            universe_df = universe_df[~universe_df["gurufocus_exchange"].isin(unsupported)].reset_index(drop=True)
            dropped = pre - len(universe_df)
            if dropped > 0:
                yield _emit({
                    "type": "warning",
                    "scope": "data",
                    "message": (
                        f"Dropped {dropped} companies on {len(unsupported)} broker-unsupported "
                        f"exchange(s) ({', '.join(sorted(unsupported))}) — adjust on /fees."
                    ),
                })
            if universe_df.empty:
                yield _emit({
                    "type": "error",
                    "message": (
                        "Every company in the universe is on a broker-unsupported exchange. "
                        "Re-enable some exchanges on /fees and re-run."
                    ),
                })
                return

        # Pre-flight DB-staleness check.
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

        # Resolve monthly_eligible from universe_label / index_universe.
        # The helper yields its own progress + error events; on error it
        # yields an `error` event and returns the (None, True) sentinel,
        # at which point we abort.
        monthly_eligible = None
        did_error = False
        async for evt in load_monthly_eligible(req):
            if isinstance(evt, tuple) and len(evt) >= 1 and evt[0] == "__result__":
                _, monthly_eligible, did_error = evt
                continue
            yield evt
        if did_error:
            return

        data_cutoff = date.fromisoformat(req.end_date)

        excluded_ids: set[int] = set()

        # When a universe / index_universe is selected, drop every company
        # that doesn't appear in any month of that universe. Otherwise
        # price+volume gets fetched for unrelated companies that the
        # scoring pipeline would discard anyway, wasting GuruFocus API
        # calls and wall-time.
        if monthly_eligible is not None:
            eligible_ids: set[int] = set()
            for month_map in monthly_eligible.values():
                eligible_ids.update(month_map.keys())
            before = len(universe_df)
            universe_df = universe_df[universe_df["company_id"].isin(eligible_ids)].reset_index(drop=True)
            dropped = before - len(universe_df)
            if dropped:
                yield _emit({"type": "progress", "pct": 8, "message": f"Trimmed {dropped} companies not in selected universe ({len(universe_df)} remaining)"})

        # If max_companies is set, pre-trim the universe alphabetically so
        # we only fetch what we need.
        if req.max_companies > 0 and len(universe_df) > req.max_companies:
            universe_df = universe_df.sort_values("gurufocus_ticker").head(req.max_companies).reset_index(drop=True)

        total_companies = len(universe_df)
        blocked_exchanges: set[str] = set()
        pass1_transient: set[int] = set()
        counters = {"ok_count": 0, "skipped_count": 0}

        # In db_only mode (the default for the user-facing buttons) we
        # bypass the per-company API ensure-loop and just consume whatever
        # is already in the DB. The pre-flight staleness check above has
        # already errored if the DB isn't current enough; missing-data
        # filtering happens later in signals.py via the 30-day staleness
        # guard.
        if req.db_only:
            yield _emit({"type": "progress", "pct": 60, "message": f"DB-only mode: skipping API fetches for {total_companies} companies"})
        else:
            async for evt in run_fetch_loop(
                universe_df, data_cutoff,
                excluded_ids=excluded_ids,
                blocked_exchanges=blocked_exchanges,
                pass1_transient=pass1_transient,
                counters=counters,
            ):
                yield evt

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
        price_start = date.fromisoformat(req.start_date) - timedelta(days=300)
        price_end = date.fromisoformat(req.end_date) + timedelta(days=35)

        prices_df = None
        async for evt in load_prices_streamed(company_ids, price_start, price_end):
            if isinstance(evt, tuple) and evt[0] == "__result__":
                prices_df = evt[1]
                continue
            yield evt

        if prices_df is None or prices_df.empty:
            yield _emit({"type": "error", "message": "No price data found after ingestion."})
            return

        n_companies_with_prices = prices_df["company_id"].nunique()
        yield _emit({"type": "progress", "pct": 65, "message": f"Loaded {len(prices_df):,} prices for {n_companies_with_prices} companies"})

        # FX conversion: convert local-currency prices to EUR so signals
        # and returns are expressed in a single currency for a EUR-based
        # investor. Momentum ratios are scale-invariant so signals are
        # unaffected, but forward returns change with FX drift (e.g.
        # JPY weakness vs EUR).
        yield _emit({"type": "progress", "pct": 65, "message": "Resolving trading currency per company..."})
        yield _keepalive()
        from momentum.data import load_company_currency
        company_currency = await asyncio.to_thread(
            load_company_currency, supabase, company_ids,
        )
        currencies_needed = sorted({c for c in company_currency.values() if c})
        yield _emit({"type": "progress", "pct": 65, "message": f"Found {len(currencies_needed)} distinct currencies: {', '.join(currencies_needed)}"})

        # Sync fx_rate table from ECB for every currency in range. This
        # is idempotent and cheap — only fetches what's missing past the
        # highest existing rate_date per currency. Skipped under db_only.
        if req.db_only:
            yield _emit({"type": "progress", "pct": 65, "message": "DB-only mode: skipping ECB FX sync, using cached FX rates"})
        else:
            async for evt in sync_fx_streamed(currencies_needed, price_start, price_end):
                if isinstance(evt, tuple) and evt[0] == "__result__":
                    continue  # the sync result dict isn't needed downstream
                yield evt

        # Load FX rates + convert prices to EUR.
        prices_eur_df = None
        prices_local_df = None
        fx_rates = None
        async for evt in load_fx_and_convert(prices_df, company_currency, currencies_needed, price_start, price_end):
            if isinstance(evt, tuple) and evt[0] == "__result__":
                prices_eur_df, prices_local_df, fx_rates = evt[1]
                continue
            yield evt
        prices_df = prices_eur_df

        if prices_df is None or prices_df.empty:
            yield _emit({"type": "error", "message": "No price data left after FX conversion."})
            return

        # Audit price coverage.
        audit = audit_price_coverage(universe_df, prices_df, company_ids)
        for evt in audit.events:
            yield evt
        audit.events.clear()

        # Load volumes from DB.
        volumes_df = None
        async for evt in load_volumes_streamed(company_ids, price_start, price_end):
            if isinstance(evt, tuple) and evt[0] == "__result__":
                volumes_df = evt[1]
                continue
            yield evt

        n_vol = volumes_df["company_id"].nunique() if not volumes_df.empty else 0
        yield _emit({"type": "progress", "pct": 67, "message": f"Loaded {len(volumes_df):,} volume records for {n_vol} companies"})

        # Audit volume coverage (mutates `audit` in place — appends events).
        audit_volume_coverage(audit, volumes_df, company_ids)
        for evt in audit.events:
            yield evt
        audit.events.clear()

        # Self-heal: refetch missing data for subscribed-exchange gaps.
        # Runs even in db_only mode (backtest path) — these are TRUE gaps
        # (zero rows) on subscribed exchanges, which means the pipeline
        # hasn't reached them yet. Healing them inline lets a backtest
        # produce correct results without the user having to babysit the
        # pipeline first. Cost is bounded: only fires for companies with
        # ZERO data (audit.no_price_gap_cids / no_vol_gap_cids), not for
        # stale-but-present data which remains the pipeline's job.
        gap_cids, info_events = compute_gap_cids(req, audit, pass1_transient, company_ids)
        for evt in info_events:
            yield evt
        if gap_cids:
            sample = ", ".join(audit.label_for_cid.get(int(c), str(c)) for c in gap_cids[:8])
            more = f" (+{len(gap_cids) - 8} more)" if len(gap_cids) > 8 else ""
            yield _emit({
                "type": "info",
                "scope": "self-heal",
                "message": f"{len(gap_cids)} companies on subscribed exchanges have no data yet — fetching inline before the backtest: {sample}{more}",
            })
            async for evt in run_self_heal(
                gap_cids, universe_df, audit,
                prices_df, prices_local_df, volumes_df,
                company_currency, fx_rates,
                price_start, price_end,
            ):
                if isinstance(evt, tuple) and evt[0] == "__result__":
                    prices_df, prices_local_df, volumes_df = evt[1]
                    continue
                yield evt

        # Build universe snapshot once — used by both single-run and
        # variants paths.
        universe_snapshot = build_universe_snapshot(universe_df)

        # Dispatch to variants sweep or single-run path.
        if req.variants:
            async for evt in run_variants_sweep(
                req, prices_df, prices_local_df, volumes_df,
                universe_df, monthly_eligible, company_currency, universe_snapshot,
            ):
                yield evt
            return

        async for evt in run_single(
            req, prices_df, prices_local_df, volumes_df,
            universe_df, monthly_eligible, company_currency, universe_snapshot,
        ):
            yield evt

    except Exception as e:
        yield _emit({"type": "error", "message": str(e)})


@router.post("/api/momentum/backtest")
async def momentum_backtest(req: BacktestRequest):
    return StreamingResponse(
        _momentum_backtest_stream(req),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
