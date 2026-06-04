"""Phase 4 — momentum compute (current-picks snapshots).

Two entry points sharing the per-strategy isolation pattern (a single
failing strategy never aborts the phase; each result lands as an entry
in `ingest_run.momentum_summary` with a full traceback on failure):

  _run_momentum_phase     the weekly/full pipeline's Phase 4. Per enabled
                          `scheduled_strategy`, either a fresh rebalance
                          (when `next_due_at` has arrived) or a price
                          update on the last rebalance's holdings.
  _run_daily_mtd_phase    the daily MTD refresh's momentum phase. Always
                          a price_update insert, de-duplicated against an
                          existing snapshot for the same open period.

Heavy imports (pandas-pulling momentum stream, BacktestRequest) are kept
function-local so importing this module stays cheap at boot.
"""
from __future__ import annotations

import asyncio
import json as _json
import logging
from datetime import datetime, timezone

from deps import supabase

from .runlog import _Throttle, _now_utc_iso, _update_run


def _run_momentum_phase(run_id: int) -> None:
    """Phase 3 — compute current-portfolio holdings for every enabled row
    in `scheduled_strategy`. Each strategy gets its own
    `current_picks_snapshot` tagged with `ingest_run_id` +
    `scheduled_strategy_id`, so the /schedule per-strategy detail view
    can JOIN them back to this run.

    Each scheduled_strategy carries its own `config` (BacktestRequest
    payload) and `frequency`. The phase only computes strategies whose
    `next_due_at` is in the past (or NULL — fresh entries). After a
    successful compute the row's `last_run_at` is set to now and
    `next_due_at` is advanced per `frequency` (see
    `routers.scheduled_strategies.compute_next_due_at`).

    Per-strategy isolation: a single failing strategy doesn't abort
    the phase. Each result lands as a `templates_summary`-style entry
    in `ingest_run.momentum_summary` (with full Python traceback on
    failure, for debugging from /schedule). If ANY strategy errored,
    the phase raises a summarized error so the outer pipeline marks
    the run `error` — but every successful snapshot is still
    persisted, and every strategy's `next_due_at` is still bumped on
    success."""
    log = logging.getLogger(__name__)
    now_iso = _now_utc_iso()

    # Pull EVERY enabled scheduled strategy. Each one will produce
    # exactly one snapshot per tick — a fresh rebalance if it's due
    # (`next_due_at` past), otherwise a `price_update` on the last
    # rebalance's holdings.
    sched_resp = (
        supabase.table("scheduled_strategy")
        .select("id, name, frequency, config, enabled, last_run_at, next_due_at")
        .eq("enabled", True)
        .order("created_at")
        .execute()
    )
    scheduled = sched_resp.data or []
    if not scheduled:
        log.info(
            "[pipeline.momentum] run_id=%s no scheduled strategies — skipping",
            run_id,
        )
        _update_run(run_id, momentum_summary=[])
        return

    # Imports are local so the module loads cheaply at boot — the momentum
    # stream pulls in pandas/numpy etc.
    import traceback as _traceback  # noqa: PLC0415
    from routers.momentum.backtest_stream.models import BacktestRequest  # noqa: PLC0415
    from routers.momentum.backtest_stream.stream import (  # noqa: PLC0415
        _momentum_backtest_stream,
    )
    from routers.scheduled_strategies import (  # noqa: PLC0415
        compute_and_save_price_update as _compute_and_save_price_update,
        compute_next_due_at as _compute_next_due_at,
    )

    summaries: list[dict] = []
    errors: list[str] = []
    total = len(scheduled)

    for idx, sched in enumerate(scheduled, start=1):
        strategy_id = sched["id"]
        strategy_name = sched.get("name") or f"Strategy #{strategy_id}"
        frequency = sched.get("frequency")
        next_due_iso = sched.get("next_due_at")
        # "Due to rebalance" — first-run (next_due_at IS NULL) or its
        # next-due tick has arrived. Otherwise the tick just does a
        # price update on the last rebalance.
        is_due_to_rebalance = (next_due_iso is None) or (next_due_iso <= now_iso)
        kind = "rebalance" if is_due_to_rebalance else "price_update"
        _update_run(
            run_id,
            current_message=(
                f"Strategy {idx} of {total} · "
                f"{'rebalancing' if is_due_to_rebalance else 'price-updating'}: {strategy_name}…"
            ),
        )

        entry: dict = {
            "strategy_id": strategy_id,
            "strategy_name": strategy_name,
            "frequency": frequency,
            "kind": kind,
            "config": sched.get("config") or {},
            "snapshot_id": None,
            "holdings_count": 0,
            "latest_price_date": None,
            "status": "error",
            "error_message": None,
            "error_traceback": None,
        }

        # ── Branch A: not due → price update on last rebalance ────
        if not is_due_to_rebalance:
            try:
                snapshot_id = _compute_and_save_price_update(
                    strategy_id=strategy_id,
                    ingest_run_id=run_id,
                    is_backfill=False,
                )
                if snapshot_id is None:
                    # No prior rebalance to price-update from. The very
                    # first tick after add should always be a rebalance,
                    # so this is a strange-but-non-fatal state.
                    entry["status"] = "ok"
                    entry["error_message"] = "No prior rebalance to price-update from"
                else:
                    # Hydrate the entry summary from the fresh snapshot.
                    pu_resp = supabase.table("current_picks_snapshot").select(
                        "holdings, latest_price_date"
                    ).eq("snapshot_id", snapshot_id).limit(1).execute()
                    pu = (pu_resp.data or [{}])[0]
                    entry["snapshot_id"] = snapshot_id
                    entry["holdings_count"] = len(pu.get("holdings") or [])
                    entry["latest_price_date"] = pu.get("latest_price_date")
                    entry["status"] = "ok"
                # Bump last_run_at (but NOT next_due_at — the strategy
                # didn't actually rebalance, it just got re-priced).
                supabase.table("scheduled_strategy").update({
                    "last_run_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                    "updated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                }).eq("id", strategy_id).execute()
            except Exception as e:
                msg = f"{type(e).__name__}: {e}"
                tb = _traceback.format_exc()
                entry["error_message"] = msg
                entry["error_traceback"] = tb
                errors.append(f"[{strategy_name}] {msg}")
                log.warning(
                    "[pipeline.momentum] run_id=%s strategy=%s price_update failed: %s\n%s",
                    run_id, strategy_name, msg, tb,
                )

            summaries.append(entry)
            _update_run(run_id, momentum_summary=summaries)
            continue

        # ── Branch B: due → fresh rebalance ───────────────────────
        try:
            cfg = dict(sched.get("config") or {})
            if not cfg:
                raise RuntimeError(
                    f"Scheduled strategy #{strategy_id} has no config"
                )
            # Pipeline-only overrides — the saved config is the user's
            # intent; we only force the mode/cache flags so it computes
            # a fresh current-portfolio snapshot.
            cfg["mode"] = "current_portfolio"
            cfg["force_recompute"] = True
            cfg["db_only"] = False
            cfg.pop("variants", None)
            cfg.pop("n_trials", None)
            try:
                req = BacktestRequest(**cfg)
            except Exception as e:
                raise RuntimeError(
                    f"Scheduled strategy config doesn't validate as BacktestRequest: {e}"
                )

            snapshot_id: int | None = None
            stream_err: str | None = None
            holdings_count = 0
            latest_price_date: str | None = None
            msg_throttle = _Throttle()

            async def _drain() -> None:
                nonlocal snapshot_id, stream_err, holdings_count, latest_price_date
                async for chunk in _momentum_backtest_stream(req):
                    if not isinstance(chunk, str) or not chunk.startswith("data: "):
                        continue
                    try:
                        evt = _json.loads(chunk[len("data: "):].strip())
                    except _json.JSONDecodeError:
                        continue
                    t = evt.get("type")
                    if t == "progress":
                        m = evt.get("message")
                        if m and msg_throttle.should_write():
                            await asyncio.to_thread(
                                _update_run,
                                run_id,
                                current_message=f"[{idx}/{total} {strategy_name}] {m}",
                            )
                    elif t == "current_portfolio":
                        payload = evt.get("data") or {}
                        snapshot_id = payload.get("snapshot_id")
                        holdings_count = len(payload.get("holdings") or [])
                        latest_price_date = payload.get("latest_price_date")
                    elif t == "error":
                        stream_err = evt.get("message") or "unknown error"

            asyncio.run(_drain())

            if stream_err:
                raise RuntimeError(stream_err)
            if snapshot_id is None:
                raise RuntimeError("Momentum compute finished without persisting a snapshot")

            # Tag the snapshot with the pipeline run + scheduled
            # strategy it came from, and re-tag as 'auto' (the SSE flow
            # inside the stream writes 'manual').
            try:
                supabase.table("current_picks_snapshot").update({
                    "triggered_by": "auto",
                    "ingest_run_id": run_id,
                    "scheduled_strategy_id": strategy_id,
                }).eq("snapshot_id", snapshot_id).execute()
            except Exception as e:
                log.warning(
                    "[pipeline.momentum] run_id=%s failed to tag snapshot=%s: %s: %s",
                    run_id, snapshot_id, type(e).__name__, e,
                )

            # Advance the schedule clock: mark this strategy as just
            # ran + compute its next due tick from the frequency. Best-
            # effort — a checkpoint write failure here doesn't roll back
            # the snapshot.
            try:
                ran_at = datetime.now(timezone.utc).replace(microsecond=0)
                next_due = (
                    _compute_next_due_at(frequency, ran_at).isoformat()
                    if frequency else None
                )
                supabase.table("scheduled_strategy").update({
                    "last_run_at": ran_at.isoformat(),
                    "next_due_at": next_due,
                    "updated_at": ran_at.isoformat(),
                }).eq("id", strategy_id).execute()
            except Exception as e:
                log.warning(
                    "[pipeline.momentum] run_id=%s strategy=%s failed to bump schedule clock: %s: %s",
                    run_id, strategy_name, type(e).__name__, e,
                )

            entry["snapshot_id"] = snapshot_id
            entry["holdings_count"] = holdings_count
            entry["latest_price_date"] = latest_price_date
            entry["status"] = "ok"
            log.info(
                "[pipeline.momentum] run_id=%s strategy=%s snapshot=%s holdings=%s",
                run_id, strategy_name, snapshot_id, holdings_count,
            )
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            tb = _traceback.format_exc()
            entry["error_message"] = msg
            entry["error_traceback"] = tb
            errors.append(f"[{strategy_name}] {msg}")
            log.warning(
                "[pipeline.momentum] run_id=%s strategy=%s failed: %s\n%s",
                run_id, strategy_name, msg, tb,
            )

        summaries.append(entry)
        # Persist incremental progress so the UI sees each strategy
        # land as it completes, not only after the whole phase is done.
        _update_run(run_id, momentum_summary=summaries)

    if errors:
        raise RuntimeError(
            f"{len(errors)} of {total} strategies failed: " + " | ".join(errors[:3])
        )


def _run_daily_mtd_phase(run_id: int) -> None:
    """Phase 'momentum' for the daily MTD refresh. For each enabled
    scheduled strategy, INSERTS a fresh `kind='price_update'` snapshot
    that re-prices the most-recent rebalance's holdings against the
    latest available closes. Mirrors the weekly pipeline's Branch A
    (`_run_momentum_phase` → `compute_and_save_price_update`) — same
    primitive, just fired daily instead of weekly.

    Why insert rather than mutate? Past snapshots (rebalances, prior
    price_updates, backfill rows) are immutable history. The freshest
    "to-date" view becomes the newest row at the top of the run
    history; the UI naturally picks it up on the next fetch.

    Dedup: a fresh price_update for `(strategy, as_of, lpd)` is treated
    as identical noise if a row with the SAME triple already exists
    (excluding the row we just inserted). The new row is deleted again
    so running the daily job twice the same day, or on a day with no
    new prices, doesn't grow the history.

    Why match on `as_of` AND `lpd` rather than just lpd-vs-most-recent?
    Backfills insert N rebalance rows for the strategy at once, and
    earlier buggy code could have left one of them with a misleading
    lpd. Scoping dedup to "same open period" (same `as_of_date`)
    isolates the comparison to the row representing the strategy's
    current period, which is what the dedup actually cares about."""
    import traceback as _traceback  # noqa: PLC0415
    from routers.scheduled_strategies import (  # noqa: PLC0415
        compute_and_save_price_update as _compute_and_save_price_update,
    )

    log = logging.getLogger(__name__)

    strat_resp = (
        supabase.table("scheduled_strategy")
        .select("id, name, frequency, config")
        .eq("enabled", True)
        .execute()
    )
    strategies = strat_resp.data or []
    if not strategies:
        _update_run(run_id, current_message="No enabled strategies — nothing to refresh.")
        return

    summaries: list[dict] = []
    errors: list[str] = []
    total = len(strategies)
    for idx, strat in enumerate(strategies, 1):
        sid = strat["id"]
        sname = strat.get("name") or f"Strategy #{sid}"
        entry: dict = {
            "strategy_id": sid,
            "strategy_name": sname,
            "frequency": strat.get("frequency"),
            "kind": "price_update",
            "config": strat.get("config") or {},
            "snapshot_id": None,
            "holdings_count": 0,
            "latest_price_date": None,
            "status": "ok",
            "error_message": None,
            "error_traceback": None,
        }

        try:
            new_snapshot_id = _compute_and_save_price_update(
                strategy_id=sid,
                ingest_run_id=run_id,
                is_backfill=False,
            )
            if new_snapshot_id is None:
                # No prior rebalance to price-update from — strategy
                # has never produced a real picks row (and has no
                # backfill rebalance either). Skip silently.
                entry["error_message"] = "no rebalance to price-update from"
                summaries.append(entry)
                _update_run(
                    run_id,
                    momentum_summary=summaries,
                    current_message=f"MTD refresh: {idx}/{total} ({sname}: skipped)",
                )
                continue

            # Read back the newly-inserted row so we can (a) report
            # accurate counts and (b) check whether an identical
            # snapshot already exists for the same open period.
            new_resp = (
                supabase.table("current_picks_snapshot")
                .select("snapshot_id, as_of_date, latest_price_date, holdings")
                .eq("snapshot_id", new_snapshot_id)
                .limit(1)
                .execute()
            )
            new_row = (new_resp.data or [None])[0]
            new_as_of = (new_row or {}).get("as_of_date") if new_row else None
            new_lpd = (new_row or {}).get("latest_price_date") if new_row else None

            # Same-period dedup: is there ALREADY a snapshot for this
            # strategy + open period + lpd? If yes, the new row is
            # identical noise.
            dup_existing_id: int | None = None
            if new_as_of and new_lpd:
                dup_resp = (
                    supabase.table("current_picks_snapshot")
                    .select("snapshot_id, created_at")
                    .eq("scheduled_strategy_id", sid)
                    .eq("as_of_date", new_as_of)
                    .eq("latest_price_date", new_lpd)
                    .neq("snapshot_id", new_snapshot_id)
                    .order("created_at", desc=True)
                    .limit(1)
                    .execute()
                )
                if dup_resp.data:
                    dup_existing_id = dup_resp.data[0]["snapshot_id"]

            if dup_existing_id is not None:
                # Identical snapshot for this open period already exists
                # — delete the redundant new one and point the summary
                # at the surviving row.
                supabase.table("current_picks_snapshot").delete().eq(
                    "snapshot_id", new_snapshot_id,
                ).execute()
                entry["snapshot_id"] = dup_existing_id
                entry["latest_price_date"] = new_lpd
                entry["error_message"] = (
                    f"no change since prior snapshot for period "
                    f"as_of={new_as_of} (lpd={new_lpd}) — skipped"
                )
                log.info(
                    "[daily_mtd] run_id=%s strategy=%s dedup: as_of=%s lpd=%s "
                    "matched existing snapshot=%s",
                    run_id, sname, new_as_of, new_lpd, dup_existing_id,
                )
            else:
                entry["snapshot_id"] = new_snapshot_id
                entry["holdings_count"] = (
                    len(new_row.get("holdings") or []) if new_row else 0
                )
                entry["latest_price_date"] = new_lpd
                log.info(
                    "[daily_mtd] run_id=%s strategy=%s snapshot=%s as_of=%s "
                    "lpd=%s",
                    run_id, sname, new_snapshot_id, new_as_of, new_lpd,
                )
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            tb = _traceback.format_exc()
            entry["status"] = "error"
            entry["error_message"] = msg
            entry["error_traceback"] = tb
            errors.append(f"[{sname}] {msg}")
            log.warning(
                "[daily_mtd] run_id=%s strategy=%s failed: %s\n%s",
                run_id, sname, msg, tb,
            )

        summaries.append(entry)
        _update_run(
            run_id,
            momentum_summary=summaries,
            current_message=f"MTD refresh: {idx}/{total} ({sname})",
        )

    if errors:
        raise RuntimeError(
            f"{len(errors)} of {total} strategies failed: " + " | ".join(errors[:3])
        )
