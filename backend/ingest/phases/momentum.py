"""Phase 4 — momentum compute (current-picks snapshots).

Per-strategy isolation pattern: a single failing strategy never aborts the
phase; each result lands as an entry in `ingest_run.momentum_summary` with a
full traceback on failure.

  _run_momentum_phase        per enabled `scheduled_strategy`, either a fresh
                             rebalance (when due) or a price update on the
                             last rebalance's holdings.
  _run_smart_momentum_phase  thin wrapper that drives `_run_momentum_phase`
                             with the smart plan's per-strategy due decision
                             + same-period dedup on the price updates.

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


def _run_momentum_phase(
    run_id: int,
    *,
    due_override: dict[int, bool] | None = None,
    dedupe_price_updates: bool = False,
) -> None:
    """Phase 3 — compute current-portfolio holdings for every enabled row
    in `scheduled_strategy`. Each strategy gets its own
    `current_picks_snapshot` tagged with `ingest_run_id` +
    `scheduled_strategy_id`, so the /schedule per-strategy detail view
    can JOIN them back to this run.

    Each scheduled_strategy carries its own `config` (BacktestRequest
    payload) and `frequency`. The phase only computes strategies whose
    `next_due_at` is in the past (or NULL — fresh entries). After a
    successful compute the row's `last_run_at` is set to now and
    `next_due_at` is advanced per `frequency` + the baked
    `rebalance_weekday` (see
    `momentum.schedule.compute_next_due_at`).

    `due_override` (smart pipeline) supplies the per-strategy due decision
    from the derived plan instead of re-reading `next_due_at` — keeping the
    pipeline's behaviour identical to the plan the UI shows. When a
    strategy isn't in the map it falls back to the `next_due_at` check.

    `dedupe_price_updates` (smart pipeline) makes a non-due strategy's
    price-update behave like the daily MTD refresh: if an identical
    snapshot already exists for the same open period + latest-price-date,
    the freshly-inserted one is deleted so re-running the tick doesn't
    grow the history.

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
    from momentum.schedule import (  # noqa: PLC0415
        compute_next_due_at as _compute_next_due_at,
    )
    from routers._schedule_snapshots import (  # noqa: PLC0415
        compute_and_save_price_update as _compute_and_save_price_update,
    )

    summaries: list[dict] = []
    errors: list[str] = []
    total = len(scheduled)

    for idx, sched in enumerate(scheduled, start=1):
        strategy_id = sched["id"]
        strategy_name = sched.get("name") or f"Strategy #{strategy_id}"
        frequency = sched.get("frequency")
        next_due_iso = sched.get("next_due_at")
        # "Due to rebalance" — the derived plan decides when supplied
        # (smart pipeline); otherwise first-run (next_due_at IS NULL) or
        # the next-due tick has arrived. Not due → price update on the last
        # rebalance's holdings.
        if due_override is not None and strategy_id in due_override:
            is_due_to_rebalance = due_override[strategy_id]
        else:
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
                        "as_of_date, holdings, latest_price_date"
                    ).eq("snapshot_id", snapshot_id).limit(1).execute()
                    pu = (pu_resp.data or [{}])[0]
                    entry["status"] = "ok"
                    # Same-period dedup (smart pipeline): if an identical
                    # snapshot already exists for this strategy's open
                    # period + latest-price-date, drop the redundant new
                    # one so re-running the tick doesn't grow history.
                    deduped_to = (
                        _dedupe_price_update(strategy_id, snapshot_id, pu)
                        if dedupe_price_updates else None
                    )
                    if deduped_to is not None:
                        entry["snapshot_id"] = deduped_to
                        entry["latest_price_date"] = pu.get("latest_price_date")
                        entry["error_message"] = (
                            f"no change since prior snapshot for period "
                            f"as_of={pu.get('as_of_date')} "
                            f"(lpd={pu.get('latest_price_date')}) — skipped"
                        )
                    else:
                        entry["snapshot_id"] = snapshot_id
                        entry["holdings_count"] = len(pu.get("holdings") or [])
                        entry["latest_price_date"] = pu.get("latest_price_date")
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
                weekday = int(cfg.get("rebalance_weekday", 0) or 0)
                next_due = (
                    _compute_next_due_at(frequency, ran_at, weekday).isoformat()
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


def _dedupe_price_update(strategy_id: int, new_snapshot_id: int, new_row: dict) -> int | None:
    """Same-period dedup, shared by the smart momentum phase + daily MTD.

    If a snapshot identical to the just-inserted one already exists for the
    SAME strategy + open period (`as_of_date`) + `latest_price_date`, delete
    the redundant new row and return the surviving snapshot_id. Returns None
    when there's no duplicate (the new row stands)."""
    new_as_of = new_row.get("as_of_date")
    new_lpd = new_row.get("latest_price_date")
    if not new_as_of or not new_lpd:
        return None
    dup = (
        supabase.table("current_picks_snapshot")
        .select("snapshot_id")
        .eq("scheduled_strategy_id", strategy_id)
        .eq("as_of_date", new_as_of)
        .eq("latest_price_date", new_lpd)
        .neq("snapshot_id", new_snapshot_id)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    if not dup.data:
        return None
    surviving = int(dup.data[0]["snapshot_id"])
    supabase.table("current_picks_snapshot").delete().eq(
        "snapshot_id", new_snapshot_id
    ).execute()
    return surviving


def _run_smart_momentum_phase(run_id: int, plan) -> None:
    """Phase 4 for the smart pipeline. Drives `_run_momentum_phase` with the
    derived plan's per-strategy due decision (so the pipeline rebalances
    exactly the strategies the plan marked due) and the daily-MTD dedup on
    the non-due price updates (so re-running the tick doesn't grow history).

    `plan` is a `planner.SmartPlan`."""
    due = {sp.strategy_id: sp.is_due for sp in plan.strategies}
    _run_momentum_phase(run_id, due_override=due, dedupe_price_updates=True)
