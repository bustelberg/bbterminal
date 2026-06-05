"""Backfill worker for newly-added scheduled strategies.

Extracted from `routers.scheduled_strategies`. Runs the backtest engine over
the recent past in a background daemon thread and persists the last few
rebalance snapshots, writing live progress to the `scheduled_strategy.
backfill_*` columns so the UI can render a progress bar. `reset_stale_backfills`
is the startup cleanup (called from `main.py`) that clears rows stuck at
'running' after a restart.

The `_spawn_backfill` / `_run_backfill` path is a dormant fallback today —
the add flow seeds from a saved backtest (`_schedule_snapshots.
_seed_snapshot_from_backtest`) instead — but is kept intact for manual
backfills and as the no-saved-backtest path.
"""
from __future__ import annotations

import asyncio
import json as _json
import logging
import threading
from datetime import date, datetime, timedelta, timezone

from deps import supabase

from ._schedule_snapshots import _latest_exit_date, compute_and_save_price_update

_log = logging.getLogger(__name__)

# Global lock so backfills run one-at-a-time. Each backfill loads the
# ACWI price panel (~3500 companies, ~30 months) and builds rolling
# signal series per company — running multiple in parallel triples
# memory + multiplies wall-clock without any throughput gain. Threads
# block on this lock; the UI shows backfill_status='running' the whole
# time, with a "Waiting for prior backfill…" message while queued.
_BACKFILL_LOCK = threading.Lock()


def reset_stale_backfills() -> int:
    """Mark any `scheduled_strategy.backfill_status='running'` rows as
    'error' on app startup. The lock + worker thread live in-process,
    so a backend restart drops both — the DB row would otherwise sit
    at 'running' forever, the UI would poll it forever (every 2s),
    and the user would think their backfill was stuck. Idempotent;
    called from `main.py` once at startup."""
    try:
        resp = (
            supabase.table("scheduled_strategy")
            .update({
                "backfill_status": "error",
                "backfill_error": "Backend restarted; previous backfill state lost. Delete + re-add to retry.",
                "backfill_finished_at": datetime.now(timezone.utc).isoformat(),
            })
            .eq("backfill_status", "running")
            .execute()
        )
        count = len(resp.data or [])
        if count > 0:
            _log.info("[startup] reset %s stale backfill_status='running' row(s)", count)
        return count
    except Exception as e:
        _log.warning(
            "[startup] failed to reset stale backfill rows: %s: %s",
            type(e).__name__, e,
        )
        return 0


def _spawn_backfill(strategy_id: int) -> None:
    """Background daemon thread that runs the backtest engine over the
    past ~4 months and persists the last 3 monthly rebalance snapshots
    as backfill entries on the new schedule entry. Called by POST
    /api/scheduled-strategies right after the row is inserted.

    Live status is written to `scheduled_strategy.backfill_*` columns
    (status / progress_pct / message / error / timestamps) so the
    frontend can poll for a real-time progress bar instead of staring
    at an empty run-history while the 30-60s compute finishes."""
    # Mark as running synchronously before spawning so the POST response
    # already carries `backfill_status='running'` for the first poll.
    try:
        supabase.table("scheduled_strategy").update({
            "backfill_status": "running",
            "backfill_progress_pct": 0,
            "backfill_message": "Starting backfill…",
            "backfill_error": None,
            "backfill_started_at": datetime.now(timezone.utc).isoformat(),
            "backfill_finished_at": None,
        }).eq("id", strategy_id).execute()
    except Exception as e:
        _log.warning(
            "[backfill] strategy=%s initial-status write failed: %s: %s",
            strategy_id, type(e).__name__, e,
        )

    def _worker() -> None:
        # Block here if another backfill is already running. While
        # waiting, surface a "queued" status so the UI's progress bar
        # makes sense rather than sitting at 0% with no message.
        if not _BACKFILL_LOCK.acquire(blocking=False):
            try:
                supabase.table("scheduled_strategy").update({
                    "backfill_message": "Waiting for prior backfill to finish…",
                }).eq("id", strategy_id).execute()
            except Exception:
                pass  # Best-effort — proceed even if the status write fails
            _BACKFILL_LOCK.acquire()  # blocks until released
        try:
            _run_backfill(strategy_id)
            _mark_backfill_done(strategy_id, ok=True, error=None)
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            _log.warning("[backfill] strategy=%s failed: %s", strategy_id, msg)
            _mark_backfill_done(strategy_id, ok=False, error=msg)
        finally:
            _BACKFILL_LOCK.release()
    threading.Thread(
        target=_worker,
        daemon=True,
        name=f"scheduled-strategy-backfill-{strategy_id}",
    ).start()


def _mark_backfill_done(strategy_id: int, *, ok: bool, error: str | None) -> None:
    """Final-status writer. Doesn't raise on DB failure (final
    log-line is enough for debugging — the user can re-trigger if the
    UI never moves past 'running')."""
    try:
        supabase.table("scheduled_strategy").update({
            "backfill_status": "done" if ok else "error",
            "backfill_progress_pct": 100 if ok else None,
            "backfill_error": error,
            "backfill_finished_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", strategy_id).execute()
    except Exception as e:
        _log.warning(
            "[backfill] strategy=%s final-status write failed: %s: %s",
            strategy_id, type(e).__name__, e,
        )


class _BackfillProgressWriter:
    """Throttled writer that pushes the latest progress message + pct
    to `scheduled_strategy.backfill_*` columns. The backtest engine
    can emit progress events ~10/s; we throttle to a short min-interval
    so the DB isn't hammered, but we always let a write through when
    either pct or message *changed* — the user perceives a frozen bar
    when phase transitions get swallowed by the throttle."""
    def __init__(self, strategy_id: int, min_interval: float = 0.3):
        import time as _t  # noqa: PLC0415
        self._time = _t
        self._strategy_id = strategy_id
        self._min_interval = min_interval
        self._last_write = 0.0
        self._last_pct: int | None = None
        self._last_message: str | None = None

    def write(self, *, pct: int | None, message: str | None) -> None:
        now = self._time.time()
        capped_pct = max(0, min(100, int(pct))) if pct is not None else None
        capped_msg = message[:400] if message is not None else None
        changed = (
            (capped_pct is not None and capped_pct != self._last_pct)
            or (capped_msg is not None and capped_msg != self._last_message)
        )
        # Throttle only when nothing changed — don't drop phase
        # transitions even if they're tight against the previous write.
        if not changed and now - self._last_write < self._min_interval:
            return
        if changed and now - self._last_write < self._min_interval and capped_pct == self._last_pct:
            # Same pct, message changed: still respect a tiny floor (50ms)
            # so we don't write 20 times/sec on a chatty engine phase.
            if now - self._last_write < 0.05:
                return
        self._last_write = now
        update: dict = {}
        if capped_pct is not None:
            update["backfill_progress_pct"] = capped_pct
            self._last_pct = capped_pct
        if capped_msg is not None:
            update["backfill_message"] = capped_msg
            self._last_message = capped_msg
        if not update:
            return
        try:
            supabase.table("scheduled_strategy").update(update).eq(
                "id", self._strategy_id
            ).execute()
        except Exception as e:
            _log.warning(
                "[backfill] strategy=%s progress write failed: %s: %s",
                self._strategy_id, type(e).__name__, e,
            )


def _run_backfill(strategy_id: int) -> None:
    """Synchronous backfill core. Drives the backtest engine, extracts
    the last 5 rebalance period records, persists each as a
    `current_picks_snapshot` with `is_backfill=True, kind='rebalance'`.

    Progress events from the engine are forwarded (throttled) to the
    scheduled_strategy row so the frontend can render a live bar."""
    # Local imports — keep module boot cheap.
    from routers.momentum.backtest_stream.models import BacktestRequest  # noqa: PLC0415
    from routers.momentum.backtest_stream.stream import (  # noqa: PLC0415
        _momentum_backtest_stream,
    )

    sched_resp = (
        supabase.table("scheduled_strategy")
        .select("id, name, frequency, config")
        .eq("id", strategy_id)
        .limit(1)
        .execute()
    )
    if not sched_resp.data:
        _log.warning("[backfill] strategy=%s not found, skipping", strategy_id)
        return
    sched = sched_resp.data[0]
    config = dict(sched.get("config") or {})
    if not config:
        raise RuntimeError(f"Strategy #{strategy_id} has no config")
    name = sched.get("name") or f"Strategy #{strategy_id}"
    frequency = sched.get("frequency") or "weekly"

    # Map the scheduled_strategy.frequency enum to the backtest
    # engine's rebalance_frequency. Every frequency maps 1:1 to the
    # engine's equivalent — the engine's every_N_months grid is anchored
    # (see momentum.backtest.dates), so its rebalance Mondays match what
    # the live pipeline would have produced regardless of start_date.
    bt_freq_map = {
        "daily": "daily",
        "weekly": "weekly",
        "monthly": "monthly",
        "bimonthly": "every_2_months",
        "quarterly": "every_3_months",
    }
    bt_freq = bt_freq_map.get(frequency, "monthly")
    # Per-frequency lookback. Has to cover BOTH:
    #   - 5 rebalance points (the visible rows), spanning N × frequency
    #     back from today, AND
    #   - 12 months of signal history before the OLDEST rebalance,
    #     because mom_12_1 + other momentum signals need a year of
    #     prior prices. Without that buffer the oldest rebalance sees
    #     fewer eligible companies → fewer sectors → empty chips.
    lookback_days = {
        "daily": 400,           # 5 trading days + 12mo signal lookback
        "weekly": 450,          # 5w rebalance span + 12mo signal lookback
        "monthly": 550,         # 5mo + 12mo
        "every_2_months": 730,  # 10mo + 12mo
        "every_3_months": 900,  # 15mo + 12mo
    }.get(bt_freq, 550)
    today = date.today()
    # Pop any variant-add transport fields that don't belong in
    # BacktestRequest (backfill_start_date / _end_date were a prior
    # experiment; the variant-add flow now skips backfill entirely and
    # uses backtest_run_id instead).
    config.pop("backfill_start_date", None)
    config.pop("backfill_end_date", None)
    start = today - timedelta(days=lookback_days)
    config["mode"] = "backtest"
    config["start_date"] = start.isoformat()
    config["end_date"] = today.isoformat()
    config["rebalance_frequency"] = bt_freq
    config["force_recompute"] = True
    config["db_only"] = False
    config.pop("variants", None)
    config.pop("n_trials", None)

    try:
        req = BacktestRequest(**config)
    except Exception as e:
        raise RuntimeError(
            f"Strategy {strategy_id} config doesn't validate as BacktestRequest: {e}"
        )

    progress = _BackfillProgressWriter(strategy_id)
    captured_result: dict | None = None
    captured_error: str | None = None

    async def _drain() -> None:
        nonlocal captured_result, captured_error
        async for chunk in _momentum_backtest_stream(req):
            if not isinstance(chunk, str) or not chunk.startswith("data: "):
                continue
            try:
                evt = _json.loads(chunk[len("data: "):].strip())
            except _json.JSONDecodeError:
                continue
            t = evt.get("type")
            if t == "progress":
                # Engine pct is on [0, 100]; we reserve the top ~5% for
                # the local insert step below so the bar reaches 100
                # only when everything is durable.
                pct = evt.get("pct")
                scaled = int(pct * 0.95) if isinstance(pct, (int, float)) else None
                await asyncio.to_thread(
                    progress.write,
                    pct=scaled,
                    message=evt.get("message"),
                )
            elif t == "result":
                captured_result = evt.get("data") or {}
            elif t == "error":
                captured_error = evt.get("message") or "unknown error"

    asyncio.run(_drain())
    if captured_error:
        raise RuntimeError(f"Backtest engine error: {captured_error}")
    if not captured_result:
        raise RuntimeError("Backtest engine produced no result")

    # `monthly_records` is the array of per-rebalance period records
    # the engine emits. With anchored every_N_months grids + daily/
    # weekly using true trading days/Mondays, each record's `date` is
    # the exact Monday (or trading day) the live pipeline would have
    # rebalanced on. We persist each record verbatim — no relabel — so
    # the backfill's as_of_date and period_return_pct exactly match
    # what `/backtest` produces for the same frequency.
    monthly = captured_result.get("monthly_records") or []
    if not monthly:
        _log.info("[backfill] strategy=%s no monthly records produced", strategy_id)
        return
    # Newest 5 records, ordered newest-first for tidier insert logs.
    # Variant-add flow (the "+ Schedule" button on /backtest) skips
    # this whole `_run_backfill` codepath entirely — it preserves the
    # full BacktestResult via `scheduled_strategy.backtest_run_id`
    # instead. Manual /schedule adds without a saved backtest still
    # get a 5-period preview so the run history isn't empty until
    # the first live pipeline tick.
    engine_tail = list(reversed(monthly[-5:]))

    progress.write(pct=95, message=f"Persisting {len(engine_tail)} backfill snapshots…")
    persisted = 0
    for rec in engine_tail:
        rec_date = (rec.get("date") or "")[:10]
        if not rec_date:
            _log.warning(
                "[backfill] strategy=%s record missing date, skipping: %s",
                strategy_id, rec,
            )
            continue
        try:
            row = {
                "triggered_by": "auto",
                "as_of_date": rec_date,
                "latest_price_date": _latest_exit_date(rec),
                "config": sched.get("config"),
                "holdings": rec.get("holdings") or [],
                "daily_picks": [],
                "strategy_hash": None,
                "name": name,
                "kind": "rebalance",
                "is_backfill": True,
                "scheduled_strategy_id": strategy_id,
                # Engine's `portfolio_return_pct` is the % gain of this
                # rebalance's picks over its holding period (or MTD for
                # the trailing open period). Matches /backtest exactly
                # because the rebalance grid is shared.
                "period_return_pct": rec.get("portfolio_return_pct"),
            }
            supabase.table("current_picks_snapshot").insert(row).execute()
            persisted += 1
        except Exception as e:
            _log.warning(
                "[backfill] strategy=%s target=%s insert failed: %s: %s",
                strategy_id, rec_date, type(e).__name__, e,
            )
    # After the rebalance rows are durable, refresh the OPEN period
    # with each holding's actual-latest close. The engine's open-period
    # `as_of_date` is the EARLIEST of per-holding latest-close dates
    # (so every holding lines up on one shared date for the curve);
    # that's the right call for the equity curve, but it means a
    # single thinly-traded name with a stale close drags the whole
    # period's MTD back days or weeks. `compute_and_save_price_update`
    # fetches each cid's latest close independently and writes a
    # `price_update` row whose `period_return_pct` overrides the open
    # period in `_compute_period_returns`'s walker — so /schedule
    # immediately reads the freshest per-holding prices instead of
    # waiting for the next Tuesday 02:00 UTC tick to fire.
    try:
        pu_id = compute_and_save_price_update(
            strategy_id, ingest_run_id=None, is_backfill=True,
        )
        if pu_id is not None:
            _log.info(
                "[backfill] strategy=%s post-backfill price_update=%s",
                strategy_id, pu_id,
            )
    except Exception as e:
        # Don't fail the whole backfill on a price_update hiccup —
        # the rebalance rows are durable, the next pipeline tick
        # will retry.
        _log.warning(
            "[backfill] strategy=%s post-backfill price_update failed: %s: %s",
            strategy_id, type(e).__name__, e,
        )

    progress.write(
        pct=100,
        message=f"Backfill complete: {persisted} of {len(engine_tail)} snapshots persisted",
    )
    _log.info(
        "[backfill] strategy=%s wrote %s/%s backfill snapshots",
        strategy_id, persisted, len(engine_tail),
    )
