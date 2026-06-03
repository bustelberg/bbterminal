"""CRUD + run-history endpoints for the schedule.

Each `scheduled_strategy` row is self-contained: it carries its own
`config` (BacktestRequest shape) + `frequency`. The pipeline's momentum
phase iterates every enabled row on each weekly Tuesday tick and
produces one snapshot per strategy: a `rebalance` (fresh picks) when
the strategy is due, or a `price_update` (last rebalance's holdings
re-priced) otherwise.

The `frequency` enum controls the rebalance cadence:
  daily, weekly  → run every Tuesday tick
  monthly        → run on the Tuesday after the 1st of each new month
  bimonthly      → same, every 2nd month
  quarterly      → same, every 3rd month

The pipeline tick fires Tuesday 02:00 UTC (after Monday close). Both
`last_run_at` and `next_due_at` are stored in UTC and align to that
Tuesday tick — so the UI can render a clean "next: Tue 2026-06-02
02:00 UTC" without rederiving from the frequency every render.
"""
from __future__ import annotations

import asyncio
import calendar
import json as _json
import logging
import threading
from datetime import date, datetime, time, timedelta, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from deps import supabase, IN_CHUNK_SIZE

_log = logging.getLogger(__name__)

router = APIRouter(tags=["schedule"])

FREQUENCIES = ("daily", "weekly", "monthly", "bimonthly", "quarterly")

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


# ─── Pydantic shapes ──────────────────────────────────────────────


class ScheduledStrategyCreate(BaseModel):
    """Body for POST. `config` is the full BacktestRequest payload (we
    don't re-validate it here; the pipeline drives it through
    `BacktestRequest(**config)` and surfaces any failure as a per-
    strategy error in the run's templates_summary).

    `backtest_run_id` is REQUIRED. Every scheduled strategy must
    originate from a backtested variant — that gives /schedule a
    persistent equity-curve / monthly-history record to anchor the
    live snapshots against. The manual-add flow has been retired."""
    name: str
    frequency: str
    config: dict
    backtest_run_id: int
    # Optional go-live date. NULL/omitted → the strategy's created_at is
    # used as the equity-curve marker + live cutoff.
    start_date: date | None = None


class ScheduledStrategyPatch(BaseModel):
    enabled: bool | None = None
    # Rename the strategy. Whitespace-trimmed; empty/blank is rejected.
    name: str | None = None
    # Configurable go-live date (red dashed marker + live cutoff). A
    # present `start_date` sets it; `clear_start_date=True` resets it to
    # NULL (fall back to created_at). They're mutually exclusive — a
    # non-null start_date wins if both are sent.
    start_date: date | None = None
    clear_start_date: bool | None = None
    # Edit the rebalance weekday (Mon=0..Sun=6) in-place on the stored
    # config blob. This is the one config field we allow patching on the
    # schedule — it's a pure scheduling knob (which weekday of the period
    # the strategy rebalances on) and doesn't change selection identity,
    # so it's safe to tweak without a delete + re-add. None = leave as-is.
    rebalance_weekday: int | None = None


# ─── Frequency math ───────────────────────────────────────────────


def _add_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    last_day = calendar.monthrange(y, m)[1]
    return d.replace(year=y, month=m, day=min(d.day, last_day))


def _first_monday_on_or_after(d: date) -> date:
    """Mon=0, Sun=6 → days to add to land on Monday."""
    return d + timedelta(days=(0 - d.weekday()) % 7)


def compute_next_due_at(frequency: str, just_ran_at_utc: datetime) -> datetime:
    """Given the strategy just ran at a pipeline tick (Tue 02:00 UTC),
    return when the next eligible pipeline tick will be per frequency.

    - daily/weekly: the next Tuesday (just_ran + 7 days)
    - monthly/bimonthly/quarterly: the Tuesday following the first
      Monday on-or-after the 1st of the next/2nd/3rd month."""
    just_ran_date = just_ran_at_utc.date()
    if frequency in ("daily", "weekly"):
        next_tue = just_ran_date + timedelta(days=7)
    else:
        months_step = {"monthly": 1, "bimonthly": 2, "quarterly": 3}[frequency]
        target_month_first = _add_months(just_ran_date.replace(day=1), months_step)
        first_monday = _first_monday_on_or_after(target_month_first)
        # Pipeline tick that captures that Monday's close fires the next
        # day (Tuesday 02:00 UTC).
        next_tue = first_monday + timedelta(days=1)
    return datetime.combine(next_tue, time(2, 0), tzinfo=timezone.utc)


def compute_and_save_price_update(
    strategy_id: int,
    ingest_run_id: int | None,
    is_backfill: bool = False,
    as_of_iso: str | None = None,
) -> int | None:
    """Build a price_update snapshot for `strategy_id` by re-pricing the
    most recent rebalance's holdings against the latest available close
    prices. Returns the new snapshot_id, or None when no prior rebalance
    exists (nothing to update from).

    Output snapshot fields:
      * `holdings`: same set as the rebalance, but each holding's
        `exit_price_local` + `exit_date` + `forward_return_pct` are
        updated to reflect the latest close.
      * `as_of_date`: unchanged from the rebalance (the entry point
        the returns are measured against).
      * `latest_price_date`: the most recent close-price date seen
        across holdings.
      * `kind`: 'price_update'.

    Used by:
      - the weekly pipeline tick, for every enabled strategy that
        isn't due to rebalance on this tick;
      - the backfill flow, for past Tuesdays where the strategy
        wouldn't have rebalanced (`is_backfill=True`).
    """
    # Order by `as_of_date` (the rebalance date itself), NOT `created_at`.
    # Backfill inserts every historical period's rebalance row in one
    # batch, so all 5 of them share a created_at within milliseconds of
    # each other — `created_at desc` then picks an essentially random
    # row, often the OLDEST as_of_date (the period inserted last by the
    # backfill loop). Ordering by `as_of_date desc` deterministically
    # picks the most-recent rebalance, which is what "the strategy's
    # current open period" actually means.
    rebal_resp = (
        supabase.table("current_picks_snapshot")
        .select("*")
        .eq("scheduled_strategy_id", strategy_id)
        .eq("kind", "rebalance")
        .order("as_of_date", desc=True)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    if not rebal_resp.data:
        return None
    rebal = rebal_resp.data[0]
    holdings = rebal.get("holdings") or []
    if not holdings:
        return None

    # Fetch the latest close-price observation for every holding's
    # company_id in one batched query. We `order desc` and pick the
    # first hit per cid in-process — Postgres has no efficient
    # DISTINCT ON via PostgREST.
    cids = [h.get("company_id") for h in holdings if h.get("company_id") is not None]
    latest_by_cid: dict[int, dict] = {}
    if cids:
        # Chunk by IN_CHUNK_SIZE to stay under PostgREST URL limits.
        for i in range(0, len(cids), IN_CHUNK_SIZE):
            chunk = cids[i:i + IN_CHUNK_SIZE]
            p_resp = (
                supabase.table("metric_data")
                .select("company_id, target_date, numeric_value")
                .eq("metric_code", "close_price")
                .in_("company_id", chunk)
                .order("target_date", desc=True)
                .execute()
            )
            for r in (p_resp.data or []):
                cid = r["company_id"]
                if cid not in latest_by_cid:
                    latest_by_cid[cid] = r

    updated_holdings: list[dict] = []
    weighted_return_sum = 0.0
    total_weight = 0.0
    latest_price_date: str | None = None
    for h in holdings:
        new_h = dict(h)
        cid = h.get("company_id")
        entry_local = h.get("entry_price_local")
        weight = float(h.get("weight") or 0.0)
        latest = latest_by_cid.get(cid)
        if latest and entry_local:
            current_local = float(latest["numeric_value"])
            target_d = str(latest["target_date"])[:10]
            new_h["exit_price_local"] = current_local
            new_h["exit_date"] = target_d
            ret = ((current_local - float(entry_local)) / float(entry_local)) * 100.0
            new_h["forward_return_pct"] = ret
            if latest_price_date is None or target_d > latest_price_date:
                latest_price_date = target_d
            weighted_return_sum += ret * weight
            total_weight += weight
        updated_holdings.append(new_h)

    portfolio_return = weighted_return_sum / total_weight if total_weight > 0 else None

    new_row = {
        "triggered_by": "auto",
        "as_of_date": rebal["as_of_date"],
        "latest_price_date": latest_price_date,
        "config": rebal.get("config"),
        "holdings": updated_holdings,
        "daily_picks": [],
        "strategy_hash": rebal.get("strategy_hash"),
        "name": rebal.get("name"),
        "kind": "price_update",
        "is_backfill": is_backfill,
        "ingest_run_id": ingest_run_id,
        "scheduled_strategy_id": strategy_id,
        # Weighted aggregate of per-holding returns since the prior
        # rebalance — the % gain so far on this position. Renders on
        # the run-history row.
        "period_return_pct": portfolio_return,
    }
    ins = supabase.table("current_picks_snapshot").insert(new_row).execute()
    if not ins.data:
        return None
    # Best-effort log; signature noise is intentional for debugging
    # later.
    import logging  # noqa: PLC0415
    logging.getLogger(__name__).info(
        "[price_update] strategy=%s prior_rebal=%s new=%s portfolio_return=%.2f%% "
        "(backfill=%s)",
        strategy_id, rebal.get("snapshot_id"), ins.data[0].get("snapshot_id"),
        portfolio_return or 0.0, is_backfill,
    )
    return int(ins.data[0]["snapshot_id"])


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


def _coerce_as_of_date(raw: str | None) -> str:
    """Backtest period dates are YYYY-MM strings; current_picks_snapshot
    expects YYYY-MM-DD. Convert by appending '-01'."""
    if not raw:
        return date.today().isoformat()
    s = str(raw)
    if len(s) == 7 and s[4] == "-":  # YYYY-MM
        return s + "-01"
    return s[:10]


def _latest_exit_date(rec: dict) -> str | None:
    """Highest `exit_date` across the record's holdings — a reasonable
    proxy for the snapshot's `latest_price_date`."""
    out: str | None = None
    for h in (rec.get("holdings") or []):
        d = h.get("exit_date") or h.get("entry_date")
        if d and (out is None or d > out):
            out = d
    return out


def _initial_next_due_at(reference_now: datetime | None = None) -> datetime:
    """When a strategy is freshly added (no last run yet), it's due at
    the next upcoming Tuesday 02:00 UTC pipeline tick. Returns that
    moment; the pipeline then picks it up the next time it fires."""
    now = reference_now or datetime.now(timezone.utc)
    # Tuesday is weekday 1; days to next Tuesday on-or-after today.
    days_ahead = (1 - now.weekday()) % 7
    if days_ahead == 0 and now.time() >= time(2, 0):
        days_ahead = 7  # Already past today's 02:00 tick → next week
    target = (now + timedelta(days=days_ahead)).date()
    return datetime.combine(target, time(2, 0), tzinfo=timezone.utc)


# ─── Hydration helper ─────────────────────────────────────────────


def _extract_sectors(holdings: list[dict] | None) -> list[dict]:
    """Distinct sectors from a holdings list, ordered by count desc then
    alpha. Empty list when no holdings or no sectors. Used for the
    /schedule collapsed-row summary."""
    if not holdings:
        return []
    counts: dict[str, int] = {}
    for h in holdings:
        sec = (h.get("sector") or "").strip()
        if not sec:
            continue
        counts[sec] = counts.get(sec, 0) + 1
    if not counts:
        return []
    return [
        {"sector": sec, "count": cnt}
        for sec, cnt in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    ]


def _compute_period_returns(snapshots: list[dict], today: date) -> dict:
    """MTD + YTD returns for a strategy.

    Walks the strategy's full snapshot history chronologically to build
    a relative equity curve, then reads off the ratio between current
    equity and equity at the start of the current month / year.

    Snapshot convention: `period_return_pct` on each row is the running
    return for THAT row's open period as of the row's `latest_price_date`.
    For a BACKFILL rebalance this is the full closed-period return (the
    backfill knew the whole period at compute time). For a LIVE rebalance
    inserted by the pipeline tick it's 0% at creation, then refreshed by
    the daily MTD price_update flow.

    Walker rules:
      - On rebalance: close prior period at its running return, then
        open a new one whose initial running return = THIS row's stored
        `period_return_pct` (full-period value for backfill, 0 for
        live).
      - On price_update: refresh the open period's running return with
        this row's `period_return_pct`.

    This treats backfill + live snapshots uniformly — both have the
    same "running return on this row" semantic.

    Returns {mtd_return_pct, ytd_return_pct, as_of_date} all None-able.
    `as_of_date` is the latest_price_date of the newest snapshot, surfaced
    so the UI can render "+12.7% (as of 2026-05-22)" without a second
    lookup."""
    if not snapshots:
        return {"mtd_return_pct": None, "ytd_return_pct": None, "as_of_date": None}

    # Iterate in chronological order; snapshots are passed already sorted
    # by (latest_price_date asc, created_at asc).
    open_period_return_pct = 0.0
    open_period_start_equity = 1.0
    last_rebalance_eff_date: str | None = None
    # Equity curve points keyed by effective date; multiple snapshots on
    # the same date overwrite each other (last wins), which is what we
    # want — a price_update later in the day reflects more-current data.
    curve: list[tuple[str, float]] = []

    for s in snapshots:
        eff_date = (s.get("latest_price_date") or s.get("as_of_date") or "")
        eff_date = str(eff_date)[:10]
        if not eff_date:
            continue
        kind = s.get("kind") or "rebalance"
        pct = s.get("period_return_pct")
        if kind == "rebalance":
            # Close prior period at its last-known running return.
            equity_after_close = open_period_start_equity * (1.0 + open_period_return_pct / 100.0)
            # Open new period. The new period's initial running return =
            # whatever this rebalance row stored — for a backfill row
            # that's the full closed-period return; for a live-pipeline
            # rebalance row that's 0 (no observed return yet).
            open_period_start_equity = equity_after_close
            open_period_return_pct = float(pct) if pct is not None else 0.0
            last_rebalance_eff_date = eff_date
        else:
            # price_update: refresh the open period's running return.
            if pct is not None:
                open_period_return_pct = float(pct)
        # Snapshot the current equity (open period running return applied).
        equity_now = open_period_start_equity * (1.0 + open_period_return_pct / 100.0)
        curve.append((eff_date, equity_now))

    if not curve:
        return {"mtd_return_pct": None, "ytd_return_pct": None, "as_of_date": None}

    latest_date, latest_equity = curve[-1]
    month_start = today.replace(day=1).isoformat()
    year_start = today.replace(month=1, day=1).isoformat()

    # YTD anchor: last equity point strictly before year start, else 1.0
    # (strategy started inside the year — measure from inception).
    ytd_anchor = 1.0
    for d, e in curve:
        if d < year_start:
            ytd_anchor = e

    # MTD anchor:
    #   Default — last equity point strictly before month_start.
    #   Override — when the latest rebalance fired IN this month, anchor
    #     at the open-period start equity (post-close of the prior period
    #     by THIS rebalance). That way MTD reads as "return since the
    #     latest rebalance" for cadences where the rebalance landed in
    #     this month (monthly / weekly / daily), rather than including a
    #     chunk of the prior month's open period that we can't cleanly
    #     attribute to either calendar month.
    mtd_anchor = 1.0
    for d, e in curve:
        if d < month_start:
            mtd_anchor = e
    if last_rebalance_eff_date and last_rebalance_eff_date >= month_start:
        mtd_anchor = open_period_start_equity

    def _pct(end: float, start: float) -> float | None:
        if start <= 0:
            return None
        return round((end / start - 1.0) * 100.0, 2)

    return {
        "mtd_return_pct": _pct(latest_equity, mtd_anchor),
        "ytd_return_pct": _pct(latest_equity, ytd_anchor),
        "as_of_date": latest_date,
    }


def _hydrate(rows: list[dict]) -> list[dict]:
    """Attach the most recent snapshot summary + period-return rollups to
    each row, joined via the `current_picks_snapshot.scheduled_strategy_id`
    FK.

    Two queries (both batched by strategy_id), each pulling only what's
    needed:
      1. Latest-snapshot holdings -- so we can extract sectors + count.
      2. Full snapshot history without holdings -- for the MTD/YTD walk.
    """
    if not rows:
        return []
    sched_ids = [r["id"] for r in rows]

    # Query 1: every snapshot row, no holdings yet (so the historical walk
    # stays cheap). Ordered chronologically; the period-return helper
    # assumes ascending.
    history_resp = (
        supabase.table("current_picks_snapshot")
        .select(
            "snapshot_id, scheduled_strategy_id, ingest_run_id, "
            "kind, as_of_date, latest_price_date, period_return_pct, created_at"
        )
        .in_("scheduled_strategy_id", sched_ids)
        .order("latest_price_date", desc=False)
        .order("created_at", desc=False)
        .execute()
    )
    history_by_sched: dict[int, list[dict]] = {}
    for s in history_resp.data or []:
        sid = s.get("scheduled_strategy_id")
        if sid is None:
            continue
        history_by_sched.setdefault(sid, []).append(s)

    # Query 2: holdings of just the latest snapshot per strategy. Doing
    # this as a separate call (rather than embedding holdings in query 1)
    # avoids hauling the full per-snapshot holdings blob across the wire
    # for every historical row.
    latest_ids: list[int] = []
    for sid, hist in history_by_sched.items():
        if hist:
            latest_ids.append(int(hist[-1]["snapshot_id"]))
    holdings_by_snap: dict[int, list[dict]] = {}
    if latest_ids:
        for start in range(0, len(latest_ids), IN_CHUNK_SIZE):
            chunk = latest_ids[start : start + IN_CHUNK_SIZE]
            h_resp = (
                supabase.table("current_picks_snapshot")
                .select("snapshot_id, holdings")
                .in_("snapshot_id", chunk)
                .execute()
            )
            for hr in h_resp.data or []:
                holdings_by_snap[int(hr["snapshot_id"])] = hr.get("holdings") or []

    today = date.today()

    out: list[dict] = []
    for r in rows:
        hist = history_by_sched.get(r["id"]) or []
        latest = hist[-1] if hist else None
        holdings = holdings_by_snap.get(int(latest["snapshot_id"])) if latest else None
        last_snapshot: dict | None = None
        if latest:
            returns = _compute_period_returns(hist, today)
            last_snapshot = {
                "snapshot_id": latest["snapshot_id"],
                "ingest_run_id": latest.get("ingest_run_id"),
                "created_at": latest["created_at"],
                "latest_price_date": latest.get("latest_price_date"),
                "holdings_count": len(holdings or []),
                "sectors": _extract_sectors(holdings),
                "mtd_return_pct": returns["mtd_return_pct"],
                "ytd_return_pct": returns["ytd_return_pct"],
                "as_of_date": returns["as_of_date"] or latest.get("latest_price_date"),
            }
        out.append({
            "id": r["id"],
            "name": r.get("name") or f"Strategy #{r['id']}",
            "frequency": r.get("frequency"),
            "config": r.get("config") or {},
            "enabled": r.get("enabled", True),
            "created_at": r.get("created_at"),
            "updated_at": r.get("updated_at"),
            # Configurable go-live date (red dashed equity-curve marker +
            # live cutoff). NULL → frontend defaults to created_at.
            "start_date": r.get("start_date"),
            "last_run_at": r.get("last_run_at"),
            "next_due_at": r.get("next_due_at"),
            "backfill": {
                "status": r.get("backfill_status"),
                "progress_pct": r.get("backfill_progress_pct"),
                "message": r.get("backfill_message"),
                "error": r.get("backfill_error"),
                "started_at": r.get("backfill_started_at"),
                "finished_at": r.get("backfill_finished_at"),
            },
            "last_snapshot": last_snapshot,
        })
    return out


# ─── Endpoints ────────────────────────────────────────────────────


@router.get("/api/scheduled-strategies/held-companies")
async def list_held_companies():
    """Pooled set of companies currently held across every enabled
    scheduled strategy. Drives the /schedule "Misc jobs → Currently held
    companies" panel — gives the user full transparency over which
    company is in which strategy's portfolio, when each position was
    opened, and where the next daily price refresh will be writing data.

    Aggregation: for each enabled strategy, take the most-recent
    `current_picks_snapshot` (any kind — rebalance or price_update;
    they share the same holdings shape). Pool the holdings, dedup by
    `company_id`, and attach one `held_by` entry per strategy that holds
    that company. Companies with no snapshot yet are skipped silently.

    Returns:
        {
          "total_companies": int,           # distinct companies pooled
          "total_strategies": int,          # strategies contributing
          "freshness_summary": {            # what date prices we actually have
            "latest_close_date": str|None,  # max(target_date) across held companies
            "fresh_count": int,             # companies at the latest_close_date
            "stale_count": int,             # companies with an older latest target_date
            "missing_count": int,           # companies with NO close_price data at all
          },
          "companies": [{
            "company_id", "ticker", "exchange",
            "company_name", "sector",
            "latest_close_price_date": str|None,  # max(target_date) in metric_data for this company
            "held_by": [{
              "strategy_id", "strategy_name",
              "snapshot_id", "snapshot_kind",  # "rebalance"|"price_update"
              "as_of_date",                    # when this position was opened
              "latest_price_date",             # most recent close seen for it
              "target_weight",                 # fractional, 0..1
              "score", "entry_price_local", "entry_date",
            }]
          }]
        }
    """
    def _query() -> dict:
        # Step 1 — every enabled scheduled strategy.
        strat_resp = (
            supabase.table("scheduled_strategy")
            .select("id, name")
            .eq("enabled", True)
            .execute()
        )
        strategies = strat_resp.data or []
        if not strategies:
            return {"total_companies": 0, "total_strategies": 0, "companies": []}
        strategy_name_by_id: dict[int, str] = {
            int(s["id"]): (s.get("name") or f"Strategy #{s['id']}")
            for s in strategies
        }
        sched_ids = list(strategy_name_by_id.keys())

        # Step 2 — latest snapshot per strategy (regardless of kind).
        snap_resp = (
            supabase.table("current_picks_snapshot")
            .select(
                "snapshot_id, scheduled_strategy_id, kind, as_of_date, "
                "latest_price_date, holdings, created_at"
            )
            .in_("scheduled_strategy_id", sched_ids)
            .order("created_at", desc=True)
            .execute()
        )
        latest_by_sched: dict[int, dict] = {}
        for s in (snap_resp.data or []):
            sid = s.get("scheduled_strategy_id")
            if sid is None or sid in latest_by_sched:
                continue
            latest_by_sched[int(sid)] = s

        # Step 3 — pool holdings, attaching attribution per strategy.
        # Keyed by company_id; each entry's held_by list grows as we
        # iterate. Strategies with no snapshot yet are silently
        # skipped (first-run before backfill or pipeline ever touched them).
        pooled: dict[int, dict] = {}
        for sched_id, snap in latest_by_sched.items():
            for h in (snap.get("holdings") or []):
                cid_raw = h.get("company_id")
                if cid_raw is None:
                    continue
                cid = int(cid_raw)
                bucket = pooled.setdefault(cid, {
                    "company_id": cid,
                    "ticker": h.get("ticker"),
                    "company_name": h.get("company_name"),
                    "sector": h.get("sector"),
                    "exchange": "",  # filled in step 4 below
                    "held_by": [],
                })
                # Holdings stored on the snapshot don't carry exchange;
                # but they do carry ticker + name + sector. We pick the
                # first non-null value across strategies for stability,
                # then overwrite from the company table below.
                if not bucket.get("ticker"):
                    bucket["ticker"] = h.get("ticker")
                if not bucket.get("company_name"):
                    bucket["company_name"] = h.get("company_name")
                if not bucket.get("sector"):
                    bucket["sector"] = h.get("sector")
                bucket["held_by"].append({
                    "strategy_id": sched_id,
                    "strategy_name": strategy_name_by_id[sched_id],
                    "snapshot_id": snap.get("snapshot_id"),
                    "snapshot_kind": snap.get("kind"),
                    "as_of_date": snap.get("as_of_date"),
                    "latest_price_date": snap.get("latest_price_date"),
                    "target_weight": float(h.get("weight") or 0.0),
                    "score": h.get("score"),
                    "entry_price_local": h.get("entry_price_local"),
                    "entry_date": h.get("entry_date") or snap.get("as_of_date"),
                })

        if not pooled:
            return {
                "total_companies": 0,
                "total_strategies": len(latest_by_sched),
                "companies": [],
            }

        # Step 4 — exchange lookup. Holdings JSONB doesn't include the
        # GuruFocus exchange code; fetch it from `company` joined to
        # `gurufocus_exchange`. Batched by IN_CHUNK_SIZE to stay under
        # the PostgREST URL-length window.
        cids = list(pooled.keys())
        for start in range(0, len(cids), IN_CHUNK_SIZE):
            chunk = cids[start : start + IN_CHUNK_SIZE]
            comp_resp = (
                supabase.table("company")
                .select(
                    "company_id, company_name, gurufocus_ticker, "
                    "gurufocus_exchange:gurufocus_exchange(exchange_code)"
                )
                .in_("company_id", chunk)
                .execute()
            )
            for r in (comp_resp.data or []):
                cid = int(r["company_id"])
                if cid not in pooled:
                    continue
                exch = (r.get("gurufocus_exchange") or {}).get("exchange_code") or ""
                pooled[cid]["exchange"] = exch
                # Prefer the authoritative ticker/name from `company`
                # — the snapshot's holdings can carry slightly stale
                # values after a renamed-ticker override.
                if r.get("gurufocus_ticker"):
                    pooled[cid]["ticker"] = r["gurufocus_ticker"]
                if r.get("company_name"):
                    pooled[cid]["company_name"] = r["company_name"]

        # Step 5 — freshness lookup. For each held company, fetch its
        # actual latest `close_price` target_date from `metric_data`
        # via the `company_latest_close_price_dates` RPC (one row per
        # company, NULL when no close_price data exists). Paginated to
        # bypass the cloud project's PostgREST max-rows cap — see
        # `project_postgrest_max_rows_trap`.
        latest_close_by_cid: dict[int, str | None] = {}
        try:
            page = 1000
            offset = 0
            for _attempt in range(20):
                latest_resp = (
                    supabase.rpc("company_latest_close_price_dates", {})
                    .range(offset, offset + page - 1)
                    .execute()
                )
                batch = latest_resp.data or []
                if not batch:
                    break
                added = 0
                for row in batch:
                    cid_raw = row.get("company_id")
                    if cid_raw is None:
                        continue
                    cid = int(cid_raw)
                    if cid in latest_close_by_cid:
                        continue
                    latest_close_by_cid[cid] = row.get("latest_target_date")
                    added += 1
                if added == 0 or len(batch) < page:
                    break
                offset += page
        except Exception:
            # If the RPC is unavailable (migration not applied) the
            # endpoint still returns the holdings — freshness data
            # just renders as "unknown" in the UI.
            latest_close_by_cid = {}

        for cid, bucket in pooled.items():
            bucket["latest_close_price_date"] = latest_close_by_cid.get(cid)

        # Compute the freshness summary. `latest_close_date` is the
        # max across the held set — i.e. "the freshest close-price
        # observation that exists for any company we currently hold".
        # `fresh_count` is "how many held companies actually have data
        # through this date"; `stale_count` is the held companies that
        # don't (their last close is older). `missing_count` is the
        # subset whose `latest_close_price_date` is None entirely (no
        # close_price data ever — usually a newly added ticker that
        # hasn't been pulled yet).
        dates = [v for v in latest_close_by_cid.values() if v]
        latest_close_date = max(dates) if dates else None
        fresh_count = 0
        stale_count = 0
        missing_count = 0
        for cid in pooled.keys():
            d = pooled[cid].get("latest_close_price_date")
            if d is None:
                missing_count += 1
            elif d == latest_close_date:
                fresh_count += 1
            else:
                stale_count += 1

        companies = list(pooled.values())
        # Sort by (sector, ticker) for stable rendering. Empty sector
        # bucket lands at the bottom.
        companies.sort(key=lambda c: (
            (c.get("sector") or "~"),  # ~ sorts after letters in ASCII
            (c.get("ticker") or "").upper(),
        ))

        return {
            "total_companies": len(companies),
            "total_strategies": len(latest_by_sched),
            "freshness_summary": {
                "latest_close_date": latest_close_date,
                "fresh_count": fresh_count,
                "stale_count": stale_count,
                "missing_count": missing_count,
            },
            "companies": companies,
        }
    return await asyncio.to_thread(_query)


@router.get("/api/scheduled-strategies")
async def list_scheduled_strategies():
    """Every scheduled strategy, newest first by created_at desc, with
    its last snapshot summary attached."""
    def _query() -> list[dict]:
        resp = (
            supabase.table("scheduled_strategy")
            .select("*")
            .order("created_at")
            .execute()
        )
        return _hydrate(resp.data or [])
    return await asyncio.to_thread(_query)


@router.post("/api/scheduled-strategies")
async def add_scheduled_strategy(body: ScheduledStrategyCreate):
    """Create a new scheduled strategy. Sets `next_due_at` to the next
    upcoming Tuesday 02:00 UTC pipeline tick so the entry runs on the
    next eligible tick regardless of frequency."""
    if body.frequency not in FREQUENCIES:
        raise HTTPException(
            400,
            f"Unknown frequency {body.frequency!r}; expected one of {list(FREQUENCIES)}",
        )
    if not body.name.strip():
        raise HTTPException(400, "name must be non-empty")
    if not isinstance(body.config, dict) or not body.config:
        raise HTTPException(400, "config must be a non-empty object")

    def _insert() -> dict:
        next_due = _initial_next_due_at().isoformat()
        insert_row: dict = {
            "name": body.name.strip(),
            "frequency": body.frequency,
            "config": body.config,
            "enabled": True,
            "next_due_at": next_due,
            "backtest_run_id": body.backtest_run_id,
        }
        if body.start_date is not None:
            insert_row["start_date"] = body.start_date.isoformat()
        try:
            resp = (
                supabase.table("scheduled_strategy")
                .insert(insert_row)
                .execute()
            )
        except Exception as e:
            raise HTTPException(500, f"Insert failed: {type(e).__name__}: {e}")
        if not resp.data:
            raise HTTPException(500, "Insert returned no row")
        new_row = resp.data[0]
        # No backfill — the saved backtest_run IS the pre-schedule
        # history. Live snapshots accumulate from the next pipeline
        # tick onwards.
        return _hydrate([new_row])[0]
    return await asyncio.to_thread(_insert)


@router.patch("/api/scheduled-strategies/{strategy_id}")
async def patch_scheduled_strategy(strategy_id: int, body: ScheduledStrategyPatch):
    """Toggle `enabled` and/or set the configurable `start_date` (the
    go-live marker + live cutoff). Re-pointing at a different config isn't
    allowed in place — delete + re-add to keep per-snapshot attribution
    unambiguous."""
    update_dict: dict = {"updated_at": datetime.now(timezone.utc).isoformat()}
    if body.enabled is not None:
        update_dict["enabled"] = body.enabled
    if body.name is not None:
        trimmed = body.name.strip()
        if not trimmed:
            raise HTTPException(400, "name must be non-empty")
        update_dict["name"] = trimmed
    if body.clear_start_date:
        update_dict["start_date"] = None
    elif body.start_date is not None:
        update_dict["start_date"] = body.start_date.isoformat()
    if body.rebalance_weekday is not None and not (0 <= body.rebalance_weekday <= 6):
        raise HTTPException(400, "rebalance_weekday must be 0 (Mon) … 6 (Sun)")
    # `updated_at` is always present — require at least one real field so a
    # no-op PATCH is a clear 400 rather than a silent timestamp bump.
    if len(update_dict) == 1 and body.rebalance_weekday is None:
        raise HTTPException(
            400,
            "Nothing to update (pass `enabled`, `name`, `start_date`, "
            "`clear_start_date`, or `rebalance_weekday`).",
        )

    def _update() -> dict:
        # Merge rebalance_weekday into the stored config blob (read-modify-
        # write — config is JSONB and we only touch this one key).
        if body.rebalance_weekday is not None:
            cur = (
                supabase.table("scheduled_strategy")
                .select("config")
                .eq("id", strategy_id)
                .limit(1)
                .execute()
            )
            if not cur.data:
                raise HTTPException(404, f"Scheduled strategy #{strategy_id} not found")
            cfg = dict(cur.data[0].get("config") or {})
            cfg["rebalance_weekday"] = body.rebalance_weekday
            update_dict["config"] = cfg
        resp = (
            supabase.table("scheduled_strategy")
            .update(update_dict)
            .eq("id", strategy_id)
            .execute()
        )
        if not resp.data:
            raise HTTPException(404, f"Scheduled strategy #{strategy_id} not found")
        return _hydrate(resp.data)[0]
    return await asyncio.to_thread(_update)


@router.delete("/api/scheduled-strategies")
async def delete_all_scheduled_strategies():
    """Wipe every scheduled strategy. Snapshots stay (their
    `scheduled_strategy_id` FK is set to NULL via cascade) so the
    historical run-history view remains inspectable. Mostly used to
    reset the /schedule page after experimenting with multiple
    permutations."""
    def _delete() -> dict:
        # Fetch the ids first so we can return a count — `delete()`
        # without a filter is rejected by Supabase by default, so use
        # `neq(id, 0)` to match all rows.
        resp = (
            supabase.table("scheduled_strategy")
            .delete()
            .neq("id", 0)
            .execute()
        )
        return {"deleted_count": len(resp.data or [])}
    return await asyncio.to_thread(_delete)


@router.delete("/api/scheduled-strategies/{strategy_id}")
async def delete_scheduled_strategy(strategy_id: int):
    """Remove from the schedule. Past snapshots are preserved (the
    snapshot's `scheduled_strategy_id` FK is set to NULL via the
    foreign-key cascade, so they're orphaned but visible for historical
    inspection)."""
    def _delete() -> dict:
        resp = (
            supabase.table("scheduled_strategy")
            .delete()
            .eq("id", strategy_id)
            .execute()
        )
        if not resp.data:
            raise HTTPException(404, f"Scheduled strategy #{strategy_id} not found")
        return {"deleted": strategy_id}
    return await asyncio.to_thread(_delete)


@router.get("/api/scheduled-strategies/{strategy_id}/runs")
async def list_strategy_runs(strategy_id: int, limit: int = 50):
    """Run history for one scheduled strategy. Joins via the new
    `current_picks_snapshot.scheduled_strategy_id` FK so it stays clean
    even after schema-evolution churn on adjacent tables."""
    limit = max(1, min(200, limit))

    def _query() -> dict:
        sched_resp = (
            supabase.table("scheduled_strategy")
            .select("*")
            .eq("id", strategy_id)
            .limit(1)
            .execute()
        )
        if not sched_resp.data:
            raise HTTPException(404, f"Scheduled strategy #{strategy_id} not found")
        sched = sched_resp.data[0]

        snap_resp = (
            supabase.table("current_picks_snapshot")
            .select(
                "snapshot_id, ingest_run_id, created_at, as_of_date, "
                "latest_price_date, holdings, kind, is_backfill, period_return_pct"
            )
            .eq("scheduled_strategy_id", strategy_id)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        snapshots = snap_resp.data or []

        # Suppress backfill rebalance rows whose `as_of_date` is also
        # covered by a NEWER non-backfill snapshot (a daily-refresh
        # price_update or a live pipeline rebalance). The backfill
        # row's data is point-in-time stale at that point — the user
        # already has the latest data via the newer snapshot, and
        # showing both creates a confusing "2026-05-04 backfill
        # +0.45% (data through 05-06)" alongside "2026-05-04 price
        # update +2.07% (data through 05-25)" pair for the same
        # open period. `_compute_period_returns` keeps the full
        # history above — this filter is purely cosmetic.
        non_backfill_asofs = {
            s["as_of_date"] for s in snapshots
            if not (s.get("kind") == "rebalance" and s.get("is_backfill"))
            and s.get("as_of_date")
        }
        snapshots = [
            s for s in snapshots
            if not (s.get("kind") == "rebalance" and s.get("is_backfill"))
            or s.get("as_of_date") not in non_backfill_asofs
        ]

        run_ids = list({s["ingest_run_id"] for s in snapshots if s.get("ingest_run_id")})
        runs_by_id: dict[int, dict] = {}
        if run_ids:
            runs_resp = (
                supabase.table("ingest_run")
                .select("*")
                .in_("run_id", run_ids)
                .execute()
            )
            runs_by_id = {r["run_id"]: r for r in (runs_resp.data or [])}

        def _sector_counts(holdings: list[dict] | None) -> dict[str, int]:
            """Group this snapshot's holdings by sector. Used by the
            UI's per-row sector grid (vertically aligned across rows so
            persistent sectors are easy to eyeball)."""
            out: dict[str, int] = {}
            for h in holdings or []:
                sec = (h.get("sector") or "").strip() or "—"
                out[sec] = out.get(sec, 0) + 1
            return out

        history = [
            {
                "snapshot_id": s["snapshot_id"],
                "created_at": s["created_at"],
                "as_of_date": s["as_of_date"],
                "latest_price_date": s.get("latest_price_date"),
                "holdings_count": len(s.get("holdings") or []),
                "kind": s.get("kind"),
                "is_backfill": bool(s.get("is_backfill")),
                "period_return_pct": s.get("period_return_pct"),
                "sector_counts": _sector_counts(s.get("holdings")),
                # `ingest_run` is null for backfill rows (they weren't
                # produced by any pipeline tick).
                "ingest_run": runs_by_id.get(s["ingest_run_id"]) if s.get("ingest_run_id") else None,
            }
            for s in snapshots
        ]

        return {
            "id": sched["id"],
            "name": sched.get("name") or f"Strategy #{sched['id']}",
            "frequency": sched.get("frequency"),
            "config": sched.get("config") or {},
            "enabled": sched.get("enabled", True),
            "created_at": sched.get("created_at"),
            # Configurable go-live date (red dashed equity-curve marker +
            # live cutoff). NULL → frontend defaults to created_at.
            "start_date": sched.get("start_date"),
            "last_run_at": sched.get("last_run_at"),
            "next_due_at": sched.get("next_due_at"),
            # Variant-add flow stores the source backtest here. Frontend
            # fetches /api/momentum/backtests/{run_id} on expansion to
            # render the full equity curve + monthly history with the
            # red dashed go-live marker at `start_date` (or created_at).
            "backtest_run_id": sched.get("backtest_run_id"),
            "backfill": {
                "status": sched.get("backfill_status"),
                "progress_pct": sched.get("backfill_progress_pct"),
                "message": sched.get("backfill_message"),
                "error": sched.get("backfill_error"),
                "started_at": sched.get("backfill_started_at"),
                "finished_at": sched.get("backfill_finished_at"),
            },
            "runs": history,
        }

    return await asyncio.to_thread(_query)
