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

from deps import supabase

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
    strategy error in the run's templates_summary)."""
    name: str
    frequency: str
    config: dict


class ScheduledStrategyPatch(BaseModel):
    enabled: bool | None = None


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
    rebal_resp = (
        supabase.table("current_picks_snapshot")
        .select("*")
        .eq("scheduled_strategy_id", strategy_id)
        .eq("kind", "rebalance")
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
        # Chunk by 50 to stay under PostgREST URL limits.
        for i in range(0, len(cids), 50):
            chunk = cids[i:i + 50]
            p_resp = (
                supabase.table("metric_data")
                .select("company_id, target_date, value")
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
            current_local = float(latest["value"])
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


def _hydrate(rows: list[dict]) -> list[dict]:
    """Attach the most recent snapshot summary to each row, joined via
    the new `current_picks_snapshot.scheduled_strategy_id` FK. One
    additional query (batched by strategy_id) rather than per-row."""
    if not rows:
        return []
    sched_ids = [r["id"] for r in rows]
    snap_resp = (
        supabase.table("current_picks_snapshot")
        .select(
            "snapshot_id, scheduled_strategy_id, ingest_run_id, "
            "created_at, latest_price_date, holdings"
        )
        .in_("scheduled_strategy_id", sched_ids)
        .order("created_at", desc=True)
        .execute()
    )
    latest_by_sched: dict[int, dict] = {}
    for s in snap_resp.data or []:
        sid = s.get("scheduled_strategy_id")
        if sid is None or sid in latest_by_sched:
            continue
        latest_by_sched[sid] = s

    out: list[dict] = []
    for r in rows:
        latest = latest_by_sched.get(r["id"])
        out.append({
            "id": r["id"],
            "name": r.get("name") or f"Strategy #{r['id']}",
            "frequency": r.get("frequency"),
            "config": r.get("config") or {},
            "enabled": r.get("enabled", True),
            "created_at": r.get("created_at"),
            "updated_at": r.get("updated_at"),
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
            "last_snapshot": (
                {
                    "snapshot_id": latest["snapshot_id"],
                    "ingest_run_id": latest.get("ingest_run_id"),
                    "created_at": latest["created_at"],
                    "latest_price_date": latest.get("latest_price_date"),
                    "holdings_count": len(latest.get("holdings") or []),
                }
                if latest
                else None
            ),
        })
    return out


# ─── Endpoints ────────────────────────────────────────────────────


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
        try:
            resp = (
                supabase.table("scheduled_strategy")
                .insert({
                    "name": body.name.strip(),
                    "frequency": body.frequency,
                    "config": body.config,
                    "enabled": True,
                    "next_due_at": next_due,
                })
                .execute()
            )
        except Exception as e:
            raise HTTPException(500, f"Insert failed: {type(e).__name__}: {e}")
        if not resp.data:
            raise HTTPException(500, "Insert returned no row")
        new_row = resp.data[0]
        # Kick off the backfill in a daemon thread so the POST
        # returns immediately. The backfill snapshots appear in the
        # strategy's run history as they're persisted; the UI polls
        # the runs endpoint to pick them up.
        _spawn_backfill(int(new_row["id"]))
        return _hydrate([new_row])[0]
    return await asyncio.to_thread(_insert)


@router.patch("/api/scheduled-strategies/{strategy_id}")
async def patch_scheduled_strategy(strategy_id: int, body: ScheduledStrategyPatch):
    """Toggle `enabled`. Re-pointing at a different config isn't
    allowed in place — delete + re-add to keep per-snapshot
    attribution unambiguous."""
    if body.enabled is None:
        raise HTTPException(400, "Nothing to update (pass `enabled`).")
    def _update() -> dict:
        resp = (
            supabase.table("scheduled_strategy")
            .update({
                "enabled": body.enabled,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
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
            "last_run_at": sched.get("last_run_at"),
            "next_due_at": sched.get("next_due_at"),
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
