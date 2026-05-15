"""Scheduled refresh pipeline.

Backs the /schedule page. Each run executes three phases in order:

  1. ACWI universe refresh — pulls the latest iShares ACWI XLS,
     reconciles companies, persists this month's `universe_membership`
     rows, and computes the diff vs the previous month (additions,
     removals, renames). Result lands on `ingest_run.acwi_summary` /
     `acwi_universe_id` / `acwi_target_month`.
  2. Price + volume refresh — walks every company in `company` through
     `ensure_prices_for_company` + `ensure_volume_for_company`,
     tallying forbidden / delisted / errors silently. This is the
     phase that used to be the whole job pre-pipeline.
  3. Momentum compute — when a "scheduled strategy" is selected via
     `schedule_config`, drains `_momentum_backtest_stream` with
     `mode=current_portfolio` and the selected backtest run's config.
     Persists the resulting `current_picks_snapshot` and links it on
     `ingest_run.momentum_snapshot_id`.

Phases run independently — a failure in one is captured in
`error_summary` (first ~5 errors across all phases) but the next phase
still attempts. The overall `status` is `error` if any phase errored,
`ok` otherwise. `current_phase` reflects either the active phase or
`done` when the pipeline finishes.

Two scheduled triggers (defined in `scheduler.py`):
    weekly_price_volume   Tuesday 02:00 UTC      — captures Monday closes
    monthly_price_volume  2nd of month 02:00 UTC — captures month-start

Both run the SAME pipeline; the names just record cadence on the row.
Manual "Run now" from /schedule uses `triggered_by='manual'`.
"""
from __future__ import annotations

import asyncio
import json as _json
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone

from fastapi import APIRouter, Header, HTTPException

from deps import supabase

router = APIRouter(tags=["ingest"])

_VALID_JOB_NAMES = {"weekly_price_volume", "monthly_price_volume", "manual"}
# Concurrency cap — same as self_heal. GuruFocus is rate-limit-sensitive
# and the ensure_* helpers short-circuit on fresh DB rows, so the bound
# only matters when we're actually pulling fresh data. 4 keeps a typical
# weekly run under ~10 minutes for ~1,800 companies without 429s.
_MAX_WORKERS = 4
# Checkpoint frequency — write progress to the row every N companies so
# the /schedule page sees the bar move during a long run. Tuned low
# enough that the first checkpoint arrives within seconds (so the UI
# never sits on "starting…" for long) but not so low that every
# completion triggers a DB write.
_CHECKPOINT_EVERY = 25
# Minimum interval between `current_message` writes for the ACWI and
# momentum phases (which emit many events per second). Prevents
# hammering the DB while still keeping the live status fresh.
_MESSAGE_THROTTLE_SECONDS = 1.0


class _Throttle:
    """Wall-clock throttle for `current_message` writes. Phases create
    one per invocation; the first call always passes, subsequent calls
    skip until `min_interval` has elapsed."""
    def __init__(self, min_interval: float = _MESSAGE_THROTTLE_SECONDS):
        import time as _t
        self._time = _t
        self.min_interval = min_interval
        self.last_at = 0.0

    def should_write(self) -> bool:
        now = self._time.time()
        if now - self.last_at < self.min_interval:
            return False
        self.last_at = now
        return True


def _now_utc_iso() -> str:
    """ISO timestamp matching Supabase's timestamptz format."""
    return datetime.now(timezone.utc).isoformat()


def _create_run(job_name: str, triggered_by: str) -> int:
    resp = supabase.table("ingest_run").insert({
        "job_name": job_name,
        "triggered_by": triggered_by,
        "status": "running",
    }).execute()
    if not resp.data:
        raise RuntimeError("Failed to insert ingest_run row")
    return int(resp.data[0]["run_id"])


def _update_run(run_id: int, **fields) -> None:
    """Best-effort update. Checkpoint writes shouldn't abort the whole run
    on a transient DB blip, so we swallow + log rather than raise."""
    try:
        supabase.table("ingest_run").update(fields).eq("run_id", run_id).execute()
    except Exception as e:
        logging.getLogger(__name__).warning(
            "[ingest_run] update failed for run_id=%s: %s: %s",
            run_id, type(e).__name__, e,
        )


def _load_all_companies() -> list[dict]:
    """Paginate the `company` table, returning rows usable by ensure_*. Rows
    without a ticker or an exchange code are dropped (nothing to fetch)."""
    out: list[dict] = []
    offset = 0
    page = 1000
    while True:
        resp = (
            supabase.table("company")
            .select(
                "company_id, gurufocus_ticker, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code)"
            )
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        for r in batch:
            exch = (r.get("gurufocus_exchange") or {}).get("exchange_code") or ""
            ticker = r.get("gurufocus_ticker") or ""
            if not ticker or not exch:
                continue
            out.append({
                "cid": int(r["company_id"]),
                "ticker": ticker,
                "exchange": exch,
            })
        if len(batch) < page:
            break
        offset += page
    return out


def _run_pipeline_sync(run_id: int) -> None:
    """Orchestrate the three-phase pipeline. Each phase is independent —
    a failure is recorded in `accumulated_errors` but doesn't abort the
    rest of the run. Runs in a daemon thread spawned by the trigger
    endpoint / scheduler."""
    log = logging.getLogger(__name__)
    accumulated_errors: list[str] = []

    # ── Phase 1: ACWI universe refresh ─────────────────────────
    # Clear the message from the previous phase (a re-run of the same
    # ingest_run id would otherwise carry stale text across phases).
    _update_run(run_id, current_phase="acwi", current_message="Starting ACWI refresh…")
    try:
        _run_acwi_phase(run_id)
    except Exception as e:
        msg = f"ACWI phase failed: {type(e).__name__}: {e}"
        log.warning("[pipeline] run_id=%s %s", run_id, msg)
        accumulated_errors.append(msg)

    # ── Phase 2: price + volume refresh ────────────────────────
    _update_run(run_id, current_phase="prices", current_message="Loading company list…")
    try:
        _run_prices_phase(run_id, accumulated_errors)
    except Exception as e:
        msg = f"Prices phase failed: {type(e).__name__}: {e}"
        log.warning("[pipeline] run_id=%s %s", run_id, msg)
        accumulated_errors.append(msg)

    # ── Phase 3: momentum compute ──────────────────────────────
    _update_run(run_id, current_phase="momentum", current_message="Preparing momentum compute…")
    try:
        _run_momentum_phase(run_id)
    except Exception as e:
        msg = f"Momentum phase failed: {type(e).__name__}: {e}"
        log.warning("[pipeline] run_id=%s %s", run_id, msg)
        accumulated_errors.append(msg)

    # ── Finalize ───────────────────────────────────────────────
    final_status = "error" if accumulated_errors else "ok"
    summary = ("First errors:\n" + "\n".join(accumulated_errors[:5]))[:1000] if accumulated_errors else None
    _update_run(
        run_id,
        current_phase="done",
        status=final_status,
        error_summary=summary,
        finished_at=_now_utc_iso(),
    )
    log.info("[pipeline] run_id=%s finished status=%s", run_id, final_status)


def _run_acwi_phase(run_id: int) -> None:
    """Phase 1 — refresh the ACWI universe via the extracted sync worker
    in `routers/index_universe/acwi.py`. After the save lands, compute
    the additions / removals / renames diff against the previous month
    of `universe_membership` and persist as `acwi_summary` JSONB."""
    from routers.index_universe.acwi import run_acwi_save_universe

    # Two-month window — enough to reconstruct the previous month (the
    # diff baseline) and the current month (the new state). A wider
    # window would re-reconstruct earlier months unnecessarily.
    today = date.today()
    if today.month == 1:
        prev_month_start = date(today.year - 1, 12, 1)
    else:
        prev_month_start = date(today.year, today.month - 1, 1)
    this_month = today.strftime("%Y-%m")
    prev_month = prev_month_start.strftime("%Y-%m")

    # Throttle on_progress writes so the company-reconciliation loop
    # (which emits one event every ~200 companies) doesn't hammer the DB.
    throttle = _Throttle()

    def _on_acwi_progress(message: str, _pct: int | None = None) -> None:
        if message and throttle.should_write():
            _update_run(run_id, current_message=message)

    run_acwi_save_universe(
        "ACWI",
        prev_month_start.isoformat(),
        today.isoformat(),
        on_progress=_on_acwi_progress,
    )
    _update_run(run_id, current_message="Computing ACWI diff vs previous month…")

    # Look up the universe row by label so we can stamp the FK + filter
    # the membership diff. The save step always uses the same label.
    u_resp = (
        supabase.table("universe")
        .select("universe_id")
        .eq("label", "ACWI")
        .limit(1)
        .execute()
    )
    if not u_resp.data:
        raise RuntimeError("ACWI universe not found after save-universe call")
    universe_id = int(u_resp.data[0]["universe_id"])

    diff = _compute_acwi_diff(universe_id, prev_month, this_month)

    _update_run(
        run_id,
        acwi_universe_id=universe_id,
        acwi_target_month=this_month,
        acwi_summary=diff,
    )


def _compute_acwi_diff(universe_id: int, prev_month: str, this_month: str) -> dict:
    """Compare two months of `universe_membership` for the ACWI universe.
    Returns:
      additions: companies present this month but not last month
      removals: present last month but not this month
      renames:  same company_id in both, but with a different
                `universe_ticker` (most common case: cross-exchange
                override remap, e.g. WAR:SPL → WAR:EBP)
    Each entry carries company_id + ticker + name + sector so the UI
    can render meaningful rows without an extra fetch.
    """
    def _load_month(month: str) -> dict[int, dict]:
        out: dict[int, dict] = {}
        offset = 0
        while True:
            r = (
                supabase.table("universe_membership")
                .select("company_id, universe_ticker, sector")
                .eq("universe_id", universe_id)
                .eq("target_month", month)
                .range(offset, offset + 999)
                .execute()
            )
            batch = r.data or []
            for row in batch:
                out[int(row["company_id"])] = row
            if len(batch) < 1000:
                break
            offset += 1000
        return out

    old = _load_month(prev_month)
    new = _load_month(this_month)

    added_cids = sorted(set(new) - set(old))
    removed_cids = sorted(set(old) - set(new))
    rename_cids = sorted(
        cid for cid in (set(old) & set(new))
        if old[cid].get("universe_ticker") != new[cid].get("universe_ticker")
    )

    # One round-trip to fetch names for every cid involved (chunks of 50
    # to stay under Cloudflare's URL length limits).
    all_cids = list(set(added_cids) | set(removed_cids) | set(rename_cids))
    names: dict[int, str] = {}
    for chunk_start in range(0, len(all_cids), 50):
        chunk = all_cids[chunk_start : chunk_start + 50]
        n_resp = (
            supabase.table("company")
            .select("company_id, company_name")
            .in_("company_id", chunk)
            .execute()
        )
        for r in (n_resp.data or []):
            names[int(r["company_id"])] = r.get("company_name") or ""

    return {
        "this_month": this_month,
        "prev_month": prev_month,
        "additions_count": len(added_cids),
        "removals_count": len(removed_cids),
        "renames_count": len(rename_cids),
        "additions": [
            {
                "company_id": cid,
                "ticker": new[cid].get("universe_ticker"),
                "name": names.get(cid),
                "sector": new[cid].get("sector"),
            }
            for cid in added_cids
        ],
        "removals": [
            {
                "company_id": cid,
                "ticker": old[cid].get("universe_ticker"),
                "name": names.get(cid),
                "sector": old[cid].get("sector"),
            }
            for cid in removed_cids
        ],
        "renames": [
            {
                "company_id": cid,
                "old_ticker": old[cid].get("universe_ticker"),
                "new_ticker": new[cid].get("universe_ticker"),
                "name": names.get(cid),
            }
            for cid in rename_cids
        ],
    }


def _run_prices_phase(run_id: int, accumulated_errors: list[str]) -> None:
    """Phase 2 — the price/volume refresh that used to be the whole job.
    Walks every row in `company`, parallel-pumps each through
    `ensure_prices_for_company` + `ensure_volume_for_company`, and
    updates `ingest_run` with the per-class counters every
    `_CHECKPOINT_EVERY` companies. Forbidden / delisted are tallied
    silently; the first 5 unexpected errors land in `error_summary`."""
    from ingest.prices import (  # noqa: PLC0415
        ensure_prices_for_company,
        ensure_volume_for_company,
    )

    log = logging.getLogger(__name__)
    counters = {
        "processed": 0,
        "prices": 0,
        "volumes": 0,
        "forbidden": 0,
        "delisted": 0,
        "errors": 0,
    }
    forbidden_exchanges: set[str] = set()
    error_examples: list[str] = []
    lock = threading.Lock()

    companies = _load_all_companies()

    if not companies:
        # Empty universe — still considered a successful prices phase.
        _update_run(run_id, current_message="No companies to refresh.")
        return

    total = len(companies)
    # Surface the denominator immediately so the UI shows "0 of N"
    # instead of "starting…" while the first 25 companies process.
    _update_run(
        run_id,
        companies_total=total,
        current_message=f"Refreshing 0 of {total} companies (concurrency {_MAX_WORKERS})…",
    )

    def _refresh_one(c: dict) -> None:
        cid = c["cid"]
        ticker = c["ticker"]
        exch = c["exchange"]
        checkpoint: dict | None = None

        # Short-circuit on known-forbidden exchanges. Same pattern as
        # `momentum.data.self_heal`: a single 403 marks the exchange so
        # the next ~80 companies on it skip the API call entirely.
        with lock:
            if exch in forbidden_exchanges:
                counters["processed"] += 1
                counters["forbidden"] += 1
                if counters["processed"] % _CHECKPOINT_EVERY == 0:
                    checkpoint = dict(counters)
            if checkpoint:
                _checkpoint(run_id, checkpoint, total)
                checkpoint = None
        if exch in forbidden_exchanges:
            return

        try:
            r_p = ensure_prices_for_company(supabase, cid, ticker, exch)
        except Exception as e:
            with lock:
                counters["processed"] += 1
                counters["errors"] += 1
                if len(error_examples) < 5:
                    error_examples.append(
                        f"cid={cid} ({exch}:{ticker}) price: {type(e).__name__}: {e}"
                    )
                if counters["processed"] % _CHECKPOINT_EVERY == 0:
                    checkpoint = dict(counters)
            if checkpoint:
                _checkpoint(run_id, checkpoint, total)
            return

        if r_p.is_forbidden:
            with lock:
                forbidden_exchanges.add(exch)
                counters["processed"] += 1
                counters["forbidden"] += 1
                if counters["processed"] % _CHECKPOINT_EVERY == 0:
                    checkpoint = dict(counters)
            if checkpoint:
                _checkpoint(run_id, checkpoint, total)
            return
        if r_p.is_delisted:
            with lock:
                counters["processed"] += 1
                counters["delisted"] += 1
                if counters["processed"] % _CHECKPOINT_EVERY == 0:
                    checkpoint = dict(counters)
            if checkpoint:
                _checkpoint(run_id, checkpoint, total)
            return

        try:
            r_v = ensure_volume_for_company(supabase, cid, ticker, exch)
        except Exception as e:
            with lock:
                counters["processed"] += 1
                counters["errors"] += 1
                if r_p.rows_loaded > 0:
                    counters["prices"] += 1
                if len(error_examples) < 5:
                    error_examples.append(
                        f"cid={cid} ({exch}:{ticker}) volume: {type(e).__name__}: {e}"
                    )
                if counters["processed"] % _CHECKPOINT_EVERY == 0:
                    checkpoint = dict(counters)
            if checkpoint:
                _checkpoint(run_id, checkpoint, total)
            return

        with lock:
            counters["processed"] += 1
            if r_p.rows_loaded > 0:
                counters["prices"] += 1
            if r_v.rows_loaded > 0:
                counters["volumes"] += 1
            if counters["processed"] % _CHECKPOINT_EVERY == 0:
                checkpoint = dict(counters)
        if checkpoint:
            _checkpoint(run_id, checkpoint, total)

    with ThreadPoolExecutor(
        max_workers=_MAX_WORKERS, thread_name_prefix=f"ingest-{run_id}"
    ) as executor:
        list(executor.map(_refresh_one, companies))

    # Final counter write — orchestrator handles status/finished_at.
    _update_run(
        run_id,
        companies_processed=counters["processed"],
        prices_refreshed=counters["prices"],
        volumes_refreshed=counters["volumes"],
        forbidden_count=counters["forbidden"],
        delisted_count=counters["delisted"],
        error_count=counters["errors"],
        current_message=(
            f"Prices phase done: {counters['processed']} of {total} processed · "
            f"{counters['prices']} prices / {counters['volumes']} volumes refreshed · "
            f"{counters['forbidden']} forbidden, {counters['errors']} errors"
        ),
    )
    if error_examples:
        accumulated_errors.append(
            "Prices phase per-company errors:\n" + "\n".join(error_examples[:5])
        )
    log.info(
        "[pipeline.prices] run_id=%s done: %s processed, %s prices, %s volumes, "
        "%s forbidden, %s delisted, %s errors",
        run_id, counters["processed"], counters["prices"], counters["volumes"],
        counters["forbidden"], counters["delisted"], counters["errors"],
    )


def _run_momentum_phase(run_id: int) -> None:
    """Phase 3 — compute current-portfolio holdings using the scheduled
    strategy's config. Skipped silently when no strategy is selected on
    /schedule (the row stays with `momentum_snapshot_id=NULL`)."""
    log = logging.getLogger(__name__)
    cfg_resp = (
        supabase.table("schedule_config")
        .select("selected_run_id")
        .eq("id", 1)
        .limit(1)
        .execute()
    )
    selected = (cfg_resp.data or [{}])[0].get("selected_run_id")
    if selected is None:
        log.info("[pipeline.momentum] run_id=%s no strategy selected — skipping", run_id)
        return

    # Pull the saved backtest's config blob. We override mode +
    # force_recompute and strip variants — the cron always computes a
    # single current-portfolio snapshot, never a sweep.
    bt_resp = (
        supabase.table("backtest_run")
        .select("config, name")
        .eq("run_id", selected)
        .limit(1)
        .execute()
    )
    if not bt_resp.data:
        raise RuntimeError(f"Selected backtest run #{selected} not found")
    cfg = dict(bt_resp.data[0].get("config") or {})
    cfg["mode"] = "current_portfolio"
    cfg["force_recompute"] = True
    # Pipeline's prices phase just refreshed everything; still allow self-
    # heal so any leftover gaps surface a clear warning instead of
    # silently dropping companies from the snapshot.
    cfg["db_only"] = False
    cfg.pop("variants", None)
    cfg.pop("n_trials", None)

    # Imports are local so the module loads cheaply at boot — the momentum
    # stream pulls in pandas/numpy etc.
    from routers.momentum.backtest_stream.models import BacktestRequest  # noqa: PLC0415
    from routers.momentum.backtest_stream.stream import (  # noqa: PLC0415
        _momentum_backtest_stream,
    )

    try:
        req = BacktestRequest(**cfg)
    except Exception as e:
        raise RuntimeError(
            f"Selected backtest config doesn't validate as a BacktestRequest: {e}"
        )

    snapshot_id: int | None = None
    error_msg: str | None = None
    holdings_count = 0
    latest_price_date: str | None = None
    # Throttle current_message writes so the inner stream's per-month
    # signal-compute progress (many events per second on a long
    # backtest) doesn't generate a write storm.
    msg_throttle = _Throttle()

    async def _drain() -> None:
        nonlocal snapshot_id, error_msg, holdings_count, latest_price_date
        async for chunk in _momentum_backtest_stream(req):
            if not isinstance(chunk, str) or not chunk.startswith("data: "):
                continue
            try:
                evt = _json.loads(chunk[len("data: "):].strip())
            except _json.JSONDecodeError:
                continue
            t = evt.get("type")
            if t == "progress":
                # Surface the inner backtest's live status to /schedule
                # so the momentum phase shows the same level of detail
                # /momentum does ("Loading universe", "Computing signals
                # for 2025-04…", "Building backtest result", etc.).
                m = evt.get("message")
                if m and msg_throttle.should_write():
                    await asyncio.to_thread(_update_run, run_id, current_message=m)
            elif t == "current_portfolio":
                payload = evt.get("data") or {}
                snapshot_id = payload.get("snapshot_id")
                holdings_count = len(payload.get("holdings") or [])
                latest_price_date = payload.get("latest_price_date")
            elif t == "error":
                error_msg = evt.get("message") or "unknown error"

    # asyncio.run gives us a fresh event loop for the drain — we're on a
    # daemon thread, no existing loop to clash with.
    asyncio.run(_drain())

    if error_msg:
        raise RuntimeError(error_msg)
    if snapshot_id is None:
        raise RuntimeError("Momentum compute finished without persisting a snapshot")

    # The SSE flow tags the snapshot triggered_by='manual'. Re-tag as
    # 'auto' since the pipeline writes it on behalf of the cron, so the
    # /momentum saved-picks list correctly attributes provenance.
    try:
        supabase.table("current_picks_snapshot").update(
            {"triggered_by": "auto"}
        ).eq("snapshot_id", snapshot_id).execute()
    except Exception as e:
        log.warning(
            "[pipeline.momentum] run_id=%s failed to re-tag snapshot=%s: %s: %s",
            run_id, snapshot_id, type(e).__name__, e,
        )

    _update_run(
        run_id,
        momentum_snapshot_id=snapshot_id,
        momentum_summary={
            "holdings_count": holdings_count,
            "latest_price_date": latest_price_date,
            "strategy_run_id": selected,
            "strategy_name": bt_resp.data[0].get("name"),
        },
    )
    log.info(
        "[pipeline.momentum] run_id=%s snapshot=%s holdings=%s",
        run_id, snapshot_id, holdings_count,
    )


def _checkpoint(run_id: int, snap: dict, total: int | None = None) -> None:
    """Periodic progress write. Best-effort — a transient blip on the
    checkpoint is harmless; the next one (or the final summary) will
    catch up. Includes a `current_message` summarizing per-class
    counters so /schedule renders an actionable status line between
    structured-counter updates."""
    if total is not None:
        msg = (
            f"Refreshing {snap['processed']} of {total} companies · "
            f"{snap['prices']}p / {snap['volumes']}v refreshed · "
            f"{snap['forbidden']} forbidden, {snap['errors']} errors"
        )
    else:
        msg = (
            f"{snap['processed']} processed · "
            f"{snap['prices']}p / {snap['volumes']}v refreshed · "
            f"{snap['forbidden']} forbidden, {snap['errors']} errors"
        )
    _update_run(
        run_id,
        companies_processed=snap["processed"],
        prices_refreshed=snap["prices"],
        volumes_refreshed=snap["volumes"],
        forbidden_count=snap["forbidden"],
        delisted_count=snap["delisted"],
        error_count=snap["errors"],
        current_message=msg,
    )


def _spawn_ingest(run_id: int) -> None:
    threading.Thread(
        target=_run_pipeline_sync,
        args=(run_id,),
        daemon=True,
        name=f"pipeline-run-{run_id}",
    ).start()


def kick_off_refresh(job_name: str, triggered_by: str) -> int:
    """Public entry point. Inserts an `ingest_run` row + spawns the daemon
    thread; returns the new `run_id`. Used by both the HTTP endpoints below
    and the in-process APScheduler defined in `scheduler.py`."""
    if job_name not in _VALID_JOB_NAMES:
        raise ValueError(
            f"Unknown job_name {job_name!r}; expected one of {sorted(_VALID_JOB_NAMES)}"
        )
    if triggered_by not in ("auto", "manual"):
        raise ValueError(f"triggered_by must be 'auto' or 'manual', got {triggered_by!r}")
    run_id = _create_run(job_name, triggered_by)
    _spawn_ingest(run_id)
    return run_id


@router.post("/api/ingest/scheduled-refresh/cron")
async def cron_scheduled_refresh(
    job_name: str = "weekly_price_volume",
    x_cron_secret: str = Header(default=""),
):
    """Cron entry point. Verifies `X-Cron-Secret`, inserts an `ingest_run`
    row tagged `triggered_by='auto'`, spawns the work in a daemon thread,
    and returns the run_id immediately so the cron's curl exits fast.
    The Railway cron just needs the 200 — it doesn't wait for completion."""
    expected = os.environ.get("CRON_SECRET", "")
    if not expected:
        raise HTTPException(500, "CRON_SECRET env var is not set on the server")
    if x_cron_secret != expected:
        raise HTTPException(401, "Invalid cron secret")
    if job_name not in _VALID_JOB_NAMES:
        raise HTTPException(
            400,
            f"Unknown job_name {job_name!r}; expected one of {sorted(_VALID_JOB_NAMES)}",
        )

    run_id = await asyncio.to_thread(_create_run, job_name, "auto")
    _spawn_ingest(run_id)
    return {"run_id": run_id, "status": "running", "job_name": job_name}


@router.post("/api/ingest/scheduled-refresh/trigger")
async def trigger_scheduled_refresh(job_name: str = "manual"):
    """Manual trigger from the /schedule UI's Run-now button. Same work
    as the cron path, just `triggered_by='manual'`. No cron-secret —
    auth is enforced by the frontend proxy middleware in `frontend/proxy.ts`."""
    if job_name not in _VALID_JOB_NAMES:
        raise HTTPException(
            400,
            f"Unknown job_name {job_name!r}; expected one of {sorted(_VALID_JOB_NAMES)}",
        )
    run_id = await asyncio.to_thread(_create_run, job_name, "manual")
    _spawn_ingest(run_id)
    return {"run_id": run_id, "status": "running", "job_name": job_name}


@router.get("/api/ingest/runs")
async def list_ingest_runs(limit: int = 25):
    """Recent ingest runs (newest first). Caps `limit` to 200."""
    limit = max(1, min(200, limit))
    resp = await asyncio.to_thread(
        lambda: supabase.table("ingest_run")
        .select("*")
        .order("started_at", desc=True)
        .limit(limit)
        .execute()
    )
    return resp.data or []


@router.get("/api/ingest/runs/{run_id}")
async def get_ingest_run(run_id: int):
    """Single ingest_run row by id."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("ingest_run")
        .select("*")
        .eq("run_id", run_id)
        .limit(1)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Run not found")
    return resp.data[0]


@router.get("/api/ingest/runs/{run_id}/acwi-membership")
async def get_acwi_membership_for_run(run_id: int, q: str = "", limit: int = 500):
    """ACWI universe membership as of the run's `acwi_target_month`,
    optionally filtered by `q` matching ticker or company_name.
    Returns up to `limit` rows (capped at 5000) for the membership viewer
    on /schedule. Returns 404 when the run didn't reach the ACWI phase."""
    limit = max(1, min(5000, limit))

    def _query() -> list[dict]:
        run_resp = (
            supabase.table("ingest_run")
            .select("acwi_universe_id, acwi_target_month")
            .eq("run_id", run_id)
            .limit(1)
            .execute()
        )
        if not run_resp.data:
            raise HTTPException(404, "Run not found")
        row = run_resp.data[0]
        uid = row.get("acwi_universe_id")
        month = row.get("acwi_target_month")
        if uid is None or month is None:
            raise HTTPException(404, "Run has no ACWI universe captured")

        # Pull the membership rows + join to company for ticker / name /
        # exchange. supabase-py's PostgREST nested-select syntax handles
        # the join in one round-trip.
        mem_resp = (
            supabase.table("universe_membership")
            .select(
                "company_id, universe_ticker, sector, "
                "company:company(company_name, gurufocus_ticker, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code))"
            )
            .eq("universe_id", uid)
            .eq("target_month", month)
            .limit(limit)
            .execute()
        )
        rows = mem_resp.data or []
        ql = q.strip().lower()
        out: list[dict] = []
        for r in rows:
            company = r.get("company") or {}
            exchange = ((company.get("gurufocus_exchange") or {}).get("exchange_code")) or ""
            name = company.get("company_name") or ""
            ticker = r.get("universe_ticker") or company.get("gurufocus_ticker") or ""
            if ql and ql not in name.lower() and ql not in ticker.lower():
                continue
            out.append({
                "company_id": r.get("company_id"),
                "ticker": ticker,
                "company_name": name,
                "exchange": exchange,
                "sector": r.get("sector"),
            })
        # Stable sort by ticker for predictable display.
        out.sort(key=lambda x: (x.get("ticker") or "").upper())
        return out

    return await asyncio.to_thread(_query)
