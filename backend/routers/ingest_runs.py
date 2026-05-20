"""Scheduled refresh pipeline.

Backs the /schedule page. Each run executes four phases in order:

  1. Template-managed universe refresh — iterates every registered
     `UniverseTemplate` (currently just ACWI; SP500 will plug in once
     migrated). For each: calls `template.refresh()`, which reconstructs
     monthly memberships in the canonical universe row and produces a
     diff (additions/removals/renames) vs the previous month. Each
     template's diff lands as one entry in `ingest_run.templates_summary`
     (a JSONB array, one entry per template).
  2. Orphan prune — delete any `company` row that's no longer a member
     of one of the three source universes (LongEquity / ACWI / Leonteq).
     The invariant: every row in `company` must come from one of these
     three sources. Runs AFTER the template refresh so the kept-set
     reflects the latest universe state, and BEFORE the price phase so
     we don't waste GuruFocus API calls on rows we're about to delete.
  3. Price + volume refresh — walks every company in `company` through
     `ensure_prices_for_company` + `ensure_volume_for_company`,
     tallying forbidden / delisted / errors silently. This is the
     phase that used to be the whole job pre-pipeline.
  4. Momentum compute — loops over every enabled row in
     `scheduled_strategy`. For each, drains `_momentum_backtest_stream`
     with `mode=current_portfolio` and that strategy's backtest config,
     persisting one `current_picks_snapshot` per strategy. The inverse
     FK `current_picks_snapshot.ingest_run_id` ties snapshots back to
     this run; `ingest_run.momentum_summary` JSONB holds an array of
     per-strategy result entries (status, holdings_count, etc.).

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
from datetime import datetime, timezone

from fastapi import APIRouter, Header, HTTPException

from deps import supabase

router = APIRouter(tags=["ingest"])

_VALID_JOB_NAMES = {"weekly_price_volume", "monthly_price_volume", "manual"}
# Concurrency cap — same as self_heal. GuruFocus is rate-limit-sensitive
# and the ensure_* helpers short-circuit on fresh DB rows, so the bound
# only matters when we're actually pulling fresh data. 12 keeps a typical
# weekly run roughly in line with the ~10-minute target now that the universe
# is ~2.8k companies (instead of ~1.8k when 4 was chosen). curl_cffi's
# Cloudflare ladder handles 12-wide comfortably; bump further only if 429s
# stay absent across multiple runs.
_MAX_WORKERS = 12
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
    without a ticker or an exchange code are dropped (nothing to fetch).

    Result is sorted "most-stale first": companies with NO close_price data
    come first, then companies whose latest close_price target_date is
    oldest. This guarantees that on every run the genuinely-missing data
    (the rows that drive the /backtest "N companies have NO price data"
    warning) gets fetched in the first few minutes rather than after the
    full universe has been re-checked. Already-fresh companies still get
    touched at the end of the run via the fast db_max freshness short-circuit
    in `ensure_*_for_company`, so this ordering doesn't drop any work."""
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

    # Most-stale-first ordering. One RPC fetches the latest close_price
    # target_date per company; companies with no row come back with NULL
    # which we map to the empty string so they sort lexicographically
    # before any real date. Failure here just falls back to insertion
    # order — the phase still works, just without prioritization.
    try:
        latest_resp = supabase.rpc("company_latest_close_price_dates", {}).execute()
        latest_by_cid: dict[int, str] = {
            int(row["company_id"]): (row.get("latest_target_date") or "")
            for row in (latest_resp.data or [])
        }
        out.sort(key=lambda c: latest_by_cid.get(c["cid"], ""))
    except Exception as e:
        logging.getLogger(__name__).warning(
            "[_load_all_companies] could not fetch latest close_price dates, "
            "falling back to insertion order: %s: %s",
            type(e).__name__, e,
        )
    return out


def _run_pipeline_sync(run_id: int) -> None:
    """Orchestrate the five-phase pipeline. Each phase is independent —
    a failure is recorded in `accumulated_errors` but doesn't abort the
    rest of the run. Runs in a daemon thread spawned by the trigger
    endpoint / scheduler."""
    log = logging.getLogger(__name__)
    accumulated_errors: list[str] = []

    # ── Phase 0: source acquisition ────────────────────────────
    # Probes upstream sources for new data and pulls it in. Currently:
    # LongEquity (auto-ingest if upstream has a newer month than what
    # we've loaded). Leonteq is API-driven and refreshes from inside
    # Phase 1 already; ACWI's iShares XLS is gated behind region
    # cookies that we can't bypass server-side so it's left manual —
    # see /api/acwi/xls-age for the staleness probe. Acquisition
    # failures don't abort the run: Phase 1 still reconstructs ACWI /
    # Leonteq against whatever the existing iShares XLS + Leonteq API
    # produce.
    _update_run(run_id, current_phase="acquisition", current_message="Probing upstream sources…")
    try:
        _run_acquisition_phase(run_id)
    except Exception as e:
        msg = f"Acquisition phase failed: {type(e).__name__}: {e}"
        log.warning("[pipeline] run_id=%s %s", run_id, msg)
        accumulated_errors.append(msg)

    # ── Phase 1: template-managed universe refresh ─────────────
    # Walks every registered `UniverseTemplate` (currently just ACWI;
    # SP500 will plug in here once migrated). Each template's diff
    # lands as one entry in `ingest_run.templates_summary`.
    _update_run(run_id, current_phase="templates", current_message="Starting template refresh…")
    try:
        _run_templates_phase(run_id)
    except Exception as e:
        msg = f"Templates phase failed: {type(e).__name__}: {e}"
        log.warning("[pipeline] run_id=%s %s", run_id, msg)
        accumulated_errors.append(msg)

    # ── Phase 2: orphan prune ──────────────────────────────────
    # Delete `company` rows that no longer belong to LongEquity, ACWI,
    # or Leonteq. Runs here so the kept-set reflects the latest
    # universe state AND the price phase doesn't refresh rows we're
    # about to delete.
    _update_run(run_id, current_phase="prune", current_message="Pruning orphan companies…")
    try:
        _run_prune_phase(run_id)
    except Exception as e:
        msg = f"Prune phase failed: {type(e).__name__}: {e}"
        log.warning("[pipeline] run_id=%s %s", run_id, msg)
        accumulated_errors.append(msg)

    # ── Phase 3: price + volume refresh ────────────────────────
    _update_run(run_id, current_phase="prices", current_message="Loading company list…")
    try:
        _run_prices_phase(run_id, accumulated_errors)
    except Exception as e:
        msg = f"Prices phase failed: {type(e).__name__}: {e}"
        log.warning("[pipeline] run_id=%s %s", run_id, msg)
        accumulated_errors.append(msg)

    # ── Phase 4: momentum compute ──────────────────────────────
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


def _run_acquisition_phase(run_id: int) -> None:
    """Phase 0 — pull fresh source data from upstream before Phase 1
    rebuilds universes against it.

    LongEquity: probe `check_latest_available_month` and run the sync
    ingest only when a newer month is available than what's loaded.
    Skipping when nothing new is the dominant happy-path on a weekly
    tick. Result lands in `current_message` so /schedule shows whether
    a new month was loaded or "nothing new".

    ACWI: the iShares XLS gating prevents an automated download
    (region-cookie + JS challenge). Stale-file warning lives at
    /api/acwi/xls-age; this phase just records the age in the run
    message so it surfaces in /schedule recent-runs.

    Leonteq: nothing to do — the template refresh in Phase 1 hits the
    Leonteq API directly, no separate acquisition step needed.
    """
    log = logging.getLogger(__name__)
    throttle = _Throttle()
    log_lines: list[str] = []

    def emit(msg: str) -> None:
        log_lines.append(msg)
        if msg and throttle.should_write():
            _update_run(run_id, current_message=f"[acquisition] {msg}")

    # ── LongEquity ────────────────────────────────────────────
    try:
        from routers.longequity import run_longequity_ingest_sync  # noqa: PLC0415
        emit("LongEquity: probing upstream…")
        le_result = run_longequity_ingest_sync(supabase, on_progress=emit)
        if le_result.get("status") == "no_new_data":
            emit("LongEquity: nothing new upstream.")
        elif le_result.get("status") == "ok":
            months = le_result.get("months_loaded") or []
            emit(
                f"LongEquity: ingested {le_result.get('files_processed', 0)} month(s) — "
                f"{', '.join(months) or 'none'} "
                f"({le_result.get('companies_inserted', 0)} new companies, "
                f"{le_result.get('metric_rows_inserted', 0)} metric rows)"
            )
        else:
            emit(f"LongEquity: failed — {le_result.get('error')}")
    except Exception as e:
        msg = f"LongEquity acquisition crashed: {type(e).__name__}: {e}"
        log.warning("[pipeline.acquisition] run_id=%s %s", run_id, msg)
        emit(msg)

    # ── ACWI XLS age check ────────────────────────────────────
    try:
        from index_universe.acwi.holdings import _FILE as _ACWI_XLS_PATH  # noqa: PLC0415
        import os as _os  # noqa: PLC0415
        from datetime import datetime as _dt, timezone as _tz  # noqa: PLC0415
        mtime = _os.path.getmtime(_ACWI_XLS_PATH)
        age_days = (_dt.now(_tz.utc) - _dt.fromtimestamp(mtime, _tz.utc)).days
        if age_days >= 14:
            emit(
                f"ACWI XLS is {age_days} days old — commit a fresh "
                f"`iShares-MSCI-ACWI-ETF_fund.xls` (iShares blocks "
                f"automated downloads)."
            )
        else:
            emit(f"ACWI XLS age: {age_days} day(s).")
    except Exception as e:
        emit(f"ACWI XLS age check failed: {type(e).__name__}: {e}")

    # Final phase message picks up the last log line.
    if log_lines:
        _update_run(run_id, current_message=f"[acquisition done] {log_lines[-1]}")


def _run_templates_phase(run_id: int) -> None:
    """Phase 1 — refresh every registered `UniverseTemplate` (currently
    just ACWI). Each template's `refresh()` is delegated to in turn;
    per-template failures are captured in the result array as
    `status='error'` entries (instead of bringing down the whole phase),
    matching the per-strategy isolation pattern in the momentum phase.

    The final `templates_summary` JSONB is the array of per-template
    diff entries; `current_picks_snapshot.backtest_run_id` ties momentum
    output back to its source strategy."""
    from index_universe.templates import all_templates  # noqa: PLC0415

    templates = all_templates()
    if not templates:
        _update_run(run_id, templates_summary=[])
        return

    throttle = _Throttle()
    summaries: list[dict] = []
    errors: list[str] = []

    for idx, t in enumerate(templates, start=1):
        prefix = f"[{idx}/{len(templates)} {t.label}]"
        _update_run(run_id, current_message=f"{prefix} starting refresh…")

        def on_progress(message: str, _pct: int | None = None, _prefix=prefix) -> None:
            if message and throttle.should_write():
                _update_run(run_id, current_message=f"{_prefix} {message}")

        try:
            result = t.refresh(supabase, on_progress=on_progress)
            summaries.append(result.diff.to_summary_entry())
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            errors.append(f"[{t.label}] {msg}")
            # Stub entry so the UI can still render which template
            # was attempted, with an inline error.
            summaries.append({
                "template_key": t.template_key,
                "universe_id": t.universe_id(supabase),
                "this_month": None,
                "prev_month": None,
                "additions_count": 0,
                "removals_count": 0,
                "renames_count": 0,
                "additions": [],
                "removals": [],
                "renames": [],
                "error": msg,
            })

        # Persist incrementally — multi-template runs let the UI see
        # each template land independently rather than waiting for the
        # whole phase to finish.
        _update_run(run_id, templates_summary=summaries)

    if errors:
        raise RuntimeError(
            f"{len(errors)} of {len(templates)} templates failed: "
            + " | ".join(errors[:3])
        )


def _run_prune_phase(run_id: int) -> None:
    """Phase 2 — delete `company` rows that no longer belong to any
    source universe (LongEquity / ACWI / Leonteq). The kept-set is the
    union of `universe_membership` rows joined to the three canonical
    universes, plus a `metric_data.source_code='longequity'` fallback
    for the months ingested before universe_membership existed. See
    `ingest.prune_companies` for the full definition.

    Lands a short summary string in `current_message` (no separate
    column on `ingest_run` — orphan churn is normally <10 per week and
    the message is enough for the /schedule UI to surface)."""
    from ingest.prune_companies import (  # noqa: PLC0415 (heavy module)
        prune_orphan_companies,
    )

    log = logging.getLogger(__name__)
    _update_run(run_id, current_message="Computing orphan set…")
    result = prune_orphan_companies(supabase, dry_run=False)

    msg = (
        f"Prune done: deleted {result.companies_deleted} orphan companies "
        f"({result.metric_data_deleted} metric_data, "
        f"{result.portfolio_weight_deleted} portfolio_weight rows). "
        f"Kept {result.kept_count} "
        f"(LongEquity {result.longequity_kept}, ACWI {result.acwi_kept}, "
        f"Leonteq {result.leonteq_kept}). "
        f"Company table: {result.company_count_before} → {result.company_count_after}."
    )
    _update_run(run_id, current_message=msg)
    log.info("[pipeline.prune] run_id=%s %s", run_id, msg)


def _run_prices_phase(run_id: int, accumulated_errors: list[str]) -> None:
    """Phase 3 — the price/volume refresh that used to be the whole job.
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


@router.get("/api/ingest/runs/{run_id}/templates/{template_key}/membership")
async def get_template_membership_for_run(
    run_id: int, template_key: str, q: str = "", limit: int = 500,
):
    """Universe membership captured by this run's templates phase, for
    the given `template_key`. Reads `universe_id` + `this_month` from
    the run's `templates_summary` array entry (set by the templates
    phase as each template's refresh completes). Returns 404 when the
    run didn't include the requested template, or when its diff entry
    failed (no `this_month`)."""
    limit = max(1, min(5000, limit))

    def _query() -> list[dict]:
        run_resp = (
            supabase.table("ingest_run")
            .select("templates_summary")
            .eq("run_id", run_id)
            .limit(1)
            .execute()
        )
        if not run_resp.data:
            raise HTTPException(404, "Run not found")
        summaries = run_resp.data[0].get("templates_summary") or []
        entry = next((s for s in summaries if s.get("template_key") == template_key), None)
        if entry is None:
            raise HTTPException(404, f"Run has no entry for template {template_key}")
        uid = entry.get("universe_id")
        month = entry.get("this_month")
        if uid is None or month is None:
            raise HTTPException(404, f"Run's {template_key} entry has no universe captured")

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
        out.sort(key=lambda x: (x.get("ticker") or "").upper())
        return out

    return await asyncio.to_thread(_query)
