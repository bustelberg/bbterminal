"""Pipeline orchestrators — sequence the phase modules.

Two sync orchestrators, each run in a daemon thread spawned by
`routers.ingest_runs._spawn_ingest`. Phases run independently: a failure
is captured in `accumulated_errors` (first ~5 surface in
`error_summary`) but the next phase still attempts. The run's overall
`status` is `error` if any phase errored, `ok` otherwise.

  _run_pipeline_sync        full manual/bootstrap pipeline (all five phases)
  _run_smart_pipeline_sync  the dependency-driven `smart_daily` tick —
                            refreshes only what the enabled scheduled
                            strategies need (see the function docstring)
"""
from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from datetime import date, datetime, timezone

from .acquisition import _run_acquisition_phase
from .momentum import _run_momentum_phase, _run_smart_momentum_phase
from .planner import (
    _TEMPLATE_PARENTS,
    build_plan,
    collect_template_universe_companies,
    collect_universe_companies,
)
from .prices import (
    _collect_held_companies,
    _run_prices_phase,
    refresh_held_benchmarks,
    universe_freshness,
)
from .prune import _run_dedupe_phase, _run_delisting_phase, _run_prune_phase
from .runlog import _now_utc_iso, _update_run, log_step
from .templates import _run_templates_phase, templates_needing_refresh

# Global serializer for the split pipeline. The price-update and rebalance
# operations are independently triggerable (scheduler tick + per-section
# Run-now buttons), but must never run concurrently — they both touch the
# same `current_picks_snapshot` rows and would race on GuruFocus + the DB.
# Whichever acquires first runs to completion; the other blocks on the lock
# and runs immediately after. Single-instance assumption (same as the
# scheduler — see CLAUDE.md "Single-instance assumption").
_PIPELINE_LOCK = threading.Lock()


@contextmanager
def _serialized(run_id: int):
    """Acquire the global pipeline lock, surfacing a 'waiting' message on the
    run row when another operation is already in flight so the /schedule UI
    shows the queued state instead of a frozen spinner."""
    if not _PIPELINE_LOCK.acquire(blocking=False):
        _update_run(
            run_id,
            current_message="Waiting for another pipeline operation to finish…",
        )
        _PIPELINE_LOCK.acquire()
    try:
        yield
    finally:
        _PIPELINE_LOCK.release()


def _deciding_bar_for(due, today: date | None = None) -> date:
    """The newest close the DUE strategies need in hand before they can rebalance.

    Each strategy's rebalance lands on the first `rebalance_weekday` of the
    period and is decided on the trading day STRICTLY BEFORE it — a first-Monday
    strategy on the preceding Friday, a first-Wednesday one on the Tuesday. So
    the requirement across a mixed set is the LATEST of their deciding bars;
    fetching to that satisfies every one.

    ONE definition of that bar, shared with the engine (`momentum.backtest.dates`)
    and with the compute's pre-flight gate. A pipeline that fetched to a
    different date than the gate demands would fetch diligently and still be
    rejected."""
    # Function-local: `momentum.backtest.dates` pulls pandas, and this module is
    # imported at boot by `routers/ingest_runs`.
    from momentum.backtest.dates import (  # noqa: PLC0415
        current_rebalance_date,
        deciding_bar,
    )

    t = today or date.today()
    weekdays = [int(getattr(sp, "rebalance_weekday", 0) or 0) for sp in due] or [0]
    return max(deciding_bar(current_rebalance_date(t, w)) for w in weekdays)


def _maybe_full_refetch(run_id: int, cids: list[int], accumulated_errors: list[str]) -> None:
    """Re-read the due universe's FULL history from the vendor, once a month,
    before anything is selected from it.

    ⚠ THE NORMAL PIPELINE CANNOT CORRECT THE PAST. It writes only bars newer than
    what it holds, so a vendor re-scale — a split, a reverse split, a free share
    attribution — leaves our history on the old basis for ever while new bars
    arrive on the new one. Measured on Leonteq 2026-08-02: 173 of 1,479 companies
    had wrong close history and 887 had wrong volume history, and one of them
    (Worldline, 1-for-40) was sitting in the live book on a +1142% momentum for a
    stock that had fallen 69%.

    ⚠ AND A DETECTOR IS NOT ENOUGH, WHICH IS WHY THIS IS UNCONDITIONAL. Worldline's
    seam was a 40× overnight jump — findable. Air Liquide's 1-for-10 attribution
    re-scales by 10/11 and looks like a −9.1% day; no threshold separates it from
    an ordinary move. The only reliable question is "vendor, what do you say every
    bar is?", asked on a schedule.

    ONCE PER CALENDAR MONTH, keyed off the DATA rather than a flag: an old bar can
    only have a recent `recorded_at` if a full refetch wrote it (see
    `ingest.refetch_history`). So a monthly strategy pays it on its rebalance, a
    weekly one pays it on the first rebalance of the month, and a Force
    re-rebalance doesn't pay it again. `REBALANCE_FULL_REFETCH=0` disables it;
    `=force` runs it regardless of the month guard.

    Never fatal: a failed refetch leaves the previously-stored history in place
    and the rebalance proceeds on it — degraded, and said out loud, rather than
    skipped."""
    import os  # noqa: PLC0415

    mode = os.environ.get("REBALANCE_FULL_REFETCH", "1").strip().lower()
    if mode in ("0", "off", "false", "no"):
        log_step(run_id, "Phase: refetch — DISABLED via REBALANCE_FULL_REFETCH",
                 level="warn", phase="refetch")
        return
    try:
        from ingest.refetch_history import refetch_full_history, refetched_this_month  # noqa: PLC0415

        if mode != "force" and refetched_this_month(cids):
            log_step(
                run_id,
                "Phase: refetch — skipped, a full history refetch already ran this calendar "
                "month (set REBALANCE_FULL_REFETCH=force to re-run)",
                phase="refetch",
            )
            return
        _update_run(run_id, current_phase="refetch",
                    current_message=f"Re-reading full price/volume history for {len(cids)} companies…")
        log_step(
            run_id,
            f"Phase: refetch — asking GuruFocus for EVERY bar of {len(cids)} companies "
            "(the only way a vendor correction to the past reaches us); writing back only "
            "what moved",
            phase="refetch",
        )
        refetch_full_history(
            cids, apply=True,
            on_step=lambda m, lvl: log_step(run_id, m, level=lvl, phase="refetch"),
        )
    except Exception as e:
        msg = f"Full history refetch failed: {type(e).__name__}: {e}"
        logging.getLogger(__name__).warning("[rebalance] run_id=%s %s", run_id, msg)
        log_step(run_id, msg + " — rebalancing on the history we already hold",
                 level="error", phase="refetch")
        accumulated_errors.append(msg)


def _log_universe_freshness(run_id: int, companies: list[dict], *, when: str) -> None:
    """Transcribe how far a company set is from the deciding bar, BEFORE and AFTER
    a fetch — the difference is the only proof the fetch achieved anything.

    Names the laggards (capped, with the overflow stated), because "1,431 stale"
    tells you to refresh and "these 2 are still stale" tells you WHICH vendor gaps
    you are living with. Best-effort: a diagnostic must never fail the refresh."""
    try:
        from momentum.data._pg import load_latest_metric_dates_via_copy  # noqa: PLC0415

        cids = [int(c["cid"]) for c in companies if c.get("cid") is not None]
        required = _deciding_bar_for([])
        close = load_latest_metric_dates_via_copy(cids, "close_price") or {}
        vol = load_latest_metric_dates_via_copy(cids, "volume") or {}
        if not close:
            log_step(run_id, f"  ({when}) freshness probe unavailable — no COPY path configured",
                     level="warn", phase="freshness")
            return
        by_ticker = {int(c["cid"]): f"{c.get('exchange') or '?'}:{c.get('ticker') or '?'}"
                     for c in companies if c.get("cid") is not None}
        anchor = required.isoformat()
        behind = [
            (cid, close.get(cid), vol.get(cid)) for cid in cids
            if (close.get(cid) or "") < anchor or (vol.get(cid) or "") < anchor
        ]
        log_step(
            run_id,
            f"  ({when} the fetch) {len(cids) - len(behind)} of {len(cids)} companies have BOTH "
            f"price and volume through {anchor}; {len(behind)} short",
            level="info" if not behind else "warn", phase="freshness",
        )
        for cid, p, v in sorted(behind, key=lambda r: (r[1] or ""))[:40]:
            log_step(run_id, f"      {by_ticker.get(cid, cid)} price={p or '—'} volume={v or '—'}",
                     level="warn", phase="freshness")
        if len(behind) > 40:
            log_step(run_id, f"      … and {len(behind) - 40} more", level="warn", phase="freshness")
    except Exception as e:  # noqa: BLE001
        log_step(run_id, f"  ({when}) freshness probe failed: {type(e).__name__}: {e}",
                 level="warn", phase="freshness")


def _run_is_manual(run_id: int) -> bool:
    """True when this ingest_run was triggered by a manual 'Run now'
    (`triggered_by='manual'`) rather than the scheduler (`'auto'`). A manual
    rebalance skips the universe re-scrape (recomputes from the DB — fast).
    Best-effort → False (treat as scheduled)."""
    try:
        from deps import supabase  # noqa: PLC0415
        r = (
            supabase.table("ingest_run")
            .select("triggered_by")
            .eq("run_id", run_id)
            .limit(1)
            .execute()
        )
        return bool(r.data) and r.data[0].get("triggered_by") == "manual"
    except Exception:
        return False


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

    # ── Phase 2.5: duplicate merge ─────────────────────────────
    # Collapse cross-source dupes (same issuer ingested as separate
    # rows by ACWI + Leonteq + LongEquity) so the prices phase doesn't
    # spend API calls on losers we're about to delete.
    _update_run(run_id, current_phase="dedupe", current_message="Merging duplicate companies…")
    try:
        _run_dedupe_phase(run_id)
    except Exception as e:
        msg = f"Dedupe phase failed: {type(e).__name__}: {e}"
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

    # ── Phase 3.5: delisting sweep (stale-price → delisted) ─────
    _update_run(run_id, current_phase="delisting", current_message="Sweeping for delisted companies…")
    try:
        _run_delisting_phase(run_id)
    except Exception as e:
        msg = f"Delisting phase failed: {type(e).__name__}: {e}"
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


def _run_smart_pipeline_sync(run_id: int) -> None:
    """Dependency-driven daily orchestrator (the `smart_daily` tick).

    Derives, from the enabled scheduled strategies, exactly what's needed —
    then runs ONLY that, in order, recording the derived plan on the run for
    observability:

      plan        build the SmartPlan; persist to `ingest_run.plan_summary`
      acquisition scoped to the templates the plan needs
      templates   refresh ONLY the needed templates
      prune+dedupe only when a template was refreshed this tick
      prices      held companies (every enabled strategy — daily MTD freshness)
      prices      due strategies' full universe (so newly-eligible names have
                  price history before they're scored) — only when ≥1 strategy
                  is due to rebalance
      momentum    rebalance the due strategies / price-update the rest

    Each phase is independent — a failure is captured in `error_summary` but
    the next phase still attempts."""
    log = logging.getLogger(__name__)
    accumulated_errors: list[str] = []

    # ── Phase: plan ────────────────────────────────────────────
    _update_run(run_id, current_phase="plan", current_message="Deriving pipeline plan…")
    plan = None
    try:
        plan = build_plan(datetime.now(timezone.utc))
        _update_run(
            run_id,
            plan_summary=plan.to_summary(),
            current_message=(
                f"Plan: {len(plan.strategies)} enabled strategies · "
                f"{len(plan.needed_template_keys)} universe(s) needed · "
                f"{len(plan.due_strategy_ids)} due to rebalance"
                + (f" · {len(plan.unresolved_labels)} unresolved" if plan.unresolved_labels else "")
            ),
        )
    except Exception as e:
        msg = f"Plan phase failed: {type(e).__name__}: {e}"
        log.warning("[smart] run_id=%s %s", run_id, msg)
        accumulated_errors.append(msg)

    needed_keys = set(plan.needed_template_keys) if plan else set()
    any_due = bool(plan and plan.due_strategy_ids)
    rebalance_today = any_due and bool(needed_keys)

    # Templates to refresh this tick:
    #  * on a rebalance, the universes the DUE strategies use (so they
    #    re-select from a current universe), AND
    #  * EVERY tick, any template-managed universe that's unbuilt or behind the
    #    current month — so memberships stay maintained even with ZERO enabled
    #    strategies. `/backtest` + `/acwi` read `universe_membership` directly
    #    and don't go through scheduled strategies, so maintenance can't be
    #    gated on strategy demand (the gap that left prod memberships frozen).
    # Orphan prune + acquisition stay full-pipeline-only (they serve nothing
    # on a scoped tick). Dedupe runs below, but ONLY when a template actually
    # refreshed — that's the only point new companies (hence cross-exchange
    # phantoms) get introduced, so we clean them up exactly then without
    # per-tick noise.
    maintenance_keys = templates_needing_refresh()
    keys_to_refresh = (needed_keys if rebalance_today else set()) | maintenance_keys

    # ── Phase: templates — due-rebalance + stale/unbuilt universes ──
    templates_refreshed = 0
    if keys_to_refresh:
        _update_run(
            run_id, current_phase="templates",
            current_message=f"Refreshing {len(keys_to_refresh)} universe(s)…",
        )
        try:
            templates_refreshed = _run_templates_phase(run_id, only_keys=keys_to_refresh)
        except Exception as e:
            msg = f"Templates phase failed: {type(e).__name__}: {e}"
            log.warning("[smart] run_id=%s %s", run_id, msg)
            accumulated_errors.append(msg)

    # ── Phase: dedupe — merge cross-exchange phantom duplicates ─
    # Gated on a template actually rebuilding memberships (≈monthly at the
    # rollover) — the moment new companies land. pick_winner keeps the viable
    # listing and discards out-of-scope / lookup-failed phantoms.
    if templates_refreshed:
        _update_run(run_id, current_phase="dedupe", current_message="Merging duplicate listings…")
        try:
            _run_dedupe_phase(run_id)
        except Exception as e:
            msg = f"Dedupe phase failed: {type(e).__name__}: {e}"
            log.warning("[smart] run_id=%s %s", run_id, msg)
            accumulated_errors.append(msg)

    # ── Phase: prices — refreshed template universes ───────────
    # Load prices for the constituents of every template universe we just
    # (re)built — so it's backtestable even with no scheduled strategy. The
    # price phase is freshness-gated, so steady state is a no-op; the heavy
    # one-time fetch of a never-loaded universe (e.g. LEONTEQ's ~1645 names)
    # happens HERE, in the background pipeline, instead of inline in a user's
    # backtest where it OOM-killed the backend. Gated on `templates_refreshed`
    # so it only fires on an initial build / monthly rollover / rebalance, not
    # every tick.
    if templates_refreshed and keys_to_refresh:
        _update_run(run_id, current_phase="prices", current_message="Loading template-universe prices…")
        try:
            tmpl_companies = collect_template_universe_companies(keys_to_refresh)
            if tmpl_companies:
                _update_run(run_id, current_message=f"Refreshing {len(tmpl_companies)} template-universe companies…")
                _run_prices_phase(run_id, accumulated_errors, companies_override=tmpl_companies)
        except Exception as e:
            msg = f"Template-universe price phase failed: {type(e).__name__}: {e}"
            log.warning("[smart] run_id=%s %s", run_id, msg)
            accumulated_errors.append(msg)

    # ── Phase: prices — held companies (all enabled strategies) ─
    held_count = 0
    _update_run(run_id, current_phase="prices", current_message="Collecting held companies…")
    try:
        held = _collect_held_companies(run_id)
        held_count = len(held)
        if held:
            _update_run(run_id, current_message=f"Refreshing {held_count} held companies…")
            _run_prices_phase(run_id, accumulated_errors, companies_override=held)
        else:
            _update_run(run_id, current_message="No held companies yet — skipping held-price refresh.")
    except Exception as e:
        msg = f"Held-price phase failed: {type(e).__name__}: {e}"
        log.warning("[smart] run_id=%s %s", run_id, msg)
        accumulated_errors.append(msg)

    # ── Phase: prices — due strategies' full universe ──────────
    universe_count = 0
    if any_due and plan is not None:
        due_plans = [sp for sp in plan.strategies if sp.is_due]
        _update_run(run_id, current_phase="prices", current_message="Collecting due strategies' universes…")
        try:
            universe_companies = collect_universe_companies(due_plans)
            universe_count = len(universe_companies)
            if universe_companies:
                _update_run(run_id, current_message=f"Refreshing {universe_count} universe companies…")
                _run_prices_phase(run_id, accumulated_errors, companies_override=universe_companies)
        except Exception as e:
            msg = f"Universe-price phase failed: {type(e).__name__}: {e}"
            log.warning("[smart] run_id=%s %s", run_id, msg)
            accumulated_errors.append(msg)

    # ── Phase: delisting sweep (stale-price → delisted) ────────
    # DB-only + cheap, so it runs every tick over the WHOLE company table
    # (not just held names) — catching delistings the held-only price
    # refresh would otherwise never re-probe.
    _update_run(run_id, current_phase="delisting", current_message="Sweeping for delisted companies…")
    try:
        _run_delisting_phase(run_id)
    except Exception as e:
        msg = f"Delisting phase failed: {type(e).__name__}: {e}"
        log.warning("[smart] run_id=%s %s", run_id, msg)
        accumulated_errors.append(msg)

    # ── Phase: momentum (rebalance due / price-update the rest) ─
    _update_run(run_id, current_phase="momentum", current_message="Computing current picks…")
    if plan is not None:
        try:
            _run_smart_momentum_phase(run_id, plan)
        except Exception as e:
            msg = f"Momentum phase failed: {type(e).__name__}: {e}"
            log.warning("[smart] run_id=%s %s", run_id, msg)
            accumulated_errors.append(msg)

    # ── Finalize ───────────────────────────────────────────────
    # Enrich the persisted plan with what actually happened, for the UI.
    if plan is not None:
        plan.universes_refreshed = sorted(keys_to_refresh)
        plan.held_company_count = held_count
        plan.universe_company_count = universe_count
        _update_run(run_id, plan_summary=plan.to_summary())

    final_status = "error" if accumulated_errors else "ok"
    summary = ("First errors:\n" + "\n".join(accumulated_errors[:5]))[:1000] if accumulated_errors else None
    _update_run(
        run_id,
        current_phase="done",
        status=final_status,
        error_summary=summary,
        finished_at=_now_utc_iso(),
    )
    log.info("[smart] run_id=%s finished status=%s", run_id, final_status)


def _run_price_update_pipeline_sync(run_id: int) -> None:
    """Operation 1 of the split pipeline — keep the enabled strategies' HELD
    companies priced and re-price each strategy's open positions (MTD).

    Scope is deliberately tiny: the ~24 companies currently held across every
    enabled scheduled strategy, nothing else. No template maintenance, no
    universe refresh, no rebalance. This is the daily (and Run-now) heartbeat
    that keeps the /schedule MTD numbers current between rebalances.

    Serialized against the rebalance op via `_PIPELINE_LOCK` — if a rebalance
    is in flight this blocks until it finishes, then runs."""
    log = logging.getLogger(__name__)
    accumulated_errors: list[str] = []

    with _serialized(run_id):
        # ── Phase: prices — held companies only ────────────────────
        held_count = 0
        _update_run(run_id, current_phase="prices", current_message="Collecting held companies…")
        log_step(run_id, "Price-update op starting — scope is the HELD companies only", phase="start")
        try:
            held = _collect_held_companies(run_id)
            held_count = len(held)
            if held:
                _update_run(run_id, current_message=f"Refreshing {held_count} held companies…")
                log_step(
                    run_id,
                    f"Phase: prices — {held_count} held companies across the enabled strategies: "
                    + ", ".join(f"{c.get('exchange')}:{c.get('ticker')}" for c in held[:40])
                    + (f" … +{held_count - 40} more" if held_count > 40 else ""),
                    phase="prices",
                )
                _run_prices_phase(run_id, accumulated_errors, companies_override=held)
            else:
                _update_run(run_id, current_message="No held companies yet — nothing to price.")
                log_step(run_id, "No held companies yet — nothing to price", level="warn",
                         phase="prices")
        except Exception as e:
            msg = f"Held-price phase failed: {type(e).__name__}: {e}"
            log.warning("[price_update] run_id=%s %s", run_id, msg)
            log_step(run_id, msg, level="error", phase="prices")
            accumulated_errors.append(msg)

        # ── Phase: benchmarks — held ETF overlays ──────────────────
        # Keep held ETF benchmarks (negative company_id holdings) as fresh as the
        # held companies, so the re-price below marks them to the latest close
        # instead of a stale `benchmark_price`. Runs BEFORE momentum so the
        # snapshot picks up the fresh benchmark data.
        try:
            n_bm = refresh_held_benchmarks(run_id)
            if n_bm:
                _update_run(run_id, current_message=f"Refreshed {n_bm} held ETF benchmark(s).")
            log_step(
                run_id,
                f"Phase: benchmarks — refreshed {n_bm} held ETF overlay series "
                "(without this an overlay's exit price goes stale against the stocks beside it)",
                phase="benchmarks",
            )
        except Exception as e:
            msg = f"Held-benchmark refresh failed: {type(e).__name__}: {e}"
            log.warning("[price_update] run_id=%s %s", run_id, msg)
            log_step(run_id, msg, level="error", phase="benchmarks")
            accumulated_errors.append(msg)

        # ── Phase: momentum — price-update only (no rebalances) ────
        _update_run(run_id, current_phase="momentum", current_message="Re-pricing open positions…")
        log_step(run_id, "Phase: momentum — re-pricing each strategy's open positions (no "
                         "rebalance; the rebalance op owns that)", phase="momentum")
        try:
            _run_momentum_phase(
                run_id,
                include_rebalances=False,
                dedupe_price_updates=True,
            )
        except Exception as e:
            msg = f"Momentum price-update phase failed: {type(e).__name__}: {e}"
            log.warning("[price_update] run_id=%s %s", run_id, msg)
            log_step(run_id, msg, level="error", phase="momentum")
            accumulated_errors.append(msg)

    _finalize_run(run_id, accumulated_errors, log, tag="price_update")

    # If GuruFocus still hadn't published some held names' latest close (publish
    # lag — common for the slower EU EOD feeds at the 05:00 UTC tick), don't
    # wait a whole day: schedule a one-shot retry a few hours out. Bounded per
    # day inside the scheduler. Lazy import + best-effort so a scheduler-less
    # context (CI, DISABLE_SCHEDULER, unit tests) is a silent no-op and can
    # never fail the run.
    try:
        from scheduler import maybe_schedule_price_retry  # noqa: PLC0415
        maybe_schedule_price_retry(reason="after price_update")
    except Exception as e:
        log.warning(
            "[price_update] run_id=%s stale-price retry hook failed: %s: %s",
            run_id, type(e).__name__, e,
        )


def _run_rebalance_pipeline_sync(run_id: int, force: bool = False) -> None:
    """Operation 2 of the split pipeline — rebalance the DUE scheduled
    strategies (re-select holdings from the current universe).

    For each strategy whose `next_due_at` has arrived: bring the strategy's
    UNIVERSE up to the deciding bar, then run the momentum rebalance calculation
    off the DB.

    ⚠ ONCE A MONTH IT FIRST RE-READS THE UNIVERSE'S ENTIRE HISTORY from the vendor
    (`_maybe_full_refetch`). The normal fetch can only ever ADD newer bars, so a
    split or a free-share attribution leaves our past on the old basis — and the
    small ones are indistinguishable from an ordinary day's move, so nothing but
    asking finds them. A rebalance decides on 12 months of history; it is the one
    moment that history has to be right.

    ⚠ THE PRICES COME FIRST, AND THEY ARE THE UNIVERSE'S, NOT THE HOLDINGS'. The
    price-update op keeps the ~24 held names current daily and never touches the
    ~1,455 other candidates — which are exactly the ones a rebalance ranks. On
    stale prices the failure is silent, not loud: the engine's 30-day staleness
    guard DROPS those names, so the selection is made from whatever subset
    happened to be fresh. The target is the deciding bar (the close strictly
    before the rebalance date — first Monday → the preceding Friday), the only
    bar the `<` cutoff ever reads, and the fetch is scoped to the names behind
    it. Whatever is still behind afterwards (holiday, publication lag) is a
    warning: the rebalance always computes.

    PER-PERIOD LOCK: a strategy is only rebalanced when it's actually DUE, so a
    period that was already decided is NOT re-decided on newer/revised data —
    keeping each historical decision reproducible. `force=True` (an explicit
    "Force re-rebalance") overrides the lock: it re-decides EVERY enabled
    strategy's current period; the original decisions stay in the run history.

    A MANUAL trigger ("Run now" / Force) skips the universe re-scrape — it just
    recomputes from the membership already in the DB (fast, no external calls);
    only the SCHEDULED (auto) tick refreshes universes. Never a silent no-op:
    when nothing is due it says so (and points at Force re-rebalance). Serialized
    against the price-update op via `_PIPELINE_LOCK`."""
    log = logging.getLogger(__name__)
    accumulated_errors: list[str] = []

    log_step(run_id, f"Rebalance op starting (force={force})", phase="start")
    with _serialized(run_id):
        # ── Phase: plan — which strategies are due ─────────────────
        _update_run(run_id, current_phase="plan", current_message="Checking which strategies are due…")
        log_step(run_id, "Phase: plan — deciding which strategies are due", phase="plan")
        plan = None
        try:
            plan = build_plan(datetime.now(timezone.utc))
            _update_run(run_id, plan_summary=plan.to_summary())
            for sp in plan.strategies:
                log_step(
                    run_id,
                    f"  {sp.strategy_name}: {'DUE' if sp.is_due else 'locked'} "
                    f"({sp.due_reason}; next due {str(sp.next_due_at)[:10] or '—'}; "
                    f"{sp.frequency or '—'} on weekday {sp.rebalance_weekday}; "
                    f"universe {sp.label or sp.resolved_template_key or '—'})",
                    phase="plan",
                )
        except Exception as e:
            msg = f"Plan phase failed: {type(e).__name__}: {e}"
            log.warning("[rebalance] run_id=%s %s", run_id, msg)
            log_step(run_id, msg, level="error", phase="plan")
            accumulated_errors.append(msg)

        is_manual = _run_is_manual(run_id)
        # LOCK vs FORCE: by default rebalance only DUE strategies (an
        # already-decided period stays put). `force` re-decides every enabled
        # strategy's current period — the explicit override.
        if force and plan:
            for sp in plan.strategies:
                sp.is_due = True

        due_plans = [sp for sp in plan.strategies if sp.is_due] if plan else []
        if not due_plans:
            # Never a silent no-op — say exactly why nothing ran.
            enabled = plan.strategies if plan else []
            nxt = min((sp.next_due_at for sp in enabled if sp.next_due_at), default=None)
            if not enabled:
                msg = "No enabled strategies to rebalance."
            else:
                msg = (
                    "All enabled strategies are already rebalanced for their current "
                    "period (locked)"
                    + (f" — next rebalance due {str(nxt)[:10]}" if nxt else "")
                    + ". Use ‘Force re-rebalance’ to re-decide the current period now."
                )
            _update_run(run_id, current_message=msg)
            log_step(run_id, msg, level="warn", phase="plan")
            _finalize_run(run_id, accumulated_errors, log, tag="rebalance")
            return
        log_step(
            run_id,
            f"{len(due_plans)} strategy(ies) to rebalance: "
            + ", ".join(sp.strategy_name for sp in due_plans),
            phase="plan",
        )

        # Template universes the due strategies select from (+ parents for
        # derived templates), so each re-selects from current membership. A
        # MANUAL run skips this re-scrape entirely — it just recomputes the
        # selection from the membership + prices already in the DB (fast, no
        # external calls); refresh the universe from /acwi · /leonteq if you want
        # it current first. The scheduled tick keeps refreshing so the automated
        # grid stays up to date.
        needed_keys: set[str] = set()
        if not is_manual:
            for sp in due_plans:
                if sp.resolved_template_key:
                    needed_keys.add(sp.resolved_template_key)
                    for parent in _TEMPLATE_PARENTS.get(sp.resolved_template_key, ()):
                        needed_keys.add(parent)

        # ── Phase: templates — due strategies' universes ───────────
        templates_refreshed = 0
        if needed_keys:
            _update_run(
                run_id, current_phase="templates",
                current_message=f"Refreshing {len(needed_keys)} universe(s) for rebalance…",
            )
            log_step(
                run_id,
                f"Phase: templates — re-scraping {len(needed_keys)} universe(s): "
                + ", ".join(sorted(needed_keys)),
                phase="templates",
            )
            try:
                templates_refreshed = _run_templates_phase(run_id, only_keys=needed_keys)
                log_step(run_id, f"  {templates_refreshed} template(s) rebuilt", phase="templates")
            except Exception as e:
                msg = f"Templates phase failed: {type(e).__name__}: {e}"
                log.warning("[rebalance] run_id=%s %s", run_id, msg)
                log_step(run_id, msg, level="error", phase="templates")
                accumulated_errors.append(msg)
        else:
            log_step(
                run_id,
                "Phase: templates — SKIPPED (manual run: recompute from the membership "
                "already in the DB; refresh from /acwi · /leonteq to re-scrape)"
                if is_manual else
                "Phase: templates — nothing to refresh (no template-managed universe)",
                phase="templates",
            )

        # ── Phase: dedupe — only when a template actually rebuilt ──
        if templates_refreshed:
            _update_run(run_id, current_phase="dedupe", current_message="Merging duplicate listings…")
            try:
                _run_dedupe_phase(run_id)
            except Exception as e:
                msg = f"Dedupe phase failed: {type(e).__name__}: {e}"
                log.warning("[rebalance] run_id=%s %s", run_id, msg)
                accumulated_errors.append(msg)

        # ── Phase: prices — the universe, up to the DECIDING BAR ──
        # ⚠ A REBALANCE PICKS FROM THE WHOLE UNIVERSE, SO THE WHOLE UNIVERSE HAS
        # TO BE PRICED. The price-update op keeps the ~24 HELD names current
        # daily; the other ~1,455 candidates it never touches, and they are the
        # ones being ranked. Selecting on prices weeks old doesn't fail loudly —
        # signals.py's 30-day guard silently DROPS the stale names, so the pick
        # is made from whatever subset happened to be fresh, which is a different
        # strategy than the one that was configured.
        #
        # The target is the DECIDING BAR — the close strictly before the
        # rebalance date (first Monday → the preceding Friday). That, and not
        # "today", is the only bar a rebalance needs; the engine's `<` cutoff
        # never looks past it.
        #
        # This used to be a warning only, on the reasoning that a rebalance must
        # not hang on ~1,479 GuruFocus calls. It still doesn't in the normal
        # case: the fetch is scoped to the names actually BEHIND the bar and
        # GuruFocus fetches only the missing dates, so a second run is nearly
        # free. And it runs only when a strategy is genuinely due — a few times
        # a month, not every tick.
        universe_count = 0
        freshness_warning: str | None = None
        universe_cids: list[int] = []
        try:
            _update_run(run_id, current_phase="freshness",
                        current_message="Checking price freshness…")
            universe_companies = collect_universe_companies(due_plans)
            universe_count = len(universe_companies)
            universe_cids = [c["cid"] for c in universe_companies]
            if universe_cids:
                required = _deciding_bar_for(due_plans)
                _maybe_full_refetch(run_id, universe_cids, accumulated_errors)
                report = universe_freshness(universe_cids)  # cheap DB read, no fetch
                log_step(
                    run_id,
                    f"Phase: prices — {universe_count} companies in the due strategies' "
                    f"universe; need the {required.isoformat()} deciding bar; universe's "
                    f"freshest close is {report.global_latest}; "
                    f"{len(report.fresh)} fresh, {len(report.lagging)} lagging, "
                    f"{len(report.missing)} with no prices, {len(report.excluded)} excluded "
                    f"(delisted/out-of-scope/illiquid/unsubscribed)",
                    phase="prices",
                )
                # Names behind their exchange peers, PLUS — when nothing in the
                # universe has reached the deciding bar yet — everyone: a
                # peer-anchored probe can only see relative lag, and a universe
                # uniformly a week old looks perfectly "fresh" to it.
                behind = set(report.to_fetch)
                if report.global_latest is None or report.global_latest < required:
                    behind |= set(report.fresh)
                    log_step(
                        run_id,
                        f"  the WHOLE universe is behind the {required.isoformat()} bar "
                        "(peer-relative freshness can't see that — nobody is behind anybody), "
                        "so every active company is in the fetch set",
                        level="warn", phase="prices",
                    )
                to_fetch = [c for c in universe_companies if c["cid"] in behind]
                if to_fetch:
                    _update_run(
                        run_id, current_phase="prices",
                        current_message=(
                            f"Fetching prices for {len(to_fetch)} of {universe_count} universe "
                            f"companies (need the {required.isoformat()} close)…"
                        ),
                    )
                    log.warning(
                        "[rebalance] run_id=%s fetching %s of %s universe companies up to the "
                        "%s deciding bar (universe latest: %s)",
                        run_id, len(to_fetch), universe_count, required, report.global_latest,
                    )
                    log_step(
                        run_id,
                        f"  fetching {len(to_fetch)} of {universe_count} companies from "
                        "GuruFocus (only the dates each one is missing)…",
                        phase="prices",
                    )
                    _run_prices_phase(run_id, accumulated_errors, companies_override=to_fetch)
                    log_step(run_id, "  price fetch complete", phase="prices")
                else:
                    log.info(
                        "[rebalance] run_id=%s universe already priced through the %s deciding "
                        "bar — no fetch needed", run_id, required)
                    log_step(
                        run_id,
                        f"  every company is already priced through {required.isoformat()} — "
                        "no fetch needed", phase="prices",
                    )
        except Exception as e:
            msg = f"Universe price refresh failed: {type(e).__name__}: {e}"
            log.warning("[rebalance] run_id=%s %s", run_id, msg)
            log_step(run_id, msg, level="error", phase="prices")
            accumulated_errors.append(msg)

        # Re-probe AFTER the fetch: what's still behind is what GuruFocus could
        # not supply (a market holiday its peers didn't trade either, a
        # publication lag, an unsubscribed venue). That is a warning, not a
        # blocker — the rebalance always computes, and the engine's staleness
        # guard decides what it can still rank.
        if universe_cids:
            try:
                _update_run(run_id, current_phase="freshness",
                            current_message="Re-checking price freshness…")
                report = universe_freshness(universe_cids)
                stale = len(report.to_fetch)
                if stale:
                    freshness_warning = (
                        f"{stale} of {report.active_total} universe companies are STILL behind "
                        "after the price refresh — used their last close")
                    log.warning("[rebalance] run_id=%s %s", run_id, freshness_warning)
                    log_step(run_id, freshness_warning, level="warn", phase="freshness")
                else:
                    log_step(
                        run_id,
                        f"All {report.active_total} active universe companies are caught up "
                        f"(freshest close {report.global_latest})", phase="freshness",
                    )
            except Exception as e:
                log.warning(
                    "[rebalance] run_id=%s freshness re-probe failed (non-blocking): %s: %s",
                    run_id, type(e).__name__, e)

        # ── Phase: momentum — rebalance the due strategies (always) ────
        _update_run(run_id, current_phase="momentum", current_message="Running the rebalance calculation…")
        log_step(
            run_id,
            f"Phase: momentum — computing new holdings for {len(due_plans)} strategy(ies)",
            phase="momentum",
        )
        try:
            _run_momentum_phase(
                run_id,
                due_override={sp.strategy_id: sp.is_due for sp in plan.strategies},
                include_price_updates=False,
            )
        except Exception as e:
            msg = f"Momentum rebalance phase failed: {type(e).__name__}: {e}"
            log.warning("[rebalance] run_id=%s %s", run_id, msg)
            log_step(run_id, msg, level="error", phase="momentum")
            accumulated_errors.append(msg)

        # Enrich the persisted plan with what actually happened, for the UI.
        if plan is not None:
            plan.universes_refreshed = sorted(needed_keys)
            plan.universe_company_count = universe_count
            _update_run(run_id, plan_summary=plan.to_summary())
        # Leave a CLEAR final message stating the outcome (the /schedule card
        # shows it verbatim), so a completed run never reads as a stale
        # in-progress line. On error, leave the last message + let _finalize_run
        # set status/error_summary.
        if not accumulated_errors:
            n = len(due_plans)
            noun = "strategy" if n == 1 else "strategies"
            done_msg = (
                f"{'Force-rebalanced' if force else 'Rebalanced'} {n} {noun} "
                f"({universe_count} universe companies, priced through the "
                f"{_deciding_bar_for(due_plans).isoformat()} deciding bar)."
            )
            if force:
                done_msg += " Re-decided the current period; the original decision is kept in the run history."
            if freshness_warning:
                done_msg += f" ⚠ {freshness_warning}"
            _update_run(run_id, current_message=done_msg)
            log_step(run_id, done_msg, phase="done")
        else:
            log_step(
                run_id,
                "Rebalance op finished with errors: " + " | ".join(accumulated_errors[:5]),
                level="error", phase="done",
            )

    _finalize_run(run_id, accumulated_errors, log, tag="rebalance")


def _run_full_price_refresh_pipeline_sync(run_id: int) -> None:
    """Operation 3 of the split pipeline — month-end FULL price refresh.

    Walks EVERY company in the DB (most-stale-first) and refreshes prices +
    volumes, but bounded by the monthly GuruFocus quota still available per
    region: each region stops once its remaining budget is spent (the rest of
    that region's companies are skipped, not errored). The monthly quota resets
    at midnight EST on the 1st, so this "uses up" whatever's left before it's
    lost. Prices only — no templates/prune/momentum.

    Serialized against the other ops via `_PIPELINE_LOCK`."""
    log = logging.getLogger(__name__)
    accumulated_errors: list[str] = []

    with _serialized(run_id):
        from ingest.api_usage import remaining_budget  # noqa: PLC0415
        from deps import supabase  # noqa: PLC0415

        _update_run(
            run_id, current_phase="prices",
            current_message="Computing remaining monthly API budget…",
        )
        log_step(run_id, "Month-end FULL price refresh starting — EVERY company, most-stale "
                         "first, bounded by the monthly quota", phase="start")
        try:
            budget = remaining_budget(supabase)
            _update_run(
                run_id,
                current_message=(
                    f"Budget left this month — USA {budget.get('usa', 0)}, "
                    f"EU {budget.get('europe', 0)}, Asia {budget.get('asia', 0)}. "
                    "Refreshing all companies, most-stale first…"
                ),
            )
            log_step(
                run_id,
                f"Phase: prices — monthly GuruFocus budget left: USA {budget.get('usa', 0)}, "
                f"EU {budget.get('europe', 0)}, Asia {budget.get('asia', 0)}. A region that hits "
                "0 SKIPS its remaining companies (counted as budget_skipped, not errors).",
                phase="prices",
            )
            _run_prices_phase(run_id, accumulated_errors, budget_by_region=budget)
        except Exception as e:
            msg = f"Full price refresh failed: {type(e).__name__}: {e}"
            log.warning("[full_price_refresh] run_id=%s %s", run_id, msg)
            log_step(run_id, msg, level="error", phase="prices")
            accumulated_errors.append(msg)

    _finalize_run(run_id, accumulated_errors, log, tag="full_price_refresh")


def _run_universe_price_refresh_pipeline_sync(run_id: int, universe_label: str) -> None:
    """Manual per-universe price + volume refresh — re-fetch every company in
    `universe_label`'s latest-month membership, bounded by the remaining monthly
    GuruFocus quota per region (already-fresh companies short-circuit cheaply,
    so the budget is spent on the laggards). Prices only — no
    templates/prune/momentum. Serialized against the other ops via
    `_PIPELINE_LOCK`. Backs the /schedule per-universe "Refresh" button."""
    log = logging.getLogger(__name__)
    accumulated_errors: list[str] = []

    with _serialized(run_id):
        from ingest.api_usage import remaining_budget  # noqa: PLC0415
        from deps import supabase  # noqa: PLC0415

        from .planner import collect_universe_companies_by_label  # noqa: PLC0415

        _update_run(
            run_id, current_phase="prices",
            current_message=f"Collecting {universe_label} companies…",
        )
        log_step(run_id, f"Universe price refresh starting — {universe_label!r}", phase="start")
        try:
            companies = collect_universe_companies_by_label(universe_label)
            if not companies:
                msg = f"Universe {universe_label!r} resolved to no companies to refresh"
                log_step(run_id, msg, level="error", phase="prices")
                accumulated_errors.append(msg)
            else:
                budget = remaining_budget(supabase)
                _update_run(
                    run_id,
                    current_message=(
                        f"Refreshing {len(companies)} companies in {universe_label} — "
                        f"budget left USA {budget.get('usa', 0)}, EU {budget.get('europe', 0)}, "
                        f"Asia {budget.get('asia', 0)}…"
                    ),
                )
                log_step(
                    run_id,
                    f"Phase: prices — {len(companies)} companies in {universe_label}; monthly "
                    f"GuruFocus budget left: USA {budget.get('usa', 0)}, EU "
                    f"{budget.get('europe', 0)}, Asia {budget.get('asia', 0)}. A company already "
                    "current short-circuits without an API call.",
                    phase="prices",
                )
                _log_universe_freshness(run_id, companies, when="before")
                _run_prices_phase(
                    run_id, accumulated_errors,
                    companies_override=companies, budget_by_region=budget,
                )
                _log_universe_freshness(run_id, companies, when="after")
        except Exception as e:
            msg = f"Universe price refresh failed: {type(e).__name__}: {e}"
            log.warning("[universe_price_refresh] run_id=%s %s", run_id, msg)
            log_step(run_id, msg, level="error", phase="prices")
            accumulated_errors.append(msg)

    _finalize_run(run_id, accumulated_errors, log, tag="universe_price_refresh")


def _run_companies_price_refresh_pipeline_sync(run_id: int, company_ids: list[int]) -> None:
    """Manual targeted price + volume refresh of an EXPLICIT company set — the
    'Refresh stale' button (after Inspect freshness) re-fetches ONLY the flagged
    laggards instead of the whole universe, so it's cheap on the GuruFocus quota.
    Bounded by the remaining monthly budget per region; prices only — no
    templates/prune/momentum. Serialized against the other ops via
    `_PIPELINE_LOCK`."""
    log = logging.getLogger(__name__)
    accumulated_errors: list[str] = []

    with _serialized(run_id):
        from ingest.api_usage import remaining_budget  # noqa: PLC0415
        from deps import supabase  # noqa: PLC0415

        from .planner import collect_companies_by_ids  # noqa: PLC0415

        _update_run(
            run_id, current_phase="prices",
            current_message=f"Collecting {len(company_ids)} flagged companies to refresh…",
        )
        try:
            companies = collect_companies_by_ids(company_ids)
            if not companies:
                accumulated_errors.append(
                    "No fetchable companies in the requested set (missing ticker/exchange?)"
                )
            else:
                budget = remaining_budget(supabase)
                _update_run(
                    run_id,
                    current_message=(
                        f"Refreshing {len(companies)} flagged companies — budget left "
                        f"USA {budget.get('usa', 0)}, EU {budget.get('europe', 0)}, "
                        f"Asia {budget.get('asia', 0)}…"
                    ),
                )
                _run_prices_phase(
                    run_id, accumulated_errors,
                    companies_override=companies, budget_by_region=budget,
                )
        except Exception as e:
            msg = f"Targeted price refresh failed: {type(e).__name__}: {e}"
            log.warning("[companies_price_refresh] run_id=%s %s", run_id, msg)
            accumulated_errors.append(msg)

    _finalize_run(run_id, accumulated_errors, log, tag="companies_price_refresh")


def _finalize_run(run_id: int, accumulated_errors: list[str], log, *, tag: str) -> None:
    """Shared run-finalizer for the split orchestrators: marks `done`, sets
    `status` from whether any phase errored, and rolls the first few errors
    into `error_summary`."""
    final_status = "error" if accumulated_errors else "ok"
    summary = ("First errors:\n" + "\n".join(accumulated_errors[:5]))[:1000] if accumulated_errors else None
    _update_run(
        run_id,
        current_phase="done",
        status=final_status,
        error_summary=summary,
        finished_at=_now_utc_iso(),
    )
    log.info("[%s] run_id=%s finished status=%s", tag, run_id, final_status)
