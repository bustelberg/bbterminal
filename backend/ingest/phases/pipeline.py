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
from datetime import datetime, timezone

from .acquisition import _run_acquisition_phase
from .momentum import _run_momentum_phase, _run_smart_momentum_phase
from .planner import (
    build_plan,
    collect_template_universe_companies,
    collect_universe_companies,
)
from .prices import _collect_held_companies, _run_prices_phase
from .prune import _run_dedupe_phase, _run_delisting_phase, _run_prune_phase
from .runlog import _now_utc_iso, _update_run
from .templates import _run_templates_phase, templates_needing_refresh


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
