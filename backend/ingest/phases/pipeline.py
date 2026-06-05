"""Pipeline orchestrators — sequence the phase modules.

Three sync orchestrators, each run in a daemon thread spawned by
`routers.ingest_runs._spawn_ingest`. Phases run independently: a failure
is captured in `accumulated_errors` (first ~5 surface in
`error_summary`) but the next phase still attempts. The run's overall
`status` is `error` if any phase errored, `ok` otherwise.

  _run_pipeline_sync                       full weekly/manual/bootstrap
                                           pipeline (all five phases)
  _run_daily_mtd_pipeline_sync             prices(held) + MTD persist
  _run_daily_template_refresh_pipeline_sync acquisition → templates →
                                           prune → dedupe (no prices/momentum)
"""
from __future__ import annotations

import logging

from .acquisition import _run_acquisition_phase
from .momentum import _run_daily_mtd_phase, _run_momentum_phase
from .prices import _collect_held_companies, _run_prices_phase
from .prune import _run_dedupe_phase, _run_prune_phase
from .runlog import _now_utc_iso, _update_run
from .templates import _run_templates_phase


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


def _run_daily_mtd_pipeline_sync(run_id: int) -> None:
    """Two-phase daily orchestrator. Cheaper sibling of
    `_run_pipeline_sync`: skips acquisition/templates/prune entirely —
    only the price+volume refresh (limited to held companies) and the
    per-strategy MTD persist. Runs Wed-Sat at 02:00 UTC."""
    log = logging.getLogger(__name__)
    accumulated_errors: list[str] = []

    # Phase 'prices' — pooled refresh across all held companies.
    _update_run(
        run_id,
        current_phase="prices",
        current_message="Collecting held companies across enabled strategies…",
    )
    try:
        companies = _collect_held_companies(run_id)
        if not companies:
            _update_run(
                run_id,
                companies_total=0,
                current_message="No enabled strategies with snapshots — skipping prices phase.",
            )
        else:
            _update_run(
                run_id,
                companies_total=len(companies),
                current_message=f"Refreshing {len(companies)} held companies…",
            )
            _run_prices_phase(
                run_id, accumulated_errors, companies_override=companies,
            )
    except Exception as e:
        msg = f"Prices phase failed: {type(e).__name__}: {e}"
        log.warning("[daily_mtd] run_id=%s %s", run_id, msg)
        accumulated_errors.append(msg)

    # Phase 'momentum' — refresh-MTD + persist per strategy.
    _update_run(
        run_id,
        current_phase="momentum",
        current_message="Recomputing MTD on latest snapshots…",
    )
    try:
        _run_daily_mtd_phase(run_id)
    except Exception as e:
        msg = f"MTD persist phase failed: {type(e).__name__}: {e}"
        log.warning("[daily_mtd] run_id=%s %s", run_id, msg)
        accumulated_errors.append(msg)

    final_status = "error" if accumulated_errors else "ok"
    summary = (
        ("First errors:\n" + "\n".join(accumulated_errors[:5]))[:1000]
        if accumulated_errors else None
    )
    _update_run(
        run_id,
        current_phase="done",
        status=final_status,
        error_summary=summary,
        finished_at=_now_utc_iso(),
    )
    log.info("[daily_mtd] run_id=%s finished status=%s", run_id, final_status)


def _run_daily_template_refresh_pipeline_sync(run_id: int) -> None:
    """Lightweight daily template-refresh pipeline. Runs Phases 0-2.5
    only — acquisition → templates → prune → dedupe — and skips the
    heavy prices + momentum phases entirely. Fires daily so
    /schedule's per-template additions/removals view picks up MSCI
    announcement changes and Leonteq eligibility list updates within
    24h instead of waiting for the weekly full pipeline."""
    log = logging.getLogger(__name__)
    accumulated_errors: list[str] = []

    _update_run(run_id, current_phase="acquisition", current_message="Probing upstream sources…")
    try:
        _run_acquisition_phase(run_id)
    except Exception as e:
        msg = f"Acquisition phase failed: {type(e).__name__}: {e}"
        log.warning("[daily_templates] run_id=%s %s", run_id, msg)
        accumulated_errors.append(msg)

    _update_run(run_id, current_phase="templates", current_message="Starting template refresh…")
    try:
        _run_templates_phase(run_id)
    except Exception as e:
        msg = f"Templates phase failed: {type(e).__name__}: {e}"
        log.warning("[daily_templates] run_id=%s %s", run_id, msg)
        accumulated_errors.append(msg)

    _update_run(run_id, current_phase="prune", current_message="Pruning orphan companies…")
    try:
        _run_prune_phase(run_id)
    except Exception as e:
        msg = f"Prune phase failed: {type(e).__name__}: {e}"
        log.warning("[daily_templates] run_id=%s %s", run_id, msg)
        accumulated_errors.append(msg)

    _update_run(run_id, current_phase="dedupe", current_message="Merging duplicate companies…")
    try:
        _run_dedupe_phase(run_id)
    except Exception as e:
        msg = f"Dedupe phase failed: {type(e).__name__}: {e}"
        log.warning("[daily_templates] run_id=%s %s", run_id, msg)
        accumulated_errors.append(msg)

    final_status = "error" if accumulated_errors else "ok"
    summary = (
        ("First errors:\n" + "\n".join(accumulated_errors[:5]))[:1000]
        if accumulated_errors else None
    )
    _update_run(
        run_id,
        current_phase="done",
        status=final_status,
        error_summary=summary,
        finished_at=_now_utc_iso(),
    )
    log.info("[daily_templates] run_id=%s finished status=%s", run_id, final_status)
