"""Phases 2 + 2.5 — orphan prune and duplicate merge.

Both phases clean up the `company` table before the prices phase spends
GuruFocus calls on rows it shouldn't:

  Phase 2 (prune)  — delete `company` rows that no longer belong to any
                     source universe (LongEquity / ACWI / Leonteq).
  Phase 2.5 (dedupe) — collapse cross-source duplicates (same issuer
                     ingested by ACWI + Leonteq + LongEquity as separate
                     rows) into one canonical row, rewiring every FK.

Order matters: prune first (so dedupe doesn't merge into a row that's
about to be deleted), dedupe second (so prices doesn't fetch losers).
The heavy lifting lives in `ingest.prune_companies` and `ingest.dedupe`;
these wrappers just drive them and write a summary to `current_message`.
"""
from __future__ import annotations

import logging

from deps import supabase

from .runlog import _update_run


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


def _run_dedupe_phase(run_id: int) -> None:
    """Phase 2.5 — collapse duplicate `company` rows that ended up in
    the table because three different sources (LongEquity / ACWI /
    Leonteq) all ingested the same issuer. Calls
    `ingest.dedupe.merge_existing_duplicates` which groups by
    canonical_name, picks a winner per `EXCHANGE_PRIORITY`, and rewires
    every FK (metric_data, universe_membership, portfolio_weight,
    company_source, leonteq_equity, current_picks_snapshot) before
    deleting the loser rows.

    Runs AFTER prune (so we don't merge into a row that's about to be
    deleted) and BEFORE prices (so we don't spend GuruFocus calls on
    losers)."""
    from ingest.dedupe import (  # noqa: PLC0415
        merge_existing_duplicates,
    )

    log = logging.getLogger(__name__)
    _update_run(run_id, current_message="Scanning for duplicate companies…")
    report = merge_existing_duplicates(supabase, dry_run=False)

    msg = (
        f"Dedupe done: merged {report.groups_merged} dupe groups, "
        f"deleted {report.rows_deleted} loser rows "
        f"(HKSE pad: {report.hkse_tickers_normalized}, "
        f"metric_data: {report.metric_data_reassigned} moved / "
        f"{report.metric_data_dropped} dropped, "
        f"membership: {report.universe_membership_reassigned}, "
        f"weights: {report.portfolio_weight_reassigned})."
    )
    _update_run(run_id, current_message=msg)
    log.info("[pipeline.dedupe] run_id=%s %s", run_id, msg)


def _run_delisting_phase(run_id: int) -> None:
    """Stale-price delisting sweep — mark companies whose latest close is many
    trading days behind the market as `delisted_at`. DB-only (no GuruFocus
    calls), so it's cheap enough to run on every tick, including the held-only
    daily pipeline. See `ingest.delisting`."""
    from ingest.delisting import sweep_delisted_companies  # noqa: PLC0415

    log = logging.getLogger(__name__)
    _update_run(run_id, current_message="Sweeping for delisted (stale-price) companies…")
    res = sweep_delisted_companies(supabase)
    msg = (
        f"Delisting sweep: {res.newly_delisted} newly delisted "
        f"({res.with_data} of {res.checked} active companies have price data)."
        + (" Skipped — no direct Postgres (SUPABASE_DB_URL)." if res.skipped_no_pg else "")
    )
    if res.examples:
        msg += " e.g. " + ", ".join(res.examples[:5])
    _update_run(run_id, current_message=msg)
    log.info("[pipeline.delisting] run_id=%s %s", run_id, msg)
