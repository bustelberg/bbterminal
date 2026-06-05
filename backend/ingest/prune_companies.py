"""Prune `company` rows that no longer belong to any source universe.

The invariant: every row in `company` must be a member of at least one
of these "source universes":

  * **LongEquity** — ever appeared in a LongEquity snapshot.
    Membership signal: `universe_membership.company_id` where
    `universe.label = 'longequity'`. Falls back to
    `metric_data.company_id` where `source_code = 'longequity'` to
    catch companies ingested before the universe_membership write
    path existed (see `_get_db_longequity_months` in
    `routers/longequity.py`).
  * **ACWI** — template-managed canonical universe.
    Membership signal: `universe_membership.company_id` joined to
    `universe.template_key = 'ACWI'`.
  * **Leonteq** — template-managed canonical universe.
    Membership signal: `universe_membership.company_id` joined to
    `universe.template_key = 'LEONTEQ'`.

Anything in `company` that satisfies none of the three is an orphan
and gets deleted, manually cascading the FKs that aren't
`ON DELETE CASCADE` (notably `metric_data` and `portfolio_weight` —
both predate the cascade pattern). All other `company_id` FKs in the
schema use either `ON DELETE CASCADE` (universe_membership,
company_source, universe_snapshot, current_picks_*) or
`ON DELETE SET NULL` (index_membership, leonteq_equity).

Used in two places:
  1. Manual one-shot via `uv run python -m ingest.prune_companies`.
  2. Pipeline Phase 1.5 (after template refresh, before price refresh)
     so the prices phase doesn't waste GuruFocus API calls on rows
     that are about to be deleted.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from supabase import Client

from deps import IN_CHUNK_SIZE

_log = logging.getLogger(__name__)


@dataclass
class PruneResult:
    """Outcome of a prune pass."""
    company_count_before: int
    kept_count: int
    orphan_count: int
    metric_data_deleted: int = 0
    portfolio_weight_deleted: int = 0
    companies_deleted: int = 0
    company_count_after: int = 0
    # Sample of orphan rows, up to 20, for audit emit.
    orphan_sample: list[dict] = field(default_factory=list)
    # Per-source kept counts (for the audit summary).
    longequity_kept: int = 0
    acwi_kept: int = 0
    leonteq_kept: int = 0
    # Companies kept by virtue of an out-of-scope marker (override-set
    # via gf_ticker_overrides.json `{"unavailable": true, ...}`). These
    # rows have no universe_membership but should stay visible in
    # /companies as an explicit "we know this exists, deliberately not
    # covered" record.
    out_of_scope_kept: int = 0


def _chunked(items: list[int], size: int = IN_CHUNK_SIZE) -> list[list[int]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def _load_all_company_ids(supabase: Client) -> set[int]:
    """Every row in `company`. Paginates because Supabase's PostgREST
    caps a single response at 1000 rows."""
    out: set[int] = set()
    offset = 0
    page_size = 1000
    while True:
        resp = (
            supabase.table("company")
            .select("company_id")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        for r in batch:
            cid = r.get("company_id")
            if cid is not None:
                out.add(int(cid))
        if len(batch) < page_size:
            break
        offset += page_size
    return out


def _load_universe_ids(supabase: Client) -> dict[str, int]:
    """`{label-or-template-key: universe_id}` for the three source
    universes we care about. Missing rows are simply absent from the
    dict — the caller treats absence as 'no members'."""
    out: dict[str, int] = {}
    # LongEquity is keyed by label (legacy — predates template machinery).
    le = (
        supabase.table("universe")
        .select("universe_id")
        .eq("label", "LongEquity")
        .limit(1)
        .execute()
    )
    if le.data:
        out["LongEquity"] = le.data[0]["universe_id"]

    # ACWI and Leonteq are template-managed.
    for tk in ("ACWI", "LEONTEQ"):
        r = (
            supabase.table("universe")
            .select("universe_id")
            .eq("template_key", tk)
            .limit(1)
            .execute()
        )
        if r.data:
            out[tk] = r.data[0]["universe_id"]
    return out


def _load_membership_company_ids(supabase: Client, universe_id: int) -> set[int]:
    """Distinct `company_id` across all months for a single universe."""
    out: set[int] = set()
    offset = 0
    page_size = 1000
    while True:
        resp = (
            supabase.table("universe_membership")
            .select("company_id")
            .eq("universe_id", universe_id)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        for r in batch:
            cid = r.get("company_id")
            if cid is not None:
                out.add(int(cid))
        if len(batch) < page_size:
            break
        offset += page_size
    return out


def _load_out_of_scope_company_ids(supabase: Client) -> set[int]:
    """Companies the override file marked out-of-scope. Kept even though
    they have no universe_membership — that's the whole point of the
    out-of-scope flag (preserve a row in `company` for visibility).
    Paginates because PostgREST caps at 1000 per request."""
    out: set[int] = set()
    offset = 0
    page_size = 1000
    while True:
        resp = (
            supabase.table("company")
            .select("company_id")
            .not_.is_("out_of_scope_at", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        for r in batch:
            cid = r.get("company_id")
            if cid is not None:
                out.add(int(cid))
        if len(batch) < page_size:
            break
        offset += page_size
    return out


def _load_longequity_metric_company_ids(supabase: Client) -> set[int]:
    """Legacy safety net: companies that have any `metric_data` row
    with `source_code='longequity'`. Catches the months that were
    ingested before the universe_membership write path existed."""
    out: set[int] = set()
    offset = 0
    page_size = 1000
    while True:
        resp = (
            supabase.table("metric_data")
            .select("company_id")
            .eq("source_code", "longequity")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        for r in batch:
            cid = r.get("company_id")
            if cid is not None:
                out.add(int(cid))
        if len(batch) < page_size:
            break
        offset += page_size
    return out


def _load_orphan_sample(supabase: Client, orphan_ids: list[int], n: int = 20) -> list[dict]:
    """Pull a small sample of orphan rows with enough context for the
    audit emit (name, ticker, exchange)."""
    if not orphan_ids:
        return []
    sample_ids = orphan_ids[:n]
    resp = (
        supabase.table("company")
        .select(
            "company_id, gurufocus_ticker, company_name, "
            "gurufocus_exchange:gurufocus_exchange(exchange_code)"
        )
        .in_("company_id", sample_ids)
        .execute()
    )
    rows = resp.data or []
    out: list[dict] = []
    for r in rows:
        exch = (r.get("gurufocus_exchange") or {}).get("exchange_code")
        out.append({
            "company_id": r.get("company_id"),
            "gurufocus_ticker": r.get("gurufocus_ticker"),
            "company_name": r.get("company_name"),
            "exchange_code": exch,
        })
    return out


def compute_orphans(supabase: Client) -> PruneResult:
    """Identify orphan company rows without deleting anything.

    Returns a `PruneResult` with counts + a 20-row sample. Callers that
    just want the audit pass through `prune_orphan_companies(dry_run=True)`."""
    all_ids = _load_all_company_ids(supabase)
    universe_ids = _load_universe_ids(supabase)

    longequity_kept: set[int] = set()
    if "LongEquity" in universe_ids:
        longequity_kept |= _load_membership_company_ids(supabase, universe_ids["LongEquity"])
    longequity_kept |= _load_longequity_metric_company_ids(supabase)

    acwi_kept: set[int] = set()
    if "ACWI" in universe_ids:
        acwi_kept = _load_membership_company_ids(supabase, universe_ids["ACWI"])

    leonteq_kept: set[int] = set()
    if "LEONTEQ" in universe_ids:
        leonteq_kept = _load_membership_company_ids(supabase, universe_ids["LEONTEQ"])

    out_of_scope_kept = _load_out_of_scope_company_ids(supabase)

    kept = longequity_kept | acwi_kept | leonteq_kept | out_of_scope_kept
    orphans = sorted(all_ids - kept)

    result = PruneResult(
        company_count_before=len(all_ids),
        kept_count=len(kept),
        orphan_count=len(orphans),
        longequity_kept=len(longequity_kept),
        acwi_kept=len(acwi_kept),
        leonteq_kept=len(leonteq_kept),
        out_of_scope_kept=len(out_of_scope_kept),
        orphan_sample=_load_orphan_sample(supabase, orphans, n=20),
    )
    return result


def prune_orphan_companies(
    supabase: Client,
    *,
    dry_run: bool = False,
) -> PruneResult:
    """Compute orphans and delete them (unless `dry_run=True`).

    Cascade order:
      1. `metric_data` (no FK cascade)
      2. `portfolio_weight` (no FK cascade)
      3. `company` (everything else cascades via FK)
    """
    result = compute_orphans(supabase)
    if dry_run or result.orphan_count == 0:
        result.company_count_after = result.company_count_before
        return result

    # Re-derive the delete list from the same snapshot the audit used —
    # `compute_orphans` already paid the page-through-everything cost.
    all_ids = _load_all_company_ids(supabase)
    kept = _kept_union(supabase)
    orphan_ids = sorted(all_ids - kept)
    result.orphan_count = len(orphan_ids)
    if not orphan_ids:
        result.company_count_after = result.company_count_before
        return result

    md_deleted = 0
    pw_deleted = 0
    co_deleted = 0
    for chunk in _chunked(orphan_ids, IN_CHUNK_SIZE):
        try:
            r = supabase.table("metric_data").delete().in_("company_id", chunk).execute()
            md_deleted += len(r.data or [])
        except Exception as e:
            _log.warning("[prune] metric_data delete chunk failed: %s: %s", type(e).__name__, e)
        try:
            r = supabase.table("portfolio_weight").delete().in_("company_id", chunk).execute()
            pw_deleted += len(r.data or [])
        except Exception as e:
            _log.warning("[prune] portfolio_weight delete chunk failed: %s: %s", type(e).__name__, e)
        try:
            r = supabase.table("company").delete().in_("company_id", chunk).execute()
            co_deleted += len(r.data or [])
        except Exception as e:
            _log.warning("[prune] company delete chunk failed: %s: %s", type(e).__name__, e)

    result.metric_data_deleted = md_deleted
    result.portfolio_weight_deleted = pw_deleted
    result.companies_deleted = co_deleted
    result.company_count_after = len(_load_all_company_ids(supabase))
    return result


def _kept_union(supabase: Client) -> set[int]:
    """Union of company_ids that belong to any source universe OR are
    explicitly tagged out-of-scope (deliberate out-of-coverage record,
    kept for /companies visibility — see `out_of_scope_at`)."""
    universe_ids = _load_universe_ids(supabase)
    kept: set[int] = set()
    if "LongEquity" in universe_ids:
        kept |= _load_membership_company_ids(supabase, universe_ids["LongEquity"])
    kept |= _load_longequity_metric_company_ids(supabase)
    if "ACWI" in universe_ids:
        kept |= _load_membership_company_ids(supabase, universe_ids["ACWI"])
    if "LEONTEQ" in universe_ids:
        kept |= _load_membership_company_ids(supabase, universe_ids["LEONTEQ"])
    kept |= _load_out_of_scope_company_ids(supabase)
    return kept


def format_audit(result: PruneResult) -> str:
    """Human-readable summary, suitable for stdout or the pipeline SSE."""
    lines = [
        f"Company table: {result.company_count_before} rows",
        f"  Kept: {result.kept_count}",
        f"    LongEquity:   {result.longequity_kept}",
        f"    ACWI:         {result.acwi_kept}",
        f"    Leonteq:      {result.leonteq_kept}",
        f"    Out-of-scope: {result.out_of_scope_kept}",
        f"  Orphans: {result.orphan_count}",
    ]
    if result.orphan_sample:
        lines.append("  Orphan sample (first 20):")
        for r in result.orphan_sample:
            lines.append(
                f"    cid={r['company_id']:>6}  "
                f"{(r.get('exchange_code') or '?'):>8}:{(r.get('gurufocus_ticker') or '?'):<12}  "
                f"{r.get('company_name') or '?'}"
            )
    if result.companies_deleted:
        lines.append(
            f"Deleted: {result.companies_deleted} companies, "
            f"{result.metric_data_deleted} metric_data rows, "
            f"{result.portfolio_weight_deleted} portfolio_weight rows"
        )
        lines.append(f"Company table after: {result.company_count_after} rows")
    return "\n".join(lines)


if __name__ == "__main__":
    import sys  # noqa: PLC0415
    from deps import supabase  # noqa: PLC0415

    dry_run = "--apply" not in sys.argv
    res = prune_orphan_companies(supabase, dry_run=dry_run)
    print(format_audit(res))
    if dry_run:
        print("\n(dry run — pass --apply to actually delete)")
