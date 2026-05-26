"""Retroactive sweep: walk `company`, apply `gf_ticker_overrides.json`
to every existing row.

Historically the override file was only consulted by the ACWI ingest
path (`index_universe/acwi/reconstruction.py`). Companies created by
the Leonteq auto-create (OpenFIGI ISIN → company row) or by older
ingests can have a (exchange, ticker) pair that an override would now
remap or mark unavailable, but the row sits unchanged in `company`
forever because nothing ever re-reads the overrides for it.

This module is the catch-up: it consults the override for every row
in `company` and brings each one into compliance:

  * Remap, target row doesn't exist  → UPDATE in place
        (preserves the cid + all FK references → metric_data,
        portfolio_weight, universe_membership stay attached, no FK
        churn).
  * Remap, target row already exists → DELETE the old row
        (CASCADE clears universe_membership / company_source; the
        next template refresh re-links the source's scraped row to
        the existing target cid via the new override-aware
        `_auto_create_via_openfigi` path).
  * Unavailable, not yet stamped     → UPDATE: set out_of_scope_at +
        out_of_scope_reason.
  * Unavailable, already stamped     → no-op (correct state).
  * No override but currently stamped → UPDATE: clear out_of_scope_at
        + out_of_scope_reason (override was removed).

Designed to be safe to run repeatedly. Pair it with a re-run of any
template that historically populated the bad rows (in practice:
Leonteq) so the membership tables re-bind to the right cids.

Usage:
    uv run python -m index_universe.apply_overrides             # dry-run
    uv run python -m index_universe.apply_overrides --apply     # actually mutate
"""
from __future__ import annotations

import datetime as _dt
import logging
from dataclasses import dataclass, field

from supabase import Client

from index_universe.acwi.exchange_map import apply_company_override

_log = logging.getLogger(__name__)


@dataclass
class SweepResult:
    rows_scanned: int = 0
    remapped_in_place: int = 0
    deleted_as_duplicate: int = 0
    marked_unavailable: int = 0
    cleared_unavailable: int = 0
    skipped_no_target_exchange: int = 0
    errors: list[str] = field(default_factory=list)
    # Sample of actions taken (first ~20) — useful for a sanity audit
    # without dumping every cid.
    sample_actions: list[str] = field(default_factory=list)

    def record_sample(self, msg: str) -> None:
        if len(self.sample_actions) < 20:
            self.sample_actions.append(msg)


def _merge_universe_membership(supabase: Client, *, old_cid: int, target_cid: int) -> None:
    """Re-point every `universe_membership` row from `old_cid` to
    `target_cid`. When the target already has a row at the same
    `(universe_id, target_month)` pair (UNIQUE constraint), drop the
    old row instead — the target's row wins. Without this, deleting
    the old company row would CASCADE-clear all its memberships and
    the company would silently vanish from any universe that didn't
    already track the target cid (typically LongEquity)."""
    # `universe_membership` PK is composite `(universe_id, company_id,
    # target_month)` — no surrogate id. We can't UPDATE one row at a
    # time keyed on the composite, but supabase-py's chained .eq()s
    # work because the composite is naturally addressable that way.
    # Fetch every old-cid membership row up front so we can iterate
    # without contending with the table during the per-row updates.
    old_rows: list[dict] = []
    offset = 0
    page = 1000
    while True:
        resp = (
            supabase.table("universe_membership")
            .select("universe_id, target_month")
            .eq("company_id", old_cid)
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        old_rows.extend(batch)
        if len(batch) < page:
            break
        offset += page

    if not old_rows:
        return

    # Group the target's existing membership by universe so the
    # conflict check is O(rows) rather than O(rows * 1 query each).
    universe_ids = sorted({int(r["universe_id"]) for r in old_rows})
    target_by_universe: dict[int, set[str]] = {uid: set() for uid in universe_ids}
    for uid in universe_ids:
        t_offset = 0
        while True:
            t_resp = (
                supabase.table("universe_membership")
                .select("target_month")
                .eq("universe_id", uid)
                .eq("company_id", target_cid)
                .range(t_offset, t_offset + page - 1)
                .execute()
            )
            batch = t_resp.data or []
            for r in batch:
                tm = r.get("target_month")
                if tm:
                    target_by_universe[uid].add(str(tm))
            if len(batch) < page:
                break
            t_offset += page

    # Partition old rows into "would conflict (drop)" and
    # "no conflict (re-point)". Then issue the writes per-row keyed
    # on the composite PK. Per-row is acceptable here because the
    # merge-affected old_cid only has up to a few hundred rows in
    # practice (universe_membership is per-month).
    for r in old_rows:
        uid = int(r["universe_id"])
        tm = str(r["target_month"])
        if tm in target_by_universe.get(uid, set()):
            supabase.table("universe_membership").delete().eq(
                "universe_id", uid,
            ).eq("company_id", old_cid).eq("target_month", tm).execute()
        else:
            supabase.table("universe_membership").update(
                {"company_id": target_cid}
            ).eq("universe_id", uid).eq("company_id", old_cid).eq(
                "target_month", tm,
            ).execute()
            target_by_universe.setdefault(uid, set()).add(tm)


def _merge_company_source(supabase: Client, *, old_cid: int, target_cid: int) -> None:
    """Re-point every `company_source` tag from `old_cid` to `target_cid`.
    `(company_id, source_code)` is UNIQUE — when the target already
    carries the same source tag, drop the old row instead of
    duplicating it."""
    old_rows = (
        supabase.table("company_source")
        .select("company_id, source_code")
        .eq("company_id", old_cid)
        .execute()
        .data or []
    )
    if not old_rows:
        return
    target_sources = {
        r["source_code"]
        for r in (
            supabase.table("company_source")
            .select("source_code")
            .eq("company_id", target_cid)
            .execute()
            .data or []
        )
    }
    for r in old_rows:
        src = r["source_code"]
        if src in target_sources:
            supabase.table("company_source").delete().eq(
                "company_id", old_cid,
            ).eq("source_code", src).execute()
        else:
            supabase.table("company_source").update(
                {"company_id": target_cid}
            ).eq("company_id", old_cid).eq("source_code", src).execute()
            target_sources.add(src)


def _load_exchange_id_map(supabase: Client) -> dict[str, int]:
    """`exchange_code` (uppercase) → `exchange_id`. Used to look up the
    target exchange_id when a remap lands on an exchange whose row
    we already know."""
    resp = (
        supabase.table("gurufocus_exchange")
        .select("exchange_id, exchange_code")
        .limit(2000)
        .execute()
    )
    return {(r["exchange_code"] or "").upper(): r["exchange_id"] for r in (resp.data or [])}


def _load_all_companies(supabase: Client) -> list[dict]:
    """Every row in `company` with the fields the sweep needs. Paginated."""
    out: list[dict] = []
    offset = 0
    page = 1000
    while True:
        resp = (
            supabase.table("company")
            .select(
                "company_id, gurufocus_ticker, exchange_id, company_name, "
                "out_of_scope_at, out_of_scope_reason, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code)"
            )
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return out


def apply_overrides_retroactively(
    supabase: Client, *, dry_run: bool = False,
) -> SweepResult:
    """Walk `company` and bring every row's (exchange, ticker, out_of_scope)
    state into agreement with `gf_ticker_overrides.json`. See module
    docstring for the per-case decision table. Idempotent."""
    result = SweepResult()
    exch_id_map = _load_exchange_id_map(supabase)
    companies = _load_all_companies(supabase)
    result.rows_scanned = len(companies)

    # Index existing rows by (exchange_code_upper, ticker_upper) so we
    # can detect "target already exists" before issuing the UPDATE.
    by_key: dict[tuple[str, str], int] = {}
    for c in companies:
        exch = (c.get("gurufocus_exchange") or {}).get("exchange_code") or ""
        tick = c.get("gurufocus_ticker") or ""
        if exch and tick:
            by_key[(exch.upper(), tick.upper())] = int(c["company_id"])

    now_iso = _dt.datetime.now(_dt.timezone.utc).isoformat()

    for c in companies:
        cid = int(c["company_id"])
        cur_exch = ((c.get("gurufocus_exchange") or {}).get("exchange_code") or "").strip()
        cur_tick = (c.get("gurufocus_ticker") or "").strip()
        cur_oos_at = c.get("out_of_scope_at")
        cur_oos_reason = c.get("out_of_scope_reason")
        if not cur_exch or not cur_tick:
            continue

        override = apply_company_override(cur_exch, cur_tick)

        # 1. Unavailable branch — stamp or clear out_of_scope.
        if override.unavailable_reason is not None:
            already_stamped = cur_oos_at is not None and cur_oos_reason == override.unavailable_reason
            if already_stamped:
                continue
            result.marked_unavailable += 1
            result.record_sample(
                f"unavailable cid={cid} {cur_exch}:{cur_tick} reason={override.unavailable_reason[:60]!r}"
            )
            if dry_run:
                continue
            try:
                supabase.table("company").update({
                    "out_of_scope_at": now_iso,
                    "out_of_scope_reason": override.unavailable_reason,
                }).eq("company_id", cid).execute()
            except Exception as e:
                result.errors.append(f"cid={cid} mark-unavailable failed: {type(e).__name__}: {e}")
            continue

        # 2. No override matches. Clear a stale out_of_scope stamp if any.
        if override.target_exchange == cur_exch and override.target_ticker == cur_tick:
            if cur_oos_at is not None:
                result.cleared_unavailable += 1
                result.record_sample(
                    f"clear-unavailable cid={cid} {cur_exch}:{cur_tick} "
                    f"(override removed)"
                )
                if dry_run:
                    continue
                try:
                    supabase.table("company").update({
                        "out_of_scope_at": None,
                        "out_of_scope_reason": None,
                    }).eq("company_id", cid).execute()
                except Exception as e:
                    result.errors.append(f"cid={cid} clear-unavailable failed: {type(e).__name__}: {e}")
            continue

        # 3. Remap branch — exchange or ticker changed. Look up the
        #    target exchange_id; if missing we can't insert at the new
        #    key, so skip with a warning.
        target_eid = exch_id_map.get(override.target_exchange.upper())
        if target_eid is None:
            result.skipped_no_target_exchange += 1
            result.errors.append(
                f"cid={cid} {cur_exch}:{cur_tick} -> {override.target_exchange}:"
                f"{override.target_ticker} skipped — target exchange not in "
                f"`gurufocus_exchange` table."
            )
            continue

        target_key = (override.target_exchange.upper(), override.target_ticker.upper())
        target_cid = by_key.get(target_key)

        if target_cid is not None and target_cid != cid:
            # 3a. Target row already exists. Re-point universe_membership
            #     + company_source from old → target so the deletion
            #     doesn't drop the company out of any universe (e.g.
            #     LongEquity-sourced rows would otherwise vanish from
            #     LongEquity membership after merge). Then delete the
            #     stale company row. metric_data + portfolio_weight on
            #     the old cid are dropped — the target already has its
            #     own price/weight history under its (correct) key.
            result.deleted_as_duplicate += 1
            result.record_sample(
                f"merge cid={cid} {cur_exch}:{cur_tick} -> existing "
                f"cid={target_cid} {override.target_exchange}:{override.target_ticker}"
            )
            if dry_run:
                continue
            try:
                _merge_universe_membership(supabase, old_cid=cid, target_cid=target_cid)
                _merge_company_source(supabase, old_cid=cid, target_cid=target_cid)
                # metric_data / portfolio_weight on the old cid would be
                # duplicates of (potentially staler than) the target's
                # rows — drop them. The target's data is canonical.
                supabase.table("metric_data").delete().eq("company_id", cid).execute()
                supabase.table("portfolio_weight").delete().eq("company_id", cid).execute()
                supabase.table("company").delete().eq("company_id", cid).execute()
                # Update the in-memory index so a later iteration in the
                # same sweep doesn't try to remap into this just-deleted cid.
                old_key = (cur_exch.upper(), cur_tick.upper())
                by_key.pop(old_key, None)
            except Exception as e:
                result.errors.append(f"cid={cid} merge failed: {type(e).__name__}: {e}")
            continue

        # 3b. Target doesn't exist — UPDATE in place. Preserves the cid
        #     and every FK reference.
        result.remapped_in_place += 1
        result.record_sample(
            f"remap cid={cid} {cur_exch}:{cur_tick} -> "
            f"{override.target_exchange}:{override.target_ticker}"
        )
        if dry_run:
            continue
        try:
            supabase.table("company").update({
                "exchange_id": target_eid,
                "gurufocus_ticker": override.target_ticker,
            }).eq("company_id", cid).execute()
            # Update the in-memory index so subsequent iterations see
            # the new key (prevents a later row from being deleted as a
            # "duplicate" of this one).
            old_key = (cur_exch.upper(), cur_tick.upper())
            by_key.pop(old_key, None)
            by_key[target_key] = cid
        except Exception as e:
            result.errors.append(f"cid={cid} remap update failed: {type(e).__name__}: {e}")

    return result


def format_audit(r: SweepResult) -> str:
    lines = [
        f"Scanned: {r.rows_scanned} company rows",
        f"  Remap in place:        {r.remapped_in_place}",
        f"  Delete as duplicate:   {r.deleted_as_duplicate}",
        f"  Mark unavailable:      {r.marked_unavailable}",
        f"  Clear unavailable:     {r.cleared_unavailable}",
        f"  Skipped (no exchange): {r.skipped_no_target_exchange}",
        f"  Errors:                {len(r.errors)}",
    ]
    if r.sample_actions:
        lines.append("  Sample actions:")
        for s in r.sample_actions:
            lines.append(f"    {s}")
    if r.errors:
        lines.append("  Errors:")
        for e in r.errors[:10]:
            lines.append(f"    {e}")
    return "\n".join(lines)


if __name__ == "__main__":
    import sys  # noqa: PLC0415
    from deps import supabase  # noqa: PLC0415

    dry_run = "--apply" not in sys.argv
    res = apply_overrides_retroactively(supabase, dry_run=dry_run)
    print(format_audit(res))
    if dry_run:
        print("\n(dry run — pass --apply to actually mutate)")
