"""Merge two `company` rows that are the SAME security — the case no existing tool covers.

WHY THIS EXISTS
    `dedupe_by_isin` merges rows sharing an ISIN. `merge_name_dupes_keep_isin` folds a no-ISIN
    stub whose name is the keeper's plus trailing corporate forms. `company_override` (kind
    `alias`) merges by ISIN → canonical ISIN. All three need an ISIN on the row being folded, or
    a name that folds by suffix. NONE of them handles the case that actually happens when a
    company CHANGES LISTING:

        3813  EchoStar Corp  US2787681061  NASDAQ:SATS  (dead)   ACWI, Leonteq, + 2 frozen
        6418  EchoStar       (no ISIN)     NYSE:ECHO    (live)   SP500

    An index reconstruction meets the new ticker, finds no ISIN to match on, and creates a
    SECOND row. Now one company owns two rows and the memberships are SPLIT across them — the
    older row keeps the universes and stops being priced (its ticker is dead), while the live
    series accumulates on a row no universe can see. Nothing errors. The constituent simply
    goes stale, and past the 30-day signal staleness guard it drops out of selection.

    ⚠ THE ISIN-BEARING ROW IS USUALLY THE ONE TO KEEP, EVEN THOUGH ITS DATA IS STALER. It holds
    the identity every bridge joins on and — decisively — the FROZEN universe snapshots, which
    record what was true at freeze time and must not be rewritten. Keep it and repoint its
    ticker; do not migrate a frozen membership onto a different company_id.

⚠⚠ THE SAFETY GATE IS THE PRICE SERIES, NOT THE NAME. Two rows having similar names proves
    nothing (`Siemens Ltd` India vs `Siemens AG`), and this tool moves universe memberships —
    getting it wrong silently reassigns an index constituent. So the gate is that the two
    series AGREE ON THEIR OVERLAP: same company, same shares, same closes. Measured on
    EchoStar: 4,653 overlapping days, 5 disagreeing, max difference 0.08. A pair that fails
    this is refused outright.

    uv run python scripts/merge_duplicate_company.py --keep 3813 --drop 6418 \
        --set-ticker ECHO --set-exchange NYSE                       # dry run
    ... --apply
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

# WARNING: THE WINDOWS CONSOLE IS cp1252, AND AN UNENCODABLE CHARACTER IS A CRASH, NOT A GLYPH.
#   One non-ASCII arrow in a print() raised UnicodeEncodeError *after* the safety gate had passed
#   and printed its verdict - i.e. the tool died at its most misleading possible moment, looking
#   for all the world like the check had failed. Printed output below is therefore kept ASCII;
#   this is the belt-and-braces so the docstring (argparse prints it on --help) degrades to "?"
#   instead of taking the process down.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")  # type: ignore[union-attr]

import deps  # noqa: E402,F401  # loads env + Supabase before anything imports it
from deps import supabase  # noqa: E402

# Every table with a FK onto company.company_id, paired with THE REST OF ITS PRIMARY KEY.
#
# WARNING: THIS IS THE COLLISION KEY, AND GETTING IT WRONG IS A 23505 MID-MERGE. Most of these
#   tables key on (company_id, <something>), so if BOTH rows already have an entry for the same
#   <something>, re-pointing company_id lands on the keeper's existing row. That is not
#   hypothetical: EchoStar's dropped row had `company_source` (company_id, source_code) =
#   (6418, 'sp500') while the keeper already held (3813, 'sp500'), and the merge died there
#   having ALREADY moved the universe membership. A duplicate that collides is redundant by
#   definition - the keeper's copy says the same thing - so it is DELETED, not moved.
#
#   `leonteq_equity` is the exception: a surrogate `id` PK, so company_id can repeat freely and
#   there is nothing to collide on. None means "move every row, no collisions possible".
_MOVE_TABLES = (
    ("universe_membership", ("universe_id", "target_month")),
    ("company_source", ("source_code",)),
    ("portfolio_weight", ("portfolio_id",)),
    ("earnings_portfolio_member", ("portfolio_id",)),
    ("leonteq_equity", None),
)
_PRICE_CODES = ("close_price", "volume")

# Overlap gate. Both are deliberately loose: vendors round differently across listings, and the
# question is "is this the same security", not "are these byte-identical".
_MIN_OVERLAP_DAYS = 200
_MAX_DISAGREE_FRAC = 0.02
_MAX_ABS_DIFF = 1.0


def _rows(table: str, select: str, **flt) -> list[dict]:
    out, off = [], 0
    while True:
        q = supabase.table(table).select(select)
        for k, v in flt.items():
            q = q.eq(k, v)
        # Ordered so paging is deterministic - an unordered .range() can serve a row twice or
        # never, which on a merge means a silently dropped membership.
        batch = q.order("company_id").range(off, off + 999).execute().data or []
        if not batch:
            break
        out += batch
        off += len(batch)
        if len(batch) < 1000:
            break
    return out


def _series(cid: int, code: str) -> dict[str, float]:
    out: dict[str, float] = {}
    off = 0
    while True:
        batch = (supabase.table("metric_data")
                 .select("target_date,numeric_value")
                 .eq("company_id", cid).eq("metric_code", code)
                 .order("target_date").range(off, off + 999).execute().data or [])
        if not batch:
            break
        for r in batch:
            if r.get("numeric_value") is not None:
                out[r["target_date"]] = float(r["numeric_value"])
        off += len(batch)
        if len(batch) < 1000:
            break
    return out


def _gate(keep: int, drop: int) -> tuple[bool, str]:
    """Refuse unless the two close_price series agree where they overlap."""
    a, b = _series(keep, "close_price"), _series(drop, "close_price")
    shared = sorted(set(a) & set(b))
    if len(shared) < _MIN_OVERLAP_DAYS:
        return False, (f"only {len(shared)} overlapping days (need {_MIN_OVERLAP_DAYS}) - "
                       "cannot establish these are the same security")
    diffs = [abs(a[d] - b[d]) for d in shared]
    bad = [d for d in diffs if d > _MAX_ABS_DIFF]
    frac = len(bad) / len(shared)
    ok = frac <= _MAX_DISAGREE_FRAC
    msg = (f"{len(shared):,} overlapping days | {len(bad)} disagree by > {_MAX_ABS_DIFF} "
           f"({frac:.2%}) | max diff {max(diffs):.4f}")
    return ok, msg


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--keep", type=int, required=True, help="company_id to keep (the ISIN one)")
    ap.add_argument("--drop", type=int, required=True, help="company_id to merge away")
    ap.add_argument("--set-ticker", default=None, help="repoint the keeper's gurufocus_ticker")
    ap.add_argument("--set-exchange", default=None, help="repoint the keeper's exchange (code)")
    ap.add_argument("--apply", action="store_true", help="persist (default: dry run)")
    args = ap.parse_args()

    if args.keep == args.drop:
        sys.exit("--keep and --drop must differ")

    both = (supabase.table("company")
            .select("company_id,company_name,isin,gurufocus_ticker,exchange_id,market_cap_eur")
            .in_("company_id", [args.keep, args.drop]).execute().data or [])
    by_id = {int(r["company_id"]): r for r in both}
    if args.keep not in by_id or args.drop not in by_id:
        sys.exit(f"company not found: {sorted({args.keep, args.drop} - set(by_id))}")
    k, d = by_id[args.keep], by_id[args.drop]
    print(f"KEEP  {k['company_id']}  {k['company_name']!r}  isin={k['isin']}  "
          f"ticker={k['gurufocus_ticker']}")
    print(f"DROP  {d['company_id']}  {d['company_name']!r}  isin={d['isin']}  "
          f"ticker={d['gurufocus_ticker']}")

    if d.get("isin") and k.get("isin") and d["isin"] != k["isin"]:
        print("\nWARNING: BOTH rows carry a DIFFERENT ISIN. That is two securities by this app's own "
              "definition (share classes, or an ADR vs its ordinary line) - use a "
              "`company_override` alias if you really mean to consolidate them. Refusing.")
        sys.exit(1)

    ok, msg = _gate(args.keep, args.drop)
    print(f"\nprice-series gate: {msg}")
    if not ok:
        print("REFUSED - the two series do not agree on their overlap, so they are not "
              "demonstrably the same security. Nothing written.")
        sys.exit(1)
    print("   -> same security.")

    # -- what would move --------------------------------------------------------------------
    print("\nrows attached to the dropped company:")
    moves: dict[str, list[dict]] = {}
    collisions: dict[str, list[dict]] = {}
    for table, key in _MOVE_TABLES:
        try:
            rows = _rows(table, "*", company_id=args.drop)
        except Exception as e:  # noqa: BLE001  # table may not exist in every environment
            print(f"   {table:26} (skipped: {type(e).__name__})")
            continue
        keep_rows = _rows(table, "*", company_id=args.keep) if key and rows else []
        taken = {tuple(r[c] for c in key) for r in keep_rows} if key else set()
        clash = [r for r in rows if key and tuple(r[c] for c in key) in taken]
        moves[table] = [r for r in rows if r not in clash]
        collisions[table] = clash
        note = f"  ({len(clash)} already on the keeper - will be deleted, not moved)" if clash else ""
        print(f"   {table:26} {len(rows):>5}{note}")

    kmax = {c: max(_series(args.keep, c) or {"": ""}, default="") for c in _PRICE_CODES}
    dser = {c: _series(args.drop, c) for c in _PRICE_CODES}
    gap = {c: sorted(dt for dt in dser[c] if dt > (kmax[c] or "")) for c in _PRICE_CODES}
    for c in _PRICE_CODES:
        print(f"   metric_data {c:12} keeper ends {kmax[c] or '-'} | "
              f"{len(gap[c])} newer bar(s) to copy across")

    if args.set_ticker or args.set_exchange:
        print(f"\nrepoint keeper listing -> {args.set_exchange or '(unchanged)'}:"
              f"{args.set_ticker or '(unchanged)'}")

    if not args.apply:
        print("\ndry run - re-run with --apply to persist")
        return

    # -- apply ------------------------------------------------------------------------------
    #
    # ORDER IS A CONSTRAINT, NOT A PREFERENCE. `company` has a UNIQUE (gurufocus_ticker,
    # exchange_id), and in the case this tool exists for the DROPPED row is precisely the one
    # holding the listing the keeper needs to move to - the duplicate was created BY that
    # listing. Repointing first therefore dies on 23505 every time. So: copy the data across,
    # move the FK rows, delete the duplicate to free the key, and only then repoint.
    #
    # Resolve the exchange BEFORE any write, so an unknown code fails while everything is still
    # untouched rather than after the duplicate has been deleted.
    patch: dict = {}
    if args.set_ticker:
        patch["gurufocus_ticker"] = args.set_ticker
    if args.set_exchange:
        ex = (supabase.table("gurufocus_exchange").select("exchange_id")
              .eq("exchange_code", args.set_exchange).limit(1).execute().data or [])
        if not ex:
            sys.exit(f"unknown exchange code {args.set_exchange!r}")
        patch["exchange_id"] = ex[0]["exchange_id"]

    # Copy the newer bars BEFORE deleting anything, so a failure here leaves the dropped row -
    # and its data - still present rather than half-merged.
    for c in _PRICE_CODES:
        if not gap[c]:
            continue
        src = {r["target_date"]: r for r in (supabase.table("metric_data")
               .select("*").eq("company_id", args.drop).eq("metric_code", c)
               .in_("target_date", gap[c]).execute().data or [])}
        payload = [{**src[dt], "company_id": args.keep} for dt in gap[c] if dt in src]
        if payload:
            supabase.table("metric_data").upsert(
                payload, on_conflict="company_id,metric_code,source_code,target_date").execute()
            print(f"copied {len(payload)} {c} bar(s) to the keeper")

    for table, key in _MOVE_TABLES:
        # Delete the colliding rows FIRST, so the bulk update below has nothing left to clash
        # with. Scoped by this table's own key - reading it from `_MOVE_TABLES` rather than
        # naming columns here is what stops a new table silently deleting more than it should.
        for r in collisions.get(table, []):
            q = supabase.table(table).delete().eq("company_id", args.drop)
            for col in (key or ()):
                q = q.eq(col, r[col])
            q.execute()
        if collisions.get(table):
            print(f"dropped {len(collisions[table])} redundant row(s) in {table} "
                  "(the keeper already had them)")
        if moves.get(table):
            supabase.table(table).update({"company_id": args.keep}) \
                .eq("company_id", args.drop).execute()
            print(f"moved {len(moves[table])} row(s) in {table}")

    supabase.table("metric_data").delete().eq("company_id", args.drop).execute()
    supabase.table("company").delete().eq("company_id", args.drop).execute()
    print(f"deleted company {args.drop}")

    # Now the (ticker, exchange) key is free.
    if patch:
        try:
            supabase.table("company").update(patch).eq("company_id", args.keep).execute()
            print(f"repointed keeper: {patch}")
        except Exception as e:  # noqa: BLE001
            # The merge itself is already committed and correct; only the repoint is missing, so
            # say exactly how to finish rather than leaving a half-done state to be guessed at.
            print(f"\nREPOINT FAILED after a SUCCESSFUL merge: {type(e).__name__}: {e}")
            print(f"The data is merged and company {args.drop} is gone - only the keeper's "
                  f"listing is unchanged. Finish with:")
            print(f"   uv run python scripts/merge_duplicate_company.py --keep {args.keep} "
                  f"--drop {args.keep} --set-ticker {args.set_ticker or ''} "
                  f"--set-exchange {args.set_exchange or ''}   # or set it by hand")
            raise

    # No membership backfill to re-run: `universe_asset_membership` is a VIEW over
    # `universe_membership` + the ISIN bridge (migration 20260806060000), so a membership this
    # tool moved is already reflected on the asset side.
    print("\nDone.")


if __name__ == "__main__":
    main()
