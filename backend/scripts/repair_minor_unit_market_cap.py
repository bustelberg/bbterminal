"""Repair market caps stored against a MINOR-unit currency (GBp / GBX / ZAc / ILA).

THE BUG
    Yahoo's v7 quote reports ONE `currency` for TWO different units: it quotes a London listing's
    PRICE in pence and, in the same payload, its `marketCap` in POUNDS — both labelled `"GBp"`.
    `asset_backfill_marketcap.py` stored that quote currency on the cap, and `fx_to_eur("GBp")`
    then correctly applied the /100 that a PRICE needs to a figure that was already major-unit.

        SHEL.L   native 166.43bn GBP  ->  stored EUR   1.95bn   (a EUR 195bn company)
        HSBA.L          251.26bn GBP  ->  stored EUR   2.94bn
        AZN.L           223.92bn GBP  ->  stored EUR   2.62bn

    Exactly 100x too small, and still a plausible number — which is why nothing caught it. In
    ACWI the 36 minor-unit members carried 0.02% of index weight instead of ~1.93%: the UK names
    were ingested (0 -> 44) and then weighted to nothing. `covered_pct` cannot see this — it
    counts members PRICED, and all 36 counted as covered while contributing ~0.

    `ZAc`/`ILA` failed differently and worse: `fx_to_eur` special-cased "GBp" inline and knew
    nothing of them, so it asked Yahoo for a nonexistent "ZAcEUR=X", got None, and the cap landed
    NULL — dropping those companies from a cap-weighted index entirely rather than under-weighting
    them.

THE REPAIR
    `market_cap_native` is NOT affected — it is Yahoo's figure, already in the major unit, and it
    is the ground truth here. Only the two DERIVED fields are wrong, so both are recomputed from
    native rather than patched: multiplying the stored EUR by 100 would fix GBp and leave the NULL
    ZAc rows broken, and it would trust a number we know is wrong.

    One FX lookup per distinct MAJOR currency (~3), cached. Idempotent: once repaired no row
    carries a minor-unit `market_cap_currency`, so a re-run finds nothing.

    uv run python scripts/repair_minor_unit_market_cap.py           # dry run (default)
    uv run python scripts/repair_minor_unit_market_cap.py --apply
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402,F401
from deps import supabase  # noqa: E402

from asset_pipeline import yahoo  # noqa: E402
from asset_pipeline.fx import SUBUNIT  # noqa: E402

_PAGE = 1000


def _affected() -> list[dict]:
    """Every analysis whose market cap is stored against a minor-unit currency. Paged — PostgREST
    silently caps a read at 1,000 rows, and a partial repair that reports success is worse than
    no repair at all."""
    out: list[dict] = []
    off = 0
    while True:
        batch = (supabase.table("asset_analysis")
                 .select("analysis_id,symbol,market_cap_native,market_cap_currency,market_cap_eur")
                 .in_("market_cap_currency", sorted(SUBUNIT))
                 .range(off, off + _PAGE - 1).execute().data or [])
        out += batch
        if len(batch) < _PAGE:
            return out
        off += _PAGE


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="write the repair (default is a dry run that changes nothing)")
    args = ap.parse_args()

    rows = _affected()
    if not rows:
        print("Nothing to repair — no market cap is stored against a minor-unit currency.")
        return
    print(f"{len(rows):,} analyses have a market cap in a minor unit.\n")

    plans: list[tuple[dict, str, float | None]] = []
    skipped_no_native = skipped_no_fx = 0
    for r in rows:
        minor = r["market_cap_currency"]
        major = SUBUNIT[minor][0]
        native = r.get("market_cap_native")
        if not native:
            # No ground truth to recompute from. Leave it — a guessed cap is worse than none, and
            # the next backfill will write it correctly now that the writer is fixed.
            skipped_no_native += 1
            continue
        fx = yahoo.fx_to_eur(major)      # the MAJOR rate: no subunit divisor, that was the bug
        if not fx:
            skipped_no_fx += 1
            continue
        plans.append((r, major, round(float(native) * fx, 2)))

    print(f"{'symbol':<12}{'ccy':>5} ->{'':<5}{'native (bn)':>13}{'eur now (bn)':>14}{'eur after (bn)':>16}")
    for r, major, eur in sorted(plans, key=lambda p: -(p[2] or 0))[:12]:
        now = r.get("market_cap_eur")
        now_s = f"{float(now)/1e9:,.2f}" if now else "NULL"
        print(f"{(r.get('symbol') or '?'):<12}{r['market_cap_currency']:>5} -> {major:<4}"
              f"{float(r['market_cap_native'])/1e9:>13,.2f}{now_s:>14}{(eur or 0)/1e9:>16,.2f}")
    if len(plans) > 12:
        print(f"  … and {len(plans) - 12:,} more")

    print()
    if skipped_no_native:
        print(f"⚠ {skipped_no_native:,} skipped — no `market_cap_native` to recompute from.")
    if skipped_no_fx:
        print(f"⚠ {skipped_no_fx:,} skipped — no FX rate available for the major currency.")

    if not args.apply:
        print(f"\nDRY RUN — nothing written. {len(plans):,} rows would be repaired.")
        print("Re-run with --apply to write.")
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    for i, (r, major, eur) in enumerate(plans, 1):
        supabase.table("asset_analysis").update({
            "market_cap_currency": major,
            "market_cap_eur": eur,
            "market_cap_checked_at": now_iso,
        }).eq("analysis_id", r["analysis_id"]).execute()
        if i % 100 == 0:
            print(f"  {i:,}/{len(plans):,} repaired…", flush=True)
    print(f"\nDone — {len(plans):,} analyses repaired.")


if __name__ == "__main__":
    main()
