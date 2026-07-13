"""One-off: pull every ISIN out of the AIRS model portfolios and add the MISSING ones to
the Execution-instruments grid.

    uv run python scripts/add_portfolio_isins.py --dry-run      # collect + diff, add nothing
    uv run python scripts/add_portfolio_isins.py                # add them
    uv run python scripts/add_portfolio_isins.py --limit 10     # add the first 10

WHAT IT WILL NOT DO
    It never touches an ISIN that is ALREADY in the grid. That is not tidiness — re-resolving
    an existing row is DESTRUCTIVE. `resolve()` ranks Yahoo's candidates by median traded
    value and would pick the right listing every time, but Yahoo answers a search with an
    EMPTY LIST under load rather than a 429; drop the real listing from the candidate set and
    a thin foreign line wins by default. Measured: a re-resolve of Alphabet Class A repointed
    it from GOOGL (EUR 8.79bn median daily traded value, 5,502 bars) to GOOA.VI -- VIENNA,
    EUR 76,634, 2,302 bars. A 75,000x thinner listing, silently. So: ADD ONLY.

THE SAME TRAP STILL APPLIES TO THE NEW ROWS
    A bulk resolve is exactly when Yahoo starts returning empties, and a NEW ISIN has no
    previous listing to compare against -- nothing errors, the row just quietly points at
    something thin. So every added row prints its median daily traded value, and anything
    under THIN_ADV_EUR is flagged loudly. Review the flagged ones; `scripts/
    repoint_primary_listing.py` is the tool for fixing them.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deps  # noqa: E402, F401  -- loads SUPABASE_* / BROKER_* before anything else
from deps import IN_CHUNK_SIZE, supabase  # noqa: E402

from airs_scanner import (  # noqa: E402
    fetch_model_portfolios_sync,
    fetch_portfolio_positions_sync,
)

# Below this, a listing is almost certainly NOT the primary one (Vienna's Alphabet line
# trades EUR 76k/day against Nasdaq's EUR 8.8bn). Not an error -- a prompt to look.
THIN_ADV_EUR = 250_000

# Yahoo goes quiet (empty results, not 429) when hammered. A pause between resolves is the
# cheapest defence against the mis-resolution this whole script is trying not to cause.
PAUSE_S = 1.0


def _row_adv(isin: str) -> float | None:
    """The median daily traded value of the row we just wrote. `store_one` doesn't return
    it, so the THIN check has to read it back — otherwise the check never fires."""
    rows = (supabase.table("asset_grid").select("med_adv_eur")
            .eq("isin", isin).limit(1).execute().data or [])
    return rows[0].get("med_adv_eur") if rows else None


def _existing_isins(isins: list[str]) -> set[str]:
    """Which of these are ALREADY rows in the grid — the ones we must not touch."""
    have: set[str] = set()
    for i in range(0, len(isins), IN_CHUNK_SIZE):
        chunk = isins[i:i + IN_CHUNK_SIZE]
        rows = (supabase.table("asset_execution").select("isin")
                .in_("isin", chunk).execute().data or [])
        have.update(r["isin"] for r in rows)
    return have


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="collect + diff, add nothing")
    ap.add_argument("--limit", type=int, default=0, help="only add the first N missing ISINs")
    args = ap.parse_args()

    # ── 1. Every portfolio ────────────────────────────────────────────────────────────
    print("[1/3] listing AIRS model portfolios...", flush=True)
    portfolios = fetch_model_portfolios_sync(None)
    print(f"      {len(portfolios)} portfolios\n", flush=True)

    # ── 2. Every ISIN in every portfolio ──────────────────────────────────────────────
    print("[2/3] reading positions (one XLS per portfolio)...", flush=True)
    isin_sources: dict[str, list[str]] = {}     # isin -> the portfolios holding it
    empty = 0
    for i, p in enumerate(portfolios, 1):
        try:
            pos = fetch_portfolio_positions_sync(p["id"])
        except Exception as e:  # noqa: BLE001 -- one bad portfolio must not kill the run
            print(f"  [{i:>3}/{len(portfolios)}] {p['name']:<26} ERROR {type(e).__name__}: {e}",
                  flush=True)
            continue
        rows = [str(r["ISINCode"]).strip() for r in pos["rows"] if r.get("ISINCode")]
        for isin in rows:
            isin_sources.setdefault(isin, []).append(p["name"])
        if not rows:
            empty += 1
        print(f"  [{i:>3}/{len(portfolios)}] {p['name']:<26} "
              f"{pos['datum'] or '-':<11} {len(rows):>3} ISINs "
              f"({len(isin_sources)} distinct so far)", flush=True)

    all_isins = sorted(isin_sources)
    print(f"\n      {len(all_isins)} distinct ISINs across {len(portfolios) - empty} "
          f"portfolios with a composition ({empty} have none)\n", flush=True)

    # ── 3. Add only what's missing ────────────────────────────────────────────────────
    have = _existing_isins(all_isins)
    missing = [i for i in all_isins if i not in have]
    print(f"[3/3] {len(have)} already in the grid, {len(missing)} missing", flush=True)

    if args.limit:
        missing = missing[: args.limit]
        print(f"      --limit {args.limit}: adding the first {len(missing)}", flush=True)
    if args.dry_run:
        print("\n--dry-run: nothing added. The missing ISINs:\n", flush=True)
        for isin in missing:
            src = isin_sources[isin]
            print(f"   {isin}  held by {len(src)} portfolio(s): {', '.join(src[:3])}"
                  f"{' ...' if len(src) > 3 else ''}", flush=True)
        return 0

    print(flush=True)
    from asset_pipeline import store  # noqa: PLC0415

    added, unresolved, failed, thin = 0, 0, 0, []
    for n, isin in enumerate(missing, 1):
        held_by = isin_sources[isin][0]
        try:
            res = store.store_one(isin)
            sym = (res.get("analysis") or "?")
            # store_one does NOT return the traded value, so read it back off the row it
            # just wrote. Without this the THIN check silently never fires — a dead guard,
            # which is worse than no guard, because it looks like one.
            adv = float(_row_adv(isin) or 0)
            bars = res.get("rows") or 0
            flag = ""
            if adv and adv < THIN_ADV_EUR:
                flag = "  <-- THIN, check the listing"
                thin.append((isin, sym, adv))
            print(f"  [{n:>3}/{len(missing)}] {isin}  ok    {sym:<14} "
                  f"EUR {adv:>15,.0f}/day  {bars:>5} bars{flag}", flush=True)
            added += 1
        except ValueError as e:
            # Recorded in the grid as an unmapped row -- an in-house fund with no listing,
            # a bond, a delisted line. An answer, not a failure.
            print(f"  [{n:>3}/{len(missing)}] {isin}  unmapped  ({held_by}): "
                  f"{str(e)[:60]}", flush=True)
            unresolved += 1
        except Exception as e:  # noqa: BLE001
            print(f"  [{n:>3}/{len(missing)}] {isin}  FAILED  {type(e).__name__}: "
                  f"{str(e)[:60]}", flush=True)
            failed += 1
        time.sleep(PAUSE_S)

    print(f"\n{'=' * 78}")
    print(f"added {added} · unmapped {unresolved} · failed {failed}")
    if thin:
        print(f"\n{len(thin)} row(s) resolved to a THIN listing -- Yahoo may have returned an "
              f"empty search and the ranker took what was left. Review these:")
        for isin, sym, adv in thin:
            print(f"   {isin}  {sym:<14} EUR {adv:,.0f}/day")
    return 0


if __name__ == "__main__":
    sys.exit(main())
