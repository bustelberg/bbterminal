"""Repair ETF-overlay entry prices corrupted by a truncated `benchmark_price` read.

WHAT WENT WRONG
    `_schedule_snapshots` built its as-of price lookup by reading a benchmark's whole series
    ASCENDING and UNPAGED. PostgREST caps a response at 1,000 rows on Supabase cloud (10,000
    locally) and truncates SILENTLY, so in production the series stopped at the OLDEST thousand
    bars and the as-of lookup answered every recent date with the last row it happened to hold.

    Measured 2026-08-03 on SPMO (Invesco S&P 500 Momentum ETF), 2,716 bars from 2015-10-12: a
    1,000-row cut ends at 2019-10-01, price 40.18. /schedule showed exactly that as the entry —
    "Start (local) 40.18 USD, as of 2026-07-31" — beside a correct End of 143.83 on the SAME
    date. A +258% return on a position days old, which drifted the Current weight to 74.5%
    against a 45.0% target. 4 of 5 benchmarks exceed 1,000 bars, so this was live for every ETF
    overlay in production and invisible in local dev.

    The read is paged now, so new rebalances are correct. This repairs what is already stored.

⚠ IT FIXES `entry_price_local` AND NOTHING ELSE, ON PURPOSE.
    Every other field on an ETF holding is DERIVED from it, and the pipeline's own re-pricer
    owns that arithmetic: `compute_and_save_price_update` re-derives `entry_price_eur` from the
    local price and the entry-date FX on every run (unconditionally, for ETFs), and recomputes
    `exit_price_local`, `exit_date`, `exit_price_eur` and `forward_return_pct` from it. Writing
    those here would be a SECOND implementation of that arithmetic — the exact duplication this
    codebase keeps being bitten by. Fix the one stored input; let the writer own the rest.

    So after this runs the row still shows its old return until the next re-price. That is one
    tick away: press "Run now" on the /schedule price_update card, or wait for the 05:00 UTC
    daily. The script says so when it finishes.

⚠ IT ONLY TOUCHES ROWS THAT ARE ACTUALLY WRONG. Every candidate is re-derived from
    `benchmark_price` as of the holding's OWN stored `entry_date` — which is exactly what the
    fixed code would have written — and a row already matching is left completely alone. A
    repair that rewrites everything cannot be told apart from a repair that broke something.

⚠ AND IT PAGES ITS OWN READS. Repairing a truncation bug with a truncated read would write the
    same wrong number back, with more confidence.

USAGE — RUN IT FROM `backend/`, AND IT TARGETS PRODUCTION BY DEFAULT
    There are two `scripts/` directories in this repo; this is the one under `backend/`. The repo
    root has no `pyproject.toml`, so `uv run` from there picks a bare interpreter with none of the
    dependencies.

        cd backend
        uv run python scripts/fix_etf_entry_price.py           # PROD, dry run — changes nothing
        uv run python scripts/fix_etf_entry_price.py --apply   # PROD, writes the corrections
        uv run python scripts/fix_etf_entry_price.py --local   # rehearse locally (finds nothing)
        uv run python scripts/fix_etf_entry_price.py --apply --strategy-hash abc123…

    It prints the target host and the mode before touching anything. Check that line.
"""
from __future__ import annotations

import argparse
import bisect
import os
import sys
from pathlib import Path

# The standard bootstrap every script here uses — run from anywhere, import the backend package.
_BACKEND = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_BACKEND))

# ⚠ THIS SCRIPT TARGETS **PRODUCTION** BY DEFAULT, WHICH IS THE OPPOSITE OF EVERY OTHER ENTRY
#     POINT HERE — AND IT IS DELIBERATE. The corruption it repairs is caused by PostgREST's
#     1,000-row cap, which is a CLOUD setting: local runs at 10,000 and is therefore never
#     affected. A local-by-default run of this script is guaranteed to find nothing, every time,
#     which is worse than useless — it reads as "there is nothing wrong".
#
# ⚠ SO THE PROD CREDENTIALS ARE RE-APPLIED **AFTER** IMPORTING `deps`, AND THAT ORDER IS THE
#     WHOLE TRICK. `deps` does its own `load_dotenv(".env")` then
#     `load_dotenv(".env.local", override=True)` at import time, so anything set beforehand —
#     an exported variable, a `.env` we loaded ourselves — is clobbered by the local file. (I
#     wrote it the obvious way first and it silently ran against local; the target banner is what
#     caught it.)
#
#     `deps.supabase` is a LAZY proxy that reads `os.environ` when the client is first built, so
#     overwriting the two keys here, before any query, is both sufficient and the only ordering
#     that works. `dotenv_values` parses the file WITHOUT touching `os.environ`, so nothing is
#     applied until we choose to.
#
#     `--local` skips the re-apply and leaves `deps`' normal precedence intact.
from dotenv import dotenv_values  # noqa: E402

_LOCAL = "--local" in sys.argv
_PROD_ENV = dotenv_values(_BACKEND / ".env")

import deps  # noqa: E402,F401  — loads .env then .env.local(override); we undo that below
from deps import supabase  # noqa: E402

if not _LOCAL:
    for _k in ("SUPABASE_URL", "SUPABASE_SERVICE_KEY"):
        if _PROD_ENV.get(_k):
            os.environ[_k] = str(_PROD_ENV[_k])

_PAGE = 1000

# Prices this close are the same number to the cent; below it we are looking at float noise, not
# at the seven-year gap this exists to repair.
_EPS = 0.005


def _paged(build) -> list[dict]:
    """Read every matching row. See the module docstring — a truncated read here would write the
    bug back."""
    out: list[dict] = []
    off = 0
    while True:
        page = build().range(off, off + _PAGE - 1).execute().data or []
        if not page:
            break
        out += page
        off += len(page)
    return out


def _series(benchmark_id: int) -> tuple[list[str], list[float]]:
    """(dates, prices) ascending for one benchmark — the whole series, paged."""
    rows = _paged(lambda: supabase.table("benchmark_price")
                  .select("target_date, price")
                  .eq("benchmark_id", benchmark_id)
                  .order("target_date"))
    return ([str(r["target_date"])[:10] for r in rows],
            [float(r["price"]) for r in rows if r["price"] is not None])


def _asof(series: tuple[list[str], list[float]], day: str) -> float | None:
    """The last close ON OR BEFORE `day` — the identical rule the snapshot writer uses."""
    ds, ps = series
    if not ds or not day:
        return None
    i = bisect.bisect_right(ds, day) - 1
    return ps[i] if 0 <= i < len(ps) else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="write the corrections (default is a dry run that changes nothing)")
    ap.add_argument("--strategy-hash", default=None,
                    help="limit to one strategy (default: every stored snapshot)")
    ap.add_argument("--local", action="store_true",
                    help="target the LOCAL database instead of production (honours .env.local). "
                         "Local is never affected by this bug — the row cap is 10,000 there — so "
                         "this is for rehearsal only and will normally find nothing.")
    args = ap.parse_args()

    # ⚠ NAME THE TARGET BEFORE DOING ANYTHING. This is the only script here that writes to
    # production by default, so the one thing a reader must not have to infer is which database
    # is about to change. Printed for a dry run too — that is when you check it.
    url = os.environ.get("SUPABASE_URL", "<UNSET>")
    host = url.split("//", 1)[-1].split("/", 1)[0] or url
    is_local = "127.0.0.1" in url or "localhost" in url
    where = "LOCAL" if is_local else "PRODUCTION"
    # ⚠ `deps` ALREADY PRINTED A DIFFERENT URL ABOVE AND IT IS NOT THE ONE WE WILL USE. It logs
    # `[deps] SUPABASE_URL = …` at import, which is `.env.local` (local) — we overwrite it
    # immediately afterwards. Two contradicting lines in a terminal, the wrong one first, is how
    # someone concludes they ran against local and moves on; this names which one is real.
    print(f"Target: {where}  ({host})")
    if not is_local:
        print("        (supersedes the [deps] line above — that is .env.local, loaded on import)")
    if args.apply and not is_local:
        print("Mode:   APPLY — this WILL write to production.")
    elif args.apply:
        print("Mode:   APPLY (local).")
    else:
        print("Mode:   dry run — nothing will be written.")
    if is_local and not args.local:
        # The env has been overridden from outside; say so rather than silently doing nothing
        # useful, because "nothing to repair" against local is a foregone conclusion.
        print("        ⚠ SUPABASE_URL points at local although --local was not passed. This bug "
              "cannot occur locally, so expect no findings.")
    print()

    print("Reading snapshots…", flush=True)
    snaps = _paged(lambda: (
        supabase.table("current_picks_snapshot")
        .select("snapshot_id, strategy_hash, as_of_date, kind, holdings")
        .eq("strategy_hash", args.strategy_hash) if args.strategy_hash else
        supabase.table("current_picks_snapshot")
        .select("snapshot_id, strategy_hash, as_of_date, kind, holdings")
    ).order("snapshot_id"))
    print(f"  {len(snaps)} snapshot(s)")

    cache: dict[int, tuple[list[str], list[float]]] = {}
    fixes: list[tuple[int, str, dict]] = []      # (snapshot_id, label, new holdings blob)
    n_holdings = 0

    for s in snaps:
        holdings = s.get("holdings") or []
        changed = False
        new_holdings = []
        for h in holdings:
            cid = h.get("company_id")
            # ETF overlay sleeves only — the negative-company_id convention. A real company's
            # entry comes from `metric_data` through a different path and is not affected.
            if cid is None or cid >= 0:
                new_holdings.append(h)
                continue
            bid = -int(cid)
            entry_date = str(h.get("entry_date") or "")[:10]
            stored = h.get("entry_price_local")
            if not entry_date or stored in (None, 0):
                new_holdings.append(h)
                continue
            if bid not in cache:
                cache[bid] = _series(bid)
            correct = _asof(cache[bid], entry_date)
            if correct is None or abs(float(stored) - correct) < _EPS:
                new_holdings.append(h)          # already right — left completely alone
                continue
            fixed = dict(h)
            fixed["entry_price_local"] = correct
            new_holdings.append(fixed)
            changed = True
            n_holdings += 1
            print(f"  snapshot {s['snapshot_id']:<6} {s['as_of_date']} {str(s['kind'])[:13]:<13} "
                  f"{str(h.get('ticker')):<8} entry {float(stored):>10.4f} -> {correct:>10.4f} "
                  f"(as of {entry_date})")
        if changed:
            fixes.append((s["snapshot_id"], str(s["as_of_date"]), new_holdings))

    if not fixes:
        print("\nNothing to repair — every stored ETF entry already matches "
              "`benchmark_price` as of its own entry date.")
        return 0

    print(f"\n{n_holdings} ETF holding(s) across {len(fixes)} snapshot(s) have a wrong "
          f"`entry_price_local`.")
    if not args.apply:
        print("DRY RUN — nothing written. Re-run with --apply to write these corrections.")
        return 0

    written = 0
    for sid, label, blob in fixes:
        try:
            (supabase.table("current_picks_snapshot")
             .update({"holdings": blob}).eq("snapshot_id", sid).execute())
            written += 1
        except Exception as e:  # noqa: BLE001 — one bad row must not abandon the rest
            print(f"  ! snapshot {sid} ({label}) FAILED: {type(e).__name__}: {e}")
    print(f"\nWrote {written} of {len(fixes)} snapshot(s).")
    # ⚠ THE DERIVED FIELDS ARE STILL STALE UNTIL THE PIPELINE RECOMPUTES THEM — by design; see
    # the module docstring. Saying so is the difference between "the fix did not work" and "the
    # fix is one tick from visible".
    print("The Return / EUR / drifted-weight columns still show their OLD values: they are "
          "derived, and `compute_and_save_price_update` owns them. Press \"Run now\" on the "
          "/schedule price_update card (or wait for the 05:00 UTC daily) and the rows correct "
          "themselves.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
