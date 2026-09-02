"""Precompute relative 12-1 momentum for one or more universes.

    uv run python scripts/compute_relative_momentum.py --universe ACWI
    uv run python scripts/compute_relative_momentum.py --universe ACWI --universe AEX --verbose
    uv run python scripts/compute_relative_momentum.py --universe ACWI --dry-run

⚠ INTENDED CADENCE: once a day, AFTER the 05:00 UTC `price_update` tick — the ranks are only as
  current as the closes they are built from, and running before it ranks yesterday's prices under
  today's date. Not yet wired into `scheduler.py`; run it by hand until the shape is settled.

⚠ `--as-of` DEFAULTS TO THE NEWEST CLOSE WE ACTUALLY HOLD, not to today. Today is a date we may
  have no prices for (a weekend, a holiday, a pipeline that has not run), and asking for it would
  either drop every name on the staleness rule or rank an empty set. `latest_db_price_date()` is
  the same answer /backtest uses for its default end date.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

# `import deps` first — it loads .env / .env.local before anything reads the env.
import deps  # noqa: F401,E402
from momentum import relative  # noqa: E402
from routers.momentum._helpers import latest_db_price_date  # noqa: E402


def _run(universe: str, as_of: date, *, dry_run: bool, verbose: bool) -> int:
    t0 = time.perf_counter()
    prefix = f"[{universe}]"

    def step(msg: str) -> None:
        # ⚠ Printed unconditionally, not behind --verbose. This takes seconds per universe and a
        #   silent wait is indistinguishable from a hang; --verbose adds the library's own logging
        #   on top, it does not gate the fact that something is happening.
        print(f"{prefix} {msg}", flush=True)

    try:
        result = relative.compute(universe, as_of, on_step=step)
    except Exception as e:
        # ⚠ Named loudly and per universe, and the loop continues: one dead universe must not stop
        #   the others from being refreshed.
        print(f"{prefix} FAILED: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
        return 0

    dist = result.rows["state"].value_counts().reindex(range(-3, 4), fill_value=0)
    spread = " ".join(f"{relative.STATE_LABELS[s]}={int(dist[s])}" for s in range(-3, 4))
    step(f"distribution  {spread}")
    step(f"coverage      {result.universe_n} of {result.members_total} "
         f"({result.coverage_pct:.1f}%)")

    if dry_run:
        step(f"dry run — nothing written ({time.perf_counter() - t0:.1f}s)")
        if verbose:
            top = result.rows.nlargest(5, "raw_return_pct")
            for r in top.itertuples(index=False):
                print(f"{prefix}   company {int(r.company_id):>6}  "
                      f"{r.raw_return_pct:+9.1f}%  p{r.pct_rank * 100:5.1f}  "
                      f"{relative.STATE_LABELS[int(r.state)]}", flush=True)
        return 0

    written = relative.persist(result)
    step(f"wrote {written} rows in {time.perf_counter() - t0:.1f}s")
    return written


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--universe", action="append", default=None,
                    help="universe label; repeatable (default: ACWI)")
    ap.add_argument("--as-of", default=None,
                    help="YYYY-MM-DD (default: the newest close in the database)")
    ap.add_argument("--dry-run", action="store_true", help="compute and report, write nothing")
    ap.add_argument("--verbose", action="store_true", help="library logging + a sample of the top")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    universes = args.universe or ["ACWI"]
    if args.as_of:
        as_of = date.fromisoformat(args.as_of)
    else:
        as_of = latest_db_price_date()
        if as_of is None:
            print("no close prices in the database — nothing to rank", file=sys.stderr)
            return 1
        print(f"as-of {as_of} (newest close held)", flush=True)

    total = sum(_run(u, as_of, dry_run=args.dry_run, verbose=args.verbose) for u in universes)
    print(f"done — {total} rows across {len(universes)} universe(s)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
