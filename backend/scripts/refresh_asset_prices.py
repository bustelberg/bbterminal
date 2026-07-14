"""Refresh STALE `asset_price` series from Yahoo — the manual twin of the daily scheduler job.

The detection and the fetching both live in `asset_pipeline/price_refresh.py`, which the
scheduler (`_fire_asset_price_refresh`, 06:00 UTC daily) calls with the same arguments. ONE
implementation: a cron that drifts from the script you debug with is a cron nobody trusts.

Read that module for the two traps — staleness is measured against the freshest close WE HOLD
(never the calendar, or a weekend reads as a fleet-wide failure), and a refresh fetches THE GAP
(`extend_series`), not the whole history (`store_series` re-downloads KO's 16,239 bars back to
1962 to add eight days).

    cd backend && PYTHONPATH=. uv run python scripts/refresh_asset_prices.py --held
    cd backend && PYTHONPATH=. uv run python scripts/refresh_asset_prices.py --held --apply
    cd backend && PYTHONPATH=. uv run python scripts/refresh_asset_prices.py --held --apply --stale-days 1
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402, F401  — loads .env
from asset_pipeline import price_refresh  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="persist (default: dry run)")
    ap.add_argument("--held", action="store_true",
                    help="only instruments held by an AIRS model portfolio (~220, not 16k)")
    ap.add_argument("--stale-days", type=int, default=price_refresh.DEFAULT_STALE_DAYS,
                    help="refresh a row whose last close is this many days behind the GLOBAL "
                         "freshest close we hold (not behind today — a market that has not "
                         "published yet is not a stale row)")
    ap.add_argument("--limit", type=int, default=0, help="stop after N rows (0 = all)")
    ap.add_argument("--sleep", type=float, default=price_refresh.DEFAULT_SLEEP_S,
                    help="seconds between Yahoo calls; Yahoo answers an overloaded caller with "
                         "an EMPTY result rather than a 429, so do not race it")
    a = ap.parse_args()

    if not a.apply:
        stale, latest, considered = price_refresh.find_stale(a.held, a.stale_days)
        print(f"global freshest close: {latest}")
        print(f"{len(stale)} of {considered} {'held' if a.held else 'all'} instrument(s) stale "
              f"by >= {a.stale_days}d — DRY RUN\n", flush=True)
        for r in stale[: (a.limit or len(stale))]:
            print(f"  would refresh {r['yahoo_symbol']:12} last close {r['last_close']}  "
                  f"{(r.get('name') or '')[:32]}")
        print("\n  dry run — re-run with --apply to persist")
        return 0

    r = price_refresh.refresh_stale(
        held_only=a.held, stale_days=a.stale_days, limit=a.limit, sleep_s=a.sleep,
        on_progress=lambda m: print(f"  {m}", flush=True),
    )
    print(f"\n  {r['stale']} of {r['considered']} stale vs {r['global_latest']} — "
          f"moved={r['moved']}  unchanged={r['unchanged']}  failed={r['failed']}"
          + (f"  SKIPPED={r['skipped']} (--limit)" if r["skipped"] else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
