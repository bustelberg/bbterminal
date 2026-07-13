"""Bulk-resolve + fetch the per-unit payment history for /asset-pipeline rows.

    uv run python scripts/backfill_dividends.py --limit 50 --random
    uv run python scripts/backfill_dividends.py --limit 500          # most-liquid first
    uv run python scripts/backfill_dividends.py --limit 50 --dry-run # resolve only, no payments

WHAT IT COSTS
    Per ISIN, at most TWO GuruFocus calls, and only the first time:
      1. `isin/{ISIN}`          -> the listing. Cached in `gurufocus_listing` FOREVER,
                                   misses included, so an unresolvable ISIN is never
                                   re-billed.
      2. `stock/{sym}/dividend` -> the payments. Cached in Storage, and its freshness is
                                   judged against the inferred payout frequency, so a
                                   quarterly payer's cache survives ~a quarter.
    Re-running is therefore nearly free — it only pays for rows it has never seen.

WHY A SCRIPT AND NOT A PIPELINE PHASE
    This is a BACKFILL, not a refresh: the grid resolves rows lazily when you click
    Fetch, and this just does it ahead of time. It deliberately does NOT run on the
    scheduler — 16k ISINs x 2 calls would eat a month's quota in one tick.
"""
from __future__ import annotations

import argparse
import random
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Run from anywhere: scripts/ is a sibling of the backend packages, not inside them.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deps  # noqa: E402, F401  -- loads GURUFOCUS_API_KEY + SUPABASE_* before anything else
from fastapi import HTTPException  # noqa: E402

from deps import paginate, supabase  # noqa: E402
from routers._asset_dividends import (  # noqa: E402
    _payments_response_for_isin,
    _resolve_listing,
)

# GuruFocus sits behind Cloudflare and throttles a chatty client (the ingest phase logs
# a wedged tail at 12 workers). This is a backfill — nobody is waiting on it — so keep
# the pool small and let it take the time it takes.
_WORKERS = 4

_print_lock = threading.Lock()


def _usage() -> int:
    rows = supabase.table("api_usage").select("region, request_count").execute().data or []
    return sum(r.get("request_count") or 0 for r in rows)


def _grid_rows() -> list[dict]:
    return list(paginate(
        lambda lo, hi: (
            supabase.table("asset_grid")
            .select("isin, name, asset_class, currency, med_adv_eur")
            .not_.is_("isin", "null")
            .order("med_adv_eur", desc=True)
            .range(lo, hi)
            .execute()
        )
    ))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--random", action="store_true", help="random sample instead of most-liquid-first")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true", help="resolve only; don't fetch payments")
    args = ap.parse_args()

    rows = _grid_rows()
    print(f"[backfill] {len(rows)} grid rows with an ISIN")
    if args.random:
        random.Random(args.seed).shuffle(rows)
    rows = rows[: args.limit]
    print(f"[backfill] working {len(rows)} rows "
          f"({'random sample' if args.random else 'most-liquid first'}), {_WORKERS} workers\n")

    calls_before = _usage()
    t0 = time.monotonic()
    results: list[dict] = []
    done = 0

    def work(row: dict) -> dict:
        nonlocal done
        isin = row["isin"]
        out = {
            "isin": isin,
            "name": (row.get("name") or "")[:34],
            "cls": row.get("asset_class") or "?",
            "status": "?", "symbol": "", "is_home": None, "n": None, "err": "", "detail": "",
        }
        try:
            listing = _resolve_listing(isin)
            out["status"] = listing.get("status") or "?"
            out["is_home"] = listing.get("is_home")
            if listing.get("gurufocus_ticker"):
                out["symbol"] = f"{listing['exchange_code']}:{listing['gurufocus_ticker']}"
            if not args.dry_run and out["status"] == "ok":
                resp = _payments_response_for_isin(isin)
                out["n"] = len(resp.payments)
        except HTTPException as e:
            # An expected dead end (no_data / unsubscribed / not_applicable), not a
            # fault. It carries its own reason and is negative-cached — re-read the row
            # so the summary reports WHY rather than counting it as a crash.
            row2 = (supabase.table("gurufocus_listing").select("status")
                    .eq("isin", isin).limit(1).execute().data or [])
            out["status"] = (row2[0]["status"] if row2 else out["status"])
            out["detail"] = str(e.detail)[:48]
        except Exception as e:                      # noqa: BLE001 -- a bad row must not kill the run
            out["err"] = f"{type(e).__name__}: {e}"[:70]
        with _print_lock:
            done += 1
            n = out["n"]
            tail = out["err"] or out["detail"] or (
                f"{out['symbol']:<14} {'' if n is None else str(n) + ' payments':<13}"
                f"{'' if out['is_home'] in (None, True) else '  NOT-HOME'}"
            )
            print(f"  [{done:>3}/{len(rows)}] {out['cls']:<7} {out['name']:<34} "
                  f"{out['status']:<12} {tail}", flush=True)
        return out

    with ThreadPoolExecutor(max_workers=_WORKERS) as ex:
        results = list(ex.map(work, rows))

    elapsed = time.monotonic() - t0
    calls = _usage() - calls_before

    print(f"\n{'=' * 78}\n[backfill] {len(results)} rows in {elapsed:.0f}s · "
          f"{calls} GuruFocus calls ({calls / max(1, len(results)):.1f} per row)\n")

    by_status = Counter(r["status"] for r in results)
    print("resolution:")
    for s, c in by_status.most_common():
        print(f"   {s:<14} {c:>3}")

    ok = [r for r in results if r["status"] == "ok" and not r["err"]]
    paying = [r for r in ok if (r["n"] or 0) > 0]
    silent = [r for r in ok if r["n"] == 0]
    foreign = [r for r in ok if r["is_home"] is False]
    errs = [r for r in results if r["err"]]

    print(f"\npayments:\n   pays out       {len(paying):>3}"
          f"\n   NO PAYOUTS     {len(silent):>3}"
          f"\n   not-home line  {len(foreign):>3}"
          f"\n   errors         {len(errs):>3}")
    if paying:
        tot = sum(r["n"] for r in paying)
        print(f"\n   {tot} payment records fetched · "
              f"median {sorted(r['n'] for r in paying)[len(paying) // 2]} per payer")
    if errs:
        print("\nERRORS:")
        for r in errs[:12]:
            print(f"   {r['isin']}  {r['name']:<34} {r['err']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
