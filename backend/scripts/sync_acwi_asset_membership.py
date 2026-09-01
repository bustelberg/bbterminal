"""Rebuild ACWI's asset-side membership from the committed iShares file.

Run this after replacing `index_universe/iShares-MSCI-ACWI-ETF_fund.xls` — iShares blocks scripted
downloads, so the file is committed and this is what turns it into membership.

⚠ IT IS NOT THE COMPANY UNIVERSE. `universe_membership` (the GuruFocus-resolved company side) is
untouched; this only writes `index_file_membership`, which the `universe_asset_membership` view
unions in. See that migration for why the two are separate.

Usage (from backend/):
    uv run python scripts/sync_acwi_asset_membership.py
    uv run python scripts/sync_acwi_asset_membership.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # noqa: E402,F401
from index_universe.acwi.asset_membership import resolve, sync  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="resolve and report, write nothing")
    ap.add_argument("--label", default="ACWI")
    args = ap.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(message)s")

    if args.dry_run:
        rows, stats = resolve()
        print(json.dumps({**stats, "would_write": len(rows)}, indent=2))
        # A few, so a reader can see the shape of what would land rather than only a count.
        for r in rows[:8]:
            print(f"   {r['ticker']:<10} {r['yahoo_symbol']:<12} -> analysis {r['analysis_id']}")
        return 0

    out = sync(args.label)
    print(json.dumps(out, indent=2))
    return 0 if out.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
