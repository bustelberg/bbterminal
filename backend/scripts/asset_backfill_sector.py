"""Backfill the real Yahoo sector on equities the FAST path left as sector='equity'.

The fast resolver builds the price series from the chart endpoint, which carries
no sector — so fast-resolved equities (Honeywell, DuPont…) fall back to
sector='equity'. This fills the real sector from v10 quoteSummary assetProfile
(Honeywell -> Industrials), so the sector filter/column are accurate. One request
per symbol; re-runnable (only touches sector='equity' / NULL equities).

    uv run python scripts/asset_backfill_sector.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402,F401
from deps import supabase  # noqa: E402

from asset_pipeline import short_etf, yahoo  # noqa: E402


def main() -> None:
    rows: list[dict] = []
    off = 0
    while True:
        r = (
            supabase.table("asset_analysis").select("analysis_id, symbol, sector")
            .eq("asset_class", "equity").range(off, off + 999).execute().data
        ) or []
        rows += [x for x in r if (x.get("sector") in (None, "equity")) and x.get("symbol")]
        if len(r) < 1000:
            break
        off += 1000

    syms = [x["symbol"] for x in rows]
    print(f"{len(syms):,} equities missing a real sector — fetching assetProfile…", flush=True)
    profiles = yahoo.asset_profile(syms)
    print(f"got a sector for {len(profiles):,} of them.", flush=True)

    updated = 0
    for x in rows:
        p = profiles.get(x["symbol"])
        if p and p.get("sector"):
            sec = short_etf.normalize_sector(p["sector"])  # → canonical taxonomy
            supabase.table("asset_analysis").update({"sector": sec}).eq(
                "analysis_id", x["analysis_id"]
            ).execute()
            updated += 1
            if updated % 50 == 0:
                print(f"  {updated:,} updated…", flush=True)
    print(f"\nDone — {updated:,} sectors backfilled ({len(syms) - updated:,} still without one).", flush=True)


if __name__ == "__main__":
    main()
