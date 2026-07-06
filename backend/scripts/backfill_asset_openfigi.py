"""Backfill the OpenFIGI identity columns on asset_execution.

The asset-pipeline grid resolves the openfigi_* columns (figi / name / ticker /
exchCode / securityType) at INGEST time. Rows created before that wiring — or
before these columns existed — have them NULL. This script fills them in with a
light OpenFIGI-only pass (batched; NO Yahoo price re-fetch), so an existing
catalog gets its OpenFIGI columns without a full re-ingest. Idempotent.

Usage:
    uv run python scripts/backfill_asset_openfigi.py          # only rows missing figi
    uv run python scripts/backfill_asset_openfigi.py --all    # re-fetch every row

Env: OPENFIGI_API_KEY raises the batch size (100 vs 10) + rate limit.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402
from asset_pipeline import openfigi  # noqa: E402


def main(only_missing: bool = True) -> None:
    sb = deps.supabase
    rows = (
        sb.table("asset_execution").select("execution_id, isin, openfigi_figi").execute().data
        or []
    )
    todo = [
        r for r in rows
        if r.get("isin") and (not only_missing or not r.get("openfigi_figi"))
    ]
    print(f"{len(todo)} rows to backfill (of {len(rows)} total)")
    if not todo:
        return

    fmap = openfigi.lookup_isins([r["isin"] for r in todo])
    updated = missed = 0
    for r in todo:
        cols = openfigi.extract_columns(fmap.get(r["isin"].strip().upper(), []))
        if not any(cols.values()):
            missed += 1
            continue
        sb.table("asset_execution").update(cols).eq("execution_id", r["execution_id"]).execute()
        updated += 1
    print(f"updated {updated} rows, {missed} had no OpenFIGI match")


if __name__ == "__main__":
    main(only_missing="--all" not in sys.argv)
