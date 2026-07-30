"""Audit the OpenFIGI -> yfinance mapping of EVERY asset row.

Read-only by default: walks every `asset_grid` row and checks whether the
resolved yfinance analysis is the SAME company OpenFIGI identified for the ISIN
(rapidfuzz name match — the same test the dedupe/suspect logic uses). Prints each
mismatch + a summary. Pass --fix to RE-QUEUE the mismatched rows so the worker
re-resolves them cleanly (still NO Yahoo here — just a DB write), so it's safe to
run anytime, even while the worker is going.

    uv run python scripts/asset_audit_mappings.py          # report only
    uv run python scripts/asset_audit_mappings.py --fix     # report + re-queue bad rows

Caveat: this catches WRONG-COMPANY mappings (yfinance resolved to a different
company than the ISIN). It does NOT catch same-company-wrong-listing (a name that
matches but on a thin/wrong exchange) — the names match there by definition.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402,F401  (loads env + Supabase client)
from asset_pipeline import queue  # noqa: E402
from asset_pipeline.resolve import same_company  # noqa: E402

APPLY = "--fix" in sys.argv


def _load_all() -> list[dict]:
    rows: list[dict] = []
    off = 0
    while True:
        r = (
            deps.supabase.table("asset_grid")
            .select("isin, name, analysis_symbol, status, openfigi_name, openfigi_type")
            .range(off, off + 999).execute().data
        ) or []
        rows += r
        if len(r) < 1000:
            break
        off += 1000
    return rows


def main() -> None:
    rows = _load_all()
    ok = no_figi = unmapped = 0
    bad: list[dict] = []
    for r in rows:
        if r.get("status") != "ok":
            unmapped += 1
            continue
        figi_name = r.get("openfigi_name")
        if not figi_name:
            no_figi += 1
            continue
        if same_company(r.get("name"), figi_name):
            ok += 1
        else:
            bad.append(r)

    for r in sorted(bad, key=lambda x: x["isin"]):
        print(f"  MISMATCH {r['isin']}: yfinance {r.get('analysis_symbol') or '?'} "
              f"({(r.get('name') or '?')[:26]})  !=  OpenFIGI {r.get('openfigi_name')}", flush=True)

    print(f"\n{len(rows)} rows · {ok} verified-correct · {len(bad)} MISMATCH · "
          f"{no_figi} no-OpenFIGI-name · {unmapped} unmapped", flush=True)

    if bad and APPLY:
        res = queue.enqueue([r["isin"] for r in bad], skip_existing=False)
        print(f"--fix: re-queued {res['queued']} mismatched rows — the worker will re-resolve them.", flush=True)
    elif bad:
        print("Run with --fix to re-queue these for the worker to re-resolve.", flush=True)


if __name__ == "__main__":
    main()
