"""Find companies whose GuruFocus price history contradicts our own yfinance series.

⚠⚠ THE GATE IN `refuse_unsubscribed` STOPS THIS HAPPENING AGAIN; IT CANNOT UNDO WHAT IS STORED.
Diploma plc was filled before that gate existed and its rows are still in `metric_data` — a price
column that is 0 for fifteen years and then frozen for seven, feeding every Fundamental-modal chart
that touches it. This is how such a company is FOUND, since nothing about the payload itself looks
wrong (see `ingest/earnings/price_sanity` for the two detectors that were tried and rejected).

⚠ READ-ONLY. It names companies and prints the evidence; it deletes nothing. Purging a company's
rows is a separate, deliberate act.

Usage:
    uv run python scripts/audit_vendor_price_series.py
    uv run python scripts/audit_vendor_price_series.py --all      # list the passes too
    uv run python scripts/audit_vendor_price_series.py --company 188
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deps  # noqa: E402
from common.pg import load_rows_via_copy  # noqa: E402
from ingest.earnings.price_sanity import compare  # noqa: E402

PRICE_CODE = "quarterly__Valuation and Quality__Month End Stock Price"


def _paged(table: str, select: str, build, order: str) -> list[dict]:
    """⚠ PAGED. PostgREST truncates at 1,000 rows on cloud and this reads whole tables; an unpaged
    version would audit the first thousand rows and report the rest as clean."""
    out: list[dict] = []
    off = 0
    while True:
        rows = build(deps.supabase.table(table).select(select)).order(order) \
            .range(off, off + 999).execute().data or []
        if not rows:
            return out
        out += rows
        off += len(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true", help="print companies that pass, too")
    ap.add_argument("--company", type=int, default=0, help="audit one company_id")
    args = ap.parse_args()

    print("Reading companies...", flush=True)
    comps = {c["company_id"]: c for c in _paged(
        "company", "company_id,company_name,gurufocus_ticker,isin,"
                   "gurufocus_exchange:gurufocus_exchange(exchange_code)",
        lambda q: q, "company_id")}

    print("Reading the vendor's price series...", flush=True)
    vendor: dict[int, list[tuple[str, float]]] = defaultdict(list)
    off = 0
    while True:
        rows = (deps.supabase.table("metric_data")
                .select("company_id,target_date,numeric_value")
                .eq("metric_code", PRICE_CODE)
                .order("company_id").order("target_date")
                .range(off, off + 999).execute().data or [])
        if not rows:
            break
        for r in rows:
            vendor[r["company_id"]].append((r["target_date"], r["numeric_value"] or 0.0))
        off += len(rows)
    print(f"  {len(vendor)} companies carry one", flush=True)

    # ⚠ THE BRIDGE IS THE ISIN, the only key the two worlds share — see `timeseries.resolve`, which
    # refuses to mix them for exactly this reason. A company with no ISIN cannot be audited.
    print("Bridging to the asset world by ISIN...", flush=True)
    by_isin: dict[str, int] = {}
    for r in _paged("asset_execution", "isin,analysis_id,status",
                    lambda q: q.eq("status", "ok"), "isin"):
        if r.get("analysis_id") is not None:
            by_isin[r["isin"]] = r["analysis_id"]

    todo = [cid for cid in vendor
            if (comps.get(cid) or {}).get("isin") in by_isin
            and (not args.company or cid == args.company)]
    print(f"  {len(todo)} companies have both a vendor series and one of ours\n", flush=True)

    # ⚠⚠ ONE `COPY`, NOT 1,721 PAGED READS. `asset_price` holds ~9,000 bars per instrument, so
    # per-company PostgREST paging would be millions of rows over HTTP and this script would take
    # hours — the exact cost `common/pg.load_rows_via_copy` exists to remove (measured elsewhere in
    # this codebase at 17 pages/12.68s -> 0.80s). One statement, one MVCC snapshot.
    # ⚠ IT FALLS BACK RATHER THAN FAILING: `load_rows_via_copy` returns None when the direct
    # connection is unconfigured, and the paged path below is then correct, only slow.
    print("Reading our own closes...", flush=True)
    aids = sorted({by_isin[comps[cid]["isin"]] for cid in todo})
    ours_by_aid: dict[int, list[tuple[str, float]]] = defaultdict(list)
    bulk = load_rows_via_copy("asset_price", "analysis_id,target_date,close", "analysis_id", aids)
    if bulk is None:
        print("  (no direct connection - falling back to paged reads, this will be slow)",
              flush=True)
        for aid in aids:
            for r in _paged("asset_price", "analysis_id,target_date,close",
                            lambda q, a=aid: q.eq("analysis_id", a), "target_date"):
                if r.get("close") is not None:
                    ours_by_aid[aid].append((str(r["target_date"]), float(r["close"])))
    else:
        for r in bulk:
            if r.get("close") is not None:
                ours_by_aid[r["analysis_id"]].append((str(r["target_date"]), float(r["close"])))
        for v in ours_by_aid.values():
            v.sort()
    print(f"  {sum(len(v) for v in ours_by_aid.values()):,} bars "
          f"for {len(ours_by_aid)} instruments\n", flush=True)

    bad, checked, abstained = [], 0, 0
    for n, cid in enumerate(sorted(todo), start=1):
        c = comps[cid]
        ours = ours_by_aid.get(by_isin[c["isin"]], [])
        v = compare(vendor[cid], ours)
        checked += 1
        if v.compared < 6:
            abstained += 1
        if not v.ok:
            bad.append((cid, c, v))
            print(f"  [{n}/{len(todo)}] FAIL {c['company_name']} ({c['gurufocus_ticker']}) - {v.reason}",
                  flush=True)
            for d in v.detail:
                print(f"        {d}")
        elif args.all:
            print(f"  [{n}/{len(todo)}] ok   {c['company_name']}: {v.reason}", flush=True)

    print(f"\n{len(bad)} contradicted, {checked} checked, {abstained} abstained (too thin)")
    if bad:
        print("\nThese carry a vendor price history that our own data does not support:")
        for cid, c, v in bad:
            ex = ((c.get("gurufocus_exchange") or {}) or {}).get("exchange_code")
            print(f"  company_id={cid:<6} {c['company_name'][:34]:<34} {ex:<7} {v.reason}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
