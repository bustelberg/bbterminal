"""Diagnose — and optionally queue — the constituents a rebuilt index cannot price.

    uv run python scripts/close_benchmark_coverage.py --universe ACWI
    uv run python scripts/close_benchmark_coverage.py --universe ACWI --universe SP500
    uv run python scripts/close_benchmark_coverage.py --universe ACWI --apply

⚠⚠ IT MAKES NO VENDOR CALLS. `--apply` only UPSERTS rows into `asset_ingest_queue` as `pending`;
   the existing worker (`asset_pipeline.queue.process_slice`) does the resolving, paced, one clean
   pass at a time. That split is deliberate and it is the safe one: Yahoo answers an overloaded
   caller with an EMPTY LIST rather than a 429, so a script that fanned out its own resolution
   would manufacture exactly the `not_found` rows this exists to clean up.

⚠⚠ IT NEVER TOUCHES A ROW THAT ALREADY RESOLVED. `enqueue(skip_existing=True)` drops anything
   already `ok`, and that guard is the whole safety story: re-resolving a good row is DESTRUCTIVE —
   Alphabet was once re-resolved onto a Vienna listing 75,000x thinner than Nasdaq. Only rows that
   have never resolved, or resolved to `not_found`/`error`, are ever queued here.

WHAT IT IS FOR
    The Analyse modal's benchmark warning reports how much of an index we could rebuild. Measured
    on ACWI 2026-09-02 that was 1,849 of 1,998 (92.5%) — and the missing names are not spread
    evenly: **India 2 of 161**, the UK 41 of 72, Hong Kong 152 of 182, against a United States that
    is 474 of ~476. This prints that breakdown and splits it by CAUSE, because the three causes
    have three different fixes and only two of them are queueable:

      no ISIN            the bridge into the asset world is `company.isin`, so a member without one
                         cannot be reached at all. NOT queueable — it needs an ISIN first
                         (OpenFIGI ticker+exchange lookup). This is nearly all of India.
      never queued       has an ISIN, no `asset_execution` row. Queue it.
      not_found / error  has an ISIN, resolution failed — often only because Yahoo was throttled at
                         the time. Queue it, skipping OpenFIGI types that are genuinely unpriceable
                         (bonds, rights, warrants), exactly as `queue.requeue_unmapped` does.
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: F401,E402  — loads .env before anything reads it
from asset_pipeline.queue import enqueue, is_worker_active  # noqa: E402
from asset_pipeline.queue import _UNPRICEABLE_TYPES  # noqa: E402,PLC2701
from deps import IN_CHUNK_SIZE, supabase  # noqa: E402


def _universe_company_rows(label: str) -> list[dict]:
    """Every company in the universe, with its ISIN, country and asset-row state.

    ⚠ PAGED. `universe_membership` is thousands of rows and PostgREST caps a page at 1,000 on the
    cloud — an unpaged read here would report a confident, wrong, and much rosier picture.
    """
    uni = (supabase.table("universe").select("universe_id")
           .eq("label", label).limit(1).execute().data or [])
    if not uni:
        return []
    uid = uni[0]["universe_id"]
    ids: list[int] = []
    off = 0
    while True:
        page = (supabase.table("universe_membership").select("company_id")
                .eq("universe_id", uid).order("company_id")
                .range(off, off + 999).execute().data or [])
        if not page:
            break
        ids.extend(r["company_id"] for r in page)
        off += len(page)
    ids = sorted(set(ids))

    rows: list[dict] = []
    for i in range(0, len(ids), IN_CHUNK_SIZE):
        rows += (supabase.table("company")
                 .select("company_id,company_name,isin,gurufocus_exchange:gurufocus_exchange("
                         "exchange_code,country:country(country_name))")
                 .in_("company_id", ids[i:i + IN_CHUNK_SIZE]).execute().data or [])
    return rows


def _asset_state(isins: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for i in range(0, len(isins), IN_CHUNK_SIZE):
        for r in (supabase.table("asset_execution")
                  .select("isin,status,analysis_id,openfigi_figi,openfigi_type")
                  .in_("isin", isins[i:i + IN_CHUNK_SIZE]).execute().data or []):
            out[r["isin"]] = r
    return out


def _report(label: str, apply: bool) -> int:
    rows = _universe_company_rows(label)
    if not rows:
        print(f"[{label}] no members - is the label right?", file=sys.stderr)
        return 0
    state = _asset_state([r["isin"] for r in rows if r.get("isin")])

    buckets: dict[str, list[dict]] = defaultdict(list)
    by_country: dict[str, list[int]] = defaultdict(lambda: [0, 0])   # [missing, members]
    for r in rows:
        country = (((r.get("gurufocus_exchange") or {}).get("country") or {})
                   .get("country_name")) or "Unclassified"
        by_country[country][1] += 1
        isin = r.get("isin")
        if not isin:
            kind = "no ISIN"
        else:
            st = state.get(isin)
            if st is None:
                kind = "never queued"
            elif st.get("status") == "ok" and st.get("analysis_id"):
                kind = "priced"
            else:
                kind = f"resolve {st.get('status')}"
        if kind != "priced":
            by_country[country][0] += 1
        buckets[kind].append({**r, "isin": isin, "asset": state.get(isin or "")})

    priced = len(buckets.get("priced", []))
    print(f"\n[{label}] {priced} of {len(rows)} members bridge to a price series "
          f"({100.0 * priced / len(rows):.1f}%)")
    print("  by cause:")
    for kind in sorted(buckets, key=lambda k: -len(buckets[k])):
        if kind != "priced":
            print(f"    {kind:22s} {len(buckets[kind]):5d}")
    worst = sorted(((c, m, n) for c, (m, n) in by_country.items() if m), key=lambda t: -t[1])[:6]
    print("  worst countries (missing of members):")
    for c, m, n in worst:
        print(f"    {c:24s} {m:4d} of {n:4d}")

    # ⚠ QUEUEABLE = has an ISIN AND is not already `ok`. A `no ISIN` member cannot be queued at all
    #   — there is nothing to queue — and saying so is the point of splitting the buckets.
    queueable: list[str] = []
    unpriceable = 0
    for kind, items in buckets.items():
        if kind == "priced" or kind == "no ISIN":
            continue
        for it in items:
            a = it.get("asset") or {}
            # Same filter as `queue.requeue_unmapped`: an OpenFIGI type we know cannot be priced
            # (bond, right, warrant) stays unmapped rather than burning a worker slot every pass.
            if a and (a.get("openfigi_type") or "") in _UNPRICEABLE_TYPES:
                unpriceable += 1
                continue
            if it["isin"]:
                queueable.append(it["isin"])

    no_isin = len(buckets.get("no ISIN", []))
    print(f"\n  queueable now      : {len(queueable)}")
    print(f"  unpriceable type   : {unpriceable} (bonds/rights/warrants - left alone)")
    print(f"  needs an ISIN first: {no_isin}  (!) not fixable by queueing; needs an OpenFIGI "
          f"ticker+exchange lookup")

    if not apply:
        print(f"\n  DRY RUN - nothing written. Re-run with --apply to queue {len(queueable)}.")
        return 0
    if not queueable:
        print("\n  nothing to queue.")
        return 0
    # ⚠ `skip_existing=True` — the guard that makes this safe to re-run. Never `False` here: that
    #   is the flag that re-resolves rows which already work.
    res = enqueue(queueable, skip_existing=True)
    print(f"\n  queued {res['queued']} (skipped {res['skipped_existing']} already ok)")
    return res["queued"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--universe", action="append", default=None, help="label; repeatable")
    ap.add_argument("--apply", action="store_true",
                    help="actually queue the fixable ISINs (default: report only)")
    args = ap.parse_args()

    # ⚠ A LIVE WORKER IS A REASON TO WAIT, NOT TO ABORT — queueing is a DB write and harmless, but
    #   a worker draining a fresh backlog while another already runs is how Yahoo starts answering
    #   with empty lists and manufacturing `not_found`. Said out loud; the operator decides.
    if args.apply and is_worker_active():
        print("(!) the ingest worker is ACTIVE. Queueing anyway is safe (this only writes rows), but "
              "let the current pass finish before starting another drain.", file=sys.stderr)

    total = sum(_report(u, args.apply) for u in (args.universe or ["ACWI"]))
    if args.apply:
        print(f"\nqueued {total} ISINs. Drain with the /asset-pipeline worker "
              f"(`asset_pipeline.queue.process_slice`) - it paces itself against Yahoo.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
