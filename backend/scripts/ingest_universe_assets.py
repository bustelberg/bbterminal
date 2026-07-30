"""Ingest a universe's constituents into the ASSET world (`asset_execution` + `asset_price`).

WHY
    A benchmark rebuilt from yfinance can only price what is IN the asset grid. ACWI's members
    come from the `company` universe, and 422 of them have an ISIN that was simply never
    ingested — Korea (78), China A (SHSE 108 + SZSE 65), Saudi (32), Malaysia (24). Those are not
    a capability gap: yfinance prices all of them. They are an INGEST gap, and this closes it.

    (Contrast GuruFocus, whose gaps are structural — it does not sell us the UK or India at any
    price. That is the whole reason the benchmark is moving to the asset world.)

⚠ THIS ONLY REACHES MEMBERS THAT HAVE AN ISIN.
    189 ACWI members have none — 156 of them Indian (NSE), 28 British. The ISIN is the bridge, so
    those cannot be reached from here at all, and GuruFocus cannot supply the ISIN either (it is
    blind to exactly those markets). They need a different identifier route and are reported, not
    silently skipped: a coverage number that quietly excludes India is worse than no number.

⚠ ONE YAHOO CONSUMER AT A TIME.
    Yahoo answers an overloaded caller with an EMPTY result rather than a 429, and an empty
    candidate set is how a resolution lands on a thin foreign listing (NVDA-on-Stuttgart). This
    pauses between ISINs and refuses to run while the ingest-queue worker is live.

    cd backend && PYTHONPATH=. uv run python scripts/ingest_universe_assets.py --universe ACWI
    cd backend && PYTHONPATH=. uv run python scripts/ingest_universe_assets.py --universe ACWI --apply
"""
from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # backend/ on path

import deps  # noqa: E402, F401  — loads .env
from deps import IN_CHUNK_SIZE, supabase  # noqa: E402


def _members(label: str) -> list[dict]:
    uni = (supabase.table("universe").select("universe_id")
           .eq("label", label).limit(1).execute().data or [])
    if not uni:
        return []
    ids = sorted({m["company_id"] for m in
                  (supabase.table("universe_membership").select("company_id")
                   .eq("universe_id", uni[0]["universe_id"]).execute().data or [])})
    rows: list[dict] = []
    for i in range(0, len(ids), IN_CHUNK_SIZE):
        rows += (supabase.table("company")
                 .select("company_id,company_name,isin,exchange_id")
                 .in_("company_id", ids[i:i + IN_CHUNK_SIZE])
                 .is_("delisted_at", "null").is_("out_of_scope_at", "null")
                 .execute().data or [])
    return rows


def _already_in_grid(isins: list[str]) -> set[str]:
    have: set[str] = set()
    for i in range(0, len(isins), IN_CHUNK_SIZE):
        for r in (supabase.table("asset_execution").select("isin")
                  .in_("isin", isins[i:i + IN_CHUNK_SIZE]).execute().data or []):
            have.add(r["isin"])
    return have


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", default="ACWI", help="universe label (e.g. ACWI, SP500)")
    ap.add_argument("--apply", action="store_true", help="persist (default: dry run)")
    ap.add_argument("--limit", type=int, default=0, help="stop after N (0 = all)")
    ap.add_argument("--sleep", type=float, default=1.0,
                    help="seconds between ISINs — Yahoo answers an overloaded caller with an "
                         "EMPTY result, not a 429, so never race it")
    a = ap.parse_args()

    from asset_pipeline import queue as _q  # noqa: PLC0415
    from asset_pipeline import store  # noqa: PLC0415

    if _q.is_worker_active():
        print(f"REFUSING: the ingest-queue worker is live (last activity {_q.last_activity()}). "
              f"Two Yahoo consumers is how a resolution lands on the wrong listing.")
        return 1

    mem = _members(a.universe)
    if not mem:
        print(f"no universe labelled {a.universe!r}")
        return 1

    # The ones we cannot even try — no ISIN, no bridge. Reported, never silently dropped.
    no_isin = [m for m in mem if not m.get("isin")]
    with_isin = [m for m in mem if m.get("isin")]
    have = _already_in_grid(sorted({m["isin"] for m in with_isin}))
    todo = [m for m in with_isin if m["isin"] not in have]

    print(f"{a.universe}: {len(mem)} live members")
    print(f"   already in asset_execution : {len(with_isin) - len(todo)}")
    print(f"   to ingest                  : {len(todo)}")
    print(f"   UNREACHABLE (no ISIN)      : {len(no_isin)}", flush=True)
    if no_isin:
        by_ex = defaultdict(int)
        ex = {e["exchange_id"]: e["exchange_code"] for e in
              (supabase.table("gurufocus_exchange")
               .select("exchange_id,exchange_code").execute().data or [])}
        for m in no_isin:
            by_ex[ex.get(m.get("exchange_id")) or "?"] += 1
        top = ", ".join(f"{k} {v}" for k, v in sorted(by_ex.items(), key=lambda x: -x[1])[:6])
        print(f"      -> {top}   (need an ISIN before they can be bridged at all)")
    print(flush=True)

    if a.limit:
        todo = todo[: a.limit]
    if not a.apply:
        for m in todo[:15]:
            print(f"   would ingest {m['isin']}  {(m.get('company_name') or '')[:40]}")
        print(f"\n   dry run — {len(todo)} ISIN(s); re-run with --apply")
        return 0

    ok = failed = 0
    for i, m in enumerate(todo, 1):
        isin = m["isin"]
        try:
            # `store_one` returns the analysis SYMBOL as a plain string (not a dict) and the bar
            # count under `rows`. Reading it as a dict raised inside the try, which reported a
            # SUCCESSFUL ingest as a failure — the rows were in the DB the whole time.
            res = store.store_one(isin) or {}
            ok += 1
            print(f"   [{i}/{len(todo)}] {isin} -> {res.get('analysis')} "
                  f"({res.get('rows')} bars)  {(m.get('company_name') or '')[:30]}", flush=True)
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"   [{i}/{len(todo)}] {isin} FAILED {type(e).__name__}: {e}", flush=True)
        time.sleep(a.sleep)

    print(f"\n   ingested={ok}  failed={failed}  (still unreachable, no ISIN: {len(no_isin)})")
    # ⚠ A PRICE SERIES IS NOT ENOUGH — A CAP-WEIGHTED INDEX NEEDS A CAP. A freshly ingested row
    # has bars but `market_cap_eur = NULL`, so it would be silently DROPPED from the index it was
    # just ingested for. Say so; the backfill is a separate, batched Yahoo pass.
    print("\n   NOTE: newly ingested rows have NO market cap yet and cannot be weighted. Run:")
    print("      uv run python scripts/asset_backfill_marketcap.py --only-missing")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
