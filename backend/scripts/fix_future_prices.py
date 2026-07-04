"""Diagnose + clean FUTURE-DATED price rows (the SPMO +277% incident).

A close price can never be dated after today. A corrupt future-dated tick from
GuruFocus poisons the momentum engine: `latest_data_date` jumps forward, a
rebalance gets pinned to an upcoming (not-yet-happened) grid date, and the ETF
overlay prices entry against that future bar — an impossible entry_date and a
nonsensical return. `ingest/prices.py::_parse_price_series` now drops such rows
on ingest; this one-shot cleans what's ALREADY stored and flags the snapshots
that captured a bad ETF entry so you can re-rebalance them.

Scans `metric_data` (close_price + volume) and `benchmark_price` for
target_date > today, plus `current_picks_snapshot` holdings with a future
entry_date. Read-only by default.

Run from the backend dir (hits whatever SUPABASE_URL is in the loaded env —
move `.env.local` aside first to target PROD, per CLAUDE.md):

    uv run python scripts/fix_future_prices.py              # dry-run (report only)
    uv run python scripts/fix_future_prices.py --apply      # delete future rows

`--apply` deletes only the future-dated price rows. It does NOT rewrite
snapshots: after cleaning, trigger a fresh rebalance of any flagged strategy
(POST /api/ingest/scheduled-refresh/trigger?job_name=rebalance, or the
/schedule "Run now" button) so the ETF overlay re-anchors its entry correctly.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # noqa: E402,F401 — loads .env + constructs the Supabase client
from deps import supabase  # noqa: E402

TODAY = date.today().isoformat()


def _scan_metric_data() -> list[dict]:
    rows: list[dict] = []
    for metric in ("close_price", "volume"):
        resp = (
            supabase.table("metric_data")
            .select("id, company_id, metric_code, target_date, numeric_value")
            .eq("metric_code", metric)
            .gt("target_date", TODAY)
            .order("target_date", desc=True)
            .limit(1000)
            .execute()
        )
        rows.extend(resp.data or [])
    return rows


def _scan_benchmark_price() -> list[dict]:
    resp = (
        supabase.table("benchmark_price")
        .select("benchmark_id, target_date, price")
        .gt("target_date", TODAY)
        .order("target_date", desc=True)
        .limit(1000)
        .execute()
    )
    return resp.data or []


def _scan_snapshots_with_future_entry() -> list[dict]:
    """Recent snapshots whose holdings carry a future entry_date (captured
    before the fix). These need a fresh rebalance to re-anchor entry."""
    resp = (
        supabase.table("current_picks_snapshot")
        .select("snapshot_id, scheduled_strategy_id, kind, as_of_date, holdings")
        .order("snapshot_id", desc=True)
        .limit(200)
        .execute()
    )
    flagged: list[dict] = []
    for s in resp.data or []:
        bad = [
            h for h in (s.get("holdings") or [])
            if h.get("entry_date") and str(h["entry_date"])[:10] > TODAY
        ]
        if bad:
            flagged.append({
                "snapshot_id": s["snapshot_id"],
                "strategy_id": s.get("scheduled_strategy_id"),
                "kind": s.get("kind"),
                "as_of_date": s.get("as_of_date"),
                "bad_holdings": [
                    (h.get("ticker"), h.get("entry_date"), h.get("entry_price_local"),
                     h.get("forward_return_pct"))
                    for h in bad
                ],
            })
    return flagged


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="delete future-dated price rows")
    args = ap.parse_args()

    print(f"today = {TODAY}  (target SUPABASE_URL shown in the [deps] line above)\n")

    md = _scan_metric_data()
    bp = _scan_benchmark_price()
    snaps = _scan_snapshots_with_future_entry()

    print(f"=== metric_data future rows: {len(md)} ===")
    for r in md[:50]:
        print(f"  id={r['id']} cid={r['company_id']} {r['metric_code']} "
              f"{r['target_date']} = {r['numeric_value']}")

    print(f"\n=== benchmark_price future rows: {len(bp)} ===")
    for r in bp[:50]:
        print(f"  bid={r['benchmark_id']} {r['target_date']} = {r['price']}")

    print(f"\n=== snapshots with a future entry_date: {len(snaps)} ===")
    for s in snaps:
        print(f"  snap={s['snapshot_id']} strat={s['strategy_id']} kind={s['kind']} "
              f"as_of={s['as_of_date']}")
        for tk, ed, ep, fr in s["bad_holdings"]:
            print(f"      {tk}: entry_date={ed} entry_local={ep} fwd_return={fr}%")

    if not args.apply:
        print("\n(dry-run) re-run with --apply to DELETE the future-dated price rows.")
        if snaps:
            print("After --apply, re-rebalance the flagged strategies so their ETF "
                  "overlay re-anchors entry (job_name=rebalance / /schedule Run now).")
        return

    # ── Apply: delete future-dated price rows (snapshots are left for re-rebalance) ──
    del_md = 0
    for r in md:
        supabase.table("metric_data").delete().eq("id", r["id"]).execute()
        del_md += 1
    del_bp = 0
    for r in bp:
        (supabase.table("benchmark_price").delete()
         .eq("benchmark_id", r["benchmark_id"]).eq("target_date", r["target_date"]).execute())
        del_bp += 1
    print(f"\ndeleted {del_md} metric_data rows + {del_bp} benchmark_price rows.")
    if snaps:
        print(f"NEXT: re-rebalance {len(snaps)} flagged snapshot(s)' strategies to "
              "re-anchor the ETF overlay entry (the bad entry is frozen in the "
              "snapshot until a fresh rebalance).")


if __name__ == "__main__":
    main()
