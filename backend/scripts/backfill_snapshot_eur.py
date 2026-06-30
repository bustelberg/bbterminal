"""One-shot backfill: re-derive every `current_picks_snapshot`'s EUR marks +
`forward_return_pct` + `period_return_pct` from its stored LOCAL prices, so all
snapshots use the single EUR source of truth (`momentum.portfolio_math`).

Why: pre-fix snapshots computed `forward_return_pct`/`period_return_pct` in
LOCAL currency (and ETF overlay holdings had no EUR marks at all, and stock
`exit_price_eur` was left at the rebalance value while the local price advanced).
This converts each holding's stored local entry/exit price to EUR at the
entry-date / exit-date FX (the same fx_rate table the engine uses), recomputes
`forward_return_pct = exit_eur/entry_eur − 1`, then sets
`period_return_pct = Σ weight·forward` — and writes the row back. After this the
card Total, the /schedule header MTD, and the run-history rows all read the
same consistent EUR figures.

It does NOT re-fetch prices (freshness is the pipeline's job) — it only fixes
the currency basis of what's already stored. Idempotent: a row already on the
EUR basis is left unchanged.

Run from the backend dir:

    uv run python scripts/backfill_snapshot_eur.py --dry-run     # preview
    uv run python scripts/backfill_snapshot_eur.py               # apply
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # noqa: E402,F401 — loads .env + constructs the Supabase client
from deps import paginate, supabase  # noqa: E402
from momentum.data import load_company_currency, load_fx_rates  # noqa: E402
from momentum.portfolio_math import (  # noqa: E402
    holding_eur_return_pct,
    portfolio_eur_return_pct,
)
from routers._schedule_snapshots import _to_eur  # noqa: E402


def _load_all_snapshots() -> list[dict]:
    return list(paginate(
        lambda lo, hi: supabase.table("current_picks_snapshot")
        .select("snapshot_id, holdings, period_return_pct, as_of_date")
        .order("snapshot_id")
        .range(lo, hi)
        .execute()
    ))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="print changes, write nothing")
    parser.add_argument("--limit", type=int, default=0, help="only the first N snapshots (debug)")
    args = parser.parse_args()

    snaps = _load_all_snapshots()
    if args.limit:
        snaps = snaps[: args.limit]
    print(f"examining {len(snaps)} snapshots")

    # Currency per stock (from the listing exchange) + every ETF currency + the
    # earliest entry date, so we can bulk-load the FX series once.
    company_cids: set[int] = set()
    currencies: set[str] = set()
    min_date: str | None = None
    for s in snaps:
        for h in (s.get("holdings") or []):
            cid = h.get("company_id")
            if cid is not None and int(cid) >= 0:
                company_cids.add(int(cid))
            cur = (h.get("currency") or "").upper()
            if cur:
                currencies.add(cur)
            ed = str(h.get("entry_date") or "")[:10]
            if ed and (min_date is None or ed < min_date):
                min_date = ed

    ccy_by_cid = load_company_currency(supabase, list(company_cids)) if company_cids else {}
    currencies |= {(c or "").upper() for c in ccy_by_cid.values() if c}
    currencies.discard("")
    fx_start = date.fromisoformat(min_date) if min_date else date.today()
    fx_rates = load_fx_rates(supabase, sorted(currencies | {"EUR"}), fx_start, date.today())

    def _ccy(h: dict) -> str | None:
        cid = h.get("company_id")
        if cid is not None and int(cid) >= 0:
            return ccy_by_cid.get(int(cid)) or (h.get("currency") or None)
        return h.get("currency") or None  # ETF: the stored benchmark currency

    updated = unchanged = empty = 0
    for s in snaps:
        holds = s.get("holdings") or []
        if not holds:
            empty += 1
            continue
        new_holds: list[dict] = []
        changed = False
        for h in holds:
            nh = dict(h)
            ccy = _ccy(h)
            el = h.get("entry_price_local")
            xl = h.get("exit_price_local")
            ed = str(h.get("entry_date") or s.get("as_of_date") or "")[:10]
            xd = str(h.get("exit_date") or "")[:10]
            ee = _to_eur(el, ccy, ed, fx_rates) if el is not None else None
            xe = _to_eur(xl, ccy, xd, fx_rates) if xl is not None else None
            if ee is not None:
                nh["entry_price_eur"] = round(ee, 4)
            if xe is not None:
                nh["exit_price_eur"] = round(xe, 4)
            fr = holding_eur_return_pct(nh)
            if fr is not None:
                nh["forward_return_pct"] = round(fr, 2)
            if nh != h:
                changed = True
            new_holds.append(nh)

        new_period = portfolio_eur_return_pct(new_holds)
        old_period = s.get("period_return_pct")
        period_changed = new_period is not None and (
            old_period is None or abs(new_period - float(old_period)) > 1e-6
        )
        if not changed and not period_changed:
            unchanged += 1
            continue

        updated += 1
        if args.dry_run:
            op = round(float(old_period), 4) if old_period is not None else None
            np_ = round(new_period, 4) if new_period is not None else None
            print(f"  snap {s['snapshot_id']}: period {op} -> {np_}")
            continue
        supabase.table("current_picks_snapshot").update(
            {"holdings": new_holds, "period_return_pct": new_period}
        ).eq("snapshot_id", s["snapshot_id"]).execute()

    suffix = " (DRY RUN — nothing written)" if args.dry_run else ""
    print(f"done: {updated} updated, {unchanged} already consistent, {empty} empty{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
