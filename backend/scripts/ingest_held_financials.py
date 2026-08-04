"""Fetch GuruFocus financials, analyst estimates and indicators for a set of companies —
the companies an AIRS book HOLDS, or the members of a UNIVERSE (`--universe SP500`).

WHY THIS EXISTS
    The /portfolios Fundamental view can only chart a company whose financials are ingested, and
    measured 2026-07-23 that was 31 of 2,776 company rows — so a portfolio's coverage read 12%
    (and one read 0%) purely because nobody had fetched them. This closes that gap for the
    companies that matter: the ones held by a live book.

⚠ THREE INGESTS, AND RUNNING ONLY SOME IS WHAT MAKES A CHART LOOK BROKEN. Financials (the
    statements), analyst estimates (forward EPS, the owner-earnings estimate) and indicators
    (per-key series such as forward P/E) are THREE separate GuruFocus calls, and a company can
    have any one without the others. Measured 2026-07-23: fetching financials alone for 156
    companies left "Share price vs Owner Earnings" empty on every one of them, and adding the
    estimates still left Forward P/E empty — because Forward P/E is not an estimate at all, it is
    `indicator_q_forward_pe_ratio`. The suite fills in AROUND the panels that cannot, which reads
    as a bug in the charts rather than as data nobody fetched.

⚠ A NAMED SET, NEVER THE WHOLE TABLE. 2,776 companies at ~3 calls each would spend a large slice
    of the monthly quota on names nothing looks at. Two sets are worth it, and both are opt-in:
      * the HELD set (default) — ~177 companies, every one of them on a screen;
      * `--universe SP500` — the index's members, so the Long Equity BENCHMARK line describes the
        index rather than a fifth of it. Measured 2026-08-04: 92 of 503 SP500 members had any
        fundamentals, so the benchmark's cap-weighted margin was an average over 18% of the index,
        drawn in the same ink as the portfolio's own line beside it.

⚠ IT IS THE QUOTA THAT DECIDES WHETHER A UNIVERSE IS AFFORDABLE, SO CHECK THE DRY RUN FIRST. An
    index backfill is ~3 calls x the missing members (SP500: ~411 x 3 ≈ 1,233), which is a real
    fraction of a month. `--limit` exists to spend it in tranches across days rather than
    discovering the ceiling halfway through.

⚠ IT SKIPS EXCHANGES OUTSIDE THE SUBSCRIPTION RATHER THAN 403-ing THROUGH THEM. LSE, ASX and the
    rest return "unsubscribed" — the call is spent and nothing comes back. `is_gf_subscribed_exchange`
    is the same gate the rest of the pipeline uses.

⚠ AND IT CHECKS THE BUDGET BEFORE IT STARTS. `remaining_budget` is per region; a region at zero
    means the month's quota is gone and every further call is wasted.

    cd backend && uv run python scripts/ingest_held_financials.py            # dry run, held set
    cd backend && uv run python scripts/ingest_held_financials.py --apply
    cd backend && uv run python scripts/ingest_held_financials.py --universe SP500
    cd backend && uv run python scripts/ingest_held_financials.py --universe SP500 --apply --limit 100
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deps  # noqa: E402, F401
from deps import supabase  # noqa: E402
from ingest.api_usage import remaining_budget  # noqa: E402

# ⚠ THE SENTINELS, THE SUBSCRIPTION GATE AND THE THREE-CALL SEQUENCE LIVE IN ONE PLACE, shared with
# the /benchmarks table's per-row and fill-all buttons. They were inline here first; a second copy
# is how one caller quietly goes back to fetching two feeds.
from routers._fundamental_backfill import (  # noqa: E402
    company_rows as _company_rows,
    eligible,
    ingest_company,
    needs as _missing,
)

def _universe_company_ids(label: str) -> list[int]:
    """Every company in a universe, by label ("SP500", "ACWI", …).

    ⚠ A SECOND SOURCE FOR THE SAME WORK, NOT A SECOND SCRIPT. The held set and an index are two
    answers to "which companies matter"; everything after this point — the three sentinels, the
    subscription gate, the budget check, the per-row log — is identical, and a forked copy would
    be one more place for the three-ingests trap to be got wrong.
    """
    uni = (supabase.table("universe").select("universe_id")
           .eq("label", label).limit(1).execute().data or [])
    if not uni:
        raise SystemExit(f"  no universe labelled {label!r}")
    uid = uni[0]["universe_id"]
    rows, off = [], 0
    while True:                       # ⚠ paged: 500+ members is past PostgREST's silent 1,000 cap
        page = (supabase.table("universe_membership").select("company_id")
                .eq("universe_id", uid).order("company_id")
                .range(off, off + 999).execute().data or [])
        if not page:
            break
        rows += page
        off += len(page)
    return sorted({r["company_id"] for r in rows if r.get("company_id")})



def _held_company_ids() -> list[int]:
    isins = sorted({r["isin"] for r in (supabase.table("airs_holding").select("isin")
                    .not_.is_("isin", "null").limit(5000).execute().data or []) if r.get("isin")})
    out: set[int] = set()
    for i in range(0, len(isins), 100):
        for c in (supabase.table("company").select("company_id")
                  .in_("isin", isins[i:i + 100]).execute().data or []):
            out.add(c["company_id"])
    return sorted(out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="spend the calls (default: dry run)")
    ap.add_argument("--limit", type=int, default=0, help="stop after N companies (0 = all)")
    ap.add_argument("--universe", default=None, metavar="LABEL",
                    help="ingest a universe's members instead of the held set (e.g. SP500)")
    a = ap.parse_args()

    cids = _universe_company_ids(a.universe) if a.universe else _held_company_ids()
    todo = _missing(_company_rows(cids))
    print(f"\n  source: {a.universe or 'held books'} — {len(cids)} companies")
    skipped = [c for c in todo if eligible(c)]
    work = [c for c in todo if not eligible(c)]
    if a.limit:
        work = work[:a.limit]

    budget = remaining_budget(supabase)
    print(f"  missing financials / estimates / indicators: {len(todo)}")
    print(f"  skipped (no ticker / unsubscribed exchange): {len(skipped)}")
    print(f"  to fetch: {len(work)}     quota remaining: {budget}\n", flush=True)
    if not a.apply:
        for c in work[:10]:
            ex = ((c.get("gurufocus_exchange") or {}) or {}).get("exchange_code")
            print(f"    {ex}:{c['gurufocus_ticker']:<10} {(c.get('company_name') or '')[:44]}")
        print(f"    … and {max(0, len(work) - 10)} more\n  dry run — re-run with --apply\n")
        return 0

    ok = failed = 0
    for n, c in enumerate(work, 1):
        ex = ((c.get("gurufocus_exchange") or {}) or {}).get("exchange_code")
        label = f"{ex}:{c['gurufocus_ticker']}"
        # ⚠ THE SAME FUNCTION THE /benchmarks BUTTON CALLS. It never raises — a failure comes back
        # on the row — so this loop keeps the per-company reporting it always had while the
        # three-call sequence lives in one place.
        r = ingest_company(c)
        if r["error"]:
            failed += 1
            print(f"  [{n:>3}/{len(work)}] {label:<16} FAIL {r['error']}", flush=True)
        else:
            ok += 1
            print(f"  [{n:>3}/{len(work)}] {label:<16} "
                  f"{', '.join(r['done']) or 'nothing to do'}", flush=True)
        time.sleep(0.2)     # a courtesy pause; the API is not rate-limited at this volume
    print(f"\n  ok={ok}  failed={failed}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
