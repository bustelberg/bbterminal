"""Fetch GuruFocus financials AND analyst estimates for every company an AIRS book HOLDS.

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

⚠ HELD COMPANIES ONLY, NOT THE WHOLE TABLE. 2,776 companies at ~1 call each would spend an eighth
    of the monthly quota on names nothing holds. The held set is ~177, of which the missing ones
    are ~164 — a rounding error against the budget, and every one of them is on a screen.

⚠ IT SKIPS EXCHANGES OUTSIDE THE SUBSCRIPTION RATHER THAN 403-ing THROUGH THEM. LSE, ASX and the
    rest return "unsubscribed" — the call is spent and nothing comes back. `is_gf_subscribed_exchange`
    is the same gate the rest of the pipeline uses.

⚠ AND IT CHECKS THE BUDGET BEFORE IT STARTS. `remaining_budget` is per region; a region at zero
    means the month's quota is gone and every further call is wasted.

    cd backend && uv run python scripts/ingest_held_financials.py            # dry run
    cd backend && uv run python scripts/ingest_held_financials.py --apply
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deps  # noqa: E402, F401
from deps import supabase  # noqa: E402
from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: E402
from ingest.api_usage import remaining_budget  # noqa: E402
from ingest.earnings import (  # noqa: E402
    fetch_analyst_estimates,
    fetch_financials,
    fetch_indicators,
)

# The line the /portfolios blend charts. A company that has it can be charted; one that does not
# is what this script exists to fix. Probed with ONE code, never `LIKE 'annuals__%'` — a wildcard
# over 20 companies is ~40k rows against PostgREST's silent 1,000-row cap.
SENTINEL = "annuals__Cashflow Statement__Free Cash Flow"

# ⚠ TWO INGESTS, TWO SENTINELS. Financials and analyst estimates are SEPARATE GuruFocus calls and
# a company can have one without the other — running only the first leaves Forward P/E (and every
# other estimate-driven panel) empty while the rest of the suite fills in, which reads as a bug in
# the chart rather than as data nobody fetched. Measured 2026-07-23: 168 companies with financials,
# 31 with estimates.
SENTINEL_EST = "annual_pettm_estimate"

# ⚠ AND A THIRD, BECAUSE FORWARD P/E IS NOT AN ESTIMATE. The chart reads
# `indicator_q_forward_pe_ratio` — the INDICATORS feed, a third call again. Naming makes this easy
# to get wrong: `annual_pettm_estimate` is also a forward P/E and is also present, but no chart
# reads it. Measured 2026-07-23: 12,536 indicator rows fleet-wide and ZERO on any held company.
SENTINEL_IND = "indicator_q_forward_pe_ratio"


def _has(cids: list[int], metric_code: str) -> set[int]:
    """Which of these companies carry `metric_code`. ONE code, never `LIKE 'annuals__%'` — a
    wildcard over 20 companies is ~40k rows against PostgREST's silent 1,000-row cap, and every
    company past the cut-off would look like it had nothing."""
    out: set[int] = set()
    for i in range(0, len(cids), 20):
        for m in (supabase.table("metric_data").select("company_id")
                  .in_("company_id", cids[i:i + 20]).eq("metric_code", metric_code)
                  .limit(1000).execute().data or []):
            out.add(m["company_id"])
    return out


def _held_companies() -> list[dict]:
    isins = sorted({r["isin"] for r in (supabase.table("airs_holding").select("isin")
                    .not_.is_("isin", "null").limit(5000).execute().data or []) if r.get("isin")})
    comps: dict[int, dict] = {}
    for i in range(0, len(isins), 100):
        for c in (supabase.table("company")
                  .select("company_id,company_name,gurufocus_ticker,"
                          "gurufocus_exchange:gurufocus_exchange(exchange_code)")
                  .in_("isin", isins[i:i + 100]).execute().data or []):
            comps[c["company_id"]] = c
    cids = sorted(comps)
    fin, est = _has(cids, SENTINEL), _has(cids, SENTINEL_EST)
    ind = _has(cids, SENTINEL_IND)
    out = []
    for cid, c in comps.items():
        c = {**c, "need_fin": cid not in fin, "need_est": cid not in est,
             "need_ind": cid not in ind}
        if c["need_fin"] or c["need_est"] or c["need_ind"]:
            out.append(c)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="spend the calls (default: dry run)")
    ap.add_argument("--limit", type=int, default=0, help="stop after N companies (0 = all)")
    a = ap.parse_args()

    todo = _held_companies()
    skipped = [c for c in todo
               if not c.get("gurufocus_ticker")
               or not is_gf_subscribed_exchange(
                   ((c.get("gurufocus_exchange") or {}) or {}).get("exchange_code"))]
    work = [c for c in todo if c not in skipped]
    if a.limit:
        work = work[:a.limit]

    budget = remaining_budget(supabase)
    print(f"\n  held companies missing financials / estimates / indicators: {len(todo)}")
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
        try:
            done = []
            if c.get("need_fin"):
                r1 = fetch_financials(supabase, c["company_id"], c["gurufocus_ticker"], ex)
                done.append(f"fin {r1.rows_loaded} rows")
            if c.get("need_est"):
                r2 = fetch_analyst_estimates(supabase, c["company_id"], c["gurufocus_ticker"], ex)
                done.append(f"est {r2.rows_loaded} rows")
            if c.get("need_ind"):
                r3 = fetch_indicators(supabase, c["company_id"], c["gurufocus_ticker"], ex)
                done.append(f"ind {r3.rows_loaded} rows")
            ok += 1
            print(f"  [{n:>3}/{len(work)}] {label:<16} {', '.join(done) or 'nothing to do'}",
                  flush=True)
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"  [{n:>3}/{len(work)}] {label:<16} FAIL {type(e).__name__}: {str(e)[:70]}",
                  flush=True)
        time.sleep(0.2)     # a courtesy pause; the API is not rate-limited at this volume
    print(f"\n  ok={ok}  failed={failed}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
