"""The index's fundamentals summed in EUR, against the growth-averaged line the app draws.

⚠⚠ THE APP'S LEVEL LINE AVERAGES PER-MEMBER GROWTH RATES AND THEN CHAINS THEM. A growth rate is
bounded below at −100% and unbounded above, so averaging an asymmetric distribution is upward-biased
and the bias scales with DISPERSION. Measured on ACWI (`scripts/diagnose_blend_steps.py`), lowering
the accepted-growth cap from +10,000% to +1,000% costs `revenue` 0.03pp a year and `fcf_ps` 4.06pp:
revenue's growth rates are tightly clustered and FCF/share's are not (p99 +706%, p99.9 +2,183%).

⚠⚠ SUMMING EUROS HAS NO SUCH BIAS, AND NO GUARDS EITHER. `Σ FCF` is path-independent by
construction — a member at −200 subtracts 200 and a later +200 adds it back, so a round trip through
zero nets out instead of being floored at −100% one year and refused the next. It needs no growth
cap, no minimum base and no floor, because it never takes a ratio of one member to itself.

    total_i(t) = per_share_i(t) x shares_i(t) x fx(period end)
    index(t)   = Σ_i total_i(t)

⚠ THE FX IS THE PERIOD'S OWN END, NOT TODAY'S — the same rule and the same helpers `period_caps_eur`
uses. GuruFocus reports in the listing's trading currency per fiscal period; an ACWI cross-section is
19 currencies, and converting Apple's September year-end at 31 December's rate applies a rate struck
three months after the figure.

⚠⚠ AND THE COMPOSITION MUST BE HELD FIXED, WHICH IS THE WHOLE DIFFICULTY. A sum changes when its
members change, so a year where fewer constituents reported is a smaller index for that reason
alone — the sawtooth `carry_forward` and the coverage floors exist to prevent on the averaged path.
Here it is handled by intersecting: only members with BOTH inputs in BOTH endpoint years are summed,
and the count that survived is printed beside the answer. A CAGR over a moving basket is not a CAGR.

    cd backend && uv run python scripts/measure_aggregate_fundamental.py
    cd backend && uv run python scripts/measure_aggregate_fundamental.py --universe SP500
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # noqa: E402,F401  (loads .env / .env.local first)
from deps import IN_CHUNK_SIZE, supabase  # noqa: E402

#: The per-share lines worth aggregating, and the total each one becomes.
_PER_SHARE = {"fcf_ps": "free cash flow", "eps_nri": "net income (excl. NRI)"}
#: A level that is already a total — no share count needed, and the control for the method.
_TOTALS = {"revenue": "revenue"}


def _universe_ids(label: str) -> list[int]:
    uni = (supabase.table("universe").select("universe_id")
           .eq("label", label).limit(1).execute().data or [])
    if not uni:
        return []
    return sorted({r["company_id"] for r in
                   (supabase.table("universe_membership").select("company_id")
                    .eq("universe_id", uni[0]["universe_id"]).execute().data or [])
                   if r.get("company_id")})


def _by_year(ids: list[int], metric: str) -> dict[int, dict[str, float]]:
    """`{company_id: {YYYY: value}}` for one metric, paged."""
    from routers.earnings import _BLEND_START, _metric_codes

    codes = list(_metric_codes(metric))
    out: dict[int, dict[str, float]] = defaultdict(dict)
    for i in range(0, len(ids), IN_CHUNK_SIZE):
        chunk = ids[i:i + IN_CHUNK_SIZE]
        off = 0
        while True:
            rows = (supabase.table("metric_data")
                    .select("company_id,target_date,numeric_value")
                    .in_("company_id", chunk).in_("metric_code", codes)
                    .gte("target_date", _BLEND_START)
                    .order("company_id").order("target_date")
                    .range(off, off + 999).execute().data or [])
            if not rows:
                break
            for r in rows:
                if r.get("numeric_value") is not None:
                    # ⚠ LAST WINS WITHIN A YEAR. A company filing twice for one fiscal year (a
                    # restatement) must contribute once, and the newer figure is the one the app
                    # would show.
                    out[r["company_id"]][str(r["target_date"])[:4]] = float(r["numeric_value"])
            off += len(rows)
    return out


def _fx_by_company(ids: list[int]) -> tuple[dict[int, str], dict]:
    """Each company's reporting currency, and a rate table to convert it — as `period_caps_eur`."""
    from routers._benchmark_index import _fx_to_eur

    # ⚠ THE CURRENCY IS THE EXCHANGE'S, VIA THE JOIN — `company` has no currency column of its
    # own. Copied from `period_caps_eur`, which is the only other place that converts a
    # GuruFocus financial to EUR and therefore the only definition of "which currency is this
    # filed in" that can be trusted to match.
    ccy: dict[int, str] = {}
    for i in range(0, len(ids), IN_CHUNK_SIZE):
        for c in (supabase.table("company")
                  .select("company_id,gurufocus_exchange:gurufocus_exchange(currency_code)")
                  .in_("company_id", ids[i:i + IN_CHUNK_SIZE]).execute().data or []):
            code = ((c.get("gurufocus_exchange") or {}) or {}).get("currency_code")
            if code:
                ccy[c["company_id"]] = code
    fx = _fx_to_eur({c for c in ccy.values() if c}, "2014-01-01", "2026-12-31")
    return ccy, fx


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--universe", default="ACWI")
    ap.add_argument("--from-year", default="2015")
    ap.add_argument("--to-year", default="2025")
    args = ap.parse_args()

    from routers._benchmark_index import _rate

    ids = _universe_ids(args.universe)
    if not ids:
        print(f"  !! no universe labelled {args.universe!r}")
        return 2
    print(f"[1/4] {args.universe}: {len(ids)} members")

    ccy, fx = _fx_by_company(ids)
    print(f"[2/4] reporting currency known for {len(ccy)} · {len(set(ccy.values()))} currencies")

    shares = _by_year(ids, "shares")
    print(f"[3/4] share counts for {len(shares)} members")

    y0, y1 = args.from_year, args.to_year
    years = int(y1) - int(y0)
    print(f"[4/4] aggregate EUR totals, {y0} -> {y1} ({years}y)\n")

    for metric, what in {**_PER_SHARE, **_TOTALS}.items():
        vals = _by_year(ids, metric)
        per_share = metric in _PER_SHARE

        # ⚠ THE INTERSECTION IS TAKEN FIRST AND THE COUNT PRINTED. Summing whoever happens to have
        # each year compares two different baskets and calls the difference growth.
        usable = [c for c in ids
                  if y0 in vals.get(c, {}) and y1 in vals.get(c, {})
                  and ccy.get(c)
                  and (not per_share or (y0 in shares.get(c, {}) and y1 in shares.get(c, {})))]

        tot = {y0: 0.0, y1: 0.0}
        skipped_fx = 0
        for c in usable:
            for y in (y0, y1):
                # The period end GuruFocus reported against; the year end is the honest stand-in
                # here, and any error it carries applies to both endpoints alike.
                rate = _rate(fx, ccy[c], f"{y}-12-31")
                if rate is None:
                    skipped_fx += 1
                    tot[y0] = tot[y0]  # no-op; the member is simply not added
                    break
                # ⚠⚠ DIVIDE BY THE RATE AND SCALE BY 1e6 — `_rate` returns UNITS PER EUR (IDR
                # 19,640.83), and GuruFocus financials are in millions. This script shipped with
                # both wrong and still printed plausible CAGRs, because revenue and EPS are
                # near-always positive so the error only inflated a positive sum. Its first
                # numbers (fcf 15.41%, eps 10.06%, revenue 11.55%) were all wrong.
                v = vals[c][y] * (shares[c][y] if per_share else 1.0) / rate * 1e6
                tot[y] += v

        if tot[y0] <= 0:
            print(f"  {metric:<9} — aggregate {y0} is {tot[y0]:,.0f}, no CAGR to take")
            continue
        cagr = (tot[y1] / tot[y0]) ** (1 / years) - 1
        unit = "" if per_share else " (already a total)"
        print(f"  {metric:<9} {what:<24} {len(usable):>5} members{unit}")
        print(f"            {y0}: EUR {tot[y0] / 1e9:>12,.1f}bn   "
              f"{y1}: EUR {tot[y1] / 1e9:>12,.1f}bn   ->  {cagr * 100:+6.2f}%/yr")
        if skipped_fx:
            print(f"            ⚠ {skipped_fx} member-years had no FX rate and were left out")

    print("\nDone. Nothing was written.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
