"""The true free-cash-flow growth of an index, on a fixed basket, weighted by each year's own euros.

⚠⚠ IT SUMS EUROS. It does NOT average per-member growth rates, which is what the app's level line
does and which is wrong twice over: it weights a growth RATE by MARKET CAP (measured on ACWI
revenue, NVIDIA holds 4.77% of cap and supplies 0.02% of revenue — a ~240x overweight on the
quantity being measured), and averaging rates that are floored at -100% and unbounded above is
upward-biased in proportion to their dispersion.

⚠ AND SUMMING IS STILL THE CAP-WEIGHTED ANSWER, which is the part that looks wrong. A cap-weighted
index holds the SAME FRACTION of every company: buying `w_i = cap_i/Σcap` at price `p_i` leaves
`n_i = shares_i / Σcap` — the price cancels. So the claim on a fundamental is `(1/Σcap)·ΣF_i`,
exactly proportional to the sum. Cap weighting enters through the SHARE COUNT, never as a weight on
a rate. "Weighted per year" is therefore automatic: a company's euros ARE its weight that year.

Four things this corrects, each measured on ACWI and each invisible on the chart:

  1. FX. `_rate` returns UNITS PER EUR (IDR 19,640.83) and GuruFocus financials are in MILLIONS, so
     the conversion is `native / rate * 1e6`. Multiplying instead inflates a rupiah filing 19,641x.
  2. DUAL-CLASS. A dual-class constituent is two `company` rows EACH carrying the whole company —
     42 duplicated names, 5.83% of the index fictional (Alphabet read 7.60% of cap where it is
     3.80%). Averaging double-voted it; summing adds its whole income statement twice.
  3. FINANCIALS. A bank's operating cash flow moves with DEPOSIT AND LOAN FLOWS, so its "free cash
     flow" swings by trillions with no economic content (PT Bank Mandiri: -1,278bn, -1,482bn,
     +3,909bn EUR in consecutive years). Excluded — ⚠ BOTH SPELLINGS, since Yahoo says "Financials"
     (225 members) AND "Financial Services" (78) and listing one leaves 78 banks in.
  4. A FIXED BASKET. A sum changes when its members change, so a year with fewer filers is a smaller
     index for that reason alone. Only members reporting in EVERY year are counted, and the basket
     size is printed — a CAGR over a moving basket is not a CAGR.

⚠ THE 1000x SHARE-COUNT DEFECT NEEDS NO FIX HERE, and that is a property of the construction rather
than luck. Three Japanese filers carry an FY2025 share count 1,000x too small (Japan Post Bank
3,618.10 -> 3.62, Denso, Mitsubishi Heavy), which makes GuruFocus's per-share figure 1,000x too
LARGE. `per_share x shares` multiplies the two back together and the errors cancel exactly:
1,256,901.521 x 3.62 = 1,256.9 x 3,620. The per-share LINE inherits the error; the TOTAL never sees
it.

    cd backend && uv run python scripts/acwi_fcf_growth.py
    cd backend && uv run python scripts/acwi_fcf_growth.py --metric revenue --universe SP500
    cd backend && uv run python scripts/acwi_fcf_growth.py --keep-financials
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # noqa: E402,F401  (loads .env / .env.local first)
from deps import supabase  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--universe", default="ACWI")
    ap.add_argument("--metric", default="fcf_ps")
    ap.add_argument("--from-year", type=int, default=2015)
    ap.add_argument("--to-year", type=int, default=2025)
    ap.add_argument("--keep-financials", action="store_true",
                    help="do not exclude banks — for comparison only; see the module note")
    args = ap.parse_args()

    from routers.earnings import fundamental_totals

    uni = (supabase.table("universe").select("universe_id")
           .eq("label", args.universe).limit(1).execute().data or [])
    if not uni:
        print(f"  !! no universe labelled {args.universe!r}")
        return 2
    ids = sorted({r["company_id"] for r in
                  (supabase.table("universe_membership").select("company_id")
                   .eq("universe_id", uni[0]["universe_id"]).execute().data or [])
                  if r.get("company_id")})
    print(f"[1/3] {args.universe}: {len(ids)} membership rows")

    # ⚠ ONE CALL. `fundamental_totals` already dedupes dual-class rows, excludes financials for the
    # metrics where a sum is meaningless, and converts each filing at its OWN period-end rate. Doing
    # any of it again here would be a second definition of the same rule.
    if args.keep_financials:
        import routers.earnings as _e
        _e._NO_AGGREGATE_FOR_FINANCIALS = frozenset()   # noqa: SLF001
    totals = fundamental_totals(ids, [args.metric])
    if not totals:
        print(f"  !! no totals for {args.metric!r} — is it in `_AGGREGATABLE_*`?")
        return 2
    per_cid = next(iter(totals.values()))
    print(f"[2/3] euro totals for {len(per_cid)} members after dedupe/exclusions")

    years = [str(y) for y in range(args.from_year, args.to_year + 1)]

    def value_in(cid: int, year: str) -> float | None:
        # ⚠ ONE FIGURE PER YEAR, THE LATEST FILED. A company changing its year-end can file twice
        # against one year, and counting both would double it inside the sum.
        per = per_cid.get(cid) or {}
        dated = sorted((d for d in per if d[:4] == year), reverse=True)
        return per[dated[0]] if dated else None

    basket = [c for c in per_cid if all(value_in(c, y) is not None for y in years)]
    if not basket:
        print("  !! no member reports in every year — widen the window")
        return 2
    dropped = len(per_cid) - len(basket)
    print(f"[3/3] fixed basket: {len(basket)} members report every year "
          f"({dropped} dropped for a gap)\n")

    series = {y: sum(value_in(c, y) or 0.0 for c in basket) for y in years}
    prev = None
    for y in years:
        v = series[y]
        yoy = "" if prev in (None, 0) else f"   {(v / prev - 1) * 100:+7.1f}%"
        print(f"    {y}   EUR {v / 1e9:>10,.1f}bn{yoy}")
        prev = v

    a, b = series[years[0]], series[years[-1]]
    n = len(years) - 1
    if a <= 0:
        print(f"\n  {years[0]} aggregate is {a:,.0f} — no CAGR to take")
        return 1
    print(f"\n  {args.metric} CAGR {years[0]}-{years[-1]}:  "
          f"{((b / a) ** (1 / n) - 1) * 100:+.2f}%/yr   (total {(b / a - 1) * 100:+.1f}%)")
    print("\nDone. Nothing was written.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
