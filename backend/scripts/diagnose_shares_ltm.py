"""Why an LTM point disagrees with the annual line it extends — for one company, one metric.

⚠⚠ THE LTM POINT IS DRAWN ON THE SAME LINE AS THE FISCAL YEARS, AND IT COMES FROM A DIFFERENT
FEED. Annual points are `annuals__…` rows; the LTM point is assembled here from `quarterly__…`
rows (`_ttm_by_period`, per-metric roll-up in `_TTM_RULE`). So a vendor unit change, restatement or
redenomination that lands in ONE of those two feeds is invisible inside either — each is internally
consistent — and shows up only as a step at the seam between them.

⚠ `_drop_quarter_outliers` cannot see it: it judges the quarterly series against its OWN median, and
a run of four consecutive quarters at a new level is exactly the shape it is written to KEEP
(`_level_shift` — a restatement arrives and stays). The annual series is the second opinion it
never gets.

    cd backend && uv run python scripts/diagnose_shares_ltm.py 3786
    cd backend && uv run python scripts/diagnose_shares_ltm.py 3786 --metric shares
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # noqa: E402,F401  (loads .env / .env.local first)
from deps import supabase  # noqa: E402
from routers import earnings as ea  # noqa: E402


def _rows(company_id: int, codes: tuple[str, ...]) -> list[dict]:
    """Every stored point for these metric codes, oldest first.

    ⚠ PAGED. A company files ~110 codes a date and this reads a decade of quarters; an unpaged
    read is the 1,000-row cloud cap, and the rows it would drop are the NEWEST ones — which are
    the entire subject here.
    """
    out: list[dict] = []
    off, page = 0, 1000
    while True:
        rows = (supabase.table("metric_data")
                .select("target_date,numeric_value,metric_code")
                .eq("company_id", company_id).in_("metric_code", list(codes))
                .order("target_date").order("metric_code")
                .range(off, off + page - 1).execute().data or [])
        if not rows:
            break
        out += rows
        off += len(rows)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("company_id", type=int)
    ap.add_argument("--metric", default="shares")
    a = ap.parse_args()

    comp = (supabase.table("company").select("company_id,company_name,isin,gurufocus_ticker")
            .eq("company_id", a.company_id).limit(1).execute().data or [])
    print(f"company {a.company_id}: {comp[0] if comp else '(no company row)'}\n")

    annual_codes, _ = ea._codes_and_rule(a.metric, "annual")
    quarter_codes, rule = ea._codes_and_rule(a.metric, "quarterly")
    print(f"metric {a.metric!r}  roll-up={rule!r}")
    print(f"  annual codes    {annual_codes}")
    print(f"  quarterly codes {quarter_codes}\n")

    ann = _rows(a.company_id, annual_codes)
    qtr = _rows(a.company_id, quarter_codes)

    print(f"ANNUAL ({len(ann)} rows) — the line the LTM point extends")
    for r in ann[-8:]:
        print(f"  {r['target_date']}  {r['numeric_value']:>18,.4f}")

    print(f"\nQUARTERLY ({len(qtr)} rows) — what the LTM point is built from")
    for r in qtr[-10:]:
        print(f"  {r['target_date']}  {r['numeric_value']:>18,.4f}")

    # What the guard actually did to those quarters.
    by_date = {r["target_date"]: float(r["numeric_value"]) for r in qtr
               if r.get("numeric_value") is not None}
    kept = ea._drop_quarter_outliers(by_date, f"{a.company_id}/{quarter_codes[0]}")
    dropped = sorted(set(by_date) - set(kept))
    print(f"\n_drop_quarter_outliers: kept {len(kept)} of {len(by_date)}"
          f"{'' if not dropped else f', dropped {dropped}'}")

    ltm = ea._ltm_by_company([a.company_id], a.metric, "annual").get(a.company_id)
    newest_annual = ann[-1]["numeric_value"] if ann else None
    print(f"\nLTM       {ltm}")
    print(f"newest annual {newest_annual}")
    if ltm and newest_annual:
        val = ltm[1] if isinstance(ltm, tuple) else ltm
        print(f"\n>>> LTM / newest annual = {float(val) / float(newest_annual):,.2f}x"
              "   <- the step the blended index chains through, weighted by this company's cap")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
