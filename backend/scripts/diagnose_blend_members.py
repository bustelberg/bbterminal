"""WHICH MEMBERS A BLENDED LINE LEFT OUT, AND WHY — the "n of m" on a Long Equity card, explained.

⚠⚠ THE COUNT IS DELIBERATELY MUTE ABOUT INDIVIDUALS. `member_counts` says 1,509 of 1,511 and
nothing about which two, because a card cannot list names for a 1,900-name index without becoming a
table. This is the other half: run it when somebody asks.

⚠ IT CALLS THE ENDPOINT'S OWN FUNCTIONS (`_blend_inputs`, `_totals_for`, `blend_series`) rather
than re-deriving "the same way". A diagnostic that computes independently answers a question about
itself.

    uv run python scripts/diagnose_blend_members.py ACWI revenue
    uv run python scripts/diagnose_blend_members.py AEX fcf_ps
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deps  # noqa: E402,F401  — loads .env before anything reads it


async def main(universe: str, metric: str) -> None:
    from routers.earnings import (  # noqa: PLC0415
        _blend_extras, _blend_inputs, _metric_codes, FundamentalCoverageRequest,
    )

    body = FundamentalCoverageRequest(universe=universe, metrics=[metric], cadence="annual")
    covered, cov = await _blend_inputs(body)
    print(f"{universe} / {metric}: {len(covered)} covered members "
          f"({cov['covered_pct']:.1f}% of weight)")

    caps, totals = await asyncio.to_thread(_blend_extras, body, covered, [metric])
    code = next((c for c in _metric_codes(metric) if c in totals), None)
    if code is None:
        print("no euros for this metric at all — it is not on the aggregate path "
              "(see _AGGREGATABLE_PER_SHARE / _AGGREGATABLE_TOTAL)")
        return

    missing = await asyncio.to_thread(_not_in_line, covered, caps, totals, code, metric)
    print(f"{len(covered) - len(missing)} of {len(covered)} are in the line; "
          f"{len(missing)} are not\n")
    if not missing:
        return

    ids = [r["company_id"] for r in missing]
    rows = (deps.supabase.table("company")
            .select("company_id,company_name,market_cap_eur,"
                    "gurufocus_exchange:gurufocus_exchange(currency_code)")
            .in_("company_id", ids).execute().data or [])
    by_id = {r["company_id"]: r for r in rows}
    for r in missing:
        c = by_id.get(r["company_id"], {})
        ccy = ((c.get("gurufocus_exchange") or {}) or {}).get("currency_code")
        print(f"  {c.get('company_name') or r['company_id']:<42} {ccy or '-':<5} "
              f"w={float(r.get('weight_pct') or 0):.4f}%  {r['_reason']}")


def _not_in_line(covered: list[dict], caps: dict | None, totals: dict, code: str,
                 metric: str) -> list[dict]:
    """The covered members `blend_series` does NOT count in `fund_members`, each with its reason.

    ⚠⚠ IT BUILDS THE MEMBER DICTS EXACTLY AS `_blend_rows` DOES — points, per-period caps and euros
    — and then asks `blend_series` ITSELF, one member at a time, whether that member carries euros
    at a period it also has a weight for. Re-deriving the rule here would answer a question about
    this script.

    ⚠ THE CAPS ARE THE HALF THAT IS EASY TO FORGET: an index member is weighted by the market cap it
    had IN THAT PERIOD, so one with no cap history is out of every period — it has euros, it clears
    `_prepare`, and it is still not in the line. That is exactly the gap this script was written to
    name.
    """
    from routers._fundamental_blend import blend_series, year_bucket  # noqa: PLC0415
    from routers.earnings import _bulk_blend_rows, _ltm_blend_rows  # noqa: PLC0415

    cids = [r["company_id"] for r in covered]
    rows = _bulk_blend_rows(cids, [metric], "annual") + _ltm_blend_rows(cids, [metric], "annual")
    by_company: dict[int, dict[str, float]] = {}
    for r in rows:
        if r["metric_code"] == code and r.get("numeric_value") is not None:
            by_company.setdefault(r["company_id"], {})[str(r["target_date"])[:10]] =                 float(r["numeric_value"])
    fund = totals.get(code, {})

    out: list[dict] = []
    for r in covered:
        cid = r["company_id"]
        m = {"weight": r["weight_pct"],
             **({"weights": (caps or {}).get(cid, {})} if caps else {}),
             "points": by_company.get(cid, {}),
             "fund_points": fund.get(cid, {})}
        s = blend_series([m], code, year_bucket)
        if s.get("fund_members"):
            continue
        row = dict(r)
        row["_reason"] = (
            "no figures for this metric" if not m["points"]
            else "no euros — no listing currency, or no FX rate for it"
            if not m["fund_points"]
            else _no_cap_reason(cid)
            if caps and not (caps or {}).get(cid)
            else "every reported figure is non-positive, so a level series has no base"
            if not s.get("members")
            else "carries euros and a cap, but they never meet in one period")
        out.append(row)
    return out


def _no_cap_reason(cid: int) -> str:
    """WHY this member has no per-period cap — and the two answers are not the same problem.

    ⚠⚠ "NO MARKET CAP" IS THE ANSWER A READER CANNOT ACT ON, AND IT WAS THE WRONG ONE. Measured on
    ACWI 2026-08-31: CSG NV and Alpha Bank SA both HAVE `Valuation and Quality__Market Cap` rows for
    every period they report — every value is **0**. GuruFocus zero-fills, and `period_caps_eur`
    refuses a non-positive cap (absent, never 0, so the company is not put in a denominator as
    worth nothing). They are the only two of ACWI's 1,998 members in that state, and both carry a
    real company-level `market_cap_eur` (EUR 14.5bn / 9.1bn) from another source — which is exactly
    why they look fine on every other screen. Saying "no market cap" sends the reader to look for
    something that is there.
    """
    rows = (deps.supabase.table("metric_data").select("numeric_value")
            .eq("company_id", cid).eq("metric_code", "annuals__Valuation and Quality__Market Cap")
            .execute().data or [])
    if not rows:
        return "no per-period market cap filed at all, so it is in no period's average"
    return (f"{len(rows)} per-period market caps stored and every one is 0 "
            "(vendor zero-fill) — a non-positive cap is refused, so it is in no period's average")


def _sibling(name: str, ids: list[int], cid: int) -> bool:
    """Does another company anywhere share this normalised name? ⚠ The dedupe runs over the WHOLE
    member list, so the winner is usually NOT in the missing set — looking only there would report
    the wrong reason for every deduped share class."""
    if not name:
        return False
    rows = (deps.supabase.table("company").select("company_id")
            .ilike("company_name", name).execute().data or [])
    return any(r["company_id"] != cid for r in rows)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "ACWI",
                     sys.argv[2] if len(sys.argv) > 2 else "revenue"))
