"""HOW MANY ACWI CONSTITUENTS DO WE ALREADY PRICE BUT NOT KNOW ARE IN ACWI?

⚠⚠ THE QUESTION THIS ANSWERS, AND WHY IT IS NOT THE ONE THE UNIVERSE PAGE ANSWERS. ACWI asset
membership is built FROM company membership — `universe_asset_membership`'s own migration says the
backfill "resolves `company.isin -> asset_execution.isin -> analysis_id`". So an iShares constituent
that GuruFocus does not sell us never becomes a `company`, and therefore never becomes an ASSET
member either, even when `asset_execution` already holds it with a good ISIN and a live Yahoo
symbol. Constellation Software is the reported case: Toronto is not in `FEASIBLE_GF_EXCHANGES`, and
`CA21037X1006 / CSU.TO` sits in the asset grid priced and healthy.

MEASURED 2026-09-01 (file as-of 15-Apr-2026, 2,270 equities):

    already ACWI asset members       1062
    we price it, NOT marked as ACWI   189   <- the gap, AN UPPER BOUND
    no asset row found by name       1019

  and it is concentrated exactly where GuruFocus does not sell to us:
  Canada 65 | Australia 37 | United Kingdom 25 | South Africa 18 | United States 8.

⚠⚠ THE 189 IS AN UPPER BOUND AND THIS SCRIPT PROVES WHY, IN ITS OWN OUTPUT. Three of the
first twelve matches are WRONG: `BERKSHIRE HATHAWAY INC CLASS B` matched Berkshire **A**
(US0846701086 / BRK-A); `NEWMONT`, located United States, matched the Australian CDI line
(AU0000297962 / NEM.AX); and `MIZUHO FINANCIAL GROUP`, located Japan, matched MAGELLAN FINANCIAL
GROUP (AU000000MFG4 / MFG) — a different company on a different continent. That last one is the
WisdomTree Coffee -> Luckin Coffee failure reproduced exactly, on this data, today. So the SHAPE of
the answer is certain and the NUMBER is not, which is the strongest available argument for joining
on ISIN: the alternative is demonstrably unsafe.

⚠ THE NAME MATCH HERE IS FOR MEASUREMENT ONLY AND MUST NOT BE SHIPPED AS THE JOIN. This codebase has
paid for name matching twice (`NVIDIA Corporation` vs `NVIDIA CORP` scoring 75.9; WisdomTree Coffee
resolving to Luckin Coffee). It is used here because the bundled iShares export carries NO ISIN
column, so it is the only way to SIZE the gap before deciding whether an ISIN-bearing export is
worth wiring. The fix is the ISIN join; this script is the argument for building it.

Usage (from backend/):
    uv run python scripts/measure_acwi_asset_gap.py
"""
from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # noqa: E402,F401
from deps import supabase  # noqa: E402
from index_universe.acwi.holdings import load_acwi_holdings  # noqa: E402

#: Corporate forms and punctuation, stripped before comparing. Deliberately blunt — a false MATCH
#: here overstates the gap's recoverable half, so the number it produces is an upper bound and is
#: reported as one.
_NOISE = re.compile(
    r"\b(inc|corp|corporation|co|ltd|limited|plc|sa|nv|ag|se|spa|as|ab|oyj|holdings?|group|"
    r"class|cl|a|b|c|the)\b|[^a-z0-9 ]", re.I)


def norm(name: str) -> str:
    return re.sub(r"\s+", " ", _NOISE.sub(" ", (name or "").lower())).strip()


def page(table: str, cols: str, order: str = 'id', **eq) -> list[dict]:
    out: list[dict] = []
    off = 0
    while True:
        q = supabase.table(table).select(cols)
        for k, v in eq.items():
            q = q.eq(k, v)
        rows = q.order(order).range(off, off + 999).execute().data or []
        if not rows:
            return out
        out += rows
        off += len(rows)


def main() -> None:
    holdings, as_of = load_acwi_holdings()
    equities = [r for r in holdings if (r.get("Asset Class") or "").strip() == "Equity"]
    print(f"iShares file as-of {as_of}: {len(holdings)} rows, {len(equities)} equities\n")

    uni = (supabase.table("universe").select("universe_id,label")
           .eq("label", "ACWI").limit(1).execute().data or [])
    if not uni:
        print("no ACWI universe")
        return
    uid = uni[0]["universe_id"]

    members = page("universe_asset_membership", "analysis_id", order="analysis_id", universe_id=uid)
    member_ids = {m["analysis_id"] for m in members}
    print(f"ACWI asset members today: {len(member_ids)}")

    assets = page("asset_execution", "isin,name,yahoo_symbol,analysis_id,status", order="isin", status="ok")
    print(f"asset_execution status=ok: {len(assets)}\n")

    by_name: dict[str, dict] = {}
    for a in assets:
        n = norm(a.get("name") or "")
        if n and n not in by_name:
            by_name[n] = a

    matched, already, missing = [], 0, []
    for r in equities:
        a = by_name.get(norm(r.get("Name") or ""))
        if a is None:
            missing.append(r)
        elif a.get("analysis_id") in member_ids:
            already += 1
        else:
            matched.append((r, a))

    print(f"  already ACWI asset members            {already:5}")
    print(f"  WE PRICE IT, NOT MARKED AS ACWI       {len(matched):5}   <- the gap")
    print(f"  no asset row found by name            {len(missing):5}\n")

    print("Recoverable gap by country (top 12):")
    for c, n in Counter((r.get("Location") or "??") for r, _ in matched).most_common(12):
        print(f"   {c:<22} {n:4}")

    print("\nA few of them:")
    for r, a in matched[:12]:
        print(f"   {(r.get('Name') or '')[:34]:<34} {a['isin']:<14} "
              f"{(a.get('yahoo_symbol') or ''):<12} {r.get('Location')}")


if __name__ == "__main__":
    main()
