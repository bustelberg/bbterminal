"""Which constituents actually move a blended fundamental line, and by how much.

⚠⚠ THE LINE IS A CHAIN OF WEIGHTED STEPS, SO ONE BAD CELL IS NOT ONE BAD POINT — it multiplies
through every period after it. `_fundamental_blend` already carries two guards written after exactly
that (`_MIN_STEP_BASE_FRACTION` for a divisor too small to divide by, `_MAX_STEP_GROWTH` for a
result too large to have come from a business), and both were derived by reading the distribution of
real steps. This prints that distribution again, for any metric and universe, so the next suspicion
is answered with the data rather than with an argument.

⚠ IT RANKS BY INDEX IMPACT, NOT BY GROWTH. A +9,000% step in a 0.01%-weight constituent is a
curiosity; the same step at 0.07% weight is what moved ACWI's FCF/share line by +116pp. The product
`weight x growth` is the only ordering that finds the second without drowning in the first.

⚠ AND IT SHOWS WHAT THE GUARDS REFUSED, not only what they let through. A cap that is doing nothing
and a cap that is load-bearing look identical from the answer alone.

    cd backend && uv run python scripts/diagnose_blend_steps.py
    cd backend && uv run python scripts/diagnose_blend_steps.py --metric eps_nri --universe ACWI
    cd backend && uv run python scripts/diagnose_blend_steps.py --metric fcf_ps --top 40
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # noqa: E402,F401  (loads .env / .env.local first)
from routers import _fundamental_blend as fb  # noqa: E402


def _members(universe: str, metric: str) -> list[dict]:
    """`[{weight, points}]` for one universe — the SAME assembly `earnings.py` feeds the blend.

    ⚠ MIRRORED, NOT IMPORTED, because the endpoint builds it inline. A second assembly is a
    second thing to keep in step — so it is kept deliberately literal: same metric codes, same
    start date, same cap source. If this and the endpoint ever disagree, the diagnostic is wrong
    and not the line.

    ⚠ PAGED ON `.range()`. `metric_data` is far past any server row cap for a whole index, and an
    unpaged read here would silently measure a fraction of the constituents.
    """
    from collections import defaultdict

    from deps import IN_CHUNK_SIZE, supabase
    from routers.earnings import _BLEND_START, _metric_codes

    # ⚠ MEMBERSHIP AND CAPS EXACTLY AS THE ENDPOINT TAKES THEM — `universe_membership` by label,
    # then `company.market_cap_eur`. ⚠ A member with no cap is NOT dropped here: the endpoint
    # falls back to `1.0`, so dropping it would measure a different index from the one on screen.
    uni = (supabase.table("universe").select("universe_id")
           .eq("label", universe).limit(1).execute().data or [])
    if not uni:
        print(f"  !! no universe labelled {universe!r}")
        return []
    uid = uni[0]["universe_id"]
    ids = sorted({r["company_id"] for r in
                  (supabase.table("universe_membership").select("company_id")
                   .eq("universe_id", uid).execute().data or []) if r.get("company_id")})
    caps: dict[int, float] = {}
    for i in range(0, len(ids), IN_CHUNK_SIZE):
        for c in (supabase.table("company").select("company_id,market_cap_eur")
                  .in_("company_id", ids[i:i + IN_CHUNK_SIZE]).execute().data or []):
            if c.get("market_cap_eur"):
                caps[c["company_id"]] = float(c["market_cap_eur"])
    codes = list(_metric_codes(metric))
    raw: dict[int, list[dict]] = defaultdict(list)
    page_size = 1000
    for i in range(0, len(ids), IN_CHUNK_SIZE):
        chunk = ids[i:i + IN_CHUNK_SIZE]
        off = 0
        while True:
            rows = (supabase.table("metric_data")
                    .select("company_id,metric_code,target_date,numeric_value")
                    .in_("company_id", chunk).in_("metric_code", codes)
                    .gte("target_date", _BLEND_START)
                    .order("company_id").order("target_date").order("metric_code")
                    .range(off, off + page_size - 1).execute().data or [])
            if not rows:
                break
            for r in rows:
                raw[r["company_id"]].append(r)
            off += len(rows)

    names = _company_names(ids)
    out: list[dict] = []
    for cid in ids:
        pts = {str(m["target_date"])[:10]: float(m["numeric_value"])
               for m in raw.get(cid, ()) if m.get("numeric_value") is not None}
        if pts:
            out.append({"weight": caps.get(cid, 1.0), "points": pts,
                        "at": pts, "name": names.get(cid, str(cid))})
    return out


def _company_names(ids: list[int]) -> dict[int, str]:
    """⚠ NAMES, BECAUSE A company_id IN THE OUTPUT IS A ROW NOBODY CAN CHECK. The whole point is
    to be able to look the filing up."""
    from deps import IN_CHUNK_SIZE, supabase

    out: dict[int, str] = {}
    for i in range(0, len(ids), IN_CHUNK_SIZE):
        for r in (supabase.table("company").select("company_id,company_name")
                  .in_("company_id", ids[i:i + IN_CHUNK_SIZE]).execute().data or []):
            out[r["company_id"]] = r.get("company_name") or str(r["company_id"])
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--metric", default="fcf_ps", help="metric key (default fcf_ps)")
    ap.add_argument("--universe", default="ACWI", help="universe label (default ACWI)")
    ap.add_argument("--top", type=int, default=25, help="how many steps to print (default 25)")
    args = ap.parse_args()

    print(f"[1/4] loading {args.universe} members for {args.metric} …")
    members = _members(args.universe, args.metric)
    if not members:
        # ⚠ LOUD. An empty member list makes every number below zero, which reads as "no problem
        # found" rather than "nothing was measured".
        print("  !! no members — is the universe label right, and does this DB hold its caps?")
        return 2

    with_periods = sum(1 for m in members if m.get("weights") is not None)
    print(f"[2/4] {len(members)} members · {with_periods} carry PER-PERIOD weights")
    if not with_periods:
        # ⚠⚠ THE FINDING THIS SCRIPT WAS WRITTEN TO CATCH. With no `weights` map every step is
        # weighted by `_weight_at`'s scalar fallback — TODAY's market cap — so a constituent's
        # 2017 growth is weighted by its 2026 size. On a metric whose biggest movers grew INTO
        # the index that is look-ahead bias, and it inflates the line without any cell being
        # wrong.
        print("      !! none do — every step is weighted by TODAY's cap (look-ahead).")

    # ── every step, with what the guards did to it ─────────────────────────────────────────────
    # ⚠ ONE DENOMINATOR FOR EVERY STEP, taken once. Per-period totals would be more exact but
    # this assembly carries a single scalar cap per member anyway — see the finding below.
    total_w = sum(float(m.get("weight") or 0.0) for m in members) or 1.0
    accepted: list[tuple[float, str, str, float, float, float, float]] = []
    refused_base = refused_cap = refused_missing = 0
    for m in members:
        at = m.get("points") or {}
        scale = m.get("scale") or fb.member_scale(at)
        periods = sorted(at)
        for prev_p, now_p in zip(periods, periods[1:]):
            prev, now = at.get(prev_p), at.get(now_p)
            g = fb.step_growth(prev, now, scale)
            if g is None:
                if prev is None or now is None or (prev is not None and prev <= 0):
                    refused_missing += 1
                elif prev < fb._MIN_STEP_BASE_FRACTION * scale:   # noqa: SLF001
                    refused_base += 1
                else:
                    refused_cap += 1
                continue
            # ⚠ NORMALISED. `_weight_at` hands back whatever the member carries — for this
            # assembly that is a raw EUR market cap, so the raw product is unreadable and,
            # worse, looks like a percentage. Dividing by the period total makes the column
            # what it claims to be: this step's contribution to the index in points.
            w = (fb._weight_at(m, now_p) or 0.0) / total_w                # noqa: SLF001
            accepted.append((abs(w * g), m.get("name") or "?", f"{prev_p}->{now_p}",
                             float(prev), float(now), g, w))

    print(f"[3/4] {len(accepted)} accepted steps · refused: "
          f"{refused_missing} no-anchor/non-positive, {refused_base} immaterial base, "
          f"{refused_cap} over the growth cap")

    # ── the distribution, which is how both constants were chosen ──────────────────────────────
    growths = sorted(g for _, _, _, _, _, g, _ in accepted)
    if growths:
        def pct(q: float) -> str:
            return f"{growths[min(len(growths) - 1, int(len(growths) * q))] * 100:+,.0f}%"
        print(f"      accepted growth distribution: p50 {pct(0.50)} · p99 {pct(0.99)} · "
              f"p99.9 {pct(0.999)} · p99.99 {pct(0.9999)} · max {growths[-1] * 100:+,.0f}%")
        # ⚠ THE CAP IS 100x = +10,000%, so a 100x REDENOMINATION lands at +9,900% and passes.
        near = [g for g in growths if 50.0 <= g <= fb._MAX_STEP_GROWTH]   # noqa: SLF001
        print(f"      steps between +5,000% and the cap: {len(near)}"
              f"{'  <-- a 100x unit change sits in here' if near else ''}")

    print(f"[4/4] the {args.top} steps that move the line most (|weight x growth|):")
    accepted.sort(key=lambda r: -r[0])
    for impact, name, span, prev, now, g, w in accepted[:args.top]:
        print(f"  {impact * 100:8.2f}pp  {name[:34]:<34} {span:<12}"
              f" {prev:>14,.2f} -> {now:>14,.2f}  {g * 100:>+12,.0f}%  w={w * 100:5.2f}%")

    print("\nDone. Nothing was written.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
