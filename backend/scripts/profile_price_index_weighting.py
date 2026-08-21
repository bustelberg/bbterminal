"""Is the benchmark's share-price CAGR overstated by the step weighting?

⚠⚠ THE QUESTION. `blend_series`' LEVEL path chains the line from weighted growth:

    index[p] = index[anchor] x (1 + Sum w_i*g_i / Sum w_i),   g_i = v_i(p)/v_i(anchor) - 1

and takes `w_i = _weight_at(member, p)` -- the market cap at the **END** of the interval, while
`g_i` spans anchor -> p. For revenue that is a mild inconsistency. For a PRICE series it is close to
circular: cap = price x shares, so a constituent that tripled carries ~3x the weight in the very
step where it tripled, and one that halved carries half. Winners are over-weighted in their own
winning step and losers under-weighted in their own losing step, so the index reads high by
construction.

A cap-weighted index return over [anchor, p] is the ANCHOR-weighted average -- you hold the index at
the start weights and let it run. This script computes both and prints the CAGR each implies, so the
size of the bias is a measurement rather than an argument.

Run (reads only, local or prod by whatever `deps` is pointed at):

    cd backend && uv run python scripts/profile_price_index_weighting.py --label ACWI --years 10
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # noqa: E402,F401  (loads .env / .env.local before anything reads a key)
from routers._fundamental_blend import (  # noqa: E402
    _prepare, _weight_at, _weighted_arithmetic, carry_forward, member_scale, step_growth,
    year_bucket, MIN_BLEND_COVERAGE_NAMES_PCT, MIN_BLEND_COVERAGE_PCT,
)


def _members(label: str, cadence: str) -> list[dict]:
    """The index's constituents in `blend_series`' member shape, carrying `price_ps` + period caps.

    ⚠ THE SAME TWO READS THE ENDPOINT MAKES, through the same helpers -- a bespoke query here would
    measure a panel the chart never sees.
    """
    from routers import earnings as E
    from routers._benchmark_index import _members as index_members

    rows = index_members(label, require_market_cap=True)
    isins = [r["isin"] for r in rows if r.get("isin")]
    print(f"{label}: {len(rows)} constituents, {len(isins)} with an ISIN")

    comp: dict[str, dict] = {}
    for i in range(0, len(isins), deps.IN_CHUNK_SIZE):
        for c in (deps.supabase.table("company").select("company_id,company_name,isin")
                  .in_("isin", isins[i:i + deps.IN_CHUNK_SIZE]).execute().data or []):
            comp[c["isin"]] = c
    cids = [c["company_id"] for c in comp.values()]
    print(f"  {len(cids)} resolve to a company row")

    caps = E.period_caps_eur(cids, cadence)
    # ⚠ RAW-DATED POINTS, NOT PERIOD-KEYED ONES. `blend_series` takes `{target_date: value}` and
    # buckets them itself (`carry_forward` measures staleness in DAYS, so a key of "2025" raises).
    # This is the same read `_bulk_blend_rows` makes, in the same shape.
    codes = list(E._metric_codes("price_ps"))
    raw = E._rows_by_company(cids, codes)
    prices: dict[int, dict[str, float]] = {}
    for cid, mrows in raw.items():          # ⚠ NOT `rows` — that is the member list, still needed
        for r in mrows:
            if r.get("numeric_value") is not None:
                prices.setdefault(cid, {})[str(r["target_date"])[:10]] = float(r["numeric_value"])

    out: list[dict] = []
    for r in rows:
        c = comp.get(r.get("isin") or "")
        if not c:
            continue
        pts = prices.get(c["company_id"]) or {}
        if not pts:
            continue
        out.append({
            "name": c.get("company_name") or r.get("isin"),
            "weight": float(r.get("market_cap_eur") or 0),
            "weights": caps.get(c["company_id"]) or {},
            "points": pts,
        })
    print(f"  {len(out)} carry a Month End Stock Price series")
    return out


def chain(members: list[dict], *, weight_at_anchor: bool) -> list[tuple[str, float]]:
    """`blend_series`' level path, with the step weight taken at the anchor or at the end.

    Everything else -- carry-forward, the two coverage floors, `step_growth`'s materiality and
    plausibility guards -- is the production code, imported. Only the one expression under test
    differs, so a difference in the answer is that expression and nothing else.
    """
    total_w = sum(abs(float(m.get("weight") or 0)) for m in members)
    total_n = len(members)
    prepared, _ = _prepare(members, "level", year_bucket)

    cover_w: dict[str, float] = {}
    cover_n: dict[str, int] = {}
    periods: set[str] = set()
    axis = sorted({k for p in prepared for k in p["by_year"]})
    for p in prepared:
        p["at"] = {}
        for period, (v, reported) in carry_forward(p["by_year"], axis).items():
            if not _weight_at(p, period):
                continue
            p["at"][period] = v
            periods.add(period)
            if reported:
                cover_w[period] = cover_w.get(period, 0.0) + abs(float(p.get("weight") or 0))
                cover_n[period] = cover_n.get(period, 0) + 1
        p["scale"] = member_scale(p["at"])

    def clears(d: str) -> bool:
        return (100.0 * cover_w.get(d, 0.0) / total_w >= MIN_BLEND_COVERAGE_PCT
                and 100.0 * cover_n.get(d, 0) / total_n >= MIN_BLEND_COVERAGE_NAMES_PCT)

    anchor: str | None = None
    level = 100.0
    out: list[tuple[str, float]] = []
    for d in sorted(periods):
        if not clears(d):
            continue
        if anchor is None:
            anchor = d
            out.append((d, level))
            continue
        wp = anchor if weight_at_anchor else d
        pairs = [(abs(float(w)), g)
                 for p in prepared
                 for w in [_weight_at(p, wp)]
                 for g in [step_growth(p["at"].get(anchor), p["at"].get(d), p["scale"])]
                 if w and g is not None]
        step = _weighted_arithmetic(pairs)
        if step is None or 1.0 + step <= 0:
            continue
        level *= 1.0 + step
        out.append((d, level))
        anchor = d
    return out


def cagr(series: list[tuple[str, float]], years: int | None) -> tuple[float | None, str, str, int]:
    """Point-to-point rate over the last `years` periods of the drawn line, or its whole span."""
    if len(series) < 2:
        return None, "", "", 0
    end_p, end_v = series[-1]
    if years is None:
        start_p, start_v = series[0]
    else:
        want = str(int(end_p[:4]) - years)
        hit = next((s for s in series if s[0][:4] == want), None)
        if hit is None:
            return None, "", "", 0
        start_p, start_v = hit
    n = int(end_p[:4]) - int(start_p[:4])
    if n <= 0 or start_v <= 0:
        return None, start_p, end_p, 0
    return 100.0 * ((end_v / start_v) ** (1.0 / n) - 1.0), start_p, end_p, n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="ACWI")
    ap.add_argument("--cadence", default="annual")
    ap.add_argument("--years", type=int, default=10)
    a = ap.parse_args()

    members = _members(a.label, a.cadence)
    if not members:
        raise SystemExit(f"no members with a price series for {a.label}")

    for tag, at_anchor in (("END weight   (production today)", False),
                           ("ANCHOR weight (a cap-weighted index)", True)):
        s = chain(members, weight_at_anchor=at_anchor)
        full = cagr(s, None)
        win = cagr(s, a.years)
        print(f"\n{tag}")
        print(f"  drawn periods : {len(s)}  {s[0][0] if s else '-'} -> {s[-1][0] if s else '-'}")
        print(f"  index         : {s[0][1]:.1f} -> {s[-1][1]:.1f}" if s else "  index: -")
        if full[0] is not None:
            print(f"  CAGR full span: {full[0]:+.2f}%/yr  ({full[1]} -> {full[2]}, {full[3]}y)")
        if win[0] is not None:
            print(f"  CAGR {a.years}y      : {win[0]:+.2f}%/yr  ({win[1]} -> {win[2]})")


if __name__ == "__main__":
    main()
