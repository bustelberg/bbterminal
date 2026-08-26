"""What the PRODUCTION blend now reports for a benchmark's share-price line.

⚠ THE ENDPOINT'S OWN CALL PATH, not a reimplementation — `_load_and_expand_members` ->
`_bulk_blend_rows` -> `_blend_rows` -> `blend_series`, exactly as
`POST /api/earnings/fundamental-blend-metrics` runs it with `{universe, metrics:["price_ps"]}`.
`profile_price_index_weighting.py` measured the BIAS by running the level chain twice; this proves
the fix reached the thing the card actually reads.

Prints the drawn index and its point-to-point CAGR — the same `(end/start)^(1/n) - 1` the card's
tile and the Tables tab's row both report.

    cd backend && uv run python scripts/verify_price_index_cagr.py --label ACWI
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # noqa: E402,F401  (loads .env / .env.local first)


async def run(label: str, cadence: str) -> None:
    from routers.earnings import (  # noqa: PLC0415
        FundamentalCoverageRequest, _blend_inputs, _blend_rows, _bulk_blend_rows, _metric_codes,
        period_caps_eur,
    )

    body = FundamentalCoverageRequest(universe=label, cadence=cadence, metrics=["price_ps"])
    covered, cov = await _blend_inputs(body)
    cids = [r["company_id"] for r in covered]
    print(f"{label}: {len(covered)} covered constituents, {cov['covered_pct']:.1f}% of weight")

    rows = await asyncio.to_thread(_bulk_blend_rows, cids, ["price_ps"], cadence)
    caps = await asyncio.to_thread(period_caps_eur, cids, cadence)
    built = await asyncio.to_thread(_blend_rows, rows, covered, caps, cadence)

    code = _metric_codes("price_ps")[0]
    pts = sorted((str(m["target_date"])[:4], float(m["numeric_value"]))
                 for m in built["metrics"] if m["metric_code"] == code)
    if len(pts) < 2:
        raise SystemExit(f"only {len(pts)} point(s) drawn — nothing to measure")

    print(f"  drawn {pts[0][0]} -> {pts[-1][0]}   index {pts[0][1]:.1f} -> {pts[-1][1]:.1f}")
    for years in (5, 10, None):
        end_y, end_v = pts[-1]
        start = pts[0] if years is None else next(
            (p for p in pts if p[0] == str(int(end_y) - years)), None)
        if start is None:
            print(f"  {years}y: no {int(end_y) - years} point on the line")
            continue
        n = int(end_y) - int(start[0])
        if n <= 0 or start[1] <= 0:
            continue
        rate = 100.0 * ((end_v / start[1]) ** (1.0 / n) - 1.0)
        print(f"  CAGR {'full' if years is None else f'{years}y':>4}: {rate:+.2f}%/yr"
              f"   ({start[0]} -> {end_y}, {n}y)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="ACWI")
    ap.add_argument("--cadence", default="annual")
    a = ap.parse_args()
    asyncio.run(run(a.label, a.cadence))


if __name__ == "__main__":
    main()
