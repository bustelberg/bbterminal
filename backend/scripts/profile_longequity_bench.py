"""What one benchmark selection on the Long Equity tab actually costs, per endpoint.

Read-only. Calls the blend + `*-inputs` endpoint functions in-process (no HTTP, no auth) exactly as
the tab fires them — CONCURRENTLY — and reports, per endpoint:

    server seconds · rows returned · decoded JSON bytes · bytes on the wire

⚠ THE TRANSFER IS THE MEASUREMENT THAT MATTERS HERE, not the seconds. `_blend_cache` caches every
one of these responses in-process, so on a warm process the server work is a memcpy and the ONLY
remaining cost is shipping the payload to the browser and parsing it there. Each response carries
one row PER CONSTITUENT (1,514 for ACWI, 22 for AEX), each row holding two or three full metric
series — all of which the client reduces to ONE blended line. That ratio, not the query time, is
why a reader reports ACWI as slow and AEX as instant.

Measured on ACWI/annual, in the order the two changes landed (2026-08-19):

    before                              13.21 MB decoded, 13.21 MB on the wire
    + gzip the cached responses         13.21 MB decoded,  4.85 MB on the wire
    + lift out `market_cap_by_period`    9.34 MB decoded,  3.16 MB on the wire

⚠ IT ASKS FOR GZIP, AS A BROWSER DOES. `cached_blend` honours `Accept-Encoding` and caches the
COMPRESSED bytes, so `wire` below is what actually crosses the network and `raw` is what the client
sees after the browser decodes it. Measuring only `raw` is how the 2.9x looks like nothing.

    uv run python scripts/profile_longequity_bench.py             # ACWI vs AEX, annual
    uv run python scripts/profile_longequity_bench.py --labels ACWI --cadence quarterly
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # noqa: E402,F401  (loads .env / .env.local before anything reads a key)

# The per-company data-quality warnings belong to another script; they bury the eleven lines this
# one prints.
logging.disable(logging.WARNING)
from fastapi import Request  # noqa: E402
from routers.earnings import (  # noqa: E402
    FundamentalCoverageRequest,
    capex_margin_inputs,
    cash_conversion_inputs,
    cash_return_inputs,
    debt_ratio_inputs,
    dividend_yield_inputs,
    fcf_sbc_yield_inputs,
    fundamental_blend_metrics,
    gross_margin_inputs,
    interest_burden_inputs,
    margin_inputs,
    sbc_ocf_inputs,
    universe_period_caps,
)

# The exact set the tab fires when a benchmark is picked: the growth blend (one call for the three
# growth cards) plus one `*-inputs` per derived card.
ENDPOINTS = [
    ("fundamental-blend-metrics", fundamental_blend_metrics),
    # ⚠ ONE REQUEST FOR ALL TEN CARDS. `market_cap_by_period` used to ride on every row of every
    # `*-inputs` response — the same cap table ten times, 29.9% of each ACWI payload. The client
    # splices it back in `useBenchInputs`, so this line should be ~0.19 MB and the ten below should
    # each be a third smaller than they were.
    ("universe-period-caps", universe_period_caps),
    ("margin-inputs", margin_inputs),
    ("debt-ratio-inputs", debt_ratio_inputs),
    ("cash-return-inputs", cash_return_inputs),
    ("interest-burden-inputs", interest_burden_inputs),
    ("sbc-ocf-inputs", sbc_ocf_inputs),
    ("capex-margin-inputs", capex_margin_inputs),
    ("gross-margin-inputs", gross_margin_inputs),
    ("cash-conversion-inputs", cash_conversion_inputs),
    ("fcf-sbc-yield-inputs", fcf_sbc_yield_inputs),
    ("dividend-yield-inputs", dividend_yield_inputs),
]

# What LongEquityTab.tsx names on the growth blend — the three growth cards' metrics plus their
# forecast twins. Unnamed, that endpoint reads every charted code per constituent.
BLEND_METRICS = ["eps_nri", "eps_nri_estimate", "revenue", "fcf_ps", "shares"]


def _kb(n: int) -> str:
    return f"{n / 1024:,.0f} KB" if n < 1024 * 1024 else f"{n / 1024 / 1024:,.2f} MB"


def _browser_request() -> Request:
    """A minimal ASGI request that says it accepts gzip — the only thing `cached_blend` reads."""
    return Request({"type": "http", "method": "POST", "path": "/profile",
                    "headers": [(b"accept-encoding", b"gzip, deflate, br")], "query_string": b""})


async def _one(name: str, fn, label: str, cadence: str) -> dict:
    body = FundamentalCoverageRequest(universe=label, cadence=cadence)
    if name == "fundamental-blend-metrics":
        body.metrics = BLEND_METRICS
    t0 = time.perf_counter()
    try:
        out = await fn(body, _browser_request())
    except Exception as exc:                                     # noqa: BLE001
        print(f"  {name:<28} FAILED: {type(exc).__name__}: {exc}", flush=True)
        return {"name": name, "failed": True}
    secs = time.perf_counter() - t0
    wire = out.body                     # a `universe` request always comes back as a Response
    raw = gzip.decompress(wire)
    payload = json.loads(raw)
    rows = payload.get("rows")
    n_rows = len(rows) if isinstance(rows, list) else len(payload.get("metrics") or [])
    print(f"  {name:<28} {secs:6.2f}s  {n_rows:>5} rows  raw {_kb(len(raw)):>10}"
          f"  wire {_kb(len(wire)):>10}  ({len(raw) / max(len(wire), 1):.1f}x)", flush=True)
    return {"name": name, "secs": secs, "rows": n_rows, "raw": len(raw), "gz": len(wire)}


async def run(label: str, cadence: str) -> None:
    print(f"\n=== {label} / {cadence} — {len(ENDPOINTS)} requests, fired together "
          f"(as the tab does) ===", flush=True)
    t0 = time.perf_counter()
    results = await asyncio.gather(*(_one(n, f, label, cadence) for n, f in ENDPOINTS))
    wall = time.perf_counter() - t0
    ok = [r for r in results if not r.get("failed")]
    raw = sum(r["raw"] for r in ok)
    gz = sum(r["gz"] for r in ok)
    print(f"  {'TOTAL':<28} {wall:6.2f}s wall ({sum(r['secs'] for r in ok):.2f}s of work)"
          f"   raw {_kb(raw)}   wire {_kb(gz)}   saves {_kb(raw - gz)}", flush=True)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--labels", nargs="+", default=["AEX", "ACWI"])
    ap.add_argument("--cadence", default="annual", choices=["annual", "quarterly"])
    args = ap.parse_args()
    for label in args.labels:
        await run(label, args.cadence)


if __name__ == "__main__":
    asyncio.run(main())
