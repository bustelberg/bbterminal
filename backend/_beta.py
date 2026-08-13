"""Sanity-check computed beta against real data. A market-wide index against ITSELF must be 1.0.

⚠ A SCRIPT, NOT A MODULE — every line below runs AT IMPORT and hits the database. It lives in the
`backend` package root, so `import _beta` from anywhere would execute a full S&P index load as a
side effect. Nothing imports it today; if this is meant to be kept, `scripts/` is where the other
one-off probes live.
"""
import statistics
import time

from routers._airs_portfolio_beta import benchmark_returns, holding_beta
from routers._airs_portfolio_perf import _closes, _fx
from routers._asset_benchmark import index_rows

ANCHOR, END = "2025-01-02", "2026-08-11"
t = time.perf_counter()
rows, cov = index_rows("SP500", ANCHOR)
print(f"index_rows: {len(rows)} constituents, coverage {cov.get('covered_pct')}, {(time.perf_counter()-t)*1000:.0f} ms")
print("  sample row keys:", sorted(rows[0].keys())[:12] if rows else "none")

t = time.perf_counter()
bench = benchmark_returns("SP500", ANCHOR, END, rows)
print(f"benchmark_returns: {len(bench)} daily returns, {(time.perf_counter()-t)*1000:.0f} ms")

# ⚠ THE CONTROL: the biggest constituents, whose beta must be near 1 by construction if the
# maths is right — and a low-beta name must come out low.
aids = [r["analysis_id"] for r in rows if r.get("analysis_id")]
closes = _closes(aids, ANCHOR, END)
fx = _fx({(r.get("currency") or "USD") for r in rows}, ANCHOR, END)
by_cap = sorted([r for r in rows if r.get("analysis_id")],
                key=lambda r: -(r.get("start_cap_eur") or 0))
print("\n  name                              beta   (top constituents by start cap)")
for r in by_cap[:8]:
    b = holding_beta(closes.get(r["analysis_id"]), r.get("currency"), fx, ANCHOR, bench)
    print(f"    {str(r.get('name'))[:30]:<32} {b if b is None else round(b,3)}")

# The index against itself: cov(x,x)/var(x) == 1 exactly.
xs = list(bench.values())
self_beta = statistics.pvariance(xs) / statistics.pvariance(xs) if statistics.pvariance(xs) else None
print(f"\n  CONTROL — index vs itself: {self_beta} (must be 1.0)")
w = sum((r.get('start_cap_eur') or 0) for r in by_cap)
betas = [(holding_beta(closes.get(r['analysis_id']), r.get('currency'), fx, ANCHOR, bench),
          r.get('start_cap_eur') or 0) for r in by_cap]
ok = [(b, cw) for b, cw in betas if b is not None]
print(f"  CONTROL — cap-weighted mean beta of all constituents: "
      f"{sum(b*cw for b,cw in ok)/sum(cw for _,cw in ok):.4f} (must be ~1.0)")
print(f"  measurable: {len(ok)}/{len(betas)}")
