"""Benchmark + correctness check for the variants-sweep optimizations.

Runs a fixed synthetic 24-variant sweep two ways:
  (a) uncached  — every variant runs run_backtest with no shared cache
  (b) cached    — score_cache + price_cache shared across variants
                  (Win #2 + Win #3 + any later wins that hang off
                  run_backtest kwargs)

Prints wall-clock and a stable hash of the BacktestResult list so each
optimization change can be verified for both performance AND
correctness. The hash MUST match between (a) and (b) — if a new cache
silently corrupts results, the hash will catch it.

Run: `uv run python -m tests._bench_variants` (from backend/)
"""
from __future__ import annotations

import hashlib
import json
import time
from datetime import date

from momentum.backtest import BacktestConfig, run_backtest

from tests._backtest_helpers import (
    build_prices_df,
    build_universe_df,
    calendar_daily,
    equal_signal_weights,
)


_BENCH_START = date(2020, 1, 1)
_BENCH_END = date(2024, 12, 31)
_BENCH_HIST_START = "2018-09-01"  # ~16mo pre-roll for 12-1 + 200ma signals
_BENCH_PRICES_END = "2025-01-15"


def build_synthetic_universe(n_cids: int = 40, n_sectors: int = 4):
    """N cids in M sectors with deterministic-but-varied growth rates.
    Each cid is monotonic so signals rank reproducibly across runs."""
    dates = calendar_daily(_BENCH_HIST_START, _BENCH_PRICES_END)
    rates: dict[int, float] = {}
    for cid in range(1, n_cids + 1):
        # Mix of growth factors so different (top_n, per) selections
        # don't all pick the same names.
        rates[cid] = 1.0 + (0.0006 + 0.00007 * ((cid * 31) % 13))
    prices = build_prices_df(rates, dates)
    universe = build_universe_df([
        (cid, f"T{cid}", f"S{(cid - 1) % n_sectors + 1}", f"Co-{cid}")
        for cid in range(1, n_cids + 1)
    ])
    return prices, universe


def variant_specs() -> list[dict]:
    """24 variants: 4 (top_n) × 3 (per_sector) × 2 (frequency).
    Mirrors a realistic mid-sized hyperparameter sweep."""
    top_ns = (2, 3, 4, 5)
    per_ns = (2, 3, 4)
    freqs = ("monthly", "every_3_months")
    return [
        {"top_n_sectors": t, "top_n_per_sector": p, "rebalance_frequency": f}
        for t in top_ns
        for p in per_ns
        for f in freqs
    ]


def _make_config(spec: dict) -> BacktestConfig:
    return BacktestConfig(
        start_date=_BENCH_START,
        end_date=_BENCH_END,
        signal_weights=equal_signal_weights(),
        category_weights={"price": 1.0, "volume": 0.0},
        **spec,
    )


def run_sweep_uncached(prices, universe) -> list:
    """Naive: each variant runs with no shared cache."""
    results = []
    for spec in variant_specs():
        r = run_backtest(_make_config(spec), prices, universe)
        results.append(r)
    return results


def run_sweep_cached(prices, universe) -> list:
    """With every shared cache run_backtest accepts today."""
    score_cache: dict = {}
    price_cache: dict = {}
    selection_cache: dict = {}
    results = []
    for spec in variant_specs():
        r = run_backtest(
            _make_config(spec), prices, universe,
            score_cache=score_cache,
            price_cache=price_cache,
            selection_cache=selection_cache,
        )
        results.append(r)
    return results


def hash_results(results) -> str:
    """SHA-256 over the JSON-serialized result list. Stable across
    Python invocations as long as the BacktestResult.to_dict() is
    deterministic — which it must be for the bench to be useful."""
    payload = json.dumps(
        [r.to_dict() for r in results], sort_keys=True, default=str,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def main() -> None:
    prices, universe = build_synthetic_universe()
    n = len(variant_specs())
    print(f"Bench: {n} variants × ~5y synthetic data ({len(prices)} price rows, "
          f"{universe['company_id'].nunique()} cids)")
    print()
    print(f"  {'mode':<14}{'wall':>10}{'per-var':>12}    hash")
    print(f"  {'-' * 14}{'-' * 10}{'-' * 12}    {'-' * 16}")

    rows = []
    for label, runner in [
        ("uncached", run_sweep_uncached),
        ("cached", run_sweep_cached),
    ]:
        t0 = time.monotonic()
        results = runner(prices, universe)
        dt = time.monotonic() - t0
        h = hash_results(results)
        rows.append((label, dt, h))
        print(f"  {label:<14}{dt:>8.2f}s{(dt / n * 1000):>10.0f}ms    {h}")

    print()
    uncached_dt = rows[0][1]
    cached_dt = rows[1][1]
    speedup = uncached_dt / cached_dt if cached_dt > 0 else float("inf")
    print(f"  Speedup (cached vs uncached): {speedup:.2f}x")
    if rows[0][2] == rows[1][2]:
        print("  Correctness: PASS (hashes match)")
    else:
        print("  Correctness: FAIL (hashes differ — a cache changed results)")


if __name__ == "__main__":
    main()
