"""Beta per holding — how much of the benchmark's move each name carries.

    beta = cov(r_holding, r_benchmark) / var(r_benchmark)      on DAILY EUR returns

⚠⚠ IT IS COMPUTED, NOT FETCHED, AND THAT IS THE ONLY HONEST OPTION HERE. The obvious source is a
    vendor field — GuruFocus publishes a beta — and it would be WRONG on this screen. A vendor beta
    is a US stock against a US index in USD; every figure in this modal is EUR, priced from
    `asset_price` (yfinance), against an index rebuilt from OUR OWN membership. Putting a
    USD-vs-S&P beta beside a EUR-vs-ACWI return is the same category error CLAUDE.md already
    records for the benchmark itself — *the benchmark must be priced in the same world as the
    portfolio* — and it would be invisible, because a beta of 1.1 looks like a beta of 1.1.
    (Checked: no beta appears anywhere in `gurufocus_api.json` either, so it was never free.)

⚠ THE WINDOW IS SINCE-INCEPTION, NEVER YTD, and this is settled precedent rather than a choice.
    `_airs_portfolio_perf` anchors Sharpe and Sortino to the composition's own effective date for
    exactly one reason: 27 of 56 models were (re)defined this year, so a YTD-anchored ratio is a
    backtest of weights chosen with hindsight for half the list. Beta is the same kind of number
    and inherits the same rule — and the same `MIN_STAT_DAYS` floor, because a beta off five
    points is noise with two decimals rendered in the same column as one off two years.

⚠⚠ THE DATES ARE INTERSECTED, NOT FORWARD-FILLED — AND THIS IS THE ONE THAT SILENTLY BIASES THE
    NUMBER. `_index` samples the UNION of trading dates and forward-fills, which is right for a
    VALUE curve (a holding you still own on a Tokyo holiday is worth its last close, not zero).
    Carried into a covariance it is poison: a forward-filled day contributes a 0.0% holding return
    against a benchmark that really moved, which is a genuine observation of "no correlation" that
    never happened. Every such day drags beta toward zero, and a name on a foreign calendar has
    dozens of them. So the returns are taken only on dates BOTH sides actually traded.

⚠ EUR RETURNS, SO THE FX LEG IS IN THE BETA. A dollar name held by a euro investor moves with the
    dollar as well as with the market, and that is a real part of how it tracks a EUR-denominated
    index. Stripping FX out would measure a portfolio nobody holds — the same reason every return
    on this page is EUR.

⚠ THE BENCHMARK CURVE IS THE EXPENSIVE HALF AND IS CACHED PER (label, anchor). It needs ~500
    constituents' daily closes, which is precisely the load `window_marks` was written to avoid
    for the marks path. It is identical for every portfolio sharing a benchmark and window, so it
    is computed once; the per-holding series were already being loaded for the returns.
"""
from __future__ import annotations

import logging
import statistics

from routers._airs_portfolio_perf import (
    MIN_STAT_DAYS,
    _closes,
    _daily_returns,
    _eur_series,
    _fx,
    _index,
)

_log = logging.getLogger(__name__)

# One entry per (label, anchor, end) — see the module note. Bounded because a session touches a
# handful of benchmarks over a handful of inception dates, not an unbounded key space.
_BENCH_CURVE: dict[tuple[str, str, str], tuple[list[str], list[float]] | None] = {}
_MAX_BENCH = 32


def _dated_returns(dates: list[str], values: list[float]) -> dict[str, float]:
    """`{date: return}` from a dated curve.

    `_index` returns `values[0]` as the anchor base (1.0) with `dates` aligned to `values[1:]`, so
    the return ON `dates[i]` is `values[i+1] / values[i] - 1`.
    """
    out: dict[str, float] = {}
    for i, d in enumerate(dates):
        prev, cur = values[i], values[i + 1]
        if prev > 0:
            out[d] = cur / prev - 1.0
    return out


def benchmark_returns(label: str, anchor: str, end: str,
                      members: list[dict]) -> dict[str, float]:
    """`{date: daily EUR return}` for the index, over `[anchor, end]`.

    `members` is what `index_rows` already produced — start-of-window cap weights and the currency
    each constituent trades in. ⚠ The WEIGHTS ARE NOT RE-DERIVED here: weighting by today's cap is
    the look-ahead bias that turned +9.10% into +21.70%, and a second place to get that wrong is a
    second place for it to come back.

    ⚠ THE ROWS CARRY `company_id`, NOT `analysis_id` — the bridge to the price world is the ISIN,
    and it is a JOIN, never a column (the same rule the benchmark's own membership follows). Going
    looking for `r["analysis_id"]` here returns an empty curve and therefore a blank beta on every
    row, which reads as "we have no prices" rather than "the loader asked for the wrong key".
    """
    key = (label, anchor, end)
    if key in _BENCH_CURVE:
        cached = _BENCH_CURVE[key]
        return _dated_returns(*cached) if cached else {}

    isin_of = {m["isin"]: m for m in members if m.get("isin")}
    execs = _executions(sorted(isin_of))
    aids = [e["analysis_id"] for e in execs.values() if e.get("analysis_id")]
    if not aids:
        _BENCH_CURVE[key] = None
        return {}
    closes = _closes(aids, anchor, end)
    fx = _fx({(m.get("currency") or "USD") for m in members}, anchor, end)
    legs: list[tuple[float, list[tuple[str, float]] | None]] = []
    for isin, m in isin_of.items():
        ex = execs.get(isin)
        w = m.get("start_cap_eur") or 0.0
        if not ex or not ex.get("analysis_id") or w <= 0:
            continue
        # ⚠ THE EXECUTION'S currency, not the member's. The member row's `currency` is the
        # COMPANY's reporting currency; the closes in `asset_price` are quoted in whatever the
        # LISTING trades in, and converting a pence quote at the pound rate is the hundredfold
        # error `_rate` exists to prevent.
        legs.append((w, _eur_series(closes.get(ex["analysis_id"]) or [],
                                    ex.get("currency") or m.get("currency"), fx)))
    if not legs:
        _BENCH_CURVE[key] = None
        return {}
    dates, values, _w = _index(legs, anchor, return_dates=True)
    if len(_BENCH_CURVE) >= _MAX_BENCH:
        _BENCH_CURVE.clear()          # a working set, not an LRU — the key space is tiny
    _BENCH_CURVE[key] = (dates, values)
    return _dated_returns(dates, values)


def holding_beta(series: list[tuple[str, float]] | None, currency: str | None,
                 fx: dict, anchor: str, bench: dict[str, float]) -> float | None:
    """One holding's beta against `bench`, or `None` when it cannot be measured.

    ⚠ `None`, NEVER 0.0. A beta of zero is a real and meaningful value (a name that does not move
    with the market at all); "we could not measure this" is a different statement, and a column
    that prints 0.00 for both is worse than one that prints nothing for the second.
    """
    if not series or not bench:
        return None
    eur = _eur_series(series, currency, fx)
    if not eur:
        return None
    dates, values, _w = _index([(1.0, eur)], anchor, return_dates=True)
    mine = _dated_returns(dates, values)

    # ⚠ INTERSECTION — see the module note. A forward-filled non-trading day is a fabricated 0%
    # observation and every one of them pulls beta toward zero.
    both = sorted(set(mine) & set(bench))
    if len(both) < MIN_STAT_DAYS:
        return None
    xs = [bench[d] for d in both]
    ys = [mine[d] for d in both]
    var = statistics.pvariance(xs)
    if var <= 0:
        # A benchmark that did not move has no beta to measure against — dividing by it would
        # produce a number whose size depends only on floating-point noise.
        return None
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True)) / len(both)
    return cov / var
