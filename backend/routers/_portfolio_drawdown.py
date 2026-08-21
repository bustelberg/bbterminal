"""Maximum drawdown — the deepest peak-to-trough fall in the series.

    Wₜ = ∏(1 + Rₛ)        Mₜ = max_{s≤t} Wₛ        DDₜ = Wₜ/Mₜ − 1        MDD = min DDₜ

⚠⚠ THIS IS THE YFINANCE RECONSTRUCTION OF TODAY'S HOLDINGS, AND IT IS NOT THE CLIENT'S DRAWDOWN.
Two different numbers, not interchangeable, and the panel says which one it is showing:

  * AIRS returns give the drawdown the client ACTUALLY LIVED THROUGH — real trades, real costs, real
    timing. That is the number for a client report.
  * this one rebuilds a series from the holdings as they stand TODAY. It carries look-ahead bias
    (the weights are the ones chosen with hindsight) and survivorship bias (names since sold are
    simply absent, and the ones sold are disproportionately the ones that fell). It answers "how
    deep a hole has this basket been in", not "how deep a hole was the client in".

Chosen deliberately, on request, and labelled rather than implied.

⚠⚠ DAILY BY DEFAULT — THE OPPOSITE OF THE OTHER RISK VIEWS, AND FOR A REASON THAT ONLY APPLIES HERE.
Tracking error, correlation and beta default to weekly because they compare TWO series whose closes
are hours apart (the tracker closes 16:30 London, a US holding 21:00), and the mismatch biases every
one of them. A drawdown compares a series with ITSELF, so that bias does not exist — and coarsening
the cadence does real damage in the other direction: a dip that recovers inside the period is
invisible. Monthly MDD is therefore STRUCTURALLY SHALLOWER, by percentage points, not noise.

⚠ SO ALL THREE CADENCES ARE MEASURED IN ONE REQUEST and returned together. The difference between
them is the thing this view has to be honest about, and a reader cannot compare numbers they have to
re-request one at a time. One price load, three bucketings — `build_paired_series(preloaded=…)`.

⚠ AND THE PERCENTAGE IS THE LEAST USEFUL PART. "−31.4%" is one number; "peaked 19 Feb 2025, bottomed
7 Apr 2025 after 33 trading days, recovered 12 Aug 2025 after another 91" is a conversation. Peak,
trough, recovery and both durations are returned for every drawdown reported.
"""
from __future__ import annotations

from routers._tracking_error import PERIODS, SeriesError, build_paired_series

#: How many of the deepest drawdowns to describe, not just the worst.
#:
#: ⚠ ONE NUMBER HIDES WHETHER IT WAS A PATTERN OR AN EVENT. A book with one −30% and nothing else is
#: a different risk from one with four −25%s, and the max is identical.
_TOP_N = 5


def drawdown_episodes(returns: list[float], dates: list[str | None]) -> list[dict]:
    """Every peak→trough→recovery episode in the series, deepest first.

    ⚠⚠ AN EPISODE ENDS WHEN THE WEALTH INDEX REGAINS ITS OLD PEAK, NOT WHEN IT TURNS UP. A 40% fall
    that bounces 5% and then falls further is ONE drawdown, not two — splitting on direction would
    report a set of shallow dips and no crash. So the peak only advances once the previous high is
    exceeded, which is exactly what `Mₜ = max_{s≤t} Wₛ` says.

    ⚠ THE LAST EPISODE MAY BE OPEN. A book below its high water mark right now has a drawdown with
    no recovery date, and inventing one (today, or the last observation) would report a recovery
    that has not happened. `recovered` is False and `recovery_date` is None there.
    """
    if not returns:
        return []

    episodes: list[dict] = []
    w = 1.0
    peak = 1.0
    peak_i = 0
    # `cur` is the open episode, or None while at a high water mark.
    cur: dict | None = None

    for i, r in enumerate(returns):
        w *= 1.0 + r
        if w >= peak:
            # ⚠ `>=`, so a flat return at the peak closes an episode rather than leaving it open
            # for ever on a series that recovers to exactly its old high.
            if cur is not None:
                cur["recovered"] = True
                cur["recovery_index"] = i
                cur["recovery_date"] = dates[i] if i < len(dates) else None
                episodes.append(cur)
                cur = None
            peak, peak_i = w, i
            continue
        dd = w / peak - 1.0
        if cur is None:
            cur = {"peak_index": peak_i,
                   "peak_date": dates[peak_i] if peak_i < len(dates) else None,
                   "depth_pct": dd * 100.0,
                   "trough_index": i,
                   "trough_date": dates[i] if i < len(dates) else None,
                   "recovered": False, "recovery_index": None, "recovery_date": None}
        elif dd < cur["depth_pct"] / 100.0:
            cur["depth_pct"] = dd * 100.0
            cur["trough_index"] = i
            cur["trough_date"] = dates[i] if i < len(dates) else None

    if cur is not None:
        episodes.append(cur)

    for e in episodes:
        # Periods, not days — the caller names the cadence beside them so "33" is never ambiguous.
        e["decline_periods"] = e["trough_index"] - e["peak_index"]
        e["recovery_periods"] = (None if e["recovery_index"] is None
                                 else e["recovery_index"] - e["trough_index"])
        e["total_periods"] = (None if e["recovery_index"] is None
                              else e["recovery_index"] - e["peak_index"])
    episodes.sort(key=lambda e: e["depth_pct"])
    return episodes


def _mdd(returns: list[float]) -> float | None:
    """`min DDₜ` in percent, or None on an empty series. The formula, with nothing else in it."""
    if not returns:
        return None
    w = peak = 1.0
    worst = 0.0
    for r in returns:
        w *= 1.0 + r
        peak = max(peak, w)
        worst = min(worst, w / peak - 1.0)
    return worst * 100.0


def compute_drawdown(holdings: list[dict], benchmark: str,
                     frequency: str = "daily", years: int = 5) -> dict:
    """MDD of the reconstructed sleeve, its episodes with dates, and the cadence comparison."""
    from routers._airs_portfolio_analysis import _daily_eur  # noqa: PLC0415
    from routers._asset_financials import _BENCHMARK_RISK_ETF  # noqa: PLC0415

    freq = frequency if frequency in PERIODS else "daily"

    # ⚠ ONE LOAD, THREE BUCKETINGS. The first `build_paired_series` call would do the load itself;
    # doing it here and handing it over means the cadence comparison costs no extra round trips.
    bench_isin = _BENCHMARK_RISK_ETF.get((benchmark or "").upper())
    preloaded = None
    if bench_isin:
        isins = sorted({(h.get("isin") or "").strip().upper() for h in holdings
                        if not h.get("is_fund") and (h.get("isin") or "").strip()})
        if isins:
            try:
                preloaded = _daily_eur([*isins, bench_isin], years)
            except Exception:  # noqa: BLE001 — fall through; the builder reports the real reason
                preloaded = None

    try:
        built = build_paired_series(holdings, benchmark, freq, years, preloaded=preloaded)
    except SeriesError as e:
        return {"available": False, "benchmark": benchmark, "frequency": freq, "reason": e.reason}

    episodes = drawdown_episodes(built["portfolio"], built["obs_dates"])
    worst = episodes[0] if episodes else None

    # ⚠ THE SAME MEASUREMENT AT EVERY CADENCE, so the understatement is visible rather than
    # asserted. A `None` entry means that cadence had too little overlap to measure — reported as
    # absent, never silently omitted from the comparison.
    by_freq: dict[str, float | None] = {}
    for f in PERIODS:
        if f == freq:
            by_freq[f] = _mdd(built["portfolio"])
            continue
        try:
            other = build_paired_series(holdings, benchmark, f, years,
                                        preloaded=built.get("series"))
            by_freq[f] = _mdd(other["portfolio"])
        except SeriesError:
            by_freq[f] = None

    return {
        "available": True,
        "benchmark": benchmark,
        "frequency": freq,
        "periods_per_year": built["periods_per_year"],
        "observations": len(built["portfolio"]),
        "years": years,

        "max_drawdown_pct": _mdd(built["portfolio"]),
        "benchmark_max_drawdown_pct": _mdd(built["benchmark"]),
        # ⚠ CURRENT STATE, because "worst ever −31%" and "down 28% right now" are very different
        # conversations and the second is the one the client is having.
        "current_drawdown_pct": (None if not built["portfolio"]
                                 else _current_dd(built["portfolio"])),
        "in_drawdown": bool(worst and not worst["recovered"]),

        "worst": worst,
        "episodes": episodes[:_TOP_N],
        "episodes_total": len(episodes),

        # ⚠ THE HONESTY REQUIREMENT: the same number at all three cadences, measured not claimed.
        "by_frequency": by_freq,

        "priced_holdings": built["priced"],
        "total_holdings": built["total"],
    }


def _current_dd(returns: list[float]) -> float:
    """How far below its own high water mark the series ends. 0.0 when it ends at a new high."""
    w = peak = 1.0
    for r in returns:
        w *= 1.0 + r
        peak = max(peak, w)
    return (w / peak - 1.0) * 100.0
