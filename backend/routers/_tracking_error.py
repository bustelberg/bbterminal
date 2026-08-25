"""Tracking error — the volatility of the ACTIVE RETURN, annualised.

    aₜ = Rₜᵖ − Rₜᵇ          TE = √( 1/(T−1) · Σ (aₜ − ā)² ) · √f

The active return is the difference itself; the tracking error is its spread. Two different
numbers, and this module returns both so they cannot be confused for one another.

⚠⚠ THIS IS EX-POST (REALISED) TE, AND THE DISTINCTION IS NOT PEDANTRY. The other definition is
ex-ante — `√(wₐᵀ Σ wₐ)` from a covariance matrix and the vector of active weights — which is a
FORECAST, needs a risk model we do not have, and routinely disagrees with this one by a wide margin
(a concentrated book that happened to move with its index has low realised TE and high predicted
TE). Nothing on the panel may be labelled just "tracking error"; it says "realised".

⚠⚠ ā IS SUBTRACTED. Some providers do not — they report √(Σaₜ²/T)·√f, which is the same number only
when the book exactly matched the index on average, and larger otherwise. Both are defensible; what
is not defensible is being unsure which one a figure is. This one goes through `annualized_stats`,
already THE definition of volatility in this codebase (`std(r, ddof=1)·√ppy`), so TE and every vol
on the screen are computed by one function. T−1 is Bessel: these are samples, not populations.

⚠⚠ WEEKLY BY DEFAULT, AND THAT IS THE SAME MEASUREMENT THE BETA COLUMN IS BUILT ON — not a
convention borrowed for tidiness. The benchmark trackers are LONDON-listed (ISAC.L, 0KZC.L) and
close at 16:30 London; a US holding closes at 21:00, so half its trading day lands in the next
benchmark bar. That is non-synchronous trading, and it does not cancel: measured on this book,
Microsoft vs ACWI reads corr 0.38 daily against 0.50 weekly.

⚠ AND IT BIASES TE UPWARD, THE OPPOSITE DIRECTION FROM BETA. `var(a) = var(p) + var(b) − 2cov(p,b)`,
so an artificially LOW covariance makes the active return look more volatile than it is. Daily is
offered because it is what people ask for, and it is labelled as inflated rather than quietly
served.

⚠ THE PORTFOLIO SERIES IS SYNTHETIC: today's stock sleeve at today's weights, carried backwards.
It is NOT the book's realised history — a name bought in March contributes its January return here.
That is the standard "tracking error of the portfolio as it stands", it is the only series that
describes the SAME portfolio the active-share tile beside it describes, and the panel says so. The
book's true realised active return would need its weight history, which `airs_performance` carries
only for the whole book (funds and cash included) — a different portfolio again.
"""
from __future__ import annotations

#: Observations per year, per cadence — the `f` in the formula.
#: ⚠ 52, NOT 52.18. The weekly series is built by ISO week (`_by_week`), so a year contributes 52
#: or 53 buckets and the annualisation constant has to be the bucket count, not the calendar.
PERIODS: dict[str, float] = {"daily": 252.0, "weekly": 52.0, "monthly": 12.0}

#: The least data a cadence may report a TE from. ⚠ A TE over eight weekly observations is not a
#: small sample, it is a number with no meaning — the Bessel correction does not rescue n=8.
MIN_OBS: dict[str, int] = {"daily": 120, "weekly": 52, "monthly": 24}


def _bucket_end_dates(series: list[tuple[str, float]], freq: str) -> dict:
    """`{period key: the last calendar date in that period}`.

    ⚠⚠ A DRAWDOWN IS USELESS WITHOUT ITS DATES, and a bucket key is not one. `(2026, 12)` is
    an ISO week, `"2026-03"` a month; a client conversation needs "peaked 14 February, bottomed
    23 April, recovered 8 August". So the same walk that buckets the prices records which day each
    bucket actually ended on — derived from the series rather than from the calendar, because the
    last trading day of a week is not Friday when Friday was a holiday.
    """
    out: dict = {}
    for d, v in series:
        if v is None or v <= 0:
            continue
        out[_bucket_key(d, freq)] = d       # later dates overwrite → the period's last date
    return out


def _bucket_key(d: str, freq: str):
    """The period one date falls in. Split out so bucketing and dating cannot drift apart."""
    from datetime import date as _date  # noqa: PLC0415

    if freq == "daily":
        return d
    if freq == "monthly":
        return d[:7]
    y, w, _ = _date.fromisoformat(d).isocalendar()
    return (y, w)


def _bucket(series: list[tuple[str, float]], freq: str) -> dict:
    """`{period key: last close in that period}` — the cadence the returns are measured on.

    ⚠ THE LAST CLOSE IN THE PERIOD, NEVER "THE FRIDAY" OR "THE 31ST". A market shut on the last
    weekday still had a week; keying on the weekday would drop it for one series and keep it for
    the other, which is precisely the misalignment a weekly basis exists to remove. Same rule as
    `_airs_portfolio_analysis._by_week`, which the beta column uses.
    """
    out: dict = {}
    for d, v in series:
        if v is None or v <= 0:
            continue
        out[_bucket_key(d, freq)] = v      # later dates overwrite → the period's last close
    return out


class SeriesError(Exception):
    """No usable pair of series, with the reason already phrased for the reader."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def build_paired_series(holdings: list[dict], benchmark: str,
                        frequency: str = "weekly", years: int = 5,
                        preloaded: dict | None = None) -> dict:
    """The portfolio and benchmark return series, on ONE aligned period grid.

    ⚠⚠ SHARED BY TRACKING ERROR AND CORRELATION SO THE IDENTITY BETWEEN THEM HOLDS ON SCREEN.
    `σₐ² = σₚ² + σᵇ² − 2ρσₚσᵇ` is only checkable if both views measure the same periods, the
    same holdings and the same renormalisation — two builders agreeing today is two builders that
    can stop agreeing, and the disagreement would look like a maths error in whichever panel the
    reader happened to distrust.

    Returns `{keys, active, portfolio, benchmark, per_holding, weight_used, priced, total}` — the
    per-holding returns ride along because the correlation MATRIX needs them, and rebuilding them
    from a second pass over the same buckets is the same trap one level down.

    Raises `SeriesError` with a reader-ready reason rather than returning a sentinel: every caller
    turns it into the same `{available: False, reason}` shape.
    """
    from routers._airs_portfolio_analysis import _daily_eur  # noqa: PLC0415
    from routers._asset_financials import _BENCHMARK_RISK_ETF  # noqa: PLC0415

    freq = frequency if frequency in PERIODS else "weekly"

    bench_isin = _BENCHMARK_RISK_ETF.get((benchmark or "").upper())
    if not bench_isin:
        raise SeriesError(
            f"{benchmark} has no investable tracker in our price world, so there is no series to "
            f"measure against. Available: {', '.join(sorted(_BENCHMARK_RISK_ETF))}.")

    # ⚠ THE SLEEVE, EXACTLY AS ACTIVE SHARE DEFINES IT — individual stocks with an ISIN, renormalised
    # to 1. Funds and cash are dropped, not zero-weighted; see `_active_share`.
    stocks = [h for h in holdings
              if not h.get("is_fund")
              and (h.get("isin") or "").strip()
              and float(h.get("weight_pct") or 0) > 0]
    total_w = sum(float(h["weight_pct"]) for h in stocks)
    if total_w <= 0:
        raise SeriesError("This book holds no individual stocks with an ISIN to compare.")

    names: dict[str, str] = {}
    weight: dict[str, float] = {}
    for h in stocks:
        k = (h["isin"] or "").strip().upper()
        weight[k] = weight.get(k, 0.0) + float(h["weight_pct"]) / total_w
        if h.get("name"):
            names.setdefault(k, str(h["name"]))
    isins = sorted(weight)

    # ⚠ `preloaded` LETS ONE PRICE LOAD SERVE SEVERAL CADENCES. The load is the expensive part
    # (executions + closes + FX for every holding); re-bucketing it is free. The drawdown view
    # measures all three frequencies in one request precisely because the difference between them
    # is the thing it has to be honest about — see `_portfolio_drawdown`.
    if preloaded is not None:
        series = preloaded
    else:
        try:
            series = _daily_eur([*isins, bench_isin], years)
        except Exception as e:  # noqa: BLE001 — a failed panel must not raise into the modal
            raise SeriesError(
                f"The price series could not be loaded ({type(e).__name__}).") from e

    b_buckets = _bucket(series.get(bench_isin) or [], freq)
    p_buckets = {i: _bucket(series.get(i) or [], freq) for i in isins}
    priced = {i for i in isins if len(p_buckets[i]) >= 2}
    if not priced or len(b_buckets) < 2:
        raise SeriesError("Too little price history to measure against.")

    # ⚠⚠ THE INTERSECTION OF PERIODS, NEVER A ZIP. A Stockholm listing and a London-traded ETF do
    # not share a calendar, so pairing two return series by POSITION offsets them from the first
    # mismatched holiday onward and yields a perfectly plausible figure computed against the wrong
    # days. Same rule the beta column follows.
    keys = sorted(set(b_buckets) & set().union(*(set(p_buckets[i]) for i in priced)))
    if len(keys) < MIN_OBS[freq] + 1:
        raise SeriesError(
            f"Only {max(0, len(keys) - 1)} {freq} observations overlap with {benchmark}; "
            f"{MIN_OBS[freq]} is the floor.")

    active: list[float] = []
    port: list[float] = []
    bench_r: list[float] = []
    weight_used: list[float] = []
    # ⚠ PER HOLDING, ALIGNED TO `keys` WITH `None` FOR A PERIOD IT HAS NO PRICE IN — never 0.0.
    # The correlation matrix pairs each column on the periods BOTH have; a zero would be read as
    # "this stock did not move", which is a strong and false statement about a stock that had not
    # listed yet.
    per_holding: dict[str, list[float | None]] = {i: [] for i in priced}
    # ⚠⚠ THE PERIOD EACH OBSERVATION ACTUALLY IS, because the three return lists are NOT aligned
    # to `keys[1:]`. A step whose benchmark bucket is missing, or where no holding had both ends,
    # is skipped — so indexing back into `keys` to date observation `i` silently drifts by however
    # many were dropped. A drawdown reports DATES, so this is the difference between "bottomed
    # 23 April" and a plausible wrong day.
    obs_keys: list = []

    for prev, cur in zip(keys, keys[1:]):
        if prev not in b_buckets or cur not in b_buckets:
            for i in priced:
                per_holding[i].append(None)
            continue
        rb = b_buckets[cur] / b_buckets[prev] - 1.0
        # ⚠ RENORMALISED OVER THE HOLDINGS THAT HAVE BOTH ENDS OF THIS STEP, period by period. A
        # name that listed two years ago simply is not in the earlier steps, and carrying it at
        # zero return would damp the book's measured volatility — the flattering direction. What is
        # reported instead is how much weight each step actually spoke for.
        num = den = 0.0
        for i in priced:
            pb = p_buckets[i]
            if prev in pb and cur in pb and pb[prev] > 0:
                r = pb[cur] / pb[prev] - 1.0
                per_holding[i].append(r)
                w = weight.get(i, 0.0)
                num += w * r
                den += w
            else:
                per_holding[i].append(None)
        if den <= 0:
            continue
        rp = num / den
        port.append(rp)
        bench_r.append(rb)
        active.append(rp - rb)
        weight_used.append(den)
        obs_keys.append(cur)

    if len(active) < MIN_OBS[freq]:
        raise SeriesError(
            f"Only {len(active)} {freq} observations could be paired with {benchmark}; "
            f"{MIN_OBS[freq]} is the floor.")

    # The real calendar date each observation ended on — from the BENCHMARK series, which spans
    # every key in `keys` by construction (they are its own buckets, intersected).
    ends = _bucket_end_dates(series.get(bench_isin) or [], freq)
    return {"frequency": freq, "periods_per_year": PERIODS[freq], "keys": keys,
            "active": active, "portfolio": port, "benchmark": bench_r,
            "obs_keys": obs_keys, "obs_dates": [ends.get(k) for k in obs_keys],
            "per_holding": per_holding, "weight_used": weight_used,
            "names": names, "weight": weight, "series": series,
            "priced": len(priced), "total": len(isins), "benchmark_isin": bench_isin}


def compute_tracking_error(holdings: list[dict], benchmark: str,
                           frequency: str = "weekly", years: int = 5) -> dict:
    """Realised TE of the book's individual stocks against `benchmark`'s investable tracker.

    `holdings` are the rows the Analyse modal is showing — the same input, and the same stocks-only
    renormalisation, as `compute_active_share`, so the two tiles describe ONE portfolio.
    """
    import numpy as np  # noqa: PLC0415

    from momentum.diversification import annualized_stats  # noqa: PLC0415

    freq = frequency if frequency in PERIODS else "weekly"
    try:
        built = build_paired_series(holdings, benchmark, freq, years)
    except SeriesError as e:
        return {"available": False, "benchmark": benchmark, "frequency": freq, "reason": e.reason}

    active = built["active"]
    weight_used = built["weight_used"]
    ppy = built["periods_per_year"]

    a = np.asarray(active, dtype=float)
    # ⚠ THROUGH `annualized_stats` — the ONE definition of volatility here, so TE and the vol
    # column beside it cannot be two different formulas. It is `std(ddof=1) × √ppy`, i.e. Bessel,
    # with ā subtracted.
    st = annualized_stats(a.tolist(), periods_per_year=ppy)
    te = st.ann_vol
    mean_active = float(a.mean())

    return {
        "available": True,
        "benchmark": benchmark,
        "frequency": freq,
        "periods_per_year": ppy,
        "observations": len(active),
        "years": years,
        # ⚠⚠ THE WINDOW THE RETURNS ACTUALLY COVER, not `years` back from today. The card used to
        # say "trailing window — as it stands today", which is an assumption where a date belongs:
        # a holding that listed two years ago shortens the paired grid, and a stale price series
        # ends it early. `obs_dates` is each period's own last trading day, so these are real dates
        # the reader can check against a chart.
        # ⚠ FILTERED — a bucket whose end date could not be resolved contributes None, and
        # min()/max() over a list containing one would raise rather than report the gap.
        "window_from": (dates[0] if (dates := sorted(d for d in built["obs_dates"] if d)) else None),
        "window_to": (dates[-1] if dates else None),
        "tracking_error_pct": None if te is None else te * 100.0,
        # ⚠ THE ACTIVE RETURN ITSELF, because it is the quantity TE is the spread OF and reporting
        # one without the other is what makes the two get confused. Mean per period, and the same
        # figure annualised geometrically — a book can have a large TE and no active return at all.
        "mean_active_per_period_pct": mean_active * 100.0,
        "active_return_ann_pct": (float(np.prod(1.0 + a) ** (ppy / len(a)) - 1.0) * 100.0
                                  if np.all(1.0 + a > 0) else None),
        # ⚠ TE IS A DENOMINATOR HERE, so a near-zero one is refused rather than printed as a huge
        # ratio. The information ratio is the active return per unit of the risk taken to get it.
        "information_ratio": (
            None if not te or te <= 1e-9 or not np.all(1.0 + a > 0)
            else float(np.prod(1.0 + a) ** (ppy / len(a)) - 1.0) / te),
        # How much of the sleeve the average observation actually spoke for — see the renormalisation
        # note above. 100% means every holding had a price at both ends of every step.
        "avg_weight_covered_pct": float(np.mean(weight_used)) * 100.0,
        "priced_holdings": built["priced"],
        "total_holdings": built["total"],
        "benchmark_isin": built["benchmark_isin"],
        # ⚠ THE CADENCE'S OWN BIAS, CARRIED WITH THE NUMBER rather than left in a doc. See the
        # module header: non-synchronous closes inflate the spread of a DIFFERENCE.
        "cadence_note": (
            "Daily closes are not synchronous — the tracker closes at 16:30 London, a US holding at "
            "21:00 — which lowers the measured covariance and therefore INFLATES this figure. "
            "Weekly spans the gap." if freq == "daily" else None),
    }
