"""Tracking error — the volatility of the ACTIVE RETURN, annualised.

    aₜ = Rₜᵖ − Rₜᵇ          TE = √( 1/(T−1) · Σ (aₜ − ā)² ) · √f

The active return is the difference itself; the tracking error is its spread. Two different
numbers, and this module returns both so they cannot be confused for one another.

 THIS IS EX-POST (REALISED) TE, AND THE DISTINCTION IS NOT PEDANTRY. The other definition is
ex-ante — `√(wₐᵀ Σ wₐ)` from a covariance matrix and the vector of active weights — which is a
FORECAST, needs a risk model we do not have, and routinely disagrees with this one by a wide margin
(a concentrated book that happened to move with its index has low realised TE and high predicted
TE). Nothing on the panel may be labelled just "tracking error"; it says "realised".

 ā IS SUBTRACTED. Some providers do not — they report √(Σaₜ²/T)·√f, which is the same number only
when the book exactly matched the index on average, and larger otherwise. Both are defensible; what
is not defensible is being unsure which one a figure is. This one goes through `annualized_stats`,
already THE definition of volatility in this codebase (`std(r, ddof=1)·√ppy`), so TE and every vol
on the screen are computed by one function. T−1 is Bessel: these are samples, not populations.

 WEEKLY BY DEFAULT, AND THAT IS THE SAME MEASUREMENT THE BETA COLUMN IS BUILT ON — not a
convention borrowed for tidiness. The benchmark trackers are LONDON-listed (ISAC.L, 0KZC.L) and
close at 16:30 London; a US holding closes at 21:00, so half its trading day lands in the next
benchmark bar. That is non-synchronous trading, and it does not cancel: measured on this book,
Microsoft vs ACWI reads corr 0.38 daily against 0.50 weekly.

 AND IT BIASES TE UPWARD, THE OPPOSITE DIRECTION FROM BETA. `var(a) = var(p) + var(b) − 2cov(p,b)`,
so an artificially LOW covariance makes the active return look more volatile than it is. Daily is
offered because it is what people ask for, and it is labelled as inflated rather than quietly
served.

 THE PORTFOLIO SERIES IS SYNTHETIC: today's selected sleeve at today's weights, carried backwards.
It is NOT the book's realised history — a name bought in March contributes its January return here.
That is the standard "tracking error of the portfolio as it stands", it is the only series that
describes the SAME portfolio the active-share tile beside it describes, and the panel says so. The
book's true realised active return would need its weight history, which `airs_performance` carries
only for the whole book (funds and cash included) — a different portfolio again.
"""
from __future__ import annotations

import re
from functools import lru_cache
from datetime import date
import time


@lru_cache(maxsize=16)
def _risk_prices(isins: tuple[str, ...], years: int, day: str, generation: int, loader):
    """One price snapshot shared by all historical risk views for five minutes."""
    return loader(list(isins), years)


_FIXED_INCOME_NAME = re.compile(r"\b(perp|bond|obligat|coupon|treasur|gilt|bund|govie|"
                                r"sovereign|floating\s*rate|frn|high\s*yield|fixed\s*income)\b"
                                r"|\d[\.,]?\d*\s*%", re.I)


def is_direct_equity_stock(holding: dict) -> bool:
    """Direct operating-company equity only; certificates with a coupon are fixed income."""
    return (not holding.get("is_fund")
            and holding.get("bucket") in (None, "", "Equity")
            and not _FIXED_INCOME_NAME.search(str(holding.get("name") or "")))


def is_risk_profile_holding(holding: dict) -> bool:
    """A priced Stocks/Bonds policy constituent, excluding cash and alternatives.

    Unlike Active Share, a risk-profile return is deliberately a whole policy-portfolio measure:
    equity funds and bonds belong in it.  The policy benchmark itself has only these two sleeves,
    so admitting alternatives or cash here would compare unlike portfolios.
    """
    return (holding.get("bucket") in {"Equity", "ETF Equity", "Bonds"}
            and bool((holding.get("isin") or "").strip())
            and float(holding.get("weight_pct") or 0) > 0)

#: Observations per year, per cadence — the `f` in the formula.
#:  52, NOT 52.18. The weekly series is built by ISO week (`_by_week`), so a year contributes 52
#: or 53 buckets and the annualisation constant has to be the bucket count, not the calendar.
PERIODS: dict[str, float] = {"daily": 252.0, "weekly": 52.0, "monthly": 12.0}

#: The least data a cadence may report a TE from.  A TE over eight weekly observations is not a
#: small sample, it is a number with no meaning — the Bessel correction does not rescue n=8.
MIN_OBS: dict[str, int] = {"daily": 120, "weekly": 52, "monthly": 24}
MIN_SHORT_HISTORY_MONTHS = 12
# A newly launched constituent can have only one completed monthly return while already having a
# useful run of actual daily closes. Ten paired returns is deliberately a *preliminary* view, never
# a substitute for the normal 120-day realised-TE standard.
MIN_PRELIMINARY_DAILY_OBS = 10


def _repair_risk_inputs(isins: set[str], years: int) -> None:
    """One bounded repair attempt before reporting a genuinely unavailable common window."""
    from asset_pipeline.price_refresh import repair_price_histories  # noqa: PLC0415

    repair_price_histories(isins, years=years)


def _monthly_common_suffix(keys: set[str]) -> list[str]:
    """Latest uninterrupted monthly run shared by every required series."""
    if not keys:
        return []
    current = max(keys)
    out = [current]
    while True:
        year, month = map(int, current.split("-"))
        previous = f"{year - 1:04d}-12" if month == 1 else f"{year:04d}-{month - 1:02d}"
        if previous not in keys:
            return list(reversed(out))
        out.append(previous)
        current = previous


def _bucket_end_dates(series: list[tuple[str, float]], freq: str) -> dict:
    """`{period key: the last calendar date in that period}`.

     A DRAWDOWN IS USELESS WITHOUT ITS DATES, and a bucket key is not one. `(2026, 12)` is
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

     THE LAST CLOSE IN THE PERIOD, NEVER "THE FRIDAY" OR "THE 31ST". A market shut on the last
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


def assess_risk_data_quality(holdings: list[dict], benchmark: str,
                             years: int = 5,
                             benchmark_weights: dict[str, float] | None = None) -> dict:
    """The reusable admission gate for risk metrics over the exact common history.

    Keeping this separate from a particular statistic makes it callable by a production monitor:
    the answer is whether the input data supports *any* risk calculation, before a chart chooses a
    formula.  Monthly coverage is intentional—daily markets have different holiday calendars.
    """
    try:
        built = build_paired_series(
            holdings, benchmark, "monthly", years,
            require_complete_holdings=True,
            benchmark_weights=benchmark_weights,
        )
    except SeriesError as e:
        return {"available": False, "status": "incomplete", "benchmark": benchmark,
                "reason": e.reason, "years": years}
    dates = sorted(d for d in built["obs_dates"] if d)
    return {
        "available": True,
        "status": "ready",
        "benchmark": built.get("benchmark_label", benchmark),
        "reason": None,
        "years": years,
        "window_from": dates[0] if dates else None,
        "window_to": dates[-1] if dates else None,
        "priced_holdings": built["priced"],
        "total_holdings": built["total"],
        "benchmark_isin": built["benchmark_isin"],
        "history_limited": built.get("history_limited", False),
    }


def build_paired_series(holdings: list[dict], benchmark: str,
                        frequency: str = "weekly", years: int = 5,
                        preloaded: dict | None = None,
                        require_complete_holdings: bool = False,
                        benchmark_weights: dict[str, float] | None = None,
                        minimum_observations: int | None = None,
                        _repair_attempted: bool = False) -> dict:
    """The portfolio and benchmark return series, on ONE aligned period grid.

     SHARED BY TRACKING ERROR AND CORRELATION SO THE IDENTITY BETWEEN THEM HOLDS ON SCREEN.
    `σₐ² = σₚ² + σᵇ² − 2ρσₚσᵇ` is only checkable if both views measure the same periods, the
    same holdings and the same renormalisation — two builders agreeing today is two builders that
    can stop agreeing, and the disagreement would look like a maths error in whichever panel the
    reader happened to distrust.

    Returns `{keys, active, portfolio, benchmark, per_holding, weight_used, priced, total}` — the
    per-holding returns ride along because the correlation MATRIX needs them, and rebuilding them
    from a second pass over the same buckets is the same trap one level down.

    `require_complete_holdings` preserves the displayed sleeve. A missing historical FX rate must
    not make one holding disappear from early months: instead monthly calculations use the latest
    continuous period shared by every current holding and the benchmark, and flag the shorter
    window. Daily/weekly measures retain their normal exchange-calendar handling.

    Raises `SeriesError` with a reader-ready reason rather than returning a sentinel: every caller
    turns it into the same `{available: False, reason}` shape.
    """
    from routers._airs_portfolio_analysis import _daily_eur  # noqa: PLC0415
    from routers._asset_financials import _BENCHMARK_RISK_ETF  # noqa: PLC0415

    freq = frequency if frequency in PERIODS else "weekly"

    bench_isin = _BENCHMARK_RISK_ETF.get((benchmark or "").upper())
    policy_weights = (benchmark_weights or {})
    is_policy_benchmark = bool(policy_weights)
    if is_policy_benchmark:
        try:
            stocks_weight = float(policy_weights.get("Stocks", 0))
            bonds_weight = float(policy_weights.get("Bonds", 0))
        except (TypeError, ValueError):
            raise SeriesError("The risk-profile benchmark weights are invalid.")
        if (stocks_weight < 0 or bonds_weight < 0
                or abs(stocks_weight + bonds_weight - 100.0) > 0.01):
            raise SeriesError("The risk-profile benchmark must contain Stocks and Bonds totaling 100%.")
        benchmark_isins = {
            "ACWI": _BENCHMARK_RISK_ETF["ACWI"],
            "BONDS": _BENCHMARK_RISK_ETF["BONDS"],
        }
        active_benchmark_names = [name for name, weight in (("ACWI", stocks_weight), ("BONDS", bonds_weight))
                                  if weight > 0]
        if not active_benchmark_names:
            raise SeriesError("The risk-profile benchmark has no positive-weight sleeve.")
        benchmark_label = " + ".join(
            f"{(stocks_weight if name == 'ACWI' else bonds_weight):g}% "
            f"{'AGGH.AS' if name == 'BONDS' else name}"
            for name in active_benchmark_names)
    elif not bench_isin:
        raise SeriesError(
            f"{benchmark} has no investable tracker in our price world, so there is no series to "
            f"measure against. Available: {', '.join(sorted(_BENCHMARK_RISK_ETF))}.")
    else:
        benchmark_isins = {benchmark.upper(): bench_isin}
        active_benchmark_names = [benchmark.upper()]
        benchmark_label = benchmark

    # Generic baskets retain Active Share's individual-stock sleeve. Risk-profile portfolios
    # instead use their investable Stocks/Bonds sleeve, which is the only like-for-like companion
    # to their fixed ACWI/AGGH policy benchmark.
    total_book_w = sum(max(0.0, float(h.get("weight_pct") or 0)) for h in holdings)
    stocks = [h for h in holdings
              if (is_risk_profile_holding(h) if is_policy_benchmark else is_direct_equity_stock(h))
              and (h.get("isin") or "").strip()
              and float(h.get("weight_pct") or 0) > 0]
    stocks_w = sum(float(h["weight_pct"]) for h in stocks)
    if stocks_w <= 0 or total_book_w <= 0:
        raise SeriesError("This book holds no priced Stocks or Bonds constituent to compare."
                          if is_policy_benchmark
                          else "This book holds no individual stocks with an ISIN to compare.")

    names: dict[str, str] = {}
    weight: dict[str, float] = {}
    for h in stocks:
        k = (h["isin"] or "").strip().upper()
        # A profile comparison is a return on the investable Stocks/Bonds sleeve. Cash and
        # alternatives are intentionally outside that policy, so they are not a zero-return drag.
        denominator = stocks_w if is_policy_benchmark else total_book_w
        weight[k] = weight.get(k, 0.0) + float(h["weight_pct"]) / denominator
        if h.get("name"):
            names.setdefault(k, str(h["name"]))
    isins = sorted(weight)

    #  `preloaded` LETS ONE PRICE LOAD SERVE SEVERAL CADENCES. The load is the expensive part
    # (executions + closes + FX for every holding); re-bucketing it is free. The drawdown view
    # measures all three frequencies in one request precisely because the difference between them
    # is the thing it has to be honest about — see `_portfolio_drawdown`.
    if preloaded is not None:
        series = preloaded
    else:
        try:
            series = _risk_prices(tuple(sorted([*isins, *benchmark_isins.values()])), years,
                                  date.today().isoformat(), int(time.monotonic() // 300), _daily_eur)
        except Exception as e:  # noqa: BLE001 — a failed panel must not raise into the modal
            raise SeriesError(
                f"The price series could not be loaded ({type(e).__name__}).") from e

    # All measures use identical endpoints, anchored to the latest benchmark close. Retain the
    # last observation at/before the five-year opening date as the return's starting price.
    benchmark_prices = series.get(next(iter(benchmark_isins.values()))) or []
    opening_marks: dict[str, bool] = {}
    if benchmark_prices:
        end = benchmark_prices[-1][0]
        end_date = date.fromisoformat(end)
        try:
            start = end_date.replace(year=end_date.year - years).isoformat()
        except ValueError:
            start = end_date.replace(year=end_date.year - years, day=28).isoformat()
        # Keep this fact before clipping. A series that begins two years into the requested
        # window can still provide the 24 monthly observations needed for risk statistics, but it
        # cannot support a number labelled "five-year annualised return".
        opening_marks = {
            isin: any(row[0] <= start for row in prices)
            for isin, prices in series.items()
        }
        clipped = {}
        for isin, prices in series.items():
            anchor = [row for row in prices if row[0] <= start]
            clipped[isin] = (anchor[-1:] if anchor else []) + [row for row in prices if start < row[0] <= end]
        series = clipped
    raw_buckets = {name: _bucket(series.get(isin) or [], freq)
                   for name, isin in benchmark_isins.items()}
    if is_policy_benchmark:
        common_benchmark_keys = sorted(set.intersection(*(set(raw_buckets[name]) for name in active_benchmark_names)))
        b_buckets: dict = {}
        if common_benchmark_keys:
            wealth = 1.0
            b_buckets[common_benchmark_keys[0]] = wealth
            for prev, cur in zip(common_benchmark_keys, common_benchmark_keys[1:]):
                r = sum((stocks_weight if name == "ACWI" else bonds_weight) / 100.0
                        * (raw_buckets[name][cur] / raw_buckets[name][prev] - 1.0)
                        for name in active_benchmark_names)
                wealth *= 1.0 + r
                b_buckets[cur] = wealth
    else:
        b_buckets = raw_buckets[benchmark.upper()]
    p_buckets = {i: _bucket(series.get(i) or [], freq) for i in isins}
    history_limited = False
    if require_complete_holdings:
        # Keep the exact sleeve intact by using only its latest continuous common history. A fund
        # that launched later shortens the window; it never becomes a synthetic pre-launch return.
        common = set(b_buckets)
        for isin in isins:
            common &= set(p_buckets[isin])
        suffix = _monthly_common_suffix(common) if freq == "monthly" else sorted(common)
        if not suffix:
            # A request may be the first thing to encounter a newly added holding. Repair its
            # mapped Yahoo and FX histories and retry once, rather than making the reader find a
            # separate admin refresh. `preloaded` is deliberately excluded: callers supplying a
            # snapshot own that data and a repair could not alter it.
            if preloaded is None and not _repair_attempted:
                _repair_risk_inputs({*isins, *benchmark_isins.values()}, years)
                return build_paired_series(
                    holdings, benchmark, frequency, years,
                    require_complete_holdings=require_complete_holdings,
                    benchmark_weights=benchmark_weights,
                    minimum_observations=minimum_observations,
                    _repair_attempted=True,
                )
            unavailable = [names.get(isin, isin) for isin in isins if not p_buckets[isin]]
            if unavailable:
                shown = ", ".join(unavailable[:3])
                more = "" if len(unavailable) <= 3 else f", and {len(unavailable) - 3} more"
                raise SeriesError(
                    "Automatic price and FX repair could not obtain a usable history for "
                    f"{shown}{more}. The exact portfolio cannot be calculated until those "
                    "holdings have an approved historical price source.")
            raise SeriesError("No common price and FX history exists for every current holding and "
                              f"{benchmark_label}.")
        # The benchmark can itself be the young series, so comparing only its keys would miss
        # the case where an older holding must be trimmed down to the benchmark's start.
        # Also flag a short history even if every series launched together after the requested
        # five-year opening date.
        history_limited = (
            any(set(prices) != set(suffix) for prices in p_buckets.values())
            or set(b_buckets) != set(suffix)
            or any(not opening_marks.get(isin, False)
                   for isin in [*isins, *benchmark_isins.values()])
        )
        b_buckets = {key: b_buckets[key] for key in suffix}
        p_buckets = {isin: {key: prices[key] for key in suffix}
                     for isin, prices in p_buckets.items()}
    priced = {i for i in isins if len(p_buckets[i]) >= 2}
    if not priced or len(b_buckets) < 2:
        missing_trackers = [
            ("AGGH.AS" if name == "BONDS" else name)
            for name in active_benchmark_names if len(raw_buckets.get(name, {})) < 2
        ]
        if missing_trackers:
            raise SeriesError(
                "No usable price history for the risk benchmark "
                f"{' + '.join(missing_trackers)}. Run Refresh to repair its stored Yahoo history.")
        missing_holdings = [names.get(isin, isin) for isin in isins if isin not in priced]
        raise SeriesError(
            "No usable price history for "
            f"{', '.join(missing_holdings[:3])}{' and more' if len(missing_holdings) > 3 else ''}.")

    #  The intersection of periods, never a zip. A Stockholm listing and a London-traded ETF do
    # not share a calendar, so pairing two return series by POSITION offsets them from the first
    # mismatched holiday onward and yields a perfectly plausible figure computed against the wrong
    # days. Same rule the beta column follows.
    keys = sorted(set(b_buckets) & set().union(*(set(p_buckets[i]) for i in priced)))
    required_observations = (minimum_observations if minimum_observations is not None else
                             (MIN_SHORT_HISTORY_MONTHS
                              if require_complete_holdings and history_limited and freq == "monthly"
                              else MIN_OBS[freq]))
    if required_observations < 2:
        raise ValueError("minimum_observations must be at least two")
    if len(keys) < required_observations + 1:
        raise SeriesError(
            f"Only {max(0, len(keys) - 1)} {freq} observations overlap with {benchmark}; "
            f"{required_observations} is the floor.")

    if require_complete_holdings:
        assert all(set(p_buckets[isin]) == set(b_buckets) for isin in isins)

    active: list[float] = []
    port: list[float] = []
    bench_r: list[float] = []
    benchmark_component_r: dict[str, list[float]] = {name: [] for name in active_benchmark_names}
    weight_used: list[float] = []
    #  Per holding, aligned to `keys` WITH `None` FOR A PERIOD IT HAS NO PRICE IN — never 0.0.
    # The correlation matrix pairs each column on the periods BOTH have; a zero would be read as
    # "this stock did not move", which is a strong and false statement about a stock that had not
    # listed yet.
    per_holding: dict[str, list[float | None]] = {i: [] for i in priced}
    #  The period each observation actually is, because the three return lists are NOT aligned
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
        #  Renormalised over the holdings that have both ends of this step, period by period. A
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
        # The unpriced and non-stock remainder stays in the AIRS denominator, as in Attribution.
        rp = num
        port.append(rp)
        bench_r.append(rb)
        for name in active_benchmark_names:
            benchmark_component_r[name].append(
                raw_buckets[name][cur] / raw_buckets[name][prev] - 1.0)
        active.append(rp - rb)
        weight_used.append(den)
        obs_keys.append(cur)

    if len(active) < required_observations:
        raise SeriesError(
            f"Only {len(active)} {freq} observations could be paired with {benchmark}; "
            f"{required_observations} is the floor.")

    # The real calendar date each observation ended on — from the BENCHMARK series, which spans
    # every key in `keys` by construction (they are its own buckets, intersected).
    ends = _bucket_end_dates(series.get(next(iter(benchmark_isins.values()))) or [], freq)
    component_end_dates = {
        name: _bucket_end_dates(series.get(isin) or [], freq)
        for name, isin in benchmark_isins.items()
    }
    return {"frequency": freq, "periods_per_year": PERIODS[freq], "keys": keys,
            "active": active, "portfolio": port, "benchmark": bench_r,
            "obs_keys": obs_keys, "obs_dates": [ends.get(k) for k in obs_keys],
            "per_holding": per_holding, "weight_used": weight_used,
            "names": names, "weight": weight, "series": series,
            "benchmark_component_returns": benchmark_component_r,
            "benchmark_component_weights": {
                name: ((stocks_weight if name == "ACWI" else bonds_weight)
                       if is_policy_benchmark else 100.0)
                for name in active_benchmark_names
            },
            "benchmark_component_isins": benchmark_isins,
            "benchmark_component_marks": {
                name: {
                    "start_date": component_end_dates[name].get(keys[0]),
                    "start_price_eur": raw_buckets[name][keys[0]],
                    "end_date": component_end_dates[name].get(keys[-1]),
                    "end_price_eur": raw_buckets[name][keys[-1]],
                }
                for name in active_benchmark_names
            },
            "priced": len(priced), "total": len(isins),
            "benchmark_isin": ", ".join(
                f"{'AGGH.AS' if name == 'BONDS' else name}: {benchmark_isins[name]}"
                for name in active_benchmark_names),
            "benchmark_label": benchmark_label, "history_limited": history_limited}


def _benchmark_history(built: dict, years: int) -> dict:
    """Benchmark-only history; young portfolio holdings must never shorten this window."""
    weights = built["benchmark_component_weights"]
    prices = {name: dict(built["series"].get(built["benchmark_component_isins"][name]) or [])
              for name in weights}
    common = sorted(set.intersection(*(set(rows) for rows in prices.values())))
    if len(common) < 2:
        return {"components": [], "cagr_pct": None}
    start, end = common[0], common[-1]
    elapsed = (date.fromisoformat(end) - date.fromisoformat(start)).days / 365.25
    if elapsed <= 0:
        return {"components": [], "cagr_pct": None}
    components = []
    for name, rows in prices.items():
        growth = rows[end] / rows[start]
        components.append({
            "name": "AGGH.AS" if name == "BONDS" else name,
            "isin": built["benchmark_component_isins"][name], "weight_pct": weights[name],
            "start_date": start, "end_date": end,
            "start_price_eur": rows[start], "end_price_eur": rows[end],
            "window_return_pct": (growth - 1) * 100,
            "annualized_return_pct": (growth ** (1 / elapsed) - 1) * 100,
        })
    wealth = 1.0
    for prev, cur in zip(common, common[1:]):
        wealth *= 1 + sum(weights[name] / 100 * (rows[cur] / rows[prev] - 1)
                          for name, rows in prices.items())
    return {"components": components, "cagr_pct": (wealth ** (1 / elapsed) - 1) * 100,
            "start_date": start, "end_date": end,
            "complete": elapsed >= years - 10 / 365.25}


def _current_weights_history(built: dict, years: int) -> dict:
    """Reconstruct available holdings, renormalising today's weights at each historical step."""
    from bisect import bisect_right

    benchmark = _benchmark_history(built, years)
    if not benchmark.get("complete"):
        return {"available": False, "reason": "Five-year benchmark history is incomplete."}
    start, end = benchmark["start_date"], benchmark["end_date"]
    series = built["series"]
    benchmark_isins = set(built["benchmark_component_isins"].values())
    days = sorted(set.intersection(*(set(d for d, _ in series[isin]) for isin in benchmark_isins)))
    days = [d for d in days if start <= d <= end]
    marks = {}
    for isin in built["weight"]:
        rows = series.get(isin) or []
        dates = [d for d, _ in rows]
        values = []
        for day in days:
            idx = bisect_right(dates, day) - 1
            # A recent mark can bridge exchange holidays. A pre-launch or stale mark cannot
            # contribute; the holding joins only once it has valid prices at both step endpoints.
            valid = idx >= 0 and (date.fromisoformat(day) - date.fromisoformat(dates[idx])).days <= 7
            values.append(rows[idx][1] if valid else None)
        marks[isin] = values
    wealth = 1.0
    coverage = []
    first_used = {}
    for idx in range(1, len(days)):
        eligible = {isin: w for isin, w in built["weight"].items()
                    if marks[isin][idx] is not None and marks[isin][idx - 1] is not None}
        denominator = sum(eligible.values())
        if denominator <= 0:
            return {"available": False,
                    "reason": f"No holdings have usable price and FX history for {days[idx - 1]} to {days[idx]}."}
        coverage.append(denominator * 100)
        wealth *= 1 + sum(w / denominator * (marks[isin][idx] / marks[isin][idx - 1] - 1)
                          for isin, w in eligible.items())
        for isin in eligible:
            first_used.setdefault(isin, days[idx - 1])
    elapsed = (date.fromisoformat(end) - date.fromisoformat(start)).days / 365.25
    portfolio_cagr = (wealth ** (1 / elapsed) - 1) * 100
    return {"available": True, "source": "Available-history reconstruction with current weights renormalised per period",
            "coverage_adjusted": any(c < 99.999 for c in coverage),
            "min_weight_covered_pct": min(coverage),
            "avg_weight_covered_pct": sum(coverage) / len(coverage),
            "holdings_history": [{"isin": isin, "name": built["names"].get(isin, isin),
                                  "weight_pct": weight * 100,
                                  "first_used": first_used.get(isin)}
                                 for isin, weight in built["weight"].items()],
            "start_date": start, "end_date": end, "years_elapsed": elapsed,
            "total_return_pct": (wealth - 1) * 100, "portfolio_cagr_pct": portfolio_cagr,
            "benchmark_cagr_pct": benchmark["cagr_pct"],
            "active_return_pp": portfolio_cagr - benchmark["cagr_pct"],
            "benchmark_components": benchmark["components"]}


def compute_tracking_error(holdings: list[dict], benchmark: str,
                           frequency: str = "weekly", years: int = 5,
                           require_complete_return: bool = False,
                           benchmark_weights: dict[str, float] | None = None,
                           portefeuille: str | None = None) -> dict:
    """Realised TE of the displayed risk sleeve against its investable benchmark."""
    import numpy as np  # noqa: PLC0415

    from momentum.diversification import annualized_stats  # noqa: PLC0415

    freq = frequency if frequency in PERIODS else "weekly"
    preliminary = False
    try:
        built = build_paired_series(
            holdings, benchmark, freq, years,
            # The returns displayed beside the tracking error must use the exact current sleeve.
            # If it launched later or has a historical gap, shift to the shared continuous monthly
            # window and flag that it is shorter; never quietly use a smaller, data-dependent book.
            require_complete_holdings=(require_complete_return and freq == "monthly"),
            benchmark_weights=benchmark_weights,
        )
    except SeriesError as monthly_error:
        # The Risk UI normally requests monthly returns. A new fund can have only one monthly
        # endpoint while still having a real run of daily closes. Report that exact daily sample
        # as preliminary instead of manufacturing pre-launch history or leaving the panel blank.
        if not (require_complete_return and freq == "monthly"):
            return {"available": False, "benchmark": benchmark, "frequency": freq,
                    "reason": monthly_error.reason}
        try:
            built = build_paired_series(
                holdings, benchmark, "daily", years,
                require_complete_holdings=True,
                benchmark_weights=benchmark_weights,
                minimum_observations=MIN_PRELIMINARY_DAILY_OBS,
            )
            preliminary = True
            freq = "daily"
        except SeriesError:
            return {"available": False, "benchmark": benchmark, "frequency": freq,
                    "reason": monthly_error.reason}

    active = built["active"]
    weight_used = built["weight_used"]
    ppy = built["periods_per_year"]

    a = np.asarray(active, dtype=float)
    #  THROUGH `annualized_stats` — the ONE definition of volatility here, so TE and the vol
    # column beside it cannot be two different formulas. It is `std(ddof=1) × √ppy`, i.e. Bessel,
    # with ā subtracted.
    st = annualized_stats(a.tolist(), periods_per_year=ppy)
    te = st.ann_vol
    mean_active = float(a.mean())
    active_std = float(np.std(a, ddof=1)) if len(a) >= 2 else None
    portfolio_ann = annualized_stats(built["portfolio"], periods_per_year=ppy).ann_return
    benchmark_ann = annualized_stats(built["benchmark"], periods_per_year=ppy).ann_return
    portfolio_total = float(np.prod(1.0 + np.asarray(built["portfolio"], dtype=float)) - 1.0)
    benchmark_total = float(np.prod(1.0 + np.asarray(built["benchmark"], dtype=float)) - 1.0)
    benchmark_components = []
    for name, returns in built["benchmark_component_returns"].items():
        stats = annualized_stats(returns, periods_per_year=ppy)
        total = float(np.prod(1.0 + np.asarray(returns, dtype=float)) - 1.0)
        benchmark_components.append({
            "name": "AGGH.AS" if name == "BONDS" else name,
            "isin": built["benchmark_component_isins"][name],
            "weight_pct": built["benchmark_component_weights"][name],
            "window_return_pct": total * 100.0,
            "annualized_return_pct": None if stats.ann_return is None else stats.ann_return * 100.0,
            # The risk series is converted to EUR before returns are paired, so these are the
            # exact EUR marks the component return and policy CAGR are based on—not a different
            # native-currency Yahoo display quote.
            **built["benchmark_component_marks"][name],
        })
    active_ann = (portfolio_ann - benchmark_ann
                  if portfolio_ann is not None and benchmark_ann is not None else None)
    historical = _current_weights_history(built, years)

    return {
        "available": True,
        "benchmark": built.get("benchmark_label", benchmark),
        "frequency": freq,
        "periods_per_year": ppy,
        "observations": len(active),
        "years": years,
        #  The window the returns actually cover, not `years` back from today. The card used to
        # say "trailing window — as it stands today", which is an assumption where a date belongs:
        # a holding that listed two years ago shortens the paired grid, and a stale price series
        # ends it early. `obs_dates` is each period's own last trading day, so these are real dates
        # the reader can check against a chart.
        #  FILTERED — a bucket whose end date could not be resolved contributes None, and
        # min()/max() over a list containing one would raise rather than report the gap.
        "window_from": (dates[0] if (dates := sorted(d for d in built["obs_dates"] if d)) else None),
        "window_to": (dates[-1] if dates else None),
        "history_limited": built.get("history_limited", False),
        "preliminary": preliminary,
        # Audit operands for the three return tiles. Annualised return is a geometric projection
        # from these exact paired closes: `(1 + total) ** (f / T) - 1`.
        "portfolio_return_total_pct": portfolio_total * 100.0,
        "benchmark_return_total_pct": benchmark_total * 100.0,
        "benchmark_components": benchmark_components,
        "benchmark_history": _benchmark_history(built, years),
        "historical_comparison": historical,
        "annualization_factor": ppy / len(active),
        "active_return_std_per_period_pct": None if active_std is None else active_std * 100.0,
        "tracking_error_pct": None if te is None else te * 100.0,
        #  The active return itself, because it is the quantity TE is the spread OF and reporting
        # one without the other is what makes the two get confused. Mean per period, and the same
        # figure annualised geometrically — a book can have a large TE and no active return at all.
        "mean_active_per_period_pct": mean_active * 100.0,
        "active_return_ann_pct": active_ann * 100 if active_ann is not None else None,
        "portfolio_return_ann_pct": portfolio_ann * 100 if portfolio_ann is not None else None,
        "benchmark_return_ann_pct": benchmark_ann * 100 if benchmark_ann is not None else None,
        #  Te is a denominator here, so a near-zero one is refused rather than printed as a huge
        # ratio. The information ratio is the active return per unit of the risk taken to get it.
        "information_ratio": (
            None if not te or te <= 1e-9 or active_ann is None else active_ann / te),
        # How much of the sleeve the average observation actually spoke for — see the renormalisation
        # note above. 100% means every holding had a price at both ends of every step.
        "avg_weight_covered_pct": float(np.mean(weight_used)) * 100.0,
        "priced_holdings": built["priced"],
        "total_holdings": built["total"],
        "benchmark_isin": built["benchmark_isin"],
        #  The cadence's own bias, carried with the number rather than left in a doc. See the
        # module header: non-synchronous closes inflate the spread of a DIFFERENCE.
        "cadence_note": (
            "Daily closes are not synchronous — the tracker closes at 16:30 London, a US holding at "
            "21:00 — which lowers the measured covariance and therefore INFLATES this figure. "
            "Weekly spans the gap." if freq == "daily" else None),
    }
