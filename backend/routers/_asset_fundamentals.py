"""Is this company fundamentally sound — and are we paying a sensible price for it?

FOUR CHARTS, ONE BLOB, ONE PRICE READ. Each answers a different question, and each is misleading
without the others:

    1. PRICE vs FAIR VALUE   what we pay, against what five independent methods say it is worth
    2. YIELD                 what we GET per euro paid — cash thrown off, and cash handed back
    3. ROIC vs WACC          whether the business earns more than its capital costs
    4. SAFETY                whether cheap is cheap for a reason

`stock/{sym}/financials` already carries 262 line items per period over ~24-41 annual periods, and
it is already cached (the path is shared with the earnings pipeline). Charts 2-4 are pure reads off
it: GuruFocus computes those ratios itself, from its own internally-consistent numbers. Once a
company has any financials column, all four charts cost ZERO extra API calls.

⚠ THE PRICE IS YFINANCE'S, DAILY, AND NEVER GURUFOCUS'S.
    /portfolios prices everything from `asset_price` — every model, every benchmark, the
    correlation matrix. A GuruFocus price line here would be a second vendor with different
    adjustment conventions and different FX on a page whose entire claim is that its numbers are
    comparable. The blob's own `Month End Stock Price` is read ONLY as a cross-check
    (`price_crosscheck`), never drawn.

⚠ WHICH IS EXACTLY WHY BOTH LEGS OF CHART 1 GO TO EUR.
    GuruFocus FX-converts financials into ITS listing's trading currency, and its listing comes
    from `pick_listing` — a different id space from `yahoo_symbol`, resolved from the same ISIN by
    a different rule. They routinely disagree: GF may hold a name on Xetra (EUR) while our price
    series is Nasdaq (USD). Draw a USD price line through EUR fair values and the gap reads as
    mispricing when it is an exchange rate — a chart that is wrong and looks fine. Both sides are
    converted through the same `fx_rate` table the rest of the app uses; then a share is a share
    (same ISIN, same share class, one economic value) and the comparison means something.

⚠ CHARTS 2-4 NEED NO CONVERSION AT ALL — a ROIC of 18% is 18% in every currency, and a Piotroski
    score is a count. This is `_asset_financials`'s "skip the conversion, do not relabel it" rule,
    the one its unit system already implements for share counts.

⚠ EVERY SERIES REPORTS WHAT IT DROPPED. `_series` turns a GuruFocus "" / "N/A" / "-" into a
    skipped point, and those markers are FAR commoner in the ratio sections than in the statements:
    measured on one real blob, Piotroski has 17 points where Revenue has 24, Interest Coverage 20,
    GF Value 11. A loss-making year HAS no PE. Left silent, a "median over the last decade" band
    quietly becomes a median of the profitable years only — narrower, higher, and flattering.
    `points` vs `periods` is on every series so the caller can say so.

⚠ DISPLAY ONLY. The blob is today's RESTATED view of history — a 2019 ROIC here is what 2019 looks
    like NOW, not what anyone could have seen in 2019. Fine for reading a business; it must never
    feed the momentum signals, which live on as-of discipline.
"""
from __future__ import annotations

import asyncio
from datetime import date, timedelta

from fastapi import HTTPException

from routers._asset_financials import (
    _TEMPLATES,
    _fetch_financials_raw,
    _has_line,
    _series,
    resolve_gf_listing,
)

# How far back chart 1 draws. The fair-value series reach ~24 years; a price line that long makes
# the recent decade unreadable, and nobody is asking "was this cheap in 2002".
_PRICE_YEARS = 12

# ── the four charts, declared ────────────────────────────────────────────────────────────────
# `(field, label, section)`. `field` is GuruFocus's own key — verified against a real blob, not
# guessed; several are near-misses of a more obvious name (see the notes).

#: Chart 1's band. FIVE methods, each a per-share value in the LISTING's currency.
#
# ⚠ NOT the eleven in `summary.chart`. That block offers more methods (two DCFs, the liquidation
#   floors) but as SCALARS — today's number, no history — and its two DCFs read 0.00, i.e. not
#   computed. A band needs series; these are the series.
# ⚠ `Net-Net Working Capital` and `Net Current Asset Value` ARE series here and are deliberately
#   excluded: they are liquidation floors (-36.34 and -8.49 on a real blob), meaningless for a
#   going concern, and would drag the band's floor below zero on every healthy company.
_FAIR_VALUES: tuple[tuple[str, str], ...] = (
    ("Intrinsic Value: Projected FCF", "Projected FCF"),
    ("Median PS Value", "Median P/S"),
    ("Peter Lynch Fair Value", "Peter Lynch"),
    ("Graham Number", "Graham Number"),
    ("Earnings Power Value (EPV)", "Earnings Power"),
)

#: Chart 2 — what a euro of price buys. A yield, unlike a multiple, is comparable across names.
_YIELDS: tuple[tuple[str, str, str], ...] = (
    # The cash the business throws off, per euro of price.
    ("FCF Yield %", "FCF yield", "valuation"),
    # Greenblatt's: EBIT / enterprise value — capital-structure-neutral, so it is not flattered by
    # leverage the way a plain earnings yield is.
    ("Earnings Yield (Joel Greenblatt) %", "Earnings yield (Greenblatt)", "valuation"),
    # What actually reaches US: dividends + buybacks + debt paydown. The only one of the three
    # that is cash in hand rather than cash in the business.
    ("Shareholder Yield %", "Shareholder yield", "quality"),
)

#: Chart 3 — is the business worth owning at all. The SPREAD is the point.
_RETURNS: tuple[tuple[str, str, str], ...] = (
    ("ROIC %", "ROIC", "common_size"),
    ("WACC %", "WACC", "common_size"),
)

#: Chart 4 — the value-trap screen. Bounded scores, each with its own conventional thresholds.
_SAFETY: tuple[tuple[str, str, str], ...] = (
    ("Piotroski F-Score", "Piotroski F", "quality"),
    ("Altman Z-Score", "Altman Z", "quality"),
    ("Beneish M-Score", "Beneish M", "quality"),
    ("Interest Coverage", "Interest coverage", "quality"),
)

#: Read but never drawn — the honesty check on chart 1. See `price_crosscheck`.
_GF_PRICE = ("Month End Stock Price", "quality")

# ── the quality verdict ──────────────────────────────────────────────────────────────────────
# FOUR NUMBERS, AND THEY ARE NOT FOUR OPINIONS OF THE SAME THING. Each catches something the
# other three cannot, which is why there is no composite score here: a single 0-100 would hide
# exactly the disagreement that makes this readable (GuruFocus already sells one; outsourcing the
# judgement is what we are avoiding).
#
#     spread      does the business create value AT ALL
#     trend       is the moat WIDENING or MELTING — the level alone cannot see this
#     conversion  is the reported profit REAL, or an accrual it never collected
#     gm_sd       is there pricing power, or does the market set the price
#
# ⚠ THE THRESHOLDS ARE CALIBRATED, NOT LAWS. Measured over 14 large caps on our own blobs
# (2026-07-16), 10y medians:
#
#     NVDA  +65.3pp   AAPL  +18.8pp   MSFT  +14.9pp   JNJ  +9.0pp   KO  +6.0pp
#     INTC   +3.1pp   IBM    +0.3pp   KHC   -2.0pp    F   -1.9pp    AMD -8.4pp
#
# The ranking fell out with no tuning — Kraft Heinz and Ford below zero, IBM at 0.3 after a
# decade of buybacks. Treat the cut-offs as where those populations separate, not as physics.
_SPREAD_FAIL = 0.0        # <= 0: earns less than its capital costs. Not a quality business at
                          # any price — this is the only threshold that is not a judgement call.
_TREND_FAIL = -5.0        # ROIC down >5pp, 5y median vs the prior 5. INTC reads a "fine" +3.1pp
                          # spread while its ROIC fell SEVENTEEN points; AMD -22.6. Against
                          # MSFT +3.0, JNJ -1.1, CSCO -0.8 — flat is normal, this is a collapse.
_CONVERSION_FAIL = 0.8    # FCF/NI below 0.8 sustained: the profit is not turning into cash.
                          # INTC 0.74 — fab capex eats it. ⚠ HIGH IS NOT THE OPPOSITE OF GOOD:
                          # IBM 1.68 and Ford 1.54 are depreciation exceeding capex, i.e.
                          # harvesting. So this FAILS low and is never rewarded high.
_GM_SD_FAIL = 5.0         # Standard deviation of gross margin over 10y. Quality businesses have
                          # BORING margins: KO 1.2, CSCO 0.9, JNJ 1.5, MSFT 2.0. The cyclicals
                          # and the price-takers: INTC 11.1, AMD 7.8, PFE 7.5, XOM 5.8.

_MIN_MEDIAN_PERIODS = 6   # a 10y median off three points is not a median.
_MIN_TREND_PERIODS = 10   # 5 + 5. Fewer and there is no "prior 5" to compare against.
_QUALITY_YEARS = 10


def _pts(blob: dict, cadence: str, field: str, section: str) -> list[dict]:
    return [{"date": p.date, "value": p.value}
            for p in _series(blob, cadence, field=field, section=section)]


def _periods(blob: dict, cadence: str, since: str | None = None) -> int:
    """How many periods the blob has — the denominator for "what did this series drop".

    `since` counts only the periods inside the DRAWN window. Chart 1 clips its band to the price
    window, and a `dropped` measured against all 40 periods would then read 28 on a series that
    dropped nothing — conflating "we are not drawing this far back" with "GuruFocus had no value
    here", which are the two things `dropped` exists to keep apart.
    """
    pts = _series(blob, cadence, field="Revenue", section="income")
    return sum(1 for p in pts if not since or p.date >= since)


def _series_out(blob: dict, cadence: str, field: str, label: str, section: str,
                periods: int) -> dict:
    pts = _pts(blob, cadence, field, section)
    return {
        "field": field, "label": label, "points": pts,
        # ⚠ NOT decoration. `dropped > 0` means the period is not on this line — GuruFocus had no
        # value (a loss year has no PE, an unlevered year no interest coverage), or we had no FX
        # rate to convert it (see `_dropped`). A reader who takes a 27-point line for a 40-year
        # history is reading only the periods that worked.
        "period_count": periods, "dropped": max(periods - len(pts), 0), "non_positive": 0,
    }


def _dropped(series: dict) -> dict:
    """Recount `dropped` from the points as they now stand.

    ⚠ IT MUST BE RECOUNTED AFTER THE EUR CONVERSION, AND THAT IS NOT PEDANTRY. `_to_eur` drops any
    period with no FX rate on or before it, and `fx_rate`'s history is THIN — measured, Apple's
    fair values go 40 periods -> 27, losing 1986-1998 entirely. Counting drops before the
    conversion reported `dropped: 0` beside a series that had quietly lost thirteen years: the
    exact false-completeness this field exists to prevent, in the field itself.
    """
    series["dropped"] = max(series["period_count"] - len(series["points"]), 0)
    return series


def _vals(blob: dict, cadence: str, field: str, section: str, n: int = _QUALITY_YEARS) -> list[float]:
    """The last `n` values of one line, oldest-first."""
    return [p.value for p in _series(blob, cadence, field=field, section=section)][-n:]


def _metric(key: str, label: str, unit: str, value: float | None, periods: int,
            fails: bool, *, applicable: bool = True, note: str | None = None) -> dict:
    """One quality number and its verdict.

    ⚠ FOUR STATES, AND THREE OF THEM ARE NOT "BAD".
        ok       measured, and it passes
        fail     measured, and it does not
        n_a      the LINE DOES NOT EXIST for this company — a bank has no ROIC and no gross
                 margin at all (JPMorgan, template 'B': structurally absent, not empty). That is
                 an answer about the industry template, not a failure and not a gap.
        unknown  the line exists but there is too little history to say. A 10y median off three
                 points is not a median; a trend needs a prior 5 to compare against.
    Collapsing `n_a` or `unknown` into `fail` would mark every bank a bad business and every
    young company a suspect one.
    """
    if not applicable:
        status = "n_a"
    elif value is None:
        status = "unknown"
    else:
        status = "fail" if fails else "ok"
    return {"key": key, "label": label, "unit": unit, "value": value,
            "periods": periods, "status": status, "note": note}


def _quality(blob: dict, cadence: str) -> list[dict]:
    """The four-number quality verdict. See `_SPREAD_FAIL` and friends for the calibration."""
    import statistics as st  # noqa: PLC0415

    roic = _vals(blob, cadence, "ROIC %", "common_size")
    wacc = _vals(blob, cadence, "WACC %", "common_size")
    fcfm = _vals(blob, cadence, "FCF Margin %", "common_size")
    nim = _vals(blob, cadence, "Net Margin %", "common_size")
    gm = _vals(blob, cadence, "Gross Margin %", "common_size")

    has_roic = _has_line(blob, "ROIC %", "common_size")
    has_gm = _has_line(blob, "Gross Margin %", "common_size")

    # 1. THE SPREAD — does it create value at all. Median, not latest: one year is noise, and a
    #    decade of median spread is a moat.
    n = min(len(roic), len(wacc))
    spread = (st.median([roic[-i] - wacc[-i] for i in range(1, n + 1)])
              if has_roic and n >= _MIN_MEDIAN_PERIODS else None)

    # 2. THE TREND — the one the level cannot see. Intel reads a passable +3.1pp spread while its
    #    ROIC fell 17 points across the decade: a melting moat and a good business look identical
    #    in a single number.
    trend = (st.median(roic[-5:]) - st.median(roic[-10:-5])
             if has_roic and len(roic) >= _MIN_TREND_PERIODS else None)

    # 3. CASH CONVERSION — the lie detector on the other three. Both margins share a denominator
    #    (revenue), so their ratio IS FCF/net income.
    #    ⚠ A LOSS YEAR IS SKIPPED, NOT INCLUDED. With net income negative the ratio flips sign and
    #    a loss-making year would score as excellent conversion.
    pairs = [f / x for f, x in zip(fcfm, nim) if x and x > 0]
    conv = st.median(pairs) if len(pairs) >= _MIN_MEDIAN_PERIODS else None

    # ⚠ AND IT MEANS NOTHING FOR A BANK. A bank's operating cash flow is dominated by its BALANCE
    #    SHEET growing — lending IS the business, so OCF routinely goes negative in a good year
    #    (JPMorgan: -147,782, which this codebase already documents as information rather than a
    #    fault). Measured, JPM scores 0.19x here: not a company failing to collect its profits,
    #    just a bank being a bank. Reported as inapplicable rather than as a catastrophic failure.
    conv_ok = has_roic          # `ind_template` 'B' has neither; ROIC's absence identifies it

    # 4. PRICING POWER — as the ABSENCE of drama. A quality business has a boring gross margin.
    gm_sd = st.pstdev(gm) if has_gm and len(gm) >= _MIN_MEDIAN_PERIODS else None
    # ⚠ σ CANNOT TELL A COLLAPSE FROM AN IMPROVEMENT, AND THAT PRODUCED A WRONG VERDICT.
    #    NVIDIA's gross margin σ is 5.9 — because it went from ~35% to ~75%. That is the OPPOSITE
    #    of "the market sets our price": it is pricing power being acquired. Flagging it failed
    #    the one company in the sample with the strongest pricing power in it.
    #    So σ only fails when the margin is high-variance AND NOT improving. Intel (σ 11.1, margin
    #    falling) still fails; NVIDIA does not.
    gm_trend = (st.median(gm[-5:]) - st.median(gm[-10:-5])
                if has_gm and len(gm) >= _MIN_TREND_PERIODS else None)

    return [
        _metric("spread", "ROIC − WACC", "pp", spread, n, spread is not None and spread <= _SPREAD_FAIL,
                applicable=has_roic,
                note=("A bank's capital IS its product — GuruFocus publishes no ROIC for this "
                      "industry template at all." if not has_roic else
                      f"10y median. ≤ {_SPREAD_FAIL:.0f}pp earns less than its capital costs.")),
        _metric("trend", "ROIC trend", "pp", trend, len(roic),
                trend is not None and trend < _TREND_FAIL, applicable=has_roic,
                note=("No ROIC for this template." if not has_roic else
                      f"Last 5y median vs the prior 5. Below {_TREND_FAIL:.0f}pp the moat is "
                      f"melting — a level alone cannot see this.")),
        _metric("conversion", "FCF / net income", "x", conv, len(pairs),
                conv is not None and conv < _CONVERSION_FAIL, applicable=conv_ok,
                note=("A bank's operating cash flow tracks its BALANCE SHEET, not its collections "
                      "— lending is the business, so OCF goes negative in a good year. This ratio "
                      "says nothing about a bank." if not conv_ok else
                      f"10y median, loss years excluded. Below {_CONVERSION_FAIL} the profit is "
                      f"not becoming cash. High is NOT better — well above 1 is usually "
                      f"depreciation exceeding capex, i.e. harvesting.")),
        _metric("gm_sd", "Gross margin σ", "pp", gm_sd, len(gm),
                (gm_sd is not None and gm_sd > _GM_SD_FAIL
                 and not (gm_trend is not None and gm_trend > 0)),
                applicable=has_gm,
                note=("A bank has no cost of goods sold, so no gross margin exists to be stable."
                      if not has_gm else
                      f"10y standard deviation. Above {_GM_SD_FAIL:.0f}pp the market sets the "
                      f"price — UNLESS the margin is rising, which is pricing power being won, "
                      f"not lost (NVIDIA: σ 5.9 on a margin that went 35% → 75%).")),
    ]


def _eur_price(isin: str, since: str) -> tuple[list[dict], str | None, str | None]:
    """(daily EUR closes, the yfinance symbol, its native currency) — or ([], None, None).

    yfinance ONLY, split-adjusted and converted at each date's own rate, through exactly the
    helpers /portfolios prices its models with. Nothing here may reach `metric_data`.
    """
    from routers._airs_portfolio_perf import (  # noqa: PLC0415
        _closes,
        _eur_series,
        _executions,
        _fx,
    )

    ex = (_executions([isin]) or {}).get(isin)
    if not ex or not ex.get("analysis_id"):
        return [], None, None
    today = date.today().isoformat()
    raw = (_closes([ex["analysis_id"]], since, today) or {}).get(ex["analysis_id"]) or []
    if not raw:
        return [], ex.get("yahoo_symbol"), ex.get("currency")
    fx = _fx({ex.get("currency") or "USD"}, since, today)
    eur = _eur_series(raw, ex.get("currency"), fx)
    return ([{"date": d, "value": v} for d, v in eur],
            ex.get("yahoo_symbol"), ex.get("currency"))


def _to_eur(points: list[dict], currency: str | None, fx: dict[str, dict[str, float]]) -> list[dict]:
    """GuruFocus per-share values -> EUR, at each period-end's own rate.

    The band and the price line must be in ONE currency or their gap is an exchange rate. A point
    with no rate on or before its date is DROPPED, not carried at its native value — a Graham
    Number silently left in dollars beside a EUR price line is the whole bug.
    """
    from routers._benchmark_index import _rate  # noqa: PLC0415

    if not currency or currency.upper() == "EUR":
        return points
    out: list[dict] = []
    for p in points:
        r = _rate(fx, currency, p["date"])
        if r:
            out.append({"date": p["date"], "value": p["value"] / r})
    return out


def compute_fundamentals(isin: str, cadence: str = "annuals") -> dict:
    """Everything the four soundness charts need, in one payload off one cached blob."""
    if cadence not in ("annuals", "quarterly"):
        raise HTTPException(422, f"cadence must be annuals or quarterly, got {cadence!r}")

    gf = resolve_gf_listing(isin, phrase="fundamentals")
    ticker, exchange = gf["ticker"], gf["exchange"]
    currency = gf["currency"]

    blob, fetched = _fetch_financials_raw(ticker, exchange, force=False)
    if blob is None:
        raise HTTPException(404, f"GuruFocus has no financials for {exchange}:{ticker}.")

    periods = _periods(blob, cadence)
    since = (date.today() - timedelta(days=365 * _PRICE_YEARS)).isoformat()
    price, symbol, price_ccy = _eur_price(isin, since)

    # ONE fx load for every GuruFocus per-share series in this payload.
    from routers._airs_portfolio_perf import _fx  # noqa: PLC0415
    fair_from = min([p["date"] for f, _ in _FAIR_VALUES
                     for p in _pts(blob, cadence, f, "quality")] or [since])
    fx = _fx({currency or "USD"}, min(fair_from, since), date.today().isoformat())

    # ⚠ THE BAND IS CLIPPED TO THE PRICE WINDOW, AND THAT WAS A BUG BEFORE IT WAS A FEATURE.
    # The price reaches back `_PRICE_YEARS` (12); the fair values reach ~27. Merged into one frame
    # the chart spanned 1999-2026 while the price line covered only its last 44% — more than half
    # of it showing fair values with nothing to compare them against, which is the ONE comparison
    # the chart exists to make. Clipped to where the price actually starts, every band point has
    # a price beside it.
    window_start = price[0]["date"] if price else since
    band_periods = _periods(blob, cadence, since=window_start)

    fair: list[dict] = []
    for field, label in _FAIR_VALUES:
        s = _series_out(blob, cadence, field, label, "quality", band_periods)
        s["points"] = [p for p in _to_eur(s["points"], currency, fx)
                       if p["date"] >= window_start]
        # ⚠ COUNTED, NOT DROPPED HERE. A fair value <= 0 is an ANSWER — Peter Lynch needs positive
        # earnings growth, Graham needs positive EPS and book value, EPV <= 0 says the business
        # earns nothing. Measured, a QUARTER of the band's in-window points are <= 0 (Tesla 33/60,
        # Morgan Stanley 19/60, AMD 21/60). A log axis cannot plot them, so the CHART breaks its
        # line there — but the payload keeps them, because dropping data to suit an axis is how a
        # loss-making decade disappears from a valuation chart.
        s["non_positive"] = sum(1 for p in s["points"] if p["value"] <= 0)
        fair.append(_dropped(s))          # recount AFTER conversion AND clip — see `_dropped`

    gf_price = _to_eur(_pts(blob, cadence, _GF_PRICE[0], _GF_PRICE[1]), currency, fx)

    return {
        "isin": isin,
        "symbol": f"{exchange}:{ticker}",
        "company_id": gf["company_id"],
        # ⚠ The financials' currency, which is the LISTING's — not necessarily the price's.
        "currency": currency,
        "yahoo_symbol": symbol,
        "price_currency": price_ccy,
        # ⚠ A non-home listing's history has HOLES (Apple: 91 payments on Nasdaq, 63 on Zurich
        # with a five-year gap). A band drawn across one is a confident fiction, so the caller is
        # told rather than left to assume.
        "is_home": gf["is_home"],
        # ⚠ Which INDUSTRY template GuruFocus renders this company with — 'B' (bank) has no EBIT
        # and no gross profit at all, so two of these charts are structurally thinner for one.
        "template": _TEMPLATES.get(
            str(((blob.get("financials") or {}).get("financial_template_parameters") or {})
                .get("ind_template") or "").upper()),
        "cadence": cadence,
        "period_count": periods,
        "fetched": fetched,

        # ── chart 1 ──────────────────────────────────────────────────────────────────────────
        # yfinance, daily, EUR. The band is GuruFocus, per-share, converted to the SAME EUR.
        "price_eur": price,
        "fair_values_eur": fair,
        # ⚠ NEVER DRAWN. GuruFocus's own month-end price, in EUR, purely so the two vendors can be
        # compared: if this and `price_eur` diverge after FX, the ISIN resolved to two different
        # securities and every fair value on the chart belongs to the other one.
        "price_crosscheck_eur": gf_price,

        # ── charts 2-4 ───────────────────────────────────────────────────────────────────────
        # Percentages and scores. No conversion — see the module docstring.
        "yields": [_series_out(blob, cadence, f, lab, sec, periods) for f, lab, sec in _YIELDS],
        "returns": [_series_out(blob, cadence, f, lab, sec, periods) for f, lab, sec in _RETURNS],
        "safety": [_series_out(blob, cadence, f, lab, sec, periods) for f, lab, sec in _SAFETY],

        # ⚠ A BANK HAS NO EBIT AND NO GROSS PROFIT, so Greenblatt's earnings yield is absent and
        # ROIC/WACC mean very little for one (its capital IS its product). That is an ANSWER about
        # the template, not a gap — `_has_line` distinguishes absent from present-but-empty.
        # The four-number verdict: does it create value, is the moat melting, is the profit
        # real, is there pricing power. NOT a composite — see `_quality`.
        "quality": _quality(blob, cadence),

        "has_roic": _has_line(blob, "ROIC %", "common_size"),
        "has_earnings_yield": _has_line(blob, "Earnings Yield (Joel Greenblatt) %", "valuation"),
    }


async def compute_fundamentals_async(isin: str, cadence: str = "annuals") -> dict:
    return await asyncio.to_thread(compute_fundamentals, isin, cadence)
