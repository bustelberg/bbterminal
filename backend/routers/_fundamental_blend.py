"""Blending a portfolio's fundamentals — where the aggregation rule depends on WHAT is aggregated.

⚠⚠ THERE IS NO SINGLE "WEIGHTED AVERAGE" HERE, AND USING ONE PRODUCES A CONFIDENT WRONG NUMBER.
    Three kinds of metric live in the same `MetricRow` stream and each aggregates differently:

    MULTIPLE (P/E, PEG — a price OVER something)
        The portfolio's P/E is aggregate price ÷ aggregate earnings, which is the weighted
        HARMONIC mean of the components — never the arithmetic one. The arithmetic mean is
        dragged upward by any single high multiple and the error is large, one-directional and
        entirely plausible-looking. Two names at 50% each, P/E 10 and P/E 100:
            arithmetic  (10 + 100)/2      = 55.0
            harmonic    1/(0.05 + 0.005)  = 18.2      <- what the portfolio actually costs
        A 3x overstatement, and 55 is not an obviously silly number for a growth book.

    YIELD / MARGIN / RETURN (FCF Yield %, ROE %, Net Margin %, Debt-to-Equity)
        Already normalised per company, and a yield is the RECIPROCAL of a multiple — so these
        aggregate ARITHMETICALLY. Applying the harmonic rule here is the same mistake mirrored.

    LEVEL (Revenue, Net Income, EPS, FCF per share, Price)
        Absolute amounts in each company's own units. Weighting Apple's revenue by 5% and ASML's
        by 3% produces a number that is not any company's revenue and not the portfolio's either.

        ⚠⚠ SO THE LINE IS CHAINED FROM WEIGHTED **GROWTH**, NOT AVERAGED FROM REBASED LEVELS
        (2026-08-12). Between two drawn points the index moves by the cap-weighted average of what
        its constituents actually did over that interval:

            index[p] = index[anchor] x (1 + Σ w_i·g_i / Σ w_i),   g_i = v_i(p)/v_i(anchor) − 1

        Averaging rebased levels made the line an artefact of WHEN each member's history starts —
        every member is 100 at its own first period, so a constituent joining the panel dragged the
        average toward 100 and the index "moved" on composition alone. Measured on the AEX annual
        revenue line, that drew a 388 → 285 crash into 2023 that no constituent experienced; the
        same series now reads 211 → 244 → 249.

        The per-member REBASE survives for the audit views (each company's own index, anchored on
        its first POSITIVE period), but nothing sums those into the line any more.

⚠⚠ ALIGNED ON THE FISCAL YEAR, NOT THE EXACT DATE — WITHOUT THIS THE BLEND IS EMPTY.
    Companies close their books on different days. Measured across six real members: year-ends on
    2001-03-31, 2001-08-31, 2014-12-31 and 1998-12-31. Keyed on the raw `target_date`, almost no
    two members EVER share a key, so every period carries one company, every period falls under
    the coverage floor, and the whole series comes back empty — or, without a floor, becomes a
    line drawn from whichever single company happened to report that day.

⚠ RENORMALISED AT EVERY PERIOD, NOT ONCE. Members report on different fiscal calendars and start at
    different times, so the weight with data moves date by date. Dividing by the ORIGINAL weight
    instead would drag every early period toward zero — the series would show a rise that is
    nothing but coverage improving.

⚠⚠ A FORECAST IS REBASED ON THE ACTUAL IT CONTINUES, NEVER ON ITSELF.
    An estimate series (`annual_eps_nri_estimate`) and the actual it extends
    (`…__EPS without NRI`) are the same quantity — one measured, one forecast — and the chart
    indexes BOTH off the actual's base so the forecast continues the line. Rebase them
    independently and the estimate restarts at 100 while the actual has run to 1,808: measured on
    a real book, a 94% earnings collapse in the forecast year, drawn in full confidence. Members
    are rebased individually, so the caller passes each member's ACTUAL points as `base_points`
    and the forecast inherits that anchor.

⚠ AND IT REFUSES BELOW A FLOOR. A blend over 40% of a book, drawn on the same axes as one over
    95%, is the fabrication `MIN_COVERAGE_PCT` already refuses on the AIRS returns. `covered_pct`
    rides on every series and `MIN_BLEND_COVERAGE_PCT` is the point below which nothing is drawn.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date as _date
from statistics import median

# Below this share of the blended weight reporting on a date, that date has no honest value.
#
# ⚠ 60 -> 80 (2026-07-28) -> 50 (2026-08-12, on request: "if we have half the companies in a
# benchmark for a given period, we should display that data point"). Half the weight now draws —
# `<` is the comparison, so exactly 50.0% clears.
#
# ⚠⚠ AND THE REASON IT WENT TO 80 HAS NOT GONE AWAY — IT IS AN ACCEPTED COST NOW, WHICH IS WORTH
# KNOWING WHEN THE RIGHT EDGE OF A CHART LOOKS ODD. Books close on different dates, so early in a
# fiscal year a handful of holdings have filed and the rest have not; renormalising over whoever
# reported draws that as a full-height point in the same ink as a year everybody reported. It reads
# as a move in the book and it is a move in the sample. Measured on the S&P revenue blend, FY2026
# was 13.4% covered — still refused — but a 55%-covered newest year now draws where it did not.
# `covered_pct` rides on every point, so the fix if it bites is a stricter bar on the LATEST period
# only, not a return to a single high floor that also hid the mid-history periods this lowering was
# asked for.
#
# ⚠ KEPT IN LOCK-STEP WITH THE FRONTEND'S `marginData.MIN_YEAR_COVERAGE_PCT`, which applies the
# same floor to the ratio cards derived on the client. Two floors that disagree put two cards on
# the same screen spanning different fractions of the same book.
MIN_BLEND_COVERAGE_PCT = 50.0

# The same bar, counted in CONSTITUENTS rather than weight — and both must clear.
#
# ⚠⚠ WEIGHT ALONE LETS ONE GIANT DRAW A PERIOD. Measured 2026-08-12 on the AEX: 2026-Q2 had **2 of
# 22** constituents reporting and cleared the weight floor at 53.8%, because ASML is enormous. A
# point built from two companies, drawn in the same ink as one built from twenty-two. Counted in
# names it is 9.1% and refused.
#
# ⚠ AND NAMES ALONE WOULD BE WORSE, which is why this is an AND: ten 0.4% constituents would outvote
# a missing 7% one. The two answer different questions — "how much of the index reported" and "how
# many of it" — and a period has to survive both.
MIN_BLEND_COVERAGE_NAMES_PCT = 50.0

# ⚠⚠ HOW BIG A MEMBER'S STARTING FIGURE MUST BE, RELATIVE TO ITS OWN TYPICAL SIZE, FOR THE RATIO
# OFF IT TO BE A GROWTH RATE AT ALL — and this is the constant that stops ONE holding deleting a
# whole chart (2026-08-13).
#
# The step is `g = v(period)/v(anchor) − 1`, guarded only by `v(anchor) > 0`. That guard catches
# zero and negatives and misses the case that actually bites: a base that is positive and
# NEAR zero. Measured on the AEX FCF/share index — Prosus's first positive figure is **0.0090** a
# share (2021), against a median of 0.1485 over its own history, i.e. a rounding artefact of a
# holding company that hovers around break-even. Its 2022 figure of −0.24 divided by it is
# **g = −27.0**, i.e. "−2,700% growth", carried at a **26% index weight**:
#
#     step = −3.47   ->   level = 589.4 x (1 − 3.471) = −1,456
#
# From there the index is NEGATIVE, and the level cards plot on a LOG axis — so every point after
# the crossing is dropped as unplottable, silently, one per year. Measured: AEX annual drew **6 of
# 10** points and AEX quarterly **26 of 32**, with `connectNulls` drawing a confident straight line
# across the hole. Nothing on screen said anything; `benchNote` only speaks at zero or one point.
#
# ⚠ IT IS SYMMETRIC, AND THE UPSIDE IS THE HALF A SIGN-CHANGE TEST WOULD MISS. The same 0.0090 base
# gives Prosus **+7,677%** on the way back up, which is how an index quadruples on one constituent.
# The pathology is the DIVISOR, not the direction, so that is what is tested.
#
# ⚠ RELATIVE, NEVER ABSOLUTE. "0.009 is small" is not a fact about a number — it is a fact about
# Prosus. NVIDIA's whole FCF/share series lives at 0.04–0.16 a share and is perfectly real. So the
# bar is the member's own median |value|, which makes it scale-free and currency-free.
#
# ⚠ 0.10 IS READ OFF THE DISTRIBUTION, NOT PICKED. base ÷ median|value| over the two books measured:
# AMD **0.0078** and Prosus **0.0606** are the two pathological anchors; the next-lowest are Adyen
# **0.150** and Lam Research **0.184**, and the bulk sit 0.21–1.0. Adyen's is a real 6.7x growth
# story and must survive. 0.10 sits in the gap, twice over.
_MIN_STEP_BASE_FRACTION = 0.10

# The other end of the same question — and it was never asked.
#
# ⚠⚠ `_MIN_STEP_BASE_FRACTION` GUARDS THE DIVISOR AND THE NUMERATOR HAD NO CEILING AT ALL. The rule
# above refuses a base too small to divide by; nothing refused a RESULT too large to believe. So a
# vendor scale error — a per-share figure delivered in the wrong unit — passed straight through as
# growth, and the chain multiplies it by the member's weight with no bound.
#
# Measured on ACWI's annual FCF/share, 26,160 accepted steps across 1,712 constituents:
#
#     MITSUBISHI HEAVY  2024->2025      50.78 ->  86,214.52   +169,684%   moves the index +116.12pp
#     DENSO CORP        2024->2025     172.97 -> 108,415.57    +62,580%   moves the index  +17.97pp
#
# On a line indexed to 100, one corrupt cell in a 0.07%-weight constituent more than DOUBLED it.
#
# ⚠ 100x IN ONE YEAR IS READ OFF THE DISTRIBUTION, NOT PICKED — the same method as the constant
# above. FCF/share: p99 = +718%, p99.9 = +2,386%, p99.99 = +6,889%, and then nothing until DENSO at
# +62,580%. The largest step that is unambiguously REAL is Bank of America's +3,818% (2008->2009,
# recovering from the crisis). EPS excl. NRI agrees: p99.9 = +2,609%, and every one of the 20 steps
# above +10,000% is a scale error — sixteen of them in the SAME 2003->2004 transition across
# unrelated European filers (Randstad 0.56 -> 160.00, Thales 0.69 -> 193.00, Kesko 0.26 -> 47.25),
# which is a vendor redenomination and not sixteen simultaneous miracles. Revenue, a level series
# with no share-count denominator to mis-scale, has ZERO steps over +10,000% and tops out at
# +5,494% — which is the tell that this pathology belongs to PER-SHARE lines.
#
# So the gap is between ~+6,900% (the top of the real distribution) and ~+10,100% (the bottom of the
# corrupt one). 100x sits in it.
#
# ⚠ IT REFUSES THE STEP, IT DOES NOT CAP IT. Capping would invent a growth rate nobody reported;
# refusing means the member sits out that one interval and rejoins at the next, exactly as the three
# refusals above it behave. We cannot say what its growth was, so it does not vote.
#
# ⚠ AND IT IS NOT SYMMETRIC, DELIBERATELY. The downside is already handled — the floor at −100% is
# the most a level can lose — so there is no matching "too negative" case to catch.
_MAX_STEP_GROWTH = 100.0

# How long a member's last reported figure stands in for a period it did not report — see
# `carry_forward`. One year: the longest any still-reporting filer goes between filings, so nothing
# live is ever dropped, and anything that stops reporting falls out within a year instead of being
# held at a frozen value for the rest of the axis. 400 rather than 365 for fiscal drift and the
# 52/53-week filers.
_MAX_CARRY_DAYS = 400

# A price over something. Aggregates HARMONICALLY.
#
# ⚠ `indicator_q_forward_pe_ratio` IS LISTED EXPLICITLY AND MUST STAY THAT WAY. It is the code the
# Forward P/E chart actually plots, and it matches NONE of the patterns below: `RATIO_SUFFIXES`
# is case-sensitive and this one ends in lowercase "ratio". Left to fall through it is classified
# a LEVEL, gets rebased to 100, and the chart — which formats its value as "{v}x" — renders a
# portfolio trading at "100.0x forward earnings". That is not a missing number a reader would
# question; it is a confident wrong one on a familiar axis.
MULTIPLE_CODES = frozenset({
    "annuals__Valuation Ratios__PE Ratio",
    "annuals__Valuation Ratios__PEG Ratio",
    "annuals__Valuation Ratios__PS Ratio",
    "annuals__Valuation Ratios__PB Ratio",
    "indicator_q_forward_pe_ratio",
})
# Already per-company normalised (a yield is a multiple's reciprocal). Aggregates ARITHMETICALLY.
RATIO_SUFFIXES = ("%", "Ratio", "Coverage", "Debt-to-Equity")


def blend_kind(metric_code: str) -> str:
    """`multiple` | `ratio` | `level` — which of the three rules this code follows."""
    if metric_code in MULTIPLE_CODES:
        return "multiple"
    if any(metric_code.endswith(s) for s in RATIO_SUFFIXES) or "%" in metric_code:
        return "ratio"
    return "level"


def _weighted_arithmetic(pairs: list[tuple[float, float]]) -> float | None:
    """Σ(w·v) ÷ Σw over the members that HAVE a value — the renormalisation."""
    w = sum(p[0] for p in pairs)
    return None if w <= 0 else sum(p[0] * p[1] for p in pairs) / w


def _weighted_harmonic(pairs: list[tuple[float, float]]) -> float | None:
    """Σw ÷ Σ(w/v). ⚠ A non-positive multiple is DROPPED, not inverted: a negative P/E (a loss)
    has no meaningful reciprocal and one of them would flip the whole aggregate's sign."""
    usable = [(w, v) for w, v in pairs if v and v > 0]
    w = sum(p[0] for p in usable)
    if w <= 0:
        return None
    denom = sum(p[0] / p[1] for p in usable)
    return None if denom <= 0 else w / denom


def year_bucket(d: str) -> str:
    """`2025-09-30` → `2025`. The ANNUAL alignment — see the ⚠⚠ at the top of this module."""
    return d[:4]


def quarter_bucket(d: str) -> str:
    """`2025-09-30` → `2025-Q3`.

    ⚠⚠ THE QUARTERLY ALIGNMENT, AND IT MUST MATCH THE VOCABULARY THE **WEIGHTS** ARE KEYED IN.
    `period_caps_eur(cadence="quarterly")` returns `{"2025-Q3": cap}`, and `_weight_at` looks a
    member's weight up BY THE BUCKET KEY. Bucketing the points by year while the caps are keyed by
    quarter means every lookup misses, every member is dropped from every period, and the series
    comes back EMPTY — not thin, empty. Measured 2026-08-12 on the AEX quarterly Revenue benchmark:
    22 constituents, 639 rows, `contributing: 22`, and **zero** points out, while the drill-down
    table beside it showed 84–93% of the index reporting every quarter. The card then blamed the
    coverage floor, which had never run.

    ⚠ AND IT IS A CALENDAR QUARTER, deliberately the same derivation as `_ttm_by_period`'s label
    (`(month - 1) // 3 + 1`), because that is what `period_caps_eur` emits. An off-calendar filer's
    Q3 is whatever quarter its period-end falls in — the same convention as the annual bucket,
    which puts a March year-end in the calendar year it ends in.
    """
    return f"{d[:4]}-Q{(int(d[5:7]) - 1) // 3 + 1}"


def period_end(period: str) -> str:
    """A bucket key → the date it ends on. `2025` → `2025-12-31`, `2025-Q3` → `2025-09-30`.

    ⚠ A CONVENTION, AND THE ONLY HONEST ONE AVAILABLE. Members close their books on different days,
    which is why the blend aligns on a shared period rather than a raw date — so no blended point
    belongs to one real date. The period's own calendar end is the least surprising stand-in, it
    keeps a year's four quarterly points in order on the axis, and it is what `carry_forward`
    measures its staleness bound against.
    """
    # ⚠⚠ `LTM` IS A PERIOD, NOT A DATE, AND PARSING IT AS ONE RAISES. Split on `-Q` it yields
    # `LTM-12-31`, which `carry_forward` hands to `date.fromisoformat` — a ValueError that takes the
    # whole blend down, not a wrong number. Its window ends at the newest filing, which is on or
    # before today, so today is the honest bound: it never carries backwards into a real period and
    # nothing is ever carried INTO it (it is the last bucket on the axis).
    if period == "LTM":
        return _date.today().isoformat()
    head, _, q = period.partition("-Q")
    return f"{head}-{['03-31', '06-30', '09-30', '12-31'][int(q) - 1]}" if q else f"{head}-12-31"


def carry_forward(by_period: dict[str, tuple[str, float]],
                  axis: list[str]) -> dict[str, tuple[float, bool]]:
    """`{period: (value, reported)}` over `axis` — each period's own figure, or the latest one
    before it, with `reported` saying which.

    ⚠⚠ THE CARRY IS WHAT MAKES THE CONTRIBUTOR SET STABLE, and a stable set is the whole point. A
    company that files semi-annually has no trailing-twelve-month point in Q1 — but its TTM revenue
    at Q1 IS its December figure; that is what "trailing" means. Without the carry it simply drops
    out of Q1, the index alternates between two different baskets, and the line sawtooths ±20% on
    composition alone (measured on the AEX: 277 → 341 → 297 → 382).

    ⚠⚠ AND `reported` IS WHY THIS IS SAFE. A carried value is used for the AVERAGE and counts for
    NOTHING in the coverage, so the floor still sees the newest fiscal year for what it is — a
    handful of filers and everyone else held at last year's figure — and still refuses it. Merge
    the two and the floor is defeated by the very mechanism that smooths the line.

    ⚠ BOUNDED. A constituent that stops reporting (delisted, acquired, or simply never filed again)
    must fall out rather than be held at a frozen value for the rest of the axis. One year is the
    natural bound: it is the longest any live filer goes between reports, so nothing that is still
    reporting is ever dropped.
    """
    out: dict[str, tuple[float, bool]] = {}
    last: tuple[str, float] | None = None
    for period in axis:
        own = by_period.get(period)
        if own is not None:
            last = own
            out[period] = (own[1], True)
            continue
        if last is None:
            continue                     # nothing to carry yet — before this member's first report
        if (_date.fromisoformat(period_end(period))
                - _date.fromisoformat(last[0])).days > _MAX_CARRY_DAYS:
            continue                     # stale beyond the bound — this member is out of the period
        out[period] = (last[1], False)
    return out


def _latest_per_bucket(pts: dict[str, float],
                       bucket=year_bucket) -> dict[str, tuple[str, float]]:
    """One row per PERIOD -> {period: (date, value)}. A member reporting twice in a period (a
    year-end change) keeps its LATEST close rather than being counted twice."""
    latest: dict[str, tuple[str, float]] = {}
    for d, v in pts.items():
        key = bucket(d)
        if key not in latest or d > latest[key][0]:
            latest[key] = (d, v)
    return latest


# The old name, kept because three call sites in `earnings.py` read it as "one point per year".
_latest_per_year = _latest_per_bucket


def _prepare(members: list[dict], kind: str, bucket=year_bucket) -> tuple[list[dict], list[dict]]:
    """Members split into those that can contribute and those that cannot, with the REASON.

    ⚠ SHARED BY `blend_series` AND `blend_breakdown` ON PURPOSE. A drill-down that re-derives
    "the same way" is a second copy of these rules, and the copy is what drifts — a panel that
    explains a number the line does not show is worse than no panel, because it is checked once
    and believed thereafter. One preparation, two readers.

    `bucket` is the period alignment — `year_bucket` (default) or `quarter_bucket`. It travels with
    the weights: see `quarter_bucket` for what happens when the two disagree.
    """
    ok: list[dict] = []
    dropped: list[dict] = []
    for i, m in enumerate(members):
        w = abs(float(m.get("weight") or 0))
        pts = {d: float(v) for d, v in (m.get("points") or {}).items() if v is not None}
        if not w:
            dropped.append({"index": i, "weight": w, "reason": "no_weight"})
            continue
        if not pts:
            dropped.append({"index": i, "weight": w, "reason": "no_data"})
            continue
        raw = dict(pts)
        if kind == "level":
            # ⚠ The anchor comes from `base_points` when given — see the docstring. Falling back to
            # this series' own first point is right for a standalone level and WRONG for a
            # forecast, which would restart at 100 beside an actual that has run to 1,800.
            anchor = {d: float(v) for d, v in (m.get("base_points") or {}).items()
                      if v is not None} or pts
            # ⚠⚠ THE FIRST **POSITIVE** PERIOD, NOT THE FIRST REPORTED ONE. 100 × v/0 is undefined
            # and a negative base flips every later point's sign — but a leading ZERO on a flow line
            # is almost never a measurement. GuruFocus back-fills the years before a company existed
            # separately: Universal Music (2.68% of the AEX) sits inside Vivendi until the 2021
            # spin-off and its 2017 revenue is stored as `0`, which anchored the rebase on nothing
            # and threw away nine good years (2018-2025, 6,023 → 12,507). Prosus carries the same
            # artefact at 2017 and 2018 on its quarterly line. Skipping to the first positive period
            # keeps the member and starts its curve where its history really starts.
            positive = [d for d in sorted(anchor) if anchor[d] > 0]
            if not positive:
                dropped.append({"index": i, "weight": w, "reason": "non_positive_base"})
                continue
            base_date = positive[0]
            base = anchor[base_date]
            # ⚠ AND ITS PRE-BASE PERIODS GO WITH IT. A zero before the anchor would rebase to 0 and
            # read as a company that lost everything, rather than one that had not started.
            pts = {d: v for d, v in pts.items() if d >= base_date}
            if not pts:
                dropped.append({"index": i, "weight": w, "reason": "no_data"})
                continue
            pts = {d: 100.0 * v / base for d, v in pts.items()}
        ok.append({"index": i, "weight": w, "weights": m.get("weights"),
                   "points": pts, "raw": raw,
                   # ⚠ WHETHER THIS MEMBER IS A CONTINUATION — rebased on the series it EXTENDS
                   # (`base_points`) rather than on its own first point. The level chain needs it:
                   # a continuation must not restart the index at 100. See `blend_series`.
                   "continues": bool(m.get("base_points")),
                   "by_year": _latest_per_bucket(pts, bucket),
                   "raw_by_year": _latest_per_bucket(raw, bucket)})
    return ok, dropped


def member_scale(at: dict[str, float]) -> float:
    """A member's own typical magnitude — the median |value| across the periods it contributes.

    ⚠ MEDIAN, NOT MEAN. The thing being measured against is an outlier, and a mean is moved by the
    very outlier it is supposed to identify: Prosus's own values run 0.0090 … 0.70, and one of them
    is the artefact. The median is the figure the series actually lives at.

    ⚠ COMPUTED ON THE **REBASED** VALUES, WHICH IS SAFE BECAUSE THE TEST IS A RATIO. `_prepare`
    scales a level member by a per-member constant (100/base); it divides out of `prev ÷ scale`, so
    this needs neither the raw series nor a currency.
    """
    vals = [abs(v) for v in at.values()]
    return median(vals) if vals else 0.0


def step_growth(prev: float | None, now: float | None, scale: float) -> float | None:
    """One member's growth over one interval — or None when it has none to give.

    ⚠⚠ THE ONE DEFINITION, READ BY BOTH THE LINE (`blend_series`) AND THE PANEL THAT EXPLAINS IT
    (`_level_breakdown`). They each derive `prev`/`now` their own way, from their own member lists,
    and used to apply the rule twice — so a breakdown could attribute a −2,700% move to a holding
    the line no longer moved on. The client's twin in `HoldingsRevenueModal` mirrors this exactly.

    Three refusals and a floor, in order:

    * NO ANCHOR / NO VALUE — the member cannot span this interval. It sits out THIS step and joins
      at the next; it is never dropped from the metric.
    * A NON-POSITIVE ANCHOR — there is no ratio to a zero or a negative.
    * AN IMMATERIAL ANCHOR — see `_MIN_STEP_BASE_FRACTION`. This is the one that stops a single
      near-break-even holding turning an index inside out.
    * FLOORED AT −100%. ⚠ BELOW ZERO THERE IS NO SCALE. A per-share figure of −0.24 against a base
      of +0.30 is not "180% worse" in any sense an INDEX can carry: an index is a product of
      (1 + g), so a term below −1 does not make it small, it makes it NEGATIVE — and a negative
      index is not a low reading, it is not an index at all. −100% is the most a level can lose, so
      that is what a member that has gone to or below zero contributes. It costs a real distinction
      (−150% and −400% both read as −100%) and buys the structural guarantee that the line cannot
      cross zero, which is the only reason the log axis can be trusted to be showing everything.
    """
    if prev is None or now is None or prev <= 0:
        return None
    if prev < _MIN_STEP_BASE_FRACTION * scale:
        return None
    growth = now / prev - 1.0
    # ⚠ AN IMPLAUSIBLE RESULT — see `_MAX_STEP_GROWTH`. The mirror of the base test above: that one
    # asks whether the divisor is big enough to divide by, this one whether the answer is small
    # enough to have come from a business rather than from a unit.
    if growth > _MAX_STEP_GROWTH:
        return None
    return max(growth, -1.0)


def _weight_at(m: dict, period: str) -> float | None:
    """This member's weight IN THIS PERIOD, or None when it has none and is left out of it.

    ⚠ TWO BASES, ONE FUNCTION, AND `None` MEANS SOMETHING DIFFERENT FROM 0. A universe carries
    `weights` — the market cap as at each fiscal period, so the weighting is the index's own at the
    time rather than today's applied backwards. A PORTFOLIO does not: a holding weight is not a
    market cap and has no history here, so the scalar applies to every period. The absence of
    `weights` is therefore the signal for "single basis", which is why it must be `None` and not an
    empty dict.

    ⚠ A MEMBER WITH PER-PERIOD WEIGHTS BUT NO CAP THIS PERIOD IS DROPPED FROM THIS PERIOD ONLY, and
    NOT fallen back to the scalar. Mixing the two bases inside one column would weight some
    constituents by their 2018 cap and others by today's, with nothing on screen to tell them
    apart — the failure this whole change exists to remove, reintroduced one row at a time.
    """
    ws = m.get("weights")
    if ws is None:
        return m.get("weight")
    w = ws.get(period)
    if w:
        return w
    # ⚠ AS-OF, NOT EXACT-MATCH. A market cap is a STOCK: the last one filed is the current one
    # until a newer one exists, so a period we have no cap FOR is weighted by the newest cap we
    # have BEFORE it. Without this the current year is unweighted for months — measured on the AEX,
    # only 1 of 22 constituents had a 2026 cap, so 2026-Q1 weighted one company and 2026-Q2 none.
    # ⚠ The keys sort correctly for both vocabularies (`2025` < `2026`, `2025-Q3` < `2025-Q4`),
    # which is why the buckets are formatted the way they are.
    earlier = [k for k in ws if k <= period and ws[k]]
    return ws[max(earlier)] if earlier else None


def blend_series(members: list[dict], metric_code: str, bucket=year_bucket) -> dict:
    """`members` = [{weight, points: {date: value}, base_points?}] -> one blended series.

    `bucket` aligns the members onto shared periods — `year_bucket` (default) or `quarter_bucket`
    for a trailing-twelve-month series. ⚠ IT MUST MATCH HOW `weights` IS KEYED; see `quarter_bucket`
    for the empty series that results when it does not.

    `base_points` (optional, LEVELS only) is the series this one continues — a forecast passes the
    ACTUAL it extends, so both are rebased on the same anchor and the forecast picks up where the
    actual stops instead of restarting at 100.

    Returns `{kind, points: [{date, value, covered_pct, covered_names_pct}], covered_pct}` —
    `covered_pct` is the share of the blended weight that REPORTED that period and
    `covered_names_pct` the share of the members that did.

    ⚠⚠ THE THREE RULES THIS FUNCTION IMPLEMENTS, and they only work together:

      1. WEIGHT — a member's weight in a period is its cap for that YEAR over the sum of that
         year's caps (`period_caps_eur` spreads one annual cap across the year's quarters), taken
         AS-OF when this year is not filed yet (`_weight_at`).
      2. CARRY — a member's latest reported figure applies until it reports a newer one, bounded to
         `_MAX_CARRY_DAYS`. Without it the set of contributors CHANGES period to period and the
         line alternates between two different baskets: measured 2026-08-12, the AEX quarterly
         revenue index read 277 → 341 → 297 → 382 → 338 → 402, a ±20% sawtooth that is composition,
         not revenue. Q1/Q3 was the 12 constituents that file quarterly; Q2/Q4 the 21 that file at
         Jun/Dec.
      3. FLOOR — a period draws only when at least half the WEIGHT **and** half the NAMES actually
         reported it. ⚠ A CARRIED VALUE DOES NOT COUNT TOWARD EITHER, which is what stops rule 2
         defeating the floor: the newest fiscal year, where a handful have filed and everyone else
         is carried, still reads 13% covered and is still refused rather than drawn as a flat line
         of last year's figures.

    ⚠ THE TWO FLOORS CATCH DIFFERENT THINGS AND NEITHER IS ENOUGH ALONE. Weight-only lets one giant
    carry a period: measured on the AEX, 2026-Q2 had **2 of 22** constituents reporting and passed
    at 53.8% of cap because ASML is enormous. Names-only would let ten tiny constituents outvote a
    missing giant.
    """
    kind = blend_kind(metric_code)
    total_w = sum(abs(float(m.get("weight") or 0)) for m in members)
    total_n = len(members)
    if total_w <= 0 or not total_n:
        return {"kind": kind, "points": [], "covered_pct": 0.0}

    prepared, _ = _prepare(members, kind, bucket)
    by_date: dict[str, list[tuple[float, float]]] = defaultdict(list)
    # ⚠⚠ COVERAGE IS MEASURED ON THE **STABLE** WEIGHT, NOT THE PER-PERIOD ONE, AND GETTING THIS
    # WRONG DISABLES THE FLOOR ENTIRELY.
    #
    # The per-period market cap is the right basis for the AVERAGE and the wrong one for a
    # completeness measure, because it comes out of the same GuruFocus blob as the figure itself:
    # a company that has not filed FY2026 has no FY2026 market cap either. Summing per-period caps
    # on both sides therefore divides the filers by the filers — coverage reads ~100% in exactly
    # the period where almost nobody has reported.
    #
    # Measured on the S&P: FY2026 read **13.4%** covered on the stable basis (correctly under the
    # floor — under the 80 of the day and under today's 50 alike, so the chart omitted it) and
    # **100.0%** on the per-period one — which drew a full
    # -height point built almost entirely out of NVIDIA, in the same ink as a year every
    # constituent reported. That is the exact failure `MIN_BLEND_COVERAGE_PCT` exists to prevent.
    #
    # So: numerator and denominator both use `weight` — one basis, internally consistent, and
    # present whether or not the company reported. The two quantities answer different questions
    # and are allowed to use different bases; each has to be consistent with ITSELF.
    cover_w: dict[str, float] = defaultdict(float)
    cover_n: dict[str, int] = defaultdict(int)
    # The axis every member is carried across — the union of what anybody reported.
    axis = sorted({k for p in prepared for k in p["by_year"]})
    for p in prepared:
        # ⚠ THE VALUE THIS MEMBER CONTRIBUTED AT EACH PERIOD, kept so the LEVEL path can take a
        # ratio between two periods that need not be adjacent — see the chaining below. Carried
        # periods are in it: a member that has not reported since still holds its last figure, so
        # its growth over the interval is correctly zero rather than absent.
        p["at"] = {}
        for period, (v, reported) in carry_forward(p["by_year"], axis).items():
            w = _weight_at(p, period)
            if not w:
                continue
            p["at"][period] = v
            by_date[period].append((abs(float(w)), v))
            if reported:
                cover_w[period] += abs(float(p.get("weight") or 0))
                cover_n[period] += 1
        # ⚠ ONCE PER MEMBER, NOT ONCE PER STEP. It is the same figure at every interval — the
        # member's own typical magnitude — and the level chain asks for it O(periods x members)
        # times. Computed here, where `at` has just been filled, so the two cannot fall out of step.
        p["scale"] = member_scale(p["at"])

    def _clears(d: str) -> bool:
        return (100.0 * cover_w[d] / total_w >= MIN_BLEND_COVERAGE_PCT
                and 100.0 * cover_n[d] / total_n >= MIN_BLEND_COVERAGE_NAMES_PCT)

    def _point(d: str, value: float) -> dict:
        return {"period": d, "value": round(value, 6),
                "covered_pct": round(100.0 * cover_w[d] / total_w, 2),
                "covered_names_pct": round(100.0 * cover_n[d] / total_n, 2)}

    out: list[dict] = []
    if kind == "level":
        # ⚠⚠ A LEVEL SERIES IS CHAINED FROM WEIGHTED **GROWTH**, NOT AVERAGED FROM REBASED LEVELS.
        # Between two drawn points the index moves by the cap-weighted average of what its
        # constituents actually did over exactly that interval:
        #
        #     index[p] = index[anchor] x (1 + Σ w_i·g_i / Σ w_i),  g_i = v_i(p)/v_i(anchor) − 1
        #
        # Averaging rebased levels instead makes the line an artefact of WHEN each member's history
        # starts: every member is 100 at its own first period, so a constituent joining the panel
        # drags the average toward 100 and the index "moves" because the composition changed. Growth
        # has no such anchor — a member simply has no growth for a step it cannot span, and
        # contributes from the next one.
        #
        # ⚠ AND IT NEEDS NO POSITIVE BASE. A member whose earlier value is <= 0 has no meaningful
        # ratio, so it sits out THAT STEP and joins at the next — instead of being dropped from the
        # metric entirely. Universal Music's fabricated 2017 zero costs it one step, not nine years.
        #
        # ⚠ THE ANCHOR IS THE LAST **DRAWN** POINT, NOT THE PREVIOUS PERIOD. A period that fails the
        # floor is not drawn, and measuring the next step from it would compound a move nobody could
        # see; measuring from the last honest point means no constituent's growth is lost and no
        # thin period leaks into the level.
        anchor: str | None = None
        level = 100.0
        for d in sorted(by_date):
            if not _clears(d):
                continue
            if anchor is None:                    # the first honest period IS the base
                anchor = d
                # ⚠⚠ UNLESS THE SERIES IS A CONTINUATION, IN WHICH CASE 100 IS A FAKE COLLAPSE.
                # A forecast is rebased on the ACTUAL it extends (`base_points`), so its value here
                # is already an index against that base — 1,100 for an actual that ran to 1,000.
                # Stamping the chain's usual 100 discards it and draws the forecast restarting at
                # 100 beside an actual at 1,000: a ~90% earnings collapse that exists only in the
                # arithmetic, at full confidence, on a log axis. Measured on a real book at 1,808.
                #
                # ⚠ ONLY WHEN **EVERY** CONTRIBUTOR CONTINUES SOMETHING. Mixing a continuation with
                # a self-anchored member would average an index-against-the-actual with an
                # index-against-itself — two different bases in one number, which is the error this
                # whole level path exists to refuse. Anything mixed falls back to 100.
                here = [p for p in prepared if p["at"].get(d) is not None and _weight_at(p, d)]
                if here and all(p.get("continues") for p in here):
                    carried = _weighted_arithmetic(
                        [(abs(float(_weight_at(p, d))), p["at"][d]) for p in here])
                    if carried is not None:
                        level = carried
                out.append(_point(d, level))
                continue
            # ⚠ ONE RULE, IN ONE PLACE — `step_growth`. The guard that used to live inline here
            # (`prev > 0`) missed the near-zero base, which is the failure that deletes a chart.
            pairs = [(abs(float(w)), g)
                     for p in prepared
                     for w in [_weight_at(p, d)]
                     for g in [step_growth(p["at"].get(anchor), p["at"].get(d), p["scale"])]
                     if w and g is not None]
            step = _weighted_arithmetic(pairs)
            if step is None:
                continue                          # nothing spans this interval — no honest move
            # ⚠ EVERY CONSTITUENT WIPED OUT. `step_growth` floors each member at −100%, so this can
            # only be an exact −1: the whole panel went to or below zero over one interval. The
            # index is 0 from here and would stay 0 for ever — points a LOG axis cannot draw, which
            # is precisely the silent truncation this guard exists to end. Ending the series is the
            # honest form of that: a line that STOPS is visible, where a run of unplottable zeroes
            # is not. (It is a backstop, not a path anything reaches today — the materiality bar
            # above is what keeps a single member from getting anywhere near it.)
            if 1.0 + step <= 0:
                break
            level *= 1.0 + step
            out.append(_point(d, level))
            anchor = d
    else:
        combine = _weighted_harmonic if kind == "multiple" else _weighted_arithmetic
        for d in sorted(by_date):
            value = combine(by_date[d])
            if value is None or not _clears(d):
                continue    # ⚠ omitted, never drawn as a dip — see the docstring
            out.append(_point(d, value))
    spanned = max((p["covered_pct"] for p in out), default=0.0)
    return {"kind": kind, "points": out, "covered_pct": round(spanned, 2)}


def explain_empty(members: list[dict], metric_code: str, bucket=year_bucket) -> dict | None:
    """Why `blend_series` drew NOTHING even though holdings carry this metric — or None when none
    of them do.

    ⚠ TAKES THE SAME `bucket` AS THE SERIES IT EXPLAINS. A diagnostic that aligned the periods
    differently from the run it is explaining would report a different set of periods from the one
    that drew nothing — which is how "no year clears the floor" came to be printed for a series
    whose periods never existed.

    ⚠ "NO SERIES" AND "NOT INGESTED" ARE DIFFERENT ANSWERS AND THE UI CANNOT TELL THEM APART.
    An empty chart shows one thing; the two reasons behind it are opposites. Measured on a real
    book's Dividends per Share: every holding had the line, and the portfolio card still read "No
    dividend/share ingested for this portfolio" — because a level series is rebased to 100 at its
    first observation and a dividend series that starts at 0.00 has `base <= 0`, so member after
    member was dropped (`non_positive_base`) until no year cleared the coverage floor. Telling the
    reader to go ingest data they already have is worse than saying nothing.

    Returns the FACTS, never a sentence — the caller words it:
        reporting/reporting_pct   members carrying this metric, and their share of blended weight
        contributing              of those, how many survived preparation (rebasing etc.)
        dropped                   {reason: count} — `non_positive_base` | `no_weight` | `no_data`
        best_covered_pct          the best year's coverage, against `floor_pct`
        years / years_below_floor / years_no_value
    """
    kind = blend_kind(metric_code)
    total_w = sum(abs(float(m.get("weight") or 0)) for m in members)
    reporting = [m for m in members
                 if any(v is not None for v in (m.get("points") or {}).values())]
    if not reporting:
        return None                         # nothing carries it — that IS "not ingested"

    prepared, dropped = _prepare(members, kind, bucket)
    combine = _weighted_harmonic if kind == "multiple" else _weighted_arithmetic
    by_year: dict[str, list[tuple[float, float]]] = defaultdict(list)
    # The same stable basis `blend_series` measures coverage on — see the ⚠⚠ there. A diagnostic
    # that explained a floor decision using a different denominator from the one that made it
    # would send the reader after the wrong cause.
    cover_w: dict[str, float] = defaultdict(float)
    # ⚠ THE SAME CARRY AS THE SERIES IT EXPLAINS. A diagnostic that aligned or carried differently
    # from the run it is explaining reports a different set of periods from the one that drew
    # nothing — which is how "no year clears the floor" came to be printed for a series whose
    # periods never existed.
    axis = sorted({k for p in prepared for k in p["by_year"]})
    for p in prepared:
        for year, (v, reported) in carry_forward(p["by_year"], axis).items():
            w = _weight_at(p, year)
            if w:
                by_year[year].append((abs(float(w)), v))
                if reported:
                    cover_w[year] += abs(float(p.get("weight") or 0))

    best = 0.0
    below = no_value = 0
    for year, pairs in by_year.items():
        covered = 100.0 * cover_w[year] / total_w if total_w > 0 else 0.0
        best = max(best, covered)
        if combine(pairs) is None:
            no_value += 1
        elif covered < MIN_BLEND_COVERAGE_PCT:
            below += 1

    counts: dict[str, int] = {}
    for d in dropped:
        counts[d["reason"]] = counts.get(d["reason"], 0) + 1
    return {
        "kind": kind,
        "reporting": len(reporting),
        "reporting_pct": round(100.0 * sum(abs(float(m.get("weight") or 0)) for m in reporting)
                               / total_w, 2) if total_w > 0 else 0.0,
        "contributing": len(prepared),
        "dropped": counts,
        "best_covered_pct": round(best, 2),
        "floor_pct": MIN_BLEND_COVERAGE_PCT,
        "years": len(by_year),
        "years_below_floor": below,
        "years_no_value": no_value,
    }


def blend_matrix(members: list[dict], metric_code: str) -> dict:
    """The full audit grid: every holding's value at every period, with the blended line under it.

    Returns `{kind, metric_code, periods, members, blended, covered, below_floor, excluded}`:
      periods      sorted fiscal years that any holding reports.
      members      [{isin, name, weight_pct, cells:{period:{value, raw, dropped}}}] — one row per
                   holding, sorted by weight. `dropped` marks a cell the blend threw away (a
                   loss-making negative multiple), so the reader sees the number AND that it did
                   not count. `raw` is the as-reported amount for a rebased LEVEL, else = value.
      blended      {period: value|null} — the harmonic (multiple) or arithmetic aggregate.
      covered      {period: pct of blended weight reporting}.
      below_floor  {period: covered < MIN_BLEND_COVERAGE_PCT} — the years the CHART hides. The
                   matrix still shows them (flagged), because a thin year is exactly what a reader
                   verifying the line wants to see, not have silently dropped.
      excluded     holdings with no usable data for this metric at all (no_data / no_weight / …).

    ⚠ REUSES `_prepare`, LIKE `blend_series` AND `blend_breakdown`. The cells and the footer come
    from the same preparation the chart's line does, so the grid cannot show a number the line was
    not built from — the whole point of an audit view.
    """
    kind = blend_kind(metric_code)
    total_w = sum(abs(float(m.get("weight") or 0)) for m in members)
    prepared, dropped = _prepare(members, kind)

    def _label(i: int) -> dict:
        m = members[i]
        return {"isin": m.get("isin"), "name": m.get("name"),
                "weight_pct": round(100.0 * abs(float(m.get("weight") or 0)) / total_w, 2)
                if total_w > 0 else 0.0}

    periods = sorted({y for p in prepared for y in p["by_year"]})

    rows = []
    for p in prepared:
        cells: dict[str, dict] = {}
        for y, (_d, v) in p["by_year"].items():
            raw = p["raw_by_year"].get(y)
            cells[y] = {
                "value": round(v, 6),
                "raw": round(raw[1], 6) if raw else None,
                # A non-positive multiple has no reciprocal, so the harmonic blend drops it — show
                # the figure but mark that it did not contribute (vs an empty "did not report").
                "dropped": bool(kind == "multiple" and not (v and v > 0)),
            }
        rows.append({**_label(p["index"]), "cells": cells})
    rows.sort(key=lambda r: r["weight_pct"], reverse=True)

    # ⚠⚠ THE FOOTER IS THE LINE ITSELF, ASKED FOR RATHER THAN RE-DERIVED. It used to recompute the
    # aggregate here — a second implementation of the blend, in the one view whose entire job is to
    # let a reader check the first. The day the LEVEL path became a chained weighted-growth series
    # (see `blend_series`) this copy went on averaging rebased levels, so the audit grid's footer
    # disagreed with the chart above it by construction. One call, and they cannot drift again.
    #
    # ⚠ IT ALSO BRINGS THE CARRY AND THE NAMES FLOOR WITH IT. Recomputing here missed both: a
    # semi-annual filer dropped out of the periods it did not file in, and a period two giants
    # carried on their own passed a weight-only floor.
    series = blend_series(members, metric_code)
    pts = {p["period"]: p for p in series["points"]}
    blended = {y: (pts[y]["value"] if y in pts else None) for y in periods}
    covered = {y: (pts[y]["covered_pct"] if y in pts
                   else round(100.0 * sum(p["weight"] for p in prepared
                                          if y in p["by_year"]) / total_w, 2)
                   if total_w > 0 else 0.0)
               for y in periods}
    # A period the chart does not draw — under either floor, or with nothing spanning the interval.
    below_floor = {y: y not in pts for y in periods}

    excluded = [{**_label(d["index"]), "reason": d["reason"]} for d in dropped]
    excluded.sort(key=lambda r: r["weight_pct"], reverse=True)
    return {"kind": kind, "metric_code": metric_code, "periods": periods,
            "members": rows, "blended": blended, "covered": covered,
            "below_floor": below_floor, "excluded": excluded}


def merge_relative_growth(price_bd: dict, oe_bd: dict, period: str) -> dict:
    """Merge a PRICE level-breakdown and an Owner-Earnings level-breakdown (same period) into the
    price-vs-OE table behind the Share-Price-vs-Owner-Earnings chart.

    Both inputs are `blend_breakdown(..., 'level')` outputs, so each holding's `value` is its growth
    index (100 at its own first year) and `raw_value` the amount as reported. Per holding we keep
    both indices and their ratio — price ÷ OE, i.e. how much its earnings multiple has expanded.

    ⚠ THE RATIO IS THE CHART'S MESSAGE, AND IT IS INVARIANT to the extra rebasing the chart does to
    put both lines on a common start: that normalisation scales price and OE lines by the same
    per-line constant, so price ÷ OE is unchanged. Both the raw amount and the index are surfaced,
    so the number is verifiable whichever way the reader checks it.

    ⚠ REUSES `blend_breakdown` TWICE rather than re-deriving — the two lines a reader is comparing
    are decomposed by the exact rule the chart's lines are built from.
    """
    def _key(m: dict) -> str:
        return (m.get("isin") or "") or (m.get("name") or "")

    by_key: dict[str, dict] = {}
    for m in price_bd.get("members", []):
        r = by_key.setdefault(_key(m), {"isin": m.get("isin"), "name": m.get("name"),
                                        "weight_pct": m.get("weight_pct")})
        r["price_index"] = m.get("value")
        r["price_raw"] = m.get("raw_value")
    for m in oe_bd.get("members", []):
        r = by_key.setdefault(_key(m), {"isin": m.get("isin"), "name": m.get("name"),
                                        "weight_pct": m.get("weight_pct")})
        r["oe_index"] = m.get("value")
        r["oe_raw"] = m.get("raw_value")

    rows = []
    for r in by_key.values():
        pi, oi = r.get("price_index"), r.get("oe_index")
        r["ratio"] = round(pi / oi, 6) if (pi and oi and oi > 0) else None
        rows.append(r)
    rows.sort(key=lambda r: -(r.get("weight_pct") or 0))

    pv, ov = price_bd.get("value"), oe_bd.get("value")
    return {
        "period": period,
        "price": {"value": pv, "covered_pct": price_bd.get("covered_pct")},
        "oe": {"value": ov, "covered_pct": oe_bd.get("covered_pct")},
        "ratio": round(pv / ov, 6) if (pv and ov and ov > 0) else None,
        "members": rows,
        # A holding with no price series at all (the anchor line) is the one worth naming.
        "excluded": price_bd.get("excluded", []),
    }


def _level_breakdown(members: list[dict], metric_code: str, period: str, prepared: list[dict],
                     reporting: list[dict], excluded: list[dict], total_w: float,
                     _label) -> dict:
    """A LEVEL point, decomposed into the holdings that MOVED it.

    ⚠⚠ A LEVEL'S VALUE IS NOT A SUM OF ANYTHING ANY MORE, SO IT CANNOT BE SHARED OUT. The line is
    chained (`index[p] = index[anchor] × (1 + Σ w·g / Σ w)`), so its LEVEL at a period is a
    cumulative product and no set of per-member numbers can add up to it. What is decomposable is
    the STEP into the period — and that is the more useful question anyway: "who moved it", not
    "who is in it".

        growth_pct        this member's own change over the interval, v(p)/v(anchor) − 1
        contribution_pp   w·g ÷ Σw, in percentage POINTS of the step — these sum to `step_pct`
        swing             the step WITHOUT this member, in pp (leave-one-out)

    ⚠ THE ANCHOR IS THE PREVIOUS **DRAWN** PERIOD, taken from `blend_series` rather than assumed to
    be the previous column: a period under the floor is not drawn, and a decomposition measured
    over a different interval from the one the chart moved over would not reconcile with it.

    ⚠ `share_pct` IS NULL HERE, DELIBERATELY. A share of a step is unbounded — when the step is
    near zero a 0.1pp contributor reads as 400% of it, and a member that moved the other way reads
    negative. `contribution_pp` says the same thing in a unit that stays readable, and the caller
    renders that instead.
    """
    series = blend_series(members, metric_code)
    pts = {p["period"]: p for p in series["points"]}
    order = [p["period"] for p in series["points"]]
    anchor = order[order.index(period) - 1] if period in pts and order.index(period) > 0 else None

    rows: list[dict] = []
    if anchor is not None:
        # ⚠⚠ A MEMBER THAT CANNOT SPAN THE INTERVAL STAYS IN THE TABLE, WITH NO GROWTH. It reported
        # this period — it is behind the line's LEVEL — it simply has nothing to have moved FROM
        # (it did not report the anchor, or reported a non-positive figure there). Excluding it
        # would quietly shrink every consumer of this payload: `merge_relative_growth` builds the
        # price-vs-owner-earnings table by pairing two of these, and a holding present in one and
        # absent from the other loses its ratio for reasons that have nothing to do with it.
        contrib = []
        for p in reporting:
            w = _weight_at(p, period)
            at = p.get("at") or {k: v for k, (_d, v) in p["by_year"].items()}
            # ⚠ THE SAME `step_growth` THE LINE USES, INCLUDING THE MATERIALITY BAR AND THE −100%
            # FLOOR. Re-deriving "the same way" here is how a panel comes to attribute a −2,700%
            # move to a holding the chart above it no longer moved on — and this panel is checked
            # once and believed thereafter.
            g = step_growth(at.get(anchor), at.get(period), p.get("scale", member_scale(at)))
            contrib.append((p, abs(float(w or 0)), g if w else None))
        # ⚠ THE DENOMINATOR IS THE MEMBERS THAT MOVED, not everyone in the table — a member with no
        # growth to measure must not dilute the step toward zero. It appears with nulls; it is not
        # counted as 0%.
        moved = [(p, w, g) for p, w, g in contrib if g is not None]
        den = sum(w for _p, w, _g in moved)
        step = 100.0 * sum(w * g for _p, w, g in moved) / den if den else None
        for p, w, g in contrib:
            # ⚠ LEAVE-ONE-OUT BY IDENTITY, not by index — `moved` is a subset of `contrib`, so
            # positions do not line up and an index test would drop the wrong member.
            others = [(q_w, q_g) for q_p, q_w, q_g in moved if q_p is not p]
            od = sum(x for x, _ in others)
            without = 100.0 * sum(x * y for x, y in others) / od if od else None
            rows.append({
                **_label(p["index"]),
                "value": round(p["by_year"][period][1], 6) if period in p["by_year"] else None,
                "raw_value": round(p["raw_by_year"][period][1], 6)
                if period in p["raw_by_year"] else None,
                # ⚠ NULL, NEVER 0. A member with nothing to move from did not grow by zero — it has
                # no growth to state, and a 0.0% would read as "flat" and drag the eye to a holding
                # that simply has no prior figure.
                "growth_pct": round(100.0 * g, 4) if g is not None else None,
                "contribution_pp": round(100.0 * w * g / den, 4) if (g is not None and den) else None,
                "share_pct": None,
                "swing": round(step - without, 4)
                if (g is not None and step is not None and without is not None) else None,
            })
        rows.sort(key=lambda r: abs(r["contribution_pp"] or 0), reverse=True)
    else:
        step = None
    covered = pts.get(period, {}).get("covered_pct", 0.0)
    excluded.sort(key=lambda r: r["weight_pct"], reverse=True)
    return {"kind": "level", "metric_code": metric_code, "period": period,
            # The line's own level, so the panel can still show what it is decomposing the move OF.
            "value": pts.get(period, {}).get("value"),
            "anchor": anchor, "step_pct": round(step, 4) if step is not None else None,
            "covered_pct": covered, "excluded_pct": round(100.0 - covered, 2),
            "members": rows, "excluded": excluded}


def blend_breakdown(members: list[dict], metric_code: str, period: str) -> dict:
    """ONE blended point, decomposed into the holdings behind it.

    Returns `{kind, period, value, covered_pct, members: [...], excluded: [...]}`.

    ⚠⚠ "CONTRIBUTION" IS NOT ONE NUMBER, AND THE OBVIOUS ONE IS WRONG FOR A MULTIPLE.
        `w x v / Σw` is the additive share of an ARITHMETIC mean. A multiple is combined
        harmonically, where the additive quantity is the RECIPROCAL — the earnings yield, not the
        P/E. Reporting `w x PE / Σw` beside a harmonic line gives components that do not sum to
        it, and the gap grows with dispersion: exactly the book where someone would look. So
        `share_pct` is computed in the space the metric is actually combined in, and it sums to
        100% by construction in all three cases.

    ⚠ AND A SHARE IS NOT AN INFLUENCE. A 10% holding at a wild multiple and a 10% holding at the
        average both carry ~10% of the weight; only one MOVES the number. `swing` is the
        leave-one-out delta — what the line would read without this holding — in the metric's own
        displayed unit. It answers "who is doing this to my portfolio", which a share cannot.
        The two disagree constantly, and that is the point of showing both.

    ⚠ THE EXCLUSIONS ARE HALF THE ANSWER. A holding absent from a period is not a zero, and the
        reason it is absent is the difference between "has not reported yet" and "reported a loss
        and a negative multiple was dropped". Both are returned with the weight they take out of
        the denominator, so a thin point can be recognised as thin.
    """
    kind = blend_kind(metric_code)
    combine = _weighted_harmonic if kind == "multiple" else _weighted_arithmetic
    total_w = sum(abs(float(m.get("weight") or 0)) for m in members)
    prepared, dropped = _prepare(members, kind)

    def _label(i: int) -> dict:
        m = members[i]
        return {"index": i, "isin": m.get("isin"), "name": m.get("name"),
                "weight_pct": round(100.0 * abs(float(m.get("weight") or 0)) / total_w, 2)
                if total_w > 0 else 0.0}

    reporting, excluded = [], [{**_label(d["index"]), "reason": d["reason"]} for d in dropped]
    for p in prepared:
        if period in p["by_year"]:
            reporting.append(p)
        else:
            excluded.append({**_label(p["index"]), "reason": "no_point_in_period"})

    pairs = [(p["weight"], p["by_year"][period][1]) for p in reporting]
    # ⚠ The harmonic combine DROPS a non-positive multiple (see `_weighted_harmonic`). Those
    # members are in `reporting` but contribute nothing, so they are reclassified here — otherwise
    # they would show a share of 0.0% and read as "contributed nothing", which is a different
    # claim from "was excluded because a negative P/E has no reciprocal".
    if kind == "multiple":
        keep = []
        for p in reporting:
            v = p["by_year"][period][1]
            (keep if v > 0 else excluded).append(
                p if v > 0 else {**_label(p["index"]), "reason": "non_positive_multiple"})
        reporting = keep
        pairs = [(p["weight"], p["by_year"][period][1]) for p in reporting]

    value = combine(pairs)
    covered = 100.0 * sum(w for w, _ in pairs) / total_w if total_w > 0 else 0.0

    if kind == "level":
        return _level_breakdown(members, metric_code, period, prepared, reporting, excluded,
                                total_w, _label)

    # The additive quantity: 1/v for a harmonic blend, v otherwise — see the docstring.
    def _additive(v: float) -> float:
        return 1.0 / v if kind == "multiple" else v

    denom = sum(w * _additive(v) for w, v in pairs)
    rows = []
    for n, p in enumerate(reporting):
        w, v = p["weight"], p["by_year"][period][1]
        others = [(q["weight"], q["by_year"][period][1]) for j, q in enumerate(reporting) if j != n]
        without = combine(others) if others else None
        rows.append({
            **_label(p["index"]),
            # ⚠ For a LEVEL, `value` is the rebased index and `raw_value` the amount as reported.
            # Showing only the index invites "why is Nestle's revenue 143?"; only the raw invites
            # summing figures that were never in the same currency.
            "value": round(v, 6),
            "raw_value": round(p["raw_by_year"][period][1], 6)
            if period in p["raw_by_year"] else None,
            "share_pct": round(100.0 * w * _additive(v) / denom, 2) if denom else None,
            "swing": round(value - without, 6) if (value is not None and without is not None)
            else None,
        })
    rows.sort(key=lambda r: abs(r["swing"] or 0), reverse=True)
    excluded.sort(key=lambda r: r["weight_pct"], reverse=True)
    return {"kind": kind, "metric_code": metric_code, "period": period,
            "value": round(value, 6) if value is not None else None,
            "covered_pct": round(covered, 2),
            "excluded_pct": round(100.0 - covered, 2),
            "members": rows, "excluded": excluded}
