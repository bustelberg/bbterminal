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
        These are REBASED to 100 at the first date every member shares, then weighted — giving a
        growth index, which is the only honest portfolio-level statement about a level series.

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

# Below this share of the blended weight reporting on a date, that date has no honest value.
MIN_BLEND_COVERAGE_PCT = 60.0

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


def _latest_per_year(pts: dict[str, float]) -> dict[str, tuple[str, float]]:
    """One row per FISCAL YEAR -> {year: (date, value)}. A member reporting twice in a year (a
    year-end change) keeps its LATEST close rather than being counted twice."""
    latest: dict[str, tuple[str, float]] = {}
    for d, v in pts.items():
        year = d[:4]
        if year not in latest or d > latest[year][0]:
            latest[year] = (d, v)
    return latest


def _prepare(members: list[dict], kind: str) -> tuple[list[dict], list[dict]]:
    """Members split into those that can contribute and those that cannot, with the REASON.

    ⚠ SHARED BY `blend_series` AND `blend_breakdown` ON PURPOSE. A drill-down that re-derives
    "the same way" is a second copy of these rules, and the copy is what drifts — a panel that
    explains a number the line does not show is worse than no panel, because it is checked once
    and believed thereafter. One preparation, two readers.
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
            base_date = min(anchor)
            base = anchor[base_date]
            # ⚠ A zero or negative base cannot be rebased — 100 × v/0 is undefined and a negative
            # base flips every later point's sign. The member is dropped from THIS metric rather
            # than contributing an inverted curve.
            if base <= 0:
                dropped.append({"index": i, "weight": w, "reason": "non_positive_base"})
                continue
            pts = {d: 100.0 * v / base for d, v in pts.items()}
        ok.append({"index": i, "weight": w, "points": pts, "raw": raw,
                   "by_year": _latest_per_year(pts), "raw_by_year": _latest_per_year(raw)})
    return ok, dropped


def blend_series(members: list[dict], metric_code: str) -> dict:
    """`members` = [{weight, points: {date: value}, base_points?}] -> one blended series.

    `base_points` (optional, LEVELS only) is the series this one continues — a forecast passes the
    ACTUAL it extends, so both are rebased on the same anchor and the forecast picks up where the
    actual stops instead of restarting at 100.

    Returns `{kind, points: [{date, value, covered_pct}], covered_pct}` where `covered_pct` is the
    share of the blended weight that reported on that date.
    """
    kind = blend_kind(metric_code)
    total_w = sum(abs(float(m.get("weight") or 0)) for m in members)
    if total_w <= 0:
        return {"kind": kind, "points": [], "covered_pct": 0.0}

    prepared, _ = _prepare(members, kind)
    by_date: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for p in prepared:
        for year, (_d, v) in p["by_year"].items():
            by_date[year].append((p["weight"], v))

    combine = _weighted_harmonic if kind == "multiple" else _weighted_arithmetic
    out = []
    for d in sorted(by_date):
        pairs = by_date[d]
        covered = 100.0 * sum(p[0] for p in pairs) / total_w
        value = combine(pairs)
        if value is None or covered < MIN_BLEND_COVERAGE_PCT:
            continue        # ⚠ omitted, never drawn as a dip — see the docstring
        out.append({"period": d, "value": round(value, 6), "covered_pct": round(covered, 2)})
    spanned = max((p["covered_pct"] for p in out), default=0.0)
    return {"kind": kind, "points": out, "covered_pct": round(spanned, 2)}


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
