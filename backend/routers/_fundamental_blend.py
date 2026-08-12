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
from datetime import date as _date

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
            base_date = min(anchor)
            base = anchor[base_date]
            # ⚠ A zero or negative base cannot be rebased — 100 × v/0 is undefined and a negative
            # base flips every later point's sign. The member is dropped from THIS metric rather
            # than contributing an inverted curve.
            if base <= 0:
                dropped.append({"index": i, "weight": w, "reason": "non_positive_base"})
                continue
            pts = {d: 100.0 * v / base for d, v in pts.items()}
        ok.append({"index": i, "weight": w, "weights": m.get("weights"),
                   "points": pts, "raw": raw,
                   "by_year": _latest_per_bucket(pts, bucket),
                   "raw_by_year": _latest_per_bucket(raw, bucket)})
    return ok, dropped


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
        for period, (v, reported) in carry_forward(p["by_year"], axis).items():
            w = _weight_at(p, period)
            if not w:
                continue
            by_date[period].append((abs(float(w)), v))
            if reported:
                cover_w[period] += abs(float(p.get("weight") or 0))
                cover_n[period] += 1

    combine = _weighted_harmonic if kind == "multiple" else _weighted_arithmetic
    out = []
    for d in sorted(by_date):
        pairs = by_date[d]
        covered = 100.0 * cover_w[d] / total_w
        covered_n = 100.0 * cover_n[d] / total_n
        value = combine(pairs)
        if (value is None or covered < MIN_BLEND_COVERAGE_PCT
                or covered_n < MIN_BLEND_COVERAGE_NAMES_PCT):
            continue        # ⚠ omitted, never drawn as a dip — see the docstring
        out.append({"period": d, "value": round(value, 6), "covered_pct": round(covered, 2),
                    "covered_names_pct": round(covered_n, 2)})
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
    combine = _weighted_harmonic if kind == "multiple" else _weighted_arithmetic
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

    blended, covered, below_floor = {}, {}, {}
    for y in periods:
        pairs = [(p["weight"], p["by_year"][y][1]) for p in prepared if y in p["by_year"]]
        if kind == "multiple":
            pairs = [(w, v) for w, v in pairs if v and v > 0]
        cov = 100.0 * sum(w for w, _ in pairs) / total_w if total_w > 0 else 0.0
        val = combine(pairs)
        blended[y] = round(val, 6) if val is not None else None
        covered[y] = round(cov, 2)
        below_floor[y] = cov < MIN_BLEND_COVERAGE_PCT

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
