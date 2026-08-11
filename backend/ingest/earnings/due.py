"""Does this company plausibly have a fiscal period we have not fetched yet?

WHY A DETECTOR AND NOT A SCHEDULE

    GuruFocus publishes when it publishes, and there is no calendar to consult: `stock/{sym}/
    earnings` is one of the sub-paths that returns HTTP 200 with an all-zero payload for ANY
    ticker (see `docs/gurufocus_api.md`). So a report date is unknowable, and the only instrument
    available is to project when the next period ENDED and probe some sensible interval after.

⚠⚠ THE LAG CANNOT BE LEARNED FROM OUR OWN HISTORY, WHICH IS THE FIRST THING YOU WOULD TRY.
    `metric_data.recorded_at` records when WE fetched, not when the vendor published. Measured
    2026-08-11 over every quarterly period since mid-2024, the per-company FASTEST-EVER observed
    lag is:

        27 days        1 company        84-101 days      60
        35-42         24                108-118         131
        53-74         46                125-402 days  1,532   <-- us, not them

    For 1,532 of ~1,800 companies we have never once fetched promptly enough to observe the true
    lag. A model trained on that would faithfully reproduce our own laziness. Only the floor means
    anything: nothing has EVER appeared sooner than 27 days after period end, so 25 is a safe
    "not before this" and the rest is discovered by probing.

⚠ IT ANSWERS "MAYBE", AND THE CALLER MUST SAY SO. A press that finds nothing new is the NORMAL
    outcome for a company that has not reported yet — not a failure, and not a broken button. This
    returns a suspicion with its reasoning attached so the UI can phrase it honestly.

PURE ON PURPOSE — no database, no clock. `today` is a parameter so the whole thing is testable
    without a fixture, which is this repo's rule for anything worth verifying.
"""
from __future__ import annotations

import calendar
import statistics
from datetime import date

# Nothing in the fleet has ever appeared sooner than 27 days after its period ended (measured
# above), so probing before this is spending a call to be told "not yet".
MIN_PUBLICATION_LAG_DAYS = 25

# The only cadences a fiscal period series has. Held in MONTHS rather than days because the
# projection below is month arithmetic — see `_next_period_end`.
_CADENCE_MONTHS = (3, 6, 12)

# Mean days per month, for turning an observed gap in days into a cadence in months.
_DAYS_PER_MONTH = 30.44


def _as_date(v: date | str) -> date:
    return v if isinstance(v, date) else date.fromisoformat(str(v)[:10])


def _month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def _next_period_end(newest: date, cadence_months: int) -> date:
    """The end of the period following `newest`.

    ⚠ MONTHS, NOT DAYS, AND THE DIFFERENCE IS A REAL BUG. Adding a 91-day "quarter" to
    2025-12-31 gives 2026-04-01 — a date that is not a fiscal period end and that pushes every
    projection one day later for the rest of the year. GuruFocus reports periods as `YYYY-MM`, so
    a period always ENDS at a month end; advancing whole months and snapping there is exact.
    """
    total = (newest.year * 12 + (newest.month - 1)) + cadence_months
    return _month_end(total // 12, total % 12 + 1)


def infer_cadence_months(periods: list[date]) -> int | None:
    """Quarterly (3), semi-annual (6) or annual (12) — from the company's OWN spacing.

    ⚠ NOT HARDCODED TO 3. Semi-annual filing is normal outside the US, and an annual-only series
    is common for the smaller names; assuming quarterly would mark both permanently overdue and
    nag on every page load.

    ⚠ MEDIAN, SO ONE ODD GAP CANNOT MOVE IT. A fiscal-year change leaves a stub period, and a
    missed filing leaves a double gap; both are single observations among dozens and the median
    ignores them where a mean would not.

    ⚠ SNAPPED TO A REAL CADENCE. A median of 89 or 94 days is a quarterly filer with ragged month
    lengths, not a company on an 89-day cycle. Snapping keeps the projection landing on the period
    the company will actually report.

    Returns None with fewer than two periods: one date carries no spacing, and guessing a cadence
    there would invent an expectation out of nothing.
    """
    if len(periods) < 2:
        return None
    gaps = [(b - a).days for a, b in zip(periods, periods[1:]) if b > a]
    if not gaps:
        return None
    months = statistics.median(gaps) / _DAYS_PER_MONTH
    return min(_CADENCE_MONTHS, key=lambda c: abs(c - months))


def period_due(periods: list[date | str], today: date, *,
               min_lag_days: int = MIN_PUBLICATION_LAG_DAYS) -> dict | None:
    """The period this company should plausibly have filed by now, or None.

    `periods` is every fiscal period END we already hold for it, in any order (duplicates fine).

    Returns, when something is expected:

        {"period": date, "period_label": "quarter ending Jun 2026", "due_since": date,
         "cadence_months": 3, "days_overdue": 17}

    and None when the newest period we hold is still the newest one that can exist, or when the
    cadence cannot be inferred.

    ⚠ THE LABEL NAMES THE PERIOD END, NEVER A QUARTER NUMBER, and that is not fussiness. "Q2 2026"
    is only right for a December year-end: a company whose fiscal year ends in June calls the
    quarter ending 2026-06-30 its FOURTH. Naming the month is true for every filer, and a label
    that is wrong for a minority is worse than a label that is plain for everyone.
    """
    seen = sorted({_as_date(p) for p in periods})
    if not seen:
        return None
    cadence = infer_cadence_months(seen)
    if cadence is None:
        return None

    nxt = _next_period_end(seen[-1], cadence)
    # ⚠ THE PERIOD MUST HAVE ENDED. Projecting forward says when it WILL end; a company cannot
    # report a quarter that is still running, and `due_since` alone would not catch that for a
    # long-lagged annual filer.
    if nxt > today:
        return None
    due_since = date.fromordinal(nxt.toordinal() + min_lag_days)
    if today < due_since:
        return None

    unit = {3: "quarter", 6: "half-year", 12: "year"}[cadence]
    return {
        "period": nxt,
        "period_label": f"{unit} ending {nxt.strftime('%b %Y')}",
        "due_since": due_since,
        "cadence_months": cadence,
        "days_overdue": (today - due_since).days,
    }
