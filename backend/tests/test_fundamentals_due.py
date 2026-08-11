"""When does a company plausibly have a fiscal period we have not fetched?

The detector behind the "Refresh fundamentals" prompt. It is pure — no database, no clock — so
every case below is a statement about the arithmetic rather than about a fixture.

⚠ THE ANCHOR CASE IS REAL, NOT INVENTED. On 2026-08-11 our `metric_data` held ASML through
2026-03-31 while GuruFocus had 2026-06 (verified against the live API: Revenue 9,326.5, EPS 7.58).
The detector must flag exactly that, and say it has been expected since 25 July.

⚠ AND THE FLEET-WIDE SHAPE IS REAL TOO: 1,423 companies sat at 2026-03-31 with 23 already at
2026-06-30. A detector that flagged everything, or nothing, would be indistinguishable from those
numbers at a glance — which is why the boundary cases below are pinned individually.
"""
from __future__ import annotations

from datetime import date

from ingest.earnings.due import MIN_PUBLICATION_LAG_DAYS, infer_cadence_months, period_due

# Calendar-year quarterly filer, as ASML is.
QUARTERLY = ["2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"]


class TestTheAnchorCase:
    """ASML on 2026-08-11 — held through Q1, GuruFocus had Q2."""

    def test_it_flags_the_quarter_gurufocus_actually_had(self):
        got = period_due(QUARTERLY, date(2026, 8, 11))
        assert got is not None
        assert got["period"] == date(2026, 6, 30)
        assert got["due_since"] == date(2026, 7, 25)      # 30 Jun + 25 days
        assert got["cadence_months"] == 3
        assert got["days_overdue"] == 17

    def test_it_says_nothing_the_day_before_it_is_due(self):
        assert period_due(QUARTERLY, date(2026, 7, 24)) is None

    def test_the_boundary_day_IS_due(self):
        # ⚠ `>=`, NOT `>`. An off-by-one here costs a day of staleness on every company, every
        # quarter — invisible individually and a day late fleet-wide.
        assert period_due(QUARTERLY, date(2026, 7, 25)) is not None


class TestAPeriodStillRunningIsNotLate:
    """The projection says when the next period WILL end; nobody reports a quarter mid-flight."""

    def test_the_quarter_has_not_ended_yet(self):
        # 2026-05-01: the quarter ending 30 Jun is still running.
        assert period_due(QUARTERLY, date(2026, 5, 1)) is None

    def test_an_annual_filer_is_not_chased_all_year(self):
        # ⚠ THE CASE THE `nxt > today` GUARD EXISTS FOR. FY2026 ends 31 Dec; without the guard,
        # `due_since` (31 Dec + 25) is also in the future and the None comes out for the right
        # reason by luck. Pinned so a refactor cannot drop the check and still pass.
        annual = ["2022-12-31", "2023-12-31", "2024-12-31", "2025-12-31"]
        assert period_due(annual, date(2026, 6, 30)) is None
        got = period_due(annual, date(2027, 2, 1))
        assert got is not None and got["period"] == date(2026, 12, 31)
        assert got["period_label"] == "year ending Dec 2026"


class TestMonthArithmeticNotDayArithmetic:
    """⚠ A 91-DAY "QUARTER" ADDED TO 2025-12-31 GIVES 2026-04-01 — not a fiscal period end, and
    one day of drift compounding through the year. Periods end at month ends; advance by months."""

    def test_december_to_march(self):
        got = period_due(["2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31"],
                         date(2026, 6, 1))
        assert got is not None
        assert got["period"] == date(2026, 3, 31)

    def test_february_lands_on_the_short_month(self):
        # A November year-end filer: Feb / May / Aug / Nov. The next after Nov is Feb 28.
        got = period_due(["2024-11-30", "2025-02-28", "2025-05-31", "2025-08-31", "2025-11-30"],
                         date(2026, 5, 1))
        assert got is not None
        assert got["period"] == date(2026, 2, 28)

    def test_february_in_a_leap_year(self):
        got = period_due(["2027-05-31", "2027-08-31", "2027-11-30"], date(2028, 4, 1))
        assert got is not None
        assert got["period"] == date(2028, 2, 29)


class TestTheCadenceIsTheCompanySOwn:

    def test_quarterly_semi_annual_and_annual_are_told_apart(self):
        assert infer_cadence_months([date(2025, 3, 31), date(2025, 6, 30),
                                     date(2025, 9, 30)]) == 3
        assert infer_cadence_months([date(2024, 6, 30), date(2024, 12, 31),
                                     date(2025, 6, 30)]) == 6
        assert infer_cadence_months([date(2023, 12, 31), date(2024, 12, 31),
                                     date(2025, 12, 31)]) == 12

    def test_a_semi_annual_filer_is_not_chased_every_quarter(self):
        """⚠ HARDCODING 3 WOULD MARK EVERY NON-US FILER PERMANENTLY OVERDUE. Semi-annual reporting
        is normal outside the US; the prompt would never clear and would teach the reader to
        ignore it."""
        semi = ["2024-06-30", "2024-12-31", "2025-06-30", "2025-12-31"]
        assert period_due(semi, date(2026, 5, 1)) is None          # H1 2026 still running
        got = period_due(semi, date(2026, 8, 1))
        assert got is not None and got["period"] == date(2026, 6, 30)
        assert got["period_label"] == "half-year ending Jun 2026"

    def test_one_odd_gap_does_not_move_the_cadence(self):
        # A fiscal-year change leaves a stub period. The median ignores it; a mean would not.
        ragged = [date(2025, 3, 31), date(2025, 6, 30), date(2025, 8, 31),   # <- stub
                  date(2025, 11, 30), date(2026, 2, 28)]
        assert infer_cadence_months(ragged) == 3

    def test_a_ragged_series_still_projects_the_next_period(self):
        got = period_due(["2025-03-31", "2025-06-30", "2025-08-31", "2025-11-30", "2026-02-28"],
                         date(2026, 7, 1))
        assert got is not None and got["period"] == date(2026, 5, 31)


class TestItRefusesRatherThanGuesses:

    def test_a_single_period_carries_no_spacing(self):
        """One date cannot imply a cadence. Assuming quarterly would invent an expectation from
        nothing and flag a company we know almost nothing about."""
        assert infer_cadence_months([date(2026, 3, 31)]) is None
        assert period_due(["2026-03-31"], date(2027, 1, 1)) is None

    def test_no_periods_at_all(self):
        assert period_due([], date(2026, 8, 11)) is None

    def test_duplicate_dates_are_not_a_zero_gap(self):
        """⚠ metric_data has one row per (metric, period), so a caller passing raw target_dates
        hands this the same date dozens of times. Deduping is what stops the median gap collapsing
        to 0 and the cadence snapping to 3 for everyone."""
        noisy = ["2025-12-31"] * 40 + ["2025-06-30"] * 40 + ["2024-12-31"] * 40
        assert infer_cadence_months([date(2024, 12, 31), date(2025, 6, 30),
                                     date(2025, 12, 31)]) == 6
        got = period_due(noisy, date(2026, 8, 1))
        assert got is not None and got["period"] == date(2026, 6, 30)


class TestInputShape:

    def test_unsorted_input_is_sorted(self):
        assert period_due(list(reversed(QUARTERLY)), date(2026, 8, 11)) == \
            period_due(QUARTERLY, date(2026, 8, 11))

    def test_dates_and_strings_are_interchangeable(self):
        as_dates = [date.fromisoformat(p) for p in QUARTERLY]
        assert period_due(as_dates, date(2026, 8, 11)) == period_due(QUARTERLY, date(2026, 8, 11))

    def test_a_timestamp_string_is_accepted(self):
        # Some callers hand over `recorded_at`-style strings; take the date part rather than throw.
        assert period_due([p + "T00:00:00+00:00" for p in QUARTERLY],
                          date(2026, 8, 11)) is not None


class TestTheLagIsAFloorNotAGuess:
    """25 days, because nothing in the fleet has ever appeared sooner than 27 after period end."""

    def test_the_default_is_the_measured_floor(self):
        assert MIN_PUBLICATION_LAG_DAYS == 25

    def test_it_is_tunable_without_touching_the_projection(self):
        # 30 Jun + 60 days = 29 Aug, so a caller who wants to probe less eagerly gets silence on
        # 11 August and the same period a fortnight later. The PROJECTION does not move with the
        # lag — only the day we start asking.
        assert period_due(QUARTERLY, date(2026, 8, 11), min_lag_days=60) is None
        strict = period_due(QUARTERLY, date(2026, 8, 29), min_lag_days=60)
        assert strict is not None
        assert strict["period"] == date(2026, 6, 30)
        assert strict["due_since"] == date(2026, 8, 29)
        assert strict["days_overdue"] == 0
