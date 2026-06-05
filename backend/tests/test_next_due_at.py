"""Scheduling math for the smart pipeline's per-strategy due grid.

`compute_next_due_at(frequency, just_ran, weekday)` must return the next
rebalance tick (the rebalance day's 02:00 UTC) on the first-`weekday`-of-
period anchored grid — the SAME grid the backtest engine produces
(`momentum/backtest/dates.py`), so live `next_due_at` lines up with what
/backtest + the backfill computed. `_initial_next_due_at` makes a freshly
added strategy due on the next tick.
"""
from __future__ import annotations

from datetime import datetime, timezone

from momentum.schedule import (
    _initial_next_due_at,
    compute_next_due_at,
)


def _utc(y, m, d, hh=2, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=timezone.utc)


class TestComputeNextDueAt:
    def test_daily_is_next_calendar_day(self):
        # daily ignores weekday entirely.
        assert compute_next_due_at("daily", _utc(2024, 1, 1), 0) == _utc(2024, 1, 2)
        assert compute_next_due_at("daily", _utc(2024, 1, 1), 3) == _utc(2024, 1, 2)

    def test_weekly_next_same_weekday(self):
        # Ran on a Monday → next Monday.
        assert compute_next_due_at("weekly", _utc(2024, 1, 1), 0) == _utc(2024, 1, 8)
        # Ran on a Wednesday with weekday=2 → next Wednesday.
        assert compute_next_due_at("weekly", _utc(2024, 1, 3), 2) == _utc(2024, 1, 10)

    def test_weekly_from_offgrid_day(self):
        # Ran Tue, weekday=Mon → next Monday is the 8th.
        assert compute_next_due_at("weekly", _utc(2024, 1, 2), 0) == _utc(2024, 1, 8)

    def test_monthly_first_monday(self):
        # First Monday of Jan 2024 is the 1st → next is first Monday of Feb (5th).
        assert compute_next_due_at("monthly", _utc(2024, 1, 1), 0) == _utc(2024, 2, 5)
        # …and from Feb's first Monday → first Monday of March (4th). Matches
        # test_rebalance_weekday's [2024-01-01, 2024-02-05, 2024-03-04] grid.
        assert compute_next_due_at("monthly", _utc(2024, 2, 5), 0) == _utc(2024, 3, 4)

    def test_monthly_first_wednesday(self):
        # First Wednesday of Jan 2024 is the 3rd → next is first Wed of Feb (7th).
        assert compute_next_due_at("monthly", _utc(2024, 1, 3), 2) == _utc(2024, 2, 7)

    def test_bimonthly_anchored_to_jan_2000(self):
        # Stride-2 grid anchored to Jan 2000 → Jan/Mar/May/… (odd calendar
        # months). From first Monday of Jan 2024 → first Monday of Mar (4th).
        assert compute_next_due_at("bimonthly", _utc(2024, 1, 1), 0) == _utc(2024, 3, 4)

    def test_quarterly_anchored_to_calendar_quarters(self):
        # Stride-3 grid anchored to Jan 2000 → Jan/Apr/Jul/Oct. From first
        # Monday of Jan 2024 → first Monday of Apr 2024 (the 1st).
        assert compute_next_due_at("quarterly", _utc(2024, 1, 1), 0) == _utc(2024, 4, 1)

    def test_result_is_always_0200_utc_on_the_weekday(self):
        due = compute_next_due_at("monthly", _utc(2024, 1, 1, 14, 30), 0)
        assert (due.hour, due.minute, due.second) == (2, 0, 0)
        assert due.tzinfo == timezone.utc
        assert due.weekday() == 0  # Monday


class TestInitialNextDueAt:
    def test_monthly_added_mid_period_is_next_first_weekday(self):
        # Added 2024-06-05; current holdings are seeded from the backtest, so
        # the first REBALANCE is the next grid date: first Monday of July (1st).
        assert _initial_next_due_at("monthly", 0, _utc(2024, 6, 5, 12, 0)) == _utc(2024, 7, 1)

    def test_weekly_is_next_weekday(self):
        assert _initial_next_due_at("weekly", 0, _utc(2024, 6, 7, 12, 0)) == _utc(2024, 6, 10)

    def test_daily_is_next_day(self):
        assert _initial_next_due_at("daily", 0, _utc(2024, 6, 5, 12, 0)) == _utc(2024, 6, 6)
