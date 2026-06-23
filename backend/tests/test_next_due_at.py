"""Scheduling math for the smart pipeline's per-strategy due grid.

`compute_next_due_at(frequency, just_ran, weekday)` returns the next FIRE time
(02:00 UTC) — the day after the rebalance's deciding bar (the prior trading
day's close, strict-< the rebalance date). For a first-Monday rebalance the
deciding bar is Friday's close, settled by Saturday, so it fires Saturday with
picks identical to running on the Monday; a mid-week grid's deciding bar is the
day before, so it still fires on the grid date. The rebalance/grid date itself
(the period the backtest engine anchors to, `momentum/backtest/dates.py`) is
unchanged. `_initial_next_due_at` makes a freshly added strategy due on its
first such fire.
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
        # Ran Monday → next Monday is Jan 8; deciding bar = Fri Jan 5 → fire Sat Jan 6.
        assert compute_next_due_at("weekly", _utc(2024, 1, 1), 0) == _utc(2024, 1, 6)
        # weekday=2 → next Wednesday Jan 10; deciding bar = Tue Jan 9 → fire Wed Jan 10.
        assert compute_next_due_at("weekly", _utc(2024, 1, 3), 2) == _utc(2024, 1, 10)

    def test_weekly_from_offgrid_day(self):
        # Ran Tue, weekday=Mon → next Monday Jan 8 → fire the Sat before (Jan 6).
        assert compute_next_due_at("weekly", _utc(2024, 1, 2), 0) == _utc(2024, 1, 6)

    def test_monthly_first_monday(self):
        # Next is first Monday of Feb (5th) → deciding Fri Feb 2 → fire Sat Feb 3.
        assert compute_next_due_at("monthly", _utc(2024, 1, 1), 0) == _utc(2024, 2, 3)
        # …and from Feb's first Monday → first Monday of Mar (4th) → fire Sat Mar 2.
        assert compute_next_due_at("monthly", _utc(2024, 2, 5), 0) == _utc(2024, 3, 2)

    def test_monthly_first_wednesday(self):
        # First Wed of Feb is the 7th; deciding bar = Tue Feb 6 → fire Wed Feb 7
        # (mid-week grid, no early shift).
        assert compute_next_due_at("monthly", _utc(2024, 1, 3), 2) == _utc(2024, 2, 7)

    def test_bimonthly_anchored_to_jan_2000(self):
        # Stride-2 (odd calendar months). First Monday of Mar 2024 is the 4th
        # → fire Sat Mar 2.
        assert compute_next_due_at("bimonthly", _utc(2024, 1, 1), 0) == _utc(2024, 3, 2)

    def test_quarterly_anchored_to_calendar_quarters(self):
        # Stride-3 → Jan/Apr/Jul/Oct. First Monday of Apr 2024 is the 1st;
        # deciding Fri Mar 29 → fire Sat Mar 30.
        assert compute_next_due_at("quarterly", _utc(2024, 1, 1), 0) == _utc(2024, 3, 30)

    def test_result_is_always_0200_utc(self):
        # Fires at 02:00 UTC. For a first-Monday grid the fire day is the Saturday
        # before (the deciding Friday close is settled by then).
        due = compute_next_due_at("monthly", _utc(2024, 1, 1, 14, 30), 0)
        assert (due.hour, due.minute, due.second) == (2, 0, 0)
        assert due.tzinfo == timezone.utc
        assert due.weekday() == 5  # Saturday (the day after Friday's deciding close)


class TestInitialNextDueAt:
    def test_monthly_added_mid_period_fires_before_next_first_weekday(self):
        # Added 2024-06-05; first rebalance grid = first Monday of July (1st),
        # decided by Fri Jun 28's close → fires the Saturday before (Jun 29).
        assert _initial_next_due_at("monthly", 0, _utc(2024, 6, 5, 12, 0)) == _utc(2024, 6, 29)

    def test_weekly_is_next_weekday(self):
        # Next Monday is Jun 10; deciding Fri Jun 7 → fire Sat Jun 8.
        assert _initial_next_due_at("weekly", 0, _utc(2024, 6, 7, 12, 0)) == _utc(2024, 6, 8)

    def test_daily_is_next_day(self):
        assert _initial_next_due_at("daily", 0, _utc(2024, 6, 5, 12, 0)) == _utc(2024, 6, 6)
