"""Scheduling math for the smart pipeline's per-strategy due grid.

`compute_next_due_at(frequency, just_ran, weekday)` returns the next FIRE time
(02:00 UTC) on the rebalance-grid date. The daily 05:00 Amsterdam pass then
refreshes the complete universe through the prior deciding close before it
selects. A first-Monday rebalance consequently waits until Monday rather than
accepting an individual Friday bar that GuruFocus published after Saturday.
The rebalance/grid date itself (the period the backtest engine anchors to,
`momentum/backtest/dates.py`) is unchanged. `_initial_next_due_at` makes a
freshly added strategy due on its first such fire.
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
        # Ran Monday → next Monday is Jan 8; deciding bar = Fri Jan 5.
        assert compute_next_due_at("weekly", _utc(2024, 1, 1), 0) == _utc(2024, 1, 8)
        # weekday=2 → next Wednesday Jan 10; deciding bar = Tue Jan 9 → fire Wed Jan 10.
        assert compute_next_due_at("weekly", _utc(2024, 1, 3), 2) == _utc(2024, 1, 10)

    def test_weekly_from_offgrid_day(self):
        # Ran Tue, weekday=Mon → next Monday Jan 8.
        assert compute_next_due_at("weekly", _utc(2024, 1, 2), 0) == _utc(2024, 1, 8)

    def test_monthly_first_monday(self):
        # Next is first Monday of Feb (5th), decided from Fri Feb 2.
        assert compute_next_due_at("monthly", _utc(2024, 1, 1), 0) == _utc(2024, 2, 5)
        # …and from Feb's first Monday → first Monday of Mar (4th).
        assert compute_next_due_at("monthly", _utc(2024, 2, 5), 0) == _utc(2024, 3, 4)

    def test_monthly_first_wednesday(self):
        # First Wed of Feb is the 7th; deciding bar = Tue Feb 6 → fire Wed Feb 7
        # (mid-week grid, no early shift).
        assert compute_next_due_at("monthly", _utc(2024, 1, 3), 2) == _utc(2024, 2, 7)

    def test_bimonthly_anchored_to_jan_2000(self):
        # Stride-2 (odd calendar months). First Monday of Mar 2024 is the 4th
        # → decide from Fri Mar 1 and run Mon Mar 4.
        assert compute_next_due_at("bimonthly", _utc(2024, 1, 1), 0) == _utc(2024, 3, 4)

    def test_quarterly_anchored_to_calendar_quarters(self):
        # Stride-3 → Jan/Apr/Jul/Oct. First Monday of Apr 2024 is the 1st;
        # deciding Fri Mar 29 → run Mon Apr 1.
        assert compute_next_due_at("quarterly", _utc(2024, 1, 1), 0) == _utc(2024, 4, 1)

    def test_result_is_always_0200_utc(self):
        # Fires at 02:00 UTC on the first-Monday grid date, leaving the 05:00
        # Amsterdam tick time to fetch every exchange's Friday close.
        due = compute_next_due_at("monthly", _utc(2024, 1, 1, 14, 30), 0)
        assert (due.hour, due.minute, due.second) == (2, 0, 0)
        assert due.tzinfo == timezone.utc
        assert due.weekday() == 0  # Monday


class TestInitialNextDueAt:
    def test_monthly_added_mid_period_fires_before_next_first_weekday(self):
        # Added 2024-06-05; first rebalance grid = first Monday of July (1st),
        # decided by Fri Jun 28's close → runs Monday July 1.
        assert _initial_next_due_at("monthly", 0, _utc(2024, 6, 5, 12, 0)) == _utc(2024, 7, 1)

    def test_weekly_is_next_weekday(self):
        # Next Monday is Jun 10; deciding Fri Jun 7 → run Mon Jun 10.
        assert _initial_next_due_at("weekly", 0, _utc(2024, 6, 7, 12, 0)) == _utc(2024, 6, 10)

    def test_daily_is_next_day(self):
        assert _initial_next_due_at("daily", 0, _utc(2024, 6, 5, 12, 0)) == _utc(2024, 6, 6)
