"""Rebalance-date generation.

All rebalances land on a **Monday** — same convention the /schedule
pipeline uses (the pipeline tick fires every Tuesday 02:00 UTC and
captures the prior Monday's close). Backtest rebalance dates align to
this so backtest results approximate what the live pipeline would
actually produce.

Each frequency variant produces a list of Mondays the strategy enters
at; the runner walks each to the next available trading day via the
price index, so these dates don't have to be trading days themselves
(except for daily, which uses the actual trading calendar).

`every_N_months` rebalances are anchored to a fixed reference month
(Jan 2000) — a month is a rebalance month iff `(months-since-anchor)
% N == 0`. Without this anchor, slicing `[::N]` from the start_date
shifts the grid: a quarterly backtest starting Jan 2002 lands on
Jan/Apr/Jul/Oct, but starting Nov 2023 lands on Nov/Feb/May/Aug.
The anchor makes /backtest agree with the schedule backfill (which
anchors quarterly to calendar quarters by definition) regardless of
the backtest's start_date."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from .types import RebalanceFrequency


# Fixed anchor for every_N_months grids. Picked far enough in the past
# that no backtest start_date predates it; the actual value doesn't
# matter as long as it's stable across runs.
_ANCHOR_YEAR = 2000
_ANCHOR_MONTH = 1


def _first_monday_on_or_after(d: date) -> date:
    """Mon=0..Sun=6 → days to add to land on Monday."""
    return d + timedelta(days=(0 - d.weekday()) % 7)


def _months_since_anchor(d: date) -> int:
    return (d.year - _ANCHOR_YEAR) * 12 + (d.month - _ANCHOR_MONTH)


def _next_month_1st(d: date) -> date:
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)


def _generate_anchored_first_mondays(
    start: date, end: date, stride_months: int,
) -> list[date]:
    """First-Monday-of-month for every month touching [start, end]
    whose offset from `_ANCHOR_YEAR-_ANCHOR_MONTH` is a multiple of
    `stride_months`. With stride=1 this yields every month; with
    stride=3 it yields calendar quarters (Jan/Apr/Jul/Oct); etc."""
    out: list[date] = []
    cursor = date(start.year, start.month, 1)
    end_limit = date(end.year, end.month, 1)
    while cursor <= end_limit:
        if _months_since_anchor(cursor) % stride_months == 0:
            out.append(_first_monday_on_or_after(cursor))
        cursor = _next_month_1st(cursor)
    return out


def _generate_first_mondays_of_each_month(start: date, end: date) -> list[date]:
    """For each calendar month touched by [start, end], the first Monday
    on or after the 1st of that month. Equivalent to
    `_generate_anchored_first_mondays(start, end, 1)`."""
    return _generate_anchored_first_mondays(start, end, 1)


def _generate_rebalance_dates(
    start: date,
    end: date,
    freq: RebalanceFrequency,
    prices_df: pd.DataFrame | None = None,
) -> list[date]:
    """Generate rebalance dates for `freq` between [start, end]. All
    output dates are Mondays (except `daily`, which uses the trading
    calendar).

    For calendar-stride variants (monthly / 2m / 3m / …), produces the
    first Monday of every Nth calendar month anchored to Jan 2000 so
    the grid is independent of `start`. Independent of `prices_df` —
    `_price_on_or_after` walks the company's series to the next
    available trading day at entry.

    For weekly, produces every Monday in range.

    For daily, requires `prices_df` to identify the actual set of
    trading days in range (the union across all companies). Without
    `prices_df` we have no calendar to use, so we'd produce Mon-Fri
    sequences that include market holidays.
    """
    if freq == "monthly":
        return _generate_anchored_first_mondays(start, end, 1)
    # every_N_months → anchored grid. Adding new strides only needs an
    # entry in the map below + the Literal at the top.
    _MONTH_STRIDES = {
        "every_2_months": 2,
        "every_3_months": 3,
        "every_4_months": 4,
        "every_5_months": 5,
        "every_6_months": 6,
        "every_7_months": 7,
        "every_8_months": 8,
        "every_9_months": 9,
        "every_10_months": 10,
        "every_11_months": 11,
        "every_12_months": 12,
    }
    if freq in _MONTH_STRIDES:
        return _generate_anchored_first_mondays(start, end, _MONTH_STRIDES[freq])
    if freq == "weekly":
        # Every Monday in range. weekday(): Mon=0..Sun=6.
        days_until_mon = (-start.weekday()) % 7
        first_mon = start + timedelta(days=days_until_mon)
        out: list[date] = []
        d = first_mon
        while d <= end:
            out.append(d)
            d += timedelta(days=7)
        return out
    if freq == "daily":
        if prices_df is None or prices_df.empty:
            raise ValueError("daily frequency requires prices_df to identify trading days")
        all_dates = pd.to_datetime(prices_df["target_date"]).dt.date.unique()
        return sorted(d for d in all_dates if start <= d <= end)
    raise ValueError(f"Unknown rebalance frequency: {freq}")
