"""Rebalance-date generation. Each frequency variant produces a list of
calendar dates the strategy enters at; the runner walks each to the next
available trading day via the price index, so these dates don't have to
be trading days themselves (except for daily, which uses the actual
trading calendar)."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from .types import RebalanceFrequency


def _generate_month_starts(start: date, end: date) -> list[date]:
    """Generate first-of-month dates between start and end."""
    months = []
    current = date(start.year, start.month, 1)
    end_limit = date(end.year, end.month, 1)
    while current <= end_limit:
        months.append(current)
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return months


def _generate_rebalance_dates(
    start: date,
    end: date,
    freq: RebalanceFrequency,
    prices_df: pd.DataFrame | None = None,
) -> list[date]:
    """Generate rebalance dates for `freq` between [start, end].

    For calendar-stride variants (monthly / 2m / 3m), produces every Nth
    first-of-month date and is independent of prices_df — _price_on_or_after
    walks the company's series to the next available trading day at entry.

    For weekly, produces every Monday in range — actual entry still falls
    on the first available trading day on/after that Monday via
    _price_on_or_after.

    For daily, requires prices_df to identify the actual set of trading days
    in range (the union across all companies). Without prices_df we have no
    calendar to use, so we'd produce Mon-Fri sequences that include
    market holidays.
    """
    if freq == "monthly":
        return _generate_month_starts(start, end)
    # every_N_months → take every N-th month-start. Adding new strides only
    # needs an entry in the map below + the Literal at the top.
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
        return _generate_month_starts(start, end)[::_MONTH_STRIDES[freq]]
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
