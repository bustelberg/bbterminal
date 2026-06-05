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


def _first_weekday_on_or_after(d: date, weekday: int = 0) -> date:
    """First date on-or-after `d` that falls on `weekday`
    (Mon=0..Sun=6). With weekday=0 this is the first Monday — the
    historical default."""
    return d + timedelta(days=(weekday - d.weekday()) % 7)


def _first_monday_on_or_after(d: date) -> date:
    """First Monday on-or-after `d`. Thin wrapper over
    `_first_weekday_on_or_after` kept for callers/tests that still
    reference the Monday-specific name."""
    return _first_weekday_on_or_after(d, 0)


def _months_since_anchor(d: date) -> int:
    return (d.year - _ANCHOR_YEAR) * 12 + (d.month - _ANCHOR_MONTH)


def _next_month_1st(d: date) -> date:
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)


def _generate_anchored_first_weekdays(
    start: date, end: date, stride_months: int, weekday: int = 0,
) -> list[date]:
    """First-`weekday`-of-month (Mon=0..Sun=6) for every month touching
    [start, end] whose offset from `_ANCHOR_YEAR-_ANCHOR_MONTH` is a
    multiple of `stride_months`. With stride=1 this yields every month;
    with stride=3 it yields calendar quarters (Jan/Apr/Jul/Oct); etc.
    `weekday=0` (the default) reproduces the original first-Monday grid;
    `weekday=2` gives the first Wednesday of each period, and so on."""
    out: list[date] = []
    cursor = date(start.year, start.month, 1)
    end_limit = date(end.year, end.month, 1)
    while cursor <= end_limit:
        if _months_since_anchor(cursor) % stride_months == 0:
            out.append(_first_weekday_on_or_after(cursor, weekday))
        cursor = _next_month_1st(cursor)
    return out


def _generate_rebalance_dates(
    start: date,
    end: date,
    freq: RebalanceFrequency,
    prices_df: pd.DataFrame | None = None,
    *,
    weekday: int = 0,
) -> list[date]:
    """Generate rebalance dates for `freq` between [start, end]. All
    output dates fall on `weekday` (Mon=0..Sun=6; default 0 = Monday),
    except `daily`, which uses the trading calendar and ignores `weekday`.

    `weekday` lets a strategy rebalance on, e.g., the first Wednesday of
    each period instead of the first Monday. The signal cutoff stays
    strict-`<` on the rebalance date, so a first-Wednesday rebalance
    computes signals from data through the prior trading day's close
    (the Tuesday) and enters at the rebalance day's close — see
    `runner.py` / `signals.py`.

    For calendar-stride variants (monthly / 2m / 3m / …), produces the
    first `weekday` of every Nth calendar month anchored to Jan 2000 so
    the grid is independent of `start`. Independent of `prices_df` —
    `_price_on_or_after` walks the company's series to the next
    available trading day at entry.

    For weekly, produces every `weekday` in range.

    For daily, requires `prices_df` to identify the actual set of
    trading days in range (the union across all companies). Without
    `prices_df` we have no calendar to use, so we'd produce Mon-Fri
    sequences that include market holidays.
    """
    if freq == "monthly":
        return _generate_anchored_first_weekdays(start, end, 1, weekday)
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
        return _generate_anchored_first_weekdays(start, end, _MONTH_STRIDES[freq], weekday)
    if freq == "weekly":
        # Every `weekday` in range. weekday(): Mon=0..Sun=6.
        days_until = (weekday - start.weekday()) % 7
        first = start + timedelta(days=days_until)
        out: list[date] = []
        d = first
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
