"""Pure scheduling math for the smart pipeline's per-strategy due grid.

Extracted from `routers.scheduled_strategies` so the pipeline + tests can
import the date math without pulling in the HTTP router (FastAPI, the
backfill worker, the backtest stream). No I/O, no DB — just `datetime`.

The smart daily pipeline fires every day at 02:00 UTC and rebalances a
strategy when `next_due_at <= now`. A strategy's rebalance lands on the
first occurrence of its baked `rebalance_weekday` (Mon=0..Sun=6) in its
period — e.g. monthly + weekday=0 → the first Monday of the month — and
decides on the prior trading day's close (Friday for a Monday, since the
Monday 02:00 UTC tick already has Friday's settled close). The grid is
anchored to Jan 2000 so bi-/quarterly periods land on the same calendar
months the backtest engine uses (`momentum/backtest/dates.py`); the two
pure-date helpers below mirror that module so importing it (and pandas)
at request time isn't needed.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

_ANCHOR_YEAR = 2000
_ANCHOR_MONTH = 1
_STRIDE_BY_FREQUENCY = {"monthly": 1, "bimonthly": 2, "quarterly": 3}


def _first_weekday_on_or_after(d: date, weekday: int = 0) -> date:
    """First date on-or-after `d` falling on `weekday` (Mon=0..Sun=6).
    Mirror of `momentum.backtest.dates._first_weekday_on_or_after`."""
    return d + timedelta(days=(weekday - d.weekday()) % 7)


def _months_since_anchor(d: date) -> int:
    return (d.year - _ANCHOR_YEAR) * 12 + (d.month - _ANCHOR_MONTH)


def _next_month_1st(d: date) -> date:
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)


def _next_anchored_rebalance_date(after: date, stride_months: int, weekday: int) -> date:
    """First first-`weekday`-of-an-anchored-month date strictly after
    `after`. Walks calendar months from `after`'s month forward, picking
    months whose offset from the Jan-2000 anchor is a multiple of
    `stride_months` (stride 1 = every month, 2 = bimonthly, 3 = quarterly)
    and the first `weekday` within them, until that date is > `after`."""
    cursor = date(after.year, after.month, 1)
    for _ in range(64):  # safety bound: 64 months covers any stride
        if _months_since_anchor(cursor) % stride_months == 0:
            candidate = _first_weekday_on_or_after(cursor, weekday)
            if candidate > after:
                return candidate
        cursor = _next_month_1st(cursor)
    # Unreachable for valid strides; fall back to a month out.
    return _first_weekday_on_or_after(_next_month_1st(after), weekday)


def _expected_latest_trading_day(today: date) -> date:
    """Most recent weekday strictly before `today` — the last trading day
    whose close should have settled and be fetchable (the daily pipeline runs
    at 02:00 UTC and captures the prior day's close). Used as the freshness
    reference for held-company prices. A market-holiday calendar isn't
    available, so weekends are skipped but holidays aren't — that errs toward
    'stale / go-fetch', which is the safe side."""
    d = today - timedelta(days=1)
    while d.weekday() >= 5:  # 5 = Sat, 6 = Sun
        d -= timedelta(days=1)
    return d


def compute_next_due_at(
    frequency: str, just_ran_at_utc: datetime, rebalance_weekday: int = 0,
) -> datetime:
    """Given a strategy just rebalanced at `just_ran_at_utc`, return the
    next rebalance tick (the rebalance day's 02:00 UTC) per `frequency` and
    the strategy's baked `rebalance_weekday`.

    - daily: the next calendar day (the engine's daily grid ignores weekday).
    - weekly: the next occurrence of `rebalance_weekday` strictly after the
      run date.
    - monthly/bimonthly/quarterly: the first `rebalance_weekday` of the next
      eligible anchored month (Jan-2000 anchored stride 1/2/3)."""
    just_ran_date = just_ran_at_utc.date()
    weekday = rebalance_weekday if 0 <= rebalance_weekday <= 6 else 0
    if frequency == "daily":
        next_date = just_ran_date + timedelta(days=1)
    elif frequency == "weekly":
        # Next `weekday` strictly after the run date.
        ahead = (weekday - just_ran_date.weekday()) % 7
        next_date = just_ran_date + timedelta(days=ahead or 7)
    else:
        stride = _STRIDE_BY_FREQUENCY.get(frequency, 1)
        next_date = _next_anchored_rebalance_date(just_ran_date, stride, weekday)
    return datetime.combine(next_date, time(2, 0), tzinfo=timezone.utc)


def _initial_next_due_at(
    frequency: str,
    rebalance_weekday: int = 0,
    reference_now: datetime | None = None,
) -> datetime:
    """First REBALANCE for a freshly added strategy: the next date on its
    (frequency, rebalance_weekday) grid (e.g. added June 5 → first Monday of
    July). The strategy's CURRENT holdings come from its saved backtest's
    last period (seeded into a snapshot on add — see
    `_seed_snapshot_from_backtest`), so the daily refresh tracks them right
    away; the universe is only repriced + the strategy re-selected at this
    next grid rebalance."""
    now = reference_now or datetime.now(timezone.utc)
    return compute_next_due_at(frequency, now, rebalance_weekday)
