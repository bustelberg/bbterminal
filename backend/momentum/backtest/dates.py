"""Rebalance-date generation.

All rebalances land on a **Monday** — same convention the /schedule
pipeline uses (the daily pipeline tick fires at 05:00 UTC and captures
the prior trading day's close). Backtest rebalance dates align to
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


# ── Decidability: which close a rebalance is DECIDED on ────────────
# Largest calendar gap (days) we'll treat as a market HOLIDAY rather than
# stale/missing data when forgiving an un-traded deciding bar. Covers the worst
# single-closure case — a Monday holiday (MLK/Presidents/Memorial/Labor Day),
# whose prior real bar is the preceding Friday (Fri→Mon = 3 days) — with a day
# of slack. A wider gap means we're genuinely missing bars (an outage), not a
# holiday, so we DON'T forgive.
_MAX_HOLIDAY_GAP_DAYS = 4


def _prior_trading_day(d: date) -> date:
    """Last weekday strictly before `d` (skips Sat/Sun). Holidays aren't
    modelled here — `is_decidable` layers holiday-awareness on top (a weekday
    that never traded is forgiven only once it's actually in the past)."""
    p = d - timedelta(days=1)
    while p.weekday() >= 5:  # 5=Sat, 6=Sun
        p -= timedelta(days=1)
    return p


def current_rebalance_date(today: date, weekday: int = 0) -> date:
    """The rebalance date of `today`'s period — the first `weekday` of its
    month (Mon=0..Sun=6). For August 2026 with weekday=0: Monday the 3rd."""
    return _first_weekday_on_or_after(date(today.year, today.month, 1), weekday)


def deciding_bar(rebalance_date: date) -> date:
    """THE CLOSE A REBALANCE IS DECIDED ON — the trading day strictly before it.

    The signal cutoff is strict `<` on the rebalance date (never train on the
    bar we trade), so a first-Monday rebalance is decided on the preceding
    Friday's close and enters at Monday's. That Friday bar is the *only* price
    data a rebalance needs: a first-Monday August rebalance is fully decidable
    on the Friday of July.
    """
    return _prior_trading_day(rebalance_date)


def sessions_between(earlier: date, later: date) -> int:
    """Weekdays strictly after `earlier`, up to and including `later` — how many
    trading sessions a bar dated `earlier` has MISSED as of `later`. 0 when they
    are the same day (or `later` precedes `earlier`).

    Mon–Fri only, holiday-agnostic. Calendar days can't answer this question: a
    Thursday bar and a Tuesday bar are both "3 days" from the following Friday
    and Monday respectively, while one has missed a single session and the other
    has missed three."""
    if later <= earlier:
        return 0
    n = 0
    d = earlier + timedelta(days=1)
    while d <= later:
        if d.weekday() < 5:
            n += 1
        d += timedelta(days=1)
    return n


def is_decidable(
    rebalance_date: date,
    *,
    today: date,
    latest_data_date: date | None,
    max_holiday_gap_days: int = _MAX_HOLIDAY_GAP_DAYS,
) -> bool:
    """Has `rebalance_date`'s deciding bar settled into the data we hold?

    ⚠ THE TEST IS AGAINST THE TRADING CALENDAR, NEVER THE CALENDAR MONTH. "Do we
    have a close dated inside the current month?" is unsatisfiable on the 1st and
    2nd of a month that opens on a weekend: August 2026 begins on a Saturday, so
    the newest close in existence on Sunday the 2nd is Friday 31 July — correct,
    current data, and a month-anchored gate rejects it as two days stale while
    the first Monday's rebalance is already fully decidable from it.

    `_prior_trading_day` is holiday-UNAWARE: it can land on a weekday that never
    traded (the US July-4th observance on Fri 07-03, whose real deciding bar is
    Thu 07-02; any Monday holiday whose real bar is the prior Friday). So a
    calendar deciding bar that has already PASSED and sits only a holiday-sized
    gap beyond our data is forgiven — but NOT one that simply hasn't occurred yet
    (a future bar we must wait for), nor a wide gap, which signals genuinely
    stale data (an outage) rather than a holiday.
    """
    ldd = today if latest_data_date is None else min(today, latest_data_date)
    dbar = deciding_bar(rebalance_date)
    if dbar <= ldd:
        return True
    return dbar < today and (dbar - ldd).days <= max_holiday_gap_days


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
