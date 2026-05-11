"""Daily portfolio equity curve reconstruction + drawdown detection.

The runner produces period-level holdings (entry price → exit price);
these helpers walk each period day-by-day to produce a continuous daily
equity curve, then identify peak-to-trough-to-recovery drawdowns on
that curve."""
from __future__ import annotations

from datetime import date

import pandas as pd

from .types import DrawdownPeriod, PeriodRecord, StrategyType


def _build_daily_equity_curve(
    period_records: list[PeriodRecord],
    price_index: dict[int, pd.Series],
    strategy_type: StrategyType,
    benchmark_price_index: dict[int, pd.Series] | None = None,
) -> tuple[list[tuple[str, float]], list[float]]:
    """Reconstruct a daily portfolio equity curve from the period-level
    holdings.

    Within each period the portfolio's daily relative value is
        long-only:    mean over long-holdings of (price[t] / entry_price)
        long-short:   1 + mean(long_price/entry) − mean(short_price/entry)
    where each holding's `entry_price_eur` is the price it actually entered
    at and `price[t]` is the latest-available EUR close on or before day t.
    Periods are chain-linked so the curve is continuous across rebalances.

    This produces stats (max DD, Sharpe) that respect intra-period moves —
    a monthly strategy that's flat at month-end after a -15% mid-month
    drawdown now reports that drawdown, where the period-level curve
    masked it.

    Sector-ETF holdings carry a NEGATIVE company_id (-benchmark_id) so
    their price lookups can't collide with real companies in price_index.
    Pass `benchmark_price_index` (keyed by positive benchmark_id) and this
    function will route negative-cid holdings through it. Without that,
    sector-ETF runs would have an empty daily curve and the headline
    stats would fall back to period-level Sharpe (= √1 for every_12_months).

    Returns:
        daily_records: [(YYYY-MM-DD, cumulative_return_pct), …]
        daily_returns: day-over-day arithmetic returns (for Sharpe).
    """
    def _series_for(cid: int) -> pd.Series | None:
        if cid < 0 and benchmark_price_index is not None:
            return benchmark_price_index.get(-cid)
        return price_index.get(cid)
    daily_dates: list[date] = []
    daily_factors: list[float] = []
    cumulative_factor = 1.0  # carries across periods

    for pr in period_records:
        if not pr.holdings:
            continue
        long_h = [h for h in pr.holdings if h.side == "long" and h.entry_price_eur not in (None, 0)]
        short_h = [h for h in pr.holdings if h.side == "short" and h.entry_price_eur not in (None, 0)]
        if not long_h and not short_h:
            continue

        # Period window from the holdings' actual trading dates.
        entry_iso = [h.entry_date for h in pr.holdings if h.entry_date]
        exit_iso = [h.exit_date for h in pr.holdings if h.exit_date]
        if not entry_iso or not exit_iso:
            continue
        period_start = pd.Timestamp(min(entry_iso))
        period_end = pd.Timestamp(max(exit_iso))

        # Union of trading days across all holdings inside the window.
        # exit_date is exclusive — it's the next period's entry day, the
        # close that day belongs to the next period's run.
        all_days_set: set[pd.Timestamp] = set()
        for h in pr.holdings:
            s = _series_for(h.company_id)
            if s is None:
                continue
            # Inclusive on both bounds: the exit day is the day we sell at,
            # and its price IS the period's realized close. Excluding it
            # left daily-rebalance periods with zero days inside the
            # window (each period was [T, T+1), so only T survived; that
            # day's factor is always 1.0 since entry_price == price[T],
            # producing the flat-line bug). Inclusive bounds duplicate the
            # boundary date in adjacent periods, but the values are equal
            # by chain-link construction so the chart is unaffected.
            mask = (s.index >= period_start) & (s.index <= period_end)
            all_days_set.update(s.index[mask].tolist())
        if not all_days_set:
            continue
        sorted_days = sorted(all_days_set)

        for day in sorted_days:
            long_vals: list[float] = []
            short_vals: list[float] = []
            for h in long_h:
                s = _series_for(h.company_id)
                if s is None:
                    continue
                # `asof` is O(log n) vs the O(n) boolean-mask slice; on a
                # 6-year monthly run this is the difference between ~22s
                # of curve construction and well under a second.
                v = s.asof(day)
                if pd.isna(v):
                    continue
                long_vals.append(float(v) / h.entry_price_eur)
            for h in short_h:
                s = _series_for(h.company_id)
                if s is None:
                    continue
                v = s.asof(day)
                if pd.isna(v):
                    continue
                short_vals.append(float(v) / h.entry_price_eur)

            long_avg = sum(long_vals) / len(long_vals) if long_vals else 1.0
            short_avg = sum(short_vals) / len(short_vals) if short_vals else 1.0
            if strategy_type == "long_short":
                period_relative = 1.0 + (long_avg - 1.0) - (short_avg - 1.0)
            else:
                period_relative = long_avg

            daily_factor = cumulative_factor * period_relative
            daily_dates.append(day.date())
            daily_factors.append(daily_factor)

        if daily_factors:
            # Chain-link: next period starts where this one finishes.
            cumulative_factor = daily_factors[-1]

    daily_records = [
        (d.isoformat(), round((f - 1) * 100, 4))
        for d, f in zip(daily_dates, daily_factors)
    ]
    daily_returns: list[float] = []
    for i in range(1, len(daily_factors)):
        prev = daily_factors[i - 1]
        if prev > 0:
            daily_returns.append(daily_factors[i] / prev - 1)
    return daily_records, daily_returns


def _find_drawdown_periods(values: list[tuple[str, float]]) -> list[DrawdownPeriod]:
    """Find all drawdown periods from a list of (date, portfolio_value) tuples.

    A drawdown starts when value drops below a peak and ends when the value
    recovers back to the peak level (or at the end of the series).
    """
    if len(values) < 2:
        return []

    periods: list[DrawdownPeriod] = []
    peak_val = values[0][1]
    peak_date = values[0][0]
    trough_val = peak_val
    trough_date = peak_date
    in_drawdown = False

    for dt, val in values[1:]:
        if val >= peak_val:
            # Recovered or new high
            if in_drawdown:
                dd_pct = round((trough_val / peak_val - 1) * 100, 2)
                periods.append(DrawdownPeriod(
                    drawdown_pct=dd_pct,
                    peak_date=peak_date,
                    trough_date=trough_date,
                    recovery_date=dt,
                ))
                in_drawdown = False
            peak_val = val
            peak_date = dt
            trough_val = val
            trough_date = dt
        else:
            in_drawdown = True
            if val < trough_val:
                trough_val = val
                trough_date = dt

    # Handle ongoing drawdown at end of series
    if in_drawdown:
        dd_pct = round((trough_val / peak_val - 1) * 100, 2)
        periods.append(DrawdownPeriod(
            drawdown_pct=dd_pct,
            peak_date=peak_date,
            trough_date=trough_date,
            recovery_date=None,
        ))

    return periods


def _pick_top_n_non_overlapping(periods: list[DrawdownPeriod], n: int) -> list[DrawdownPeriod]:
    """Pick the top N drawdowns by magnitude, excluding overlapping periods.

    A period overlaps if its peak-to-recovery range intersects with any
    already-selected period's peak-to-recovery range.
    """
    # Sort by drawdown magnitude (most negative first)
    sorted_periods = sorted(periods, key=lambda p: p.drawdown_pct)
    selected: list[DrawdownPeriod] = []

    for p in sorted_periods:
        if len(selected) >= n:
            break
        # Check overlap with already selected
        p_end = p.recovery_date or "9999-99"
        overlaps = False
        for s in selected:
            s_end = s.recovery_date or "9999-99"
            # Two ranges [p.peak, p_end] and [s.peak, s_end] overlap if:
            if p.peak_date <= s_end and p_end >= s.peak_date:
                overlaps = True
                break
        if not overlaps:
            selected.append(p)

    return selected
