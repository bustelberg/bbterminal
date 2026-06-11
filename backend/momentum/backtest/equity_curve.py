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
    daily_timing: bool = False,
) -> tuple[list[tuple[str, float]], list[float], dict[int, int]]:
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
    # Which period (index into `period_records`) each curve day belongs to —
    # lets the daily-timing overlay attribute each cash<->stocks swap to the
    # rebalance period whose holdings it would trade, so per-exchange fees
    # apply to the right book.
    daily_period_idx: list[int] = []
    cumulative_factor = 1.0  # carries across periods

    for pidx, pr in enumerate(period_records):
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
            # Volatility-target exposure multiplier for this period (1.0 =
            # fully invested). When scale == 1.0 (vol targeting off — the
            # default + the original momentum strategy) we run the EXACT
            # original expression so non-targeted results stay byte-
            # identical. When scaled, the fully-invested book's relative
            # value `1 + book_pnl` becomes `1 + k·book_pnl` (the remaining
            # 1−k sits in cash, 0% return).
            scale = getattr(pr, "exposure_scale", 1.0) or 1.0
            if scale == 1.0:
                if strategy_type == "long_short":
                    period_relative = 1.0 + (long_avg - 1.0) - (short_avg - 1.0)
                else:
                    period_relative = long_avg
            elif strategy_type == "long_short":
                period_relative = 1.0 + scale * ((long_avg - 1.0) - (short_avg - 1.0))
            else:
                period_relative = 1.0 + scale * (long_avg - 1.0)

            daily_factor = cumulative_factor * period_relative
            daily_dates.append(day.date())
            daily_factors.append(daily_factor)
            daily_period_idx.append(pidx)

        if daily_factors:
            # Chain-link: next period starts where this one finishes.
            cumulative_factor = daily_factors[-1]

    # === Daily "tit-for-tat" timing overlay ============================
    # Re-chain the curve so we only capture a day's return when the
    # PRIOR day's underlying strategy return was non-negative; a negative
    # prior day puts us in cash (0%) today. The decision uses the
    # strategy's actual daily return (always observable), while what we
    # realize is gated. `>= 0` keeps us invested through flat days —
    # including the zero-return period-boundary duplicates this curve
    # carries — so only a genuine down day forces cash. Holdings are
    # unchanged; this only reshapes the realized daily path.
    # `swaps_per_period[idx]` = number of cash<->stocks transitions during
    # period `idx` — a full-book trade each, which the fee layer charges at
    # that period's per-exchange fees. Empty when timing is off.
    swaps_per_period: dict[int, int] = {}
    if daily_timing and len(daily_factors) >= 2:
        rets = [
            (daily_factors[i] / daily_factors[i - 1] - 1.0) if daily_factors[i - 1] > 0 else 0.0
            for i in range(1, len(daily_factors))
        ]  # rets[k] = return realized ON day k+1
        timed = [daily_factors[0]]
        prev_invested = True  # day 0 starts invested
        for t in range(1, len(daily_factors)):
            # Invested today iff yesterday's return (rets[t-2]) was >= 0;
            # day 1 has no prior-day return, so we start invested.
            invested = True if t == 1 else (rets[t - 2] >= 0.0)
            if invested != prev_invested:
                # A swap: enter cash (sell book) or re-enter (buy book) —
                # one full-book trade, billed to this day's period.
                swaps_per_period[daily_period_idx[t]] = swaps_per_period.get(daily_period_idx[t], 0) + 1
            prev_invested = invested
            r = rets[t - 1] if invested else 0.0
            timed.append(timed[-1] * (1.0 + r))
        daily_factors = timed

    daily_records = [
        (d.isoformat(), round((f - 1) * 100, 4))
        for d, f in zip(daily_dates, daily_factors)
    ]
    daily_returns: list[float] = []
    for i in range(1, len(daily_factors)):
        prev = daily_factors[i - 1]
        if prev > 0:
            daily_returns.append(daily_factors[i] / prev - 1)
    return daily_records, daily_returns, swaps_per_period


def _compute_universe_period_daily(
    eligible_cids: set[int],
    price_index: dict[int, pd.Series],
    *,
    entry_ts: pd.Timestamp,
    exit_ts: pd.Timestamp,
) -> list[tuple[pd.Timestamp, float]]:
    """Per-trading-day relative factor for the universe equal-weight
    baseline inside ONE period. The factor on day t is the mean of
    `price[t] / entry_price` over every eligible cid that had a price
    on (or just before) `entry_ts`. 1.0 ≈ "no change from entry".

    Same staleness convention as `_build_daily_equity_curve`: a cid's
    missing intra-period days carry its most recent observed price
    forward (vectorized `ffill`, equivalent to per-day `Series.asof`
    but ~50× faster on universe-sized inputs).

    Caller chain-links across periods (multiplies prior period's final
    factor into the next period's series) to produce a continuous daily
    curve. Returns [] when no cid has both an entry price and at least
    one observed price inside the window."""
    if not eligible_cids or exit_ts <= entry_ts:
        return []

    # Per-cid entry-rebased price slices. `Series.asof(entry_ts)` picks
    # the latest available close on or before the entry timestamp — same
    # rule the per-period `compute_universe_period_return` uses, so the
    # daily and per-period chains stay anchored to identical entry
    # prices.
    slices: dict[int, pd.Series] = {}
    for cid in eligible_cids:
        s = price_index.get(int(cid))
        if s is None or s.empty:
            continue
        entry_px = s.asof(entry_ts)
        if pd.isna(entry_px) or entry_px == 0:
            continue
        sub = s[(s.index >= entry_ts) & (s.index <= exit_ts)]
        if sub.empty:
            continue
        slices[int(cid)] = sub / float(entry_px)

    if not slices:
        return []

    # Combine into a wide DataFrame, ffill so each cid's missing days
    # carry its most recent value. Per-day mean across columns skips
    # leading NaN cells (cid hadn't started trading yet within the
    # window), matching how the strategy's daily curve quietly drops
    # holdings whose `asof` returns NaN.
    df = pd.DataFrame(slices).sort_index().ffill()
    daily_relative = df.mean(axis=1, skipna=True)
    return [(ts, float(v)) for ts, v in daily_relative.items() if pd.notna(v)]


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
