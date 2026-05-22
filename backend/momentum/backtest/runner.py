"""Period-by-period backtest loop + multi-trial random aggregator.

`run_backtest` is the main entry point — for each rebalance period it
computes signals, picks holdings, computes forward returns, and chain-links
into a portfolio equity curve. Headline stats are derived from the daily
curve (for max DD + Sharpe) and the period chain (for total + annualized
return).

The per-period selection logic (sector-ETF branch, regular long/short
branch, the `_make_holding` price-lookup factory) lives in `_period.py`;
the final headline-stats / `BacktestResult` build lives in `_summary.py`.
This file owns the main loop and the multi-trial wrapper only.

`run_multi_trial_backtest` runs N independent random-selection backtests
with sequential seeds and aggregates mean ± std for the headline stats.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Callable

import numpy as np
import pandas as pd

from ._period import (
    adjust_open_period_holdings,
    compute_selection_period,
    compute_sector_etf_period,
)
from ._summary import _PeriodAccumulators, build_backtest_result
from .preparation import _BacktestPrepared, _prepare_backtest
from .types import (
    BacktestConfig,
    BacktestResult,
    BacktestSummary,
    PeriodRecord,
)


def run_backtest(
    config: BacktestConfig,
    prices_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    send_event: Callable[..., Any] | None = None,
    *,
    volumes_df: pd.DataFrame | None = None,
    monthly_eligible: dict[str, dict[int, str | None]] | None = None,
    prices_local_df: pd.DataFrame | None = None,
    company_currency: dict[int, str | None] | None = None,
    prepared: _BacktestPrepared | None = None,
    # benchmark_price_index: {benchmark_id: pd.Series(price, DatetimeIndex)}.
    # Required when config.selection_mode == "sector_etf"; ignored otherwise.
    # Holds the price history of each sector ETF the strategy may rotate
    # into. Caller pre-fetches from the `benchmark_price` table.
    benchmark_price_index: dict[int, pd.Series] | None = None,
    # {benchmark_id: (ticker, name)} for display in the holdings table.
    benchmark_meta: dict[int, tuple[str, str]] | None = None,
) -> BacktestResult:
    """Run a momentum backtest at the configured rebalance cadence.

    For each rebalance period:
    1. Compute price and volume signals using data strictly before the period
    2. Score and select top companies
    3. Compute forward return through the next rebalance date
    4. Track cumulative portfolio return

    If monthly_eligible is provided (from universe_snapshot), only companies
    in the eligible set for that month are considered. The eligibility table
    is keyed by YYYY-MM regardless of cadence — sub-monthly periods
    inherit the snapshot of the month they fall in.

    `prepared` is an internal optimization for `run_multi_trial_backtest`:
    when supplied, the periods / indices / signal panel are reused instead
    of being recomputed. None means compute fresh.
    """
    # Random selection has no meaningful interpretation for long-short — a
    # randomly-picked short bucket is just noise on top of a randomly-picked
    # long bucket, with no signal-driven structure. Catch it loudly here
    # instead of silently producing nonsense.
    if config.strategy_type == "long_short" and config.selection_mode == "random":
        raise ValueError("long_short strategy is incompatible with random selection mode")
    # `all` selection holds every eligible name in the universe — there's
    # no top/bottom split to drive a long-short book either.
    if config.strategy_type == "long_short" and config.selection_mode == "all":
        raise ValueError("long_short strategy is incompatible with 'all' selection mode")

    if prepared is None:
        prepared = _prepare_backtest(
            start_date=config.start_date,
            end_date=config.end_date,
            prices_df=prices_df,
            universe_df=universe_df,
            volumes_df=volumes_df,
            prices_local_df=prices_local_df,
            monthly_eligible=monthly_eligible,
            frequency=config.rebalance_frequency,
        )
    periods = prepared.periods
    price_index = prepared.price_index
    local_price_index = prepared.local_price_index
    # volume_index isn't used directly here — it was already incorporated
    # into the precomputed signal panel during _prepare_backtest.
    panel = prepared.panel
    # Every record carries the full YYYY-MM-DD of the rebalance — including
    # monthly/2m/3m, which now align to first-Monday-of-month and so have a
    # meaningful day component. The frontend's `.slice(0, 7)` callers still
    # work (they just trim the day to bucket by month).
    sub_monthly = prepared.frequency in ("daily", "weekly")  # noqa: F841 — retained for _record_label below

    # === Open-period extension ===========================================
    # If config requests it and there's real price data available beyond the
    # last scheduled rebalance, append today's last-available trading date as
    # an extra "exit" date. The main loop then naturally processes
    # periods[-2] → periods[-1] as the open period; we mark that record
    # is_open=True and skip its return when accumulating headline stats.
    open_iter_idx = -1
    if config.include_open_period:
        last_avail_ts: pd.Timestamp | None = None
        for s in price_index.values():
            if s.empty:
                continue
            m = s.index.max()
            if last_avail_ts is None or m > last_avail_ts:
                last_avail_ts = m
        if last_avail_ts is not None and pd.Timestamp(periods[-1]) < last_avail_ts:
            periods = list(periods) + [last_avail_ts.date()]
            open_iter_idx = len(periods) - 2  # index in periods[:-1] for the open entry

    def _record_date(d: date) -> str:
        # Always the exact rebalance Monday — sub-monthly + calendar-stride
        # frequencies all return YYYY-MM-DD. The previous YYYY-MM short
        # form for monthly/Nm hid the actual day; with first-Monday
        # alignment the day matters.
        return d.isoformat()

    def _record_label(d: date) -> str:
        # Friendly log label — sub-monthly shows the exact date, longer
        # cadences round to month-name for progress lines like
        # "Computing signals for May 2024…".
        return d.isoformat() if sub_monthly else d.strftime("%b %Y")

    period_records: list[PeriodRecord] = []
    accum = _PeriodAccumulators()
    prev_holdings_set: set[int] = set()

    # Random selector RNG: seeded once per backtest so re-runs with the same
    # seed produce identical picks across all periods.
    rng = (
        np.random.default_rng(config.random_seed)
        if config.selection_mode == "random"
        else None
    )

    for i, period_date in enumerate(periods[:-1]):  # last period has no forward return
        next_period = periods[i + 1]
        # True only for the trailing open-period iteration (when extension is
        # active). The loop treats it like any other period — same signals,
        # same selection, same forward-return calc — but stats accumulators
        # below skip its return so closed-period headline numbers are
        # unaffected.
        is_open_iter = i == open_iter_idx
        pct = round((i / (len(periods) - 1)) * 100)

        if send_event:
            send_event(
                "progress",
                month=_record_date(period_date),
                pct=pct,
                message=f"Computing signals for {_record_label(period_date)}...",
            )

        # Resolve this period's eligible set + sector map (snapshot-based universes only).
        sector_map: dict[int, str | None] = {}
        eligible_ids: set[int] | None = None
        if monthly_eligible is not None:
            month_key = period_date.isoformat()[:7]
            sector_map = monthly_eligible.get(month_key) or {}
            eligible_ids = set(sector_map.keys())
            if not eligible_ids:
                snap_min = min(monthly_eligible.keys())
                snap_max = max(monthly_eligible.keys())
                if month_key < snap_min or month_key > snap_max:
                    reason = f"Month is outside universe snapshot range ({snap_min} to {snap_max})"
                else:
                    reason = "All companies in the universe snapshot failed screening criteria for this month (0 passing)"
                if send_event:
                    send_event(
                        "warning",
                        scope="universe",
                        message=f"{_record_label(period_date)}: {reason}",
                    )
                period_records.append(PeriodRecord(
                    date=_record_date(period_date),
                    holdings=[],
                    portfolio_return_pct=None,
                    cumulative_return_pct=round(accum.cumulative, 2),
                    empty_reason=reason,
                    is_open=is_open_iter,
                ))
                continue

        # Look up signals for this period from the precomputed panel, then
        # apply the per-month universe filter + sector remap when using a
        # snapshot-based universe (the panel was built from the base
        # `universe_df` whose sector is None for snapshot universes).
        signals_df = panel.get(period_date, pd.DataFrame())
        if not signals_df.empty and eligible_ids is not None:
            signals_df = signals_df[signals_df["company_id"].isin(eligible_ids)].copy()
            signals_df["sector"] = signals_df["company_id"].map(sector_map)
        if signals_df.empty:
            reason = f"No companies had enough price data (need >= 20 data points before {_record_label(period_date)})"
            if send_event:
                send_event(
                    "progress",
                    month=_record_date(period_date),
                    pct=pct,
                    message=f"{_record_label(period_date)}: 0 holdings — {reason}",
                )
                send_event(
                    "warning",
                    scope="backtest",
                    message=f"{_record_label(period_date)}: {reason}",
                )
            period_records.append(PeriodRecord(
                date=_record_date(period_date),
                holdings=[],
                portfolio_return_pct=None,
                cumulative_return_pct=round(accum.cumulative, 2),
                empty_reason=reason,
                is_open=is_open_iter,
            ))
            continue

        # Dispatch to the right per-period branch.
        if config.selection_mode == "sector_etf":
            outcome = compute_sector_etf_period(
                signals_df, config,
                period_date=period_date,
                next_period=next_period,
                benchmark_price_index=benchmark_price_index,
                benchmark_meta=benchmark_meta,
                record_label=_record_label(period_date),
            )
        else:
            outcome = compute_selection_period(
                signals_df, config,
                period_date=period_date,
                next_period=next_period,
                rng=rng,
                price_index=price_index,
                local_price_index=local_price_index,
                company_currency=company_currency,
                record_label=_record_label(period_date),
            )

        # Forward any warnings the branch raised. Done before the
        # PeriodRecord append so the SSE order mirrors the inline version.
        if send_event:
            for w in outcome.warnings:
                send_event("warning", scope="backtest", message=w)

        # Open-period re-pricing: replace the universe-wide last_avail_ts
        # exit with the most recent date common to every held company.
        # Without this, holdings whose last close is earlier than the
        # global max get None for forward_return_pct and silently drop
        # out of the open-period portfolio return.
        open_as_of: date | None = None
        if is_open_iter and outcome.empty_reason is None:
            outcome, open_as_of = adjust_open_period_holdings(
                outcome,
                price_index=price_index,
                local_price_index=local_price_index,
                benchmark_price_index=benchmark_price_index,
                strategy_type=config.strategy_type,
            )

        # Empty-holdings branch: forward the empty_reason as both a record
        # and a warning, then move on.
        if outcome.empty_reason is not None:
            if send_event:
                send_event(
                    "warning", scope="backtest",
                    message=f"{_record_label(period_date)}: {outcome.empty_reason}",
                )
            period_records.append(PeriodRecord(
                date=_record_date(period_date),
                holdings=[],
                portfolio_return_pct=None,
                cumulative_return_pct=round(accum.cumulative, 2),
                empty_reason=outcome.empty_reason,
                is_open=is_open_iter,
            ))
            continue

        accum.holdings_counts.append(len(outcome.holdings))

        # Accumulate stats from CLOSED periods only. The open period's
        # display cumulative is computed separately so the chart line
        # continues, but it doesn't shift `cumulative_factor` / `cumulative`
        # / `all_period_returns` (those drive total_return, annualized,
        # Sharpe, etc., which we keep apples-to-apples with closed periods).
        record_cum = accum.cumulative
        if outcome.port_return is not None:
            if is_open_iter:
                record_cum = (accum.cumulative_factor * (1 + outcome.port_return / 100) - 1) * 100
            else:
                accum.cumulative_factor *= (1 + outcome.port_return / 100)
                accum.cumulative = (accum.cumulative_factor - 1) * 100
                accum.all_period_returns.append(outcome.port_return)
                record_cum = accum.cumulative

        # Turnover — skip when the row is the open period (it's a partial
        # holding, comparing it against the prior closed period inflates
        # the count and contaminates avg_monthly_turnover_pct).
        current_set = {h.company_id for h in outcome.holdings}
        if prev_holdings_set and not is_open_iter:
            overlap = len(current_set & prev_holdings_set)
            total = max(len(current_set), len(prev_holdings_set))
            turnover = round((1 - overlap / total) * 100, 2) if total > 0 else 0
            accum.turnover_values.append(turnover)
        if not is_open_iter:
            prev_holdings_set = current_set

        period_records.append(PeriodRecord(
            date=_record_date(period_date),
            holdings=outcome.holdings,
            portfolio_return_pct=outcome.port_return,
            cumulative_return_pct=round(record_cum, 2),
            is_open=is_open_iter,
            as_of_date=open_as_of.isoformat() if open_as_of else None,
        ))

    if send_event:
        send_event("progress", month="done", pct=100, message="Backtest complete")

    return build_backtest_result(
        period_records, accum,
        price_index=price_index,
        strategy_type=config.strategy_type,
        benchmark_price_index=benchmark_price_index,
        rebalance_frequency=prepared.frequency,
    )


def run_multi_trial_backtest(
    config: BacktestConfig,
    prices_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    n_trials: int,
    send_event: Callable[..., Any] | None = None,
    *,
    volumes_df: pd.DataFrame | None = None,
    monthly_eligible: dict[str, dict[int, str | None]] | None = None,
    prices_local_df: pd.DataFrame | None = None,
    company_currency: dict[int, str | None] | None = None,
) -> BacktestResult:
    """Run `n_trials` independent backtests with sequential seeds and return
    an aggregated BacktestResult.

    Headline summary stats are means across trials; *_std fields hold the
    cross-trial standard deviation. The equity curve (cumulative_return_pct
    on each PeriodRecord) is the per-month mean across trials. Holdings
    on each PeriodRecord come from the first trial — they're random
    anyway, so aggregating them isn't meaningful.

    Forces selection_mode="random". Caller controls the base seed via
    config.random_seed; trial seeds are base, base+1, ..., base+N-1.
    """
    if n_trials < 1:
        raise ValueError("n_trials must be >= 1")
    if config.selection_mode != "random":
        raise ValueError("run_multi_trial_backtest requires selection_mode='random'")

    base_seed = config.random_seed if config.random_seed is not None else 0

    # Build the price/volume indices and signal panel once — they only depend
    # on dates / prices / universe, none of which change across random trials.
    # This turns N-trial wall time from O(N × panel) into O(panel + N × select).
    if send_event and n_trials > 1:
        send_event(
            "progress",
            month="prepare",
            pct=0,
            message=f"Precomputing signals for {n_trials} trials...",
        )
    prepared = _prepare_backtest(
        start_date=config.start_date,
        end_date=config.end_date,
        prices_df=prices_df,
        universe_df=universe_df,
        volumes_df=volumes_df,
        prices_local_df=prices_local_df,
        monthly_eligible=monthly_eligible,
        frequency=config.rebalance_frequency,
    )

    trial_results: list[BacktestResult] = []
    for i in range(n_trials):
        if send_event:
            pct = round((i / n_trials) * 100)
            send_event(
                "progress",
                month=f"trial-{i + 1}",
                pct=pct,
                message=f"Trial {i + 1}/{n_trials} (seed={base_seed + i})...",
            )
        trial_config = BacktestConfig(
            start_date=config.start_date,
            end_date=config.end_date,
            signal_weights=config.signal_weights,
            top_n_sectors=config.top_n_sectors,
            top_n_per_sector=config.top_n_per_sector,
            category_weights=config.category_weights,
            selection_mode="random",
            random_seed=base_seed + i,
            rebalance_frequency=config.rebalance_frequency,
            strategy_type=config.strategy_type,
        )
        # No per-month progress for individual trials — too noisy.
        result = run_backtest(
            trial_config,
            prices_df,
            universe_df,
            send_event=None,
            volumes_df=volumes_df,
            monthly_eligible=monthly_eligible,
            prices_local_df=prices_local_df,
            company_currency=company_currency,
            prepared=prepared,
        )
        trial_results.append(result)

    # Aggregate: per-month mean cumulative return across trials. All trials
    # iterate the same month grid so records align by index.
    n_months = max(len(r.monthly_records) for r in trial_results)
    aggregated_records: list[PeriodRecord] = []
    base_records = trial_results[0].monthly_records  # holdings + dates from trial 0
    for m_idx in range(n_months):
        cum_values = []
        port_returns = []
        for tr in trial_results:
            if m_idx >= len(tr.monthly_records):
                continue
            rec = tr.monthly_records[m_idx]
            cum_values.append(rec.cumulative_return_pct)
            if rec.portfolio_return_pct is not None:
                port_returns.append(rec.portfolio_return_pct)
        if not cum_values:
            continue
        base = base_records[m_idx] if m_idx < len(base_records) else None
        aggregated_records.append(PeriodRecord(
            date=base.date if base else "",
            holdings=base.holdings if base else [],
            portfolio_return_pct=round(float(np.mean(port_returns)), 2) if port_returns else None,
            cumulative_return_pct=round(float(np.mean(cum_values)), 2),
            empty_reason=base.empty_reason if base else None,
        ))

    # Aggregate summary stats across trials.
    def _arr(field: str) -> np.ndarray:
        vals = [getattr(r.summary, field) for r in trial_results if getattr(r.summary, field) is not None]
        return np.array(vals, dtype=float) if vals else np.array([])

    def _mean_std(field: str) -> tuple[float | None, float | None]:
        a = _arr(field)
        if a.size == 0:
            return None, None
        return round(float(a.mean()), 2), round(float(a.std()), 2)

    tr_mean, tr_std = _mean_std("total_return_pct")
    ann_mean, ann_std = _mean_std("annualized_return_pct")
    dd_mean, dd_std = _mean_std("max_drawdown_pct")
    sharpe_mean, sharpe_std = _mean_std("sharpe_ratio")
    turn_mean, turn_std = _mean_std("avg_monthly_turnover_pct")

    # Use trial 0's drawdown periods + total_months + avg_holdings as
    # representative; per-trial std for drawdown periods isn't meaningful.
    base_summary = trial_results[0].summary
    summary = BacktestSummary(
        total_return_pct=tr_mean if tr_mean is not None else 0.0,
        annualized_return_pct=ann_mean if ann_mean is not None else 0.0,
        max_drawdown_pct=dd_mean if dd_mean is not None else 0.0,
        sharpe_ratio=sharpe_mean,
        avg_monthly_turnover_pct=turn_mean if turn_mean is not None else 0.0,
        total_months=base_summary.total_months,
        avg_holdings=base_summary.avg_holdings,
        top_drawdowns=base_summary.top_drawdowns,
        n_trials=n_trials,
        total_return_pct_std=tr_std,
        annualized_return_pct_std=ann_std,
        max_drawdown_pct_std=dd_std,
        sharpe_ratio_std=sharpe_std,
        avg_monthly_turnover_pct_std=turn_std,
    )

    if send_event:
        send_event("progress", month="done", pct=100, message=f"{n_trials} trials complete")

    # Daily curve: use trial 0's, same convention as `holdings` (random
    # trials would otherwise need per-date alignment, and the curves
    # themselves are random anyway). Cross-trial daily mean would be more
    # principled but the multi-trial path is rarely used.
    daily_curve = trial_results[0].daily_records if trial_results else []

    return BacktestResult(
        monthly_records=aggregated_records,
        summary=summary,
        daily_records=daily_curve,
    )
