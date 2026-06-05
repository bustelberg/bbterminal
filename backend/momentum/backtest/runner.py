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
    compute_universe_period_return,
)
from ._summary import _PeriodAccumulators, build_backtest_result
from .equity_curve import _compute_universe_period_daily
from .preparation import _BacktestPrepared, _prepare_backtest
from .types import (
    BacktestConfig,
    BacktestResult,
    BacktestSummary,
    PeriodRecord,
)
from ..scoring import score_universe


def _early_empty_record(
    period_date: date, cumulative: float, reason: str, is_open: bool,
) -> PeriodRecord:
    """The bare empty-period record emitted before any universe baseline is
    computed (month outside the snapshot range, or no company with enough
    price history). Carries only the running cumulative + the reason."""
    return PeriodRecord(
        date=period_date.isoformat(),
        holdings=[],
        portfolio_return_pct=None,
        cumulative_return_pct=round(cumulative, 2),
        empty_reason=reason,
        is_open=is_open,
    )


def _chain_strategy_return(
    accum: _PeriodAccumulators, port_return: float | None, is_open_iter: bool,
) -> float:
    """Chain-link this period's strategy return into the equity curve and
    return the record's cumulative_return_pct (unrounded).

    Closed periods advance `cumulative_factor` / `cumulative` /
    `all_period_returns`; the open period computes a display cumulative only —
    no accumulator bump — so total/annualized/Sharpe stay closed-only. A None
    return leaves the curve flat at the running cumulative."""
    if port_return is None:
        return accum.cumulative
    if is_open_iter:
        return (accum.cumulative_factor * (1 + port_return / 100) - 1) * 100
    accum.cumulative_factor *= (1 + port_return / 100)
    accum.cumulative = (accum.cumulative_factor - 1) * 100
    accum.all_period_returns.append(port_return)
    return accum.cumulative


def _chain_universe_baseline(
    accum: _PeriodAccumulators, universe_ret: float | None, is_open_iter: bool,
) -> float | None:
    """Chain-link the universe equal-weight baseline the same closed-vs-open
    way as the strategy chain: closed periods bump
    `universe_cumulative_factor` + record the period return; the open period
    computes a display value only (no accumulator bump, keeping headline
    universe stats apples-to-apples with closed periods). Returns the period's
    cumulative universe return % for the record (None when universe_ret is
    None)."""
    if universe_ret is None:
        return None
    if is_open_iter:
        return (accum.universe_cumulative_factor * (1 + universe_ret / 100) - 1) * 100
    accum.universe_cumulative_factor *= (1 + universe_ret / 100)
    accum.universe_period_returns.append(universe_ret)
    return (accum.universe_cumulative_factor - 1) * 100


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
    # Cadence-independent universe baseline result, precomputed and
    # passed in by the variants sweep so the same (universe, window)
    # isn't re-computed for every variant. None → compute in-line.
    # Shape matches the return of
    # `_period.compute_monthly_universe_baseline`.
    monthly_baseline_override: dict | None = None,
    # Per-period universe baseline cache, precomputed by the variants
    # sweep per (universe, frequency). Keyed by the period's date.
    # When present, the period loop short-circuits the in-line
    # `compute_universe_period_return` call for CLOSED periods. The
    # open-period re-compute stays in-line (it uses a variant-
    # specific `open_as_of` exit). None → compute in-line every
    # period.
    period_baseline_lookup: dict | None = None,
    # Cross-variant score cache. Keyed by period_date; value is the
    # per-period scored DataFrame produced by `score_universe`. The
    # variants sweep allocates ONE dict per (universe combo) and
    # passes it to every variant on that combo — the first variant
    # to hit each period populates the cache, subsequent variants
    # short-circuit the score pass entirely. Cache scope assumes
    # `signal_weights` and `category_weights` are constant across all
    # variants that share the dict (true today: variants vary only
    # over top_n / frequency / strategy / universe / grouping). Pass
    # None to disable caching (single-run path / tests / legacy
    # callers). Mutated in place — pass a fresh dict per sweep.
    score_cache: dict | None = None,
    # Win #3: per-sweep cache for the per-cid price math in
    # `make_period_holding`. Same lifecycle as `score_cache` — variants
    # sweep allocates one dict and passes it to every variant; siblings
    # on the same (cid, entry_ts, exit_ts) triple hit the cache. Pass
    # None to disable. Mutated in place.
    price_cache: dict | None = None,
    # Win #5: per-sweep cache for `select_from_scored`. Same lifecycle
    # as `score_cache`. Caches the pandas filter/groupby/nlargest by
    # `(period_date, top_n_sectors, top_n_per_sector, min_price_score,
    # direction)` so variants sharing those params on the same combo
    # skip the selection compute entirely.
    selection_cache: dict | None = None,
    # Side-effect callback invoked once per period with the period
    # date + the per-period eligibility-filtered signals_df, right
    # after that frame is built. Used by single_run.py to populate the
    # /signal-breakdown LRU so the user's first breakdown click after
    # a backtest lands in <500ms instead of re-loading 500k+ price
    # rows from Supabase. The callback receives the SAME shape (cids
    # + sector + signal columns) that /signal-breakdown computes
    # fresh, so caching it eliminates a redundant compute. None
    # disables warming (tests / library callers).
    panel_warm_callback: "Callable[[date, pd.DataFrame], None] | None" = None,
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
            rebalance_weekday=config.rebalance_weekday,
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
    # Latest available close across all companies — used both to drop
    # future rebalance dates and to anchor the open period.
    last_avail_ts: pd.Timestamp | None = None
    for s in price_index.values():
        if s.empty:
            continue
        m = s.index.max()
        if last_avail_ts is None or m > last_avail_ts:
            last_avail_ts = m

    # Drop trailing rebalance dates that fall AFTER the latest available
    # close. Such a rebalance hasn't happened yet — there's no price to
    # enter at — so keeping it would (a) leave an un-enterable trailing
    # period and (b) null out the PRIOR period's forward return, whose
    # exit is anchored to that date with no data. Dropping it lets the
    # prior period become the open period valued at the latest close.
    # e.g. a first-Wednesday grid evaluated before that Wednesday's close
    # has settled: the June-3 rebalance is dropped so the May-6 holding
    # shows its return through the latest available date instead of "—".
    # The `> 2` floor keeps at least one entry + one exit for the loop.
    if last_avail_ts is not None:
        while len(periods) > 2 and pd.Timestamp(periods[-1]) > last_avail_ts:
            periods = periods[:-1]

    open_iter_idx = -1
    if config.include_open_period:
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

    # Global trading calendar (union of every company's price dates). Used
    # to price each rebalance at the PRIOR trading day's close — the same
    # bar the signals are computed from (strict-`<` the rebalance date). So
    # a first-Wednesday rebalance enters at Tuesday's close, first-Monday at
    # the prior Friday's close, etc. — decision and execution on the same
    # observable close, no look-ahead. The signal cutoff still uses the
    # unshifted `period_date`; only the price lookups shift.
    _all_trading_days = (
        pd.DatetimeIndex(np.unique(np.concatenate([s.index.values for s in price_index.values()])))
        if price_index else pd.DatetimeIndex([])
    )

    def _prev_trading_ts(d: date) -> pd.Timestamp:
        """The last trading day strictly before `d`. Falls back to `d`
        itself when there's no earlier bar (start of data)."""
        ts = pd.Timestamp(d)
        pos = _all_trading_days.searchsorted(ts, side="left")
        return _all_trading_days[pos - 1] if pos > 0 else ts

    period_records: list[PeriodRecord] = []
    # Roll up empty-selection events into a single warning emitted at
    # the end. Each entry is (period_date, empty_reason); we emit a
    # one-liner per run summarizing N empty periods + a sample of dates.
    _empty_reasons: list[tuple] = []
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
                period_records.append(
                    _early_empty_record(period_date, accum.cumulative, reason, is_open_iter)
                )
                continue

        # Look up signals for this period from the precomputed panel, then
        # apply the per-month universe filter + sector remap when using a
        # snapshot-based universe (the panel was built from the base
        # `universe_df` whose sector is None for snapshot universes).
        signals_df = panel.get(period_date, pd.DataFrame())
        if not signals_df.empty and eligible_ids is not None:
            signals_df = signals_df[signals_df["company_id"].isin(eligible_ids)].copy()
            signals_df["sector"] = signals_df["company_id"].map(sector_map)
        # Warm the /signal-breakdown LRU with this period's eligibility-
        # filtered panel — same shape /signal-breakdown computes fresh.
        # After the backtest, the user's first breakdown click for any
        # stock at any covered cutoff hits the cache instead of paying
        # the 10s universe-load + panel-compute cost.
        if panel_warm_callback is not None and not signals_df.empty:
            try:
                panel_warm_callback(period_date, signals_df)
            except Exception:
                # Warming is best-effort — never break the backtest if
                # the breakdown LRU misbehaves (full disk, lock contention,
                # etc.). The user just pays the cost on first breakdown.
                pass
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
            period_records.append(
                _early_empty_record(period_date, accum.cumulative, reason, is_open_iter)
            )
            continue

        # Compute the universe-equal-weight baseline ONCE per period
        # from the same `signals_df` the strategy will operate on. For
        # closed periods the exit is `next_period`; for open periods we
        # re-compute below after `adjust_open_period_holdings` resolves
        # the effective `open_as_of`. Branches 1 and 2 above already
        # `continue`d on empty `signals_df`, so we always have a non-
        # empty candidate set here.
        # Price entries/exits at the prior trading day's close (see
        # `_prev_trading_ts`). The open period's exit is a valuation at the
        # latest available close — a "value as of today" point, not a
        # rebalance boundary — so it is NOT shifted back.
        entry_ts = _prev_trading_ts(period_date)
        closed_exit_ts = (
            pd.Timestamp(next_period) if is_open_iter else _prev_trading_ts(next_period)
        )
        # Variant-sweep cache short-circuit: when the orchestrator
        # precomputed per-period baselines for this (universe, freq),
        # reuse the cached `(return_pct, n_constituents)` instead of
        # recomputing here. Identical math; just avoids re-walking
        # every eligible cid's prices N variants in a row. Falls back
        # to in-line compute for periods not in the cache (open
        # period's variant-specific exit) or when no cache was passed
        # (single-run path / legacy callers).
        if period_baseline_lookup is not None and period_date in period_baseline_lookup:
            universe_ret_closed, universe_n_closed = period_baseline_lookup[period_date]
        else:
            universe_ret_closed, universe_n_closed = compute_universe_period_return(
                signals_df, price_index,
                entry_ts=entry_ts, exit_ts=closed_exit_ts,
            )

        # Score the eligible universe ONCE per period — both the long
        # and short books (when long_short) and every variant sharing
        # the score cache reuse the same scored frame. Skipped for
        # selection modes that don't consult scores (random / all /
        # sector_etf): scoring would be wasted work and would also
        # require config.signal_weights to be valid, which "all" / etc.
        # don't necessarily satisfy.
        scored_df: pd.DataFrame | None = None
        needs_score = (
            config.selection_mode != "all"
            and config.selection_mode != "sector_etf"
            and rng is None
        )
        if needs_score:
            if score_cache is not None and period_date in score_cache:
                scored_df = score_cache[period_date]
            else:
                scored_df = score_universe(
                    signals_df,
                    config.signal_weights,
                    config.category_weights,
                )
                if score_cache is not None:
                    score_cache[period_date] = scored_df

        # Dispatch to the right per-period branch.
        if config.selection_mode == "sector_etf":
            outcome = compute_sector_etf_period(
                signals_df, config,
                period_date=period_date,
                next_period=next_period,
                benchmark_price_index=benchmark_price_index,
                benchmark_meta=benchmark_meta,
                record_label=_record_label(period_date),
                price_entry_ts=entry_ts,
                price_exit_ts=closed_exit_ts,
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
                scored_df=scored_df,
                price_cache=price_cache,
                selection_cache=selection_cache,
                price_entry_ts=entry_ts,
                price_exit_ts=closed_exit_ts,
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

        # For the open period, redo the universe baseline using the
        # SAME `open_as_of` exit the strategy used. Without this the
        # baseline reads its exit as `next_period` (in the future) and
        # silently returns None for every name, leaving the comparison
        # blank exactly when the user most wants it.
        if is_open_iter and open_as_of is not None:
            universe_ret, universe_n = compute_universe_period_return(
                signals_df, price_index,
                entry_ts=entry_ts, exit_ts=pd.Timestamp(open_as_of),
            )
        else:
            universe_ret, universe_n = universe_ret_closed, universe_n_closed

        # Daily universe equal-weight baseline curve for this period.
        # Same entry/exit semantics as the per-period universe return
        # above — closed periods exit at `next_period`, open periods at
        # `open_as_of`. Each day's relative factor is chain-linked via
        # the running `accum.universe_daily_factor` so the curve is
        # continuous across rebalances. Closed periods advance the
        # factor; open period feeds the display tail without bumping
        # the factor (matches the per-period chain's closed-only
        # accumulation, keeping headline universe stats unchanged).
        universe_period_exit_ts = (
            pd.Timestamp(open_as_of) if is_open_iter and open_as_of is not None
            else closed_exit_ts
        )
        eligible_cids_set = set(signals_df["company_id"].astype(int))
        per_period_universe_daily = _compute_universe_period_daily(
            eligible_cids_set, price_index,
            entry_ts=entry_ts, exit_ts=universe_period_exit_ts,
        )
        for day_ts, rel_factor in per_period_universe_daily:
            cum_factor = accum.universe_daily_factor * rel_factor
            accum.universe_daily_records.append(
                (day_ts.date().isoformat(), round((cum_factor - 1) * 100, 4))
            )
        if per_period_universe_daily and not is_open_iter:
            accum.universe_daily_factor *= per_period_universe_daily[-1][1]

        # Empty-holdings branch: record the empty reason on the
        # PeriodRecord below for UI surfacing, AND accumulate it into
        # a per-run summary that fires ONCE at the end of the run.
        # Per-period warnings (the previous behavior) drowned out
        # other warnings in a sweep — a single variant with an
        # aggressive min_price_score could emit 30-80 of these.
        if outcome.empty_reason is not None:
            _empty_reasons.append((period_date, outcome.empty_reason))
            universe_cum_record = _chain_universe_baseline(accum, universe_ret, is_open_iter)
            period_records.append(PeriodRecord(
                date=_record_date(period_date),
                holdings=[],
                portfolio_return_pct=None,
                cumulative_return_pct=round(accum.cumulative, 2),
                empty_reason=outcome.empty_reason,
                is_open=is_open_iter,
                universe_return_pct=universe_ret,
                universe_cumulative_return_pct=(
                    round(universe_cum_record, 2) if universe_cum_record is not None else None
                ),
                universe_constituents=universe_n if universe_ret is not None else None,
            ))
            continue

        accum.holdings_counts.append(len(outcome.holdings))

        # Accumulate stats from CLOSED periods only. The open period's
        # display cumulative is computed separately so the chart line
        # continues, but it doesn't shift `cumulative_factor` / `cumulative`
        # / `all_period_returns` (those drive total_return, annualized,
        # Sharpe, etc., which we keep apples-to-apples with closed periods).
        record_cum = _chain_strategy_return(accum, outcome.port_return, is_open_iter)

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

        # Chain-link the universe baseline using the same closed-vs-open
        # split the strategy uses above: open period updates the
        # display cumulative but does NOT bump the accumulator (so the
        # summary headline stays apples-to-apples with closed periods).
        universe_cum_record_v = _chain_universe_baseline(accum, universe_ret, is_open_iter)

        period_records.append(PeriodRecord(
            date=_record_date(period_date),
            holdings=outcome.holdings,
            portfolio_return_pct=outcome.port_return,
            cumulative_return_pct=round(record_cum, 2),
            is_open=is_open_iter,
            as_of_date=open_as_of.isoformat() if open_as_of else None,
            universe_return_pct=universe_ret,
            universe_cumulative_return_pct=(
                round(universe_cum_record_v, 2) if universe_cum_record_v is not None else None
            ),
            universe_constituents=universe_n if universe_ret is not None else None,
        ))

    # Per-run summary of empty-selection events. Replaces the per-period
    # warning storm — one variant with min_price_score=80 and a sparse
    # universe used to emit ~30-80 individual warnings; now it's a
    # single line per variant.
    if send_event and _empty_reasons:
        n_empty = len(_empty_reasons)
        first_dates = ", ".join(
            _record_label(d) for d, _ in _empty_reasons[:5]
        )
        more = f" (+{n_empty - 5} more)" if n_empty > 5 else ""
        # Pick the most common reason — typically every empty period
        # shares the same template ("X companies but none passed
        # selection (top_n=…, per=…)"), so any sample tells the story.
        sample_reason = _empty_reasons[0][1]
        send_event(
            "warning", scope="backtest",
            message=(
                f"{n_empty} period(s) produced no holdings (likely "
                f"min_price_score too restrictive). First: {first_dates}{more}. "
                f"Reason: {sample_reason}"
            ),
        )

    if send_event:
        send_event("progress", month="done", pct=100, message="Backtest complete")

    # Cadence-independent universe baseline. Walks calendar months
    # over the same window and chains equal-weighted 1-month returns,
    # so two variants on the same universe + window get IDENTICAL
    # headline universe annualized numbers (no rebalancing-drag
    # variation from the strategy's cadence). Overrides the per-period
    # cumulative-factor-derived value in `build_backtest_result`. None
    # when no universe was selected — the strategy-cadence value is
    # used as fallback.
    #
    # When `monthly_baseline_override` is supplied (variant sweeps
    # precompute one per universe-combo upfront, then pass the same
    # result to every variant on that combo), skip the in-line compute
    # — it's pure per (universe, window) so re-running it per variant
    # is wasted work.
    if monthly_baseline_override is not None:
        monthly_baseline = monthly_baseline_override
    else:
        from ._period import compute_monthly_universe_baseline  # noqa: PLC0415
        monthly_baseline = compute_monthly_universe_baseline(
            price_index, monthly_eligible,
            start_date=config.start_date,
            end_date=config.end_date,
        )

    return build_backtest_result(
        period_records, accum,
        price_index=price_index,
        strategy_type=config.strategy_type,
        benchmark_price_index=benchmark_price_index,
        rebalance_frequency=prepared.frequency,
        monthly_baseline=monthly_baseline,
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
    # Same as in `run_backtest` — variant sweeps precompute once per
    # (universe, window) and pass to every trial. Universe baseline
    # doesn't vary across random trials (deterministic per universe +
    # window), so trial 0 computes once and the rest reuse.
    monthly_baseline_override: dict | None = None,
    # Same as in `run_backtest`. Period baselines are shared across
    # trials within a multi-trial run too — the period dates +
    # eligible cids are deterministic per (universe, frequency),
    # which doesn't change between random trials.
    period_baseline_lookup: dict | None = None,
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
        rebalance_weekday=config.rebalance_weekday,
    )

    trial_results: list[BacktestResult] = []
    # Reusable monthly baseline across trials. Pre-seeded with the
    # caller's override when supplied (variant sweep case); otherwise
    # populated from trial 0's summary on first iteration so trials
    # 1..N-1 don't re-walk the calendar.
    shared_monthly_baseline: dict | None = monthly_baseline_override
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
        # Universe baseline is identical across trials (deterministic
        # per universe + window). Trial 0 computes it in-line;
        # subsequent trials reuse via the cached override.
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
            monthly_baseline_override=shared_monthly_baseline,
            period_baseline_lookup=period_baseline_lookup,
        )
        trial_results.append(result)
        # Capture trial 0's baseline for trials 1..N-1. The summary
        # carries it as `universe_*_return_pct`; we can reconstruct
        # the dict shape `run_backtest` expects from those fields.
        if shared_monthly_baseline is None:
            s = result.summary
            if s.universe_annualized_return_pct is not None or s.universe_total_return_pct is not None:
                shared_monthly_baseline = {
                    "annualized_pct": s.universe_annualized_return_pct,
                    "total_pct": s.universe_total_return_pct,
                    "n_months": 0,  # not surfaced on Summary; reused dict only uses the two pct fields
                }

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
    sortino_mean, _sortino_std = _mean_std("sortino_ratio")
    win_mean, _win_std = _mean_std("win_rate_pct")
    median_mean, _median_std = _mean_std("median_period_return_pct")
    turn_mean, turn_std = _mean_std("avg_monthly_turnover_pct")

    # Use trial 0's drawdown periods + total_months + avg_holdings as
    # representative; per-trial std for drawdown periods isn't meaningful.
    # Universe baseline is deterministic per (universe, window) — doesn't
    # vary across random trials — so trial 0's values carry through
    # unchanged for every trial.
    base_summary = trial_results[0].summary
    summary = BacktestSummary(
        total_return_pct=tr_mean if tr_mean is not None else 0.0,
        annualized_return_pct=ann_mean if ann_mean is not None else 0.0,
        max_drawdown_pct=dd_mean if dd_mean is not None else 0.0,
        sharpe_ratio=sharpe_mean,
        sortino_ratio=sortino_mean,
        win_rate_pct=win_mean,
        median_period_return_pct=median_mean,
        avg_monthly_turnover_pct=turn_mean if turn_mean is not None else 0.0,
        total_months=base_summary.total_months,
        avg_holdings=base_summary.avg_holdings,
        top_drawdowns=base_summary.top_drawdowns,
        universe_total_return_pct=base_summary.universe_total_return_pct,
        universe_annualized_return_pct=base_summary.universe_annualized_return_pct,
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
        # Universe baseline is deterministic per (universe, window),
        # not per trial — every trial produces the same daily curve.
        # Reuse trial 0's so the aggregated result carries it.
        universe_daily_records=(
            trial_results[0].universe_daily_records if trial_results else []
        ),
    )
