"""`run_current_portfolio` — compute the strategy's current month-to-date
holdings + per-trading-day picks.

Mirrors a single iteration of `run_backtest` (month-start signals → score
→ select), then walks each trading day inside the current month to build
the daily-picks panel that backs the UI's "Daily picks history" view."""
from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import Any, Callable

import pandas as pd

from ..scoring import extract_category_scores, score_and_select
from ..signals import compute_signals_panel
from .dates import _first_weekday_on_or_after
from .indices import (
    _build_price_index,
    _build_volume_index,
    _date_on_or_after,
    _price_on_or_after,
    _price_on_or_before,
)
from .types import BacktestConfig, CurrentPortfolio, DailyPick, PeriodHolding

_logger = logging.getLogger(__name__)


def _build_holding(
    row: pd.Series,
    *,
    weight: float,
    currency: str | None,
    entry_price_eur: float | None,
    exit_price_eur: float | None,
    entry_price_local: float | None,
    exit_price_local: float | None,
    entry_date: str | None,
    exit_date: str | None,
    forward_return_pct: float | None,
) -> PeriodHolding:
    """Assemble a `PeriodHolding` from a scored selection row + the caller's
    already-resolved prices/dates. Centralizes the identity + score +
    category-score + rank + rounding boilerplate shared by the month-start
    holdings and the per-day picks — which differ only in how they resolve
    entry/exit prices (on-or-after + latest close vs on-or-before + None)."""
    score_val = row.get("momentum_score")
    sec_rank = row.get("sector_rank")
    co_rank = row.get("company_rank")
    return PeriodHolding(
        company_id=int(row["company_id"]),
        ticker=str(row.get("gurufocus_ticker", "")),
        company_name=str(row.get("company_name", "")),
        sector=str(row["sector"]),
        score=round(float(score_val), 2) if pd.notna(score_val) else 0.0,
        category_scores=extract_category_scores(row),
        weight=weight,
        forward_return_pct=forward_return_pct,
        currency=currency,
        entry_price_local=round(entry_price_local, 4) if entry_price_local is not None else None,
        exit_price_local=round(exit_price_local, 4) if exit_price_local is not None else None,
        entry_price_eur=round(entry_price_eur, 4) if entry_price_eur is not None else None,
        exit_price_eur=round(exit_price_eur, 4) if exit_price_eur is not None else None,
        entry_date=entry_date,
        exit_date=exit_date,
        sector_rank=int(sec_rank) if pd.notna(sec_rank) else None,
        company_rank=int(co_rank) if pd.notna(co_rank) else None,
    )


def run_current_portfolio(
    config: BacktestConfig,
    prices_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    send_event: Callable[..., Any] | None = None,
    *,
    volumes_df: pd.DataFrame | None = None,
    monthly_eligible: dict[str, dict[int, str | None]] | None = None,
    prices_local_df: pd.DataFrame | None = None,
    company_currency: dict[int, str | None] | None = None,
    today: date | None = None,
) -> CurrentPortfolio:
    """Compute the strategy's portfolio for the current month with MTD returns.

    Mirrors a single iteration of run_backtest:
      * as_of_date = first of current month
      * signals computed using prices strictly before as_of_date
      * entry_price = first price on/after as_of_date
      * exit_price = LATEST available price (vs run_backtest's next-month price)
      * forward_return_pct field carries the MTD return

    Random selection mode is not supported here — picking randomly for "what
    should I hold today" has no useful interpretation.
    """
    if config.selection_mode == "random":
        raise ValueError("run_current_portfolio does not support random selection mode")

    t_total_start = time.perf_counter()
    today_d = today or date.today()
    weekday = getattr(config, "rebalance_weekday", 0) or 0

    # Latest available close in the loaded data. A rebalance dated after
    # this hasn't happened yet — there's no price to enter at — so we're
    # still holding the prior period. Bound by `today` so a stray future
    # row can't move the anchor. e.g. on the first Wednesday before that
    # day's close has settled, the anchor is the prior (Tuesday) close.
    latest_data_date: date | None = None
    try:
        raw_max = prices_df["target_date"].max()
        if isinstance(raw_max, str):
            latest_data_date = date.fromisoformat(raw_max[:10])
        elif isinstance(raw_max, date) and not isinstance(raw_max, pd.Timestamp):
            latest_data_date = raw_max
        else:
            latest_data_date = pd.Timestamp(raw_max).date()
    except Exception:
        latest_data_date = None
    anchor = today_d if latest_data_date is None else min(today_d, latest_data_date)

    # Effective rebalance date = the chosen weekday's first occurrence in
    # the current month (e.g. first Wednesday). If that rebalance hasn't
    # happened yet relative to available data (`anchor`), the strategy is
    # still holding the *previous* period's picks — so step back to the
    # prior month's first-<weekday> rebalance. This makes "current picks"
    # mean "what we actually hold right now", matching run_backtest's
    # first-<weekday> grid rather than the literal 1st-of-month it used
    # before — and avoids an empty portfolio when the rebalance day's
    # close isn't in the DB yet.
    this_month_start = date(today_d.year, today_d.month, 1)
    rebalance_date = _first_weekday_on_or_after(this_month_start, weekday)
    if rebalance_date > anchor:
        prev_month_last = this_month_start - timedelta(days=1)
        prev_month_start = date(prev_month_last.year, prev_month_last.month, 1)
        rebalance_date = _first_weekday_on_or_after(prev_month_start, weekday)
    # The signal-cutoff + entry anchor for the locked-at-start holdings.
    month_start = rebalance_date
    month_key = rebalance_date.isoformat()[:7]

    if send_event:
        send_event("progress", month=month_key, pct=10, message=f"Computing signals as of {rebalance_date.isoformat()}...")

    # Filter universe for this month if snapshot-based — same logic as the
    # backtest loop, just for one month.
    month_universe_df = universe_df
    if monthly_eligible is not None:
        sector_map = monthly_eligible.get(month_key) or {}
        eligible_ids = set(sector_map.keys())
        if not eligible_ids:
            # Fall back to the most recent snapshot we have, since the
            # current month may not yet be populated in universe_membership.
            available_keys = sorted(monthly_eligible.keys())
            if available_keys:
                fallback_key = available_keys[-1]
                sector_map = monthly_eligible.get(fallback_key) or {}
                eligible_ids = set(sector_map.keys())
                if send_event:
                    send_event(
                        "warning",
                        scope="universe",
                        message=f"No universe snapshot for {month_key}; using latest available ({fallback_key})",
                    )
        if eligible_ids:
            month_universe_df = universe_df[
                universe_df["company_id"].isin(eligible_ids)
            ].copy().reset_index(drop=True)
            month_universe_df["sector"] = month_universe_df["company_id"].map(sector_map)

    # Build price/volume indices once
    price_index = _build_price_index(prices_df)
    local_price_index = (
        _build_price_index(prices_local_df)
        if prices_local_df is not None and not prices_local_df.empty
        else None
    )
    volume_index = _build_volume_index(volumes_df) if volumes_df is not None and not volumes_df.empty else None

    # Trading dates that fall inside the current month, derived from prices_df.
    # Built up front so the signal panel can compute every cutoff in one pass.
    trading_dates_set: set[date] = set()
    for raw_d in prices_df["target_date"].unique():
        if isinstance(raw_d, date) and not isinstance(raw_d, pd.Timestamp):
            dd = raw_d
        elif isinstance(raw_d, str):
            try:
                dd = date.fromisoformat(raw_d[:10])
            except ValueError:
                continue
        else:
            try:
                dd = pd.Timestamp(raw_d).date()
            except Exception:
                continue
        if month_start <= dd <= today_d:
            trading_dates_set.add(dd)
    trading_dates = sorted(trading_dates_set)

    # Single vectorized pass — computes every (company, cutoff) cell up front
    # so the daily loop below is a cheap dict lookup. Includes month_start so
    # the locked-at-start holdings use the same code path.
    t_panel = time.perf_counter()
    panel_cutoffs: list[date] = sorted({month_start, *trading_dates})
    panel = compute_signals_panel(
        month_universe_df, panel_cutoffs,
        price_index=price_index,
        volume_index=volume_index,
    )
    t_panel_elapsed = time.perf_counter() - t_panel

    t_month_start_signals = time.perf_counter()
    signals_df = panel.get(month_start, pd.DataFrame())
    t_month_start_signals_elapsed = time.perf_counter() - t_month_start_signals
    if signals_df.empty:
        if send_event:
            send_event("progress", month=month_key, pct=100, message="No companies had enough data for signals")
        return CurrentPortfolio(as_of_date=month_start.isoformat(), latest_price_date=None, holdings=[])

    if send_event:
        send_event("progress", month=month_key, pct=60, message=f"Scoring {len(signals_df)} companies...")

    # Score and select — same path as backtest momentum mode
    t_month_start_select = time.perf_counter()
    selected = score_and_select(
        signals_df,
        config.signal_weights,
        top_n_sectors=config.top_n_sectors,
        top_n_per_sector=config.top_n_per_sector,
        category_weights=config.category_weights,
        min_price_score=config.min_price_score,
    )
    t_month_start_select_elapsed = time.perf_counter() - t_month_start_select

    if selected.empty:
        if send_event:
            send_event("progress", month=month_key, pct=100, message="No companies passed selection")
        return CurrentPortfolio(as_of_date=month_start.isoformat(), latest_price_date=None, holdings=[])

    if send_event:
        send_event("progress", month=month_key, pct=85, message="Computing MTD returns...")

    n_holdings = len(selected)
    weight = 1.0 / n_holdings
    entry_ts = pd.Timestamp(month_start)

    holdings: list[PeriodHolding] = []
    latest_observed: pd.Timestamp | None = None

    for _, row in selected.iterrows():
        cid = int(row["company_id"])
        series = price_index.get(cid)

        entry_price = _price_on_or_after(series, entry_ts) if series is not None else None
        # Exit = latest available price in the EUR series.
        exit_price = float(series.iloc[-1]) if series is not None and len(series) > 0 else None
        exit_dt_ts = series.index[-1] if series is not None and len(series) > 0 else None
        if exit_dt_ts is not None and (latest_observed is None or exit_dt_ts > latest_observed):
            latest_observed = exit_dt_ts

        mtd_return = None
        if entry_price and exit_price and entry_price > 0:
            mtd_return = round((exit_price / entry_price - 1) * 100, 2)

        local_series = local_price_index.get(cid) if local_price_index is not None else None
        entry_local = _price_on_or_after(local_series, entry_ts) if local_series is not None else None
        exit_local = float(local_series.iloc[-1]) if local_series is not None and len(local_series) > 0 else None

        date_series = local_series if local_series is not None else series
        entry_dt = _date_on_or_after(date_series, entry_ts) if date_series is not None else None
        exit_dt = (
            date_series.index[-1].strftime("%Y-%m-%d")
            if date_series is not None and len(date_series) > 0
            else None
        )

        holdings.append(_build_holding(
            row,
            weight=weight,
            currency=(company_currency or {}).get(cid),
            entry_price_eur=entry_price,
            exit_price_eur=exit_price,
            entry_price_local=entry_local,
            exit_price_local=exit_local,
            entry_date=entry_dt,
            exit_date=exit_dt,
            forward_return_pct=mtd_return,
        ))

    if send_event:
        send_event("progress", month=month_key, pct=85, message=f"{len(holdings)} holdings selected; computing daily picks…")

    # Daily picks: each cutoff already has its signals in `panel` from the
    # single vectorized pass above, so this loop is just per-day score+select
    # and holdings construction.
    daily_picks: list[DailyPick] = []
    prev_ids: set[int] = set()
    # Chain-linked cumulative MTD under the standard pre-rebalance convention:
    # day d's contribution to cum return = the previous day's (pre-rebalance)
    # portfolio held one trading day forward. Day 0 contributes 0% (we just
    # entered). Concretely: today's chain contribution == previous day's
    # next_day_return_pct (the same number, before % conversion). We
    # accumulate that into `cum_factor` and expose `(cum_factor − 1) × 100`
    # as `portfolio_return_pct` on each DailyPick.
    cum_factor = 1.0
    prev_d_ts: pd.Timestamp | None = None
    t_daily_loop_start = time.perf_counter()
    t_daily_signals_total = 0.0
    t_daily_select_total = 0.0
    t_daily_holdings_total = 0.0
    for i, d in enumerate(trading_dates):
        if send_event:
            pct = 85 + round(15 * (i + 1) / max(1, len(trading_dates)))
            send_event("progress", month=month_key, pct=pct, message=f"Daily picks {i + 1}/{len(trading_dates)}: {d.isoformat()}")

        t_signals = time.perf_counter()
        daily_signals = panel.get(d, pd.DataFrame())
        t_daily_signals_total += time.perf_counter() - t_signals
        if daily_signals.empty:
            continue
        t_select = time.perf_counter()
        daily_selected = score_and_select(
            daily_signals,
            config.signal_weights,
            top_n_sectors=config.top_n_sectors,
            top_n_per_sector=config.top_n_per_sector,
            category_weights=config.category_weights,
            min_price_score=config.min_price_score,
        )
        t_daily_select_total += time.perf_counter() - t_select
        if daily_selected.empty:
            continue
        t_holdings = time.perf_counter()

        day_ts = pd.Timestamp(d)
        day_weight = 1.0 / len(daily_selected)
        day_holdings: list[PeriodHolding] = []
        today_ids: set[int] = set()

        # Each daily pick is its own 1-day portfolio: bought at THAT day's
        # close, sold at the NEXT trading day's close. Per-stock exit prices
        # and forward_return_pct are filled in on the next iteration once we
        # have tomorrow's prices. The same backfill computes the prior day's
        # next_day_return_pct (= chain-link contribution to cumulative MTD).
        prior_one_day_return: float | None = None
        if daily_picks and prev_d_ts is not None:
            prev_pick = daily_picks[-1]
            forward_components: list[float] = []
            for h in prev_pick.holdings:
                series = price_index.get(h.company_id)
                if series is None:
                    continue
                today_eur_pair = _price_on_or_before(series, day_ts)
                if today_eur_pair is None:
                    continue
                today_eur, _ = today_eur_pair

                local_series = local_price_index.get(h.company_id) if local_price_index is not None else None
                local_pair = _price_on_or_before(local_series, day_ts) if local_series is not None else None
                today_local = local_pair[0] if local_pair is not None else None

                date_series = local_series if local_series is not None else series
                today_dt_pair = _price_on_or_before(date_series, day_ts) if date_series is not None else None
                today_dt = today_dt_pair[1].strftime("%Y-%m-%d") if today_dt_pair is not None else None

                # Mutate the previous day's holding object directly: it was
                # appended to daily_picks with exit fields blank.
                h.exit_price_eur = round(float(today_eur), 4)
                h.exit_price_local = round(float(today_local), 4) if today_local is not None else None
                h.exit_date = today_dt
                if h.entry_price_eur and h.entry_price_eur > 0:
                    ret = today_eur / h.entry_price_eur - 1
                    h.forward_return_pct = round(ret * 100.0, 2)
                    forward_components.append(ret)
            if forward_components:
                prior_one_day_return = sum(forward_components) / len(forward_components)
                prev_pick.next_day_return_pct = round(prior_one_day_return * 100.0, 2)

        for _, drow in daily_selected.iterrows():
            cid = int(drow["company_id"])
            today_ids.add(cid)

            series = price_index.get(cid)
            entry_pair = _price_on_or_before(series, day_ts) if series is not None else None
            entry_price = entry_pair[0] if entry_pair is not None else None

            local_series = local_price_index.get(cid) if local_price_index is not None else None
            entry_local_pair = _price_on_or_before(local_series, day_ts) if local_series is not None else None
            entry_local = entry_local_pair[0] if entry_local_pair is not None else None

            date_series = local_series if local_series is not None else series
            entry_dt_pair = _price_on_or_before(date_series, day_ts) if date_series is not None else None
            entry_dt = entry_dt_pair[1].strftime("%Y-%m-%d") if entry_dt_pair is not None else None

            # Exit fields are intentionally None here. The next iteration
            # backfills them once tomorrow's prices are available; the latest
            # day in the panel keeps None (no next trading day yet).
            day_holdings.append(_build_holding(
                drow,
                weight=day_weight,
                currency=(company_currency or {}).get(cid),
                entry_price_eur=entry_price,
                exit_price_eur=None,
                entry_price_local=entry_local,
                exit_price_local=None,
                entry_date=entry_dt,
                exit_date=None,
                forward_return_pct=None,
            ))

        # Pre-rebalance chain link: today's contribution to cum MTD is the
        # PREVIOUS day's portfolio held one trading day forward (computed
        # above as `prior_one_day_return`). Day 0 contributes 0% — we just
        # entered. After day 0, port_mtd reads (cum_factor − 1) × 100,
        # carrying the running cumulative return through rebalances.
        if i == 0:
            port_mtd = 0.0
        elif prior_one_day_return is not None:
            cum_factor *= (1.0 + prior_one_day_return)
            port_mtd = round((cum_factor - 1.0) * 100.0, 2)
        else:
            # No valid prior-portfolio prices — leave cum unchanged, no return.
            port_mtd = None

        # Turnover: max of (stocks added today, stocks removed today).
        # For a fixed-size portfolio with N swaps, both equal N — so the
        # display reads "N stocks changed" intuitively. With size drift
        # the larger side is the more honest "movement" count.
        if prev_ids:
            adds = len(today_ids - prev_ids)
            removes = len(prev_ids - today_ids)
            turnover_abs = max(adds, removes)
            denom = max(len(today_ids), len(prev_ids), 1)
            turnover_pct = round(turnover_abs / denom * 100, 2)
        else:
            turnover_abs = 0
            turnover_pct = 0.0

        daily_picks.append(DailyPick(
            date=d.isoformat(),
            holdings=day_holdings,
            turnover_abs=turnover_abs,
            turnover_pct=turnover_pct,
            portfolio_return_pct=port_mtd,
        ))
        prev_ids = today_ids
        prev_d_ts = day_ts
        t_daily_holdings_total += time.perf_counter() - t_holdings

    t_daily_loop_elapsed = time.perf_counter() - t_daily_loop_start
    t_total_elapsed = time.perf_counter() - t_total_start
    n_days = len(trading_dates)
    universe_size = int(month_universe_df["company_id"].nunique()) if not month_universe_df.empty else 0
    timing_msg = (
        f"[run_current_portfolio timing] total={t_total_elapsed:.2f}s | "
        f"panel={t_panel_elapsed:.2f}s ({len(panel_cutoffs)} cutoffs) | "
        f"month_start: signals={t_month_start_signals_elapsed * 1000:.1f}ms, "
        f"select={t_month_start_select_elapsed:.2f}s | "
        f"daily_loop={t_daily_loop_elapsed:.2f}s ({n_days} days, "
        f"signals={t_daily_signals_total * 1000:.1f}ms (lookup), "
        f"select={t_daily_select_total:.2f}s avg={t_daily_select_total / max(n_days, 1) * 1000:.0f}ms/day, "
        f"holdings={t_daily_holdings_total:.2f}s) | "
        f"universe_size={universe_size}"
    )
    _logger.info(timing_msg)
    if send_event:
        send_event("timing", message=timing_msg)
        send_event("progress", month=month_key, pct=100, message=f"{len(holdings)} holdings, {len(daily_picks)} daily snapshots")

    return CurrentPortfolio(
        as_of_date=month_start.isoformat(),
        latest_price_date=latest_observed.strftime("%Y-%m-%d") if latest_observed is not None else None,
        holdings=holdings,
        daily_picks=daily_picks,
    )
