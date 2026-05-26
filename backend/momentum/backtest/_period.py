"""Per-period helpers for run_backtest.

Two branches:

  - `compute_sector_etf_period` — ranks sectors via stock-aggregate
    momentum and holds the user-mapped ETF for each picked sector.
    Long-only; no shorts.
  - `compute_selection_period` — the regular stock-picking branch
    (momentum / random / all selection modes, with optional long-short).

Both return the same `_PeriodOutcome` so the main loop can update the
shared accumulators (cumulative return, turnover, holdings_counts)
uniformly. The previously inline `_make_holding` closure is now the
module-level `make_period_holding` so the regular branch is just a
straight function call instead of a captured local.
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from datetime import date

import numpy as np
import pandas as pd

from ..scoring import (
    _get_category_keys,
    aggregate_to_sector,
    compute_category_scores,
    random_select,
    score_and_select,
    score_universe,
    select_from_scored,
)
from .indices import _date_on_or_after, _price_on_or_after, _price_on_or_before
from .types import (
    BacktestConfig,
    HoldingSide,
    PeriodHolding,
    StrategyType,
    _norm_sector,
)


def compute_monthly_universe_baseline(
    price_index: dict[int, pd.Series],
    monthly_eligible: dict[str, dict[int, str | None]] | None,
    *,
    start_date: date,
    end_date: date,
) -> dict | None:
    """Cadence-independent universe baseline. Walks calendar months
    from `start_date` through `end_date`, equal-weighting every
    eligible company at the start of each month and chaining the
    1-month forward returns. The result depends ONLY on (universe,
    window) — two strategies that share both get IDENTICAL baselines,
    regardless of whether they rebalance daily, monthly, or yearly.

    This is the number that surfaces as the headline "Universe
    annualized" in /backtest's Variants table. The per-strategy-
    cadence baseline (`compute_universe_period_return` below) still
    drives the per-period `universe_return_pct` + the equity-curve
    overlay, where matching the strategy's rebalance schedule is
    actually what the user wants.

    Returns `{annualized_pct, total_pct, n_months}` or None when no
    universe was selected / no eligible months had usable prices.

    Implementation notes: re-uses the same vectorized average-of-
    per-name-returns model as `compute_universe_period_return`, just
    called on every calendar-month pair instead of every strategy
    rebalance. `_price_on_or_after` handles month-start dates that
    fall on weekends/holidays."""
    if not monthly_eligible:
        return None

    # Build the list of calendar-month start dates spanning the window.
    cur = date(start_date.year, start_date.month, 1)
    end_month_start = date(end_date.year, end_date.month, 1)
    months: list[date] = []
    while cur <= end_month_start:
        months.append(cur)
        cur = date(
            cur.year + (1 if cur.month == 12 else 0),
            1 if cur.month == 12 else cur.month + 1,
            1,
        )
    if len(months) < 2:
        return None

    cumulative_factor = 1.0
    valid_months = 0
    for i in range(len(months) - 1):
        m_start = months[i]
        m_end = months[i + 1]
        key = f"{m_start.year:04d}-{m_start.month:02d}"
        eligible = monthly_eligible.get(key, {})
        if not eligible:
            continue
        entry_ts = pd.Timestamp(m_start)
        exit_ts = pd.Timestamp(m_end)
        returns: list[float] = []
        for cid in eligible.keys():
            series = price_index.get(int(cid))
            if series is None or len(series) == 0:
                continue
            entry = _price_on_or_after(series, entry_ts)
            exit_p = _price_on_or_after(series, exit_ts)
            if entry is None or exit_p is None or entry <= 0:
                continue
            returns.append((exit_p / entry - 1.0) * 100.0)
        if not returns:
            continue
        period_return_frac = float(np.mean(returns)) / 100.0
        cumulative_factor *= (1.0 + period_return_frac)
        valid_months += 1

    if valid_months == 0:
        return None

    total_pct = (cumulative_factor - 1.0) * 100.0
    # Annualize over the actual window span (same day-count convention as
    # the strategy's `annualized_return_pct`).
    n_years = max(0.0, (end_date - start_date).days / 365.25)
    annualized_pct: float | None = None
    if n_years > 0:
        annualized_pct = (cumulative_factor ** (1.0 / n_years) - 1.0) * 100.0
    return {
        "annualized_pct": (
            round(float(annualized_pct), 2) if annualized_pct is not None else None
        ),
        "total_pct": round(float(total_pct), 2),
        "n_months": valid_months,
    }


def compute_universe_period_return(
    signals_df: pd.DataFrame,
    price_index: dict[int, pd.Series],
    *,
    entry_ts: pd.Timestamp,
    exit_ts: pd.Timestamp,
) -> tuple[float | None, int]:
    """Equal-weighted return across every eligible company in `signals_df`
    over (`entry_ts`, `exit_ts`). The "what if you held the entire
    universe?" baseline a backtest compares itself against.

    Returns (return_pct, n_constituents). `return_pct` is None when no
    company had usable entry+exit prices for the window. `n_constituents`
    is the count of companies that DID contribute, useful for diagnostics.

    Implementation: vectorized over the EUR price index. No FX (entry
    and exit are both EUR already), no PeriodHolding construction, no
    category-score recompute — this is a side computation that runs
    every period, so it has to be cheap. A universe of ~2800 names
    completes in single-digit milliseconds."""
    if signals_df.empty or "company_id" not in signals_df.columns:
        return None, 0
    returns: list[float] = []
    for cid in signals_df["company_id"].astype(int):
        series = price_index.get(int(cid))
        if series is None or len(series) == 0:
            continue
        entry = _price_on_or_after(series, entry_ts)
        exit_p = _price_on_or_after(series, exit_ts)
        if entry is None or exit_p is None or entry <= 0:
            continue
        returns.append((exit_p / entry - 1) * 100.0)
    if not returns:
        return None, 0
    return round(float(np.mean(returns)), 4), len(returns)


@dataclass
class _PeriodOutcome:
    """What one period contributes back to the main loop."""
    holdings: list[PeriodHolding] = field(default_factory=list)
    port_return: float | None = None
    empty_reason: str | None = None
    # Warnings the branch raised — emitted by the main loop after each
    # period so the order remains stable across refactors.
    warnings: list[str] = field(default_factory=list)


def adjust_open_period_holdings(
    outcome: _PeriodOutcome,
    *,
    price_index: dict[int, pd.Series],
    local_price_index: dict[int, pd.Series] | None,
    benchmark_price_index: dict[int, pd.Series] | None,
    strategy_type: StrategyType,
) -> tuple[_PeriodOutcome, date | None]:
    """Re-price the *open* period's exits at the most recent date common to
    every held company.

    The runner picks `last_avail_ts` from the universe-wide max in
    `price_index`, which is fine for the entry-pricing pass but leaks
    nulls into the open period when some held companies stopped updating
    earlier (e.g. an EU stock that stopped reporting before a US stock
    did). `_price_on_or_after(stale_series, last_avail_ts)` then returns
    None for those holdings — so their `forward_return_pct`,
    `exit_price`, and `exit_date` all become null and they're silently
    omitted from the portfolio return.

    This post-processor walks the selected holdings, computes
    `common_max = min(price_index[h.cid].index.max())` across them, and
    re-prices every exit at that date with `_price_on_or_before` (which
    handles the rare case where the common date isn't a trading day for
    a particular holding — it falls back to the prior available close).
    The portfolio return is recomputed from the adjusted forward
    returns and the actual common-date is returned so the runner can
    surface it on the period record. Sector-ETF holdings (negative
    company_id) are routed through `benchmark_price_index` if supplied.
    """
    if not outcome.holdings:
        return outcome, None

    def _series_for(cid: int) -> pd.Series | None:
        if cid < 0 and benchmark_price_index is not None:
            return benchmark_price_index.get(-cid)
        return price_index.get(cid)

    def _local_series_for(cid: int) -> pd.Series | None:
        if cid < 0:
            return None  # benchmarks are EUR/USD only — no local series
        return local_price_index.get(cid) if local_price_index is not None else None

    holding_max_dates: list[pd.Timestamp] = []
    for h in outcome.holdings:
        s = _series_for(h.company_id)
        if s is None or s.empty:
            continue
        holding_max_dates.append(s.index.max())
    if not holding_max_dates:
        return outcome, None

    common_max_ts = min(holding_max_dates)
    common_max_date = common_max_ts.date()

    long_returns: list[float] = []
    short_returns: list[float] = []
    new_holdings: list[PeriodHolding] = []
    for h in outcome.holdings:
        s = _series_for(h.company_id)
        if s is None:
            new_holdings.append(h)
            continue
        exit_pair = _price_on_or_before(s, common_max_ts)
        if exit_pair is None:
            new_holdings.append(h)
            continue
        exit_price, exit_actual_ts = exit_pair

        local_s = _local_series_for(h.company_id)
        exit_local_pair = (
            _price_on_or_before(local_s, common_max_ts)
            if local_s is not None
            else None
        )
        exit_local = exit_local_pair[0] if exit_local_pair is not None else None

        new_fwd_return: float | None = None
        if h.entry_price_eur and exit_price and h.entry_price_eur > 0:
            new_fwd_return = round((exit_price / h.entry_price_eur - 1) * 100, 2)

        new_h = dataclasses.replace(
            h,
            exit_price_eur=round(exit_price, 4),
            exit_price_local=round(exit_local, 4) if exit_local is not None else None,
            exit_date=exit_actual_ts.strftime("%Y-%m-%d"),
            forward_return_pct=new_fwd_return,
        )
        new_holdings.append(new_h)
        if new_fwd_return is not None:
            if new_h.side == "long":
                long_returns.append(new_fwd_return)
            else:
                short_returns.append(new_fwd_return)

    if strategy_type == "long_short":
        long_avg = float(np.mean(long_returns)) if long_returns else None
        short_avg = float(np.mean(short_returns)) if short_returns else None
        if long_avg is not None and short_avg is not None:
            port_return: float | None = round(long_avg - short_avg, 2)
        elif long_avg is not None:
            port_return = round(long_avg, 2)
        elif short_avg is not None:
            port_return = round(-short_avg, 2)
        else:
            port_return = None
    else:
        port_return = round(float(np.mean(long_returns)), 2) if long_returns else None

    adjusted = dataclasses.replace(outcome, holdings=new_holdings, port_return=port_return)
    return adjusted, common_max_date


def make_period_holding(
    row: pd.Series,
    side: HoldingSide,
    weight: float,
    *,
    price_index: dict[int, pd.Series],
    local_price_index: dict[int, pd.Series] | None,
    company_currency: dict[int, str | None] | None,
    entry_ts: pd.Timestamp,
    exit_ts: pd.Timestamp,
) -> tuple[PeriodHolding, float | None]:
    """Build one PeriodHolding from a scored row. Returns the holding plus
    its forward-return so the caller can route the return into the long
    or short bucket."""
    cid = int(row["company_id"])
    series = price_index.get(cid)
    entry_price = _price_on_or_after(series, entry_ts) if series is not None else None
    exit_price = _price_on_or_after(series, exit_ts) if series is not None else None
    fwd_return: float | None = None
    if entry_price and exit_price and entry_price > 0:
        fwd_return = round((exit_price / entry_price - 1) * 100, 2)

    local_series = local_price_index.get(cid) if local_price_index is not None else None
    entry_local = _price_on_or_after(local_series, entry_ts) if local_series is not None else None
    exit_local = _price_on_or_after(local_series, exit_ts) if local_series is not None else None

    # Actual trading dates (prefer local series, fall back to EUR series).
    date_series = local_series if local_series is not None else series
    entry_dt = _date_on_or_after(date_series, entry_ts) if date_series is not None else None
    exit_dt = _date_on_or_after(date_series, exit_ts) if date_series is not None else None

    cat_scores: dict[str, float | None] = {}
    for cat in _get_category_keys():
        col = f"score_{cat}"
        if col in row.index and pd.notna(row[col]):
            cat_scores[cat] = round(float(row[col]), 1)
        else:
            cat_scores[cat] = None

    score_val = row.get("momentum_score")
    sec_rank = row.get("sector_rank")
    co_rank = row.get("company_rank")
    return PeriodHolding(
        company_id=cid,
        ticker=str(row.get("gurufocus_ticker", "")),
        company_name=str(row.get("company_name", "")),
        sector=str(row["sector"]),
        score=round(float(score_val), 2) if pd.notna(score_val) else 0.0,
        category_scores=cat_scores,
        weight=weight,
        forward_return_pct=fwd_return,
        currency=(company_currency or {}).get(cid),
        entry_price_local=round(entry_local, 4) if entry_local is not None else None,
        exit_price_local=round(exit_local, 4) if exit_local is not None else None,
        entry_price_eur=round(entry_price, 4) if entry_price is not None else None,
        exit_price_eur=round(exit_price, 4) if exit_price is not None else None,
        entry_date=entry_dt,
        exit_date=exit_dt,
        side=side,
        sector_rank=int(sec_rank) if pd.notna(sec_rank) else None,
        company_rank=int(co_rank) if pd.notna(co_rank) else None,
    ), fwd_return


def compute_sector_etf_period(
    signals_df: pd.DataFrame,
    config: BacktestConfig,
    *,
    period_date: date,
    next_period: date,
    benchmark_price_index: dict[int, pd.Series] | None,
    benchmark_meta: dict[int, tuple[str, str]] | None,
    record_label: str,
) -> _PeriodOutcome:
    """Sector-ETF branch. Ranks sectors via stock-aggregate momentum and
    holds the user-mapped ETF for each picked sector (one holding per
    sector) instead of picking individual stocks. Long-only — picking
    short sectors via ETF wasn't requested."""
    out = _PeriodOutcome()
    if not config.sector_etfs or benchmark_price_index is None:
        out.empty_reason = "sector_etf mode requires sector_etfs config + benchmark prices"
        return out

    # Rank sectors via stock-aggregate momentum (top half of score_and_select).
    scored_for_sectors = compute_category_scores(
        signals_df, config.signal_weights, config.category_weights,
    )
    sector_scores = aggregate_to_sector(scored_for_sectors)
    # Walk the FULL ranked sector list (not just the top-N) and collect
    # sectors that have a mapped ETF until we've filled top_n_sectors
    # slots. The earlier `head(top_n).filter(has_ETF)` version silently
    # lost the slot whenever a top-N sector had no matching ETF —
    # typically due to a sector-string mismatch between the company
    # table ("Technology", "Healthcare") and the benchmark sector tag
    # ("Information Technology", "Health Care"). `_norm_sector` handles
    # the common aliases.
    sector_etfs_norm = {_norm_sector(k): v for k, v in config.sector_etfs.items()}
    unmatched_top: list[str] = []
    chosen_pairs: list[tuple[str, int, float]] = []  # (display_sector, benchmark_id, score)
    for _, row in sector_scores.iterrows():
        if len(chosen_pairs) >= config.top_n_sectors:
            break
        sec = str(row["sector"])
        bid = sector_etfs_norm.get(_norm_sector(sec))
        if bid is None:
            if len(unmatched_top) < config.top_n_sectors:
                unmatched_top.append(sec)
            continue
        chosen_pairs.append((sec, int(bid), float(row.get("momentum_score") or 0)))

    # Surface unmatched sectors as a one-shot warning so the user knows
    # a string mismatch is dropping a slot — without this the symptom is
    # silently "sometimes 3 sectors instead of 4".
    if unmatched_top:
        out.warnings.append(
            f"{record_label}: "
            f"top sector(s) without a mapped ETF, "
            f"fell through to next: {unmatched_top} "
            f"(mapped: {sorted(config.sector_etfs.keys())})"
        )

    if not chosen_pairs:
        out.empty_reason = (
            f"{len(sector_scores)} sectors ranked but none matched a mapped ETF "
            f"(sector_etfs covers: {sorted(config.sector_etfs.keys())})"
        )
        return out

    entry_ts = pd.Timestamp(period_date)
    exit_ts = pd.Timestamp(next_period)
    weight = 1.0 / len(chosen_pairs)
    holdings: list[PeriodHolding] = []
    long_returns: list[float] = []
    for sec, bid, agg_score in chosen_pairs:
        series = benchmark_price_index.get(bid)
        entry_price = _price_on_or_after(series, entry_ts) if series is not None else None
        exit_price = _price_on_or_after(series, exit_ts) if series is not None else None
        fwd_return: float | None = None
        if entry_price and exit_price and entry_price > 0:
            fwd_return = round((exit_price / entry_price - 1) * 100, 2)
        bm_ticker, bm_name = (benchmark_meta or {}).get(bid, (f"BM:{bid}", f"Benchmark {bid}"))
        entry_dt = _date_on_or_after(series, entry_ts) if series is not None else None
        exit_dt = _date_on_or_after(series, exit_ts) if series is not None else None
        holdings.append(PeriodHolding(
            # Negative IDs distinguish ETF holdings from real company
            # rows downstream (frontend never queries metric_data for
            # negative cids, so they don't collide).
            company_id=-bid,
            ticker=bm_ticker,
            company_name=bm_name,
            sector=sec,
            score=round(agg_score, 2),
            category_scores={cat: None for cat in _get_category_keys()},
            weight=weight,
            forward_return_pct=fwd_return,
            currency="USD",
            entry_price_local=round(entry_price, 4) if entry_price is not None else None,
            exit_price_local=round(exit_price, 4) if exit_price is not None else None,
            entry_price_eur=round(entry_price, 4) if entry_price is not None else None,
            exit_price_eur=round(exit_price, 4) if exit_price is not None else None,
            entry_date=entry_dt,
            exit_date=exit_dt,
            side="long",
        ))
        if fwd_return is not None:
            long_returns.append(fwd_return)
    out.holdings = holdings
    out.port_return = round(float(np.mean(long_returns)), 2) if long_returns else None
    return out


def compute_selection_period(
    signals_df: pd.DataFrame,
    config: BacktestConfig,
    *,
    period_date: date,
    next_period: date,
    rng: np.random.Generator | None,
    price_index: dict[int, pd.Series],
    local_price_index: dict[int, pd.Series] | None,
    company_currency: dict[int, str | None] | None,
    record_label: str,
    # Pre-scored DataFrame from `score_universe` (computed once per
    # period by the runner, possibly served from a cross-variant
    # cache). When supplied, the long+short selections skip scoring
    # and just call `select_from_scored` on the same frame. None →
    # this branch computes its own scoring in-line (kept for callers
    # that don't go through `run_backtest`, though there aren't any
    # today — the runner is the sole caller).
    scored_df: pd.DataFrame | None = None,
) -> _PeriodOutcome:
    """Regular stock-picking branch — momentum / random / all selection
    modes, with optional long-short. Builds the long bucket (always) and
    the short bucket (long-short only), drops cross-book collisions, and
    computes the portfolio return."""
    out = _PeriodOutcome()

    # Select longs (always) and shorts (long-short only). For random
    # mode there's only ever one bucket — `selected_top` — and shorts
    # stay empty.
    if config.selection_mode == "all":
        # "Hold the whole universe" baseline — every eligible name in
        # the period equally weighted. Useful as a market-cap-naive
        # index proxy and as a control when comparing against
        # signal-driven selections. top_n_sectors / top_n_per_sector
        # are deliberately ignored: the whole point is no filtering.
        selected_top = signals_df.copy().reset_index(drop=True)
        selected_bottom = pd.DataFrame()
    elif rng is not None:
        selected_top = random_select(
            signals_df,
            top_n_sectors=config.top_n_sectors,
            top_n_per_sector=config.top_n_per_sector,
            rng=rng,
        )
        selected_bottom = pd.DataFrame()
    else:
        # Defensive fallback: when the runner didn't pre-score (older
        # callers, tests), do it here. Within a single
        # compute_selection_period call long + short share the scored
        # frame either way — the wasteful double-scoring of the pre-
        # split code is gone.
        if scored_df is None:
            scored_df = score_universe(
                signals_df,
                config.signal_weights,
                config.category_weights,
            )
        selected_top = select_from_scored(
            scored_df,
            top_n_sectors=config.top_n_sectors,
            top_n_per_sector=config.top_n_per_sector,
            direction="top",
            min_price_score=config.min_price_score,
        )
        if config.strategy_type == "long_short":
            selected_bottom = select_from_scored(
                scored_df,
                top_n_sectors=config.top_n_sectors,
                top_n_per_sector=config.top_n_per_sector,
                direction="bottom",
            )
            # If a name lands in both books (small universe / overlapping
            # sector sets), drop it from both. The intent is "go long the
            # best and short the worst" — keeping a name on both sides
            # is just self-cancellation that distorts the gross-200%
            # weight math.
            if not selected_bottom.empty and not selected_top.empty:
                top_ids = set(selected_top["company_id"].astype(int))
                bot_ids = set(selected_bottom["company_id"].astype(int))
                collisions = top_ids & bot_ids
                if collisions:
                    selected_top = selected_top[
                        ~selected_top["company_id"].isin(collisions)
                    ].reset_index(drop=True)
                    selected_bottom = selected_bottom[
                        ~selected_bottom["company_id"].isin(collisions)
                    ].reset_index(drop=True)
                    out.warnings.append(
                        f"{record_label}: dropped "
                        f"{len(collisions)} name(s) appearing on both long and short books"
                    )
        else:
            selected_bottom = pd.DataFrame()

    if selected_top.empty and selected_bottom.empty:
        n_signals = len(signals_df)
        sectors = signals_df["sector"].nunique() if "sector" in signals_df.columns else 0
        out.empty_reason = (
            f"{n_signals} companies had signals across {sectors} sectors but none passed "
            f"selection (top_n_sectors={config.top_n_sectors}, "
            f"top_n_per_sector={config.top_n_per_sector})"
        )
        return out

    # Equal weight per side. For long-only the short bucket is empty so
    # the long book sums to 1.0 (100% gross long). For long-short each
    # side sums to 1.0 independently → 200% gross, 0% net.
    n_long = len(selected_top)
    n_short = len(selected_bottom)
    long_weight = 1.0 / n_long if n_long > 0 else 0.0
    short_weight = 1.0 / n_short if n_short > 0 else 0.0

    entry_ts = pd.Timestamp(period_date)
    exit_ts = pd.Timestamp(next_period)

    holdings: list[PeriodHolding] = []
    long_returns: list[float] = []
    short_returns: list[float] = []
    for _, row in selected_top.iterrows():
        h, ret = make_period_holding(
            row, "long", long_weight,
            price_index=price_index,
            local_price_index=local_price_index,
            company_currency=company_currency,
            entry_ts=entry_ts, exit_ts=exit_ts,
        )
        holdings.append(h)
        if ret is not None:
            long_returns.append(ret)
    for _, row in selected_bottom.iterrows():
        h, ret = make_period_holding(
            row, "short", short_weight,
            price_index=price_index,
            local_price_index=local_price_index,
            company_currency=company_currency,
            entry_ts=entry_ts, exit_ts=exit_ts,
        )
        holdings.append(h)
        if ret is not None:
            short_returns.append(ret)

    # Portfolio return:
    #   long-only: equal-weighted mean of long returns.
    #   long-short (gross 100% long + 100% short): mean(long) − mean(short).
    # If a side is empty (degenerate period), fall back to whatever is
    # available — the strategy temporarily becomes one-sided.
    if config.strategy_type == "long_short":
        long_avg = float(np.mean(long_returns)) if long_returns else None
        short_avg = float(np.mean(short_returns)) if short_returns else None
        if long_avg is not None and short_avg is not None:
            port_return = round(long_avg - short_avg, 2)
        elif long_avg is not None:
            port_return = round(long_avg, 2)
        elif short_avg is not None:
            port_return = round(-short_avg, 2)
        else:
            port_return = None
    else:
        port_return = round(float(np.mean(long_returns)), 2) if long_returns else None

    out.holdings = holdings
    out.port_return = port_return
    return out
