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

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable

import numpy as np
import pandas as pd

from ..scoring import (
    _get_category_keys,
    aggregate_to_sector,
    compute_category_scores,
    random_select,
    score_and_select,
)
from .indices import _date_on_or_after, _price_on_or_after
from .types import (
    BacktestConfig,
    HoldingSide,
    PeriodHolding,
    _norm_sector,
)


@dataclass
class _PeriodOutcome:
    """What one period contributes back to the main loop."""
    holdings: list[PeriodHolding] = field(default_factory=list)
    port_return: float | None = None
    empty_reason: str | None = None
    # Warnings the branch raised — emitted by the main loop after each
    # period so the order remains stable across refactors.
    warnings: list[str] = field(default_factory=list)


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
        selected_top = score_and_select(
            signals_df,
            config.signal_weights,
            top_n_sectors=config.top_n_sectors,
            top_n_per_sector=config.top_n_per_sector,
            category_weights=config.category_weights,
            direction="top",
        )
        if config.strategy_type == "long_short":
            selected_bottom = score_and_select(
                signals_df,
                config.signal_weights,
                top_n_sectors=config.top_n_sectors,
                top_n_per_sector=config.top_n_per_sector,
                category_weights=config.category_weights,
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
