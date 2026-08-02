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

from ..scoring import (  # noqa: F401 — sector_pool_scores backs the ranking transcript
    extract_category_scores,
    score_and_select,
    score_universe,
    sector_pool_scores,
    select_from_scored,
    signal_defs_for_mode,
)
from ..signals import compute_signals_panel
from .dates import (
    _first_weekday_on_or_after,
    deciding_bar,
    is_decidable,
    sessions_between,
)
from .indices import (
    _build_price_index,
    _build_volume_index,
    _price_on_or_before,
)
from .types import BacktestConfig, CurrentPortfolio, DailyPick, PeriodHolding

_logger = logging.getLogger(__name__)

# `_MAX_HOLIDAY_GAP_DAYS` + `_prior_trading_day` now live in `.dates`, beside
# `is_decidable` — so the pipeline's pre-flight gate can test decidability with
# the SAME rule this walk uses instead of a second, disagreeing one.

# ⚠ HOW MANY SESSIONS A COMPANY'S LAST CLOSE MAY BE BEHIND THE DECIDING BAR AND
# STILL BE BOUGHT. The basket enters at the deciding bar — the trading day before
# the rebalance (first Monday ⇒ the preceding Friday). A company whose series
# stops earlier gets entered at an OLDER close, and every session between that
# close and the deciding bar is then booked as return the strategy never earned:
# measured, a name entered at its 2026-07-28 close of 140.90 and marked at the
# 07-31 close of 143.83 reported +2.08% on a position that opened on the 31st.
#
# So a stale name is DROPPED FROM THE SELECTION rather than entered at a stale
# price — you cannot buy at a price you do not have.
#
# ONE session of tolerance, not zero, because exchange calendars genuinely
# differ: a Tokyo name whose market was shut that Friday has its last close a
# session earlier, and that IS its most recent datapoint. Counted in SESSIONS,
# not calendar days — a Tuesday bar and a Thursday bar are both three calendar
# days from the following Friday while one has missed three sessions and the
# other one. A market shut for two-plus consecutive sessions while the world
# trades will drop those names; the count is reported (`excluded_stale_count`)
# rather than silent, and the rebalance now fetches prices to this exact bar
# BEFORE computing, so a name still short of it is a data gap, not a calendar.
#
# `signal_engine.daily.MAX_STALENESS_DAYS` (30) stays where it is and means
# something different: a SIGNAL may be computed from a slightly older bar; a
# PURCHASE may not.
MAX_ENTRY_GAP_SESSIONS = 1


def _next_month_start(d: date) -> date:
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)


def _prev_month_start(d: date) -> date:
    first = date(d.year, d.month, 1)
    prev_last = first - timedelta(days=1)
    return date(prev_last.year, prev_last.month, 1)


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
    daily_from: date | None = None,
    cached_selections: dict[date, pd.DataFrame] | None = None,
    cached_sector_scores: dict[date, list[dict]] | None = None,
) -> CurrentPortfolio:
    """Compute the strategy's portfolio for the current month with MTD returns.

    Mirrors a single iteration of run_backtest:
      * as_of_date = first of current month
      * signals computed using prices strictly before as_of_date
      * entry_price = the last close STRICTLY BEFORE as_of_date (the prior
        trading day's bar — the same one the signals are computed from, and
        where run_backtest enters). See the `prior_anchor` comment below.
      * exit_price = LATEST available price (vs run_backtest's next-month price)
      * forward_return_pct field carries the MTD return

    Random selection mode is not supported here — picking randomly for "what
    should I hold today" has no useful interpretation.

    `daily_from` widens ONLY the daily-picks walk, back to that date — "what would
    this strategy have held on each trading day since then". Everything else is
    untouched: the rebalance anchor, the locked month-start holdings and their MTD
    are still the CURRENT period's, because those are the of-record decision and a
    retrospective question must not be able to move them. Default None = the current
    period only, which is what the pipeline runs.

    ⚠ THE DAYS IT PRODUCES ARE A CALCULATION, NOT A DECISION, AND MUST NOT BE
    PERSISTED. `current_picks_day` is the record of what the pipeline actually
    decided each day; writing a recomputed past into it would overwrite decisions
    that were made on the data available AT THE TIME with ones made on the data we
    hold now. Closed months are read-only for exactly that reason. The recalculation
    has its OWN store (`daily_holdings_cache`) — see the caller.

    `cached_selections` supplies an already-computed SELECTION for a date, so its
    signals + score/select are skipped. ⚠ IT SHORT-CIRCUITS ONLY THAT STEP. Entry and
    exit prices, forward returns, turnover and the chain-linked cumulative are still
    derived here, every run, from the live price index — every one of them is a
    property of the WINDOW (turnover is measured against the previous day IN IT, the
    cumulative is chained from ITS first day), so a stored value would be wrong the
    moment a different window is asked for. What is reused is the part that is both
    expensive and window-independent: which companies, at what score.
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
    # Effective rebalance date = the chosen weekday's first occurrence of a
    # month (e.g. first Monday). A rebalance is DECIDABLE once the bar it trades
    # on — the prior trading day's close (strict-< the rebalance date) — is in
    # the loaded data; that same bar drives the signals. So the first Monday's
    # rebalance becomes decidable from Friday's close (settled by Saturday).
    # Pick the latest first-<weekday> that's decidable: step FORWARD while the
    # next period already is (the early Saturday rebalance — picks identical to
    # running on the Monday), and BACK while this one isn't yet (we still hold
    # the prior period / can't price the deciding bar). Matches run_backtest's
    # first-<weekday> grid and never produces an empty portfolio from a
    # not-yet-settled rebalance-day close.
    # The deciding bar must be in the data AND not in the future: bound by both
    # the latest loaded close and `today`. On a real Saturday this is Friday's
    # close (today=Sat ≥ Fri), so the upcoming Monday is decidable; before the
    # deciding bar has settled it isn't, and we keep the prior period.
    ldd = today_d if latest_data_date is None else min(today_d, latest_data_date)

    def _decidable(rebal: date) -> bool:
        """Has `rebal`'s deciding bar — the prior trading day's close its signals
        + entry anchor to — already settled?

        ONE definition, in `.dates`: the pipeline's pre-flight gate must admit
        exactly the runs this walk can decide, or it rejects work the engine was
        about to do correctly (which is precisely what a calendar-month gate
        did: August 2026 opens on a Saturday, so on Sunday the 2nd the newest
        close in existence is Friday 31 July — and the first Monday's rebalance
        is fully decidable from it)."""
        return is_decidable(rebal, today=today_d, latest_data_date=ldd)

    rebalance_date = _first_weekday_on_or_after(
        date(today_d.year, today_d.month, 1), weekday
    )
    _walk: list[str] = [f"start {rebalance_date} ({'decidable' if _decidable(rebalance_date) else 'not yet'})"]
    while True:
        nxt = _first_weekday_on_or_after(_next_month_start(rebalance_date), weekday)
        if _decidable(nxt):
            _walk.append(f"forward → {nxt} (its {deciding_bar(nxt)} bar has settled)")
            rebalance_date = nxt
        else:
            break
    while not _decidable(rebalance_date):
        prev = _first_weekday_on_or_after(_prev_month_start(rebalance_date), weekday)
        _walk.append(f"back → {prev} ({deciding_bar(rebalance_date)} has not settled)")
        rebalance_date = prev
    # The signal-cutoff + entry anchor for the locked-at-start holdings.
    month_start = rebalance_date
    month_key = rebalance_date.isoformat()[:7]

    # ⚠ THE FIRST THING TO CHECK WHEN A REBALANCE LOOKS WRONG: which date did it
    # decide FOR, off which bar, from how much data. Every downstream number —
    # signals, entry prices, the period the return is measured over — hangs off
    # these three, and none of them is visible in the holdings table afterwards.
    _logger.info(
        "[current_portfolio] rebalance=%s (weekday %s) · deciding bar=%s · today=%s · "
        "latest loaded close=%s · walk: %s",
        rebalance_date, weekday, deciding_bar(rebalance_date), today_d, latest_data_date,
        " | ".join(_walk),
    )
    if send_event:
        send_event("progress", month=month_key, pct=10, message=(
            f"Rebalance {rebalance_date.isoformat()} (weekday {weekday}) decided on the "
            f"{deciding_bar(rebalance_date).isoformat()} close · today {today_d.isoformat()} · "
            f"latest loaded close {latest_data_date} · walk: {' | '.join(_walk)}"
        ))

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

    # The FUNNEL, stated up front. Every later count is a subset of this one, and
    # a rebalance that quietly ran on 900 of 1,479 names looks exactly like one
    # that ran on all of them once you are only reading the holdings table.
    if send_event:
        send_event("progress", month=month_key, pct=12, message=(
            f"Universe: {len(universe_df)} loaded → {len(month_universe_df)} eligible for "
            f"{month_key} across {month_universe_df['sector'].nunique() if 'sector' in month_universe_df else 0} sectors"
        ))

    # Build price/volume indices once
    price_index = _build_price_index(prices_df)
    local_price_index = (
        _build_price_index(prices_local_df)
        if prices_local_df is not None and not prices_local_df.empty
        else None
    )
    volume_index = _build_volume_index(volumes_df) if volumes_df is not None and not volumes_df.empty else None

    # Trading dates that fall inside the current month, derived from prices_df —
    # or back to `daily_from` when the caller asked for a retrospective walk.
    # Built up front so the signal panel can compute every cutoff in one pass.
    #
    # ⚠ THE FLOOR NEVER MOVES FORWARD. `min(daily_from, month_start)` — a
    # `daily_from` INSIDE the current period would otherwise silently truncate the
    # live daily-picks panel the /schedule card reads, turning a read-only question
    # into a change of what the pipeline reports.
    daily_floor = min(daily_from, month_start) if daily_from else month_start
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
        if daily_floor <= dd <= today_d:
            trading_dates_set.add(dd)
    trading_dates = sorted(trading_dates_set)

    # Single vectorized pass — computes every (company, cutoff) cell up front
    # so the daily loop below is a cheap dict lookup. Includes month_start so
    # the locked-at-start holdings use the same code path.
    t_panel = time.perf_counter()
    # ⚠ THE CACHED DAYS ARE DROPPED FROM THE CUTOFFS, WHICH IS WHERE THE SAVING IS.
    # The panel is the expensive step (signals for every company at every cutoff);
    # skipping a day's cutoff is what makes a re-run cost one day instead of forty.
    # `month_start` is NEVER skipped — it is the locked basket the pipeline reports,
    # not part of the retrospective walk, and it is not cacheable against a window.
    _cached = cached_selections or {}
    panel_cutoffs: list[date] = sorted(
        {month_start, *(d for d in trading_dates if d not in _cached)}
    )
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

    # ── The deciding bar, enforced ────────────────────────────────
    # ⚠ EVERY NAME IN THE BASKET MUST BE PRICED AT THE BAR THE BASKET ENTERS ON.
    # `_price_on_or_before` will happily walk back weeks to find a company its
    # last close, which prices the entry at a date the portfolio did not exist —
    # see `MAX_ENTRY_GAP_DAYS`. Filtering HERE, before scoring, is what keeps the
    # book at its configured size: dropping the stale names afterwards would
    # simply hold fewer than `top_n_per_sector × top_n_sectors`.
    entry_anchor = deciding_bar(month_start)
    anchor_ts = pd.Timestamp(entry_anchor)
    candidates = signals_df["company_id"].astype(int).tolist()
    stale_ids: list[int] = []
    for cid in candidates:
        s = price_index.get(cid)
        # No series at all, or nothing on/before the anchor → unpriceable HERE.
        # (A name with no bar before the anchor cannot be entered at any price.)
        pair = _price_on_or_before(s, anchor_ts) if s is not None else None
        if pair is None or sessions_between(pair[1].date(), entry_anchor) > MAX_ENTRY_GAP_SESSIONS:
            stale_ids.append(cid)
    if stale_ids:
        # NAME them, capped, with the overflow stated. "37 excluded" tells you to
        # refresh; "these 2 are excluded, at these dates" tells you WHICH vendor
        # gaps you are living with — and that is the question every one of these
        # investigations actually ends on.
        by_cid = {}
        if "company_id" in month_universe_df.columns:
            for _, r in month_universe_df.iterrows():
                by_cid[int(r["company_id"])] = str(
                    r.get("gurufocus_ticker") or r.get("company_name") or r["company_id"]
                )
        detail = []
        for cid in stale_ids[:25]:
            s = price_index.get(cid)
            pair = _price_on_or_before(s, anchor_ts) if s is not None else None
            last = pair[1].date().isoformat() if pair is not None else "no bar"
            detail.append(f"{by_cid.get(cid, cid)}@{last}")
        more = f" …+{len(stale_ids) - 25} more" if len(stale_ids) > 25 else ""

        signals_df = signals_df[~signals_df["company_id"].astype(int).isin(stale_ids)]
        _logger.warning(
            "[current_portfolio] %s of %s candidates dropped — last close more than %s "
            "session(s) before the %s deciding bar (they could only be entered at a stale "
            "price): %s%s", len(stale_ids), len(candidates), MAX_ENTRY_GAP_SESSIONS,
            entry_anchor, ", ".join(detail), more,
        )
        if send_event:
            send_event(
                "warning", scope="data", month=month_key,
                message=(
                    f"{len(stale_ids)} of {len(candidates)} candidate(s) had no close at the "
                    f"{entry_anchor.isoformat()} deciding bar and were excluded from selection "
                    f"— refresh prices to bring them back: {', '.join(detail)}{more}"
                ),
            )
    if signals_df.empty:
        if send_event:
            send_event("progress", month=month_key, pct=100, message=(
                f"No company is priced at the {entry_anchor.isoformat()} deciding bar"))
        return CurrentPortfolio(
            as_of_date=month_start.isoformat(), latest_price_date=None, holdings=[],
            entry_anchor_date=entry_anchor.isoformat(), excluded_stale_count=len(stale_ids),
        )

    if send_event:
        send_event("progress", month=month_key, pct=60, message=(
            f"Scoring {len(signals_df)} companies (of {len(candidates)} with signals, "
            f"{len(month_universe_df)} in the universe) — picking top {config.top_n_sectors} "
            f"sector(s) × {config.top_n_per_sector} name(s)"
            + (f", min price score {config.min_price_score}" if config.min_price_score else "")
        ))

    # Score and select — same path as backtest momentum mode
    t_month_start_select = time.perf_counter()
    selected = score_and_select(
        signals_df,
        config.signal_weights,
        top_n_sectors=config.top_n_sectors,
        top_n_per_sector=config.top_n_per_sector,
        category_weights=config.category_weights,
        min_price_score=config.min_price_score,
        backfill_below_min_score=config.backfill_below_min_score,
        signal_defs=signal_defs_for_mode(config.selection_mode),
    )
    t_month_start_select_elapsed = time.perf_counter() - t_month_start_select

    # ⚠ THE SECTOR RANKING IS WHERE A REBALANCE MOST OFTEN SURPRISES YOU, and it
    # is invisible afterwards: the holdings table shows the sectors that WON, never
    # the ones that lost or by how little. Scores are min-max normalized across the
    # pool and the sector score is a mean of them, so one outlier entering or
    # leaving the pool can flip a boundary (measured: one stale name swapped
    # Technology for Capital Goods across 6 of 24 holdings).
    if send_event and not selected.empty:
        try:
            # ⚠ AGGREGATED OVER `scored` — EVERY SCORED COMPANY, WHICH IS THE POOL
            # `score_and_select` RANKS SECTORS ON. Not `selection_pool(...)`: the
            # `min_price_score` floor decides which COMPANIES are bought, not
            # which SECTORS exist, and ranking on the survivors flatters exactly
            # the sectors with the fewest of them (that bias was removed on
            # 2026-07-31 and this table must not quietly reintroduce it).
            # Re-scoring here costs a vectorized pass over ~1.5k rows; it must be
            # given the SAME arguments, or the table explains a ranking that
            # never happened.
            scored_for_log = score_universe(
                signals_df, config.signal_weights,
                category_weights=config.category_weights,
                signal_defs=signal_defs_for_mode(config.selection_mode),
            )
            ranked = sector_pool_scores(scored_for_log)
            chosen = set(selected["sector"].tolist())
            line = " | ".join(
                f"{'*' if r['sector'] in chosen else ''}{r['sector']} "
                f"{r['momentum_score']} (n={r['companies']})"
                for r in ranked[:12]
            )
            send_event("progress", month=month_key, pct=80, message=(
                f"Sector ranking (* = picked, top {config.top_n_sectors}): {line}"
            ))
        except Exception as e:  # noqa: BLE001 — a diagnostic must not fail the rebalance
            _logger.warning("[current_portfolio] sector-ranking log failed: %s: %s",
                            type(e).__name__, e)

    if selected.empty:
        if send_event:
            send_event("progress", month=month_key, pct=100, message="No companies passed selection")
        return CurrentPortfolio(
            as_of_date=month_start.isoformat(), latest_price_date=None, holdings=[],
            entry_anchor_date=entry_anchor.isoformat(), excluded_stale_count=len(stale_ids),
        )

    if send_event:
        send_event("progress", month=month_key, pct=85, message="Computing MTD returns...")

    n_holdings = len(selected)
    weight = 1.0 / n_holdings
    # Entry at the DECIDING BAR — the trading day strictly before the rebalance
    # date (first Monday ⇒ the preceding Friday), which is the same bar the
    # signals are computed from and where run_backtest enters. Anchoring here
    # (not the rebalance-day close) makes the picks fully determined by that
    # prior close, so the rebalance can be computed the moment it lands (Friday's
    # close → fire Saturday) and the live entry matches the backtest's.
    # ONE definition, shared with the pipeline's pre-flight gate and its price
    # fetch (`momentum.backtest.dates.deciding_bar`) — the date the prices are
    # fetched TO must be the date the book is entered AT.
    prior_anchor = anchor_ts

    holdings: list[PeriodHolding] = []
    latest_observed: pd.Timestamp | None = None

    for _, row in selected.iterrows():
        cid = int(row["company_id"])
        series = price_index.get(cid)

        entry_pair = _price_on_or_before(series, prior_anchor) if series is not None else None
        entry_price = entry_pair[0] if entry_pair is not None else None
        # Exit = latest available price in the EUR series.
        exit_price = float(series.iloc[-1]) if series is not None and len(series) > 0 else None
        exit_dt_ts = series.index[-1] if series is not None and len(series) > 0 else None
        if exit_dt_ts is not None and (latest_observed is None or exit_dt_ts > latest_observed):
            latest_observed = exit_dt_ts

        mtd_return = None
        if entry_price and exit_price and entry_price > 0:
            mtd_return = round((exit_price / entry_price - 1) * 100, 2)

        local_series = local_price_index.get(cid) if local_price_index is not None else None
        entry_local_pair = _price_on_or_before(local_series, prior_anchor) if local_series is not None else None
        entry_local = entry_local_pair[0] if entry_local_pair is not None else None
        exit_local = float(local_series.iloc[-1]) if local_series is not None and len(local_series) > 0 else None

        date_series = local_series if local_series is not None else series
        entry_dt_pair = _price_on_or_before(date_series, prior_anchor) if date_series is not None else None
        entry_dt = entry_dt_pair[1].strftime("%Y-%m-%d") if entry_dt_pair is not None else None
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

        # A cached day skips signals + score/select entirely — that is the saving.
        # Everything below (prices, returns, turnover, cumulative) still runs on it,
        # because all of that is a property of the window rather than of the day.
        cached_sel = _cached.get(d)
        day_sector_scores: list[dict] = []
        if cached_sel is not None:
            daily_selected = cached_sel
            day_sector_scores = (cached_sector_scores or {}).get(d) or []
        else:
            t_signals = time.perf_counter()
            daily_signals = panel.get(d, pd.DataFrame())
            t_daily_signals_total += time.perf_counter() - t_signals
            if daily_signals.empty:
                continue
            t_select = time.perf_counter()
            # ⚠ SCORED ONCE, THEN SELECTED AND AGGREGATED FROM THE SAME FRAME. Calling
            # `score_and_select` and then re-scoring for the sector table would pay the
            # scoring cost twice AND let the two answers drift apart — the sector scores
            # are meant to explain THIS day's pick, not a parallel computation of it.
            scored = score_universe(
                daily_signals,
                config.signal_weights,
                config.category_weights,
                signal_defs_for_mode(config.selection_mode),
            )
            daily_selected = select_from_scored(
                scored,
                top_n_sectors=config.top_n_sectors,
                top_n_per_sector=config.top_n_per_sector,
                min_price_score=config.min_price_score,
                backfill_below_min_score=config.backfill_below_min_score,
            )
            # ⚠ OVER `scored`, THE SAME ROWS `select_from_scored` NOW RANKS SECTORS ON — not over
            # the floor-filtered pool. Aggregating these two differently is how the table stops
            # explaining the selection it sits beside.
            day_sector_scores = sector_pool_scores(scored)
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
            sector_scores=day_sector_scores,
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
        entry_anchor_date=entry_anchor.isoformat(),
        excluded_stale_count=len(stale_ids),
    )
