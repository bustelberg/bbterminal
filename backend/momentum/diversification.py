"""Diversifier correlation + blend math (pure — no DB, no I/O).

Given a strategy's monthly return series and a candidate ETF's price
history, this answers the question the /diversifier page exists for:
*how uncorrelated is this ETF with my strategy, and how much would adding
a slice of it lift my Sharpe/Sortino?*

Everything here operates on plain Python lists/dicts so it's trivially
unit-testable (see tests/test_diversification.py). The router
(`routers/diversifier.py`) handles the DB loads and hands aligned series
in here.

Conventions:
  * A monthly return series is a `dict[str, float]` keyed by month label
    "YYYY-MM" (the same label the backtest's monthly_records carry), value
    = that month's simple return as a fraction (0.03 = +3%).
  * Returns are fractions, not percents. The router converts the
    backtest's `*_pct` fields by dividing by 100.
  * Stats annualize monthly figures with periods_per_year=12.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

PERIODS_PER_YEAR = 12

# Below this, a series is treated as having no variance/volatility — guards
# against float noise (e.g. a "constant" 0.01 series whose std is ~1e-17, not
# exactly 0) producing NaN correlations or absurd Sharpe ratios.
_ZERO_TOL = 1e-12

# Coordinate-ascent restarts per core-weight in the portfolio optimizer. The
# search is a discrete hill-climb that can land in a local optimum; each restart
# is another seeded random starting sleeve, and we keep the best — so MORE
# restarts is monotonic (can only match or beat fewer) and never hurts except in
# runtime (each ≈ one extra ascent per core-weight: ~1.2s at 12 ETFs over a full
# [0,100] search). Empirically (random instances vs a 32-restart reference): by 8
# restarts the search hits the global optimum in EVERY ≤10-ETF instance; a very
# busy 14-ETF book may still land ~1-in-10 on a near-optimum, but only by a
# 0.001–0.004-Sortino hair (the objective is flat near its max — practically the
# same portfolio). So 8 makes a meaningful miss very unlikely while keeping the
# typical case ≈ 4–6s; crank it up (e.g. 16) via `search_restarts` for certainty
# on a large book. Override per call via `optimize_portfolio(search_restarts=…)`.
# Deterministic: the RNG is reseeded per core-weight (seed + cw), so same inputs
# + same restarts ⇒ identical weights.
OPTIMIZER_RESTARTS = 8


def prices_to_monthly_returns(prices: list[tuple[object, float]]) -> dict[str, float]:
    """Month-end resample a daily price series into monthly simple returns.

    `prices` is `[(date, close), ...]` (date may be a datetime.date or an
    ISO "YYYY-MM-DD" string). For each calendar month we keep the LAST
    available close; the month's return is that close over the previous
    month's last close minus one. The first month has no prior anchor and
    is dropped. Months with no data simply don't appear (a gap chains
    across it, which is the correct behaviour for a sparse series).
    """
    last_close: dict[str, tuple[str, float]] = {}
    for d, p in prices:
        iso = d if isinstance(d, str) else d.isoformat()
        if len(iso) < 7 or p is None:
            continue
        try:
            close = float(p)
        except (TypeError, ValueError):
            continue
        if close <= 0:
            continue
        month = iso[:7]
        # Keep the latest (date, close) within the month.
        prev = last_close.get(month)
        if prev is None or iso > prev[0]:
            last_close[month] = (iso, close)

    months = sorted(last_close)
    out: dict[str, float] = {}
    for i in range(1, len(months)):
        prev_close = last_close[months[i - 1]][1]
        cur_close = last_close[months[i]][1]
        if prev_close > 0:
            out[months[i]] = cur_close / prev_close - 1.0
    return out


def monthly_records_to_returns(records: list[dict]) -> dict[str, float]:
    """Strategy monthly returns keyed "YYYY-MM" from a backtest's
    `monthly_records`.

    Derived from the chain-linked `cumulative_return_pct` (always present)
    rather than `portfolio_return_pct` (which can be null on empty
    periods): r_i = (1 + cum_i) / (1 + cum_{i-1}) - 1, with cum_0 anchored
    at 0. Open/incomplete trailing periods (`is_open`) are excluded — their
    partial MTD return would distort the correlation.
    """
    out: dict[str, float] = {}
    prev_cum = 0.0
    for rec in records:
        if rec.get("is_open"):
            continue
        cum = rec.get("cumulative_return_pct")
        month = rec.get("date")
        if cum is None or not month:
            continue
        cur_cum = float(cum) / 100.0
        denom = 1.0 + prev_cum
        if denom != 0:
            out[str(month)[:7]] = (1.0 + cur_cum) / denom - 1.0
        prev_cum = cur_cum
    return out


def align(a: dict[str, float], b: dict[str, float]) -> tuple[list[float], list[float], list[str]]:
    """Two series restricted to their common month labels, sorted
    ascending. Returns (a_values, b_values, months)."""
    common = sorted(set(a) & set(b))
    return [a[m] for m in common], [b[m] for m in common], common


def pearson(a: list[float], b: list[float]) -> float | None:
    """Pearson correlation of two equal-length series. None when there are
    <2 points or either side has zero variance (correlation undefined)."""
    if len(a) != len(b) or len(a) < 2:
        return None
    av, bv = np.asarray(a, dtype=float), np.asarray(b, dtype=float)
    if np.std(av) < _ZERO_TOL or np.std(bv) < _ZERO_TOL:
        return None
    c = float(np.corrcoef(av, bv)[0, 1])
    return None if math.isnan(c) else c


@dataclass
class Stats:
    n: int
    ann_return: float | None
    ann_vol: float | None
    sharpe: float | None
    sortino: float | None
    median_month: float | None = None   # median monthly return (fraction)
    win_rate: float | None = None        # fraction of months with return > 0


def annualized_stats(returns: list[float], rf_annual: float = 0.0) -> Stats:
    """Annualized return / vol / Sharpe / Sortino for a monthly return
    series (fractions). `rf_annual` is the annual risk-free rate as a
    fraction (0.02 = 2%).

      ann_return = prod(1+r)^(12/n) - 1                 (geometric)
      ann_vol    = std(r, ddof=1) * sqrt(12)
      sharpe     = (ann_return - rf_annual) / ann_vol
      sortino    = (ann_return - rf_annual) / (downside_dev * sqrt(12))

    Sharpe/Sortino are None when their denominator is 0 (flat or never-
    below-target series) so the caller can render "n/a" instead of inf.
    """
    n = len(returns)
    if n == 0:
        return Stats(0, None, None, None, None)
    r = np.asarray(returns, dtype=float)

    growth = float(np.prod(1.0 + r))
    ann_return = growth ** (PERIODS_PER_YEAR / n) - 1.0 if growth > 0 else -1.0

    ann_vol = float(np.std(r, ddof=1) * np.sqrt(PERIODS_PER_YEAR)) if n >= 2 else None
    if ann_vol is not None and ann_vol < _ZERO_TOL:
        ann_vol = 0.0   # snap float noise so a flat series reads as zero-vol
    sharpe = (ann_return - rf_annual) / ann_vol if ann_vol else None

    # Downside deviation vs the per-period risk-free target.
    rf_period = (1.0 + rf_annual) ** (1.0 / PERIODS_PER_YEAR) - 1.0
    downside = np.minimum(r - rf_period, 0.0)
    dd = float(np.sqrt(np.mean(downside ** 2)) * np.sqrt(PERIODS_PER_YEAR))
    sortino = (ann_return - rf_annual) / dd if dd > 0 else None

    return Stats(
        n=n,
        ann_return=ann_return,
        ann_vol=ann_vol,
        sharpe=sharpe,
        sortino=sortino,
        median_month=float(np.median(r)),
        win_rate=float(np.mean(r > 0)),
    )


@dataclass
class Blend:
    weight: float            # ETF weight in [0, w_max] that maximizes Sharpe
    sharpe: float | None
    sortino: float | None
    ann_return: float | None
    ann_vol: float | None


def optimal_blend(
    strategy: list[float],
    etf: list[float],
    rf_annual: float = 0.0,
    w_max: float = 0.5,
    step: float = 0.05,
    objective: str = "sharpe",
) -> Blend:
    """Grid-search the ETF weight w in [0, w_max] (inclusive) that maximizes
    the combined `objective` ("sharpe" or "sortino") of
    `(1-w)*strategy + w*etf`, both aligned monthly series. Returns the
    winning weight and the resulting stats.

    A pure-strategy baseline (w=0) is always in the grid, so the optimizer
    never *reduces* the objective — at worst it returns w=0 (the ETF
    doesn't help). Targeting Sortino matters for crisis-alpha sleeves
    (managed futures, gold): they can lift downside-risk-adjusted return
    even when they can't beat a high Sharpe, because their payoff is
    concentrated in the left tail that Sortino rewards and symmetric Sharpe
    averages away.
    """
    use_sortino = objective == "sortino"
    s = np.asarray(strategy, dtype=float)
    e = np.asarray(etf, dtype=float)
    best: Blend | None = None
    best_score = -np.inf
    # Build an inclusive grid 0..w_max without float drift.
    steps = int(round(w_max / step))
    for k in range(steps + 1):
        w = round(k * step, 6)
        combined = ((1.0 - w) * s + w * e).tolist()
        st = annualized_stats(combined, rf_annual)
        metric = st.sortino if use_sortino else st.sharpe
        score = metric if metric is not None else -np.inf
        # `best is None` guarantees the w=0 baseline (first iteration) is
        # always taken, even when every grid point has an undefined metric
        # (e.g. a single-month overlap) — so we never return None.
        if best is None or score > best_score:
            best_score = score
            best = Blend(
                weight=w,
                sharpe=st.sharpe,
                sortino=st.sortino,
                ann_return=st.ann_return,
                ann_vol=st.ann_vol,
            )
    assert best is not None  # grid always has w=0
    return best


@dataclass
class Drawdown:
    depth_pct: float            # negative, e.g. -32.5
    peak_date: str
    trough_date: str
    recovery_date: str | None   # None = not yet recovered by series end
    length_months: int          # peak → trough span


def top_drawdowns(months: list[str], rets: list[float], top_n: int = 40) -> list[Drawdown]:
    """The `top_n` worst peak-to-trough-to-recovery drawdowns of the equity
    curve implied by a monthly return series, largest first, non-overlapping.

    Mirrors the backtest engine's drawdown detection (peak resets on a new
    high; a drawdown closes when the curve recovers to its prior peak, or
    stays open at series end). Kept inline so this module stays pure numpy.
    """
    if len(rets) < 2:
        return []
    equity: list[float] = []
    eq = 1.0
    for r in rets:
        eq *= 1.0 + r
        equity.append(eq)

    periods: list[Drawdown] = []
    peak_val, peak_i = equity[0], 0
    trough_val, trough_i = peak_val, 0
    in_dd = False
    for i in range(1, len(equity)):
        v = equity[i]
        if v >= peak_val:
            if in_dd:
                periods.append(Drawdown(
                    depth_pct=round((trough_val / peak_val - 1) * 100, 2),
                    peak_date=months[peak_i], trough_date=months[trough_i],
                    recovery_date=months[i], length_months=trough_i - peak_i,
                ))
                in_dd = False
            peak_val, peak_i = v, i
            trough_val, trough_i = v, i
        else:
            in_dd = True
            if v < trough_val:
                trough_val, trough_i = v, i
    if in_dd:
        periods.append(Drawdown(
            depth_pct=round((trough_val / peak_val - 1) * 100, 2),
            peak_date=months[peak_i], trough_date=months[trough_i],
            recovery_date=None, length_months=trough_i - peak_i,
        ))

    # Top N by depth, excluding episodes whose peak→recovery span overlaps an
    # already-picked deeper one (avoids reporting sub-drawdowns of one crash).
    periods.sort(key=lambda p: p.depth_pct)
    selected: list[Drawdown] = []
    for p in periods:
        if len(selected) >= top_n:
            break
        p_end = p.recovery_date or "9999-99"
        if not any(
            p.peak_date <= (s.recovery_date or "9999-99") and p_end >= s.peak_date
            for s in selected
        ):
            selected.append(p)
    return selected


@dataclass
class MonthStat:
    month: str                    # "YYYY-MM"
    return_before: float          # that month's return (fraction)
    return_after: float


@dataclass
class YearStats:
    year: int
    return_before: float | None   # calendar-year compounded return (fraction)
    return_after: float | None
    vol_before: float | None      # annualized vol of that year's monthly returns
    vol_after: float | None
    months: list[MonthStat]       # the year's per-month returns (before/after)


def annual_breakdown(
    months: list[str], before: list[float], after: list[float]
) -> list[YearStats]:
    """Per-calendar-year compounded return + annualized vol for the strategy-
    alone vs optimized monthly return series, each carrying its per-month
    returns. The latest year is partial (its return is the YTD figure). Vol is
    None for a year with <2 months."""
    by_year: dict[int, list[int]] = {}
    order: list[int] = []
    for i, m in enumerate(months):
        y = int(m[:4])
        if y not in by_year:
            by_year[y] = []
            order.append(y)
        by_year[y].append(i)

    def _compound(rs: list[float]) -> float:
        p = 1.0
        for r in rs:
            p *= 1.0 + r
        return p - 1.0

    def _vol(rs: list[float]) -> float | None:
        return float(np.std(rs, ddof=1) * np.sqrt(PERIODS_PER_YEAR)) if len(rs) >= 2 else None

    out: list[YearStats] = []
    for y in order:
        idxs = by_year[y]
        b = [before[i] for i in idxs]
        a = [after[i] for i in idxs]
        out.append(YearStats(
            year=y,
            return_before=_compound(b),
            return_after=_compound(a),
            vol_before=_vol(b),
            vol_after=_vol(a),
            months=[
                MonthStat(month=months[i], return_before=before[i], return_after=after[i])
                for i in idxs
            ],
        ))
    return out


@dataclass
class PortfolioOptimization:
    assets: list[str]            # labels, asset 0 is always the strategy
    weights: list[float]         # optimal weights aligned to `assets`, sum=1
    months: int
    period_from: str | None
    period_to: str | None
    limited_by: str | None       # the asset whose start bounds the common window
    before: Stats                # strategy alone over the common window
    after: Stats                 # the optimized portfolio over the common window
    # Cumulative-return-% equity curves over the common window (for the
    # before/after chart). `curve_months` are the x labels; before/after are
    # cumulative % from the window start (each begins at the first month's
    # return, not 0 — the caller can prepend a 0 anchor if wanted).
    curve_months: list[str]
    curve_before: list[float]
    curve_after: list[float]
    # Top-40 worst drawdowns of each curve (strategy-alone vs optimized).
    drawdowns_before: list[Drawdown]
    drawdowns_after: list[Drawdown]
    # Per-calendar-year return + vol, and the current-year (YTD) return.
    annual: list[YearStats]
    ytd_before: float | None
    ytd_after: float | None
    # Drift-and-rebalance: starting from the target weights, the strategy is
    # trimmed back to its start weight whenever it grows past the threshold.
    rebalance_count: int
    rebalance_dates: list[str]
    rebalance_freq_months: float | None   # avg months between rebalances


def _simulate_rebalance(
    months: list[str],
    R: np.ndarray,
    target: np.ndarray,
    core_idx: np.ndarray,
    center: float,
    band: float,
) -> tuple[list[float], list[str]]:
    """Drift-and-rebalance the target portfolio month by month within a band.

    Start at `target`; each month earn `w · r`, let the weights drift with
    returns, then renormalize. The CORE total (the sum of `core_idx` weights —
    the strategy + bonds) starts at `center`; whenever it drifts OUTSIDE the
    symmetric band [center - band, center + band] (the core grew after
    outperforming the diversifiers, or shrank after lagging), reset all weights
    back to `target` and record the rebalance month. Returns (monthly portfolio
    returns, rebalance months)."""
    lower, upper = center - band, center + band
    w = target.copy()
    rets: list[float] = []
    rebalances: list[str] = []
    for t in range(R.shape[0]):
        r = R[t]
        rets.append(float(w @ r))
        w = w * (1.0 + r)
        s = w.sum()
        if s > 0:
            w = w / s
        core_w = float(w[core_idx].sum())
        if core_w < lower or core_w > upper:
            w = target.copy()
            rebalances.append(months[t])
    return rets, rebalances


def _polish_simplex(e: np.ndarray, eval_fn) -> np.ndarray:
    """Coordinate-ascent on a simplex vector `e` (sums to 1): shuffle weight
    between element pairs in shrinking steps, keeping any improvement.
    `eval_fn(candidate)` returns the score to maximize."""
    best = e.copy()
    best_sc = eval_fn(best)
    step = 0.25
    while step > 1e-4:
        improved = True
        while improved:
            improved = False
            for i in range(len(best)):
                for j in range(len(best)):
                    if i == j or best[i] <= 0:
                        continue
                    c = best.copy()
                    d = min(step, c[i])
                    c[i] -= d
                    c[j] += d
                    sc = eval_fn(c)
                    if sc > best_sc + 1e-12:
                        best, best_sc, improved = c, sc, True
        step /= 2
    return best


def _cum_curve(rets: list[float]) -> list[float]:
    """Cumulative-return-% curve from a monthly return series."""
    out: list[float] = []
    eq = 1.0
    for r in rets:
        eq *= 1.0 + float(r)
        out.append(round((eq - 1.0) * 100.0, 4))
    return out


def _assemble_optimization(
    names: list[str],
    common: list[str],
    target: np.ndarray,
    before_rets: list[float],
    after_rets: list[float],
    rebalance_dates: list[str],
    limited_by: str | None,
    rf_annual: float,
) -> PortfolioOptimization:
    """Build the full PortfolioOptimization result (stats, curves, drawdowns,
    per-year table, rebalance info) from already-computed return series. Shared
    by the optimizer and the manual-portfolio backtester."""
    annual = annual_breakdown(list(common), before_rets, after_rets) if common else []
    n_months = len(common)
    return PortfolioOptimization(
        assets=names,
        weights=[round(float(x), 4) for x in target],
        months=n_months,
        period_from=common[0] if common else None,
        period_to=common[-1] if common else None,
        limited_by=limited_by,
        before=annualized_stats(before_rets, rf_annual),
        after=annualized_stats(after_rets, rf_annual),
        curve_months=list(common),
        curve_before=_cum_curve(before_rets),
        curve_after=_cum_curve(after_rets),
        drawdowns_before=top_drawdowns(list(common), before_rets, 40),
        drawdowns_after=top_drawdowns(list(common), after_rets, 40),
        annual=annual,
        ytd_before=annual[-1].return_before if annual else None,
        ytd_after=annual[-1].return_after if annual else None,
        rebalance_count=len(rebalance_dates),
        rebalance_dates=rebalance_dates,
        rebalance_freq_months=(n_months / len(rebalance_dates)) if rebalance_dates else None,
    )


def _simulate_with_bands(
    months: list[str], R: np.ndarray, target: np.ndarray, bands: np.ndarray,
) -> tuple[list[float], list[str]]:
    """Drift-and-rebalance a fixed-weight portfolio: reset ALL weights to target
    whenever ANY holding drifts more than its own band away from its target
    weight (a band of 0 means that holding never triggers)."""
    w = target.copy()
    rets: list[float] = []
    rebalances: list[str] = []
    for t in range(R.shape[0]):
        r = R[t]
        rets.append(float(w @ r))
        w = w * (1.0 + r)
        s = w.sum()
        if s > 0:
            w = w / s
        breach = any(
            bands[i] > 0 and abs(w[i] - target[i]) > bands[i] + 1e-12 for i in range(len(w))
        )
        if breach:
            w = target.copy()
            rebalances.append(months[t])
    return rets, rebalances


def simulate_portfolio(
    holdings: list[tuple[str, dict[str, float], float, float]],
    rf_annual: float = 0.0,
) -> PortfolioOptimization:
    """Backtest a HAND-SPECIFIED portfolio. `holdings` is
    `[(label, monthly_returns, weight, band), …]` with the STRATEGY first
    (label "Strategy"). Weights are normalized to sum to 1; each holding has its
    own rebalance band (any-breach → reset all to target). `before` is the
    strategy alone; `after` is the drift-rebalanced manual portfolio."""
    names = [h[0] for h in holdings]
    series = [h[1] for h in holdings]
    raw_w = np.array([h[2] for h in holdings], dtype=float)
    bands = np.array([h[3] for h in holdings], dtype=float)
    total = raw_w.sum()
    target = (raw_w / total) if total > 0 else raw_w
    common = sorted(set.intersection(*[set(s) for s in series])) if series else []

    limited_by = None
    if common:
        starts = [(min(s), nm) for s, nm in zip(series, names) if s]
        if starts:
            bound_month, bound_name = max(starts, key=lambda t: t[0])
            if bound_month == common[0] and bound_name != "Strategy":
                limited_by = bound_name

    n_assets = len(names)
    base_w = np.zeros(n_assets)
    base_w[0] = 1.0
    R = np.array([[s[m] for s in series] for m in common], dtype=float) if common else np.empty((0, n_assets))
    before_rets = (R @ base_w).tolist() if common else []
    if common and R.shape[0] >= 2:
        after_rets, rebalance_dates = _simulate_with_bands(list(common), R, target, bands)
    else:
        after_rets = (R @ target).tolist() if common else []
        rebalance_dates = []
    return _assemble_optimization(
        names, list(common), target, before_rets, after_rets, rebalance_dates, limited_by, rf_annual,
    )


@dataclass
class HoldingState:
    label: str
    target: float       # target weight (fraction)
    current: float      # current drifted weight (fraction)
    band: float         # rebalance band (fraction)
    breached: bool      # is the current weight outside its band right now?


@dataclass
class PortfolioState:
    enough_data: bool
    as_of: str | None             # latest month the drift was computed through
    last_rebalance: str | None    # most recent month a band-rebalance fired
    rebalance_needed: bool         # does any holding currently breach its band?
    holdings: list[HoldingState]


def portfolio_current_state(
    holdings: list[tuple[str, dict[str, float], float, float]],
) -> PortfolioState:
    """Where a saved portfolio's weights sit TODAY (the latest common month) and
    whether a rebalance is due. Drifts the target weights month by month with a
    band-rebalance (any holding outside its band → reset to target) — but does
    NOT reset on the final month, so the returned `current` weights show the live
    drift and `breached` flags whether you'd need to rebalance now."""
    names = [h[0] for h in holdings]
    series = [h[1] for h in holdings]
    raw_w = np.array([h[2] for h in holdings], dtype=float)
    bands = np.array([h[3] for h in holdings], dtype=float)
    total = raw_w.sum()
    target = (raw_w / total) if total > 0 else raw_w
    common = sorted(set.intersection(*[set(s) for s in series])) if series else []
    if not common:
        return PortfolioState(False, None, None, False, [])

    R = np.array([[s[m] for s in series] for m in common], dtype=float)
    w = target.copy()
    last_rebalance = None
    n = len(common)
    for t in range(n):
        w = w * (1.0 + R[t])
        s = w.sum()
        if s > 0:
            w = w / s
        breach = any(
            bands[i] > 0 and abs(w[i] - target[i]) > bands[i] + 1e-12 for i in range(len(w))
        )
        if breach and t < n - 1:        # never auto-reset on the last bar
            w = target.copy()
            last_rebalance = common[t]

    breached = [
        bool(bands[i] > 0 and abs(w[i] - target[i]) > bands[i] + 1e-12) for i in range(len(w))
    ]
    return PortfolioState(
        enough_data=True,
        as_of=common[-1],
        last_rebalance=last_rebalance,
        rebalance_needed=any(breached),
        holdings=[
            HoldingState(names[i], float(target[i]), float(w[i]), float(bands[i]), breached[i])
            for i in range(len(names))
        ],
    )


def _compound_pct(rets: list[float]) -> float | None:
    """Compounded total return (%) of a monthly return series. None if empty."""
    if not rets:
        return None
    eq = 1.0
    for r in rets:
        eq *= 1.0 + float(r)
    return round((eq - 1.0) * 100.0, 2)


def rets_from_cum(cum_pct: list[float]) -> list[float]:
    """Inverse of `_cum_curve`: per-period returns from a cumulative-% curve."""
    out: list[float] = []
    prev = 0.0
    for c in cum_pct:
        out.append((1.0 + c / 100.0) / (1.0 + prev / 100.0) - 1.0)
        prev = c
    return out


def _since_inception(month: str, inception_iso: str) -> bool:
    """Is the calendar month ('YYYY-MM') on/after the inception date? A month is
    counted from its FIRST day, so go-live 2026-05-30 starts at June (May's bar
    mostly predates it), matching the holdings open-period anchor."""
    inc = (inception_iso or "")[:10]
    return (not inc) or (f"{month}-01" >= inc)


@dataclass
class CalendarReturns:
    mtd_pct: float | None
    ytd_pct: float | None
    since_inception_pct: float | None


def blended_calendar_returns(
    months: list[str], after_rets: list[float], inception_iso: str,
) -> CalendarReturns:
    """MTD / YTD / since-inception from a blended MONTHLY return series.
    `months` are 'YYYY-MM' ascending, `after_rets[i]` the blend's return that
    month. MTD = the latest month; YTD = the latest month's calendar year;
    since-inception = months on/after the go-live date (`_since_inception`)."""
    if not months:
        return CalendarReturns(None, None, None)
    last = months[-1]
    year = last[:4]
    pairs = list(zip(months, after_rets))
    return CalendarReturns(
        mtd_pct=_compound_pct([r for m, r in pairs if m == last]),
        ytd_pct=_compound_pct([r for m, r in pairs if m >= f"{year}-01"]),
        since_inception_pct=_compound_pct([r for m, r in pairs if _since_inception(m, inception_iso)]),
    )


def component_return_since(
    returns: dict[str, float], months: list[str], inception_iso: str,
) -> float | None:
    """A single holding's compounded return over the since-inception months —
    so each sleeve's gain can be eyeballed against its actual price move."""
    return _compound_pct([returns[m] for m in months if m in returns and _since_inception(m, inception_iso)])


def _grid_optimize_weights(
    R: np.ndarray, n_assets: int, nb: int, ne: int,
    core_min: float, core_max: float, grid: float,
    objective: str, rf_annual: float, seed: int, restarts: int = OPTIMIZER_RESTARTS,
) -> np.ndarray:
    """Discrete-grid weight search: find the weights (each a multiple of `grid`,
    e.g. 2.5%) that maximize the objective, with the CORE bucket (strategy +
    bonds) constrained to the inclusive range ``[core_min, core_max]``.

    Method: loop the core-bucket weight over its grid; for each, coordinate-ascend
    by moving ONE `grid` unit at a time WITHIN a group (so the core total stays
    fixed) — strategy↔bonds inside the core, ETF↔ETF inside the sleeve — from an
    even split plus a couple of seeded random restarts. Returns the best weight
    vector (sums to 1). The objective is the fixed-weight (monthly-rebalanced)
    blend's risk-adjusted return — which is EXACTLY the realized portfolio, since
    rebalancing back to these weights every month makes each month's return
    `target · monthly_return`. So the optimized number and the reported `after`
    are the same quantity (no static-vs-rebalanced gap)."""
    total_u = int(round(1.0 / grid))
    core_idx = list(range(0, 1 + nb))            # strategy + bonds
    sleeve_idx = list(range(1 + nb, n_assets))   # diversifier ETFs
    lo_u = max(0, int(round(core_min / grid)))
    hi_u = min(total_u, int(round(core_max / grid)))
    if lo_u > hi_u:
        lo_u = hi_u = max(0, min(total_u, int(round(core_min / grid))))
    if ne == 0:                                  # no sleeve ⇒ the core is the book
        lo_u = hi_u = total_u

    def score_u(u: list[int]) -> float:
        st = annualized_stats((R @ (np.asarray(u, dtype=float) * grid)).tolist(), rf_annual)
        m = st.sortino if objective == "sortino" else st.sharpe
        return m if m is not None else -np.inf

    def fill_even(u: list[int], idxs: list[int], total: int) -> None:
        if not idxs:
            return
        base, rem = divmod(total, len(idxs))
        for k, idx in enumerate(idxs):
            u[idx] = base + (1 if k < rem else 0)

    def fill_random(u: list[int], idxs: list[int], total: int, rng) -> None:
        if not idxs:
            return
        draw = rng.multinomial(total, np.full(len(idxs), 1.0 / len(idxs)))
        for k, idx in enumerate(idxs):
            u[idx] = int(draw[k])

    def ascend(u: list[int]) -> tuple[list[int], float]:
        cur = score_u(u)
        improved = True
        while improved:
            improved = False
            for grp in (core_idx, sleeve_idx):   # within-group moves keep the core sum fixed
                if len(grp) < 2:
                    continue
                for i in grp:
                    for j in grp:
                        if i == j or u[i] == 0:
                            continue
                        u[i] -= 1
                        u[j] += 1
                        sc = score_u(u)
                        if sc > cur + 1e-12:
                            cur, improved = sc, True
                        else:
                            u[i] += 1
                            u[j] -= 1
        return u, cur

    best_w = None
    best_sc = -np.inf
    for cw in range(lo_u, hi_u + 1):
        sleeve_total = total_u - cw
        if ne == 0 and sleeve_total != 0:
            continue
        # Reseed PER core-value so each candidate strategy weight is searched
        # identically regardless of the [core_min, core_max] window — a wider
        # range is then always a superset and can never score worse than a
        # narrower one (no confusing non-monotonicity).
        rng = np.random.default_rng(seed + cw)
        inits: list[list[int]] = []
        even = [0] * n_assets
        fill_even(even, core_idx, cw)
        fill_even(even, sleeve_idx, sleeve_total)
        inits.append(even)
        for _ in range(restarts):
            ru = [0] * n_assets
            fill_random(ru, core_idx, cw, rng)
            fill_random(ru, sleeve_idx, sleeve_total, rng)
            inits.append(ru)
        for ini in inits:
            u, sc = ascend(list(ini))
            # `best_w is None` always takes the first valid constraint-respecting
            # allocation, so a degenerate all-tie case (constant returns →
            # undefined Sharpe everywhere) still honours [core_min, core_max].
            if best_w is None or sc > best_sc:
                best_sc, best_w = sc, np.asarray(u, dtype=float) * grid
    if best_w is None:
        best_w = np.zeros(n_assets)
        best_w[0] = 1.0
    return best_w


def optimize_portfolio(
    strategy_by_month: dict[str, float],
    etfs: list[tuple[str, dict[str, float]]],
    bonds: list[tuple[str, dict[str, float]]] | None = None,
    rf_annual: float = 0.0,
    objective: str = "sharpe",
    core_pct: float | None = None,
    core_min: float | None = None,
    core_max: float | None = None,
    grid: float = 0.025,
    n_samples: int = 4000,
    seed: int = 0,
    search_restarts: int | None = None,
) -> PortfolioOptimization:
    """Optimize a 3-group portfolio on a DISCRETE weight grid, MONTHLY-rebalanced.

    Three groups:
      * **Core** — the Strategy + the `bonds`. Its total weight is SEARCHED over
        the inclusive range ``[core_min, core_max]`` (so you bound how much the
        strategy gets; with no bonds the core IS the strategy). Within the core,
        strategy↔bonds is split optimally.
      * **Diversifier sleeve** (`1 − core`): the `etfs` (gold/USD/…), split
        optimally among themselves.
      * (If there are no diversifier ETFs the core becomes the whole book.)

    Every weight is a multiple of `grid` (default 0.025 = 2.5%) — a discrete grid,
    not a continuum. The portfolio is rebalanced back to the target weights EVERY
    month, so each month's realized return is `target · monthly_return` (the
    fixed-weight blend); `after` is that monthly-rebalanced blend. Because the
    optimizer's objective IS that same blend, the optimized number and `after`
    are the SAME quantity — so `after` can never come out worse than an achievable
    alternative (e.g. the strategy alone, when 100% is in range). `before` is
    strategy-alone. Runs over the COMMON window where every selected asset has
    data. `rebalance_*` on the result are empty (rebalancing is monthly, fixed).

    `core_pct` is a back-compat convenience: when `core_min`/`core_max` are
    omitted it pins the core to that single value (equivalent to
    `core_min == core_max == core_pct`); the default is 0.6.

    Method: coordinate-ascent on the unit grid (within-group 2.5% moves) from an
    even split + seeded random restarts, looped over the core-weight grid. No
    scipy; deterministic; robust for Sortino. `n_samples` is unused (kept for
    signature stability)."""
    if core_min is None and core_max is None:
        base = core_pct if core_pct is not None else 0.6
        core_min = core_max = base
    elif core_min is None:
        core_min = core_max
    elif core_max is None:
        core_max = core_min
    core_min = max(0.0, min(1.0, core_min))
    core_max = max(0.0, min(1.0, core_max))
    if core_min > core_max:
        core_min, core_max = core_max, core_min

    bonds = bonds or []
    nb, ne = len(bonds), len(etfs)
    names = ["Strategy"] + [n for n, _ in bonds] + [n for n, _ in etfs]
    series = [strategy_by_month] + [s for _, s in bonds] + [s for _, s in etfs]
    common = sorted(set.intersection(*[set(s) for s in series])) if series else []

    # Which asset's earliest month bounds the common start (info for the UI).
    limited_by = None
    if common:
        starts = [(min(s), nm) for s, nm in zip(series, names) if s]
        if starts:
            bound_month, bound_name = max(starts, key=lambda t: t[0])
            if bound_month == common[0] and bound_name != "Strategy":
                limited_by = bound_name

    n_assets = len(names)
    base_w = np.zeros(n_assets)
    base_w[0] = 1.0   # strategy-alone benchmark
    R = np.array([[s[m] for s in series] for m in common], dtype=float) if common else np.empty((0, n_assets))

    target = base_w.copy()
    if common and R.shape[0] >= 2 and (nb > 0 or ne > 0):
        restarts = OPTIMIZER_RESTARTS if search_restarts is None else max(1, int(search_restarts))
        target = _grid_optimize_weights(
            R, n_assets, nb, ne, core_min, core_max, grid, objective, rf_annual, seed, restarts,
        )

    before_rets = (R @ base_w).tolist() if common else []
    # The portfolio is rebalanced back to `target` EVERY month (first trading day),
    # so each month's realized return is `target · monthly_return` — i.e. the
    # fixed-weight blend. (Weights drift intra-month with prices, but the monthly
    # data resets them.) `after` is therefore the static blend; no band/drift
    # simulation, and it equals exactly the objective the optimizer maximized.
    after_rets = (R @ target).tolist() if common else []
    rebalance_dates: list[str] = []

    return _assemble_optimization(
        names, list(common), target, before_rets, after_rets, rebalance_dates, limited_by, rf_annual,
    )


def analyze_pair(
    strategy_by_month: dict[str, float],
    etf_by_month: dict[str, float],
    rf_annual: float = 0.0,
    w_max: float = 0.5,
    step: float = 0.05,
    objective: str = "sharpe",
) -> dict:
    """Full per-ETF diversification result against the strategy.

    Aligns the two monthly series, then returns correlation, the ETF's
    standalone stats, and the `objective`-optimal blend with the LIFT over
    the pure-strategy baseline in BOTH Sharpe and Sortino (so the UI can
    show "+0.18 Sharpe at 15%" regardless of which metric was optimized).
    `overlap_months` lets the caller flag thin overlaps.
    """
    s_vals, e_vals, months = align(strategy_by_month, etf_by_month)
    corr = pearson(s_vals, e_vals)
    etf_stats = annualized_stats(e_vals, rf_annual)
    base = annualized_stats(s_vals, rf_annual)
    blend = optimal_blend(s_vals, e_vals, rf_annual, w_max, step, objective)

    def _delta(a: float | None, b: float | None) -> float | None:
        return (a - b) if (a is not None and b is not None) else None

    return {
        "overlap_months": len(months),
        "overlap_from": months[0] if months else None,
        "overlap_to": months[-1] if months else None,
        "correlation": corr,
        # The strategy's OWN stats over THIS overlap window — the apples-to-
        # apples baseline the blend (and its lift) is measured against. Differs
        # from the full-period headline when the ETF's history is shorter.
        "strategy_ann_return": base.ann_return,
        "strategy_ann_vol": base.ann_vol,
        "strategy_sharpe": base.sharpe,
        "strategy_sortino": base.sortino,
        "etf_ann_return": etf_stats.ann_return,
        "etf_ann_vol": etf_stats.ann_vol,
        "etf_sharpe": etf_stats.sharpe,
        "etf_sortino": etf_stats.sortino,
        "blend_weight": blend.weight,
        "blend_sharpe": blend.sharpe,
        "blend_sortino": blend.sortino,
        "blend_ann_return": blend.ann_return,
        "blend_ann_vol": blend.ann_vol,
        "sharpe_lift": _delta(blend.sharpe, base.sharpe),
        "sortino_lift": _delta(blend.sortino, base.sortino),
    }
