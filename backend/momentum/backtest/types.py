"""Type aliases, dataclasses, and shared sector-name helpers for the
momentum backtest engine.

This module is import-cycle-free — every other backtest submodule (dates,
indices, equity_curve, preparation, runner, current_portfolio) imports
from here, but nothing in here imports from another backtest submodule.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Literal

from ..signals import PRICE_SIGNAL_DEFS


# Rebalance cadence. New variants get added here + to _periods_per_year +
# _generate_rebalance_dates. The default "monthly" preserves the original
# month-start, hold-1-month behavior; the others stride differently.
RebalanceFrequency = Literal[
    "daily", "weekly", "monthly",
    "every_2_months", "every_3_months", "every_4_months", "every_5_months",
    "every_6_months", "every_7_months", "every_8_months", "every_9_months",
    "every_10_months", "every_11_months", "every_12_months",
]

_DEFAULT_FREQUENCY: RebalanceFrequency = "monthly"


# Long-only = pick top sectors / top names per sector and equal-weight long them.
# Long-short = also pick bottom sectors / bottom names and short them at 100%
# gross on each side (200% gross, 0% net). Period return is then
# mean(long_returns) − mean(short_returns).
StrategyType = Literal["long_only", "long_short"]

_DEFAULT_STRATEGY: StrategyType = "long_only"

# A holding's `side` decides whether its forward return contributes positively
# or negatively to the period return. Long-only backtests emit only "long"
# rows; long-short emits both, dedupe-collisions removed.
HoldingSide = Literal["long", "short"]


def _periods_per_year(freq: RebalanceFrequency) -> float:
    """Approximate number of rebalance periods per calendar year.

    Used to (a) annualize total return, (b) scale Sharpe by √(periods/yr).
    Daily uses 252 trading days; weekly uses 52; calendar-month variants
    use 12 / 6 / 4. These are nominal — actual generated dates may differ
    slightly when prices_df is short of trading days at boundaries.
    """
    return {
        "daily": 252.0,
        "weekly": 52.0,
        "monthly": 12.0,
        "every_2_months": 6.0,
        "every_3_months": 4.0,
        "every_4_months": 3.0,
        "every_5_months": 12.0 / 5.0,
        "every_6_months": 2.0,
        "every_7_months": 12.0 / 7.0,
        "every_8_months": 12.0 / 8.0,
        "every_9_months": 12.0 / 9.0,
        "every_10_months": 12.0 / 10.0,
        "every_11_months": 12.0 / 11.0,
        "every_12_months": 1.0,
    }[freq]


# Common cross-source sector aliases. The universe's `sector` column may
# come from iShares fund holdings ("Technology", "Communication") or
# Wikipedia-scraped S&P 500 data ("Information Technology",
# "Communication Services") or LongEquity inputs — all referring to the
# same GICS bucket. `_norm_sector` collapses them so the sector_etf
# mapping (set once on /benchmarks with the canonical names) matches no
# matter which source provided the row. Add lowercase keys; values match
# the canonical form a normalized comparison will reach.
_SECTOR_ALIASES: dict[str, str] = {
    "technology": "information technology",
    "tech": "information technology",
    "communication": "communication services",
    "communications": "communication services",
    "telecom": "communication services",
    "telecommunication": "communication services",
    "telecommunications": "communication services",
    "telecommunication services": "communication services",
    "healthcare": "health care",
    "financial": "financials",
    "financial services": "financials",
}


def _norm_sector(s: str | None) -> str:
    """Lowercase + strip + alias-map a sector string for cross-source
    comparison. Returns "" for None/empty so unset rows never accidentally
    match each other."""
    if not s:
        return ""
    base = " ".join(s.lower().split())
    return _SECTOR_ALIASES.get(base, base)


@dataclass
class BacktestConfig:
    start_date: date
    end_date: date
    signal_weights: dict[str, float]
    top_n_sectors: int = 4
    top_n_per_sector: int = 6
    category_weights: dict[str, float] | None = None  # e.g. {"price": 0.5, "volume": 0.5}
    selection_mode: str = "momentum"  # "momentum" | "random" | "all" | "sector_etf"
    random_seed: int | None = None  # only used when selection_mode == "random"
    # Sector -> benchmark_id mapping. Required only when
    # selection_mode == "sector_etf". The strategy ranks sectors via the
    # usual stock-aggregate momentum, then holds the mapped ETF (one per
    # selected sector) instead of picking individual stocks. Returns are
    # read from benchmark_price.
    sector_etfs: dict[str, int] | None = None
    rebalance_frequency: RebalanceFrequency = _DEFAULT_FREQUENCY
    # Weekday each rebalance lands on within its period: Mon=0..Sun=6.
    # 0 (Monday) is the historical default — every period's rebalance is
    # the first Monday of the month/quarter/etc. Set to 2 (Wednesday) to
    # rebalance on the first Wednesday, computing signals from data
    # through the prior trading day's close. Ignored for `daily`
    # frequency (which rebalances every trading day).
    rebalance_weekday: int = 0
    strategy_type: StrategyType = _DEFAULT_STRATEGY
    # Optional gate: drop any company whose `score_price` (the 0-100
    # price-category score) is at or below this threshold before
    # sector aggregation. Applied only to the long bucket
    # (`direction="top"` in score_and_select) — the short bucket of a
    # long-short strategy intentionally targets low-scoring names.
    # None disables the filter (default), matching pre-feature
    # behavior.
    min_price_score: float | None = None
    # When True, run_backtest appends one trailing "open" period record
    # whose entry is the last scheduled rebalance date and whose exit is
    # the most recent available close. The open period appears in
    # monthly_records and the daily curve, but is excluded from Sharpe
    # and annualization (so the headline stats stay apples-to-apples with
    # the closed periods).
    #
    # Default here is False so library/test callers get the historical
    # closed-only behavior unchanged. The HTTP path (BacktestConfig.from_dict)
    # defaults this to True so API requests opt in by default.
    include_open_period: bool = False
    # === Volatility targeting ===========================================
    # When set, each rebalance scales the book's exposure by
    #   k_t = min(vol_target_max_leverage, vol_target / σ̂_t)
    # where σ̂_t is the trailing annualized volatility of the equal-
    # weighted basket of the selected holdings, estimated over
    # `vol_target_lookback` trading days ending strictly before the
    # rebalance bar (the prior-close `entry_ts` the runner already uses —
    # no look-ahead). The remaining `1 - k_t` sits in cash (earns 0, same
    # rf=0 convention as the Sharpe calc). `vol_target` is an ANNUALIZED
    # percent (e.g. 12.0 = run at ~12% vol). None disables (default), so
    # every existing caller keeps fixed 100% exposure.
    #
    # `vol_target_max_leverage` caps k_t. 1.0 = "de-risk only" — the book
    # can scale down into cash in turbulent regimes but never borrows;
    # >1.0 would let calm regimes lever up.
    vol_target: float | None = None
    vol_target_lookback: int = 60
    vol_target_max_leverage: float = 1.0
    # === Market-regime (trend) filter ===================================
    # When `regime_floor` is set, each rebalance scales exposure on a
    # linear ramp driven by a composite 0..1 market-health score (trend +
    # 6-month momentum + drawdown breadth — see `compute_market_health`):
    #   health >= regime_ramp_hi → fully invested (1.0)
    #   health <= regime_ramp_lo → at the floor
    #   in between                → proportional (cash ∝ market weakness).
    # `regime_floor` is the defensive floor (0.0 = all cash, 0.5 = half).
    # The factor multiplies into the same per-period exposure scale as vol
    # targeting, so the two compose. None disables (default) — the
    # original strategy is untouched.
    regime_floor: float | None = None
    regime_ramp_lo: float = 0.3
    regime_ramp_hi: float = 0.7
    # === Daily "tit-for-tat" timing overlay =============================
    # When True, gate daily exposure on the strategy's OWN prior-day return:
    # if yesterday's daily return was >= 0 we hold the full strategy today;
    # if it was negative we sit in cash today (0%). A momentum-on-momentum
    # day-trading filter. It reshapes the daily equity curve (and therefore
    # Sharpe / Sortino / max-DD / total) but does NOT change selection — the
    # holdings per period are identical; only the day-by-day in/out differs.
    # False (default) leaves the curve untouched.
    daily_timing: bool = False

    @classmethod
    def from_dict(cls, d: dict) -> BacktestConfig:
        return cls(
            start_date=date.fromisoformat(d["start_date"]),
            end_date=date.fromisoformat(d["end_date"]),
            signal_weights=d.get("signal_weights", {s["key"]: s["default_weight"] for s in PRICE_SIGNAL_DEFS}),
            top_n_sectors=d.get("top_n_sectors", 4),
            top_n_per_sector=d.get("top_n_per_sector", 6),
            category_weights=d.get("category_weights"),
            selection_mode=d.get("selection_mode", "momentum"),
            random_seed=d.get("random_seed"),
            sector_etfs=d.get("sector_etfs"),
            rebalance_frequency=d.get("rebalance_frequency", _DEFAULT_FREQUENCY),
            rebalance_weekday=int(d.get("rebalance_weekday", 0) or 0),
            strategy_type=d.get("strategy_type", _DEFAULT_STRATEGY),
            min_price_score=d.get("min_price_score"),
            include_open_period=d.get("include_open_period", True),
            vol_target=d.get("vol_target"),
            vol_target_lookback=int(d.get("vol_target_lookback", 60) or 60),
            vol_target_max_leverage=float(d.get("vol_target_max_leverage", 1.0) or 1.0),
            regime_floor=d.get("regime_floor"),
            regime_ramp_lo=float(
                d.get("regime_ramp_lo", 0.3) if d.get("regime_ramp_lo") is not None else 0.3
            ),
            regime_ramp_hi=float(
                d.get("regime_ramp_hi", 0.7) if d.get("regime_ramp_hi") is not None else 0.7
            ),
            daily_timing=bool(d.get("daily_timing", False)),
        )


@dataclass
class PeriodHolding:
    company_id: int
    ticker: str
    company_name: str
    sector: str
    score: float
    category_scores: dict[str, float | None]  # e.g. {"price": 72.5, "volume": 45.1}
    weight: float
    forward_return_pct: float | None
    currency: str | None = None
    entry_price_local: float | None = None
    exit_price_local: float | None = None
    entry_price_eur: float | None = None
    exit_price_eur: float | None = None
    entry_date: str | None = None
    exit_date: str | None = None
    # "long" or "short". forward_return_pct is always the underlying price
    # return; the period-level aggregator uses `side` to decide the sign of
    # the contribution. Long-only backtests emit "long" everywhere.
    side: HoldingSide = "long"
    # 1-indexed rank of this holding's sector within the period's
    # chosen sectors (1 = best-scoring sector picked, N = Nth-best).
    # Set by score_and_select; None for sector-ETF mode + legacy
    # snapshots persisted before the rank columns existed.
    sector_rank: int | None = None
    # 1-indexed rank of this company within its sector (1 = best-scoring
    # company in the sector, M = Mth-best). None when unavailable.
    company_rank: int | None = None


@dataclass
class PeriodRecord:
    date: str  # YYYY-MM
    holdings: list[PeriodHolding]
    portfolio_return_pct: float | None
    cumulative_return_pct: float
    empty_reason: str | None = None
    # True when this record is the trailing "open" period — the strategy's
    # current portfolio whose holding period hasn't yet completed. Exit
    # price for an open period is the most recent available close rather
    # than the next scheduled rebalance. Open periods are included in
    # monthly_records (and the daily curve) for visibility, but they are
    # excluded from Sharpe and annualization stats since they represent a
    # partial holding period.
    is_open: bool = False
    # Effective exit date for an open period — set to the most recent date
    # common to every held company (min of per-holding max trade dates).
    # The frontend surfaces this so it's clear how stale the "current
    # holding" return is when some names stopped reporting earlier than
    # others. Null on closed periods.
    as_of_date: str | None = None
    # "What if you held the entire eligible universe equally weighted?"
    # — the no-skill baseline this strategy compares itself against. Per-
    # period return and the chain-linked cumulative; same denominator as
    # the strategy (entry_ts → exit_ts) so apples-to-apples. Null when
    # no eligible company had usable prices for the window.
    universe_return_pct: float | None = None
    universe_cumulative_return_pct: float | None = None
    # Number of companies that actually contributed to universe_return_pct
    # this period (eligibility minus missing-price drops). Useful diagnostic
    # for "is the universe baseline meaningful here?".
    universe_constituents: int | None = None
    # Book-level exposure multiplier applied this period by volatility
    # targeting (1.0 = fully invested, the default + non-targeted case).
    # The period's `portfolio_return_pct` is already scaled by this; the
    # daily-equity-curve reconstruction also reads it so Sharpe / Sortino /
    # max-DD reflect the scaled exposure. Surfaced for transparency so the
    # UI can show "held 0.6x in this turbulent month".
    exposure_scale: float = 1.0
    # Composite 0..1 market-health score that drove the regime filter this
    # period (trend + 6-month momentum + drawdown breadth). Populated only
    # when the regime filter is active (regime_floor set); None otherwise.
    # Surfaced so the health signal can be charted for manual inspection.
    market_health: float | None = None
    # Number of daily cash<->stocks swaps the tit-for-tat timing overlay
    # made during this period (each = one full-book trade). 0 when timing is
    # off. The client fee model charges these at the held book's per-exchange
    # fees so the (net) figures reflect the daily churn cost.
    daily_timing_swaps: int = 0
    # Per-component breakdown of `market_health` — {trend, momentum,
    # drawdown, composite}. Display-only; lets the /regime-detector page
    # chart each sub-signal so you can see which one leads a crisis.
    # Populated alongside `market_health` (regime filter active) else None.
    market_health_components: dict | None = None
    # Average RSI(14) across the eligible universe this period, two ways —
    # {"simple": 0..100, "wilder": 0..100}. A momentum-breadth gauge
    # charted separately on /regime-detector. Populated alongside
    # `market_health` (regime filter active) else None.
    universe_rsi: dict | None = None


@dataclass
class DrawdownPeriod:
    """A peak-to-trough drawdown period."""
    drawdown_pct: float
    peak_date: str      # YYYY-MM
    trough_date: str    # YYYY-MM
    recovery_date: str | None  # YYYY-MM or None if not yet recovered


@dataclass
class BacktestSummary:
    total_return_pct: float
    annualized_return_pct: float
    max_drawdown_pct: float
    sharpe_ratio: float | None
    # Sortino: same idea as Sharpe but only the negative-return tail
    # contributes to the volatility denominator. None when too few
    # closed periods to estimate downside std reliably.
    sortino_ratio: float | None = None
    # % of closed periods whose return was strictly > 0. Combined
    # with median_period_return_pct it shows whether the strategy's
    # headline mean is carried by many small wins or a few big ones.
    win_rate_pct: float | None = None
    median_period_return_pct: float | None = None
    avg_monthly_turnover_pct: float = 0.0
    total_months: int = 0
    avg_holdings: float = 0.0
    top_drawdowns: list[DrawdownPeriod] = field(default_factory=list)
    # Universe (equal-weighted-everything) headline — the no-skill
    # baseline. Same chain-link math as `total_return_pct`, computed
    # over the same closed periods. Alpha = total_return_pct minus
    # this. Null on degenerate runs (no closed periods produced a
    # universe return).
    universe_total_return_pct: float | None = None
    universe_annualized_return_pct: float | None = None
    # Populated only when the backtest aggregates across multiple trials
    # (random-baseline mode with n_trials > 1). All headline stats above
    # are means in that case; these fields hold the cross-trial std-dev.
    n_trials: int | None = None
    total_return_pct_std: float | None = None
    annualized_return_pct_std: float | None = None
    max_drawdown_pct_std: float | None = None
    sharpe_ratio_std: float | None = None
    avg_monthly_turnover_pct_std: float | None = None


@dataclass
class DailyPick:
    """One trading day's worth of hypothetical picks + turnover vs the previous
    day. Two return numbers per day:
      - portfolio_return_pct: chain-linked cumulative MTD through this day,
        formed by multiplying each day's equal-weighted close-to-close return
        across rebalances.
      - next_day_return_pct: equal-weighted close-to-close return of *this*
        day's portfolio held until the next trading day. NULL on the latest
        day in the panel (no next day yet)."""
    date: str                              # YYYY-MM-DD
    holdings: list[PeriodHolding]
    turnover_abs: int                      # number of holdings that differ from previous day
    turnover_pct: float                    # turnover_abs / max(len(today), len(prev)) * 100
    portfolio_return_pct: float | None = None
    next_day_return_pct: float | None = None


@dataclass
class CurrentPortfolio:
    """Snapshot of what the strategy would hold right now (rebalance on
    the latest rebalance weekday on-or-before today — e.g. the first
    Wednesday of the current month — MTD return through the latest
    price)."""
    as_of_date: str       # YYYY-MM-DD — the effective rebalance date
    latest_price_date: str | None  # most recent price date observed across the portfolio
    holdings: list[PeriodHolding]
    daily_picks: list[DailyPick] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "as_of_date": self.as_of_date,
            "latest_price_date": self.latest_price_date,
            "holdings": [
                {
                    "company_id": h.company_id,
                    "ticker": h.ticker,
                    "company_name": h.company_name,
                    "sector": h.sector,
                    "score": h.score,
                    "category_scores": h.category_scores,
                    "weight": round(h.weight, 4),
                    "forward_return_pct": h.forward_return_pct,
                    "currency": h.currency,
                    "entry_price_local": h.entry_price_local,
                    "exit_price_local": h.exit_price_local,
                    "entry_price_eur": h.entry_price_eur,
                    "exit_price_eur": h.exit_price_eur,
                    "entry_date": h.entry_date,
                    "exit_date": h.exit_date,
                    "sector_rank": h.sector_rank,
                    "company_rank": h.company_rank,
                }
                for h in self.holdings
            ],
            "daily_picks": [
                {
                    "date": d.date,
                    "turnover_abs": d.turnover_abs,
                    "turnover_pct": d.turnover_pct,
                    "portfolio_return_pct": d.portfolio_return_pct,
                    "next_day_return_pct": d.next_day_return_pct,
                    "holdings": [
                        {
                            "company_id": h.company_id,
                            "ticker": h.ticker,
                            "company_name": h.company_name,
                            "sector": h.sector,
                            "score": h.score,
                            "category_scores": h.category_scores,
                            "weight": round(h.weight, 4),
                            "forward_return_pct": h.forward_return_pct,
                            "currency": h.currency,
                            "entry_price_local": h.entry_price_local,
                            "exit_price_local": h.exit_price_local,
                            "entry_price_eur": h.entry_price_eur,
                            "exit_price_eur": h.exit_price_eur,
                            "entry_date": h.entry_date,
                            "exit_date": h.exit_date,
                            "sector_rank": h.sector_rank,
                            "company_rank": h.company_rank,
                        }
                        for h in d.holdings
                    ],
                }
                for d in self.daily_picks
            ],
        }


@dataclass
class BacktestResult:
    monthly_records: list[PeriodRecord]
    summary: BacktestSummary
    # Daily portfolio equity curve, chain-linked across rebalance periods.
    # Each entry: (YYYY-MM-DD, cumulative_return_pct). Empty for degenerate
    # runs with no holdings; the frontend falls back to the period curve in
    # that case.
    daily_records: list[tuple[str, float]] = field(default_factory=list)
    # Daily universe equal-weight baseline curve — same shape as
    # `daily_records`, but computed over every eligible cid (the
    # "no-skill" baseline) rather than the strategy's selections.
    # The frontend prefers this over the per-period
    # `universe_cumulative_return_pct` on monthly_records so the gray
    # baseline line matches the strategy line's daily granularity.
    # Empty on legacy/degenerate runs; frontend then falls back to the
    # per-period chain.
    universe_daily_records: list[tuple[str, float]] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "daily_records": [
                {"date": d, "cumulative_return_pct": cum}
                for d, cum in self.daily_records
            ],
            "universe_daily_records": [
                {"date": d, "cumulative_return_pct": cum}
                for d, cum in self.universe_daily_records
            ],
            "monthly_records": [
                {
                    "date": r.date,
                    "holdings": [
                        {
                            "company_id": h.company_id,
                            "ticker": h.ticker,
                            "company_name": h.company_name,
                            "sector": h.sector,
                            "score": h.score,
                            "category_scores": h.category_scores,
                            "weight": round(h.weight, 4),
                            "forward_return_pct": h.forward_return_pct,
                            "currency": h.currency,
                            "entry_price_local": h.entry_price_local,
                            "exit_price_local": h.exit_price_local,
                            "entry_price_eur": h.entry_price_eur,
                            "exit_price_eur": h.exit_price_eur,
                            "entry_date": h.entry_date,
                            "exit_date": h.exit_date,
                            "side": h.side,
                            "sector_rank": h.sector_rank,
                            "company_rank": h.company_rank,
                        }
                        for h in r.holdings
                    ],
                    "portfolio_return_pct": r.portfolio_return_pct,
                    "cumulative_return_pct": r.cumulative_return_pct,
                    "universe_return_pct": r.universe_return_pct,
                    "universe_cumulative_return_pct": r.universe_cumulative_return_pct,
                    "universe_constituents": r.universe_constituents,
                    **({"empty_reason": r.empty_reason} if r.empty_reason else {}),
                    **({"is_open": True} if r.is_open else {}),
                    **({"as_of_date": r.as_of_date} if r.as_of_date else {}),
                    **({"exposure_scale": r.exposure_scale} if r.exposure_scale != 1.0 else {}),
                    **({"daily_timing_swaps": r.daily_timing_swaps} if r.daily_timing_swaps else {}),
                    **({"market_health": r.market_health} if r.market_health is not None else {}),
                    **({"market_health_components": r.market_health_components} if r.market_health_components is not None else {}),
                    **({"universe_rsi": r.universe_rsi} if r.universe_rsi is not None else {}),
                }
                for r in self.monthly_records
            ],
            "summary": {
                "total_return_pct": self.summary.total_return_pct,
                "annualized_return_pct": self.summary.annualized_return_pct,
                "max_drawdown_pct": self.summary.max_drawdown_pct,
                "sharpe_ratio": self.summary.sharpe_ratio,
                "sortino_ratio": self.summary.sortino_ratio,
                "win_rate_pct": self.summary.win_rate_pct,
                "median_period_return_pct": self.summary.median_period_return_pct,
                "avg_monthly_turnover_pct": self.summary.avg_monthly_turnover_pct,
                "total_months": self.summary.total_months,
                "avg_holdings": self.summary.avg_holdings,
                "universe_total_return_pct": self.summary.universe_total_return_pct,
                "universe_annualized_return_pct": self.summary.universe_annualized_return_pct,
                "top_drawdowns": [
                    {
                        "drawdown_pct": dd.drawdown_pct,
                        "peak_date": dd.peak_date,
                        "trough_date": dd.trough_date,
                        "recovery_date": dd.recovery_date,
                    }
                    for dd in self.summary.top_drawdowns
                ],
                "n_trials": self.summary.n_trials,
                "total_return_pct_std": self.summary.total_return_pct_std,
                "annualized_return_pct_std": self.summary.annualized_return_pct_std,
                "max_drawdown_pct_std": self.summary.max_drawdown_pct_std,
                "sharpe_ratio_std": self.summary.sharpe_ratio_std,
                "avg_monthly_turnover_pct_std": self.summary.avg_monthly_turnover_pct_std,
            },
        }
