"""Request models + defaults for the backtest SSE endpoint.

Keeping the Pydantic schemas in their own module isolates them from
the giant SSE orchestrator and lets `current_picks.py` import
`BacktestRequest` without dragging in the whole pipeline."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


_DEFAULT_END = "2026-01-01"
_DEFAULT_START = "2017-01-01"


class VariantSpec(BaseModel):
    frequency: Literal[
        "daily", "weekly", "monthly",
        "every_2_months", "every_3_months", "every_4_months", "every_5_months",
        "every_6_months", "every_7_months", "every_8_months", "every_9_months",
        "every_10_months", "every_11_months", "every_12_months",
    ]
    strategy_type: Literal["long_only", "long_short"]
    # Per-variant overrides for the cross-product sweep. None on any
    # field means "inherit from the base BacktestRequest" so the legacy
    # 2-axis sweep (just frequency × strategy) keeps its previous
    # behavior — the new axes are purely additive.
    top_n_sectors: int | None = None
    top_n_per_sector: int | None = None
    min_price_score: float | None = None
    universe_label: str | None = None
    index_universe: str | None = None
    grouping: Literal["sector", "industry"] | None = None
    # Weekday each rebalance lands on (Mon=0..Sun=6). None inherits the
    # base request's `rebalance_weekday`. Lets a sweep compare e.g. first-
    # Monday vs first-Wednesday rebalances — signals are computed strict-`<`
    # the rebalance date, so a Wednesday variant decides on the prior
    # trading day's (Tuesday's) close and enters at Wednesday's close.
    rebalance_weekday: int | None = None
    # Annualized volatility target (percent, e.g. 12.0). None inherits the
    # base request's `vol_target` (off by default). When set, each
    # rebalance scales the book's exposure toward the target using the
    # holdings' trailing basket vol, holding cash for the remainder
    # (de-risk only — never levers above 100%). Lets a sweep compare
    # off vs 10 vs 12 vs 15% side by side.
    vol_target: float | None = None
    # Daily "tit-for-tat" timing overlay. None inherits the base request
    # (off). True = hold the full strategy today only if yesterday's daily
    # return was >= 0, else cash — a daily in/out filter that reshapes the
    # equity curve without changing selection. Sweep alongside the plain
    # variant to A/B whether the timing helps.
    daily_timing: bool | None = None
    # Market-regime trend filter: risk-off exposure floor (0.0 = all cash,
    # 0.5 = half) applied when the universe's breadth (% above 200-MA)
    # falls below the base request's `regime_breadth_threshold`. None
    # inherits the base (off by default). Sweep `off, 0, 0.5` to compare
    # full-cash vs partial de-risk vs no filter.
    regime_floor: float | None = None


class BacktestRequest(BaseModel):
    start_date: str = _DEFAULT_START
    end_date: str = _DEFAULT_END  # also used as data cutoff — no data newer than this
    signal_weights: dict[str, float] | None = None
    category_weights: dict[str, float] | None = None  # e.g. {"price": 50, "volume": 50}
    top_n_sectors: int = 4
    top_n_per_sector: int = 6
    max_companies: int = 0  # 0 = all, otherwise limit universe (alphabetical)
    # Optional 0-100 floor on `score_price` for long selection. None
    # disables the filter (default + pre-feature behavior). When set
    # (e.g. 30), only companies whose price-category score strictly
    # exceeds the threshold are eligible for the long bucket. Short
    # bucket of long-short strategies ignores it — see
    # scoring.score_and_select for the rationale.
    min_price_score: float | None = None
    universe_label: str | None = None  # if set, use universe_membership for per-month filtering
    index_universe: str | None = None  # if set, use universe_membership for per-month filtering (e.g. "SP500")
    # How `top_n_sectors` buckets companies. "sector" is the universe-level
    # sector tag (works for every universe). "industry" is the finer Leonteq
    # taxonomy and is only meaningful when the universe is one that carries
    # industry data -- LEONTEQ or ACWI_LEONTEQ today. The label of the
    # top_n_sectors slider in the UI flips with this; the downstream scoring
    # code groups by whatever string lives in the panel's `sector` column
    # so a request boundary swap is enough.
    grouping: Literal["sector", "industry"] = "sector"
    # Literal values reject typos at the request boundary so a misspelled
    # value never silently routes through a default branch downstream
    # (e.g. an unknown `mode` quietly behaving like "backtest"). New
    # variants need to be added here AND wherever the value is consumed.
    selection_mode: Literal["momentum", "random", "all", "sector_etf"] = "momentum"
    random_seed: int | None = None  # only used when selection_mode == "random"
    n_trials: int = 1  # >1 only valid with selection_mode=="random"; aggregates mean ± std
    # Required when selection_mode == "sector_etf": maps sector name → benchmark_id.
    # The strategy ranks sectors via stock-aggregate momentum then holds the
    # mapped ETF for each picked sector (one per sector). Reuses /benchmarks
    # data for ETF prices; only benchmarks with a non-null `sector` tag are
    # eligible.
    sector_etfs: dict[str, int] | None = None
    mode: Literal["backtest", "current_portfolio"] = "backtest"
    force_recompute: bool = False  # ignore cached result and recompute (applies to backtest + current_portfolio)
    # When true (the default for the user-facing buttons), the compute uses
    # only data already in the DB — no GuruFocus / ECB API calls to fill in
    # gaps. The cron and the explicit "Recompute" button override this so
    # they can refresh stale data.
    db_only: bool = True
    rebalance_frequency: Literal[
        "daily", "weekly", "monthly",
        "every_2_months", "every_3_months", "every_4_months", "every_5_months",
        "every_6_months", "every_7_months", "every_8_months", "every_9_months",
        "every_10_months", "every_11_months", "every_12_months",
    ] = "monthly"
    # Weekday each rebalance lands on within its period: Mon=0..Sun=6.
    # 0 (Monday) is the historical default. 2 (Wednesday) rebalances on
    # the first Wednesday of each period, computing signals from data
    # through the prior trading day's close. Ignored for daily frequency.
    rebalance_weekday: int = 0
    strategy_type: Literal["long_only", "long_short"] = "long_only"
    # Annualized volatility target (percent, e.g. 12.0). None (default)
    # disables vol targeting — the book runs fully invested. When set,
    # each rebalance scales exposure toward the target from the holdings'
    # trailing basket vol, parking the remainder in cash (de-risk only,
    # capped at 100%). Used as the base for a `vol_target` sweep axis or
    # standalone on a single run.
    vol_target: float | None = None
    # Market-regime trend filter. `regime_floor` is the defensive exposure
    # floor (0.0 = all cash, 0.5 = half); None (default) disables it. The
    # book scales linearly between `regime_ramp_lo` (health → floor) and
    # `regime_ramp_hi` (health → fully invested), driven by a composite
    # 0..1 market-health score (trend + 6-month momentum + drawdown).
    # `regime_floor` is the sweep axis; the ramp endpoints stay fixed.
    regime_floor: float | None = None
    regime_ramp_lo: float = 0.3
    regime_ramp_hi: float = 0.7
    # Daily "tit-for-tat" timing overlay (see VariantSpec). False (default)
    # leaves the equity curve untouched; True gates daily exposure on the
    # prior day's return. Base for the daily-timing sweep axis.
    daily_timing: bool = False
    # When set (non-empty), the request becomes a variants sweep: the data
    # pipeline (universe load → ensure → bulk-load prices/volumes → FX) runs
    # ONCE, then the backtest computation runs per variant against the same
    # in-memory frames. Each variant emits its own `variant_start` /
    # `variant_result` / `variant_error` events identified by a key of
    # `{frequency}__{strategy_type}`. Sweeps are backtest-only; combining
    # `variants` with `mode="current_portfolio"` is rejected.
    variants: list[VariantSpec] | None = None
