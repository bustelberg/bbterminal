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
    strategy_type: Literal["long_only", "long_short"] = "long_only"
    # When set (non-empty), the request becomes a variants sweep: the data
    # pipeline (universe load → ensure → bulk-load prices/volumes → FX) runs
    # ONCE, then the backtest computation runs per variant against the same
    # in-memory frames. Each variant emits its own `variant_start` /
    # `variant_result` / `variant_error` events identified by a key of
    # `{frequency}__{strategy_type}`. Sweeps are backtest-only; combining
    # `variants` with `mode="current_portfolio"` is rejected.
    variants: list[VariantSpec] | None = None
