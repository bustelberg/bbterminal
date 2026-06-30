"""Diversifier analysis — correlate a saved backtest's returns against ETF
price series and find the Sharpe-optimal blend.

The page this backs lets you ask: *I think my strategy is good — what
low-correlation ETF can I bolt on to lift the Sharpe/Sortino of the final
portfolio?* You pick a saved backtest, select some ETFs (stored as
`benchmark` rows — added/refreshed via the existing /api/benchmarks
endpoints, which already fetch ETF prices from GuruFocus), and this
returns each ETF's correlation with the strategy plus the best blend.

Endpoints:
    GET  /api/momentum/diversifier/resolve-name   GuruFocus name for a ticker
    POST /api/momentum/diversifier/correlation    the analysis

Storage note: ETFs reuse the `benchmark` / `benchmark_price` tables (an
ETF here is just a benchmark with no sector tag). The math lives in the
pure, unit-tested `momentum.diversification` module; this router only does
the DB loads and the variant/shape plumbing.
"""
from __future__ import annotations

import asyncio
from datetime import date

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from deps import supabase
from ingest.api_usage import track_api_call
from ingest.prices import _fetch_indicator_from_api
from momentum import diversification as div
from routers.momentum.backtest_crud import load_backtest_result_sync

router = APIRouter(tags=["diversifier"])


# --------------------------------------------------------------------------- #
# Request / response models
# --------------------------------------------------------------------------- #
class CorrelationRequest(BaseModel):
    backtest_run_id: int
    benchmark_ids: list[int]
    # For variant-bundle backtests: which variant's curve to correlate.
    variant_key: str | None = None
    risk_free_rate_pct: float = 0.0
    # Upper bound (inclusive) of the ETF weight the blend optimizer searches.
    max_etf_weight_pct: float = 50.0
    # Which metric the blend optimizer maximizes: "sharpe" or "sortino".
    objective: str = "sharpe"


class StrategyStats(BaseModel):
    run_id: int
    name: str
    variant_key: str | None = None
    months: int
    period_from: str | None = None
    period_to: str | None = None
    ann_return: float | None = None
    ann_vol: float | None = None
    sharpe: float | None = None
    sortino: float | None = None


class DiversifierResult(BaseModel):
    benchmark_id: int
    ticker: str
    name: str
    overlap_months: int
    overlap_from: str | None = None
    overlap_to: str | None = None
    correlation: float | None = None
    # Strategy's own stats over this ETF's overlap window (the blend baseline).
    strategy_ann_return: float | None = None
    strategy_ann_vol: float | None = None
    strategy_sharpe: float | None = None
    strategy_sortino: float | None = None
    etf_ann_return: float | None = None
    etf_ann_vol: float | None = None
    etf_sharpe: float | None = None
    etf_sortino: float | None = None
    blend_weight: float = 0.0
    blend_sharpe: float | None = None
    blend_sortino: float | None = None
    blend_ann_return: float | None = None
    blend_ann_vol: float | None = None
    sharpe_lift: float | None = None
    sortino_lift: float | None = None


class CorrelationResponse(BaseModel):
    strategy: StrategyStats
    results: list[DiversifierResult]


class ResolveNameResponse(BaseModel):
    ticker: str
    name: str
    currency: str | None = None


class OptimizeRequest(BaseModel):
    backtest_run_id: int
    # Diversifier ETFs (gold/USD/…) — the "sleeve" group.
    benchmark_ids: list[int]
    # Bonds — share the CORE bucket with the strategy. Default none.
    bond_ids: list[int] = []
    variant_key: str | None = None
    risk_free_rate_pct: float = 0.0
    objective: str = "sortino"
    # Core bucket (strategy + bonds) weight. The optimizer SEARCHES it over
    # [min, max] on a 2.5% grid (min == max pins it); the diversifier sleeve gets
    # the rest. 100 = no sleeve (strategy + bonds are the whole book). The legacy
    # single `core_weight_pct` is still accepted (treated as min == max) and wins
    # over the range when present.
    core_weight_pct: float | None = None
    core_weight_min_pct: float = 0.0
    core_weight_max_pct: float = 100.0
    # Optional index/ETF to COMPARE against (not part of the portfolio) — its
    # per-year return + vol and overall Sharpe/Sortino are returned alongside.
    compare_benchmark_id: int | None = None
    # Coordinate-ascent restarts per core-weight (search thoroughness). Omit to
    # use the tuned default (OPTIMIZER_RESTARTS); higher = lower chance of missing
    # the global optimum, at more runtime. Deterministic for a given value.
    search_restarts: int | None = None


class PortfolioStats(BaseModel):
    ann_return: float | None = None
    ann_vol: float | None = None
    sharpe: float | None = None
    sortino: float | None = None
    median_month: float | None = None   # median monthly return (fraction)
    win_rate: float | None = None        # fraction of months with return > 0


class AssetWeight(BaseModel):
    label: str            # "Strategy" or the ETF ticker
    name: str | None = None
    weight: float         # 0..1
    group: str = "etf"    # "strategy" | "bond" | "etf"
    # The underlying benchmark row (None for the strategy sleeve) + its stored
    # ISIN, so the optimizer-result UI can show + edit the ISIN inline (PATCH
    # /api/benchmarks/{id}) without a second lookup.
    benchmark_id: int | None = None
    isin: str | None = None


class CurvePoint(BaseModel):
    date: str             # "YYYY-MM"
    before: float         # cumulative return % — strategy alone
    after: float          # cumulative return % — optimized portfolio


class DrawdownInfo(BaseModel):
    depth_pct: float
    peak_date: str
    trough_date: str
    recovery_date: str | None = None   # None = not recovered by window end
    length_months: int


class MonthStatInfo(BaseModel):
    month: str            # "YYYY-MM"
    return_before: float
    return_after: float


class YearStat(BaseModel):
    year: int
    return_before: float | None = None   # calendar-year return (fraction)
    return_after: float | None = None
    vol_before: float | None = None      # annualized vol (fraction)
    vol_after: float | None = None
    months: list[MonthStatInfo] = []     # per-month returns within the year


class BenchmarkYear(BaseModel):
    year: int
    ret: float | None = None      # calendar-year return (fraction)
    vol: float | None = None      # annualized vol (fraction)


class BenchmarkCompare(BaseModel):
    """A comparison index/ETF measured over the portfolio's common window."""
    benchmark_id: int
    ticker: str
    name: str
    stats: PortfolioStats             # Sharpe/Sortino/vol/return over the window
    ytd: float | None = None          # current-year return
    annual: list[BenchmarkYear] = []  # per-year return + vol
    monthly: dict[str, float] = {}    # "YYYY-MM" -> return, for the months view + curve
    drawdowns: list[DrawdownInfo] = []  # top-40 worst drawdowns over the window


class OptimizeResponse(BaseModel):
    objective: str
    months: int
    period_from: str | None = None
    period_to: str | None = None
    limited_by: str | None = None     # ETF whose history bounds the window
    weights: list[AssetWeight]
    before: PortfolioStats            # strategy alone over the common window
    after: PortfolioStats             # optimized mix over the same window
    curve: list[CurvePoint]           # before/after equity curves for the chart
    drawdowns_before: list[DrawdownInfo]   # top-40 worst, strategy alone
    drawdowns_after: list[DrawdownInfo]    # top-40 worst, optimized
    annual: list[YearStat]            # per-year return + vol, before/after
    ytd_before: float | None = None   # current-year return, strategy alone
    ytd_after: float | None = None    # current-year return, optimized
    rebalance_count: int = 0          # # of strategy-trim rebalances over the window
    rebalance_dates: list[str] = []   # months a rebalance fired
    rebalance_freq_months: float | None = None   # avg months between rebalances
    benchmark: BenchmarkCompare | None = None    # optional compare-against index


class BacktestStats(BaseModel):
    """The selected backtest's headline figures, read from its SAVED summary
    (the same numbers shown on /backtest) — so the page can display the
    baseline you're trying to beat the moment a run is picked."""
    run_id: int
    name: str
    variant_key: str | None = None
    # Set (only) when the run is a variant bundle and no variant was chosen —
    # lets the UI show a variant picker immediately on selection.
    available_variant_keys: list[str] | None = None
    sharpe_ratio: float | None = None
    sortino_ratio: float | None = None
    annualized_return_pct: float | None = None
    max_drawdown_pct: float | None = None
    total_return_pct: float | None = None
    months: int | None = None
    period_from: str | None = None
    period_to: str | None = None


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _select_scope(
    result: dict, variant_key: str | None
) -> tuple[dict, list[dict], list[str] | None]:
    """Resolve which curve to use from a loaded result blob.

    Single-run blobs carry `summary` + `monthly_records` at the top level.
    Variant bundles carry `{variants:[{key, label, summary, monthly_records}]}`
    — pick the requested variant (or the sole one). Returns
    `(summary, monthly_records, ambiguous_keys)`: when the run is a bundle
    and no variant could be resolved, `ambiguous_keys` is the available keys
    and the other two are empty — the caller decides whether to prompt
    (strategy-stats) or 400 (correlation).
    """
    variants = result.get("variants")
    if isinstance(variants, list) and variants:
        keys = [str(v.get("key")) for v in variants if isinstance(v, dict)]
        chosen = None
        if variant_key is not None:
            chosen = next(
                (v for v in variants if isinstance(v, dict) and str(v.get("key")) == variant_key),
                None,
            )
        elif len(variants) == 1:
            chosen = variants[0]
        if chosen is None:
            return {}, [], keys
        return chosen.get("summary") or {}, chosen.get("monthly_records") or [], None
    return result.get("summary") or {}, result.get("monthly_records") or [], None


async def _run_name(run_id: int) -> str:
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .select("name")
        .eq("run_id", run_id)
        .limit(1)
        .execute()
    )
    return (resp.data or [{}])[0].get("name") or f"Run {run_id}"


async def _load_benchmark_prices(benchmark_id: int) -> list[tuple[str, float]]:
    """All `(target_date, price)` for a benchmark, paged past the 1000-row cap."""
    out: list[tuple[str, float]] = []
    page = 1000
    offset = 0
    while True:
        resp = await asyncio.to_thread(
            lambda o=offset: supabase.table("benchmark_price")
            .select("target_date, price")
            .eq("benchmark_id", benchmark_id)
            .order("target_date")
            .range(o, o + page - 1)
            .execute()
        )
        rows = resp.data or []
        out.extend((r["target_date"], r["price"]) for r in rows)
        if len(rows) < page:
            break
        offset += page
    return out


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@router.get("/api/momentum/diversifier/resolve-name", response_model=ResolveNameResponse)
async def resolve_name(ticker: str):
    """Best-effort display name + currency for an ETF ticker, read from
    GuruFocus's stock summary (ETFs resolve on the `/stock/{ticker}` path —
    there is no separate `/etf` API endpoint). Falls back to the ticker
    itself so the caller always gets a usable name."""
    t = (ticker or "").strip().upper()
    if not t:
        raise HTTPException(400, "ticker is required")
    data, _log, _status = await asyncio.to_thread(
        _fetch_indicator_from_api, t, "NYSE", "summary"
    )
    await asyncio.to_thread(track_api_call, supabase, "NYSE")
    name, currency = t, None
    if isinstance(data, dict):
        general = (data.get("summary") or {}).get("general") or {}
        name = (general.get("company") or "").strip() or t
        currency = (general.get("currency") or "").strip() or None
    return ResolveNameResponse(ticker=t, name=name, currency=currency)


@router.post("/api/momentum/diversifier/correlation", response_model=CorrelationResponse)
async def correlation(req: CorrelationRequest):
    """Correlate a saved backtest's monthly returns against each selected
    ETF and return the Sharpe-optimal blend per ETF, sorted by correlation
    ascending (lowest = best diversifier first)."""
    result = await asyncio.to_thread(load_backtest_result_sync, req.backtest_run_id)
    if not result:
        raise HTTPException(
            404,
            "Backtest result not available (missing run or its result blob).",
        )

    _summary, monthly, ambiguous = _select_scope(result, req.variant_key)
    if ambiguous is not None:
        raise HTTPException(
            400,
            detail={
                "error": "This backtest is a variant bundle; choose a variant.",
                "available_variant_keys": ambiguous,
            },
        )
    strategy_returns = div.monthly_records_to_returns(monthly)
    if len(strategy_returns) < 2:
        raise HTTPException(
            422,
            "The selected backtest has fewer than 2 completed months — "
            "not enough to correlate.",
        )

    run_name = await _run_name(req.backtest_run_id)
    rf = req.risk_free_rate_pct / 100.0
    w_max = max(0.0, min(req.max_etf_weight_pct, 100.0)) / 100.0

    strat_months = sorted(strategy_returns)
    base = div.annualized_stats([strategy_returns[m] for m in strat_months], rf)
    strategy_stats = StrategyStats(
        run_id=req.backtest_run_id,
        name=run_name,
        variant_key=req.variant_key,
        months=len(strat_months),
        period_from=strat_months[0],
        period_to=strat_months[-1],
        ann_return=base.ann_return,
        ann_vol=base.ann_vol,
        sharpe=base.sharpe,
        sortino=base.sortino,
    )

    # Benchmark metadata for the requested ids.
    meta_resp = await asyncio.to_thread(
        lambda: supabase.table("benchmark")
        .select("benchmark_id, ticker, name")
        .in_("benchmark_id", req.benchmark_ids or [-1])
        .execute()
    )
    meta = {m["benchmark_id"]: m for m in (meta_resp.data or [])}

    results: list[DiversifierResult] = []
    for bid in req.benchmark_ids:
        m = meta.get(bid)
        if not m:
            continue
        prices = await _load_benchmark_prices(bid)
        etf_returns = div.prices_to_monthly_returns(prices)
        a = div.analyze_pair(strategy_returns, etf_returns, rf, w_max=w_max, objective=req.objective)
        results.append(
            DiversifierResult(
                benchmark_id=bid,
                ticker=m["ticker"],
                name=m["name"],
                **a,
            )
        )

    # Lowest correlation first (best diversifier); undefined correlations last.
    results.sort(key=lambda r: (r.correlation is None, r.correlation if r.correlation is not None else 0.0))
    return CorrelationResponse(strategy=strategy_stats, results=results)


def _meta_of_from(ticker_to_meta: dict):
    """Build a `label → (benchmark_id, isin)` resolver from a ticker→benchmark-row
    map. Returns `(None, None)` for the strategy sleeve / any unknown label."""
    def f(label: str) -> tuple[int | None, str | None]:
        m = ticker_to_meta.get(label)
        return (m["benchmark_id"], m.get("isin")) if m else (None, None)
    return f


def _build_benchmark_compare(
    benchmark_id: int, meta: dict, monthly: dict[str, float],
    common_months: list[str], rf: float,
) -> BenchmarkCompare | None:
    """Comparison stats for an index/ETF over the portfolio's EXACT common window
    (same months as before/after) — overall risk-adjusted stats + per-year
    return/vol + the per-month returns for the expandable view. Returns None if
    the benchmark has <2 months of overlap with the window."""
    aligned = [m for m in common_months if m in monthly]
    if len(aligned) < 2:
        return None
    rets = [monthly[m] for m in aligned]
    s = div.annualized_stats(rets, rf)
    years = div.annual_breakdown(aligned, rets, rets)   # before==after; read one side
    dds = div.top_drawdowns(aligned, rets)
    return BenchmarkCompare(
        benchmark_id=benchmark_id,
        ticker=meta.get("ticker", ""),
        name=meta.get("name") or meta.get("ticker", ""),
        stats=PortfolioStats(
            ann_return=s.ann_return, ann_vol=s.ann_vol, sharpe=s.sharpe,
            sortino=s.sortino, median_month=s.median_month, win_rate=s.win_rate,
        ),
        ytd=years[-1].return_before if years else None,
        annual=[BenchmarkYear(year=y.year, ret=y.return_before, vol=y.vol_before) for y in years],
        monthly={m: monthly[m] for m in aligned},
        drawdowns=[
            DrawdownInfo(
                depth_pct=d.depth_pct, peak_date=d.peak_date, trough_date=d.trough_date,
                recovery_date=d.recovery_date, length_months=d.length_months,
            )
            for d in dds
        ],
    )


def _build_optimize_response(opt, objective: str, group_of, name_of, meta_of=None) -> OptimizeResponse:
    """Map a PortfolioOptimization (from the optimizer OR the manual backtester)
    to the wire OptimizeResponse. `group_of(label)`/`name_of(label)` resolve the
    per-asset group + display name; `meta_of(label)` → `(benchmark_id, isin)` so
    the result rows carry their underlying benchmark (None for the strategy)."""
    meta_of = meta_of or (lambda _label: (None, None))

    def _stats(s) -> PortfolioStats:
        return PortfolioStats(
            ann_return=s.ann_return, ann_vol=s.ann_vol, sharpe=s.sharpe, sortino=s.sortino,
            median_month=s.median_month, win_rate=s.win_rate,
        )

    def _weight(label: str, w: float) -> AssetWeight:
        bid, isin = meta_of(label)
        return AssetWeight(
            label=label,
            name=None if label == "Strategy" else name_of(label),
            weight=w,
            group=group_of(label),
            benchmark_id=bid,
            isin=isin,
        )

    weights = [_weight(label, w) for label, w in zip(opt.assets, opt.weights)]
    curve = [
        CurvePoint(date=m, before=b, after=a)
        for m, b, a in zip(opt.curve_months, opt.curve_before, opt.curve_after)
    ]

    def _dds(items) -> list[DrawdownInfo]:
        return [
            DrawdownInfo(
                depth_pct=d.depth_pct, peak_date=d.peak_date, trough_date=d.trough_date,
                recovery_date=d.recovery_date, length_months=d.length_months,
            )
            for d in items
        ]

    return OptimizeResponse(
        objective=objective,
        months=opt.months,
        period_from=opt.period_from,
        period_to=opt.period_to,
        limited_by=opt.limited_by,
        weights=weights,
        before=_stats(opt.before),
        after=_stats(opt.after),
        curve=curve,
        drawdowns_before=_dds(opt.drawdowns_before),
        drawdowns_after=_dds(opt.drawdowns_after),
        annual=[
            YearStat(
                year=y.year, return_before=y.return_before, return_after=y.return_after,
                vol_before=y.vol_before, vol_after=y.vol_after,
                months=[
                    MonthStatInfo(month=m.month, return_before=m.return_before, return_after=m.return_after)
                    for m in y.months
                ],
            )
            for y in opt.annual
        ],
        ytd_before=opt.ytd_before,
        ytd_after=opt.ytd_after,
        rebalance_count=opt.rebalance_count,
        rebalance_dates=opt.rebalance_dates,
        rebalance_freq_months=opt.rebalance_freq_months,
    )


@router.post("/api/momentum/diversifier/optimize", response_model=OptimizeResponse)
async def optimize(req: OptimizeRequest):
    """Find the long-only weights across the strategy + the selected ETFs that
    maximize the chosen objective (Sharpe/Sortino), and report the before vs
    after stats plus the full weight breakdown. Runs over the common window
    where every selected ETF has data."""
    result = await asyncio.to_thread(load_backtest_result_sync, req.backtest_run_id)
    if not result:
        raise HTTPException(404, "Backtest result not available (missing run or its result blob).")

    _summary, monthly, ambiguous = _select_scope(result, req.variant_key)
    if ambiguous is not None:
        raise HTTPException(
            400,
            detail={
                "error": "This backtest is a variant bundle; choose a variant.",
                "available_variant_keys": ambiguous,
            },
        )
    strategy_returns = div.monthly_records_to_returns(monthly)
    if len(strategy_returns) < 2:
        raise HTTPException(422, "The selected backtest has fewer than 2 completed months.")

    rf = req.risk_free_rate_pct / 100.0
    # Legacy single weight pins the core; otherwise search the [min, max] range.
    if req.core_weight_pct is not None:
        core_min = core_max = max(0.0, min(req.core_weight_pct, 100.0)) / 100.0
    else:
        core_min = max(0.0, min(req.core_weight_min_pct, 100.0)) / 100.0
        core_max = max(0.0, min(req.core_weight_max_pct, 100.0)) / 100.0

    compare_ids = [req.compare_benchmark_id] if req.compare_benchmark_id is not None else []
    all_ids = list(dict.fromkeys([*req.benchmark_ids, *req.bond_ids, *compare_ids]))
    meta_resp = await asyncio.to_thread(
        lambda: supabase.table("benchmark")
        .select("benchmark_id, ticker, name, isin")
        .in_("benchmark_id", all_ids or [-1])
        .execute()
    )
    meta = {m["benchmark_id"]: m for m in (meta_resp.data or [])}
    ticker_to_meta = {m["ticker"]: m for m in meta.values()}

    ticker_to_name: dict[str, str] = {"Strategy": await _run_name(req.backtest_run_id)}
    bond_tickers: set[str] = set()

    async def _series_for(ids: list[int], is_bond: bool) -> list[tuple[str, dict]]:
        out: list[tuple[str, dict]] = []
        for bid in ids:
            m = meta.get(bid)
            if not m:
                continue
            prices = await _load_benchmark_prices(bid)
            out.append((m["ticker"], div.prices_to_monthly_returns(prices)))
            ticker_to_name[m["ticker"]] = m["name"]
            if is_bond:
                bond_tickers.add(m["ticker"])
        return out

    etf_series = await _series_for(req.benchmark_ids, is_bond=False)
    bond_series = await _series_for(req.bond_ids, is_bond=True)

    opt = div.optimize_portfolio(
        strategy_returns, etf_series, bonds=bond_series, rf_annual=rf, objective=req.objective,
        core_min=core_min, core_max=core_max, search_restarts=req.search_restarts,
    )

    def _group(label: str) -> str:
        if label == "Strategy":
            return "strategy"
        return "bond" if label in bond_tickers else "etf"

    resp = _build_optimize_response(
        opt, req.objective, _group, ticker_to_name.get, _meta_of_from(ticker_to_meta),
    )

    # Optional compare-against benchmark over the SAME common window.
    if req.compare_benchmark_id is not None and (bmeta := meta.get(req.compare_benchmark_id)):
        bench_prices = await _load_benchmark_prices(req.compare_benchmark_id)
        bench_monthly = div.prices_to_monthly_returns(bench_prices)
        common_months = [m.month for y in opt.annual for m in y.months]
        resp.benchmark = _build_benchmark_compare(
            req.compare_benchmark_id, bmeta, bench_monthly, common_months, rf,
        )
    return resp


class SimHolding(BaseModel):
    benchmark_id: int | None = None   # None = the strategy
    weight_pct: float                 # target weight (need not sum to 100 — normalized)
    band_pct: float = 10.0            # rebalance when it drifts ± this from target


class SimulateRequest(BaseModel):
    backtest_run_id: int
    variant_key: str | None = None
    risk_free_rate_pct: float = 0.0
    holdings: list[SimHolding]        # include the strategy (benchmark_id = null)


@router.post("/api/momentum/diversifier/simulate", response_model=OptimizeResponse)
async def simulate(req: SimulateRequest):
    """Backtest a HAND-SPECIFIED portfolio: fixed target weights + a per-holding
    rebalance band (reset all to target when ANY holding drifts outside its
    band). Returns the same shape as /optimize — before (strategy alone) vs
    after (the rebalanced manual portfolio) — over the common window."""
    result = await asyncio.to_thread(load_backtest_result_sync, req.backtest_run_id)
    if not result:
        raise HTTPException(404, "Backtest result not available (missing run or its result blob).")

    _summary, monthly, ambiguous = _select_scope(result, req.variant_key)
    if ambiguous is not None:
        raise HTTPException(
            400,
            detail={"error": "This backtest is a variant bundle; choose a variant.", "available_variant_keys": ambiguous},
        )
    strategy_returns = div.monthly_records_to_returns(monthly)
    if len(strategy_returns) < 2:
        raise HTTPException(422, "The selected backtest has fewer than 2 completed months.")

    rf = req.risk_free_rate_pct / 100.0
    holdings, name_of, ticker_to_meta = await _assemble_holdings(
        strategy_returns, await _run_name(req.backtest_run_id), req.holdings
    )
    opt = div.simulate_portfolio(holdings, rf_annual=rf)
    return _build_optimize_response(
        opt, "manual", lambda lbl: "strategy" if lbl == "Strategy" else "etf", name_of.get,
        _meta_of_from(ticker_to_meta),
    )


def _scheduled_strategy_monthly_returns(
    strategy_id: int,
) -> tuple[dict[str, float], str, str | None, str | None]:
    """A scheduled strategy's LIVE monthly returns (backtest + live tail) + its
    name + inception (go-live) date + as-of (latest priced) date. Reuses the
    exact extended curve behind /api/admin/schedules/{id}/performance, converted
    equity → monthly returns."""
    from routers._schedule_hydration import _extended_curve  # noqa: PLC0415
    from routers.admin import _load_strategy_row, _strategy_snapshots  # noqa: PLC0415

    strat = _load_strategy_row(strategy_id)
    run_id = strat.get("backtest_run_id")
    name = strat.get("name") or f"Strategy #{strategy_id}"
    inception = (
        str(strat["start_date"])[:10] if strat.get("start_date")
        else str(strat.get("created_at") or "")[:10]
    ) or None
    if not run_id:
        return {}, name, inception, None
    curve = _extended_curve(int(run_id), _strategy_snapshots(strategy_id))   # [(date, cum_pct)]
    equity = [(d, 1.0 + c / 100.0) for d, c in curve]                        # equity series
    as_of = curve[-1][0] if curve else None
    return div.prices_to_monthly_returns(equity), name, inception, as_of


async def _assemble_holdings(
    strategy_returns: dict, strategy_name: str, sim_holdings: list,
) -> tuple[list[tuple[str, dict, float, float]], dict[str, str], dict[str, dict]]:
    """Build `[(label, monthly_returns, weight, band_fraction), …]` (strategy
    first) + a label→name map + a ticker→benchmark-row map (for benchmark_id +
    isin) from a list of SimHoldings. Loads each fund's monthly returns from its
    benchmark prices."""
    strat_h = next((h for h in sim_holdings if h.benchmark_id is None), None)
    if strat_h is None:
        raise HTTPException(422, "Include the strategy holding (benchmark_id = null) with a weight.")
    fund_hs = [h for h in sim_holdings if h.benchmark_id is not None]
    meta_resp = await asyncio.to_thread(
        lambda: supabase.table("benchmark")
        .select("benchmark_id, ticker, name, isin")
        .in_("benchmark_id", [h.benchmark_id for h in fund_hs] or [-1])
        .execute()
    )
    meta = {m["benchmark_id"]: m for m in (meta_resp.data or [])}
    name_of: dict[str, str] = {"Strategy": strategy_name}
    ticker_to_meta: dict[str, dict] = {}
    holdings: list[tuple[str, dict, float, float]] = [
        ("Strategy", strategy_returns, strat_h.weight_pct, max(0.0, strat_h.band_pct) / 100.0),
    ]
    for h in fund_hs:
        m = meta.get(h.benchmark_id)
        if not m:
            continue
        prices = await _load_benchmark_prices(h.benchmark_id)
        holdings.append((m["ticker"], div.prices_to_monthly_returns(prices), h.weight_pct, max(0.0, h.band_pct) / 100.0))
        name_of[m["ticker"]] = m["name"]
        ticker_to_meta[m["ticker"]] = m
    return holdings, name_of, ticker_to_meta


# --------------------------------------------------------------------------- #
# Saved "diversified portfolios" — a named overlay (strategy + funds + bands)
# built on top of a base backtest. Save / list / delete + an on-demand "state"
# (current drifted weights + whether a rebalance is due).
# --------------------------------------------------------------------------- #
_PORTFOLIO_TABLE = "diversified_portfolio"


class PortfolioSaveRequest(BaseModel):
    name: str
    # Exactly one base: a saved backtest (on-demand mode) OR a scheduled
    # strategy (live-tracked, shows on /schedule).
    backtest_run_id: int | None = None
    scheduled_strategy_id: int | None = None
    variant_key: str | None = None
    risk_free_rate_pct: float = 0.0
    holdings: list[SimHolding]   # include the strategy (benchmark_id = null)


class SavedPortfolio(BaseModel):
    id: int
    name: str
    backtest_run_id: int | None = None
    scheduled_strategy_id: int | None = None
    variant_key: str | None = None
    risk_free_rate_pct: float = 0.0
    holdings: list[SimHolding]
    created_at: str
    strategy_name: str | None = None   # resolved base name (backtest or strategy)
    scheduled: bool = False            # live-tracked (scheduled_strategy_id set)?


class HoldingStateInfo(BaseModel):
    label: str
    name: str | None = None
    group: str
    target_pct: float
    current_pct: float
    band_pct: float
    breached: bool
    # The sleeve's own compounded return over the since-inception window — so
    # the strategy + each ETF's gain can be eyeballed against its price move.
    return_since_inception_pct: float | None = None


class PortfolioStateResponse(BaseModel):
    id: int
    name: str
    enough_data: bool
    as_of: str | None = None            # latest priced date (e.g. 2026-06-25)
    inception_date: str | None = None   # go-live date the since-inception spans
    last_rebalance: str | None = None
    rebalance_needed: bool
    # Blended-portfolio calendar returns (the same anchoring as a scheduled
    # strategy's performance header).
    mtd_return_pct: float | None = None
    ytd_return_pct: float | None = None
    since_inception_pct: float | None = None
    holdings: list[HoldingStateInfo]    # only ACTUAL holdings (0%-target dropped)
    result: OptimizeResponse   # full backtest card (before/after stats, curve, …)


def _portfolio_row_to_model(row: dict, strategy_name: str | None = None) -> SavedPortfolio:
    return SavedPortfolio(
        id=row["id"],
        name=row["name"],
        backtest_run_id=row.get("backtest_run_id"),
        scheduled_strategy_id=row.get("scheduled_strategy_id"),
        variant_key=row.get("variant_key"),
        risk_free_rate_pct=row.get("risk_free_rate_pct") or 0.0,
        holdings=[SimHolding(**h) for h in (row.get("holdings") or [])],
        created_at=str(row.get("created_at")),
        strategy_name=strategy_name,
        scheduled=row.get("scheduled_strategy_id") is not None,
    )


async def _scheduled_strategy_name(strategy_id: int) -> str | None:
    resp = await asyncio.to_thread(
        lambda: supabase.table("scheduled_strategy").select("name").eq("id", strategy_id).limit(1).execute()
    )
    return (resp.data or [{}])[0].get("name")


async def _portfolio_strategy_returns(p: dict) -> tuple[dict, str, str | None, str | None]:
    """A saved portfolio's strategy monthly returns + display name + inception
    (go-live) + as-of date — from its scheduled strategy (LIVE) when set, else
    its base backtest. For backtest mode inception is None (since-inception spans
    the whole curve) and as-of is the last month."""
    sid = p.get("scheduled_strategy_id")
    if sid is not None:
        returns, name, inception, as_of = await asyncio.to_thread(
            _scheduled_strategy_monthly_returns, int(sid)
        )
        if len(returns) < 2:
            raise HTTPException(422, "The base scheduled strategy has too little live history yet.")
        return returns, name, inception, as_of
    rid = p.get("backtest_run_id")
    if not rid:
        raise HTTPException(422, "Portfolio has no base strategy.")
    blob = await asyncio.to_thread(load_backtest_result_sync, int(rid))
    if not blob:
        raise HTTPException(404, "Base backtest result not available (missing run or blob).")
    _summary, monthly, ambiguous = _select_scope(blob, p.get("variant_key"))
    if ambiguous is not None:
        raise HTTPException(422, "Base backtest is an unresolved variant bundle.")
    returns = div.monthly_records_to_returns(monthly)
    if len(returns) < 2:
        raise HTTPException(422, "Base backtest has fewer than 2 completed months.")
    return returns, await _run_name(int(rid)), None, (max(returns) if returns else None)


@router.post("/api/momentum/diversifier/portfolios", response_model=SavedPortfolio)
async def save_portfolio(req: PortfolioSaveRequest):
    """Persist a named diversified portfolio (overlay over a base backtest or a
    live scheduled strategy)."""
    if not any(h.benchmark_id is None for h in req.holdings):
        raise HTTPException(422, "Include the strategy holding (benchmark_id = null).")
    if (req.backtest_run_id is None) == (req.scheduled_strategy_id is None):
        raise HTTPException(422, "Provide exactly one of backtest_run_id or scheduled_strategy_id.")
    row = {
        "name": req.name.strip() or "Untitled portfolio",
        "backtest_run_id": req.backtest_run_id,
        "scheduled_strategy_id": req.scheduled_strategy_id,
        "variant_key": req.variant_key,
        "risk_free_rate_pct": req.risk_free_rate_pct,
        "holdings": [h.model_dump() for h in req.holdings],
    }
    resp = await asyncio.to_thread(lambda: supabase.table(_PORTFOLIO_TABLE).insert(row).execute())
    if not resp.data:
        raise HTTPException(500, "Failed to save portfolio")
    name = (
        await _scheduled_strategy_name(req.scheduled_strategy_id)
        if req.scheduled_strategy_id is not None
        else await _run_name(req.backtest_run_id)
    )
    return _portfolio_row_to_model(resp.data[0], strategy_name=name)


@router.get("/api/momentum/diversifier/portfolios", response_model=list[SavedPortfolio])
async def list_portfolios(scheduled: bool | None = None):
    """List saved diversified portfolios (newest first). `scheduled=true` →
    only live-tracked ones (a scheduled-strategy base); `false` → only backtest-
    based; omit for all."""
    resp = await asyncio.to_thread(
        lambda: supabase.table(_PORTFOLIO_TABLE).select("*").order("created_at", desc=True).execute()
    )
    rows = resp.data or []
    if scheduled is True:
        rows = [r for r in rows if r.get("scheduled_strategy_id") is not None]
    elif scheduled is False:
        rows = [r for r in rows if r.get("scheduled_strategy_id") is None]

    run_ids = list({r["backtest_run_id"] for r in rows if r.get("backtest_run_id")})
    sched_ids = list({r["scheduled_strategy_id"] for r in rows if r.get("scheduled_strategy_id")})
    bt_names: dict[int, str] = {}
    sc_names: dict[int, str] = {}
    if run_ids:
        nresp = await asyncio.to_thread(
            lambda: supabase.table("backtest_run").select("run_id, name").in_("run_id", run_ids).execute()
        )
        bt_names = {n["run_id"]: n["name"] for n in (nresp.data or [])}
    if sched_ids:
        sresp = await asyncio.to_thread(
            lambda: supabase.table("scheduled_strategy").select("id, name").in_("id", sched_ids).execute()
        )
        sc_names = {n["id"]: n["name"] for n in (sresp.data or [])}

    def _name(r: dict) -> str | None:
        if r.get("scheduled_strategy_id") is not None:
            return sc_names.get(r["scheduled_strategy_id"])
        return bt_names.get(r.get("backtest_run_id"))

    return [_portfolio_row_to_model(r, strategy_name=_name(r)) for r in rows]


@router.delete("/api/momentum/diversifier/portfolios/{portfolio_id}")
async def delete_portfolio(portfolio_id: int):
    resp = await asyncio.to_thread(
        lambda: supabase.table(_PORTFOLIO_TABLE).delete().eq("id", portfolio_id).execute()
    )
    if not resp.data:
        raise HTTPException(404, "Portfolio not found")
    return {"ok": True}


@router.get(
    "/api/momentum/diversifier/portfolios/{portfolio_id}/state",
    response_model=PortfolioStateResponse,
)
async def portfolio_state(portfolio_id: int):
    """A saved portfolio's CURRENT drifted weights + whether a rebalance is due,
    plus the full backtest card (before/after) over the common window."""
    presp = await asyncio.to_thread(
        lambda: supabase.table(_PORTFOLIO_TABLE).select("*").eq("id", portfolio_id).limit(1).execute()
    )
    if not presp.data:
        raise HTTPException(404, "Portfolio not found")
    p = presp.data[0]

    strategy_returns, strategy_name, inception, as_of = await _portfolio_strategy_returns(p)
    sim_holdings = [SimHolding(**h) for h in (p.get("holdings") or [])]
    holdings, name_of, ticker_to_meta = await _assemble_holdings(strategy_returns, strategy_name, sim_holdings)

    state = div.portfolio_current_state(holdings)
    rf = (p.get("risk_free_rate_pct") or 0.0) / 100.0
    opt = div.simulate_portfolio(holdings, rf_annual=rf)
    result = _build_optimize_response(
        opt, "manual", lambda lbl: "strategy" if lbl == "Strategy" else "etf", name_of.get,
        _meta_of_from(ticker_to_meta),
    )

    # Blended-portfolio calendar returns off the realized (band-rebalanced) curve.
    months = list(opt.curve_months or [])
    after_rets = div.rets_from_cum(list(opt.curve_after or []))
    cal = div.blended_calendar_returns(months, after_rets, inception or "")
    # Each sleeve's own gain over the since-inception window (price-verification).
    comp_ret = {label: div.component_return_since(series, months, inception or "") for label, series, *_ in holdings}

    return PortfolioStateResponse(
        id=p["id"],
        name=p["name"],
        enough_data=state.enough_data,
        as_of=as_of or state.as_of,
        inception_date=inception,
        last_rebalance=state.last_rebalance,
        rebalance_needed=state.rebalance_needed,
        mtd_return_pct=cal.mtd_pct,
        ytd_return_pct=cal.ytd_pct,
        since_inception_pct=cal.since_inception_pct,
        holdings=[
            HoldingStateInfo(
                label=h.label,
                name=None if h.label == "Strategy" else name_of.get(h.label),
                group="strategy" if h.label == "Strategy" else "etf",
                target_pct=h.target * 100.0,
                current_pct=h.current * 100.0,
                band_pct=h.band * 100.0,
                breached=h.breached,
                return_since_inception_pct=comp_ret.get(h.label),
            )
            for h in state.holdings
            if h.target > 1e-9          # only ACTUAL holdings (drop 0%-target sleeves)
        ],
        result=result,
    )


@router.get(
    "/api/momentum/diversifier/strategy-stats/{run_id}",
    response_model=BacktestStats,
)
async def strategy_stats(run_id: int, variant_key: str | None = None):
    """The selected backtest's saved headline stats (Sharpe/Sortino/annualized
    return/max drawdown), so the page can show the baseline as soon as a run
    is picked. For a variant bundle without a chosen variant, returns the
    available variant keys instead so the UI can prompt up front."""
    result = await asyncio.to_thread(load_backtest_result_sync, run_id)
    if not result:
        raise HTTPException(
            404,
            "Backtest result not available (missing run or its result blob).",
        )
    run_name = await _run_name(run_id)
    summary, monthly, ambiguous = _select_scope(result, variant_key)
    if ambiguous is not None:
        return BacktestStats(run_id=run_id, name=run_name, available_variant_keys=ambiguous)

    months = sorted(div.monthly_records_to_returns(monthly))
    return BacktestStats(
        run_id=run_id,
        name=run_name,
        variant_key=variant_key,
        sharpe_ratio=summary.get("sharpe_ratio"),
        sortino_ratio=summary.get("sortino_ratio"),
        annualized_return_pct=summary.get("annualized_return_pct"),
        max_drawdown_pct=summary.get("max_drawdown_pct"),
        total_return_pct=summary.get("total_return_pct"),
        months=len(months),
        period_from=months[0] if months else None,
        period_to=months[-1] if months else None,
    )


# --------------------------------------------------------------------------- #
# Schedule a backtest (vanilla or + ETF overlay) as a NEW standalone scheduled
# strategy — it appears in /schedule's "Scheduled strategies" list and is
# rebalanced by the pipeline. A blended variant carries an `etf_overlay` on its
# config + is seeded from a blended `backtest_run`, so the ETFs show as real
# holdings (with weights) and the backtested Sharpe/Sortino/curve reflect them.
# --------------------------------------------------------------------------- #
class ScheduleAsStrategyRequest(BaseModel):
    name: str
    backtest_run_id: int
    variant_key: str | None = None
    frequency: str
    risk_free_rate_pct: float = 0.0
    # The diversifier's holdings list: the strategy sleeve (benchmark_id = null)
    # + each ETF. Weights are normalized so strategy + ETFs sum to 100%.
    holdings: list[SimHolding]
    start_date: date | None = None


async def _load_run_config(run_id: int) -> dict | None:
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .select("config")
        .eq("run_id", run_id)
        .limit(1)
        .execute()
    )
    if not resp.data:
        return None
    return resp.data[0].get("config") or {}


@router.post("/api/momentum/diversifier/schedule-as-strategy")
async def schedule_as_strategy(req: ScheduleAsStrategyRequest):
    """Create a new scheduled strategy from a saved backtest. With no ETF
    holdings it's a plain momentum schedule (identical to /backtest's
    "+ Schedule"); with ETFs it's a BLEND — the momentum sleeve scaled to its
    weight + the ETFs at theirs, reset on each grid rebalance. Rejects variant
    bundles (a scheduled strategy needs a single-variant config)."""
    from routers import scheduled_strategies as sched  # noqa: PLC0415
    from routers._schedule_hydration import _hydrate  # noqa: PLC0415
    from routers.momentum.backtest_crud import (  # noqa: PLC0415
        SaveBacktestRequest, save_backtest,
    )

    if req.frequency not in sched.FREQUENCIES:
        raise HTTPException(
            400, f"Unknown frequency {req.frequency!r}; expected one of {list(sched.FREQUENCIES)}"
        )
    if not req.name.strip():
        raise HTTPException(400, "name must be non-empty")
    strat_h = next((h for h in req.holdings if h.benchmark_id is None), None)
    if strat_h is None:
        raise HTTPException(422, "Include the strategy holding (benchmark_id = null) with a weight.")

    source_config = await _load_run_config(req.backtest_run_id)
    if source_config is None:
        raise HTTPException(404, "Backtest run not found.")
    result = await asyncio.to_thread(load_backtest_result_sync, req.backtest_run_id)
    if not result:
        raise HTTPException(404, "Backtest result not available (missing run or its result blob).")
    if result.get("variants") or result.get("kind") == "variants":
        raise HTTPException(
            400,
            "This backtest is a variant bundle. Schedule a single-variant backtest first "
            "(save one variant from /backtest), then diversify + schedule it.",
        )

    etf_hs = [h for h in req.holdings if h.benchmark_id is not None and (h.weight_pct or 0) > 0]
    base_config = {k: v for k, v in source_config.items() if k not in ("variants", "n_trials")}

    # ── Vanilla (no ETFs): schedule the existing backtest run directly. ──
    if not etf_hs:
        def _create_vanilla() -> dict:
            row = sched.create_scheduled_strategy_row(
                req.name, req.frequency, base_config, req.backtest_run_id, req.start_date,
            )
            return _hydrate([row])[0]
        return await asyncio.to_thread(_create_vanilla)

    # ── Blended: build + save a blended backtest_run, then schedule it. ──
    from momentum import blend_backtest as bb  # noqa: PLC0415

    bids = [int(h.benchmark_id) for h in etf_hs]
    meta_resp = await asyncio.to_thread(
        lambda: supabase.table("benchmark")
        .select("benchmark_id, ticker, name, sector, currency")
        .in_("benchmark_id", bids)
        .execute()
    )
    meta = {m["benchmark_id"]: m for m in (meta_resp.data or [])}

    # Normalize so the strategy sleeve + ETFs sum to 100%.
    total_pct = float(strat_h.weight_pct) + sum(float(h.weight_pct) for h in etf_hs)
    if total_pct <= 0:
        raise HTTPException(422, "Total weight must be positive.")
    strat_w = float(strat_h.weight_pct) / total_pct

    overlay_etfs: list[bb.OverlayEtf] = []
    overlay_config: list[dict] = []
    for h in etf_hs:
        bid = int(h.benchmark_id)
        m = meta.get(bid) or {}
        prices = await _load_benchmark_prices(bid)
        norm_pct = float(h.weight_pct) / total_pct * 100.0
        overlay_etfs.append(bb.OverlayEtf(
            benchmark_id=bid,
            ticker=m.get("ticker") or f"BM{bid}",
            name=m.get("name") or m.get("ticker") or f"Benchmark {bid}",
            sector=m.get("sector"),
            weight=norm_pct / 100.0,
            band=max(0.0, float(h.band_pct or 0.0)) / 100.0,
            prices=prices,
            currency=m.get("currency"),
        ))
        overlay_config.append({
            "benchmark_id": bid,
            "weight_pct": round(norm_pct, 6),
            "band_pct": float(h.band_pct or 0.0),
        })

    rf = req.risk_free_rate_pct / 100.0
    blended = await asyncio.to_thread(
        bb.build_blended_result, result, overlay_etfs, strat_w, rf
    )
    if not blended.get("monthly_records"):
        raise HTTPException(
            422,
            "No overlapping history between the strategy and the selected ETFs — "
            "the blend has no periods to backtest.",
        )

    blend_name = f"{req.name.strip()} (blend)"
    saved = await save_backtest(SaveBacktestRequest(
        name=blend_name,
        config={**base_config, "etf_overlay": overlay_config},
        summary=blended["summary"],
        monthly_records=blended["monthly_records"],
        daily_records=blended["daily_records"],
        universe_daily_records=blended.get("universe_daily_records") or [],
        universe=blended.get("universe") or [],
    ))
    blended_run_id = saved.get("run_id")
    if not blended_run_id:
        raise HTTPException(500, "Failed to save the blended backtest.")

    def _create_blended() -> dict:
        row = sched.create_scheduled_strategy_row(
            req.name, req.frequency,
            {**base_config, "etf_overlay": overlay_config},
            int(blended_run_id), req.start_date,
        )
        return _hydrate([row])[0]
    return await asyncio.to_thread(_create_blended)
