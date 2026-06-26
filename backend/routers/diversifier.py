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
    benchmark_ids: list[int]
    variant_key: str | None = None
    risk_free_rate_pct: float = 0.0
    objective: str = "sharpe"
    # Cap on the TOTAL ETF sleeve (strategy keeps the rest). 100 = unconstrained.
    max_total_etf_weight_pct: float = 50.0


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
    cap = max(0.0, min(req.max_total_etf_weight_pct, 100.0)) / 100.0

    meta_resp = await asyncio.to_thread(
        lambda: supabase.table("benchmark")
        .select("benchmark_id, ticker, name")
        .in_("benchmark_id", req.benchmark_ids or [-1])
        .execute()
    )
    meta = {m["benchmark_id"]: m for m in (meta_resp.data or [])}

    # Preserve the caller's selection order; label series by ticker.
    etf_series: list[tuple[str, dict]] = []
    ticker_to_name: dict[str, str] = {"Strategy": await _run_name(req.backtest_run_id)}
    for bid in req.benchmark_ids:
        m = meta.get(bid)
        if not m:
            continue
        prices = await _load_benchmark_prices(bid)
        etf_series.append((m["ticker"], div.prices_to_monthly_returns(prices)))
        ticker_to_name[m["ticker"]] = m["name"]

    opt = div.optimize_portfolio(
        strategy_returns, etf_series, rf_annual=rf, objective=req.objective, max_total_etf=cap,
    )

    weights = [
        AssetWeight(
            label=label,
            name=None if label == "Strategy" else ticker_to_name.get(label),
            weight=w,
        )
        for label, w in zip(opt.assets, opt.weights)
    ]

    def _stats(s) -> PortfolioStats:
        return PortfolioStats(
            ann_return=s.ann_return, ann_vol=s.ann_vol, sharpe=s.sharpe, sortino=s.sortino,
            median_month=s.median_month, win_rate=s.win_rate,
        )

    curve = [
        CurvePoint(date=m, before=b, after=a)
        for m, b, a in zip(opt.curve_months, opt.curve_before, opt.curve_after)
    ]

    def _dds(items) -> list[DrawdownInfo]:
        return [
            DrawdownInfo(
                depth_pct=d.depth_pct,
                peak_date=d.peak_date,
                trough_date=d.trough_date,
                recovery_date=d.recovery_date,
                length_months=d.length_months,
            )
            for d in items
        ]

    return OptimizeResponse(
        objective=req.objective,
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
                year=y.year,
                return_before=y.return_before,
                return_after=y.return_after,
                vol_before=y.vol_before,
                vol_after=y.vol_after,
                months=[
                    MonthStatInfo(month=m.month, return_before=m.return_before, return_after=m.return_after)
                    for m in y.months
                ],
            )
            for y in opt.annual
        ],
        ytd_before=opt.ytd_before,
        ytd_after=opt.ytd_after,
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
