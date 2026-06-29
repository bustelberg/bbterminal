"""Blend a saved momentum backtest with a fixed-weight ETF overlay into a
new BacktestResult-shaped blob — the seed a "diversified variant" is
scheduled from (see `routers/diversifier.py::schedule_as_strategy`).

The blend semantics match the live scheduled pipeline exactly: at each
momentum rebalance the whole book RESETS to target (the momentum sleeve at
`strategy_weight`, each ETF at its weight). Weights then drift within the
period; the ±band is only a "rebalance due" alert on /schedule, never an
off-grid trade. So the blended period return is simply the target-weighted
sum of the sleeve's own period return and each ETF's period return over the
same entry→exit window.

ETF holdings reuse the engine-wide convention `company_id = -benchmark_id`
(the negative id sector-ETF mode already uses), so the existing /schedule
holdings table + live pricing paths render/price them with no UI change.

Headline summary stats (Sharpe/Sortino/annualized return/…) are computed
from the blended MONTHLY return series via `diversification.annualized_stats`
— the SAME math the Diversifier page already shows — so a scheduled variant's
numbers equal what the user saw when they built the blend.

Pure module: the caller (`routers/diversifier.py`) does the DB loads and
hands in the source result blob + each ETF's `(target_date, price)` series.
"""
from __future__ import annotations

import bisect
from dataclasses import dataclass

from . import diversification as div


@dataclass
class OverlayEtf:
    """One ETF in the overlay, with its full price history."""
    benchmark_id: int
    ticker: str
    name: str
    sector: str | None
    weight: float                      # target weight as a fraction (0..1)
    band: float                        # rebalance band as a fraction (0..1)
    prices: list[tuple[str, float]]    # [(YYYY-MM-DD, price), …] ascending
    currency: str | None = None        # native trading currency (display only)


def _asof_price(dates: list[str], prices: list[float], day: str) -> float | None:
    """Last price on-or-before `day` (ISO). None when `day` predates the series."""
    if not dates:
        return None
    i = bisect.bisect_right(dates, day) - 1
    return prices[i] if i >= 0 else None


def _period_bounds(rec: dict, next_rec: dict | None) -> tuple[str | None, str | None]:
    """A source period's (entry_date, exit_date) as ISO days, derived from its
    holdings. Entry = earliest holding entry_date; exit = earliest holding
    exit_date (closed) or the period's `as_of_date` (open). Falls back to the
    next period's entry for the exit when holdings lack exit dates."""
    holdings = rec.get("holdings") or []
    entries = [str(h["entry_date"])[:10] for h in holdings if h.get("entry_date")]
    exits = [str(h["exit_date"])[:10] for h in holdings if h.get("exit_date")]
    entry = min(entries) if entries else (str(rec.get("date"))[:10] if rec.get("date") else None)
    exit_d = min(exits) if exits else None
    if exit_d is None:
        if rec.get("as_of_date"):
            exit_d = str(rec["as_of_date"])[:10]
        elif next_rec is not None:
            n_entries = [str(h["entry_date"])[:10] for h in (next_rec.get("holdings") or []) if h.get("entry_date")]
            exit_d = min(n_entries) if n_entries else None
    return entry, exit_d


def _etf_holding(etf: OverlayEtf, entry_price: float | None, exit_price: float | None,
                 entry_date: str | None, exit_date: str | None) -> dict:
    """An ETF holding dict in the engine's PeriodHolding shape, with the
    NEGATIVE-company_id convention so it can't collide with a real company.

    The EUR price fields are populated 1:1 with the local price ONLY when the
    benchmark trades in EUR (or its currency is unknown — the prior
    treat-as-EUR behaviour). For a known foreign currency they're left null so
    the /schedule EUR + FX columns honestly read "—" rather than implying a
    1.00 rate we haven't actually applied (a real FX pass is a follow-up)."""
    ret = (
        (exit_price / entry_price - 1.0) * 100.0
        if entry_price and exit_price and entry_price > 0
        else None
    )
    ccy = (etf.currency or "").strip().upper() or None
    eur_ok = ccy in (None, "EUR")
    return {
        "company_id": -etf.benchmark_id,
        "ticker": etf.ticker,
        "company_name": etf.name,
        "sector": etf.sector or "ETF",
        "score": 0.0,
        "category_scores": {},
        "weight": round(etf.weight, 4),
        "forward_return_pct": ret,
        "currency": ccy,
        "entry_price_local": entry_price,
        "exit_price_local": exit_price,
        "entry_price_eur": entry_price if eur_ok else None,
        "exit_price_eur": exit_price if eur_ok else None,
        "entry_date": entry_date,
        "exit_date": exit_date,
        "side": "long",
        "sector_rank": None,
        "company_rank": None,
    }


def make_etf_holding(
    benchmark_id: int, ticker: str, name: str, sector: str | None, weight: float,
    entry_price: float | None, exit_price: float | None,
    entry_date: str | None, exit_date: str | None, currency: str | None = None,
) -> dict:
    """Public builder for one ETF holding dict (negative-company_id shape).
    Used by the live rebalance path in `ingest/phases/momentum.py`."""
    e = OverlayEtf(
        benchmark_id=benchmark_id, ticker=ticker, name=name, sector=sector,
        weight=weight, band=0.0, prices=[], currency=currency,
    )
    return _etf_holding(e, entry_price, exit_price, entry_date, exit_date)


def scale_stock_weights(holdings: list[dict], strategy_weight: float) -> list[dict]:
    """Copy of the momentum holdings with each `weight` scaled to the
    strategy sleeve's share of the book (so the blend sums to ~1)."""
    out: list[dict] = []
    for h in holdings:
        nh = dict(h)
        nh["weight"] = round(float(h.get("weight") or 0.0) * strategy_weight, 6)
        out.append(nh)
    return out


def assemble_blended_rebalance_holdings(
    stock_holdings: list[dict],
    etfs: list[OverlayEtf],
    entry_prices: dict[int, float | None],
    as_of: str,
    strategy_weight: float,
) -> list[dict]:
    """Holdings for a LIVE blended rebalance: the momentum stock picks scaled
    to `strategy_weight`, plus one freshly-entered ETF holding per overlay ETF
    (entered at `as_of`, no exit yet — the price-update job fills exit/return).
    `entry_prices` maps benchmark_id → latest close on-or-before `as_of`."""
    out = scale_stock_weights(stock_holdings, strategy_weight)
    for e in etfs:
        out.append(_etf_holding(e, entry_prices.get(e.benchmark_id), None, as_of, None))
    return out


def build_blended_result(
    source_result: dict,
    etfs: list[OverlayEtf],
    strategy_weight: float,
    rf_annual: float = 0.0,
) -> dict:
    """Build a BacktestResult-shaped blob blending `source_result` (a saved
    momentum backtest) with the `etfs` overlay at `strategy_weight` (the
    momentum sleeve's fraction; ETFs take the rest).

    The blended window starts at the first source period in which EVERY ETF
    already has price history (mirrors the Diversifier's `limited_by`).
    """
    src_records = source_result.get("monthly_records") or []
    # Pre-index each ETF's price series for as-of lookups.
    indexed = [
        (e, [d for d, _ in e.prices], [p for _, p in e.prices]) for e in etfs
    ]

    blended_records: list[dict] = []
    cum = 1.0
    holdings_counts: list[int] = []
    for i, rec in enumerate(src_records):
        nxt = src_records[i + 1] if i + 1 < len(src_records) else None
        entry_d, exit_d = _period_bounds(rec, nxt)
        strat_ret = rec.get("portfolio_return_pct")

        # Require every ETF to have a price at the period entry — otherwise this
        # period predates an ETF's history and the blend isn't defined yet.
        etf_entry_prices: list[float | None] = [
            _asof_price(ds, ps, entry_d) if entry_d else None for _e, ds, ps in indexed
        ]
        if etfs and any(p is None for p in etf_entry_prices):
            continue  # before the common window — skip
        if not blended_records and strat_ret is None:
            continue  # don't open the curve on an empty period

        etf_holdings: list[dict] = []
        etf_ret_contrib = 0.0
        for (e, ds, ps), entry_price in zip(indexed, etf_entry_prices):
            exit_price = _asof_price(ds, ps, exit_d) if exit_d else entry_price
            h = _etf_holding(e, entry_price, exit_price, entry_d, exit_d)
            etf_holdings.append(h)
            if h["forward_return_pct"] is not None:
                etf_ret_contrib += e.weight * (h["forward_return_pct"] / 100.0)

        # Grid-reset blended period return: strategy sleeve + ETF sleeves at
        # their target weights (reset to target this period).
        blended_ret_pct: float | None
        if strat_ret is None:
            blended_ret_pct = None
        else:
            blended_ret_pct = (strategy_weight * (strat_ret / 100.0) + etf_ret_contrib) * 100.0
            cum *= 1.0 + blended_ret_pct / 100.0

        merged_holdings = scale_stock_weights(rec.get("holdings") or [], strategy_weight) + etf_holdings
        holdings_counts.append(len(merged_holdings))
        out_rec: dict = {
            "date": rec.get("date"),
            "holdings": merged_holdings,
            "portfolio_return_pct": blended_ret_pct,
            "cumulative_return_pct": round((cum - 1.0) * 100.0, 6),
        }
        if rec.get("is_open"):
            out_rec["is_open"] = True
        if rec.get("as_of_date"):
            out_rec["as_of_date"] = rec["as_of_date"]
        blended_records.append(out_rec)

    # Headline stats from the blended MONTHLY return series (same math the
    # Diversifier shows). Built off the chained cumulative so weekly/daily
    # source cadences collapse to month-end the same way.
    monthly = div.monthly_records_to_returns(blended_records)
    months_sorted = sorted(monthly)
    rets = [monthly[m] for m in months_sorted]
    stats = div.annualized_stats(rets, rf_annual)
    dds = div.top_drawdowns(months_sorted, rets, 40)
    max_dd = min((d.depth_pct for d in dds), default=0.0)
    total_ret = blended_records[-1]["cumulative_return_pct"] if blended_records else 0.0
    avg_holdings = (sum(holdings_counts) / len(holdings_counts)) if holdings_counts else 0.0

    summary = {
        "total_return_pct": total_ret,
        "annualized_return_pct": (stats.ann_return * 100.0) if stats.ann_return is not None else 0.0,
        "max_drawdown_pct": max_dd,
        "sharpe_ratio": stats.sharpe,
        "sortino_ratio": stats.sortino,
        "win_rate_pct": (stats.win_rate * 100.0) if stats.win_rate is not None else None,
        "median_period_return_pct": (stats.median_month * 100.0) if stats.median_month is not None else None,
        "avg_monthly_turnover_pct": 0.0,
        "total_months": len(months_sorted),
        "avg_holdings": round(avg_holdings, 2),
        "universe_total_return_pct": None,
        "universe_annualized_return_pct": None,
        "top_drawdowns": [
            {
                "drawdown_pct": d.depth_pct,
                "peak_date": d.peak_date,
                "trough_date": d.trough_date,
                "recovery_date": d.recovery_date,
            }
            for d in dds
        ],
        "n_trials": None,
    }

    daily_records = _blended_daily_curve(source_result, blended_records, indexed, strategy_weight)

    return {
        "monthly_records": blended_records,
        "summary": summary,
        "daily_records": daily_records,
        "universe_daily_records": [],
        "universe": source_result.get("universe") or [],
    }


def _blended_daily_curve(
    source_result: dict,
    blended_records: list[dict],
    indexed: list[tuple[OverlayEtf, list[str], list[float]]],
    strategy_weight: float,
) -> list[dict]:
    """A dense daily blended equity curve. Within each blended period the
    portfolio grows as `strategy_weight·(strat intra-period growth) +
    Σ etf_weight·(etf intra-period growth)`, chained across periods (grid
    reset at each period boundary). Falls back to the per-period cumulative
    points when the source has no daily curve.

    Returns `[{date, cumulative_return_pct}, …]`."""
    src_daily = source_result.get("daily_records") or []
    # Map source daily curve → strategy growth factor by date.
    sd_dates: list[str] = []
    sd_growth: list[float] = []
    for d in src_daily:
        dt = str(d.get("date") or "")[:10]
        cum = d.get("cumulative_return_pct")
        if dt and cum is not None:
            sd_dates.append(dt)
            sd_growth.append(1.0 + float(cum) / 100.0)

    if not sd_dates or not blended_records:
        # No daily source curve — fall back to period-end points.
        return [
            {"date": (r.get("as_of_date") or r.get("date")), "cumulative_return_pct": r["cumulative_return_pct"]}
            for r in blended_records
            if (r.get("as_of_date") or r.get("date"))
        ]

    def _sd_at(day: str) -> float | None:
        i = bisect.bisect_right(sd_dates, day) - 1
        return sd_growth[i] if i >= 0 else None

    curve: list[dict] = []
    base_equity = 1.0
    for r in blended_records:
        entry_d, exit_d = _period_bounds(r, None)
        # The period's window in the daily series. Closed period exit comes from
        # the next record's entry; recompute bounds against the blended chain.
        if entry_d is None:
            continue
        # Strategy growth anchor at entry.
        strat_entry = _sd_at(entry_d)
        etf_entries = [(_e, ds, ps, _asof_price(ds, ps, entry_d)) for _e, ds, ps in indexed]
        # Walk every source daily date inside [entry, exit] for this period.
        lo = bisect.bisect_left(sd_dates, entry_d)
        hi = bisect.bisect_right(sd_dates, exit_d) if exit_d else len(sd_dates)
        period_end_equity = base_equity
        for k in range(lo, hi):
            day = sd_dates[k]
            strat_growth = (sd_growth[k] / strat_entry) if strat_entry else 1.0
            port_growth = strategy_weight * strat_growth
            for e, ds, ps, ep in etf_entries:
                cur = _asof_price(ds, ps, day)
                g = (cur / ep) if (ep and cur and ep > 0) else 1.0
                port_growth += e.weight * g
            eq = base_equity * port_growth
            curve.append({"date": day, "cumulative_return_pct": round((eq - 1.0) * 100.0, 6)})
            period_end_equity = eq
        base_equity = period_end_equity
    return curve
