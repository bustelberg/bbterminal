"""Daily sector-momentum timeline over the Yahoo Finance price archive.

This is intentionally a research surface, not a second live strategy. The
scheduled momentum book is priced from GuruFocus; this endpoint uses the asset
pipeline's Yahoo close and volume series. It reuses the live engine's daily
signals, strict ``<`` as-of rule, cross-sectional normalization and mean sector
aggregation.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
import threading
import time
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from asset_pipeline.alphalab import _load_close_volume, _panel, load_panel
from deps import supabase
from momentum.scoring import DEFAULT_SCORE_NORMALIZATION, _normalize, score_universe, sector_pool_scores
from momentum.explain import explain_all_signals
from momentum.signals import PRICE_SIGNAL_DEFS
from signal_engine.daily import evaluate_panel

router = APIRouter(tags=["momentum"])

_CACHE_TTL_SECONDS = 300.0
_CALCULATION_VERSION = 3  # fixed LEONTEQ MomentumTopSelectie universe
_cache: dict[tuple[int, int, int], tuple[float, dict]] = {}
_cache_lock = threading.Lock()
_MOMENTUM_TOP_UNIVERSE = "LEONTEQ (as of 2026-06-17)"


def _momentum_top_analysis_ids() -> tuple[list[int], dict, dict[int, str | None]]:
    """Yahoo ids and LEONTEQ's own sector labels for MomentumTopSelectie."""
    universe = (supabase.table("universe").select("universe_id,label")
                .eq("label", _MOMENTUM_TOP_UNIVERSE).limit(1).execute().data) or []
    if not universe:
        raise HTTPException(status_code=503, detail=f"Universe {_MOMENTUM_TOP_UNIVERSE!r} is unavailable")
    universe_id = int(universe[0]["universe_id"])
    latest = (supabase.table("universe_membership").select("target_month")
              .eq("universe_id", universe_id).order("target_month", desc=True).limit(1).execute().data) or []
    if not latest:
        return [], {"name": _MOMENTUM_TOP_UNIVERSE, "size": 0, "mapped": 0}, {}
    members = (supabase.table("universe_membership").select("company_id,sector")
               .eq("universe_id", universe_id).eq("target_month", latest[0]["target_month"]).execute().data) or []
    company_ids = [int(row["company_id"]) for row in members]
    sector_by_company = {int(row["company_id"]): row.get("sector") for row in members}
    analysis_ids: set[int] = set()
    sectors: dict[int, str | None] = {}
    for start in range(0, len(company_ids), 200):
        rows = (supabase.table("asset_grid").select("company_id,analysis_id")
                .in_("company_id", company_ids[start:start + 200]).execute().data) or []
        for row in rows:
            if row.get("analysis_id") is None:
                continue
            analysis_id = int(row["analysis_id"])
            analysis_ids.add(analysis_id)
            sectors.setdefault(analysis_id, sector_by_company.get(int(row["company_id"])))
    return sorted(analysis_ids), {
        "name": _MOMENTUM_TOP_UNIVERSE, "size": len(company_ids), "mapped": len(analysis_ids),
        "as_of": "2026-06-17",
    }, sectors


def _volume_index(analysis_ids: list[int], since: str) -> dict[int, pd.Series] | None:
    """Yahoo volume series keyed like the close panel, from the same COPY path."""
    raw = _load_close_volume(analysis_ids, since=since)
    if raw is None:
        return None
    panel = _panel(raw, "volume")
    return {
        analysis_id: panel[analysis_id].dropna().astype("float64")
        for analysis_id in analysis_ids
        if analysis_id in panel.columns and panel[analysis_id].notna().any()
    }


def build_sector_timeline(
    panel: pd.DataFrame, secmap: dict[int, str | None], dates: list[pd.Timestamp],
    volume_index: dict[int, pd.Series],
) -> list[dict]:
    """Score each daily as-of cross-section and return its sector ranking.

    Kept separate from the HTTP handler so the price-source boundary and the
    strategy-parity arithmetic are easy to test without a database.
    """
    ids = [int(i) for i in panel.columns if secmap.get(int(i))]
    if not ids or not dates:
        return []

    price_index = {
        aid: panel[aid].dropna().astype("float64")
        for aid in ids
        if panel[aid].notna().any()
    }
    # The evaluator's strict `< cutoff means the value displayed for a trading
    # date is exactly the information available before entering at that close.
    # The evaluator is strict `< cutoff`. Move each cutoff one calendar day
    # forward so a rank labelled 2026-10-01 is calculated *from* the 1 Oct
    # close, rather than from information available before that close.
    evaluation_dates = [day + pd.Timedelta(days=1) for day in dates]
    raw = evaluate_panel(
        list(price_index), [d.date() for d in evaluation_dates], price_index=price_index,
        volume_index=volume_index, id_col="analysis_id",
    )
    weights = {s["key"]: s["default_weight"] for s in PRICE_SIGNAL_DEFS}
    rows: list[dict] = []
    for display_date, cutoff in zip(dates, evaluation_dates):
        values = raw.get(cutoff, [])
        if not values:
            continue
        signals = pd.DataFrame(values)
        signals["sector"] = signals["analysis_id"].map(secmap)
        signals = signals.dropna(subset=["sector"])
        if signals.empty:
            continue
        scored = score_universe(signals, weights, signal_defs=PRICE_SIGNAL_DEFS)
        for sector in sector_pool_scores(scored):
            rows.append({
                "date": display_date.date().isoformat(),
                "sector": sector["sector"],
                "rank": sector["rank"],
                "score": sector["momentum_score"],
                "companies": sector["companies"],
            })
    return rows


def _instrument_labels(analysis_ids: list[int]) -> dict[int, dict[str, str | None]]:
    """Most-liquid execution's display identity for a set of analysis ids."""
    analysis_ids = list(dict.fromkeys(int(analysis_id) for analysis_id in analysis_ids))
    best: dict[int, tuple[float, dict[str, str | None]]] = {}
    for start in range(0, len(analysis_ids), 200):
        rows = (supabase.table("asset_grid")
                .select("analysis_id,name,yahoo_symbol,med_adv_eur")
                .in_("analysis_id", analysis_ids[start:start + 200]).execute().data) or []
        for row in rows:
            analysis_id = int(row["analysis_id"])
            adv = float(row.get("med_adv_eur") or 0)
            if analysis_id not in best or adv > best[analysis_id][0]:
                best[analysis_id] = (adv, {
                    "name": row.get("name"), "ticker": row.get("yahoo_symbol"),
                })

    labels = {analysis_id: label for analysis_id, (_, label) in best.items()}
    # Some scored assets are not represented in asset_grid.  The analysis
    # symbol is still authoritative and prevents unexplained "Unknown company"
    # extrema in the normalization trace.
    missing_ids = [
        analysis_id for analysis_id in analysis_ids
        if not (labels.get(analysis_id, {}).get("name") or labels.get(analysis_id, {}).get("ticker"))
    ]
    for start in range(0, len(missing_ids), 200):
        rows = (supabase.table("asset_analysis")
                .select("analysis_id,symbol")
                .in_("analysis_id", missing_ids[start:start + 200]).execute().data) or []
        for row in rows:
            analysis_id = int(row["analysis_id"])
            symbol = row.get("symbol")
            if symbol:
                labels[analysis_id] = {"name": symbol, "ticker": symbol}
    return labels


def _price_return_legs(
    series: pd.Series, signal_key: str, strict_cutoff: pd.Timestamp,
) -> dict[str, str | float] | None:
    """Return the actual close-date inputs behind a price-return signal."""
    history = series[series.index < strict_cutoff].dropna()
    if history.empty:
        return None

    end_date, end_price = history.index[-1], history.iloc[-1]
    if signal_key == "mom_12_1":
        start_rows = history[history.index <= end_date - pd.DateOffset(months=12)]
        return_rows = history[history.index <= end_date - pd.DateOffset(months=1)]
        if start_rows.empty or return_rows.empty:
            return None
        start_date, start_price = start_rows.index[-1], start_rows.iloc[-1]
        return_date, return_price = return_rows.index[-1], return_rows.iloc[-1]
    elif signal_key in {"mom_6m", "volatility_adjusted_return_6m"}:
        start_rows = history[history.index <= end_date - pd.DateOffset(months=6)]
        if start_rows.empty:
            return None
        start_date, start_price = start_rows.index[-1], start_rows.iloc[-1]
        return_date, return_price = end_date, end_price
    elif signal_key == "drawdown_from_recent_high_pct":
        window = history.tail(252)
        start_date, start_price = window.idxmax(), window.max()
        return_date, return_price = end_date, end_price
    else:
        return None

    if not start_price:
        return None
    return {
        "start_date": start_date.date().isoformat(), "start_price": round(float(start_price), 4),
        "end_date": return_date.date().isoformat(), "end_price": round(float(return_price), 4),
        "return_pct": round((float(return_price) / float(start_price) - 1) * 100, 2),
    }


@router.get("/api/momentum/sector-timeline/detail")
def get_sector_timeline_detail(
    date_: str = Query(alias="date"),
    sector: str = Query(min_length=1),
    max_assets: int = Query(600, ge=100, le=1200),
):
    """Explain one daily sector rank, including every company in its mean."""
    try:
        cutoff = pd.Timestamp(date_).normalize()
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="date must be YYYY-MM-DD") from exc

    # ``load_panel`` gives this one-day calculation the same 1,100-day warm-up
    # as the timeline, so its signals and strict as-of convention agree exactly.
    analysis_ids, _, leonteq_sectors = _momentum_top_analysis_ids()
    panel, _, _ = load_panel(
        analysis_ids=analysis_ids, start=cutoff.date().isoformat(),
    )
    secmap = leonteq_sectors
    if panel is None:
        raise HTTPException(status_code=503, detail="Fast price loader unavailable")
    if panel.empty or cutoff not in panel.index:
        raise HTTPException(status_code=404, detail="No price data for this date")

    ids = [int(i) for i in panel.columns if secmap.get(int(i))]
    price_index = {
        analysis_id: panel[analysis_id].dropna().astype("float64")
        for analysis_id in ids if panel[analysis_id].notna().any()
    }
    volume_index = _volume_index(
        ids, (cutoff - pd.Timedelta(days=90)).date().isoformat(),
    )
    if volume_index is None:
        raise HTTPException(status_code=503, detail="Yahoo volume loader unavailable")
    raw = evaluate_panel(
        list(price_index), [(cutoff + pd.Timedelta(days=1)).date()],
        price_index=price_index, volume_index=volume_index, id_col="analysis_id",
    )
    values = next((rows for day, rows in raw.items()
                   if day.date() == (cutoff + pd.Timedelta(days=1)).date()), [])
    if not values:
        raise HTTPException(status_code=404, detail="Not enough history to calculate this date")

    signals = pd.DataFrame(values)
    signals["sector"] = signals["analysis_id"].map(secmap)
    scored = score_universe(
        signals.dropna(subset=["sector"]),
        {signal["key"]: signal["default_weight"] for signal in PRICE_SIGNAL_DEFS},
        signal_defs=PRICE_SIGNAL_DEFS,
    )
    sector_scores = sector_pool_scores(scored)
    summary = next((row for row in sector_scores if row["sector"] == sector), None)
    if summary is None:
        raise HTTPException(status_code=404, detail="Sector has no score for this date")

    members = scored[scored["sector"] == sector].sort_values("momentum_score", ascending=False)
    signal_keys = [signal["key"] for signal in PRICE_SIGNAL_DEFS]
    normalized_signals = {
        key: _normalize(scored[key], DEFAULT_SCORE_NORMALIZATION).fillna(0.5) * 100
        for key in signal_keys if key in scored.columns
    }
    signal_ranges = {
        key: (scored[key].min(skipna=True), scored[key].max(skipna=True))
        for key in signal_keys if key in scored.columns
    }
    signal_extremes: dict[str, tuple[int, int]] = {}
    for key in signal_keys:
        if key not in scored.columns:
            continue
        valid = scored[["analysis_id", key]].dropna(subset=[key])
        if valid.empty:
            continue
        signal_extremes[key] = (
            int(valid.loc[valid[key].idxmin(), "analysis_id"]),
            int(valid.loc[valid[key].idxmax(), "analysis_id"]),
        )
    labels = _instrument_labels([
        *[int(i) for i in members["analysis_id"].tolist()],
        *(analysis_id for pair in signal_extremes.values() for analysis_id in pair),
    ])

    def company_reference(analysis_id: int) -> dict[str, int | str | None]:
        """A display-safe identity for an extrema row, even if metadata is sparse."""
        label = labels.get(analysis_id, {})
        ticker = label.get("ticker")
        name = label.get("name") or ticker or f"Asset #{analysis_id}"
        return {"analysis_id": analysis_id, "name": name, "ticker": ticker}

    categories = sorted({signal["group"] for signal in PRICE_SIGNAL_DEFS})
    category_weights = {category: round(1 / len(categories), 4) for category in categories}
    strict_cutoff = cutoff + pd.Timedelta(days=1)
    explained_ids = set(int(analysis_id) for analysis_id in members["analysis_id"].tolist())
    explained_ids.update(analysis_id for pair in signal_extremes.values() for analysis_id in pair)
    raw_explanations_by_id = {
        analysis_id: explain_all_signals(
            price_index[analysis_id][price_index[analysis_id].index < strict_cutoff],
            (volume_index[analysis_id][volume_index[analysis_id].index < strict_cutoff]
             if analysis_id in volume_index else None),
        )
        for analysis_id in explained_ids
    }
    companies = []
    for row_index, row in members.iterrows():
        analysis_id = int(row["analysis_id"])
        values = row.to_dict()
        raw_explanations = raw_explanations_by_id[analysis_id]
        companies.append({
            "analysis_id": analysis_id,
            **company_reference(analysis_id),
            "score": round(float(row["momentum_score"]), 2),
            "price_score": round(float(values["score_price"]), 2)
            if "score_price" in values and pd.notna(values["score_price"]) else None,
            "volume_score": round(float(values["score_volume"]), 2)
            if "score_volume" in values and pd.notna(values["score_volume"]) else None,
            "signals": {
                key: round(float(values[key]), 2)
                if key in values and pd.notna(values[key]) else None
                for key in signal_keys
            },
            "signal_details": {
                signal["key"]: {
                    "raw": round(float(values[signal["key"]]), 4)
                    if signal["key"] in values and pd.notna(values[signal["key"]]) else None,
                    "raw_price_legs": _price_return_legs(
                        price_index[analysis_id], signal["key"], cutoff + pd.Timedelta(days=1),
                    ),
                    "raw_explanation": raw_explanations.get(signal["key"]),
                    "normalized": round(float(normalized_signals[signal["key"]].loc[row_index]), 2)
                    if signal["key"] in normalized_signals else None,
                    "universe_min": round(float(signal_ranges[signal["key"]][0]), 4)
                    if signal["key"] in signal_ranges and pd.notna(signal_ranges[signal["key"]][0]) else None,
                    "universe_max": round(float(signal_ranges[signal["key"]][1]), 4)
                    if signal["key"] in signal_ranges and pd.notna(signal_ranges[signal["key"]][1]) else None,
                    "universe_min_company": (
                        company_reference(signal_extremes[signal["key"]][0])
                        if signal["key"] in signal_extremes else None
                    ),
                    "universe_max_company": (
                        company_reference(signal_extremes[signal["key"]][1])
                        if signal["key"] in signal_extremes else None
                    ),
                    "universe_min_price_legs": (
                        _price_return_legs(
                            price_index[signal_extremes[signal["key"]][0]], signal["key"],
                            cutoff + pd.Timedelta(days=1),
                        ) if signal["key"] in signal_extremes else None
                    ),
                    "universe_max_price_legs": (
                        _price_return_legs(
                            price_index[signal_extremes[signal["key"]][1]], signal["key"],
                            cutoff + pd.Timedelta(days=1),
                        ) if signal["key"] in signal_extremes else None
                    ),
                    "universe_min_raw_explanation": (
                        raw_explanations_by_id[signal_extremes[signal["key"]][0]].get(signal["key"])
                        if signal["key"] in signal_extremes else None
                    ),
                    "universe_max_raw_explanation": (
                        raw_explanations_by_id[signal_extremes[signal["key"]][1]].get(signal["key"])
                        if signal["key"] in signal_extremes else None
                    ),
                    "weight": signal["default_weight"],
                    "category": signal["group"],
                }
                for signal in PRICE_SIGNAL_DEFS
            },
        })
    return {
        "date": cutoff.date().isoformat(), "sector": sector,
        "rank": summary["rank"], "score": summary["momentum_score"],
        "category_scores": summary["category_scores"], "companies": companies,
        "category_weights": category_weights,
        "method": "Sector score is the arithmetic mean of the company momentum scores below.",
    }


@router.get("/api/momentum/sector-timeline")
def get_sector_timeline(
    days: int = Query(252, ge=20, le=756),
    max_assets: int = Query(600, ge=100, le=1200),
):
    """Daily Yahoo-close sector rankings for the liquid equity asset universe."""
    key = (_CALCULATION_VERSION, days, max_assets)
    with _cache_lock:
        hit = _cache.get(key)
        if hit and time.time() - hit[0] < _CACHE_TTL_SECONDS:
            return hit[1]

    # ``load_panel`` adds its own 1,100-calendar-day warm-up, enough for every
    # 12-1 / 200-day signal.  The requested window itself is calendar-based;
    # the final slice below contains exactly `days` available trading dates.
    start = (date.today() - timedelta(days=int(days * 1.8))).isoformat()
    analysis_ids, universe, secmap = _momentum_top_analysis_ids()
    panel, _, _ = load_panel(analysis_ids=analysis_ids, start=start)
    base = {
        "source": "Yahoo Finance close and volume (asset_price.yf.close / yf.volume)",
        "method": "Daily live-momentum price and volume signals, strict as-of, then mean company score by sector",
        "universe": universe,
        "days": days,
        "rows": [],
    }
    if panel is None:
        return {**base, "note": "Fast price loader unavailable (set SUPABASE_DB_URL)."}
    if panel.empty:
        return {**base, "note": "No liquid, sector-classified Yahoo-priced equities found."}

    volume_index = _volume_index(
        [int(i) for i in panel.columns if secmap.get(int(i))],
        (pd.Timestamp(start) - pd.Timedelta(days=1100)).date().isoformat(),
    )
    if volume_index is None:
        return {**base, "note": "Yahoo volume data is unavailable for this calculation."}

    # An intraday bar must never be presented as a completed daily close.
    # Waiting until the following UTC day is conservative across the global
    # equity universe and leaves today's column as a future `?` in the UI.
    amsterdam_today = datetime.now(ZoneInfo("Europe/Amsterdam")).date()
    completed = panel.index[panel.index < pd.Timestamp(amsterdam_today)]
    dates = list(completed[-days:])
    rows = build_sector_timeline(panel, secmap, dates, volume_index)
    result = {**base, "days": len(dates), "rows": rows}
    if not rows:
        result["note"] = "Not enough price history to calculate the daily price signals."
    with _cache_lock:
        _cache[key] = (time.time(), result)
    return result
