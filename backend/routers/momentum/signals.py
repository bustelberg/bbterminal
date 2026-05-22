"""Signal listing + per-company signal-breakdown SSE.

Endpoints:
    GET  /api/momentum/signals          available signal definitions + categories
    POST /api/momentum/signal-breakdown  SSE: step-by-step decomposition of a
                                         single company's score at a given cutoff

The breakdown endpoint is heavy on the cache-miss path (universe load +
panel compute). Subsequent clicks for any stock in the same
(universe, cutoff) reuse the cached panel and return in <500 ms.
"""

from __future__ import annotations

import asyncio
import json
import queue as _queue
import threading
from collections import OrderedDict
from datetime import date, timedelta

import pandas as pd
from fastapi import APIRouter, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from deps import supabase
from momentum.data import load_all_prices, load_all_volumes, load_universe
from momentum.signals import PRICE_SIGNAL_DEFS
from routers._cache_headers import CACHE_STATIC

router = APIRouter(tags=["momentum"])


@router.get("/api/momentum/signals")
async def get_momentum_signals(response: Response):
    """Available signal definitions + the category buckets used by scoring."""
    # Signal defs are code-defined (PRICE_SIGNAL_DEFS in momentum.signals) and
    # only change with a backend deploy -- safe to cache aggressively.
    response.headers["Cache-Control"] = CACHE_STATIC
    from momentum.scoring import _get_category_keys
    return {"signals": PRICE_SIGNAL_DEFS, "categories": list(_get_category_keys().keys())}


class SignalBreakdownRequest(BaseModel):
    company_id: int
    as_of_date: str  # "YYYY-MM-01" for the start of a backtest month, or any YYYY-MM-DD
    universe_label: str | None = None
    index_universe: str | None = None
    signal_weights: dict[str, float] | None = None
    category_weights: dict[str, float] | None = None


# In-process LRU cache for (loaded universe, computed signal panel) at a
# given cutoff. The expensive part of /signal-breakdown is loading 500+
# companies' prices from Supabase + computing the panel — both depend ONLY
# on (universe_label, index_universe, cutoff), not on the requesting
# company or the user's signal/category weights. Caching this lets the
# first click in a session pay the full cost (~3-8s) and every subsequent
# click for any stock in any month already-cached return in <500ms.
# Bounded to 50 entries (~3 MB total).
_BREAKDOWN_PANEL_CACHE: "OrderedDict[tuple, dict]" = OrderedDict()
_BREAKDOWN_PANEL_CACHE_LOCK = threading.Lock()
_BREAKDOWN_PANEL_CACHE_MAX = 50


def _cache_get(key: tuple) -> dict | None:
    with _BREAKDOWN_PANEL_CACHE_LOCK:
        if key in _BREAKDOWN_PANEL_CACHE:
            _BREAKDOWN_PANEL_CACHE.move_to_end(key)
            return _BREAKDOWN_PANEL_CACHE[key]
    return None


def _cache_put(key: tuple, value: dict) -> None:
    with _BREAKDOWN_PANEL_CACHE_LOCK:
        _BREAKDOWN_PANEL_CACHE[key] = value
        _BREAKDOWN_PANEL_CACHE.move_to_end(key)
        while len(_BREAKDOWN_PANEL_CACHE) > _BREAKDOWN_PANEL_CACHE_MAX:
            _BREAKDOWN_PANEL_CACHE.popitem(last=False)


async def _signal_breakdown_stream(req: SignalBreakdownRequest):
    """SSE generator: emits progress events during the slow universe-load +
    panel-compute path, then a final `result` event with the breakdown.
    Instant on cache hit — straight to per-company explain + scoring."""
    from momentum.backtest import _build_price_index, _build_volume_index
    from momentum.explain import _date_str, explain_all_signals
    from momentum.scoring import _get_category_keys, compute_category_scores
    from momentum.signals import compute_signals_panel

    def _emit(payload: dict) -> str:
        return f"data: {json.dumps(payload)}\n\n"

    try:
        cutoff = date.fromisoformat(req.as_of_date)
    except ValueError:
        yield _emit({"type": "error", "message": f"as_of_date must be ISO YYYY-MM-DD, got {req.as_of_date!r}"})
        return
    cutoff_ts = pd.Timestamp(cutoff)

    label = req.universe_label or req.index_universe
    cache_key = (req.universe_label, req.index_universe, cutoff.isoformat())

    cached = _cache_get(cache_key)
    panel_df: pd.DataFrame | None = None
    if cached is not None:
        panel_df = cached["panel_df"]
        yield _emit({"type": "progress", "pct": 75, "message": "Cache hit — universe panel already computed for this month"})

    if panel_df is None:
        # SLOW PATH (cache miss): full universe load + panel computation.

        # 1. Resolve the universe at the cutoff.
        yield _emit({"type": "progress", "pct": 2, "message": "Loading universe..."})
        universe_df = await asyncio.to_thread(load_universe, supabase)
        if universe_df.empty:
            yield _emit({"type": "error", "message": "No companies found in the database"})
            return
        yield _emit({"type": "progress", "pct": 6, "message": f"Loaded universe ({len(universe_df)} companies)"})

        monthly_eligible: dict[str, dict[int, str | None]] | None = None
        target_month_key = cutoff.strftime("%Y-%m")

        def _load_membership(label: str) -> dict[str, dict[int, str | None]]:
            u_resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
            if not u_resp.data:
                return {}
            universe_id = u_resp.data[0]["universe_id"]
            rows: list[dict] = []
            offset, page_size = 0, 1000
            while True:
                resp = (
                    supabase.table("universe_membership")
                    .select("target_month, company_id, sector")
                    .eq("universe_id", universe_id)
                    .order("target_month").order("company_id")
                    .range(offset, offset + page_size - 1)
                    .execute()
                )
                batch = resp.data or []
                rows.extend(batch)
                if len(batch) < page_size:
                    break
                offset += page_size
            out: dict[str, dict[int, str | None]] = {}
            for r in rows:
                m = (r.get("target_month") or "")[:7]
                if not m:
                    continue
                out.setdefault(m, {})[r["company_id"]] = r.get("sector")
            return out

        if label:
            yield _emit({"type": "progress", "pct": 8, "message": f"Loading universe membership for {label}..."})
            monthly_eligible = await asyncio.to_thread(_load_membership, label)
            if not monthly_eligible:
                yield _emit({"type": "error", "message": f"No universe data for label {label!r}"})
                return

        if monthly_eligible is not None:
            eligible = monthly_eligible.get(target_month_key) or {}
            if not eligible:
                available = sorted(monthly_eligible.keys())
                hint = f" (available range: {available[0]} … {available[-1]})" if available else ""
                yield _emit({"type": "error", "message": f"No companies in {label!r} for {target_month_key}{hint}"})
                return
            eligible_ids = set(eligible.keys())
            universe_df = (
                universe_df[universe_df["company_id"].isin(eligible_ids)]
                .copy().reset_index(drop=True)
            )
            universe_df["sector"] = universe_df["company_id"].map(eligible)
            yield _emit({"type": "progress", "pct": 12, "message": f"Filtered to {len(universe_df)} companies in {label} for {target_month_key}"})

        universe_company_ids = sorted({int(c) for c in universe_df["company_id"]})

        # 2. Load prices for the universe — pct 12 → 50.
        price_start = cutoff - timedelta(days=420)
        prices_q: _queue.Queue = _queue.Queue()

        def _on_prices_progress(rows: int, page: int, chunks_done: int = 0, chunks_total: int = 0):
            prices_q.put({"rows": rows, "chunks_done": chunks_done, "chunks_total": chunks_total})

        prices_task = asyncio.create_task(asyncio.to_thread(
            load_all_prices, supabase, universe_company_ids, price_start, cutoff,
            on_progress=_on_prices_progress,
        ))
        last_emit = 0
        while not prices_task.done():
            drained = []
            while True:
                try:
                    drained.append(prices_q.get_nowait())
                except _queue.Empty:
                    break
            if drained:
                latest = drained[-1]
                ct = latest.get("chunks_total", 0) or 1
                cd = latest.get("chunks_done", 0)
                # Map chunk progress into the 12-50% band.
                pct = 12 + round(cd / ct * 38)
                if pct - last_emit >= 2:
                    last_emit = pct
                    yield _emit({"type": "progress", "pct": pct, "message": f"Loading prices: {latest['rows']:,} rows ({cd}/{ct} chunks)..."})
            await asyncio.sleep(0.15)
        u_prices_df = await prices_task
        if u_prices_df.empty:
            yield _emit({"type": "error", "message": "No price data available for any company in the universe at this date"})
            return
        yield _emit({"type": "progress", "pct": 50, "message": f"Loaded {len(u_prices_df):,} price rows"})

        # 3. Load volumes — pct 50 → 65.
        volumes_q: _queue.Queue = _queue.Queue()

        def _on_volumes_progress(rows: int, page: int, chunks_done: int = 0, chunks_total: int = 0):
            volumes_q.put({"rows": rows, "chunks_done": chunks_done, "chunks_total": chunks_total})

        volumes_task = asyncio.create_task(asyncio.to_thread(
            load_all_volumes, supabase, universe_company_ids, price_start, cutoff,
            on_progress=_on_volumes_progress,
        ))
        last_emit = 50
        while not volumes_task.done():
            drained = []
            while True:
                try:
                    drained.append(volumes_q.get_nowait())
                except _queue.Empty:
                    break
            if drained:
                latest = drained[-1]
                ct = latest.get("chunks_total", 0) or 1
                cd = latest.get("chunks_done", 0)
                pct = 50 + round(cd / ct * 15)
                if pct - last_emit >= 2:
                    last_emit = pct
                    yield _emit({"type": "progress", "pct": pct, "message": f"Loading volumes: {latest['rows']:,} rows ({cd}/{ct} chunks)..."})
            await asyncio.sleep(0.15)
        u_volumes_df = await volumes_task
        yield _emit({"type": "progress", "pct": 65, "message": f"Loaded {len(u_volumes_df):,} volume rows"})

        # 4. Build the signal panel for this single cutoff — pct 65 → 80.
        yield _emit({"type": "progress", "pct": 68, "message": "Building price + volume indices..."})
        u_price_index = await asyncio.to_thread(_build_price_index, u_prices_df)
        u_volume_index = await asyncio.to_thread(_build_volume_index, u_volumes_df) if not u_volumes_df.empty else None

        yield _emit({"type": "progress", "pct": 72, "message": f"Computing signals for {len(universe_df)} companies (rolling indicators)..."})
        panel_df = await asyncio.to_thread(
            lambda: compute_signals_panel(
                universe_df, [cutoff], price_index=u_price_index, volume_index=u_volume_index,
            ).get(cutoff, pd.DataFrame())
        )
        yield _emit({"type": "progress", "pct": 80, "message": f"Computed panel for {len(panel_df)} companies (the others lacked enough history)"})

        # Cache the panel only — small (~30-60 KB).
        _cache_put(cache_key, {"panel_df": panel_df})

    # Per-company prices/volumes — needed every call for explain helpers.
    yield _emit({"type": "progress", "pct": 82, "message": f"Loading prices/volumes for company #{req.company_id}..."})
    price_start = cutoff - timedelta(days=420)
    co_prices_df = await asyncio.to_thread(load_all_prices, supabase, [int(req.company_id)], price_start, cutoff)
    if co_prices_df.empty:
        yield _emit({"type": "error", "message": f"No price data for company {req.company_id} before {req.as_of_date}"})
        return
    co_volumes_df = await asyncio.to_thread(load_all_volumes, supabase, [int(req.company_id)], price_start, cutoff)
    price_index = await asyncio.to_thread(_build_price_index, co_prices_df)
    volume_index = await asyncio.to_thread(_build_volume_index, co_volumes_df) if not co_volumes_df.empty else None

    # 5. Per-signal universe min/max (what 0-100 normalization saw).
    yield _emit({"type": "progress", "pct": 88, "message": "Computing universe-wide signal min/max..."})
    signal_keys = [s["key"] for s in PRICE_SIGNAL_DEFS]
    universe_minmax: dict[str, dict[str, float | None]] = {}
    for k in signal_keys:
        if k in panel_df.columns:
            col = pd.to_numeric(panel_df[k], errors="coerce")
            if col.notna().any():
                universe_minmax[k] = {"min": float(col.min()), "max": float(col.max())}
            else:
                universe_minmax[k] = {"min": None, "max": None}
        else:
            universe_minmax[k] = {"min": None, "max": None}

    # 6. Score the universe + look up this company's row.
    yield _emit({"type": "progress", "pct": 92, "message": "Running scoring engine + explain helpers..."})
    sig_weights = req.signal_weights or {s["key"]: s["default_weight"] for s in PRICE_SIGNAL_DEFS}
    cw = req.category_weights
    cats_keys = _get_category_keys()
    if cw and any(v != 0 for v in cw.values()):
        cw_sum = sum(abs(v) for v in cw.values()) or 1.0
        cw_normalized = {c: (cw.get(c, 0) / cw_sum) for c in cats_keys}
    else:
        n = len(cats_keys)
        cw_normalized = {c: 1.0 / n for c in cats_keys}

    scored_df = compute_category_scores(panel_df, sig_weights, req.category_weights) if not panel_df.empty else pd.DataFrame()

    company_row = None
    if not scored_df.empty:
        match = scored_df[scored_df["company_id"] == int(req.company_id)]
        if not match.empty:
            company_row = match.iloc[0].to_dict()

    # 7. Run explain helpers against this company's trimmed series.
    company_series = price_index.get(int(req.company_id))
    if company_series is None or company_series.empty:
        yield _emit({"type": "error", "message": f"No price data for company {req.company_id} before {req.as_of_date}"})
        return
    trimmed = company_series[company_series.index < cutoff_ts]
    if trimmed.empty:
        yield _emit({"type": "error", "message": f"No price data for company {req.company_id} strictly before {req.as_of_date}"})
        return

    company_vol = volume_index.get(int(req.company_id)) if volume_index else None
    vol_trimmed = company_vol[company_vol.index < cutoff_ts] if company_vol is not None else None
    explanations = explain_all_signals(trimmed, vol_trimmed)

    # 8. Build per-signal + per-category response.
    signals_response: list[dict] = []
    for sig_def in PRICE_SIGNAL_DEFS:
        key = sig_def["key"]
        if key not in explanations:
            continue
        exp = explanations[key]
        mm = universe_minmax.get(key, {})
        sig_min = mm.get("min")
        sig_max = mm.get("max")
        normalized: float | None = None
        if exp["value"] is not None and sig_min is not None and sig_max is not None:
            if sig_max > sig_min:
                normalized = round((exp["value"] - sig_min) / (sig_max - sig_min) * 100, 2)
            else:
                normalized = 50.0
        signals_response.append({
            "key": key,
            "label": sig_def["label"],
            "description": sig_def["description"],
            "category": sig_def.get("group", "price"),
            "raw_value": exp["value"],
            "components": exp["components"],
            "universe_min": sig_min,
            "universe_max": sig_max,
            "normalized_score": normalized,
            "weight": sig_weights.get(key, 0),
        })

    category_scores: list[dict] = []
    for cat_name, weight in cw_normalized.items():
        score_val = company_row.get(f"score_{cat_name}") if company_row else None
        score = float(score_val) if score_val is not None and not pd.isna(score_val) else None
        category_scores.append({
            "category": cat_name,
            "score": score,
            "weight": weight,
            "contribution": (score * weight) if score is not None else None,
        })

    momentum_score = None
    if company_row and "momentum_score" in company_row and not pd.isna(company_row["momentum_score"]):
        momentum_score = float(company_row["momentum_score"])

    # 9. Company metadata.
    yield _emit({"type": "progress", "pct": 98, "message": "Looking up company metadata..."})
    meta = await asyncio.to_thread(
        lambda: (
            supabase.table("company")
            .select("company_id, gurufocus_ticker, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code)")
            .eq("company_id", req.company_id).limit(1).execute()
        )
    )
    if not meta.data:
        yield _emit({"type": "error", "message": f"Company {req.company_id} not found"})
        return
    m = meta.data[0]
    exchange_code = (m.get("gurufocus_exchange") or {}).get("exchange_code") or ""

    yield _emit({"type": "progress", "pct": 100, "message": "Done"})
    yield _emit({
        "type": "result",
        "data": {
            "company_id": int(req.company_id),
            "ticker": m.get("gurufocus_ticker", ""),
            "exchange": exchange_code,
            "company_name": m.get("company_name", ""),
            "as_of_date": req.as_of_date,
            "anchor_date": _date_str(trimmed.index[-1]),
            "anchor_price": float(trimmed.iloc[-1]),
            "signals": signals_response,
            "category_scores": category_scores,
            "category_weights_normalized": cw_normalized,
            "momentum_score": momentum_score,
            "universe_size": int(panel_df.shape[0]) if not panel_df.empty else 0,
            "in_universe_at_cutoff": company_row is not None,
            "universe_label_used": label,
        },
    })


@router.post("/api/momentum/signal-breakdown")
async def signal_breakdown(req: SignalBreakdownRequest):
    """SSE stream of step-by-step signal-breakdown computation. Emits
    `progress` events with pct + message during the heavy universe load,
    then a final `result` event with the full breakdown payload (or an
    `error` event on failure). On cache hit the slow steps are skipped."""
    return StreamingResponse(
        _signal_breakdown_stream(req),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
