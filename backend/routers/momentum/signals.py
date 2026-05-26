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
#
# Bumped to 200 entries (~12 MB total) to accommodate post-backtest cache
# warming: after a backtest, each covered cutoff lands here so the
# user's first /signal-breakdown click for any stock in any month from
# the run is instant. A 60-period monthly run on 2 universes fills 120
# slots; the cap leaves headroom without hoarding RAM.
_BREAKDOWN_PANEL_CACHE: "OrderedDict[tuple, dict]" = OrderedDict()
_BREAKDOWN_PANEL_CACHE_LOCK = threading.Lock()
_BREAKDOWN_PANEL_CACHE_MAX = 200


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


def _panel_cache_db_get(
    universe_label: str | None,
    index_universe: str | None,
    cutoff_date: date,
) -> "pd.DataFrame | None":
    """Read the durable panel cache from Postgres. Returns the panel
    DataFrame on hit, None on miss / any error. Synchronous; call via
    asyncio.to_thread from async paths.

    Used as the second tier of the panel cache: in-process LRU first
    (hot), DB second (cold but survives restarts). After a hit here,
    callers should also populate the LRU so subsequent clicks in the
    same process skip the DB roundtrip."""
    try:
        # COALESCE in the SQL is implicit in PostgREST: passing eq()
        # with None translates to `IS NULL`. The unique index on the
        # table uses COALESCE so this query matches what was written.
        q = (
            supabase.table("panel_cache")
            .select("panel_jsonb")
            .eq("cutoff_date", cutoff_date.isoformat())
        )
        if universe_label is None:
            q = q.is_("universe_label", "null")
        else:
            q = q.eq("universe_label", universe_label)
        if index_universe is None:
            q = q.is_("index_universe", "null")
        else:
            q = q.eq("index_universe", index_universe)
        resp = q.limit(1).execute()
        if not resp.data:
            return None
        records = resp.data[0].get("panel_jsonb") or []
        if not records:
            return None
        return pd.DataFrame(records)
    except Exception:
        # DB hiccup → treat as miss; caller falls through to compute.
        # Never break a breakdown over a cache lookup failure.
        return None


def _panel_cache_db_put(
    universe_label: str | None,
    index_universe: str | None,
    cutoff_date: date,
    panel_df: "pd.DataFrame",
) -> None:
    """Write a freshly-computed panel to the durable cache. Upserts
    on (universe_label, index_universe, cutoff_date). Synchronous;
    call via asyncio.to_thread from async paths.

    Best-effort: failures are swallowed and logged at DEBUG so a slow
    INSERT doesn't slow the user-facing /signal-breakdown response.
    Worst case is the next click after a restart pays the cost
    again."""
    if panel_df is None or panel_df.empty:
        return
    try:
        records = panel_df.to_dict(orient="records")
        # Delete-then-insert: the migration uses a UNIQUE INDEX with
        # COALESCE() to handle NULL labels, but PostgREST's
        # `upsert(on_conflict=...)` requires a real UNIQUE CONSTRAINT
        # and rejects index-with-expression as a conflict target. Two
        # roundtrips instead of one, but correct under the table's
        # NULL-as-equal semantics.
        del_q = (
            supabase.table("panel_cache")
            .delete()
            .eq("cutoff_date", cutoff_date.isoformat())
        )
        if universe_label is None:
            del_q = del_q.is_("universe_label", "null")
        else:
            del_q = del_q.eq("universe_label", universe_label)
        if index_universe is None:
            del_q = del_q.is_("index_universe", "null")
        else:
            del_q = del_q.eq("index_universe", index_universe)
        del_q.execute()
        supabase.table("panel_cache").insert({
            "universe_label": universe_label,
            "index_universe": index_universe,
            "cutoff_date": cutoff_date.isoformat(),
            "panel_jsonb": records,
            "n_companies": len(records),
        }).execute()
    except Exception as e:
        import logging  # noqa: PLC0415
        logging.getLogger(__name__).debug(
            "[signals] panel_cache DB put failed for (%s, %s, %s): %s",
            universe_label, index_universe, cutoff_date.isoformat(), e,
        )


def warm_breakdown_panel_cache(
    universe_label: str | None,
    index_universe: str | None,
    cutoff_date: date,
    panel_df: "pd.DataFrame",
) -> None:
    """Public hook for backtest paths (single-run runner + variants
    sweep) to pre-populate this cache after they've already computed
    each cutoff's panel. After a backtest / sweep finishes, any
    subsequent /signal-breakdown click at any company in any covered
    (universe, cutoff) returns in <500ms instead of re-loading 500k+
    price rows.

    Caller must pass the panel SLICE that's already filtered to the
    per-cutoff eligible cids (so the cached universe min/max match what
    a fresh /signal-breakdown compute would produce). The runner's
    per-period `signals_df` already has this shape; variants.py
    re-applies the per-combo monthly_eligible filter to the union
    panel before calling in.

    Defensive `.copy()` so a caller mutating the df after warming
    doesn't corrupt the cached panel. Empty / None inputs are no-ops
    so the call site doesn't have to gate."""
    if panel_df is None or panel_df.empty:
        return
    key = (universe_label, index_universe, cutoff_date.isoformat())
    _cache_put(key, {"panel_df": panel_df.copy()})


# ──────────────────────────────────────────────────────────────────────
# Win #1: persistent universe price/volume index cache
# ──────────────────────────────────────────────────────────────────────
# Companion to the panel cache above. The panel cache covers cutoffs
# the user already backtested; for /signal-breakdown clicks at
# different months (NEW cutoff, same universe) the panel is a miss but
# the underlying price + volume data is identical to what the backtest
# loaded. Caching the indices means the cold load step (~5-10s of
# Supabase round-trips for ~500K rows) is skipped — the breakdown only
# needs to compute the panel for the single requested cutoff (~2s).
#
# Cache key is `(universe_label, index_universe, start_iso, end_iso)`
# where start/end describe the window the cached indices COVER. A
# breakdown request hits the cache when its requested window is a
# subset of any cached entry — same universe, cached range fully
# contains [requested_start, requested_end].
#
# Each entry holds {price_index, volume_index, universe_df,
# monthly_eligible}. ~20MB per typical universe-4yr cache entry, so
# the cap of 5 caps total RAM at ~100MB. LRU eviction handles
# overflow.
_PRICE_VOLUME_CACHE: "OrderedDict[tuple, dict]" = OrderedDict()
_PRICE_VOLUME_CACHE_LOCK = threading.Lock()
_PRICE_VOLUME_CACHE_MAX = 5


def get_cached_price_volume_indices(
    universe_label: str | None,
    index_universe: str | None,
    requested_start: date,
    requested_end: date,
) -> dict | None:
    """Cache lookup with date-range subset semantics. A cached entry
    hits when its window fully contains [requested_start, requested_end]
    AND its universe identity matches. The lookup walks newest-first
    (most recent puts) so the most-likely-useful entry is checked first.

    Returns the cached dict {price_index, volume_index, universe_df,
    monthly_eligible, start_date, end_date} on hit, None on miss. The
    caller is responsible for slicing universe_df / monthly_eligible
    down to just the requested cutoff's eligible cids."""
    req_start = requested_start.isoformat()
    req_end = requested_end.isoformat()
    with _PRICE_VOLUME_CACHE_LOCK:
        # `reversed` over OrderedDict.items() — newest entries first
        # (LRU `move_to_end` puts hot entries at the back).
        for key in reversed(list(_PRICE_VOLUME_CACHE.keys())):
            cached_label, cached_idx_univ, cached_start, cached_end = key
            if cached_label != universe_label or cached_idx_univ != index_universe:
                continue
            # Subset check: cached window must fully contain requested.
            if cached_start <= req_start and cached_end >= req_end:
                _PRICE_VOLUME_CACHE.move_to_end(key)
                return _PRICE_VOLUME_CACHE[key]
    return None


def warm_price_volume_cache(
    universe_label: str | None,
    index_universe: str | None,
    start_date: date,
    end_date: date,
    price_index: dict,
    volume_index: dict | None,
    universe_df: "pd.DataFrame | None" = None,
    monthly_eligible: "dict | None" = None,
) -> None:
    """Public hook for backtest paths to pre-populate the price/volume
    index LRU after they've loaded data. Subsequent /signal-breakdown
    cold loads at any cutoff within [start_date, end_date] for the
    same universe skip the data-load step entirely (saves ~5-10s of
    Supabase round-trips).

    `universe_df` and `monthly_eligible` are included in the cached
    bundle so the breakdown can also skip the universe-load +
    membership-load steps. Both optional — breakdown reloads them
    fresh when absent.

    Empty `price_index` is a no-op (defensive — caller doesn't have
    to gate). Multiple calls under the same key (e.g. one per combo
    in a multi-universe sweep) overwrite each other; the last write
    wins, which is fine since the underlying data is shared."""
    if not price_index:
        return
    key = (
        universe_label, index_universe,
        start_date.isoformat(), end_date.isoformat(),
    )
    with _PRICE_VOLUME_CACHE_LOCK:
        _PRICE_VOLUME_CACHE[key] = {
            "price_index": price_index,
            "volume_index": volume_index or {},
            "universe_df": universe_df,
            "monthly_eligible": monthly_eligible,
            "start_date": start_date,
            "end_date": end_date,
        }
        _PRICE_VOLUME_CACHE.move_to_end(key)
        while len(_PRICE_VOLUME_CACHE) > _PRICE_VOLUME_CACHE_MAX:
            _PRICE_VOLUME_CACHE.popitem(last=False)


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

    # Second-tier cache: durable DB lookup. The in-process LRU above
    # serves <500ms in-session, but the LRU dies on Railway redeploy.
    # The panel_cache table persists every computed panel across
    # restarts AND across user sessions — a deploy that cleared RAM
    # still gets sub-second cold breakdowns from the DB cache. Only
    # hit when the LRU misses (avoid round-trip cost on hot path).
    if panel_df is None:
        db_panel = await asyncio.to_thread(
            _panel_cache_db_get, req.universe_label, req.index_universe, cutoff,
        )
        if db_panel is not None and not db_panel.empty:
            panel_df = db_panel
            _cache_put(cache_key, {"panel_df": panel_df})  # promote to LRU
            yield _emit({"type": "progress", "pct": 75, "message": f"DB cache hit — universe panel ({len(panel_df)} companies) loaded from durable store"})

    if panel_df is None:
        # SLOW PATH (panel cache miss): full universe load + panel
        # computation. Before paying the 10s Supabase round-trip, check
        # the persistent price/volume cache — if the user just ran a
        # backtest on this universe, the indices + universe_df +
        # monthly_eligible are already in RAM and we can skip every
        # load step + jump straight to panel compute. The breakdown
        # window is [cutoff - 420 days, cutoff]; the backtest's load
        # window typically covers a much wider range so most breakdown
        # cutoffs in the backtest are subsets.
        price_start = cutoff - timedelta(days=420)
        cached_pv = get_cached_price_volume_indices(
            req.universe_label, req.index_universe, price_start, cutoff,
        )

        # 1. Resolve the universe at the cutoff.
        if cached_pv is not None and cached_pv.get("universe_df") is not None:
            universe_df = cached_pv["universe_df"]
            yield _emit({"type": "progress", "pct": 6, "message": f"Price/volume cache hit — reusing universe ({len(universe_df)} companies) from earlier backtest"})
        else:
            yield _emit({"type": "progress", "pct": 2, "message": "Loading universe..."})
            universe_df = await asyncio.to_thread(load_universe, supabase)
            if universe_df.empty:
                yield _emit({"type": "error", "message": "No companies found in the database"})
                return
            yield _emit({"type": "progress", "pct": 6, "message": f"Loaded universe ({len(universe_df)} companies)"})

        monthly_eligible: dict[str, dict[int, str | None]] | None = (
            cached_pv["monthly_eligible"] if cached_pv is not None else None
        )
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

        if label and monthly_eligible is None:
            yield _emit({"type": "progress", "pct": 8, "message": f"Loading universe membership for {label}..."})
            monthly_eligible = await asyncio.to_thread(_load_membership, label)
            if not monthly_eligible:
                yield _emit({"type": "error", "message": f"No universe data for label {label!r}"})
                return
        elif label and monthly_eligible is not None:
            yield _emit({"type": "progress", "pct": 8, "message": f"Universe membership for {label} reused from cache"})

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

        # 2. Resolve price + volume indices for the universe.
        # FAST PATH: the price/volume cache supplied indices already
        # covering [price_start, cutoff]. Skip the ~500K-row Supabase
        # load entirely and jump straight to panel compute.
        # SLOW PATH: load prices + volumes from Supabase in chunks,
        # build the indices, then compute the panel. `price_start` was
        # computed at the top of this branch for the cache lookup.
        if cached_pv is not None:
            u_price_index = cached_pv["price_index"]
            u_volume_index = cached_pv["volume_index"] or None
            yield _emit({"type": "progress", "pct": 65, "message": f"Price/volume indices reused from cache ({len(u_price_index):,} companies' price series)"})
        else:
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

            # 4a. Build the price + volume indices from the loaded
            # DataFrames. Warm the cache so subsequent breakdown clicks
            # at any cutoff within this window skip the load step.
            yield _emit({"type": "progress", "pct": 68, "message": "Building price + volume indices..."})
            u_price_index = await asyncio.to_thread(_build_price_index, u_prices_df)
            u_volume_index = await asyncio.to_thread(_build_volume_index, u_volumes_df) if not u_volumes_df.empty else None
            warm_price_volume_cache(
                req.universe_label, req.index_universe,
                price_start, cutoff,
                u_price_index, u_volume_index,
                universe_df=universe_df, monthly_eligible=monthly_eligible,
            )

        # 4b. Compute the signal panel for this single cutoff — pct 65 → 80.
        yield _emit({"type": "progress", "pct": 72, "message": f"Computing signals for {len(universe_df)} companies (rolling indicators)..."})
        panel_df = await asyncio.to_thread(
            lambda: compute_signals_panel(
                universe_df, [cutoff], price_index=u_price_index, volume_index=u_volume_index,
            ).get(cutoff, pd.DataFrame())
        )
        yield _emit({"type": "progress", "pct": 80, "message": f"Computed panel for {len(panel_df)} companies (the others lacked enough history)"})

        # Cache the panel: in-process LRU (instant subsequent clicks
        # in-session) AND durable DB (survives redeploys, shared
        # across replicas). DB write is fire-and-forget on a thread so
        # the user-facing response isn't blocked by a ~100ms upsert.
        _cache_put(cache_key, {"panel_df": panel_df})
        asyncio.create_task(asyncio.to_thread(
            _panel_cache_db_put,
            req.universe_label, req.index_universe, cutoff, panel_df,
        ))

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
