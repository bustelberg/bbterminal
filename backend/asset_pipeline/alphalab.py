"""AlphaLab — a signal IC scoreboard over the asset-pipeline price archive.

A first cut of the "alpha lab": for each candidate momentum/
reversal signal, measure its cross-sectional predictive power (Information
Coefficient = monthly rank correlation between the signal and next-month forward
return) across the most-liquid stored assets, with a t-stat, hit rate, and a
top-vs-bottom quintile spread. Self-contained (doesn't depend on the momentum
backtester); prices load via the fast single-COPY path.

Not (yet) the full four-gate admission battery (FDR bootstrap, tradability net of
spreads, robustness, decile monotonicity) — that's the deepening step.
"""
from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable, Iterator

import numpy as np
import pandas as pd

from deps import supabase
from signal_engine import by_cadence
from signal_engine.daily import evaluate_panel
from timeseries import ENTITY_COL, SeriesUnavailable, load_series, to_panel

# Signals: name -> builder over the month-end price panel `m` (index=month-end,
# columns=analysis_id). Each returns a same-shaped cross-sectional DataFrame.
_MIN_NAMES_PER_MONTH = 20   # need enough assets cross-sectionally for a stable IC
_QUINTILE_MIN = 25


def _signals(m: pd.DataFrame, ret1: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Legacy price-only scoreboard signals — delegates to the unified module so
    definitions never drift (volume signals need the fuller panel; see
    `compute_signal_lab`)."""
    from asset_pipeline import signals as _sig  # noqa: PLC0415
    c = _sig.Ctx(m, ret1, m * 0.0)
    return {s.name: s.build(c) for s in _sig.SIGNALS if s.group == "price"}


def _load_asset_series(
    analysis_ids: list[int],
    series: list[str],
    since: str | None,
    until: str | None,
) -> pd.DataFrame | None:
    """Adapter over `timeseries.load_series` keeping this module's legacy column
    names. Returns `None` when the COPY fast path is unavailable — `asset_price`
    has no PostgREST fallback, and every caller here degrades to a "set
    SUPABASE_DB_URL" note rather than erroring."""
    try:
        df = load_series(analysis_ids, series, since, until, order=False)
    except SeriesUnavailable:
        return None
    return df.rename(columns={ENTITY_COL: "analysis_id", "date": "target_date"})


def _panel(df: pd.DataFrame, value: str) -> pd.DataFrame:
    """Long -> wide (index=target_date, columns=analysis_id).

    `timeseries.to_panel` is a factorize + numpy scatter: identical output to the
    `pivot_table(...).sort_index()` this replaces (both source tables have a PK on
    (entity, date), so there are no duplicates to aggregate), ~7x faster on a
    large panel — 1,591 ms -> 232 ms over 5.58M rows."""
    return to_panel(df, value, entity_col="analysis_id", date_col="target_date")


def _load_closes(
    analysis_ids: list[int], since: str | None = None, until: str | None = None,
) -> pd.DataFrame | None:
    """(analysis_id, target_date, close) for the given assets via one COPY.
    `since`/`until` (YYYY-MM-DD, inclusive) bound the scan to a date window —
    crucial for a large universe (loading ALL history for thousands of ids is a
    slow full scan) and for the train/test split."""
    return _load_asset_series(analysis_ids, ["yf.close"], since, until)


def _sector_map(aids: list[int]) -> dict[int, str | None]:
    """analysis_id -> sector for the given ids, taking each instrument's
    most-liquid listing's sector (asset_grid has one row per execution)."""
    best: dict[int, tuple[float, str | None]] = {}
    for i in range(0, len(aids), 200):
        rows = (
            supabase.table("asset_grid")
            .select("analysis_id, med_adv_eur, sector")
            .in_("analysis_id", aids[i:i + 200]).execute().data
        ) or []
        for r in rows:
            aid = r["analysis_id"]
            adv = r.get("med_adv_eur") or 0.0
            cur = best.get(aid)
            if cur is None or adv > cur[0]:
                best[aid] = (adv, r.get("sector"))
    return {aid: sec for aid, (_, sec) in best.items()}


def _select_universe(
    min_adv_eur: float, require_sector: bool, asset_class: str | None, max_assets: int,
) -> tuple[list[int], dict]:
    """Build a sensible analysis-instrument universe: dedupe executions to one row
    per analysis instrument (keeping its most-liquid listing's ADV + sector),
    then keep those meeting the liquidity floor / sector / asset-class filters,
    most-liquid first, capped at `max_assets`. Returns (analysis_ids, meta)."""
    rows: list[dict] = []
    off = 0
    while True:
        r = (
            supabase.table("asset_grid")
            .select("analysis_id, med_adv_eur, sector, asset_class, status")
            .eq("status", "ok").not_.is_("analysis_id", "null")
            .range(off, off + 999).execute().data
        ) or []
        rows += r
        if len(r) < 1000:
            break
        off += 1000

    # One row per analysis instrument — keep the most-liquid listing's ADV.
    best: dict[int, tuple[float, str | None, str | None]] = {}
    for r in rows:
        aid = r["analysis_id"]
        adv = r.get("med_adv_eur") or 0.0
        cur = best.get(aid)
        if cur is None or adv > cur[0]:
            best[aid] = (adv, r.get("sector"), r.get("asset_class"))

    def _passes(adv: float, sector: str | None, cls: str | None) -> bool:
        return (
            adv >= min_adv_eur
            and (bool(sector) or not require_sector)
            and (cls == asset_class or not asset_class)
        )

    matched = [(aid, adv, sec) for aid, (adv, sec, cls) in best.items() if _passes(adv, sec, cls)]
    matched.sort(key=lambda x: x[1], reverse=True)
    picked = matched[:max_assets]

    breakdown: dict[str, int] = {}
    for _, _, sec in picked:
        if sec:
            breakdown[sec] = breakdown.get(sec, 0) + 1
    meta = {
        "size": len(picked),
        "matched": len(matched),          # before the max_assets cap
        "sectors": sorted(
            ({"sector": k, "count": v} for k, v in breakdown.items()),
            key=lambda x: -x["count"],
        ),
    }
    return [aid for aid, _, _ in picked], meta


def _eq_index(R: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Equal-weight universe index level + its trailing 63-day realized vol, from a
    days×instruments daily-return matrix. Daily returns are clipped to ±50% before
    averaging so a single blow-up bar can't hijack the index."""
    with np.errstate(all="ignore"):
        clipped = np.clip(R, -0.5, 0.5)
        cnt = np.isfinite(clipped).sum(axis=1)
        eq_ret = np.where(cnt > 0, np.nansum(clipped, axis=1) / np.maximum(cnt, 1), 0.0)
    eq_curve = np.cumprod(1.0 + eq_ret)
    roll_vol = pd.Series(eq_ret).rolling(63).std().to_numpy()
    return eq_curve, roll_vol


def _universe_analysis_ids(universe_id: int) -> tuple[list[int], dict]:
    """Analysis ids for a SAVED universe (asset_universe): its member
    analysis_symbols mapped back to analysis_ids. Returns (ids, meta)."""
    syms: list[str] = []
    off = 0
    while True:
        r = (supabase.table("asset_universe_member").select("analysis_symbol")
             .eq("universe_id", universe_id).range(off, off + 999).execute().data) or []
        syms += [x["analysis_symbol"] for x in r]
        if len(r) < 1000:
            break
        off += 1000
    aids: list[int] = []
    for i in range(0, len(syms), 200):
        r = (supabase.table("asset_analysis").select("analysis_id, symbol")
             .in_("symbol", syms[i:i + 200]).execute().data) or []
        aids += [x["analysis_id"] for x in r]
    u = (supabase.table("asset_universe").select("name").eq("id", universe_id).limit(1).execute().data) or []
    name = u[0]["name"] if u else f"Universe {universe_id}"
    return aids, {"size": len(aids), "matched": len(syms), "name": name, "sectors": []}


# A sector needs at least this many daily bars to build a meaningful index.
_SECTOR_MIN_DAYS = 30

# Regime hyperparameters. The turbulence baseline is the median of the 63-day vol
# over a TRAILING window (not an expanding-from-origin median) so the
# classification is load-window-INDEPENDENT — a train/test slice classifies
# identically whether we load just that slice (+warm-up) or the full history.
# This is what makes windowed loading scientifically reproducible.
_MA_LOOKBACK_D = 200          # bull/bear: trend vs trailing 200-day mean
_VOL_MEDIAN_LOOKBACK_D = 504  # calm/turbulent: vol vs its trailing ~2y median
# Calendar warm-up loaded BEFORE a window's start so the 200-day trend + 2y vol
# median are fully populated at the window's first displayed bar (~3y > the
# 504+63 trading days the vol median needs). Warm-up is sliced off the output.
_WARMUP_CAL_DAYS = 1100

# In-memory price-panel cache — keyed by (UNIVERSE, window) so the overall
# benchmark, the sector breakdown, and every exclusion toggle reuse ONE COPY of
# that window's price history. TTL matches the endpoint result caches (~30 min).
_PANEL_TTL = 1800.0
_panel_cache: dict[tuple, tuple[float, pd.DataFrame | None, dict, dict]] = {}
_panel_lock = threading.Lock()


def _panel_key(
    min_adv_eur: float, require_sector: bool, asset_class: str | None,
    max_assets: int, universe_id: int | None,
    start: str | None = None, end: str | None = None,
) -> tuple:
    base = (
        ("uni", int(universe_id)) if universe_id is not None
        else ("filt", float(min_adv_eur), bool(require_sector), asset_class, int(max_assets))
    )
    return (*base, start or "", end or "")


def load_panel(
    min_adv_eur: float = 1_000_000.0,
    require_sector: bool = True,
    asset_class: str | None = "equity",
    max_assets: int = 600,
    universe_id: int | None = None,
    start: str | None = None,
    end: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> tuple[pd.DataFrame | None, dict, dict]:
    """Load (or reuse) the close-price panel (index=date, columns=analysis_id) for a
    universe — ONE `COPY`, pivoted once, cached in memory by (universe, window).
    When `start`/`end` are given the load is bounded to `[start - warm-up, end]`
    (less data, faster) — the warm-up buffer keeps the 200-day trend + 2y vol
    median valid at the window start; callers slice the buffer off the output.
    Returns `(panel, secmap, uni)`; `panel` is `None` only when the fast COPY
    loader is unavailable, empty when the universe has no members. A module lock
    serializes concurrent loads of the same key so two near-simultaneous requests
    (benchmark + sector breakdown) share one COPY instead of racing into two."""
    def _p(stage: str) -> None:
        if progress:
            progress(stage)

    min_adv_eur = max(0.0, float(min_adv_eur))
    max_assets = max(20, min(int(max_assets), 2500))
    asset_class = asset_class or None
    key = _panel_key(min_adv_eur, require_sector, asset_class, max_assets, universe_id, start, end)

    hit = _panel_cache.get(key)
    if hit and (time.time() - hit[0] < _PANEL_TTL):
        return hit[1], hit[2], hit[3]

    _p("Resolving universe")
    with _panel_lock:
        # Re-check: another thread may have loaded it while we waited on the lock.
        hit = _panel_cache.get(key)
        if hit and (time.time() - hit[0] < _PANEL_TTL):
            return hit[1], hit[2], hit[3]

        if universe_id is not None:
            aids, uni = _universe_analysis_ids(universe_id)
        else:
            aids, uni = _select_universe(min_adv_eur, require_sector, asset_class, max_assets)
        if not aids:
            _panel_cache[key] = (time.time(), pd.DataFrame(), {}, uni)
            return pd.DataFrame(), {}, uni

        secmap = _sector_map(aids)
        # Bound the scan to [start - warm-up, end] when a window is requested.
        since = None
        if start:
            since = (pd.Timestamp(start) - pd.Timedelta(days=_WARMUP_CAL_DAYS)).date().isoformat()
        _p(f"Loading price history · {len(aids)} instruments")
        df = _load_closes(aids, since=since, until=end)
        if df is None:
            return None, {}, uni  # COPY unavailable — do NOT cache the miss
        _p("Building price panel")
        panel = _panel(df, "close")
        _panel_cache[key] = (time.time(), panel, secmap, uni)
        return panel, secmap, uni


def _trim_leading_nan(sub: pd.DataFrame) -> pd.DataFrame:
    """Drop leading rows where every column is NaN so the index starts at the
    first date any member of the subset has data (the panel spans the WHOLE
    universe, so a sector/exclusion subset can have an empty head)."""
    mask = sub.notna().any(axis=1)
    if not mask.any():
        return sub.iloc[0:0]
    return sub.iloc[int(mask.to_numpy().argmax()):]


def _score_regime(
    eq_curve: np.ndarray, roll_vol: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(ma200, bull, turb) for an equal-weight curve + its 63-day vol. bull = level
    at/above its trailing 200-day mean; turb = current vol above the median of its
    vol over the trailing ~2-year window (`_VOL_MEDIAN_LOOKBACK_D`). Each day uses
    only PRIOR data (no look-ahead); the trailing (vs expanding-from-origin) vol
    baseline makes the classification independent of how much history was loaded,
    so a train/test slice scores identically to a full-history run."""
    n = len(eq_curve)
    ma200 = np.empty(n)
    bull = np.empty(n, dtype=bool)
    turb = np.empty(n, dtype=bool)
    for p in range(n):
        prior = eq_curve[max(0, p - _MA_LOOKBACK_D):p]
        ma200[p] = float(np.mean(prior)) if len(prior) else eq_curve[p]
        bull[p] = bool(eq_curve[p] >= np.mean(prior)) if len(prior) else True
        hist = roll_vol[max(63, p - _VOL_MEDIAN_LOOKBACK_D):p]
        hist = hist[np.isfinite(hist)]
        turb[p] = bool(len(hist) > 60 and np.isfinite(roll_vol[p]) and roll_vol[p] > np.median(hist))
    return ma200, bull, turb


def regime_from_panel(
    panel: pd.DataFrame | None, secmap: dict, uni: dict,
    exclude_sectors: list[str] | None = None,
    start: str | None = None, end: str | None = None,
) -> dict:
    """Bull/bear × calm/turbulent regime of the equal-weight index, computed from
    an already-loaded panel. `exclude_sectors` selects the column subset (dropping
    e.g. commodities). Cheap — no DB. `start`/`end` slice the returned window."""
    excl = {s.strip().lower() for s in (exclude_sectors or []) if s.strip()}
    if panel is not None and not panel.empty and excl:
        keep = [c for c in panel.columns if (secmap.get(c) or "").lower() not in excl]
        sub = panel[keep]
        uni = {**uni, "size": len(keep), "excluded_sectors": sorted(excl)}
    else:
        sub = panel if panel is not None else pd.DataFrame()
    base = {"universe": uni, "filters": {"exclude_sectors": sorted(excl)}}
    if sub is None or sub.empty or sub.shape[1] == 0:
        return {**base, "dates": [], "note": "no instruments left after excluding sectors"}

    sub = _trim_leading_nan(sub)
    if len(sub) < 220:
        return {**base, "dates": [], "note": "not enough daily history for the 200-day regime"}

    idx = sub.index
    eq_curve, roll_vol = _eq_index(sub.pct_change().to_numpy())
    ma200, bull, turb = _score_regime(eq_curve, roll_vol)
    n = len(eq_curve)
    lo = 0 if start is None else int(idx.searchsorted(pd.Timestamp(start), "left"))
    hi = n if end is None else int(idx.searchsorted(pd.Timestamp(end), "right"))
    sl = slice(lo, hi)
    return {
        **base,
        "dates": [str(d.date()) for d in idx[sl]],
        "index": [round(float(v), 4) for v in eq_curve[sl]],
        "ma200": [round(float(v), 4) for v in ma200[sl]],
        "bull": [bool(v) for v in bull[sl]],
        "turb": [bool(v) for v in turb[sl]],
        "current": {"bull": bool(bull[-1]), "turb": bool(turb[-1]), "date": str(idx[-1].date())},
    }


# A sector needs at least this many days for a meaningful 200-day regime overlay;
# shorter sectors still chart, just without bull/turb bands.
_SECTOR_REGIME_MIN_DAYS = 220


def iter_sector_indices(
    panel: pd.DataFrame | None, secmap: dict,
    progress: Callable[[str], None] | None = None,
    start: str | None = None, end: str | None = None,
) -> Iterator[dict]:
    """Yield one {sector, size, dates, index} equal-weight index per sector in the
    panel, largest sector first, computed from the already-loaded panel (no DB).
    The index + regime are computed over each sector's full loaded history (so the
    warm-up buffer counts) then sliced to `[start, end]`. Sectors with enough
    warmed-up history also carry {ma200, bull, turb} for the regime overlay.
    Streaming-friendly: the caller can emit each as it's produced."""
    if panel is None or panel.empty:
        return
    by_sector: dict[str, list] = {}
    for aid in panel.columns:
        sec = secmap.get(aid)
        if sec:
            by_sector.setdefault(sec, []).append(aid)
    ordered = sorted(by_sector.items(), key=lambda kv: -len(kv[1]))
    total = len(ordered)
    for i, (sector, cols) in enumerate(ordered, 1):
        if progress:
            progress(f"Sector {i}/{total}: {sector}")
        sub = _trim_leading_nan(panel[cols])
        if len(sub) < _SECTOR_MIN_DAYS:
            continue
        eq_curve, roll_vol = _eq_index(sub.pct_change().to_numpy())
        idx = sub.index
        n = len(eq_curve)
        lo = 0 if start is None else int(idx.searchsorted(pd.Timestamp(start), "left"))
        hi = n if end is None else int(idx.searchsorted(pd.Timestamp(end), "right"))
        sl = slice(lo, hi)
        if hi - lo < _SECTOR_MIN_DAYS:  # too little history inside the window
            continue
        out = {
            "sector": str(sector),
            "size": len(cols),
            "dates": [str(d.date()) for d in idx[sl]],
            "index": [round(float(v), 4) for v in eq_curve[sl]],
        }
        # Regime overlay only when the sector is warmed up (≥220 loaded days) AND
        # has enough in-window bars to be meaningful.
        if len(sub) >= _SECTOR_REGIME_MIN_DAYS and (hi - lo) >= _SECTOR_REGIME_MIN_DAYS:
            ma200, bull, turb = _score_regime(eq_curve, roll_vol)
            out["ma200"] = [round(float(v), 4) for v in ma200[sl]]
            out["bull"] = [bool(v) for v in bull[sl]]
            out["turb"] = [bool(v) for v in turb[sl]]
        yield out


def compute_regime(
    min_adv_eur: float = 1_000_000.0,
    require_sector: bool = True,
    asset_class: str | None = "equity",
    max_assets: int = 600,
    start: str | None = None,
    end: str | None = None,
    universe_id: int | None = None,
    exclude_sectors: list[str] | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict:
    """Daily bull/bear × calm/turbulent regime of the equal-weight universe index.
    Loads the universe's price panel (cached) then scores the regime from it.
    `exclude_sectors` drops those sectors from the benchmark (e.g. commodities) so
    the index is a fairer bar to beat. `progress(stage)` reports each step for the
    SSE progress UI."""
    panel, secmap, uni = load_panel(
        min_adv_eur, require_sector, asset_class, max_assets, universe_id, start, end, progress,
    )
    if panel is None:
        return {"universe": uni, "dates": [], "note": "fast COPY loader unavailable (set SUPABASE_DB_URL)"}
    if progress:
        progress("Building equal-weight index")
    return regime_from_panel(panel, secmap, uni, exclude_sectors, start, end)


def compute_sector_regime(
    min_adv_eur: float = 1_000_000.0,
    require_sector: bool = True,
    asset_class: str | None = "equity",
    max_assets: int = 600,
    universe_id: int | None = None,
) -> dict:
    """Per-sector equal-weight price index of the universe (one index per sector,
    over each sector's own price history). Reuses the cached price panel. Returns
    {universe, sectors:[{sector,size,dates,index}]} sorted by member count."""
    panel, secmap, uni = load_panel(min_adv_eur, require_sector, asset_class, max_assets, universe_id)
    if panel is None:
        return {"universe": uni, "sectors": [], "note": "fast COPY loader unavailable (set SUPABASE_DB_URL)"}
    return {"universe": uni, "sectors": list(iter_sector_indices(panel, secmap))}


def _monthly_ic(sig: pd.DataFrame, fwd: pd.DataFrame) -> pd.Series:
    """Per-month Spearman rank IC between a signal and next-month forward return."""
    ics: list[float] = []
    for t in sig.index:
        pair = pd.concat([sig.loc[t], fwd.loc[t]], axis=1).dropna()
        if len(pair) >= _MIN_NAMES_PER_MONTH:
            # Spearman = Pearson of ranks (avoids the scipy dependency).
            ic = pair.iloc[:, 0].rank().corr(pair.iloc[:, 1].rank())
            if pd.notna(ic):
                ics.append(float(ic))
    return pd.Series(ics, dtype="float64")


# Forward returns are winsorized before any MEAN-based stat (quintile spread /
# decile profile) — a handful of micro-caps post +1000% months that otherwise
# swamp the averages. The rank IC is outlier-immune, so it's left raw.
_FWD_CLIP = (-0.9, 2.0)


def _quintile_spread(sig: pd.DataFrame, fwd: pd.DataFrame) -> float | None:
    """Mean (top-quintile − bottom-quintile) next-month return, by signal rank."""
    spreads: list[float] = []
    for t in sig.index:
        pair = pd.concat([sig.loc[t], fwd.loc[t]], axis=1).dropna()
        if len(pair) < _QUINTILE_MIN:
            continue
        pair.columns = ["s", "f"]
        f = pair["f"].clip(*_FWD_CLIP)
        q = pair["s"].rank(pct=True)
        top, bot = f[q >= 0.8].mean(), f[q <= 0.2].mean()
        if pd.notna(top) and pd.notna(bot):
            spreads.append(float(top - bot))
    return float(pd.Series(spreads).mean()) if spreads else None


def compute_scoreboard(
    min_adv_eur: float = 1_000_000.0,
    require_sector: bool = True,
    asset_class: str | None = "equity",
    max_assets: int = 600,
    preview: bool = False,
) -> dict:
    """IC scoreboard over a DEFINED universe of analysis instruments — filtered by
    a liquidity floor (`min_adv_eur`), sector presence, and asset class, capped at
    `max_assets` most-liquid. `preview=True` returns just the universe (size +
    sector breakdown) without loading prices — cheap, for tuning the filters."""
    min_adv_eur = max(0.0, float(min_adv_eur))
    max_assets = max(20, min(int(max_assets), 2500))
    asset_class = asset_class or None

    aids, uni = _select_universe(min_adv_eur, require_sector, asset_class, max_assets)
    base = {
        "universe": uni,
        "filters": {
            "min_adv_eur": min_adv_eur, "require_sector": require_sector,
            "asset_class": asset_class, "max_assets": max_assets,
        },
    }
    if preview:
        return {**base, "months": 0, "signals": [], "preview": True}
    if not aids:
        return {**base, "months": 0, "signals": [], "note": "no instruments match these filters"}

    df = _load_closes(aids)
    if df is None:
        return {**base, "months": 0, "signals": [],
                "note": "fast COPY loader unavailable (set SUPABASE_DB_URL) — scoreboard skipped"}

    panel = _panel(df, "close")
    # Daily → month-end last close; need enough history for the 12m lookbacks.
    m = panel.resample("ME").last()
    if len(m) < 18:
        return {**base, "months": int(len(m)), "signals": [],
                "note": "not enough monthly history for 12m signals"}
    ret1 = m.pct_change(1)
    fwd = ret1.shift(-1)  # next-month forward return, aligned to decision month

    out: list[dict] = []
    for name, sig in _signals(m, ret1).items():
        ic = _monthly_ic(sig, fwd)
        n = int(ic.size)
        if n < 6:
            continue
        mean_ic, std_ic = float(ic.mean()), float(ic.std(ddof=1))
        t_stat = (mean_ic / std_ic * math.sqrt(n)) if std_ic > 0 else 0.0
        p_value = math.erfc(abs(t_stat) / math.sqrt(2))  # 2-sided normal approx
        out.append({
            "signal": name,
            "mean_ic": round(mean_ic, 4),
            "t_stat": round(t_stat, 2),
            "p_value": round(p_value, 4),
            "hit_rate": round(float((ic > 0).mean()), 3),
            "quintile_spread": (round(qs, 4) if (qs := _quintile_spread(sig, fwd)) is not None else None),
            "months": n,
            "significant": abs(t_stat) >= 2.0,
        })
    out.sort(key=lambda r: abs(r["t_stat"]), reverse=True)
    return {
        **base,
        "months": int(len(m)),
        "from": str(m.index.min().date()),
        "to": str(m.index.max().date()),
        "signals": out,
    }


# --- Signal Lab: predictive-power research over the unified signal panel ---
_SECTOR_IC_MIN_NAMES = 10  # per-sector IC tolerates smaller cross-sections than overall


def _load_close_volume(
    analysis_ids: list[int], since: str | None = None, until: str | None = None,
) -> pd.DataFrame | None:
    """(analysis_id, target_date, close, volume) via one COPY. `since`/`until`
    (inclusive) bound the scan to a date window for the train/test split.

    Both series are columns of `asset_price`, so `load_series` fuses them into a
    single scan — same one query as before, not two."""
    return _load_asset_series(analysis_ids, ["yf.close", "yf.volume"], since, until)


_DAILY_SPECS = by_cadence("daily_asof")


def _daily_signal_panels(
    close: pd.DataFrame, volume: pd.DataFrame, months: pd.DatetimeIndex,
) -> dict[str, pd.DataFrame]:
    """Score the LIVE strategy's signals at this lab's decision points.

    `/schedule` trades the seven `daily_asof` signals; the Signal Lab only ever
    measured the nine `month_end` ones. Both batteries now come from
    `signal_engine`, so the lab can report the IC of the signals actually traded.

    Alignment: `evaluate_panel`'s cutoff is exclusive (strict `<`), so a cutoff of
    `month_end + 1 day` anchors on the month-end bar itself — exactly the decision
    point the month-end signals use, and exactly the bar `fwd` starts its return
    from. The two cadences are therefore measured on identical information.

    What this cadence adds, beyond different formulas: `evaluate_panel` drops an
    entity whose newest bar is more than `MAX_STALENESS_DAYS` before the cutoff, or
    that has fewer than `MIN_BARS` of history. A halted or delisted name that
    `resample("ME").last()` would still carry becomes NaN here, and NaN is excluded
    from the IC.

    Returns `{registry_key: month x analysis_id panel}` — keyed by `daily.mom_12_1`
    rather than `mom_12_1`, because the month-end battery already claims the bare
    name and they are NOT the same measure (see `signal_engine.registry.PARITY`).
    """
    aids = [int(a) for a in close.columns]
    price_index = {int(a): close[a].dropna() for a in close.columns}
    volume_index = {int(a): volume[a].dropna() for a in volume.columns}

    cutoffs = [(pd.Timestamp(me) + pd.Timedelta(days=1)).date() for me in months]
    per_cutoff = evaluate_panel(
        aids, cutoffs, price_index=price_index, volume_index=volume_index, id_col="entity_id",
    )

    panels = {
        s.key: pd.DataFrame(np.nan, index=months, columns=close.columns, dtype="float64")
        for s in _DAILY_SPECS
    }
    for me, c in zip(months, cutoffs):
        rows = per_cutoff.get(pd.Timestamp(c)) or []
        if not rows:
            continue
        block = pd.DataFrame(rows).set_index("entity_id")
        for s in _DAILY_SPECS:
            if s.name in block.columns:
                panels[s.key].loc[me, block.index] = block[s.name].to_numpy(dtype="float64")
    return panels


def _ic_series(sig: pd.DataFrame, fwd: pd.DataFrame, min_names: int) -> pd.Series:
    """Per-month Spearman rank IC (signal vs next-month return), indexed by month."""
    out: dict = {}
    for t in sig.index:
        pair = pd.concat([sig.loc[t], fwd.loc[t]], axis=1).dropna()
        if len(pair) >= min_names:
            ic = pair.iloc[:, 0].rank().corr(pair.iloc[:, 1].rank())
            if pd.notna(ic):
                out[t] = float(ic)
    return pd.Series(out, dtype="float64")


def _decile_profile(sig: pd.DataFrame, fwd: pd.DataFrame, min_names: int = 30, k: int = 10):
    """Mean next-month return per signal-decile (averaged across months) +
    monotonicity (rank corr of decile index vs its mean return)."""
    buckets: list[list[float]] = [[] for _ in range(k)]
    for t in sig.index:
        pair = pd.concat([sig.loc[t], fwd.loc[t]], axis=1).dropna()
        if len(pair) < min_names:
            continue
        pair.columns = ["s", "f"]
        f = pair["f"].clip(*_FWD_CLIP)
        q = (pair["s"].rank(pct=True) * k).clip(upper=k - 1e-9).astype(int)
        for d in range(k):
            v = f[q == d].mean()
            if pd.notna(v):
                buckets[d].append(float(v))
    means = [(float(pd.Series(b).mean()) if b else None) for b in buckets]
    valid = [(i, v) for i, v in enumerate(means) if v is not None]
    mono = None
    if len(valid) >= 4:
        idx = pd.Series([v[0] for v in valid])
        ret = pd.Series([v[1] for v in valid])
        c = idx.rank().corr(ret.rank())
        mono = round(float(c), 3) if pd.notna(c) else None
    return [(round(x, 4) if x is not None else None) for x in means], mono


def compute_signal_lab(
    min_adv_eur: float = 1_000_000.0,
    require_sector: bool = True,
    asset_class: str | None = "equity",
    max_assets: int = 600,
    universe_id: int | None = None,
    start: str | None = None,
    end: str | None = None,
    include_daily: bool = True,
) -> dict:
    """Predictive-power research over the universe: for each signal (price +
    volume) the cross-sectional rank IC vs next-month return, t-stat, hit rate,
    quintile spread, decile monotonicity, a PER-SECTOR IC breakdown, IC by regime,
    and the monthly IC series. `start`/`end` restrict the EVALUATED months (with a
    warm-up so the 12m lookbacks are valid) — develop on train, validate on test.
    Pure research — no portfolio."""
    from asset_pipeline import signals as _sig  # noqa: PLC0415

    min_adv_eur = max(0.0, float(min_adv_eur))
    max_assets = max(20, min(int(max_assets), 2500))
    asset_class = asset_class or None
    if universe_id is not None:
        aids, uni = _universe_analysis_ids(universe_id)
    else:
        aids, uni = _select_universe(min_adv_eur, require_sector, asset_class, max_assets)
    base = {"universe": uni}
    if not aids:
        return {**base, "signals": [], "sectors": [], "note": "no instruments match these filters"}
    secmap = _sector_map(aids)
    # Windowed load with warm-up so the 12m signal lookbacks are valid at the
    # window's first evaluated month (buffer sliced off below).
    since = None
    if start:
        since = (pd.Timestamp(start) - pd.Timedelta(days=_WARMUP_CAL_DAYS)).date().isoformat()
    df = _load_close_volume(aids, since=since, until=end)
    if df is None:
        return {**base, "signals": [], "sectors": [], "note": "fast COPY loader unavailable (set SUPABASE_DB_URL)"}
    close = _panel(df, "close")
    volume = _panel(df, "volume")
    m, ret1, vol = _sig.monthly_panels(close, volume)
    if len(m) < 18:
        return {**base, "signals": [], "sectors": [], "note": "not enough monthly history for 12m signals"}
    fwd = ret1.shift(-1)  # next-month forward return, aligned to the decision month
    sigs = _sig.build_signals(m, ret1, vol)

    # Evaluate IC only over the [start, end] months (warm-up buffer sliced off).
    lo = 0 if start is None else int(m.index.searchsorted(pd.Timestamp(start), "left"))
    hi = len(m) if end is None else int(m.index.searchsorted(pd.Timestamp(end), "right"))
    win = m.index[lo:hi]
    if len(win) < 6:
        return {**base, "signals": [], "sectors": [], "note": "not enough months in this window"}
    fwd = fwd.loc[win]
    sigs = {name: panel.loc[win] for name, panel in sigs.items()}

    # The daily-as-of battery — the signals /schedule actually trades — measured at
    # the SAME decision points against the SAME forward return. Keyed by registry
    # key so `daily.mom_12_1` can't be confused with the month-end `mom_12_1`.
    daily_panels = _daily_signal_panels(close, volume, win) if include_daily else {}

    # (key, label, group, cadence, panel) for every signal the lab reports.
    evaluated: list[tuple[str, str, str, str, pd.DataFrame]] = [
        (s.name, s.label, s.group, "month_end", sigs[s.name]) for s in _sig.SIGNALS
    ]
    evaluated += [
        (s.key, s.label, s.group, "daily_asof", daily_panels[s.key])
        for s in _DAILY_SPECS
        if s.key in daily_panels
    ]

    secs = sorted({s for a in close.columns if (s := secmap.get(a))})
    sec_cols = {sec: [a for a in close.columns if secmap.get(a) == sec] for sec in secs}

    # Regime label per DECISION month (causal — the state you'd know in real time),
    # from the universe equal-weight index. Used to bucket each signal's IC by
    # bull/bear × calm/turbulent.
    eq_curve, roll_vol = _eq_index(close.pct_change().to_numpy())
    _, r_bull, r_turb = _score_regime(eq_curve, roll_vol)
    reg_key = np.where(r_bull, np.where(r_turb, "bt", "bc"), np.where(r_turb, "rt", "rc"))
    reg_month = pd.Series(reg_key, index=close.index).resample("ME").last().reindex(win)  # window only
    regime_months = {k: int((reg_month == k).sum()) for k in ("bc", "bt", "rc", "rt")}

    out: list[dict] = []
    for sig_key, sig_label, sig_group, sig_cadence, sig in evaluated:
        ser = _ic_series(sig, fwd, _MIN_NAMES_PER_MONTH)
        n = int(ser.size)
        if n < 6:
            continue
        mean_ic, std_ic = float(ser.mean()), float(ser.std(ddof=1))
        t_stat = (mean_ic / std_ic * math.sqrt(n)) if std_ic > 0 else 0.0
        p_value = math.erfc(abs(t_stat) / math.sqrt(2))
        deciles, mono = _decile_profile(sig, fwd)
        sector_ic: dict = {}
        for sec, cols in sec_cols.items():
            if len(cols) < _SECTOR_IC_MIN_NAMES:
                continue
            ss = _ic_series(sig[cols], fwd[cols], _SECTOR_IC_MIN_NAMES)
            if ss.size >= 6:
                sector_ic[sec] = round(float(ss.mean()), 4)
        # IC conditioned on the decision-month regime (≥3 months to report).
        reg_at = reg_month.reindex(ser.index)
        regime_ic = {}
        for k in ("bc", "bt", "rc", "rt"):
            vals = ser[reg_at == k]
            if len(vals) >= 3:
                regime_ic[k] = round(float(vals.mean()), 4)
        out.append({
            "signal": sig_key, "label": sig_label, "group": sig_group,
            "cadence": sig_cadence,
            "mean_ic": round(mean_ic, 4), "t_stat": round(t_stat, 2), "p_value": round(p_value, 4),
            "hit_rate": round(float((ser > 0).mean()), 3),
            "quintile_spread": (round(qs, 4) if (qs := _quintile_spread(sig, fwd)) is not None else None),
            "monotonicity": mono, "deciles": deciles, "months": n,
            "significant": abs(t_stat) >= 2.0,
            "sector_ic": sector_ic,
            "regime_ic": regime_ic,
            "ic_series": [{"date": str(idx.date()), "ic": round(v, 4)} for idx, v in ser.items()],
        })
    out.sort(key=lambda r: abs(r["t_stat"]), reverse=True)
    return {
        **base,
        "months": int(len(win)),
        "from": str(win.min().date()),
        "to": str(win.max().date()),
        "sectors": secs,
        "regime_months": regime_months,
        "signals": out,
    }
