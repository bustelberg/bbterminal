"""AlphaLab — a signal IC scoreboard over the asset-pipeline price archive.

A first cut of the etoro-yfinance "alpha lab": for each candidate momentum/
reversal signal, measure its cross-sectional predictive power (Information
Coefficient = monthly rank correlation between the signal and next-month forward
return) across the most-liquid stored assets, with a t-stat, hit rate, and a
top-vs-bottom quintile spread. Self-contained (doesn't depend on the momentum
backtester); prices load via the fast single-COPY path.

Not (yet) the full four-gate admission battery (FDR bootstrap, tradability net of
spreads, robustness, decile monotonicity) — that's the deepening step.
"""
from __future__ import annotations

import io
import math

import numpy as np
import pandas as pd

from deps import supabase
from momentum.data import _pg

# Signals: name -> builder over the month-end price panel `m` (index=month-end,
# columns=analysis_id). Each returns a same-shaped cross-sectional DataFrame.
_MIN_NAMES_PER_MONTH = 20   # need enough assets cross-sectionally for a stable IC
_QUINTILE_MIN = 25


def _signals(m: pd.DataFrame, ret1: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        "mom_12_1":     m.shift(1) / m.shift(12) - 1,          # 12m momentum, skip last month
        "mom_6_1":      m.shift(1) / m.shift(7) - 1,           # 6m momentum, skip last month
        "mom_3m":       m / m.shift(3) - 1,                    # 3m momentum
        "reversal_1m":  ret1,                                  # short-term reversal (expect IC<0)
        "vol_adj_12_1": (m.shift(1) / m.shift(12) - 1) / ret1.rolling(12).std(),
        "dist_from_high_12m": m / m.rolling(12).max() - 1,     # proximity to 12m high
    }


def _load_closes(analysis_ids: list[int], since: str | None = None) -> pd.DataFrame | None:
    """(analysis_id, target_date, close) for the given assets via one COPY. `since`
    (YYYY-MM-DD) bounds the scan to recent history — crucial for a large universe
    (loading ALL history for thousands of ids is a slow full scan)."""
    where = "analysis_id = ANY(%s::int[]) AND close IS NOT NULL"
    params: tuple = (analysis_ids,)
    if since:
        where += " AND target_date >= %s"
        params = (analysis_ids, since)
    buf: io.BytesIO | None = _pg._run_copy(
        f"COPY (SELECT analysis_id, target_date, close FROM asset_price WHERE {where}) TO STDOUT WITH CSV",
        params,
    )
    if buf is None:
        return None
    return pd.read_csv(buf, names=["analysis_id", "target_date", "close"])


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
    averaging so a single blow-up bar can't hijack the index. (Faithful to the
    etoro-yfinance regime detector.)"""
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


def compute_regime(
    min_adv_eur: float = 1_000_000.0,
    require_sector: bool = True,
    asset_class: str | None = "equity",
    max_assets: int = 600,
    start: str | None = None,
    end: str | None = None,
    universe_id: int | None = None,
) -> dict:
    """Daily bull/bear × calm/turbulent regime of the equal-weight universe index
    (the same universe the IC scoreboard uses). bull = index at/above its trailing
    200-day mean; turbulent = current 63-day vol above the median of its own prior
    history — each day uses ONLY pre-day data (no look-ahead). Returns the index,
    its 200-day mean, and the bull/turb flags over [start, end] (warm-up kept
    behind the scenes). Faithful to etoro-yfinance `signals.regime_series`."""
    min_adv_eur = max(0.0, float(min_adv_eur))
    max_assets = max(20, min(int(max_assets), 2500))
    asset_class = asset_class or None
    if universe_id is not None:
        aids, uni = _universe_analysis_ids(universe_id)
    else:
        aids, uni = _select_universe(min_adv_eur, require_sector, asset_class, max_assets)
    base = {"universe": uni, "filters": {
        "min_adv_eur": min_adv_eur, "require_sector": require_sector,
        "asset_class": asset_class, "max_assets": max_assets, "universe_id": universe_id}}
    if not aids:
        return {**base, "dates": [], "note": "no instruments match these filters"}
    # Bound the scan to ~400 days before `start` so the 200-day mean + 63-day-vol
    # median still warm up, without loading every id's full history.
    since = None
    if start:
        since = (pd.Timestamp(start) - pd.Timedelta(days=400)).date().isoformat()
    df = _load_closes(aids, since=since)
    if df is None:
        return {**base, "dates": [], "note": "fast COPY loader unavailable (set SUPABASE_DB_URL)"}
    df["target_date"] = pd.to_datetime(df["target_date"])
    panel = df.pivot_table(index="target_date", columns="analysis_id", values="close").sort_index()
    if len(panel) < 220:
        return {**base, "dates": [], "note": "not enough daily history for the 200-day regime"}

    idx = panel.index
    eq_curve, roll_vol = _eq_index(panel.pct_change().to_numpy())
    n = len(eq_curve)
    ma200 = np.empty(n)
    bull = np.empty(n, dtype=bool)
    turb = np.empty(n, dtype=bool)
    for p in range(n):
        prior = eq_curve[max(0, p - 200):p]
        ma200[p] = float(np.mean(prior)) if len(prior) else eq_curve[p]
        bull[p] = bool(eq_curve[p] >= np.mean(prior)) if len(prior) else True
        hist = roll_vol[63:p]
        hist = hist[np.isfinite(hist)]
        turb[p] = bool(len(hist) > 60 and np.isfinite(roll_vol[p]) and roll_vol[p] > np.median(hist))

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


def _quintile_spread(sig: pd.DataFrame, fwd: pd.DataFrame) -> float | None:
    """Mean (top-quintile − bottom-quintile) next-month return, by signal rank."""
    spreads: list[float] = []
    for t in sig.index:
        pair = pd.concat([sig.loc[t], fwd.loc[t]], axis=1).dropna()
        if len(pair) < _QUINTILE_MIN:
            continue
        pair.columns = ["s", "f"]
        q = pair["s"].rank(pct=True)
        top, bot = pair.loc[q >= 0.8, "f"].mean(), pair.loc[q <= 0.2, "f"].mean()
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

    df["target_date"] = pd.to_datetime(df["target_date"])
    panel = df.pivot_table(index="target_date", columns="analysis_id", values="close").sort_index()
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
