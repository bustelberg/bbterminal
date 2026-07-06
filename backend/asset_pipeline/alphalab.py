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


def _load_closes(analysis_ids: list[int]) -> pd.DataFrame | None:
    """(analysis_id, target_date, close) for the given assets via one COPY."""
    buf: io.BytesIO | None = _pg._run_copy(
        "COPY (SELECT analysis_id, target_date, close FROM asset_price "
        "WHERE analysis_id = ANY(%s::int[]) AND close IS NOT NULL) TO STDOUT WITH CSV",
        (analysis_ids,),
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
