"""Pairwise correlation of the AIRS model portfolios' daily EUR returns.

WHY THIS SHARES EVERY PRIMITIVE WITH `_airs_portfolio_perf`
    A correlation is only as trustworthy as the return series under it, and the /portfolios
    table already defines exactly one such series per portfolio: the daily EUR value of a
    buy-and-hold of its composition, read off `_index`. This module builds NOTHING new — it
    asks `_index` for that same curve WITH its dates (`return_dates=True`), turns it into a
    dated daily-return series, and correlates. If it rolled its own curve, the matrix could
    disagree with the YTD column two rows above it, which is the class of bug the perf module
    is obsessive about preventing.

WHICH PORTFOLIOS (the "42 of 95")
    The /portfolios table's default view hides "small" portfolios — a KEEP rule that shows only
    a countable FIXED model of MORE THAN 5 distinct instruments (`holdings > 5`). The three
    absence states (no fixed model / never counted / no snapshot) are not portfolios you can
    correlate. We replicate that rule off `airs_model_portfolio_grid` (whose `holdings` is the
    same DISTINCT-ISIN count the column shows), so the matrix covers exactly the listed 42.

WINDOWS
    YTD          — each portfolio from `max(1 Jan, its inception)`; a pair correlates on the
                   trading days they BOTH have a return (pairwise-complete), so different
                   inception dates just shorten the overlap rather than invent zeros.
    trailing 12m — each portfolio from `max(today - 365d, its inception)`.

    A pair with fewer than `MIN_OVERLAP_DAYS` common returns gets a null cell, not a number —
    the same refusal `_airs_portfolio_perf` applies to a Sharpe off five points.
"""
from __future__ import annotations

import asyncio
from datetime import date, timedelta

import numpy as np
import pandas as pd

from deps import supabase
from routers._airs_portfolio_links import _load_context, link_key, resolve_links
from routers._airs_portfolio_store import portfolio_label
from routers._airs_portfolio_variant import portfolio_variant
from routers._airs_portfolio_perf import (
    _ANCHOR_LOOKBACK_DAYS,
    _closes,
    _eur_series,
    _executions,
    _fx,
    _index,
    _lookthrough_series,
    _prepend_opening_bars,
    ytd_anchor_for,
)
from routers._airs_portfolio_perf import (
    MIN_COVERAGE_PCT as _MIN_COVERAGE_PCT,
)

# A correlation over fewer than this many common daily returns is noise with decimals — the
# same floor `_airs_portfolio_perf.MIN_STAT_DAYS` puts on a Sharpe. ~1 month of trading.
MIN_OVERLAP_DAYS = 20

# The fixed model must hold MORE than this many distinct instruments to be "listed" — the exact
# `holdings > 5` KEEP rule the /portfolios table applies (PortfoliosPanel `MIN_HOLDINGS_SHOWN`).
_MIN_HOLDINGS = 5

_MIN_COVERAGE = _MIN_COVERAGE_PCT / 100.0


def _returns_from_curve(legs, anchor: str, total_w: float) -> dict[str, float] | None:
    """The portfolio's daily EUR returns from `anchor`, keyed by date — or None when too little
    of it is priceable for the renormalised curve to mean anything (the perf module's coverage
    floor, applied here so an under-covered portfolio can't contribute a fabricated series)."""
    dates, values, held_w = _index(legs, anchor, return_dates=True)
    if total_w <= 0 or held_w / total_w < _MIN_COVERAGE:
        return None
    # values[0] is the anchor base (1.0); values[k+1] is the value on dates[k].
    out: dict[str, float] = {}
    for k, d in enumerate(dates):
        if values[k] > 0:
            out[d] = values[k + 1] / values[k] - 1.0
    return out


def _matrix(series_by_pf: dict[int, dict[str, float] | None],
            order: list[int]) -> tuple[list[list[float | None]], list[int]]:
    """Pairwise-complete Pearson correlation over `order`. Returns (NxN, obs-count-per-pf).

    Built through pandas so alignment on the union of dates and the per-pair NaN handling are
    the library's, not hand-rolled: a column is a portfolio's return series, an outer join on
    dates leaves NaN where a portfolio had no return that day (a foreign holiday), and
    `.corr(min_periods=…)` uses only the days a pair BOTH observed.
    """
    frame = {pid: pd.Series(series_by_pf.get(pid) or {}) for pid in order}
    df = pd.DataFrame(frame)                       # index = union of dates, columns = order
    obs = [int(df[pid].notna().sum()) for pid in order]
    if df.empty:
        return [[None] * len(order) for _ in order], obs
    corr = df.corr(method="pearson", min_periods=MIN_OVERLAP_DAYS)
    corr = corr.reindex(index=order, columns=order)
    out: list[list[float | None]] = []
    for pid in order:
        row: list[float | None] = []
        for other in order:
            v = corr.at[pid, other]
            row.append(None if v is None or (isinstance(v, float) and np.isnan(v)) else float(v))
        out.append(row)
    return out, obs


def compute_portfolio_correlations(year: int | None = None) -> dict:
    """YTD + trailing-12m correlation matrices over the listed (>5-holding) model portfolios."""
    year = year or date.today().year
    jan1 = f"{year}-01-01"
    today = date.today().isoformat()
    t12_start = (date.today() - timedelta(days=365)).isoformat()

    # ── the "42": fixed models with > 5 distinct instruments, off the same grid view the UI reads
    grid = (supabase.table("airs_model_portfolio_grid")
            .select("id,name,display_name,omschrijving,positions_datum,has_fixed_model,holdings")
            .execute().data or [])
    ports = [g for g in grid
             if g.get("has_fixed_model")
             and isinstance(g.get("holdings"), int) and g["holdings"] > _MIN_HOLDINGS
             and g.get("positions_datum")]
    # ⚠ SORTED BY WHAT IS SHOWN, not by AIRS's code. Both axes are labelled with
    # `portfolio_label`, so ordering by `name` while displaying the chosen name would render a
    # matrix whose axes look shuffled — alphabetical by a key the reader cannot see is
    # indistinguishable from unsorted.
    ports.sort(key=lambda g: portfolio_label(g).lower())
    if not ports:
        return {"portfolio_ids": [], "labels": [], "codes": [], "variants": [], "as_of": today,
                "min_overlap_days": MIN_OVERLAP_DAYS, "ytd": [], "ytd_obs": [],
                "trailing_12m": [], "trailing_12m_obs": []}

    ids_order = [g["id"] for g in ports]
    # The chosen name where there is one, else AIRS's code — an axis needs a label, so unlike the
    # /portfolios table this cannot render a "—". `codes` carries AIRS's own name alongside, so a
    # renamed model can still be found in AIRS itself (the axis truncates at 11rem; the tooltip
    # is where the identifier survives).
    names = {g["id"]: portfolio_label(g) for g in ports}
    codes = {g["id"]: (g.get("name") or "") for g in ports}
    # The risk profile, read off AIRS's OWN name — never off the chosen `display_name`. A name you
    # picked is a label, not a taxonomy: rename BUS_Neutraal_FX to "Steady Eddie" and its profile
    # must not change, nor must naming something "Offensive Growth" invent one.
    variants = {g["id"]: portfolio_variant(g.get("name"), g.get("omschrijving")) for g in ports}
    eff_by_pf = {g["id"]: g["positions_datum"] for g in ports}
    ytd_anchor = {pid: ytd_anchor_for(eff_by_pf[pid], year) for pid in ids_order}
    # Trailing 12m opens 365d back, but never before the composition took effect — pricing these
    # weights earlier than the model held them is the hindsight bias the perf module refuses.
    t12_anchor = {pid: max(t12_start, eff_by_pf[pid]) for pid in ids_order}

    # ── the shared price/FX load (mirrors compute_portfolio_performance's setup) ───────────────
    pos = (supabase.table("airs_model_portfolio_position")
           .select("portfolio_id,isin,percentage,fonds").execute().data or [])
    by_pf: dict[int, list[dict]] = {}
    for r in pos:
        by_pf.setdefault(r["portfolio_id"], []).append(r)

    link_ctx = _load_context(supabase)
    lookthrough_cache: dict[int, list[tuple[str, float]]] = {}

    # Prices must reach the OLDEST anchor in play — the trailing-12m start (365d back) is earlier
    # than 1 Jan, so this window can go back well past the YTD one.
    all_anchors = list(ytd_anchor.values()) + list(t12_anchor.values())
    earliest = min([jan1, t12_start, *all_anchors])
    lookback = (date.fromisoformat(earliest) - timedelta(days=_ANCHOR_LOOKBACK_DAYS)).isoformat()

    isins = sorted({r["isin"] for r in pos if r.get("isin")})
    ex = _executions(isins)
    aids = sorted({e["analysis_id"] for e in ex.values()})
    closes = _closes(aids, lookback, today)
    for anchor in sorted(set(all_anchors) | set(eff_by_pf.values())):
        _prepend_opening_bars(closes, aids, anchor)

    fx_from = min([lookback, *(s[0][0] for s in closes.values() if s)])
    fx = _fx({e.get("currency") for e in ex.values()}, fx_from, today)
    eur: dict[int, list[tuple[str, float]]] = {
        e["analysis_id"]: _eur_series(closes[e["analysis_id"]], e.get("currency"), fx)
        for e in ex.values() if closes.get(e["analysis_id"])
    }

    # ── per-portfolio legs, then a dated return series per window ──────────────────────────────
    ytd_series: dict[int, dict[str, float] | None] = {}
    t12_series: dict[int, dict[str, float] | None] = {}
    for pid in ids_order:
        rows = by_pf.get(pid, [])
        links = resolve_links(supabase, pid,
                              [{"isin": r.get("isin"), "fonds": r.get("fonds")} for r in rows],
                              context=link_ctx)
        legs: list[tuple[float, list[tuple[str, float]] | None]] = []
        total_w = 0.0
        for r in rows:
            w = float(r.get("percentage") or 0)
            if w <= 0:
                continue
            total_w += w
            isin = r.get("isin")
            if not isin:
                legs.append((w, None))            # cash — a real leg, flat 0% (its drag is a fact)
                continue
            e = ex.get(isin)
            s = eur.get(e["analysis_id"]) if e else None
            if s is None:                          # maybe a certificate wrapping another model
                lk = links.get(link_key(isin, r.get("fonds")))
                tgt = lk.linked_portfolio_id if lk else None
                if tgt and tgt != pid:
                    if tgt not in lookthrough_cache:
                        lookthrough_cache[tgt] = _lookthrough_series(by_pf.get(tgt, []), ex, eur)
                    s = lookthrough_cache[tgt] or None
            if s is None:
                continue                           # unpriceable — its weight counts against coverage
            legs.append((w, s))

        ytd_series[pid] = _returns_from_curve(legs, ytd_anchor[pid], total_w)
        t12_series[pid] = _returns_from_curve(legs, t12_anchor[pid], total_w)

    ytd_m, ytd_obs = _matrix(ytd_series, ids_order)
    t12_m, t12_obs = _matrix(t12_series, ids_order)

    return {
        "portfolio_ids": ids_order,
        "labels": [names[pid] for pid in ids_order],
        # AIRS's own code, aligned to `labels`. Kept so a renamed model is still identifiable in
        # AIRS — the label is for reading, this is for finding.
        "codes": [codes[pid] for pid in ids_order],
        # The risk profile, aligned to `labels`. `null` = this model is not offered at one (8 of
        # the 42) — an answer, not a gap. See `_airs_portfolio_variant`.
        "variants": [variants[pid] for pid in ids_order],
        "as_of": today,
        "min_overlap_days": MIN_OVERLAP_DAYS,
        "ytd": ytd_m,
        "ytd_obs": ytd_obs,
        "trailing_12m": t12_m,
        "trailing_12m_obs": t12_obs,
    }


async def compute_portfolio_correlations_async(year: int | None = None) -> dict:
    return await asyncio.to_thread(compute_portfolio_correlations, year)
