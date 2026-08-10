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

from deps import IN_CHUNK_SIZE, supabase
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


# Below this median daily traded value a listing is flagged THIN — the same EUR 250k/day bar
# `scripts/add_portfolio_isins.py` prints a warning at when it adds a row.
THIN_ADV_EUR = 250_000.0

# Every price on this page comes from ONE vendor. Named as a constant so the table states it
# rather than the reader inferring it — see `_fx_source` for why that is worth a column.
PRICE_SOURCE = "yfinance"


def _fx_source(currency: str | None) -> str | None:
    """Which vendor supplied the EUR conversion for a holding quoted in `currency`.

    ⚠⚠ THE ANSWER TO "GURUFOCUS OR YFINANCE?" IS: NEITHER, FOR THIS LEG — AND THAT IS THE POINT
    OF SHOWING IT. Every price behind this matrix is yfinance (`asset_price`); GuruFocus
    (`metric_data`) prices the /benchmarks index and the momentum engine and never enters this
    path, because the AIRS books live in the ISIN/asset world and GuruFocus lives in the
    company world. A source column that only ever said "yfinance" would still be worth having
    for that reason — but it would also be INCOMPLETE, because a EUR figure for a USD holding
    is two vendors' numbers multiplied together, and the second one is not yfinance.

    The FX routing is `fx_rates.py`'s, imported rather than restated: ECB for its ~30 published
    currencies, a USD peg for AED/SAR/QAR/KWD, and Yahoo for TWD alone (ECB does not publish
    it). `fx_rate` stores no `source` column — only `(currency_code, rate_date, rate)` — so this
    is derived from the same routing the fetcher uses, which is why it must import that routing
    and not keep its own list.

    ⚠ A MINOR UNIT IS NOT A CURRENCY. `GBp` is pence and `fx_rate` has no such row; it resolves
    through `SUBUNIT` to GBP, which is ECB's. Classifying the literal string would report nine
    London holdings as having no FX source at all.

    Returns None for a EUR holding — no conversion happens, so naming a vendor would credit one
    with a number it never supplied.
    """
    from asset_pipeline.fx import SUBUNIT  # noqa: PLC0415
    from fx_rates import ECB_CURRENCIES, _USD_PEGS  # noqa: PLC0415

    if not currency:
        return None
    code = SUBUNIT.get(currency, (currency, 1.0))[0]
    if code == "EUR":
        return None
    if code in _USD_PEGS:
        return "pegged to USD"
    if code == "TWD" or code not in ECB_CURRENCIES:
        # TWD is Yahoo by design. Anything else absent from ECB's list has no route we know of,
        # and saying "ECB" would be a guess about where a number came from.
        return "Yahoo" if code == "TWD" else None
    return "ECB"


def _median_adv(analysis_ids: list[int]) -> dict[int, float]:
    """`analysis_id -> median daily traded value in EUR`, from `asset_grid`.

    ⚠⚠ THIS COLUMN IS HERE BECAUSE THE CORRELATION IS ONLY AS GOOD AS THE VENUE ITS PRICES COME
    FROM, AND THIS BOOK HAS A BAD ONE AT THE TOP OF IT. Measured 2026-08-10: Hermès
    (`FR0000052292`) is priced off **HMI.HA — Hanover — at a median EUR 4,946/day** against a
    EUR 173bn market cap, and it is held by 19 of the 44 models, joint-most of any instrument
    here. Paris (`RMS.PA`) is not in the grid at all. Its ADV/market-cap ratio is 2.9e-8, three
    orders of magnitude under the 1e-5 that `scripts/repoint_primary_listing.py` treats as proof
    of a wrong listing.

    A near-untraded line still produces 251 bars a year, so nothing upstream complains — but its
    closes are stale and jumpy against the real market, and a correlation OF DAILY RETURNS is
    precisely the statistic that mangles: the noise is idiosyncratic to the venue, so it pushes
    every pair involving that name toward zero. The number looks like a diversification finding.

    Surfaced rather than fixed here: repointing a listing is a deliberate, ISIN-anchored act with
    its own script, not something a read endpoint should do on the fly.
    """
    out: dict[int, float] = {}
    for i in range(0, len(analysis_ids), IN_CHUNK_SIZE):
        rows = (supabase.table("asset_grid").select("analysis_id,med_adv_eur")
                .in_("analysis_id", analysis_ids[i:i + IN_CHUNK_SIZE]).execute().data or [])
        for r in rows:
            if r.get("med_adv_eur") is not None:
                out[r["analysis_id"]] = float(r["med_adv_eur"])
    return out


def _series_block(inst: dict[str, dict],
                  eur: dict[int, list[tuple[str, float]]],
                  lookthrough: dict[int, list[tuple[str, float]]],
                  start: str) -> dict:
    """The charted series for every instrument, on ONE shared date axis.

    ⚠⚠ THE ENCODING IS MEASURED, NOT PREFERRED. 229 assets over a trailing year is ~57,000 points,
    and the obvious shape — `[[date, value], …]` per instrument — repeats a 10-byte date string
    once per instrument per day. Measured on this data:

        [[d, v], …] per instrument        1,270 KB raw    406 KB gzip
        {dates, values} per instrument    1,164 KB raw    273 KB gzip
        ONE axis + aligned value arrays     452 KB raw    207 KB gzip   <-- this
        downsampled to 120 points           614 KB raw    204 KB gzip

    Raw size is the number that matters most: it is JSON PARSE time on the client, not just
    transfer. And note the last row — thinning the series to 120 points saves 3 KB of gzip and
    throws away half the resolution, so there is no version of this worth downsampling.

    ⚠ A NULL IS A DAY THAT INSTRUMENT DID NOT TRADE, NOT A ZERO. The axis is the UNION of every
    instrument's dates, so a Tokyo name has a null on a Japanese holiday that Paris traded
    through. Rendering those as 0 would draw a spike to the floor on every foreign holiday.

    ⚠ TWO UNITS SHARE THIS BLOCK AND THEY ARE NOT THE SAME KIND OF NUMBER. A direct instrument's
    values are EUR PRICES; a look-through certificate's are an INDEX based at 100 on the first day
    its wrapped basket was fully priceable (`_lookthrough_series`), because the certificate itself
    has no price we can fetch — that index IS what entered the correlation for that leg. Each row
    carries its own `unit` so the chart can label it; printing "EUR" over an index would invent a
    currency figure out of a level.
    """
    keyed: dict[str, list[tuple[str, float]]] = {}
    for rec in inst.values():
        key = rec.get("series_key")
        if not key or key in keyed:
            continue
        kind, _, ident = key.partition(":")
        s = eur.get(int(ident)) if kind == "a" else lookthrough.get(int(ident))
        if s:
            keyed[key] = [(d, v) for d, v in s if d >= start]

    dates = sorted({d for s in keyed.values() for d, _ in s})
    at = {d: i for i, d in enumerate(dates)}
    values: dict[str, list[float | None]] = {}
    for key, s in keyed.items():
        col: list[float | None] = [None] * len(dates)
        for d, v in s:
            col[at[d]] = round(v, 4)
        values[key] = col
    return {"dates": dates, "values": values}


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
                "trailing_12m": [], "trailing_12m_obs": [],
                "instruments": [], "series": {"dates": [], "values": {}}}

    ids_order = [g["id"] for g in ports]
    # The chosen name where there is one, else AIRS's code — an axis needs a label, so unlike the
    # /portfolios table this cannot render a "—". `codes` carries AIRS's own name alongside, so a
    # renamed model can still be found in AIRS itself (the axis truncates at 11rem; the tooltip
    # is where the identifier survives).
    names = {g["id"]: portfolio_label(g) for g in ports}
    codes = {g["id"]: (g.get("name") or "") for g in ports}
    # ⚠ LABELS FOR EVERY MODEL, NOT JUST THE LISTED ONES. A certificate can wrap a model that is
    # NOT in the matrix — the wrapped `…TopSelectie OFF FX` books are frequently under the >5
    # holdings rule or hold a single line — and naming its row off `names` alone would leave the
    # one field that explains the row blank precisely where it is needed. The matrix axes still
    # use `names`; this is for the instrument table's `linked_label`.
    all_names = {g["id"]: portfolio_label(g) for g in grid}
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
    adv = _median_adv(aids)
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
    # ⚠ THE INSTRUMENT TABLE IS BUILT IN THIS LOOP, NOT IN A SECOND PASS. Every fact it shows —
    # which series a leg resolved to, whether that came from the ISIN or from looking through a
    # certificate, whose weight fell out as unpriceable — is decided HERE, once, and a second pass
    # asking the same questions again is a second implementation that can answer them differently.
    # The table then cannot claim an instrument fed the matrix when the matrix dropped it.
    inst: dict[str, dict] = {}

    def _touch(isin: str, row: dict, pid: int) -> dict:
        rec = inst.get(isin)
        if rec is None:
            e = ex.get(isin)
            rec = inst[isin] = {
                "isin": isin,
                # AIRS's own name for it — this table is read against the AIRS model, so its
                # vocabulary wins; our asset name rides along for the rows where they disagree.
                "name": row.get("fonds"),
                "asset_name": (e or {}).get("name"),
                "symbol": (e or {}).get("yahoo_symbol"),
                "currency": (e or {}).get("currency"),
                "analysis_id": (e or {}).get("analysis_id"),
                "state": "unpriced",
                "med_adv_eur": adv.get((e or {}).get("analysis_id")),
                "series_key": None,
                "linked_portfolio_id": None,
                "linked_label": None,
                "weight_pct_sum": 0.0,
                "_pids": set(),
            }
        # ⚠ DISTINCT PORTFOLIOS, hence a set: a model may list one instrument TWICE (VTopSelectie
        # OFF FX holds CapitaLand at 2% and again at 3%), and counting rows would report it held
        # by more books than exist. The weights still both count — that is one 5% position.
        rec["_pids"].add(pid)
        rec["weight_pct_sum"] = round(rec["weight_pct_sum"] + float(row.get("percentage") or 0), 4)
        return rec

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
            rec = _touch(isin, r, pid)
            e = ex.get(isin)
            s = eur.get(e["analysis_id"]) if e else None
            if s is not None:
                rec["state"] = "direct"
                rec["series_key"] = f"a:{e['analysis_id']}"
            else:                                  # maybe a certificate wrapping another model
                lk = links.get(link_key(isin, r.get("fonds")))
                tgt = lk.linked_portfolio_id if lk else None
                if tgt and tgt != pid:
                    if tgt not in lookthrough_cache:
                        lookthrough_cache[tgt] = _lookthrough_series(by_pf.get(tgt, []), ex, eur)
                    s = lookthrough_cache[tgt] or None
                    # ⚠ RECORDED EVEN WHEN THE SERIES CAME BACK EMPTY. `_lookthrough_series`
                    # returns [] when the wrapped model is itself under-covered, and a row that
                    # says only "unpriced" would send the reader looking for a missing listing
                    # when the truth is that we know exactly what it wraps and could not price
                    # THAT. The state stays `unpriced`; the link is what explains it.
                    rec["linked_portfolio_id"] = tgt
                    rec["linked_label"] = all_names.get(tgt)
                    if s is not None:
                        rec["state"] = "lookthrough"
                        rec["series_key"] = f"p:{tgt}"
            if s is None:
                continue                           # unpriceable — its weight counts against coverage
            legs.append((w, s))

        ytd_series[pid] = _returns_from_curve(legs, ytd_anchor[pid], total_w)
        t12_series[pid] = _returns_from_curve(legs, t12_anchor[pid], total_w)

    ytd_m, ytd_obs = _matrix(ytd_series, ids_order)
    t12_m, t12_obs = _matrix(t12_series, ids_order)

    # ⚠ THE WIDEST WINDOW, ONCE — the UI's YTD/12m toggle then slices client-side. Shipping the
    # window the toggle currently shows would make switching it a REFETCH, and the refetch would
    # repeat the price load, which is the entire cost of this endpoint (the matrices themselves
    # are 44x44 floats). Trailing-12m contains YTD, so one load answers both.
    series = _series_block(inst, eur, lookthrough_cache, t12_start)
    charted = set(series["values"])
    instruments = []
    for rec in sorted(inst.values(),
                      key=lambda r: (-len(r["_pids"]), (r.get("name") or r["isin"]).lower())):
        col = series["values"].get(rec.get("series_key") or "")
        obs = sum(1 for v in col if v is not None) if col else 0
        first = next((d for d, v in zip(series["dates"], col) if v is not None), None) if col else None
        last = next((d for d, v in zip(reversed(series["dates"]), reversed(col)) if v is not None),
                    None) if col else None
        instruments.append({
            **{k: v for k, v in rec.items() if k != "_pids"},
            "in_portfolios": len(rec["_pids"]),
            # ⚠ THE UNIT IS PER ROW — see `_series_block`. EUR for a real listing, an index based
            # at 100 for a certificate priced through the model it wraps.
            "unit": ("index" if rec["state"] == "lookthrough"
                     else "eur" if rec["state"] == "direct" else None),
            # ⚠ THE VENDOR BEHIND THE ROW, and it is the same one for every priced row here —
            # which IS the finding, not a redundancy. A look-through row is still yfinance
            # underneath: it is the wrapped model's basket, priced the same way.
            "price_source": PRICE_SOURCE if rec["series_key"] else None,
            # ...and the SECOND vendor, the one a "which source?" question usually forgets: a EUR
            # level for a USD holding is a yfinance close times an ECB rate. None for a EUR
            # holding, which needs no conversion.
            # ⚠ A LOOK-THROUGH ROW HAS NO CURRENCY OF ITS OWN — it is a BASKET, and each holding
            # inside it converts on its own rate (the certificate itself is a CH line we cannot
            # price at all, so `currency` is None here anyway). Naming one vendor would describe
            # a conversion that never happened at this level.
            "fx_source": ("per holding" if rec["state"] == "lookthrough"
                          else _fx_source(rec.get("currency")) if rec["series_key"] else None),
            "series_key": rec["series_key"] if rec.get("series_key") in charted else None,
            "observations": obs,
            "first_date": first,
            "last_date": last,
        })

    return {
        "instruments": instruments,
        "series": series,
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
