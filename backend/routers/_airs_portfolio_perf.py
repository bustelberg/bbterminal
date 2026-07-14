"""Performance of the AIRS model portfolios, in EUR: YTD, since-inception, Sharpe, Sortino.

⚠ READ THIS BEFORE TRUSTING `ytd_pct` — IT IS NOT ALWAYS A FULL YEAR.
    A model portfolio is a COMPOSITION, not an account — AIRS stores what it should hold, not
    a track record. We hold exactly one composition per portfolio (the current one), because
    AirSPMS's snapshot dropdown offers only 2-3 dates and they are not a monthly history:

        BUS_BM_AAND_ww_EUR_2026   ['2026-07-13', '2026-01-06']
        BUS_FTS_OFF_AFS           ['2026-07-13', '2025-03-24', '2025-04-11']

    Those dates are when the model was DEFINED, and `positions_datum` is the latest of them —
    i.e. the composition's EFFECTIVE DATE, its INCEPTION. That single fact decides where a YTD
    can honestly start:

      * effective BEFORE Jan 1  (29 of 56) — the model has held these weights all year, so
        buy-and-hold from Jan 1 IS what it earned. A real, full YTD.
      * effective DURING the year (27 of 56) — the weights we hold were NOT the weights it
        held in January, and January's composition is not recoverable from AIRS. Pricing these
        weights back to Jan 1 would backtest a basket chosen KNOWING how the year had gone.

    So the YTD window starts at `max(Jan 1, inception)` — never before the composition existed.
    A young model's figure is therefore a PARTIAL year: realized, not backtested, but covering
    days rather than months. `model_changed_in_period` marks exactly those rows, because a
    6-day return and a 12-month one are not comparable merely by sharing a column, and `ytd_from`
    carries the date the window actually opened.

    (Until 2026-07-14 the anchor was an unconditional Jan 1, and MoTopSelectie_FX read +75.85%
    on a model defined EIGHT DAYS earlier — the best portfolio in the list, on weights it had
    never held. Its realized return over those eight days was +0.51%.)

⚠ SHARPE AND SORTINO ARE MEASURED OVER THE SINCE-INCEPTION WINDOW, NEVER YTD.
    They are ratios of a return to the volatility that produced it, so they are only as honest
    as the return underneath them — and a YTD-anchored one is a backtest for half the list. So
    the ratios ride the SAME window as `since_model_pct`: from the model's own effective date,
    the only stretch in which its weights were actually held. That is also why they are absent
    (not zero, not a small number) for a model defined last week: `MIN_STAT_DAYS` daily returns
    are needed before a ratio is a statistic rather than noise with two decimals.

⚠ WEIGHTS ARE THE MODEL'S OWN, RENORMALISED OVER WHAT WE CAN PRICE.
    25 of 248 held ISINs have no price series (structured products, in-house funds — see
    `store_one`'s zero-bar guard). Renormalising assumes the unpriced behave like the priced,
    which is a real assumption, so `covered_pct` is returned and shown. Cash IS priced — at a
    flat 0% — because cash's drag on a portfolio return is a fact, not a gap.
"""
from __future__ import annotations

import asyncio
from datetime import date, timedelta

import pandas as pd

from asset_pipeline.fx import SUBUNIT
from deps import IN_CHUNK_SIZE, supabase
from momentum.diversification import annualized_stats
from routers._benchmark_index import _at_or_before, _rate, _split_adjust
from timeseries import DATE_COL, ENTITY_COL, SeriesUnavailable, load_series


# Below this share of the model's weight, we REFUSE to return a return.
#
# Renormalising over what we can price silently assumes the rest behaved the same. At 95%
# that is a rounding error; at 1% it is a fabrication. Measured, before this floor existed:
# TOPS_OFF_BEH reported "+0.00% YTD" — that was its 1% CASH line, renormalised to 100%, while
# the nine structured products making up the other 99% were simply dropped. A confident,
# precise, entirely invented number. The portfolios that trip this are the ones holding
# Leonteq structured products and in-house funds, which have no price series at all (see the
# zero-bar guard in `store_one`) — so this is not a bug to fix, it is a limit to state.
MIN_COVERAGE_PCT = 60.0

# Above this we show it plainly; between the two it is shown but flagged as partial.
GOOD_COVERAGE_PCT = 90.0

# Trading days in a year — the annualization factor for a DAILY return series.
TRADING_DAYS = 252

# Daily returns needed before a CAGR is reported at all: ONE YEAR.
#
# A CAGR compounds a window's return out to a year, so a short window is not merely noisy — it is
# systematically amplified. Vermogensopbouw_OFF_FX made +11.20% over 99 trading days; annualized
# that is +30.6%, a headline number resting on four months. Sharpe survives a short window (it is
# a ratio — both halves scale); a CAGR does not, and it would sit in the same column, same font,
# as one earned over two years.
#
# This is also the standard: fund reporting does not annualize a period under a year, it shows
# the cumulative return — which is exactly what `since_model_pct` already is. So under this floor
# we return NOTHING and let that column speak.
MIN_CAGR_DAYS = TRADING_DAYS

# Daily returns needed before a Sharpe/Sortino is reported at all.
#
# A model defined eight days ago (and 27 of 56 were defined this year) yields five daily
# returns. A ratio computed off five points is noise with two decimals — and it renders in the
# same column, same font, as one measured over two years. So under this floor we return NOTHING
# rather than a number that cannot be told apart from a real one. ~1 month of trading.
MIN_STAT_DAYS = 20

# How far before the earliest anchor to start reading prices. The anchor's opening mark is the
# last close ON OR BEFORE it (`_at_or_before`), which may be days back over a holiday break, and
# an inception date can itself fall on a weekend.
#
# ⚠ This window is a PERFORMANCE bound, never a correctness one — a series sparse enough to have
# no bar inside it still gets its opening bar, fetched directly by `_prepend_opening_bars`. It
# was a correctness bound until 2026-07-14, and silently: iShares Euro HY Corp Bd is mapped to a
# dead US OTC line (`ISHHF`, 54 bars in TEN years) whose last close before 1 Jan was 2025-11-03 —
# 14 days outside this window. The expanded row, loading 45 days, saw no opening bar and showed
# nothing; the portfolio figure, loading from the earliest inception, saw it and priced the
# holding. Two windows, two answers, no error — for 23.6% of that portfolio.
_ANCHOR_LOOKBACK_DAYS = 45

# A close this many days before the anchor is the anchor's price: markets shut for weekends and
# holidays, so 31 Dec IS the 1 Jan mark. Beyond it the series genuinely has no price near the
# date, and we interpolate rather than mark the position at a months-old close.
_MARK_GAP_TOLERANCE_DAYS = 7

# ...but interpolating between two closes more than this far apart is not an estimate, it is a
# guess with decimals. (Same refusal as `_trailing_12m`'s 450-day span cap: a number we cannot
# support is worse than a blank, because a blank cannot be traded on.)
_MAX_INTERP_SPAN_DAYS = 400


def _executions(isins: list[str]) -> dict[str, dict]:
    """ISIN -> its priceable execution row. The bridge between the AIRS world and ours."""
    out: dict[str, dict] = {}
    for i in range(0, len(isins), IN_CHUNK_SIZE):
        rows = (supabase.table("asset_execution")
                .select("isin,analysis_id,currency,yahoo_symbol,name")
                .in_("isin", isins[i:i + IN_CHUNK_SIZE]).execute().data or [])
        for r in rows:
            if r.get("analysis_id") and r["isin"] not in out:
                out[r["isin"]] = r
    return out


def _closes_paged(analysis_ids: list[int], start: str, end: str,
                  ) -> dict[int, list[tuple[str, float]]]:
    """Yahoo closes per analysis_id, ascending, via PostgREST. Local currency."""
    out: dict[int, list[tuple[str, float]]] = {}
    for i in range(0, len(analysis_ids), IN_CHUNK_SIZE):
        chunk = analysis_ids[i:i + IN_CHUNK_SIZE]
        off = 0
        while True:
            # PostgREST caps a response at 1,000 rows and TRUNCATES SILENTLY. 223 holdings ×
            # ~500 trading days (the since-inception window reaches back to 2024) is ~118,000
            # rows — an unpaged read here returns 1% of the data and computes a confident,
            # wrong number off it. (I hit exactly this while probing coverage: it reported 102
            # priced holdings when the answer is 221.)
            batch = (supabase.table("asset_price")
                     .select("analysis_id,target_date,close")
                     .in_("analysis_id", chunk)
                     .gte("target_date", start).lte("target_date", end)
                     .order("target_date").range(off, off + 999).execute().data or [])
            for r in batch:
                if r["close"] is not None:
                    out.setdefault(r["analysis_id"], []).append(
                        (r["target_date"], float(r["close"])))
            if len(batch) < 1000:
                break
            off += 1000
    for s in out.values():
        s.sort()
    return out


def _closes(analysis_ids: list[int], start: str, end: str) -> dict[int, list[tuple[str, float]]]:
    """Yahoo closes per analysis_id, ascending. Local currency.

    Prefers the single-`COPY` transport and falls back to the paged PostgREST reader when it
    is unavailable (no `SUPABASE_DB_URL`, psycopg missing, connection error) — same rows either
    way, and the fallback is why this endpoint still works where COPY isn't configured.

    The transport matters here because the window does: Sharpe needs a since-inception curve,
    and the oldest model dates to 2024-06, so this reads ~118,000 rows rather than the ~37,000
    a YTD-only window needed — 120 paged round-trips through Cloudflare, or one COPY.
    """
    if not analysis_ids:
        return {}
    try:
        df = load_series(analysis_ids, "yf.close", start, end)
    except SeriesUnavailable:
        return _closes_paged(analysis_ids, start, end)

    out: dict[int, list[tuple[str, float]]] = {}
    if df.empty:
        return out
    dates = df[DATE_COL].dt.strftime("%Y-%m-%d")
    for aid, d, c in zip(df[ENTITY_COL], dates, df["close"], strict=True):
        if pd.notna(c):
            out.setdefault(int(aid), []).append((d, float(c)))
    for s in out.values():
        s.sort()      # `load_series` already sorts by (entity, date); belt and braces
    return out


def _fx(currencies: set[str], start: str, end: str) -> dict[str, dict[str, float]]:
    """Rates keyed by MAJOR currency — `_rate` resolves a minor unit (GBp) to its base and
    applies the divisor. Asking `fx_rate` for "GBp" returns nothing at all: pence is a quoting
    convention, not a currency, and 343 of our rows are quoted in it."""
    out: dict[str, dict[str, float]] = {}
    cur = sorted({SUBUNIT.get(c, (c, 1.0))[0] for c in currencies if c and c != "EUR"})
    for i in range(0, len(cur), IN_CHUNK_SIZE):
        rows = (supabase.table("fx_rate")
                .select("currency_code,rate_date,rate")
                .in_("currency_code", cur[i:i + IN_CHUNK_SIZE])
                .gte("rate_date", start).lte("rate_date", end).execute().data or [])
        for r in rows:
            if r["rate"]:
                out.setdefault(r["currency_code"], {})[r["rate_date"]] = float(r["rate"])
    return out


def _eur_series(series: list[tuple[str, float]], ccy: str | None,
                fx: dict[str, dict[str, float]]) -> list[tuple[str, float]]:
    """One holding's closes, split-adjusted and converted to EUR at each date's own rate.

    Converted ONCE, here, rather than at each anchor: the return and the daily curve then read
    off the same EUR series and cannot disagree about what a holding did."""
    series, _ = _split_adjust(series)          # Yahoo closes are not guaranteed adjusted either
    out: list[tuple[str, float]] = []
    for d, p in series:
        if p <= 0:
            continue
        r = _rate(fx, ccy, d)
        if not r:                              # before this currency's FX history — omit, not 0
            continue
        out.append((d, p / r))
    return out


def _prepend_opening_bars(closes: dict[int, list[tuple[str, float]]],
                          analysis_ids: list[int], anchor: str) -> None:
    """Guarantee every series has a close ON OR BEFORE `anchor`, whatever the load window was.

    A daily series always has one — 31 December sits days before 1 January. A SPARSE one may
    not: the iShares Euro HY line trades so rarely on its (wrong, OTC) listing that its last
    close before 1 Jan 2026 was 2025-11-03, outside any sane fixed lookback. Without the opening
    bar the holding is not "unpriceable", it is priceable and we failed to load it — and the two
    are indistinguishable downstream, which is how one caller showed a blank while another
    priced 23.6% of the same portfolio.

    So the missing bar is fetched directly, per series, and only for the series that lack it —
    normally none. Mutates `closes` in place; the bar is real history, so a series it is added
    to is simply more complete for every other anchor too.
    """
    missing = [aid for aid in analysis_ids
               if not closes.get(aid) or closes[aid][0][0] > anchor]
    for aid in missing:
        got = (supabase.table("asset_price").select("target_date,close")
               .eq("analysis_id", aid).lte("target_date", anchor)
               .not_.is_("close", "null")
               .order("target_date", desc=True).limit(1).execute().data or [])
        if got and got[0]["close"] is not None:
            closes.setdefault(aid, []).insert(
                0, (got[0]["target_date"], float(got[0]["close"])))


def _mark_at(series: list[tuple[str, float]],
             anchor: str) -> tuple[str, float, bool, int] | None:
    """What one holding was worth ON `anchor`: `(date, price, interpolated, gap_days)`.

    Three cases, and they are not the same fact:

      * a close within `_MARK_GAP_TOLERANCE_DAYS` before the anchor — that IS the anchor's price
        (markets are shut on 1 January; 31 December is the mark). Returned as-is, `interpolated`
        false. This is every normal holding, and its behaviour is unchanged.

      * no close near the anchor, but closes on BOTH sides of it — the series simply does not
        trade often (or is pointed at a listing that barely trades). Rather than marking the
        position at a close months stale, the price is LINEARLY INTERPOLATED between the two
        real closes that bracket the date. It is an estimate, so it is flagged as one and the
        span it was drawn across comes back with it: the caller must be able to say so, because
        a synthetic price that renders like a real one is worse than no price at all.

      * a bracket wider than `_MAX_INTERP_SPAN_DAYS` — refused (None). Straight-lining a price
        across more than a year is not interpolation, it is invention.

    Interpolating in EUR (the series is already converted) means the estimate carries the FX
    path too, which is the quantity we actually want: it is the euro value of the position.
    """
    if not series:
        return None
    prev = _at_or_before(series, anchor)
    nxt = next(((d, p) for d, p in series if d > anchor), None)

    if prev and _days_between(anchor, prev[0]) <= _MARK_GAP_TOLERANCE_DAYS:
        return prev[0], prev[1], False, 0

    if prev and nxt:
        d0, p0 = prev
        d1, p1 = nxt
        span = _days_between(d1, d0)
        if span > _MAX_INTERP_SPAN_DAYS or span <= 0:
            return None
        frac = _days_between(anchor, d0) / span
        return anchor, p0 + (p1 - p0) * frac, True, span

    # A close before the anchor and nothing after it: the series ENDS before the window opens.
    # Real, stale, and nothing to bracket it with — hand it back rather than inventing an end.
    if prev:
        return prev[0], prev[1], False, 0
    return None


def _days_between(a: str, b: str) -> int:
    return abs((date.fromisoformat(a) - date.fromisoformat(b)).days)


def _eur_return(series: list[tuple[str, float]], anchor: str) -> float | None:
    """EUR return from the last close on or before `anchor` to the latest close, in %."""
    first = _at_or_before(series, anchor)
    if not first or not series:
        return None
    d0, p0 = first
    d1, p1 = series[-1]
    if p0 <= 0 or d1 <= d0:
        return None
    return (p1 / p0 - 1.0) * 100.0


def _index(legs: list[tuple[float, list[tuple[str, float]] | None]],
           anchor: str) -> tuple[list[float], float]:
    """Daily EUR value of a buy-and-hold of `legs` from `anchor`, starting at 1.0, and the
    weight it was actually able to hold there.

    That second number is not a diagnostic, it is a GATE. A leg with no close on or before the
    anchor was not held at the anchor — an ETF that listed in 2025 cannot be in a model whose
    inception is 2024-06 — and this renormalises over the rest, so without the weight coming
    back out, a curve built from a quarter of the portfolio would render exactly like one built
    from all of it. YTD's `covered_pct` cannot stand in for it: coverage at Jan 1 says nothing
    about coverage two years earlier.

    A leg is `(weight, eur_series)`; `series is None` is CASH, which holds its value — a flat
    0% return, because cash's drag is a fact, not a gap.

    Each leg is bought at its last close on or before the anchor and held. The curve is sampled
    on the UNION of the legs' trading dates and each leg is forward-filled onto it: holdings sit
    on different exchange calendars (a Tokyo holiday is a normal Wednesday in Paris), so an
    intersection would throw most of the window away, and a gap would have to be read as either
    a missing day or a 0% day. Forward-filling reads it as what it is — the position still held,
    at its last known price.

    The end value is Σ wᵢ·(Pᵢ(T)/Pᵢ(anchor)) / Σ wᵢ, which is exactly the weighted point-to-point
    return `_eur_return` computes — the curve is the same number, sampled daily. That identity is
    the reason `since_model_pct` is read off the curve rather than computed twice.
    """
    tracks: list[tuple[float, list[tuple[str, float]] | None, float]] = []
    w_total = 0.0
    for w, s in legs:
        if s is None:
            tracks.append((w, None, 1.0))
            w_total += w
            continue
        # The SAME mark the expanded per-holding rows are drawn from (`_mark_at`) — interpolation
        # included. If the curve took a raw `_at_or_before` base while the rows interpolated,
        # the itemised returns would no longer weight to the portfolio's, and the table would be
        # explaining its own number with different arithmetic.
        m = _mark_at(s, anchor)
        if not m or m[1] <= 0:
            continue                     # not held at the anchor — it has no window to measure
        tracks.append((w, s, m[1]))
        w_total += w
    if not tracks or w_total <= 0:
        return [], 0.0

    dates = sorted({d for _, s, _ in tracks if s for d, _ in s if d > anchor})
    if not dates:
        return [], w_total

    cursor = [0] * len(tracks)
    rel = [1.0] * len(tracks)            # price relative to base; 1.0 at the anchor by construction
    values = [1.0]
    for d in dates:
        v = 0.0
        for i, (w, s, base) in enumerate(tracks):
            if s is not None:
                j = cursor[i]
                while j < len(s) and s[j][0] <= d:
                    rel[i] = s[j][1] / base
                    j += 1
                cursor[i] = j
            v += w * rel[i]
        values.append(v / w_total)
    return values, w_total


def _daily_returns(values: list[float]) -> list[float]:
    return [values[i] / values[i - 1] - 1.0
            for i in range(1, len(values)) if values[i - 1] > 0]


def ytd_anchor_for(effective: str | None, year: int | None = None) -> str:
    """The date a YTD window opens for a composition that took effect on `effective`:
    `max(1 Jan, inception)`.

    ONE definition, exported, because the per-holding marks shown when a portfolio is expanded
    must be anchored on the SAME day as the row's YTD. Anything else and the entry prices printed
    underneath a +51.48% belong to a different window, and the two quietly fail to reconcile —
    which is precisely the kind of disagreement a reader trusts one half of."""
    jan1 = f"{year or date.today().year}-01-01"
    return effective if (effective and effective > jan1) else jan1


def compute_portfolio_performance(year: int | None = None) -> list[dict]:
    """Per-portfolio YTD (EUR), plus the return / Sharpe / Sortino since the composition's own
    effective date — the window in which its weights were actually held."""
    year = year or date.today().year
    jan1 = f"{year}-01-01"
    today = date.today().isoformat()

    ports = (supabase.table("airs_model_portfolio")
             .select("id,name,positions_datum,positions_scanned_at")
             .not_.is_("positions_datum", "null").execute().data or [])
    if not ports:
        return []

    pos = (supabase.table("airs_model_portfolio_position")
           .select("portfolio_id,isin,percentage").execute().data or [])
    by_pf: dict[int, list[dict]] = {}
    for r in pos:
        by_pf.setdefault(r["portfolio_id"], []).append(r)

    # Prices have to reach back to the OLDEST inception, not just to January: since-inception is
    # measured from the model's own date, and the oldest is 2024-06-29.
    earliest = min([jan1, *(p["positions_datum"] for p in ports if p["positions_datum"])])
    lookback = (date.fromisoformat(earliest)
                - timedelta(days=_ANCHOR_LOOKBACK_DAYS)).isoformat()

    isins = sorted({r["isin"] for r in pos if r.get("isin")})
    ex = _executions(isins)
    ids = sorted({e["analysis_id"] for e in ex.values()})
    closes = _closes(ids, lookback, today)
    # A sparse series may have no bar inside the window at all. Fetch its opening bar for EVERY
    # anchor in play (each portfolio's YTD window and its inception) — a real bar, so adding it
    # only makes the series more complete for all of them.
    for anchor in sorted({ytd_anchor_for(p["positions_datum"], year) for p in ports}
                         | {p["positions_datum"] for p in ports if p["positions_datum"]}):
        _prepend_opening_bars(closes, ids, anchor)

    # FX has to reach back to the OLDEST bar we ended up with, not just to `lookback` — a bar
    # with no rate on or before it is dropped by `_eur_series`, so a short FX window would throw
    # away the very opening bar we just went and fetched.
    fx_from = min([lookback, *(s[0][0] for s in closes.values() if s)])
    fx = _fx({e.get("currency") for e in ex.values()}, fx_from, today)
    eur: dict[int, list[tuple[str, float]]] = {
        e["analysis_id"]: _eur_series(closes[e["analysis_id"]], e.get("currency"), fx)
        for e in ex.values() if closes.get(e["analysis_id"])
    }

    out: list[dict] = []
    for p in ports:
        rows = by_pf.get(p["id"], [])
        # The composition's own start line — its INCEPTION. This is the only window in which the
        # weights we hold were the weights it held, so it is the only one whose return is
        # realized rather than backtested.
        eff = p["positions_datum"]

        # ⚠ THE YTD ANCHOR IS max(1 Jan, INCEPTION) — NOT 1 January. See `ytd_anchor_for`, which
        # the per-holding marks behind this row share, so the entry prices shown when a portfolio
        # is expanded are the entry prices this number was actually computed from.
        #
        # A model (re)defined during the year did not hold these weights in January, and AIRS
        # keeps no January composition to recover. Anchoring its YTD at 1 Jan therefore prices a
        # basket chosen KNOWING how the year had gone: MoTopSelectie_FX read +75.85% on a model
        # defined eight days earlier, and unflagged it was the best portfolio in the list. So the
        # window starts where the composition does, and the figure is a partial year — realized,
        # not backtested. `model_changed_in_period` says which rows those are, because a 6-day
        # return and a 12-month one are not comparable just because they share a column.
        ytd_anchor = ytd_anchor_for(eff, year)

        cash_w = 0.0
        has_cash = False
        total_w = 0.0
        legs: list[tuple[float, list[tuple[str, float]] | None]] = []
        # Counted as DISTINCT ISINs, not as rows — a model can list one instrument on two lines
        # (VTopSelectie OFF FX holds CapitaLand at 2% and again at 3%), and the Holdings column
        # counts `DISTINCT isin`. Counting rows here would report 29 resolved of 28 held.
        resolved: set[str] = set()
        unresolved: set[str] = set()
        # Holdings whose OPENING price is an interpolation, not a close. The portfolio's return
        # is partly synthetic when this is non-empty, and it is 23.6% of one of them — so the
        # count travels with the number rather than living only in the expanded row.
        interpolated: set[str] = set()

        for r in rows:
            w = float(r.get("percentage") or 0)
            if w <= 0:
                continue
            total_w += w
            isin = r.get("isin")
            if not isin:
                # Cash. A 0% return is a FACT — its drag is real, so it is priced, not skipped.
                # It is NOT a resolved holding, though: it has no ISIN and no Yahoo series, and
                # the Holdings column doesn't count it either. Counting it would make a model
                # look one instrument better-covered than it is.
                cash_w += w
                has_cash = True
                legs.append((w, None))
                continue
            e = ex.get(isin)
            s = eur.get(e["analysis_id"]) if e else None
            # No opening mark at the anchor = not held there. `ytd_anchor >= eff` always, so a
            # holding with no mark HERE has none at inception either: it is priceable in neither
            # window and drops out of both.
            mark = _mark_at(s, ytd_anchor) if s else None
            if not e or not s or not mark:
                unresolved.add(isin)
                continue
            resolved.add(isin)
            if mark[2]:
                interpolated.add(isin)
            legs.append((w, s))

        # TWO curves, ONE builder, and every figure below is read off them — a cumulative return
        # is a curve's last point and the ratios are its daily steps. Computing a return a second
        # way is how two surfaces of the same portfolio end up disagreeing.
        ytd_curve, ytd_w = _index(legs, ytd_anchor)
        since_curve, since_w = _index(legs, eff) if eff else ([], 0.0)

        covered = (ytd_w / total_w * 100.0) if total_w > 0 else 0.0
        # Too little of the portfolio is priceable for a renormalised number to mean anything.
        # Return NOTHING rather than a precise-looking invention — see MIN_COVERAGE_PCT.
        enough = covered >= MIN_COVERAGE_PCT and ytd_w > 0
        if not enough:
            ytd_curve = []

        # The same floor, applied to the window it actually belongs to. A holding that had not
        # listed yet at inception is not priceable THERE, whatever its coverage at Jan 1 says.
        # (When the model is younger than the year the two windows coincide and so do the two.)
        since_covered = (since_w / total_w * 100.0) if total_w > 0 else 0.0
        if since_covered < MIN_COVERAGE_PCT:
            since_curve = []

        ytd_pct = (ytd_curve[-1] - 1.0) * 100.0 if ytd_curve else None
        since_pct = (since_curve[-1] - 1.0) * 100.0 if since_curve else None

        rets = _daily_returns(since_curve)
        stats = (annualized_stats(rets, periods_per_year=TRADING_DAYS)
                 if len(rets) >= MIN_STAT_DAYS else None)

        out.append({
            "portfolio_id": p["id"],
            "name": p["name"],
            "model_effective": eff,
            # This model is YOUNGER than the year, so its "YTD" starts at its inception, not at
            # 1 January — a PARTIAL year. The number is realized (we no longer backtest the
            # weights back to January), but it covers days, not months, and it sits in the same
            # column as a full-year return. Say so.
            "model_changed_in_period": bool(eff and eff > jan1),
            # Anchored at `ytd_anchor` = max(1 Jan, inception). Where the model predates the
            # year, that IS 1 Jan and this is a true YTD.
            "ytd_pct": ytd_pct,
            "ytd_from": ytd_anchor if enough else None,
            "since_model_pct": since_pct,
            # Sharpe/Sortino over the since-inception window, rf = 0. Absent — not zero — when
            # the model is younger than MIN_STAT_DAYS trading days, or when its curve never
            # moved (a zero denominator is undefined, not infinite).
            "sharpe": stats.sharpe if stats else None,
            "sortino": stats.sortino if stats else None,
            # The geometric annualized return over that same window — and ONLY when the window is
            # at least a year long. Compounding a four-month return out to a year is an
            # extrapolation, not a measurement; `since_model_pct` is the realized number.
            "cagr_pct": ((stats.ann_return * 100.0)
                         if (stats and stats.ann_return is not None
                             and len(rets) >= MIN_CAGR_DAYS) else None),
            "ann_vol_pct": (stats.ann_vol * 100.0) if (stats and stats.ann_vol is not None)
                           else None,
            # How long that window is. A ratio over 22 days and one over 500 render identically;
            # this is what tells them apart.
            "stat_days": len(rets),
            # ...and the same thing in the unit a reader actually thinks in: how long this model
            # has been running, inception -> today, in calendar years. It is what makes an absent
            # CAGR legible (under 1.00 there is none) and what a Sharpe must be read against.
            "years_running": (_days_between(today, eff) / 365.25) if eff else None,
            # The since-inception column's own coverage — the reason a row can show a YTD and
            # no since-inception figure (a holding that had not listed at the model's inception).
            "since_covered_pct": since_covered if total_w > 0 else None,
            # DISTINCT instruments we can actually price off Yahoo (`asset_price`), and the ones
            # we cannot. Both exclude cash — it is not an instrument, and the Holdings column it
            # is read against doesn't count it either. `resolved_holdings + unresolved_holdings`
            # therefore reconciles exactly with that column.
            "resolved_holdings": len(resolved),
            "unresolved_holdings": len(unresolved),
            # How many of them were marked at an INTERPOLATED opening price rather than a real
            # close. Non-zero means part of this return is an estimate, and the row says so.
            "interpolated_holdings": len(interpolated),
            # Kept for the API's existing shape: every leg the curve holds, cash included.
            "priced_holdings": len(resolved) + (1 if has_cash else 0),
            "unpriced_holdings": len(unresolved),
            # How much of the model's weight we could price. Always returned, even when we
            # refuse the number — it is the reason we refused.
            "covered_pct": covered if total_w > 0 else None,
            "low_coverage": not enough,
            "partial_coverage": enough and covered < GOOD_COVERAGE_PCT,
            "cash_pct": cash_w,
        })

    out.sort(key=lambda r: (r["ytd_pct"] is None, -(r["ytd_pct"] or 0)))
    return out


async def compute_portfolio_performance_async(year: int | None = None) -> list[dict]:
    return await asyncio.to_thread(compute_portfolio_performance, year)


def compute_holding_marks(isins: list[str], anchor: str) -> dict[str, dict]:
    """Per-ISIN entry/exit marks over the window opening at `anchor`: what each holding was
    worth when the window opened, what it is worth now, and the EUR return between them.

    This is the arithmetic BEHIND a portfolio's YTD, one row at a time — so it reuses the same
    EUR series the curve is built from (`_split_adjust` → per-date FX), and its `return_pct` is
    the identical quantity `_index` weights into the portfolio figure.

    ⚠ THE PRICES ARE IN EUR, and that is not cosmetic. The return is an EUR return (it carries
    the FX leg), so printing the NATIVE closes beside it would show a reader two numbers whose
    ratio is not the third: Main Street Capital's local close can rise while the euro figure
    falls, purely on USD/EUR. The native close and the currency ride along too — but as the
    tooltip, never as the arithmetic.
    """
    if not isins:
        return {}
    lookback = (date.fromisoformat(anchor)
                - timedelta(days=_ANCHOR_LOOKBACK_DAYS)).isoformat()
    today = date.today().isoformat()

    ex = _executions(sorted(set(isins)))
    ids = sorted({e["analysis_id"] for e in ex.values()})
    closes = _closes(ids, lookback, today)
    # THE SAME opening-bar guarantee the portfolio figure gets. Without it this loader's shorter
    # window silently sees less history than the one it is supposed to be itemising, and a
    # sparsely-traded holding shows a blank row underneath a return it is part of.
    _prepend_opening_bars(closes, ids, anchor)

    fx_from = min([lookback, *(s[0][0] for s in closes.values() if s)])
    fx = _fx({e.get("currency") for e in ex.values()}, fx_from, today)

    out: dict[str, dict] = {}
    for isin, e in ex.items():
        raw = closes.get(e["analysis_id"])
        if not raw:
            continue
        adjusted, _ = _split_adjust(raw)
        native = dict(adjusted)
        eur = _eur_series(raw, e.get("currency"), fx)
        if not eur:
            continue

        # `last_close` is returned EVEN WHEN NO MARKS CAN BE COMPUTED, because it is the only
        # thing that distinguishes the two ways a priced holding still shows a blank row — and
        # they are opposite problems:
        #
        #   last_close BEFORE the window opened  -> the SERIES IS STALE. Meta Platforms is mapped
        #       correctly to META and has years of data, but its last close is 2026-07-02 while
        #       BUS_2.0_NEU_FX's window opens 2026-07-09: there is no price INSIDE the window, so
        #       no return over it can exist. Nothing is wrong with the mapping. The prices are old.
        #       (197 of 223 held instruments were stale when this was written — `asset_price` has
        #       no scheduled refresh the way `metric_data` does.)
        #
        #   no opening mark                      -> the holding was not there when the window
        #       opened (it listed later).
        #
        # Without this, both render as an unexplained dash and the reader is invited to go looking
        # for a broken mapping that is in fact fine.
        base = {"currency": e.get("currency"), "last_close": eur[-1][0]}

        mark = _mark_at(eur, anchor)
        if not mark:
            out[isin] = base
            continue
        d0, p0, interpolated, gap = mark
        d1, p1 = eur[-1]
        if p0 <= 0 or d1 <= d0:
            out[isin] = base             # the series ends at/before the window: stale, not broken
            continue
        out[isin] = {
            **base,
            "start_date": d0,
            "start_price_eur": p0,
            # An interpolated price has NO native close behind it — there was no trade that day.
            # Handing back the neighbouring day's local price would dress the estimate up as an
            # observation, so the field is simply empty.
            "start_price_local": None if interpolated else native.get(d0),
            "start_interpolated": interpolated,
            "start_gap_days": gap,
            "end_date": d1,
            "end_price_eur": p1,
            "end_price_local": native.get(d1),
            "return_pct": (p1 / p0 - 1.0) * 100.0,
        }
    return out
