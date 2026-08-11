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

from common.fx_load import load_fx_to_eur
from common.pg import load_rows_via_copy
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

# Rows requested per PostgREST page. Matches Supabase cloud's own response cap, so a full page
# costs one round-trip and no more. The paged readers never ASSUME the server honours it — see
# `_fx` — they advance by what actually came back.
_PAGE = 1000


_EXEC_COLS = "isin,analysis_id,currency,yahoo_symbol,name"


def _executions(isins: list[str]) -> dict[str, dict]:
    """ISIN -> its priceable execution row. The bridge between the AIRS world and ours.

    ONE COPY for the whole ISIN list instead of `ceil(len/200)` PostgREST round trips (the
    IN-clause goes in the URL, hence the chunking); the chunked loop stays as the fallback.

    The `r["isin"] not in out` guard reads like a "first listing wins" tie-break that would make
    this transport-order-dependent — it is not. ⚠ **`asset_execution.isin` carries a UNIQUE
    constraint** (`asset_execution_isin_key`; measured 16,613 rows / 16,613 distinct ISINs), so
    there is never a second row to lose a tie. Checked rather than assumed, because if a duplicate
    were possible the two transports could disagree about the winner and only under COPY.
    (Verified anyway on 300 ISINs: identical rows and identical winners from both paths.)
    """
    rows = load_rows_via_copy("asset_execution", _EXEC_COLS, "isin", isins)
    if rows is None:
        rows = []
        for i in range(0, len(isins), IN_CHUNK_SIZE):
            rows += (supabase.table("asset_execution").select(_EXEC_COLS)
                     .in_("isin", isins[i:i + IN_CHUNK_SIZE]).execute().data or [])
    out: dict[str, dict] = {}
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
            #
            # ⚠ THE SORT KEY MUST BE UNIQUE OR THE PAGING SILENTLY LOSES ROWS. `target_date`
            # alone is not: ~220 holdings share every trading day, so almost every 1,000-row
            # page boundary falls INSIDE a date. Postgres makes no promise about the order of
            # tied rows between two separate LIMIT/OFFSET queries, so a row can be served on
            # both pages or on neither — and a dropped opening bar re-marks a holding at a
            # different price, changing the portfolio return with no error anywhere. This path
            # only runs where COPY is unconfigured (`SUPABASE_DB_URL` absent), which is exactly
            # the local-vs-production asymmetry that makes such a bug show up in one and not
            # the other. `(target_date, analysis_id)` IS unique in `asset_price`.
            batch = (supabase.table("asset_price")
                     .select("analysis_id,target_date,close")
                     .in_("analysis_id", chunk)
                     .gte("target_date", start).lte("target_date", end)
                     .order("target_date").order("analysis_id")
                     .range(off, off + _PAGE - 1).execute().data or [])
            if not batch:
                break
            for r in batch:
                if r["close"] is not None:
                    out.setdefault(r["analysis_id"], []).append(
                        (r["target_date"], float(r["close"])))
            # Advance by what came back, not by what was asked for — see `_fx`.
            off += len(batch)
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
    # ⚠ `.tolist()` FIRST — DO NOT ZIP THE SERIES DIRECTLY. Iterating a pandas Series yields one
    # boxed scalar per element, and these columns are ARROW-backed, so every element costs an
    # ArrowExtensionArray.__iter__ step. Profiled 2026-08-11 on one Analyse call: **102,348 calls
    # to that iterator, 0.26s of self time** — the largest pure-Python cost in the endpoint, spent
    # entirely on boxing. `.tolist()` does the whole column in one C-level pass and hands back
    # native Python objects, after which this is an ordinary zip over lists.
    #
    # ⚠ NULLS ARRIVE AS `None` FROM AN ARROW COLUMN AND AS `nan` FROM A NUMPY ONE, and `pd.notna`
    # is gone now, so BOTH have to be handled here: `c is not None` catches Arrow, `c == c`
    # catches NaN. Dropping either check silently turns a missing close into a real price of
    # `nan`, which then propagates through every return computed off this series.
    dates = df[DATE_COL].dt.strftime("%Y-%m-%d").tolist()
    aids = df[ENTITY_COL].tolist()
    closes = df["close"].tolist()
    for aid, d, c in zip(aids, dates, closes, strict=True):
        if c is not None and c == c:
            out.setdefault(int(aid), []).append((d, float(c)))
    for s in out.values():
        s.sort()      # `load_series` already sorts by (entity, date); belt and braces
    return out


def _fx(currencies: set[str], start: str, end: str) -> dict[str, dict[str, float]]:
    """Rates keyed by MAJOR currency — `_rate` resolves a minor unit (GBp) to its base and
    applies the divisor. Asking `fx_rate` for "GBp" returns nothing at all: pence is a quoting
    convention, not a currency, and 343 of our rows are quoted in it.

    ⚠ THIS READ MUST PAGE, AND IT IS THE READ WHERE TRUNCATION IS INVISIBLE.
        PostgREST caps a response and truncates SILENTLY — at 1,000 rows on Supabase cloud and
        10,000 locally. This window spans every currency our holdings quote in, back to the
        oldest bar in the price load: measured 2026-08-03, that is 19,037 rows over 27
        currencies. Unpaged, it came back with exactly 10,000 of them locally — and there is
        NOTHING in the result that says so.

        What a missing rate does is worse than a missing price, because it is silent twice
        over. `_eur_series` drops any close with no rate on or before it, so a currency whose
        early rows were cut simply has no EUR series before the cut: the holding then has no
        mark at the anchor, is classed unpriceable, drops out of the basket, and the return is
        renormalised over the rest. No error, no gap in the table — a confident number computed
        over a different portfolio.

        Measured on AITopSelectie OFF FX: the truncated read returned TWD as 20 rows starting
        2026-05-27 (its real history goes back to 2014), so Taiwan Semiconductor — 5% of the
        model, with 6,606 price bars — silently left the basket and the YTD was renormalised
        over the other 19 names. THE CAP IS 10x TIGHTER IN PRODUCTION, so the two environments
        cut different currencies and reported different YTDs for the same model on the same
        code. That is the whole discrepancy: not a pricing difference, a paging bug that only
        one row limit at a time makes visible.

        `(rate_date, currency_code)` is the sort key because a page boundary that falls inside
        a tie can serve a row twice or never — see `_closes_paged`, same trap. And the loop
        advances by WHAT CAME BACK rather than by what it asked for: "a short page is the last
        page" is only true while the server's cap is at least the page size, which is the very
        assumption that just failed. Advancing by `len(rows)` and stopping on empty is correct
        under any cap, at the cost of one extra empty request per chunk.

    ⚠ THE BODY MOVED TO `common/fx_load.py` (2026-08-11) AND SO DID ITS TWIN'S. Every rule in the
    docstring above is a correctness rule with an incident behind it, and it had to be right in
    TWO places at once — here and in `_benchmark_index._fx_to_eur`, which each docstring already
    called the other's twin. They had drifted in exactly the way that arrangement invites: the
    benchmark side gained a one-request COPY fast path and this one did not, so the Analyse modal
    paid **17 sequential PostgREST requests for `fx_rate` (13,617 rows), 14 of them the same query
    differing only by `offset`**, while the benchmark did the same job in 4 COPYs. One definition
    now; the shared loader keeps the paging as its fallback, so behaviour is unchanged when the
    direct-Postgres path is unavailable.
    """
    return load_fx_to_eur(currencies, start, end)


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

    So the missing bar is fetched directly, and only for the series that lack it. Mutates `closes`
    in place; the bar is real history, so a series it is added to is simply more complete for every
    other anchor too.

    ⚠⚠ "NORMALLY NONE" WAS WRONG, AND IT WAS THE SLOWEST THING ON /management-dashboard. Measured
    2026-08-11 on the portfolios grid: **71 series lacked their opening bar**, each fetched by its
    own `analysis_id=eq.N … limit 1` round trip. Locally that is 483ms of a 6.4s endpoint; in
    production, where a PostgREST call is a ~60ms network hop rather than ~5ms, it is **~4.3
    seconds of latency spent one row at a time**. The models' inception dates reach back before the
    loaded window, so the "sparse listing" this was written for turns out to be the common case.
    A comment's guess about frequency is not a measurement, and this one was hiding an N+1.
    """
    missing = [aid for aid in analysis_ids
               if not closes.get(aid) or closes[aid][0][0] > anchor]
    if not missing:
        return

    # ⚠ ONE ANCHOR PER CALL IS WHAT MAKES THIS COLLAPSIBLE. Every row wants "the last close on or
    # before the SAME date", so `DISTINCT ON (analysis_id) … ORDER BY analysis_id, target_date DESC`
    # answers all of them in one pass — the identical result the loop produced, in one round trip.
    from common.pg import _run_copy  # noqa: PLC0415

    buf = _run_copy(
        "COPY (SELECT DISTINCT ON (analysis_id) analysis_id, target_date::text, close "
        "FROM asset_price WHERE analysis_id = ANY(%s::int[]) AND target_date <= %s "
        "AND close IS NOT NULL ORDER BY analysis_id, target_date DESC) TO STDOUT WITH CSV",
        (list(missing), anchor))
    if buf is not None:
        for line in buf.getvalue().decode().splitlines():
            aid_s, d, c = line.split(",")
            if c:
                closes.setdefault(int(aid_s), []).insert(0, (d, float(c)))
        return

    # ⚠ THE PER-SERIES FALLBACK STAYS, because COPY is optional (`SUPABASE_DB_URL` absent, psycopg
    # missing, a connection fault) and this bar is a CORRECTNESS input, not an optimisation: a
    # holding without it reads as unpriceable and silently leaves the basket.
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
           anchor: str, *, return_dates: bool = False):
    """Daily EUR value of a buy-and-hold of `legs` from `anchor`, starting at 1.0, and the
    weight it was actually able to hold there.

    `return_dates=True` additionally returns the trading dates the curve is sampled on, as
    `(dates, values, w_total)` — `dates` aligns with `values[1:]` (`values[0]` is the anchor
    base 1.0). This exists so the SAME buy-and-hold curve every /portfolios figure is read off
    can be turned into a DATED daily-return series and correlated across portfolios, without a
    second builder where the look-ahead bias could creep back in.

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
        return ([], [], 0.0) if return_dates else ([], 0.0)

    dates = sorted({d for _, s, _ in tracks if s for d, _ in s if d > anchor})
    if not dates:
        return ([], [], w_total) if return_dates else ([], w_total)

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
    if return_dates:
        return dates, values, w_total
    return values, w_total


def _daily_returns(values: list[float]) -> list[float]:
    return [values[i] / values[i - 1] - 1.0
            for i in range(1, len(values)) if values[i - 1] > 0]


# Look-through pricing needs at least this share of the LINKED model to be priceable, or the
# certificate's return is a renormalisation over too little of the basket to mean anything — the
# same floor `compute_portfolio_performance` applies to a portfolio, applied to the model behind
# a certificate. Below it, we look through to nothing and the row stays a dead row.
LOOKTHROUGH_MIN_COVERAGE = MIN_COVERAGE_PCT / 100.0

# The index base for a look-through level. Arbitrary — only ratios of it are ever read — but 100
# is the convention a reader expects from an "indexed to 100 at the start" series, and it keeps
# the synthetic value visibly distinct from a real share price.
_LOOKTHROUGH_BASE = 100.0


def _lookthrough_series(
    linked_rows: list[dict],
    ex: dict[str, dict],
    eur: dict[int, list[tuple[str, float]]],
) -> list[tuple[str, float]]:
    """A linked model's basket as a single EUR PRICE SERIES — the certificate's price we cannot
    get from Yahoo, reconstructed from the model it wraps.

    Some AIRS holdings are not instruments, they are other models wrapped as a Leonteq
    certificate (CH1381833321 "Star Selection Index" IS StarTopSelectie OFF FX). Yahoo has no
    listing for a structured product, so those rows are dead — no price, no return, weight lost
    from the coverage denominator. This looks THROUGH the certificate to the model and prices the
    basket instead.

    ⚠ ANCHOR-INDEPENDENT, ON PURPOSE. It returns an absolute level (a buy-and-hold return index
    of the model's CURRENT weights, based at 100 on the date the whole basket is first priceable),
    NOT a curve normalised to one anchor. That is what lets the SAME series price the certificate
    at the parent's YTD anchor AND at its inception: `level(t) / level(anchor)` is the model's
    buy-and-hold return over `[anchor, t]`, so the certificate reconciles into the parent exactly
    like a stock does, at any window. A per-anchor normalised curve would need rebuilding for
    every parent that holds it and could not be marked before its own anchor.

    We hold only the CURRENT composition (AIRS keeps no weight history), so the weights are
    applied from the base date — the standard approximation for a certificate whose NAV history
    we cannot see. Priced from the SAME EUR series as everything else (`eur`), renormalised over
    what is priceable, cash held flat. ONE LEVEL DEEP: a certificate held INSIDE the linked model
    is just unpriceable here and drops out — which also makes a link cycle impossible to recurse
    into.

    `ex`  : isin -> execution row (as `_executions` returns).
    `eur` : analysis_id -> EUR close series (as `compute_portfolio_performance` builds).
    Returns [] when too little of the linked model is priceable to look through to honestly.
    """
    total_w = 0.0
    # (weight, eur_series | None-for-cash), only the legs we can actually price.
    legs: list[tuple[float, list[tuple[str, float]] | None]] = []
    priced_w = 0.0
    for r in linked_rows:
        w = float(r.get("percentage") or 0)
        if w <= 0:
            continue
        total_w += w
        isin = r.get("isin")
        if not isin:
            legs.append((w, None))          # cash — holds its value, a flat leg
            priced_w += w
            continue
        e = ex.get(isin)
        s = eur.get(e["analysis_id"]) if e else None
        if s:
            legs.append((w, s))
            priced_w += w
        # else: unpriceable within the linked model (its own structured products / nested
        # certificates) — renormalised over the rest, exactly as a portfolio's own figure is.

    if total_w <= 0 or priced_w / total_w < LOOKTHROUGH_MIN_COVERAGE:
        return []
    stock_legs = [(w, s) for w, s in legs if s is not None]
    if not stock_legs:
        return []                            # an all-cash basket has no price path to track

    # Base = the date by which EVERY priced stock leg has a close, so none is dropped for "not
    # held at the base" and the index opens with the full basket. A model whose newest holding
    # listed recently is therefore only priceable FROM then — honest: we cannot price the whole
    # basket back before its last constituent existed.
    base = max(s[0][0] for _, s in stock_legs)
    per_base: list[tuple[float, list[tuple[str, float]] | None, float | None]] = []
    for w, s in legs:
        if s is None:
            per_base.append((w, None, None))
            continue
        b = _at_or_before(s, base)
        per_base.append((w, s, b[1] if b else None))

    dates = sorted({d for _, s, _ in per_base if s for d, _ in s if d >= base})
    if not dates:
        return []

    cursor = [0] * len(per_base)
    rel = [1.0] * len(per_base)              # price relative to base; 1.0 at the base by construction
    out: list[tuple[str, float]] = []
    for d in dates:
        v = 0.0
        for i, (w, s, b0) in enumerate(per_base):
            if s is not None and b0:
                j = cursor[i]
                while j < len(s) and s[j][0] <= d:
                    rel[i] = s[j][1] / b0
                    j += 1
                cursor[i] = j
            v += w * rel[i]
        out.append((d, _LOOKTHROUGH_BASE * v / priced_w))
    return out


def ytd_anchor_for(effective: str | None, year: int | None = None) -> str:
    """The date a YTD window opens for a composition that took effect on `effective`:
    `max(1 Jan, inception)`.

    ONE definition, exported, because the per-holding marks shown when a portfolio is expanded
    must be anchored on the SAME day as the row's YTD. Anything else and the entry prices printed
    underneath a +51.48% belong to a different window, and the two quietly fail to reconcile —
    which is precisely the kind of disagreement a reader trusts one half of."""
    jan1 = f"{year or date.today().year}-01-01"
    return effective if (effective and effective > jan1) else jan1


def compute_portfolio_performance(year: int | None = None, *,
                                  only_portfolio_id: int | None = None,
                                  trace_portfolio_id: int | None = None,
                                  trace_out: dict | None = None) -> list[dict]:
    """Per-portfolio YTD (EUR), plus the return / Sharpe / Sortino since the composition's own
    effective date — the window in which its weights were actually held.

    `trace_portfolio_id` + `trace_out` INSTRUMENT this function rather than reproducing it.
    When both are given, `trace_out` is filled with every input and intermediate behind that one
    portfolio's number: the load fingerprint, the anchor, and one row per leg with the mark it
    was bought at, the close it is marked to, its EUR return and its contribution in percentage
    points. The contributions sum to `ytd_pct` exactly (`reconciles`), because they are read off
    the very legs `_index` weights — a diagnostic that computed the number a second way could
    agree with itself while disagreeing with the grid, which is the one thing it must not do.
    Tracing costs a few dicts and changes nothing about the result.
    """
    year = year or date.today().year
    jan1 = f"{year}-01-01"
    today = date.today().isoformat()

    ports = (supabase.table("airs_model_portfolio")
             .select("id,name,positions_datum,positions_scanned_at")
             .not_.is_("positions_datum", "null").execute().data or [])
    # ⚠ ONE PORTFOLIO'S NUMBERS SHOULD NOT COST FIFTY-SIX PORTFOLIOS' PRICES. The Analyse modal
    # reads exactly one row out of this function's output, and paid for the whole fleet to get it
    # — every model's holdings resolved, priced and FX-converted, then 55/56 of that thrown away.
    # Measured: 5.56s to open the modal.
    #
    # ⚠ AND IT IS THE SAME ANSWER, NOT AN APPROXIMATION OF IT. The only fleet-wide inputs are the
    # price and FX WINDOWS, and both are lower bounds: `lookback` reaches back to the earliest
    # inception, `fx_from` to the oldest bar loaded. Narrowing them to one portfolio's own history
    # cannot change ITS marks — `_prepend_opening_bars` fetches each anchor's opening bar
    # explicitly however far back it sits, which is exactly why that guarantee exists. Verified by
    # comparing every field against the fleet-wide run.
    #
    # `explain_portfolio_ytd` deliberately does NOT use this: its job is to reproduce the grid
    # byte for byte while a discrepancy is being chased, and "we narrowed the window" is precisely
    # the kind of difference that would then have to be ruled out by hand.
    if only_portfolio_id is not None:
        ports = [p for p in ports if p["id"] == only_portfolio_id]
    if not ports:
        return []

    pos = (supabase.table("airs_model_portfolio_position")
           .select("portfolio_id,isin,percentage,fonds").execute().data or [])
    by_pf: dict[int, list[dict]] = {}
    for r in pos:
        by_pf.setdefault(r["portfolio_id"], []).append(r)

    # Link context, loaded ONCE for the whole pass: which holdings are certificates wrapping
    # another model, so those dead rows can be priced by looking THROUGH to the model behind them.
    # `_load_context` is three full-table reads — resolving each portfolio off a shared copy keeps
    # this a single load rather than one per portfolio. A stored manual link wins over the guess.
    from routers._airs_portfolio_links import (  # noqa: PLC0415
        _load_context,
        link_key,
        resolve_links,
    )
    link_ctx = _load_context(supabase)
    # A linked model's basket, priced once and shared by every parent that holds its certificate.
    lookthrough_cache: dict[int, list[tuple[str, float]]] = {}

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
    # Latest FX rate date behind every EUR figure — surfaced for per-value traceability, not recomputed.
    fx_asof = max((d for rates in fx.values() for d in rates), default=None)
    eur: dict[int, list[tuple[str, float]]] = {
        e["analysis_id"]: _eur_series(closes[e["analysis_id"]], e.get("currency"), fx)
        for e in ex.values() if closes.get(e["analysis_id"])
    }

    if trace_out is not None:
        # The SHARED inputs — what this process actually loaded, before any one portfolio is
        # priced. Two deployments computing different numbers off the same code differ HERE
        # first: a different price transport, a shorter FX history, a stale `asset_price`.
        from common.pg import copy_path_enabled  # noqa: PLC0415
        trace_out["load"] = {
            "server_today": today,
            "year": year,
            "price_transport": "copy" if copy_path_enabled() else "postgrest-paged",
            "price_window": {"from": lookback, "to": today},
            "fx_window": {"from": fx_from, "to": today},
            "isins_in_all_compositions": len(isins),
            "isins_with_execution": len(ex),
            "analysis_ids": len(ids),
            "series_loaded": sum(1 for s in closes.values() if s),
            # The freshest close ANYWHERE in the load. A whole-fleet staleness gap between two
            # environments shows up as this one date, not as 220 leg rows.
            "latest_close_loaded": max((s[-1][0] for s in closes.values() if s), default=None),
            "fx_currencies": {
                c: {"n": len(r), "from": min(r), "to": max(r)}
                for c, r in sorted(fx.items()) if r
            },
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

        # One dict per composition row, recorded ONLY for the portfolio being traced. Populated
        # from the same branches that build `legs`, so a leg that is missing here is a leg the
        # return does not have either.
        tracing = trace_out is not None and p["id"] == trace_portfolio_id
        leg_trace: list[dict] = []

        # Which of this portfolio's rows are certificates wrapping another model — resolved off
        # the shared context, so a dead certificate row can be priced by looking THROUGH it.
        links = resolve_links(supabase, p["id"],
                              [{"isin": r.get("isin"), "fonds": r.get("fonds")} for r in rows],
                              context=link_ctx)

        for r in rows:
            w = float(r.get("percentage") or 0)
            if w <= 0:
                if tracing:
                    leg_trace.append({"isin": r.get("isin"), "fonds": r.get("fonds"),
                                      "weight": w, "status": "zero_weight"})
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
                if tracing:
                    leg_trace.append({"isin": None, "fonds": r.get("fonds"), "weight": w,
                                      "status": "cash", "return_pct": 0.0})
                continue
            e = ex.get(isin)
            s = eur.get(e["analysis_id"]) if e else None
            lookthrough = False
            # No Yahoo series, but the row may be a certificate wrapping ANOTHER model — price it
            # by looking through to that model's basket. Anchor-independent, so the one series
            # marks correctly at both `ytd_anchor` and `eff` below.
            if s is None:
                lk = links.get(link_key(isin, r.get("fonds")))
                tgt = lk.linked_portfolio_id if lk else None
                if tgt and tgt != p["id"]:
                    if tgt not in lookthrough_cache:
                        lookthrough_cache[tgt] = _lookthrough_series(
                            by_pf.get(tgt, []), ex, eur)
                    s = lookthrough_cache[tgt] or None
                    lookthrough = s is not None
            # No opening mark at the anchor = not held there. `ytd_anchor >= eff` always, so a
            # holding with no mark HERE has none at inception either: it is priceable in neither
            # window and drops out of both.
            mark = _mark_at(s, ytd_anchor) if s else None
            if not s or not mark:
                unresolved.add(isin)
                if tracing:
                    leg_trace.append({
                        "isin": isin, "fonds": r.get("fonds"), "weight": w,
                        "analysis_id": e["analysis_id"] if e else None,
                        "yahoo_symbol": e.get("yahoo_symbol") if e else None,
                        "currency": e.get("currency") if e else None,
                        # Three different failures that all render as a missing row: no bridge
                        # into our instrument grid at all, a bridge with no price series behind
                        # it, and a series that simply does not reach back to the anchor.
                        "status": ("no_execution" if not e
                                   else "no_price_series" if not s
                                   else "no_mark_at_anchor"),
                        "series_bars": len(s) if s else 0,
                        "series_first": s[0][0] if s else None,
                        "series_last": s[-1][0] if s else None,
                    })
                continue
            resolved.add(isin)
            if mark[2]:
                interpolated.add(isin)
            legs.append((w, s))
            if tracing:
                _d1, _p1 = s[-1]
                leg_trace.append({
                    "isin": isin, "fonds": r.get("fonds"), "weight": w,
                    "analysis_id": None if lookthrough else (e["analysis_id"] if e else None),
                    "yahoo_symbol": None if lookthrough else (e.get("yahoo_symbol") if e else None),
                    "currency": None if lookthrough else (e.get("currency") if e else None),
                    "status": "priced",
                    "lookthrough": lookthrough,
                    "series_bars": len(s),
                    "series_first": s[0][0],
                    "series_last": _d1,
                    "start_date": mark[0],
                    "start_price_eur": mark[1],
                    "start_interpolated": mark[2],
                    "start_gap_days": mark[3],
                    "end_date": _d1,
                    "end_price_eur": _p1,
                    "return_pct": (_p1 / mark[1] - 1.0) * 100.0,
                })

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

        # The latest yfinance close date behind THIS model's return — the max over the legs it
        # actually priced (a leg with a stale series pins its own older date). Surfaced, not
        # recomputed, for the per-value ⓘ on the grid.
        yf_asof = max((s[-1][0] for _, s in legs if s), default=None)
        scanned = p.get("positions_scanned_at")

        if tracing:
            # THE ARITHMETIC, not a re-derivation of it. `_index`'s end value is
            # Σ wᵢ·(Pᵢ(T)/Pᵢ(anchor)) / Σwᵢ, so each priced leg's contribution in percentage
            # points is its own EUR return scaled by the weight it holds AFTER renormalisation
            # over what could be priced. `reconciles` asserts that: three columns that don't sum
            # to the number they explain are not an explanation of it.
            for leg in leg_trace:
                leg["weight_pct_of_priced"] = (
                    leg["weight"] / ytd_w * 100.0
                    if ytd_w > 0 and leg["status"] in ("priced", "cash") else None)
                leg["contribution_pp"] = (
                    leg["weight"] / ytd_w * leg.get("return_pct", 0.0)
                    if ytd_w > 0 and leg["status"] in ("priced", "cash") else None)
            contrib = sum(leg["contribution_pp"] or 0.0 for leg in leg_trace)
            leg_trace.sort(key=lambda x: -(x["contribution_pp"] or 0.0))
            trace_out["portfolio"] = {
                "portfolio_id": p["id"],
                "name": p["name"],
                # The composition's effective date — AIRS's own, and the thing most likely to
                # differ between two environments that have scanned at different times. It
                # decides the anchor, which decides the whole window.
                "positions_datum": eff,
                "positions_scanned_at": str(scanned) if scanned else None,
                "jan1": jan1,
                "ytd_anchor": ytd_anchor,
                "anchor_is_inception": bool(eff and eff > jan1),
                "composition_rows": len(rows),
                "total_weight": total_w,
                "priced_weight": ytd_w,
                "covered_pct": covered,
                "low_coverage": not enough,
                "cash_pct": cash_w,
                "resolved_holdings": len(resolved),
                "unresolved_holdings": len(unresolved),
                "interpolated_holdings": len(interpolated),
                "ytd_pct": ytd_pct,
                "ytd_curve_points": len(ytd_curve),
                "since_model_pct": since_pct,
                "since_covered_pct": since_covered,
                "stat_days": len(rets),
                "latest_close_in_portfolio": yf_asof,
                "sum_of_contributions_pp": contrib,
                "reconciles": ytd_pct is not None and abs(contrib - ytd_pct) < 1e-6,
            }
            trace_out["legs"] = leg_trace

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
            # Where these numbers came from, as-of when — the model return is a yfinance close
            # series converted at FX over a composition scraped from AIRS.
            "sources": {
                "yf_close": yf_asof,
                "fx": fx_asof,
                "model_scan": str(scanned)[:10] if scanned else None,
            },
        })

    out.sort(key=lambda r: (r["ytd_pct"] is None, -(r["ytd_pct"] or 0)))
    return out


async def compute_portfolio_performance_async(year: int | None = None) -> list[dict]:
    return await asyncio.to_thread(compute_portfolio_performance, year)


def explain_portfolio_ytd(portfolio_id: int, year: int | None = None) -> dict:
    """Everything behind ONE portfolio's YTD, for diffing two environments against each other.

    The whole fleet is priced, not just this portfolio — deliberately. The price window reaches
    back to the OLDEST inception across all models and the FX window to the oldest bar that load
    returned, so a single-portfolio load would read a shorter window than the grid does and could
    answer differently for reasons that have nothing to do with the discrepancy being chased. A
    diagnostic that does not reproduce the number it is explaining is worse than none.

    Read it top-down: `load` is what this deployment fetched (transport, windows, freshest close);
    `portfolio` is the window and the coverage; `legs` is one row per composition line, ordered by
    contribution, each carrying the mark it was bought at and the close it is marked to. Diff two
    environments in that order — the first level that differs is the cause, and the levels below
    it are consequences.
    """
    trace: dict = {}
    rows = compute_portfolio_performance(
        year, trace_portfolio_id=portfolio_id, trace_out=trace)
    if "portfolio" not in trace:
        # Either no such portfolio, or it has no `positions_datum` (never scanned a composition)
        # and is filtered out of the fleet before pricing. Say which.
        exists = (supabase.table("airs_model_portfolio").select("id,name,positions_datum")
                  .eq("id", portfolio_id).limit(1).execute().data or [])
        trace["portfolio"] = None
        trace["error"] = ("no such model portfolio" if not exists
                          else "no composition stored (positions_datum is null) — nothing to price")
    trace["row"] = next((r for r in rows if r["portfolio_id"] == portfolio_id), None)
    return trace


async def explain_portfolio_ytd_async(portfolio_id: int, year: int | None = None) -> dict:
    return await asyncio.to_thread(explain_portfolio_ytd, portfolio_id, year)


def compute_holding_marks(isins: list[str], anchor: str,
                          *, linked: dict[str, int] | None = None) -> dict[str, dict]:
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

    `linked` maps a certificate's ISIN to the model portfolio it wraps. Those rows have no Yahoo
    series of their own, so their marks are priced by looking THROUGH to that model's basket —
    the SAME `_lookthrough_series` the portfolio figure uses, at the SAME anchor, so the row's
    return is exactly the leg the parent's YTD weights in.
    """
    if not isins:
        return {}
    linked = linked or {}
    lookback = (date.fromisoformat(anchor)
                - timedelta(days=_ANCHOR_LOOKBACK_DAYS)).isoformat()
    today = date.today().isoformat()

    # A certificate is priced from the model it wraps, so those models' compositions join the
    # price load — their member ISINs are what the look-through basket is built from.
    linked_rows_by_pf: dict[int, list[dict]] = {}
    if linked:
        pids = sorted(set(linked.values()))
        for i in range(0, len(pids), IN_CHUNK_SIZE):
            got = (supabase.table("airs_model_portfolio_position")
                   .select("portfolio_id,isin,percentage")
                   .in_("portfolio_id", pids[i:i + IN_CHUNK_SIZE]).execute().data or [])
            for r in got:
                linked_rows_by_pf.setdefault(r["portfolio_id"], []).append(r)
    member_isins = {r["isin"] for rows in linked_rows_by_pf.values()
                    for r in rows if r.get("isin")}

    ex = _executions(sorted(set(isins) | member_isins))
    ids = sorted({e["analysis_id"] for e in ex.values()})
    closes = _closes(ids, lookback, today)
    # THE SAME opening-bar guarantee the portfolio figure gets. Without it this loader's shorter
    # window silently sees less history than the one it is supposed to be itemising, and a
    # sparsely-traded holding shows a blank row underneath a return it is part of.
    _prepend_opening_bars(closes, ids, anchor)

    fx_from = min([lookback, *(s[0][0] for s in closes.values() if s)])
    fx = _fx({e.get("currency") for e in ex.values()}, fx_from, today)

    # EUR series per analysis_id — reused by BOTH the itemised holdings below and the
    # look-through baskets, so a certificate is priced off the identical closes as everything.
    eur_by_aid: dict[int, list[tuple[str, float]]] = {}
    for e in ex.values():
        raw = closes.get(e["analysis_id"])
        if not raw:
            continue
        s = _eur_series(raw, e.get("currency"), fx)
        if s:
            eur_by_aid[e["analysis_id"]] = s

    out: dict[str, dict] = {}
    for isin in sorted(set(isins)):
        e = ex.get(isin)
        if not e:
            continue
        raw = closes.get(e["analysis_id"])
        if not raw:
            continue
        adjusted, _ = _split_adjust(raw)
        native = dict(adjusted)
        eur = eur_by_aid.get(e["analysis_id"])
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

    # LOOK-THROUGH marks: a certificate wrapping another model has no traded price of its own, so
    # its Start/End/Return come from that model's basket — indexed to 100 when the window opened
    # (a basket has no single share price; only its ratio, the return, is meaningful). Marked
    # through the SAME `_mark_at` at the SAME anchor as `_index` uses in the portfolio figure, so
    # weighting this row's return reproduces the parent's YTD exactly, just as a stock's does.
    # A direct listing, if one ever existed, always wins — look-through only fills a dead row.
    for isin, pid in linked.items():
        if out.get(isin, {}).get("return_pct") is not None:
            continue
        series = _lookthrough_series(linked_rows_by_pf.get(pid, []), ex, eur_by_aid)
        if not series:
            continue
        mark = _mark_at(series, anchor)
        if not mark:
            continue
        d0, p0, _interp, _gap = mark
        d1, p1 = series[-1]
        if p0 <= 0 or d1 <= d0:
            continue
        out[isin] = {
            # A basket is not quoted in one currency and has no native close — the EUR level IS
            # the number, and the tooltip says it is a look-through index, not a traded price.
            "currency": None,
            "last_close": d1,
            "start_date": d0,
            "start_price_eur": p0,
            "start_price_local": None,
            "start_interpolated": False,
            "start_gap_days": 0,
            "end_date": d1,
            "end_price_eur": p1,
            "end_price_local": None,
            "return_pct": (p1 / p0 - 1.0) * 100.0,
            "lookthrough": True,
        }
    return out
