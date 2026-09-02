"""The benchmark's return read off the INDEX ETF's own price series, in EUR.

WHAT THIS REPLACES, AND WHY
    `_asset_benchmark.compute_index` rebuilds an index from its constituents: 1,514 names for
    ACWI, each with a cap weight and two prices. It is the only way to get per-constituent index
    weights (the Brinson attribution needs them), and it is a poor way to get the HEADLINE number.
    Measured 2026-08-19, ACWI YTD:

        our reconstruction              +11.83% EUR
        iShares ACWI ETF, price return  +13.14% USD  ->  +14.67% EUR
        iShares ACWI ETF, total return  +13.85% USD  ->  +15.39% EUR

    The 2.8pp gap is structural, not noise. The rebuild weights on FULL market cap where MSCI
    float-adjusts (Saudi Aramco: 1.50% of our rebuild against 0.044% published — 34x), prices
    1,678 of a 1,998-member universe against the real index's ~2,270 lines and redistributes the
    rest, holds a static membership snapshot, and lets every constituent run to its own last close
    with no common as-of date. None of that is fixable by arithmetic. The ETF has one price series
    and no coverage holes.

⚠⚠ THE CANARY WAS RUN BEFORE ANY OF THIS WAS BUILT, AND IT HAD TO BE. This repo's standing warning
    is that the GuruFocus legacy API NEVER 404s — `stock/{sym}/<anything>` returns 200 and a
    46-point all-zero series, so "it returned data" proves nothing. It does not apply to this call
    shape: `stock/ACWI/__canary__` genuinely 404s ("Stock not found, exchange [NAS], symbol
    [ACWI]"). Confirmed twice over by inception dates, which a placebo series cannot fake —
    ACWI's first bar is 2008-03-28 (the fund launched 26 Mar 2008), VT's 2008-06-26 (launched
    24 Jun), URTH's 2012-01-12 (launched 9 Jan), SPY's 1993-01-29 (launched 22 Jan).

⚠ THE TWO-VENDOR OBJECTION DOES NOT CARRY OVER, and it is worth saying why rather than leaving it
    to be re-litigated. The panel was moved off GuruFocus in 2026-07 because RECONSTRUCTING an
    index from a vendor with coverage holes silently redistributes the weight it cannot price —
    ~7.8% of ACWI, 31.96% of the AEX. That is an argument about summing 1,514 names, not about one
    US-listed line, which GuruFocus serves in full. On a single ETF close the vendors agree to the
    cent, and the benchmark remains the same KIND of number as the portfolio: an EUR price return
    over the same window, converted at each mark's own rate.

⚠ ONLY US-LISTED ETFs ARE REACHABLE. Every European UCITS line 404s (`IAEX`, `IWDA`, `IUSA` on
    AMS and XAMS), so the AEX has no proxy and keeps the reconstruction — which is the cheap one
    anyway (22 names, 2.3s) and the one whose 15% cap the rebuild already models.

⚠ THIS IS A PRICE RETURN, NOT A TOTAL RETURN. Distributions are excluded, exactly as the
    reconstruction's are, so the two remain the same KIND of number and the switch does not
    quietly change what the tile means as well as where it comes from. GuruFocus does serve the
    distributions (`stock/ACWI/dividend`: $1.0097 ex-15 Jun 2026, worth +0.71pp YTD) — adding a
    total-return line is a deliberate future step, not a side effect of this one.

⚠ THE ATTRIBUTION PANEL STILL RECONCILES TO THE RECONSTRUCTION, because it decomposes the index
    NAME BY NAME and an ETF price has no names in it. That is now two different benchmark numbers
    one click apart. It is survivable only because both panels SAY which one they are showing —
    `benchmarkSourceNote` on the tile, and the panel's own `account_excess_pct` vs sleeve
    `excess_pct` gap, written for exactly this hazard in its book/sleeve form. Anything that stops
    naming the difference turns it into a silent contradiction.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

from deps import supabase

_log = logging.getLogger(__name__)

# label -> the US-listed ETF whose price series IS that index, for our purposes.
#
# ⚠ AEX IS ABSENT ON PURPOSE, not pending. GuruFocus sells us continental Europe but not the
# Amsterdam UCITS lines that track it; every candidate 404s. An entry here with no series behind
# it would silently fall back and look like it worked.
PROXY: dict[str, str] = {
    "ACWI": "ACWI",     # iShares MSCI ACWI ETF (NASDAQ), 4,627 bars from 2008-03-28
    "SP500": "SPY",     # SPDR S&P 500 ETF Trust, 8,445 bars from 1993-01-29
}

_NAMES = {"ACWI": "iShares MSCI ACWI ETF", "SPY": "SPDR S&P 500 ETF Trust"}

# How far behind today the newest stored bar may be before we go back to the vendor. Three days
# clears a normal weekend; a long weekend costs one extra call and nothing else.
_STALE_DAYS = 3

# ⚠ ONE VENDOR CALL PER TICKER PER PROCESS PER DAY, AT MOST. Without this a stale series would be
# re-fetched by every Analyse-modal open that missed the leg cache — and the modal is ONE request
# with no partial paint, so a vendor round trip lands directly in the wait the reader sees.
_fetched_today: dict[tuple[str, str], bool] = {}


def _benchmark_id(ticker: str) -> int | None:
    """The `benchmark` row for this ticker, created if absent. None if it cannot be had.

    ⚠ GET-OR-CREATE RATHER THAN A MIGRATION. `benchmark` rows are DATA — the table already holds
    48 of them, all added by running code — so seeding ACWI through a migration would put one row
    of one environment's data in the schema history and still leave the next environment without
    it. This runs the same on local and prod, and twice is the same as once.
    """
    try:
        got = (supabase.table("benchmark").select("benchmark_id")
               .eq("ticker", ticker).limit(1).execute().data or [])
        if got:
            return got[0]["benchmark_id"]
        ins = (supabase.table("benchmark")
               .insert({"ticker": ticker, "name": _NAMES.get(ticker, ticker), "currency": "USD"})
               .execute().data or [])
        return ins[0]["benchmark_id"] if ins else None
    except Exception as exc:                                    # noqa: BLE001
        _log.warning("[bench-etf] %s: could not resolve a benchmark row: %s: %s",
                     ticker, type(exc).__name__, exc)
        return None


def _latest(bid: int) -> tuple[str, float] | None:
    """The newest stored close, or None.

    ⚠ TWO PRICES ARE ALL THIS MODULE EVER NEEDS, so it asks for two rather than reading a series.
    The first cut paged the whole thing — 4,627 rows for ACWI, five PostgREST round trips, 258 ms —
    to use the first and last of them, on the critical path of a modal that paints nothing until it
    is done. `.limit(1)` also sidesteps this repo's silent-truncation trap outright: there is no
    large read here to page wrongly.
    """
    rows = (supabase.table("benchmark_price").select("target_date,price")
            .eq("benchmark_id", bid).order("target_date", desc=True)
            .limit(1).execute().data or [])
    return (str(rows[0]["target_date"])[:10], float(rows[0]["price"])) if rows else None


def _at_or_before(bid: int, anchor: str) -> tuple[str, float] | None:
    """The last close ON OR BEFORE `anchor` — the same opening rule the reconstruction uses.

    None when the fund did not exist yet, which the caller reads as "fall back to the rebuild for
    this window" rather than as a missing price.
    """
    rows = (supabase.table("benchmark_price").select("target_date,price")
            .eq("benchmark_id", bid).lte("target_date", anchor)
            .order("target_date", desc=True).limit(1).execute().data or [])
    return (str(rows[0]["target_date"])[:10], float(rows[0]["price"])) if rows else None


def _refresh(ticker: str, bid: int, have_max: str | None) -> int:
    """Fetch the ETF from GuruFocus and store the bars we do not have. Returns rows written.

    ⚠ THE FULL HISTORY COMES BACK IN ONE CALL and we upsert only what is NEWER than `have_max`.
    The unfiltered endpoint is the one that stops at the last settled close — a WINDOWED fetch
    invents a bar dated today carrying yesterday's price whenever today has not settled (measured
    on AAPL, 2026-08-02). So: ask for everything, write the tail.
    """
    from ingest.api_usage import track_api_call  # noqa: PLC0415
    from ingest.constants import DATA_CUTOFF  # noqa: PLC0415
    from ingest.prices import _fetch_price_from_api, _parse_price_series  # noqa: PLC0415

    data, fetch_log, _status = _fetch_price_from_api(ticker, "NYSE")
    track_api_call(supabase, "NYSE")
    if not data:
        _log.warning("[bench-etf] %s: fetch returned nothing — %s", ticker, fetch_log)
        return 0
    # ⚠ THE SHARED PARSER, which already drops future-dated ticks. GuruFocus occasionally emits a
    # stray bar dated ahead of today; storing one is the SPMO +277% incident.
    parsed = _parse_price_series(data)
    rows = [{"benchmark_id": bid, "target_date": d.isoformat(), "price": p}
            for d, p in parsed
            if d >= DATA_CUTOFF and (have_max is None or d.isoformat() > have_max)]
    for i in range(0, len(rows), 500):
        supabase.table("benchmark_price").upsert(
            rows[i:i + 500], on_conflict="benchmark_id,target_date").execute()
    if rows:
        _log.warning("[bench-etf] %s: stored %d new bar(s), newest %s",
                     ticker, len(rows), rows[-1]["target_date"])
    return len(rows)


def ensure_fresh(label: str, *, force: bool = False) -> tuple[int, tuple[str, float]] | None:
    """`(benchmark_id, newest bar)` for this label's proxy ETF, refreshed if it has gone stale.

    None when there is no proxy, or when nothing can be had at all. Self-healing: the scheduled
    price phase normally keeps this current (`refresh_index_proxies`), and this is what makes the
    number right anyway on a box where that has not run yet.

    ⚠⚠ `force` IS WHAT MAKES THE SCHEDULED PATH ACTUALLY REFRESH, AND ITS ABSENCE WAS A BUG THAT
    LOOKED LIKE A WORKING JOB. `refresh_index_proxies` runs daily inside `price_update` and did
    nothing but call this — which refuses to fetch until the series is MORE than `_STALE_DAYS`
    behind. So the "daily refresh" was a no-op on any series under four days old: the tile could
    only ever be repaired once it was already badly stale, and in between it sat a day or two
    behind everything else on the page with nothing reporting it. Measured 2026-09-02: ACWI and
    SPY newest bar **2026-08-31** against a **2026-09-01** newest close in both `metric_data` and
    `asset_price`.

    ⚠ THE TWO CALLERS WANT OPPOSITE THINGS, WHICH IS WHY THIS IS A FLAG AND NOT A LOWER CONSTANT.
    The scheduled path is the one that SHOULD spend a vendor call every day — that is its whole
    job, and it runs where nobody is waiting. The reader path must not: it is inside the Analyse
    modal, ONE request with no partial paint, where a 1.35s GuruFocus round trip is the whole of
    somebody's wait. Lowering `_STALE_DAYS` instead would have moved the daily call onto the
    reader, which is exactly what the scheduled pass exists to prevent.
    """
    ticker = PROXY.get(label)
    if not ticker:
        return None
    bid = _benchmark_id(ticker)
    if bid is None:
        return None
    last = _latest(bid)
    # ⚠ CALENDAR DAYS, DELIBERATELY, ON THE LAZY PATH ONLY. A Friday close is the correct newest
    #   bar all weekend, so a trading-day rule here would buy nothing and a same-day rule would
    #   fetch on every Saturday open. `force` sidesteps the question entirely for the scheduled
    #   caller, which simply asks the vendor what it has.
    stale = last is None or date.fromisoformat(last[0]) < date.today() - timedelta(days=_STALE_DAYS)
    guard = (ticker, date.today().isoformat())
    if (force or stale) and not _fetched_today.get(guard):
        _fetched_today[guard] = True
        try:
            if _refresh(ticker, bid, last[0] if last else None):
                last = _latest(bid)
        except Exception as exc:                                # noqa: BLE001
            # ⚠ A VENDOR FAILURE COSTS FRESHNESS, NEVER THE NUMBER. What we already hold is still
            # a real price series; refusing to answer would blank a tile over a transient 500.
            _log.warning("[bench-etf] %s: refresh failed, serving what is stored: %s: %s",
                         ticker, type(exc).__name__, exc)
    return (bid, last) if last else None


def refresh_index_proxies() -> int:
    """Bring every proxy ETF up to date. For the scheduled price phase. Returns tickers refreshed.

    ⚠ SO THE 1.35s VENDOR CALL DOES NOT LAND ON A READER. `ensure_fresh` will do it lazily, but
    lazily means inside the Analyse modal — ONE request with no partial paint, where it is the
    whole of somebody's wait. Run daily beside the other benchmark prices, the lazy path becomes
    the repair it was meant to be rather than the normal case.

    ⚠⚠ `force=True`, AND WITHOUT IT THIS FUNCTION DID NOTHING. It called `ensure_fresh` plainly,
    which declines to fetch until the series is more than `_STALE_DAYS` (3) behind — so a job that
    runs every morning could not keep a series current, only rescue one already four days gone.
    The tile sat a day or two behind the rest of the page, every day, and nothing said so. This is
    the caller that is SUPPOSED to spend a vendor call daily; the freshness test belongs on the
    reader, not here.

    ⚠ The per-process, per-day guard inside `ensure_fresh` still applies, so a restart-looping box
    does not turn this into a fetch per boot.
    """
    n = 0
    for label in PROXY:
        try:
            if ensure_fresh(label, force=True):
                n += 1
        except Exception as exc:                                # noqa: BLE001
            _log.warning("[bench-etf] %s: proxy refresh failed: %s: %s",
                         label, type(exc).__name__, exc)
    return n


def etf_returns(label: str, starts: list[str]) -> dict[str, dict]:
    """`{start: {eur_pct, local_pct, as_of, start_date, ticker, currency}}` for the windows this
    ETF can price. A window that opens before the fund existed is ABSENT, not null — the caller
    falls back to the reconstruction for it rather than printing a dash.

    ⚠ THE SAME FX HELPERS AS THE RECONSTRUCTION (`_benchmark_index._fx_to_eur` / `_rate`), so
    there is ONE definition of "converted to EUR" on this page. Both marks are converted at THEIR
    OWN date's rate — converting both at today's rate would strip the currency leg out of the
    return entirely, and EUR is the return basis everywhere in this app. Measured on ACWI YTD the
    two differ by 1.5pp, which is the whole of the dollar's move over the window.
    """
    from routers._benchmark_index import _fx_to_eur, _rate  # noqa: PLC0415

    fresh = ensure_fresh(label)
    if not fresh or not starts:
        return {}
    bid, (end_d, end_p) = fresh
    ticker, ccy = PROXY[label], "USD"

    wanted = sorted(set(starts))
    # 45 days of run-up so an anchor on a holiday still finds a rate, matching `_window_rows`.
    lookback = (date.fromisoformat(min(wanted)) - timedelta(days=45)).isoformat()
    fx = _fx_to_eur({ccy}, lookback, end_d)
    r_end = _rate(fx, ccy, end_d)

    out: dict[str, dict] = {}
    for start in wanted:
        opened = _at_or_before(bid, start)
        if not opened:
            continue                        # the fund did not exist yet; the caller falls back
        start_d, start_p = opened
        r_start = _rate(fx, ccy, start_d)
        if start_p <= 0 or not r_start or not r_end or end_d <= start_d:
            continue
        out[start] = {
            "eur_pct": ((end_p / r_end) / (start_p / r_start) - 1.0) * 100.0,
            "local_pct": (end_p / start_p - 1.0) * 100.0,
            "start_date": start_d,
            "as_of": end_d,
            # ⚠ THE FOUR NUMBERS THE RETURN IS MADE OF, CARRIED SO THE ⓘ CAN SHOW ITS WORKING.
            # The card states the rule and then the same rule with this window's own figures under
            # it; without these it can only assert the method and ask to be believed. `fx_*` is the
            # rate in the ETF's currency PER EUR (1.1750 USD/EUR), which is the direction the
            # formula divides by — quoting it the other way up would make the worked line wrong
            # while the result stayed right, the hardest kind of error to notice.
            "start_price": start_p,
            "end_price": end_p,
            "fx_start": r_start,
            "fx_end": r_end,
            "ticker": ticker,
            "currency": ccy,
            # Named so a consumer can say WHERE the number came from rather than implying it is
            # the reconstruction's. `members` mirrors the rebuild's key and is 1 by construction:
            # an ETF is one instrument, and pretending otherwise would let a caller divide by it.
            "source": "etf",
            "members": 1,
        }
    return out
