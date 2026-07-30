"""Keep `asset_price` current: detect stale series and fetch only the gap.

WHY THIS EXISTS
    `metric_data` (GuruFocus, the `company` universe) is refreshed by the scheduler every day.
    `asset_price` (Yahoo, the `asset_execution` universe) was NOT — it was written when a row was
    added or explicitly refreshed, and then it simply aged. Nothing tells you: a stale series
    still returns prices, still charts, still computes a return. It is just an old one.

    Measured 2026-07-14, over the instruments held by the AIRS model portfolios:

        223 held instruments with prices
          1 current
        197 STALE                                       <- 88%

    And it is not cosmetic. A model portfolio whose window opens AFTER its holdings' last close
    has no price inside the window at all, so no return over it exists and the row renders blank:
    Meta Platforms — correctly mapped to META, 3,556 bars — showed nothing in BUS_2.0_NEU_FX
    (window opens 2026-07-09; Meta's last close was 2026-07-02). The mapping was never the problem.

⚠ STALENESS IS MEASURED AGAINST THE FRESHEST CLOSE WE HOLD, NEVER AGAINST TODAY.
    Anchoring on the calendar flags every row every weekend, calls a bank holiday a fleet-wide
    failure, and — worst — turns a total Yahoo outage into "refresh all 6,000 instruments". The
    global-latest anchor is the same one the delisting sweep uses, and it self-corrects: if
    nothing anywhere has published, nothing is stale.

⚠ THE GAP, NOT THE HISTORY.
    `store.store_series` re-downloads every bar an instrument has ever had (KO: 16,239, back to
    1962) — correct for a first load, absurd for a refresh, and over ~200 rows the difference
    between minutes and seconds. `store.extend_series` fetches only the window after the last
    close and recomputes the grid's coverage stats FROM THE DATABASE (see its docstring: deriving
    them from the fetched slice would record that KO has 8 bars beginning in 2026). It returns
    None when it cannot do that exactly, and then — and only then — we fall back to the full path.
"""
from __future__ import annotations

import logging
import time
from typing import Callable

from deps import IN_CHUNK_SIZE, supabase

log = logging.getLogger(__name__)

_PAGE = 1000

# Days behind the freshest close we hold before a series counts as stale. 1 would flag every
# market whose close simply hasn't published yet; 3 clears a weekend.
DEFAULT_STALE_DAYS = 3

# Yahoo answers an overloaded caller with an EMPTY result rather than a 429, so we never race it.
DEFAULT_SLEEP_S = 0.4


def _days_between(a: str, b: str) -> int:
    from datetime import date  # noqa: PLC0415
    return abs((date.fromisoformat(a) - date.fromisoformat(b)).days)


def _paged_isins(table: str) -> set[str]:
    rows, off = [], 0
    while True:
        b = (supabase.table(table).select("isin")
             .not_.is_("isin", "null").range(off, off + _PAGE - 1).execute().data or [])
        rows += b
        if len(b) < _PAGE:
            break
        off += _PAGE
    return {r["isin"] for r in rows if r.get("isin")}


def held_isins() -> set[str]:
    """Every ISIN an AIRS model portfolio NAMES or an AIRS account actually HOLDS — the instruments
    whose staleness surfaces to a reader.

    ⚠ BOTH TABLES, BECAUSE THEY ARE NOT THE SAME SET, AND THE SECOND ONE IS WHERE THE PRICE CHECK
    LOOKS. `airs_model_portfolio_position` is what a strategy SAYS to hold; `airs_holding` is what a
    book DOES hold — a legacy position the model has since dropped, an instrument bought between
    rebalances, a line the model never named. Refreshing only the first left exactly those
    instruments ageing for ever, and the per-holding price check on /management-dashboard reads the
    second: our months-old close against AIRS's current implied price is a >15% gap on any mover,
    reported as `price_mismatch` — i.e. "our listing is wrong" about a listing that is perfect.
    Same failure the market anchor fixed at the fleet level (`market_latest_close`), one level in:
    a series nothing refreshes cannot be found stale by any anchor.
    """
    return _paged_isins("airs_model_portfolio_position") | _paged_isins("airs_holding")


def _executions(isins: set[str] | None) -> list[dict]:
    cols = "isin,analysis_id,yahoo_symbol,name,status"
    rows: list[dict] = []
    if isins is None:
        off = 0
        while True:
            b = (supabase.table("asset_execution").select(cols).eq("status", "ok")
                 .not_.is_("analysis_id", "null").range(off, off + _PAGE - 1).execute().data or [])
            rows += b
            if len(b) < _PAGE:
                break
            off += _PAGE
        return rows

    ids = sorted(isins)
    for i in range(0, len(ids), IN_CHUNK_SIZE):
        rows += (supabase.table("asset_execution").select(cols).eq("status", "ok")
                 .not_.is_("analysis_id", "null")
                 .in_("isin", ids[i:i + IN_CHUNK_SIZE]).execute().data or [])
    return rows


def latest_close_by_analysis(ids: list[int]) -> dict[int, str]:
    """analysis_id -> its most recent stored close date, for the ids we care about.

    ONE grouped query, not a scan. `asset_price` is millions of rows: paging it through PostgREST
    to find each series' max is a Postgres statement timeout (57014) — which is exactly what the
    first version of this did. Falls back to one cheap indexed lookup per id without COPY.
    """
    if not ids:
        return {}
    from common.pg import _run_copy  # noqa: PLC0415

    # psycopg placeholders are `%s`, not `$1` — `$1` parses as zero placeholders and the COPY
    # silently degrades to the slow path behind a warning nobody reads.
    buf = _run_copy(
        "COPY (SELECT analysis_id, max(target_date)::text FROM asset_price "
        "WHERE analysis_id = ANY(%s) AND close IS NOT NULL GROUP BY analysis_id) TO STDOUT WITH CSV",
        (list(ids),),
    )
    if buf is not None:
        out: dict[int, str] = {}
        for line in buf.getvalue().decode().splitlines():
            aid, d = line.split(",", 1)
            out[int(aid)] = d
        return out

    out = {}
    for aid in ids:
        r = (supabase.table("asset_price").select("target_date").eq("analysis_id", aid)
             .not_.is_("close", "null")
             .order("target_date", desc=True).limit(1).execute().data or [])
        if r:
            out[aid] = r[0]["target_date"]
    return out


def global_latest_close() -> str | None:
    """The freshest close ANYWHERE. THE anchor everything else is measured against.

    ⚠ NOT `SELECT target_date FROM asset_price ORDER BY target_date DESC LIMIT 1`.
        That reads like one indexed row and is not. `asset_price`'s ONLY index is the primary
        key `(analysis_id, target_date)`; nothing leads with `target_date`, so Postgres has no
        ordered path to the newest date and falls back to a full scan + top-N sort over 14M+
        rows. Through PostgREST that is a statement timeout (57014) — measured, in production,
        and it takes the whole refresh with it because this is the FIRST thing `find_stale`
        asks for. `latest_close_by_analysis` is safe from the same trap only because its
        grouped aggregate goes over COPY, where the timeout is disabled.

    So the anchor comes from `asset_analysis.price_to` — the per-asset max that migration
    20260703010000 denormalized for exactly this reason, and that `store.store_series` /
    `store.extend_series` (the ONLY writers of `asset_price`) both maintain. Same fact, over a
    few thousand rows instead of fourteen million.

    The exact aggregate stays available as the fallback, but only over COPY (`statement_timeout
    = 0`), for a database whose denormalized column has never been populated. It is deliberately
    NOT the primary path: this function runs on every startup kickstart, and a 14M-row seq scan
    on every `uvicorn --reload` restart is not the near-free no-op that detection promises.
    """
    r = (supabase.table("asset_analysis").select("price_to")
         .not_.is_("price_to", "null")
         .order("price_to", desc=True).limit(1).execute().data or [])
    if r and r[0].get("price_to"):
        return r[0]["price_to"]

    from common.pg import _run_copy  # noqa: PLC0415
    buf = _run_copy(
        "COPY (SELECT max(target_date)::text FROM asset_price WHERE close IS NOT NULL) "
        "TO STDOUT WITH CSV",
        (),
    )
    if buf is None:
        return None
    line = buf.getvalue().decode().strip()
    return line or None


def market_latest_close() -> str | None:
    """The freshest close YAHOO has, from one probe of a symbol that trades every session.

    ⚠ THIS EXISTS BECAUSE A FLEET CANNOT SEE ITS OWN DRIFT. `global_latest_close` anchors on the
    newest close WE HOLD, which is right for a weekend, a holiday and a Yahoo outage — and blind to
    the one failure that matters most, because in it every row ages TOGETHER. Measured 2026-07-29
    on the local DB: the newest close anywhere was 2026-07-23, six days earlier; AMD's own last
    close was 2026-07-22, i.e. ONE day behind that anchor, so `0 of 232` held instruments were
    stale and the daily refresh had nothing to do — for ever. Meanwhile AMD had gone 552.33 →
    430.05 (−22%), and the AIRS price check was reporting the ~21% gap as though our LISTING were
    wrong. The mapping was perfect: NasdaqGS, USD, `AMD`, every stored bar matching Yahoo to the
    cent. The series had simply stopped, and nothing inside the database could tell.

    ⚠ IT IS AN ANCHOR, NEVER A PRICE. Nothing is stored from this call and no instrument is priced
    off the canary; it answers exactly one question — "has the market published since we last
    looked?" — and `find_stale` takes the LATER of this and our own maximum.

    ⚠ FAILURE MUST MEAN "USE OUR OWN MAXIMUM", NOT "EVERYTHING IS STALE". A throttled or dead probe
    returning None leaves the previous behaviour intact. Returning, say, today's date on failure
    would turn a Yahoo outage into a 6,000-instrument stampede at the one moment fetching cannot
    work — the exact stampede the self-anchoring rule was written to prevent.
    """
    from asset_pipeline import yahoo  # noqa: PLC0415

    # The SAME canary the throttle probes with (`YAHOO_CANARY`, default AAPL) — a symbol already
    # chosen for trading every session. A second, divergent "known liquid symbol" is a second thing
    # to keep true.
    sym = getattr(getattr(yahoo, "_throttle", None), "canary", None) or "AAPL"
    try:
        r = yahoo.chart(sym, rng="5d", interval="1d")
    except Exception as e:  # noqa: BLE001 — a probe must never fail the refresh
        log.warning("[price_refresh] market anchor probe failed: %s: %s", type(e).__name__, e)
        return None
    return newest_dated_close(r)


def newest_dated_close(chart_result: dict | None) -> str | None:
    """The date of the newest bar in a Yahoo chart result that actually HAS a close.

    ⚠ THE NEWEST BAR CAN BE TODAY'S UNFINISHED SESSION. Yahoo returns today's bar with
    `close: null` until the bell (and it returned a null bar for 2026-07-28 mid-session, a
    real hole, not the last row). Anchoring on a null bar claims a close the market has not
    printed, which makes every series in the fleet read one day stale every single morning —
    a daily full-fleet refresh that finds nothing to do.
    """
    from datetime import datetime, timezone  # noqa: PLC0415

    if not chart_result:
        return None
    ts = chart_result.get("timestamp") or []
    quote = (chart_result.get("indicators", {}).get("quote") or [{}])
    closes = (quote[0] if quote else {}).get("close") or []
    for i in range(min(len(ts), len(closes)) - 1, -1, -1):
        if closes[i] is not None:
            return datetime.fromtimestamp(ts[i], timezone.utc).date().isoformat()
    return None


def find_stale(held_only: bool = True,
               stale_days: int = DEFAULT_STALE_DAYS,
               use_market_anchor: bool = True,
               isins: set[str] | None = None) -> tuple[list[dict], str | None, int]:
    """Instruments whose last close lags the freshest close. Most stale first.

    Returns `(stale_rows, anchor, n_considered)` — the last two so a caller can say what it looked
    at, not just what it found.

    `isins` narrows the worklist to one caller's instruments (one account's holdings, say) instead
    of the whole held fleet; it overrides `held_only`. The ANCHOR is unchanged either way — it is a
    fact about the market, not about the subset being asked, and computing it from the subset would
    let a handful of instruments that all stopped together look current.

    ⚠ THE ANCHOR IS THE LATER OF WHAT WE HOLD AND WHAT THE MARKET HAS PUBLISHED (see
    `market_latest_close`). Our own maximum alone cannot detect a fleet that stopped updating as a
    block, because every row stays within `stale_days` of every other one.
    """
    ex = _executions(isins if isins is not None else (held_isins() if held_only else None))
    latest_all = global_latest_close()
    if use_market_anchor:
        market = market_latest_close()
        # max() on ISO dates is a string compare, which is a date compare — and `None` is skipped
        # rather than compared, so a failed probe cannot drag the anchor backwards.
        if market and (not latest_all or market > latest_all):
            log.info("[price_refresh] anchor %s -> %s (market has published since our newest bar)",
                     latest_all, market)
            latest_all = market
    if not latest_all:
        return [], None, len(ex)

    latest = latest_close_by_analysis(sorted({r["analysis_id"] for r in ex}))
    stale = []
    for r in ex:
        last = latest.get(r["analysis_id"])
        if not last or not r.get("yahoo_symbol"):
            continue          # never priced / never resolved — that is `store_one`'s job, not this
        if _days_between(latest_all, last) >= stale_days:
            stale.append({**r, "last_close": last})
    stale.sort(key=lambda r: r["last_close"])
    return stale, latest_all, len(ex)


def refresh_stale(
    held_only: bool = True,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = 0,
    sleep_s: float = DEFAULT_SLEEP_S,
    on_progress: Callable[[str], None] | None = None,
    isins: set[str] | None = None,
) -> dict:
    """Detect stale series and fetch the gap. Returns a summary; never raises for one bad row.

    `isins` scopes it to one caller's instruments — see `find_stale`.
    """
    from asset_pipeline import store  # noqa: PLC0415

    stale, latest_all, considered = find_stale(held_only, stale_days, isins=isins)
    if latest_all is None:
        return {"considered": considered, "stale": 0, "moved": 0, "unchanged": 0, "failed": 0,
                "skipped": 0, "global_latest": None}

    total_stale = len(stale)
    # A cap is honest only if it SAYS it capped. "6 refreshed" over a silent 197 reads like a
    # clean bill of health.
    skipped = max(0, total_stale - limit) if limit else 0
    if limit:
        stale = stale[:limit]

    moved = unchanged = failed = 0
    for i, r in enumerate(stale, 1):
        sym, aid, was = r["yahoo_symbol"], r["analysis_id"], r["last_close"]
        try:
            if store.extend_series(aid, sym, was) is None:
                store.store_series(aid, sym, None)      # no COPY path: slow, correct, never wrong
        except Exception as e:  # noqa: BLE001
            failed += 1
            if on_progress:
                on_progress(f"[{i}/{len(stale)}] {sym}: FAILED {type(e).__name__}: {e}")
            continue

        got = (supabase.table("asset_price").select("target_date").eq("analysis_id", aid)
               .order("target_date", desc=True).limit(1).execute().data or [])
        now = got[0]["target_date"] if got else was
        if now > was:
            moved += 1
            if on_progress:
                on_progress(f"[{i}/{len(stale)}] {sym}: {was} -> {now}")
        else:
            unchanged += 1
            if on_progress:
                on_progress(f"[{i}/{len(stale)}] {sym}: {was} — Yahoo has nothing newer")
        time.sleep(sleep_s)

    return {"considered": considered, "stale": total_stale, "moved": moved,
            "unchanged": unchanged, "failed": failed, "skipped": skipped,
            "global_latest": latest_all}
