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


def held_isins() -> set[str]:
    """Every ISIN held by an AIRS model portfolio — the instruments whose staleness actually
    surfaces, as a blank row on /portfolios."""
    rows, off = [], 0
    while True:
        b = (supabase.table("airs_model_portfolio_position").select("isin")
             .not_.is_("isin", "null").range(off, off + _PAGE - 1).execute().data or [])
        rows += b
        if len(b) < _PAGE:
            break
        off += _PAGE
    return {r["isin"] for r in rows if r.get("isin")}


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
    """The freshest close ANYWHERE in the table — one indexed row, not a scan. THE anchor."""
    r = (supabase.table("asset_price").select("target_date")
         .order("target_date", desc=True).limit(1).execute().data or [])
    return r[0]["target_date"] if r else None


def find_stale(held_only: bool = True,
               stale_days: int = DEFAULT_STALE_DAYS) -> tuple[list[dict], str | None, int]:
    """Instruments whose last close lags the global freshest close. Most stale first.

    Returns `(stale_rows, global_latest, n_considered)` — the last two so a caller can say what
    it looked at, not just what it found.
    """
    ex = _executions(held_isins() if held_only else None)
    latest_all = global_latest_close()
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
) -> dict:
    """Detect stale series and fetch the gap. Returns a summary; never raises for one bad row."""
    from asset_pipeline import store  # noqa: PLC0415

    stale, latest_all, considered = find_stale(held_only, stale_days)
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
