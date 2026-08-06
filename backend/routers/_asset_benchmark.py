"""A cap-weighted index built in the ASSET world — yfinance prices, joined by ISIN.

WHY A SECOND PATH AT ALL (`_benchmark_index` already rebuilds the S&P)
    That one prices off GuruFocus (`metric_data`, the `company` universe), and GuruFocus SELLS US
    A SUBSCRIPTION WITH HOLES IN IT: no UK, no India, no Ireland, no Australia/NZ, no Africa, no
    LatAm. For the S&P 500 that is invisible — it is a US index. For ACWI it is disqualifying:

        LSE (UK)          72 ACWI members    GuruFocus prices   0
        NSE (India)      160 ACWI members    GuruFocus prices   0
        Australia, Brazil, South Africa, Ireland, Chile...

    ~7.8% of ACWI's published weight sits in countries GuruFocus will never price, and a
    reconstruction that renormalises over the other 92% does not lose that weight — it silently
    redistributes it into everything else. That is a bias, not noise, and no amount of care
    inside the maths removes it.

    yfinance has no such holes. It prices LSE, NSE, ASX, B3. So the fix is not better arithmetic,
    it is a better source — and we already hold those prices in `asset_price`.

THE BRIDGE IS THE ISIN, AND IT IS A JOIN, NOT A COLUMN
    `universe_membership.company_id -> company.isin -> asset_execution.isin`. A membership FLAG
    on `asset_execution` was the obvious alternative and it is a trap: the ACWI universe is
    reconstructed on a schedule, so the flag would have to be re-synced on every refresh, and the
    day it drifts the benchmark is quietly wrong with no error anywhere. Same rule the holdings
    count already follows — *the count is a VIEW, never a column*. The join has no drift because
    it has nothing to keep in sync.

⚠ THE WEIGHTING MATHS IS NOT COPIED — IT IS REUSED (`_benchmark_index._window_rows`).
    Start-of-window cap weights (roll the cap back on the price move; weighting by TODAY's cap is
    look-ahead bias and turned +9.10% into +21.70%), split-adjustment, per-date FX. A second copy
    of that loop is a second place for the bias to grow back. This module's job is only to supply
    the same shape of `members` and `closes` from a different source.

⚠ STILL NOT FLOAT-ADJUSTED. `market_cap_eur` is a FULL market cap, and MSCI weights ACWI on free
    float. That over-weights state- and family-held names (mostly EM and Asia) whichever price
    source we use — it is a property of the weight, not of the price. iShares' own file carries
    the float weights; using them is the next step, not this one.
"""
from __future__ import annotations

import asyncio

from deps import IN_CHUNK_SIZE, supabase
from routers._airs_portfolio_perf import _closes as _asset_closes
from routers._benchmark_index import (
    _JUMP_HI,
    _JUMP_LO,
    INDEX_CAP_PCT,
    _fx_to_eur,
    _window_rows,
    index_weights,
)


def window_marks(analysis_ids: list[int], lookback: str, start_anchor: str,
                 end: str) -> dict[int, dict]:
    """Per instrument, ONLY what pricing one window needs: the opening mark, the closing mark, and
    the consecutive-bar jumps between them. One `COPY`; the selection happens in Postgres.

    ⚠ AN INDEX RETURN READS TWO PRICES PER MEMBER AND WE WERE SHIPPING THE WHOLE SERIES. Measured
    2026-07-30 on the local DB: ACWI's YTD loaded **264,678 close rows** to use 3,368 numbers —
    roughly 8 MB across the wire for 0.1% of it, on every panel load. SP500 loaded 77,567 for 978.
    The window function below returns ~2 rows per member instead.

    ⚠ IT IS NOT "JUST THE TWO PRICES", AND THAT IS THE WHOLE DESIGN. Our stored closes are NOT
    split-adjusted and cannot self-heal (`_split_adjust`), and a split is visible ONLY as a one-day
    discontinuity between CONSECUTIVE bars. Two bare marks cannot tell KLA's 9:1 from an −89% year
    — and the bogus ratio would hit the index twice, because the start weight is backed out through
    the same broken price. So the query also returns every consecutive pair whose ratio leaves the
    `_JUMP_LO.._JUMP_HI` band, and `split_factor` applies the whitelist to those in Python. The
    band is a cheap PRE-FILTER; the decision stays where the whitelist lives.

    ⚠ `end` IS AN UPPER BOUND, NEVER "TODAY'S PRICE". The closing mark is the newest bar at or
    before it, per member — a name whose vendor lags by a day is marked at its own last close, the
    same rule the full-series path's `s[-1]` gave.

    ⚠ AND THE SCAN MUST BEGIN AT `lookback`, NOT AT `start_anchor`. The opening mark is the last
    close ON OR BEFORE the anchor — 31 December IS the 1 January mark, and no exchange prints a bar
    on New Year's Day. Bounding the window at the anchor found an opening mark for nobody: measured,
    every one of the 25 AEX members came back with `start: None` and the index priced zero
    constituents. The caller passes the same 45-day lookback the series path always used.

    ⚠ THE JUMPS ARE THE ONES AT OR AFTER EACH MEMBER'S OWN OPENING MARK — hence the join back to
    `s`. A split BEFORE the opening mark has already been absorbed into it and must not be applied
    again; one AFTER it must (`_split_adjust` rescales every close earlier than the split, which
    the opening mark then is). Filtering on the anchor date instead would miss a split falling
    between a 31 December mark and the first bar of January.

    Returns `{}` when COPY is unavailable, so the caller falls back to the series path rather than
    silently pricing an index off nothing.
    """
    if not analysis_ids:
        return {}
    from common.pg import _run_copy  # noqa: PLC0415

    # psycopg placeholders are `%s`; `$1` parses as zero placeholders and degrades to no COPY.
    sql = """
COPY (
  WITH w AS (
    SELECT analysis_id, target_date, close,
           lag(close)       OVER (PARTITION BY analysis_id ORDER BY target_date) AS prev_close,
           lag(target_date) OVER (PARTITION BY analysis_id ORDER BY target_date) AS prev_date
    FROM asset_price
    WHERE analysis_id = ANY(%s) AND close IS NOT NULL AND close > 0
      AND target_date >= %s AND target_date <= %s
  ),
  s AS (
    SELECT DISTINCT ON (analysis_id) analysis_id, target_date, close
      FROM w WHERE target_date <= %s
     ORDER BY analysis_id, target_date DESC
  )
  SELECT 'start' AS kind, analysis_id, target_date::text, close, NULL::numeric FROM s
  UNION ALL
  SELECT 'end', analysis_id, target_date::text, close, NULL::numeric
    FROM (SELECT DISTINCT ON (analysis_id) analysis_id, target_date, close
            FROM w ORDER BY analysis_id, target_date DESC) e
  UNION ALL
  SELECT 'jump', w.analysis_id, w.target_date::text, w.close, w.prev_close
    FROM w JOIN s ON s.analysis_id = w.analysis_id
   WHERE w.prev_close IS NOT NULL AND w.prev_date >= s.target_date
     AND (w.close / w.prev_close < %s OR w.close / w.prev_close > %s)
) TO STDOUT WITH CSV
"""
    buf = _run_copy(sql, (list(analysis_ids), lookback, end, start_anchor,
                          _JUMP_LO, _JUMP_HI))
    if buf is None:
        return {}
    out: dict[int, dict] = {}
    for line in buf.getvalue().decode().splitlines():
        kind, aid, d, close, prev = line.split(",")
        rec = out.setdefault(int(aid), {"start": None, "end": None, "jumps": []})
        if kind == "jump":
            rec["jumps"].append((float(prev), float(close)))
        else:
            rec[kind] = (d, float(close))
    return out


def _universe_company_ids(label: str) -> list[int]:
    """Every company in the universe.

    ⚠⚠ PAGED, AND THE UNPAGED VERSION WAS A LIVE PRODUCTION BUG THAT LOCAL DEV COULD NOT SEE.
    This was a bare `.execute()`. PostgREST caps a response at 1,000 rows on Supabase cloud and
    10,000 locally, and truncates SILENTLY — so every local run returned all 1,998 ACWI rows and
    looked correct, while production built the ACWI index from about half its constituents and
    Leonteq from 1,000 of 1,639. A cap-weighted index over an arbitrary half of its members is not
    a slightly-off number; it is a different index, and nothing anywhere reported a problem.

    ⚠ ORDERED ON THE PK. Postgres makes no promise about row order across separate LIMIT/OFFSET
    queries, so an unordered page boundary serves some rows twice and skips others — which is how
    the (since-removed) membership backfill script came to report the S&P at 28, 411 and 503
    members on three consecutive runs of identical code.

    ⚠ ADVANCE BY WHAT CAME BACK, break on an empty page. `len(rows) < page` is only correct while
    the server's cap is at least the page size, which is precisely the assumption that failed here.
    """
    uni = (supabase.table("universe").select("universe_id")
           .eq("label", label).limit(1).execute().data or [])
    if not uni:
        return []
    out: set[int] = set()
    off = 0
    while True:
        rows = (supabase.table("universe_membership").select("company_id")
                .eq("universe_id", uni[0]["universe_id"])
                .order("company_id").range(off, off + 999).execute().data or [])
        if not rows:
            break
        out.update(r["company_id"] for r in rows)
        off += len(rows)
    return sorted(out)


def _universe_analysis_ids(label: str) -> list[int]:
    """The universe's constituents as ASSET ids, straight from `universe_asset_membership`.

    ⚠ `universe_asset_membership` IS A VIEW, NOT A TABLE (migration 20260806060000). It IS the
    three-hop join `universe_membership.company_id -> company.isin -> asset_execution.isin`,
    evaluated live, so it cannot drift from the membership it mirrors. It was briefly a
    backfilled table and that was a mistake: nothing wrote to it except a manual script, and it
    was measurably stale within hours of being populated while two read paths — this one and the
    `/asset-pipeline` Benchmarks chips — already depended on it. Measured cost of the view on the
    full grid: 28.0 ms against the table's 29.9-33.9 ms, i.e. none.

    ⚠ PAGED AND ORDERED, for the same reason as `_universe_company_ids` — ACWI is 1,723 rows here
    and PostgREST silently caps at 1,000 on cloud. Ordering on `analysis_id` alone is safe ONLY
    because of the `.eq(universe_id)` filter above it: the view is DISTINCT on the pair, so the
    key is unique within one universe. A reader that drops that filter needs its own tiebreaker.
    """
    uni = (supabase.table("universe").select("universe_id")
           .eq("label", label).limit(1).execute().data or [])
    if not uni:
        return []
    out: set[int] = set()
    off = 0
    while True:
        rows = (supabase.table("universe_asset_membership").select("analysis_id")
                .eq("universe_id", uni[0]["universe_id"])
                .order("analysis_id").range(off, off + 999).execute().data or [])
        if not rows:
            break
        out.update(r["analysis_id"] for r in rows)
        off += len(rows)
    return sorted(out)


def members(label: str) -> tuple[list[dict], dict]:
    """The index's constituents, priced from the ASSET world. Returns (members, coverage).

    Shaped for `_benchmark_index._window_rows`, which keys prices by `company_id` — here that
    slot carries the `analysis_id`, because the price series is `asset_price`, not `metric_data`.
    The name is the loop's, not ours; what matters is that ONE loop does the weighting.

    ⚠ ONE COMPANY, ONE ROW. Yahoo, like GuruFocus, reports the FULL company market cap on EVERY
    share class — Alphabet is GOOGL *and* GOOG, each carrying the whole cap, so a naive sum counts
    it twice (11.3% of the S&P's weight, fictional). Deduped on the COMPANY name, keeping the
    largest cap, exactly as `_benchmark_index._members` does.
    """
    # ⚠⚠ TWO DIFFERENT COUNTS, AND CONFLATING THEM WOULD HIDE THE THING COVERAGE EXISTS TO SHOW.
    #   `universe_members` is the size of the INDEX and still comes from the company world, because
    #   that is where membership is authored. `universe_asset_membership` is what we could bridge
    #   into the asset world — smaller by construction (ACWI 1,723 of 1,982; the gap is India and
    #   the UK, where GuruFocus can supply no ISIN). Taking the denominator from the asset table
    #   would make `covered_pct` read ~100% while a fifth of ACWI was missing: the bridge loss
    #   would vanish into the number designed to report exactly that kind of loss.
    ids = _universe_company_ids(label)
    if not ids:
        return [], {"universe_members": 0, "priced": 0, "covered_pct": None}

    grid_rows: list[dict] = []
    for i in range(0, len(aids := _universe_analysis_ids(label)), IN_CHUNK_SIZE):
        grid_rows += (supabase.table("asset_grid")
                      .select("isin,analysis_id,yahoo_symbol,name,gf_company_name,currency,"
                              "market_cap_eur,market_cap_currency,status,bars,is_default,"
                              "delisted_at,out_of_scope_at")
                      .in_("analysis_id", aids[i:i + IN_CHUNK_SIZE]).execute().data or [])

    # ⚠ ONE ROW PER ANALYSIS ASSET, NOT PER LISTING. `asset_grid` is one row per EXECUTION, so a
    #   company traded on several venues appears several times — measured on the S&P, 501 assets
    #   come back as 506 rows. Keeping all of them would weight those companies twice. `is_default`
    #   marks the execution the pipeline chose; where it is unset, the deepest price history wins,
    #   which is deterministic rather than whichever row the database returned first.
    grid: dict[int, dict] = {}
    for r in grid_rows:
        if r.get("status") != "ok" or not r.get("analysis_id") or (r.get("bars") or 0) <= 0:
            continue
        # The company-world status markers now ride on the grid (migration 20260806030000), so
        # the delisted / out-of-scope exclusion no longer needs a second read of `company`.
        if r.get("delisted_at") or r.get("out_of_scope_at"):
            continue
        prev = grid.get(r["analysis_id"])
        better = (prev is None
                  or (bool(r.get("is_default")) and not bool(prev.get("is_default")))
                  or (bool(r.get("is_default")) == bool(prev.get("is_default"))
                      and (r.get("bars") or 0) > (prev.get("bars") or 0)))
        if better:
            grid[r["analysis_id"]] = r

    # WHERE THE CAP CAME FROM AND WHEN — for the panel's per-row provenance, not for the maths.
    # `asset_grid` carries `market_cap_eur` and its currency but not the native figure or the
    # timestamp; both live on `asset_analysis`, which the Refresh button stamps every run
    # (`_benchmark_refresh._caps`). Surfaced because a cap is a fetched number with an age, and a
    # weight computed off a three-week-old cap is a three-week-old weight — invisible otherwise.
    caps: dict[int, dict] = {}
    priced_aids = sorted(grid)
    for i in range(0, len(priced_aids), IN_CHUNK_SIZE):
        for r in (supabase.table("asset_analysis")
                  .select("analysis_id,market_cap_native,market_cap_currency,"
                          "market_cap_checked_at")
                  .in_("analysis_id", priced_aids[i:i + IN_CHUNK_SIZE]).execute().data or []):
            caps[r["analysis_id"]] = r

    # ⚠⚠ ONE COMPANY, ONE ROW — AND THE ASSET WORLD DOES NOT MAKE THIS UNNECESSARY. Keying
    #   membership on `analysis_id` collapses a company's LISTINGS, not its SHARE CLASSES: those
    #   have different ISINs, hence different assets. Measured on the S&P after the repoint,
    #   Alphabet is still two rows (`US02079K3059` and `US02079K1079`) each carrying the FULL
    #   ~EUR 3.9tn cap, and Fox Corp likewise. Dropping this dedupe would add ~11% of fictional
    #   weight to the index — the exact figure `_benchmark_index` records for the same trap.
    #
    # ⚠ THE KEY PREFERS `gf_company_name`, THE COMPANY-WORLD NAME, so the dedupe behaves exactly as
    #   it did before the repoint. It falls back to the asset name for a constituent with no
    #   company row — those cannot collide with a share-class sibling, since a sibling would have
    #   brought a company row with it.
    by_name: dict[str, dict] = {}
    for aid, g in grid.items():
        cap = float(g.get("market_cap_eur") or 0)
        if cap <= 0:
            continue                      # no cap to weight it by
        prov = caps.get(aid) or {}
        key = ((g.get("gf_company_name") or g.get("name") or "").strip().lower())
        prev = by_name.get(key)
        if prev is None or cap > float(prev["market_cap_eur"]):
            by_name[key] = {
                # `_window_rows` looks prices up under this key — here it is the analysis_id.
                "company_id": aid,
                "company_name": g.get("gf_company_name") or g.get("name"),
                "gurufocus_ticker": g.get("yahoo_symbol"),   # the loop's field name; a yf symbol
                "isin": g.get("isin"),
                "currency": g.get("currency"),
                "market_cap_eur": cap,
                # Provenance only — nothing downstream weights off these.
                "market_cap_native": prov.get("market_cap_native"),
                "market_cap_currency": (prov.get("market_cap_currency")
                                        or g.get("market_cap_currency")),
                "market_cap_checked_at": prov.get("market_cap_checked_at"),
            }

    out = list(by_name.values())
    coverage = {
        "universe_members": len(ids),
        "priced": len(out),
        # How much of the universe we could actually price. ALWAYS reported: a cap-weighted index
        # renormalised over a fraction of its constituents is exactly the invention the portfolio
        # returns refuse to make, and here the missing names are systematic (a whole country), not
        # random.
        "covered_pct": (len(out) / len(ids) * 100.0) if ids else None,
    }
    return out, coverage


def index_returns(label: str, starts: list[str]) -> dict[str, dict]:
    """Cap-weighted EUR/local return for `label` over several windows — ONE price load, ONE
    weighting (`_window_rows`, shared with the GuruFocus path and with /benchmarks)."""
    from datetime import date, timedelta  # noqa: PLC0415

    mem, coverage = members(label)
    if not mem or not starts:
        return {}
    earliest = min(starts)
    lookback = (date.fromisoformat(earliest) - timedelta(days=45)).isoformat()
    today = date.today().isoformat()

    ids = [m["company_id"] for m in mem]
    fx = _fx_to_eur({(m.get("currency") or "USD") for m in mem}, lookback, today)
    # ⚠ MARKS PER WINDOW, ONE QUERY EACH — still far less than one whole-series load. Each window
    # has its own opening mark and its own jump set, and a mark selected for January cannot answer
    # for a since-inception start. `starts` is one or two dates in practice.
    marks = {s: window_marks(ids, lookback, s, today) for s in sorted(set(starts))}
    closes = ({} if all(marks.values()) or not mem
              else _asset_closes(ids, lookback, today))   # COPY unavailable -> the series path

    out: dict[str, dict] = {}
    for s in sorted(set(starts)):
        rows, _ = _window_rows(mem, closes, fx, s, marks=marks[s] or None)
        total = sum(r["start_cap_eur"] for r in rows)
        if not rows or total <= 0:
            out[s] = {"eur_pct": None, "local_pct": None, "members": 0,
                      "start_date": None, **coverage}
            continue
        # ONE weighting, shared with `index_rows` below and with /benchmarks — an index whose
        # headline return is weighted differently from the constituents behind it reconciles
        # against nothing. This is also where a capped index (AEX) gets its cap.
        w = index_weights(rows, label)
        eur = sum(x / 100.0 * r["return_eur_pct"] for x, r in zip(w, rows))
        loc = sum(x / 100.0 * r["return_local_pct"] for x, r in zip(w, rows))
        out[s] = {"eur_pct": eur, "local_pct": loc, "members": len(rows),
                  "start_date": min(r["start_date"] for r in rows), **coverage}
    return out


def index_rows(label: str, start: str) -> tuple[list[dict], dict]:
    """The index's CONSTITUENTS over one window — each with its start-of-window weight and its
    EUR return. Returns (rows, coverage).

    Attribution needs the index name by name, not just its total: "did your Technology picks beat
    the index's Technology picks?" is unanswerable from an aggregate. Same `_window_rows`, so the
    weights are the same start-of-window weights the headline return is built from — an
    attribution that reconciles against a DIFFERENT weighting reconciles against nothing.
    """
    from datetime import date, timedelta  # noqa: PLC0415

    mem, coverage = members(label)
    if not mem:
        return [], coverage
    lookback = (date.fromisoformat(start) - timedelta(days=45)).isoformat()
    today = date.today().isoformat()
    ids = [m["company_id"] for m in mem]
    fx = _fx_to_eur({(m.get("currency") or "USD") for m in mem}, lookback, today)
    marks = window_marks(ids, lookback, start, today)
    closes = {} if marks else _asset_closes(ids, lookback, today)

    rows, _ = _window_rows(mem, closes, fx, start, marks=marks or None)
    total = sum(r["start_cap_eur"] for r in rows)
    if total <= 0:
        return [], coverage
    # Same `index_weights` as `index_returns` — Brinson reconciles the constituents against the
    # index total, so the two MUST be the same weights (capped ones included).
    for r, x in zip(rows, index_weights(rows, label)):
        r["weight_pct"] = x
    return rows, coverage


def compute_index(label: str, year: int | None = None, start: str | None = None) -> dict:
    """The `/benchmarks` panel's index — the ASSET-path twin of `_benchmark_index.compute_index`,
    returning the identical shape.

    ⚠ WHY THE PANEL MOVED HERE (2026-07-16). Its own subtitle promises "same basis as a portfolio,
    so the numbers are comparable" — and every portfolio on that page is priced from `asset_price`
    (yfinance) while the panel was priced from GuruFocus. Two price vendors, two adjustment
    conventions, two FX sources; the difference between them reads as alpha. That is the rule the
    composition modal was built on, applied to the panel that states it out loud.

    The measured cost, stated rather than buried: against SPY's +9.02% USD, the GuruFocus rebuild
    was +9.05% and this one is +9.23% — so for the S&P specifically, the vendor we left was ~0.2pp
    closer. That trade is deliberate. GuruFocus is closer on the ONE index whose constituents it
    fully covers, and structurally unable to price the others:

        ACWI   ~7.8% of published weight in countries GuruFocus will never price
        AEX    31.96% — Shell, Unilever and RELX are all LSE rows with no GuruFocus market cap

    A cap-weighted rebuild does not LOSE that weight, it redistributes it: the GuruFocus AEX
    printed +14.80% against this path's +12.12%, with Prosus capped at 15% — a name that is really
    10.46% of the index, pushed onto the cap by absorbing the missing third. Nothing about that
    output looks wrong. Trading 0.2pp on the S&P for that is not a close call.

    `_benchmark_index.compute_index` stays exactly where it is: it is the SPY cross-check, which
    validates the METHOD (start-of-window weights, split-adjustment, per-date FX) against a real
    ETF. It is no longer any route's basis — with this move its GuruFocus price loader
    (`_benchmark_index._closes`, the last reader of `metric_data` on this page's side of the app)
    has NO production caller left. That is the point, not an oversight: keep the cross-check,
    retire the vendor as a basis.
    """
    from datetime import date, timedelta  # noqa: PLC0415

    year = year or date.today().year
    start_anchor = start or f"{year}-01-01"
    lookback = (date.fromisoformat(start_anchor) - timedelta(days=45)).isoformat()
    today = date.today().isoformat()

    mem, coverage = members(label)
    if not mem:
        return {"label": label, "year": year, "members": [], "member_count": 0,
                "ytd_eur_pct": None, "ytd_local_pct": None,
                "note": f"No universe labelled {label!r}."}

    ids = [m["company_id"] for m in mem]
    fx = _fx_to_eur({(m.get("currency") or "USD") for m in mem}, lookback, today)
    # ⚠ ONLY THE MARKS THIS WINDOW USES — see `window_marks`. The panel was loading every close of
    # every constituent (ACWI: 264,678 rows) to read two prices each. Falls back to the whole
    # series when COPY is unavailable, which is the only way `window_marks` can return nothing.
    marks = window_marks(ids, lookback, start_anchor, today)
    closes = {} if marks else _asset_closes(ids, lookback, today)

    # `adjusted` is KEPT here, not dropped. Our stored closes are not split-adjusted and cannot
    # self-heal; a rescaled price is a CLAIM and the panel shows it. (The `index_returns` /
    # `index_rows` callers above discard it because their surfaces have nowhere to say so — this
    # one does.)
    rows, adjusted = _window_rows(mem, closes, fx, start_anchor, marks=marks or None)
    if not rows or sum(r["start_cap_eur"] for r in rows) <= 0:
        return {"label": label, "year": year, "members": [], "member_count": 0,
                "ytd_eur_pct": None, "ytd_local_pct": None,
                "note": "No constituent had a price on both ends of the window."}

    # THE SAME `index_weights` as every other surface — capped where the index caps.
    for r, x in zip(rows, index_weights(rows, label)):
        r["weight_pct"] = x
    ytd_eur = sum(r["weight_pct"] / 100.0 * r["return_eur_pct"] for r in rows)
    ytd_loc = sum(r["weight_pct"] / 100.0 * r["return_local_pct"] for r in rows)
    rows.sort(key=lambda r: -r["weight_pct"])

    cap = INDEX_CAP_PCT.get(label)
    note = ("Cap-weighted on FULL market cap (the real index float-adjusts) using "
            "start-of-year weights; membership is a snapshot, so mid-year index changes are not "
            "replayed. Price return, not total return — dividends are not included. "
            "Priced from yfinance, the same source as the portfolios above.")
    if cap:
        note += (f" Capped at {cap:.0f}% per constituent, as the real index is — applied at the "
                 f"window open rather than at the index's review date.")

    return {
        "label": label,
        "year": year,
        "member_count": len(rows),
        # `priced_of_universe` is the honest denominator: `coverage` counts what the UNIVERSE has,
        # `rows` what actually had a price on both ends of this window.
        "priced_of_universe": f"{len(rows)}/{coverage['universe_members']}",
        "as_of": max(r["end_date"] for r in rows),
        "start_date": min(r["start_date"] for r in rows),
        "ytd_eur_pct": ytd_eur,
        "ytd_local_pct": ytd_loc,
        "members": rows,
        "split_adjusted": adjusted,
        "note": note,
    }


async def compute_index_async(label: str, year: int | None = None) -> dict:
    return await asyncio.to_thread(compute_index, label, year)


async def index_returns_async(label: str, starts: list[str]) -> dict[str, dict]:
    return await asyncio.to_thread(index_returns, label, starts)
