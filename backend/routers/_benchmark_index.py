"""A cap-weighted index rebuilt from our own constituents + prices. Currently: the S&P 500.

WHY REBUILD SOMETHING WE ALREADY HAVE
    `SPY` is in `benchmark` with 7,168 bars back to 1998, and it IS the S&P 500. This module
    is not a replacement for it — it is the index computed the SAME WAY a portfolio is
    (our membership, our prices, our FX), so a benchmark number and a portfolio number are
    comparable line for line. It will drift from SPY, and the two reasons are both known:
    we cap-weight on FULL market cap (S&P float-adjusts) and our membership is a snapshot
    (S&P's committee adds and drops names mid-year).

⚠ THE WEIGHT IS AS OF THE START OF THE PERIOD, NOT TODAY.
    This is the whole correctness story. Weighting a year-to-date return by TODAY's market
    cap is LOOK-AHEAD BIAS: a stock that doubled has (roughly) twice the cap it started with,
    so weighting by today's cap retroactively gives the winners more of the portfolio than
    they ever had, and the index return comes out systematically too high. It is a very easy
    mistake to make, because "market cap" reads like a static property of a company.

    The start weight is recovered from the end weight instead:

        cap_start  ≈  cap_now × (price_start / price_now)

    i.e. hold the share count constant across the period and let the PRICE carry the cap.
    (Buybacks and issuance make this an approximation, but it is a second-order one — and it
    is the difference between a bias of the same sign as the return, and none.)

    Equivalently: this is a buy-and-hold of the start-of-year basket. Which is what an index
    fund tracking a cap-weighted index actually does — cap weights need no rebalancing to
    stay cap weights.

⚠ ONE COMPANY, ONE ROW.
    GuruFocus reports the FULL company market cap on EVERY share class. Alphabet is in the
    grid twice (GOOGL + GOOG), each carrying €3,785bn — so a naive sum counts it twice. Over
    a top-500-by-cap pool that was €7,683bn of €67,863bn, 11.3% of total weight, fictional.
    We dedupe by company_name, keeping the most liquid row. (The real S&P holds both classes,
    but each at its own float — which we do not have, so one row is the honest simplification.)
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

from asset_pipeline.fx import SUBUNIT
from common.fx_load import load_fx_to_eur
from deps import IN_CHUNK_SIZE, supabase

_log = logging.getLogger(__name__)

# The `universe.label` of the reconstructed membership (Wikipedia + OpenFIGI, the /sp500 page).
SP500_LABEL = "SP500"

# GuruFocus daily close, in the LISTING's currency. (`metric_data` is EAV — the value lives
# in `numeric_value`, and the same table holds `market_cap_bn`, `score`, … so the
# source_code filter is not optional.)
_CLOSE_METRIC = "close_price"
_CLOSE_SOURCE = "gurufocus"


def _members(label: str, *, require_market_cap: bool = True) -> list[dict]:
    """The index's companies, deduped to ONE ROW PER COMPANY (see the module header).

    The price currency comes from the EXCHANGE, not from `market_cap_currency` — they are
    different fields and can disagree, and it is the exchange's currency the close is
    quoted in.

    ⚠ `require_market_cap=False` KEEPS THE CONSTITUENTS THIS OTHERWISE DELETES, and it exists for
    a caller that LISTS the index rather than weighting it. The default drops any company with no
    stored `market_cap_eur`, which is right here — the index is cap-weighted, so a member with no
    cap cannot be given a weight and silently taking a share of the others' would be worse. But
    "cannot be weighted" is not "is not a constituent": on the AEX the three it removes are
    **Shell, Unilever and RELX** (LSE listings GuruFocus does not cover), so a table built on the
    default shows 22 rows and calls it the AEX. `_benchmark_fundamental_grid` passes False to list
    all 25 and mark the three as unpriceable, which is the honest form for a grid of figures.

    ⚠ THE DEDUPE SURVIVES EITHER WAY. It keeps the highest-cap row per company NAME, and a missing
    cap reads as 0 there — so an unfiltered call still folds share classes (GOOGL+GOOG are one
    company) and still prefers the row that has a cap. Removing the filter cannot resurrect a
    duplicate.
    """
    uni = (supabase.table("universe").select("universe_id")
           .eq("label", label).limit(1).execute().data or [])
    if not uni:
        return []
    mem = (supabase.table("universe_membership").select("company_id")
           .eq("universe_id", uni[0]["universe_id"]).execute().data or [])
    ids = sorted({m["company_id"] for m in mem})

    ccy_by_exch = {e["exchange_id"]: e["currency_code"] for e in
                   (supabase.table("gurufocus_exchange")
                    .select("exchange_id,currency_code").execute().data or [])}

    rows: list[dict] = []
    for i in range(0, len(ids), IN_CHUNK_SIZE):
        q = (supabase.table("company")
             .select("company_id,company_name,gurufocus_ticker,isin,market_cap_eur,exchange_id")
             .in_("company_id", ids[i:i + IN_CHUNK_SIZE])
             .is_("delisted_at", "null").is_("out_of_scope_at", "null"))
        if require_market_cap:
            q = q.not_.is_("market_cap_eur", "null")
        rows += (q.execute().data or [])
    for r in rows:
        r["currency"] = ccy_by_exch.get(r.get("exchange_id"))

    # One company, one row — a share class is not a second company, and GuruFocus puts the
    # FULL company cap on each class, so keeping both double-counts it.
    by_name: dict[str, dict] = {}
    for r in rows:
        k = (r.get("company_name") or "").strip().lower()
        prev = by_name.get(k)
        if prev is None or (r.get("market_cap_eur") or 0) > (prev.get("market_cap_eur") or 0):
            by_name[k] = r
    return list(by_name.values())


def weight_basis(label: str) -> dict:
    """How this index's constituent weights were arrived at, and who fell out on the way.

    ⚠ THE FUNDAMENTAL BLEND WEIGHTS ON **TODAY's** CAP, NOT THE START-OF-WINDOW CAP THE PRICE
    INDEX USES. `_window_rows` backs the start weight out through the price precisely because
    weighting a RETURN by today's cap is look-ahead bias (measured: +9.10% → +21.70%). A growth
    blend has no single window to back a cap out to — each constituent's series starts in a
    different year — so it uses `market_cap_eur` as stored. That is a real tilt toward companies
    that have since grown, and it is stated rather than buried: a constituent whose revenue rose
    tenfold carries its post-growth weight over its whole history.

    ⚠ AND A CONSTITUENT WITH NO STORED CAP IS NOT IN THE INDEX AT ALL. `_members` requires
    `market_cap_eur`, and the names that lack one are systematically the ones GuruFocus does not
    cover — LSE listings above all. Measured 2026-08-04 on the AEX: **Shell, Unilever and RELX**
    are all missing, so the 22 that remain are renormalised over 100% and **ASML alone reads
    51.76%**. Against the real AEX — which float-adjusts and caps any constituent at 15% — that
    is not a small difference, and a reader comparing the two has to be told.

    Returns the counts + the dropped names so the surface that shows the weights can say so.
    """
    uni = (supabase.table("universe").select("universe_id")
           .eq("label", label).limit(1).execute().data or [])
    if not uni:
        return {"members": 0, "weighted": 0, "excluded": []}
    ids = sorted({m["company_id"] for m in
                  (supabase.table("universe_membership").select("company_id")
                   .eq("universe_id", uni[0]["universe_id"]).execute().data or [])})
    rows: list[dict] = []
    for i in range(0, len(ids), IN_CHUNK_SIZE):
        rows += (supabase.table("company")
                 .select("company_name,market_cap_eur,delisted_at,out_of_scope_at")
                 .in_("company_id", ids[i:i + IN_CHUNK_SIZE]).execute().data or [])
    excluded = [
        {"name": r.get("company_name"),
         "reason": ("delisted" if r.get("delisted_at")
                    else "out of scope" if r.get("out_of_scope_at")
                    else "no market cap")}
        for r in rows
        if r.get("market_cap_eur") is None or r.get("delisted_at") or r.get("out_of_scope_at")
    ]
    return {"members": len(ids), "weighted": len(ids) - len(excluded),
            "excluded": sorted(excluded, key=lambda e: (e["reason"], e["name"] or ""))}


def _closes(company_ids: list[int], start: str, end: str) -> dict[int, list[tuple[str, float]]]:
    """Each company's closes in [start, end], ascending. Local currency."""
    out: dict[int, list[tuple[str, float]]] = {}
    for i in range(0, len(company_ids), IN_CHUNK_SIZE):
        chunk = company_ids[i:i + IN_CHUNK_SIZE]
        off = 0
        while True:
            # PostgREST caps a response at 1,000 rows and TRUNCATES SILENTLY — 500 names ×
            # ~130 trading days is far past that, so this must page or the index is computed
            # off whatever fraction happened to fit.
            batch = (supabase.table("metric_data")
                     .select("company_id,target_date,numeric_value")
                     .in_("company_id", chunk)
                     .eq("metric_code", _CLOSE_METRIC).eq("source_code", _CLOSE_SOURCE)
                     .gte("target_date", start).lte("target_date", end)
                     .order("target_date").range(off, off + 999).execute().data or [])
            for r in batch:
                if r["numeric_value"] is None:
                    continue
                out.setdefault(r["company_id"], []).append(
                    (r["target_date"], float(r["numeric_value"])))
            if len(batch) < 1000:
                break
            off += 1000
    for s in out.values():
        s.sort()
    return out


def _fx_to_eur(currencies: set[str], start: str, end: str) -> dict[str, dict[str, float]]:
    """{currency: {date: units per EUR}} — the same `fx_rate` table the rest of the app uses.

    Direction matters: `rate` is units of the currency PER EUR, so EUR = price / rate (this
    mirrors `momentum/data/fx.py`, which divides). Getting it upside down would invert every
    FX move.

    ⚠ THE IMPLEMENTATION — INCLUDING THE PAGING RULES AND THE COPY FAST PATH — NOW LIVES IN
    `common/fx_load.py`, TOGETHER WITH ITS TWIN'S. This function and `_airs_portfolio_perf._fx`
    each documented the other as its twin and then drifted: only this side had the one-request
    COPY, so the Analyse modal paid **17 sequential PostgREST requests for `fx_rate` (13,617
    rows)** on the AIRS side while doing the same job here in 4 COPYs. Every rule involved is a
    correctness rule with an incident behind it (silent 1,000/10,000-row truncation dropping a
    currency, and with it a fully-priced holding, out of its own basket) — and it had to be right
    in two files at once. That arrangement is what produced the bug. One definition now.
    """
    return load_fx_to_eur(currencies, start, end)


# A split shows up as a single-day price ratio nothing like a real market move. Outside this
# band we STOP AND LOOK — we do not immediately assume a split (see `_split_adjust`).
_JUMP_LO, _JUMP_HI = 0.6, 1.7

# The ratios a split ACTUALLY comes in. This must be an explicit whitelist, not "any small
# rational n/d": the set of n/d for n,d <= 20 is dense enough to sit within 5% of almost any
# number, so it matches a -45% crash (1.818 = 20/11) and the guard becomes decorative. A test
# caught exactly that.
_SPLIT_FORWARD = (2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 3 / 2, 5 / 2, 4 / 3, 5 / 4, 7 / 5)
_SPLIT_RATIOS = sorted({*_SPLIT_FORWARD, *(1 / r for r in _SPLIT_FORWARD)})
_SPLIT_TOL = 0.05          # within 5% of one of those -> a corporate action, not a market


def split_factor(jumps: list[tuple[float, float]]) -> float | None:
    """The cumulative rescaling a window's splits imply, from its CONSECUTIVE-BAR jumps alone.

    `jumps` is `[(previous_close, close)]` for bars that actually sit next to each other. Returns
    the factor to multiply every price BEFORE the jumps by, to put it on the latest basis — the
    same `factor` `_split_adjust` returns, which is why that function now calls this one. There is
    exactly one copy of the whitelist test, and both the full-series path and the two-mark path
    reach it.

    ⚠ THE PAIRS MUST BE CONSECUTIVE BARS, AND THAT IS THE WHOLE SAFETY PROPERTY. Hand it a pair
    that merely brackets a window and the test becomes a test of the RETURN: a stock that doubles
    over the year gives a ratio of exactly 2.0, matches the 1:2 whitelist entry to 0%, and its gain
    is "corrected" away. A split is a one-DAY discontinuity; nothing else may be offered here.
    """
    factor: float | None = None
    for prev_p, cur_p in jumps:
        if prev_p <= 0:
            continue
        ratio = cur_p / prev_p
        if _JUMP_LO <= ratio <= _JUMP_HI:
            continue
        inv = 1.0 / ratio
        near = min(_SPLIT_RATIOS, key=lambda s: abs(s - inv))
        if near <= 0 or abs(inv - near) / near > _SPLIT_TOL:
            continue                      # a violent day, not a corporate action. Leave it.
        factor = (factor or 1.0) * ratio
    return factor


def _split_adjust(series: list[tuple[str, float]]) -> tuple[list[tuple[str, float]], float | None]:
    """Put a price series back on ONE scale across an unadjusted split.

    ⚠ OUR STORED PRICES ARE NOT SPLIT-ADJUSTED, AND CANNOT SELF-HEAL. `ingest/prices.py`
    fetches only dates NEWER than what we already hold, so when the vendor retroactively
    rewrites history for a split, we never re-read it: the pre-split prices sit at the old
    scale for ever. Measured on the S&P 500, 2026 YTD — 3 of 493 constituents:

        KLA Corp        2026-06-08   1929.20 ->  210.81   x0.109   (~9:1)
        CrowdStrike     2026-07-01    763.14 ->  193.19   x0.253   (~4:1)
        DuPont          2026-06-23     48.19 ->  140.01   x2.905   (~1:3 reverse)

    Uncorrected, KLA reads as -80.6% — AND takes a 2.70% index weight, because the start
    weight is backed out through the same broken price (`cap_now × price_start/price_now`),
    so the bogus ratio inflates the weight by exactly the factor it fakes the loss. One bad
    series thus hits the index twice, in the same direction.

    ⚠ WE DO NOT ADJUST EVERY BIG MOVE. A real stock CAN fall 45% in a day (a failed trial, a
    fraud, a bid collapsing), and "correcting" that would erase a genuine loss — the same
    error with the sign flipped. So a jump is only treated as a split when its ratio lands
    within 5% of an ACTUAL SPLIT RATIO (`_SPLIT_RATIOS` — an explicit whitelist). KLA's 9.151
    -> 9:1 (1.7% off); CrowdStrike's 3.953 -> 4:1 (1.2%); DuPont's 2.905 -> 1:3 (3.2%). A -45%
    crash gives 1.818, nearest 2:1 and 9% off, so it is left alone and stays a loss.

    ⚠ KNOWN LIMIT: a 2:1 split and a stock that HALVES in a day are numerically identical, and
    no amount of looking at the price series can separate them. (A split leaves market cap
    unchanged; a crash halves it — but we store only the CURRENT cap, so that check isn't
    available.) Every adjustment is therefore surfaced in the response and shown in the UI,
    never applied silently — a human can see the three names and disagree.

    (This is a PATCH, not the fix. The fix is to re-fetch full history for these companies —
    the same defect corrupts MOMENTUM SIGNALS, which read the very same `close_price` rows.)
    """
    if len(series) < 2:
        return series, None

    out = [list(p) for p in series]
    factor: float | None = None
    # Walk backwards: everything BEFORE a split has to be scaled onto today's basis. The test for
    # "is this a split" is `split_factor`'s — ONE copy of the whitelist, shared with the marks path.
    for k in range(len(out) - 1, 0, -1):
        ratio = split_factor([(out[k - 1][1], out[k][1])])
        if ratio is None:
            continue
        for j in range(k):                # rescale every earlier close onto the new basis
            out[j][1] *= ratio
        factor = (factor or 1.0) * ratio
    return [(d, v) for d, v in out], factor


def _at_or_before(series: list[tuple[str, float]], when: str) -> tuple[str, float] | None:
    """The last observation on or before `when` — the price the period actually STARTED at.

    Not "the first price after Jan 1": in a holiday-shortened start that silently measures
    from a later, higher/lower base. The last close of the previous year IS the opening mark.
    """
    hit = None
    for d, v in series:
        if d <= when:
            hit = (d, v)
        else:
            break
    return hit


def _rate(fx: dict[str, dict[str, float]], ccy: str | None, when: str) -> float | None:
    """FX on `when`, else the most recent earlier rate (the table is not dense on holidays).

    Returns UNITS OF `ccy` PER EUR, so `eur = native / rate`.

    ⚠ MINOR UNITS. Yahoo quotes London in PENCE (`GBp`), and `fx_rate` has no such code — so
    passing it through returned None, and every caller reads a missing rate as "unpriceable".
    343 asset rows are quoted that way: Judges Scientific has 5,930 bars going back to 2003 and
    was dropped from every portfolio holding it, silently, as if we had no prices for it.

    The rate is scaled by the divisor rather than the price being divided, which is the same
    arithmetic (`eur = pence/100/rate_gbp == pence/(100*rate_gbp)`) but keeps it in ONE place:
    a caller that converts a price can no longer forget the ÷100 and quote a £46.75 share at
    £4,675. `SUBUNIT` is `asset_pipeline.fx`'s map — shared, not re-derived.
    """
    if not ccy or ccy == "EUR":
        return 1.0
    base, divisor = SUBUNIT.get(ccy, (ccy, 1.0))
    tbl = fx.get(base)
    if not tbl:
        return None
    if when in tbl:
        return tbl[when] * divisor
    earlier = [d for d in tbl if d <= when]
    return tbl[max(earlier)] * divisor if earlier else None


def _window_rows(members: list[dict], closes: dict[int, list[tuple[str, float]]],
                 fx: dict[str, dict[str, float]],
                 start_anchor: str,
                 marks: dict[int, dict] | None = None) -> tuple[list[dict], list[dict]]:
    """Per-member return + start-of-window cap, for ONE window. Returns (rows, split_adjusted).

    Extracted so a caller can price SEVERAL windows off ONE price load — and, more importantly,
    so they all use the SAME weighting. A second copy of this loop is a second place for the
    look-ahead bias to creep back in.

    ⚠ TWO WAYS IN, ONE LOOP. `closes` is the whole series per member; `marks` (when given) is just
    what this window consumes — `{start:(d,p), end:(d,p), jumps:[(prev,cur)]}`, selected in
    Postgres. The arithmetic below is identical either way, which is the point: the loader is an
    optimisation and must never become a second definition of an index return.

    ⚠ WHY THE SERIES WAS EVER LOADED WHOLE. This loop reads exactly TWO prices per member — but
    `_split_adjust` reads all of them, because our stored closes are not split-adjusted and a split
    is only visible as a one-day discontinuity between CONSECUTIVE bars. So the marks path has to
    carry that evidence with it (`jumps`); two bare prices cannot tell a 9:1 split from a −89% year
    and KLA would read −80.6% at a weight it never had. See `split_factor`.
    """
    rows: list[dict] = []
    adjusted: list[dict] = []
    for m in members:
        if marks is not None:
            mk = marks.get(m["company_id"])
            if not mk or not mk.get("start") or not mk.get("end"):
                continue                    # no opening mark -> it was not in the basket
            (first_d, first_p), (last_d, last_p) = mk["start"], mk["end"]
            split = split_factor(mk.get("jumps") or [])
            if split:
                # The start sits before every jump in the window, so it alone needs rebasing —
                # `_split_adjust` scales "every earlier close" by exactly this product.
                first_p *= split
                adjusted.append({"company_name": m.get("company_name"),
                                 "ticker": m.get("gurufocus_ticker"), "factor": split})
        else:
            s = closes.get(m["company_id"]) or []
            # Our stored closes are NOT split-adjusted and never self-heal — see `_split_adjust`.
            s, split = _split_adjust(s)
            if split:
                adjusted.append({"company_name": m.get("company_name"),
                                 "ticker": m.get("gurufocus_ticker"), "factor": split})
            first = _at_or_before(s, start_anchor)
            if not first or not s:
                continue                    # no opening mark -> it was not in the basket
            last_d, last_p = s[-1]
            first_d, first_p = first
        if first_p <= 0 or last_d <= first_d:
            continue

        ccy = m.get("currency") or "USD"
        r0 = _rate(fx, ccy, first_d)
        r1 = _rate(fx, ccy, last_d)
        if not r0 or not r1:
            continue

        # THE WEIGHT IS AS OF THE START. Roll the CURRENT cap back on the price move — the
        # share count is what stays put, not the cap. Using cap_now here is look-ahead bias.
        cap_now_eur = float(m["market_cap_eur"])
        cap_start_eur = cap_now_eur * (first_p / last_p)

        rows.append({
            "company_id": m["company_id"],
            "company_name": m.get("company_name"),
            "ticker": m.get("gurufocus_ticker"),
            "isin": m.get("isin"),
            "currency": ccy,
            "start_date": first_d, "start_price": first_p,
            "end_date": last_d, "end_price": last_p,
            "return_local_pct": (last_p / first_p - 1.0) * 100.0,
            "return_eur_pct": ((last_p / r1) / (first_p / r0) - 1.0) * 100.0,
            "market_cap_eur": cap_now_eur,
            "start_cap_eur": cap_start_eur,
            # ⚠ PROVENANCE FOR THE CAP, AND IT DESCRIBES `market_cap_eur` — NOT THE WEIGHT.
            # `cap_start_eur` above is the number the weight is formed from, rolled back on the
            # price move; the cap shown to a reader is TODAY's. So `cap / Σcap` does not reproduce
            # the Weight column and is not supposed to — the surface has to say so.
            # `.get()` because this builder is shared with the GuruFocus path, whose members
            # carry no such fields.
            "market_cap_native": m.get("market_cap_native"),
            "market_cap_currency": m.get("market_cap_currency"),
            "market_cap_checked_at": (str(m["market_cap_checked_at"])
                                      if m.get("market_cap_checked_at") else None),
        })
    return rows, adjusted


# ── the cap ──────────────────────────────────────────────────────────────────────────────────
# A CAP IS A PROPERTY OF THE INDEX, NOT OF THE ARITHMETIC.
#
# The S&P 500 and ACWI are uncapped: no constituent is near a level where a cap would bind, so
# raw cap weights ARE the index's weights. The AEX is not that kind of index. It holds 25 names
# and Euronext caps a constituent at 15% at each review — precisely BECAUSE ASML would otherwise
# swallow it. Measured on our own data: uncapped, ASML is 37.53% of the AEX; the real index says
# 15.00%. Shipping the uncapped number would not be an approximation of the AEX, it would be an
# ASML tracker wearing the AEX's name.
#
# So the cap lives here, keyed by label, and `index_weights` is the ONE place a weight is formed.
# It had already leaked into FOUR copies of `start_cap_eur / total` (two here, two in
# `_asset_benchmark`) — exactly what `_window_rows`' own docstring warns about, and a cap applied
# to three of four would be worse than no cap at all.
INDEX_CAP_PCT: dict[str, float] = {
    "AEX": 15.0,
}


def index_weights(rows: list[dict], label: str) -> list[float]:
    """Start-of-window weights in PERCENT, aligned to `rows`, summing to 100.

    Uncapped (the default, and every index but the AEX) this is just `start_cap_eur / total` —
    bit-identical to what the four call sites each used to compute inline.

    Where the index caps, the excess above the cap is redistributed pro rata across the members
    still under it, repeatedly: lifting the others can push one of THEM over, so a single pass
    silently leaves a constituent above the cap. The loop is bounded — each pass adds at least
    one member to the capped set, so it cannot run longer than there are rows.

    ⚠ THE CAP IS APPLIED AT THE WINDOW OPEN, NOT AT THE INDEX'S REVIEW DATE. Euronext caps at a
    quarterly review and then lets the weights DRIFT with prices until the next one; this engine
    is buy-and-hold from the window open, so what we produce is "capped as at the window open,
    then held". For a YTD window that is close (the last real review was December). For an
    arbitrary window it is an approximation, and it is the honest one available: capping at the
    true review date would need the review calendar AND a rebalance the rest of this engine does
    not model.

    ⚠ A CAP IS NOT A FLOAT ADJUSTMENT, AND IT DOES NOT STAND IN FOR ONE. The AEX weights on free
    float; `market_cap_eur` is a FULL cap (see the module docstring — it over-weights family- and
    state-held names whatever the price source). Heineken is ~50% held by Heineken Holding and
    Prosus carries the Naspers cross-holding, so both stay over-weighted here even after capping.
    The cap fixes the ASML problem. It does not fix that one.
    """
    total = sum(r["start_cap_eur"] for r in rows)
    if total <= 0 or not rows:
        return [0.0] * len(rows)
    w = [r["start_cap_eur"] / total * 100.0 for r in rows]

    cap = INDEX_CAP_PCT.get(label)
    if cap is None:
        return w

    # ⚠ REFUSED, NOT FUDGED. With n members a cap of `cap`% can hold at most n*cap% of weight; if
    # that is under 100 the constituents cannot sum to the index and no redistribution exists.
    # Silently returning weights that sum to 75% would understate every return by a quarter. For
    # the AEX (25 names, 15%) the ceiling is 375% — so this fires only when the universe itself
    # has collapsed to a handful of priced names, which is a fact worth raising, not smoothing.
    if len(w) * cap <= 100.0:
        raise ValueError(
            f"{label}: a {cap}% cap over {len(w)} priced members caps out at {len(w) * cap:.0f}% "
            f"— the weights cannot sum to 100%. The universe is too thin to cap, not the cap "
            f"too tight.")

    capped: set[int] = set()
    for _ in range(len(w) + 1):
        over = [i for i, x in enumerate(w) if x > cap + 1e-9]
        if not over:
            break
        excess = sum(w[i] - cap for i in over)
        for i in over:
            w[i] = cap
        capped.update(over)
        rest = [i for i in range(len(w)) if i not in capped]
        rest_total = sum(w[i] for i in rest)
        if rest_total <= 0:
            break                    # unreachable given the guard above; never divide by zero
        for i in rest:
            w[i] += excess * w[i] / rest_total
    return w


def index_returns(label: str, starts: list[str]) -> dict[str, dict]:
    """Cap-weighted EUR/local return for `label` over SEVERAL windows, from ONE price load.

    Exists because a benchmark must be measured over the SAME window as whatever it is compared
    against — a model portfolio's YTD opens at `max(1 Jan, inception)` and its since-inception
    window earlier still, and putting either beside a 1-January index return compares two
    different periods and calls the difference alpha. Two `compute_index` calls would reload
    every close twice (4–9s each); this loads once and prices each window off it, through the
    same `_window_rows` — so they cannot weight differently from each other or from /benchmarks.
    """
    members = _members(label)
    if not members or not starts:
        return {}
    earliest = min(starts)
    lookback = (date.fromisoformat(earliest) - timedelta(days=45)).isoformat()
    today = date.today().isoformat()

    closes = _closes([m["company_id"] for m in members], lookback, today)
    fx = _fx_to_eur({(m.get("currency") or "USD") for m in members}, lookback, today)

    out: dict[str, dict] = {}
    for s in sorted(set(starts)):
        rows, _ = _window_rows(members, closes, fx, s)
        total = sum(r["start_cap_eur"] for r in rows)
        if not rows or total <= 0:
            out[s] = {"eur_pct": None, "local_pct": None, "members": 0, "start_date": None}
            continue
        w = index_weights(rows, label)
        eur = sum(x / 100.0 * r["return_eur_pct"] for x, r in zip(w, rows))
        loc = sum(x / 100.0 * r["return_local_pct"] for x, r in zip(w, rows))
        out[s] = {"eur_pct": eur, "local_pct": loc, "members": len(rows),
                  "start_date": min(r["start_date"] for r in rows)}
    return out


def compute_index(label: str = SP500_LABEL, year: int | None = None,
                  start: str | None = None) -> dict:
    """Cap-weighted return for `label` from `start` (default: 1 Jan of `year`) to today, in EUR
    and in local (USD for the S&P).

    Returns the members too, each with the weight it ACTUALLY had at the start of the WINDOW —
    so the number can be audited rather than believed.

    `start` exists so a benchmark can be measured over the SAME window as whatever it is being
    compared against. A model portfolio's "YTD" opens at `max(1 Jan, its inception)`, and its
    since-inception window opens earlier still — putting either beside a 1-January index return
    would compare two different periods and call the difference alpha. The start-of-window
    weighting below is what makes an arbitrary window safe: the weights are rolled back to
    `start`, never taken as of today (that is the look-ahead bias documented on `_split_adjust`'s
    neighbours — it turned +9.10% into +21.70%).
    """
    year = year or date.today().year
    start_anchor = start or f"{year}-01-01"   # the mark is the last close ON OR BEFORE this
    # Far enough back to find that mark across a holiday break — and, for an arbitrary start, a
    # thin name may not have traded for weeks.
    lookback = (date.fromisoformat(start_anchor) - timedelta(days=45)).isoformat()
    today = date.today().isoformat()

    members = _members(label)
    if not members:
        return {"label": label, "year": year, "members": [], "member_count": 0,
                "ytd_eur_pct": None, "ytd_local_pct": None,
                "note": f"No universe labelled {label!r}."}

    ids = [m["company_id"] for m in members]
    closes = _closes(ids, lookback, today)
    fx = _fx_to_eur({(m.get("currency") or "USD") for m in members}, lookback, today)

    rows, adjusted = _window_rows(members, closes, fx, start_anchor)

    total_start = sum(r["start_cap_eur"] for r in rows)
    if not rows or total_start <= 0:
        return {"label": label, "year": year, "members": [], "member_count": 0,
                "ytd_eur_pct": None, "ytd_local_pct": None,
                "note": "No constituent had a price on both ends of the window."}

    for r, x in zip(rows, index_weights(rows, label)):
        r["weight_pct"] = x

    ytd_eur = sum(r["weight_pct"] / 100.0 * r["return_eur_pct"] for r in rows)
    ytd_loc = sum(r["weight_pct"] / 100.0 * r["return_local_pct"] for r in rows)
    rows.sort(key=lambda r: -r["weight_pct"])

    return {
        "label": label,
        "year": year,
        "member_count": len(rows),
        "priced_of_universe": f"{len(rows)}/{len(members)}",
        "as_of": max(r["end_date"] for r in rows),
        "start_date": min(r["start_date"] for r in rows),
        "ytd_eur_pct": ytd_eur,
        "ytd_local_pct": ytd_loc,
        "members": rows,
        # Never silent: a corrected price is a claim, and the reader gets to see it.
        "split_adjusted": adjusted,
        "note": ("Cap-weighted on FULL market cap (the real index float-adjusts) using "
                 "start-of-year weights; membership is a snapshot, so mid-year index "
                 "changes are not replayed. Price return, not total return — dividends "
                 "are not included."),
    }


async def compute_index_async(label: str = SP500_LABEL, year: int | None = None) -> dict:
    return await asyncio.to_thread(compute_index, label, year)
