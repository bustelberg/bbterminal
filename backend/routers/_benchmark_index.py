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
from datetime import date

from deps import IN_CHUNK_SIZE, supabase

# The `universe.label` of the reconstructed membership (Wikipedia + OpenFIGI, the /sp500 page).
SP500_LABEL = "SP500"

# GuruFocus daily close, in the LISTING's currency. (`metric_data` is EAV — the value lives
# in `numeric_value`, and the same table holds `market_cap_bn`, `score`, … so the
# source_code filter is not optional.)
_CLOSE_METRIC = "close_price"
_CLOSE_SOURCE = "gurufocus"


def _members(label: str) -> list[dict]:
    """The index's companies, deduped to ONE ROW PER COMPANY (see the module header).

    The price currency comes from the EXCHANGE, not from `market_cap_currency` — they are
    different fields and can disagree, and it is the exchange's currency the close is
    quoted in.
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
        rows += (supabase.table("company")
                 .select("company_id,company_name,gurufocus_ticker,isin,market_cap_eur,"
                         "exchange_id")
                 .in_("company_id", ids[i:i + IN_CHUNK_SIZE])
                 .is_("delisted_at", "null").is_("out_of_scope_at", "null")
                 .not_.is_("market_cap_eur", "null").execute().data or [])
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
    """
    out: dict[str, dict[str, float]] = {}
    cur = sorted(c for c in currencies if c and c != "EUR")
    for i in range(0, len(cur), IN_CHUNK_SIZE):
        rows = (supabase.table("fx_rate")
                .select("currency_code,rate_date,rate")
                .in_("currency_code", cur[i:i + IN_CHUNK_SIZE])
                .gte("rate_date", start).lte("rate_date", end).execute().data or [])
        for r in rows:
            if r["rate"]:
                out.setdefault(r["currency_code"], {})[r["rate_date"]] = float(r["rate"])
    return out


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
    # Walk backwards: everything BEFORE a split has to be scaled onto today's basis.
    for k in range(len(out) - 1, 0, -1):
        prev_p, cur_p = out[k - 1][1], out[k][1]
        if prev_p <= 0:
            continue
        ratio = cur_p / prev_p
        if _JUMP_LO <= ratio <= _JUMP_HI:
            continue
        inv = 1.0 / ratio
        near = min(_SPLIT_RATIOS, key=lambda s: abs(s - inv))
        if near <= 0 or abs(inv - near) / near > _SPLIT_TOL:
            continue                      # a violent day, not a corporate action. Leave it.
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
    """FX on `when`, else the most recent earlier rate (the table is not dense on holidays)."""
    if not ccy or ccy == "EUR":
        return 1.0
    tbl = fx.get(ccy)
    if not tbl:
        return None
    if when in tbl:
        return tbl[when]
    earlier = [d for d in tbl if d <= when]
    return tbl[max(earlier)] if earlier else None


def compute_index(label: str = SP500_LABEL, year: int | None = None) -> dict:
    """Cap-weighted YTD for `label`, in EUR and in local (USD for the S&P).

    Returns the members too, each with the weight it ACTUALLY had at the start of the year —
    so the number can be audited rather than believed.
    """
    year = year or date.today().year
    start_anchor = f"{year}-01-01"          # the mark is the last close ON OR BEFORE this
    lookback = f"{year - 1}-12-01"          # enough to find it across the New Year holiday
    today = date.today().isoformat()

    members = _members(label)
    if not members:
        return {"label": label, "year": year, "members": [], "member_count": 0,
                "ytd_eur_pct": None, "ytd_local_pct": None,
                "note": f"No universe labelled {label!r}."}

    ids = [m["company_id"] for m in members]
    closes = _closes(ids, lookback, today)
    fx = _fx_to_eur({(m.get("currency") or "USD") for m in members}, lookback, today)

    rows: list[dict] = []
    adjusted: list[dict] = []
    for m in members:
        s = closes.get(m["company_id"]) or []
        # Our stored closes are NOT split-adjusted and never self-heal — see `_split_adjust`.
        s, split = _split_adjust(s)
        if split:
            adjusted.append({"company_name": m.get("company_name"),
                             "ticker": m.get("gurufocus_ticker"), "factor": split})
        first = _at_or_before(s, start_anchor)
        if not first or not s:
            continue                        # no opening mark -> it was not in the basket
        last_d, last_p = s[-1]
        first_d, first_p = first
        if first_p <= 0:
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
        })

    total_start = sum(r["start_cap_eur"] for r in rows)
    if not rows or total_start <= 0:
        return {"label": label, "year": year, "members": [], "member_count": 0,
                "ytd_eur_pct": None, "ytd_local_pct": None,
                "note": "No constituent had a price on both ends of the window."}

    for r in rows:
        r["weight_pct"] = r["start_cap_eur"] / total_start * 100.0

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
