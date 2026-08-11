"""Refresh a reconstructed benchmark: constituents, market caps, and the two prices that
make its return — streamed, one line per step.

WHAT THE BUTTON DOES, IN THE ORDER IT DOES IT

    1. CONSTITUENTS   who is actually in this index right now. Rebuild the universe if it has
                      none, read its membership, bridge each member into the asset world by
                      ISIN, and resolve a bounded slice of whatever is not there yet.
    2. MARKET CAPS    one batched Yahoo quote per 100 symbols, for EVERY constituent — not
                      only the uncapped ones. The cap is the weight; a stale cap is a stale
                      index, and re-quoting all of them costs ~5 calls for the S&P.
    3. PRICES         per constituent, its START-OF-YEAR price and its CURRENT price. That is
                      the whole of what a YTD needs, and it is what the panel reads.

    Nothing else. No queue to watch, no second pass, no bounded slice that leaves the job half
    done and asks to be pressed again.

⚠ IT STREAMS BECAUSE IT CANNOT NOT. Step 3 is one Yahoo call per constituent — 491 for the S&P,
    1,684 for ACWI. Even several at a time that is minutes, which is not a POST, and a button that
    hangs with no output is indistinguishable from a broken one. Every step emits a line; the
    console shows the run as it happens.

⚠ THE PACING IS `asset_pipeline.yahoo`'s, NOT THIS MODULE'S — steps 2 and 3 run on a small pool
    (`_PRICE_WORKERS`) and let the shared governor decide the request rate: a token bucket on
    request starts, a semaphore on requests in flight, a canary probe and a cooldown on a ban.
    This module used to hold ONE of those slots and sleep 0.4s per constituent on top, which is
    two pacing schemes stacked and neither of them the one that knows when Yahoo is unhappy.
    Concurrency here is safe for a reason that does not generalise: fetching a KNOWN symbol is not
    resolution — see `_PRICE_WORKERS`.

⚠ PRICES ARE FETCHED BY SYMBOL, NEVER BY RE-RESOLVING. `extend_series(analysis_id, symbol, …)`
    asks Yahoo for an instrument we have already identified. Re-resolution asks *which listing
    is this*, and Yahoo answers an overloaded caller with an EMPTY search rather than a 429 —
    which is how Alphabet moved from GOOGL to a Vienna line 75,000x thinner. Step 1 is the only
    place identity is ever decided, and there it goes through the single paced queue worker.

⚠ A PRESS ALWAYS FETCHES. EVERY CONSTITUENT. NO STALENESS TOLERANCE.
    This briefly skipped any constituent whose newest close was within `DEFAULT_STALE_DAYS` of
    the market anchor, to save throttled calls. It was wrong, and wrong in the way that costs
    trust: ING sat at its 2026-07-30 close of 30.215 while the AEX's `as_of` read 2026-07-31, the
    row wore a stale-price warning, and pressing Refresh reported "already current" without ever
    looking. A human pressing Refresh is asking us to LOOK; deciding on their behalf that there
    is nothing to find is not a saving, it is a refusal dressed as a result.

    The saving it bought was also mostly imaginary. `extend_series` fetches only the gap after the
    last stored close, so a constituent with nothing new costs one small windowed request, and it
    comes back with an honest answer instead of an assumed one.

⚠ WHAT DOES NOT MOVE IS REPORTED AS SUCH, WITH THE REASON. "unchanged" is an ANSWER — the vendor
    has no closed bar after the one we hold — and it is a different fact from "we skipped it".
    Measured on the AEX, 2026-08-03: Yahoo's 2026-07-31 bar is NULL for every Amsterdam listing
    (INGA.AS, AD.AS — the venue has no close that day) while ASML's US line and the London lines
    do have one, which is why the index's `as_of` runs ahead of its Dutch constituents. That is a
    vendor gap, not a stale fetch, and no amount of refreshing will close it. The log says so per
    row rather than leaving a reader to suspect the button.

⚠ AND TODAY'S UNFINISHED SESSION IS NEVER STORED. Amsterdam was open when the above was measured
    and Yahoo happily returned an 08-03 bar at 30.13 — a live quote, not a close. `store.
    extend_series` drops any bar failing `yahoo.is_closed_bar`, so fetching mid-session is safe:
    it cannot write an intraday price that would move the index and then be overwritten at the
    bell. That guarantee is what makes "always fetch" the right default.
"""
from __future__ import annotations

import itertools
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone

from deps import IN_CHUNK_SIZE, supabase
from routers._benchmark_fill import (
    _NEEDS_CAP,
    _NEEDS_RESOLVE,
    _NO_ISIN,
    _USABLE,
    _build_universe,
    _classify,
    _drain_now,
    _grid_for,
    window_bounds,
)

_log = logging.getLogger(__name__)

# How many constituents are priced at once — and, in `_caps`, how many cap writes are in flight.
#
# ⚠⚠ THE POOL IS NOT THE RATE LIMIT. `asset_pipeline.yahoo` is, and always was: it paces every
# request START through a token bucket (`YAHOO_RPS`, default 10/s), caps in-flight requests with a
# semaphore (`YAHOO_CONCURRENCY`, default 4), then detects a ban with a canary probe and cools
# down. This loop used to run strictly serially AND sleep 0.4s per constituent on top of that — so
# it held one of the four permitted slots, paced itself twice, and spent ~3 minutes of the S&P's
# price step asleep before counting a single round trip.
#
# ⚠ TWICE THE SEMAPHORE, CAPPED AT 8. A constituent is one Yahoo call plus real database work (the
# COPY inside `extend_series`, then its marks), so a pool the size of the semaphore leaves Yahoo
# slots idle whenever a thread is talking to Postgres. Twice keeps the governor saturated without
# pretending the pool is what decides the request rate — raise `YAHOO_CONCURRENCY` for that.
#
# ⚠⚠ AND THIS IS NOT RESOLUTION, WHICH IS THE ONLY REASON IT MAY BE CONCURRENT AT ALL. The hazard
# that makes this repo a single Yahoo consumer — an overloaded caller gets an EMPTY search rather
# than a 429, and an empty candidate set hands the win to a thin foreign listing (NVDA on
# Stuttgart, Alphabet on Vienna) — belongs to `resolve()`. `extend_series` asks about a symbol we
# have ALREADY identified; an empty answer there means "no new bars", which is a fact and not a
# wrong listing. Step 1 still routes every identity decision through the one paced queue worker.
_PRICE_WORKERS = min(8, 2 * max(1, int(os.environ.get("YAHOO_CONCURRENCY", "4"))))

# Symbols per batched quote call. Yahoo's own chunk size — we chunk here too so the run can
# report progress per batch instead of going silent for five calls.
_QUOTE_BATCH = 100


def _pct(a: float, b: float) -> float | None:
    return ((b / a - 1.0) * 100.0) if a > 0 else None


def _days(a: str, b: str) -> int:
    return abs((date.fromisoformat(a) - date.fromisoformat(b)).days)


def _marks(aid: int, lookback: str, anchor: str) -> tuple[tuple[str, float] | None,
                                                          tuple[str, float] | None]:
    """This constituent's opening and closing mark: `(start, end)`, each `(date, close)`.

    The SAME two numbers `window_marks` selects for the index — the opening mark is the last
    close ON OR BEFORE the anchor (31 December IS the 1 January mark; no exchange prints a bar
    on New Year's Day), and the closing mark is simply its newest close.

    Two small indexed reads rather than one `COPY` per constituent: the primary key is
    `(analysis_id, target_date)`, so each is a single index seek, and next to the Yahoo call
    they cost nothing. `window_marks` stays the bulk reader for the panel itself.
    """
    start = (supabase.table("asset_price").select("target_date,close")
             .eq("analysis_id", aid).gte("target_date", lookback).lte("target_date", anchor)
             .not_.is_("close", "null")
             .order("target_date", desc=True).limit(1).execute().data or [])
    end = (supabase.table("asset_price").select("target_date,close")
           .eq("analysis_id", aid).not_.is_("close", "null")
           .order("target_date", desc=True).limit(1).execute().data or [])
    s = (start[0]["target_date"], float(start[0]["close"])) if start else None
    e = (end[0]["target_date"], float(end[0]["close"])) if end else None
    return s, e


def _market_anchor(emit) -> str | None:
    """The freshest close there is: ours, or the market's if it has published since.

    ⚠ NEVER THE CALENDAR. Anchoring on today flags every instrument every weekend, calls a bank
    holiday a fleet-wide failure and — worst — turns a Yahoo outage into "re-fetch all 1,684
    constituents" at the one moment fetching cannot work. Both halves come from
    `price_refresh`, which owns this definition; a second copy here would be free to drift from
    the daily tick's.
    """
    from asset_pipeline.price_refresh import global_latest_close, market_latest_close

    ours = global_latest_close()
    market = market_latest_close()
    anchor = max([d for d in (ours, market) if d], default=None)
    emit("progress", message=(
        f"  market anchor {anchor or '—'} "
        f"(our freshest close {ours or '—'}, Yahoo's {market or '—'})"))
    return anchor


def _constituents(label: str, emit) -> tuple[list[dict], dict, dict]:
    """Step 1 — who is in this index, and which of them we can reach in the asset world.

    Returns `(companies, grid, buckets)`. Builds the universe when the label has none (AEX had
    no `universe` row at all — a different fault from "constituents unpriced" with the identical
    symptom, 0 members) and resolves a bounded slice of whatever is not yet in the grid.

    ⚠ RESOLUTION GOES THROUGH THE QUEUE'S OWN SLICE, NOT A SECOND RESOLVER. `_drain_now` runs
    the single paced worker's unit of work and stands down if something else is already draining
    — two concurrent Yahoo consumers is exactly how a constituent lands on a thin foreign
    listing.
    """
    from asset_pipeline import queue as _queue
    from routers._asset_benchmark import _universe_company_ids

    emit("phase", phase="constituents", message="1/3 Gathering constituents…")
    ids = _universe_company_ids(label)
    if not ids:
        emit("progress", message=f"  no universe for {label} — running its reconstruction…")
        if _build_universe(label):
            ids = _universe_company_ids(label)
            emit("progress", message=f"  built {label}: {len(ids)} members")
        else:
            emit("progress", message=f"  ⚠ nothing can build {label}'s universe")
    if not ids:
        return [], {}, {_USABLE: [], _NEEDS_CAP: [], _NEEDS_RESOLVE: [], _NO_ISIN: []}

    companies: list[dict] = []
    for i in range(0, len(ids), IN_CHUNK_SIZE):
        companies += (supabase.table("company")
                      .select("company_id,company_name,isin")
                      .in_("company_id", ids[i:i + IN_CHUNK_SIZE])
                      .is_("delisted_at", "null").is_("out_of_scope_at", "null")
                      .execute().data or [])
    emit("progress", message=(
        f"  {len(ids)} members, {len(companies)} active "
        f"({len(ids) - len(companies)} delisted or out of scope)"))

    isins = sorted({(c.get("isin") or "").strip().upper() for c in companies if c.get("isin")})
    grid = _grid_for(isins)
    buckets = _classify(companies, grid)
    emit("progress", message=(
        f"  in the asset grid: {len(buckets[_USABLE]) + len(buckets[_NEEDS_CAP])} priceable, "
        f"{len(buckets[_NEEDS_RESOLVE])} not resolved, "
        f"{len(buckets[_NO_ISIN])} with no ISIN (unreachable from here)"))
    if buckets[_NO_ISIN]:
        # Named, never quietly dropped: 189 ACWI members have no ISIN (156 Indian, 28 British)
        # and GuruFocus cannot supply one either. A coverage figure that silently excludes India
        # is worse than no figure.
        emit("progress", message="  no ISIN: " + ", ".join(buckets[_NO_ISIN][:15])
             + (f" … +{len(buckets[_NO_ISIN]) - 15} more" if len(buckets[_NO_ISIN]) > 15 else ""))

    if buckets[_NEEDS_RESOLVE]:
        _queue.enqueue(buckets[_NEEDS_RESOLVE])
        emit("progress", message=f"  resolving {len(buckets[_NEEDS_RESOLVE])} unmapped ISIN(s)…")
        d = _drain_now(buckets[_NEEDS_RESOLVE])
        if d.get("worker_live"):
            emit("progress", message="  the ingest worker is already draining the queue — left to it")
        else:
            emit("progress", message=(
                f"  resolved {d['ok']}, {d['unmapped']} have no Yahoo listing (bonds, structured "
                f"products — they can never be priced), {d['failed']} failed, "
                f"{d['remaining']} left for the next press"))
        if d.get("ok"):
            grid = _grid_for(isins)
            buckets = _classify(companies, grid)
    return companies, grid, buckets


def _caps(isins: list[str], grid: dict[str, dict], emit) -> dict:
    """Step 2 — a market cap for EVERY constituent, from Yahoo, now.

    ⚠ ALL OF THEM, NOT ONLY THE UNCAPPED ONES. The cap IS the weight, and it moves every day —
    an index re-weighted from caps quoted three weeks ago is a three-week-old index wearing
    today's prices. It is also nearly free: `yahoo.quote` is batched at 100 symbols per call, so
    the S&P is five requests.

    ⚠ THE CAP CURRENCY IS NORMALISED TO ITS MAJOR UNIT, AND THE RATE IS ASKED FOR *THAT*. Yahoo
    quotes a London listing in PENCE but reports its `marketCap` in POUNDS — same payload, same
    `currency: "GBp"`, two different units. Passing "GBp" to `fx_to_eur` divides an already-major
    figure by 100 and yields a cap 100x too small that still looks like a number (Shell as a
    EUR 1.95bn company). `_cap_currency` is imported, never re-derived.
    """
    from asset_pipeline import yahoo
    from scripts.asset_backfill_marketcap import _cap_currency

    by_symbol: dict[str, int] = {}
    for isin in isins:
        g = grid.get(isin) or {}
        if g.get("yahoo_symbol") and g.get("analysis_id"):
            by_symbol.setdefault(g["yahoo_symbol"], g["analysis_id"])
    emit("phase", phase="caps",
         message=f"2/3 Market caps from Yahoo for {len(by_symbol)} constituent(s)…")
    if not by_symbol:
        return {"quoted": 0, "capped": 0, "no_cap": 0}

    syms = sorted(by_symbol)
    now = datetime.now(timezone.utc).isoformat()
    capped = quoted = 0
    no_cap: list[str] = []
    write_lock = threading.Lock()

    def _write(item: tuple[str, dict]) -> bool:
        """Store one constituent's cap. True when it came out weighable.

        ⚠ THE QUOTE IS PASSED IN, NOT CLOSED OVER. `quotes` is rebound once per batch inside the
        loop below; a closure would read whichever batch happened to be current when the thread
        got there — which is the right answer only because the pool is joined before the next
        batch, i.e. correct by accident.
        """
        sym, q = item
        native = q.get("marketCap")
        ccy = _cap_currency(q.get("currency"))
        eur = None
        if native and ccy:
            fx = yahoo.fx_to_eur(ccy) or 0.0
            eur = round(float(native) * fx, 2) if fx else None
        # ⚠ WRITTEN EVEN WHEN NULL, with the timestamp — otherwise every run re-asks Yahoo
        # about the same names it already knows have no cap (an ETF, a delisted line).
        supabase.table("asset_analysis").update({
            "market_cap_native": native, "market_cap_currency": ccy,
            "market_cap_eur": eur, "market_cap_checked_at": now,
        }).eq("analysis_id", by_symbol[sym]).execute()
        if eur:
            return True
        with write_lock:
            no_cap.append(sym)
        return False

    for i in range(0, len(syms), _QUOTE_BATCH):
        chunk = syms[i:i + _QUOTE_BATCH]
        quotes = yahoo.quote(chunk)
        quoted += len(quotes)
        # ⚠⚠ THE QUOTES ARE BATCHED AND THE WRITES WERE NOT — WHICH IS WHERE THIS STEP'S TIME WENT.
        # One Yahoo call answers 100 symbols; storing them was 100 separate PostgREST round trips,
        # serially, so the S&P spent five requests learning the caps and ~490 writing them down.
        # They are independent single-row updates against distinct primary keys, so they overlap
        # cleanly — the same client the fundamentals fill already drives from eight threads.
        #
        # ⚠ NOT AN `upsert` OF THE BATCH, WHICH IS THE OBVIOUS FASTER THING. PostgREST would turn
        # that into INSERT … ON CONFLICT, so a constituent whose `asset_analysis` row is missing
        # would be CREATED here from four cap columns — a junk row that then looks like an
        # instrument. An UPDATE that matches nothing is the honest no-op.
        with ThreadPoolExecutor(max_workers=_PRICE_WORKERS,
                                thread_name_prefix="bmcap") as pool:
            got = sum(pool.map(_write, [(s, quotes.get(s) or {}) for s in chunk]))
        capped += got
        emit("progress", message=(
            f"  batch {i // _QUOTE_BATCH + 1}: {len(chunk)} symbols → {got} caps written"))
    if no_cap:
        # A constituent with no cap weighs nothing, so it is invisible in a cap-weighted index
        # while looking perfectly healthy in the asset grid. Name them.
        emit("progress", message=(
            f"  ⚠ {len(no_cap)} with no market cap (they weigh nothing): "
            + ", ".join(no_cap[:15])
            + (f" … +{len(no_cap) - 15} more" if len(no_cap) > 15 else "")))
    return {"quoted": quoted, "capped": capped, "no_cap": len(no_cap)}


def _prices(companies: list[dict], isins: list[str], grid: dict[str, dict],
            anchor: str | None, emit, should_stop=None) -> dict:
    """Step 3 — the start-of-year price and the current price, per constituent.

    ⚠ THE ONLY CANCELLATION POINT THAT MATTERS IS IN HERE, and it is between constituents. This
    loop IS the run — 491 Yahoo calls for the S&P, 1,684 for ACWI, minutes either way, while steps
    1 and 2 are seconds. A Cancel that could only land between the three steps would, in practice,
    never land at all.

    ⚠ BETWEEN CONSTITUENTS, NEVER MID-ONE. Each unit fetches a gap and reads its two marks back;
    stopping inside that would leave the series written and the marks unread. The boundary here is
    where the database is consistent, which is the same rule the fundamentals fill follows
    (`ctx.check()` first thing in `_one`).

    `should_stop` is an optional predicate rather than an exception so this module keeps knowing
    nothing about `jobs.py` — the SSE caller passes nothing and behaves exactly as before.

    ⚠ ON A POOL (`_PRICE_WORKERS`), NOT IN A `for`, AND THE PACING LIVES IN `yahoo.py` — see that
    constant. What changes here as a consequence:

      * `[n/total]` COMES FROM AN ATOMIC COUNTER, NOT THE ITEM'S POSITION. Threads finish out of
        order, so a positional index would send the progress bar backwards; `n` is the count of
        constituents COMPLETED, which only ever rises. (Same rule, same reason, as the fundamentals
        fill.)
      * `stopped_at` IS A COUNT, NOT A PREFIX. With several constituents in flight there is no
        "everything before index k"; what is true, and what the reader needs, is how many finished
        before the stop. Everything fetched is written either way.
      * THE "WAS IT ALREADY AT THIS CLOSE?" READ IS HOISTED OUT — one grouped COPY for the whole
        index instead of a per-constituent round trip (see `before` below).

    Those two numbers ARE the index's YTD; everything else this button does exists to make them
    obtainable. Each constituent is one Yahoo call (`extend_series`, which fetches only the gap
    and recomputes the grid's coverage stats FROM THE DATABASE — `store_series` would re-download
    KO's 16,239 bars back to 1962 to add eight days, and would record "KO has 8 bars beginning
    2026" from the slice it fetched).

    The two marks are then read back and printed. They are the raw stored closes: the panel
    applies `_split_adjust` when it prices the index, so a name that split mid-window will show a
    change here that the index itself corrects. Printing the adjusted number instead would show a
    price that is in no database and on no exchange.
    """
    from asset_pipeline import store
    from asset_pipeline.price_refresh import latest_close_by_analysis

    lookback, start_anchor = window_bounds()
    by_isin_name = {(c.get("isin") or "").strip().upper(): c.get("company_name") or ""
                    for c in companies}
    todo: list[tuple[str, int, str]] = []       # (isin, analysis_id, symbol)
    for isin in isins:
        g = grid.get(isin) or {}
        if g.get("analysis_id") and g.get("yahoo_symbol"):
            todo.append((isin, g["analysis_id"], g["yahoo_symbol"]))
    todo.sort(key=lambda t: t[2])

    total = len(todo)
    emit("phase", phase="prices", message=(
        f"3/3 Start-of-year and current price for {total} constituent(s) — "
        f"window opens {start_anchor}, {_PRICE_WORKERS} at a time"))

    # ⚠ THE "WHAT DID WE HOLD BEFORE?" READ IS ONE GROUPED COPY FOR THE WHOLE INDEX. It used to be
    # a `_marks` call per constituent — two indexed round trips each, on the critical path, purely
    # to learn ONE date: the newest close we already had, which is what tells `moved` from
    # `unchanged` afterwards. On the S&P that was ~980 round trips to read 490 dates.
    #
    # ⚠ `latest_close_by_analysis` IS THE FLEET'S OWN DEFINITION, IMPORTED, NOT RE-DERIVED — the
    # same `max(target_date) WHERE close IS NOT NULL` the daily staleness sweep anchors on. A
    # second copy here would be free to disagree with the tick about what "our newest close" means.
    before = latest_close_by_analysis([aid for _, aid, _ in todo]) if todo else {}

    # `moved` = the series gained a closed bar. `unchanged` = the vendor has none after the one we
    # hold, which is an ANSWER (a venue with no close that day, a delisted line, a session still
    # open) and is why it is counted apart from a failure.
    out = {"total": total, "fetched": 0, "moved": 0, "unchanged": 0,
           "failed": 0, "no_start": 0, "no_end": 0}
    # ⚠ ONE LOCK OVER THE TALLY *AND* THE COUNTER. They are read together to build a line, and a
    # count that is incremented outside the lock can be reported twice under the same `n`.
    tally = threading.Lock()
    counter = itertools.count(1)
    stopped = threading.Event()

    def _one(item: tuple[str, int, str]) -> None:
        isin, aid, sym = item
        if should_stop and should_stop():
            # ⚠ SAID ONCE, BY WHICHEVER THREAD SEES IT FIRST. Every queued constituent passes
            # through here after a Cancel, and one line each would be hundreds of them.
            with tally:
                first = not stopped.is_set()
                stopped.set()
            if first:
                emit("progress", message=(
                    "  cancelling — the constituents already in flight will finish and be stored; "
                    "nothing further is started"))
            return
        name = by_isin_name.get(isin, "")[:28]
        was_end = before.get(aid)
        try:
            # `since` is the LOOKBACK, not the last close: the opening mark is the last bar on or
            # before 1 January, which sits inside it, and a constituent whose window was deleted
            # has nothing there at all.
            if store.extend_series(aid, sym, lookback) is None:
                store.store_series(aid, sym, None)   # no COPY path: slow, correct, never wrong
        except Exception as e:  # noqa: BLE001 — one dead symbol must not end the run
            with tally:
                out["failed"] += 1
                n = next(counter)
            emit("progress", message=(
                f"  [{n}/{total}] {sym:<12} {name:<28} FAILED {type(e).__name__}: {e}"))
            _log.warning("[benchmark_refresh] %s: %s: %s", sym, type(e).__name__, e)
            return

        start, end = _marks(aid, lookback, start_anchor)
        with tally:
            out["fetched"] += 1
            # `no_start` is not a failure of this fetch: the instrument had not listed when the
            # year opened, or its series begins later. It cannot contribute a YTD, and the index
            # drops it.
            kind = ("no_start" if not start else "no_end" if not end
                    else "unchanged" if (was_end and end[0] == was_end) else "moved")
            out[kind] += 1
            n = next(counter)
        head = f"  [{n}/{total}] {sym:<12} {name:<28} "
        if kind == "no_start":
            emit("progress", message=(
                head + f"no start-of-year price (first close {end[0] if end else '—'}) "
                "— cannot be priced over this window"))
        elif kind == "no_end":
            emit("progress", message=head + "no current price")
        else:
            chg = _pct(start[1], end[1])
            marks = (f"{start[0]} {start[1]:.4g} → {end[0]} {end[1]:.4g}"
                     + (f" ({chg:+.2f}%)" if chg is not None else ""))
            # ⚠ DID IT ACTUALLY MOVE? Printing the marks alone cannot answer the question the
            # press was asking. A row that comes back on the same date it went in is the vendor
            # saying "there is nothing after this" — and when that date trails the index's own
            # `as_of`, that gap is a property of the LISTING, not of our fetch. Measured on the
            # AEX: Yahoo's 2026-07-31 bar is null for every Amsterdam line, so ING stays at its
            # 07-30 close of 30.215 no matter how often this runs. Saying so is the difference
            # between a data gap and a suspected bug.
            if kind == "unchanged":
                behind = (f" — trails the index's {anchor}, so this LISTING has no close that day"
                          if anchor and end[0] < anchor else "")
                emit("progress", message=(
                    head + marks
                    + f"  · unchanged, Yahoo has no closed bar after {end[0]}{behind}"))
            else:
                emit("progress", message=(
                    head + marks + f"  · NEW close ({was_end or 'nothing'} → {end[0]})"))

    if todo:
        with ThreadPoolExecutor(max_workers=_PRICE_WORKERS,
                                thread_name_prefix="bmprice") as pool:
            # `list(...)` so an exception surfaces here rather than being swallowed by the
            # executor's lazy iterator.
            list(pool.map(_one, todo))
    if stopped.is_set():
        # ⚠ REPORTED, AND THE COUNTS SO FAR ARE KEPT. Everything already fetched is written and
        # real; returning the tally is what lets the summary say "stopped after 140 of 491" rather
        # than implying the whole step was lost.
        ran = out["fetched"] + out["failed"]
        out["stopped_at"] = ran
        emit("progress", message=(
            f"  cancelled — {ran} of {total} constituent(s) were fetched; all of it is stored"))
    return out


def refresh_benchmark(label: str, emit, should_stop=None) -> dict:
    """The whole run, emitting one line per step. Returns the summary the `done` event carries.

    `emit(msg_type, **fields)` is the SSE sender — the same shape the AIRS scan uses.

    `should_stop` is an optional `() -> bool` the JOB wrapper passes so a Cancel can land; it is
    checked between the three steps and, far more usefully, between constituents inside `_prices`
    (see there — that loop is the whole runtime). Absent, nothing changes: the plain SSE caller
    passes nothing and this behaves exactly as it always did.

    ⚠ A CANCELLED RUN STILL RETURNS ITS SUMMARY, and `stopped` says so. Raising here instead would
    throw away the counts for work that really happened — a run stopped after 300 of 491 has 300
    constituents freshly priced, and reporting that as nothing would invite pressing the button
    again from scratch.
    """
    started = time.time()
    emit("progress", message=f"[{label}] refresh started")
    companies, grid, buckets = _constituents(label, emit)
    if not companies:
        return {"label": label, "universe_members": 0,
                "note": f"No universe labelled {label!r} and nothing able to build one."}

    priceable = buckets[_USABLE] + buckets[_NEEDS_CAP]
    if should_stop and should_stop():
        return {"label": label, "universe_members": len(companies), "priceable": len(priceable),
                "stopped": True, "note": "cancelled after the constituents step"}
    caps = _caps(priceable, grid, emit)
    # Re-read the grid: the caps just written are what makes a `needs_cap` constituent weighable,
    # and step 3 prices everything the grid can reach either way.
    grid = _grid_for(sorted({(c.get("isin") or "").strip().upper()
                             for c in companies if c.get("isin")}))
    anchor = _market_anchor(emit)
    px = _prices(companies, priceable, grid, anchor, emit, should_stop)

    took = time.time() - started
    summary = {
        "label": label,
        # Present ONLY on a cancelled run, with the count that was reached. A key that is absent on
        # every healthy run cannot be mistaken for a zero.
        **({"stopped": True, "stopped_at": px["stopped_at"]} if "stopped_at" in px else {}),
        "universe_members": len(companies),
        "priceable": len(priceable),
        "needs_resolve": len(buckets[_NEEDS_RESOLVE]),
        "no_isin": len(buckets[_NO_ISIN]),
        "capped": caps["capped"],
        "no_cap": caps["no_cap"],
        "prices_total": px["total"],
        "prices_fetched": px["fetched"],
        # Of the ones fetched: how many gained a closed bar, and how many the vendor had nothing
        # newer for. The second is an answer, not a miss — see `_prices`.
        "prices_moved": px["moved"],
        "prices_unchanged": px["unchanged"],
        "prices_failed": px["failed"],
        "no_start_price": px["no_start"],
        "market_anchor": anchor,
        "seconds": round(took, 1),
    }
    emit("progress", message=(
        f"[{label}] done in {took:.0f}s — {len(priceable)} constituents, {caps['capped']} caps, "
        f"{px['fetched']} price series fetched — {px['moved']} gained a new close, "
        f"{px['unchanged']} already at the vendor's latest"
        + (f", {px['failed']} failed" if px["failed"] else "")))
    return summary


def refresh_year() -> int:
    """The year the window belongs to — surfaced so the log can state it."""
    return date.today().year
