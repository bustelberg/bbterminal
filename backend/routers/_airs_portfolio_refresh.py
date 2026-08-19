"""Refresh ONE AIRS model portfolio from source, end to end, and show its YTD being rebuilt.

WHY THIS EXISTS
    `AITopSelectie OFF FX` read +36.64% locally and +55.20% in production off IDENTICAL code
    (the FX paging fix shipped in 3cec3eb; both environments have it). Same code, two answers,
    means the inputs differ — and a YTD has FIVE of them, only one of which comes from AIRS:

        composition   airs_model_portfolio_position   weights + ISINs        <- AIRS
        instruments   asset_execution                 ISIN -> symbol, ccy    <- Yahoo/OpenFIGI
        prices        asset_price                     the two closes         <- Yahoo
        FX            fx_rate                         EUR conversion         <- ECB / Yahoo
        links         airs_portfolio_link             certificates           <- our own choices

    ⚠ SO "REFRESH FROM AIRS" CANNOT FIX A WRONG RETURN ON ITS OWN, AND THAT IS THE WHOLE POINT
    OF THIS MODULE. The existing per-row button re-scrapes the composition and nothing else. If
    production's disagreement comes from a missing price series or a short FX history — and
    those are the two that have actually bitten — re-reading AIRS all day changes nothing. This
    re-acquires all four fetchable inputs, in dependency order, and then recomputes the number
    and prints the arithmetic.

    The FX step is the one with real teeth. `sync_fx_rates_to_db` only ever extends FORWARD (it
    reads the stored max and fetches from max+1), so a currency whose history simply STARTS too
    late in one database is never repaired by anything, in either environment, for ever. A
    holding whose currency has no rate on or before the YTD anchor is dropped by `_eur_series`,
    silently leaves the basket, and the return is renormalised over what is left — which reads
    as a higher number, not as an error. That is exactly the shape of +55.20% against +36.64%.

⚠ IDENTITY IS DECIDED IN ONE PLACE, AND IT IS NOT THE PRICE STEP. Step 2 resolves unmapped ISINs
    through the ingest queue's own paced slice; step 4 fetches by the symbol we already hold.
    Yahoo answers an overloaded caller with an EMPTY search rather than a 429, so a second
    concurrent resolver is how Alphabet moved from GOOGL to a Vienna line 75,000x thinner.

⚠ IT STREAMS. An AIRS scrape plus one paced Yahoo call per holding is tens of seconds, and the
    point is to WATCH which input moves. Every step emits a line.
"""
from __future__ import annotations

import logging
import time
from datetime import date, timedelta

from deps import supabase
from routers._airs_portfolio_perf import ytd_anchor_for

_log = logging.getLogger(__name__)

# Yahoo answers an overloaded caller with an EMPTY result rather than a 429 — never race it.
_SLEEP_S = 0.4

# How far before the YTD anchor the FX history must reach. The opening mark is the last close on
# or before 1 January and may sit days earlier over a holiday break; a rate is needed on or
# before THAT bar, not on the anchor. `_ANCHOR_LOOKBACK_DAYS` in the perf module is the same
# 45-day idea applied to prices.
_FX_LEAD_DAYS = 45


def _emit_holdings(rows: list[dict], emit) -> None:
    for r in rows:
        w = r.get("percentage")
        emit("progress", message=(
            f"    {str(r.get('isin') or '(cash)'):<14} {str(r.get('fonds') or '')[:34]:<34} "
            f"{(f'{float(w):.4g}%' if w is not None else '—'):>9}"))


def _composition(portfolio_id: int, emit, wait: float | None = None) -> dict:
    """Step 1 — the composition, live from AirSPMS, written to our DB as it lands.

    This is the ONLY input AIRS owns. It is also the one most likely to differ between two
    environments that scanned at different times — and `positions_datum` is not cosmetic: it is
    the composition's effective date, so it decides where the YTD window opens
    (`max(1 Jan, inception)`). Two deployments holding compositions dated differently are not
    computing the same window, and their numbers were never comparable.

    ⚠⚠ AND IT IS THE ONLY STEP THAT TOUCHES THE SHARED AirSPMS SESSION, SO IT TAKES THE LOCK —
    steps 2-5 (Yahoo, OpenFIGI, the ECB, our own database) deliberately run outside it. That split
    is what lets several `refresh_portfolio_fully` calls run at once: the AIRS legs queue on one
    session, everything expensive overlaps.

    ⚠ THIS CLOSES A GAP THAT WAS DOCUMENTED AS KNOWN AND LEFT OPEN (`routers/airs.py`: "the
    scheduler's own model-scan ticks do not take it, and neither does the SSE endpoint above, so
    those two can still overlap this"). It was survivable while exactly one human pressed one
    button; a fan-out over this function would have made two threads drive one cookie jar, whose
    failure mode is not an error but two interleaved report downloads.

    `wait=None` keeps the old behaviour for the standalone SSE button — refuse rather than hang —
    and the step reports the refusal instead of quietly returning a stale composition.
    """
    from airs_vermogen import _LOCK, _acquire_session  # noqa: PLC0415

    from routers.airs import _live_positions  # noqa: PLC0415

    emit("phase", phase="composition", message="1/5 Composition — reading AirSPMS…")
    before = (supabase.table("airs_model_portfolio")
              .select("name,positions_datum,positions_scanned_at")
              .eq("id", portfolio_id).limit(1).execute().data or [])
    prev = before[0] if before else {}
    emit("progress", message=(
        f"  stored: {prev.get('name') or '?'} — composition dated "
        f"{prev.get('positions_datum') or 'none'}, scanned "
        f"{str(prev.get('positions_scanned_at') or 'never')[:19]}"))

    # ⚠ THE LOCK SPANS THE AIRS READ AND NOTHING ELSE — the DB write inside `_live_positions` is
    # part of that read (it persists what came back, so a live read cannot leave the stored copy
    # disagreeing with it) and belongs inside; the parsing below does not.
    if not _acquire_session(wait):
        # ⚠ REPORTED, NOT SWALLOWED. Returning the stored composition here would be the worst
        # outcome available: the step's own line would say "read AirSPMS" over a date nobody
        # re-read, and steps 2-5 would then rebuild a YTD on it and call the result a refresh.
        emit("progress", message="  ⚠ AIRS session busy — the composition was NOT re-read")
        raise RuntimeError(
            "the AirSPMS session is held by another scan; the composition was not re-read")
    try:
        raw = _live_positions(portfolio_id, None)
    finally:
        _LOCK.release()
    rows = raw.get("rows") or []
    # AIRS's own column names — `_live_positions` hands back the sheet, not our shape.
    norm = [{"isin": (str(r["ISINCode"]).strip() if r.get("ISINCode") else None),
             "fonds": (str(r["Fonds"]).strip() if r.get("Fonds") else None),
             "percentage": r.get("Percentage")} for r in rows]
    emit("progress", message=(
        f"  AIRS: composition dated {raw.get('datum') or 'none'}, {len(norm)} line(s)"
        + ("  ⚠ THE DATE MOVED — the YTD window opens somewhere else now"
           if raw.get("datum") and raw["datum"] != prev.get("positions_datum") else "")))
    _emit_holdings(norm, emit)
    return {"datum": raw.get("datum"), "rows": norm,
            "previous_datum": prev.get("positions_datum")}


def _instruments(isins: list[str], emit) -> dict[str, dict]:
    """Step 2 — ISIN → the asset row that can be priced. Resolve whatever is missing.

    ⚠ THROUGH THE QUEUE'S OWN SLICE (`_drain_now`), never a resolver of our own. It also stands
    down when something else is already draining: two Yahoo consumers is how a resolution lands
    on a thin foreign listing.
    """
    from asset_pipeline import queue as _queue  # noqa: PLC0415
    from routers._benchmark_fill import _drain_now  # noqa: PLC0415

    emit("phase", phase="instruments",
         message=f"2/5 Instruments — {len(isins)} ISIN(s) to bridge into the asset grid…")

    def _read() -> dict[str, dict]:
        out: dict[str, dict] = {}
        for r in (supabase.table("asset_execution")
                  .select("isin,analysis_id,yahoo_symbol,currency,status,name")
                  .in_("isin", isins).execute().data or []):
            if r.get("isin") and r["isin"] not in out:
                out[r["isin"]] = r
        return out

    ex = _read()
    missing = [i for i in isins if not (ex.get(i) or {}).get("analysis_id")]
    for isin in isins:
        e = ex.get(isin)
        emit("progress", message=(
            f"    {isin:<14} "
            + (f"{e['yahoo_symbol']:<12} {e.get('currency') or '?':<4} {str(e.get('name') or '')[:32]}"
               if e and e.get("analysis_id")
               else f"NOT RESOLVED ({(e or {}).get('status') or 'not in the grid'})")))
    if missing:
        # ⚠ THE COUNT AND THE PACE, BOTH. Each ISIN is a paced Yahoo resolve — search, quote and
        # profile, with 10-30s timeouts — run a few wide, so sixteen of them legitimately take
        # minutes. Emitting only this line and then going quiet until all sixteen finish is what
        # made a working refresh read as "stuck at 50%"; `on_each` relays one line per ISIN, so the
        # reader can see it moving and, if it ever does stall, see WHICH one it stalled on.
        emit("progress", message=f"  resolving {len(missing)} unmapped ISIN(s) — one Yahoo lookup "
                                 "each, so this is the slow step")
        _queue.enqueue(missing)
        d = _drain_now(missing,
                       on_each=lambda isin, outcome: emit("progress",
                                                          message=f"    {isin:<14} {outcome}"))
        if d.get("worker_live"):
            emit("progress", message="  the ingest worker is already draining — left to it")
        else:
            emit("progress", message=(
                f"  resolved {d['ok']}, {d['unmapped']} have no Yahoo listing (structured "
                f"products, in-house funds — they can never be priced), {d['failed']} failed"))
        if d.get("ok"):
            ex = _read()
    return ex


def _fx(currencies: set[str], anchor: str, emit) -> dict:
    """Step 3 — make sure `fx_rate` actually covers this portfolio's window, BOTH WAYS.

    ⚠ THE BACKWARDS HALF IS THE ONE NOTHING ELSE DOES. `sync_fx_rates_to_db` reads the stored
    max and fetches from max+1, so it can only ever extend forward. A currency whose history
    STARTS after the YTD anchor is therefore never repaired — not by the daily tick, not by a
    rescan, not by anything — and it fails silently in the worst possible direction:
    `_eur_series` drops every close with no rate on or before it, the holding loses its opening
    mark, it is classed unpriceable, it leaves the basket, and the return is renormalised over
    the rest. A portfolio missing its laggards reads HIGH, and reads like a number.

    One ECB/peg/Yahoo call per currency that needs it, upserted, idempotent.
    """
    from asset_pipeline.fx import SUBUNIT  # noqa: PLC0415
    from fx_rates import fetch_history  # noqa: PLC0415

    # The MAJOR code: `fx_rate` has GBP and has never had `GBp` — pence is a quoting convention,
    # not a currency, and asking for the literal string returns nothing at all.
    codes = sorted({SUBUNIT.get(c, (c, 1.0))[0] for c in currencies if c and c != "EUR"})
    need_from = (date.fromisoformat(anchor) - timedelta(days=_FX_LEAD_DAYS)).isoformat()
    emit("phase", phase="fx", message=(
        f"3/5 FX — {len(codes)} currency(ies) must reach back to {need_from} "
        f"(the window opens {anchor})"))

    out = {"checked": len(codes), "backfilled": 0, "extended": 0, "failed": 0, "currencies": {}}
    today = date.today().isoformat()
    for code in codes:
        lo = (supabase.table("fx_rate").select("rate_date").eq("currency_code", code)
              .order("rate_date").limit(1).execute().data or [])
        hi = (supabase.table("fx_rate").select("rate_date").eq("currency_code", code)
              .order("rate_date", desc=True).limit(1).execute().data or [])
        have_lo = lo[0]["rate_date"] if lo else None
        have_hi = hi[0]["rate_date"] if hi else None
        gap_back = (not have_lo) or have_lo > need_from
        gap_fwd = (not have_hi) or have_hi < today

        if not gap_back and not gap_fwd:
            emit("progress", message=f"    {code}  {have_lo} → {have_hi}  covered")
            out["currencies"][code] = {"from": have_lo, "to": have_hi, "action": "none"}
            continue
        emit("progress", message=(
            f"    {code}  {have_lo or 'nothing'} → {have_hi or 'nothing'}  "
            + ("⚠ HISTORY STARTS AFTER THE WINDOW — every close before it converts to nothing, "
               "so the holding silently leaves the basket. Backfilling…" if gap_back
               else "extending forward…")))
        try:
            # ONE call covering the whole span, so a currency needing both ends is not fetched
            # twice. `fetch_history` is ECB / USD-peg / TWD behind one name.
            rates = fetch_history(code, need_from) or []
            rows = [{"currency_code": code, "rate_date": r["date"], "rate": r["rate"]}
                    for r in rates if r.get("date") and r.get("rate")]
            for i in range(0, len(rows), 500):
                supabase.table("fx_rate").upsert(
                    rows[i:i + 500], on_conflict="currency_code,rate_date").execute()
            lo2 = (supabase.table("fx_rate").select("rate_date").eq("currency_code", code)
                   .order("rate_date").limit(1).execute().data or [])
            hi2 = (supabase.table("fx_rate").select("rate_date").eq("currency_code", code)
                   .order("rate_date", desc=True).limit(1).execute().data or [])
            new_lo = lo2[0]["rate_date"] if lo2 else None
            new_hi = hi2[0]["rate_date"] if hi2 else None
            out["backfilled" if gap_back else "extended"] += 1
            out["currencies"][code] = {"from": new_lo, "to": new_hi,
                                       "action": "backfilled" if gap_back else "extended",
                                       "rows": len(rows)}
            emit("progress", message=(
                f"    {code}  → {new_lo} → {new_hi}  ({len(rows)} rate(s) written)"
                + ("  ⚠ STILL SHORT — this currency has no published history that far back"
                   if new_lo and new_lo > need_from else "")))
        except Exception as e:  # noqa: BLE001 — one dead currency must not end the run
            out["failed"] += 1
            out["currencies"][code] = {"from": have_lo, "to": have_hi, "action": "failed"}
            emit("progress", message=f"    {code}  FAILED {type(e).__name__}: {e}")
            _log.warning("[airs_refresh] fx %s: %s: %s", code, type(e).__name__, e)
    return out


def _prices(ex: dict[str, dict], isins: list[str], anchor: str, emit) -> dict:
    """Step 4 — each holding's price series brought up to date from Yahoo.

    ⚠ BY SYMBOL, NEVER BY RE-RESOLVING (step 2 is the only place identity is decided), and via
    `extend_series`, which fetches the GAP and recomputes the grid's coverage stats FROM THE
    DATABASE — `store_series` would re-download a 16,000-bar history to add eight days and would
    record `bars`/`price_from` from the slice it fetched.

    "Current" carries `price_refresh.DEFAULT_STALE_DAYS`' tolerance, imported rather than
    reinvented: requiring the newest close to sit exactly on the anchor makes every market whose
    close has not published yet look behind, and re-fetches the fleet daily to learn nothing.
    """
    from asset_pipeline import store  # noqa: PLC0415
    from asset_pipeline.price_refresh import (  # noqa: PLC0415
        DEFAULT_STALE_DAYS,
        global_latest_close,
        market_latest_close,
    )

    ours, market = global_latest_close(), market_latest_close()
    mkt = max([d for d in (ours, market) if d], default=None)
    todo = [(i, ex[i]) for i in isins
            if (ex.get(i) or {}).get("analysis_id") and (ex.get(i) or {}).get("yahoo_symbol")]
    emit("phase", phase="prices", message=(
        f"4/5 Prices — {len(todo)} priceable holding(s); market anchor {mkt or '—'} "
        f"(ours {ours or '—'}, Yahoo's {market or '—'})"))

    out = {"total": len(todo), "fetched": 0, "already_current": 0, "failed": 0}
    for n, (isin, e) in enumerate(todo, 1):
        aid, sym = e["analysis_id"], e["yahoo_symbol"]
        last = (supabase.table("asset_price").select("target_date")
                .eq("analysis_id", aid).not_.is_("close", "null")
                .order("target_date", desc=True).limit(1).execute().data or [])
        was = last[0]["target_date"] if last else None
        if was and mkt and abs((date.fromisoformat(mkt)
                                - date.fromisoformat(was)).days) < DEFAULT_STALE_DAYS:
            out["already_current"] += 1
            emit("progress", message=(
                f"    [{n}/{len(todo)}] {sym:<12} {isin:<14} already current — last close {was}"))
            continue
        try:
            # From the window's own lookback, not from the last close: a holding whose series
            # stops before 1 January has no opening mark, and that is the gap worth closing.
            since = min(was or anchor, anchor)
            if store.extend_series(aid, sym, since) is None:
                store.store_series(aid, sym, None)
            out["fetched"] += 1
        except Exception as e2:  # noqa: BLE001
            out["failed"] += 1
            emit("progress", message=(
                f"    [{n}/{len(todo)}] {sym:<12} {isin:<14} FAILED "
                f"{type(e2).__name__}: {e2}"))
            time.sleep(_SLEEP_S)
            continue
        now = (supabase.table("asset_price").select("target_date")
               .eq("analysis_id", aid).not_.is_("close", "null")
               .order("target_date", desc=True).limit(1).execute().data or [])
        got = now[0]["target_date"] if now else None
        emit("progress", message=(
            f"    [{n}/{len(todo)}] {sym:<12} {isin:<14} {was or 'nothing'} → {got or 'nothing'}"
            + ("  (Yahoo has nothing newer)" if got == was else "")))
        time.sleep(_SLEEP_S)
    return out


def _recompute(portfolio_id: int, emit) -> dict:
    """Step 5 — the number, and the arithmetic under it.

    Reads `explain_portfolio_ytd`, which INSTRUMENTS `compute_portfolio_performance` rather than
    recomputing it — so the figure printed here is the figure the grid will show, and the legs
    below it sum to that figure exactly (`reconciles`). A refresh that reported a YTD computed
    its own way could agree with itself while disagreeing with the table it just refreshed.
    """
    from routers._airs_portfolio_perf import explain_portfolio_ytd  # noqa: PLC0415

    emit("phase", phase="recompute", message="5/5 Recomputing the YTD…")
    t = explain_portfolio_ytd(portfolio_id)
    p = t.get("portfolio") or {}
    load = t.get("load") or {}
    if not p:
        emit("progress", message=f"  ⚠ {t.get('error') or 'no derivation available'}")
        return t

    emit("progress", message=(
        f"  window {p['ytd_anchor']} → {p.get('latest_close_in_portfolio') or '?'}  "
        f"coverage {p['covered_pct']:.1f}%  "
        f"({p['resolved_holdings']} priced, {p['unresolved_holdings']} not)"))
    for code, info in sorted((load.get("fx_currencies") or {}).items()):
        emit("progress", message=(
            f"    fx {code}: {info['n']} rate(s) {info['from']} → {info['to']}"))
    for leg in t.get("legs") or []:
        if leg["status"] == "priced":
            emit("progress", message=(
                f"    {str(leg.get('yahoo_symbol') or leg.get('isin') or ''):<12} "
                f"{str(leg.get('fonds') or '')[:26]:<26} w={leg['weight']:>6.4g}%  "
                f"{leg['start_date']} {leg['start_price_eur']:.4g} → "
                f"{leg['end_date']} {leg['end_price_eur']:.4g}  "
                f"{leg['return_pct']:+.2f}%  contributes {leg['contribution_pp']:+.3f}pp"))
        elif leg["status"] != "zero_weight":
            emit("progress", message=(
                f"    {str(leg.get('isin') or '(cash)'):<12} "
                f"{str(leg.get('fonds') or '')[:26]:<26} w={leg['weight']:>6.4g}%  "
                f"{leg['status'].upper()}"
                + (f" (series {leg.get('series_first')} → {leg.get('series_last')})"
                   if leg.get("series_bars") else "")))
    emit("progress", message=(
        f"  YTD {p['ytd_pct']:+.4f}%  "
        f"(contributions sum to {p['sum_of_contributions_pp']:+.4f}pp — "
        f"{'reconciles' if p['reconciles'] else '⚠ DOES NOT RECONCILE'})"
        if p.get("ytd_pct") is not None else
        f"  no YTD — coverage {p['covered_pct']:.1f}% is under the floor"))
    return t


def refresh_portfolio(portfolio_id: int, emit, wait: float | None = None) -> dict:
    """All five inputs, re-acquired in dependency order, then the number rebuilt.

    Order matters and is not cosmetic: the composition decides which ISINs and which window, the
    instruments decide which currencies, the currencies decide what FX must cover, and only then
    can the prices be fetched and converted. Running FX before the composition would sync the
    currencies of the composition we USED to hold.

    ⚠⚠ THIS IS THE MODEL HALF OF A PORTFOLIO, NOT A PORTFOLIO. The other half is the AIRS BOOK
    (`airs_vermogen.refresh_one_portfolio`) and for months WHICH HALF A PRESS REFRESHED DEPENDED
    ON WHICH PAGE THE BUTTON WAS ON — /portfolios refreshed the model, /management-dashboard the
    book, and the Analyse modal inherited whichever one opened it. `refresh_portfolio_fully` is
    the single entry point that does both, and it is what every button now calls; nothing new
    should reach this directly.

    `wait` is passed to step 1 only — steps 2-5 take no lock. See `_composition`.
    """
    started = time.time()
    emit("progress", message=f"[portfolio {portfolio_id}] refresh started")
    comp = _composition(portfolio_id, emit, wait)
    rows = comp["rows"]
    isins = sorted({r["isin"] for r in rows if r.get("isin")})
    if not isins:
        emit("progress", message="  no ISIN-bearing holdings — nothing to price")
        return {"portfolio_id": portfolio_id, "note": "the composition has no instruments"}

    anchor = ytd_anchor_for(comp["datum"])
    ex = _instruments(isins, emit)
    fx = _fx({(e.get("currency") or "") for e in ex.values()}, anchor, emit)
    px = _prices(ex, isins, anchor, emit)
    trace = _recompute(portfolio_id, emit)

    p = trace.get("portfolio") or {}
    took = time.time() - started
    emit("progress", message=(
        f"[portfolio {portfolio_id}] done in {took:.0f}s — "
        f"composition {comp['datum']}, {px['fetched']} price series fetched, "
        f"{fx['backfilled']} FX history backfilled, YTD "
        + (f"{p['ytd_pct']:+.2f}%" if p.get("ytd_pct") is not None else "n/a")))
    return {
        "portfolio_id": portfolio_id,
        "name": p.get("name"),
        "composition_datum": comp["datum"],
        "composition_datum_changed": comp["datum"] != comp["previous_datum"],
        "holdings": len(rows),
        "instruments": len(isins),
        "resolved": sum(1 for e in ex.values() if e.get("analysis_id")),
        "fx_backfilled": fx["backfilled"],
        "fx_extended": fx["extended"],
        "prices_fetched": px["fetched"],
        "prices_already_current": px["already_current"],
        "prices_failed": px["failed"],
        "ytd_pct": p.get("ytd_pct"),
        "ytd_from": p.get("ytd_anchor"),
        "covered_pct": p.get("covered_pct"),
        "unresolved_holdings": p.get("unresolved_holdings"),
        "reconciles": p.get("reconciles"),
        "seconds": round(took, 1),
    }
