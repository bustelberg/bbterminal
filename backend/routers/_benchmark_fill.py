"""Fill a reconstructed benchmark's ASSET-world gap — what the panel's "Fill" button runs.

WHY A BENCHMARK READS 0 MEMBERS WITH A FULL UNIVERSE BEHIND IT
    `/api/benchmarks/index/{label}` prices from the asset world (yfinance), bridging
    `universe_membership -> company.isin -> asset_grid -> asset_price`. A constituent counts only
    when its ISIN is in the grid with `status='ok'`, an `analysis_id`, `bars > 0` AND a positive
    `market_cap_eur` — the cap is what weights it, so a resolved-but-uncapped name is invisible.
    Measured on the hosted project 2026-07-29: SP500 had 493 members, 493 ISINs, and **1** of them
    in the grid. The universe was never the problem.

    The panel could not say which of those four conditions failed, so this returns the breakdown
    and does the two things that are safe to do from a request.

⚠ RESOLUTION IS ENQUEUED, NEVER RUN HERE. Yahoo answers an overloaded caller with an EMPTY result
    rather than a 429, and an empty candidate set is exactly how a resolution lands on a thin
    foreign listing (NVDA-on-Stuttgart, Alphabet-on-Vienna). The repo's answer is ONE Yahoo
    consumer: `asset_ingest_queue`, drained by a single paced worker. Resolving inline would put a
    second consumer on the throttle and corrupt the very rows it was asked to create — so this
    enqueues and returns immediately.

⚠ CAPS ARE DIFFERENT, AND THAT IS WHY THEY RUN INLINE. `yahoo.quote` is BATCHED — 100 symbols per
    call — so capping 493 constituents is ~5 requests, not 493. It is also the only step that can
    finish in a request, and without it the queue can drain completely and the panel still shows
    zero, which reads as the button having done nothing.

⚠ A MEMBER WITH NO ISIN CANNOT BE REACHED FROM HERE AT ALL. 189 ACWI members have none (156
    Indian, 28 British) and GuruFocus cannot supply one either — it is blind to those markets.
    They are counted and named, never quietly dropped: a coverage figure that silently excludes
    India is worse than no figure.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import logging

from deps import IN_CHUNK_SIZE, supabase

_log = logging.getLogger(__name__)

# The four states a constituent can be in, in the order they must be fixed.
_USABLE, _NEEDS_CAP, _NEEDS_RESOLVE, _NO_ISIN = "usable", "needs_cap", "needs_resolve", "no_isin"


def _classify(companies: list[dict], grid: dict[str, dict]) -> dict[str, list[str]]:
    """Bucket each constituent by what is missing. Pure — the reason this is testable."""
    out: dict[str, list[str]] = {_USABLE: [], _NEEDS_CAP: [], _NEEDS_RESOLVE: [], _NO_ISIN: []}
    for c in companies:
        isin = (c.get("isin") or "").strip().upper()
        if not isin:
            out[_NO_ISIN].append(c.get("company_name") or f"company {c.get('company_id')}")
            continue
        g = grid.get(isin)
        priced = bool(g and g.get("status") == "ok" and g.get("analysis_id")
                      and (g.get("bars") or 0) > 0)
        if not priced:
            out[_NEEDS_RESOLVE].append(isin)
        elif float(g.get("market_cap_eur") or 0) <= 0:
            # ⚠ RESOLVED AND PRICED BUT UNWEIGHABLE — the state that looks like success in the
            # asset grid and still contributes nothing to the index.
            out[_NEEDS_CAP].append(isin)
        else:
            out[_USABLE].append(isin)
    return out


def _grid_for(isins: list[str]) -> dict[str, dict]:
    grid: dict[str, dict] = {}
    for i in range(0, len(isins), IN_CHUNK_SIZE):
        for r in (supabase.table("asset_grid")
                  .select("isin,analysis_id,yahoo_symbol,market_cap_eur,status,bars")
                  .in_("isin", isins[i:i + IN_CHUNK_SIZE]).execute().data or []):
            grid[(r.get("isin") or "").strip().upper()] = r
    return grid


def _backfill_caps(isins: list[str], grid: dict[str, dict]) -> dict:
    """Market caps for already-resolved constituents, via ONE batched Yahoo quote per 100 symbols.

    ⚠ THE CAP CURRENCY IS NORMALISED TO ITS MAJOR UNIT, AND THE RATE IS ASKED FOR *THAT*. Yahoo
    quotes a London listing in PENCE but reports its `marketCap` in POUNDS — same payload, same
    `currency: "GBp"`, two different units. Passing "GBp" to `fx_to_eur` divides an already-major
    figure by 100 and yields a cap 100x too small that still looks like a number (Shell as a
    EUR 1.95bn company). `_cap_currency` is imported, never re-derived — it is the same map the
    backfill script and `tests/test_minor_unit_fx.py` already hold.
    """
    from asset_pipeline import yahoo  # noqa: PLC0415
    from scripts.asset_backfill_marketcap import _cap_currency  # noqa: PLC0415

    by_symbol: dict[str, int] = {}
    for isin in isins:
        g = grid.get(isin) or {}
        sym, aid = g.get("yahoo_symbol"), g.get("analysis_id")
        if sym and aid:
            by_symbol.setdefault(sym, aid)
    if not by_symbol:
        return {"quoted": 0, "capped": 0}

    quotes = yahoo.quote(sorted(by_symbol))
    now = datetime.now(timezone.utc).isoformat()
    capped = 0
    for sym, aid in by_symbol.items():
        q = quotes.get(sym) or {}
        native = q.get("marketCap")
        ccy = _cap_currency(q.get("currency"))
        eur = None
        if native and ccy:
            fx = yahoo.fx_to_eur(ccy) or 0.0
            eur = round(float(native) * fx, 2) if fx else None
        # ⚠ WRITTEN EVEN WHEN NULL, with the timestamp — otherwise every run re-asks Yahoo about
        # the same names it already knows have no cap (an ETF, a delisted line), for ever.
        supabase.table("asset_analysis").update({
            "market_cap_native": native, "market_cap_currency": ccy,
            "market_cap_eur": eur, "market_cap_checked_at": now,
        }).eq("analysis_id", aid).execute()
        if eur:
            capped += 1
    return {"quoted": len(quotes), "capped": capped}


def _rebuild_sp500() -> bool:
    """Rebuild the SP500 universe from the Wikipedia reconstruction — the same code the /sp500
    page's import runs, scoped to what the benchmark actually stores.

    ⚠ SP500 IS NOT A `UniverseTemplate`, AND MUST NOT BECOME ONE. Registering it in `TEMPLATES`
    would stamp `template_key` on its universe row, and `/api/index-universe/indexes` — the list
    the /sp500 page itself renders — EXCLUDES any row with one. The index would vanish from its own
    page as a side effect of making a button work elsewhere. So the route back is wired here, where
    only Fill sees it, instead of in the registry everything else reads.

    ⚠ ONLY THE LATEST MONTH'S TICKERS ARE RESOLVED. `reconstruct_monthly_holdings` walks back to
    2000 and its union is 852 tickers, 286 of which have no company row — every one an OpenFIGI
    lookup for a name delisted a decade ago. `store_index_membership` keeps ONLY the newest month
    anyway (the single-snapshot model), so resolving the history buys nothing and costs the slowest
    part of the job. Measured 2026-07-30: the current set is 503 tickers, 491 already present — 12
    to resolve.

    ⚠ THE FULL CHANGELOG IS PASSED BACK, NOT `[]`. `store_index_membership` OVERWRITES the stored
    `index_changes/SP500.json` with whatever it is given, and that file backs
    `/api/index-universe/changes` and the /sp500 page's history. Deleting a universe never touched
    it; handing over an empty list would erase it as a side effect.

    Same best-effort contract as the template path — see `_build_universe`.
    """
    from index_universe.sp500 import (  # noqa: PLC0415
        reconstruct_monthly_holdings,
        resolve_and_create_companies,
        scrape_sp500,
        store_index_membership,
    )

    current, changes, info = scrape_sp500()
    monthly, filtered_changes = reconstruct_monthly_holdings(current, changes)
    if not monthly:
        return False
    latest = max(monthly)
    tickers = monthly[latest]
    lookup = resolve_and_create_companies(supabase, tickers, company_info=info)
    store_index_membership(
        supabase, "SP500", {latest: tickers}, filtered_changes, lookup,
        sector_lookup={t: (info.get(t) or {}).get("sector") for t in tickers},
    )
    return True


def rebuildable(label: str) -> bool:
    """Can Fill put this label's universe back? The ONE answer both Delete and Fill ask.

    A Delete offered for a label Fill cannot rebuild is a one-way door behind a button whose whole
    point is that it is reversible — so the delete guard and the rebuild route must never be able
    to disagree about which labels those are. They read this.
    """
    from index_universe.templates import TEMPLATES  # noqa: PLC0415

    return label in TEMPLATES or label == "SP500"


def _build_universe(label: str) -> bool:
    """Run the label's universe reconstruction. True when it ran.

    Two routes, because the labels arrived by two roads: a registered `UniverseTemplate`
    (ACWI / AEX / LEONTEQ / LONGEQUITY), or — for SP500 alone — the Wikipedia reconstruction, which
    deliberately stayed out of the template registry (see `_rebuild_sp500`).

    ⚠ BEST-EFFORT, AND A FAILURE IS NOT AN EXCEPTION HERE. The reconstruction scrapes third parties
    (iShares, Wikipedia, MSCI); a bad day there must leave the caller reporting "could not build"
    rather than 500-ing a button whose other work — enqueuing and capping — is unaffected.
    """
    from index_universe.templates import TEMPLATES, get_template  # noqa: PLC0415

    if not rebuildable(label):
        return False
    try:
        if label in TEMPLATES:
            get_template(label).refresh(supabase)
        else:
            return _rebuild_sp500()
        return True
    except Exception as e:  # noqa: BLE001
        _log.warning("[benchmark_fill] building the %s universe failed: %s: %s",
                     label, type(e).__name__, e)
        return False


def window_bounds(year: int | None = None) -> tuple[str, str]:
    """(lookback, start_anchor) for a YTD window — the SAME two dates `compute_index` prices from.

    The opening mark is the last close on or BEFORE 1 January, which lives in the 45-day lookback;
    deleting from the anchor would leave 31 December behind and the benchmark would still price.
    """
    from datetime import date, timedelta  # noqa: PLC0415

    start = f"{year or date.today().year}-01-01"
    return (date.fromisoformat(start) - timedelta(days=45)).isoformat(), start


def _constituent_analysis_ids(label: str) -> list[int]:
    """The `asset_analysis` ids the benchmark prices from — its constituents in the asset world."""
    from routers._asset_benchmark import members  # noqa: PLC0415

    mem, _ = members(label)
    return [m["company_id"] for m in mem]      # `_asset_benchmark.members` puts analysis_id here


def _drop_caps(analysis_ids: list[int]) -> int:
    """Clear the market caps Fill's `_backfill_caps` re-quotes. Returns rows cleared.

    ⚠ `market_cap_checked_at` GOES TOO. It exists so a name Yahoo has no cap for is not re-asked
    for ever; leaving it set would make this look like a cap we had already given up on rather than
    one we deliberately cleared.
    """
    n = 0
    for i in range(0, len(analysis_ids), IN_CHUNK_SIZE):
        chunk = analysis_ids[i:i + IN_CHUNK_SIZE]
        resp = (supabase.table("asset_analysis").update({
            "market_cap_native": None, "market_cap_currency": None,
            "market_cap_eur": None, "market_cap_checked_at": None,
        }, count="exact").in_("analysis_id", chunk).execute())
        n += resp.count or 0
    return n


def _drop_window_prices(analysis_ids: list[int], lookback: str) -> int:
    """Delete every close from `lookback` onward for these instruments. Returns rows deleted.

    ⚠ THE TAIL, NEVER A HOLE IN THE MIDDLE. Everything from the lookback forward goes, so each
    series simply ENDS earlier — which is a state the fleet already knows how to repair: the last
    close falls behind the market anchor, `price_refresh.find_stale` sees it, and `extend_series`
    fetches the gap. Deleting an interior slice instead would leave the newest close untouched, no
    staleness detector would ever fire, and the hole would be permanent (`extend_series` appends
    after the last close — it does not backfill).

    That is why this is safe to offer at all: **the damage is self-healing.** Fill refills it
    explicitly (`_refill_prices`), the 06:00 asset-price tick refills it overnight, and the AIRS
    holdings page refills a held name the moment someone expands its row.

    ⚠ IT IS STILL SHARED DATA. Measured 2026-07-30: 152 of ACWI's 1,684 constituents (86 of the
    S&P's, 11 of the AEX's) are also held in an AIRS book, so their YTD marks vanish here and those
    portfolio figures read short until something refills them. That cost is stated at the click.
    """
    n = 0
    for i in range(0, len(analysis_ids), IN_CHUNK_SIZE):
        chunk = analysis_ids[i:i + IN_CHUNK_SIZE]
        resp = (supabase.table("asset_price").delete(count="exact")
                .in_("analysis_id", chunk).gte("target_date", lookback).execute())
        n += resp.count or 0
    return n


def reset_benchmark(label: str, *, drop_caps: bool = True, drop_prices: bool = True) -> dict:
    """Undo everything Fill puts in place, so the whole button can be tested end to end.

    Three deletions, matching Fill's three jobs:

        membership   the `universe` row + its members    -> Fill's `_build_universe` rebuilds it
        market caps  `asset_analysis.market_cap_*`       -> Fill's `_backfill_caps` re-quotes them
        prices       `asset_price` from the lookback on  -> Fill's `_refill_prices` re-fetches them

    Deleting only the membership left two thirds of Fill untested: a constituent that is already
    resolved and already capped goes straight into the `usable` bucket, so the cap backfill and the
    price fetch never run and the counts read like success without either having done anything.

    ⚠ IT DOES NOT TOUCH THE ASSET GRID, THE SYMBOL, OR THE PRE-WINDOW HISTORY. Those are what make
    the refill cheap AND safe: every instrument keeps `status='ok'`, its `analysis_id` and its
    Yahoo symbol, so Fill re-fetches prices for a KNOWN symbol (`extend_series`) and nothing is
    ever re-RESOLVED. A re-resolve is the documented way a constituent lands on a thin foreign
    listing — Yahoo answers an overloaded caller with an empty search, and Alphabet went from GOOGL
    to a Vienna line 75,000x thinner. Deleting `bars` or the grid row is what would trigger that,
    and it is exactly what this does not do.

    ⚠ IT REFUSES A FROZEN SNAPSHOT. A frozen universe is a saved artifact a backtest is pinned to
    — reproducibility is its entire purpose — and no template can rebuild one. Only the live
    (`frozen_at IS NULL`) row is deletable, so a snapshot that happens to share this label survives.

    ⚠ AND IT REFUSES TO ORPHAN A DERIVED CHILD. `parent_universe_id` is ON DELETE SET NULL, not
    CASCADE, so a tightened variant of this universe would quietly lose its parent and go on being
    listed as though nothing had happened. Naming them and stopping is the honest answer; there is
    none for this label today, so the normal case never sees it.

    ⚠⚠ AND — THE ONE THAT MATTERS — IT REFUSES A LABEL FILL CANNOT REBUILD. This exists so the
    operator can delete and watch Fill refill, which is a promise about the label: `_build_universe`
    is the only route back. `rebuildable()` is that question, asked in ONE place by both this guard
    and the rebuild itself, so a label can never be deletable and unrebuildable at the same time.
    """
    if not rebuildable(label):
        from index_universe.templates import TEMPLATES  # noqa: PLC0415

        raise ValueError(
            f"Fill has no way to rebuild {label!r}, so deleting it here would be one-way. "
            f"Rebuildable labels: {', '.join(sorted(set(TEMPLATES) | {'SP500'}))}.")

    # The constituents' analysis ids, read BEFORE the membership goes — afterwards there is no
    # universe to ask, and the caps and prices below are keyed on exactly this set.
    priced = _constituent_analysis_ids(label) if (drop_caps or drop_prices) else []

    rows = (supabase.table("universe")
            .select("universe_id,label,frozen_at,template_key")
            .eq("label", label).execute().data or [])
    live = [r for r in rows if not r.get("frozen_at")]
    if not live:
        return {"label": label, "deleted": False, "members_deleted": 0,
                "note": (f"{label!r} has only frozen snapshots, which are never deleted here."
                         if rows else f"No universe labelled {label!r} to delete.")}
    uid = live[0]["universe_id"]

    children = (supabase.table("universe").select("label")
                .eq("parent_universe_id", uid).execute().data or [])
    if children:
        raise ValueError(
            f"{label!r} is the parent of {len(children)} derived universe(s) "
            f"({', '.join(c['label'] for c in children[:5])}) — deleting it would leave them "
            f"with no parent. Delete those first.")

    n = (supabase.table("universe_membership").select("company_id", count="exact")
         .eq("universe_id", uid).limit(1).execute().count or 0)
    # Caps and prices FIRST, while the membership still names the constituents — see `priced`,
    # which is read before any of this for the same reason.
    lookback, _ = window_bounds()
    caps = _drop_caps(priced) if (drop_caps and priced) else 0
    px = _drop_window_prices(priced, lookback) if (drop_prices and priced) else 0
    # Membership explicitly, then the row — the same order every other delete path here uses, so
    # a failure halfway leaves an empty universe rather than orphaned membership rows.
    supabase.table("universe_membership").delete().eq("universe_id", uid).execute()
    supabase.table("universe").delete().eq("universe_id", uid).execute()
    _log.warning("[benchmark_fill] reset %s — %d members, %d caps, %d price rows from %s. "
                 "Fill rebuilds all three.", label, n, caps, px, lookback)
    return {"label": label, "deleted": True, "members_deleted": n,
            "caps_cleared": caps, "price_rows_deleted": px, "prices_from": lookback,
            "had_template": bool(live[0].get("template_key")), "note": None}


# How many queued ISINs one press of Fill resolves inline. `process_slice` runs them concurrently
# behind the shared Yahoo throttle (`YAHOO_CONCURRENCY`, default 4), so this is ~20s, not 25×3s.
_RESOLVE_PER_PRESS = 25

# The queue's newest activity timestamp as it stood after OUR last drain — see `_drain_now`. Any
# value other than this means somebody else has been draining since, and we stand down.
_LAST_SELF_DRAIN: str | None = None


def _drain_now(isins: list[str], limit: int = _RESOLVE_PER_PRESS) -> dict:
    """Resolve a bounded slice of the ingest queue RIGHT NOW, rather than leaving it for a worker.

    ⚠ "QUEUED FOR INGEST" IS A PROMISE ABOUT A PROCESS THAT MAY NOT EXIST. The in-process worker is
    OPT-IN (`ASSET_QUEUE_INPROCESS`); the default drainer is the standalone
    `scripts/asset_queue_worker.py`, which on a single-service deployment is nobody. Measured in
    production 2026-07-30: Fill on the AEX reported "25 queued for ingest (a paced worker drains
    them — minutes to hours)" and nothing ever drained them. A button that reports work no one will
    do is worse than a button that does nothing, because it reads like progress.

    ⚠ IT IS THE WORKER'S OWN STEP, NOT A SECOND RESOLVER. `queue.process_slice` is THE one Yahoo
    consumer's unit of work — OpenFIGI batch lookup, then the throttled resolve+store, marking each
    done/failed. Writing a faster path here would be a second consumer with its own idea of pacing,
    which is precisely how a resolution lands on a thin foreign listing (Yahoo answers an
    overloaded caller with an EMPTY search, not a 429).

    ⚠ AND IT STANDS DOWN IF A WORKER IS ALREADY LIVE. Two drainers competing for the throttle is
    the same failure from the other direction, so when `is_worker_active()` says something is
    already consuming Yahoo, the queue is left to it and the caller is told that is what happened.

    ⚠ SCOPED TO **THIS BENCHMARK'S** ISINs. The queue is FIFO by `added_at` and holds ~10,000
    pending rows, so an unscoped slice resolves ten-thousand-place-old strangers and leaves the 71
    constituents this press just enqueued exactly where they were. Measured 2026-07-30 — the press
    reported work and the benchmark did not move.
    """
    global _LAST_SELF_DRAIN
    from asset_pipeline import queue as _queue  # noqa: PLC0415

    # ⚠ THE GUARD MUST NOT SEE ITS OWN FOOTPRINTS. `is_worker_active()` answers "has anything moved
    # a row out of pending recently" — and draining a slice IS that. So the first press made the
    # second one stand down for ten minutes and report `worker_live`, i.e. pressing Fill twice did
    # nothing the second time, which is the exact symptom this whole change exists to remove.
    # Comparing the queue's newest activity with the timestamp OUR last drain left tells the two
    # apart: unchanged means we are the only thing touching it, and we may carry on.
    # ⚠ ONE MINUTE, NOT THE DEFAULT TEN. The in-process worker ticks every 20 SECONDS, so anything
    # actually draining shows activity inside a minute. The 10-minute default is calibrated for the
    # opposite question — "is this backlog abandoned?" — and here it means a single drain (ours or
    # anyone's) locks the button out for ten minutes, which to the operator is indistinguishable
    # from the button being broken.
    seen = _queue.last_activity()
    if _queue.is_worker_active(within_minutes=1) and seen != _LAST_SELF_DRAIN:
        _log.info("[benchmark_fill] queue worker is live (last activity %s) — leaving it to them",
                  seen)
        return {"processed": 0, "ok": 0, "failed": 0, "unmapped": 0,
                "remaining": len(isins), "worker_live": True}
    done = _queue.process_slice(limit, isins=isins)
    _LAST_SELF_DRAIN = _queue.last_activity()
    # `remaining` from the queue is the WHOLE backlog; what this caller wants to know is how many
    # of ITS OWN are still pending, which is what the next press will work on.
    return {**done, "remaining": max(0, len(isins) - done["processed"]), "worker_live": False}


# How many instruments one press of Fill may re-price. Yahoo is one windowed call each, paced —
# so ACWI's 1,684 cannot be done in a request and the honest answer is to do a bounded slice, SAY
# how many are left, and converge over presses. The 06:00 asset-price tick finishes the rest
# unattended, because a series ending at the lookback is exactly what its staleness check looks for.
_REPRICE_PER_PRESS = 50
_REPRICE_SLEEP_S = 0.4


def _refill_prices(isins: list[str], grid: dict[str, dict], lookback: str,
                   start_anchor: str, limit: int = _REPRICE_PER_PRESS) -> dict:
    """Re-fetch the window's closes for constituents that have no mark in it.

    ⚠ BY SYMBOL, NEVER BY RE-RESOLVING. `extend_series(analysis_id, yahoo_symbol, since)` fetches
    a window for an instrument we have already identified. The resolve queue is the other thing
    Fill can do and it is NOT this: re-resolution asks Yahoo *which listing is this*, and Yahoo
    answers an overloaded caller with an empty search rather than a 429 — which is how Alphabet
    moved from GOOGL to a Vienna line 75,000x thinner. Nothing here re-opens that question.

    ⚠ IT ASKS `window_marks` WHO NEEDS IT, rather than assuming everyone does. That is the same
    selection the panel itself prices from, so "needs a price" means exactly "the panel cannot
    price it", and a press after a completed refill costs one query and no Yahoo calls at all.
    """
    from asset_pipeline import store  # noqa: PLC0415
    from routers._asset_benchmark import window_marks  # noqa: PLC0415

    by_aid: dict[int, str] = {}
    for isin in isins:
        g = grid.get(isin) or {}
        if g.get("analysis_id") and g.get("yahoo_symbol"):
            by_aid[g["analysis_id"]] = g["yahoo_symbol"]
    if not by_aid:
        return {"needed": 0, "repriced": 0, "failed": 0, "pending": 0}

    today = datetime.now(timezone.utc).date().isoformat()
    marks = window_marks(sorted(by_aid), lookback, start_anchor, today)
    need = [aid for aid in sorted(by_aid)
            if not (marks.get(aid) or {}).get("start") or not (marks.get(aid) or {}).get("end")]
    if not need:
        return {"needed": 0, "repriced": 0, "failed": 0, "pending": 0}

    todo, pending = need[:limit], max(0, len(need) - limit)
    repriced = failed = 0
    for aid in todo:
        try:
            # `since` is the lookback, not the last stored close: the deleted rows START there, and
            # `extend_series` fetches from a few days before `since` forward.
            store.extend_series(aid, by_aid[aid], lookback)
            repriced += 1
        except Exception as e:  # noqa: BLE001 — one dead symbol must not fail the button
            failed += 1
            _log.warning("[benchmark_fill] reprice %s failed: %s: %s",
                         by_aid[aid], type(e).__name__, e)
        time.sleep(_REPRICE_SLEEP_S)
    return {"needed": len(need), "repriced": repriced, "failed": failed, "pending": pending}


def fill_benchmark(label: str, *, do_caps: bool = True, do_prices: bool = True,
                   do_resolve: bool = True) -> dict:
    """Close the asset-world gap for one benchmark, and report exactly what remains.

    Returns the breakdown plus what it did: `queued` (handed to the ingest worker) and `capped`
    (market caps written now). Neither number is a promise that the panel will be non-zero on the
    next load — the queue is paced on purpose — so the caller shows the counts rather than a
    "done".
    """
    from asset_pipeline import queue as _queue  # noqa: PLC0415
    from routers._asset_benchmark import _universe_company_ids  # noqa: PLC0415

    ids = _universe_company_ids(label)
    built = False
    if not ids:
        # ⚠ THE MISSING UNIVERSE IS BUILT HERE, NOT REPORTED AS A CHORE. AEX has no `universe` row
        # at all — a different fault from "constituents unpriced" with the identical symptom (0
        # members) — and telling the operator to go and POST a second endpoint made the button a
        # riddle rather than a button. Where a template exists for the label, run it.
        #
        # ⚠ IT IS THE SAME REFRESH `/api/universe-templates/{key}/refresh` RUNS, not a second
        # reconstruction: `get_template` reads the one registry, and `refresh` is documented
        # idempotent, so pressing Fill twice converges rather than duplicating membership.
        built = _build_universe(label)
        ids = _universe_company_ids(label) if built else []
    if not ids:
        return {"label": label, "universe_members": 0, "queued": 0, "capped": 0,
                "usable": 0, "needs_resolve": 0, "needs_cap": 0, "no_isin": 0, "no_isin_names": [],
                "universe_built": built,
                "note": (f"Built the {label!r} universe but it has no members — the reconstruction "
                         f"returned nothing." if built else
                         f"No universe labelled {label!r} and no template registered to build one.")}

    companies: list[dict] = []
    for i in range(0, len(ids), IN_CHUNK_SIZE):
        companies += (supabase.table("company")
                      .select("company_id,company_name,isin")
                      .in_("company_id", ids[i:i + IN_CHUNK_SIZE])
                      .is_("delisted_at", "null").is_("out_of_scope_at", "null")
                      .execute().data or [])

    isins = sorted({(c.get("isin") or "").strip().upper() for c in companies if c.get("isin")})
    grid = _grid_for(isins)
    buckets = _classify(companies, grid)

    queued = _queue.enqueue(buckets[_NEEDS_RESOLVE]) if buckets[_NEEDS_RESOLVE] else {}
    # ⚠ ENQUEUE **THEN DRAIN**. The queue row is still written first — it is the durable record and
    # what a worker (or the next press) picks up — but the work no longer waits on a process that
    # may not be running. See `_drain_now`.
    drained = (_drain_now(buckets[_NEEDS_RESOLVE]) if (do_resolve and buckets[_NEEDS_RESOLVE])
               else {"processed": 0, "ok": 0, "failed": 0, "unmapped": 0, "remaining": 0,
                     "worker_live": False})
    if drained.get("ok"):
        # The rows just resolved are in the grid now, so re-read it — otherwise the cap and price
        # steps below run against the grid as it was BEFORE this press did its work, and report
        # every fresh row as still needing a resolve.
        grid = _grid_for(isins)
        buckets = _classify(companies, grid)
    caps = (_backfill_caps(buckets[_NEEDS_CAP], grid)
            if (do_caps and buckets[_NEEDS_CAP]) else {"quoted": 0, "capped": 0})
    # ⚠ THE THIRD JOB, AND IT RUNS OVER THE ROWS THAT ARE ALREADY "USABLE". A constituent resolved,
    # capped and holding a decade of history still prices nothing if its window is empty — which is
    # precisely the state Delete leaves behind, and the state the panel reports as 0 members.
    lookback, start_anchor = window_bounds()
    px = (_refill_prices(buckets[_USABLE] + buckets[_NEEDS_CAP], grid, lookback, start_anchor)
          if do_prices else {"needed": 0, "repriced": 0, "failed": 0, "pending": 0})

    return {
        "label": label,
        "universe_members": len(companies),
        "usable": len(buckets[_USABLE]),
        "needs_resolve": len(buckets[_NEEDS_RESOLVE]),
        "needs_cap": len(buckets[_NEEDS_CAP]),
        "no_isin": len(buckets[_NO_ISIN]),
        # Named, not just counted — these can never be fixed by this button and someone has to
        # know which they are.
        "no_isin_names": buckets[_NO_ISIN][:50],
        "queued": int(queued.get("queued") or 0),
        "skipped_existing": int(queued.get("skipped_existing") or 0),
        # What this press RESOLVED, as opposed to merely queued — see `_drain_now`.
        "resolved": int(drained.get("ok") or 0),
        # ⚠ NOT A FAILURE AND NOT A RETRY. OpenFIGI identified the security and Yahoo has no daily
        # series for it — a bond, a structured product, a listing on an exchange Yahoo does not
        # carry. The row is marked done for ever. Reported separately because a press that resolved
        # 25 such names shows `resolved: 0`, which reads exactly like a press that did nothing.
        "resolve_unmapped": int(drained.get("unmapped") or 0),
        "resolve_failed": int(drained.get("failed") or 0),
        "resolve_pending": int(drained.get("remaining") or 0),
        # True = something else is already consuming Yahoo, so the queue was left to it. The
        # distinction matters: it is the ONE case where "queued" really is the whole answer.
        "worker_live": bool(drained.get("worker_live")),
        "capped": caps["capped"],
        # Prices re-fetched this press, and how many still have no mark in the window. `pending`
        # is not a failure — one press is a bounded slice on purpose (see `_REPRICE_PER_PRESS`),
        # and the 06:00 tick clears the remainder unattended.
        "repriced": px["repriced"],
        "price_pending": px["pending"],
        "price_failed": px["failed"],
        "universe_built": built,
        "note": None,
    }
