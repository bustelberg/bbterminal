"""Shared machinery behind the Benchmarks panel's **Refresh** and **Reset** buttons.

The run itself lives in `_benchmark_refresh.py` (constituents → market caps → the two prices).
What stays here is what BOTH buttons need and neither may answer twice: how a constituent is
classified, how a universe is rebuilt, which labels are rebuildable at all, where the YTD window
opens, how the ingest queue's slice is drained, and Reset's three deletions.

WHY A BENCHMARK READS 0 MEMBERS WITH A FULL UNIVERSE BEHIND IT
    `/api/benchmarks/index/{label}` prices from the asset world (yfinance), bridging
    `universe_membership -> company.isin -> asset_grid -> asset_price`. A constituent counts only
    when its ISIN is in the grid with `status='ok'`, an `analysis_id`, `bars > 0` AND a positive
    `market_cap_eur` — the cap is what weights it, so a resolved-but-uncapped name is invisible.
    Measured on the hosted project 2026-07-29: SP500 had 493 members, 493 ISINs, and **1** of them
    in the grid. The universe was never the problem. `_classify` is what tells those four states
    apart, and it is pure, which is what makes it testable without a database.

⚠ RESOLUTION GOES THROUGH THE QUEUE'S OWN SLICE, NEVER A SECOND RESOLVER. Yahoo answers an
    overloaded caller with an EMPTY result rather than a 429, and an empty candidate set is
    exactly how a resolution lands on a thin foreign listing (NVDA-on-Stuttgart,
    Alphabet-on-Vienna). The repo's answer is ONE Yahoo consumer: `asset_ingest_queue`, drained by
    a single paced worker. `_drain_now` runs that worker's own unit of work and stands down when
    something else is already draining.

⚠ A MEMBER WITH NO ISIN CANNOT BE REACHED FROM HERE AT ALL. 189 ACWI members have none (156
    Indian, 28 British) and GuruFocus cannot supply one either — it is blind to those markets.
    They are counted and named, never quietly dropped: a coverage figure that silently excludes
    India is worse than no figure.
"""
from __future__ import annotations

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


def _rebuild_sp500() -> bool:
    """Rebuild the SP500 universe from the Wikipedia reconstruction — the same code the /sp500
    page's import runs, scoped to what the benchmark actually stores.

    ⚠ SP500 IS NOT A `UniverseTemplate`, AND MUST NOT BECOME ONE. Registering it in `TEMPLATES`
    would stamp `template_key` on its universe row, and `/api/index-universe/indexes` — the list
    the /sp500 page itself renders — EXCLUDES any row with one. The index would vanish from its own
    page as a side effect of making a button work elsewhere. So the route back is wired here, where
    only Refresh sees it, instead of in the registry everything else reads.

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
    """Can Refresh put this label's universe back? The ONE answer both Reset and Refresh ask.

    A Reset offered for a label Refresh cannot rebuild is a one-way door behind a button whose whole
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
    rather than 500-ing a run whose other steps — capping and pricing — are unaffected.
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
    """Clear the market caps Refresh re-quotes (`_benchmark_refresh._caps`). Returns rows cleared.

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

    That is why this is safe to offer at all: **the damage is self-healing.** Refresh refills it
    explicitly (`_benchmark_refresh._prices`), the 06:00 asset-price tick refills it overnight, and the AIRS
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
    """Undo everything Refresh puts in place, so the whole button can be tested end to end.

    Three deletions, matching Refresh's three steps:

        membership   the `universe` row + its members    -> Refresh step 1 rebuilds it
        market caps  `asset_analysis.market_cap_*`       -> Refresh step 2 re-quotes them
        prices       `asset_price` from the lookback on  -> Refresh step 3 re-fetches them

    Deleting only the membership left two thirds of Refresh untested: a constituent that is already
    resolved and already capped goes straight into the `usable` bucket, so the cap backfill and the
    price fetch never run and the counts read like success without either having done anything.

    ⚠ IT DOES NOT TOUCH THE ASSET GRID, THE SYMBOL, OR THE PRE-WINDOW HISTORY. Those are what make
    the refill cheap AND safe: every instrument keeps `status='ok'`, its `analysis_id` and its
    Yahoo symbol, so Refresh re-fetches prices for a KNOWN symbol (`extend_series`) and nothing is
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

    ⚠⚠ AND — THE ONE THAT MATTERS — IT REFUSES A LABEL REFRESH CANNOT REBUILD. This exists so the
    operator can reset and watch Refresh rebuild, which is a promise about the label: `_build_universe`
    is the only route back. `rebuildable()` is that question, asked in ONE place by both this guard
    and the rebuild itself, so a label can never be deletable and unrebuildable at the same time.
    """
    if not rebuildable(label):
        from index_universe.templates import TEMPLATES  # noqa: PLC0415

        raise ValueError(
            f"Refresh has no way to rebuild {label!r}, so deleting it here would be one-way. "
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
                 "Refresh rebuilds all three.", label, n, caps, px, lookback)
    return {"label": label, "deleted": True, "members_deleted": n,
            "caps_cleared": caps, "price_rows_deleted": px, "prices_from": lookback,
            "had_template": bool(live[0].get("template_key")), "note": None}


# How many queued ISINs one press of Refresh resolves inline. `process_slice` runs them concurrently
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
    production 2026-07-30: the button on the AEX reported "25 queued for ingest (a paced worker drains
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
    # second one stand down for ten minutes and report `worker_live`, i.e. pressing it twice did
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
