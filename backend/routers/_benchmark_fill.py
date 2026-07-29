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


def _build_universe(label: str) -> bool:
    """Run the label's universe template, if one is registered. True when it ran.

    ⚠ BEST-EFFORT, AND A FAILURE IS NOT AN EXCEPTION HERE. The reconstruction scrapes third parties
    (iShares, Wikipedia, MSCI); a bad day there must leave the caller reporting "could not build"
    rather than 500-ing a button whose other work — enqueuing and capping — is unaffected.
    """
    from index_universe.templates import TEMPLATES, get_template  # noqa: PLC0415

    if label not in TEMPLATES:
        return False
    try:
        get_template(label).refresh(supabase)
        return True
    except Exception as e:  # noqa: BLE001
        _log.warning("[benchmark_fill] building the %s universe failed: %s: %s",
                     label, type(e).__name__, e)
        return False


def fill_benchmark(label: str, *, do_caps: bool = True) -> dict:
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
    caps = (_backfill_caps(buckets[_NEEDS_CAP], grid)
            if (do_caps and buckets[_NEEDS_CAP]) else {"quoted": 0, "capped": 0})

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
        "capped": caps["capped"],
        "universe_built": built,
        "note": None,
    }
