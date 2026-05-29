"""Refetch missing prices + volumes for a small subset of companies.

Use this on the universe IDs that came back empty from a bulk DB load —
calling it on every company would be wasteful (hundreds of redundant
HEAD calls). The downstream `ensure_*` helpers in `ingest.prices`
already short-circuit if the DB is fresh, so even a misuse just costs
extra DB round-trips, not API calls."""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from supabase import Client


def self_heal_missing_data(
    supabase: Client,
    company_ids: list[int],
    ticker_lookup: dict[int, str],
    exchange_lookup: dict[int, str],
    *,
    on_progress=None,
    cancel_event: "threading.Event | None" = None,
) -> dict:
    """For each company in `company_ids`, ensure both close_price and volume
    are present in `metric_data` by re-running the ingest pipeline (Storage
    cache check → GF API fetch → cache + DB load).

    A 403/"unsubscribed region" response on any company causes the helper
    to mark its exchange as forbidden and skip every subsequent company on
    the same exchange (ingest already does the same thing in its own
    pipeline). A 403 for a single bad ticker (delisted, wrong symbol) does
    NOT taint the whole exchange.

    `on_progress(cid, status, message)` is called from worker threads —
    callbacks must be thread-safe.

    Returns:
        {"healed_company_ids": [...], "stats": {...}}
        where "stats" includes processed/prices_fetched/volumes_fetched/
        forbidden_exchanges/errors counts.
    """
    # Imported lazily to avoid making this module always pay the ingest
    # module's transitive imports (urllib, supabase storage helpers, etc.).
    from ingest.prices import (  # noqa: PLC0415
        ensure_prices_for_company, ensure_volume_for_company,
    )

    if not company_ids:
        return {
            "healed_company_ids": [],
            "stats": {
                "processed": 0, "prices_fetched": 0, "volumes_fetched": 0,
                "forbidden_exchanges": [], "errors": 0,
            },
        }

    forbidden_exchanges: set[str] = set()
    healed: list[int] = []
    stats = {
        "processed": 0,
        "prices_fetched": 0,
        "volumes_fetched": 0,
        "errors": 0,
    }
    lock = threading.Lock()

    def _heal_one(cid: int) -> None:
        # Honor client-disconnect cancellation. Checked at the start of
        # every per-company call so already-queued workers exit promptly
        # — Python threads can't be interrupted mid-API-call, but the
        # 4 in-flight workers finish in seconds while the long tail
        # (hundreds of queued companies) gets skipped immediately.
        if cancel_event is not None and cancel_event.is_set():
            if on_progress:
                on_progress(cid, "skipped", "cancelled")
            return
        ticker = ticker_lookup.get(cid)
        exch = exchange_lookup.get(cid)
        if not ticker or not exch:
            with lock:
                stats["errors"] += 1
            if on_progress:
                on_progress(cid, "skipped", "missing ticker/exchange")
            return
        with lock:
            if exch in forbidden_exchanges:
                if on_progress:
                    on_progress(cid, "skipped", f"exchange {exch} known forbidden")
                return
        try:
            r_p = ensure_prices_for_company(supabase, cid, ticker, exch)
            if r_p.is_forbidden:
                with lock:
                    forbidden_exchanges.add(exch)
                if on_progress:
                    on_progress(cid, "forbidden", f"{exch}: unsubscribed")
                return
            r_v = ensure_volume_for_company(supabase, cid, ticker, exch)
        except Exception as e:  # noqa: BLE001
            with lock:
                stats["errors"] += 1
            if on_progress:
                on_progress(cid, "error", str(e))
            return
        any_loaded = r_p.rows_loaded > 0 or r_v.rows_loaded > 0
        with lock:
            stats["processed"] += 1
            if r_p.rows_loaded > 0:
                stats["prices_fetched"] += 1
            if r_v.rows_loaded > 0:
                stats["volumes_fetched"] += 1
            if any_loaded:
                healed.append(cid)
        if on_progress:
            on_progress(
                cid,
                "ok" if any_loaded else "noop",
                f"prices={r_p.source}({r_p.rows_loaded}) volumes={r_v.source}({r_v.rows_loaded})",
                prices_loaded=r_p.rows_loaded,
                volumes_loaded=r_v.rows_loaded,
            )

    # Use fewer workers than the bulk load: each call hits the GF API,
    # which is rate-limit-sensitive — overdoing parallelism risks 429s.
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(_heal_one, company_ids))

    return {
        "healed_company_ids": sorted(healed),
        "stats": {**stats, "forbidden_exchanges": sorted(forbidden_exchanges)},
    }
