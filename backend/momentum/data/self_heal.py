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
    from asset_pipeline.store import extend_series, store_series  # noqa: PLC0415

    if not company_ids:
        return {
            "healed_company_ids": [],
            "stats": {
                "processed": 0, "prices_fetched": 0, "volumes_fetched": 0,
                "forbidden_exchanges": [], "errors": 0,
            },
        }

    # ``company_id`` is the momentum universe's key, whereas Yahoo's stored
    # close/volume bars are keyed by ``analysis_id``. Resolve that bridge once
    # before the workers start; a company with no reviewed Yahoo instrument is
    # reported as such rather than being silently repaired from GuruFocus.
    def _mapped() -> dict[int, tuple[int, str, str | None]]:
        out: dict[int, tuple[int, str, str | None]] = {}
        for start in range(0, len(company_ids), 200):
            rows = (supabase.table("asset_grid")
                    .select("company_id,analysis_id,yahoo_symbol,price_to,status")
                    .in_("company_id", company_ids[start:start + 200])
                    .eq("status", "ok").execute().data or [])
            for row in rows:
                cid, aid, symbol = row.get("company_id"), row.get("analysis_id"), row.get("yahoo_symbol")
                if cid is not None and aid is not None and symbol:
                    out.setdefault(int(cid), (int(aid), str(symbol), row.get("price_to")))
        return out

    mapped = _mapped()
    missing_mapping = [cid for cid in company_ids if cid not in mapped]
    if missing_mapping:
        # A backtest requested these companies, so resolve its bounded repair
        # slice now. Previously this only re-fetched legacy GuruFocus rows,
        # leaving the new Yahoo loader with exactly the same empty panel.
        from asset_pipeline import queue as asset_queue  # noqa: PLC0415

        isin_rows = []
        for start in range(0, len(missing_mapping), 200):
            isin_rows += (supabase.table("company").select("company_id,isin")
                          .in_("company_id", missing_mapping[start:start + 200])
                          .not_.is_("isin", "null").execute().data or [])
        isins = [str(row["isin"]).strip().upper() for row in isin_rows if row.get("isin")]
        if isins:
            asset_queue.enqueue(isins)
            if asset_queue.is_worker_active():
                if on_progress:
                    on_progress(missing_mapping[0], "skipped", "Yahoo resolver is already working")
            else:
                if on_progress:
                    on_progress(missing_mapping[0], "ok", f"resolving {len(isins)} Yahoo instrument(s)")
                asset_queue.process_slice(
                    limit=len(isins), priority_isins=isins,
                    on_each=lambda isin, outcome: on_progress(
                        next((int(row["company_id"]) for row in isin_rows
                              if str(row.get("isin") or "").upper() == isin), missing_mapping[0]),
                        "ok", f"Yahoo {isin}: {outcome}",
                    ) if on_progress else None,
                )
                mapped = _mapped()

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
        asset = mapped.get(cid)
        if asset is None:
            with lock:
                stats["errors"] += 1
            if on_progress:
                on_progress(cid, "skipped", "no resolved Yahoo instrument")
            return
        try:
            analysis_id, symbol, last_close = asset
            loaded = extend_series(analysis_id, symbol, str(last_close)) if last_close else None
            if loaded is None:
                loaded = store_series(analysis_id, symbol, None)
        except Exception as e:  # noqa: BLE001
            with lock:
                stats["errors"] += 1
            if on_progress:
                on_progress(cid, "error", str(e))
            return
        any_loaded = loaded > 0
        with lock:
            stats["processed"] += 1
            if loaded > 0:
                stats["prices_fetched"] += 1
                stats["volumes_fetched"] += 1
            if any_loaded:
                healed.append(cid)
        if on_progress:
            on_progress(
                cid,
                "ok" if any_loaded else "noop",
                f"Yahoo {symbol}: {loaded} close/volume bar(s)",
                prices_loaded=loaded,
                volumes_loaded=loaded,
            )

    # Use fewer workers than the bulk load: each call hits the GF API,
    # which is rate-limit-sensitive — overdoing parallelism risks 429s.
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(_heal_one, company_ids))

    return {
        "healed_company_ids": sorted(healed),
        "stats": {**stats, "forbidden_exchanges": sorted(forbidden_exchanges)},
    }
