"""Self-heal block: for any subscribed-exchange company missing prices
or volumes, re-run the ingest pipeline (cache check → API fetch → DB
load) and merge the recovered rows back into the in-memory frames.

This is a no-op in steady state — the gap sets are empty when
everything is already loaded — and only fires for genuinely missing
data (empty Storage JSONs, failed prior loads, new tickers, etc.).

When the user explicitly asks for fresh data on a current_portfolio
run (force_recompute=True), also retry every company whose pass-1
ensure call hit a transient API failure (source == "stale_cache").
This is the per-company pass-1 outcome, not a re-derivation of "stale"
from the bulk frame — pass-1 already runs the same `is_daily_data_fresh`
predicate, so a successful "api" outcome cannot produce a stale row
in the audit. Only transient API failures need a second attempt."""
from __future__ import annotations

import asyncio
from routers._sse import sse_event as _emit, sse_keepalive as _keepalive
import queue as _queue
import threading

import pandas as pd

from deps import supabase
from momentum.data import (
    convert_prices_to_eur,
    load_all_prices,
    load_all_volumes,
    self_heal_missing_data,
)

from .audit import AuditResult

# Hard cap on how many missing-data companies the backtest will refetch from
# GuruFocus INLINE. Self-heal is meant for a handful of stragglers (empty
# Storage JSONs, a new ticker, a failed prior load). When the gap is in the
# hundreds it means the universe simply hasn't been loaded by the data pipeline
# yet — refetching all of it inline (e.g. ~1645 LEONTEQ names) would pull the
# whole universe's price history in one request and OOM-kill the backend. We
# heal up to this many and tell the user to run the pipeline for the rest.
_MAX_INLINE_HEAL = 200


def compute_gap_cids(
    req,
    audit: AuditResult,
    pass1_transient: set[int],
    company_ids: list[int],
) -> tuple[list[int], list[str]]:
    """Compute the (capped) union of gap company IDs to self-heal, plus
    optional `info`/`warning` event strings (returned as a list so the caller
    can `yield from` them)."""
    info_events: list[str] = []
    _stale_cids: set[int] = set()
    if req.mode == "current_portfolio" and req.force_recompute:
        company_id_set = {int(c) for c in company_ids}
        _stale_cids = {
            cid for cid in pass1_transient
            if cid in company_id_set
            and audit.exchange_for_cid.get(cid, "UNKNOWN") not in audit.unsubscribed_exchanges
        }
        if _stale_cids:
            info_events.append(_emit({
                "type": "info",
                "scope": "self-heal",
                "message": f"Force-recompute: {len(_stale_cids)} companies hit transient API errors during pass 1 — retrying.",
            }))

    gap_cids = sorted(set(audit.no_price_gap_cids) | set(audit.no_vol_gap_cids) | _stale_cids)

    # Cap the inline heal. A gap in the hundreds means the universe hasn't been
    # loaded by the pipeline (e.g. a template universe with no scheduled
    # strategy keeping its prices fresh) — refetching all of it from GuruFocus
    # inline would OOM-kill the backend. Heal up to the cap; the pipeline loads
    # the rest. Steady state has 0 gaps, so this never fires on a loaded
    # universe.
    if len(gap_cids) > _MAX_INLINE_HEAL:
        total = len(gap_cids)
        gap_cids = gap_cids[:_MAX_INLINE_HEAL]
        info_events.append(_emit({
            "type": "warning",
            "scope": "self-heal",
            "message": (
                f"{total} companies have no price data yet — this universe "
                f"hasn't been loaded by the data pipeline. Refetching only "
                f"{_MAX_INLINE_HEAL} inline to protect the backend; run the "
                f"pipeline (/schedule → Run now) to load the rest, then re-run "
                f"the backtest."
            ),
        }))

    return gap_cids, info_events


async def run_self_heal(
    gap_cids: list[int],
    universe_df: pd.DataFrame,
    audit: AuditResult,
    prices_df: pd.DataFrame,
    prices_local_df: pd.DataFrame,
    volumes_df: pd.DataFrame,
    company_currency: dict[int, str | None],
    fx_rates: dict[str, pd.Series],
    price_start,
    price_end,
    *,
    cancel_event: threading.Event | None = None,
):
    """Yield SSE progress events while running the self-heal task and
    merging the recovered frames. Finally yields
    `("__result__", (prices_eur_df, prices_local_df, volumes_df))` so the
    orchestrator can swap in the merged frames."""
    yield _emit({"type": "progress", "pct": 67, "message": f"Self-heal: refetching missing data for {len(gap_cids)} companies on subscribed exchanges..."})
    yield _keepalive()

    ticker_lookup = {
        int(r["company_id"]): str(r["gurufocus_ticker"])
        for _, r in universe_df.iterrows()
    }
    exchange_lookup = {
        int(r["company_id"]): str(r.get("gurufocus_exchange") or "")
        for _, r in universe_df.iterrows()
    }

    heal_progress_q: _queue.Queue = _queue.Queue()

    def _on_heal_progress(cid, status, msg, *, prices_loaded=0, volumes_loaded=0):
        heal_progress_q.put({
            "cid": cid,
            "status": status,
            "msg": msg,
            "prices_loaded": prices_loaded,
            "volumes_loaded": volumes_loaded,
        })

    heal_task = asyncio.create_task(asyncio.to_thread(
        self_heal_missing_data,
        supabase, gap_cids, ticker_lookup, exchange_lookup,
        on_progress=_on_heal_progress,
        cancel_event=cancel_event,
    ))

    # Track which originally-flagged cids have actually been recovered.
    # The audit's gap sets are the ground-truth denominator for the live
    # warning-update counters — we don't trust the per-cid stream alone
    # because forbidden/skipped cids never produce prices_loaded > 0 but
    # also shouldn't be counted as "still missing in self-heal".
    price_gap_set = {int(c) for c in (audit.no_price_gap_cids or [])}
    vol_gap_set = {int(c) for c in (audit.no_vol_gap_cids or [])}
    recovered_price: set[int] = set()
    recovered_vol: set[int] = set()
    processed_cids: set[int] = set()
    status_icons = {"ok": "✓", "noop": "—", "skipped": "—", "forbidden": "✗", "error": "✗"}

    def _live_warning(scope_label: str, gap_set: set[int], recovered_set: set[int]) -> str:
        still_missing = len(gap_set) - len(recovered_set)
        if still_missing == 0:
            # All recovered — dismiss the standing warning entirely.
            return _emit({
                "type": "warning",
                "id": f"{scope_label}-gap",
                "scope": scope_label,
                "dismiss": True,
                "message": "",
            })
        return _emit({
            "type": "warning",
            "id": f"{scope_label}-gap",
            "scope": scope_label,
            "message": (
                f"{still_missing} of {len(gap_set)} companies still missing "
                f"{scope_label} ({len(recovered_set)} recovered so far, self-heal in progress…)"
            ),
        })

    done_count = 0
    while not heal_task.done():
        if cancel_event is not None and cancel_event.is_set():
            # Stop yielding new progress events — the worker's seeing the
            # same flag and is bailing out fast. Returning here lets the
            # async generator unwind so the client-disconnect cancellation
            # actually halts the stream instead of waiting for the worker
            # to drain its full queue.
            yield _emit({
                "type": "info",
                "scope": "self-heal",
                "message": f"Self-heal cancelled at {done_count}/{len(gap_cids)}.",
            })
            return
        drained = []
        while True:
            try:
                drained.append(heal_progress_q.get_nowait())
            except _queue.Empty:
                break
        if drained:
            done_count += len(drained)
            # Update recovery tallies from every drained item.
            for item in drained:
                cid = int(item["cid"])
                processed_cids.add(cid)
                if item.get("prices_loaded", 0) > 0 and cid in price_gap_set:
                    recovered_price.add(cid)
                if item.get("volumes_loaded", 0) > 0 and cid in vol_gap_set:
                    recovered_vol.add(cid)
            # Per-iteration progress with the latest cid's outcome.
            latest = drained[-1]
            latest_cid = int(latest["cid"])
            icon = status_icons.get(latest["status"], "·")
            label = audit.label_for_cid.get(latest_cid, str(latest_cid))
            yield _emit({
                "type": "progress",
                "pct": 67,
                "message": (
                    f"  Self-heal: {done_count}/{len(gap_cids)} processed — "
                    f"last: {icon} {label} ({latest['msg']})"
                ),
            })
            # Live warning updates (deduped on the frontend by `id`).
            if price_gap_set:
                yield _live_warning("prices", price_gap_set, recovered_price)
            if vol_gap_set:
                yield _live_warning("volumes", vol_gap_set, recovered_vol)
        try:
            await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            # Cancellation arrived mid-loop. Set the worker-visible flag
            # so the next per-company call short-circuits, then re-raise
            # so the async generator unwinds cleanly. Without setting
            # the flag here, workers wouldn't notice until the outer
            # stream's except handler set it — a few-hundred-ms gap that
            # would otherwise start one more GF fetch per worker thread.
            if cancel_event is not None:
                cancel_event.set()
            raise

    heal_result = await heal_task
    healed_cids = heal_result["healed_company_ids"]
    heal_stats = heal_result["stats"]

    heal_msg_parts = [
        f"{heal_stats['prices_fetched']} price fetches",
        f"{heal_stats['volumes_fetched']} volume fetches",
    ]
    if heal_stats["forbidden_exchanges"]:
        heal_msg_parts.append(f"forbidden exchanges (skipped): {', '.join(heal_stats['forbidden_exchanges'])}")
    if heal_stats["errors"]:
        heal_msg_parts.append(f"{heal_stats['errors']} errors")
    yield _emit({
        "type": "info",
        "scope": "self-heal",
        "message": f"Self-heal complete: {' · '.join(heal_msg_parts)}.",
    })

    if healed_cids:
        yield _emit({"type": "progress", "pct": 67, "message": f"Re-loading {len(healed_cids)} healed companies into memory..."})
        yield _keepalive()
        new_local = await asyncio.to_thread(
            load_all_prices, supabase, healed_cids, price_start, price_end,
        )
        if not new_local.empty:
            new_eur, _new_fx = await asyncio.to_thread(
                convert_prices_to_eur, new_local, company_currency, fx_rates,
            )
            prices_local_df = pd.concat(
                [prices_local_df, new_local], ignore_index=True
            ).sort_values(["company_id", "target_date"]).reset_index(drop=True)
            if not new_eur.empty:
                prices_df = pd.concat(
                    [prices_df, new_eur], ignore_index=True
                ).sort_values(["company_id", "target_date"]).reset_index(drop=True)
        new_volumes = await asyncio.to_thread(
            load_all_volumes, supabase, healed_cids, price_start, price_end,
        )
        if not new_volumes.empty:
            volumes_df = pd.concat(
                [volumes_df, new_volumes], ignore_index=True
            ).sort_values(["company_id", "target_date"]).reset_index(drop=True)

    yield ("__result__", (prices_df, prices_local_df, volumes_df))
