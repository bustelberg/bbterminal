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
import json
import queue as _queue

import pandas as pd

from deps import supabase
from momentum.data import (
    convert_prices_to_eur,
    load_all_prices,
    load_all_volumes,
    self_heal_missing_data,
)

from .audit import AuditResult


def _emit(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"


def _keepalive() -> str:
    return ": keepalive\n\n"


def compute_gap_cids(
    req,
    audit: AuditResult,
    pass1_transient: set[int],
    company_ids: list[int],
) -> tuple[list[int], list[str]]:
    """Compute the union of gap company IDs to self-heal, plus an
    optional `info` event string for the force-recompute retry case
    (returned as a list so the caller can `yield from` it)."""
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

    def _on_heal_progress(cid, status, msg):
        heal_progress_q.put({"cid": cid, "status": status, "msg": msg})

    heal_task = asyncio.create_task(asyncio.to_thread(
        self_heal_missing_data,
        supabase, gap_cids, ticker_lookup, exchange_lookup,
        on_progress=_on_heal_progress,
    ))

    done_count = 0
    while not heal_task.done():
        drained = []
        while True:
            try:
                drained.append(heal_progress_q.get_nowait())
            except _queue.Empty:
                break
        if drained:
            done_count += len(drained)
            yield _emit({"type": "progress", "pct": 67, "message": f"  Self-heal: {done_count}/{len(gap_cids)} companies processed..."})
        await asyncio.sleep(0.2)

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
