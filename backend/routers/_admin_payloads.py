"""Response-shaping helpers for the admin API.

Extracted from `routers.admin` (the HTTP layer). These build the IBKR-ready
portfolio payload + the compact schedule/run summaries the external monitoring
script consumes. `_build_portfolio_payload` and `_fetch_latest_snapshots_for`
read the DB; `_summarize_schedule` / `_summarize_run` are pure transforms.
"""
from __future__ import annotations

from deps import fetch_in_chunks, supabase


def _build_portfolio_payload(snapshot_row: dict) -> dict:
    """Convert a `current_picks_snapshot` row into the IBKR-friendly
    response shape — every field a rebalancing script would need:

        ticker            symbol on the home exchange (GuruFocus form)
        exchange          GuruFocus exchange code (NYSE, NASDAQ, OHEL, …)
        currency          ISO 4217 currency code
        side              "long" or "short"
        target_weight     fractional weight in the portfolio (sum ≈ 1.0)
        company_id        DB id, useful for cross-referencing
        company_name      display name
        sector            GICS sector (for verification)
        entry_price_local most recent close in the listing currency
        entry_price_eur   …same converted to EUR
        score             the momentum score at selection time

    The IBKR symbol/exchange mapping isn't done here — callers know
    their own broker conventions and we don't want to lock in any
    particular translation. We just hand back the canonical GuruFocus
    fields and let the script adapt.
    """
    raw_holdings = snapshot_row.get("holdings") or []
    cfg = snapshot_row.get("config") or {}

    # Resolve company → exchange. The snapshot's holdings don't carry
    # exchange directly (only currency); we look it up via the company
    # table joined to gurufocus_exchange.
    cids = [int(h["company_id"]) for h in raw_holdings if h.get("company_id") is not None]
    exchange_by_cid: dict[int, str] = {}
    for row in fetch_in_chunks(
        cids,
        lambda chunk: supabase.table("company")
        .select("company_id, gurufocus_exchange:gurufocus_exchange(exchange_code)")
        .in_("company_id", chunk)
        .execute(),
    ):
        exch = (row.get("gurufocus_exchange") or {}).get("exchange_code") or ""
        exchange_by_cid[int(row["company_id"])] = exch

    total_weight = 0.0
    holdings_out: list[dict] = []
    for h in raw_holdings:
        cid = int(h.get("company_id")) if h.get("company_id") is not None else None
        weight = float(h.get("weight") or 0.0)
        total_weight += weight
        holdings_out.append({
            "company_id": cid,
            "ticker": h.get("ticker"),
            "exchange": exchange_by_cid.get(cid, "") if cid is not None else "",
            "currency": h.get("currency"),
            "side": h.get("side") or "long",
            "target_weight": round(weight, 6),
            "company_name": h.get("company_name"),
            "sector": h.get("sector"),
            "entry_price_local": h.get("entry_price_local"),
            "entry_price_eur": h.get("entry_price_eur"),
            "entry_date": h.get("entry_date"),
            "score": h.get("score"),
        })

    return {
        "snapshot_id": snapshot_row.get("snapshot_id"),
        "as_of_date": snapshot_row.get("as_of_date"),
        "latest_price_date": snapshot_row.get("latest_price_date"),
        "triggered_by": snapshot_row.get("triggered_by"),
        "created_at": snapshot_row.get("created_at"),
        "strategy": {
            "name": snapshot_row.get("name"),
            "selection_mode": cfg.get("selection_mode"),
            "strategy_type": cfg.get("strategy_type", "long_only"),
            "index_universe": cfg.get("index_universe"),
            "top_n_sectors": cfg.get("top_n_sectors"),
            "top_n_per_sector": cfg.get("top_n_per_sector"),
            "rebalance_frequency": cfg.get("rebalance_frequency"),
        },
        "holdings": holdings_out,
        "holdings_count": len(holdings_out),
        "total_weight": round(total_weight, 6),
    }


def _summarize_schedule(strat_row: dict, latest_snapshot_row: dict | None) -> dict:
    """One row per scheduled strategy with everything an external caller
    needs to act on it: identity, cadence, next/last run timestamps, and
    the full latest holdings in the same IBKR-ready shape as
    /api/admin/portfolio/{id}. `latest_snapshot_row` is the full snapshot
    row from `current_picks_snapshot` (or None if the strategy has never
    produced one yet)."""
    portfolio = _build_portfolio_payload(latest_snapshot_row) if latest_snapshot_row else None
    return {
        "id": strat_row["id"],
        "name": strat_row.get("name") or f"Strategy #{strat_row['id']}",
        "frequency": strat_row.get("frequency"),
        "enabled": strat_row.get("enabled", True),
        "last_run_at": strat_row.get("last_run_at"),
        "next_due_at": strat_row.get("next_due_at"),
        "config": strat_row.get("config") or {},
        "latest_portfolio": portfolio,
    }


def _fetch_latest_snapshots_for(strategy_ids: list[int]) -> dict[int, dict]:
    """For each strategy id, return its most-recent snapshot row (or omit
    when none exists). Batches in IN_CHUNK_SIZE chunks to dodge
    Cloudflare 502s on Supabase, same convention as elsewhere."""
    latest: dict[int, dict] = {}
    for row in fetch_in_chunks(
        strategy_ids,
        lambda chunk: supabase.table("current_picks_snapshot")
        .select("*")
        .in_("scheduled_strategy_id", chunk)
        .order("created_at", desc=True)
        .execute(),
    ):
        sid = row.get("scheduled_strategy_id")
        if sid is None or sid in latest:
            continue
        latest[int(sid)] = row
    return latest


def _summarize_run(row: dict) -> dict:
    """Compact summary of an ingest_run row, dropping the verbose
    additions/removals lists from templates_summary (counts only).
    Useful for monitoring endpoints where the caller wants a quick
    health snapshot, not the full diff."""
    templates_raw = row.get("templates_summary") or []
    if not isinstance(templates_raw, list):
        templates_raw = []
    mom_raw = row.get("momentum_summary")
    if isinstance(mom_raw, list):
        mom_list = mom_raw
    elif isinstance(mom_raw, dict):
        mom_list = [mom_raw]
    else:
        mom_list = []
    return {
        "run_id": row.get("run_id"),
        "job_name": row.get("job_name"),
        "triggered_by": row.get("triggered_by"),
        "started_at": row.get("started_at"),
        "finished_at": row.get("finished_at"),
        "status": row.get("status"),
        "current_phase": row.get("current_phase"),
        "templates": [
            {
                "template_key": t.get("template_key"),
                "universe_id": t.get("universe_id"),
                "target_month": t.get("this_month"),
                "additions": t.get("additions_count"),
                "removals": t.get("removals_count"),
                "renames": t.get("renames_count"),
                "error": t.get("error"),
            }
            for t in templates_raw
        ],
        "prices": {
            "companies_processed": row.get("companies_processed") or 0,
            "prices_refreshed": row.get("prices_refreshed") or 0,
            "volumes_refreshed": row.get("volumes_refreshed") or 0,
            "forbidden": row.get("forbidden_count") or 0,
            "delisted": row.get("delisted_count") or 0,
            "errors": row.get("error_count") or 0,
        },
        "momentum": [
            {
                "strategy_id": m.get("strategy_id"),
                "strategy_name": m.get("strategy_name"),
                "snapshot_id": m.get("snapshot_id"),
                "holdings_count": m.get("holdings_count"),
                "latest_price_date": m.get("latest_price_date"),
                "status": m.get("status"),
                "error_message": m.get("error_message"),
            }
            for m in mom_list
        ],
        "error_summary": row.get("error_summary"),
    }
