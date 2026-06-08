"""Response-shaping helpers for the admin API.

Extracted from `routers.admin` (the HTTP layer). `_build_portfolio_payload`
turns a `current_picks_snapshot` row into the IBKR-ready order shape (used by
`GET /api/admin/schedules/{id}`); `_fetch_latest_snapshots_for` loads the most
recent snapshot per strategy. Both read the DB.
"""
from __future__ import annotations

from deps import fetch_in_chunks, supabase


def _build_portfolio_payload(snapshot_row: dict) -> dict:
    """Convert a `current_picks_snapshot` row into the IBKR-friendly
    response shape — every field a rebalancing script would need:

        ticker            symbol on the home exchange (GuruFocus form)
        exchange          GuruFocus exchange code (NYSE, NASDAQ, OHEL, …)
        country           listing country name (via the exchange)
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

    # Resolve company → exchange + country. The snapshot's holdings don't
    # carry exchange/country directly (only currency); we look them up via
    # the company table joined to gurufocus_exchange → country.
    cids = [int(h["company_id"]) for h in raw_holdings if h.get("company_id") is not None]
    exchange_by_cid: dict[int, str] = {}
    country_by_cid: dict[int, str | None] = {}
    for row in fetch_in_chunks(
        cids,
        lambda chunk: supabase.table("company")
        .select(
            "company_id, gurufocus_exchange:gurufocus_exchange("
            "exchange_code, country:country(country_name))"
        )
        .in_("company_id", chunk)
        .execute(),
    ):
        exch_info = row.get("gurufocus_exchange") or {}
        cid = int(row["company_id"])
        exchange_by_cid[cid] = exch_info.get("exchange_code") or ""
        country_by_cid[cid] = (exch_info.get("country") or {}).get("country_name")

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
            "country": country_by_cid.get(cid) if cid is not None else None,
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
