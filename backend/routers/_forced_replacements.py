"""Apply confirmed corporate actions to the current scheduled-strategy book."""
from __future__ import annotations

from datetime import date, datetime, timezone

from deps import supabase
from momentum.forced_replacement import next_same_sector_reserve


def apply_confirmed_actions(today: date | None = None, *,
                            strategy_ids: set[int] | None = None) -> int:
    """Create one replacement snapshot per affected live holding.

    The source rebalance is immutable.  A replacement is a new price-update
    snapshot so the audit trail says exactly when a corporate action changed
    the book. A legacy snapshot with no frozen reserves is left untouched for
    explicit reconstruction; a modern book with no usable same-sector reserve
    moves that sleeve to cash instead of crossing sectors.
    """
    day = (today or date.today()).isoformat()
    actions = (supabase.table("corporate_action").select("*")
               .lte("effective_date", day).execute().data or [])
    applied = 0
    for action in actions:
        removed = int(action["company_id"])
        snaps = (supabase.table("current_picks_snapshot")
                 .select("*").order("created_at", desc=True).execute().data or [])
        latest: dict[int, dict] = {}
        for snap in snaps:
            sid = snap.get("scheduled_strategy_id")
            if sid is not None and int(sid) not in latest:
                latest[int(sid)] = snap
        for strategy_id, snap in latest.items():
            if strategy_ids is not None and strategy_id not in strategy_ids:
                continue
            holdings = list(snap.get("holdings") or [])
            victim = next((h for h in holdings if int(h.get("company_id") or 0) == removed), None)
            if victim is None:
                continue
            prior = (supabase.table("scheduled_strategy_forced_replacement")
                     .select("forced_replacement_id").eq("scheduled_strategy_id", strategy_id)
                     .eq("corporate_action_id", action["corporate_action_id"])
                     .eq("removed_company_id", removed).limit(1).execute().data or [])
            if prior:
                continue
            reserves = snap.get("selection_reserves") or []
            # A pre-feature snapshot contains no evidence for what ranked
            # next. Do not guess or write a terminal `cash` audit row: the
            # guarded repair endpoint needs to be able to reconstruct it.
            if not reserves:
                continue
            held = {int(h.get("company_id") or 0) for h in holdings}
            unavailable_rows = (supabase.table("company").select("company_id")
                                .or_("delisted_at.not.is.null,out_of_scope_at.not.is.null").execute().data or [])
            reserve = next_same_sector_reserve(victim, reserves, held,
                                                {int(r["company_id"]) for r in unavailable_rows})
            status = "cash" if reserve is None else "applied"
            replacement_id = int(reserve["company_id"]) if reserve else None
            if reserve:
                replacement = {**victim, "company_id": replacement_id,
                               "ticker": reserve.get("ticker"), "company_name": reserve.get("company_name"),
                               "sector": reserve.get("sector"), "score": reserve.get("score"),
                               "entry_date": day, "entry_price_local": None, "entry_price_eur": None,
                               "exit_date": None, "exit_price_local": None, "exit_price_eur": None,
                               "forward_return_pct": None}
            else:
                replacement = {**victim, "company_id": 0, "ticker": "CASH",
                               "company_name": "Cash (no same-sector reserve)", "is_cash": True,
                               "entry_date": day, "entry_price_local": 1.0, "entry_price_eur": 1.0,
                               "exit_date": day, "exit_price_local": 1.0, "exit_price_eur": 1.0,
                               "forward_return_pct": 0.0}
            holdings = [replacement if h is victim else h for h in holdings]
            supabase.table("current_picks_snapshot").insert({
                "triggered_by": "auto", "as_of_date": snap["as_of_date"],
                "latest_price_date": snap.get("latest_price_date"), "config": snap.get("config"),
                "holdings": holdings, "selection_reserves": reserves,
                "daily_picks": [], "strategy_hash": snap.get("strategy_hash"), "name": snap.get("name"),
                "kind": "price_update", "scheduled_strategy_id": strategy_id,
            }).execute()
            supabase.table("scheduled_strategy_forced_replacement").insert({
                "scheduled_strategy_id": strategy_id, "rebalance_snapshot_id": snap["snapshot_id"],
                "corporate_action_id": action["corporate_action_id"], "removed_company_id": removed,
                "replacement_company_id": replacement_id, "effective_date": day,
                "target_weight": victim["weight"], "reserve_rank": reserve.get("reserve_rank") if reserve else None,
                "status": status, "applied_at": datetime.now(timezone.utc).isoformat(),
            }).execute()
            applied += 1
    return applied
