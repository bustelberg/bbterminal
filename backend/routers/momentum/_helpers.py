"""Shared momentum-domain helpers — pulled out of `main.py` so the four
submodules under `routers/momentum/` can import without circular deps.

Nothing in here is a FastAPI endpoint. The startup hook
(`_verify_acwi_exchange_codes`) is registered against the global app at
import time because it has no per-request state and must run once per
process.

Key concepts:
- `_strategy_hash` — sliding "this month" identity used by current-picks
  caching. Does NOT include date range so the cache survives day-to-day.
- `_backtest_strategy_hash` — full-config identity used by /backtest
  caching. Includes start/end dates + random/n_trials so different runs
  don't collide.
- `_save_current_picks_snapshot` — auto-fills `name` only when the
  20260507 migration is applied; degrades gracefully otherwise.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import date, datetime
from typing import TYPE_CHECKING

from deps import supabase

if TYPE_CHECKING:
    # Avoid the import cycle: BacktestRequest lives in backtest_stream.py
    # which imports this module. The TYPE_CHECKING guard means the import
    # is only resolved by type-checkers, not at runtime.
    from .backtest_stream import BacktestRequest


def register_startup_hooks(app) -> None:
    """Attach FastAPI startup hooks owned by this domain to `app`. Called
    once from main.py after the FastAPI() instance is created."""

    @app.on_event("startup")
    def _verify_acwi_exchange_codes() -> None:
        """Warn loudly if any exchange_code acwi.py can emit is missing
        from `gurufocus_exchange`. Holdings on missing codes are silently
        dropped during ACWI sync — that bug ate MSFT once already, this
        check is so it doesn't happen again."""
        try:
            from index_universe.acwi import expected_db_exchange_codes

            expected = expected_db_exchange_codes()
            resp = supabase.table("gurufocus_exchange").select("exchange_code").execute()
            present = {r["exchange_code"] for r in (resp.data or [])}
            missing = sorted(expected - present)
            if missing:
                logging.getLogger(__name__).warning(
                    "[acwi] exchange codes missing from gurufocus_exchange: %s. "
                    "Holdings on these exchanges will be silently skipped during "
                    "ACWI sync. Add a migration that seeds them.",
                    ", ".join(missing),
                )
        except Exception as e:
            logging.getLogger(__name__).warning(
                "[acwi] exchange code sanity check failed: %s", e
            )


def latest_db_price_date() -> date | None:
    """Latest target_date for `close_price` across the whole metric_data
    table. Used as a fast pre-flight gate so we don't run a heavy compute
    against stale DB data. Returns None if the table is empty."""
    resp = (
        supabase.table("metric_data")
        .select("target_date")
        .eq("metric_code", "close_price")
        .order("target_date", desc=True)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        return None
    raw = rows[0].get("target_date")
    if not raw:
        return None
    return date.fromisoformat(str(raw)[:10])


def strategy_hash(req: "BacktestRequest") -> str:
    """Sliding-view identity for current-picks. Same params → same hash;
    date range intentionally excluded so the cache survives across days
    when only the requested "current month" moves."""
    payload = {
        "signal_weights": req.signal_weights or {},
        "category_weights": req.category_weights or {},
        "top_n_sectors": req.top_n_sectors,
        "top_n_per_sector": req.top_n_per_sector,
        "max_companies": req.max_companies,
        "min_price_score": req.min_price_score,
        "universe_label": req.universe_label,
        "index_universe": req.index_universe,
        "selection_mode": req.selection_mode,
        "rebalance_frequency": req.rebalance_frequency,
        "strategy_type": req.strategy_type,
    }
    s = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode()).hexdigest()[:16]


def backtest_strategy_hash(req: "BacktestRequest") -> str:
    """Full-config identity used by /backtest caching. Unlike
    `strategy_hash`, this includes start/end dates and random/n_trials —
    two runs cache to the same row only when their FULL config matches."""
    payload = {
        "start_date": req.start_date,
        "end_date": req.end_date,
        "signal_weights": req.signal_weights or {},
        "category_weights": req.category_weights or {},
        "top_n_sectors": req.top_n_sectors,
        "top_n_per_sector": req.top_n_per_sector,
        "max_companies": req.max_companies,
        "min_price_score": req.min_price_score,
        "universe_label": req.universe_label,
        "index_universe": req.index_universe,
        "selection_mode": req.selection_mode,
        "random_seed": req.random_seed,
        "n_trials": req.n_trials,
        "rebalance_frequency": req.rebalance_frequency,
        "strategy_type": req.strategy_type,
    }
    s = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode()).hexdigest()[:16]


def find_cached_backtest(hash_: str) -> dict | None:
    """Today's cached backtest for this strategy, or None.
    Cache validity is scoped to the current UTC day — once `data_date`
    rolls over (after the next daily price refresh) the next replay misses.

    Also enforces a payload-shape contract: rows produced before fields
    that are now required (e.g. `universe_daily_records`) are treated as
    misses so the next compute writes the new shape. Once everyone has
    re-run after a schema bump, the check is a no-op. Cheaper and less
    invasive than a manual cache wipe."""
    today_iso = date.today().isoformat()
    resp = (
        supabase.table("backtest_cache")
        .select("*")
        .eq("strategy_hash", hash_)
        .eq("data_date", today_iso)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        return None
    row = rows[0]
    result = (row.get("payload") or {}).get("result") or {}
    # Stale-shape gate: every shipped payload from this point forward
    # carries `universe_daily_records` (added when the chart started
    # rendering a daily-granularity universe baseline). A row that lacks
    # the key is a pre-feature cache entry; bypass it so the next compute
    # writes the new shape. Bump the required-field list when adding
    # more fields downstream.
    if "universe_daily_records" not in result:
        return None
    return row


def save_backtest_cache(hash_: str, config: dict, payload: dict) -> None:
    """Replace any prior cache row for this strategy with today's payload.
    Synchronous; call via asyncio.to_thread."""
    today_iso = date.today().isoformat()
    # Drop stale-day rows first so each strategy has at most one row in
    # the cache at any time.
    supabase.table("backtest_cache").delete().eq("strategy_hash", hash_).execute()
    supabase.table("backtest_cache").insert({
        "strategy_hash": hash_,
        "data_date": today_iso,
        "config": config,
        "payload": payload,
    }).execute()


def persist_daily_picks(hash_: str, config: dict, daily_picks: list[dict]) -> None:
    """Upsert each day in daily_picks into current_picks_day. Synchronous;
    call via asyncio.to_thread."""
    if not daily_picks:
        return
    rows: list[dict] = []
    for dp in daily_picks:
        target_date = dp.get("date")
        if not target_date:
            continue
        as_of = f"{target_date[:7]}-01"
        rows.append({
            "strategy_hash": hash_,
            "target_date": target_date,
            "as_of_date": as_of,
            "holdings": dp.get("holdings") or [],
            "portfolio_return_pct": dp.get("portfolio_return_pct"),
            "next_day_return_pct": dp.get("next_day_return_pct"),
            "turnover_abs": dp.get("turnover_abs", 0),
            "turnover_pct": dp.get("turnover_pct", 0),
            "config": config,
        })
    if rows:
        supabase.table("current_picks_day").upsert(
            rows, on_conflict="strategy_hash,target_date"
        ).execute()


def fetch_daily_picks_history(hash_: str) -> list[dict]:
    """All stored daily picks for a strategy, ascending by target_date.
    Shape matches the in-memory DailyPick.to_dict()."""
    resp = supabase.table("current_picks_day").select(
        "target_date, holdings, portfolio_return_pct, next_day_return_pct, turnover_abs, turnover_pct"
    ).eq("strategy_hash", hash_).order("target_date").execute()
    rows = resp.data or []
    return [
        {
            "date": r["target_date"],
            "holdings": r.get("holdings") or [],
            "portfolio_return_pct": r.get("portfolio_return_pct"),
            "next_day_return_pct": r.get("next_day_return_pct"),
            "turnover_abs": r.get("turnover_abs") or 0,
            "turnover_pct": float(r.get("turnover_pct") or 0),
        }
        for r in rows
    ]


def find_cached_snapshot(hash_: str, as_of_date: str) -> dict | None:
    """Most recent snapshot for (hash, as_of_date), or None."""
    resp = supabase.table("current_picks_snapshot").select("*").eq(
        "strategy_hash", hash_
    ).eq("as_of_date", as_of_date).order(
        "created_at", desc=True
    ).limit(1).execute()
    rows = resp.data or []
    return rows[0] if rows else None


def default_snapshot_name(config: dict) -> str:
    """Sensible default label for an auto-saved current-picks snapshot.
    Format: '{universe or All companies} · {YYYY-MM-DD HH:MM}'."""
    universe = (config.get("index_universe") or config.get("universe_label") or "All companies").strip() or "All companies"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    return f"{universe} · {ts}"


# Cached probe for the `name` column on `current_picks_snapshot`. Migration
# 20260507000000_current_picks_name.sql adds it; if it hasn't been applied
# yet the rest of the page keeps working (dropdown, auto-save) without
# 500ing every request. Probed lazily on first use, then cached.
_HAS_CURRENT_PICKS_NAME_COLUMN: bool | None = None


def has_current_picks_name_column() -> bool:
    global _HAS_CURRENT_PICKS_NAME_COLUMN
    if _HAS_CURRENT_PICKS_NAME_COLUMN is None:
        try:
            supabase.table("current_picks_snapshot").select("name").limit(0).execute()
            _HAS_CURRENT_PICKS_NAME_COLUMN = True
        except Exception as e:
            _HAS_CURRENT_PICKS_NAME_COLUMN = False
            logging.getLogger(__name__).warning(
                "[current-picks] `name` column not present on current_picks_snapshot — "
                "rename UX is disabled and the dropdown shows the auto-generated label. "
                "Apply migration 20260507000000_current_picks_name.sql to enable. (%s: %s)",
                type(e).__name__, e,
            )
    return _HAS_CURRENT_PICKS_NAME_COLUMN


def save_current_picks_snapshot(
    payload: dict,
    config: dict,
    triggered_by: str,
    strategy_hash: str | None = None,
    name: str | None = None,
    kind: str = "rebalance",
    is_backfill: bool = False,
) -> int:
    """Insert a current_picks snapshot and return its snapshot_id.
    Synchronous (call via asyncio.to_thread from async paths). Auto-fills
    `name` with `default_snapshot_name(config)` when caller didn't supply
    one. Skips the `name` column entirely when the schema migration
    hasn't been applied yet.

    `kind` distinguishes 'rebalance' (fresh picks computed at this tick)
    from 'price_update' (last rebalance's holdings re-priced because the
    strategy wasn't due to rebalance on this tick). `is_backfill=True`
    marks snapshots created synthetically on add — historical 'what
    would have happened' views, NOT real pipeline runs."""
    if triggered_by not in ("auto", "manual"):
        raise ValueError(f"triggered_by must be 'auto' or 'manual', got {triggered_by!r}")
    if kind not in ("rebalance", "price_update"):
        raise ValueError(f"kind must be 'rebalance' or 'price_update', got {kind!r}")
    row = {
        "triggered_by": triggered_by,
        "as_of_date": payload["as_of_date"],
        "latest_price_date": payload.get("latest_price_date"),
        "config": config,
        "holdings": payload["holdings"],
        "daily_picks": payload.get("daily_picks") or [],
        "strategy_hash": strategy_hash,
        "kind": kind,
        "is_backfill": is_backfill,
    }
    if has_current_picks_name_column():
        row["name"] = name if name is not None else default_snapshot_name(config)
    resp = supabase.table("current_picks_snapshot").insert(row).execute()
    if not resp.data:
        raise RuntimeError("insert returned no data")
    return int(resp.data[0]["snapshot_id"])
