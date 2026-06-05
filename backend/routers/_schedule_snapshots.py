"""Current-picks snapshot writers for the schedule.

Extracted from `routers.scheduled_strategies` (the HTTP layer). These build
`current_picks_snapshot` rows — the price-update re-pricer the pipeline's
momentum phase calls every non-rebalance tick, plus the backtest-seed used
when a strategy is first added. No FastAPI here; pure DB + the backtest
loader.
"""
from __future__ import annotations

import logging
from datetime import date

from deps import fetch_in_chunks, supabase

_log = logging.getLogger(__name__)


def compute_and_save_price_update(
    strategy_id: int,
    ingest_run_id: int | None,
    is_backfill: bool = False,
    as_of_iso: str | None = None,
) -> int | None:
    """Build a price_update snapshot for `strategy_id` by re-pricing the
    most recent rebalance's holdings against the latest available close
    prices. Returns the new snapshot_id, or None when no prior rebalance
    exists (nothing to update from).

    Output snapshot fields:
      * `holdings`: same set as the rebalance, but each holding's
        `exit_price_local` + `exit_date` + `forward_return_pct` are
        updated to reflect the latest close.
      * `as_of_date`: unchanged from the rebalance (the entry point
        the returns are measured against).
      * `latest_price_date`: the most recent close-price date seen
        across holdings.
      * `kind`: 'price_update'.

    Used by:
      - the weekly pipeline tick, for every enabled strategy that
        isn't due to rebalance on this tick;
      - the backfill flow, for past Tuesdays where the strategy
        wouldn't have rebalanced (`is_backfill=True`).
    """
    # Order by `as_of_date` (the rebalance date itself), NOT `created_at`.
    # Backfill inserts every historical period's rebalance row in one
    # batch, so all 5 of them share a created_at within milliseconds of
    # each other — `created_at desc` then picks an essentially random
    # row, often the OLDEST as_of_date (the period inserted last by the
    # backfill loop). Ordering by `as_of_date desc` deterministically
    # picks the most-recent rebalance, which is what "the strategy's
    # current open period" actually means.
    rebal_resp = (
        supabase.table("current_picks_snapshot")
        .select("*")
        .eq("scheduled_strategy_id", strategy_id)
        .eq("kind", "rebalance")
        .order("as_of_date", desc=True)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    if not rebal_resp.data:
        return None
    rebal = rebal_resp.data[0]
    holdings = rebal.get("holdings") or []
    if not holdings:
        return None

    # Fetch the latest close-price observation for every holding's
    # company_id in one batched query. We `order desc` and pick the
    # first hit per cid in-process — Postgres has no efficient
    # DISTINCT ON via PostgREST.
    cids = [h.get("company_id") for h in holdings if h.get("company_id") is not None]
    latest_by_cid: dict[int, dict] = {}
    # Chunk to stay under the PostgREST URL-length window (see fetch_in_chunks).
    for r in fetch_in_chunks(
        cids,
        lambda chunk: supabase.table("metric_data")
        .select("company_id, target_date, numeric_value")
        .eq("metric_code", "close_price")
        .in_("company_id", chunk)
        .order("target_date", desc=True)
        .execute(),
    ):
        cid = r["company_id"]
        if cid not in latest_by_cid:
            latest_by_cid[cid] = r

    updated_holdings: list[dict] = []
    weighted_return_sum = 0.0
    total_weight = 0.0
    latest_price_date: str | None = None
    for h in holdings:
        new_h = dict(h)
        cid = h.get("company_id")
        entry_local = h.get("entry_price_local")
        weight = float(h.get("weight") or 0.0)
        latest = latest_by_cid.get(cid)
        if latest and entry_local:
            current_local = float(latest["numeric_value"])
            target_d = str(latest["target_date"])[:10]
            new_h["exit_price_local"] = current_local
            new_h["exit_date"] = target_d
            ret = ((current_local - float(entry_local)) / float(entry_local)) * 100.0
            new_h["forward_return_pct"] = ret
            if latest_price_date is None or target_d > latest_price_date:
                latest_price_date = target_d
            weighted_return_sum += ret * weight
            total_weight += weight
        updated_holdings.append(new_h)

    portfolio_return = weighted_return_sum / total_weight if total_weight > 0 else None

    new_row = {
        "triggered_by": "auto",
        "as_of_date": rebal["as_of_date"],
        "latest_price_date": latest_price_date,
        "config": rebal.get("config"),
        "holdings": updated_holdings,
        "daily_picks": [],
        "strategy_hash": rebal.get("strategy_hash"),
        "name": rebal.get("name"),
        "kind": "price_update",
        "is_backfill": is_backfill,
        "ingest_run_id": ingest_run_id,
        "scheduled_strategy_id": strategy_id,
        # Weighted aggregate of per-holding returns since the prior
        # rebalance — the % gain so far on this position. Renders on
        # the run-history row.
        "period_return_pct": portfolio_return,
    }
    ins = supabase.table("current_picks_snapshot").insert(new_row).execute()
    if not ins.data:
        return None
    # Best-effort log; signature noise is intentional for debugging later.
    _log.info(
        "[price_update] strategy=%s prior_rebal=%s new=%s portfolio_return=%.2f%% "
        "(backfill=%s)",
        strategy_id, rebal.get("snapshot_id"), ins.data[0].get("snapshot_id"),
        portfolio_return or 0.0, is_backfill,
    )
    return int(ins.data[0]["snapshot_id"])


def _coerce_as_of_date(raw: str | None) -> str:
    """Backtest period dates are YYYY-MM strings; current_picks_snapshot
    expects YYYY-MM-DD. Convert by appending '-01'."""
    if not raw:
        return date.today().isoformat()
    s = str(raw)
    if len(s) == 7 and s[4] == "-":  # YYYY-MM
        return s + "-01"
    return s[:10]


def _latest_exit_date(rec: dict) -> str | None:
    """Highest `exit_date` across the record's holdings — a reasonable
    proxy for the snapshot's `latest_price_date`."""
    out: str | None = None
    for h in (rec.get("holdings") or []):
        d = h.get("exit_date") or h.get("entry_date")
        if d and (out is None or d > out):
            out = d
    return out


def _seed_snapshot_from_backtest(
    strategy_id: int, backtest_run_id: int | None, name: str, config: dict,
) -> int | None:
    """Seed the strategy's first `current_picks_snapshot` from its saved
    backtest's most-recent period — so it has live holdings the daily price
    refresh can track IMMEDIATELY, with no off-cycle rebalance (no universe
    reprice / template scrape). The holdings ARE the backtest's current
    picks. Then re-price them against the latest DB closes so the seed
    reflects today's data, not the backtest's end date.

    Returns the seeded snapshot_id (or None when there's nothing to seed:
    no backtest run, empty result, or no holdings)."""
    from routers.momentum.backtest_crud import load_backtest_result_sync  # noqa: PLC0415

    if not backtest_run_id:
        return None
    result = load_backtest_result_sync(backtest_run_id)
    monthly = (result or {}).get("monthly_records") or []
    if not monthly:
        return None
    last = monthly[-1]
    holdings = last.get("holdings") or []
    if not holdings:
        return None

    row = {
        "triggered_by": "auto",
        # The backtest's last period is the current open period; its date is
        # the period's first-<weekday> (e.g. 2026-06-01). Anchor the snapshot
        # there so it reads as the current-period rebalance.
        "as_of_date": _coerce_as_of_date(last.get("as_of_date") or last.get("date")),
        "latest_price_date": _latest_exit_date(last),
        "config": config,
        "holdings": holdings,
        "daily_picks": [],
        "strategy_hash": None,
        "name": name,
        "kind": "rebalance",
        # Seeded from the backtest, not a live pipeline rebalance.
        "is_backfill": True,
        "scheduled_strategy_id": strategy_id,
        "period_return_pct": last.get("portfolio_return_pct"),
    }
    ins = supabase.table("current_picks_snapshot").insert(row).execute()
    if not ins.data:
        return None
    seeded_id = int(ins.data[0]["snapshot_id"])
    # Re-price against the latest available closes so 'since go-live' starts
    # from current data. Best-effort — the daily refresh would do it anyway.
    try:
        compute_and_save_price_update(strategy_id, ingest_run_id=None, is_backfill=True)
    except Exception as e:
        _log.warning(
            "[seed] strategy=%s post-seed price_update failed: %s: %s",
            strategy_id, type(e).__name__, e,
        )
    _log.info("[seed] strategy=%s seeded snapshot=%s from backtest_run=%s (%s holdings)",
              strategy_id, seeded_id, backtest_run_id, len(holdings))
    return seeded_id
