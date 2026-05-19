"""CRUD + run-history endpoints for the list of strategies the pipeline's
momentum phase computes on every tick.

Each `scheduled_strategy` row pins a `backtest_run` (whose `config` blob
drives the momentum compute). The pipeline writes one
`current_picks_snapshot` per enabled scheduled strategy per run, tagged
with `ingest_run_id` + `backtest_run_id` so the per-strategy run-history
view is a single JOIN.

Replaces the old singleton `schedule_config` router (deleted in the same
rebuild).
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from deps import supabase

router = APIRouter(tags=["schedule"])


class ScheduledStrategyCreate(BaseModel):
    backtest_run_id: int


class ScheduledStrategyPatch(BaseModel):
    # Only field that's editable in place. To change which backtest a
    # schedule entry uses, delete + re-add — keeps history attribution
    # unambiguous.
    enabled: bool | None = None


def _hydrate(rows: list[dict]) -> list[dict]:
    """Attach backtest_run name/config + last-run summary to each row.
    Runs a couple of small lookups so the list endpoint serves the full
    payload the UI needs in one round-trip."""
    if not rows:
        return []
    run_ids = [r["backtest_run_id"] for r in rows]
    bt_resp = (
        supabase.table("backtest_run")
        .select("run_id, name, config")
        .in_("run_id", run_ids)
        .execute()
    )
    bt_by_id = {r["run_id"]: r for r in (bt_resp.data or [])}

    # Latest pipeline-created snapshot per backtest_run, so the list
    # rows can show "last computed: 2026-05-12 · 12 holdings". We pull
    # all matching snapshots in one shot and pick the newest per
    # backtest_run_id in-process.
    snap_resp = (
        supabase.table("current_picks_snapshot")
        .select("snapshot_id, backtest_run_id, ingest_run_id, created_at, "
                "holdings, latest_price_date")
        .in_("backtest_run_id", run_ids)
        .not_.is_("ingest_run_id", "null")
        .order("created_at", desc=True)
        .execute()
    )
    latest_by_bt: dict[int, dict] = {}
    for s in snap_resp.data or []:
        bt = s.get("backtest_run_id")
        if bt is None or bt in latest_by_bt:
            continue
        latest_by_bt[bt] = s

    out: list[dict] = []
    for r in rows:
        bt = bt_by_id.get(r["backtest_run_id"])
        latest = latest_by_bt.get(r["backtest_run_id"])
        out.append({
            "id": r["id"],
            "backtest_run_id": r["backtest_run_id"],
            "enabled": r["enabled"],
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
            "backtest_name": (bt or {}).get("name"),
            "backtest_config": (bt or {}).get("config"),
            "last_snapshot": (
                {
                    "snapshot_id": latest["snapshot_id"],
                    "ingest_run_id": latest["ingest_run_id"],
                    "created_at": latest["created_at"],
                    "latest_price_date": latest.get("latest_price_date"),
                    "holdings_count": len(latest.get("holdings") or []),
                }
                if latest
                else None
            ),
        })
    return out


@router.get("/api/scheduled-strategies")
async def list_scheduled_strategies():
    """Every scheduled strategy with backtest name + last-run summary."""
    def _query() -> list[dict]:
        resp = (
            supabase.table("scheduled_strategy")
            .select("*")
            .order("created_at")
            .execute()
        )
        return _hydrate(resp.data or [])
    return await asyncio.to_thread(_query)


@router.get("/api/scheduled-strategies/available-backtests")
async def list_available_backtests():
    """Saved backtest_run rows that haven't been scheduled yet — the add
    picker uses this to avoid showing already-scheduled ones."""
    def _query() -> list[dict]:
        scheduled_resp = (
            supabase.table("scheduled_strategy")
            .select("backtest_run_id")
            .execute()
        )
        already = {r["backtest_run_id"] for r in (scheduled_resp.data or [])}
        runs_resp = (
            supabase.table("backtest_run")
            .select("run_id, name, created_at, config")
            .order("created_at", desc=True)
            .execute()
        )
        return [
            r for r in (runs_resp.data or [])
            if r["run_id"] not in already
        ]
    return await asyncio.to_thread(_query)


@router.post("/api/scheduled-strategies")
async def add_scheduled_strategy(body: ScheduledStrategyCreate):
    """Pin a saved backtest to the schedule. Idempotent on the UNIQUE
    `backtest_run_id` — re-adding the same run returns the existing row."""
    def _insert() -> dict:
        existing = (
            supabase.table("scheduled_strategy")
            .select("*")
            .eq("backtest_run_id", body.backtest_run_id)
            .limit(1)
            .execute()
        )
        if existing.data:
            return _hydrate(existing.data)[0]
        try:
            resp = (
                supabase.table("scheduled_strategy")
                .insert({"backtest_run_id": body.backtest_run_id})
                .execute()
            )
        except Exception as e:
            msg = str(e).lower()
            if "foreign key" in msg or "violates" in msg:
                raise HTTPException(404, f"Unknown backtest_run.run_id: {body.backtest_run_id}")
            raise HTTPException(500, f"Insert failed: {type(e).__name__}: {e}")
        if not resp.data:
            raise HTTPException(500, "Insert returned no row")
        return _hydrate(resp.data)[0]
    return await asyncio.to_thread(_insert)


@router.patch("/api/scheduled-strategies/{strategy_id}")
async def patch_scheduled_strategy(strategy_id: int, body: ScheduledStrategyPatch):
    """Toggle enabled. (Re-pointing at a different backtest isn't allowed
    here — delete + re-add instead, so per-snapshot attribution stays clean.)"""
    if body.enabled is None:
        raise HTTPException(400, "Nothing to update (pass `enabled`).")
    def _update() -> dict:
        resp = (
            supabase.table("scheduled_strategy")
            .update({
                "enabled": body.enabled,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
            .eq("id", strategy_id)
            .execute()
        )
        if not resp.data:
            raise HTTPException(404, f"Scheduled strategy #{strategy_id} not found")
        return _hydrate(resp.data)[0]
    return await asyncio.to_thread(_update)


@router.delete("/api/scheduled-strategies/{strategy_id}")
async def delete_scheduled_strategy(strategy_id: int):
    """Remove from the schedule. Past snapshots stay (they keep their
    `backtest_run_id` for historical viewing) but new pipeline runs
    skip this strategy."""
    def _delete() -> dict:
        resp = (
            supabase.table("scheduled_strategy")
            .delete()
            .eq("id", strategy_id)
            .execute()
        )
        if not resp.data:
            raise HTTPException(404, f"Scheduled strategy #{strategy_id} not found")
        return {"deleted": strategy_id}
    return await asyncio.to_thread(_delete)


@router.get("/api/scheduled-strategies/{strategy_id}/runs")
async def list_strategy_runs(strategy_id: int, limit: int = 50):
    """Run history for one scheduled strategy.

    Returns the snapshots this strategy's backtest_run produced via
    pipeline runs (i.e. `ingest_run_id IS NOT NULL`), joined with the
    `ingest_run` row so the UI can render each row with run status, ACWI
    summary, started_at, etc. Newest first."""
    limit = max(1, min(200, limit))

    def _query() -> dict:
        sched_resp = (
            supabase.table("scheduled_strategy")
            .select("*")
            .eq("id", strategy_id)
            .limit(1)
            .execute()
        )
        if not sched_resp.data:
            raise HTTPException(404, f"Scheduled strategy #{strategy_id} not found")
        sched = sched_resp.data[0]

        bt_resp = (
            supabase.table("backtest_run")
            .select("run_id, name, config")
            .eq("run_id", sched["backtest_run_id"])
            .limit(1)
            .execute()
        )
        bt = (bt_resp.data or [{}])[0]

        snap_resp = (
            supabase.table("current_picks_snapshot")
            .select("snapshot_id, ingest_run_id, created_at, as_of_date, "
                    "latest_price_date, holdings")
            .eq("backtest_run_id", sched["backtest_run_id"])
            .not_.is_("ingest_run_id", "null")
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        snapshots = snap_resp.data or []

        # Hydrate each snapshot with its ingest_run row.
        run_ids = list({s["ingest_run_id"] for s in snapshots if s.get("ingest_run_id")})
        runs_by_id: dict[int, dict] = {}
        if run_ids:
            runs_resp = (
                supabase.table("ingest_run")
                .select("*")
                .in_("run_id", run_ids)
                .execute()
            )
            runs_by_id = {r["run_id"]: r for r in (runs_resp.data or [])}

        history = [
            {
                "snapshot_id": s["snapshot_id"],
                "created_at": s["created_at"],
                "as_of_date": s["as_of_date"],
                "latest_price_date": s.get("latest_price_date"),
                "holdings_count": len(s.get("holdings") or []),
                "ingest_run": runs_by_id.get(s["ingest_run_id"]),
            }
            for s in snapshots
        ]

        return {
            "id": sched["id"],
            "backtest_run_id": sched["backtest_run_id"],
            "enabled": sched["enabled"],
            "created_at": sched["created_at"],
            "backtest_name": bt.get("name"),
            "backtest_config": bt.get("config"),
            "runs": history,
        }

    return await asyncio.to_thread(_query)
