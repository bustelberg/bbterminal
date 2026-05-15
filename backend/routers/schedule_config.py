"""Single-row `schedule_config` CRUD — backs the "Scheduled strategy"
picker on the /schedule page.

The selected `backtest_run` row's `config` blob is what the pipeline's
momentum phase passes through to `_momentum_backtest_stream` (with
`mode=current_portfolio` overridden). A NULL `selected_run_id` means
"skip the momentum compute phase".
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from deps import supabase

router = APIRouter(tags=["schedule"])


class ScheduleConfigUpdate(BaseModel):
    # None = clear the selection. Otherwise must reference an existing
    # backtest_run; the FK constraint enforces that at the DB level.
    selected_run_id: int | None = None


@router.get("/api/schedule-config")
async def get_schedule_config():
    """Return the single config row, the list of available saved backtest
    runs the user can pick from, and the full config blob of the
    currently-selected run (when one is selected) so the UI can render
    the picked strategy's signal weights / sectors / top-N inline."""
    def _query() -> dict:
        cfg_resp = (
            supabase.table("schedule_config")
            .select("selected_run_id, updated_at")
            .eq("id", 1)
            .limit(1)
            .execute()
        )
        cfg = (cfg_resp.data or [{}])[0]
        runs_resp = (
            supabase.table("backtest_run")
            .select("run_id, name, created_at")
            .order("created_at", desc=True)
            .execute()
        )
        selected_config: dict | None = None
        selected_run_name: str | None = None
        sel_id = cfg.get("selected_run_id")
        if sel_id is not None:
            bt_resp = (
                supabase.table("backtest_run")
                .select("name, config")
                .eq("run_id", sel_id)
                .limit(1)
                .execute()
            )
            if bt_resp.data:
                selected_config = bt_resp.data[0].get("config")
                selected_run_name = bt_resp.data[0].get("name")
        return {
            "selected_run_id": sel_id,
            "selected_run_name": selected_run_name,
            "selected_run_config": selected_config,
            "updated_at": cfg.get("updated_at"),
            "available_runs": runs_resp.data or [],
        }
    return await asyncio.to_thread(_query)


@router.put("/api/schedule-config")
async def put_schedule_config(body: ScheduleConfigUpdate):
    """Update which saved backtest the pipeline's momentum phase uses.
    Pass `selected_run_id=null` to clear and have the pipeline skip the
    momentum phase entirely."""
    def _update() -> dict:
        try:
            resp = (
                supabase.table("schedule_config")
                .update({
                    "selected_run_id": body.selected_run_id,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
                .eq("id", 1)
                .execute()
            )
        except Exception as e:
            msg = str(e).lower()
            if "foreign key" in msg or "violates" in msg:
                raise HTTPException(404, f"Unknown backtest_run.run_id: {body.selected_run_id}")
            raise HTTPException(500, f"Update failed: {type(e).__name__}: {e}")
        if not resp.data:
            raise HTTPException(500, "Update returned no row (singleton missing?)")
        return resp.data[0]
    return await asyncio.to_thread(_update)
