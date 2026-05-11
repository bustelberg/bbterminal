"""Saved-backtests CRUD.

Endpoints:
    POST   /api/momentum/backtests          save a run (single or variant bundle)
    GET    /api/momentum/backtests          list (metadata only — see note)
    GET    /api/momentum/backtests/{run_id} full payload for one run
    DELETE /api/momentum/backtests/{run_id} drop one
    PATCH  /api/momentum/backtests/{run_id} rename

The list endpoint deliberately ships only (run_id, name, created_at). An
earlier version included the full result blob — for variant bundles that
ballooned the response to >50 MB and made the dropdown unusable. Full
payload is fetched on demand via the per-run GET when the user clicks
into a saved run.
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from deps import supabase

router = APIRouter(tags=["momentum"])


class SaveBacktestRequest(BaseModel):
    name: str
    config: dict
    # Single-run shape: provide summary + monthly_records.
    summary: dict | None = None
    monthly_records: list | None = None
    # Variant-bundle shape: provide a list of variants, each
    # {key, label, summary, monthly_records}. When present, `summary` /
    # `monthly_records` are ignored and the row is stored as
    # `result = {kind: "variants", variants, universe}`.
    variants: list | None = None
    universe: list  # [{company_id, ticker, exchange, company_name, sector}]


class RenameBacktestRequest(BaseModel):
    name: str


@router.post("/api/momentum/backtests")
async def save_backtest(req: SaveBacktestRequest):
    """Save a backtest run. Accepts single-run or variant-bundle shape."""
    if req.variants is not None:
        result_blob = {
            "kind": "variants",
            "variants": req.variants,
            "universe": req.universe,
        }
    else:
        if req.summary is None or req.monthly_records is None:
            raise HTTPException(
                422,
                "Single-run save requires summary and monthly_records",
            )
        result_blob = {
            "summary": req.summary,
            "monthly_records": req.monthly_records,
            "universe": req.universe,
        }
    row = {
        "name": req.name.strip(),
        "config": req.config,
        "result": result_blob,
    }
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run").insert(row).execute()
    )
    if not resp.data:
        raise HTTPException(500, "Failed to save backtest")
    return resp.data[0]


@router.get("/api/momentum/backtests")
async def list_backtests():
    """List saved backtests — metadata only, no result blob.

    The frontend dropdown only consumes (run_id, name, created_at). An
    earlier version returned the result blob too, which ballooned the
    response to ~50 MB for ~13 variant-bundle saves and made the dropdown
    unusable. Per-run load is on demand below.
    """
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .select("run_id, name, created_at")
        .order("created_at", desc=True)
        .execute()
    )
    return resp.data or []


@router.get("/api/momentum/backtests/{run_id}")
async def load_backtest(run_id: int):
    """Full backtest payload for one run."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .select("*")
        .eq("run_id", run_id)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Backtest not found")
    return resp.data[0]


@router.delete("/api/momentum/backtests/{run_id}")
async def delete_backtest(run_id: int):
    """Delete a saved backtest run."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .delete()
        .eq("run_id", run_id)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Backtest not found")
    return {"ok": True}


@router.patch("/api/momentum/backtests/{run_id}")
async def rename_backtest(run_id: int, req: RenameBacktestRequest):
    """Rename a saved backtest run."""
    new_name = req.name.strip()
    if not new_name:
        raise HTTPException(400, "Name cannot be empty")
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .update({"name": new_name})
        .eq("run_id", run_id)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Backtest not found")
    return resp.data[0]
