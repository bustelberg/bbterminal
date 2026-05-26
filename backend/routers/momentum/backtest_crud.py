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

`daily_records` are transparently re-encoded into a parallel-array
`{dates, returns}` form on save and re-expanded to the verbose
`[{date, cumulative_return_pct}, ...]` shape on load. For a 24y × 14-variant
bundle this drops the JSONB blob from ~5.6 MB to ~2 MB — well under
Supabase's statement_timeout — so the daily equity curve survives a save
+ reload instead of falling back to the period-step line.
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from deps import supabase

router = APIRouter(tags=["momentum"])


def _compact_daily_records(records):
    """Encode a verbose `[{date, cumulative_return_pct}, ...]` list as the
    parallel-array `{dates: [...], returns: [...]}` form. Returns the input
    untouched if it isn't a list (already compact, or missing)."""
    if not isinstance(records, list):
        return records
    dates: list[str] = []
    returns: list[float] = []
    for r in records:
        if not isinstance(r, dict):
            continue
        d = r.get("date")
        v = r.get("cumulative_return_pct")
        if d is None or v is None:
            continue
        dates.append(d)
        returns.append(v)
    return {"dates": dates, "returns": returns}


def _expand_daily_records(value):
    """Inverse of `_compact_daily_records`. Accepts either compact dict form
    or verbose list form (legacy rows) and always returns the verbose list
    shape the frontend expects."""
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        dates = value.get("dates") or []
        returns = value.get("returns") or []
        n = min(len(dates), len(returns))
        return [
            {"date": dates[i], "cumulative_return_pct": returns[i]}
            for i in range(n)
        ]
    return []


def _compact_in_place(blob: dict) -> None:
    """Compact every `daily_records` field inside a result blob — both the
    top-level (single-run shape) and per-variant copies (variant-bundle
    shape). Mutates the blob in place; safe to call on either shape."""
    if "daily_records" in blob:
        blob["daily_records"] = _compact_daily_records(blob["daily_records"])
    variants = blob.get("variants")
    if isinstance(variants, list):
        for v in variants:
            if isinstance(v, dict) and "daily_records" in v:
                v["daily_records"] = _compact_daily_records(v["daily_records"])


def _expand_in_place(blob: dict) -> None:
    """Inverse of `_compact_in_place`. Handles legacy verbose rows
    transparently (passthrough)."""
    if "daily_records" in blob:
        blob["daily_records"] = _expand_daily_records(blob["daily_records"])
    variants = blob.get("variants")
    if isinstance(variants, list):
        for v in variants:
            if isinstance(v, dict) and "daily_records" in v:
                v["daily_records"] = _expand_daily_records(v["daily_records"])


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
    """Save a backtest run. Accepts single-run or variant-bundle shape.

    `daily_records` on the inbound payload — verbose
    `[{date, cumulative_return_pct}, ...]` from the frontend — is encoded
    into the compact parallel-array form before insert so the JSONB blob
    stays small enough to clear Supabase's statement_timeout for
    multi-decade × N-variant bundles."""
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
    _compact_in_place(result_blob)
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
    """List saved backtests — metadata + config, no result blob.

    The frontend dropdown consumes (run_id, name, created_at, config).
    `config` is included so the dropdown can render a one-line subtext
    that disambiguates same-name runs by their parameters (top_n × per
    sector, date range, selection mode, signal weights). It's small
    (~1-3 KB per row) compared to the result blob which can be
    multi-MB for variant bundles.
    """
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .select("run_id, name, created_at, config")
        .order("created_at", desc=True)
        .execute()
    )
    return resp.data or []


@router.get("/api/momentum/backtests/{run_id}")
async def load_backtest(run_id: int):
    """Full backtest payload for one run. Re-expands compact `daily_records`
    back to the verbose `[{date, cumulative_return_pct}, ...]` shape so the
    frontend can keep treating saved runs identically to in-memory ones.
    Legacy rows (verbose-on-disk) pass through unchanged."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .select("*")
        .eq("run_id", run_id)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Backtest not found")
    row = resp.data[0]
    result = row.get("result")
    if isinstance(result, dict):
        _expand_in_place(result)
    return row


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
