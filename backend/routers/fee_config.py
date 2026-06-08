"""Global fee configuration — the four parameters backing the backtest
fee waterfall.

Single-row table (`fee_config`, id pinned to 1). The /fees page reads it
to render the editable "Fee structure" card and writes it on Save; every
backtest view reads it to compute the layered net returns (Leonteq costs
→ Bustelberg fees) entirely client-side.

Endpoints:
    GET /api/fee-config   → the current config (creates defaults if missing)
    PUT /api/fee-config   → upsert all four parameters

Per-exchange transaction fees (`exchange_fee.fee_bps`) are no longer part
of the cost model — the transaction cost is now the single global
`transaction_bps` here. `exchange_fee.is_broker_supported` is still used
by the backtest universe filter (see backtest_stream/stream.py).
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from deps import supabase

router = APIRouter(tags=["fees"])

# Agreed defaults — also encoded in the migration's column DEFAULTs, kept
# here so a GET that finds an empty table can self-heal to the same values.
_DEFAULTS = {
    "leonteq_annual_bps": 35.0,
    "transaction_bps": 10.0,
    "bustelberg_mgmt_bps": 10.0,  # per MONTH (≈1.2%/yr); was 100 bps/yr
    "bustelberg_perf_pct": 10.0,
}


class FeeConfigIn(BaseModel):
    # All four are capped well above any sane value (10000 bps = 100%;
    # perf fee capped at 100%) so an obvious typo can't silently distort
    # every backtest's waterfall. Lower bound 0 so a fee can be zeroed.
    leonteq_annual_bps: float = Field(ge=0, le=10000)
    transaction_bps: float = Field(ge=0, le=10000)
    bustelberg_mgmt_bps: float = Field(ge=0, le=10000)
    bustelberg_perf_pct: float = Field(ge=0, le=100)


def _read_or_seed() -> dict:
    resp = supabase.table("fee_config").select("*").eq("id", 1).limit(1).execute()
    rows = resp.data or []
    if rows:
        return rows[0]
    # Empty table (fresh env where the seed INSERT didn't run) — create
    # the single row with defaults so the frontend always gets a config.
    ins = supabase.table("fee_config").insert({"id": 1, **_DEFAULTS}).execute()
    return (ins.data or [{"id": 1, **_DEFAULTS}])[0]


@router.get("/api/fee-config")
async def get_fee_config():
    """The single global fee config. Self-heals a missing row to defaults."""
    return await asyncio.to_thread(_read_or_seed)


@router.put("/api/fee-config")
async def put_fee_config(body: FeeConfigIn):
    """Upsert all four fee parameters on the single config row."""
    def _update() -> dict:
        payload = {
            "id": 1,
            "leonteq_annual_bps": body.leonteq_annual_bps,
            "transaction_bps": body.transaction_bps,
            "bustelberg_mgmt_bps": body.bustelberg_mgmt_bps,
            "bustelberg_perf_pct": body.bustelberg_perf_pct,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            resp = supabase.table("fee_config").upsert(payload, on_conflict="id").execute()
        except Exception as e:
            raise HTTPException(500, f"Fee-config upsert failed: {type(e).__name__}: {e}")
        if not resp.data:
            raise HTTPException(500, "Fee-config upsert returned no row")
        return resp.data[0]
    return await asyncio.to_thread(_update)
