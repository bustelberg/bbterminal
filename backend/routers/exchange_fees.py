"""Per-exchange transaction-fee CRUD.

Backs the /fees page. The frontend lists every exchange in
`gurufocus_exchange` (left-joined to `exchange_fee` so unset rows show
`fee_bps=0`) and lets the user set a one-way bps value per exchange.
The backtest UI then reads these fees and renders `gross (net)` next to
every return / Sharpe / drawdown stat using a trade-aware fee model
defined in `frontend/app/components/momentum/feeStats.ts`.

Endpoints:
    GET    /api/exchange-fees                  → list every exchange + its fee
    PUT    /api/exchange-fees/{exchange_code}  → upsert fee_bps
    DELETE /api/exchange-fees/{exchange_code}  → remove the row (effectively 0)
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from deps import supabase

router = APIRouter(tags=["fees"])


class ExchangeFeeIn(BaseModel):
    # Allow 0 so the user can explicitly zero-out a fee without DELETEing
    # the row. Capped at 10000 bps (100%) — fees above that are almost
    # certainly typos and would distort backtest stats unrecognizably.
    fee_bps: float = Field(ge=0, le=10000)
    # Broker-support flag. True = the broker can trade this exchange and
    # its companies stay in the backtest universe. False = drop every
    # company on this exchange before the backtest's universe load.
    # Default-true so omitting the field doesn't accidentally disable an
    # exchange.
    is_broker_supported: bool = True


@router.get("/api/exchange-fees")
async def list_exchange_fees():
    """Every exchange with its currently-configured fee. Exchanges with no
    `exchange_fee` row return `fee_bps=0` so the UI shows them as
    not-yet-set rather than missing. Sorted by exchange_code for stable
    display order."""
    def _query() -> list[dict]:
        # Pull every exchange (including those without a fee row) — Supabase
        # doesn't expose a clean LEFT JOIN through the REST client, so do
        # it client-side instead. Both tables are small (~50 rows) so this
        # is cheap.
        ex_resp = (
            supabase.table("gurufocus_exchange")
            .select("exchange_code, exchange_name, is_us, country_code, currency_code")
            .order("exchange_code")
            .execute()
        )
        exchanges = ex_resp.data or []

        fee_resp = (
            supabase.table("exchange_fee")
            .select("exchange_code, fee_bps, is_broker_supported, updated_at")
            .execute()
        )
        fees_by_code: dict[str, dict] = {r["exchange_code"]: r for r in (fee_resp.data or [])}

        out: list[dict] = []
        for e in exchanges:
            f = fees_by_code.get(e["exchange_code"])
            out.append({
                "exchange_code": e["exchange_code"],
                "exchange_name": e.get("exchange_name"),
                "is_us": e.get("is_us"),
                "country_code": e.get("country_code"),
                "currency_code": e.get("currency_code"),
                "fee_bps": float(f["fee_bps"]) if f else 0.0,
                # Missing row defaults to "supported" — that's the path
                # of least surprise; the user only has to act on the
                # exchanges their broker can't reach.
                "is_broker_supported": bool(f["is_broker_supported"]) if f else True,
                "updated_at": f["updated_at"] if f else None,
            })
        return out

    return await asyncio.to_thread(_query)


@router.put("/api/exchange-fees/{exchange_code}")
async def upsert_exchange_fee(exchange_code: str, body: ExchangeFeeIn):
    """Set or update the fee for one exchange. Upsert keyed on
    `exchange_code` so PUT is idempotent."""
    row = {
        "exchange_code": exchange_code,
        "fee_bps": body.fee_bps,
        "is_broker_supported": body.is_broker_supported,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        resp = await asyncio.to_thread(
            lambda: supabase.table("exchange_fee")
            .upsert(row, on_conflict="exchange_code")
            .execute()
        )
    except Exception as e:
        msg = str(e)
        if "foreign key" in msg.lower() or "violates" in msg.lower():
            raise HTTPException(404, f"Unknown exchange_code: {exchange_code}")
        raise HTTPException(500, f"Upsert failed: {type(e).__name__}: {e}")
    if not resp.data:
        raise HTTPException(500, "Upsert returned no row")
    return resp.data[0]


@router.delete("/api/exchange-fees/{exchange_code}")
async def delete_exchange_fee(exchange_code: str):
    """Reset an exchange to "no fee" by dropping the row. Equivalent to
    PUT with fee_bps=0 in terms of what the frontend reads, just keeps the
    table smaller."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("exchange_fee")
        .delete()
        .eq("exchange_code", exchange_code)
        .execute()
    )
    # delete on a non-existent row returns empty data — treat as success
    # (the UI's intent of "this exchange has no fee" is already true).
    return {"ok": True, "deleted": len(resp.data or [])}
