"""Current-picks snapshots: persistence + refresh-MTD + weekly cron.

Endpoints:
    GET    /api/momentum/current-picks               list snapshots (no heavy JSONB)
    GET    /api/momentum/current-picks/{id}          full snapshot incl. holdings
    DELETE /api/momentum/current-picks/{id}          drop one
    PATCH  /api/momentum/current-picks/{id}          set / clear the custom name
    POST   /api/momentum/current-picks/{id}/refresh-mtd
                                                     recompute MTD on a STORED
                                                     snapshot's holdings (doesn't mutate)
    POST   /api/momentum/current-picks/cron          weekly fresh compute + auto-save

The cron entrypoint requires the X-Cron-Secret header to match
CRON_SECRET in the env. It forces mode=current_portfolio and bypasses
the cache so the weekly snapshot is always a fresh compute.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from deps import supabase
from ingest.prices import ensure_prices_for_company
from momentum.data import (
    convert_prices_to_eur,
    load_all_prices,
    load_company_currency,
    load_fx_rates,
)

from ._helpers import (
    has_current_picks_name_column as _has_current_picks_name_column,
    save_current_picks_snapshot as _save_current_picks_snapshot,
)
from .backtest_stream import BacktestRequest, _momentum_backtest_stream

router = APIRouter(tags=["momentum"])


@router.get("/api/momentum/current-picks")
async def list_current_picks():
    """List snapshots, most recent first. Excludes the heavy holdings JSONB.
    Drops the `name` column from the SELECT when the migration hasn't been
    applied yet — the frontend already treats `name` as optional and falls
    back to the auto-generated date/trigger label."""
    has_name = await asyncio.to_thread(_has_current_picks_name_column)
    cols = "snapshot_id, created_at, triggered_by, as_of_date, latest_price_date"
    if has_name:
        cols += ", name"
    resp = await asyncio.to_thread(
        lambda: supabase.table("current_picks_snapshot")
        .select(cols)
        .order("created_at", desc=True)
        .execute()
    )
    return resp.data or []


@router.get("/api/momentum/current-picks/{snapshot_id}")
async def get_current_picks(snapshot_id: int):
    """Full snapshot for one id, including holdings."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("current_picks_snapshot")
        .select("*")
        .eq("snapshot_id", snapshot_id)
        .limit(1)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Snapshot not found")
    return resp.data[0]


@router.delete("/api/momentum/current-picks/{snapshot_id}")
async def delete_current_picks(snapshot_id: int):
    """Delete a current-picks snapshot."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("current_picks_snapshot")
        .delete()
        .eq("snapshot_id", snapshot_id)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Snapshot not found")
    return {"ok": True}


class RenameCurrentPicksRequest(BaseModel):
    # Empty string clears the custom label and falls the dropdown back to
    # the auto-generated date/trigger title.
    name: str | None = None


@router.patch("/api/momentum/current-picks/{snapshot_id}")
async def rename_current_picks(snapshot_id: int, req: RenameCurrentPicksRequest):
    """Set or clear a custom name on a snapshot."""
    if not await asyncio.to_thread(_has_current_picks_name_column):
        raise HTTPException(
            503,
            "Snapshot rename requires the `name` column. Apply migration "
            "20260507000000_current_picks_name.sql to enable.",
        )
    new_name = (req.name or "").strip() or None
    resp = await asyncio.to_thread(
        lambda: supabase.table("current_picks_snapshot")
        .update({"name": new_name})
        .eq("snapshot_id", snapshot_id)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Snapshot not found")
    return resp.data[0]


def _refresh_mtd_for_holdings(holdings: list[dict]) -> tuple[list[dict], str | None]:
    """Recompute MTD (forward_return_pct) for already-picked holdings using
    the latest available prices. Returns (updated_holdings, latest_price_date)."""
    company_ids = [int(h["company_id"]) for h in holdings if h.get("company_id") is not None]
    if not company_ids:
        return holdings, None

    # Freshen GuruFocus prices for the held companies before reading the DB.
    # Bounded by the number of held names (~20–30); each call has its own
    # DB-freshness fast path so it's a no-op for any company whose latest
    # close already covers today. Unblocks the "daily picks last day is 0%
    # because we never fetched the next-day close" case.
    meta_resp = (
        supabase.table("company")
        .select("company_id,gurufocus_ticker,gurufocus_exchange:gurufocus_exchange(exchange_code)")
        .in_("company_id", company_ids)
        .execute()
    )
    company_meta: dict[int, tuple[str, str]] = {}
    for r in (meta_resp.data or []):
        cid = int(r["company_id"])
        ticker = r.get("gurufocus_ticker") or ""
        exch = (r.get("gurufocus_exchange") or {}).get("exchange_code") or ""
        if ticker:
            company_meta[cid] = (ticker, exch)

    def _ensure(cid: int) -> None:
        m = company_meta.get(cid)
        if not m:
            return
        try:
            ensure_prices_for_company(supabase, cid, m[0], m[1])
        except Exception as e:
            # Best-effort: downstream still uses whatever's in the DB. But
            # silent failure here is exactly how the WAR:SPL "no MTD update"
            # bug went unnoticed — log so the next regression is googleable.
            logging.getLogger(__name__).warning(
                "[refresh_mtd] ensure_prices_for_company failed for cid=%s ticker=%s exch=%s: %s: %s",
                cid, m[0], m[1], type(e).__name__, e,
            )

    if company_meta:
        with ThreadPoolExecutor(max_workers=min(8, len(company_meta))) as pool:
            list(pool.map(_ensure, list(company_meta.keys())))

    # Look back ~14 days from today — the latest close should always land
    # inside that window even after long weekends / holidays.
    today = date.today()
    start = today - timedelta(days=14)

    prices_local_df = load_all_prices(supabase, company_ids, start, today)
    if prices_local_df.empty:
        return holdings, None

    company_currency = load_company_currency(supabase, company_ids)
    currencies = sorted({c for c in company_currency.values() if c})
    fx_rates = load_fx_rates(supabase, currencies, start, today) if currencies else {}
    prices_eur_df, _ = convert_prices_to_eur(prices_local_df, company_currency, fx_rates)

    # Index latest price per company.
    latest_eur: dict[int, tuple[date, float]] = {}
    if not prices_eur_df.empty:
        for cid, group in prices_eur_df.groupby("company_id"):
            row = group.sort_values("target_date").iloc[-1]
            latest_eur[int(cid)] = (row["target_date"], float(row["price"]))
    latest_local: dict[int, tuple[date, float]] = {}
    for cid, group in prices_local_df.groupby("company_id"):
        row = group.sort_values("target_date").iloc[-1]
        latest_local[int(cid)] = (row["target_date"], float(row["price"]))

    overall_latest: date | None = None
    updated: list[dict] = []
    for h in holdings:
        cid = int(h.get("company_id")) if h.get("company_id") is not None else None
        new_h = dict(h)
        if cid is not None and cid in latest_local:
            ld, lp_local = latest_local[cid]
            new_h["exit_price_local"] = round(lp_local, 4)
            new_h["exit_date"] = ld.isoformat() if hasattr(ld, "isoformat") else str(ld)
            if overall_latest is None or ld > overall_latest:
                overall_latest = ld
        if cid is not None and cid in latest_eur:
            _, lp_eur = latest_eur[cid]
            new_h["exit_price_eur"] = round(lp_eur, 4)
            entry_eur = h.get("entry_price_eur")
            if entry_eur and entry_eur > 0:
                new_h["forward_return_pct"] = round((lp_eur / float(entry_eur) - 1) * 100, 2)
        updated.append(new_h)

    latest_iso = overall_latest.isoformat() if overall_latest else None
    return updated, latest_iso


@router.get("/api/momentum/prices-at")
async def prices_at(as_of: str | None = None, company_ids: str | None = None):
    """Close price (local + EUR) for each company at the nearest trading day
    on/before `as_of`. Read-only DB lookup — the Portfolio table uses it to
    re-price holdings at a go-live split boundary so each sub-period shows
    the entry/exit prices for its own dates. `company_ids` is comma-separated.

    Both params are optional so a no-arg probe (e.g. CI's GET smoke) gets a
    valid empty `{prices: {}}` rather than a 422 — the frontend always sends
    both.
    """
    def _q() -> dict:
        if not as_of or not company_ids:
            return {"prices": {}}
        try:
            target = date.fromisoformat(as_of[:10])
        except ValueError:
            raise HTTPException(400, "as_of must be YYYY-MM-DD")
        cids = [int(x) for x in company_ids.split(",") if x.strip().isdigit()]
        if not cids:
            return {"prices": {}}
        # 14-day lookback comfortably spans weekends/holidays to find the
        # most recent close on/before the target date.
        start = target - timedelta(days=14)
        prices_local_df = load_all_prices(supabase, cids, start, target)
        if prices_local_df.empty:
            return {"prices": {}}
        company_currency = load_company_currency(supabase, cids)
        currencies = sorted({c for c in company_currency.values() if c})
        fx_rates = load_fx_rates(supabase, currencies, start, target) if currencies else {}
        prices_eur_df, _ = convert_prices_to_eur(prices_local_df, company_currency, fx_rates)

        out: dict[str, dict] = {}
        for cid, group in prices_local_df.groupby("company_id"):
            row = group.sort_values("target_date").iloc[-1]
            td = row["target_date"]
            out[str(int(cid))] = {
                "price_local": round(float(row["price"]), 4),
                "target_date": td.isoformat() if hasattr(td, "isoformat") else str(td),
            }
        if not prices_eur_df.empty:
            for cid, group in prices_eur_df.groupby("company_id"):
                row = group.sort_values("target_date").iloc[-1]
                key = str(int(cid))
                if key in out:
                    out[key]["price_eur"] = round(float(row["price"]), 4)
        return {"prices": out}
    return await asyncio.to_thread(_q)


@router.post("/api/momentum/current-picks/{snapshot_id}/refresh-mtd")
async def refresh_current_picks_mtd(snapshot_id: int):
    """Recompute MTD on a STORED snapshot using the latest available prices.
    Does NOT mutate the stored snapshot — this is a read-side recompute."""
    snap_resp = await asyncio.to_thread(
        lambda: supabase.table("current_picks_snapshot")
        .select("*")
        .eq("snapshot_id", snapshot_id)
        .limit(1)
        .execute()
    )
    if not snap_resp.data:
        raise HTTPException(404, "Snapshot not found")
    snap = snap_resp.data[0]
    holdings = snap.get("holdings") or []
    updated, latest = await asyncio.to_thread(_refresh_mtd_for_holdings, holdings)
    return {
        "snapshot_id": snapshot_id,
        "as_of_date": snap.get("as_of_date"),
        "latest_price_date": latest,
        "holdings": updated,
    }


@router.post("/api/momentum/current-picks/cron")
async def cron_current_picks(req: BacktestRequest, x_cron_secret: str = Header(default="")):
    """Cron entry point — forces mode=current_portfolio, runs the full
    compute, and persists with triggered_by='auto'.

    Auth: X-Cron-Secret header must match the CRON_SECRET env var.
    """
    expected = os.environ.get("CRON_SECRET", "")
    if not expected:
        raise HTTPException(500, "CRON_SECRET env var is not set on the server")
    if x_cron_secret != expected:
        raise HTTPException(401, "Invalid cron secret")

    # Force the right mode regardless of body content. Cron always
    # recomputes against fresh API data — its purpose is to land a fresh
    # weekly snapshot, so the db_only default is overridden here.
    req.mode = "current_portfolio"
    req.force_recompute = True
    req.db_only = False
    if req.selection_mode == "random":
        raise HTTPException(400, "Cron does not support random selection mode")

    # Drain the SSE stream to completion, then persist + return JSON.
    # The SSE stream's universe payload is for the frontend's display layer;
    # the cron only needs the snapshot itself, so we drop it.
    payload: dict | None = None
    error_msg: str | None = None
    async for chunk in _momentum_backtest_stream(req):
        # Each chunk is "data: {json}\n\n" or ": keepalive\n\n"
        if not chunk.startswith("data: "):
            continue
        try:
            evt = json.loads(chunk[len("data: "):].strip())
        except json.JSONDecodeError:
            continue
        if evt.get("type") == "current_portfolio":
            payload = evt.get("data") or {}
        elif evt.get("type") == "error":
            error_msg = evt.get("message") or "unknown error"

    if error_msg:
        raise HTTPException(500, error_msg)
    if payload is None:
        raise HTTPException(500, "Compute completed but no portfolio payload was produced")

    # The SSE path already inserted a row with triggered_by='manual'. Replace
    # it with an 'auto' row by inserting fresh + deleting the manual one we
    # just created.
    auto_id = None
    try:
        manual_id = payload.pop("snapshot_id", None)
        auto_id = await asyncio.to_thread(
            _save_current_picks_snapshot,
            payload,
            req.model_dump(),
            "auto",
            payload.get("strategy_hash"),
        )
        if manual_id is not None:
            await asyncio.to_thread(
                lambda: supabase.table("current_picks_snapshot")
                .delete()
                .eq("snapshot_id", manual_id)
                .execute()
            )
    except Exception as e:
        raise HTTPException(500, f"Cron compute succeeded but persist failed: {type(e).__name__}: {e}")

    return {"snapshot_id": auto_id, "as_of_date": payload.get("as_of_date"), "holdings_count": len(payload.get("holdings", []))}
