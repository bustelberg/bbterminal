"""Shared investment-committee log and GuruFocus news feed."""
from __future__ import annotations

import asyncio
import json
import os
from datetime import date, datetime, timezone
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from deps import supabase
from ingest._gurufocus_http import cf_get

router = APIRouter(tags=["log dashboard"])


class LogEntryIn(BaseModel):
    company_name: str = Field(min_length=1, max_length=200)
    isin: str = Field(min_length=1, max_length=20)
    decision: str = Field(min_length=1, max_length=100)
    notes: str = Field(default="", max_length=8000)
    conviction: int = Field(ge=1, le=5)
    portfolio_weight: float | None = Field(default=None, ge=0, le=100)
    review_on: date | None = None


def _entries() -> list[dict]:
    return (supabase.table("bc_log_entry").select("*")
            .order("meeting_date", desc=True).order("id", desc=True).execute().data or [])


@router.get("/api/log-dashboard/entries")
async def get_entries():
    return await asyncio.to_thread(_entries)


@router.post("/api/log-dashboard/entries")
async def create_entry(body: LogEntryIn):
    def create() -> dict:
        payload = body.model_dump(mode="json")
        try:
            result = supabase.table("bc_log_entry").insert(payload).execute().data or []
        except Exception as exc:
            raise HTTPException(500, f"Could not save log entry: {exc}") from exc
        if not result:
            raise HTTPException(500, "Could not save log entry")
        return result[0]
    return await asyncio.to_thread(create)


@router.delete("/api/log-dashboard/entries/{entry_id}")
async def delete_entry(entry_id: int):
    def delete() -> None:
        result = (supabase.table("bc_log_entry").delete()
                  .eq("id", entry_id).execute())
        if not result.data:
            raise HTTPException(404, "Log entry not found")
    await asyncio.to_thread(delete)
    return {"deleted": True}


@router.get("/api/log-dashboard/yahoo-return/{isin}")
async def yahoo_return_since(isin: str, since: str = Query(...)):
    """Stored Yahoo close-price return from the first close on/after ``since``.

    Uses the asset pipeline's persisted Yahoo series, so opening a log entry
    never silently makes a fresh vendor request. This is price return only;
    dividends are intentionally not implied to be included.
    """
    try:
        start = date.fromisoformat(since)
    except ValueError as exc:
        raise HTTPException(422, "since must be an ISO date") from exc

    def _return() -> dict:
        execution = (supabase.table("asset_execution").select("analysis_id")
                     .eq("isin", isin.strip().upper()).limit(1).execute().data or [])
        if not execution or not execution[0].get("analysis_id"):
            return {"available": False, "message": "No Yahoo price series is linked to this ISIN."}
        aid = execution[0]["analysis_id"]
        first = (supabase.table("asset_price").select("target_date,close")
                 .eq("analysis_id", aid).gte("target_date", start.isoformat())
                 .not_.is_("close", "null").order("target_date").limit(1).execute().data or [])
        latest = (supabase.table("asset_price").select("target_date,close")
                  .eq("analysis_id", aid).not_.is_("close", "null")
                  .order("target_date", desc=True).limit(1).execute().data or [])
        if not first or not latest or not first[0].get("close"):
            return {"available": False, "message": "No Yahoo close is available from that date."}
        start_row, end_row = first[0], latest[0]
        start_close, end_close = float(start_row["close"]), float(end_row["close"])
        if start_close <= 0:
            return {"available": False, "message": "The starting Yahoo close is invalid."}
        return {
            "available": True,
            "return_pct": round((end_close / start_close - 1) * 100, 2),
            "start_date": start_row["target_date"], "start_close": start_close,
            "as_of": end_row["target_date"], "end_close": end_close,
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
        }

    return await asyncio.to_thread(_return)


@router.get("/api/log-dashboard/news/{ticker}")
async def stock_news(ticker: str):
    """Latest headlines from the legacy GuruFocus API used by this app."""
    symbol = ticker.strip().upper()
    if not symbol or any(c not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ.-" for c in symbol):
        raise HTTPException(422, "A valid ticker is required")
    base_url = os.environ.get("GURUFOCUS_BASE_URL", "").strip().rstrip("/")
    key = os.environ.get("GURUFOCUS_API_KEY", "").strip()
    if not base_url or not key:
        return {"ticker": symbol, "articles": [],
                "message": "GuruFocus is not configured in this backend."}
    url = (f"{base_url}/public/user/{key}/stock/news_feed"
           f"?symbol={quote(symbol, safe=':.')}")
    response = await asyncio.to_thread(cf_get, url, {"Accept": "application/json"}, 30)
    if not response.ok:
        if response.status_code in (401, 403):
            return {"ticker": symbol, "articles": [],
                    "message": "GuruFocus did not accept the configured API token."}
        raise HTTPException(502, "GuruFocus news could not be retrieved")
    try:
        payload = json.loads(response.text)
    except json.JSONDecodeError as exc:
        raise HTTPException(502, "GuruFocus news returned an invalid response") from exc
    rows = payload.get("data", payload) if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        rows = []
    return {"ticker": symbol, "articles": [
        {
            "id": item.get("url") or item.get("headline"),
            "subject": item.get("headline") or item.get("subject"),
            "subtitle": item.get("subtitle") or "",
            "publish_time": item.get("date") or item.get("publish_time"),
            "link": item.get("url") or item.get("link"),
        }
        for item in rows if isinstance(item, dict)
    ]}
