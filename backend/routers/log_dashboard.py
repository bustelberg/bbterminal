"""Shared investment-committee log and GuruFocus news feed."""
from __future__ import annotations

import asyncio
import json
import os
from datetime import date, datetime, timezone
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from deps import IN_CHUNK_SIZE, supabase
from ingest._gurufocus_http import cf_get

router = APIRouter(tags=["log dashboard"])


class LogEntryIn(BaseModel):
    company_name: str = Field(min_length=1, max_length=200)
    isin: str = Field(default="", max_length=20)
    decision: str = Field(min_length=1, max_length=100)
    notes: str = Field(default="", max_length=8000)
    conviction: int = Field(ge=1, le=5)
    portfolio_weight: float | None = Field(default=None, ge=0, le=100)
    review_on: date | None = None


def _entries() -> list[dict]:
    entries = (supabase.table("bc_log_entry").select("*")
               .order("meeting_date", desc=True).order("id", desc=True).execute().data or [])
    isins = sorted({entry["isin"] for entry in entries if entry.get("isin")})
    symbols = {}
    for offset in range(0, len(isins), IN_CHUNK_SIZE):
        rows = (supabase.table("asset_execution").select("isin,yahoo_symbol")
                .in_("isin", isins[offset:offset + IN_CHUNK_SIZE]).execute().data or [])
        symbols.update({row["isin"]: row.get("yahoo_symbol") for row in rows})
    return [{**entry, "yahoo_symbol": symbols.get(entry.get("isin"))} for entry in entries]


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


@router.put("/api/log-dashboard/entries/{entry_id}")
async def update_entry(entry_id: int, body: LogEntryIn):
    """Update a recorded decision without changing its original meeting date."""
    def update() -> dict:
        # A missing optional field must never erase a value saved by a previous version of the
        # client. `exclude_unset` still preserves an explicit `null`, so a user who clears the
        # weight in the edit form can deliberately remove it.
        payload = body.model_dump(mode="json", exclude_unset=True)
        try:
            result = (supabase.table("bc_log_entry").update(payload)
                      .eq("id", entry_id).execute().data or [])
        except Exception as exc:
            raise HTTPException(500, f"Could not update log entry: {exc}") from exc
        if not result:
            raise HTTPException(404, "Log entry not found")
        return result[0]
    return await asyncio.to_thread(update)


@router.delete("/api/log-dashboard/entries/{entry_id}")
async def delete_entry(entry_id: int):
    def delete() -> None:
        result = (supabase.table("bc_log_entry").delete()
                  .eq("id", entry_id).execute())
        if not result.data:
            raise HTTPException(404, "Log entry not found")
    await asyncio.to_thread(delete)
    return {"deleted": True}


@router.post("/api/log-dashboard/entries/{entry_id}/refresh-price")
async def refresh_entry_price(entry_id: int):
    """Fetch prices for one saved decision, preserving an existing Yahoo mapping."""
    def refresh() -> dict:
        from asset_pipeline import price_refresh, store  # noqa: PLC0415

        rows = (supabase.table("bc_log_entry").select("*")
                .eq("id", entry_id).limit(1).execute().data or [])
        if not rows:
            raise HTTPException(404, "Log entry not found")
        entry = rows[0]
        isin = str(entry.get("isin") or "").strip().upper()
        if not isin:
            return {"available": False, "message": "Decision saved. No ISIN supplied; Yahoo prices cannot be fetched."}
        executions = (supabase.table("asset_execution").select("analysis_id,yahoo_symbol")
                      .eq("isin", isin).limit(1).execute().data or [])
        if not executions:
            store.store_one(isin)
        elif not executions[0].get("analysis_id") or not executions[0].get("yahoo_symbol"):
            return {"available": False, "message": "Decision saved. No Yahoo price series is linked to this ISIN."}
        backfill = price_refresh.backfill_missing({isin})
        prices = price_refresh.refresh_stale(held_only=False, stale_days=0, isins={isin})
        if backfill.get("failed") or prices.get("failed"):
            raise HTTPException(502, "Decision saved, but fetching Yahoo prices failed. Retry using Refresh Yahoo returns.")
        return {"available": True, "message": "Decision saved. Yahoo prices fetched."}

    return await asyncio.to_thread(refresh)


def _yahoo_return_since(isin: str, start: date) -> dict:
    """Calculate a return from the review-date close through the latest stored close."""
    if start > date.today():
        return {
            "available": False,
            "message": (
                f"The review date {start.isoformat()} is in the future; "
                "a Yahoo return cannot be calculated yet."
            ),
        }
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
    if not first or not latest or first[0].get("close") is None or latest[0].get("close") is None:
        return {"available": False,
                "message": f"No Yahoo close is stored on or after the review date {start.isoformat()}."}
    start_row, end_row = first[0], latest[0]
    start_close, end_close = float(start_row["close"]), float(end_row["close"])
    if start_close <= 0:
        return {"available": False, "message": "The review-date Yahoo close is invalid."}
    return {
        "available": True,
        "return_pct": round((end_close / start_close - 1) * 100, 2),
        "start_date": start_row["target_date"], "start_close": start_close,
        "as_of": end_row["target_date"], "end_close": end_close,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/api/log-dashboard/yahoo-return/{isin}")
async def yahoo_return_since(isin: str, since: str = Query(...)):
    """Stored Yahoo price return from the review-date close through the latest close.

    Uses the asset pipeline's persisted Yahoo series, so opening a log entry
    never silently makes a fresh vendor request.
    """
    try:
        start = date.fromisoformat(since)
    except ValueError as exc:
        raise HTTPException(422, "since must be an ISO date") from exc
    return await asyncio.to_thread(_yahoo_return_since, isin, start)


def _refresh_logbook_job(ctx) -> str:
    from asset_pipeline import price_refresh  # noqa: PLC0415
    from jobs import UserFacingJobError  # noqa: PLC0415

    ctx.emit("progress", "Loading logbook decisions...", done=0, total=0)
    entries = _entries()
    total = len(entries)
    available = skipped = failed = 0
    refreshed: dict[str, str | None] = {}
    ctx.progress(0, total, f"Found {total} decisions; checking ISINs and Yahoo mappings.")
    for index, entry in enumerate(entries):
        ctx.check()
        name = entry.get("company_name") or entry.get("isin") or "Unnamed instrument"
        isin = str(entry.get("isin") or "").strip().upper()
        prefix = f"[{index + 1}/{total}] {name}"
        def report(message):
            ctx.emit("progress", f"{prefix}: {message}", done=index, total=total)
        try:
            if not isin:
                skipped += 1
                ctx.progress(index + 1, total, f"{prefix}: skipped - no ISIN.")
                continue
            if isin not in refreshed:
                report(f"Checking Yahoo mapping for {isin}...")
                mappings = (supabase.table("asset_execution").select("analysis_id,yahoo_symbol")
                            .eq("isin", isin).limit(1).execute().data or [])
                if not mappings or not mappings[0].get("analysis_id") or not mappings[0].get("yahoo_symbol"):
                    refreshed[isin] = "no Yahoo mapping; link this ISIN in the asset pipeline first"
                else:
                    report(f"{mappings[0]['yahoo_symbol']}: checking and fetching missing price history...")
                    backfill = price_refresh.backfill_missing({isin})
                    ctx.check()
                    report(f"{mappings[0]['yahoo_symbol']}: fetching latest Yahoo prices...")
                    prices = price_refresh.refresh_stale(held_only=False, stale_days=0, isins={isin}, on_progress=report)
                    if backfill.get("failed") or prices.get("failed") or prices.get("skipped") or backfill.get("skipped"):
                        raise RuntimeError("Yahoo price fetch failed or was deferred; please retry")
                    refreshed[isin] = None
            if refreshed[isin]:
                skipped += 1
                outcome = f"unavailable - {refreshed[isin]}."
            elif not entry.get("review_on"):
                skipped += 1
                outcome = "return unavailable - no review date."
            else:
                report("Reading latest stored Yahoo close...")
                result = _yahoo_return_since(isin, date.fromisoformat(str(entry["review_on"])[:10]))
                if result.get("available"):
                    available += 1
                    outcome = f"{result['return_pct']:+g}% from {result['start_date']} to {result['as_of']}."
                else:
                    skipped += 1
                    outcome = f"return unavailable - {result.get('message', 'no prices available')}"
            # A name is not a stable UI key (two decisions may name the same company), while the
            # log-entry id identifies exactly which visible Yahoo-return cell can now repaint.
            # It rides only on this completed-entry event; the intermediate fetch lines remain
            # narration for the job receipt.
            ctx.progress(index + 1, total, f"{prefix}: {outcome}", entry_id=int(entry["id"]))
        except Exception as exc:
            from jobs import JobCancelled  # noqa: PLC0415
            if isinstance(exc, JobCancelled):
                raise
            failed += 1
            ctx.progress(index + 1, total, f"{prefix}: FAILED - {exc}")
    summary = f"{available} returns available; {skipped} unavailable/skipped; {failed} failed. {total} decisions checked."
    if failed:
        raise UserFacingJobError(summary)
    return summary


@router.post("/api/log-dashboard/yahoo-returns/refresh/job")
async def refresh_yahoo_returns_job():
    from jobs import start  # noqa: PLC0415
    job, reused = start("log-yahoo-returns", "BC logbook", _refresh_logbook_job)
    return {"job_id": job.id, "already_running": reused}


@router.post("/api/log-dashboard/yahoo-returns/refresh")
async def refresh_yahoo_returns():
    """Refresh each dated log entry's Yahoo series, then return all recalculated returns.

    This is deliberately an explicit batch action. Rendering the log only reads
    stored closes; a page view must never cause a burst of vendor requests.
    """
    def _refresh() -> dict:
        from asset_pipeline.price_refresh import backfill_missing, refresh_stale  # noqa: PLC0415

        dated = [entry for entry in _entries()
                 if entry.get("review_on") and str(entry.get("isin") or "").strip()]
        isins = {str(entry.get("isin") or "").strip().upper() for entry in dated}
        isins.discard("")
        # An already-resolved entry can legitimately have no historical rows
        # yet; backfill it before the incremental update below.
        backfill = backfill_missing(isins) if isins else {"missing": 0, "backfilled": 0, "failed": 0}
        prices = (refresh_stale(held_only=False, stale_days=0, isins=isins)
                  if isins else {"considered": 0, "moved": 0, "unchanged": 0, "failed": 0})
        returns = {
            str(entry["id"]): _yahoo_return_since(
                str(entry["isin"]), date.fromisoformat(str(entry["review_on"])[:10]))
            for entry in dated
        }
        return {"entries": len(dated), "prices": prices, "backfill": backfill, "returns": returns}

    return await asyncio.to_thread(_refresh)


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
