"""AIRS portfolio scraper + AT&T performance Excel parser.

Endpoints:
    GET  /api/airs/portfolios               portfolios we already have data for (DB-served)
    GET  /api/airs/scan                     SSE: live Playwright scan of AirSPMS
    GET  /api/airs/portfolio/{name}         performance rows (DB cache or fresh download)
    POST /api/portfolios/parse              parse an uploaded AIRS Excel without persisting

`/api/portfolios/parse` is the drag-and-drop path on the frontend; the
other three back the broker-scan flow.
"""

from __future__ import annotations

import asyncio
import io
from routers._sse import sse_event, sse_message
import queue as thread_queue
import threading
from datetime import date as dt_date

import pandas as pd
from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from airs_scanner import download_portfolio_sync, scan_portfolios_sync
from deps import supabase
from portfolio import parse_airs_excel

router = APIRouter(tags=["airs"])


def _save_performance_to_db(portfolio_name: str, rows: list[dict]):
    """Upsert performance rows into the airs_performance table."""
    if not rows:
        return
    for r in rows:
        supabase.table("airs_performance").upsert({
            "portefeuille": portfolio_name,
            "periode": r["periode"],
            "beginvermogen": r["beginvermogen"],
            "koersresultaat": r["koersresultaat"],
            "opbrengsten": r["opbrengsten"],
            "beleggingsresultaat": r["beleggingsresultaat"],
            "eindvermogen": r["eindvermogen"],
            "rendement": r["rendement"],
            "cumulatief_rendement": r["cumulatief_rendement"],
        }, on_conflict="portefeuille,periode").execute()


def _parse_att_excel(content: bytes) -> list[dict]:
    """Parse AT&T Excel bytes into a list of performance row dicts."""
    df = pd.read_excel(io.BytesIO(content), engine="xlrd")
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "periode": str(r.get("Periode", ""))[:10],
            "beginvermogen": round(float(r["Beginvermogen"]), 2) if pd.notna(r.get("Beginvermogen")) else None,
            "koersresultaat": round(float(r["Koersresultaat"]), 2) if pd.notna(r.get("Koersresultaat")) else None,
            "opbrengsten": round(float(r["Opbrengsten"]), 2) if pd.notna(r.get("Opbrengsten")) else None,
            "beleggingsresultaat": round(float(r["Beleggingsresultaat"]), 2) if pd.notna(r.get("Beleggingsresultaat")) else None,
            "eindvermogen": round(float(r["Eindvermogen"]), 2) if pd.notna(r.get("Eindvermogen")) else None,
            "rendement": round(float(r["Rendement"]), 6) if pd.notna(r.get("Rendement")) else None,
            "cumulatief_rendement": round(float(r["Cumulatief rendement"]), 6) if pd.notna(r.get("Cumulatief rendement")) else None,
        })
    return rows


@router.get("/api/airs/portfolios")
async def airs_portfolios_from_db():
    """Portfolios we already have performance data for, with latest YTD."""
    try:
        resp = await asyncio.to_thread(
            lambda: supabase.table("airs_performance")
            .select("portefeuille,cumulatief_rendement,periode,fetched_at")
            .order("portefeuille")
            .order("periode", desc=True)
            .execute()
        )
        # Dedupe to latest row per portfolio.
        seen: dict[str, dict] = {}
        for r in (resp.data or []):
            name = r["portefeuille"]
            if name not in seen:
                seen[name] = {
                    "portefeuille": name,
                    "cumulatief_rendement": r["cumulatief_rendement"],
                    "periode": r["periode"],
                    "fetched_at": r["fetched_at"],
                }
        return list(seen.values())
    except Exception:
        return []


async def _airs_scan_stream():
    q: thread_queue.Queue = thread_queue.Queue()

    def send_event(msg_type: str, **kwargs):
        payload = {"type": msg_type, **kwargs}
        q.put(sse_event(payload))

    def run_scanner():
        try:
            scan_portfolios_sync(send_event)
        except Exception as e:
            q.put(sse_message("error", f"{type(e).__name__}: {e}"))
        finally:
            q.put(None)

    thread = threading.Thread(target=run_scanner, daemon=True)
    thread.start()

    while True:
        item = await asyncio.to_thread(q.get)
        if item is None:
            break
        yield item


@router.get("/api/airs/scan")
async def airs_scan():
    return StreamingResponse(
        _airs_scan_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/api/airs/portfolio/{portfolio_name}")
async def airs_portfolio_download(
    portfolio_name: str,
    datum_van: str | None = None,
    datum_tot: str | None = None,
    refresh: bool = False,
):
    """Return performance data. Serves from DB cache unless refresh=true or no cache."""
    today = dt_date.today()
    if not datum_van:
        datum_van = f"{today.year}-01-01"
    if not datum_tot:
        datum_tot = today.isoformat()

    # Check what we already have in DB.
    db_rows: list[dict] = []
    needs_refresh = True
    try:
        resp = await asyncio.to_thread(
            lambda: supabase.table("airs_performance")
            .select("periode,beginvermogen,koersresultaat,opbrengsten,beleggingsresultaat,eindvermogen,rendement,cumulatief_rendement,fetched_at")
            .eq("portefeuille", portfolio_name)
            .order("periode")
            .execute()
        )
        db_rows = resp.data or []
        if db_rows and not refresh:
            last_fetched = db_rows[-1].get("fetched_at", "")[:10]
            needs_refresh = last_fetched != today.isoformat()
    except Exception:
        pass  # table may not exist yet

    if needs_refresh:
        try:
            content = await asyncio.to_thread(download_portfolio_sync, portfolio_name, datum_van, datum_tot)
            fresh_rows = await asyncio.to_thread(_parse_att_excel, content)
        except Exception as e:
            if db_rows:
                rows = [{k: v for k, v in r.items() if k != "fetched_at"} for r in db_rows]
                return {
                    "portfolio_name": portfolio_name,
                    "datum_van": datum_van,
                    "datum_tot": datum_tot,
                    "rows": rows,
                    "cached": True,
                }
            raise HTTPException(status_code=500, detail=f"Download failed: {e}")

        try:
            await asyncio.to_thread(_save_performance_to_db, portfolio_name, fresh_rows)
        except Exception:
            pass

        try:
            resp = await asyncio.to_thread(
                lambda: supabase.table("airs_performance")
                .select("periode,beginvermogen,koersresultaat,opbrengsten,beleggingsresultaat,eindvermogen,rendement,cumulatief_rendement")
                .eq("portefeuille", portfolio_name)
                .order("periode")
                .execute()
            )
            return {
                "portfolio_name": portfolio_name,
                "datum_van": datum_van,
                "datum_tot": datum_tot,
                "rows": resp.data or fresh_rows,
                "cached": False,
            }
        except Exception:
            return {
                "portfolio_name": portfolio_name,
                "datum_van": datum_van,
                "datum_tot": datum_tot,
                "rows": fresh_rows,
                "cached": False,
            }

    rows = [{k: v for k, v in r.items() if k != "fetched_at"} for r in db_rows]
    return {
        "portfolio_name": portfolio_name,
        "datum_van": datum_van,
        "datum_tot": datum_tot,
        "rows": rows,
        "cached": True,
    }


# ─── Vermogensoverzicht (holdings) — daily scheduled scrape + store ──────────


@router.post("/api/airs/vermogen/refresh")
async def airs_vermogen_refresh():
    """Trigger the per-portfolio Vermogensoverzicht refresh now (the /airs-
    portfolio "Refresh now" button). Re-discovers the live portfolio list, then
    downloads + stores each portfolio's holdings. Runs in a daemon thread and
    returns immediately; poll `/api/airs/vermogen/status` for progress."""
    from airs_vermogen import _STATUS, run_airs_vermogen_refresh_sync  # noqa: PLC0415

    if _STATUS.get("running"):
        return {"status": "busy", "message": "A refresh is already running"}
    threading.Thread(
        target=run_airs_vermogen_refresh_sync,
        kwargs={"triggered_by": "manual"},
        daemon=True,
        name="airs-vermogen-manual",
    ).start()
    return {"status": "started"}


@router.get("/api/airs/vermogen/status")
async def airs_vermogen_status():
    """Status of the Vermogensoverzicht refresh job: in-flight progress, last
    result, the freshest stored snapshot date, and the next scheduled run."""
    from airs_vermogen import get_status  # noqa: PLC0415
    from scheduler import list_scheduled_jobs  # noqa: PLC0415

    status = await asyncio.to_thread(get_status)
    next_run_at = None
    for j in list_scheduled_jobs():
        if j.get("id") == "airs_vermogen_refresh":
            next_run_at = j.get("next_run_at")
            break
    return {**status, "next_run_at": next_run_at}


@router.get("/api/airs/vermogen/{portfolio_name}")
async def airs_vermogen_holdings(portfolio_name: str, as_of: str | None = None):
    """Stored Vermogensoverzicht holdings for a portfolio — the latest snapshot
    by default, or a specific `as_of` date. Returns `{portfolio_name,
    as_of_date, holdings: [...]}` (holdings shaped like `/api/portfolios/parse`)."""
    def _q() -> dict:
        date_q = as_of
        if not date_q:
            latest = (
                supabase.table("airs_holding")
                .select("as_of_date").eq("portefeuille", portfolio_name)
                .order("as_of_date", desc=True).limit(1).execute()
            )
            if not latest.data:
                return {"portfolio_name": portfolio_name, "as_of_date": None, "holdings": []}
            date_q = latest.data[0]["as_of_date"]
        rows = (
            supabase.table("airs_holding")
            .select("holding_name, quantity, currency, weight, start_value_eur, "
                    "current_value_eur, ytd_return_eur, ytd_return_pct, ytd_return_local_pct")
            .eq("portefeuille", portfolio_name).eq("as_of_date", date_q)
            .order("current_value_eur", desc=True).execute()
        ).data or []
        return {"portfolio_name": portfolio_name, "as_of_date": date_q, "holdings": rows}

    return await asyncio.to_thread(_q)


@router.post("/api/portfolios/parse")
async def parse_portfolio(file: UploadFile = File(...)):
    content = await file.read()
    try:
        holdings = await asyncio.to_thread(parse_airs_excel, content)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    total_start = sum(h.start_value_eur for h in holdings if h.start_value_eur is not None)
    total_current = sum(h.current_value_eur for h in holdings if h.current_value_eur is not None)
    total_ytd_eur = round(total_current - total_start, 2) if total_start else None
    total_ytd_pct = round((total_current - total_start) / abs(total_start), 6) if total_start else None

    return {
        "holdings": [
            {
                "holding_name": h.holding_name,
                "quantity": h.quantity,
                "currency": h.currency,
                "weight": h.weight,
                "start_value_eur": h.start_value_eur,
                "current_value_eur": h.current_value_eur,
                "ytd_return_eur": h.ytd_return_eur,
                "ytd_return_pct": h.ytd_return_pct,
                "ytd_return_local_pct": h.ytd_return_local_pct,
            }
            for h in holdings
        ],
        "total_start_eur": round(total_start, 2) if total_start else None,
        "total_current_eur": round(total_current, 2) if total_current else None,
        "total_ytd_eur": total_ytd_eur,
        "total_ytd_pct": total_ytd_pct,
    }
