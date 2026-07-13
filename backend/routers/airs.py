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
from routers import _airs_portfolio_store as store
from routers._sse import sse_event, sse_message
import queue as thread_queue
import threading
from datetime import date as dt_date

import pandas as pd
from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from airs_scanner import (
    download_portfolio_sync,
    count_model_portfolio_holdings_sync,
    fetch_model_portfolios_sync,
    fetch_portfolio_positions_sync,
    scan_portfolios_sync,
)
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


async def _model_portfolios_stream():
    """SSE because it is SLOW and chatty: one request per list page, plus one per row whose
    name the list truncated (the full name lives only on the edit page). ~95 portfolios is
    a couple of minutes of authenticated round-trips, so it streams progress rather than
    hanging a GET."""
    q: thread_queue.Queue = thread_queue.Queue()

    def send_event(msg_type: str, **kwargs):
        q.put(sse_event({"type": msg_type, **kwargs}))

    def run():
        try:
            # Two phases, deliberately. The LIST is fast (~6s) and is emitted as
            # "portfolios" the moment it's ready, so the table renders. Counting each
            # portfolio's holdings is an edit-page GET + an XLS download per row and takes
            # minutes — it streams "count" events into the already-visible table instead of
            # holding the whole thing back for its slowest part.
            #
            # Both phases WRITE as they go, rather than at the end: a scan that dies halfway
            # should leave behind the portfolios it did reach, not nothing.
            rows = fetch_model_portfolios_sync(send_event)
            store.save_portfolios(rows)
            count_model_portfolio_holdings_sync(
                rows, send_event,
                # Counting a portfolio means downloading its XLS — so persisting the
                # positions costs no extra AIRS traffic, and `isin` is the whole prize.
                on_positions=store.save_positions,
                on_error=store.save_positions_error,
            )
            send_event("done", count=len(rows), portfolios=rows)
        except Exception as e:  # noqa: BLE001 — surface it to the client, don't 500 the stream
            q.put(sse_message("error", f"{type(e).__name__}: {e}"))
        finally:
            q.put(None)

    threading.Thread(target=run, daemon=True).start()

    while True:
        item = await asyncio.to_thread(q.get)
        if item is None:
            break
        yield item


@router.get("/api/airs/model-portfolios/scan")
async def airs_model_portfolios_scan():
    """Every model portfolio from Stamgegevens > Onderhoud portefeuilles > Model
    portefeuilles, with its FULL name (the list page truncates them). Persists as it goes."""
    return StreamingResponse(
        _model_portfolios_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


class StoredModelPortfolio(BaseModel):
    """A stored portfolio row. `holdings` is derived by the view from the positions, so it
    cannot drift from them — and it keeps three absences apart that are NOT the same thing:

      has_fixed_model=false   -> NO MODEL EXISTS (a `normaal`/`meervoudig` portfolio). AIRS
                                 stores no composition at all; "0 holdings" would describe a
                                 model that isn't there.
      positions_scanned_at=None -> never counted. Unknown, not zero.
      no_snapshot=true        -> we looked, and AIRS had no DATED composition: its date
                                 dropdown held nothing but the empty "today" placeholder.
                                 Measured on BUS_DUTD_DEF_AFS + EuropaTopSelect OFF FX, both
                                 of which I first mis-reported as "0 holdings".
      holdings=0              -> a real, EMPTY fixed model. Not currently observed on any
                                 portfolio, but expressible — and it must stay distinct from
                                 the three absences above.

    `holdings` counts DISTINCT ISINs: a portfolio can list one instrument on two lines
    (VTopSelectie OFF FX holds CapitaLand at 2% and again at 3%), and that is one instrument.
    """

    id: int
    name: str
    truncated: bool = False
    omschrijving: str | None = None
    portfolio_type: str | None = None
    fixed_datum: str | None = None
    has_fixed_model: bool = False
    no_snapshot: bool = False
    holdings: int | None = None
    positions_datum: str | None = None
    positions_scanned_at: str | None = None
    positions_error: str | None = None
    scanned_at: str | None = None


@router.get("/api/airs/model-portfolios", response_model=list[StoredModelPortfolio])
async def airs_model_portfolios_stored():
    """The stored portfolios — an instant DB read. The page opens on this; `/scan` is the
    explicit refresh, because re-scraping AirSPMS costs minutes."""
    return await asyncio.to_thread(store.load_portfolios)


class ModelPortfolioPerformance(BaseModel):
    """One model portfolio's YTD, in EUR.

    ⚠ `ytd_pct` is a buy-and-hold of the composition WE HOLD, which is the CURRENT one. AIRS
    keeps only 2-3 snapshot dates and no monthly history, so the January composition is not
    recoverable. Read `model_changed_in_period` before trusting the number:

      * false (29 of 56) — the model has held these weights since before Jan 1, so this IS
        what it earned.
      * true  (27 of 56) — the weights are NEWER than the window. Applying them back to Jan 1
        backtests a basket chosen knowing how the year went. Measured: MoTopSelectie_FX shows
        +75.85% YTD on a model defined 8 DAYS AGO — its return since that model took effect
        is +0.86%.

    `since_model_pct` is that honest number: the return since the composition's own effective
    date. It never borrows hindsight, for any portfolio.

    `ytd_pct` is NULL when `low_coverage` — under 60% of the model's weight is priceable, so a
    renormalised return would be an invention (TOPS_OFF_BEH once reported "+0.00%" off its 1%
    cash line while 99% of it, in structured products, was silently dropped).
    """

    portfolio_id: int
    name: str
    model_effective: str | None = None
    model_changed_in_period: bool = False
    ytd_pct: float | None = None
    since_model_pct: float | None = None
    priced_holdings: int = 0
    unpriced_holdings: int = 0
    covered_pct: float | None = None
    low_coverage: bool = False
    partial_coverage: bool = False
    cash_pct: float = 0.0


@router.get("/api/airs/model-portfolios/performance",
            response_model=list[ModelPortfolioPerformance])
async def airs_model_portfolio_performance(year: int | None = None):
    """YTD (EUR) for every stored model portfolio. Read `ModelPortfolioPerformance`'s
    docstring — for half of them the number is a backtest, not a track record."""
    from routers._airs_portfolio_perf import (  # noqa: PLC0415
        compute_portfolio_performance_async,
    )

    return await compute_portfolio_performance_async(year)


class ModelPortfolioPosition(BaseModel):
    """One row of the portfolio's XLS export. `isin` is the point of the whole exercise —
    it is the exact join into `asset_execution`, and it's the identifier the AIRS
    *holdings* sheet never gave us (that one only has a fund NAME)."""

    fonds: str | None = None
    isin: str | None = None            # NULL for the cash line ("Liquiditeiten")
    percentage: float | None = None
    valuta: str | None = None
    categorie: str | None = None
    sector: str | None = None
    regio: str | None = None
    # True when this ISIN is already an instrument in our grid (`asset_execution`).
    known_instrument: bool = False


class ModelPortfolioPositions(BaseModel):
    portfolio: str
    portfolio_id: int
    datum: str | None = None           # the snapshot actually used
    dates: list[str]                   # every snapshot AIRS offers, for a date picker
    rows: list[ModelPortfolioPosition]
    matched: int                       # how many ISINs we already hold
    unmatched: int
    # When this came from OUR cache rather than a live AirSPMS fetch, and when it was taken.
    # The UI says so — a cached answer presented as fresh is how a stale holding gets trusted.
    cached_at: str | None = None


def _shape_positions(raw: dict) -> ModelPortfolioPositions:
    from deps import IN_CHUNK_SIZE  # noqa: PLC0415

    rows = raw["rows"]
    isins = sorted({str(r.get("ISINCode")).strip() for r in rows if r.get("ISINCode")})

    known: set[str] = set()
    for i in range(0, len(isins), IN_CHUNK_SIZE):
        chunk = isins[i:i + IN_CHUNK_SIZE]
        got = (supabase.table("asset_execution").select("isin")
               .in_("isin", chunk).execute().data or [])
        known.update(r["isin"] for r in got)

    out: list[ModelPortfolioPosition] = []
    for r in rows:
        isin = (str(r["ISINCode"]).strip() if r.get("ISINCode") else None) or None
        out.append(ModelPortfolioPosition(
            fonds=(str(r["Fonds"]).strip() if r.get("Fonds") else None),
            isin=isin,
            percentage=(float(r["Percentage"]) if r.get("Percentage") is not None else None),
            valuta=(str(r["valuta"]).strip() if r.get("valuta") else None),
            categorie=(str(r["Beleggingscategorie"]).strip() if r.get("Beleggingscategorie") else None),
            sector=(str(r["Beleggingssector"]).strip() if r.get("Beleggingssector") else None),
            regio=(str(r["regio"]).strip() if r.get("regio") else None),
            known_instrument=bool(isin and isin in known),
        ))

    matched = sum(1 for r in out if r.known_instrument)
    return ModelPortfolioPositions(
        portfolio=raw["portfolio"], portfolio_id=raw["portfolio_id"],
        datum=raw["datum"], dates=raw["dates"], rows=out,
        matched=matched, unmatched=len([r for r in out if r.isin]) - matched,
        cached_at=raw.get("cached_at"),
    )


def _live_positions(portfolio_id: int, datum: str | None) -> dict:
    """Fetch from AIRS and REFRESH THE CACHE with what came back — a live read that left the
    stored copy behind would guarantee the two disagree."""
    raw = fetch_portfolio_positions_sync(portfolio_id, datum=datum)
    # Only the DEFAULT snapshot is cached: we store one composition per portfolio (the newest
    # with rows). Persisting an ad-hoc historical `datum` the user picked would overwrite the
    # current one with an old one — the cache would silently rot backwards.
    if datum is None:
        try:
            store.save_positions(portfolio_id, raw["datum"], raw["rows"], raw.get("dates"))
        except Exception:  # noqa: BLE001 — a cache write must never fail the read
            pass
    return raw


@router.get("/api/airs/model-portfolios/{portfolio_id}/positions",
            response_model=ModelPortfolioPositions)
async def airs_model_portfolio_positions(
    portfolio_id: int, datum: str | None = None, refresh: bool = False,
):
    """One model portfolio's positions — the XLS export that DOES carry an ISIN (the AIRS
    *holdings* sheet does not; it has only a fund name).

    SERVED FROM OUR CACHE by default: the scan already downloaded this XLS to count the
    portfolio's holdings, so re-scraping AirSPMS on every expand is pure waste (and a
    several-second wait on an authenticated round-trip). Goes to AIRS only when:
      * `refresh=true`   — the user explicitly wants the current truth, or
      * `datum` is given — a historical snapshot, of which we cache only the newest, or
      * we have nothing stored for this portfolio yet.

    A cached answer carries `cached_at` and the UI says so. A cached response presented as
    fresh is exactly how a stale holding gets trusted.

    `known_instrument` is NEVER cached — it is a join against `asset_execution`, which grows
    every time we add an instrument, so it is recomputed on every read. Cached, a "not in
    grid" flag would be wrong the moment the grid catches up.
    """
    if not refresh and datum is None:
        cached = await asyncio.to_thread(store.load_positions, portfolio_id)
        if cached is not None:
            return await asyncio.to_thread(_shape_positions, cached)

    raw = await asyncio.to_thread(_live_positions, portfolio_id, datum)
    return await asyncio.to_thread(_shape_positions, raw)


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


@router.get("/api/airs/crm-relaties")
async def airs_crm_relaties():
    """The latest stored CRM 'Alle relaties' export, parsed on the fly from the
    raw .xls in `airs_crm_relaties_raw` into a generic `{columns, rows}` table
    (whatever columns the export has). Empty until the daily job has run it."""
    import base64 as _b64  # noqa: PLC0415

    def _q() -> dict:
        latest = (
            supabase.table("airs_crm_relaties_raw")
            .select("as_of_date, filename, content_base64, byte_size, retrieved_at")
            .order("as_of_date", desc=True).limit(1).execute()
        ).data
        if not latest:
            return {"as_of_date": None, "columns": [], "rows": [], "row_count": 0}
        r = latest[0]
        raw = _b64.b64decode(r["content_base64"])
        try:
            df = pd.read_excel(io.BytesIO(raw), engine="xlrd")  # AIRS exports .xls (BIFF)
        except Exception:
            df = pd.read_excel(io.BytesIO(raw))  # fall back to pandas auto-detect (.xlsx)
        columns = [str(c) for c in df.columns]
        # to_json handles NaN→null, dates→iso, numpy→native; round-trip to plain dicts.
        import json as _json  # noqa: PLC0415
        rows = _json.loads(df.to_json(orient="records", date_format="iso"))
        return {
            "as_of_date": r["as_of_date"],
            "retrieved_at": r.get("retrieved_at"),
            "byte_size": r.get("byte_size"),
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
        }

    try:
        return await asyncio.to_thread(_q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse CRM relaties: {e}")


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
