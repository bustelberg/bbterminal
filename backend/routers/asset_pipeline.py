"""Asset-pipeline prototype endpoints.

  GET  /api/asset-pipeline/resolve?identifier=  — resolve one (no writes).
  POST /api/asset-pipeline/ingest               — batch-resolve + PERSIST a list
                                                   of ISINs (SSE progress).
  GET  /api/asset-pipeline/storage              — live counts + size estimate.

Admin-only via the API auth gate — these paths aren't on the non-admin
allow-list, so only admins reach them."""
from __future__ import annotations

import asyncio
import json
import queue as _queue
import threading

from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from asset_pipeline.resolve import resolve as _resolve
from asset_pipeline.yahoo import YahooThrottled

router = APIRouter(tags=["asset-pipeline"])


class AssetGridRow(BaseModel):
    """One row of the flat per-ISIN grid (from the `asset_grid` view). The
    resolved yahoo_symbol + exchange + currency are 'the info to request
    yfinance'; price_from/to + bars are Yahoo coverage; status is the
    resolution outcome (ok | bond | not_found | error)."""
    execution_id: int
    isin: str
    analysis_id: int | None = None
    yahoo_symbol: str | None = None
    name: str | None = None
    exchange: str | None = None
    currency: str | None = None
    asset_class: str | None = None
    sector: str | None = None
    analysis_symbol: str | None = None
    med_adv_eur: float | None = None
    first_date: str | None = None
    years: float | None = None
    wrapper: str | None = None
    is_leveraged: bool | None = None
    is_default: bool | None = None
    status: str
    reason: str | None = None
    # OpenFIGI identity (between the ISIN and the yfinance columns)
    openfigi_figi: str | None = None
    openfigi_name: str | None = None
    openfigi_ticker: str | None = None
    openfigi_exch: str | None = None
    openfigi_type: str | None = None
    # Yahoo coverage + parquet OHLCV archive pointer
    price_from: str | None = None
    price_to: str | None = None
    bars: int | None = None
    volume_from: str | None = None
    volume_to: str | None = None
    zero_vol_frac: float | None = None
    parquet_path: str | None = None
    parquet_rows: int | None = None
    leonteq_verified: bool = False


class AssetGridResponse(BaseModel):
    rows: list[AssetGridRow]


@router.get("/api/asset-pipeline/resolve")
async def resolve_asset(
    identifier: str = Query(..., min_length=1),
    id_type: str | None = Query(None),
):
    """Resolve one identifier (ISIN or native Yahoo symbol). Returns the ranked
    candidate listings, the chosen ANALYSIS instrument (longest + most-liquid,
    or the underlying for a crypto/commodity ETF wrapper), the rationale,
    oldest/newest candles, an auto label, and the IBKR EXECUTION step (stub).
    Does NOT write to the DB — use /ingest for that."""
    ident = identifier.strip()
    if not ident:
        raise HTTPException(400, "identifier required")
    try:
        return await asyncio.to_thread(_resolve, ident, id_type)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"resolve failed: {type(e).__name__}: {e}")


class _StoreBody(BaseModel):
    identifier: str


@router.post("/api/asset-pipeline/store")
async def store_one(body: _StoreBody):
    """Persist ONE identifier (from the single-ISIN view): resolve → upsert the
    analysis asset + execution → store the analysis series' close+volume. Returns
    what was stored, incl. the exact `stored_fields`."""
    ident = body.identifier.strip()
    if not ident:
        raise HTTPException(400, "identifier required")
    from asset_pipeline import store  # noqa: PLC0415
    try:
        return await asyncio.to_thread(store.store_one, ident)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"store failed: {type(e).__name__}: {e}")


class _IngestBody(BaseModel):
    identifiers: list[str]


class _ExistingBody(BaseModel):
    identifiers: list[str]


@router.post("/api/asset-pipeline/existing")
async def existing(body: _ExistingBody):
    """Fast check: which of these ISINs are ALREADY ingested (status='ok', i.e.
    what a batch would skip)? Returns the matching set + counts so the UI can show
    new-vs-stored the instant a CSV is uploaded.

    Loads the whole already-stored `ok` ISIN set ONCE and intersects (same as the
    ingest skip). The stored set is small + bounded (~thousands), so this is a
    couple of paginated round-trips regardless of how big the uploaded list is —
    cheaper than a chunked `.in_()` that scales with the (large) input."""
    from deps import supabase  # noqa: PLC0415

    ids = {x.strip().upper() for x in body.identifiers if x and x.strip()}

    def _q() -> dict:
        stored: set[str] = set()
        off = 0
        while True:
            r = (
                supabase.table("asset_execution").select("isin")
                .eq("status", "ok").range(off, off + 999).execute()
            )
            batch = r.data or []
            stored.update(x["isin"] for x in batch)
            if len(batch) < 1000:
                break
            off += 1000
        hit = ids & stored
        return {"input": len(ids), "stored": len(hit), "new": len(ids) - len(hit),
                "existing": sorted(hit)}

    return await asyncio.to_thread(_q)


@router.post("/api/asset-pipeline/queue")
async def enqueue(body: _IngestBody):
    """Add ISINs to the async ingest queue as `pending` (instant — no Yahoo).
    A single background worker drains them. Skips already-ingested (ok) ISINs.
    Returns {queued, skipped_existing, input}."""
    from asset_pipeline import queue as _queue  # noqa: PLC0415
    return await asyncio.to_thread(_queue.enqueue, body.identifiers)


@router.get("/api/asset-pipeline/queue/status")
async def queue_status():
    """Queue counts by status (pending / done / failed) — for the UI progress."""
    from asset_pipeline import queue as _queue  # noqa: PLC0415
    return await asyncio.to_thread(_queue.status)


@router.post("/api/asset-pipeline/queue/process")
async def queue_process(limit: int = 40):
    """Manually run ONE worker slice (the scheduler runs this automatically).
    Handy to kick the queue immediately or in envs with the scheduler disabled."""
    from asset_pipeline import queue as _queue  # noqa: PLC0415
    return await asyncio.to_thread(_queue.process_slice, limit)


@router.post("/api/asset-pipeline/queue/requeue-suspects")
async def queue_requeue_suspects():
    """Re-queue wrong-company mis-mapped rows for a clean worker pass (fixes the
    throttle-corrupted re-resolutions)."""
    from asset_pipeline import queue as _queue  # noqa: PLC0415
    return await asyncio.to_thread(_queue.requeue_suspects)


def _frame(obj: dict) -> str:
    return f"data: {json.dumps(obj)}\n\n"


@router.post("/api/asset-pipeline/ingest")
async def ingest(body: _IngestBody):
    """Batch-resolve + PERSIST a list of ISINs (SSE progress). For each: resolve
    → upsert the analysis asset (dedup by symbol) + the execution (by ISIN) →
    store the analysis instrument's daily close+volume. Robust: per-ISIN errors
    are captured, not fatal. Ends with a summary (counts + size estimate) after
    flagging the default execution per asset."""
    # Dedup, preserve order.
    seen: set[str] = set()
    ids = [x.strip() for x in body.identifiers if x and x.strip()]
    ids = [x for x in ids if not (x in seen or seen.add(x))]

    async def gen():
        q: _queue.Queue = _queue.Queue()

        def work():
            try:
                import os  # noqa: PLC0415
                from deps import supabase  # noqa: PLC0415
                from asset_pipeline import openfigi, store  # noqa: PLC0415

                q.put(_frame({"type": "status", "message": "Checking already-stored ISINs…"}))
                # Skip ISINs we already ingested successfully (status='ok') — only
                # ADD the new ones. Paginated (PostgREST caps at 1000/page).
                existing_ok: set[str] = set()
                _off = 0
                while True:
                    _r = (
                        supabase.table("asset_execution").select("isin")
                        .eq("status", "ok").range(_off, _off + 999).execute()
                    )
                    _batch = _r.data or []
                    existing_ok.update(x["isin"] for x in _batch)
                    if len(_batch) < 1000:
                        break
                    _off += 1000
                todo = [x for x in ids if x.upper() not in existing_ok]
                skipped_existing = len(ids) - len(todo)
                total = len(todo)
                # Tell the UI what we're actually doing (skip vs process) right away.
                q.put(_frame({
                    "type": "preflight", "input": len(ids),
                    "skipped_existing": skipped_existing, "total": total,
                }))

                ok = fail = skipped = 0
                assets: set[str] = set()
                # Process in OpenFIGI-batch-sized chunks: fetch figi for the chunk
                # (ONE request), then resolve+store each ISIN, emitting a frame per
                # item — so progress flows immediately instead of blocking on a huge
                # up-front OpenFIGI batch over the whole list.
                figi_chunk = 100 if os.environ.get("OPENFIGI_API_KEY") else 10
                i = 0
                throttled = False
                for _cstart in range(0, total, figi_chunk):
                    if throttled:
                        break
                    chunk = todo[_cstart:_cstart + figi_chunk]
                    try:
                        figi_map = openfigi.lookup_isins(chunk)
                    except Exception:  # noqa: BLE001
                        figi_map = {}
                    for ident in chunk:
                        i += 1
                        q.put(_frame({"type": "current", "i": i, "total": total, "input": ident}))
                        fig = openfigi.extract_columns(figi_map.get(ident.strip().upper(), []))
                        try:
                            res = _resolve(ident, with_candles=False, figi_hint=fig)  # anchor to OpenFIGI identity
                            an = res.get("analysis") or {}
                            if not an.get("symbol"):
                                # No Yahoo series: a bond/gilt or an ISIN nothing could
                                # price. Persist an UNMAPPED grid row (analysis_id NULL)
                                # with its status, then skip cleanly (not a failure).
                                ac = res.get("asset_class")
                                db_status = "bond" if ac == "bond" else "not_found"
                                store.upsert_unmapped(ident, db_status, res.get("reason"), ac, res.get("sector"), figi=fig)
                                skipped += 1
                                q.put(_frame({
                                    "type": "item", "i": i, "total": total, "input": ident,
                                    "status": "skipped", "asset_class": ac or db_status,
                                    "reason": res.get("reason"),
                                }))
                                continue
                            ids_ = store.upsert_asset(res, figi=fig)
                            rows = store.store_series(ids_["analysis_id"], an["symbol"], an.get("first_ts"))
                            assets.add(an["symbol"])
                            ok += 1
                            q.put(_frame({
                                "type": "item", "i": i, "total": total, "input": ident,
                                "status": "ok", "analysis": an.get("symbol"),
                                "execution": (res.get("execution") or {}).get("symbol"),
                                "asset_class": res.get("asset_class"),
                                "leveraged": bool(res.get("is_leveraged")),
                                "wrapper": res.get("wrapper"), "rows": rows,
                            }))
                        except YahooThrottled as e:
                            # Yahoo has banned us past recovery — stop the batch rather
                            # than hammer a throttled endpoint. Partial progress is fine
                            # (upserts are idempotent); re-run resumes + skips these.
                            q.put(_frame({
                                "type": "error",
                                "error": f"Yahoo rate-limited — batch stopped at {i}/{total}. {e}",
                            }))
                            throttled = True
                            break
                        except Exception as e:  # noqa: BLE001
                            fail += 1
                            try:  # record the failure in the grid so the ISIN isn't invisible
                                store.upsert_unmapped(ident, "error", f"{type(e).__name__}: {e}", figi=fig)
                            except Exception:  # noqa: BLE001
                                pass
                            q.put(_frame({
                                "type": "item", "i": i, "total": total, "input": ident,
                                "status": "error", "error": f"{type(e).__name__}: {e}",
                            }))
                defaults = 0
                try:
                    defaults = store.set_default_executions()
                except Exception:  # noqa: BLE001
                    pass
                q.put(_frame({
                    "type": "summary", "processed": total, "ok": ok, "failed": fail,
                    "skipped": skipped, "skipped_existing": skipped_existing,
                    "unique_assets": len(assets), "defaults_set": defaults,
                    **store.storage_summary(),
                }))
            except Exception as e:  # noqa: BLE001
                q.put(_frame({"type": "error", "error": f"{type(e).__name__}: {e}"}))
            finally:
                q.put(None)

        threading.Thread(target=work, daemon=True).start()
        yield ": keepalive\n\n"
        while True:
            item = await asyncio.to_thread(q.get)
            if item is None:
                break
            yield item

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/api/asset-pipeline/assets/{analysis_id}/series")
async def asset_series(analysis_id: int, max_points: int = 20000):
    """The stored daily series for one analysis asset (for the dual chart).
    Paginated to defeat PostgREST's 1000-row cap. `max_points` is a safety cap
    only (stride downsample, first+last kept); the default (20k) is far above any
    realistic series length (~7k daily bars since the 1998 cutoff, ~4k for 7-day
    crypto), so FULL daily resolution is preserved — Lightweight Charts renders
    the whole series fine. It only ever trips for a pathological outlier.

    Each bar carries BOTH the native `close`+`volume` (as Yahoo gives it) and
    the EUR-converted `close_eur`+`volume_eur` — price via the fx_rate table
    (minor units like GBp handled), and volume-in-EUR as turnover
    (price×shares×fx) for equities/ETFs or notional×fx for crypto (per the
    etoro-yfinance methodology). Bars with no FX rate get null *_eur."""
    from deps import supabase  # noqa: PLC0415

    def _q() -> dict:
        arow = (
            supabase.table("asset_analysis").select("currency, asset_class")
            .eq("analysis_id", analysis_id).limit(1).execute().data
        )
        native_ccy = arow[0].get("currency") if arow else None
        asset_class = arow[0].get("asset_class") if arow else None
        rows: list[dict] = []
        offset = 0
        while True:
            r = (
                supabase.table("asset_price")
                .select("target_date, close, volume")
                .eq("analysis_id", analysis_id)
                .order("target_date")
                .range(offset, offset + 999)
                .execute()
            )
            batch = r.data or []
            rows.extend(batch)
            if len(batch) < 1000:
                break
            offset += 1000
        total = len(rows)
        if max_points and total > max_points:
            stride = -(-total // max_points)  # ceil
            sampled = rows[::stride]
            if sampled and sampled[-1] is not rows[-1]:
                sampled.append(rows[-1])  # always keep the latest bar
            rows = sampled
        series = [
            {"date": str(x["target_date"])[:10], "close": x.get("close"), "volume": x.get("volume")}
            for x in rows
        ]
        from asset_pipeline.fx import to_eur_series  # noqa: PLC0415
        series = to_eur_series(series, native_ccy, asset_class)  # adds close_eur + volume_eur
        return {
            "analysis_id": analysis_id, "total": total, "points": len(series),
            "native_currency": native_ccy, "asset_class": asset_class, "series": series,
        }

    return await asyncio.to_thread(_q)


@router.get("/api/asset-pipeline/assets/{analysis_id}/parquet")
async def asset_parquet(analysis_id: int):
    """A short-lived signed download URL for the asset's full-OHLCV parquet
    archive (date/open/high/low/close/adj_close/volume/dividends/splits). 404
    when the asset has no stored parquet yet."""
    from asset_pipeline import parquet  # noqa: PLC0415
    from deps import supabase  # noqa: PLC0415

    def _q() -> dict:
        row = (
            supabase.table("asset_analysis").select("parquet_path, symbol")
            .eq("analysis_id", analysis_id).limit(1).execute().data
        )
        path = row[0].get("parquet_path") if row else None
        if not path:
            raise HTTPException(404, "no parquet stored for this asset")
        url = parquet.signed_url(path)
        if not url:
            raise HTTPException(502, "could not sign parquet URL")
        return {"analysis_id": analysis_id, "path": path, "url": url}

    return await asyncio.to_thread(_q)


@router.post("/api/asset-pipeline/upload/scan")
async def upload_scan(file: UploadFile = File(...)):
    """Parse an uploaded CSV or Excel file and, per column, list the VALID ISINs
    it contains (structure + check-digit) — so the frontend can show columns with
    their ISIN counts and let the user pick which one to enqueue. Columns are
    returned most-ISINs-first. No DB writes — the picked column's ISINs go through
    the normal /queue enqueue."""
    raw = await file.read()
    name = (file.filename or "").lower()

    def _parse() -> dict:
        import io  # noqa: PLC0415

        import pandas as pd  # noqa: PLC0415

        from asset_pipeline.isin_util import extract_isins  # noqa: PLC0415
        if name.endswith((".xlsx", ".xlsm")):
            df = pd.read_excel(io.BytesIO(raw), dtype=str, engine="openpyxl")
        elif name.endswith(".xls"):
            df = pd.read_excel(io.BytesIO(raw), dtype=str, engine="xlrd")
        else:  # csv / tsv / txt — pick the delimiter from a SAFE set (never a
            # digit/letter, so ISINs aren't split); default "," for a 1-col list.
            head = raw.decode("utf-8-sig", errors="replace").lstrip().splitlines()
            first = head[0] if head else ""
            delim = next((d for d in (",", ";", "\t", "|") if d in first), ",")
            df = pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False,
                             sep=delim, engine="python")
        df = df.fillna("")
        cols = []
        for c in df.columns:
            seen: dict[str, None] = {}
            raw_vals: dict[str, None] = {}
            # include the header text itself — a header-less file makes the first
            # ISIN the column name, so this recovers it.
            for val in [str(c), *df[c].astype(str).tolist()]:
                v = val.strip()
                if v and v.lower() != "nan":
                    raw_vals.setdefault(v, None)
                for isin in extract_isins(val):
                    seen.setdefault(isin, None)
            # `values` = distinct raw cell values (for an identifier column that
            # mixes ISINs + Yahoo symbols like BTC-USD, which the ISIN extractor
            # would drop). Capped to bound the payload.
            cols.append({
                "name": str(c),
                "count": len(seen),
                "isins": list(seen.keys()),
                "values_count": len(raw_vals),
                "values": list(raw_vals.keys())[:30000],
            })
        cols.sort(key=lambda x: x["count"], reverse=True)
        return {"filename": file.filename, "rows": int(len(df)), "columns": cols}

    try:
        return await asyncio.to_thread(_parse)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"Could not parse file: {type(e).__name__}: {e}")


@router.get("/api/asset-pipeline/storage")
async def storage():
    """Live row counts + rough on-disk estimate for the asset-pipeline tables."""
    from asset_pipeline import store  # noqa: PLC0415
    return await asyncio.to_thread(store.storage_summary)


_alphalab_cache: dict[tuple, tuple[float, dict]] = {}


@router.get("/api/asset-pipeline/alphalab")
async def alphalab(
    min_adv_eur: float = Query(1_000_000.0, ge=0),
    require_sector: bool = True,
    asset_class: str = Query("equity"),
    max_assets: int = Query(600, ge=20, le=2500),
    preview: bool = False,
    refresh: bool = False,
):
    """AlphaLab IC scoreboard over a DEFINED universe of analysis instruments —
    filtered by a liquidity floor (`min_adv_eur`), sector presence, and asset
    class, capped at `max_assets` most-liquid. Each signal's cross-sectional
    Information Coefficient (rank-corr vs next-month return) + t-stat / hit rate /
    quintile spread. `preview=true` returns just the universe (size + sector
    breakdown), cheaply. Cached ~30 min per filter-set (`refresh=true` recomputes)."""
    import time  # noqa: PLC0415

    from asset_pipeline import alphalab as _al  # noqa: PLC0415
    ac = asset_class or None
    key = (min_adv_eur, require_sector, ac, int(max_assets), bool(preview))
    hit = _alphalab_cache.get(key)
    if hit and not refresh and (time.time() - hit[0] < 1800):
        return hit[1]
    res = await asyncio.to_thread(
        _al.compute_scoreboard, min_adv_eur, require_sector, ac, max_assets, preview,
    )
    _alphalab_cache[key] = (time.time(), res)
    return res


@router.get("/api/asset-pipeline/grid", response_model=AssetGridResponse)
async def grid():
    """The flat one-row-per-ISIN grid (from the `asset_grid` view): every input
    ISIN — mapped OR unmapped — with the resolved yfinance-request fields
    (symbol/exchange/currency), history + liquidity, Yahoo price coverage (span +
    bar count), and its resolution status. Offset-paginated to beat PostgREST's
    1000-row cap. Read-only."""
    from deps import supabase  # noqa: PLC0415

    def _q() -> dict:
        rows: list[dict] = []
        offset = 0
        while True:
            r = (
                supabase.table("asset_grid")
                .select("*")
                # Order by the append-only PK, not isin: offset pagination stays
                # stable while a batch is concurrently INSERTING rows (new rows get
                # higher execution_ids → appended at the end, so earlier pages don't
                # shift and duplicate). The frontend re-sorts for display anyway.
                .order("execution_id")
                .range(offset, offset + 999)
                .execute()
            )
            batch = r.data or []
            rows.extend(batch)
            if len(batch) < 1000:
                break
            offset += 1000
        return {"rows": rows}

    return await asyncio.to_thread(_q)


@router.get("/api/asset-pipeline/assets")
async def list_assets():
    """Browse what's stored: every analysis asset (from asset_catalog — execution
    count + price coverage) with its execution instruments (many-to-one) nested,
    most-liquid first. One round-trip for the catalog + one for executions."""
    from deps import supabase  # noqa: PLC0415

    def _q() -> dict:
        cat = (
            supabase.table("asset_catalog").select("*").order("symbol").execute().data or []
        )
        execs = (
            supabase.table("asset_execution")
            .select(
                "execution_id, analysis_id, isin, yahoo_symbol, name, exchange, "
                "currency, med_adv_eur, first_date, years, wrapper, is_leveraged, is_default"
            )
            .execute()
            .data or []
        )
        by: dict[int, list[dict]] = {}
        for e in execs:
            by.setdefault(e["analysis_id"], []).append(e)
        for a in cat:
            a["executions_list"] = sorted(
                by.get(a["analysis_id"], []), key=lambda x: -(x.get("med_adv_eur") or 0)
            )
        return {"assets": cat}

    return await asyncio.to_thread(_q)
