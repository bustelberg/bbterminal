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

from ._asset_dividends import router as _dividends_router

router = APIRouter(tags=["asset-pipeline"])
# GuruFocus dividends for the grid's rightmost column. Kept in its own module
# because it is the ONLY place the Yahoo asset universe is bridged to the
# GuruFocus company universe (by ISIN) — see its docstring for the coverage math.
router.include_router(_dividends_router)


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
    short_multiplier: int | None = None  # -1x/-2x/-3x for a "Short …" sector
    analysis_symbol: str | None = None
    # Geography (see asset_pipeline/geo.py). `listing_country` is THIS execution's
    # venue; `domicile_country` is the issuer's HQ (NULL for every ETF/crypto —
    # they have no Yahoo assetProfile). `country` = domicile, else listing.
    # `continent` is geographic, `msci_region` financial — they diverge on
    # purpose (Israel: Asia / Europe).
    listing_country: str | None = None
    domicile_country: str | None = None
    country: str | None = None
    continent: str | None = None
    msci_region: str | None = None
    med_adv_eur: float | None = None
    market_cap_eur: float | None = None
    market_cap_currency: str | None = None
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
    # OpenFIGI confirmation verdict: verified | mismatch | unknown
    identity_status: str | None = None
    # Yahoo coverage + parquet OHLCV archive pointer
    price_from: str | None = None
    price_to: str | None = None
    bars: int | None = None
    volume_from: str | None = None
    volume_to: str | None = None
    zero_vol_frac: float | None = None
    parquet_path: str | None = None
    parquet_rows: int | None = None
    # Leonteq (lynqs) list metadata — present only for a Leonteq-Verified row
    leonteq_name: str | None = None
    leonteq_currency: str | None = None
    leonteq_product_type: str | None = None
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


class _RowRefreshBody(BaseModel):
    identifier: str


@router.post("/api/asset-pipeline/rows/refresh")
async def refresh_row(body: _RowRefreshBody):
    """Fetch OpenFIGI + yfinance for ONE row and persist. Returns a per-source
    outcome ({openfigi:{found,…}, yfinance:{found,…}, identity_status, status})
    so the UI can show what got filled vs. what's missing. Never 502s on a plain
    'not found' — that's a `found:false` result, not an error."""
    ident = body.identifier.strip()
    if not ident:
        raise HTTPException(400, "identifier required")
    from asset_pipeline import store  # noqa: PLC0415
    try:
        return await asyncio.to_thread(store.refresh_row, ident)
    except YahooThrottled as e:
        raise HTTPException(429, f"Yahoo rate-limited — try again shortly. {e}")
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"refresh failed: {type(e).__name__}: {e}")


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
    (price×shares×fx) for equities/ETFs or notional×fx for crypto. Bars with
    no FX rate get null *_eur."""
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


@router.post("/api/asset-pipeline/leonteq/upload")
async def leonteq_upload(file: UploadFile = File(...), enqueue: bool = Query(True)):
    """Upload a Leonteq (lynqs) CSV/Excel — columns id, ticker, name, productType,
    ric, isin, currency. REPLACES the Leonteq-Verified set with the file's valid
    ISINs + their name/currency/productType (so the grid badges + surfaces them),
    and (unless `enqueue=false`) queues those ISINs for the background ingest so
    unseen ones get resolved + priced. Returns row/member/queue counts."""
    raw = await file.read()
    fname = (file.filename or "").lower()

    def _work() -> dict:
        import io  # noqa: PLC0415

        import pandas as pd  # noqa: PLC0415

        from asset_pipeline import leonteq as _lt  # noqa: PLC0415
        from asset_pipeline import queue as _q  # noqa: PLC0415
        from asset_pipeline.isin_util import is_valid_isin  # noqa: PLC0415

        if fname.endswith((".xlsx", ".xlsm")):
            df = pd.read_excel(io.BytesIO(raw), dtype=str, engine="openpyxl")
        elif fname.endswith(".xls"):
            df = pd.read_excel(io.BytesIO(raw), dtype=str, engine="xlrd")
        else:  # csv / tsv — delimiter from a SAFE set so ISINs aren't split
            head = raw.decode("utf-8-sig", errors="replace").lstrip().splitlines()
            first = head[0] if head else ""
            delim = next((d for d in (",", ";", "\t", "|") if d in first), ",")
            df = pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False,
                             sep=delim, engine="python")
        df = df.fillna("")
        colmap = {str(c).strip().lower(): c for c in df.columns}

        def _col(*names: str) -> str | None:
            for n in names:
                if n in colmap:
                    return colmap[n]
            return None

        isin_c = _col("isin")
        if isin_c is None:
            raise ValueError("no 'isin' column in the file")
        name_c = _col("name")
        ccy_c = _col("currency", "ccy")
        pt_c = _col("producttype", "product_type", "type")

        members: list[dict] = []
        isins: list[str] = []
        for _, row in df.iterrows():
            isin = str(row[isin_c]).strip().upper()
            if not is_valid_isin(isin):
                continue
            members.append({
                "identifier": isin,
                "name": str(row[name_c]).strip() if name_c else None,
                "currency": str(row[ccy_c]).strip() if ccy_c else None,
                "product_type": str(row[pt_c]).strip() if pt_c else None,
            })
            isins.append(isin)

        res = _lt.replace_universe(members)
        # Seed a placeholder grid row per instrument so the WHOLE universe shows
        # immediately (badged + name/ccy/productType), then enqueue for the worker
        # to enrich each with yfinance/OpenFIGI in the background.
        seeded = _lt.seed_execution_placeholders(isins)
        q = _q.enqueue(isins) if enqueue else {"queued": 0, "skipped_existing": 0, "input": 0}
        return {"filename": file.filename, "rows": int(len(df)),
                "valid_isins": len(isins), "seeded": seeded, **res, "queue": q}

    try:
        return await asyncio.to_thread(_work)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"Could not process Leonteq file: {type(e).__name__}: {e}")


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


_signal_lab_cache: dict[tuple, tuple[float, dict]] = {}


@router.get("/api/asset-pipeline/signal-lab")
async def signal_lab(
    min_adv_eur: float = Query(1_000_000.0, ge=0),
    require_sector: bool = True,
    asset_class: str = Query("equity"),
    max_assets: int = Query(600, ge=20, le=2500),
    universe_id: int | None = Query(None, description="use a SAVED universe's members"),
    start: str | None = Query(None, description="evaluate IC from this month (train/test split)"),
    end: str | None = Query(None, description="evaluate IC to this month"),
    include_daily: bool = Query(
        False,
        description="also score the daily as-of signals /schedule trades (keys "
                    "prefixed `daily.`). Off by default: `evaluate_panel` loops "
                    "per entity, so on the 4,006-name 'liquid' universe it takes "
                    "the call from ~31s to ~110s.",
    ),
    refresh: bool = False,
):
    """Signal Lab — predictive-power research over the unified price/volume signal
    panel. Per signal: cross-sectional rank IC vs next-month return, t-stat, hit
    rate, quintile spread, decile monotonicity, PER-SECTOR + PER-REGIME IC, and the
    monthly IC series. `start`/`end` = the train/test evaluation window. Pure
    research (no portfolio). Cached ~30 min.

    Each row carries a `cadence`: `month_end` for the lab's own battery, or
    `daily_asof` for the signals the live /schedule strategy trades. Daily rows are
    keyed by registry key (`daily.mom_12_1`) because the bare names collide and are
    NOT the same measure — see `signal_engine.registry.PARITY`."""
    import time  # noqa: PLC0415

    from asset_pipeline import alphalab as _al  # noqa: PLC0415
    ac = asset_class or None
    # `include_daily` MUST be in the key: it changes the payload, so a `false`
    # call would otherwise serve a truncated response to a `true` one.
    key = (min_adv_eur, require_sector, ac, int(max_assets), universe_id, start, end, include_daily)
    hit = _signal_lab_cache.get(key)
    if hit and not refresh and (time.time() - hit[0] < 1800):
        return hit[1]
    res = await asyncio.to_thread(
        _al.compute_signal_lab, min_adv_eur, require_sector, ac, max_assets, universe_id,
        start, end, include_daily,
    )
    _signal_lab_cache[key] = (time.time(), res)
    return res


_regime_cache: dict[tuple, tuple[float, dict]] = {}


@router.get("/api/asset-pipeline/alphalab/regime")
async def alphalab_regime(
    min_adv_eur: float = Query(1_000_000.0, ge=0),
    require_sector: bool = True,
    asset_class: str = Query("equity"),
    max_assets: int = Query(600, ge=20, le=2500),
    start: str | None = Query(None),
    end: str | None = Query(None),
    universe_id: int | None = Query(None, description="use a SAVED universe's members instead of the ADV/sector filters"),
    exclude_sectors: str | None = Query(None, description="comma-separated sectors to drop from the benchmark (e.g. 'commodity')"),
    refresh: bool = False,
):
    """Bull/bear × calm/turbulent regime timeline of the equal-weight index, over
    either the ADV/sector filters OR a saved `universe_id`'s members. bull = index ≥
    its trailing 200-day mean; turbulent = 63-day vol above the median of its own
    prior history. `exclude_sectors` drops weak sectors from the benchmark index.
    Returns daily {dates, index, ma200, bull[], turb[], current}. The frontend
    rebases the index to 100 at the window start. Cached ~30 min."""
    import time  # noqa: PLC0415

    from asset_pipeline import alphalab as _al  # noqa: PLC0415
    ac = asset_class or None
    excl = [s for s in (exclude_sectors or "").split(",") if s.strip()]
    key = (min_adv_eur, require_sector, ac, int(max_assets), start, end, universe_id, tuple(sorted(excl)))
    hit = _regime_cache.get(key)
    if hit and not refresh and (time.time() - hit[0] < 1800):
        return hit[1]
    res = await asyncio.to_thread(
        _al.compute_regime, min_adv_eur, require_sector, ac, max_assets, start, end, universe_id, excl,
    )
    _regime_cache[key] = (time.time(), res)
    return res


@router.get("/api/asset-pipeline/alphalab/regime/stream")
async def alphalab_regime_stream(
    min_adv_eur: float = Query(1_000_000.0, ge=0),
    require_sector: bool = True,
    asset_class: str = Query("equity"),
    max_assets: int = Query(600, ge=20, le=2500),
    start: str | None = Query(None),
    end: str | None = Query(None),
    universe_id: int | None = Query(None),
    exclude_sectors: str | None = Query(None),
):
    """SSE variant of /alphalab/regime — emits `{stage}` frames as the compute
    progresses (resolve → load prices → build index → score), then one final
    `{stage:"done", result}`. Same 30-min cache as the plain endpoint (a cache
    hit streams straight to `done`). Lets the UI show live stage progress."""
    import time  # noqa: PLC0415

    from asset_pipeline import alphalab as _al  # noqa: PLC0415
    ac = asset_class or None
    excl = [s for s in (exclude_sectors or "").split(",") if s.strip()]
    key = (min_adv_eur, require_sector, ac, int(max_assets), start, end, universe_id, tuple(sorted(excl)))

    async def gen():
        q: _queue.Queue = _queue.Queue()
        hit = _regime_cache.get(key)
        cached = hit[1] if (hit and (time.time() - hit[0] < 1800)) else None

        def work():
            try:
                if cached is not None:
                    q.put(_frame({"stage": "done", "result": cached}))
                    return
                res = _al.compute_regime(
                    min_adv_eur, require_sector, ac, max_assets, start, end, universe_id, excl,
                    lambda stage: q.put(_frame({"stage": stage})),
                )
                _regime_cache[key] = (time.time(), res)
                q.put(_frame({"stage": "done", "result": res}))
            except Exception as e:  # noqa: BLE001
                q.put(_frame({"stage": "error", "error": f"{type(e).__name__}: {e}"}))
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


_sector_regime_cache: dict[tuple, tuple[float, dict]] = {}


@router.get("/api/asset-pipeline/alphalab/sectors")
async def alphalab_sectors(
    min_adv_eur: float = Query(1_000_000.0, ge=0),
    require_sector: bool = True,
    asset_class: str = Query("equity"),
    max_assets: int = Query(600, ge=20, le=2500),
    universe_id: int | None = Query(None, description="use a SAVED universe's members instead of the ADV/sector filters"),
    refresh: bool = False,
):
    """Per-sector equal-weight price index of the universe — one {sector, size,
    dates, index} entry per sector present, each index built over that sector's
    own price history. Feeds the AlphaLab per-sector charts + risk/return tables.
    Cached ~30 min."""
    import time  # noqa: PLC0415

    from asset_pipeline import alphalab as _al  # noqa: PLC0415
    ac = asset_class or None
    key = (min_adv_eur, require_sector, ac, int(max_assets), universe_id)
    hit = _sector_regime_cache.get(key)
    if hit and not refresh and (time.time() - hit[0] < 1800):
        return hit[1]
    res = await asyncio.to_thread(
        _al.compute_sector_regime, min_adv_eur, require_sector, ac, max_assets, universe_id,
    )
    _sector_regime_cache[key] = (time.time(), res)
    return res


@router.get("/api/asset-pipeline/alphalab/sectors/stream")
async def alphalab_sectors_stream(
    min_adv_eur: float = Query(1_000_000.0, ge=0),
    require_sector: bool = True,
    asset_class: str = Query("equity"),
    max_assets: int = Query(600, ge=20, le=2500),
    universe_id: int | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """SSE variant of /alphalab/sectors — emits `{stage}` progress, then one
    `{topic:"sector", result}` frame PER sector as it's computed (largest first),
    so the UI renders sector cards progressively instead of waiting for all of
    them. `start`/`end` bound the window (with warm-up) like the benchmark stream,
    and reuse the same cached price panel (no second COPY). Ends `{topic:"done"}`."""
    from asset_pipeline import alphalab as _al  # noqa: PLC0415
    ac = asset_class or None

    async def gen():
        q: _queue.Queue = _queue.Queue()

        def work():
            try:
                panel, secmap, uni = _al.load_panel(
                    min_adv_eur, require_sector, ac, max_assets, universe_id, start, end,
                    lambda stage: q.put(_frame({"stage": stage})),
                )
                if panel is None:
                    q.put(_frame({"stage": "error", "error": "fast COPY loader unavailable (set SUPABASE_DB_URL)"}))
                    return
                q.put(_frame({"topic": "universe", "result": uni}))
                for sec in _al.iter_sector_indices(
                    panel, secmap, lambda stage: q.put(_frame({"stage": stage})), start, end,
                ):
                    q.put(_frame({"topic": "sector", "result": sec}))
                q.put(_frame({"topic": "done"}))
            except Exception as e:  # noqa: BLE001
                q.put(_frame({"stage": "error", "error": f"{type(e).__name__}: {e}"}))
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


class UniverseTicker(BaseModel):
    """One member of the liquid universe — a UNIQUE yfinance ticker (the analysis
    instrument), backed by its most-liquid tradeable listing (execution)."""
    analysis_symbol: str
    name: str | None = None
    asset_class: str | None = None
    sector: str | None = None
    currency: str | None = None
    med_adv_eur: float | None = None
    market_cap_eur: float | None = None
    bars: int | None = None
    price_from: str | None = None
    price_to: str | None = None
    zero_vol_frac: float | None = None
    n_listings: int = 1                       # how many execution listings map to this ticker
    # the chosen (most-liquid) tradeable listing behind the ticker
    execution_isin: str | None = None
    execution_symbol: str | None = None
    exchange: str | None = None
    leonteq_name: str | None = None
    leonteq_product_type: str | None = None


class UniverseResponse(BaseModel):
    count: int
    params: dict
    tickers: list[UniverseTicker]


class UniverseParams(BaseModel):
    min_adv_eur: float = 1_000_000.0
    min_market_cap_eur: float = 0.0            # 0 = no market-cap floor
    max_zero_vol: float = 0.05
    require_leonteq: bool = True
    require_openfigi_match: bool = True
    require_volume: bool = True
    asset_class: str | None = None
    sectors: list[str] | None = None          # None/empty = all sectors


def _universe_members(supabase, p: UniverseParams) -> list[dict]:
    """The deduped unique-yfinance-ticker members for a filter: pull grid listings
    meeting the identity + liquidity constraints, dedupe by `analysis_symbol`
    keeping the most-liquid listing (17 BTC ETPs → one BTC-USD), then apply the ADV
    floor to that listing. Returns shaped ticker dicts, most-liquid first."""
    rows: list[dict] = []
    off = 0
    while True:
        qb = (
            supabase.table("asset_grid").select(
                "isin, analysis_symbol, name, asset_class, sector, currency, yahoo_symbol, "
                "exchange, med_adv_eur, market_cap_eur, bars, price_from, price_to, zero_vol_frac, "
                "volume_from, leonteq_verified, leonteq_name, leonteq_currency, leonteq_product_type, "
                "openfigi_name, identity_status"
            )
            .eq("status", "ok")
            .not_.is_("analysis_symbol", "null")
            .gt("bars", 0)
            .lte("zero_vol_frac", p.max_zero_vol)
        )
        if p.require_volume:
            qb = qb.not_.is_("volume_from", "null")
        if p.require_leonteq:
            qb = (qb.eq("leonteq_verified", True)
                  .not_.is_("leonteq_name", "null")
                  .not_.is_("leonteq_currency", "null")
                  .not_.is_("leonteq_product_type", "null"))
        if p.require_openfigi_match:
            qb = qb.eq("identity_status", "verified").not_.is_("openfigi_name", "null")
        if p.asset_class:
            qb = qb.eq("asset_class", p.asset_class)
        if p.sectors:
            qb = qb.in_("sector", p.sectors)
        r = qb.order("isin").range(off, off + 999).execute().data or []
        rows += r
        if len(r) < 1000:
            break
        off += 1000

    best: dict[str, dict] = {}
    counts: dict[str, int] = {}
    for x in rows:
        s = x["analysis_symbol"]
        counts[s] = counts.get(s, 0) + 1
        cur = best.get(s)
        if cur is None or (x.get("med_adv_eur") or 0) > (cur.get("med_adv_eur") or 0):
            best[s] = x
    # HYBRID size/liquidity gate: use market cap when we have it (listing-
    # independent, so a mega-cap stranded on a thin listing still qualifies), and
    # fall back to ADV for cap-less tickers (ETFs, crypto, commodity underlyings).
    def _passes(x: dict) -> bool:
        mc = x.get("market_cap_eur")
        if mc is not None:
            return mc >= p.min_market_cap_eur
        return (x.get("med_adv_eur") or 0) >= p.min_adv_eur

    members = [x for x in best.values() if _passes(x)]
    members.sort(key=lambda x: -(x.get("market_cap_eur") or x.get("med_adv_eur") or 0))
    return [{
        "analysis_symbol": x["analysis_symbol"], "name": x.get("name"),
        "asset_class": x.get("asset_class"), "sector": x.get("sector"),
        "currency": x.get("currency"), "med_adv_eur": x.get("med_adv_eur"),
        "market_cap_eur": x.get("market_cap_eur"),
        "bars": x.get("bars"), "price_from": x.get("price_from"),
        "price_to": x.get("price_to"), "zero_vol_frac": x.get("zero_vol_frac"),
        "n_listings": counts.get(x["analysis_symbol"], 1),
        "execution_isin": x.get("isin"), "execution_symbol": x.get("yahoo_symbol"),
        "exchange": x.get("exchange"), "leonteq_name": x.get("leonteq_name"),
        "leonteq_product_type": x.get("leonteq_product_type"),
    } for x in members]


@router.get("/api/asset-pipeline/universe", response_model=UniverseResponse)
async def universe(
    min_adv_eur: float = Query(1_000_000.0, ge=0, description="HYBRID fallback: min ADV (EUR) for tickers with NO market cap (ETFs/crypto)"),
    min_market_cap_eur: float = Query(0.0, ge=0, description="HYBRID primary: min market cap (EUR) for tickers that have one; 0 = no floor. Listing-independent"),
    max_zero_vol: float = Query(0.05, ge=0, le=1, description="max zero-volume bar fraction (illiquidity guard)"),
    require_leonteq: bool = Query(True, description="require all 4 Leonteq columns (verified + name + currency + productType)"),
    require_openfigi_match: bool = Query(True, description="require OpenFIGI name + a 'verified' identity match"),
    require_volume: bool = Query(True, description="require stored traded-volume data"),
    asset_class: str | None = Query(None, description="restrict to one asset class (equity/etf/crypto/commodity/…)"),
    sectors: str | None = Query(None, description="comma-separated sectors to include; omit = all"),
    count_only: bool = Query(False, description="return only the count (for the live create-universe preview)"),
):
    """PREVIEW a large, LIQUID universe of UNIQUE yfinance tickers with price +
    volume history, from the resolved grid. Read-only — tune the params, read the
    `count`, then POST /universe/create to save it. `count_only=true` skips the
    ticker list (cheap live preview)."""
    from deps import supabase  # noqa: PLC0415
    p = UniverseParams(min_adv_eur=min_adv_eur, min_market_cap_eur=min_market_cap_eur,
                       max_zero_vol=max_zero_vol, require_leonteq=require_leonteq,
                       require_openfigi_match=require_openfigi_match,
                       require_volume=require_volume, asset_class=asset_class,
                       sectors=[s for s in sectors.split(",") if s.strip()] if sectors else None)
    tickers = await asyncio.to_thread(_universe_members, supabase, p)
    return {"count": len(tickers), "params": p.model_dump(),
            "tickers": [] if count_only else tickers}


class _CreateUniverseBody(UniverseParams):
    name: str


@router.post("/api/asset-pipeline/universe/create")
async def create_universe(body: _CreateUniverseBody):
    """Materialise + SAVE the filtered universe under `name`: computes the unique
    tickers, replaces any same-named universe, stores membership. Returns
    {id, name, ticker_count}."""
    from deps import supabase  # noqa: PLC0415
    name = body.name.strip()
    if not name:
        raise HTTPException(400, "name required")
    p = UniverseParams(**body.model_dump(exclude={"name"}))

    def _work() -> dict:
        tickers = _universe_members(supabase, p)
        syms = [t["analysis_symbol"] for t in tickers]
        # Replace any existing universe of the same name (idempotent re-create).
        supabase.table("asset_universe").delete().eq("name", name).execute()
        ins = supabase.table("asset_universe").insert(
            {"name": name, "params": p.model_dump(), "ticker_count": len(syms)}
        ).execute()
        uid = ins.data[0]["id"]
        rows = [{"universe_id": uid, "analysis_symbol": s} for s in syms]
        for i in range(0, len(rows), 500):
            supabase.table("asset_universe_member").insert(rows[i:i + 500]).execute()
        return {"id": uid, "name": name, "ticker_count": len(syms)}

    return await asyncio.to_thread(_work)


@router.get("/api/asset-pipeline/universes")
async def list_universes():
    """Saved universes (id, name, params, ticker_count, created_at), newest first."""
    from deps import supabase  # noqa: PLC0415
    return await asyncio.to_thread(
        lambda: {"universes": supabase.table("asset_universe").select("*")
                 .order("created_at", desc=True).execute().data or []}
    )


@router.get("/api/asset-pipeline/universes/{universe_id}/members")
async def universe_members(universe_id: int):
    """The member analysis_symbols of a saved universe (for the grid filter)."""
    from deps import supabase  # noqa: PLC0415

    def _q() -> dict:
        syms: list[str] = []
        off = 0
        while True:
            r = (supabase.table("asset_universe_member").select("analysis_symbol")
                 .eq("universe_id", universe_id).range(off, off + 999).execute().data) or []
            syms += [x["analysis_symbol"] for x in r]
            if len(r) < 1000:
                break
            off += 1000
        return {"universe_id": universe_id, "members": syms}

    return await asyncio.to_thread(_q)


@router.delete("/api/asset-pipeline/universes/{universe_id}")
async def delete_universe(universe_id: int):
    """Delete a saved universe (members cascade)."""
    from deps import supabase  # noqa: PLC0415
    await asyncio.to_thread(
        lambda: supabase.table("asset_universe").delete().eq("id", universe_id).execute()
    )
    return {"deleted": universe_id}


@router.get("/api/asset-pipeline/etf-sectors/candidates")
async def etf_sector_candidates():
    """Scan fund-like instruments (ETF/crypto/commodity/fx/index/bond) whose
    sector is still the lazy asset-class fallback ('etf', …) and propose a REAL
    category for each — the sector it'd have if held long (`Equity`, `Real Estate`,
    `Bonds`, `Commodity`, `FX`, `Crypto`, …), or `Short <category>` + a leverage
    multiplier for inverse products. Real Yahoo sectors + already-tagged rows are
    left out. HEURISTIC → for human review: returns the proposal alongside the
    current sector; nothing changes until POST /etf-sectors/apply."""
    from deps import supabase  # noqa: PLC0415
    from asset_pipeline import short_etf as _se  # noqa: PLC0415

    def _q() -> dict:
        # One row per analysis instrument (prefer the default execution's name).
        best: dict[int, dict] = {}
        off = 0
        while True:
            rows = (
                supabase.table("asset_grid")
                .select("analysis_id, name, asset_class, sector, short_multiplier, analysis_symbol, is_default")
                .in_("asset_class", list(_se.CANDIDATE_CLASSES))
                .range(off, off + 999).execute().data
            ) or []
            for r in rows:
                aid = r["analysis_id"]
                if aid is None:
                    continue
                cur = best.get(aid)
                if cur is None or (r.get("is_default") and not cur.get("is_default")):
                    best[aid] = r
            if len(rows) < 1000:
                break
            off += 1000

        out: list[dict] = []
        for aid, r in best.items():
            # Only rows still on the asset-class fallback — never overwrite a real
            # Yahoo sector or an already-tagged Short sector.
            if not _se.is_fallback_sector(r.get("sector"), r.get("asset_class")):
                continue
            cls = _se.classify_sector(r.get("name"), r.get("asset_class"))
            out.append({
                "analysis_id": aid,
                "analysis_symbol": r.get("analysis_symbol"),
                "name": r.get("name"),
                "asset_class": r.get("asset_class"),
                "current_sector": r.get("sector"),
                "multiplier": cls["multiplier"],
                "category": cls["category"],
                "is_short": cls["is_short"],
                "proposed_sector": cls["sector"],
            })
        # Shorts first, then by category / leverage / name.
        out.sort(key=lambda x: (not x["is_short"], x["category"], -(x["multiplier"] or 0), (x["name"] or "")))
        return {
            "candidates": out,
            "sectors": list(_se.SECTORS),
            "short_sectors": list(_se.SHORT_SECTORS),
        }

    return await asyncio.to_thread(_q)


class _SectorTag(BaseModel):
    analysis_id: int
    sector: str | None = None       # a category / Short sector, or null to clear (→ asset_class)
    multiplier: int | None = None   # leverage for a Short sector; null for long


class _SectorApplyBody(BaseModel):
    tags: list[_SectorTag]


@router.post("/api/asset-pipeline/etf-sectors/apply")
async def etf_sector_apply(body: _SectorApplyBody):
    """Commit confirmed ETF sector tags: set `asset_analysis.sector` +
    `short_multiplier` per analysis_id. `sector` must be a category or a Short
    sector, or null to CLEAR (reset to the plain asset_class, multiplier→null).
    The multiplier is only kept for a `Short …` sector. Admin-only."""
    from deps import supabase  # noqa: PLC0415
    from asset_pipeline import short_etf as _se  # noqa: PLC0415

    allowed = set(_se.SECTORS) | set(_se.SHORT_SECTORS)

    def _apply() -> dict:
        updated = 0
        for t in body.tags:
            if t.sector is not None and t.sector not in allowed:
                continue  # ignore unexpected sector values
            if t.sector is None:  # clear → fall back to the asset class, drop the multiplier
                a = (supabase.table("asset_analysis").select("asset_class")
                     .eq("analysis_id", t.analysis_id).limit(1).execute().data) or []
                patch = {"sector": (a[0].get("asset_class") if a else None) or "etf", "short_multiplier": None}
            else:
                mult = t.multiplier if t.sector.startswith("Short ") else None
                patch = {"sector": t.sector, "short_multiplier": mult}
            supabase.table("asset_analysis").update(patch).eq("analysis_id", t.analysis_id).execute()
            updated += 1
        return {"updated": updated}

    return await asyncio.to_thread(_apply)


@router.get("/api/asset-pipeline/equity-sectors/stuck")
async def equity_sector_stuck():
    """Equities STILL on the `equity`/NULL sector fallback (Yahoo assetProfile had
    no sector for them — foreign/holding/ADR names, delisted symbols, …). Returns
    `{stuck:[{analysis_id, symbol, name, current_sector, guess}], sectors}` for
    manual assignment (a name-based `guess` pre-fills the dropdown when it's
    confident). Apply via POST /etf-sectors/apply."""
    from deps import supabase  # noqa: PLC0415
    from asset_pipeline import short_etf as _se  # noqa: PLC0415

    def _q() -> dict:
        best: dict[int, dict] = {}
        off = 0
        while True:
            rows = (
                supabase.table("asset_grid")
                .select("analysis_id, name, sector, analysis_symbol, is_default")
                .eq("asset_class", "equity").range(off, off + 999).execute().data
            ) or []
            for r in rows:
                aid = r["analysis_id"]
                if aid is None or r.get("sector") not in (None, "equity"):
                    continue
                cur = best.get(aid)
                if cur is None or (r.get("is_default") and not cur.get("is_default")):
                    best[aid] = r
            if len(rows) < 1000:
                break
            off += 1000

        out = []
        for aid, r in best.items():
            name = r.get("name")
            known = _se.known_sector(name)  # curated override — wins, pre-fills even "Equity"
            if known:
                guess, mult = known, None
            else:
                cls = _se.classify_sector(name, "equity")
                # A confident pre-fill (skip the generic Equity/Single-Stock fallbacks).
                guess = cls["sector"] if cls["sector"] not in ("Equity", "Single Stock") else None
                mult = cls["multiplier"]
            out.append({
                "analysis_id": aid,
                "symbol": r.get("analysis_symbol"),
                "name": name,
                "current_sector": r.get("sector"),
                "guess": guess,
                "multiplier": mult,
            })
        out.sort(key=lambda x: (x["name"] or ""))
        return {"stuck": out, "sectors": list(_se.SECTORS), "short_sectors": list(_se.SHORT_SECTORS)}

    return await asyncio.to_thread(_q)


@router.post("/api/asset-pipeline/equity-sectors/backfill")
async def equity_sector_backfill():
    """SSE: fill the REAL Yahoo sector on equities still stuck on the `equity`
    fallback (the fast chart-resolver carries no sector). Fetches v10
    assetProfile per symbol, normalizes onto the canonical taxonomy (Apple →
    Technology, JPMorgan → Financials), and updates `asset_analysis.sector`.
    Emits per-symbol progress + a final summary. Re-runnable; only touches
    equity-class rows on the fallback. Admin-only."""
    from deps import supabase  # noqa: PLC0415
    from asset_pipeline import short_etf as _se, yahoo  # noqa: PLC0415

    async def gen():
        q: _queue.Queue = _queue.Queue()

        def work():
            try:
                q.put(_frame({"type": "status", "message": "Finding equities without a real sector…"}))
                rows: list[dict] = []
                off = 0
                while True:
                    r = (
                        supabase.table("asset_analysis").select("analysis_id, symbol, sector")
                        .eq("asset_class", "equity").range(off, off + 999).execute().data
                    ) or []
                    rows += [x for x in r if (x.get("sector") in (None, "equity")) and x.get("symbol")]
                    if len(r) < 1000:
                        break
                    off += 1000

                total = len(rows)
                q.put(_frame({"type": "start", "total": total}))
                updated = 0
                for i, x in enumerate(rows, 1):
                    prof = yahoo.asset_profile([x["symbol"]])  # paced internally
                    p = prof.get(x["symbol"])
                    sec = _se.normalize_sector(p.get("sector")) if p else None
                    if sec:
                        supabase.table("asset_analysis").update({"sector": sec}).eq(
                            "analysis_id", x["analysis_id"]).execute()
                        updated += 1
                    q.put(_frame({"type": "item", "i": i, "total": total, "symbol": x["symbol"], "sector": sec}))
                q.put(_frame({"type": "summary", "total": total, "updated": updated}))
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
