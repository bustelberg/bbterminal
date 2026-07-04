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

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from asset_pipeline.resolve import resolve as _resolve

router = APIRouter(tags=["asset-pipeline"])


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
                from asset_pipeline import store  # noqa: PLC0415
                ok = fail = skipped = 0
                assets: set[str] = set()
                total = len(ids)
                for i, ident in enumerate(ids, 1):
                    try:
                        res = _resolve(ident, with_candles=False)  # batch doesn't display candles
                        an = res.get("analysis") or {}
                        if not an.get("symbol"):
                            # Bonds/gilts have no Yahoo series — SKIP cleanly (not a failure).
                            if res.get("asset_class") == "bond":
                                skipped += 1
                                q.put(_frame({
                                    "type": "item", "i": i, "total": total, "input": ident,
                                    "status": "skipped", "asset_class": "bond",
                                    "reason": res.get("reason"),
                                }))
                                continue
                            raise ValueError(res.get("reason") or "no analysis instrument")
                        ids_ = store.upsert_asset(res)
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
                    except Exception as e:  # noqa: BLE001
                        fail += 1
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
                    "skipped": skipped, "unique_assets": len(assets), "defaults_set": defaults,
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
async def asset_series(analysis_id: int, max_points: int = 3000, in_eur: bool = False):
    """The stored daily close+volume series for one analysis asset (for the
    catalog chart). Paginated to defeat PostgREST's 1000-row cap, then
    downsampled to ~`max_points` (stride, first+last kept) so a 10k-bar series
    stays light to ship + render.

    `in_eur=true` converts each close to EUR (via the fx_rate table, GBp
    handled); bars with no available FX rate come back with a null close."""
    from deps import supabase  # noqa: PLC0415

    def _q() -> dict:
        arow = (
            supabase.table("asset_analysis").select("currency")
            .eq("analysis_id", analysis_id).limit(1).execute().data
        )
        native_ccy = arow[0].get("currency") if arow else None
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
        currency = native_ccy
        if in_eur:
            from asset_pipeline.fx import to_eur  # noqa: PLC0415
            converted = to_eur(series, native_ccy)
            for s, c in zip(series, converted):
                s["close"] = c["close_eur"]  # EUR close (null where no FX rate)
            currency = "EUR"
        return {
            "analysis_id": analysis_id, "total": total, "points": len(series),
            "currency": currency, "native_currency": native_ccy, "series": series,
        }

    return await asyncio.to_thread(_q)


@router.get("/api/asset-pipeline/storage")
async def storage():
    """Live row counts + rough on-disk estimate for the asset-pipeline tables."""
    from asset_pipeline import store  # noqa: PLC0415
    return await asyncio.to_thread(store.storage_summary)


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
