"""Saved-backtests CRUD.

Endpoints:
    POST   /api/momentum/backtests          save a run (single or variant bundle)
    GET    /api/momentum/backtests          list (metadata only — see note)
    GET    /api/momentum/backtests/{run_id} full payload for one run
    DELETE /api/momentum/backtests/{run_id} drop one
    PATCH  /api/momentum/backtests/{run_id} rename

The list endpoint deliberately ships only (run_id, name, created_at). An
earlier version included the full result blob — for variant bundles that
ballooned the response to >50 MB and made the dropdown unusable. Full
payload is fetched on demand via the per-run GET when the user clicks
into a saved run.

`daily_records` and `universe_daily_records` are transparently re-encoded
into a parallel-array `{dates, returns}` form on save and re-expanded to
the verbose `[{date, cumulative_return_pct}, ...]` shape on load.

Result storage: the compacted blob is gzipped and uploaded to the
`backtest-results` Supabase Storage bucket; the DB row stores only the
bucket path in `result_path`. The in-row `result` JSONB column is
preserved for legacy rows written before this change — load transparently
falls back to it when `result_path` is null. This lifts the size ceiling
entirely (Storage uploads aren't subject to statement_timeout) so a
24y × 14-variant bundle no longer 500s on insert.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import uuid

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from deps import supabase

log = logging.getLogger(__name__)

router = APIRouter(tags=["momentum"])

# Bucket lives in Supabase Storage. Created by migration
# 20260529000000_backtest_results_storage.sql; the storage call below
# also self-heals via `create_bucket` if the migration hasn't been run.
_RESULT_BUCKET = "backtest-results"
_GZIP_MAGIC = b"\x1f\x8b"


def _ensure_result_bucket() -> None:
    """Idempotent bucket creation — safe to call repeatedly. The
    migration creates the bucket in prod; this guards against a freshly-
    reset local Supabase where the migration may not have re-run yet."""
    try:
        supabase.storage.create_bucket(_RESULT_BUCKET, options={"public": False})
    except Exception:
        # 409 / "already exists" / "Bucket already exists" — the most
        # common case after the migration has run. Storage's REST layer
        # returns various error shapes, so we don't try to parse them;
        # subsequent upload/download will surface real failures.
        pass


def _decode_result_payload(raw: bytes) -> dict:
    """Parse a downloaded result blob, transparently gunzip'ing when the
    bytes start with the gzip magic. Mirrors the price-cache decoder so
    both code paths handle the same on-disk format."""
    if raw.startswith(_GZIP_MAGIC):
        raw = gzip.decompress(raw)
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError(
            f"Stored backtest result is not a dict (got {type(parsed).__name__}); "
            f"the storage object may be corrupt."
        )
    return parsed


def _compact_daily_records(records):
    """Encode a verbose `[{date, cumulative_return_pct}, ...]` list as the
    parallel-array `{dates: [...], returns: [...]}` form. Returns the input
    untouched if it isn't a list (already compact, or missing)."""
    if not isinstance(records, list):
        return records
    dates: list[str] = []
    returns: list[float] = []
    for r in records:
        if not isinstance(r, dict):
            continue
        d = r.get("date")
        v = r.get("cumulative_return_pct")
        if d is None or v is None:
            continue
        dates.append(d)
        returns.append(v)
    return {"dates": dates, "returns": returns}


def _expand_daily_records(value):
    """Inverse of `_compact_daily_records`. Accepts either compact dict form
    or verbose list form (legacy rows) and always returns the verbose list
    shape the frontend expects."""
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        dates = value.get("dates") or []
        returns = value.get("returns") or []
        n = min(len(dates), len(returns))
        return [
            {"date": dates[i], "cumulative_return_pct": returns[i]}
            for i in range(n)
        ]
    return []


def _compact_in_place(blob: dict) -> None:
    """Compact every daily curve inside a result blob — both
    `daily_records` (the strategy's chained equity curve) AND
    `universe_daily_records` (the equal-weight baseline curve), at the
    top level (single-run shape) and inside every variant (bundle
    shape). Each daily list goes from
    ``[{date, cumulative_return_pct}, ...]`` to the parallel-array
    ``{dates, returns}`` form — same algorithm for both fields. For a
    24y × 14-variant bundle the universe baseline is the same size as
    the strategy curve, so compacting it ~halves the JSONB blob over
    just compacting `daily_records` alone (the difference between
    fitting under Supabase's statement_timeout and a 500). Mutates the
    blob in place; safe to call on either shape."""
    _DAILY_FIELDS = ("daily_records", "universe_daily_records")
    for f in _DAILY_FIELDS:
        if f in blob:
            blob[f] = _compact_daily_records(blob[f])
    variants = blob.get("variants")
    if isinstance(variants, list):
        for v in variants:
            if not isinstance(v, dict):
                continue
            for f in _DAILY_FIELDS:
                if f in v:
                    v[f] = _compact_daily_records(v[f])


def _expand_in_place(blob: dict) -> None:
    """Inverse of `_compact_in_place`. Handles legacy verbose rows
    transparently (passthrough), including legacy rows that only
    compacted `daily_records`."""
    _DAILY_FIELDS = ("daily_records", "universe_daily_records")
    for f in _DAILY_FIELDS:
        if f in blob:
            blob[f] = _expand_daily_records(blob[f])
    variants = blob.get("variants")
    if isinstance(variants, list):
        for v in variants:
            if not isinstance(v, dict):
                continue
            for f in _DAILY_FIELDS:
                if f in v:
                    v[f] = _expand_daily_records(v[f])


class SaveBacktestRequest(BaseModel):
    name: str
    config: dict
    # Single-run shape: provide summary + monthly_records. The two daily
    # curves are optional but strongly recommended — without them a loaded
    # single run falls back to month-end points and the equity / vs-universe
    # / alpha charts render monthly instead of daily.
    summary: dict | None = None
    monthly_records: list | None = None
    daily_records: list | None = None
    universe_daily_records: list | None = None
    # Variant-bundle shape: provide a list of variants, each
    # {key, label, summary, monthly_records}. When present, `summary` /
    # `monthly_records` are ignored and the row is stored as
    # `result = {kind: "variants", variants, universe}`.
    variants: list | None = None
    universe: list  # [{company_id, ticker, exchange, company_name, sector}]


class RenameBacktestRequest(BaseModel):
    name: str


@router.post("/api/momentum/backtests")
async def save_backtest(req: SaveBacktestRequest):
    """Save a backtest run. Accepts single-run or variant-bundle shape.

    The result blob is compacted (daily-curve parallel-array encoding),
    gzipped, and uploaded to the `backtest-results` Storage bucket. The
    DB row stores only the bucket path in `result_path` — the in-row
    `result` JSONB column is left NULL on new saves. This lifts the
    statement_timeout ceiling that previously bit multi-decade
    × N-variant bundles even after compaction (postgrest 57014)."""
    if req.variants is not None:
        result_blob = {
            "kind": "variants",
            "variants": req.variants,
            "universe": req.universe,
        }
    else:
        if req.summary is None or req.monthly_records is None:
            raise HTTPException(
                422,
                "Single-run save requires summary and monthly_records",
            )
        result_blob = {
            "summary": req.summary,
            "monthly_records": req.monthly_records,
            "universe": req.universe,
        }
        # Persist the daily curves when the caller supplied them so a
        # loaded single run keeps daily granularity. Only set the keys when
        # present — _compact_in_place / _expand_in_place key off `f in blob`,
        # and legacy rows that never had them must stay absent (not []).
        if req.daily_records is not None:
            result_blob["daily_records"] = req.daily_records
        if req.universe_daily_records is not None:
            result_blob["universe_daily_records"] = req.universe_daily_records
    _compact_in_place(result_blob)

    # Serialize + gzip. JSON is canonical (sorted=False; preserve insert
    # order from the frontend so visual debugging matches the saved
    # form). gzip is a big win — variant bundles are mostly repeated
    # tickers/dates, which compress to ~15-25% of the raw size.
    payload = gzip.compress(
        json.dumps(result_blob, ensure_ascii=False).encode("utf-8"),
        compresslevel=6,
    )

    # Upload BEFORE inserting the row. If the upload fails, no row gets
    # created (clean failure mode). If the insert fails afterward, we
    # clean up the orphan storage object below.
    _ensure_result_bucket()
    path = f"{uuid.uuid4().hex}.json"
    file_options = {
        "content-type": "application/json",
        "content-encoding": "gzip",
    }
    try:
        await asyncio.to_thread(
            lambda: supabase.storage.from_(_RESULT_BUCKET).upload(
                path, payload, file_options=file_options,
            )
        )
    except Exception as e:
        log.error(
            "[backtest_crud] storage upload failed for %s: %s: %s",
            path, type(e).__name__, e,
        )
        raise HTTPException(
            500,
            f"Failed to upload backtest result to storage: {type(e).__name__}: {e}",
        ) from e

    row = {
        "name": req.name.strip(),
        "config": req.config,
        "result_path": path,
        # `result` is intentionally NOT set — legacy rows keep theirs;
        # new saves store the blob in Storage only.
    }
    try:
        resp = await asyncio.to_thread(
            lambda: supabase.table("backtest_run").insert(row).execute()
        )
    except Exception as e:
        # Roll back the storage upload so we don't accumulate orphan
        # files when the row insert fails for any reason (RLS, schema
        # mismatch, network). Best-effort: a failed cleanup is logged
        # but not propagated — the user already has an insert error.
        try:
            await asyncio.to_thread(
                lambda: supabase.storage.from_(_RESULT_BUCKET).remove([path])
            )
        except Exception as cleanup_e:
            log.warning(
                "[backtest_crud] orphan-cleanup failed for %s: %s: %s",
                path, type(cleanup_e).__name__, cleanup_e,
            )
        raise HTTPException(
            500,
            f"Failed to save backtest row after storage upload: {type(e).__name__}: {e}",
        ) from e

    if not resp.data:
        # Same orphan-cleanup logic as above — the insert call returned
        # but produced no row (PostgREST sometimes returns 201 with []).
        try:
            await asyncio.to_thread(
                lambda: supabase.storage.from_(_RESULT_BUCKET).remove([path])
            )
        except Exception:
            pass
        raise HTTPException(500, "Failed to save backtest")
    return resp.data[0]


@router.get("/api/momentum/backtests")
async def list_backtests():
    """List saved backtests — metadata + config, no result blob.

    The frontend dropdown consumes (run_id, name, created_at, config).
    `config` is included so the dropdown can render a one-line subtext
    that disambiguates same-name runs by their parameters (top_n × per
    sector, date range, selection mode, signal weights). It's small
    (~1-3 KB per row) compared to the result blob which can be
    multi-MB for variant bundles.
    """
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .select("run_id, name, created_at, config")
        .order("created_at", desc=True)
        .execute()
    )
    return resp.data or []


@router.get("/api/momentum/backtests/{run_id}")
async def load_backtest(run_id: int):
    """Full backtest payload for one run. Two source paths, transparent to
    the caller:

      * `result_path` populated → download the gzipped JSON from the
        `backtest-results` Storage bucket and decode.
      * `result_path` null → use the in-row `result` JSONB (legacy rows
        written before the storage move).

    Compacted daily-curve fields are re-expanded back to the verbose
    `[{date, cumulative_return_pct}, ...]` shape so the frontend can
    keep treating saved runs identically to in-memory ones."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .select("*")
        .eq("run_id", run_id)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Backtest not found")
    row = resp.data[0]

    result: dict | None = None
    path = row.get("result_path")
    if path:
        try:
            raw = await asyncio.to_thread(
                lambda: supabase.storage.from_(_RESULT_BUCKET).download(path)
            )
        except Exception as e:
            log.error(
                "[backtest_crud] storage download failed for run_id=%s path=%s: %s: %s",
                run_id, path, type(e).__name__, e,
            )
            raise HTTPException(
                500,
                f"Backtest row exists but its result blob could not be fetched from "
                f"storage ({type(e).__name__}: {e}). The object may have been "
                f"deleted out-of-band.",
            ) from e
        try:
            result = _decode_result_payload(raw)
        except Exception as e:
            log.error(
                "[backtest_crud] storage decode failed for run_id=%s path=%s: %s: %s",
                run_id, path, type(e).__name__, e,
            )
            raise HTTPException(
                500,
                f"Backtest result blob downloaded but couldn't be decoded "
                f"({type(e).__name__}: {e}).",
            ) from e
    else:
        in_row = row.get("result")
        if isinstance(in_row, dict):
            result = in_row

    if isinstance(result, dict):
        _expand_in_place(result)
    # Always expose the result under `row.result` for backwards compat
    # with the frontend (which reads `row.result`, not `row.result_path`).
    row["result"] = result
    return row


@router.delete("/api/momentum/backtests/{run_id}")
async def delete_backtest(run_id: int):
    """Delete a saved backtest run + its Storage object (if any). Storage
    cleanup is best-effort — an orphaned file is acceptable and visible
    in the bucket; an orphaned DB row is not (the user would still see
    the row in the dropdown). So the DB delete always runs; the storage
    delete failures are logged but don't fail the request."""
    # Capture the path BEFORE deleting the row so we know what to clean
    # up afterwards.
    fetch = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .select("result_path")
        .eq("run_id", run_id)
        .limit(1)
        .execute()
    )
    path: str | None = None
    if fetch.data:
        path = fetch.data[0].get("result_path")

    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .delete()
        .eq("run_id", run_id)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Backtest not found")

    if path:
        try:
            await asyncio.to_thread(
                lambda: supabase.storage.from_(_RESULT_BUCKET).remove([path])
            )
        except Exception as e:
            log.warning(
                "[backtest_crud] storage remove failed for run_id=%s path=%s: "
                "%s: %s — orphan file remains.",
                run_id, path, type(e).__name__, e,
            )
    return {"ok": True}


@router.patch("/api/momentum/backtests/{run_id}")
async def rename_backtest(run_id: int, req: RenameBacktestRequest):
    """Rename a saved backtest run."""
    new_name = req.name.strip()
    if not new_name:
        raise HTTPException(400, "Name cannot be empty")
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .update({"name": new_name})
        .eq("run_id", run_id)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Backtest not found")
    return resp.data[0]
