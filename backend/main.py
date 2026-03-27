import asyncio
import json
import os
import re
from datetime import date

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from supabase import create_client
from dotenv import load_dotenv

from ingest.acquire import acquire_raw_longequity_backfill
from ingest.flatten import flatten_excel
from ingest.extend_primary import enrich_flattened_df_with_primary_listing
from ingest.transformation import prepare_flattened_for_schema
from ingest.load_into_supabase import (
    load_prepared_into_supabase,
    get_ticker_overrides,
    save_ticker_overrides,
    fix_company_primary_keys,
)
from ingest.resolve_tickers import detect_unknown_tickers, resolve_via_openfigi

load_dotenv()

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_SERVICE_KEY"]
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://bbterminal.vercel.app",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _as_of_date_from_filename(filename: str) -> date:
    m = re.search(
        r"(january|february|march|april|may|june|july|august|september|october|november|december)"
        r"[-_](\d{4})",
        filename.lower(),
    )
    if not m:
        raise ValueError(f"Could not parse month-year from filename: {filename}")
    month = _MONTHS[m.group(1)]
    year = int(m.group(2))
    if month == 12:
        return date(year + 1, 1, 1)
    return date(year, month + 1, 1)


@app.get("/api/hello")
def hello():
    return {"message": "Hello from FastAPI + uv!"}


@app.get("/api/items")
def get_items():
    try:
        result = supabase.table("items").select("*").execute()
        return {"items": result.data}
    except Exception:
        return {"items": []}


async def _ingest_long_equity_stream():
    def event(msg_type: str, message: str) -> str:
        return f"data: {json.dumps({'type': msg_type, 'message': message})}\n\n"

    # ------------------------------------------------------------------ #
    # Acquire files
    # ------------------------------------------------------------------ #
    yield event("info", "Acquiring Long Equity files (Storage → remote URL)...")
    try:
        files = await asyncio.to_thread(acquire_raw_longequity_backfill, supabase)
    except Exception as e:
        yield event("error", f"Acquire failed: {e}")
        return

    if not files:
        yield event("done", "Pipeline finished — no files found.")
        return

    files = sorted(files, key=lambda t: _as_of_date_from_filename(t[0]))
    yield event("info", f"Found {len(files)} file(s). Processing oldest → newest.")

    # ------------------------------------------------------------------ #
    # Load persisted ticker overrides once (shared across all files)
    # ------------------------------------------------------------------ #
    try:
        db_overrides: list[dict] = await asyncio.to_thread(get_ticker_overrides, supabase)
        yield event("info", f"Loaded {len(db_overrides)} ticker override(s) from DB.")
    except Exception as e:
        yield event("info", f"Could not load ticker overrides (table may not exist yet): {e}")
        db_overrides = []

    total_companies = 0
    total_fn = 0
    total_ft = 0
    all_new_resolutions: list[dict] = []

    # ------------------------------------------------------------------ #
    # Process each file
    # ------------------------------------------------------------------ #
    for i, (filename, content) in enumerate(files, 1):
        as_of = _as_of_date_from_filename(filename)

        yield event("info", "")
        yield event("info", f"[{i}/{len(files)}] {filename}  (as_of: {as_of})")

        # Flatten
        yield event("info", "  Flattening grouped headers...")
        try:
            df = await asyncio.to_thread(flatten_excel, content)
        except Exception as e:
            yield event("error", f"  Flatten failed: {e}")
            continue
        yield event("info", f"  {len(df)} rows, {len(df.columns)} columns")

        # Detect unknown tickers (not in fill_ticker.json or DB overrides)
        try:
            unknowns = await asyncio.to_thread(
                detect_unknown_tickers, df, db_overrides=db_overrides
            )
        except Exception as e:
            yield event("info", f"  Ticker detection failed (skipping): {e}")
            unknowns = []

        if unknowns:
            yield event("info", f"  {len(unknowns)} unknown ticker(s): {', '.join(u['ticker'] for u in unknowns)}")
            yield event("info", "  Resolving via OpenFIGI...")
            try:
                resolved = await asyncio.to_thread(resolve_via_openfigi, unknowns)
            except Exception as e:
                yield event("info", f"  OpenFIGI failed (continuing without): {e}")
                resolved = []

            if resolved:
                yield event("info", f"  Resolved {len(resolved)}/{len(unknowns)} ticker(s).")
                # Persist to DB
                try:
                    saved = await asyncio.to_thread(save_ticker_overrides, supabase, resolved)
                    if saved:
                        yield event("info", f"  Saved {saved} new resolution(s) to ticker_override table.")
                except Exception as e:
                    yield event("info", f"  Could not save resolutions to DB: {e}")

                # Update in-memory overrides so later files in this run benefit too
                db_overrides = db_overrides + resolved
                all_new_resolutions.extend(resolved)
            else:
                yield event("info", f"  Could not resolve {len(unknowns)} ticker(s) — will use fallback values.")
        else:
            yield event("info", "  All tickers covered by existing mappings.")

        # Enrich — pass all overrides (DB + newly resolved) as extra_overrides
        yield event("info", "  Enriching tickers...")
        try:
            df = await asyncio.to_thread(
                enrich_flattened_df_with_primary_listing, df,
                extra_overrides=db_overrides if db_overrides else None,
            )
        except Exception as e:
            yield event("error", f"  Enrich failed: {e}")
            continue

        # Transform
        yield event("info", "  Transforming to star schema...")
        try:
            prepared = await asyncio.to_thread(
                prepare_flattened_for_schema, df,
                as_of_date=as_of, source_code="longequity",
            )
        except Exception as e:
            yield event("error", f"  Transform failed: {e}")
            continue
        yield event("info", f"  {len(prepared.company)} companies, {len(prepared.metric)} metrics")

        # Load
        yield event("info", "  Loading into Supabase...")
        try:
            result = await asyncio.to_thread(load_prepared_into_supabase, prepared, supabase)
        except Exception as e:
            yield event("error", f"  Load failed: {e}")
            continue

        total_companies += result.company_inserted
        total_fn += result.facts_number_inserted
        total_ft += result.facts_text_inserted

        yield event("info", (
            f"  Inserted: {result.company_inserted} companies, "
            f"{result.facts_number_inserted} numeric facts, "
            f"{result.facts_text_inserted} text facts"
        ))

    # ------------------------------------------------------------------ #
    # Fix company rows that were loaded with primary_exchange='UNKNOWN'
    # in previous runs (before Phase 3 existed)
    # ------------------------------------------------------------------ #
    if all_new_resolutions:
        yield event("info", "")
        yield event("info", "Fixing company records from previous runs with UNKNOWN exchange...")
        try:
            fixed = await asyncio.to_thread(fix_company_primary_keys, supabase, all_new_resolutions)
            if fixed:
                yield event("info", f"  Fixed {fixed} company record(s).")
            else:
                yield event("info", "  No records needed fixing.")
        except Exception as e:
            yield event("info", f"  Fix step failed (non-critical): {e}")

    yield event("info", "")
    yield event("done", (
        f"Pipeline complete. {len(files)} file(s) processed. "
        f"Total new rows — companies: {total_companies}, "
        f"numeric facts: {total_fn}, text facts: {total_ft}."
    ))


@app.get("/api/longequity/snapshots")
def get_longequity_snapshots():
    resp = (
        supabase.table("snapshot")
        .select("snapshot_id,target_date,published_at")
        .order("target_date")
        .execute()
    )
    return resp.data or []


@app.get("/api/longequity/companies/{snapshot_id}")
def get_longequity_companies(snapshot_id: int):
    # Fetch all companies upfront (small table ~400 rows) to avoid large .in_() URL params
    all_companies_resp = (
        supabase.table("company")
        .select("company_id,primary_ticker,primary_exchange,country,company_name,longequity_ticker")
        .limit(10000)
        .execute()
    )
    all_companies: dict[int, dict] = {
        c["company_id"]: c for c in (all_companies_resp.data or [])
    }

    def _company_ids_in_snapshot(sid: int) -> set[int]:
        """Return distinct company_ids that have facts in a snapshot via RPC."""
        resp = supabase.rpc("get_snapshot_company_ids", {"p_snapshot_id": sid}).execute()
        return {r["company_id"] for r in (resp.data or [])}

    current_ids = _company_ids_in_snapshot(snapshot_id)
    companies = [all_companies[cid] for cid in current_ids if cid in all_companies]

    snap_resp = (
        supabase.table("snapshot")
        .select("snapshot_id,target_date")
        .eq("snapshot_id", snapshot_id)
        .execute()
    )

    added: list[dict] = []
    removed: list[dict] = []

    if snap_resp.data:
        current_date = snap_resp.data[0]["target_date"]
        prev_resp = (
            supabase.table("snapshot")
            .select("snapshot_id,target_date")
            .lt("target_date", current_date)
            .order("target_date", desc=True)
            .limit(1)
            .execute()
        )
        if prev_resp.data:
            prev_ids = _company_ids_in_snapshot(prev_resp.data[0]["snapshot_id"])
            added   = [all_companies[cid] for cid in (current_ids - prev_ids) if cid in all_companies]
            removed = [all_companies[cid] for cid in (prev_ids - current_ids) if cid in all_companies]

    return {"companies": companies, "added": added, "removed": removed}


@app.post("/api/ingest/long-equity")
async def ingest_long_equity():
    return StreamingResponse(
        _ingest_long_equity_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
