import asyncio
import functools
import hashlib
import json
import logging
import os
import re
import threading
import time
from collections import OrderedDict

import pandas as pd
from datetime import date, datetime

from fastapi import FastAPI, UploadFile, File, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Literal

# Supabase client + env loading lives in deps.py so routers can share it
# without re-importing main (which would create a circular import as we
# move endpoints out).
from deps import supabase

from portfolio import parse_airs_excel
from airs_scanner import scan_portfolios_sync, download_portfolio_sync

from ingest.acquire import acquire_raw_longequity_backfill, check_latest_available_month
from ingest.flatten import flatten_excel
from ingest.extend_primary import enrich_flattened_df_with_primary_listing
from ingest.transformation import prepare_flattened_for_schema
from ingest.load_into_supabase import (
    load_prepared_into_supabase,
    get_ticker_overrides,
    save_ticker_overrides,
    fix_company_primary_keys,
    merge_duplicate_companies,
)
from ingest.resolve_tickers import detect_unknown_tickers, resolve_via_openfigi
from ingest.earnings import fetch_financials, fetch_analyst_estimates, fetch_indicators
from ingest.prices import ensure_prices_for_company, ensure_volume_for_company, PriceResult, _fetch_price_from_api, _parse_price_series, _ensure_bucket
from ingest.api_usage import track_api_call, get_usage
from momentum.data import (
    load_universe, load_all_prices, load_all_volumes,
    load_company_currency, load_fx_rates, convert_prices_to_eur,
    sync_fx_rates_to_db, self_heal_missing_data,
)
from momentum.signals import PRICE_SIGNAL_DEFS
from momentum.backtest import (
    BacktestConfig, run_backtest, run_multi_trial_backtest, run_current_portfolio,
    build_shared_backtest_inputs, prepare_variant_from_shared,
    _generate_rebalance_dates,
)
from universe.screen import screen_universe, build_and_store_universes, validate_vs_longequity
from universe.criteria import CRITERIA_NAMES, CRITERIA_DESCRIPTIONS, CRITERIA_MIN_YEARS
from index_universe.sp500 import (
    scrape_sp500, reconstruct_monthly_holdings,
    resolve_and_create_companies, store_index_membership,
    check_gurufocus_availability, load_changes,
)
from fx_rates import (
    get_coverage_info,
    fetch_latest_from_db, fetch_history_from_db,
    ECB_CURRENCIES, _USD_PEGS,
)
from index_universe.acwi import (
    load_acwi_holdings, get_msci_announcements,
    fetch_announcement_detail_cached, fetch_bulk_details,
    _load_detail_cache, fetch_announcement_detail, _save_detail_cache,
    compute_net_additions, gurufocus_url,
    gurufocus_exchange, gurufocus_exchange_for_db,
    reconstruct_monthly_holdings as reconstruct_acwi_monthly_holdings,
    feasible_holdings_for_db as acwi_feasible_holdings_for_db,
)

app = FastAPI()

# Domain routers — endpoints moved out of this file live here. Each router
# imports `supabase` from `deps` rather than from `main` so there's no
# circular import. Add a new router by creating routers/<name>.py with an
# `APIRouter` and including it below.
from routers import (  # noqa: E402
    auth as _auth_router,
    airs as _airs_router,
    benchmarks as _benchmarks_router,
    companies as _companies_router,
    earnings as _earnings_router,
    fx as _fx_router,
    indicators as _indicators_router,
    system as _system_router,
)

for _r in (
    _system_router.router,
    _auth_router.router,
    _benchmarks_router.router,
    _fx_router.router,
    _indicators_router.router,
    _airs_router.router,
    _companies_router.router,
    _earnings_router.router,
):
    app.include_router(_r)

_cors_origins = [
    "http://localhost:3000",
    "http://localhost:3001",
    "https://bbterminal.vercel.app",
    "https://bbterminal-api.vercel.app",
]
if os.environ.get("RAILWAY_PUBLIC_DOMAIN"):
    _cors_origins.append(f"https://{os.environ['RAILWAY_PUBLIC_DOMAIN']}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _verify_acwi_exchange_codes() -> None:
    """Warn loudly if any exchange_code acwi.py can emit is missing from
    `gurufocus_exchange`. Holdings on missing codes are silently dropped
    during ACWI sync (see main.py around line 4905) — that bug ate MSFT
    once already, this check is so it doesn't happen again."""
    try:
        from index_universe.acwi import expected_db_exchange_codes

        expected = expected_db_exchange_codes()
        resp = supabase.table("gurufocus_exchange").select("exchange_code").execute()
        present = {r["exchange_code"] for r in (resp.data or [])}
        missing = sorted(expected - present)
        if missing:
            logging.getLogger(__name__).warning(
                "[acwi] exchange codes missing from gurufocus_exchange: %s. "
                "Holdings on these exchanges will be silently skipped during "
                "ACWI sync. Add a migration that seeds them.",
                ", ".join(missing),
            )
    except Exception as e:
        logging.getLogger(__name__).warning(
            "[acwi] exchange code sanity check failed: %s", e
        )

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _latest_db_price_date() -> date | None:
    """Latest target_date in metric_data for metric_code='close_price' across
    the whole table. Used as a fast pre-flight gate so we don't run a heavy
    compute against stale DB data. Returns None if the table is empty."""
    resp = (
        supabase.table("metric_data")
        .select("target_date")
        .eq("metric_code", "close_price")
        .order("target_date", desc=True)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        return None
    raw = rows[0].get("target_date")
    if not raw:
        return None
    # Supabase returns ISO date strings; normalize.
    return date.fromisoformat(str(raw)[:10])


def _strategy_hash(req: "BacktestRequest") -> str:
    """Deterministic identifier for a strategy. Same parameters → same hash.
    Date range is intentionally excluded — current-picks is a sliding "this
    month" view and should cache across runs that differ only in dates."""
    payload = {
        "signal_weights": req.signal_weights or {},
        "category_weights": req.category_weights or {},
        "top_n_sectors": req.top_n_sectors,
        "top_n_per_sector": req.top_n_per_sector,
        "max_companies": req.max_companies,
        "universe_label": req.universe_label,
        "index_universe": req.index_universe,
        "selection_mode": req.selection_mode,
        "rebalance_frequency": req.rebalance_frequency,
        "strategy_type": req.strategy_type,
    }
    s = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode()).hexdigest()[:16]


def _backtest_strategy_hash(req: "BacktestRequest") -> str:
    """Deterministic identifier for a backtest config. Unlike `_strategy_hash`
    (Current Picks, sliding view), this includes start/end dates and the
    random-trial fields, so two runs cache to the same row only when their
    full config — including the date range — matches.
    """
    payload = {
        "start_date": req.start_date,
        "end_date": req.end_date,
        "signal_weights": req.signal_weights or {},
        "category_weights": req.category_weights or {},
        "top_n_sectors": req.top_n_sectors,
        "top_n_per_sector": req.top_n_per_sector,
        "max_companies": req.max_companies,
        "universe_label": req.universe_label,
        "index_universe": req.index_universe,
        "selection_mode": req.selection_mode,
        "random_seed": req.random_seed,
        "n_trials": req.n_trials,
        "rebalance_frequency": req.rebalance_frequency,
        "strategy_type": req.strategy_type,
    }
    s = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode()).hexdigest()[:16]


def _find_cached_backtest(strategy_hash: str) -> dict | None:
    """Return today's cached backtest for this strategy, or None.
    Cache validity is scoped to the current UTC day — once `data_date` rolls
    over (after the next daily price refresh) the next replay misses."""
    today_iso = date.today().isoformat()
    resp = (
        supabase.table("backtest_cache")
        .select("*")
        .eq("strategy_hash", strategy_hash)
        .eq("data_date", today_iso)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    return rows[0] if rows else None


def _save_backtest_cache(strategy_hash: str, config: dict, payload: dict) -> None:
    """Replace any prior cache row for this strategy with today's payload.
    Synchronous; call via asyncio.to_thread."""
    today_iso = date.today().isoformat()
    # Drop stale-day rows for this strategy first so each strategy has at
    # most one row in the cache at any time.
    supabase.table("backtest_cache").delete().eq("strategy_hash", strategy_hash).execute()
    supabase.table("backtest_cache").insert({
        "strategy_hash": strategy_hash,
        "data_date": today_iso,
        "config": config,
        "payload": payload,
    }).execute()


def _persist_daily_picks(strategy_hash: str, config: dict, daily_picks: list[dict]) -> None:
    """Upsert each day in daily_picks into current_picks_day for this strategy.
    Synchronous; call via asyncio.to_thread."""
    if not daily_picks:
        return
    rows: list[dict] = []
    for dp in daily_picks:
        target_date = dp.get("date")
        if not target_date:
            continue
        as_of = f"{target_date[:7]}-01"
        rows.append({
            "strategy_hash": strategy_hash,
            "target_date": target_date,
            "as_of_date": as_of,
            "holdings": dp.get("holdings") or [],
            "portfolio_return_pct": dp.get("portfolio_return_pct"),
            "next_day_return_pct": dp.get("next_day_return_pct"),
            "turnover_abs": dp.get("turnover_abs", 0),
            "turnover_pct": dp.get("turnover_pct", 0),
            "config": config,
        })
    if rows:
        supabase.table("current_picks_day").upsert(
            rows, on_conflict="strategy_hash,target_date"
        ).execute()


def _fetch_daily_picks_history(strategy_hash: str) -> list[dict]:
    """Return all stored daily picks for a strategy, sorted ascending by
    target_date. Shape matches the in-memory DailyPick.to_dict()."""
    resp = supabase.table("current_picks_day").select(
        "target_date, holdings, portfolio_return_pct, next_day_return_pct, turnover_abs, turnover_pct"
    ).eq("strategy_hash", strategy_hash).order("target_date").execute()
    rows = resp.data or []
    return [
        {
            "date": r["target_date"],
            "holdings": r.get("holdings") or [],
            "portfolio_return_pct": r.get("portfolio_return_pct"),
            "next_day_return_pct": r.get("next_day_return_pct"),
            "turnover_abs": r.get("turnover_abs") or 0,
            "turnover_pct": float(r.get("turnover_pct") or 0),
        }
        for r in rows
    ]


def _find_cached_snapshot(strategy_hash: str, as_of_date: str) -> dict | None:
    """Most recent snapshot for (hash, as_of_date), or None."""
    resp = supabase.table("current_picks_snapshot").select("*").eq(
        "strategy_hash", strategy_hash
    ).eq("as_of_date", as_of_date).order(
        "created_at", desc=True
    ).limit(1).execute()
    rows = resp.data or []
    return rows[0] if rows else None


def _default_snapshot_name(config: dict) -> str:
    """Sensible default label for an auto-saved current-picks snapshot.
    Format: "{universe or 'All companies'} · {YYYY-MM-DD HH:MM}". Picked
    so multiple snapshots of the same strategy across days/hours are
    distinguishable at a glance in the dropdown."""
    universe = (config.get("index_universe") or config.get("universe_label") or "All companies").strip() or "All companies"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    return f"{universe} · {ts}"


# Cached probe for the `name` column on `current_picks_snapshot`. Migration
# 20260507000000_current_picks_name.sql adds this column; if it hasn't been
# applied yet we want the rest of the page to keep working (the snapshot
# dropdown, the auto-save path) instead of 500ing every request. Probed
# lazily on first use, then cached.
_HAS_CURRENT_PICKS_NAME_COLUMN: bool | None = None


def _has_current_picks_name_column() -> bool:
    global _HAS_CURRENT_PICKS_NAME_COLUMN
    if _HAS_CURRENT_PICKS_NAME_COLUMN is None:
        try:
            supabase.table("current_picks_snapshot").select("name").limit(0).execute()
            _HAS_CURRENT_PICKS_NAME_COLUMN = True
        except Exception as e:
            _HAS_CURRENT_PICKS_NAME_COLUMN = False
            logging.getLogger(__name__).warning(
                "[current-picks] `name` column not present on current_picks_snapshot — "
                "rename UX is disabled and the dropdown shows the auto-generated label. "
                "Apply migration 20260507000000_current_picks_name.sql to enable. (%s: %s)",
                type(e).__name__, e,
            )
    return _HAS_CURRENT_PICKS_NAME_COLUMN


def _save_current_picks_snapshot(payload: dict, config: dict, triggered_by: str, strategy_hash: str | None = None, name: str | None = None) -> int:
    """Insert a current_picks snapshot and return its snapshot_id.
    Synchronous (call via asyncio.to_thread from async paths). When `name`
    is None, fills in a sensible default — the dropdown then shows
    something readable instead of an empty label. Skips the name column
    entirely when the schema migration hasn't been applied yet."""
    if triggered_by not in ("auto", "manual"):
        raise ValueError(f"triggered_by must be 'auto' or 'manual', got {triggered_by!r}")
    row = {
        "triggered_by": triggered_by,
        "as_of_date": payload["as_of_date"],
        "latest_price_date": payload.get("latest_price_date"),
        "config": config,
        "holdings": payload["holdings"],
        "daily_picks": payload.get("daily_picks") or [],
        "strategy_hash": strategy_hash,
    }
    if _has_current_picks_name_column():
        row["name"] = name if name is not None else _default_snapshot_name(config)
    resp = supabase.table("current_picks_snapshot").insert(row).execute()
    if not resp.data:
        raise RuntimeError("insert returned no data")
    return int(resp.data[0]["snapshot_id"])


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
    return date(year, month, 1)


async def _ingest_long_equity_stream():
    def event(msg_type: str, message: str) -> str:
        return f"data: {json.dumps({'type': msg_type, 'message': message})}\n\n"

    # ------------------------------------------------------------------ #
    # Check which months are already in the DB
    # ------------------------------------------------------------------ #
    existing_months = await asyncio.to_thread(_get_db_longequity_months)
    yield event("info", f"{len(existing_months)} month(s) already in DB.")

    # ------------------------------------------------------------------ #
    # Acquire files (only months not yet in DB)
    # ------------------------------------------------------------------ #
    yield event("info", "Acquiring Long Equity files (Storage → remote URL)...")
    try:
        all_files = await asyncio.to_thread(acquire_raw_longequity_backfill, supabase)
    except Exception as e:
        yield event("error", f"Acquire failed: {e}")
        return

    # Filter to only new months
    files: list[tuple[str, bytes]] = []
    for filename, content in all_files:
        try:
            as_of = _as_of_date_from_filename(filename)
            ym = f"{as_of.year:04d}-{as_of.month:02d}"
            if ym in existing_months:
                continue
            files.append((filename, content))
        except ValueError:
            files.append((filename, content))  # can't parse date — process anyway

    if not files:
        yield event("done", f"Pipeline finished — all {len(all_files)} month(s) already loaded.")
        return

    yield event("info", f"{len(files)} new file(s) to process (skipped {len(all_files) - len(files)} already in DB).")

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
    total_metric_rows = 0
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
        yield event("info", "  Transforming to metric_data...")
        try:
            prepared = await asyncio.to_thread(
                prepare_flattened_for_schema, df,
                as_of_date=as_of, source_code="longequity",
            )
        except Exception as e:
            yield event("error", f"  Transform failed: {e}")
            continue
        yield event("info", f"  {len(prepared.company)} companies, {len(prepared.metric_data)} metric rows")

        # Load
        yield event("info", "  Loading into Supabase...")
        try:
            result = await asyncio.to_thread(
                load_prepared_into_supabase, prepared, supabase,
                universe_label="longequity",
            )
        except Exception as e:
            yield event("error", f"  Load failed: {e}")
            continue

        total_companies += result.company_inserted
        total_metric_rows += result.metric_data_inserted

        yield event("info", (
            f"  Inserted: {result.company_inserted} companies, "
            f"{result.metric_data_inserted} metric rows"
        ))

    # ------------------------------------------------------------------ #
    # Fix company rows that were loaded with exchange_id=NULL
    # in previous runs (before ticker resolution existed)
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

    # ------------------------------------------------------------------ #
    # Merge duplicate companies (same name + exchange, different ticker)
    # ------------------------------------------------------------------ #
    yield event("info", "")
    yield event("info", "Checking for duplicate companies...")
    try:
        merge_logs = await asyncio.to_thread(merge_duplicate_companies, supabase)
        if merge_logs:
            for msg in merge_logs:
                yield event("info", f"  {msg}")
        else:
            yield event("info", "  No duplicates found.")
    except Exception as e:
        yield event("info", f"  Dedup step failed (non-critical): {e}")

    yield event("info", "")
    yield event("done", (
        f"Pipeline complete. {len(files)} file(s) processed. "
        f"Total new rows — companies: {total_companies}, "
        f"metric data: {total_metric_rows}."
    ))


def _get_db_longequity_months() -> set[str]:
    """Return months that are fully loaded for longequity.

    A month counts as "done" only if it has BOTH metric_data rows AND
    universe_membership rows. Early ingests populated metric_data before the
    universe_membership write path existed, so relying on metric_data alone
    silently skips months that still need their universe rows.
    """
    md_resp = supabase.rpc("get_distinct_dates", {"p_source_code": "longequity"}).execute()
    metric_months = {str(row["target_date"])[:7] for row in (md_resp.data or [])}

    u_resp = supabase.table("universe").select("universe_id").eq("label", "longequity").limit(1).execute()
    if not u_resp.data:
        return set()
    universe_id = u_resp.data[0]["universe_id"]

    membership_months: set[str] = set()
    offset = 0
    page_size = 1000
    while True:
        resp = (
            supabase.table("universe_membership")
            .select("target_month")
            .eq("universe_id", universe_id)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data or []
        for r in batch:
            if r.get("target_month"):
                membership_months.add(r["target_month"])
        if len(batch) < page_size:
            break
        offset += page_size

    return metric_months & membership_months


@app.get("/api/longequity/latest-available")
async def get_latest_available():
    try:
        spec = await asyncio.to_thread(check_latest_available_month, supabase=supabase)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"check_latest_available_month failed: {e}")
    if spec is None:
        return {"available": False, "year": None, "month": None}
    return {"available": True, "year": spec.year, "month": spec.month}


@app.get("/api/longequity/snapshots")
def get_longequity_snapshots():
    try:
        resp = supabase.rpc("get_distinct_dates", {"p_source_code": "longequity"}).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"get_longequity_snapshots failed: {e}")
    return [{"target_date": row["target_date"]} for row in (resp.data or [])]


@app.get("/api/longequity/companies")
def get_longequity_companies(target_date: str):
    # Fetch all companies upfront (small table ~400 rows)
    all_companies_resp = (
        supabase.table("company")
        .select("company_id,gurufocus_ticker,exchange_id,company_name,gurufocus_exchange:gurufocus_exchange(exchange_code,country:country(country_name))")
        .limit(10000)
        .execute()
    )
    # Flatten nested joins
    for c in (all_companies_resp.data or []):
        exch_info = c.pop("gurufocus_exchange", None) or {}
        country_info = exch_info.pop("country", None) or {}
        c["gurufocus_exchange"] = exch_info.get("exchange_code")
        c["country"] = country_info.get("country_name")
    all_companies: dict[int, dict] = {
        c["company_id"]: c for c in (all_companies_resp.data or [])
    }

    def _company_ids_for_date(td: str) -> set[int]:
        resp = supabase.rpc("get_company_ids_for_date", {
            "p_source_code": "longequity",
            "p_target_date": td,
        }).execute()
        return {r["company_id"] for r in (resp.data or [])}

    current_ids = _company_ids_for_date(target_date)
    companies = [all_companies[cid] for cid in current_ids if cid in all_companies]

    # Find previous month for diff
    all_dates = supabase.rpc("get_distinct_dates", {"p_source_code": "longequity"}).execute()
    dates_list = [r["target_date"] for r in (all_dates.data or [])]
    prev_date = None
    for d in dates_list:
        if d < target_date:
            prev_date = d
        else:
            break

    added: list[dict] = []
    removed: list[dict] = []
    if prev_date:
        prev_ids = _company_ids_for_date(prev_date)
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


class LongEquitySaveUniverseRequest(BaseModel):
    name: str = "longequity_cumulative"
    description: str | None = None
    start_date: str = "2002-01-01"
    end_date: str | None = None  # defaults to first day of current month


@app.post("/api/longequity/save-universe")
async def longequity_save_universe(req: LongEquitySaveUniverseRequest):
    """SSE stream: save a constant-per-month universe spanning a date range.

    Every month from start_date to end_date contains the same set of companies:
    the union of every company that has ever appeared in any LongEquity
    snapshot. Sector and universe_ticker are carried forward from the most
    recent snapshot in which each company appeared.
    """
    import queue as _queue
    import threading

    def _run(q: _queue.Queue):
        def emit(step: str, status: str, message: str):
            q.put(json.dumps({
                "type": "progress", "step": step, "status": status, "message": message,
            }))

        try:
            label = (req.name or "").strip()
            if not label:
                q.put(json.dumps({"type": "error", "message": "name is required"}))
                return

            # --- Resolve date range -------------------------------------------
            from datetime import date as _date
            try:
                start_d = _date.fromisoformat(req.start_date)
            except Exception:
                q.put(json.dumps({"type": "error", "message": f"invalid start_date: {req.start_date!r}"}))
                return
            if req.end_date:
                try:
                    end_d = _date.fromisoformat(req.end_date)
                except Exception:
                    q.put(json.dumps({"type": "error", "message": f"invalid end_date: {req.end_date!r}"}))
                    return
            else:
                today = _date.today()
                end_d = today.replace(day=1)
            start_d = start_d.replace(day=1)
            end_d = end_d.replace(day=1)
            if end_d < start_d:
                q.put(json.dumps({"type": "error", "message": "end_date must be >= start_date"}))
                return

            # Universe rows elsewhere key target_month as "YYYY-MM" (see
            # universe/screen.py). Match that convention so the backtest
            # loader (which does month_date.isoformat()[:7]) actually hits.
            month_list: list[str] = []
            cur = start_d
            while cur <= end_d:
                month_list.append(cur.strftime("%Y-%m"))
                # advance one month
                if cur.month == 12:
                    cur = cur.replace(year=cur.year + 1, month=1)
                else:
                    cur = cur.replace(month=cur.month + 1)

            # --- Load source memberships from the 'longequity' universe -------
            emit("load", "in_progress", "Locating 'longequity' source universe...")
            u_resp = supabase.table("universe").select("universe_id").eq("label", "longequity").limit(1).execute()
            if not u_resp.data:
                q.put(json.dumps({"type": "error", "message": "'longequity' universe not found — run ingest first."}))
                return
            source_uid = u_resp.data[0]["universe_id"]

            rows: list[dict] = []
            offset = 0
            page = 1000
            while True:
                r = (
                    supabase.table("universe_membership")
                    .select("company_id, target_month, universe_ticker, sector")
                    .eq("universe_id", source_uid)
                    .range(offset, offset + page - 1)
                    .execute()
                )
                batch = r.data or []
                rows.extend(batch)
                if len(batch) < page:
                    break
                offset += page

            source_months = sorted({r["target_month"] for r in rows if r.get("target_month")})
            if not source_months:
                q.put(json.dumps({"type": "error", "message": "no LongEquity snapshots found"}))
                return
            unique_companies = len({r["company_id"] for r in rows})
            emit(
                "load", "done",
                f"Loaded {len(rows):,} memberships across {len(source_months)} source months "
                f"({unique_companies} distinct companies).",
            )

            # --- Build constant union set + latest-known ticker/sector --------
            emit("build", "in_progress", "Building union set across all snapshots...")
            # Walk rows in ascending month order so the latest snapshot's
            # ticker/sector wins for each company.
            latest_info: dict[int, dict] = {}
            union_set: set[int] = set()
            for r in sorted(rows, key=lambda r: r.get("target_month") or ""):
                cid = r["company_id"]
                union_set.add(cid)
                latest_info[cid] = {
                    "universe_ticker": r.get("universe_ticker"),
                    "sector": r.get("sector"),
                }

            # --- Prepare target universe --------------------------------------
            emit("target", "in_progress", f"Preparing target universe '{label}'...")
            t_resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
            if t_resp.data:
                target_uid = t_resp.data[0]["universe_id"]
                # PostgREST may cap the number of rows affected by a single delete.
                # Loop until count reads zero so we never re-insert on top of stragglers.
                deleted_total = 0
                for _attempt in range(20):
                    supabase.table("universe_membership").delete().eq("universe_id", target_uid).execute()
                    remaining_resp = (
                        supabase.table("universe_membership")
                        .select("company_id", count="exact", head=True)
                        .eq("universe_id", target_uid)
                        .execute()
                    )
                    remaining = remaining_resp.count or 0
                    if remaining == 0:
                        break
                    emit(
                        "target", "in_progress",
                        f"Still {remaining:,} rows after delete; looping...",
                    )
                    deleted_total += 1
                emit("target", "done", f"Cleared existing rows in '{label}' (id={target_uid}).")
            else:
                c_resp = supabase.table("universe").insert({
                    "label": label,
                    "description": req.description or "Cumulative LongEquity universe",
                }).execute()
                target_uid = c_resp.data[0]["universe_id"]
                emit("target", "done", f"Created new universe '{label}' (id={target_uid}).")

            # Replicate the union set across every month in [start_date, end_date].
            payload: list[dict] = []
            for m in month_list:
                for cid in union_set:
                    info = latest_info.get(cid, {})
                    payload.append({
                        "universe_id": target_uid,
                        "company_id": cid,
                        "target_month": m,
                        "universe_ticker": info.get("universe_ticker"),
                        "sector": info.get("sector"),
                    })
            emit(
                "build", "done",
                f"Prepared {len(payload):,} rows = {len(union_set)} companies × {len(month_list)} months "
                f"({month_list[0]} → {month_list[-1]}).",
            )

            # --- Insert in batches --------------------------------------------
            import time as _time
            from universe.derived_metrics import _fmt_duration as _fmt_dur
            batch_size = 500
            total_batches = (len(payload) + batch_size - 1) // batch_size
            started = _time.monotonic()
            total_inserted = 0
            emit(
                "insert", "in_progress",
                f"Inserting {len(payload):,} rows in {total_batches} batches...",
            )
            for bi, i in enumerate(range(0, len(payload), batch_size), start=1):
                chunk = payload[i:i + batch_size]
                elapsed = _time.monotonic() - started
                rate = (bi - 1) / elapsed if elapsed > 0 and bi > 1 else 0
                remaining = (total_batches - bi + 1) / rate if rate > 0 else 0
                emit(
                    "insert", "in_progress",
                    f"Starting batch {bi}/{total_batches} ({len(chunk):,} rows) · "
                    f"{_fmt_dur(elapsed)} elapsed · ~{_fmt_dur(remaining)} left",
                )
                resp = supabase.table("universe_membership").insert(chunk).execute()
                total_inserted += len(resp.data or [])
                elapsed = _time.monotonic() - started
                rate = bi / elapsed if elapsed > 0 else 0
                remaining = (total_batches - bi) / rate if rate > 0 else 0
                emit(
                    "insert", "in_progress",
                    f"Batch {bi}/{total_batches} done · {total_inserted:,}/{len(payload):,} rows · "
                    f"{_fmt_dur(elapsed)} elapsed · ~{_fmt_dur(remaining)} left",
                )
            emit(
                "insert", "done",
                f"Inserted {total_inserted:,} rows in {_fmt_dur(_time.monotonic() - started)}.",
            )

            q.put(json.dumps({
                "type": "done",
                "message": (
                    f"Saved '{label}': {total_inserted:,} rows across {len(month_list)} months "
                    f"({len(union_set)} unique companies, {month_list[0]} → {month_list[-1]})."
                ),
                "data": {
                    "universe_id": target_uid,
                    "label": label,
                    "months": len(month_list),
                    "rows_inserted": total_inserted,
                    "total_companies": len(union_set),
                    "start_date": month_list[0],
                    "end_date": month_list[-1],
                },
            }))
        except Exception as e:
            import traceback
            q.put(json.dumps({"type": "error", "message": f"{e}\n{traceback.format_exc()}"}))
        finally:
            q.put(None)

    q: _queue.Queue = _queue.Queue()
    threading.Thread(target=_run, args=(q,), daemon=True).start()

    async def generate():
        yield ": keepalive\n\n"
        while True:
            msg = await asyncio.to_thread(q.get)
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")




# ─────────────────────────── Momentum Backtest ─────────────────────────────


@app.get("/api/momentum/signals")
async def get_momentum_signals():
    """Return available signal definitions and categories for the frontend."""
    from momentum.scoring import _get_category_keys
    categories = list(_get_category_keys().keys())
    return {"signals": PRICE_SIGNAL_DEFS, "categories": categories}


class SignalBreakdownRequest(BaseModel):
    company_id: int
    as_of_date: str  # "YYYY-MM-01" for the start of a backtest month, or any YYYY-MM-DD
    universe_label: str | None = None
    index_universe: str | None = None
    signal_weights: dict[str, float] | None = None
    category_weights: dict[str, float] | None = None


# In-process LRU cache for the (loaded universe, computed signal panel) at a
# given cutoff. The expensive part of /signal-breakdown is loading 500+
# companies' prices from Supabase + computing the panel — both depend ONLY
# on (universe_label, index_universe, cutoff), not on the requesting company
# or the user's signal/category weights. Caching this lets the first click
# in a session pay the full cost (~3-8s) and every subsequent click for any
# stock in any month already-cached return in <500ms (just one company's
# prices fresh + cheap scoring + explain helpers). Bounded to 50 entries
# (~3 MB total) so memory stays trivial.
_BREAKDOWN_PANEL_CACHE: "OrderedDict[tuple, dict]" = OrderedDict()
_BREAKDOWN_PANEL_CACHE_LOCK = threading.Lock()
_BREAKDOWN_PANEL_CACHE_MAX = 50


def _breakdown_cache_get(key: tuple) -> dict | None:
    with _BREAKDOWN_PANEL_CACHE_LOCK:
        if key in _BREAKDOWN_PANEL_CACHE:
            _BREAKDOWN_PANEL_CACHE.move_to_end(key)
            return _BREAKDOWN_PANEL_CACHE[key]
    return None


def _breakdown_cache_put(key: tuple, value: dict) -> None:
    with _BREAKDOWN_PANEL_CACHE_LOCK:
        _BREAKDOWN_PANEL_CACHE[key] = value
        _BREAKDOWN_PANEL_CACHE.move_to_end(key)
        while len(_BREAKDOWN_PANEL_CACHE) > _BREAKDOWN_PANEL_CACHE_MAX:
            _BREAKDOWN_PANEL_CACHE.popitem(last=False)


async def _signal_breakdown_stream(req: SignalBreakdownRequest):
    """SSE generator for /api/momentum/signal-breakdown. Emits progress
    events during the slow universe-load + panel-compute path so the UI
    can show a meaningful progress bar; instant on cache hit."""
    from datetime import timedelta as _td
    from momentum.backtest import _build_price_index, _build_volume_index
    from momentum.signals import compute_signals_panel
    from momentum.scoring import compute_category_scores, _get_category_keys
    from momentum.explain import explain_all_signals, _date_str
    import pandas as _pd
    import queue as _queue

    def _emit(payload: dict) -> str:
        return f"data: {json.dumps(payload)}\n\n"

    try:
        cutoff = date.fromisoformat(req.as_of_date)
    except ValueError:
        yield _emit({"type": "error", "message": f"as_of_date must be ISO YYYY-MM-DD, got {req.as_of_date!r}"})
        return
    cutoff_ts = _pd.Timestamp(cutoff)

    label = req.universe_label or req.index_universe
    cache_key = (req.universe_label, req.index_universe, cutoff.isoformat())

    cached = _breakdown_cache_get(cache_key)
    panel_df: _pd.DataFrame | None = None
    if cached is not None:
        panel_df = cached["panel_df"]
        yield _emit({"type": "progress", "pct": 75, "message": "Cache hit — universe panel already computed for this month"})

    if panel_df is None:
        # SLOW PATH (cache miss): full universe load + panel computation.

        # 1. Resolve the universe at the cutoff.
        yield _emit({"type": "progress", "pct": 2, "message": "Loading universe..."})
        universe_df = await asyncio.to_thread(load_universe, supabase)
        if universe_df.empty:
            yield _emit({"type": "error", "message": "No companies found in the database"})
            return
        yield _emit({"type": "progress", "pct": 6, "message": f"Loaded universe ({len(universe_df)} companies)"})

        monthly_eligible: dict[str, dict[int, str | None]] | None = None
        target_month_key = cutoff.strftime('%Y-%m')

        def _load_membership(label: str) -> dict[str, dict[int, str | None]]:
            u_resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
            if not u_resp.data:
                return {}
            universe_id = u_resp.data[0]["universe_id"]
            rows: list[dict] = []
            offset, page_size = 0, 1000
            while True:
                resp = (
                    supabase.table("universe_membership")
                    .select("target_month, company_id, sector")
                    .eq("universe_id", universe_id)
                    .order("target_month").order("company_id")
                    .range(offset, offset + page_size - 1)
                    .execute()
                )
                batch = resp.data or []
                rows.extend(batch)
                if len(batch) < page_size:
                    break
                offset += page_size
            out: dict[str, dict[int, str | None]] = {}
            for r in rows:
                m = (r.get("target_month") or "")[:7]
                if not m:
                    continue
                out.setdefault(m, {})[r["company_id"]] = r.get("sector")
            return out

        if label:
            yield _emit({"type": "progress", "pct": 8, "message": f"Loading universe membership for {label}..."})
            monthly_eligible = await asyncio.to_thread(_load_membership, label)
            if not monthly_eligible:
                yield _emit({"type": "error", "message": f"No universe data for label {label!r}"})
                return

        if monthly_eligible is not None:
            eligible = monthly_eligible.get(target_month_key) or {}
            if not eligible:
                available = sorted(monthly_eligible.keys())
                hint = f" (available range: {available[0]} … {available[-1]})" if available else ""
                yield _emit({"type": "error", "message": f"No companies in {label!r} for {target_month_key}{hint}"})
                return
            eligible_ids = set(eligible.keys())
            universe_df = (
                universe_df[universe_df["company_id"].isin(eligible_ids)]
                .copy().reset_index(drop=True)
            )
            universe_df["sector"] = universe_df["company_id"].map(eligible)
            yield _emit({"type": "progress", "pct": 12, "message": f"Filtered to {len(universe_df)} companies in {label} for {target_month_key}"})

        universe_company_ids = sorted({int(c) for c in universe_df["company_id"]})

        # 2. Load prices for the universe — pct 12 → 50 — granular via on_progress.
        price_start = cutoff - _td(days=420)
        prices_q: _queue.Queue = _queue.Queue()

        def _on_prices_progress(rows: int, page: int, chunks_done: int = 0, chunks_total: int = 0):
            prices_q.put({"rows": rows, "chunks_done": chunks_done, "chunks_total": chunks_total})

        prices_task = asyncio.create_task(asyncio.to_thread(
            load_all_prices, supabase, universe_company_ids, price_start, cutoff,
            on_progress=_on_prices_progress,
        ))
        last_emit = 0
        while not prices_task.done():
            drained = []
            while True:
                try:
                    drained.append(prices_q.get_nowait())
                except _queue.Empty:
                    break
            if drained:
                latest = drained[-1]
                ct = latest.get("chunks_total", 0) or 1
                cd = latest.get("chunks_done", 0)
                # Map chunk progress into 12-50% band.
                pct = 12 + round(cd / ct * 38)
                if pct - last_emit >= 2:
                    last_emit = pct
                    yield _emit({"type": "progress", "pct": pct, "message": f"Loading prices: {latest['rows']:,} rows ({cd}/{ct} chunks)..."})
            await asyncio.sleep(0.15)
        u_prices_df = await prices_task
        if u_prices_df.empty:
            yield _emit({"type": "error", "message": "No price data available for any company in the universe at this date"})
            return
        yield _emit({"type": "progress", "pct": 50, "message": f"Loaded {len(u_prices_df):,} price rows"})

        # 3. Load volumes for the universe — pct 50 → 65.
        volumes_q: _queue.Queue = _queue.Queue()

        def _on_volumes_progress(rows: int, page: int, chunks_done: int = 0, chunks_total: int = 0):
            volumes_q.put({"rows": rows, "chunks_done": chunks_done, "chunks_total": chunks_total})

        volumes_task = asyncio.create_task(asyncio.to_thread(
            load_all_volumes, supabase, universe_company_ids, price_start, cutoff,
            on_progress=_on_volumes_progress,
        ))
        last_emit = 50
        while not volumes_task.done():
            drained = []
            while True:
                try:
                    drained.append(volumes_q.get_nowait())
                except _queue.Empty:
                    break
            if drained:
                latest = drained[-1]
                ct = latest.get("chunks_total", 0) or 1
                cd = latest.get("chunks_done", 0)
                pct = 50 + round(cd / ct * 15)
                if pct - last_emit >= 2:
                    last_emit = pct
                    yield _emit({"type": "progress", "pct": pct, "message": f"Loading volumes: {latest['rows']:,} rows ({cd}/{ct} chunks)..."})
            await asyncio.sleep(0.15)
        u_volumes_df = await volumes_task
        yield _emit({"type": "progress", "pct": 65, "message": f"Loaded {len(u_volumes_df):,} volume rows"})

        # 4. Build the signal panel for this single cutoff — pct 65 → 80.
        yield _emit({"type": "progress", "pct": 68, "message": "Building price + volume indices..."})
        u_price_index = await asyncio.to_thread(_build_price_index, u_prices_df)
        u_volume_index = await asyncio.to_thread(_build_volume_index, u_volumes_df) if not u_volumes_df.empty else None

        yield _emit({"type": "progress", "pct": 72, "message": f"Computing signals for {len(universe_df)} companies (rolling indicators)..."})
        panel_df = await asyncio.to_thread(
            lambda: compute_signals_panel(
                universe_df, [cutoff], price_index=u_price_index, volume_index=u_volume_index,
            ).get(cutoff, _pd.DataFrame())
        )
        yield _emit({"type": "progress", "pct": 80, "message": f"Computed panel for {len(panel_df)} companies (the others lacked enough history)"})

        # Cache the panel only — keeps the entry small (~30-60 KB).
        _breakdown_cache_put(cache_key, {"panel_df": panel_df})

    # Per-company prices/volumes — needed every call for explain helpers.
    yield _emit({"type": "progress", "pct": 82, "message": f"Loading prices/volumes for company #{req.company_id}..."})
    price_start = cutoff - _td(days=420)
    co_prices_df = await asyncio.to_thread(load_all_prices, supabase, [int(req.company_id)], price_start, cutoff)
    if co_prices_df.empty:
        yield _emit({"type": "error", "message": f"No price data for company {req.company_id} before {req.as_of_date}"})
        return
    co_volumes_df = await asyncio.to_thread(load_all_volumes, supabase, [int(req.company_id)], price_start, cutoff)
    price_index = await asyncio.to_thread(_build_price_index, co_prices_df)
    volume_index = await asyncio.to_thread(_build_volume_index, co_volumes_df) if not co_volumes_df.empty else None

    # 5. Per-signal universe min/max — what the 0-100 normalization saw.
    yield _emit({"type": "progress", "pct": 88, "message": "Computing universe-wide signal min/max..."})
    signal_keys = [s["key"] for s in PRICE_SIGNAL_DEFS]
    universe_minmax: dict[str, dict[str, float | None]] = {}
    for k in signal_keys:
        if k in panel_df.columns:
            col = _pd.to_numeric(panel_df[k], errors="coerce")
            if col.notna().any():
                universe_minmax[k] = {"min": float(col.min()), "max": float(col.max())}
            else:
                universe_minmax[k] = {"min": None, "max": None}
        else:
            universe_minmax[k] = {"min": None, "max": None}

    # 6. Score the universe + look up this company's row.
    yield _emit({"type": "progress", "pct": 92, "message": "Running scoring engine + explain helpers..."})
    sig_weights = req.signal_weights or {s["key"]: s["default_weight"] for s in PRICE_SIGNAL_DEFS}
    cw = req.category_weights
    cats_keys = _get_category_keys()
    if cw and any(v != 0 for v in cw.values()):
        cw_sum = sum(abs(v) for v in cw.values()) or 1.0
        cw_normalized = {c: (cw.get(c, 0) / cw_sum) for c in cats_keys}
    else:
        n = len(cats_keys)
        cw_normalized = {c: 1.0 / n for c in cats_keys}

    scored_df = compute_category_scores(panel_df, sig_weights, req.category_weights) if not panel_df.empty else _pd.DataFrame()

    company_row = None
    if not scored_df.empty:
        match = scored_df[scored_df["company_id"] == int(req.company_id)]
        if not match.empty:
            company_row = match.iloc[0].to_dict()

    # 7. Run explain helpers against this company's trimmed series.
    company_series = price_index.get(int(req.company_id))
    if company_series is None or company_series.empty:
        yield _emit({"type": "error", "message": f"No price data for company {req.company_id} before {req.as_of_date}"})
        return
    trimmed = company_series[company_series.index < cutoff_ts]
    if trimmed.empty:
        yield _emit({"type": "error", "message": f"No price data for company {req.company_id} strictly before {req.as_of_date}"})
        return

    company_vol = volume_index.get(int(req.company_id)) if volume_index else None
    vol_trimmed = company_vol[company_vol.index < cutoff_ts] if company_vol is not None else None
    explanations = explain_all_signals(trimmed, vol_trimmed)

    # 8. Build per-signal + per-category response.
    signals_response: list[dict] = []
    for sig_def in PRICE_SIGNAL_DEFS:
        key = sig_def["key"]
        if key not in explanations:
            continue
        exp = explanations[key]
        mm = universe_minmax.get(key, {})
        sig_min = mm.get("min")
        sig_max = mm.get("max")
        normalized: float | None = None
        if exp["value"] is not None and sig_min is not None and sig_max is not None:
            if sig_max > sig_min:
                normalized = round((exp["value"] - sig_min) / (sig_max - sig_min) * 100, 2)
            else:
                normalized = 50.0
        signals_response.append({
            "key": key,
            "label": sig_def["label"],
            "description": sig_def["description"],
            "category": sig_def.get("group", "price"),
            "raw_value": exp["value"],
            "components": exp["components"],
            "universe_min": sig_min,
            "universe_max": sig_max,
            "normalized_score": normalized,
            "weight": sig_weights.get(key, 0),
        })

    category_scores: list[dict] = []
    for cat_name, weight in cw_normalized.items():
        score_val = company_row.get(f"score_{cat_name}") if company_row else None
        score = float(score_val) if score_val is not None and not _pd.isna(score_val) else None
        category_scores.append({
            "category": cat_name,
            "score": score,
            "weight": weight,
            "contribution": (score * weight) if score is not None else None,
        })

    momentum_score = None
    if company_row and "momentum_score" in company_row and not _pd.isna(company_row["momentum_score"]):
        momentum_score = float(company_row["momentum_score"])

    # 9. Company metadata.
    yield _emit({"type": "progress", "pct": 98, "message": "Looking up company metadata..."})
    meta = await asyncio.to_thread(
        lambda: (
            supabase.table("company")
            .select("company_id, gurufocus_ticker, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code)")
            .eq("company_id", req.company_id).limit(1).execute()
        )
    )
    if not meta.data:
        yield _emit({"type": "error", "message": f"Company {req.company_id} not found"})
        return
    m = meta.data[0]
    exchange_code = (m.get("gurufocus_exchange") or {}).get("exchange_code") or ""

    yield _emit({"type": "progress", "pct": 100, "message": "Done"})
    yield _emit({
        "type": "result",
        "data": {
            "company_id": int(req.company_id),
            "ticker": m.get("gurufocus_ticker", ""),
            "exchange": exchange_code,
            "company_name": m.get("company_name", ""),
            "as_of_date": req.as_of_date,
            "anchor_date": _date_str(trimmed.index[-1]),
            "anchor_price": float(trimmed.iloc[-1]),
            "signals": signals_response,
            "category_scores": category_scores,
            "category_weights_normalized": cw_normalized,
            "momentum_score": momentum_score,
            "universe_size": int(panel_df.shape[0]) if not panel_df.empty else 0,
            "in_universe_at_cutoff": company_row is not None,
            "universe_label_used": label,
        },
    })


@app.post("/api/momentum/signal-breakdown")
async def signal_breakdown(req: SignalBreakdownRequest):
    """SSE stream of step-by-step signal-breakdown computation. Emits
    `progress` events with pct + message during the heavy universe load,
    then a final `result` event with the full breakdown payload (or an
    `error` event on failure). On cache hit the slow steps are skipped
    and we go straight to per-company explain + scoring."""
    return StreamingResponse(
        _signal_breakdown_stream(req),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


_DEFAULT_END = "2026-01-01"
_DEFAULT_START = "2017-01-01"


class VariantSpec(BaseModel):
    frequency: Literal[
        "daily", "weekly", "monthly",
        "every_2_months", "every_3_months", "every_4_months", "every_5_months",
        "every_6_months", "every_7_months", "every_8_months", "every_9_months",
        "every_10_months", "every_11_months", "every_12_months",
    ]
    strategy_type: Literal["long_only", "long_short"]


class BacktestRequest(BaseModel):
    start_date: str = _DEFAULT_START
    end_date: str = _DEFAULT_END  # also used as data cutoff — no data newer than this
    signal_weights: dict[str, float] | None = None
    category_weights: dict[str, float] | None = None  # e.g. {"price": 50, "volume": 50}
    top_n_sectors: int = 4
    top_n_per_sector: int = 6
    max_companies: int = 0  # 0 = all, otherwise limit universe (alphabetical)
    universe_label: str | None = None  # if set, use universe_membership for per-month filtering
    index_universe: str | None = None  # if set, use universe_membership for per-month filtering (e.g. "SP500")
    # Literal values reject typos at the request boundary so a misspelled
    # value never silently routes through a default branch downstream
    # (e.g. an unknown `mode` quietly behaving like "backtest"). New
    # variants need to be added here AND wherever the value is consumed.
    selection_mode: Literal["momentum", "random", "all", "sector_etf"] = "momentum"
    random_seed: int | None = None  # only used when selection_mode == "random"
    n_trials: int = 1  # >1 only valid with selection_mode=="random"; aggregates mean ± std
    # Required when selection_mode == "sector_etf": maps sector name → benchmark_id.
    # The strategy ranks sectors via stock-aggregate momentum then holds the
    # mapped ETF for each picked sector (one per sector). Reuses /benchmarks
    # data for ETF prices; only benchmarks with a non-null `sector` tag are
    # eligible.
    sector_etfs: dict[str, int] | None = None
    mode: Literal["backtest", "current_portfolio"] = "backtest"
    force_recompute: bool = False  # ignore cached result and recompute (applies to backtest + current_portfolio)
    # When true (the default for the user-facing buttons), the compute uses
    # only data already in the DB — no GuruFocus / ECB API calls to fill in
    # gaps. The cron and the explicit "Recompute" button override this so
    # they can refresh stale data.
    db_only: bool = True
    rebalance_frequency: Literal[
        "daily", "weekly", "monthly",
        "every_2_months", "every_3_months", "every_4_months", "every_5_months",
        "every_6_months", "every_7_months", "every_8_months", "every_9_months",
        "every_10_months", "every_11_months", "every_12_months",
    ] = "monthly"
    strategy_type: Literal["long_only", "long_short"] = "long_only"
    # When set (non-empty), the request becomes a variants sweep: the data
    # pipeline (universe load → ensure → bulk-load prices/volumes → FX) runs
    # ONCE, then the backtest computation runs per variant against the same
    # in-memory frames. Each variant emits its own `variant_start` /
    # `variant_result` / `variant_error` events identified by a key of
    # `{frequency}__{strategy_type}`. Sweeps are backtest-only; combining
    # `variants` with `mode="current_portfolio"` is rejected.
    variants: list[VariantSpec] | None = None


async def _momentum_backtest_stream(req: BacktestRequest):
    """SSE generator for the momentum backtest."""
    def _emit(data: dict) -> str:
        return f"data: {json.dumps(data)}\n\n"

    def _keepalive() -> str:
        return ": keepalive\n\n"

    # Variants sweep is backtest-only and not cached as a bundle. Per-variant
    # results are streamed individually; if the user wants caching they can
    # save the bundle from the UI.
    if req.variants and req.mode == "current_portfolio":
        yield _emit({"type": "error", "message": "Variants sweep is not supported with mode='current_portfolio'"})
        return

    # current_portfolio mode runs against today only; coerce the date range
    # so price loading covers ~14 months of history (12m momentum + buffer)
    # without requiring the caller to pick the right window.
    if req.mode == "current_portfolio":
        from datetime import timedelta as _td
        _today = date.today()
        req.start_date = (_today - _td(days=14 * 31)).isoformat()
        req.end_date = _today.isoformat()

        # Cache hit short-circuit. Same strategy clicked twice in the same
        # month → serve the stored snapshot, no recompute. Recompute button
        # passes force_recompute=True to bypass.
        if not req.force_recompute:
            try:
                hash_ = _strategy_hash(req)
                month_start = date(_today.year, _today.month, 1).isoformat()
                cached = await asyncio.to_thread(_find_cached_snapshot, hash_, month_start)
                if cached:
                    history = await asyncio.to_thread(_fetch_daily_picks_history, hash_)
                    payload = {
                        "snapshot_id": cached.get("snapshot_id"),
                        "as_of_date": cached.get("as_of_date"),
                        "latest_price_date": cached.get("latest_price_date"),
                        "holdings": cached.get("holdings") or [],
                        "daily_picks": cached.get("daily_picks") or [],
                        "daily_picks_history": history,
                        "strategy_hash": hash_,
                        "from_cache": True,
                    }
                    yield _emit({"type": "progress", "pct": 100, "message": "Loaded cached current picks"})
                    yield _emit({"type": "current_portfolio", "data": payload, "universe": []})
                    yield _emit({"type": "done", "message": "Served from cache"})
                    return
            except Exception as e:
                # Cache lookup failed — fall through to a fresh compute and
                # surface the issue as a non-fatal warning.
                yield _emit({"type": "warning", "scope": "cache", "message": f"Cache lookup failed: {type(e).__name__}: {e}"})

    # Backtest replay cache. Same config + same UTC day → return the stored
    # payload instead of re-loading prices, re-running signals. Bypassed by
    # force_recompute=true. Skipped entirely for variants sweeps — the
    # per-variant results are streamed and not cached as a bundle. The
    # data_date column on backtest_cache scopes validity to today;
    # tomorrow's first run misses naturally.
    if req.mode != "current_portfolio" and not req.force_recompute and not req.variants:
        try:
            bt_hash = _backtest_strategy_hash(req)
            cached_bt = await asyncio.to_thread(_find_cached_backtest, bt_hash)
            if cached_bt:
                cached_payload = cached_bt.get("payload") or {}
                yield _emit({"type": "progress", "pct": 100, "message": "Loaded cached backtest result"})
                yield _emit({
                    "type": "result",
                    "data": cached_payload.get("result"),
                    "universe": cached_payload.get("universe", []),
                    "from_cache": True,
                    "strategy_hash": bt_hash,
                })
                yield _emit({"type": "done", "message": "Served from cache"})
                return
        except Exception as e:
            yield _emit({"type": "warning", "scope": "cache", "message": f"Backtest cache lookup failed: {type(e).__name__}: {e}"})

    try:
        yield _emit({"type": "progress", "pct": 0, "message": "Loading universe..."})
        universe_df = await asyncio.to_thread(load_universe, supabase)
        if universe_df.empty:
            yield _emit({"type": "error", "message": "No companies found in database"})
            return
        yield _emit({"type": "progress", "pct": 5, "message": f"Found {len(universe_df)} companies"})

        # Pre-flight DB-staleness check. A heavy compute against data that's
        # too old to support the requested window is a waste — surface it
        # before we spin up the fetch loop / load gigabytes of prices.
        # current_portfolio: needs at least one trade ON OR AFTER the start
        # of the current month (otherwise we can't price the entry leg).
        # backtest: only a soft warning — the loop truncates to whatever
        # data exists, but the user should know the requested window won't
        # be honoured in full.
        latest_price_date = await asyncio.to_thread(_latest_db_price_date)
        if latest_price_date is None:
            yield _emit({"type": "error", "message": "DB has no price data — run an ingest first"})
            return
        if req.mode == "current_portfolio":
            _today = date.today()
            month_start = date(_today.year, _today.month, 1)
            if latest_price_date < month_start:
                lag_days = (_today - latest_price_date).days
                yield _emit({
                    "type": "error",
                    "message": (
                        f"Cannot compute current picks for {month_start.isoformat()[:7]}: "
                        f"latest price in DB is {latest_price_date.isoformat()} "
                        f"({lag_days} days behind today). "
                        f"Use 'Recompute' to fetch fresh data, or run an ingest first."
                    ),
                })
                return
        else:
            req_end = date.fromisoformat(req.end_date)
            if latest_price_date < req_end:
                yield _emit({
                    "type": "warning",
                    "scope": "data",
                    "message": (
                        f"Backtest end is {req_end.isoformat()} but DB only has prices "
                        f"through {latest_price_date.isoformat()} — the run will truncate."
                    ),
                })

        # Load universe membership if a label is specified
        monthly_eligible: dict[str, set[int]] | None = None
        if req.universe_label:
            yield _emit({"type": "progress", "pct": 6, "message": f"Loading universe '{req.universe_label}'..."})

            def _load_universe_membership():
                # Get universe_id
                u_resp = supabase.table("universe").select("universe_id").eq("label", req.universe_label).limit(1).execute()
                if not u_resp.data:
                    return {}
                universe_id = u_resp.data[0]["universe_id"]
                rows = []
                offset = 0
                page_size = 1000
                while True:
                    resp = supabase.table("universe_membership").select(
                        "target_month, company_id, sector"
                    ).eq("universe_id", universe_id).order(
                        "target_month"
                    ).order("company_id").range(offset, offset + page_size - 1).execute()
                    batch = resp.data or []
                    rows.extend(batch)
                    if len(batch) < page_size:
                        break
                    offset += page_size
                result: dict[str, dict[int, str | None]] = {}
                for r in rows:
                    # Normalize to "YYYY-MM" — the backtest loop keys on
                    # month_date.isoformat()[:7] so any stored "YYYY-MM-DD"
                    # value (e.g. from an older longequity_cumulative build)
                    # would otherwise never match.
                    m = (r.get("target_month") or "")[:7]
                    if not m:
                        continue
                    if m not in result:
                        result[m] = {}
                    result[m][r["company_id"]] = r.get("sector")
                return result

            monthly_eligible = await asyncio.to_thread(_load_universe_membership)
            n_months = len(monthly_eligible)
            if n_months == 0:
                yield _emit({"type": "error", "message": f"No universe data for label '{req.universe_label}'"})
                return
            avg_pass = sum(len(v) for v in monthly_eligible.values()) // n_months
            yield _emit({"type": "progress", "pct": 7, "message": f"Universe: {n_months} months, ~{avg_pass} companies/month"})

            # Diagnose missing sector data: if no membership row has a sector
            # value, sector-based selection will silently pick zero companies.
            # Fail loudly so the user knows to re-save the universe.
            total_sec = sum(
                1 for month_map in monthly_eligible.values()
                for s in month_map.values() if s
            )
            if total_sec == 0:
                yield _emit({"type": "error", "message": f"Universe '{req.universe_label}' has no sector data in universe_membership — re-save this universe from its source page so sectors are populated."})
                return

        # Load index universe if specified (e.g. SP500 — stored as a universe label)
        if req.index_universe and monthly_eligible is None:
            yield _emit({"type": "progress", "pct": 6, "message": f"Loading index universe '{req.index_universe}'..."})

            def _load_index_universe():
                # Index universes are now stored as regular universes
                u_resp = supabase.table("universe").select("universe_id").eq("label", req.index_universe).limit(1).execute()
                if not u_resp.data:
                    return {}
                universe_id = u_resp.data[0]["universe_id"]
                rows: list[dict] = []
                offset = 0
                page_size = 1000
                while True:
                    resp = (
                        supabase.table("universe_membership")
                        .select("target_month, company_id, sector")
                        .eq("universe_id", universe_id)
                        .order("target_month")
                        .range(offset, offset + page_size - 1)
                        .execute()
                    )
                    batch = resp.data or []
                    rows.extend(batch)
                    if len(batch) < page_size:
                        break
                    offset += page_size
                result: dict[str, dict[int, str | None]] = {}
                for r in rows:
                    # Normalize to "YYYY-MM" — the backtest loop keys on
                    # month_date.isoformat()[:7] so any stored "YYYY-MM-DD"
                    # value (e.g. from an older longequity_cumulative build)
                    # would otherwise never match.
                    m = (r.get("target_month") or "")[:7]
                    if not m:
                        continue
                    if m not in result:
                        result[m] = {}
                    result[m][r["company_id"]] = r.get("sector")
                return result

            monthly_eligible = await asyncio.to_thread(_load_index_universe)
            n_months = len(monthly_eligible)
            if n_months == 0:
                yield _emit({"type": "error", "message": f"No index universe data for '{req.index_universe}'"})
                return
            avg_co = sum(len(v) for v in monthly_eligible.values()) // n_months
            yield _emit({"type": "progress", "pct": 7, "message": f"Index universe: {n_months} months, ~{avg_co} companies/month"})

            total_sec = sum(
                1 for month_map in monthly_eligible.values()
                for s in month_map.values() if s
            )
            if total_sec == 0:
                yield _emit({"type": "error", "message": f"Index universe '{req.index_universe}' has no sector data — re-save this universe from its source page so sectors are populated."})
                return

        # Also fail cleanly when no universe was selected at all — the
        # scoring pipeline requires per-company sectors, and `load_universe`
        # leaves them all None in that fallback.
        if monthly_eligible is None and (req.top_n_sectors or 0) > 0:
            yield _emit({"type": "error", "message": "No universe selected. Sector-based selection requires a universe (or index universe) with stored sector data."})
            return

        config = BacktestConfig.from_dict({
            "start_date": req.start_date,
            "end_date": req.end_date,
            "signal_weights": req.signal_weights or {s["key"]: s["default_weight"] for s in PRICE_SIGNAL_DEFS},
            "category_weights": req.category_weights,
            "top_n_sectors": req.top_n_sectors,
            "top_n_per_sector": req.top_n_per_sector,
            "selection_mode": req.selection_mode,
            "random_seed": req.random_seed,
            "rebalance_frequency": req.rebalance_frequency,
            "strategy_type": req.strategy_type,
            "sector_etfs": req.sector_etfs,
        })

        data_cutoff = date.fromisoformat(req.end_date)

        excluded_ids: set[int] = set()

        # When a universe / index_universe is selected, drop every company
        # that doesn't appear in any month of that universe. Otherwise
        # price+volume gets fetched for unrelated companies (LongEquity-only
        # adds, manual /companies entries, members of other saved universes)
        # that the scoring pipeline would discard anyway, wasting GuruFocus
        # API calls and wall-time. The filter is the union across months —
        # per-month membership filtering still runs at scoring time.
        if monthly_eligible is not None:
            eligible_ids: set[int] = set()
            for month_map in monthly_eligible.values():
                eligible_ids.update(month_map.keys())
            before = len(universe_df)
            universe_df = universe_df[universe_df["company_id"].isin(eligible_ids)].reset_index(drop=True)
            dropped = before - len(universe_df)
            if dropped:
                yield _emit({"type": "progress", "pct": 8, "message": f"Trimmed {dropped} companies not in selected universe ({len(universe_df)} remaining)"})

        # If max_companies is set, pre-trim the universe alphabetically so we
        # only fetch what we need. (Parallel fetch makes the old "stop at
        # ok_count" optimization hard to preserve.)
        if req.max_companies > 0 and len(universe_df) > req.max_companies:
            universe_df = universe_df.sort_values("gurufocus_ticker").head(req.max_companies).reset_index(drop=True)

        total_companies = len(universe_df)
        concurrency = int(os.environ.get("BACKTEST_FETCH_CONCURRENCY", "16"))
        blocked_exchanges: set[str] = set()
        skipped_count = 0
        ok_count = 0
        fetch_start_ts = time.monotonic()

        # Companies whose pass-1 ensure call hit a transient API failure
        # (source == "stale_cache" for either price or volume): the DB has
        # older data and the live API call couldn't refresh it. The audit
        # below uses this set as the self-heal retry list instead of
        # re-deriving "stale" from the bulk-loaded frame — the per-company
        # pass-1 outcome is the authoritative signal of "tried and failed".
        pass1_transient: set[int] = set()

        # In db_only mode (the default for the user-facing buttons) we
        # bypass the per-company API ensure-loop and just consume whatever
        # is already in the DB. The pre-flight staleness check above has
        # already errored if the DB isn't current enough; missing-data
        # filtering happens later in signals.py via the 30-day staleness
        # guard. This skips the bucket warmup, the executor, fetch_one,
        # blocked-exchange detection, and delisted-company pruning.
        if req.db_only:
            yield _emit({"type": "progress", "pct": 60, "message": f"DB-only mode: skipping API fetches for {total_companies} companies"})
        else:
            # Warm the storage bucket once before launching tasks — otherwise the
            # first N workers would race and each fire a bucket-create HTTP call.
            await asyncio.to_thread(_ensure_bucket, supabase)

            # Each company task submits 2 blocking HTTP calls in parallel (price +
            # volume), so the executor needs 2 slots per concurrent task or the
            # second call queues behind the first and inflates wall-clock timings.
            from concurrent.futures import ThreadPoolExecutor
            pool_size = concurrency * 2 + 4
            executor = ThreadPoolExecutor(max_workers=pool_size, thread_name_prefix="fetch")
            loop = asyncio.get_event_loop()

            yield _emit({"type": "progress", "pct": 5, "message": f"Ensuring price & volume data for {total_companies} companies (cutoff: {data_cutoff}, concurrency: {concurrency}, pool: {pool_size})..."})
            yield _keepalive()

            result_queue: asyncio.Queue = asyncio.Queue()
            sema = asyncio.Semaphore(concurrency)
            inflight = {"count": 0, "peak": 0}

            async def _fetch_one(row_cid: int, row_ticker: str, row_exchange: str):
                sym = f"{row_exchange}:{row_ticker}"
                if row_exchange in blocked_exchanges:
                    await result_queue.put({"cid": row_cid, "symbol": sym, "exchange": row_exchange, "status": "skipped_blocked"})
                    return
                async with sema:
                    if row_exchange in blocked_exchanges:
                        await result_queue.put({"cid": row_cid, "symbol": sym, "exchange": row_exchange, "status": "skipped_blocked"})
                        return
                    inflight["count"] += 1
                    if inflight["count"] > inflight["peak"]:
                        inflight["peak"] = inflight["count"]
                    task_start = time.monotonic()
                    try:
                        # Run price + volume concurrently inside one company task
                        pr_fut = loop.run_in_executor(
                            executor,
                            functools.partial(
                                ensure_prices_for_company,
                                supabase, row_cid, row_ticker, row_exchange,
                                data_cutoff=data_cutoff,
                            ),
                        )
                        vr_fut = loop.run_in_executor(
                            executor,
                            functools.partial(
                                ensure_volume_for_company,
                                supabase, row_cid, row_ticker, row_exchange,
                                data_cutoff=data_cutoff,
                            ),
                        )
                        pr_res, vr_res = await asyncio.gather(pr_fut, vr_fut, return_exceptions=True)
                        if isinstance(pr_res, BaseException):
                            raise pr_res
                        pr = pr_res
                        if isinstance(vr_res, BaseException):
                            vr = PriceResult()
                            vr.source = "error"
                            vr.error = str(vr_res)
                        else:
                            vr = vr_res
                        elapsed_ms = int((time.monotonic() - task_start) * 1000)
                        await result_queue.put({"cid": row_cid, "symbol": sym, "exchange": row_exchange, "pr": pr, "vr": vr, "status": "ok", "ms": elapsed_ms})
                    except Exception as e:
                        elapsed_ms = int((time.monotonic() - task_start) * 1000)
                        await result_queue.put({"cid": row_cid, "symbol": sym, "exchange": row_exchange, "error": f"{type(e).__name__}: {e}", "status": "error", "ms": elapsed_ms})
                    finally:
                        inflight["count"] -= 1

            tasks = [
                asyncio.create_task(_fetch_one(
                    int(row["company_id"]),
                    row["gurufocus_ticker"],
                    row["gurufocus_exchange"] or "UNKNOWN",
                ))
                for _, row in universe_df.iterrows()
            ]

            async def _sentinel():
                await asyncio.gather(*tasks, return_exceptions=True)
                await result_queue.put(None)

            sentinel_task = asyncio.create_task(_sentinel())

            done_count = 0
            try:
                while True:
                    evt = await result_queue.get()
                    if evt is None:
                        break
                    done_count += 1
                    pct = 5 + round((done_count / max(1, total_companies)) * 55)
                    status = evt["status"]
                    cid = evt["cid"]
                    symbol = evt["symbol"]
                    exchange = evt["exchange"]

                    if status == "skipped_blocked":
                        skipped_count += 1
                        excluded_ids.add(cid)
                        continue
                    if status == "error":
                        excluded_ids.add(cid)
                        yield _emit({"type": "warning", "scope": "fetch", "symbol": symbol, "message": f"{symbol}: fetch failed — {evt['error']}"})
                        continue

                    pr = evt["pr"]
                    vr = evt["vr"]

                    if pr.is_forbidden:
                        blocked_exchanges.add(exchange)
                        excluded_ids.add(cid)
                        yield _emit({"type": "progress", "pct": pct, "message": f"  {symbol}: unsubscribed region — future {exchange} calls will be skipped"})
                    elif pr.is_delisted:
                        excluded_ids.add(cid)
                        await asyncio.to_thread(
                            lambda c=cid: (
                                supabase.table("metric_data").delete().eq("company_id", c).execute(),
                                supabase.table("portfolio_weight").delete().eq("company_id", c).execute(),
                                supabase.table("company").delete().eq("company_id", c).execute(),
                            )
                        )
                        yield _emit({"type": "progress", "pct": pct, "message": f"  {symbol}: DELISTED — removed from database"})
                    else:
                        ok_count += 1
                        if pr.source == "stale_cache" or (vr and vr.source == "stale_cache"):
                            pass1_transient.add(cid)
                        parts: list[str] = []
                        if pr.source == "cache":
                            parts.append(f"price: cache ({pr.rows_loaded})")
                        elif pr.source == "api":
                            parts.append(f"price: API ({pr.rows_loaded})")
                        elif pr.source == "stale_cache":
                            parts.append(f"price: stale cache ({pr.rows_loaded})")
                        elif pr.source == "none":
                            parts.append("price: none")
                        else:
                            parts.append(f"price: {pr.source}")
                        if vr:
                            if vr.source == "cache":
                                parts.append(f"vol: cache ({vr.rows_loaded})")
                            elif vr.source == "api":
                                parts.append(f"vol: API ({vr.rows_loaded})")
                            elif vr.source == "stale_cache":
                                parts.append(f"vol: stale cache ({vr.rows_loaded})")
                            elif vr.source == "error":
                                parts.append(f"vol: error ({vr.error})")
                            else:
                                parts.append(f"vol: none ({vr.error or 'unknown'})")
                        else:
                            parts.append("vol: failed")
                        ms = evt.get("ms", 0)
                        peak = inflight["peak"]
                        yield _emit({"type": "progress", "pct": pct, "message": f"  {symbol} ({done_count}/{total_companies}, {ms}ms, peak:{peak}): {' | '.join(parts)}"})
            finally:
                # Make sure the sentinel task completes before we leave this block
                try:
                    await sentinel_task
                except Exception:
                    pass
                executor.shutdown(wait=False)

            total_elapsed = time.monotonic() - fetch_start_ts
            yield _emit({"type": "progress", "pct": 60, "message": f"Fetch complete in {total_elapsed:.1f}s (peak concurrency: {inflight['peak']}/{concurrency})"})

            if blocked_exchanges:
                yield _emit({"type": "warning", "scope": "fetch", "message": f"Blocked exchanges (unsubscribed): {', '.join(sorted(blocked_exchanges))} — {skipped_count} companies skipped"})

            # Remove excluded companies (blocked exchanges, delisted) from universe
            if excluded_ids:
                universe_df = universe_df[~universe_df["company_id"].isin(excluded_ids)].reset_index(drop=True)
                yield _emit({"type": "progress", "pct": 61, "message": f"Universe after exclusions: {len(universe_df)} companies"})

            # Optionally limit universe size (alphabetical by ticker) — applied after exclusions
            if req.max_companies > 0 and len(universe_df) > req.max_companies:
                universe_df = universe_df.sort_values("gurufocus_ticker").head(req.max_companies).reset_index(drop=True)
                yield _emit({"type": "progress", "pct": 61, "message": f"Limited to {len(universe_df)} companies (alphabetical)"})

        company_ids = universe_df["company_id"].tolist()

        # Load all prices in bulk — capped at data_cutoff
        from datetime import timedelta
        price_start = date.fromisoformat(req.start_date) - timedelta(days=300)
        price_end = date.fromisoformat(req.end_date) + timedelta(days=35)

        yield _emit({"type": "progress", "pct": 62, "message": f"Loading prices from DB ({price_start} to {price_end}, starts early for 200-day MA)..."})
        yield _keepalive()

        # Stream load progress in real time. The loader runs chunks in
        # parallel from worker threads; each thread calls on_progress as it
        # finishes a page. We push those into a queue and drain it from the
        # async generator while awaiting the load task.
        import queue as _queue
        prices_progress_q: _queue.Queue = _queue.Queue()

        def _on_prices_progress(rows_so_far: int, page_num: int, chunks_done: int = 0, chunks_total: int = 0):
            prices_progress_q.put({
                "rows": rows_so_far,
                "page": page_num,
                "chunks_done": chunks_done,
                "chunks_total": chunks_total,
            })

        prices_task = asyncio.create_task(asyncio.to_thread(
            load_all_prices, supabase, company_ids, price_start, price_end,
            on_progress=_on_prices_progress,
        ))

        # Throttle: emit at most every PROGRESS_THROTTLE pages so the SSE
        # stream isn't drowned in updates on very large loads. Percentage is
        # based on chunks-completed (each chunk is a fixed-size company batch
        # — exact denominator known up front), not row count (unknown total).
        PROGRESS_THROTTLE = 25
        last_emitted_page = 0
        def _fmt_progress(p: dict) -> str:
            ct = p.get("chunks_total", 0)
            cd = p.get("chunks_done", 0)
            pct_str = f" ≈ {round(cd / ct * 100)}%" if ct else ""
            return f"  Loaded {p['rows']:,} price rows ({cd}/{ct} chunks{pct_str})..."

        while not prices_task.done():
            drained = []
            while True:
                try:
                    drained.append(prices_progress_q.get_nowait())
                except _queue.Empty:
                    break
            if drained:
                latest = drained[-1]
                if latest["page"] - last_emitted_page >= PROGRESS_THROTTLE:
                    last_emitted_page = latest["page"]
                    yield _emit({"type": "progress", "pct": 63, "message": _fmt_progress(latest)})
            await asyncio.sleep(0.1)
        # Final drain after task completion
        final_total = None
        while True:
            try:
                final_total = prices_progress_q.get_nowait()
            except _queue.Empty:
                break
        if final_total is not None and final_total["page"] != last_emitted_page:
            yield _emit({"type": "progress", "pct": 64, "message": _fmt_progress(final_total)})

        prices_df = await prices_task

        if prices_df.empty:
            yield _emit({"type": "error", "message": "No price data found after ingestion."})
            return

        n_companies_with_prices = prices_df["company_id"].nunique()
        yield _emit({"type": "progress", "pct": 65, "message": f"Loaded {len(prices_df):,} prices for {n_companies_with_prices} companies"})

        # ------------------------------------------------------------------ #
        # FX conversion: convert local-currency prices to EUR so signals and
        # returns are expressed in a single currency for a EUR-based investor.
        # Momentum ratios are scale-invariant so signals are unaffected, but
        # forward returns change with FX drift (e.g. JPY weakness vs EUR).
        # ------------------------------------------------------------------ #
        yield _emit({"type": "progress", "pct": 65, "message": "Resolving trading currency per company..."})
        yield _keepalive()
        company_currency = await asyncio.to_thread(
            load_company_currency, supabase, company_ids,
        )
        currencies_needed = sorted({c for c in company_currency.values() if c})
        yield _emit({"type": "progress", "pct": 65, "message": f"Found {len(currencies_needed)} distinct currencies: {', '.join(currencies_needed)}"})

        # Sync fx_rate table from ECB for every currency in range. This is
        # idempotent and cheap — it only fetches what's missing past the
        # highest existing rate_date per currency. We stream per-currency
        # progress so the user can see the sync isn't stuck.
        # Skipped under db_only — no external ECB calls; rely on whatever
        # FX rows are already in the DB (gaps surface as the existing
        # "no FX history for X" warning during conversion).
        if req.db_only:
            yield _emit({"type": "progress", "pct": 65, "message": "DB-only mode: skipping ECB FX sync, using cached FX rates"})
        else:
            yield _emit({"type": "progress", "pct": 65, "message": f"Syncing FX rates from ECB (through {price_end})..."})
            yield _keepalive()

            fx_progress_q: _queue.Queue = _queue.Queue()
            fx_done = [0]
            fx_total = len(currencies_needed)

            def _on_fx_progress(code: str, status: dict):
                fx_done[0] += 1
                fx_progress_q.put({
                    "code": code,
                    "done": fx_done[0],
                    "total": fx_total,
                    "status": status.get("status"),
                })

            fx_task = asyncio.create_task(asyncio.to_thread(
                sync_fx_rates_to_db, supabase, currencies_needed, price_start, price_end,
                on_progress=_on_fx_progress,
            ))
            while not fx_task.done():
                drained = []
                while True:
                    try:
                        drained.append(fx_progress_q.get_nowait())
                    except _queue.Empty:
                        break
                if drained:
                    latest = drained[-1]
                    pct = round(latest["done"] / max(1, latest["total"]) * 100)
                    yield _emit({
                        "type": "progress",
                        "pct": 65,
                        "message": f"  FX sync {latest['done']}/{latest['total']} ≈ {pct}% (latest: {latest['code']} → {latest['status']})",
                    })
                await asyncio.sleep(0.15)
            fx_sync = await fx_task
            synced_codes = sorted(c for c, s in fx_sync.items() if s.get("status") == "synced")
            cached_codes = sorted(c for c, s in fx_sync.items() if s.get("status") == "cached")
            failed_codes = sorted(c for c, s in fx_sync.items() if s.get("status") == "error")
            nodata_codes = sorted(c for c, s in fx_sync.items() if s.get("status") == "no_data")
            total_rows = sum(s.get("rows", 0) for s in fx_sync.values())
            yield _emit({
                "type": "progress",
                "pct": 65,
                "message": (
                    f"FX sync done: {len(synced_codes)} updated ({total_rows:,} rows), "
                    f"{len(cached_codes)} already current, "
                    f"{len(failed_codes)} failed, {len(nodata_codes)} no_data"
                ),
            })
            if failed_codes:
                for code in failed_codes:
                    err = fx_sync[code].get("error", "unknown")
                    yield _emit({"type": "warning", "scope": "fx", "message": f"FX sync failed for {code}: {err}"})
            if nodata_codes:
                _ccy_names = {
                    "AED": "UAE Dirham", "ARS": "Argentine Peso", "AUD": "Australian Dollar",
                    "BRL": "Brazilian Real", "CAD": "Canadian Dollar", "CHF": "Swiss Franc",
                    "CLP": "Chilean Peso", "CNY": "Chinese Yuan", "COP": "Colombian Peso",
                    "CZK": "Czech Koruna", "DKK": "Danish Krone", "EGP": "Egyptian Pound",
                    "EUR": "Euro", "GBP": "British Pound", "GBX": "British Penny",
                    "HKD": "Hong Kong Dollar", "HUF": "Hungarian Forint", "IDR": "Indonesian Rupiah",
                    "ILS": "Israeli Shekel", "INR": "Indian Rupee", "ISK": "Icelandic Krona",
                    "JPY": "Japanese Yen", "KRW": "South Korean Won", "MXN": "Mexican Peso",
                    "MYR": "Malaysian Ringgit", "NOK": "Norwegian Krone", "NZD": "New Zealand Dollar",
                    "PEN": "Peruvian Sol", "PHP": "Philippine Peso", "PKR": "Pakistani Rupee",
                    "PLN": "Polish Zloty", "QAR": "Qatari Riyal", "RON": "Romanian Leu",
                    "RUB": "Russian Ruble", "SAR": "Saudi Riyal", "SEK": "Swedish Krona",
                    "SGD": "Singapore Dollar", "THB": "Thai Baht", "TRY": "Turkish Lira",
                    "TWD": "Taiwan Dollar", "USD": "US Dollar", "VND": "Vietnamese Dong",
                    "ZAR": "South African Rand",
                }
                labeled = ", ".join(
                    f"{c} ({_ccy_names[c]})" if c in _ccy_names else c for c in nodata_codes
                )
                yield _emit({
                    "type": "warning",
                    "scope": "fx",
                    "message": f"No FX data returned for: {labeled} (ECB may not cover these)",
                })

        yield _emit({"type": "progress", "pct": 65, "message": f"Loading FX rates ({price_start} to {price_end}) for {len(currencies_needed)} currencies..."})
        yield _keepalive()
        fx_rates = await asyncio.to_thread(
            load_fx_rates, supabase, currencies_needed, price_start, price_end,
        )
        loaded_codes = [c for c, s in fx_rates.items() if s is not None and not s.empty]
        missing_codes = sorted(set(currencies_needed) - set(loaded_codes))
        yield _emit({"type": "progress", "pct": 65, "message": f"FX rates loaded for {len(loaded_codes)} currencies"})
        if missing_codes:
            yield _emit({
                "type": "warning",
                "scope": "fx",
                "message": f"No FX history for: {', '.join(missing_codes)} — companies on those currencies will be dropped",
            })

        yield _emit({"type": "progress", "pct": 65, "message": f"Converting {len(prices_df):,} price rows to EUR..."})
        yield _keepalive()
        prices_local_df = prices_df
        prices_df, fx_stats = await asyncio.to_thread(
            convert_prices_to_eur, prices_df, company_currency, fx_rates,
        )
        yield _emit({
            "type": "progress",
            "pct": 65,
            "message": (
                f"FX done: {fx_stats['converted_rows']:,} rows converted "
                f"({', '.join(fx_stats['converted_currencies']) or 'none'}), "
                f"{fx_stats['passthrough_rows']:,} already EUR, "
                f"{fx_stats['dropped_no_currency']:,} dropped (no currency), "
                f"{fx_stats['dropped_no_fx']:,} dropped (no FX rate)"
            ),
        })
        if fx_stats["missing_currencies"]:
            yield _emit({
                "type": "warning",
                "scope": "fx",
                "message": f"Currencies with no FX series in date range: {', '.join(fx_stats['missing_currencies'])}",
            })

        if prices_df.empty:
            yield _emit({"type": "error", "message": "No price data left after FX conversion."})
            return

        # Audit price coverage: flag universe companies with zero or sparse price rows
        _price_counts = prices_df.groupby("company_id").size().to_dict() if not prices_df.empty else {}
        _universe_symbol = {
            int(r["company_id"]): f"{r.get('gurufocus_exchange') or '?'}:{r['gurufocus_ticker']}"
            for _, r in universe_df.iterrows()
        }
        _universe_name = {
            int(r["company_id"]): r.get("company_name") or ""
            for _, r in universe_df.iterrows()
        }
        def _label(cid: int) -> str:
            sym = _universe_symbol.get(int(cid), str(cid))
            name = _universe_name.get(int(cid), "")
            return f"{sym} ({name})" if name else sym
        _no_price = [cid for cid in company_ids if _price_counts.get(int(cid), 0) == 0]
        _sparse_price = [cid for cid in company_ids if 0 < _price_counts.get(int(cid), 0) < 20]

        # Group no-price companies by exchange. An exchange where every
        # universe company has zero price rows is almost certainly
        # unsubscribed on GuruFocus (or fully blocked) — surface it
        # separately from one-off gaps so the user can tell the difference.
        _universe_exchange = {
            int(r["company_id"]): r.get("gurufocus_exchange") or "UNKNOWN"
            for _, r in universe_df.iterrows()
        }
        _exchange_totals: dict[str, int] = {}
        _exchange_no_price: dict[str, int] = {}
        for cid in company_ids:
            exch = _universe_exchange.get(int(cid), "UNKNOWN")
            _exchange_totals[exch] = _exchange_totals.get(exch, 0) + 1
            if _price_counts.get(int(cid), 0) == 0:
                _exchange_no_price[exch] = _exchange_no_price.get(exch, 0) + 1
        _unsubscribed_exchanges = sorted(
            exch for exch, no_price in _exchange_no_price.items()
            if _exchange_totals.get(exch, 0) > 0 and no_price == _exchange_totals[exch]
        )
        if _unsubscribed_exchanges:
            parts = [f"{exch}({_exchange_no_price[exch]})" for exch in _unsubscribed_exchanges]
            total_unsub = sum(_exchange_no_price[e] for e in _unsubscribed_exchanges)
            yield _emit({
                "type": "info",
                "scope": "prices",
                "message": f"Unsubscribed/blocked exchanges (expected to have no data): {', '.join(parts)} — {total_unsub} companies",
            })

        # Remaining no-price cases: exchanges where some companies have
        # data but specific tickers don't — true one-off gaps.
        _no_price_gap = [
            cid for cid in _no_price
            if _universe_exchange.get(int(cid), "UNKNOWN") not in _unsubscribed_exchanges
        ]
        if _no_price_gap:
            sample = ", ".join(_label(int(c)) for c in _no_price_gap[:10])
            more = f" (+{len(_no_price_gap) - 10} more)" if len(_no_price_gap) > 10 else ""
            yield _emit({"type": "warning", "scope": "prices", "message": f"{len(_no_price_gap)} companies on subscribed exchanges have NO price data: {sample}{more}"})
        if _sparse_price:
            sample = ", ".join(
                f"{_label(int(c))}[{_price_counts.get(int(c), 0)} rows]" for c in _sparse_price[:10]
            )
            more = f" (+{len(_sparse_price) - 10} more)" if len(_sparse_price) > 10 else ""
            yield _emit({"type": "warning", "scope": "prices", "message": f"{len(_sparse_price)} companies have < 20 price rows (insufficient for signals): {sample}{more}"})

        # Load volumes from DB — same parallel-load + streamed-progress pattern
        # as prices.
        yield _emit({"type": "progress", "pct": 66, "message": "Loading volumes from DB..."})
        yield _keepalive()

        volumes_progress_q: _queue.Queue = _queue.Queue()

        def _on_volumes_progress(rows_so_far: int, page_num: int, chunks_done: int = 0, chunks_total: int = 0):
            volumes_progress_q.put({
                "rows": rows_so_far,
                "page": page_num,
                "chunks_done": chunks_done,
                "chunks_total": chunks_total,
            })

        volumes_task = asyncio.create_task(asyncio.to_thread(
            load_all_volumes, supabase, company_ids, price_start, price_end,
            on_progress=_on_volumes_progress,
        ))

        def _fmt_v_progress(p: dict) -> str:
            ct = p.get("chunks_total", 0)
            cd = p.get("chunks_done", 0)
            pct_str = f" ≈ {round(cd / ct * 100)}%" if ct else ""
            return f"  Loaded {p['rows']:,} volume rows ({cd}/{ct} chunks{pct_str})..."

        last_emitted_vpage = 0
        while not volumes_task.done():
            drained = []
            while True:
                try:
                    drained.append(volumes_progress_q.get_nowait())
                except _queue.Empty:
                    break
            if drained:
                latest = drained[-1]
                if latest["page"] - last_emitted_vpage >= PROGRESS_THROTTLE:
                    last_emitted_vpage = latest["page"]
                    yield _emit({"type": "progress", "pct": 66, "message": _fmt_v_progress(latest)})
            await asyncio.sleep(0.1)
        final_v = None
        while True:
            try:
                final_v = volumes_progress_q.get_nowait()
            except _queue.Empty:
                break
        if final_v is not None and final_v["page"] != last_emitted_vpage:
            yield _emit({"type": "progress", "pct": 67, "message": _fmt_v_progress(final_v)})

        volumes_df = await volumes_task
        n_vol = volumes_df["company_id"].nunique() if not volumes_df.empty else 0
        yield _emit({"type": "progress", "pct": 67, "message": f"Loaded {len(volumes_df):,} volume records for {n_vol} companies"})

        # Audit volume coverage. Companies on unsubscribed exchanges (already
        # flagged in the prices info message) are expected to have no volume
        # either, so filter them out of the warning set to avoid noise.
        _vol_counts = volumes_df.groupby("company_id").size().to_dict() if not volumes_df.empty else {}
        _no_vol_all = [cid for cid in company_ids if _vol_counts.get(int(cid), 0) == 0]
        _sparse_vol = [cid for cid in company_ids if 0 < _vol_counts.get(int(cid), 0) < 20]
        _no_vol_gap = [
            cid for cid in _no_vol_all
            if _universe_exchange.get(int(cid), "UNKNOWN") not in _unsubscribed_exchanges
        ]
        if _no_vol_gap:
            sample = ", ".join(_label(int(c)) for c in _no_vol_gap[:10])
            more = f" (+{len(_no_vol_gap) - 10} more)" if len(_no_vol_gap) > 10 else ""
            yield _emit({"type": "warning", "scope": "volumes", "message": f"{len(_no_vol_gap)} companies on subscribed exchanges have NO volume data — volume signals will be skipped for them: {sample}{more}"})
        if _sparse_vol:
            sample = ", ".join(
                f"{_label(int(c))}[{_vol_counts.get(int(c), 0)} rows]" for c in _sparse_vol[:10]
            )
            more = f" (+{len(_sparse_vol) - 10} more)" if len(_sparse_vol) > 10 else ""
            yield _emit({"type": "warning", "scope": "volumes", "message": f"{len(_sparse_vol)} companies have < 20 volume rows: {sample}{more}"})

        # Self-heal: for any subscribed-exchange company missing prices or
        # volumes, re-run the ingest pipeline (cache check → API fetch → DB
        # load) and merge the recovered rows back into the in-memory frames.
        # This is a no-op in steady state — the gap sets are empty when
        # everything is already loaded — and only fires for genuinely missing
        # data (empty Storage JSONs, failed prior loads, new tickers, etc.).
        #
        # When the user explicitly asks for fresh data on a current_portfolio
        # run (force_recompute=True), also retry every company whose pass-1
        # ensure call hit a transient API failure (source == "stale_cache").
        # This is the per-company pass-1 outcome, not a re-derivation of
        # "stale" from the bulk frame — pass-1 already runs the same
        # is_daily_data_fresh predicate, so a successful "api" outcome cannot
        # produce a stale row in the audit. Only transient API failures need
        # a second attempt.
        _stale_cids: set[int] = set()
        if req.mode == "current_portfolio" and req.force_recompute:
            company_id_set = {int(c) for c in company_ids}
            _stale_cids = {
                cid for cid in pass1_transient
                if cid in company_id_set
                and _universe_exchange.get(cid, "UNKNOWN") not in _unsubscribed_exchanges
            }
            if _stale_cids:
                yield _emit({
                    "type": "info",
                    "scope": "self-heal",
                    "message": f"Force-recompute: {len(_stale_cids)} companies hit transient API errors during pass 1 — retrying.",
                })

        gap_cids = sorted(set(_no_price_gap) | set(_no_vol_gap) | _stale_cids)
        if req.db_only and gap_cids:
            # Don't refetch under db_only — surface the gap as a non-fatal
            # warning so the user knows some companies will be filtered out
            # of the universe by the staleness guard in signals.py.
            sample = ", ".join(_label(int(c)) for c in gap_cids[:8])
            more = f" (+{len(gap_cids) - 8} more)" if len(gap_cids) > 8 else ""
            yield _emit({
                "type": "warning",
                "scope": "data",
                "message": f"DB-only mode: {len(gap_cids)} companies have missing price/volume data and will be excluded from this run: {sample}{more}",
            })
            gap_cids = []
        if gap_cids:
            yield _emit({"type": "progress", "pct": 67, "message": f"Self-heal: refetching missing data for {len(gap_cids)} companies on subscribed exchanges..."})
            yield _keepalive()

            ticker_lookup = {
                int(r["company_id"]): str(r["gurufocus_ticker"])
                for _, r in universe_df.iterrows()
            }
            exchange_lookup = {
                int(r["company_id"]): str(r.get("gurufocus_exchange") or "")
                for _, r in universe_df.iterrows()
            }

            heal_progress_q: _queue.Queue = _queue.Queue()

            def _on_heal_progress(cid, status, msg):
                heal_progress_q.put({"cid": cid, "status": status, "msg": msg})

            heal_task = asyncio.create_task(asyncio.to_thread(
                self_heal_missing_data,
                supabase, gap_cids, ticker_lookup, exchange_lookup,
                on_progress=_on_heal_progress,
            ))

            done_count = 0
            while not heal_task.done():
                drained = []
                while True:
                    try:
                        drained.append(heal_progress_q.get_nowait())
                    except _queue.Empty:
                        break
                if drained:
                    done_count += len(drained)
                    yield _emit({"type": "progress", "pct": 67, "message": f"  Self-heal: {done_count}/{len(gap_cids)} companies processed..."})
                await asyncio.sleep(0.2)

            heal_result = await heal_task
            healed_cids = heal_result["healed_company_ids"]
            heal_stats = heal_result["stats"]

            heal_msg_parts = [
                f"{heal_stats['prices_fetched']} price fetches",
                f"{heal_stats['volumes_fetched']} volume fetches",
            ]
            if heal_stats["forbidden_exchanges"]:
                heal_msg_parts.append(f"forbidden exchanges (skipped): {', '.join(heal_stats['forbidden_exchanges'])}")
            if heal_stats["errors"]:
                heal_msg_parts.append(f"{heal_stats['errors']} errors")
            yield _emit({
                "type": "info",
                "scope": "self-heal",
                "message": f"Self-heal complete: {' · '.join(heal_msg_parts)}.",
            })

            if healed_cids:
                yield _emit({"type": "progress", "pct": 67, "message": f"Re-loading {len(healed_cids)} healed companies into memory..."})
                yield _keepalive()
                new_local = await asyncio.to_thread(
                    load_all_prices, supabase, healed_cids, price_start, price_end,
                )
                if not new_local.empty:
                    new_eur, _new_fx = await asyncio.to_thread(
                        convert_prices_to_eur, new_local, company_currency, fx_rates,
                    )
                    prices_local_df = pd.concat(
                        [prices_local_df, new_local], ignore_index=True
                    ).sort_values(["company_id", "target_date"]).reset_index(drop=True)
                    if not new_eur.empty:
                        prices_df = pd.concat(
                            [prices_df, new_eur], ignore_index=True
                        ).sort_values(["company_id", "target_date"]).reset_index(drop=True)
                new_volumes = await asyncio.to_thread(
                    load_all_volumes, supabase, healed_cids, price_start, price_end,
                )
                if not new_volumes.empty:
                    volumes_df = pd.concat(
                        [volumes_df, new_volumes], ignore_index=True
                    ).sort_values(["company_id", "target_date"]).reset_index(drop=True)

        # Build universe snapshot once — used by both single-run and variants
        # paths. Variants reuse this for every per-variant `variant_result`
        # event so the client gets the same shape it would from a single run.
        # `_norm_str` handles None / NaN explicitly: pandas Series .get("col",
        # default) only falls through to the default when the COLUMN is
        # missing, not when the cell is None or NaN. Without this normalization
        # an exchange link that's absent in the DB ends up as the literal
        # string "None" or "nan" in the JSON payload, which (a) breaks the
        # frontend's GuruFocus URL helper (US-vs-non-US classifier sees
        # "None" as non-US and produces "/stock/None:TICKER/summary") and
        # (b) renders "(None)" or "(nan)" in the holdings table.
        def _norm_str(val) -> str:
            if val is None:
                return ""
            try:
                if pd.isna(val):
                    return ""
            except (TypeError, ValueError):
                pass
            return str(val)

        universe_snapshot = [
            {
                "company_id": int(row["company_id"]),
                "ticker": _norm_str(row.get("gurufocus_ticker")),
                "exchange": _norm_str(row.get("gurufocus_exchange")),
                "company_name": _norm_str(row.get("company_name")),
                "sector": _norm_str(row.get("sector")),
                "country": _norm_str(row.get("country")),
            }
            for _, row in universe_df.iterrows()
        ]

        # ── Variants sweep path ─────────────────────────────────────────────
        # All data is loaded; iterate variants, running just the backtest
        # computation per (frequency × strategy_type). Each variant emits its
        # own `variant_start` / `variant_result` / `variant_error` event so
        # the frontend can update the variants table row-by-row. The data
        # frames (prices_df, prices_local_df, volumes_df, fx_rates,
        # company_currency, monthly_eligible, universe_df) are reused
        # verbatim — frequency / strategy_type only affect the rebalance
        # date generator and portfolio construction, not the underlying
        # data, so this is the cheap loop the sweep should always have been.
        if req.variants:
            # Pre-build the sweep-shared inputs ONCE: price/volume indices
            # plus a single signal panel covering the *union* of every
            # variant's cutoff dates. The per-company rolling signal scan
            # is the dominant cost of `_prepare_backtest`, and it's
            # identical across every variant in the sweep — running it
            # N times for N variants was wasting ~(N-1) full passes over
            # the universe's price history. Skip the precompute when
            # `selection_mode == 'all'` since the all-universe path
            # doesn't consult signals.
            shared_backtest = None
            if req.selection_mode != "all":
                _start_d = date.fromisoformat(req.start_date)
                _end_d = date.fromisoformat(req.end_date)
                _union_cutoffs: set[date] = set()
                for _v in req.variants:
                    try:
                        _periods = _generate_rebalance_dates(
                            _start_d, _end_d, _v.frequency, prices_df,
                        )
                    except Exception:
                        # If a variant can't even produce dates, skip it
                        # here — the per-variant loop below will surface
                        # the error properly.
                        continue
                    if len(_periods) >= 2:
                        # Include every rebalance date (not just periods[:-1])
                        # — the last entry becomes the open-period entry in
                        # run_backtest and needs signals at that cutoff too.
                        _union_cutoffs.update(_periods)
                if _union_cutoffs:
                    yield _emit({
                        "type": "progress",
                        "pct": 68,
                        "message": f"Precomputing signal panel over {len(_union_cutoffs)} union cutoffs (shared by all {len(req.variants)} variants)...",
                    })
                    yield _keepalive()
                    shared_backtest = await asyncio.to_thread(
                        build_shared_backtest_inputs,
                        prices_df=prices_df,
                        universe_df=universe_df,
                        volumes_df=volumes_df,
                        prices_local_df=prices_local_df,
                        monthly_eligible=monthly_eligible,
                        union_cutoffs=sorted(_union_cutoffs),
                    )

            # Sector-ETF mode: prefetch benchmark prices once for the whole
            # sweep so every variant shares them (same shape as the
            # single-run path above; pulled out to its own block so the
            # variant loop can reference the names).
            variant_benchmark_price_index: dict[int, pd.Series] | None = None
            variant_benchmark_meta: dict[int, tuple[str, str]] | None = None
            if req.selection_mode == "sector_etf" and req.sector_etfs:
                _bm_ids = sorted({int(v) for v in req.sector_etfs.values()})
                if _bm_ids:
                    _meta_resp = await asyncio.to_thread(
                        lambda: supabase.table("benchmark")
                        .select("benchmark_id, ticker, name")
                        .in_("benchmark_id", _bm_ids)
                        .execute()
                    )
                    variant_benchmark_meta = {
                        int(r["benchmark_id"]): (r["ticker"], r["name"])
                        for r in (_meta_resp.data or [])
                    }
                    # Paginate per benchmark_id to bypass Supabase's silent
                    # 1000-row default (see single-run path for the full
                    # story). Without this, all variants share an empty
                    # post-1999 price series and emit 0% returns.
                    _px_rows: list[dict] = []
                    _page_size = 1000
                    for _bid in _bm_ids:
                        _offset = 0
                        while True:
                            _px_resp = await asyncio.to_thread(
                                lambda b=_bid, o=_offset: supabase.table("benchmark_price")
                                .select("benchmark_id, target_date, price")
                                .eq("benchmark_id", b)
                                .order("target_date")
                                .range(o, o + _page_size - 1)
                                .execute()
                            )
                            _batch = _px_resp.data or []
                            _px_rows.extend(_batch)
                            if len(_batch) < _page_size:
                                break
                            _offset += _page_size
                    variant_benchmark_price_index = {}
                    if _px_rows:
                        _df_bm = pd.DataFrame(_px_rows)
                        for _bid, _group in _df_bm.groupby("benchmark_id"):
                            variant_benchmark_price_index[int(_bid)] = pd.Series(
                                _group["price"].values,
                                index=pd.DatetimeIndex(_group["target_date"]),
                                dtype="float64",
                            ).sort_index()

            for v_idx, vspec in enumerate(req.variants):
                variant_key = f"{vspec.frequency}__{vspec.strategy_type}"
                yield _emit({"type": "variant_start", "variant_key": variant_key})
                yield _keepalive()

                # Per-variant config: same base, overridden frequency + strategy.
                # Reject long_short + random/all at the variant level —
                # the same combination check the single-run path applies,
                # but per-row.
                if vspec.strategy_type == "long_short" and req.selection_mode in ("random", "all"):
                    yield _emit({
                        "type": "variant_error",
                        "variant_key": variant_key,
                        "message": f"long_short is not supported with selection_mode='{req.selection_mode}'",
                    })
                    continue

                v_config = BacktestConfig.from_dict({
                    "start_date": req.start_date,
                    "end_date": req.end_date,
                    "signal_weights": req.signal_weights or {s["key"]: s["default_weight"] for s in PRICE_SIGNAL_DEFS},
                    "category_weights": req.category_weights,
                    "top_n_sectors": req.top_n_sectors,
                    "top_n_per_sector": req.top_n_per_sector,
                    "selection_mode": req.selection_mode,
                    "random_seed": req.random_seed,
                    "rebalance_frequency": vspec.frequency,
                    "strategy_type": vspec.strategy_type,
                    "sector_etfs": req.sector_etfs,
                })

                v_progress_queue: _queue.Queue = _queue.Queue()
                v_result_holder: list = []
                v_error_holder: list = []

                def _v_send_event(event_type: str, **kwargs):
                    v_progress_queue.put({"type": event_type, **kwargs})

                # Per-variant `_BacktestPrepared` built from the
                # sweep-shared signal panel + indices. `prepared` already
                # carries the variant's frequency, periods, and the
                # filtered panel; `run_backtest` short-circuits its own
                # `_prepare_backtest` call when `prepared` is supplied.
                v_prepared = None
                if shared_backtest is not None:
                    try:
                        v_prepared = prepare_variant_from_shared(
                            shared=shared_backtest,
                            start_date=date.fromisoformat(req.start_date),
                            end_date=date.fromisoformat(req.end_date),
                            frequency=vspec.frequency,
                            prices_df=prices_df,
                        )
                    except Exception as _prep_err:
                        # Fall through to the regular path which will
                        # raise the same error inside the variant thread
                        # so it surfaces as a per-variant error event.
                        v_prepared = None
                        logging.getLogger(__name__).debug(
                            "[variants] prepare_variant_from_shared failed for %s: %s",
                            variant_key, _prep_err,
                        )

                def _v_run(cfg=v_config, prepared=v_prepared):
                    try:
                        if req.selection_mode == "random" and req.n_trials > 1:
                            # Multi-trial random repeats `run_backtest`
                            # under the hood; it builds its own prepared.
                            # The shared panel still helps if/when we
                            # wire it through here, but for now we just
                            # leave the per-trial path to use its own
                            # cache (which already shares prepared
                            # across trials of the SAME variant).
                            r = run_multi_trial_backtest(
                                cfg, prices_df, universe_df, req.n_trials, _v_send_event,
                                volumes_df=volumes_df,
                                monthly_eligible=monthly_eligible,
                                prices_local_df=prices_local_df,
                                company_currency=company_currency,
                            )
                        else:
                            r = run_backtest(
                                cfg, prices_df, universe_df, _v_send_event,
                                volumes_df=volumes_df,
                                monthly_eligible=monthly_eligible,
                                prices_local_df=prices_local_df,
                                company_currency=company_currency,
                                prepared=prepared,
                                benchmark_price_index=variant_benchmark_price_index,
                                benchmark_meta=variant_benchmark_meta,
                            )
                        v_result_holder.append(r)
                    except Exception as e:
                        v_error_holder.append(e)
                    finally:
                        v_progress_queue.put(None)

                yield _emit({
                    "type": "progress",
                    "pct": 68 + round((v_idx / max(1, len(req.variants))) * 32),
                    "message": f"[{variant_key}] running backtest computation ({v_idx + 1}/{len(req.variants)})...",
                })
                yield _keepalive()

                loop = asyncio.get_event_loop()
                loop.run_in_executor(None, _v_run)

                # Same keepalive-15s pattern as the single-run path so the
                # proxy doesn't kill the connection during long signal
                # computation phases.
                v_last_yield = time.monotonic()
                while True:
                    try:
                        evt = await asyncio.to_thread(v_progress_queue.get, timeout=0.2)
                    except Exception:
                        if time.monotonic() - v_last_yield >= 15.0:
                            yield _keepalive()
                            v_last_yield = time.monotonic()
                        continue
                    if evt is None:
                        break
                    if evt["type"] == "progress":
                        # Scale this variant's internal pct (0-100) into
                        # the overall sweep progress: each variant owns a
                        # 32/N slice of the [68, 100] band, and within
                        # the slice we advance proportionally to the
                        # variant's own progress event. Without this the
                        # bar was locked at the variant's start pct
                        # throughout its run, then frozen at
                        # `68 + ((N-1)/N)*32` after the last variant
                        # finished — e.g. 84% for N=8.
                        local_pct = float(evt.get("pct") or 0)
                        sweep_fraction = (v_idx + max(0.0, min(100.0, local_pct)) / 100.0) / max(1, len(req.variants))
                        yield _emit({
                            "type": "progress",
                            "pct": 68 + round(sweep_fraction * 32),
                            "message": f"[{variant_key}] {evt.get('message', '')}",
                        })
                    elif evt["type"] == "warning":
                        yield _emit({
                            "type": "warning",
                            "scope": evt.get("scope", "backtest"),
                            "message": f"[{variant_key}] {evt.get('message', '')}",
                        })
                    v_last_yield = time.monotonic()

                if v_error_holder:
                    yield _emit({
                        "type": "variant_error",
                        "variant_key": variant_key,
                        "message": f"{type(v_error_holder[0]).__name__}: {v_error_holder[0]}",
                    })
                    continue

                v_result_dict = v_result_holder[0].to_dict()
                yield _emit({
                    "type": "variant_result",
                    "variant_key": variant_key,
                    "data": v_result_dict,
                    "universe": universe_snapshot,
                })

            # Belt-and-suspenders 100% emit: even if the final variant
            # didn't deliver a closing progress event (e.g. it errored,
            # or was skipped because long_short+random is forbidden),
            # the user-facing progress bar lands at 100 before `done`.
            yield _emit({"type": "progress", "pct": 100, "message": f"Variants sweep complete ({len(req.variants)})"})
            yield _emit({"type": "done", "message": f"Variants sweep complete ({len(req.variants)})"})
            return

        # ── Single-run path ─────────────────────────────────────────────────
        # When selection_mode == "sector_etf", pre-fetch the prices for every
        # mapped benchmark ETF once and pass them through to run_backtest /
        # run_current_portfolio. We avoid the (cheap) fetch when the mode
        # doesn't need it.
        benchmark_price_index: dict[int, pd.Series] | None = None
        benchmark_meta: dict[int, tuple[str, str]] | None = None
        if req.selection_mode == "sector_etf" and req.sector_etfs:
            bm_ids = sorted({int(v) for v in req.sector_etfs.values()})
            if bm_ids:
                meta_resp = await asyncio.to_thread(
                    lambda: supabase.table("benchmark")
                    .select("benchmark_id, ticker, name")
                    .in_("benchmark_id", bm_ids)
                    .execute()
                )
                benchmark_meta = {
                    int(r["benchmark_id"]): (r["ticker"], r["name"])
                    for r in (meta_resp.data or [])
                }
                # Pull every price row per benchmark, paginating to defeat
                # Supabase's silent 1000-row limit. A single ETF since
                # 1998 has ~6,886 daily bars and 11 ETFs together exceed
                # 75k rows — a single .in_() query would truncate to the
                # earliest ~90 days per benchmark, and every entry/exit
                # lookup downstream would return None.
                px_rows: list[dict] = []
                page_size = 1000
                for bid in bm_ids:
                    offset = 0
                    while True:
                        px_resp = await asyncio.to_thread(
                            lambda b=bid, o=offset: supabase.table("benchmark_price")
                            .select("benchmark_id, target_date, price")
                            .eq("benchmark_id", b)
                            .order("target_date")
                            .range(o, o + page_size - 1)
                            .execute()
                        )
                        batch = px_resp.data or []
                        px_rows.extend(batch)
                        if len(batch) < page_size:
                            break
                        offset += page_size
                benchmark_price_index = {}
                # Build per-benchmark pd.Series. Same shape as `price_index`
                # so run_backtest's price-lookup helpers (_price_on_or_after,
                # _date_on_or_after) work without modification.
                if px_rows:
                    df_bm = pd.DataFrame(px_rows)
                    for bid, group in df_bm.groupby("benchmark_id"):
                        benchmark_price_index[int(bid)] = pd.Series(
                            group["price"].values,
                            index=pd.DatetimeIndex(group["target_date"]),
                            dtype="float64",
                        ).sort_index()

        # Run backtest with progress callback via queue for real-time streaming
        progress_queue: _queue.Queue = _queue.Queue()
        backtest_result_holder: list = []
        backtest_error_holder: list = []

        def send_event(event_type: str, **kwargs):
            progress_queue.put({"type": event_type, **kwargs})

        def _run_backtest():
            try:
                if req.mode == "current_portfolio":
                    r = run_current_portfolio(
                        config, prices_df, universe_df, send_event,
                        volumes_df=volumes_df,
                        monthly_eligible=monthly_eligible,
                        prices_local_df=prices_local_df,
                        company_currency=company_currency,
                    )
                elif req.selection_mode == "random" and req.n_trials > 1:
                    r = run_multi_trial_backtest(
                        config, prices_df, universe_df, req.n_trials, send_event,
                        volumes_df=volumes_df,
                        monthly_eligible=monthly_eligible,
                        prices_local_df=prices_local_df,
                        company_currency=company_currency,
                    )
                else:
                    r = run_backtest(config, prices_df, universe_df, send_event,
                        volumes_df=volumes_df,
                        monthly_eligible=monthly_eligible,
                        prices_local_df=prices_local_df,
                        company_currency=company_currency,
                        benchmark_price_index=benchmark_price_index,
                        benchmark_meta=benchmark_meta,
                    )
                backtest_result_holder.append(r)
            except Exception as e:
                backtest_error_holder.append(e)
            finally:
                progress_queue.put(None)  # sentinel

        yield _emit({"type": "progress", "pct": 68, "message": "Running backtest computation..."})
        yield _keepalive()

        loop = asyncio.get_event_loop()
        loop.run_in_executor(None, _run_backtest)

        # Stream progress events in real-time as the backtest runs. Emit a
        # keepalive comment every ~15s of silence so the proxy doesn't close
        # the connection during long signal-computation steps that produce no
        # visible events (current_portfolio on a wide universe can sit silent
        # for >30s between emissions).
        last_yield = time.monotonic()
        keepalive_interval = 15.0
        while True:
            try:
                evt = await asyncio.to_thread(progress_queue.get, timeout=0.2)
            except Exception:
                if time.monotonic() - last_yield >= keepalive_interval:
                    yield _keepalive()
                    last_yield = time.monotonic()
                continue
            if evt is None:
                break
            if evt["type"] == "progress":
                scaled_pct = 68 + round(evt.get("pct", 0) * 0.32)
                yield _emit({"type": "progress", "pct": scaled_pct, "message": evt.get("message", "")})
            elif evt["type"] == "warning":
                yield _emit({"type": "warning", "scope": evt.get("scope", "backtest"), "message": evt.get("message", "")})
            last_yield = time.monotonic()

        if backtest_error_holder:
            raise backtest_error_holder[0]
        result = backtest_result_holder[0]

        # universe_snapshot was built above (shared with the variants path).

        if req.mode == "current_portfolio":
            payload = result.to_dict()
            hash_ = _strategy_hash(req)
            payload["strategy_hash"] = hash_
            cfg_dump = req.model_dump()
            # Persist snapshot + per-day rows so subsequent loads are instant.
            # Failures are surfaced as non-fatal warnings; the user still sees
            # the freshly computed result.
            try:
                snapshot_id = await asyncio.to_thread(
                    _save_current_picks_snapshot,
                    payload,
                    cfg_dump,
                    "manual",
                    hash_,
                )
                payload["snapshot_id"] = snapshot_id
            except Exception as e:
                yield _emit({"type": "warning", "scope": "snapshot", "message": f"Could not persist snapshot: {type(e).__name__}: {e}"})
            try:
                await asyncio.to_thread(
                    _persist_daily_picks,
                    hash_,
                    cfg_dump,
                    payload.get("daily_picks") or [],
                )
            except Exception as e:
                yield _emit({"type": "warning", "scope": "daily-picks", "message": f"Could not persist daily picks: {type(e).__name__}: {e}"})
            try:
                payload["daily_picks_history"] = await asyncio.to_thread(_fetch_daily_picks_history, hash_)
            except Exception as e:
                payload["daily_picks_history"] = payload.get("daily_picks") or []
                yield _emit({"type": "warning", "scope": "daily-picks", "message": f"Could not fetch daily picks history: {type(e).__name__}: {e}"})
            yield _emit({"type": "current_portfolio", "data": payload, "universe": universe_snapshot})
            yield _emit({"type": "done", "message": "Current portfolio computed"})
        else:
            result_dict = result.to_dict()
            yield _emit({"type": "result", "data": result_dict, "universe": universe_snapshot})
            # Cache the result for replay. Failures are non-fatal — the user
            # already received their result; we just won't have it cached.
            try:
                await asyncio.to_thread(
                    _save_backtest_cache,
                    _backtest_strategy_hash(req),
                    req.model_dump(),
                    {"result": result_dict, "universe": universe_snapshot},
                )
            except Exception as e:
                yield _emit({"type": "warning", "scope": "cache", "message": f"Could not cache backtest: {type(e).__name__}: {e}"})
            yield _emit({"type": "done", "message": "Backtest complete"})

    except Exception as e:
        yield _emit({"type": "error", "message": str(e)})


@app.post("/api/momentum/backtest")
async def momentum_backtest(req: BacktestRequest):
    return StreamingResponse(
        _momentum_backtest_stream(req),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ─── Backtest save / load ────────────────────────────────────────────────────


class SaveBacktestRequest(BaseModel):
    name: str
    config: dict
    # Single-run shape — provide summary + monthly_records.
    summary: dict | None = None
    monthly_records: list | None = None
    # Variant-bundle shape — provide a list of variants, each
    # {key, label, summary, monthly_records}. When present,
    # `summary` / `monthly_records` are ignored and the row is stored as
    # `result = {kind: "variants", variants, universe}`.
    variants: list | None = None
    universe: list  # [{company_id, ticker, exchange, company_name, sector}]


@app.post("/api/momentum/backtests")
async def save_backtest(req: SaveBacktestRequest):
    """Save a backtest run to the database. Accepts either a single-run
    shape (summary + monthly_records) or a variant bundle (variants[])."""
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
    row = {
        "name": req.name.strip(),
        "config": req.config,
        "result": result_blob,
    }
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run").insert(row).execute()
    )
    if not resp.data:
        raise HTTPException(500, "Failed to save backtest")
    return resp.data[0]


@app.get("/api/momentum/backtests")
async def list_backtests():
    """List saved backtests (metadata only — no result blob).

    The frontend dropdown only consumes (run_id, name, created_at) — the
    full payload is fetched on demand via /api/momentum/backtests/{run_id}
    when the user actually loads a run. Returning the result blob here
    instead inflated the response to >50 MB for ~13 saved runs (each
    variant bundle carries hundreds of period records with holdings),
    which the frontend couldn't render and the user saw as broken rows.
    """
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .select("run_id, name, created_at")
        .order("created_at", desc=True)
        .execute()
    )
    return resp.data or []


@app.get("/api/momentum/backtests/{run_id}")
async def load_backtest(run_id: int):
    """Load a full backtest run."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .select("*")
        .eq("run_id", run_id)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Backtest not found")
    return resp.data[0]


@app.delete("/api/momentum/backtests/{run_id}")
async def delete_backtest(run_id: int):
    """Delete a saved backtest run."""
    resp = await asyncio.to_thread(
        lambda: supabase.table("backtest_run")
        .delete()
        .eq("run_id", run_id)
        .execute()
    )
    if not resp.data:
        raise HTTPException(404, "Backtest not found")
    return {"ok": True}


class RenameBacktestRequest(BaseModel):
    name: str


@app.patch("/api/momentum/backtests/{run_id}")
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


# ─── Current Picks snapshots ─────────────────────────────────────────────────


@app.get("/api/momentum/current-picks")
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


@app.get("/api/momentum/current-picks/{snapshot_id}")
async def get_current_picks(snapshot_id: int):
    """Load one full snapshot, including holdings."""
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


@app.delete("/api/momentum/current-picks/{snapshot_id}")
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


@app.patch("/api/momentum/current-picks/{snapshot_id}")
async def rename_current_picks(snapshot_id: int, req: RenameCurrentPicksRequest):
    """Set or clear a custom name on a current-picks snapshot."""
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
    """Recompute MTD (forward_return_pct) for already-picked holdings using the
    latest available prices. Returns (updated_holdings, latest_price_date)."""
    company_ids = [int(h["company_id"]) for h in holdings if h.get("company_id") is not None]
    if not company_ids:
        return holdings, None

    # Freshen GuruFocus prices for the held companies before reading the DB.
    # Bounded by the number of held names (~20–30), and each call has its own
    # DB-freshness fast path so it's a no-op for any company whose latest
    # close already covers today. This is what unblocks the "daily picks last
    # day is 0% because we never fetched the next-day close" case.
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
            # bug went unnoticed — surface it in logs so the next regression
            # is at least googleable.
            logging.getLogger(__name__).warning(
                "[refresh_mtd] ensure_prices_for_company failed for cid=%s ticker=%s exch=%s: %s: %s",
                cid, m[0], m[1], type(e).__name__, e,
            )

    if company_meta:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(8, len(company_meta))) as pool:
            list(pool.map(_ensure, list(company_meta.keys())))

    # Look back ~14 days from today — the latest close should always land
    # inside that window even after long weekends / holidays.
    from datetime import timedelta
    today = date.today()
    start = today - timedelta(days=14)

    prices_local_df = load_all_prices(supabase, company_ids, start, today)
    if prices_local_df.empty:
        return holdings, None

    company_currency = load_company_currency(supabase, company_ids)
    currencies = sorted({c for c in company_currency.values() if c})
    fx_rates = load_fx_rates(supabase, currencies, start, today) if currencies else {}
    prices_eur_df, _ = convert_prices_to_eur(prices_local_df, company_currency, fx_rates)

    # Index latest price per company
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


@app.post("/api/momentum/current-picks/{snapshot_id}/refresh-mtd")
async def refresh_current_picks_mtd(snapshot_id: int):
    """Recompute MTD on a stored snapshot using the latest available prices.
    Does NOT mutate the stored snapshot — this is a read-side recomputation."""
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


@app.post("/api/momentum/current-picks/cron")
async def cron_current_picks(req: BacktestRequest, x_cron_secret: str = Header(default="")):
    """Cron entry point. Forces mode=current_portfolio, runs the full
    compute, and persists with triggered_by='auto'.

    Auth: requires the X-Cron-Secret header to match the CRON_SECRET env var.
    """
    expected = os.environ.get("CRON_SECRET", "")
    if not expected:
        raise HTTPException(500, "CRON_SECRET env var is not set on the server")
    if x_cron_secret != expected:
        raise HTTPException(401, "Invalid cron secret")

    # Force the right mode regardless of what the caller sent. Cron always
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
    # that with an 'auto' row by upserting a fresh one and deleting the manual
    # one we just created.
    auto_id = None
    try:
        # The SSE path stored its snapshot_id on the payload; remove it first.
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


# ===========================================================================
# UNIVERSE SCREENING
# ===========================================================================

@app.get("/api/universe/criteria")
async def universe_criteria():
    """Return the list of LongEquity quality criteria."""
    return [
        {
            "key": key,
            "label": label,
            "description": CRITERIA_DESCRIPTIONS.get(key, ""),
            "min_years": CRITERIA_MIN_YEARS.get(key, 1),
        }
        for key, label in CRITERIA_NAMES
    ]


class ScreenRequest(BaseModel):
    as_of_year: str | None = None  # e.g. "2025-12"
    force_refresh: bool = False


@app.post("/api/universe/screen")
async def universe_screen(body: ScreenRequest = ScreenRequest()):
    """Screen all companies against LongEquity criteria. Returns SSE stream."""
    import queue

    as_of = body.as_of_year
    force = body.force_refresh

    def _run(q: queue.Queue):
        # Get all companies
        resp = supabase.table("company").select(
            "company_id, gurufocus_ticker, exchange_id, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))"
        ).limit(10000).execute()
        companies = []
        for c in (resp.data or []):
            exch_info = c.pop("gurufocus_exchange", None) or {}
            country_info = exch_info.pop("country", None) or {}
            c["gurufocus_exchange"] = exch_info.get("exchange_code")
            c["country"] = country_info.get("country_name")
            companies.append(c)
        label = f" as of {as_of}" if as_of else ""
        q.put(json.dumps({"type": "progress", "message": f"Found {len(companies)} companies to screen{label}."}))

        for event in screen_universe(supabase, companies, as_of_year=as_of, force_refresh=force):
            q.put(json.dumps(event))
        q.put(None)  # sentinel

    async def generate():
        yield ": keepalive\n\n"
        q: queue.Queue = queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, q)

        while True:
            try:
                msg = await asyncio.to_thread(q.get, timeout=0.15)
            except Exception:
                if task.done():
                    # Drain remaining messages
                    while not q.empty():
                        msg = q.get_nowait()
                        if msg is not None:
                            yield f"data: {msg}\n\n"
                    break
                continue
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


class BuildUniverseRequest(BaseModel):
    start_month: str  # "YYYY-MM"
    end_month: str    # "YYYY-MM"
    label: str = "default"
    max_companies: int = 5


@app.post("/api/universe/build")
async def universe_build(body: BuildUniverseRequest):
    """Build monthly universes for a date range and store in DB. Returns SSE stream."""
    import queue

    start = body.start_month
    end = body.end_month
    lbl = body.label
    max_co = body.max_companies

    def _run(q: queue.Queue):
        resp = supabase.table("company").select(
            "company_id, gurufocus_ticker, exchange_id, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))"
        ).limit(10000).execute()
        companies = []
        for c in (resp.data or []):
            exch_info = c.pop("gurufocus_exchange", None) or {}
            country_info = exch_info.pop("country", None) or {}
            c["gurufocus_exchange"] = exch_info.get("exchange_code")
            c["country"] = country_info.get("country_name")
            companies.append(c)

        for event in build_and_store_universes(supabase, companies, start, end, label=lbl, max_companies=max_co):
            q.put(json.dumps(event))
        q.put(None)

    async def generate():
        yield ": keepalive\n\n"
        q: queue.Queue = queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, q)

        while True:
            try:
                msg = await asyncio.to_thread(q.get, timeout=0.15)
            except Exception:
                if task.done():
                    while not q.empty():
                        msg = q.get_nowait()
                        if msg is not None:
                            yield f"data: {msg}\n\n"
                    break
                continue
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/api/universe/labels")
async def universe_labels():
    """List all universes with stats. Aggregation runs in Postgres via the
    `universe_full_stats` RPC so the endpoint is two round trips regardless of
    universe size."""
    def _run():
        u_rows = (
            supabase.table("universe")
            .select("universe_id, label, description, created_at, parent_universe_id, filter_config")
            .order("label")
            .execute()
            .data or []
        )
        stats_rows = supabase.rpc("universe_full_stats").execute().data or []
        stats_by_id: dict[int, dict] = {r["universe_id"]: r for r in stats_rows}

        label_by_id = {u["universe_id"]: u["label"] for u in u_rows}

        result = []
        for u in u_rows:
            uid = u["universe_id"]
            s = stats_by_id.get(uid, {})
            monthly = s.get("monthly_counts") or []
            sectors = s.get("sector_counts") or []

            total_rows = s.get("total_rows", 0)
            month_count = s.get("month_count", 0)
            avg_per_month = round(total_rows / month_count, 1) if month_count else 0

            parent_id = u.get("parent_universe_id")
            result.append({
                "universe_id": uid,
                "label": u["label"],
                "description": u.get("description"),
                "created_at": u.get("created_at"),
                "parent_universe_id": parent_id,
                "parent_label": label_by_id.get(parent_id) if parent_id else None,
                "filter_config": u.get("filter_config"),
                "is_derived": parent_id is not None,
                "start_month": s.get("start_month"),
                "end_month": s.get("end_month"),
                "month_count": month_count,
                "total_rows": total_rows,
                "unique_companies": s.get("unique_companies", 0),
                "unique_tickers": s.get("unique_tickers", 0),
                "avg_per_month": avg_per_month,
                "first_month_count": monthly[0]["count"] if monthly else 0,
                "last_month_count": monthly[-1]["count"] if monthly else 0,
                "monthly_counts": monthly,
                "sectors": sectors,
            })

        return result

    return await asyncio.to_thread(_run)


@app.get("/api/universe/months")
async def universe_months(label: str = "default"):
    """List stored universe months with counts for a label."""
    def _run():
        # Get universe_id for this label
        u_resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
        if not u_resp.data:
            return []
        universe_id = u_resp.data[0]["universe_id"]
        # Get distinct months with counts
        resp = supabase.table("universe_membership").select("target_month").eq("universe_id", universe_id).limit(10000).execute()
        rows = resp.data or []
        from collections import Counter
        counts = Counter(r["target_month"] for r in rows)
        return [{"target_month": m, "count": c} for m, c in sorted(counts.items())]

    return await asyncio.to_thread(_run)


@app.get("/api/universe/months/{month}")
async def universe_month_detail(month: str, label: str = "default"):
    """Get all companies for a specific month in a universe."""
    def _run():
        # Get universe_id
        u_resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
        if not u_resp.data:
            return []
        universe_id = u_resp.data[0]["universe_id"]

        resp = supabase.table("universe_membership").select(
            "company_id, universe_ticker, sector"
        ).eq("universe_id", universe_id).eq("target_month", month).limit(10000).execute()
        membership_rows = resp.data or []

        if not membership_rows:
            return []

        # Get company info
        cids = [r["company_id"] for r in membership_rows]
        company_map: dict[int, dict] = {}
        for i in range(0, len(cids), 50):
            batch = cids[i:i + 50]
            cr = supabase.table("company").select(
                "company_id, gurufocus_ticker, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))"
            ).in_("company_id", batch).execute()
            for c in (cr.data or []):
                exch_info = c.pop("gurufocus_exchange", None) or {}
                country_info = exch_info.pop("country", None) or {}
                c["gurufocus_exchange"] = exch_info.get("exchange_code")
                c["country"] = country_info.get("country_name")
                company_map[c["company_id"]] = c

        result = []
        for r in membership_rows:
            info = company_map.get(r["company_id"], {})
            result.append({
                "company_id": r["company_id"],
                "ticker": info.get("gurufocus_ticker", ""),
                "exchange": info.get("gurufocus_exchange", ""),
                "company_name": info.get("company_name", ""),
                "sector": r.get("sector", ""),
                "country": info.get("country", ""),
                "universe_ticker": r.get("universe_ticker", ""),
            })
        return result

    return await asyncio.to_thread(_run)


@app.delete("/api/universe/months/{month}")
async def universe_delete_month(month: str, label: str = "default"):
    """Delete a specific month's universe membership."""
    def _run():
        u_resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
        if not u_resp.data:
            return {"deleted": month}
        universe_id = u_resp.data[0]["universe_id"]
        supabase.table("universe_membership").delete().eq(
            "universe_id", universe_id
        ).eq("target_month", month).execute()
        return {"deleted": month}
    return await asyncio.to_thread(_run)


@app.delete("/api/universe/labels/{label}")
async def universe_delete_label(label: str):
    """Delete a universe and all its memberships.

    If the target is a base universe, its derived children (tight variants)
    are deleted first so callers don't leave orphaned universes behind — the
    parent FK is ON DELETE SET NULL, not CASCADE.
    """
    def _run():
        resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
        if not resp.data:
            return {"deleted": label, "children": []}
        uid = resp.data[0]["universe_id"]

        child_resp = (
            supabase.table("universe")
            .select("universe_id, label")
            .eq("parent_universe_id", uid)
            .execute()
        )
        children = child_resp.data or []
        if children:
            child_ids = [c["universe_id"] for c in children]
            supabase.table("universe").delete().in_("universe_id", child_ids).execute()

        supabase.table("universe").delete().eq("universe_id", uid).execute()
        return {"deleted": label, "children": [c["label"] for c in children]}
    return await asyncio.to_thread(_run)


class UniverseRenameRequest(BaseModel):
    new_label: str


@app.put("/api/universe/labels/{label}")
async def universe_rename_label(label: str, req: UniverseRenameRequest):
    """Rename a universe label."""
    def _run():
        new_label = (req.new_label or "").strip()
        if not new_label:
            raise HTTPException(status_code=400, detail="new_label is required")
        existing = supabase.table("universe").select("universe_id").eq("label", new_label).limit(1).execute()
        if existing.data:
            raise HTTPException(status_code=409, detail=f"Label '{new_label}' already exists")
        supabase.table("universe").update({"label": new_label}).eq("label", label).execute()
        return {"old_label": label, "new_label": new_label}
    return await asyncio.to_thread(_run)


@app.delete("/api/universe/labels")
async def universe_delete_all():
    """Delete ALL universes and their memberships (cascade)."""
    def _run():
        resp = supabase.table("universe").select("universe_id").limit(100000).execute()
        ids = [r["universe_id"] for r in (resp.data or [])]
        if not ids:
            return {"deleted": 0}
        for i in range(0, len(ids), 50):
            batch = ids[i:i + 50]
            supabase.table("universe").delete().in_("universe_id", batch).execute()
        return {"deleted": len(ids)}
    return await asyncio.to_thread(_run)


# ---------------------------------------------------------------------------
# Derived universes (tightened base universes via quality-metric thresholds)
# ---------------------------------------------------------------------------

def _cutoff_for_target_month(target_month: str) -> date:
    """Latest fiscal-year-end date to consider for a given target month.

    Matches the existing convention in screen.py: for target month 'YYYY-MM'
    we use the previous calendar year's FY data. Here we express that as a
    cutoff date of (YYYY-1)-12-31.
    """
    year = int(target_month[:4])
    return date(year - 1, 12, 31)


def _load_derived_metrics(
    company_ids: list[int],
    metric_codes: list[str],
) -> dict[int, list[tuple[date, dict[str, float]]]]:
    """Fetch derived metric rows for the given companies+codes.

    Returns {company_id -> [(fy_end_date, {code: value}), ...]}, sorted by date asc.
    Batched in chunks of 50 company_ids (Cloudflare 502 avoidance).
    """
    out: dict[int, dict[str, dict[str, float]]] = {}  # cid -> {iso_date -> {code -> value}}
    for i in range(0, len(company_ids), 50):
        batch = company_ids[i:i + 50]
        resp = (
            supabase.table("metric_data")
            .select("company_id, metric_code, target_date, numeric_value")
            .in_("company_id", batch)
            .eq("source_code", "derived")
            .in_("metric_code", metric_codes)
            .limit(100000)
            .execute()
        )
        for row in (resp.data or []):
            cid = row["company_id"]
            d = row["target_date"]
            code = row["metric_code"]
            v = row["numeric_value"]
            if v is None:
                continue
            out.setdefault(cid, {}).setdefault(d, {})[code] = float(v)

    result: dict[int, list[tuple[date, dict[str, float]]]] = {}
    for cid, by_date in out.items():
        rows = []
        for iso, metrics in by_date.items():
            try:
                rows.append((date.fromisoformat(iso), metrics))
            except ValueError:
                continue
        rows.sort(key=lambda x: x[0])
        result[cid] = rows
    return result


def _applicable_metrics(
    rows: list[tuple[date, dict[str, float]]],
    cutoff: date,
) -> dict[str, float]:
    """Return a merged view of all derived metric values as of `cutoff`.

    Walks FYs in ascending order and overlays each, so later FYs overwrite
    earlier ones. Any code we've ever seen up to the cutoff is returned —
    this matters because a given FY entry may not include every metric.
    """
    merged: dict[str, float] = {}
    for d, metrics in rows:
        if d > cutoff:
            break
        merged.update(metrics)
    return merged


class DeriveUniverseRequest(BaseModel):
    base_universe_id: int
    label: str | None = None  # required for non-preview
    description: str | None = None
    filter_config: dict


@app.get("/api/universe/derived-metrics/criteria")
async def universe_derived_criteria():
    """Return criterion specs + default filter_config for the /universe UI."""
    from universe.derived_metrics import CRITERIA_SPECS, default_filter_config

    specs = []
    for s in CRITERIA_SPECS:
        entry: dict = {
            "key": s.key,
            "label": s.label,
            "default_threshold": s.default_threshold,
            "default_enabled": s.default_enabled,
        }
        if s.components is not None:
            entry["components"] = [
                {"label": label, "code": code, "default": default}
                for label, code, default in s.components
            ]
        else:
            entry["metric"] = s.metric
            entry["op"] = s.op
        specs.append(entry)
    return {"specs": specs, "default_filter_config": default_filter_config()}


@app.get("/api/universe/derived-metrics/status")
async def universe_derived_status():
    """How many companies have at least one derived metric row stored."""
    def _run():
        resp = (
            supabase.table("metric_data")
            .select("company_id", count="exact")
            .eq("source_code", "derived")
            .limit(1)
            .execute()
        )
        total_rows = resp.count or 0

        # Distinct companies: pull company_ids in pages (Supabase has no DISTINCT here)
        seen: set[int] = set()
        offset = 0
        page = 1000
        while True:
            r = (
                supabase.table("metric_data")
                .select("company_id")
                .eq("source_code", "derived")
                .range(offset, offset + page - 1)
                .execute()
            )
            batch = r.data or []
            for row in batch:
                seen.add(row["company_id"])
            if len(batch) < page:
                break
            offset += page
        return {"companies_with_derived_metrics": len(seen), "total_rows": total_rows}

    return await asyncio.to_thread(_run)


class RecomputeRequest(BaseModel):
    universe_ids: list[int] | None = None  # None = all companies in any universe


@app.post("/api/universe/derived-metrics/recompute")
async def universe_derived_recompute(body: RecomputeRequest = RecomputeRequest()):
    """Recompute derived metric values from cached GuruFocus annuals. SSE stream."""
    import queue
    from universe.derived_metrics import precompute_for_companies

    def _collect_companies() -> list[dict]:
        # Scope: companies that appear in at least one universe_membership row.
        # If universe_ids is given, restrict to those.
        cid_set: set[int] = set()
        offset = 0
        page = 1000
        q = supabase.table("universe_membership").select("company_id")
        if body.universe_ids:
            q = q.in_("universe_id", body.universe_ids)
        while True:
            r = q.range(offset, offset + page - 1).execute()
            batch = r.data or []
            for row in batch:
                if row.get("company_id"):
                    cid_set.add(row["company_id"])
            if len(batch) < page:
                break
            offset += page

        ids = sorted(cid_set)
        if not ids:
            return []

        # Hydrate ticker + exchange
        companies: list[dict] = []
        for i in range(0, len(ids), 50):
            batch = ids[i:i + 50]
            r = supabase.table("company").select(
                "company_id, gurufocus_ticker, company_name, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code)"
            ).in_("company_id", batch).execute()
            for c in (r.data or []):
                exch = c.pop("gurufocus_exchange", None) or {}
                c["gurufocus_exchange"] = exch.get("exchange_code")
                companies.append(c)
        return companies

    def _run(q: "queue.Queue"):
        companies = _collect_companies()
        q.put(json.dumps({
            "type": "progress",
            "message": f"Found {len(companies)} companies across selected universes.",
        }))
        for event in precompute_for_companies(supabase, companies):
            q.put(json.dumps(event))
        q.put(None)

    async def generate():
        yield ": keepalive\n\n"
        qq: "queue.Queue" = queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, qq)
        while True:
            try:
                msg = await asyncio.to_thread(qq.get, timeout=0.15)
            except Exception:
                if task.done():
                    while not qq.empty():
                        m = qq.get_nowait()
                        if m is not None:
                            yield f"data: {m}\n\n"
                    break
                continue
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.post("/api/universe/derive/preview")
async def universe_derive_preview(body: DeriveUniverseRequest):
    """Count how many companies per month would survive the filter. No writes."""
    from universe.derived_metrics import company_passes, required_metric_codes

    def _run():
        base_resp = supabase.table("universe").select(
            "universe_id, label"
        ).eq("universe_id", body.base_universe_id).limit(1).execute()
        if not base_resp.data:
            raise HTTPException(status_code=404, detail="base universe not found")

        # Load all memberships of the base
        rows: list[dict] = []
        offset = 0
        page = 1000
        while True:
            r = (
                supabase.table("universe_membership")
                .select("target_month, company_id")
                .eq("universe_id", body.base_universe_id)
                .range(offset, offset + page - 1)
                .execute()
            )
            batch = r.data or []
            rows.extend(batch)
            if len(batch) < page:
                break
            offset += page

        if not rows:
            return {"monthly_counts": [], "base_rows": 0, "passed_rows": 0, "missing_metrics": 0}

        codes = required_metric_codes(body.filter_config)
        cids = sorted({r["company_id"] for r in rows})

        if not codes:
            # Nothing enabled → filter is a no-op, every base row passes.
            by_month: dict[str, int] = {}
            for r in rows:
                by_month[r["target_month"]] = by_month.get(r["target_month"], 0) + 1
            return {
                "monthly_counts": [
                    {"month": m, "count": c} for m, c in sorted(by_month.items())
                ],
                "base_rows": len(rows),
                "passed_rows": len(rows),
                "missing_metrics": 0,
                "base_label": base_resp.data[0]["label"],
            }

        metrics_by_cid = _load_derived_metrics(cids, codes)

        missing = 0
        pass_by_month: dict[str, int] = {}
        base_by_month: dict[str, int] = {}
        for row in rows:
            m = row["target_month"]
            cid = row["company_id"]
            base_by_month[m] = base_by_month.get(m, 0) + 1
            fy_rows = metrics_by_cid.get(cid, [])
            if not fy_rows:
                missing += 1
                continue
            cutoff = _cutoff_for_target_month(m)
            applicable = _applicable_metrics(fy_rows, cutoff)
            if company_passes(body.filter_config, applicable):
                pass_by_month[m] = pass_by_month.get(m, 0) + 1

        months_sorted = sorted(base_by_month.keys())
        return {
            "monthly_counts": [
                {"month": m, "count": pass_by_month.get(m, 0), "base_count": base_by_month[m]}
                for m in months_sorted
            ],
            "base_rows": len(rows),
            "passed_rows": sum(pass_by_month.values()),
            "missing_metrics": missing,
            "base_label": base_resp.data[0]["label"],
        }

    return await asyncio.to_thread(_run)


@app.post("/api/universe/derive")
async def universe_derive_create(body: DeriveUniverseRequest):
    """Create a new derived universe (SSE: precompute derived metrics → filter → insert)."""
    import queue
    from universe.derived_metrics import (
        company_passes,
        required_metric_codes,
        precompute_for_companies,
    )

    def _run(q: "queue.Queue"):
        def emit(step: str, status: str, message: str, **extra):
            q.put(json.dumps({
                "type": "progress", "step": step, "status": status, "message": message, **extra,
            }))

        try:
            label = (body.label or "").strip()
            if not label:
                q.put(json.dumps({"type": "error", "message": "label is required"}))
                return

            emit("validate", "in_progress", "Validating inputs...")
            base_resp = supabase.table("universe").select(
                "universe_id, label"
            ).eq("universe_id", body.base_universe_id).limit(1).execute()
            if not base_resp.data:
                q.put(json.dumps({"type": "error", "message": "base universe not found"}))
                return
            base_label = base_resp.data[0]["label"]

            dup = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
            if dup.data:
                q.put(json.dumps({"type": "error", "message": f"label '{label}' already exists"}))
                return
            emit("validate", "done", f"Base: {base_label} → new label: {label}")

            # --- Load base memberships -------------------------------------------------
            emit("load_base", "in_progress", f"Loading memberships from {base_label}...")
            rows: list[dict] = []
            offset = 0
            page = 1000
            while True:
                r = (
                    supabase.table("universe_membership")
                    .select("target_month, company_id, universe_ticker, sector")
                    .eq("universe_id", body.base_universe_id)
                    .range(offset, offset + page - 1)
                    .execute()
                )
                batch = r.data or []
                rows.extend(batch)
                if len(batch) < page:
                    break
                offset += page
            months = sorted({r["target_month"] for r in rows if r.get("target_month")})
            cids = sorted({r["company_id"] for r in rows})
            emit(
                "load_base", "done",
                f"Loaded {len(rows):,} memberships across {len(months)} months, {len(cids)} companies.",
            )

            # --- Precompute derived metrics for the base companies ---------------------
            codes = required_metric_codes(body.filter_config)
            if codes and cids:
                emit(
                    "precompute", "in_progress",
                    f"Precomputing derived metrics for {len(cids)} companies...",
                )
                companies: list[dict] = []
                for i in range(0, len(cids), 50):
                    batch = cids[i:i + 50]
                    r = supabase.table("company").select(
                        "company_id, gurufocus_ticker, company_name, "
                        "gurufocus_exchange:gurufocus_exchange(exchange_code)"
                    ).in_("company_id", batch).execute()
                    for c in (r.data or []):
                        exch = c.pop("gurufocus_exchange", None) or {}
                        c["gurufocus_exchange"] = exch.get("exchange_code")
                        companies.append(c)

                last_done_summary = ""
                for ev in precompute_for_companies(supabase, companies):
                    etype = ev.get("type")
                    msg = ev.get("message", "")
                    if etype == "progress_update":
                        emit("precompute", "in_progress", msg)
                    elif etype == "done":
                        last_done_summary = msg
                emit("precompute", "done", last_done_summary or "Derived metrics up to date.")
            else:
                emit("precompute", "done", "No filters enabled; skipping precompute.")

            # --- Load metrics and apply filter ----------------------------------------
            emit("filter", "in_progress", f"Applying filter to {len(rows):,} rows...")
            metrics_by_cid = _load_derived_metrics(cids, codes) if codes else {}

            kept: list[dict] = []
            missing = 0
            for row in rows:
                cid = row["company_id"]
                if not codes:
                    kept.append(row)
                    continue
                fy_rows = metrics_by_cid.get(cid, [])
                if not fy_rows:
                    missing += 1
                    continue
                cutoff = _cutoff_for_target_month(row["target_month"])
                applicable = _applicable_metrics(fy_rows, cutoff)
                if company_passes(body.filter_config, applicable):
                    kept.append(row)
            emit(
                "filter", "done",
                f"{len(kept):,} / {len(rows):,} rows pass"
                + (f" ({missing:,} excluded for missing metrics)." if missing else "."),
            )

            if not kept:
                q.put(json.dumps({
                    "type": "error",
                    "message": "Filter matches zero rows — adjust thresholds or precompute metrics.",
                }))
                return

            # --- Create universe row --------------------------------------------------
            emit("create", "in_progress", "Creating universe row...")
            created = supabase.table("universe").insert({
                "label": label,
                "description": body.description,
                "parent_universe_id": body.base_universe_id,
                "filter_config": body.filter_config,
            }).execute()
            new_id = created.data[0]["universe_id"]
            emit("create", "done", f"Universe created (id={new_id}).")

            # --- Insert memberships in batches ----------------------------------------
            # Dedup defensively on (company_id, target_month). The base universe
            # may carry stale duplicate rows from prior runs; the universe_membership
            # PK would reject them mid-insert otherwise.
            seen_keys: set[tuple] = set()
            payload: list[dict] = []
            dropped_dupes = 0
            for r in kept:
                key = (r["company_id"], r["target_month"])
                if key in seen_keys:
                    dropped_dupes += 1
                    continue
                seen_keys.add(key)
                payload.append({
                    "universe_id": new_id,
                    "company_id": r["company_id"],
                    "target_month": r["target_month"],
                    "universe_ticker": r.get("universe_ticker"),
                    "sector": r.get("sector"),
                })
            if dropped_dupes:
                emit(
                    "filter", "done",
                    f"{len(kept):,} rows passed filter; dropped {dropped_dupes:,} duplicate "
                    f"(company_id, target_month) row(s) before insert.",
                )
            import time as _time
            from universe.derived_metrics import _fmt_duration as _fmt_dur
            batch_size = 500
            total_inserted = 0
            total_batches = (len(payload) + batch_size - 1) // batch_size
            insert_started = _time.monotonic()
            emit(
                "insert", "in_progress",
                f"Inserting {len(payload):,} rows in {total_batches} batches...",
            )
            for bi, i in enumerate(range(0, len(payload), batch_size), start=1):
                chunk = payload[i:i + batch_size]
                elapsed = _time.monotonic() - insert_started
                rate = (bi - 1) / elapsed if elapsed > 0 and bi > 1 else 0
                remaining = (total_batches - bi + 1) / rate if rate > 0 else 0
                emit(
                    "insert", "in_progress",
                    f"Starting batch {bi}/{total_batches} ({len(chunk):,} rows) · "
                    f"{_fmt_dur(elapsed)} elapsed · ~{_fmt_dur(remaining)} left",
                )
                try:
                    resp = supabase.table("universe_membership").insert(chunk).execute()
                    total_inserted += len(resp.data or [])
                except Exception as batch_exc:
                    emit(
                        "insert", "in_progress",
                        f"Batch {bi}/{total_batches} failed: {batch_exc}. Retrying once...",
                    )
                    resp = supabase.table("universe_membership").insert(chunk).execute()
                    total_inserted += len(resp.data or [])
                elapsed = _time.monotonic() - insert_started
                rate = bi / elapsed if elapsed > 0 else 0
                remaining = (total_batches - bi) / rate if rate > 0 else 0
                emit(
                    "insert", "in_progress",
                    f"Batch {bi}/{total_batches} done · {total_inserted:,}/{len(payload):,} rows · "
                    f"{_fmt_dur(elapsed)} elapsed · ~{_fmt_dur(remaining)} left",
                )
            emit("insert", "done", f"Inserted {total_inserted:,} rows in {_fmt_dur(_time.monotonic() - insert_started)}.")

            q.put(json.dumps({
                "type": "done",
                "message": f"Created '{label}' from {base_label} with {total_inserted:,} rows.",
                "data": {
                    "universe_id": new_id,
                    "label": label,
                    "rows_inserted": total_inserted,
                    "base_universe_id": body.base_universe_id,
                    "base_label": base_label,
                },
            }))
        except Exception as exc:
            logging.getLogger(__name__).exception("universe/derive failed")
            q.put(json.dumps({"type": "error", "message": str(exc)}))
        finally:
            q.put(None)

    async def generate():
        yield ": keepalive\n\n"
        qq: "queue.Queue" = queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, qq)
        while True:
            try:
                msg = await asyncio.to_thread(qq.get, timeout=0.15)
            except Exception:
                if task.done():
                    while not qq.empty():
                        m = qq.get_nowait()
                        if m is not None:
                            yield f"data: {m}\n\n"
                    break
                continue
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/api/universe/validate")
async def universe_validate():
    """Compare current screening results against LongEquity snapshots."""
    def _run():
        resp = supabase.table("company").select(
            "company_id, gurufocus_ticker, exchange_id, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))"
        ).limit(10000).execute()
        companies = []
        for c in (resp.data or []):
            exch_info = c.pop("gurufocus_exchange", None) or {}
            country_info = exch_info.pop("country", None) or {}
            c["gurufocus_exchange"] = exch_info.get("exchange_code")
            c["country"] = country_info.get("country_name")
            companies.append(c)

        results = []
        for event in screen_universe(supabase, companies):
            if event["type"] == "done":
                results = event["data"]["results"]
                break

        return validate_vs_longequity(supabase, results)

    return await asyncio.to_thread(_run)


# ───────────────────────────────────────────────────────────
# Index Universe endpoints
# ───────────────────────────────────────────────────────────

@app.post("/api/index-universe/import-sp500")
async def index_universe_import_sp500():
    """Scrape S&P 500 from Wikipedia, reconstruct monthly holdings, store in DB."""
    import queue as _queue

    def _run(q: _queue.Queue):
        def emit(msg: str):
            q.put(json.dumps({"type": "progress", "message": msg}))

        try:
            emit("Scraping S&P 500 from Wikipedia...")
            current, changes, wiki_company_info = scrape_sp500()
            emit(f"Found {len(current)} current tickers, {len(changes)} historical changes")

            emit("Reconstructing monthly holdings (2000-01 onwards)...")
            monthly, filtered_changes = reconstruct_monthly_holdings(current, changes)
            emit(f"Reconstructed {len(monthly)} months ({min(monthly)}..{max(monthly)}), {len(filtered_changes)} changes")

            # Collect all unique tickers and resolve via OpenFIGI
            all_tickers: set[str] = set()
            for t in monthly.values():
                all_tickers |= t
            emit(f"Resolving {len(all_tickers)} unique tickers...")

            company_lookup = resolve_and_create_companies(
                supabase, all_tickers, on_progress=emit, company_info=wiki_company_info,
            )

            emit("Storing in database...")
            stats = store_index_membership(
                supabase, "SP500", monthly, filtered_changes, company_lookup,
                on_progress=emit,
            )

            q.put(json.dumps({
                "type": "done",
                "message": (
                    f"Import complete. {stats['months']} months, "
                    f"{stats['total_rows']} rows, "
                    f"{stats['unique_tickers']} unique tickers "
                    f"({stats['matched_companies']} matched to companies), "
                    f"{stats['changes_count']} changes stored"
                ),
            }))
        except Exception as e:
            import traceback
            q.put(json.dumps({"type": "error", "message": f"{e}\n{traceback.format_exc()}"}))
        q.put(None)

    async def generate():
        yield ": keepalive\n\n"
        q: _queue.Queue = _queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, q)

        while True:
            try:
                msg = await asyncio.to_thread(q.get, timeout=0.15)
            except Exception:
                if task.done():
                    while not q.empty():
                        msg = q.get_nowait()
                        if msg is not None:
                            yield f"data: {msg}\n\n"
                    break
                continue
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


# Module-level cache for the universe-stats list. The underlying view does
# COUNT(DISTINCT universe_ticker) over the full universe_membership table,
# which sometimes trips Supabase's 8s statement_timeout once the table grows
# past ~500k rows (S&P 500 history × ACWI × monthly entries). Reads change
# rarely (only after an index ingest), so a 5-minute TTL avoids paying that
# cost on every dropdown render. On timeout we fall back to a stale cached
# entry if we have one, then to a cheap universe-table-only read so the UI
# still loads — month/ticker counts come back as 0 in that degraded mode.
_UNIVERSE_STATS_CACHE: dict = {"ts": 0.0, "data": None}
_UNIVERSE_STATS_TTL = 300.0


@app.get("/api/index-universe/indexes")
async def index_universe_list():
    """List all stored index universes with month range and unique ticker counts.
    Aggregates are precomputed by the universe_stats view — querying membership
    rows directly and counting in Python ran ~70s for SP500 + ACWI."""
    def _run():
        now = time.time()
        cached = _UNIVERSE_STATS_CACHE.get("data")
        if cached is not None and (now - _UNIVERSE_STATS_CACHE["ts"]) < _UNIVERSE_STATS_TTL:
            return cached
        try:
            resp = (
                supabase.table("universe_stats")
                .select("*")
                .order("label")
                .execute()
            )
            result = []
            for r in (resp.data or []):
                if not r.get("start_month"):
                    continue  # skip empty universes
                result.append({
                    "index_name": r["label"],
                    "description": r.get("description"),
                    "created_at": r.get("created_at"),
                    "start_month": r.get("start_month"),
                    "end_month": r.get("end_month"),
                    "month_count": r.get("month_count") or 0,
                    "total_unique_tickers": r.get("total_unique_tickers") or 0,
                })
            _UNIVERSE_STATS_CACHE["data"] = result
            _UNIVERSE_STATS_CACHE["ts"] = now
            return result
        except Exception as e:
            logging.getLogger(__name__).warning(
                "[index-universe] universe_stats query failed (%s: %s); "
                "serving %s",
                type(e).__name__, e,
                "stale cache" if cached is not None else "degraded universe-table fallback",
            )
            if cached is not None:
                return cached
            # Final fallback: just list universes without aggregates so the
            # dropdown still populates. Frontend tolerates 0/null counts.
            try:
                u_resp = (
                    supabase.table("universe")
                    .select("universe_id, label, description, created_at")
                    .order("label")
                    .execute()
                )
                return [
                    {
                        "index_name": r["label"],
                        "description": r.get("description"),
                        "created_at": r.get("created_at"),
                        "start_month": None,
                        "end_month": None,
                        "month_count": 0,
                        "total_unique_tickers": 0,
                    }
                    for r in (u_resp.data or [])
                ]
            except Exception:
                raise e
    return await asyncio.to_thread(_run)


@app.get("/api/index-universe/months")
async def index_universe_months(index: str = "SP500"):
    """List months for a given index with ticker counts."""
    def _run():
        u_resp = supabase.table("universe").select("universe_id").eq("label", index).limit(1).execute()
        if not u_resp.data:
            return []
        universe_id = u_resp.data[0]["universe_id"]
        resp = supabase.table("universe_membership").select("target_month").eq("universe_id", universe_id).limit(100000).execute()
        from collections import Counter
        counts = Counter(r["target_month"] for r in (resp.data or []))
        return [{"target_month": m, "count": c} for m, c in sorted(counts.items())]
    return await asyncio.to_thread(_run)


def _enrich_tickers(rows: list[dict]) -> list[dict]:
    """Add company_name, exchange, and gurufocus_url to ticker rows."""
    company_ids = [r["company_id"] for r in rows if r["company_id"]]
    company_info: dict[int, dict] = {}
    for i in range(0, len(company_ids), 50):
        chunk = company_ids[i : i + 50]
        resp = supabase.table("company").select(
            "company_id, company_name, gurufocus_exchange:gurufocus_exchange(exchange_code)"
        ).in_("company_id", chunk).execute()
        for c in resp.data or []:
            exch_info = c.get("gurufocus_exchange") or {}
            company_info[c["company_id"]] = {
                "company_name": c.get("company_name") or "",
                "exchange": exch_info.get("exchange_code") or "",
            }

    result = []
    for r in rows:
        info = company_info.get(r["company_id"], {}) if r["company_id"] else {}
        ticker = r["ticker"]
        result.append({
            "ticker": ticker,
            "company_id": r["company_id"],
            "company_name": info.get("company_name") or None,
            "exchange": info.get("exchange") or None,
            "gurufocus_url": f"https://www.gurufocus.com/stock/{ticker}/summary",
        })
    return result


@app.get("/api/index-universe/tickers")
async def index_universe_tickers(index: str = "SP500", month: str = ""):
    """Get tickers for a specific month of an index."""
    if not month:
        raise HTTPException(400, "month query param required")

    def _run():
        u_resp = supabase.table("universe").select("universe_id").eq("label", index).limit(1).execute()
        if not u_resp.data:
            return []
        universe_id = u_resp.data[0]["universe_id"]
        rows = (
            supabase.table("universe_membership")
            .select("universe_ticker, company_id")
            .eq("universe_id", universe_id)
            .eq("target_month", month)
            .order("universe_ticker")
            .execute()
        ).data or []
        # Map universe_ticker -> ticker for _enrich_tickers compatibility
        for r in rows:
            r["ticker"] = r.pop("universe_ticker", "")
        return _enrich_tickers(rows)

    return await asyncio.to_thread(_run)


@app.get("/api/index-universe/cumulative")
async def index_universe_cumulative(index: str = "SP500"):
    """Get all unique tickers across all months for an index, with company + GF info."""
    def _run():
        u_resp = supabase.table("universe").select("universe_id").eq("label", index).limit(1).execute()
        if not u_resp.data:
            return []
        universe_id = u_resp.data[0]["universe_id"]
        # Get all distinct (universe_ticker, company_id) pairs
        resp = supabase.table("universe_membership").select(
            "universe_ticker, company_id"
        ).eq("universe_id", universe_id).limit(100000).execute()
        # Deduplicate by ticker
        seen: dict[str, dict] = {}
        for r in (resp.data or []):
            t = r.get("universe_ticker")
            if t and t not in seen:
                seen[t] = {"ticker": t, "company_id": r["company_id"]}
        return _enrich_tickers(list(seen.values()))

    return await asyncio.to_thread(_run)


@app.post("/api/index-universe/check-gurufocus")
async def index_universe_check_gf(index: str = "SP500"):
    """Check GuruFocus cache coverage for all tickers in an index. SSE stream."""
    import queue as _queue

    def _run(q: _queue.Queue):
        def emit(msg: str):
            q.put(json.dumps({"type": "progress", "message": msg}))

        try:
            # Collect all unique tickers for this index
            emit(f"Loading tickers for {index}...")
            u_resp = supabase.table("universe").select("universe_id").eq("label", index).limit(1).execute()
            if not u_resp.data:
                q.put(json.dumps({"type": "error", "message": f"Universe '{index}' not found"}))
                q.put(None)
                return
            universe_id = u_resp.data[0]["universe_id"]
            all_tickers: set[str] = set()
            resp = (
                supabase.table("universe_membership")
                .select("universe_ticker")
                .eq("universe_id", universe_id)
                .limit(100000)
                .execute()
            )
            for r in resp.data or []:
                if r.get("universe_ticker"):
                    all_tickers.add(r["universe_ticker"])

            emit(f"Found {len(all_tickers)} unique tickers across all months")

            result = check_gurufocus_availability(supabase, all_tickers, on_progress=emit)
            q.put(json.dumps({"type": "done", "data": result}))
        except Exception as e:
            q.put(json.dumps({"type": "error", "message": str(e)}))
        q.put(None)

    async def generate():
        yield ": keepalive\n\n"
        q: _queue.Queue = _queue.Queue()
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, _run, q)

        while True:
            try:
                msg = await asyncio.to_thread(q.get, timeout=0.15)
            except Exception:
                if task.done():
                    while not q.empty():
                        msg = q.get_nowait()
                        if msg is not None:
                            yield f"data: {msg}\n\n"
                    break
                continue
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/api/index-universe/changes")
async def index_universe_changes(index: str = "SP500"):
    """Get the changelog (additions/removals) for an index."""
    return await asyncio.to_thread(lambda: load_changes(supabase, index))


@app.delete("/api/index-universe/indexes/{index_name}")
async def index_universe_delete(index_name: str):
    """Delete all data for an index (deletes the universe and cascades memberships)."""
    await asyncio.to_thread(
        lambda: supabase.table("universe").delete().eq("label", index_name).execute()
    )
    return {"ok": True}


@app.get("/api/acwi/holdings")
async def acwi_holdings():
    """Return parsed ACWI ETF holdings from the local XLS file."""
    def work():
        holdings, as_of = load_acwi_holdings()

        # Load exchange→currency from gurufocus_exchange table
        try:
            rows = supabase.table("gurufocus_exchange").select("exchange_code,currency_code").execute()
            db_currencies = {r["exchange_code"]: r["currency_code"] for r in (rows.data or [])}
        except Exception:
            db_currencies = {}

        for h in holdings:
            exch = h.get("Exchange", "")
            h["gurufocus_url"] = gurufocus_url(h.get("Ticker", ""), exch)
            gf_code = gurufocus_exchange(exch)
            h["gf_exchange"] = gf_code if gf_code else None
            # Look up currency from DB using the exchange code
            db_code = gurufocus_exchange_for_db(exch)
            h["gf_currency"] = db_currencies.get(db_code) if db_code else None

        return {"holdings": holdings, "count": len(holdings), "as_of": as_of}

    return await asyncio.to_thread(work)


@app.get("/api/acwi/announcements")
async def acwi_announcements(refresh: bool = False):
    """Get MSCI index announcements (cached locally, 24h TTL)."""
    rows = await asyncio.to_thread(get_msci_announcements, refresh)
    return {"announcements": rows, "count": len(rows)}


@app.get("/api/acwi/announcement-detail")
async def acwi_announcement_detail(url: str):
    """Fetch detail (STANDARD action + EFFECTIVE DATE) from an individual MSCI announcement."""
    detail = await asyncio.to_thread(fetch_announcement_detail_cached, url)
    return detail


@app.post("/api/acwi/announcement-details-bulk")
async def acwi_announcement_details_bulk(body: dict):
    """Fetch details for multiple announcement URLs. Body: {"urls": [...]}."""
    urls = body.get("urls", [])
    results = await asyncio.to_thread(fetch_bulk_details, urls)
    return {"details": results}


@app.get("/api/acwi/net-additions")
async def acwi_net_additions():
    """Compute net additions matched against current holdings."""
    results = await asyncio.to_thread(compute_net_additions)
    matched = sum(1 for r in results if r["matched"])
    return {"net_additions": results, "total": len(results), "matched": matched}


class AcwiSaveUniverseRequest(BaseModel):
    name: str = "ACWI"
    start_date: str
    end_date: str


@app.post("/api/acwi/save-universe")
async def acwi_save_universe(req: AcwiSaveUniverseRequest):
    """SSE stream: reconstruct monthly ACWI feasible-universe holdings and save as a universe.

    The saved universe can be selected in the momentum backtester via `index_universe`.
    """
    import queue as _queue
    import threading

    def _run(q: _queue.Queue):
        def emit(message: str, pct: int | None = None):
            payload = {"type": "progress", "message": message}
            if pct is not None:
                payload["pct"] = pct
            q.put(json.dumps(payload))

        try:
            emit("Loading feasible ACWI holdings from iShares XLS...", 3)
            feasible = acwi_feasible_holdings_for_db()
            emit(f"Found {len(feasible)} feasible holdings", 5)

            # Load exchange_id map: exchange_code -> exchange_id
            exch_resp = supabase.table("gurufocus_exchange").select("exchange_id, exchange_code").execute()
            exch_id_map = {r["exchange_code"]: r["exchange_id"] for r in (exch_resp.data or [])}

            # Bulk-load existing company rows for all exchanges we care about
            # (single query). Two indexes built off the same fetch:
            #   - existing_by_key[(exchange_id, gf_ticker)] → cid (primary)
            #   - existing_by_name[(exchange_id, NORMALIZED_NAME)] → list[cid]
            # The name index is the fallback that catches override renames
            # (e.g. WAR:SPL → WAR:EBP) and prevents the duplicate-row bug
            # that fired when the primary key changed but the company is
            # still the same iShares row underneath.
            needed_exchanges = {fh["db_exchange"] for fh in feasible}
            needed_eids = [exch_id_map[e] for e in needed_exchanges if e in exch_id_map]
            existing_by_key: dict[tuple[int, str], int] = {}
            existing_by_name: dict[tuple[int, str], list[int]] = {}
            if needed_eids:
                offset = 0
                page_size = 1000
                while True:
                    c_resp = (
                        supabase.table("company")
                        .select("company_id, gurufocus_ticker, exchange_id, company_name")
                        .in_("exchange_id", needed_eids)
                        .range(offset, offset + page_size - 1)
                        .execute()
                    )
                    batch = c_resp.data or []
                    for c in batch:
                        if c.get("gurufocus_ticker") and c.get("exchange_id") is not None:
                            existing_by_key[(c["exchange_id"], c["gurufocus_ticker"])] = c["company_id"]
                        name_norm = (c.get("company_name") or "").strip().upper()
                        if name_norm and c.get("exchange_id") is not None:
                            existing_by_name.setdefault((c["exchange_id"], name_norm), []).append(c["company_id"])
                    if len(batch) < page_size:
                        break
                    offset += page_size
            emit(f"Loaded {len(existing_by_key)} existing company rows across {len(needed_eids)} exchanges", 10)

            company_lookup: dict[str, int] = {}
            sector_lookup: dict[str, str] = {
                fh["symbol"]: fh["sector"] for fh in feasible if fh.get("sector")
            }
            created = 0
            already = 0
            renamed = 0
            skipped = 0
            unknown_exchanges: set[str] = set()
            for idx, fh in enumerate(feasible):
                eid = exch_id_map.get(fh["db_exchange"])
                if eid is None:
                    skipped += 1
                    unknown_exchanges.add(fh["db_exchange"])
                    continue
                key = (eid, fh["gf_ticker"])
                cid = existing_by_key.get(key)
                if cid is not None:
                    company_lookup[fh["symbol"]] = cid
                    already += 1
                    continue

                # Primary key miss — try the name-based fallback to catch a
                # ticker rename (e.g. an override added since last ingest).
                # Only honor it when the name → cid mapping is unique on
                # this exchange; ambiguous matches fall through to insert.
                name_norm = (fh.get("company_name") or "").strip().upper()
                rename_target: int | None = None
                if name_norm:
                    candidates = existing_by_name.get((eid, name_norm))
                    if candidates and len(candidates) == 1:
                        rename_target = candidates[0]

                if rename_target is not None:
                    try:
                        supabase.table("company").update({
                            "gurufocus_ticker": fh["gf_ticker"],
                            "company_name": fh["company_name"] or None,
                        }).eq("company_id", rename_target).execute()
                        existing_by_key[key] = rename_target
                        company_lookup[fh["symbol"]] = rename_target
                        renamed += 1
                        emit(
                            f"  renamed {fh['db_exchange']}:* → {fh['symbol']} "
                            f"({fh['company_name']}, company_id={rename_target})",
                            None,
                        )
                    except Exception as e:
                        skipped += 1
                        emit(f"  failed to rename to {fh['symbol']} ({fh['company_name']}): {e}", None)
                    continue

                # Genuinely new row — no existing match by ticker or by name.
                try:
                    ins = supabase.table("company").insert({
                        "gurufocus_ticker": fh["gf_ticker"],
                        "exchange_id": eid,
                        "company_name": fh["company_name"] or None,
                    }).execute()
                    if ins.data:
                        cid = ins.data[0]["company_id"]
                        existing_by_key[key] = cid
                        company_lookup[fh["symbol"]] = cid
                        created += 1
                except Exception as e:
                    skipped += 1
                    emit(f"  failed to create {fh['symbol']} ({fh['company_name']}): {e}", None)
                    continue
                # Tag with 'acwi' source
                try:
                    supabase.table("company_source").upsert(
                        {"company_id": cid, "source_code": "acwi"},
                        on_conflict="company_id,source_code",
                        ignore_duplicates=True,
                    ).execute()
                except Exception:
                    pass

                if (idx + 1) % 200 == 0 or idx == len(feasible) - 1:
                    pct = 10 + round((idx + 1) / len(feasible) * 30)
                    emit(
                        f"Companies: {created} created, {renamed} renamed, "
                        f"{already} existing, {skipped} skipped ({idx + 1}/{len(feasible)})",
                        pct,
                    )

            if unknown_exchanges:
                emit(f"Unknown exchanges (missing from gurufocus_exchange): {sorted(unknown_exchanges)}", None)
            emit(
                f"Company sync done: {created} new, {renamed} renamed, "
                f"{already} existing, {skipped} skipped",
                42,
            )

            emit(f"Reconstructing monthly holdings {req.start_date}..{req.end_date}...", 45)
            monthly, stats = reconstruct_acwi_monthly_holdings(req.start_date, req.end_date)
            emit(
                f"Built {stats['months']} months: {stats['feasible_count']} feasible tickers "
                f"({stats['with_addition']} with matched addition, {stats['grandfathered']} grandfathered)",
                55,
            )

            emit(f"Writing universe '{req.name}' to database...", 60)
            store_stats = store_index_membership(
                supabase, req.name, monthly, [], company_lookup,
                on_progress=lambda m: emit(m, None),
                sector_lookup=sector_lookup,
            )

            q.put(json.dumps({
                "type": "done",
                "message": (
                    f"Saved '{req.name}': {store_stats['months']} months, "
                    f"{store_stats['total_rows']} rows, "
                    f"{store_stats['matched_companies']}/{store_stats['unique_tickers']} tickers matched "
                    f"({created} new companies created, {renamed} renamed, {already} existing)"
                ),
                "stats": {
                    "name": req.name,
                    "months": store_stats["months"],
                    "total_rows": store_stats["total_rows"],
                    "unique_tickers": store_stats["unique_tickers"],
                    "matched_companies": store_stats["matched_companies"],
                    "companies_created": created,
                    "companies_renamed": renamed,
                    "companies_existing": already,
                    "companies_skipped": skipped,
                    "feasible_count": stats["feasible_count"],
                    "grandfathered": stats["grandfathered"],
                    "with_addition": stats["with_addition"],
                },
            }))
        except Exception as e:
            import traceback
            q.put(json.dumps({"type": "error", "message": f"{e}\n{traceback.format_exc()}"}))
        q.put(None)

    q: _queue.Queue = _queue.Queue()
    threading.Thread(target=_run, args=(q,), daemon=True).start()

    async def generate():
        yield ": keepalive\n\n"
        while True:
            msg = await asyncio.to_thread(q.get)
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/api/acwi/fetch-all-details")
async def acwi_fetch_all_details():
    """SSE stream: fetch details for all constituent changes not yet cached."""
    import queue
    import threading

    def _emit(obj: dict) -> str:
        return json.dumps(obj, default=str)

    q: queue.Queue[str | None] = queue.Queue()

    def worker():
        try:
            anns = get_msci_announcements()
            constituent = [a for a in anns if a.get("is_constituent_change") and a.get("href")]
            cache = _load_detail_cache()

            to_fetch = [a for a in constituent if a["href"] not in cache]
            total = len(to_fetch)
            already_cached = len(constituent) - total

            q.put(_emit({"type": "progress", "message": f"{already_cached} cached, {total} to fetch", "fetched": 0, "total": total}))

            if total == 0:
                q.put(_emit({"type": "done", "message": "All details already cached", "fetched": 0, "total": 0, "errors": 0, "cached": already_cached}))
                q.put(None)
                return

            fetched = 0
            errors = 0
            error_list: list[dict] = []
            for a in to_fetch:
                try:
                    detail = fetch_announcement_detail(a["href"])
                except Exception as e:
                    detail = {"standard": None, "effective_date": None, "error": str(e)}
                    errors += 1
                    error_list.append({"title": a.get("title", ""), "href": a["href"], "error": str(e)})
                cache[a["href"]] = detail
                fetched += 1

                if fetched % 10 == 0 or fetched == total:
                    _save_detail_cache(cache)
                    q.put(_emit({
                        "type": "progress",
                        "message": f"Fetched {fetched}/{total}" + (f" ({errors} errors)" if errors else ""),
                        "fetched": fetched,
                        "total": total,
                        "pct": round(fetched / total * 100),
                        "errors": errors,
                    }))

            _save_detail_cache(cache)
            q.put(_emit({
                "type": "done",
                "message": f"Done. Fetched {fetched}, {errors} errors, {already_cached} were cached",
                "fetched": fetched,
                "total": total,
                "errors": errors,
                "cached": already_cached,
                "error_list": error_list[:50],  # cap to avoid huge payload
            }))
        except Exception as e:
            q.put(_emit({"type": "error", "message": str(e)}))
        q.put(None)

    threading.Thread(target=worker, daemon=True).start()

    async def generate():
        yield ": keepalive\n\n"
        while True:
            msg = await asyncio.to_thread(q.get)
            if msg is None:
                break
            yield f"data: {msg}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


