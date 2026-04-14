"""
Universe screening pipeline: fetch financials for all companies,
evaluate LongEquity quality criteria, build monthly universes.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime
from typing import Generator

from supabase import Client

from ingest.prices import _build_symbol, _ensure_bucket, _fetch_from_storage, _upload_to_storage
from ingest.earnings import _build_api_url, _api_request, _mask_url
from ingest.api_usage import track_api_call
from universe.criteria import evaluate_criteria, CriteriaResult, CRITERIA_NAMES

logger = logging.getLogger(__name__)


def _storage_path(ticker: str, exchange: str) -> str:
    return f"{exchange.upper()}_{ticker.upper()}/financials.json"


def _fetch_financials_cached(
    supabase: Client,
    ticker: str,
    exchange: str,
) -> dict | None:
    """Fetch financials from Supabase Storage cache."""
    path = _storage_path(ticker, exchange)
    try:
        raw = supabase.storage.from_("gurufocus-raw").download(path)
        return json.loads(raw)
    except Exception:
        return None


def _fetch_financials_api(
    supabase: Client,
    ticker: str,
    exchange: str,
) -> tuple[dict | None, str]:
    """Fetch financials from GuruFocus API and cache."""
    from urllib.parse import quote

    symbol = _build_symbol(ticker, exchange)
    url = _build_api_url(f"stock/{quote(symbol, safe=':')}/financials", {"order": "desc"})
    api = _api_request(url)
    track_api_call(supabase, exchange)

    if api.data is None:
        return None, api.log

    # Cache
    path = _storage_path(ticker, exchange)
    _ensure_bucket(supabase)
    _upload_to_storage(supabase, path, api.data)

    return api.data, api.log


def _get_annuals(data: dict) -> dict | None:
    """Extract the annuals block from financials data."""
    financials = data.get("financials")
    if not isinstance(financials, dict):
        return None
    return financials.get("annuals")


def screen_universe(
    supabase: Client,
    companies: list[dict],
    *,
    as_of_year: str | None = None,
    force_refresh: bool = False,
    on_progress: callable = None,
) -> Generator[dict, None, None]:
    """Screen all companies against LongEquity criteria.

    Yields SSE-style event dicts as it progresses.

    Args:
        companies: List of {company_id, primary_ticker, primary_exchange, company_name, ...}
        as_of_year: Evaluate criteria as of this year (YYYY-MM format, e.g. "2025-12").
                    If None, uses latest available data.
        force_refresh: If True, always fetch from API even if cached.
        on_progress: Optional callback(company_index, total, message).

    Yields:
        {"type": "progress", "message": "..."} during processing
        {"type": "company_result", "data": {...}} per company
        {"type": "done", "data": {...}} at the end with summary
    """
    total = len(companies)
    results: list[dict] = []
    api_calls = 0
    errors = 0
    skipped = 0
    delisted = 0
    forbidden_exchanges: set[str] = set()  # exchanges we know are unsubscribed

    for i, company in enumerate(companies):
        cid = company["company_id"]
        ticker = company["primary_ticker"]
        exchange = company["primary_exchange"]
        name = company.get("company_name", ticker)

        # Skip exchanges we already know are forbidden
        if exchange.upper() in forbidden_exchanges:
            skipped += 1
            continue

        yield {
            "type": "progress",
            "message": f"[{i + 1}/{total}] Screening {ticker} ({name})...",
        }

        # Try cache first
        data = None
        if not force_refresh:
            data = _fetch_financials_cached(supabase, ticker, exchange)

        # Fetch from API if no cache
        source = "cache"
        if data is None:
            data, log = _fetch_financials_api(supabase, ticker, exchange)
            api_calls += 1
            source = "api"
            if data is None:
                # Detect unsubscribed region — skip all future companies on this exchange
                if "unsubscribed region" in log.lower():
                    forbidden_exchanges.add(exchange.upper())
                    yield {
                        "type": "progress",
                        "message": f"  {ticker}: unsubscribed exchange {exchange} — skipping all {exchange} companies",
                    }
                    skipped += 1
                    continue

                # Detect delisted stocks
                if "delisted" in log.lower() or "don't have authorization" in log.lower():
                    delisted += 1
                    yield {
                        "type": "progress",
                        "message": f"  {ticker}: delisted — skipping",
                    }
                    continue

                errors += 1
                yield {
                    "type": "progress",
                    "message": f"  {ticker}: no financials available ({log[:100]})",
                }
                results.append({
                    "company_id": cid,
                    "ticker": ticker,
                    "exchange": exchange,
                    "company_name": name,
                    "error": log[:200],
                    "scores": {},
                    "total_score": 0,
                    "passes": False,
                })
                # Rate limit: small delay between API calls
                time.sleep(0.3)
                continue

            # Rate limit between API calls
            time.sleep(0.3)

        annuals = _get_annuals(data)
        if not annuals:
            errors += 1
            results.append({
                "company_id": cid,
                "ticker": ticker,
                "exchange": exchange,
                "company_name": name,
                "error": "no annuals data in financials",
                "scores": {},
                "total_score": 0,
                "passes": False,
            })
            continue

        # Evaluate criteria
        cr = evaluate_criteria(annuals, cid, as_of_year=as_of_year)
        entry = {
            "company_id": cid,
            "ticker": ticker,
            "exchange": exchange,
            "company_name": name,
            "sector": company.get("sector", ""),
            "country": company.get("country", ""),
            "source": source,
            "scores": cr.scores,
            "details": cr.details,
            "total_score": cr.total_score,
            "passes": cr.passes,
            "eval_date": cr.eval_date.isoformat(),
        }
        results.append(entry)

        score_str = " ".join(f"{k}={'Y' if v else 'N'}" for k, v in cr.scores.items())
        yield {
            "type": "progress",
            "message": f"  {ticker}: score={cr.total_score}/7 [{score_str}] ({source})",
        }

    # Summary
    passing = [r for r in results if r["passes"]]
    screened = total - skipped - delisted
    yield {
        "type": "done",
        "message": f"Screening complete. {len(passing)}/{screened} pass. Skipped: {skipped} (unsubscribed), {delisted} (delisted). API calls: {api_calls}. Errors: {errors}.",
        "data": {
            "results": results,
            "summary": {
                "total": total,
                "screened": screened,
                "passing": len(passing),
                "skipped": skipped,
                "delisted": delisted,
                "forbidden_exchanges": sorted(forbidden_exchanges),
                "api_calls": api_calls,
                "errors": errors,
            },
        },
    }


def build_and_store_universes(
    supabase: Client,
    companies: list[dict],
    start_month: str,
    end_month: str,
    label: str = "default",
    max_companies: int = 0,
) -> Generator[dict, None, None]:
    """Build month-by-month universes and store in universe_snapshot table.

    Uses cached financials (must be fetched first via screen_universe).
    For each month, evaluates all companies and stores results.

    Args:
        start_month: "YYYY-MM" start (inclusive)
        end_month: "YYYY-MM" end (inclusive)
        label: Label for this universe build.
        max_companies: Stop loading after finding this many companies with data (0 = all).

    Yields SSE events with progress and results.
    """
    total = len(companies)
    limit_label = f" (limit: {max_companies})" if max_companies > 0 else ""

    # Step 1: Load financials (cache first, then API fallback)
    yield {"type": "progress", "message": f"Loading financials for {total} companies{limit_label}..."}

    company_annuals: dict[int, dict] = {}
    company_info: dict[int, dict] = {}
    loaded = 0
    missed = 0
    api_calls = 0
    forbidden_exchanges: set[str] = set()

    for i, company in enumerate(companies):
        cid = company["company_id"]
        ticker = company["primary_ticker"]
        exchange = company["primary_exchange"]
        company_info[cid] = company

        # Skip exchanges we know are unsubscribed
        if exchange.upper() in forbidden_exchanges:
            missed += 1
            yield {
                "type": "progress_update",
                "message": f"  Loading financials: {i + 1}/{total} checked, {loaded} loaded, {missed} skipped ({api_calls} API calls)",
            }
            continue

        # Try cache first
        data = _fetch_financials_cached(supabase, ticker, exchange)

        # Fetch from API if not cached
        if data is None:
            data, log = _fetch_financials_api(supabase, ticker, exchange)
            api_calls += 1

            if data is None:
                if "unsubscribed region" in log.lower():
                    forbidden_exchanges.add(exchange.upper())
                    yield {
                        "type": "progress",
                        "message": f"  {ticker}: unsubscribed exchange {exchange} — skipping all {exchange} companies",
                    }
                elif "delisted" in log.lower() or "don't have authorization" in log.lower():
                    pass  # silently skip delisted
                missed += 1
                time.sleep(0.3)
                yield {
                    "type": "progress_update",
                    "message": f"  Loading financials: {i + 1}/{total} checked, {loaded} loaded, {missed} skipped ({api_calls} API calls)",
                }
                continue

            time.sleep(0.3)

        if data:
            annuals = _get_annuals(data)
            if annuals:
                company_annuals[cid] = annuals
                loaded += 1
            else:
                missed += 1
        else:
            missed += 1

        yield {
            "type": "progress_update",
            "message": f"  Loading financials: {i + 1}/{total} checked, {loaded} loaded, {missed} skipped ({api_calls} API calls)",
        }

        if max_companies > 0 and loaded >= max_companies:
            yield {
                "type": "progress",
                "message": f"  Reached limit of {max_companies} companies — stopping load.",
            }
            break

    yield {
        "type": "progress",
        "message": f"Loaded financials for {loaded}/{total} companies ({missed} skipped, {api_calls} API calls).",
    }

    # Step 2: Generate month list
    months = _month_range(start_month, end_month)
    yield {
        "type": "progress",
        "message": f"Building universes for {len(months)} months ({start_month} to {end_month})...",
    }

    # Step 3: For each month, evaluate all companies and store
    monthly_summary: list[dict] = []
    n_companies = len(company_annuals)
    n_months = len(months)

    for mi, month_key in enumerate(months):
        # Use previous year's December as evaluation date
        # e.g. for 2020-03 we evaluate as of 2019-12
        year = int(month_key[:4])
        as_of = f"{year - 1}-12"

        prefix = f"  [{mi + 1}/{n_months}] {month_key}"

        # Phase 1: Evaluate all companies
        rows: list[dict] = []
        passing_count = 0
        cid_list = list(company_annuals.items())

        yield {
            "type": "progress_update",
            "message": f"{prefix}: evaluating 0/{n_companies}...",
        }

        for idx, (cid, annuals) in enumerate(cid_list):
            cr = evaluate_criteria(annuals, cid, as_of_year=as_of)
            rows.append({
                "label": label,
                "target_month": month_key,
                "company_id": cid,
                "total_score": cr.total_score,
                "scores": cr.scores,
                "details": cr.details,
                "passes": cr.passes,
            })
            if cr.passes:
                passing_count += 1
            yield {
                "type": "progress_update",
                "message": f"{prefix}: {passing_count} pass — evaluated {idx + 1}/{n_companies}",
            }

        # Phase 2: Save to Supabase in small batches
        batch_size = 10
        saved = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            yield {
                "type": "progress_update",
                "message": f"{prefix}: {passing_count} pass — writing to DB {saved}/{n_companies}...",
            }
            supabase.table("universe_snapshot").upsert(
                batch, on_conflict="label,target_month,company_id"
            ).execute()
            saved += len(batch)
            yield {
                "type": "progress_update",
                "message": f"{prefix}: {passing_count} pass — written {saved}/{n_companies} to DB",
            }

        monthly_summary.append({
            "month": month_key,
            "total": n_companies,
            "passing": passing_count,
            "failing": n_companies - passing_count,
        })

        # Final line for this month — append so it stays in the log
        yield {
            "type": "progress",
            "message": f"{prefix}: {passing_count}/{n_companies} pass — done",
        }

    yield {
        "type": "done",
        "message": f"Built and stored {len(months)} monthly universes.",
        "data": {
            "monthly_summary": monthly_summary,
        },
    }


def _month_range(start: str, end: str) -> list[str]:
    """Generate list of YYYY-MM strings from start to end inclusive."""
    sy, sm = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    months = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append(f"{y}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def validate_vs_longequity(
    supabase: Client,
    screen_results: list[dict],
) -> dict:
    """Compare screened universe against actual LongEquity snapshots.

    Returns comparison stats: how many LongEquity companies pass our criteria,
    how many of our qualifying companies are in LongEquity, etc.
    """
    # Get LongEquity snapshot dates
    try:
        resp = supabase.rpc("get_distinct_dates", {"p_source_code": "longequity"}).execute()
        le_dates = sorted([r["target_date"] for r in (resp.data or [])])
    except Exception:
        return {"error": "Could not fetch LongEquity dates"}

    if not le_dates:
        return {"error": "No LongEquity snapshots found"}

    screen_passing_ids = {r["company_id"] for r in screen_results if r["passes"]}

    comparisons = []
    for le_date in le_dates[-6:]:  # Last 6 snapshots
        try:
            resp = supabase.rpc("get_company_ids_for_date", {
                "p_source_code": "longequity",
                "p_target_date": le_date,
            }).execute()
            le_ids = {r["company_id"] for r in (resp.data or [])}
        except Exception:
            continue

        in_both = screen_passing_ids & le_ids
        in_le_only = le_ids - screen_passing_ids
        in_screen_only = screen_passing_ids - le_ids

        comparisons.append({
            "date": le_date,
            "longequity_count": len(le_ids),
            "screen_passing_count": len(screen_passing_ids),
            "overlap": len(in_both),
            "in_longequity_only": len(in_le_only),
            "in_screen_only": len(in_screen_only),
            "overlap_pct": round(len(in_both) / len(le_ids) * 100, 1) if le_ids else 0,
            "le_only_ids": sorted(in_le_only),
            "screen_only_ids": sorted(in_screen_only),
        })

    return {"comparisons": comparisons}
