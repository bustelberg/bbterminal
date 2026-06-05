"""Persistence + storage-coverage probes for index universes.

`store_index_membership` is the shared write path — both SP500 import
and ACWI save go through it. Membership rows go into
`universe_membership` (one row per ticker × month), changes go into
`gurufocus-raw` storage as JSON, and the per-universe `universe` row is
created on demand.

`load_changes` reads the changes JSON back out of storage.
`check_gurufocus_availability` is the cache-coverage probe used by the
/api/index-universe/check-gurufocus SSE endpoint."""
from __future__ import annotations

import json
import logging
from typing import Callable

from supabase import Client

from deps import chunked


log = logging.getLogger(__name__)
_BUCKET = "gurufocus-raw"


def _clear_membership_batched(
    supabase: Client,
    universe_id: int,
    emit: Callable[[str], None],
) -> None:
    """Delete every membership row for a universe WITHOUT a single huge
    DELETE.

    A `DELETE ... WHERE universe_id = ?` over a large universe exceeds
    Postgres' statement_timeout and 500s with code 57014. ACWI alone is
    ~9k membership rows PER MONTH × hundreds of months → 500k+ rows in one
    statement. We instead delete a couple months at a time.

    The month list comes from the `universe_available_months` RPC, which
    does a server-side `SELECT DISTINCT target_month` — so the list itself
    is tiny (~290 rows) and never hits PostgREST's max-rows cap. That's the
    trap the old single-statement delete was working around: the previous
    "paginate-then-dedupe in Python" approach silently lost months when the
    SELECT capped out, leaving orphan rows. Using the RPC keeps the
    every-month-including-orphans guarantee while staying under the timeout.
    """
    try:
        resp = supabase.rpc(
            "universe_available_months", {"p_universe_id": universe_id}
        ).execute()
        months = sorted({
            r.get("target_month")
            for r in (resp.data or [])
            if r.get("target_month")
        })
    except Exception as e:
        log.warning(
            "universe_available_months RPC failed for universe_id=%s (%s); "
            "falling back to single-statement delete",
            universe_id, e,
        )
        supabase.table("universe_membership").delete().eq(
            "universe_id", universe_id
        ).execute()
        return

    if not months:
        return  # nothing to clear

    # Delete 2 months at a time via an `in_` predicate on the indexed
    # (universe_id, target_month) columns. At ~9k rows/month that's ~18k
    # rows per statement — well under the timeout (an indexed delete of
    # that size is sub-second) while keeping round-trips reasonable. Tune
    # down if a denser universe ever creeps toward the limit.
    done = 0
    for batch in chunked(months, 2):
        supabase.table("universe_membership").delete().eq(
            "universe_id", universe_id
        ).in_("target_month", batch).execute()
        done += len(batch)
        emit(f"Clearing existing data: {done}/{len(months)} months")


def store_index_membership(
    supabase: Client,
    index_name: str,
    monthly_holdings: dict[str, set[str]],
    changes: list[dict],
    company_lookup: dict[str, int],
    on_progress: Callable[[str], None] | None = None,
    sector_lookup: dict[str, str] | None = None,
) -> dict:
    """Store monthly holdings and changes in the database.

    Uses universe + universe_membership tables. Deletes existing data
    for the universe and batch-inserts rows.
    Returns summary stats.
    """
    emit = on_progress or (lambda _: None)

    # Ensure universe exists
    emit(f"Setting up universe '{index_name}'...")
    u_resp = supabase.table("universe").select("universe_id").eq("label", index_name).limit(1).execute()
    if u_resp.data:
        universe_id = u_resp.data[0]["universe_id"]
        emit(f"Clearing existing {index_name} membership data...")
        _clear_membership_batched(supabase, universe_id, emit)
    else:
        resp = supabase.table("universe").insert({"label": index_name, "description": f"{index_name} index"}).execute()
        universe_id = resp.data[0]["universe_id"]

    # Collect all unique tickers for stats
    all_tickers: set[str] = set()
    for tickers in monthly_holdings.values():
        all_tickers |= tickers

    matched = sum(1 for t in all_tickers if t in company_lookup)
    emit(f"Ticker matching: {matched}/{len(all_tickers)} unique tickers have company records")

    # Batch insert membership rows
    months = sorted(monthly_holdings.keys())
    total_rows = 0
    batch: list[dict] = []
    batch_size = 500

    for i, month in enumerate(months):
        # Dedupe by company_id within the month. Multiple iShares
        # tickers can resolve to the same `company_id` (e.g. when the
        # rename path collapses two listings into one canonical row),
        # which would otherwise put duplicate (universe_id, company_id,
        # target_month) tuples into the same upsert batch — Postgres
        # rejects the whole batch with "ON CONFLICT DO UPDATE command
        # cannot affect row a second time" and silently loses all
        # writes from that batch. First-ticker-wins by sort order is
        # arbitrary but deterministic across runs.
        seen_cids: set[int] = set()
        for ticker in sorted(monthly_holdings[month]):
            cid = company_lookup.get(ticker)
            if cid is None:
                continue  # Can't store without a company_id (FK constraint)
            if cid in seen_cids:
                continue
            seen_cids.add(cid)
            row: dict = {
                "universe_id": universe_id,
                "target_month": month,
                "company_id": cid,
                "universe_ticker": ticker,
            }
            if sector_lookup:
                sec = sector_lookup.get(ticker)
                if sec:
                    row["sector"] = sec
            batch.append(row)
            if len(batch) >= batch_size:
                supabase.table("universe_membership").upsert(
                    batch, on_conflict="universe_id,company_id,target_month"
                ).execute()
                total_rows += len(batch)
                batch = []

        if (i + 1) % 50 == 0 or i == len(months) - 1:
            emit(f"Storing months: {i + 1}/{len(months)} ({total_rows + len(batch)} rows)")

    if batch:
        supabase.table("universe_membership").upsert(
            batch, on_conflict="universe_id,company_id,target_month"
        ).execute()
        total_rows += len(batch)

    # Store changes as a JSON blob in the first row's metadata (or a separate approach)
    # For simplicity, store as a separate storage file
    changes_path = f"index_changes/{index_name}.json"
    changes_json = json.dumps(changes, ensure_ascii=False).encode("utf-8")
    try:
        supabase.storage.from_(_BUCKET).upload(
            changes_path, changes_json,
            file_options={"content-type": "application/json"},
        )
    except Exception:
        try:
            supabase.storage.from_(_BUCKET).update(
                changes_path, changes_json,
                file_options={"content-type": "application/json"},
            )
        except Exception:
            log.warning("Could not store changes file for %s", index_name)

    return {
        "months": len(months),
        "total_rows": total_rows,
        "unique_tickers": len(all_tickers),
        "matched_companies": matched,
        "changes_count": len(changes),
    }


def load_changes(supabase: Client, index_name: str) -> list[dict]:
    """Load stored changes for an index from storage."""
    path = f"index_changes/{index_name}.json"
    try:
        raw = supabase.storage.from_(_BUCKET).download(path)
        return json.loads(raw)
    except Exception:
        return []


def check_gurufocus_availability(
    supabase: Client,
    tickers: set[str],
    on_progress: Callable[[str], None] | None = None,
) -> dict:
    """Check which tickers have cached GuruFocus financials data.

    Uses company table to find the correct exchange, then checks storage.
    Falls back to trying NYSE/NASDAQ/AMEX if ticker isn't in company table.
    """
    emit = on_progress or (lambda _: None)

    # Build ticker → exchange lookup from company table
    ticker_exchange: dict[str, str] = {}
    exch_resp = supabase.table("gurufocus_exchange").select("exchange_id, exchange_code").execute()
    eid_to_code = {r["exchange_id"]: r["exchange_code"] for r in (exch_resp.data or [])}
    for exchange in ("NYSE", "NASDAQ"):
        eid = next((eid for eid, code in eid_to_code.items() if code == exchange), None)
        if eid is None:
            continue
        resp = supabase.table("company").select("gurufocus_ticker, exchange_id").eq("exchange_id", eid).execute()
        for row in resp.data:
            ticker_exchange[row["gurufocus_ticker"]] = exchange

    emit(f"Loaded {len(ticker_exchange)} company exchange mappings")

    available: list[str] = []
    missing: list[str] = []
    sorted_tickers = sorted(tickers)

    for i, ticker in enumerate(sorted_tickers):
        found = False

        # Try known exchange first
        known_ex = ticker_exchange.get(ticker)
        if known_ex:
            path = f"{known_ex}_{ticker}/financials.json"
            try:
                supabase.storage.from_(_BUCKET).download(path)
                available.append(ticker)
                found = True
            except Exception:
                pass

        # Fallback: try all US exchanges
        if not found:
            for exchange in ("NYSE", "NASDAQ", "AMEX"):
                path = f"{exchange}_{ticker}/financials.json"
                try:
                    supabase.storage.from_(_BUCKET).download(path)
                    available.append(ticker)
                    found = True
                    break
                except Exception:
                    continue

        if not found:
            missing.append(ticker)

        if (i + 1) % 25 == 0 or i == len(sorted_tickers) - 1:
            emit(f"Checking GuruFocus coverage: {i + 1}/{len(sorted_tickers)} ({len(available)} found)")

    coverage_pct = (len(available) / len(sorted_tickers) * 100) if sorted_tickers else 0
    return {
        "available": available,
        "missing": missing,
        "total": len(sorted_tickers),
        "available_count": len(available),
        "missing_count": len(missing),
        "coverage_pct": round(coverage_pct, 1),
    }
