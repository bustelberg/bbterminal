"""Resolve S&P 500 tickers via OpenFIGI and create missing company records.

The S&P 500 lookup path is simpler than the global one because we know
every ticker is US: we only accept matches with a US exchange code,
default to NYSE if OpenFIGI returns nothing useful, and tag every
resolved row with `source_code='sp500'`."""
from __future__ import annotations

import logging
import os
from typing import Callable

import requests
from supabase import Client


log = logging.getLogger(__name__)


def resolve_and_create_companies(
    supabase: Client,
    tickers: set[str],
    on_progress: Callable[[str], None] | None = None,
    company_info: dict[str, dict] | None = None,
) -> dict[str, int]:
    """Resolve S&P 500 tickers via OpenFIGI and create missing company records.

    If company_info is provided ({ticker: {name, sector, country}}), it will be
    used to fill in empty company_name fields — existing values are never overwritten.

    Returns ticker → company_id mapping for all resolved tickers.
    """
    wiki_info = company_info or {}
    emit = on_progress or (lambda _: None)

    # Load exchange_id map for US exchanges
    exch_resp = supabase.table("gurufocus_exchange").select("exchange_id, exchange_code").execute()
    exchange_id_map = {r["exchange_code"]: r["exchange_id"] for r in (exch_resp.data or [])}

    # Load existing US companies
    existing: dict[str, int] = {}
    _company_cache: dict[int, dict] = {}
    for exchange in ("NYSE", "NASDAQ"):
        eid = exchange_id_map.get(exchange)
        if eid is None:
            continue
        resp = supabase.table("company").select(
            "company_id, gurufocus_ticker, company_name"
        ).eq("exchange_id", eid).execute()
        for row in resp.data:
            existing[row["gurufocus_ticker"]] = row["company_id"]
            _company_cache[row["company_id"]] = row

    already_matched = {t for t in tickers if t in existing}
    to_resolve = sorted(tickers - already_matched)

    emit(f"Company lookup: {len(already_matched)} already in DB, {len(to_resolve)} need resolution")

    # Tag already-matched companies with 'sp500' source via company_source table
    tagged = 0
    enriched = 0
    for t in already_matched:
        cid = existing[t]
        cached = _company_cache.get(cid, {})
        # Ensure company_source row exists for sp500
        try:
            supabase.table("company_source").upsert(
                {"company_id": cid, "source_code": "sp500"},
                on_conflict="company_id,source_code",
                ignore_duplicates=True,
            ).execute()
            tagged += 1
        except Exception:
            pass
        # Fill empty company_name from Wikipedia info
        info = wiki_info.get(t, {})
        updates: dict = {}
        if info.get("name") and not cached.get("company_name"):
            updates["company_name"] = info["name"]
        if updates:
            try:
                supabase.table("company").update(updates).eq("company_id", cid).execute()
                enriched += 1
            except Exception:
                pass
    if tagged or enriched:
        emit(f"Updated {tagged} source tags, enriched {enriched} companies with Wikipedia data")

    if not to_resolve:
        return existing

    # Resolve via OpenFIGI in batches with progress
    unknowns = [{"ticker": t, "country": "USA"} for t in to_resolve]
    total = len(unknowns)
    batch_size = 100
    resolved: list[dict] = []
    unresolved: list[str] = []

    from ingest.resolve_tickers import _exchcode_to_exchange, _best_match, _normalize_ticker_for_gurufocus

    api_key = os.environ.get("OPENFIGI_API_KEY")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    _OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
    _COUNTRY_EXCHCODE = "US"  # All S&P 500 tickers are US

    for i in range(0, total, batch_size):
        batch = unknowns[i : i + batch_size]
        jobs = [{"idType": "TICKER", "idValue": u["ticker"].replace("-", " "), "exchCode": _COUNTRY_EXCHCODE} for u in batch]

        try:
            resp = requests.post(_OPENFIGI_URL, json=jobs, headers=headers, timeout=30)
            resp.raise_for_status()
            items = resp.json()
        except Exception as e:
            emit(f"  OpenFIGI batch error: {e}")
            unresolved.extend(u["ticker"] for u in batch)
            continue

        _US_EXCHCODES = {"US", "UW", "UN", "UA", "UP", "UR", "UQ"}

        for u, item in zip(batch, items):
            if "data" not in item or not item["data"]:
                resolved.append({
                    "ticker": u["ticker"],
                    "gurufocus_ticker": u["ticker"],
                    "gurufocus_exchange": "NYSE",
                })
                continue

            match = _best_match(item["data"])
            if not match:
                resolved.append({
                    "ticker": u["ticker"],
                    "gurufocus_ticker": u["ticker"],
                    "gurufocus_exchange": "NYSE",
                })
                continue

            # Only accept US exchange matches for S&P 500 tickers
            exchcode = match.get("exchCode", "")
            if exchcode not in _US_EXCHCODES:
                resolved.append({
                    "ticker": u["ticker"],
                    "gurufocus_ticker": u["ticker"],
                    "gurufocus_exchange": "NYSE",
                })
                continue

            exchange = _exchcode_to_exchange(exchcode)
            raw_ticker = match.get("ticker") or u["ticker"]
            gf_ticker = _normalize_ticker_for_gurufocus(raw_ticker, exchange)
            resolved.append({
                "ticker": u["ticker"],
                "gurufocus_ticker": gf_ticker,
                "gurufocus_exchange": exchange,
            })

        done = min(i + batch_size, total)
        emit(f"OpenFIGI: {done}/{total} resolved ({len(resolved)} found, {len(unresolved)} missed)")

    # Create company records for resolved tickers not yet in DB
    created = 0
    already_existed = 0
    for j, r in enumerate(resolved):
        gt = r["gurufocus_ticker"]
        ge = r["gurufocus_exchange"]
        eid = exchange_id_map.get(ge)

        # Check if already exists (might match under different raw ticker)
        query = supabase.table("company").select("company_id").eq("gurufocus_ticker", gt)
        if eid is not None:
            query = query.eq("exchange_id", eid)
        check = query.limit(1).execute()

        if check.data:
            cid = check.data[0]["company_id"]
            existing[r["ticker"]] = cid
            existing[gt] = cid
            already_existed += 1
            # Ensure company_source row
            try:
                supabase.table("company_source").upsert(
                    {"company_id": cid, "source_code": "sp500"},
                    on_conflict="company_id,source_code",
                    ignore_duplicates=True,
                ).execute()
            except Exception:
                pass
            continue

        # Create new company record, using Wikipedia info if available
        info = wiki_info.get(r["ticker"], {})
        row = {
            "gurufocus_ticker": gt,
            "exchange_id": eid,
            "company_name": info.get("name") or None,
        }
        try:
            ins = supabase.table("company").insert(row).execute()
            if ins.data:
                cid = ins.data[0]["company_id"]
                existing[r["ticker"]] = cid
                existing[gt] = cid
                created += 1
                # Add company_source
                supabase.table("company_source").upsert(
                    {"company_id": cid, "source_code": "sp500"},
                    on_conflict="company_id,source_code",
                    ignore_duplicates=True,
                ).execute()
        except Exception as e:
            log.warning("Failed to create company %s/%s: %s", gt, ge, e)

        if (j + 1) % 50 == 0:
            emit(f"Creating companies: {j + 1}/{len(resolved)} ({created} new, {already_existed} existing)")

    emit(f"Companies: {created} created, {already_existed} already existed, {len(unresolved)} unresolved")
    return existing
