"""GuruFocus indicator fetch + exchange/currency metadata.

Endpoints:
    POST /api/indicators/fetch                fetch an indicator (price, volume, …)
                                             for an (exchange, ticker), cache the
                                             raw API response in Storage, and
                                             upsert parsed time-series into
                                             metric_data.
    GET  /api/gurufocus/exchanges            list of GuruFocus exchanges (cached)
    GET  /api/gurufocus/exchange-currencies  exchange_code → currency mapping
                                             derived from country_currency
"""

from __future__ import annotations

import asyncio
import os
from datetime import date

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel

from deps import supabase
from routers._cache_headers import CACHE_STATIC
from ingest.api_usage import track_api_call
from ingest.prices import (
    _PRICE_CUTOFF,
    _USER_AGENT,
    _build_symbol,
    _ensure_bucket,
    _fetch_from_storage,
    _fetch_indicator_from_api,
    _parse_price_series,
    _storage_path,
    _upload_to_storage,
)

router = APIRouter(tags=["indicators"])


class IndicatorRequest(BaseModel):
    exchange: str
    ticker: str
    indicator: str = "price"
    force_refresh: bool = False
    from_date: str | None = None
    to_date: str | None = None


@router.post("/api/indicators/fetch")
async def indicators_fetch(req: IndicatorRequest):
    """Fetch an indicator from GuruFocus, cache the raw response in Storage,
    upsert parsed time-series into metric_data. Resolves (creates if
    needed) the company row first."""
    exchange = req.exchange.upper()
    ticker = req.ticker.upper()
    indicator = req.indicator.lower()

    def work():
        symbol = _build_symbol(ticker, exchange)
        path = _storage_path(ticker, exchange, indicator)
        logs: list[str] = []

        # 1. Find or create company row.
        exch_resp = supabase.table("gurufocus_exchange").select("exchange_id").eq("exchange_code", exchange).limit(1).execute()
        exchange_id = exch_resp.data[0]["exchange_id"] if exch_resp.data else None

        existing = supabase.table("company").select("company_id").eq(
            "gurufocus_ticker", ticker
        ).eq("exchange_id", exchange_id).execute()

        if existing.data:
            company_id = existing.data[0]["company_id"]
            logs.append(f"Found company {symbol} (id={company_id})")
        else:
            created = supabase.table("company").insert({
                "gurufocus_ticker": ticker,
                "exchange_id": exchange_id,
                "company_name": symbol,
            }).execute()
            company_id = created.data[0]["company_id"]
            logs.append(f"Created company {symbol} (id={company_id})")

        # 2. Check Storage cache.
        _ensure_bucket(supabase)
        cached = None
        if not req.force_refresh:
            cached = _fetch_from_storage(supabase, path)
            if cached is not None:
                logs.append(f"Found cached data at {path}")

        # 3. Fetch from API if needed.
        api_data = None
        if cached is None or req.force_refresh:
            data, api_log, _http_status = _fetch_indicator_from_api(ticker, exchange, indicator)
            track_api_call(supabase, exchange)
            logs.append(api_log)
            if data is not None:
                _upload_to_storage(supabase, path, data)
                logs.append(f"Cached raw response to {path}")
                api_data = data
            elif cached is not None:
                logs.append("API failed, falling back to stale cache")
                api_data = cached
            else:
                return {
                    "success": False,
                    "symbol": symbol,
                    "error": f"No data available for {symbol}/{indicator}",
                    "logs": logs,
                }
        else:
            api_data = cached

        # 4. Parse time-series.
        parsed = _parse_price_series(api_data)
        logs.append(f"Parsed {len(parsed)} data points")

        if not parsed:
            return {
                "success": False,
                "symbol": symbol,
                "error": "Parsed 0 data points from response",
                "logs": logs,
                "raw_preview": str(api_data)[:500],
            }

        # 5. Map indicator name to metric_code.
        metric_code_map = {"price": "close_price", "volume": "volume"}
        metric_code = metric_code_map.get(indicator, indicator)

        # 6. Upsert into metric_data.
        rows = [
            {
                "company_id": company_id,
                "metric_code": metric_code,
                "source_code": "gurufocus",
                "target_date": d.isoformat(),
                "numeric_value": v,
            }
            for d, v in parsed
            if d >= _PRICE_CUTOFF
        ]
        total_loaded = 0
        batch_size = 500
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            resp = supabase.table("metric_data").upsert(
                batch,
                on_conflict="company_id,metric_code,source_code,target_date",
                ignore_duplicates=False,
            ).execute()
            total_loaded += len(resp.data)

        logs.append(f"Loaded {total_loaded} rows into metric_data (metric_code={metric_code})")

        # 7. Build the response window — explicit from/to if provided,
        # otherwise last 30 points.
        sorted_parsed = sorted(parsed, key=lambda x: x[0])
        date_range = {
            "first": sorted_parsed[0][0].isoformat(),
            "last": sorted_parsed[-1][0].isoformat(),
        }

        def _parse_iso(s: str | None):
            if not s:
                return None
            try:
                return date.fromisoformat(s)
            except ValueError:
                return None

        frm = _parse_iso(req.from_date)
        to = _parse_iso(req.to_date)
        if frm or to:
            window = [
                (d, v) for d, v in sorted_parsed
                if (frm is None or d >= frm) and (to is None or d <= to)
            ]
            recent = [{"date": d.isoformat(), "value": v} for d, v in window]
        else:
            recent = [{"date": d.isoformat(), "value": v} for d, v in sorted_parsed[-30:]]

        return {
            "success": True,
            "symbol": symbol,
            "company_id": company_id,
            "indicator": indicator,
            "metric_code": metric_code,
            "total_points": len(parsed),
            "rows_loaded": total_loaded,
            "date_range": date_range,
            "source": "cache" if cached is not None and not req.force_refresh else "api",
            "recent": recent,
            "logs": logs,
        }

    return await asyncio.to_thread(work)


def _gf_creds():
    base_url = os.environ.get("GURUFOCUS_BASE_URL", "").strip().rstrip("/")
    if base_url.endswith("/data"):
        base_url = base_url[: -len("/data")]
    api_key = os.environ.get("GURUFOCUS_API_KEY", "")
    if not base_url or not api_key:
        raise HTTPException(status_code=500, detail="GURUFOCUS env vars not set")
    return base_url, api_key


@router.get("/api/gurufocus/exchanges")
async def gurufocus_exchanges(response: Response, force_refresh: bool = False):
    """Supported GuruFocus exchanges. Raw response cached in Supabase Storage."""
    # Exchange list is essentially immutable -- GuruFocus may add/remove exchanges
    # but never within the duration of a normal user session. Long browser cache
    # is safe; a hard refresh on the frontend bypasses it.
    response.headers["Cache-Control"] = CACHE_STATIC
    def work():
        path = "meta/exchange_list.json"
        _ensure_bucket(supabase)

        if not force_refresh:
            cached = _fetch_from_storage(supabase, path)
            if cached is not None:
                return {"exchanges": cached, "source": "cache"}

        base_url, api_key = _gf_creds()
        import requests as req
        url = f"{base_url}/public/user/{api_key}/exchange_list"
        resp = req.get(url, timeout=30, headers={"User-Agent": _USER_AGENT})
        resp.raise_for_status()
        data = resp.json()
        _upload_to_storage(supabase, path, data)
        return {"exchanges": data, "source": "api"}

    return await asyncio.to_thread(work)


@router.get("/api/gurufocus/exchange-currencies")
async def gurufocus_exchange_currencies(response: Response, force_refresh: bool = False):
    """Build exchange_code → currency map by joining exchange_list with
    country_currency from GuruFocus. Raw responses cached in Storage."""
    response.headers["Cache-Control"] = CACHE_STATIC
    def work():
        _ensure_bucket(supabase)
        import requests as req

        base_url, api_key = _gf_creds()

        # 1. exchange_list: country → [codes]
        exch_path = "meta/exchange_list.json"
        exchanges = None
        if not force_refresh:
            exchanges = _fetch_from_storage(supabase, exch_path)
        if exchanges is None:
            r = req.get(
                f"{base_url}/public/user/{api_key}/exchange_list",
                timeout=30, headers={"User-Agent": _USER_AGENT},
            )
            r.raise_for_status()
            exchanges = r.json()
            _upload_to_storage(supabase, exch_path, exchanges)

        # 2. country_currency: [{country, country_ISO, currency}]
        curr_path = "meta/country_currency.json"
        currencies_raw = None
        if not force_refresh:
            currencies_raw = _fetch_from_storage(supabase, curr_path)
        if currencies_raw is None:
            r = req.get(
                f"{base_url}/public/user/{api_key}/country_currency",
                timeout=30, headers={"User-Agent": _USER_AGENT},
            )
            r.raise_for_status()
            currencies_raw = r.json()
            _upload_to_storage(supabase, curr_path, currencies_raw)

        # 3. Join: exchange_code → {country, currency}
        country_to_currency = {c["country"]: c["currency"] for c in currencies_raw}
        mapping: list[dict] = []
        unmapped: list[dict] = []
        for country, codes in exchanges.items():
            curr = country_to_currency.get(country)
            if curr:
                for code in codes:
                    mapping.append({
                        "exchange_code": code,
                        "country": country,
                        "currency": curr,
                        "source": "gurufocus",
                    })
            else:
                unmapped.append({"country": country, "codes": codes})

        return {
            "mapping": mapping,
            "total": len(mapping),
            "unmapped": unmapped,
            "source": "cache" if not force_refresh else "api",
        }

    return await asyncio.to_thread(work)
