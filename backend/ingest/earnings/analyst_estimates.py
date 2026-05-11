"""GuruFocus analyst_estimate endpoint: parse + cache + load.

Shape is simpler than financials — two top-level period blocks
(`annual`, `quarterly`), each a dict of parallel arrays keyed by metric
name. All rows land with `is_prediction=True` so the frontend knows to
mark them as forward estimates."""
from __future__ import annotations

from datetime import date
from urllib.parse import quote

from supabase import Client

from ingest.api_usage import track_api_call
from ingest.staleness import is_cache_fresh

from ._api_client import _api_request, _build_api_url, _mask_url
from ._common import (
    _CUTOFF,
    EarningsResult,
    _build_symbol,
    _coerce_float,
    _ensure_bucket,
    _fetch_from_storage,
    _storage_path,
    _upload_to_storage,
    _upsert_metric_rows,
    _yyyy_mm_to_month_end,
)


def _extract_analyst_dates(data: dict) -> list[date]:
    """Extract all target dates from an analyst_estimate JSON response."""
    dates: set[date] = set()
    for freq in ("annual", "quarterly"):
        block = data.get(freq) or {}
        for d in (block.get("date") or []):
            td = _yyyy_mm_to_month_end(d)
            if td and td >= _CUTOFF:
                dates.add(td)
    return sorted(dates)


def _parse_analyst_estimates(data: dict, company_id: int) -> list[dict]:
    rows = []
    for freq in ("annual", "quarterly"):
        block = data.get(freq) or {}
        dates_raw = block.get("date") or []
        target_dates: dict[str, date] = {}
        for d in dates_raw:
            td = _yyyy_mm_to_month_end(d)
            if td and td >= _CUTOFF:
                target_dates[d] = td

        for key, value in block.items():
            if key == "date":
                continue
            metric_code = f"{freq}_{key}"
            if isinstance(value, list):
                for d, v in zip(dates_raw, value):
                    td = target_dates.get(d)
                    if td is None:
                        continue
                    val = _coerce_float(v)
                    if val is None:
                        continue
                    rows.append({
                        "company_id": company_id,
                        "metric_code": metric_code,
                        "source_code": "gurufocus",
                        "target_date": td.isoformat(),
                        "numeric_value": val,
                        "is_prediction": True,
                    })
    return rows


def fetch_analyst_estimates(
    supabase: Client,
    company_id: int,
    ticker: str,
    exchange: str,
    *,
    force_refresh: bool = False,
    on_log: callable = None,
) -> EarningsResult:
    def _log(msg: str):
        result.logs.append(msg)
        if on_log:
            on_log(msg)

    result = EarningsResult(source="analyst_estimates")
    _ensure_bucket(supabase)
    path = _storage_path(ticker, exchange, "analyst_estimate")
    symbol = _build_symbol(ticker, exchange)

    cached = None
    need_api = True
    if not force_refresh:
        cached = _fetch_from_storage(supabase, path)
        if cached is not None:
            dates = _extract_analyst_dates(cached) if isinstance(cached, dict) else []
            fresh, reason = is_cache_fresh(dates) if dates else (False, "no dates parsed")
            if fresh:
                need_api = False
                result.cache_status = "cache_hit"
                _log(f"Cache fresh ({reason})")
            else:
                _log(f"Cache stale ({reason}), refreshing from API")

    if need_api:
        url = _build_api_url(f"stock/{quote(symbol, safe=':')}/analyst_estimate")
        _log(f"Calling {_mask_url(url)} ...")
        api = _api_request(url)
        track_api_call(supabase, exchange)
        result.api_calls += 1
        _log(api.log)
        if api.is_forbidden:
            result.cache_status = "forbidden"
            result.is_forbidden = True
            result.error = f"403 unsubscribed region for {symbol}"
            _log(f"Forbidden — exchange {exchange} not in subscription")
            return result
        if api.data is None:
            if cached is not None:
                _log("API failed, using stale cache")
            else:
                result.cache_status = "api_error"
                result.error = api.log
                return result
        else:
            cached = api.data
            result.cache_status = "api_fresh"
            _upload_to_storage(supabase, path, api.data)
            _log("Cached to storage")

    # Always load into DB
    rows = _parse_analyst_estimates(cached, company_id)
    result.metrics_found = len(set(r["metric_code"] for r in rows))
    _log(f"Parsed {len(rows)} rows, {result.metrics_found} metrics")
    result.rows_loaded = _upsert_metric_rows(supabase, rows)
    _log(f"Loaded {result.rows_loaded} rows into DB")
    return result
