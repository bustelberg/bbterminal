"""GuruFocus per-indicator endpoint: parse + cache + load.

Indicators (forward P/E and similar forward-looking series) come from
their own endpoint, one HTTP call per key. The response shape is
inconsistent across endpoints — sometimes a list of [date, value] pairs,
sometimes parallel `date` + `value` arrays, sometimes nested under
`indicator` / `data`. `_extract_indicator_series` walks every variant
we've seen."""
from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any
from urllib.parse import quote

from supabase import Client

from ingest.api_usage import track_api_call
from ingest.staleness import is_cache_fresh

from ._api_client import _api_request, _build_api_url, _mask_url
from ._common import (
    _CUTOFF,
    INDICATOR_KEYS,
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


_RE_YYYYMM = re.compile(r"^\d{6}$")
_RE_ISO_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_indicator_date(d: Any) -> date | None:
    if d is None:
        return None
    s = str(d).strip()
    if not s:
        return None
    if _RE_YYYYMM.match(s):
        return _yyyy_mm_to_month_end(s)
    if _RE_ISO_DAY.match(s):
        try:
            return date.fromisoformat(s)
        except Exception:
            return None
    # Unix timestamp
    if s.isdigit() and len(s) >= 10:
        try:
            n = int(s)
            if len(s) == 13:
                n = n // 1000
            return datetime.utcfromtimestamp(n).date()
        except Exception:
            return None
    return None


def _extract_indicator_series(obj: Any) -> list[tuple[Any, Any]]:
    """Extract (date, value) pairs from various GuruFocus indicator response shapes."""
    if isinstance(obj, list):
        # List of [date, value] pairs
        if obj and isinstance(obj[0], (list, tuple)) and len(obj[0]) >= 2:
            return [(r[0], r[1]) for r in obj if isinstance(r, (list, tuple)) and len(r) >= 2]
        # List of {date, value} dicts
        if obj and isinstance(obj[0], dict):
            out = []
            for r in obj:
                if not isinstance(r, dict):
                    continue
                d = r.get("date") or r.get("asOfDate") or r.get("period")
                v = r.get("value")
                if d is not None and v is not None:
                    out.append((d, v))
            return out
        return []

    if not isinstance(obj, dict):
        return []

    # Check nested "indicator" key
    ind = obj.get("indicator")
    if isinstance(ind, (dict, list)):
        pairs = _extract_indicator_series(ind)
        if pairs:
            return pairs

    # Check "data" key
    if "data" in obj:
        pairs = _extract_indicator_series(obj["data"])
        if pairs:
            return pairs

    # Parallel date/value arrays
    if isinstance(obj.get("date"), list):
        dates = obj["date"]
        if isinstance(obj.get("value"), list):
            return list(zip(dates, obj["value"]))
        for k, v in obj.items():
            if k != "date" and isinstance(v, list) and len(v) == len(dates):
                return list(zip(dates, v))

    # Recurse into dict values
    for v in obj.values():
        if isinstance(v, (dict, list)):
            pairs = _extract_indicator_series(v)
            if pairs:
                return pairs

    return []


def _extract_indicator_dates(data: Any) -> list[date]:
    """Extract all dates from a single indicator JSON response."""
    pairs = _extract_indicator_series(data)
    dates: list[date] = []
    for d_raw, _ in pairs:
        td = _parse_indicator_date(d_raw)
        if td and td >= _CUTOFF:
            dates.append(td)
    return sorted(set(dates))


def _parse_single_indicator(data: Any, indicator_key: str, company_id: int) -> list[dict]:
    metric_code = f"indicator_q_{indicator_key}"
    pairs = _extract_indicator_series(data)
    rows = []
    for d_raw, v_raw in pairs:
        td = _parse_indicator_date(d_raw)
        val = _coerce_float(v_raw)
        if td is None or val is None or td < _CUTOFF:
            continue
        rows.append({
            "company_id": company_id,
            "metric_code": metric_code,
            "source_code": "gurufocus",
            "target_date": td.isoformat(),
            "numeric_value": val,
            "is_prediction": False,
        })
    return rows


def fetch_indicators(
    supabase: Client,
    company_id: int,
    ticker: str,
    exchange: str,
    *,
    force_refresh: bool = False,
    indicator_keys: list[str] | None = None,
    on_log: callable = None,
) -> EarningsResult:
    def _log(msg: str):
        result.logs.append(msg)
        if on_log:
            on_log(msg)

    keys = indicator_keys or INDICATOR_KEYS
    result = EarningsResult(source="indicators")
    _ensure_bucket(supabase)
    symbol = _build_symbol(ticker, exchange)

    all_rows: list[dict] = []

    for key in keys:
        path = _storage_path(ticker, exchange, f"indicator_q_{key}")

        cached = None
        need_api = True
        if not force_refresh:
            cached = _fetch_from_storage(supabase, path)
            if cached is not None:
                dates = _extract_indicator_dates(cached)
                fresh, reason = is_cache_fresh(dates) if dates else (False, "no dates parsed")
                if fresh:
                    need_api = False
                    _log(f"{key}: cache fresh ({reason})")
                else:
                    _log(f"{key}: cache stale ({reason})")

        if need_api:
            url = _build_api_url(
                f"stock/{quote(symbol, safe=':')}/{quote(key, safe='')}",
                {"type": "quarterly"},
            )
            _log(f"{key}: calling {_mask_url(url)} ...")
            api = _api_request(url)
            track_api_call(supabase, exchange)
            result.api_calls += 1
            _log(f"{key}: {api.log}")
            if api.is_forbidden:
                result.is_forbidden = True
                _log(f"{key}: Forbidden — exchange {exchange} not in subscription")
                # No point trying more indicators for this exchange
                break
            if api.data is None:
                if cached is not None:
                    _log(f"{key}: API failed, using stale cache")
                else:
                    continue
            else:
                cached = api.data
                _upload_to_storage(supabase, path, api.data)

        # Always parse and load into DB
        rows = _parse_single_indicator(cached, key, company_id)
        all_rows.extend(rows)
        _log(f"{key}: {len(rows)} rows parsed")

    result.cache_status = "mixed"
    result.metrics_found = len(set(r["metric_code"] for r in all_rows))
    _log(f"Total: {len(all_rows)} rows, {result.metrics_found} metrics")
    result.rows_loaded = _upsert_metric_rows(supabase, all_rows)
    _log(f"Loaded {result.rows_loaded} rows into DB")
    return result
