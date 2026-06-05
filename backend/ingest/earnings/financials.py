"""GuruFocus financials endpoint: parse + cache + load.

The response is a deeply nested dict with three top-level period blocks
(`annuals`, `quarterly`, `quarterlys`). Each leaf field is a parallel
array aligned with the block's period column. We flatten that nested
structure into one `metric_data` row per (field path, period)."""
from __future__ import annotations

from datetime import date
from itertools import zip_longest
from typing import Any
from urllib.parse import quote

from supabase import Client

from ingest.api_usage import track_api_call
from ingest.constants import DATA_CUTOFF
from ingest.staleness import is_cache_fresh

from ._api_client import _api_request, _build_api_url, _mask_url
from ._common import (
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


def _extract_financials_dates(data: dict) -> list[date]:
    """Extract all target dates from a financials JSON response."""
    financials = data.get("financials")
    if not isinstance(financials, dict):
        return []
    dates: set[date] = set()
    for block_name in ("annuals", "quarterly", "quarterlys"):
        block = financials.get(block_name)
        if not isinstance(block, dict):
            continue
        for c in ("Fiscal Year", "Fiscal Quarter", "Quarter", "Date", "date"):
            if c in block and isinstance(block[c], list):
                for ps in block[c]:
                    td = _yyyy_mm_to_month_end(str(ps).strip())
                    if td and td >= DATA_CUTOFF:
                        dates.add(td)
                break
    return sorted(dates)


def _parse_financials(data: dict, company_id: int) -> list[dict]:
    """Parse GuruFocus /financials response into metric_data rows."""
    financials = data.get("financials")
    if not isinstance(financials, dict):
        return []

    rows = []
    for block_name in ("annuals", "quarterly", "quarterlys"):
        block = financials.get(block_name)
        if not isinstance(block, dict) or not block:
            continue

        # Find period column
        period_key = None
        for c in ("Fiscal Year", "Fiscal Quarter", "Quarter", "Date", "date"):
            if c in block and isinstance(block[c], list) and block[c]:
                period_key = c
                break
        if not period_key:
            continue

        period_strs = [str(p).strip() for p in block[period_key]]

        # Build date map
        target_dates: dict[str, date] = {}
        for ps in period_strs:
            if ps.upper() == "TTM":
                continue
            td = _yyyy_mm_to_month_end(ps)
            if td and td >= DATA_CUTOFF:
                target_dates[ps] = td

        # Flatten nested structure
        def _flatten(node: Any, prefix: list[str]):
            if isinstance(node, dict):
                for k, v in node.items():
                    yield from _flatten(v, prefix + [str(k)])
            else:
                yield prefix, node

        for top_key, top_val in block.items():
            if top_key == period_key:
                continue
            for path_parts, leaf in _flatten(top_val, [block_name, str(top_key)]):
                metric_code = "__".join(path_parts)
                if isinstance(leaf, list):
                    # Pre-scan: only record this field's period rows if at
                    # least one period has a real number. That keeps storage
                    # bounded for fields GF never populates for this company
                    # (e.g. "Effective Interest Rate on Debt %" on a debt-free
                    # name) while still letting us emit null rows for periods
                    # where GF returns "N/A". Without this, the dashboard
                    # silently falls back to a stale numeric value from years
                    # ago — see AAPL Interest Coverage 2023 vs the actual 2025
                    # period being "N/A".
                    if not any(_coerce_float(v) is not None for v in leaf):
                        continue
                    for ps, v in zip_longest(period_strs, leaf, fillvalue=None):
                        if ps is None or ps.upper() == "TTM":
                            continue
                        td = target_dates.get(ps)
                        if td is None:
                            continue
                        val = _coerce_float(v)
                        # `val` may be None when GF reported "N/A" for this
                        # period; we still emit the row so the frontend can
                        # show the period exists (with no meaningful value)
                        # rather than walk back to the last numeric.
                        rows.append({
                            "company_id": company_id,
                            "metric_code": metric_code,
                            "source_code": "gurufocus",
                            "target_date": td.isoformat(),
                            "numeric_value": val,
                            "is_prediction": False,
                        })

    return rows


def fetch_financials(
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

    result = EarningsResult(source="financials")
    _ensure_bucket(supabase)
    path = _storage_path(ticker, exchange, "financials")
    symbol = _build_symbol(ticker, exchange)

    # Check cache
    cached = None
    need_api = True
    if not force_refresh:
        cached = _fetch_from_storage(supabase, path)
        if cached is not None:
            dates = _extract_financials_dates(cached) if isinstance(cached, dict) else []
            fresh, reason = is_cache_fresh(dates) if dates else (False, "no dates parsed")
            if fresh:
                need_api = False
                result.cache_status = "cache_hit"
                _log(f"Cache fresh ({reason})")
            else:
                _log(f"Cache stale ({reason}), refreshing from API")

    # Fetch from API if needed
    if need_api:
        url = _build_api_url(f"stock/{quote(symbol, safe=':')}/financials", {"order": "desc"})
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

    # Parse and load into DB (always, even on cache hit)
    rows = _parse_financials(cached, company_id)
    result.metrics_found = len(set(r["metric_code"] for r in rows))
    _log(f"Parsed {len(rows)} rows, {result.metrics_found} metrics")
    result.rows_loaded = _upsert_metric_rows(supabase, rows)
    _log(f"Loaded {result.rows_loaded} rows into DB")
    return result
