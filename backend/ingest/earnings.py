"""
Fetch earnings-related data from GuruFocus (financials, analyst estimates,
stock indicators), cache raw JSON in Supabase Storage, parse and load
processed metrics into metric_data.

Data cutoff: only dates >= 2015-01-01 are stored in the DB.
"""
from __future__ import annotations

import calendar
import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from itertools import zip_longest
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from supabase import Client

from ingest.staleness import is_cache_fresh
from ingest.api_usage import track_api_call

_BUCKET = "gurufocus-raw"
_CUTOFF = date(2015, 1, 1)

US_EXCHANGES = {"NYSE", "NASDAQ", "AMEX"}

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Indicator keys we need for the earnings dashboard (quarterly variants)
INDICATOR_KEYS = [
    "interest_coverage",
    "roe",
    "roic",
    "gross_margin",
    "net_margin",
    "forward_pe_ratio",
    "peg_ratio",
    "fcf_yield",
]


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class EarningsResult:
    source: str = ""  # "financials", "analyst_estimates", "indicators"
    rows_loaded: int = 0
    metrics_found: int = 0
    cache_status: str = ""  # "cache_hit", "api_fresh", "api_error"
    logs: list[str] = field(default_factory=list)
    error: str | None = None
    is_forbidden: bool = False  # True if 403 / unsubscribed region
    api_calls: int = 0  # Number of GuruFocus API requests made


# ---------------------------------------------------------------------------
# Helpers (shared with prices.py patterns)
# ---------------------------------------------------------------------------

def _build_symbol(ticker: str, exchange: str) -> str:
    if exchange.upper() in US_EXCHANGES:
        return ticker
    return f"{exchange}:{ticker}"


def _storage_path(ticker: str, exchange: str, endpoint: str) -> str:
    return f"{exchange.upper()}_{ticker.upper()}/{endpoint}.json"


def _ensure_bucket(supabase: Client) -> None:
    try:
        supabase.storage.create_bucket(_BUCKET, options={"public": False})
    except Exception:
        pass


def _fetch_from_storage(supabase: Client, path: str) -> dict | list | None:
    try:
        raw = supabase.storage.from_(_BUCKET).download(path)
        return json.loads(raw)
    except Exception:
        return None


def _upload_to_storage(supabase: Client, path: str, data: Any) -> None:
    content = json.dumps(data, ensure_ascii=False).encode("utf-8")
    try:
        supabase.storage.from_(_BUCKET).upload(
            path, content, file_options={"content-type": "application/json"}
        )
    except Exception as e:
        msg = str(e).lower()
        if "already exists" not in msg and "duplicate" not in msg and "409" not in msg:
            raise
        try:
            supabase.storage.from_(_BUCKET).update(
                path, content, file_options={"content-type": "application/json"}
            )
        except Exception:
            pass


_last_api_call: float = 0.0
_API_MIN_INTERVAL = 1.5  # seconds between requests

# Use curl if available — Python's urllib TLS fingerprint gets blocked by Cloudflare
_HAS_CURL = shutil.which("curl") is not None


class ApiResult:
    """Structured API response with status code for 403 detection."""
    __slots__ = ("data", "log", "status_code")

    def __init__(self, data: Any | None, log: str, status_code: int | None = None):
        self.data = data
        self.log = log
        self.status_code = status_code

    @property
    def is_forbidden(self) -> bool:
        """True if the response indicates an unsubscribed region.

        Only triggers on 'unsubscribed region' in the body, NOT on bare 403s,
        because a 403 can also mean a specific ticker is restricted/delisted.
        """
        if self.data is None and self.log and "unsubscribed region" in self.log.lower():
            return True
        return False


def _api_request_curl(url: str, timeout: int = 30) -> ApiResult:
    """Fetch via curl subprocess to bypass Cloudflare TLS fingerprinting."""
    masked_url = url
    api_key = os.environ.get("GURUFOCUS_API_KEY", "")
    if api_key:
        masked_url = url.replace(api_key, api_key[:4] + "***")

    try:
        # Use -w to capture HTTP status code, remove -f so we get the body on errors
        result = subprocess.run(
            ["curl", "-s", "--max-time", str(timeout),
             "-w", "\n%{http_code}",
             "-H", f"User-Agent: {_USER_AGENT}",
             "-H", "Accept: application/json",
             url],
            capture_output=True, text=True, timeout=timeout + 5,
        )
        if result.returncode != 0 and result.returncode != 22:
            stderr = result.stderr.strip()[:200] if result.stderr else ""
            return ApiResult(None, f"curl exit {result.returncode}: {stderr} ({masked_url})")

        # Split body from status code (last line)
        output = result.stdout.rsplit("\n", 1)
        body = output[0] if len(output) > 1 else result.stdout
        status_code = int(output[-1]) if len(output) > 1 and output[-1].isdigit() else None

        if status_code and status_code >= 400:
            return ApiResult(None, f"API HTTP {status_code} body={body[:200]} ({masked_url})", status_code)
        if not body:
            return ApiResult(None, f"API empty response ({masked_url})", status_code)
        return ApiResult(json.loads(body), f"API OK ({masked_url})", status_code)
    except subprocess.TimeoutExpired:
        return ApiResult(None, f"API timeout after {timeout}s ({masked_url})")
    except json.JSONDecodeError as e:
        return ApiResult(None, f"API returned invalid JSON: {e} ({masked_url})")
    except Exception as e:
        return ApiResult(None, f"curl error: {type(e).__name__}: {e} ({masked_url})")


def _api_request_urllib(url: str, timeout: int = 30) -> ApiResult:
    """Fetch via urllib (fallback if curl not available)."""
    masked_url = url
    api_key = os.environ.get("GURUFOCUS_API_KEY", "")
    if api_key:
        masked_url = url.replace(api_key, api_key[:4] + "***")

    req = Request(url, headers={"User-Agent": _USER_AGENT, "Accept": "application/json"})
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            if not raw:
                return ApiResult(None, f"API empty response ({masked_url})", resp.status)
            return ApiResult(json.loads(raw), f"API OK ({masked_url})", resp.status)
    except HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
        return ApiResult(None, f"API HTTP {e.code}: {e.reason} body={body} ({masked_url})", e.code)
    except URLError as e:
        return ApiResult(None, f"API URL error: {e.reason}")
    except Exception as e:
        return ApiResult(None, f"API error: {type(e).__name__}: {e}")


def _api_request(url: str, timeout: int = 30) -> ApiResult:
    global _last_api_call
    elapsed = time.time() - _last_api_call
    if elapsed < _API_MIN_INTERVAL:
        time.sleep(_API_MIN_INTERVAL - elapsed)
    _last_api_call = time.time()

    if _HAS_CURL:
        return _api_request_curl(url, timeout)
    return _api_request_urllib(url, timeout)


def _mask_url(url: str) -> str:
    api_key = os.environ.get("GURUFOCUS_API_KEY", "")
    if api_key:
        return url.replace(api_key, api_key[:4] + "***")
    return url


def _build_api_url(path: str, query: dict[str, str] | None = None) -> str:
    base_url = os.environ.get("GURUFOCUS_BASE_URL", "").strip().rstrip("/")
    if base_url.endswith("/data"):
        base_url = base_url[: -len("/data")]
    api_key = os.environ.get("GURUFOCUS_API_KEY", "")
    url = f"{base_url}/public/user/{api_key}/{path}"
    if query:
        url = f"{url}?{urlencode(query)}"
    return url


def _coerce_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    if s.upper() in {"", "N/A", "NA", "NONE", "NULL", "-"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _yyyy_mm_to_month_end(yyyy_mm: str) -> date | None:
    """'YYYY-MM' → last day of that month."""
    s = str(yyyy_mm).strip().replace("-", "")
    if len(s) < 6:
        return None
    try:
        year = int(s[:4])
        month = int(s[4:6])
        day = calendar.monthrange(year, month)[1]
        return date(year, month, day)
    except Exception:
        return None


def _upsert_metric_rows(supabase: Client, rows: list[dict]) -> int:
    if not rows:
        return 0
    batch_size = 500
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        resp = supabase.table("metric_data").upsert(
            batch, on_conflict="company_id,metric_code,source_code,target_date",
            ignore_duplicates=False,
        ).execute()
        total += len(resp.data)
    return total


# ---------------------------------------------------------------------------
# Date extraction helpers (for staleness checks)
# ---------------------------------------------------------------------------

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
                    if td and td >= _CUTOFF:
                        dates.add(td)
                break
    return sorted(dates)


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


def _extract_indicator_dates(data: Any) -> list[date]:
    """Extract all dates from a single indicator JSON response."""
    pairs = _extract_indicator_series(data)
    dates: list[date] = []
    for d_raw, _ in pairs:
        td = _parse_indicator_date(d_raw)
        if td and td >= _CUTOFF:
            dates.append(td)
    return sorted(set(dates))


# ===========================================================================
# 1. FINANCIALS
# ===========================================================================

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
            if td and td >= _CUTOFF:
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
                    for ps, v in zip_longest(period_strs, leaf, fillvalue=None):
                        if ps is None or ps.upper() == "TTM":
                            continue
                        td = target_dates.get(ps)
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


# ===========================================================================
# 2. ANALYST ESTIMATES
# ===========================================================================

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


# ===========================================================================
# 3. STOCK INDICATORS (quarterly)
# ===========================================================================

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
