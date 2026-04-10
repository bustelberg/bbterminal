"""
Fetch daily closing prices from GuruFocus, cache raw JSON in Supabase Storage,
and load into metric_data (only dates >= 2023-01-01).
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from supabase import Client

from ingest.staleness import is_cache_fresh
from ingest.api_usage import track_api_call

_BUCKET = "gurufocus-raw"
_PRICE_CUTOFF = date(2015, 1, 1)

US_EXCHANGES = {"NYSE", "NASDAQ", "AMEX"}

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


@dataclass
class PriceResult:
    rows_loaded: int = 0
    total_prices: int = 0
    source: str = ""  # "cache", "api", "stale_cache", "none"
    logs: list[str] = field(default_factory=list)
    error: str | None = None
    is_forbidden: bool = False  # True if 403 / unsubscribed region
    is_delisted: bool = False   # True if 403 / delisted stock
    api_calls: int = 0  # Number of GuruFocus API requests made


def _build_symbol(ticker: str, exchange: str) -> str:
    if exchange.upper() in US_EXCHANGES:
        return ticker
    return f"{exchange}:{ticker}"


def _storage_path(ticker: str, exchange: str, indicator: str = "price") -> str:
    return f"{exchange.upper()}_{ticker.upper()}/indicator__{indicator}.json"


def _ensure_bucket(supabase: Client) -> None:
    try:
        supabase.storage.create_bucket(_BUCKET, options={"public": False})
    except Exception:
        pass


def _fetch_from_storage(supabase: Client, path: str) -> list | None:
    try:
        raw = supabase.storage.from_(_BUCKET).download(path)
        return json.loads(raw)
    except Exception:
        return None


def _upload_to_storage(supabase: Client, path: str, data: list) -> None:
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


_HAS_CURL = shutil.which("curl") is not None


def _fetch_indicator_from_api(ticker: str, exchange: str, indicator: str = "price", timeout: int = 30) -> tuple[list | None, str, int | None]:
    """Fetch an indicator from GuruFocus API. Returns (raw_data, log_message, http_status)."""
    base_url = os.environ.get("GURUFOCUS_BASE_URL", "")
    api_key = os.environ.get("GURUFOCUS_API_KEY", "")
    if not base_url:
        return None, "GURUFOCUS_BASE_URL env var not set", None
    if not api_key:
        return None, "GURUFOCUS_API_KEY env var not set", None

    base = base_url.strip().rstrip("/")
    if base.endswith("/data"):
        base = base[: -len("/data")]

    symbol = _build_symbol(ticker, exchange)
    url = f"{base}/public/user/{api_key}/stock/{quote(symbol, safe=':')}/{indicator}"
    masked_url = url.replace(api_key, api_key[:4] + "***")

    if _HAS_CURL:
        try:
            result = subprocess.run(
                ["curl", "-s", "--max-time", str(timeout),
                 "-w", "\n%{http_code}",
                 "-H", f"User-Agent: {_USER_AGENT}",
                 "-H", "Accept: application/json",
                 url],
                capture_output=True, text=True, timeout=timeout + 5,
            )
            output = result.stdout.rsplit("\n", 1)
            body = output[0] if len(output) > 1 else result.stdout
            status = int(output[-1]) if len(output) > 1 and output[-1].isdigit() else None

            if status and status >= 400:
                return None, f"API HTTP {status} for {symbol} body={body[:200]} ({masked_url})", status
            if result.returncode != 0:
                return None, f"API error for {symbol}: curl exit {result.returncode} ({masked_url})", status
            if not body:
                return None, f"API returned empty response for {symbol} ({masked_url})", status
            return json.loads(body), f"API OK for {symbol} ({masked_url})", status
        except Exception as e:
            return None, f"API error for {symbol}: {type(e).__name__}: {e}", None
    else:
        req = Request(url, headers={"User-Agent": _USER_AGENT, "Accept": "application/json"})
        try:
            with urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                if not raw:
                    return None, f"API returned empty response for {symbol} ({masked_url})", resp.status
                return json.loads(raw), f"API OK for {symbol} ({masked_url})", resp.status
        except HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")[:200]
            except Exception:
                pass
            return None, f"API HTTP {e.code} for {symbol}: {e.reason} body={body} ({masked_url})", e.code
        except URLError as e:
            return None, f"API URL error for {symbol}: {e.reason}", None
        except Exception as e:
            return None, f"API error for {symbol}: {type(e).__name__}: {e}", None


def _fetch_price_from_api(ticker: str, exchange: str, timeout: int = 30) -> tuple[list | None, str, int | None]:
    """Backwards-compatible wrapper for price fetching."""
    return _fetch_indicator_from_api(ticker, exchange, "price", timeout)


def _parse_price_series(data: list | dict) -> list[tuple[date, float]]:
    """Parse GuruFocus price response into [(date, price)] pairs."""
    results: list[tuple[date, float]] = []

    items: list = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                items = v
                break

    for item in items:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        date_str, price_val = item[0], item[1]
        try:
            if isinstance(date_str, str) and len(date_str) == 10:
                # Try ISO format (YYYY-MM-DD) first, then MM-DD-YYYY
                try:
                    d = date.fromisoformat(date_str)
                except ValueError:
                    d = datetime.strptime(date_str, "%m-%d-%Y").date()
            elif isinstance(date_str, (int, float)):
                d = datetime.utcfromtimestamp(float(date_str)).date()
            else:
                continue
            p = float(price_val)
            results.append((d, p))
        except (ValueError, TypeError, OverflowError):
            continue

    return results


def load_prices_into_db(
    supabase: Client,
    company_id: int,
    prices: list[tuple[date, float]],
) -> int:
    """Upsert price data into metric_data. Only loads dates >= 2023-01-01."""
    rows = [
        {
            "company_id": company_id,
            "metric_code": "close_price",
            "source_code": "gurufocus",
            "target_date": d.isoformat(),
            "numeric_value": p,
        }
        for d, p in prices
        if d >= _PRICE_CUTOFF
    ]
    if not rows:
        return 0

    batch_size = 500
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        resp = supabase.table("metric_data").upsert(
            batch, on_conflict="company_id,metric_code,source_code,target_date", ignore_duplicates=False
        ).execute()
        total += len(resp.data)
    return total


def load_volume_into_db(
    supabase: Client,
    company_id: int,
    volumes: list[tuple[date, float]],
) -> int:
    """Upsert volume data into metric_data. Only loads dates >= 2015-01-01."""
    rows = [
        {
            "company_id": company_id,
            "metric_code": "volume",
            "source_code": "gurufocus",
            "target_date": d.isoformat(),
            "numeric_value": v,
        }
        for d, v in volumes
        if d >= _PRICE_CUTOFF
    ]
    if not rows:
        return 0

    batch_size = 500
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        resp = supabase.table("metric_data").upsert(
            batch, on_conflict="company_id,metric_code,source_code,target_date", ignore_duplicates=False
        ).execute()
        total += len(resp.data)
    return total


def ensure_volume_for_company(
    supabase: Client,
    company_id: int,
    ticker: str,
    exchange: str,
    *,
    data_cutoff: date | None = None,
) -> PriceResult:
    """Fetch volume data from GuruFocus, cache, and load into DB."""
    result = PriceResult()
    _ensure_bucket(supabase)
    path = _storage_path(ticker, exchange, "volume")
    symbol = _build_symbol(ticker, exchange)

    # 1. Check cache
    cached = _fetch_from_storage(supabase, path)
    if cached is not None:
        parsed = _parse_price_series(cached)  # same [date, value] format
        if parsed:
            dates = sorted(d for d, _ in parsed)
            fresh, reason = is_cache_fresh(dates, today=data_cutoff)
            if fresh:
                result.source = "cache"
                result.total_prices = len(parsed)
                result.rows_loaded = load_volume_into_db(supabase, company_id, parsed)
                return result

    # 2. Fetch from API
    data, api_log, http_status = _fetch_indicator_from_api(ticker, exchange, "volume")
    track_api_call(supabase, exchange)
    result.api_calls += 1
    result.logs.append(api_log)

    if data is None:
        # Fall back to stale cache
        if cached is not None:
            parsed = _parse_price_series(cached)
            if parsed:
                result.source = "stale_cache"
                result.total_prices = len(parsed)
                result.rows_loaded = load_volume_into_db(supabase, company_id, parsed)
                return result
        result.source = "none"
        result.error = f"no volume data for {symbol}"
        return result

    # 3. Cache and load
    _upload_to_storage(supabase, path, data)
    parsed = _parse_price_series(data)
    if parsed:
        result.source = "api"
        result.total_prices = len(parsed)
        result.rows_loaded = load_volume_into_db(supabase, company_id, parsed)
    else:
        result.source = "none"
        result.error = f"parsed 0 volume entries for {symbol}"
    return result


def _mask_url(url: str) -> str:
    api_key = os.environ.get("GURUFOCUS_API_KEY", "")
    if api_key:
        return url.replace(api_key, api_key[:4] + "***")
    return url


def ensure_prices_for_company(
    supabase: Client,
    company_id: int,
    ticker: str,
    exchange: str,
    *,
    force_refresh: bool = False,
    data_cutoff: date | None = None,
    on_log: callable = None,
) -> PriceResult:
    """Full pipeline: fetch -> cache -> load. Returns detailed result.

    Args:
        data_cutoff: If set, judge cache freshness against this date instead of
                     today. When the cache covers up to this date, skip the API
                     fetch entirely. Data newer than this date is ignored.
    """
    def _log(msg: str):
        result.logs.append(msg)
        if on_log:
            on_log(msg)

    result = PriceResult()
    _ensure_bucket(supabase)
    path = _storage_path(ticker, exchange)
    symbol = _build_symbol(ticker, exchange)

    # 1. Check cache
    if not force_refresh:
        cached = _fetch_from_storage(supabase, path)
        if cached is not None:
            parsed = _parse_price_series(cached)
            if parsed:
                dates = sorted(d for d, _ in parsed)
                fresh, reason = is_cache_fresh(dates, today=data_cutoff)
                if fresh:
                    result.source = "cache"
                    result.total_prices = len(parsed)
                    _log(f"cache fresh ({reason})")
                    result.rows_loaded = load_prices_into_db(supabase, company_id, parsed)
                    return result
                else:
                    _log(f"cache stale ({reason}), refreshing from API")
            else:
                _log(f"cache exists but parsed 0 prices from it")
        else:
            _log(f"no cache at {path}")

    # 2. Fetch from API
    base_url = os.environ.get("GURUFOCUS_BASE_URL", "").strip().rstrip("/")
    if base_url.endswith("/data"):
        base_url = base_url[: -len("/data")]
    api_key = os.environ.get("GURUFOCUS_API_KEY", "")
    raw_url = f"{base_url}/public/user/{api_key}/stock/{quote(symbol, safe=':')}/price"
    _log(f"Calling {_mask_url(raw_url)} ...")
    data, api_log, http_status = _fetch_price_from_api(ticker, exchange)
    track_api_call(supabase, exchange)
    result.api_calls += 1
    _log(api_log)

    # Detect unsubscribed region (check body, not just status code —
    # a bare 403 can mean a specific ticker is restricted/delisted)
    if data is None and api_log and "unsubscribed region" in api_log.lower():
        result.source = "forbidden"
        result.is_forbidden = True
        result.error = f"403 unsubscribed region for {symbol}"
        _log(f"Forbidden — exchange {exchange} not in subscription")
        return result

    # Detect delisted stocks (403 with "Delisted stocks" in body)
    if data is None and api_log and "delisted" in api_log.lower():
        result.source = "delisted"
        result.is_delisted = True
        result.error = f"delisted: {symbol}"
        _log(f"Delisted — {symbol}")
        return result

    if data is None:
        # Fall back to stale cache
        cached = _fetch_from_storage(supabase, path)
        if cached is not None:
            parsed = _parse_price_series(cached)
            result.source = "stale_cache"
            result.total_prices = len(parsed)
            _log(f"using stale cache ({len(parsed)} prices)")
            result.rows_loaded = load_prices_into_db(supabase, company_id, parsed)
        else:
            result.source = "none"
            result.error = "no prices available (API failed, no cache)"
            _log("no prices available at all")
        return result

    # 3. Parse API response
    parsed = _parse_price_series(data)
    if not parsed:
        # Log what the API actually returned for debugging
        data_preview = str(data)[:200]
        _log(f"API returned data but parsed 0 prices. Data preview: {data_preview}")
        result.source = "none"
        result.error = "API returned unparseable data"
        return result

    _log(f"parsed {len(parsed)} prices from API (range {min(d for d,_ in parsed)} to {max(d for d,_ in parsed)})")

    # 4. Cache to storage
    _upload_to_storage(supabase, path, data)
    _log("cached to storage")

    # 5. Load into DB
    result.source = "api"
    result.total_prices = len(parsed)
    result.rows_loaded = load_prices_into_db(supabase, company_id, parsed)
    _log(f"loaded {result.rows_loaded} rows into DB (>= {_PRICE_CUTOFF})")

    return result
