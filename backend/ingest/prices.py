"""
Fetch daily closing prices from GuruFocus, cache raw JSON in Supabase Storage,
and load into metric_data (only dates >= 2023-01-01).
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from supabase import Client

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


def _build_symbol(ticker: str, exchange: str) -> str:
    if exchange.upper() in US_EXCHANGES:
        return ticker
    return f"{exchange}:{ticker}"


def _storage_path(ticker: str, exchange: str) -> str:
    return f"{exchange.upper()}_{ticker.upper()}/indicator__price.json"


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


def _fetch_price_from_api(ticker: str, exchange: str, timeout: int = 30) -> tuple[list | None, str]:
    """Fetch price indicator from GuruFocus API. Returns (raw_data, log_message)."""
    base_url = os.environ.get("GURUFOCUS_BASE_URL", "")
    api_key = os.environ.get("GURUFOCUS_API_KEY", "")
    if not base_url:
        return None, "GURUFOCUS_BASE_URL env var not set"
    if not api_key:
        return None, "GURUFOCUS_API_KEY env var not set"

    base = base_url.strip().rstrip("/")
    if base.endswith("/data"):
        base = base[: -len("/data")]

    symbol = _build_symbol(ticker, exchange)
    url = f"{base}/public/user/{api_key}/stock/{quote(symbol, safe=':')}/price"

    # Log URL with masked API key
    masked_url = url.replace(api_key, api_key[:4] + "***")

    for attempt in range(3):
        req = Request(url, headers={"User-Agent": _USER_AGENT, "Accept": "application/json"})
        try:
            with urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                if not raw:
                    return None, f"API returned empty response for {symbol} ({masked_url})"
                data = json.loads(raw)
                return data, f"API OK for {symbol}"
        except HTTPError as e:
            if e.code == 403 and attempt < 2:
                time.sleep(3 * (attempt + 1))
                continue
            return None, f"API HTTP {e.code} for {symbol}: {e.reason} ({masked_url})"
        except URLError as e:
            return None, f"API URL error for {symbol}: {e.reason}"
        except Exception as e:
            return None, f"API error for {symbol}: {type(e).__name__}: {e}"
    return None, f"API failed after 3 attempts for {symbol} ({masked_url})"


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


def ensure_prices_for_company(
    supabase: Client,
    company_id: int,
    ticker: str,
    exchange: str,
    *,
    force_refresh: bool = False,
) -> PriceResult:
    """Full pipeline: fetch -> cache -> load. Returns detailed result."""
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
                latest = max(d for d, _ in parsed)
                age_days = (date.today() - latest).days
                if age_days <= 3:
                    result.source = "cache"
                    result.total_prices = len(parsed)
                    result.logs.append(f"cache hit ({len(parsed)} prices, latest {latest}, {age_days}d old)")
                    result.rows_loaded = load_prices_into_db(supabase, company_id, parsed)
                    return result
                else:
                    result.logs.append(f"cache stale ({age_days}d old, latest {latest}), refreshing from API")
            else:
                result.logs.append(f"cache exists but parsed 0 prices from it")
        else:
            result.logs.append(f"no cache at {path}")

    # 2. Fetch from API
    data, api_log = _fetch_price_from_api(ticker, exchange)
    result.logs.append(api_log)

    if data is None:
        # Fall back to stale cache
        cached = _fetch_from_storage(supabase, path)
        if cached is not None:
            parsed = _parse_price_series(cached)
            result.source = "stale_cache"
            result.total_prices = len(parsed)
            result.logs.append(f"using stale cache ({len(parsed)} prices)")
            result.rows_loaded = load_prices_into_db(supabase, company_id, parsed)
        else:
            result.source = "none"
            result.error = "no prices available (API failed, no cache)"
            result.logs.append("no prices available at all")
        return result

    # 3. Parse API response
    parsed = _parse_price_series(data)
    if not parsed:
        # Log what the API actually returned for debugging
        data_preview = str(data)[:200]
        result.logs.append(f"API returned data but parsed 0 prices. Data preview: {data_preview}")
        result.source = "none"
        result.error = "API returned unparseable data"
        return result

    result.logs.append(f"parsed {len(parsed)} prices from API (range {min(d for d,_ in parsed)} to {max(d for d,_ in parsed)})")

    # 4. Cache to storage
    _upload_to_storage(supabase, path, data)
    result.logs.append("cached to storage")

    # 5. Load into DB
    result.source = "api"
    result.total_prices = len(parsed)
    result.rows_loaded = load_prices_into_db(supabase, company_id, parsed)
    result.logs.append(f"loaded {result.rows_loaded} rows into DB (>= {_PRICE_CUTOFF})")

    return result
