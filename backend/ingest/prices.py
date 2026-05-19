"""
Fetch daily closing prices from GuruFocus, cache raw JSON in Supabase Storage,
and load into metric_data (only dates >= 2023-01-01).
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

try:
    from curl_cffi import requests as cf_requests  # type: ignore[import-not-found]
    _HAS_CURL_CFFI = True
    _CURL_CFFI_IMPORT_ERROR: str | None = None
except ImportError as _e:
    _HAS_CURL_CFFI = False
    _CURL_CFFI_IMPORT_ERROR = f"{type(_e).__name__}: {_e}"

from supabase import Client

from ingest.staleness import is_cache_fresh, is_daily_data_fresh
from ingest.api_usage import track_api_call

# Mirrors the diagnostic in `earnings/_api_client.py`. The prices path
# and the earnings path each have their own copy of the curl_cffi client
# — both must succeed on Cloudflare-protected endpoints.
if _HAS_CURL_CFFI:
    logging.getLogger(__name__).warning(
        "gurufocus prices client: using curl_cffi (Chrome impersonation)"
    )
else:
    logging.getLogger(__name__).error(
        "gurufocus prices client: FALLBACK to urllib (curl_cffi import failed: %s) — "
        "Cloudflare will block production calls",
        _CURL_CFFI_IMPORT_ERROR,
    )

_BUCKET = "gurufocus-raw"
_PRICE_CUTOFF = date(1998, 1, 1)

US_EXCHANGES = {"NYSE", "NASDAQ", "AMEX", "CBOE"}

# Keep in sync with `earnings/_api_client.py::_IMPERSONATE`. Cloudflare's
# bot allowlist for "real Chrome" includes recent versions only; older
# JA3/JA4 fingerprints aged out roughly every few months (chrome120 → Apr,
# chrome131 → May). Bumping to the newest Chrome target curl_cffi ships
# is the standard workaround.
_IMPERSONATE = "chrome146"

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
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


_BUCKET_READY = False


_MAX_RETRIES = 3
_RETRY_DELAY = 2  # seconds; multiplied by attempt number


def _is_transient_error(e: BaseException) -> bool:
    """Heuristic: catch socket timeouts and HTTP 5xx / bad-gateway errors
    coming from Supabase Storage and metric_data calls."""
    name = type(e).__name__.lower()
    err = str(e).lower()
    if "timeout" in name or "timeout" in err or "timed out" in err:
        return True
    if "502" in err or "503" in err or "504" in err or "bad gateway" in err:
        return True
    if "connection" in err and ("reset" in err or "aborted" in err):
        return True
    return False


def _retry_transient(fn, *, description: str, max_retries: int = _MAX_RETRIES):
    """Run fn(), retrying on transient errors (timeouts, 5xx). Other
    exceptions propagate immediately. Returns fn()'s value."""
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except Exception as e:
            if _is_transient_error(e) and attempt < max_retries:
                time.sleep(_RETRY_DELAY * attempt)
                continue
            raise


def _ensure_bucket(supabase: Client) -> None:
    """Idempotent bucket creation. Guarded so it fires at most once per process —
    previous version fired an HTTP call on every price/volume fetch."""
    global _BUCKET_READY
    if _BUCKET_READY:
        return
    try:
        supabase.storage.create_bucket(_BUCKET, options={"public": False})
    except Exception:
        pass
    _BUCKET_READY = True


def _fetch_from_storage(supabase: Client, path: str) -> list | None:
    try:
        raw = _retry_transient(
            lambda: supabase.storage.from_(_BUCKET).download(path),
            description=f"storage.download({path})",
        )
        return json.loads(raw)
    except Exception:
        return None


def _upload_to_storage(supabase: Client, path: str, data: list) -> None:
    content = json.dumps(data, ensure_ascii=False).encode("utf-8")
    try:
        _retry_transient(
            lambda: supabase.storage.from_(_BUCKET).upload(
                path, content, file_options={"content-type": "application/json"}
            ),
            description=f"storage.upload({path})",
        )
    except Exception as e:
        msg = str(e).lower()
        if "already exists" not in msg and "duplicate" not in msg and "409" not in msg:
            raise
        try:
            _retry_transient(
                lambda: supabase.storage.from_(_BUCKET).update(
                    path, content, file_options={"content-type": "application/json"}
                ),
                description=f"storage.update({path})",
            )
        except Exception as upd_e:
            # Both create and update of the cache JSON failed. Storage
            # is now stale, but the DB-side freshness check in
            # ensure_*_for_company will trigger a fresh API fetch on
            # the next run, so this is recoverable. Log so it's visible.
            logging.getLogger(__name__).warning(
                "[storage] update fallback failed for %s: %s: %s",
                path, type(upd_e).__name__, upd_e,
            )


def _fetch_indicator_from_api(ticker: str, exchange: str, indicator: str = "price", timeout: int = 30) -> tuple[list | None, str, int | None]:
    """Fetch an indicator from GuruFocus API. Returns (raw_data, log_message, http_status).

    Uses curl_cffi with Chrome120 impersonation to bypass GuruFocus's
    Cloudflare TLS-fingerprint check (urllib's fingerprint gets
    blocked). Falls back to urllib only when curl_cffi can't import."""
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

    if _HAS_CURL_CFFI:
        try:
            resp = cf_requests.get(
                url,
                headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
                timeout=timeout,
                impersonate=_IMPERSONATE,
            )
            status = resp.status_code
            body = resp.text or ""
            if status >= 400:
                return None, f"API HTTP {status} via curl_cffi/{_IMPERSONATE} for {symbol} body={body[:200]} ({masked_url})", status
            if not body:
                return None, f"API returned empty response for {symbol} ({masked_url})", status
            return json.loads(body), f"API OK for {symbol} ({masked_url})", status
        except json.JSONDecodeError as e:
            return None, f"API returned invalid JSON for {symbol}: {e} ({masked_url})", None
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
            return None, f"API HTTP {e.code} via urllib (curl_cffi unavailable) for {symbol}: {e.reason} body={body} ({masked_url})", e.code
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


def _db_max_date(supabase: Client, company_id: int, metric_code: str) -> date | None:
    """Return the latest target_date already stored for this company/metric, or None."""
    try:
        resp = (
            supabase.table("metric_data")
            .select("target_date")
            .eq("company_id", company_id)
            .eq("metric_code", metric_code)
            .eq("source_code", "gurufocus")
            .order("target_date", desc=True)
            .limit(1)
            .execute()
        )
        if resp.data and resp.data[0].get("target_date"):
            return date.fromisoformat(resp.data[0]["target_date"])
    except Exception as e:
        # Returning None makes the caller treat this as "nothing in DB" and
        # fall through to the Storage / API path — the right behavior on a
        # transient blip. But log so the right behavior isn't masking a
        # persistent Supabase outage.
        logging.getLogger(__name__).warning(
            "[_db_max_date] query failed for cid=%s metric=%s: %s: %s",
            company_id, metric_code, type(e).__name__, e,
        )
    return None


def _upsert_metric_rows(
    supabase: Client,
    company_id: int,
    metric_code: str,
    pairs: list[tuple[date, float]],
) -> int:
    """Upsert only rows newer than what's already in the DB. Skips the whole
    write when the DB already has everything the cache does — otherwise a
    fresh cache would still trigger ~15 redundant upsert round-trips."""
    existing_max = _db_max_date(supabase, company_id, metric_code)
    rows = [
        {
            "company_id": company_id,
            "metric_code": metric_code,
            "source_code": "gurufocus",
            "target_date": d.isoformat(),
            "numeric_value": v,
        }
        for d, v in pairs
        if d >= _PRICE_CUTOFF and (existing_max is None or d > existing_max)
    ]
    if not rows:
        return 0

    batch_size = 500
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        resp = _retry_transient(
            lambda b=batch: supabase.table("metric_data").upsert(
                b, on_conflict="company_id,metric_code,source_code,target_date", ignore_duplicates=False
            ).execute(),
            description=f"metric_data.upsert(company={company_id}, {metric_code}, {len(batch)} rows)",
        )
        total += len(resp.data)
    return total


def load_prices_into_db(
    supabase: Client,
    company_id: int,
    prices: list[tuple[date, float]],
) -> int:
    return _upsert_metric_rows(supabase, company_id, "close_price", prices)


def load_volume_into_db(
    supabase: Client,
    company_id: int,
    volumes: list[tuple[date, float]],
) -> int:
    return _upsert_metric_rows(supabase, company_id, "volume", volumes)


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

    # Fast path: if the DB's latest row is already fresh we skip Storage
    # entirely. Without this, cache hits still pay for a multi-hundred-KB JSON
    # download + full reparse per company before discovering nothing to write.
    db_max = _db_max_date(supabase, company_id, "volume")
    if db_max is not None:
        fresh, _reason = is_daily_data_fresh(db_max, today=data_cutoff)
        if fresh:
            result.source = "cache"
            return result

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

    # Fast path: if the DB's latest row is already fresh we skip Storage
    # entirely. Without this, cache hits still pay for a multi-hundred-KB JSON
    # download + full reparse per company before discovering nothing to write.
    if not force_refresh:
        db_max = _db_max_date(supabase, company_id, "close_price")
        if db_max is not None:
            fresh, reason = is_daily_data_fresh(db_max, today=data_cutoff)
            if fresh:
                result.source = "cache"
                _log(f"DB fresh ({reason}) — skipping Storage")
                return result

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
                _log("cache exists but parsed 0 prices from it")
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
