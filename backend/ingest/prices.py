"""
Fetch daily closing prices from GuruFocus, cache raw JSON in Supabase Storage,
and load into metric_data (only dates >= DATA_CUTOFF, see ingest/constants.py).
"""
from __future__ import annotations

import gzip
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from supabase import Client

from common.retry import retry
from ingest._gurufocus_http import (
    cf_get,
    current_preferred_target,
    explain_failure,
    is_available as _cf_is_available,
    ladder as _cf_ladder,
)
from ingest.constants import DATA_CUTOFF
from ingest.staleness import is_cache_fresh, is_daily_data_fresh
from ingest.api_usage import track_api_call
from .gurufocus_url import US_EXCHANGE_CODES as US_EXCHANGES  # single source of truth

# Boot-time diagnostic. Same line both clients log — grepping the
# Railway logs for `gurufocus` immediately shows which fingerprint
# ladder is in play.
if _cf_is_available():
    logging.getLogger(__name__).warning(
        "gurufocus prices client: curl_cffi ladder %s (preferred=%s)",
        _cf_ladder(), current_preferred_target(),
    )
else:
    logging.getLogger(__name__).error(
        "gurufocus prices client: FALLBACK to urllib (curl_cffi unavailable) — "
        "Cloudflare will block production calls"
    )

_BUCKET = "gurufocus-raw"

# When GuruFocus returns 404 "Stock not found" for a (ticker, exchange)
# pair, try these alternative exchanges before giving up. iShares ACWI
# often lists German cross-listings on Xetra (XTER) but GuruFocus only
# covers Stuttgart (STU) for the same ticker — those need a fallback to
# get any data at all. Listed in priority order (first match wins). If
# all fallbacks 404 too, the company stays in "no data" state without
# being marked delisted — we genuinely don't know whether it's gone or
# just on yet another German exchange we haven't tried.
FALLBACK_EXCHANGES: dict[str, list[str]] = {
    "XTER": ["STU"],
}

# Modern Chrome UA. Defense-in-depth — the real fingerprint signal
# is the TLS handshake (handled by curl_cffi via `_gurufocus_http`).
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
    # When the primary exchange 404s and a fallback succeeds, this is
    # the exchange the data was actually fetched from. The caller uses
    # it to (a) update `company.exchange_id` so future runs go straight
    # to the working exchange, and (b) pass the same exchange to the
    # subsequent volume fetch so it doesn't re-try the dead primary.
    resolved_exchange: str | None = None
    api_calls: int = 0  # Number of GuruFocus API requests made


def normalize_gurufocus_ticker(ticker: str, exchange: str) -> str:
    """Apply per-exchange normalization required by GuruFocus before
    we send the ticker out (API URL or storage path).

    Currently the only rule is HKSE: numeric tickers must be 5 digits
    with leading zeros (`1288` → `01288`, `241` → `00241`). The
    upstream LongEquity / OpenFIGI sources sometimes hand us the
    stripped form, and old `company.gurufocus_ticker` rows from before
    the ingest-side `canonical_ticker` fix can still hold the short
    form — applying the same normalization here makes the read side
    resilient regardless. Idempotent: a ticker already in canonical
    form passes through unchanged.

    Mirrors `ingest.dedupe.canonical_ticker` for HKSE, but stays
    narrower (no Nordic share-class collapsing) — those transforms are
    for dupe detection, not for GuruFocus URL form."""
    t = (ticker or "").strip()
    exch = (exchange or "").strip().upper()
    if exch == "HKSE" and t.isdigit() and len(t) < 5:
        return t.zfill(5)
    return t


def _build_symbol(ticker: str, exchange: str) -> str:
    ticker = normalize_gurufocus_ticker(ticker, exchange)
    if exchange.upper() in US_EXCHANGES:
        return ticker
    return f"{exchange}:{ticker}"


def _storage_path(ticker: str, exchange: str, indicator: str = "price") -> str:
    ticker = normalize_gurufocus_ticker(ticker, exchange)
    return f"{exchange.upper()}_{ticker.upper()}/indicator__{indicator}.json"


_BUCKET_READY = False


_MAX_RETRIES = 3
_RETRY_DELAY = 2  # seconds; multiplied by attempt number (linear backoff)


def _retry_transient(fn, *, description: str, max_retries: int = _MAX_RETRIES):
    """Run fn(), retrying on transient errors (timeouts, 5xx) with linear
    backoff. Other exceptions propagate immediately. Returns fn()'s value.
    Thin binding over `common.retry.retry`."""
    return retry(
        fn,
        attempts=max_retries,
        base_delay=_RETRY_DELAY,
        backoff="linear",
        description=description,
    )


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


# Magic bytes for the gzip file format (RFC 1952). Used to detect whether a
# downloaded blob is gzipped (new format) or raw JSON (legacy format), so
# this layer stays backward compatible with objects written before the gzip
# rollout. Storage backfill is implicit: every refresh that overwrites an
# object now writes the gzipped form.
_GZIP_MAGIC = b"\x1f\x8b"


def _decode_storage_payload(raw: bytes) -> list:
    """Parse a downloaded blob as JSON, transparently decompressing if it
    starts with gzip magic bytes. Returns the parsed JSON payload (which for
    GuruFocus price/volume responses is always a list-of-rows)."""
    if raw.startswith(_GZIP_MAGIC):
        raw = gzip.decompress(raw)
    return json.loads(raw)


def _fetch_from_storage(supabase: Client, path: str) -> list | None:
    try:
        raw = _retry_transient(
            lambda: supabase.storage.from_(_BUCKET).download(path),
            description=f"storage.download({path})",
        )
        return _decode_storage_payload(raw)
    except Exception:
        return None


def _upload_to_storage(supabase: Client, path: str, data: list) -> None:
    # Gzip the JSON before upload. Typical GuruFocus price/volume responses
    # for a single ticker are 50-300 KB of JSON; gzip cuts that by 8-12x
    # because most of the payload is tiny floats and repeated date strings.
    # `content-encoding: gzip` is the HTTP-standard signal -- anything that
    # downloads via a signed URL with an HTTP-aware client auto-decompresses;
    # the supabase-py `.download()` path returns raw bytes regardless, so we
    # rely on `_decode_storage_payload`'s magic-byte sniff on read.
    json_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
    content = gzip.compress(json_bytes, compresslevel=6)
    file_options = {
        "content-type": "application/json",
        "content-encoding": "gzip",
    }
    try:
        _retry_transient(
            lambda: supabase.storage.from_(_BUCKET).upload(
                path, content, file_options=file_options,
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
                    path, content, file_options=file_options,
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

    if _cf_is_available():
        resp = cf_get(
            url,
            headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
            timeout=timeout,
        )
        # Route every failure through `explain_failure` so the message
        # tells the user the actual root cause (Cloudflare IP block,
        # circuit breaker open, network error, real upstream 4xx, …)
        # rather than dumping HTML / debug noise.
        if resp.error is not None or (resp.status_code or 0) >= 400:
            return None, explain_failure(resp, masked_url, subject=symbol), resp.status_code
        status = resp.status_code or 0
        body = resp.text or ""
        if not body:
            return None, f"GuruFocus returned empty body for {symbol} ({masked_url})", status
        try:
            return json.loads(body), f"OK for {symbol} ({masked_url})", status
        except json.JSONDecodeError as e:
            return None, f"GuruFocus returned non-JSON content for {symbol}: {e} ({masked_url})", None
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


def _try_with_fallbacks(
    ticker: str,
    exchange: str,
    indicator: str,
    *,
    on_log=None,
) -> tuple[list | None, str, int | None, str]:
    """Fetch (ticker, exchange) on GuruFocus, falling through to entries
    in `FALLBACK_EXCHANGES[exchange]` when the primary returns 404
    "Stock not found".

    Returns (data, log, http_status, used_exchange) — `used_exchange` is
    the exchange that actually produced data (or the original exchange
    if every attempt failed). Anything other than 404 (200, 403, network
    error, …) short-circuits the fallback chain — it's only the
    "wrong-exchange" symptom we're trying to recover from."""
    def _log(msg: str):
        if on_log:
            on_log(msg)
    data, api_log, status = _fetch_indicator_from_api(ticker, exchange, indicator)
    if data is not None:
        return data, api_log, status, exchange
    not_found = bool(api_log and "stock not found" in api_log.lower())
    if not not_found:
        # 403 / network error / etc — fallbacks won't help, return as-is.
        return data, api_log, status, exchange

    # Remember a fallback that signals "delisted" so we can propagate it
    # instead of the primary's bare 404. The primary returned "Stock
    # not found" (unknown listing on that exchange), but if the same
    # ticker on a fallback exchange explicitly reports as delisted,
    # that's the more informative classification — the caller's
    # `is_delisted` detection reads `api_log`, so propagating the
    # delisted body is what flips the flag.
    delisted_fallback: tuple[list | None, str, int | None, str] | None = None

    for alt in FALLBACK_EXCHANGES.get(exchange, []):
        _log(f"{exchange}:{ticker} 404 — trying fallback {alt}:{ticker}")
        d2, l2, s2 = _fetch_indicator_from_api(ticker, alt, indicator)
        if d2 is not None:
            _log(f"  fallback {alt}:{ticker} succeeded (status {s2})")
            # Combine logs so the caller sees both attempts in the
            # PriceResult log trail.
            return d2, f"{api_log} || fallback OK on {alt}: {l2}", s2, alt
        if l2 and "delisted" in l2.lower() and delisted_fallback is None:
            _log(f"  fallback {alt}:{ticker} returned delisted signal")
            # Stash but keep iterating — a later fallback might return
            # data, which we'd prefer. If none does, this delisted
            # response wins over the primary's 404.
            delisted_fallback = (
                None,
                f"{api_log} || fallback delisted on {alt}: {l2}",
                s2,
                exchange,  # keep primary exchange — there's no data to repoint to
            )

    if delisted_fallback is not None:
        return delisted_fallback

    # All fallbacks exhausted with no useful signal — return the
    # original 404 so the caller records the actual failure mode (not
    # the last fallback's).
    return data, api_log, status, exchange


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
        if d >= DATA_CUTOFF and (existing_max is None or d > existing_max)
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

    # 2. Fetch from API (with fallback exchange resolution on 404).
    data, api_log, http_status, used_exchange = _try_with_fallbacks(
        ticker, exchange, "volume", on_log=lambda m: result.logs.append(m),
    )
    track_api_call(supabase, used_exchange)
    result.api_calls += 1
    result.logs.append(api_log)
    if used_exchange != exchange:
        result.resolved_exchange = used_exchange
        # Repoint the volume Storage cache at the resolved exchange too
        # so subsequent runs hit the right path.
        path = _storage_path(ticker, used_exchange, "volume")

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

    # 2. Fetch from API (with fallback exchange resolution on 404).
    base_url = os.environ.get("GURUFOCUS_BASE_URL", "").strip().rstrip("/")
    if base_url.endswith("/data"):
        base_url = base_url[: -len("/data")]
    api_key = os.environ.get("GURUFOCUS_API_KEY", "")
    raw_url = f"{base_url}/public/user/{api_key}/stock/{quote(symbol, safe=':')}/price"
    _log(f"Calling {_mask_url(raw_url)} ...")
    data, api_log, http_status, used_exchange = _try_with_fallbacks(
        ticker, exchange, "price", on_log=_log,
    )
    track_api_call(supabase, used_exchange)
    result.api_calls += 1
    _log(api_log)
    if used_exchange != exchange:
        result.resolved_exchange = used_exchange
        # Repoint storage cache to the resolved exchange so subsequent
        # runs hit the right path immediately.
        path = _storage_path(ticker, used_exchange)
        symbol = _build_symbol(ticker, used_exchange)

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
        # Persist the marker so every caller picks it up (the prices
        # phase has its own write too — that path stays idempotent
        # because we filter on `delisted_at IS NULL`). Best-effort —
        # a transient DB blip just means a later run will re-detect
        # and re-mark.
        try:
            import datetime as _dt  # noqa: PLC0415
            supabase.table("company").update(
                {"delisted_at": _dt.datetime.now(_dt.timezone.utc).isoformat()}
            ).eq("company_id", company_id).is_("delisted_at", "null").execute()
        except Exception as e:
            logging.getLogger(__name__).warning(
                "[ensure_prices] failed to mark cid=%s delisted: %s: %s",
                company_id, type(e).__name__, e,
            )
        return result

    # NOTE: 404 "Stock not found" is NOT treated as delisted here. iShares
    # ACWI often lists German cross-listings on XTER but GuruFocus only has
    # the same ticker on STU (Stuttgart) — a 404 might just mean wrong
    # exchange. The caller is expected to retry via `FALLBACK_EXCHANGES`
    # before declaring the listing dead. See `_try_with_fallbacks` below.

    # Stamp lookup-failed when every fallback exhausted with "Stock not
    # found". Surfaced in the /companies UI so the user can spot rows
    # whose exchange is wrong before the next backtest complains.
    if data is None and api_log and "stock not found" in api_log.lower():
        try:
            import datetime as _dt  # noqa: PLC0415
            supabase.table("company").update(
                {"gurufocus_lookup_failed_at": _dt.datetime.now(_dt.timezone.utc).isoformat()}
            ).eq("company_id", company_id).execute()
        except Exception as e:
            logging.getLogger(__name__).warning(
                "[ensure_prices] failed to stamp lookup_failed cid=%s: %s: %s",
                company_id, type(e).__name__, e,
            )

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
    _log(f"loaded {result.rows_loaded} rows into DB (>= {DATA_CUTOFF})")

    # Clear lookup-failed stamp once a fetch succeeds (only if it was set,
    # to avoid the write entirely for the >99% of healthy rows).
    try:
        supabase.table("company").update(
            {"gurufocus_lookup_failed_at": None}
        ).eq("company_id", company_id).not_.is_("gurufocus_lookup_failed_at", "null").execute()
    except Exception as e:
        logging.getLogger(__name__).warning(
            "[ensure_prices] failed to clear lookup_failed cid=%s: %s: %s",
            company_id, type(e).__name__, e,
        )

    return result
