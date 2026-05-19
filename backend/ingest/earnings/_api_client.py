"""GuruFocus HTTP client for the earnings ingest path.

The actual Cloudflare-bypass plumbing (curl_cffi + auto-fingerprint
ladder) lives in `ingest/_gurufocus_http.py` and is shared with the
prices ingest. This module is now thin: URL building, key masking,
JSON parsing, rate limit, urllib fallback.

The 1.5s per-process rate limit protects us against bursting the API in
parallel-fetch scenarios — the worker pool in the backtest stream can
launch dozens of tasks concurrently, and a bare-bursting client trips
the GuruFocus daily call cap fast."""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ingest._gurufocus_http import (
    cf_get,
    current_preferred_target,
    is_available as _cf_is_available,
    ladder as _cf_ladder,
)

log = logging.getLogger(__name__)

# Boot-time diagnostic — same line both clients write so a grep through
# the Railway logs immediately shows what's going to be tried.
if _cf_is_available():
    log.warning(
        "gurufocus client: curl_cffi ladder %s (preferred=%s)",
        _cf_ladder(), current_preferred_target(),
    )
else:
    log.error(
        "gurufocus client: FALLBACK to urllib (curl_cffi unavailable) — "
        "Cloudflare will block production calls"
    )

# Plain Chrome UA string. Modern enough to match a real browser; the
# important fingerprint signal is the TLS handshake (handled by
# curl_cffi), not this header. Kept reasonably current as a no-cost
# defense-in-depth.
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)

_last_api_call: float = 0.0
_API_MIN_INTERVAL = 1.5  # seconds between requests


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


def _mask(url: str) -> str:
    api_key = os.environ.get("GURUFOCUS_API_KEY", "")
    return url.replace(api_key, api_key[:4] + "***") if api_key else url


def _api_request_cf(url: str, timeout: int = 30) -> ApiResult:
    """Fetch via the shared Cloudflare-aware client (auto-fingerprint
    ladder). The shared client handles 403-with-HTML retries on its own;
    we just parse what comes back."""
    masked_url = _mask(url)
    resp = cf_get(
        url,
        headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
        timeout=timeout,
    )

    if resp.error is not None and resp.status_code is None:
        # Network / library error before we got a response at all.
        return ApiResult(None, f"curl_cffi error: {resp.error} ({masked_url})")

    status_code = resp.status_code or 0
    body = resp.text or ""
    if status_code >= 400 or resp.error is not None:
        attempted = ",".join(resp.attempted) if resp.attempted else "n/a"
        return ApiResult(
            None,
            f"API HTTP {status_code} via curl_cffi/{resp.used_target} "
            f"(tried={attempted}) body={body[:200]} ({masked_url})",
            status_code,
        )
    if not body:
        return ApiResult(None, f"API empty response ({masked_url})", status_code)
    try:
        return ApiResult(json.loads(body), f"API OK ({masked_url})", status_code)
    except json.JSONDecodeError as e:
        return ApiResult(None, f"API returned invalid JSON: {e} ({masked_url})")


def _api_request_urllib(url: str, timeout: int = 30) -> ApiResult:
    """Fetch via urllib (fallback if curl_cffi isn't installed). On
    Cloudflare-protected endpoints this gets 403'd — but it's correct
    for non-protected paths in dev environments where curl_cffi might
    be missing."""
    masked_url = _mask(url)
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
        return ApiResult(
            None,
            f"API HTTP {e.code} via urllib (curl_cffi unavailable): {e.reason} body={body} ({masked_url})",
            e.code,
        )
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

    if _cf_is_available():
        return _api_request_cf(url, timeout)
    return _api_request_urllib(url, timeout)


def _mask_url(url: str) -> str:
    return _mask(url)


def _build_api_url(path: str, query: dict[str, str] | None = None) -> str:
    base_url = os.environ.get("GURUFOCUS_BASE_URL", "").strip().rstrip("/")
    if base_url.endswith("/data"):
        base_url = base_url[: -len("/data")]
    api_key = os.environ.get("GURUFOCUS_API_KEY", "")
    url = f"{base_url}/public/user/{api_key}/{path}"
    if query:
        url = f"{url}?{urlencode(query)}"
    return url
