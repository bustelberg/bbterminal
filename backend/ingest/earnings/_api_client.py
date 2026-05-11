"""GuruFocus HTTP client — curl-first with urllib fallback.

Python's urllib TLS fingerprint gets blocked by Cloudflare on the
GuruFocus edge, so we shell out to `curl` when it's available and only
fall back to urllib when it isn't.

A 1.5s per-process rate limit protects us against bursting the API in
parallel-fetch scenarios — the worker pool in the backtest stream can
launch dozens of tasks concurrently, and a bare-bursting client trips
the GuruFocus daily call cap fast."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

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
