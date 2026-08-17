"""GuruFocus HTTP client for the earnings ingest path.

The actual Cloudflare-bypass plumbing (curl_cffi + auto-fingerprint
ladder) lives in `ingest/_gurufocus_http.py` and is shared with the
prices ingest. This module is now thin: URL building, key masking,
JSON parsing, rate limit, urllib fallback.

The per-process rate limit protects us against bursting the API in
parallel-fetch scenarios — the worker pool in the backtest stream can
launch dozens of tasks concurrently, and a bare-bursting client trips
the GuruFocus daily call cap fast. It is 0.75s by default and settable
with `GURUFOCUS_MIN_INTERVAL_SECONDS`; see `_min_interval` and
`_API_MIN_INTERVAL_DEFAULT` for the measurement behind the number, and
`scripts/measure_gurufocus_rate.py` to re-run it."""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ingest._gurufocus_http import (
    cf_get,
    explain_failure,
    is_available as _cf_is_available,
)

log = logging.getLogger(__name__)

# The curl_cffi ladder / preferred target is logged once at boot by
# `_gurufocus_http` itself — no need to repeat it per client.

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


def _min_interval() -> float:
    """Seconds between GuruFocus requests, process-wide.

    ⚠⚠ THIS ONE NUMBER SETS THE WALL CLOCK OF EVERY BULK FILL, AND IT HAS NEVER BEEN MEASURED. The
    lock below makes it a hard global serializer, so a run costs `calls x interval` no matter how
    many workers there are: an ACWI smart press is ~4,619 calls, which is **1.92 hours at 1.5s** and
    would be 1.28h at 1.0s or 0.64h at 0.5s. Nothing else in the fill comes close — the database
    half, after the row-diffing, is well under it.

    ⚠ AND THE FIGURE THAT LOOKS LIKE EVIDENCE FOR 1.5 IS NOT. CLAUDE.md records "6 calls take 15.42s
    serially and 4.56s on six threads (3.4x, zero refusals)" — but that predates the `_RATE_LIMIT`
    lock, when the limiter leaked: every thread read the same `_last_api_call`, slept the same short
    time and fired together. It measured a BURST, not headroom. GuruFocus's real ceiling is unknown.

    ⚠ SO IT IS AN ENV VAR, NOT AN EDIT — because the way to find the ceiling is to measure it, and
    the measurement has to be repeatable and instantly revertible on a live deployment.
    `scripts/measure_gurufocus_rate.py` runs the experiment on a bounded sample and reports
    throughput, refusals and empty bodies. Lower it only on that evidence: the downside is not a
    slower run, it is throttling that returns EMPTY BODIES rather than 429s (the same failure mode
    Yahoo has, and the one that has already put a wrong listing in this database once), plus a
    monthly quota spent on calls that came back with nothing.

    ⚠ READ PER CALL, NOT CACHED AT IMPORT, so a Railway variable change takes effect on the next
    request rather than the next deploy — which matters when the thing you are tuning is running.
    """
    raw = os.environ.get("GURUFOCUS_MIN_INTERVAL_SECONDS")
    if not raw:
        return _API_MIN_INTERVAL_DEFAULT
    try:
        v = float(raw)
    except ValueError:
        log.warning("[gurufocus] GURUFOCUS_MIN_INTERVAL_SECONDS=%r is not a number; using %.2fs",
                    raw, _API_MIN_INTERVAL_DEFAULT)
        return _API_MIN_INTERVAL_DEFAULT
    # ⚠ A FLOOR, NOT A CLAMP TO TASTE. Zero would remove the limiter entirely and let three workers
    # burst — the exact shape that produced the `ReadTimeout`s on the production SP500 run. 0.2s
    # still leaves a real minimum interval while allowing a genuine experiment.
    if v < 0.2:
        log.warning("[gurufocus] GURUFOCUS_MIN_INTERVAL_SECONDS=%.3f is below the 0.2s floor; "
                    "using 0.2s", v)
        return 0.2
    return v


#: The default. ⚠⚠ 0.75s, AND IT IS MEASURED — see `scripts/measure_gurufocus_rate.py`, which
#: produced this. It was 1.5s on no evidence at all (the "3.4x on six threads" figure predates the
#: `_RATE_LIMIT` lock and measured a burst). Run 2026-08-17, 3 workers, 12 calls per interval:
#:
#:     interval   achieved      wall     refusals   empty bodies
#:       1.5s     0.67 calls/s  18.1s        0           0
#:       0.75s    1.34 calls/s  11.2s        0           0
#:       0.5s     1.81 calls/s   6.6s        0           0
#:       0.35s    2.35 calls/s   5.1s        0           0
#:
#: The vendor's OWN latency, un-gated and serial, was 0.77-1.12s median throughout — it did not
#: degrade as the interval fell, so at 1.5s the limiter was throttling us roughly 4x below what
#: three workers could sustain. On an ACWI press (4,316 calls) this takes 1.80h to 0.90h.
#:
#: ⚠ HALF, NOT A THIRD, DELIBERATELY. 0.5s and 0.35s measured just as clean, but every sample here
#: is a ~10-second BURST and a real press is thousands of calls over an hour — a sustained-rate or
#: daily policy cannot show up in a 12-call probe, and CLAUDE.md records one empty response in
#: twelve at 12 threads, so something does degrade under pressure somewhere. 0.75s takes the
#: well-evidenced half and leaves the rest of the headroom unspent.
#:
#: ⚠ BELOW ~0.4s THE GATE STOPS BEING THE CONSTRAINT ANYWAY: three workers at ~1.1s latency cap out
#: near 2.7 calls/s, so going faster than that needs more workers, not a smaller interval — and
#: `FILL_WORKERS` has its own ⚠⚠ about why it is three.
#:
#: To go back, or to try lower: `GURUFOCUS_MIN_INTERVAL_SECONDS=1.5` (read per call, so a Railway
#: variable takes effect without a deploy).
_API_MIN_INTERVAL_DEFAULT = 0.75
# Guards the read-modify-write of `_last_api_call` above — see `_api_request`.
_RATE_LIMIT = threading.Lock()


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

    # Any failure (pre-response error, Cloudflare block, real upstream
    # 4xx/5xx) goes through `explain_failure` so the message tells the
    # user the actual root cause rather than dumping HTML / debug noise.
    if resp.error is not None or (resp.status_code or 0) >= 400:
        return ApiResult(
            None,
            explain_failure(resp, masked_url),
            resp.status_code,
        )

    body = resp.text or ""
    if not body:
        return ApiResult(None, f"GuruFocus returned empty body ({masked_url})", resp.status_code)
    try:
        return ApiResult(json.loads(body), f"OK ({masked_url})", resp.status_code)
    except json.JSONDecodeError as e:
        return ApiResult(None, f"GuruFocus returned non-JSON content: {e} ({masked_url})")


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
    """One GuruFocus call, no faster than `_min_interval()` after the previous one.

    ⚠⚠ THE LOCK IS NOT DEFENSIVE — WITHOUT IT THIS LIMITER LEAKS UNDER CONCURRENCY, AND IT HAS
    HAD CONCURRENT CALLERS SINCE THE FUNDAMENTALS FILL WENT MULTI-THREADED. The body is a
    read-modify-write of a module global: every thread reads the SAME `_last_api_call`, each
    computes the same short sleep, they all wake together and fire at once. So N threads produce a
    BURST of N simultaneous requests and then a gap — the precise opposite of a minimum interval,
    and a plausible cause of the `ReadTimeout`s an SP500 refresh returned (GuruFocus answering an
    overloaded caller slowly, exactly as Yahoo answers one with an empty list).

    ⚠ THE SLEEP IS INSIDE THE LOCK, DELIBERATELY. Holding it across the wait is what SERIALISES
    callers; releasing before sleeping would let them all through together again and reduce this
    to the racy version with extra steps. The cost is that a caller waits for the queue ahead of
    it, which is what a global rate limit means.
    ⚠ THE HTTP CALL IS OUTSIDE IT. Holding the lock across a 30-second request would serialise the
    RESPONSES too, making the limiter a global mutex on GuruFocus and undoing every worker.
    """
    global _last_api_call
    interval = _min_interval()
    with _RATE_LIMIT:
        elapsed = time.time() - _last_api_call
        if elapsed < interval:
            time.sleep(interval - elapsed)
        # Stamped AFTER the wait, so the interval is measured between DEPARTURES.
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
