"""Shared retry-with-backoff primitive + the default transient-error heuristic.

One backoff loop, four bindings. Before this module each of these rolled its
own attempt loop / sleep / error sniff:

  * `ingest.prices._retry_transient`        — Supabase Storage + metric upsert
  * `momentum.data._helpers._query_with_retry` — Supabase metric reads
  * `fx_rates._ecb_get`                      — flaky ECB FX API
  * `leonteq.api_client._post_with_retry`    — Leonteq website API

They now keep their public signatures (so call sites are untouched) but
delegate the loop here, binding their own attempts / base delay / backoff
shape / `should_retry` predicate. The Supabase callers share the default
`is_transient_error` sniff; the HTTP clients pass status-code-precise
predicates.

NOT in scope: `ingest._gurufocus_http.cf_get` rotates Cloudflare impersonation
profiles instead of sleeping between identical retries — a different control
flow that this `time.sleep`-based loop doesn't model.
"""
from __future__ import annotations

import logging
import time
from typing import Callable, TypeVar

_log = logging.getLogger(__name__)

T = TypeVar("T")


def is_transient_error(e: BaseException) -> bool:
    """Default `should_retry` predicate: socket timeouts, HTTP 5xx /
    bad-gateway, and dropped connections — sniffed from the exception type +
    message. Covers the Supabase Storage / PostgREST (Cloudflare-fronted)
    failure modes. HTTP clients that need status-code-precise rules pass their
    own predicate instead."""
    name = type(e).__name__.lower()
    err = str(e).lower()
    if "timeout" in name or "timeout" in err or "timed out" in err:
        return True
    if "502" in err or "503" in err or "504" in err or "bad gateway" in err:
        return True
    if "connection" in err and ("reset" in err or "aborted" in err):
        return True
    return False


def retry(
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    base_delay: float = 2.0,
    backoff: str = "linear",
    should_retry: Callable[[BaseException], bool] = is_transient_error,
    description: str = "operation",
) -> T:
    """Call ``fn``, retrying while ``should_retry(exc)`` holds, up to
    ``attempts`` total tries. The last failure — or any exception
    ``should_retry`` rejects — propagates unchanged.

    Sleep between tries depends on ``backoff``:
      * ``"linear"``      → ``base_delay * attempt``        (base, 2·base, …)
      * ``"exponential"`` → ``base_delay * 2 ** (attempt-1)`` (base, 2·base, 4·base, …)

    A retry is logged at WARNING with the chosen delay. No sleep happens after
    the final attempt.
    """
    if attempts < 1:
        raise ValueError(f"attempts must be >= 1, got {attempts}")
    if backoff not in ("linear", "exponential"):
        raise ValueError(f"backoff must be 'linear' or 'exponential', got {backoff!r}")

    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:
            if attempt >= attempts or not should_retry(e):
                raise
            delay = (
                base_delay * attempt
                if backoff == "linear"
                else base_delay * (2 ** (attempt - 1))
            )
            _log.warning(
                "%s: attempt %d/%d failed (%s: %s) — retrying in %gs",
                description, attempt, attempts, type(e).__name__, e, delay,
            )
            time.sleep(delay)

    raise AssertionError("unreachable")  # pragma: no cover — loop returns or raises
