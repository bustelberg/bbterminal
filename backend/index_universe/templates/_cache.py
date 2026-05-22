"""In-process LRU+TTL cache for universe-membership reads.

The membership endpoint and the backtest universe-loader both serve
repeat queries against the same data — every date-scrubber click, every
saved backtest, every external script polling. The data only changes
when `UniverseTemplate.refresh()` runs (weekly via the pipeline, plus
manual triggers). So we cache aggressively and invalidate explicitly.

Two layers:
  * `MembershipCache` — one entry per (template_key, target_month).
    Stores the materialized membership list. Used by
    `UniverseTemplate.membership_at()` and the HTTP endpoint.
  * `FullUniverseCache` — one entry per template_key. Stores the
    `{YYYY-MM: {company_id: sector}}` dict the momentum backtest stream
    loads. Hot on the backtest path.

Both caches:
  * Cap at `max_size` entries (LRU eviction).
  * TTL of `ttl_seconds` (default 60s) as a safety net for multi-
    process deployments where another replica's `refresh()` won't be
    able to call our `invalidate()` directly.
  * Thread-safe via a coarse lock — the worker pool in the backtest
    stream is the main concurrent caller; the lock contention is
    immaterial vs the cost of an SQL roundtrip.
  * Explicit `invalidate(template_key)` is called by
    `UniverseTemplate.refresh()` after a successful write so the
    refreshing process serves fresh data immediately.

`last_refreshed_at` from the universe row is included in the cache
value tuple so the HTTP layer can compute an ETag without a second
DB lookup.
"""
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Generic, TypeVar

T = TypeVar("T")


class _LruTtlCache(Generic[T]):
    """Generic LRU+TTL cache. Keys are tuples (so cache by
    `(template_key, target_month)` etc.); values are arbitrary objects."""

    def __init__(self, max_size: int = 64, ttl_seconds: float = 60.0):
        self._max_size = max_size
        self._ttl = ttl_seconds
        self._store: OrderedDict[tuple, tuple[float, T]] = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: tuple) -> T | None:
        now = time.monotonic()
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            cached_at, value = entry
            if now - cached_at > self._ttl:
                # Expired — purge and miss.
                self._store.pop(key, None)
                return None
            # Mark as recently used.
            self._store.move_to_end(key)
            return value

    def put(self, key: tuple, value: T) -> None:
        with self._lock:
            self._store[key] = (time.monotonic(), value)
            self._store.move_to_end(key)
            # Evict oldest until under cap.
            while len(self._store) > self._max_size:
                self._store.popitem(last=False)

    def invalidate_prefix(self, prefix: tuple) -> int:
        """Drop every key whose initial elements match `prefix`. Returns
        the number of entries removed. Used by `invalidate(template_key)`
        to clear all months for a template after a refresh."""
        n = len(prefix)
        with self._lock:
            to_drop = [k for k in self._store if len(k) >= n and k[:n] == prefix]
            for k in to_drop:
                self._store.pop(k, None)
            return len(to_drop)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def size(self) -> int:
        with self._lock:
            return len(self._store)


# Membership cache: keyed by `(template_key, target_month)`. Values are
# `(last_refreshed_at_iso: str | None, rows: list[dict])` so the HTTP
# ETag can be computed without a second DB hit.
membership_cache: _LruTtlCache[tuple[str | None, list[dict]]] = _LruTtlCache(
    max_size=128, ttl_seconds=60.0,
)

# Full-universe cache: keyed by `(template_key,)`. Values are the
# pre-indexed dict `{YYYY-MM: {company_id: sector}}` the backtest
# stream consumes directly.
full_universe_cache: _LruTtlCache[tuple[str | None, dict[str, dict[int, str | None]]]] = _LruTtlCache(
    max_size=8, ttl_seconds=60.0,
)


def invalidate_template(template_key: str) -> dict:
    """Called by `UniverseTemplate.refresh()` after a successful write.
    Drops every cached entry tied to this template so the refreshing
    process serves fresh data immediately (other processes catch up at
    next TTL expiry)."""
    n_mem = membership_cache.invalidate_prefix((template_key,))
    n_full = full_universe_cache.invalidate_prefix((template_key,))
    # Any refresh changes the list-summary payload (latest month, member
    # count). Drop the cached payload so the next list-endpoint hit
    # recomputes.
    list_summary_invalidate()
    return {"membership_cleared": n_mem, "full_universe_cleared": n_full}


# ----------------------------------------------------------------------------
# List-summary cache: a single global slot holding the `/api/universe-templates`
# response. The handler computes 4 Supabase queries per template, which adds up
# to a noticeable dropdown lag on the /backtest page. Data only changes when a
# template's refresh() runs (which calls `invalidate_template` -> wipes this).
# ----------------------------------------------------------------------------
_LIST_SUMMARY_TTL_SECONDS = 300.0  # 5 min safety net for missed invalidations
_list_summary_lock = threading.Lock()
_list_summary_entry: tuple[float, list[dict]] | None = None


def list_summary_get() -> list[dict] | None:
    """Return the cached list-summary payload, or None when missing/expired."""
    global _list_summary_entry
    with _list_summary_lock:
        if _list_summary_entry is None:
            return None
        ts, data = _list_summary_entry
        if time.time() - ts > _LIST_SUMMARY_TTL_SECONDS:
            _list_summary_entry = None
            return None
        return data


def list_summary_set(data: list[dict]) -> None:
    """Store the freshly-computed list-summary payload."""
    global _list_summary_entry
    with _list_summary_lock:
        _list_summary_entry = (time.time(), data)


def list_summary_invalidate() -> None:
    """Drop the cached payload. Called from `invalidate_template`."""
    global _list_summary_entry
    with _list_summary_lock:
        _list_summary_entry = None
