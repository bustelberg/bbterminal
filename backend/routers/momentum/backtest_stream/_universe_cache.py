"""In-process LRU+TTL cache for the backtest universe-membership panel.

The backtest stream loads the same `{YYYY-MM: {company_id: grouping}}` panel for
the same universe on every saved run / variant sweep, so we cache it. Universes
are fixed frozen snapshots now (no template refresh to invalidate against), so a
plain TTL is enough; `_load_index_universe` additionally re-validates against the
universe row's `last_refreshed_at` before serving a hit.

(Lifted out of the removed `index_universe/templates/_cache.py` — only the
backtester's `full_universe_cache` survived the frozen-snapshots-only refactor.)
"""
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Generic, TypeVar

T = TypeVar("T")


class _LruTtlCache(Generic[T]):
    """Generic LRU+TTL cache. Keys are tuples; values are arbitrary objects."""

    def __init__(self, max_size: int = 8, ttl_seconds: float = 60.0):
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
                self._store.pop(key, None)
                return None
            self._store.move_to_end(key)
            return value

    def put(self, key: tuple, value: T) -> None:
        with self._lock:
            self._store[key] = (time.monotonic(), value)
            self._store.move_to_end(key)
            while len(self._store) > self._max_size:
                self._store.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def size(self) -> int:
        with self._lock:
            return len(self._store)


# Keyed by `(label, grouping_field)`. Values are `(last_refreshed_at, dict)`.
full_universe_cache: _LruTtlCache[tuple[str | None, dict[str, dict[int, str | None]]]] = _LruTtlCache(
    max_size=8, ttl_seconds=60.0,
)
