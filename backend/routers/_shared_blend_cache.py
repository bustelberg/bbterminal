"""Optional Redis tier for the public benchmark-blend response cache.

The in-process cache is the fastest tier, but a serverless invocation or a second
replica has empty RAM.  When ``BLEND_CACHE_REDIS_URL`` is configured this module
stores the already-gzipped response bytes in Redis and namespaces every entry by
a Redis-backed generation counter.  A completed fundamentals ingest increments
that counter, so every replica misses its old generation immediately without a
slow or error-prone key scan.

Redis is deliberately optional: local development and single-process deploys
retain the existing in-memory cache with no network dependency.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from typing import Any

try:
    from redis import Redis
except ImportError:  # pragma: no cover - only useful before dependencies install
    Redis = None  # type: ignore[assignment,misc]

_log = logging.getLogger(__name__)
_URL_ENV = "BLEND_CACHE_REDIS_URL"
_PREFIX = "bb:blend-cache:v1"
_TTL_SECONDS = 6 * 60 * 60
_RETRY_AFTER_SECONDS = 30.0


class SharedBlendCache:
    """Best-effort Redis cache; a Redis outage is always a normal local-cache miss."""

    def __init__(self) -> None:
        self._client: Any | None = None
        self._disabled_until = 0.0

    def _redis(self) -> Any | None:
        if time.monotonic() < self._disabled_until:
            return None
        url = os.environ.get(_URL_ENV, "").strip()
        if not url or Redis is None:
            return None
        if self._client is None:
            try:
                self._client = Redis.from_url(
                    url, decode_responses=False, socket_connect_timeout=0.25,
                    socket_timeout=0.5, health_check_interval=30,
                )
            except Exception as exc:  # noqa: BLE001 - optional optimisation
                self._fail(exc)
        return self._client

    def _fail(self, exc: Exception) -> None:
        self._client = None
        self._disabled_until = time.monotonic() + _RETRY_AFTER_SECONDS
        _log.warning("[blend-cache] shared Redis unavailable; using local cache for %.0fs: %s",
                     _RETRY_AFTER_SECONDS, type(exc).__name__)

    def generation(self) -> str | None:
        client = self._redis()
        if client is None:
            return None
        try:
            raw = client.get(f"{_PREFIX}:generation")
            if raw is None:
                client.set(f"{_PREFIX}:generation", b"1", nx=True)
                raw = client.get(f"{_PREFIX}:generation")
            return raw.decode("ascii") if isinstance(raw, bytes) else str(raw)
        except Exception as exc:  # noqa: BLE001 - cache failure must not fail a chart
            self._fail(exc)
            return None

    def key(self, generation: str, request_key: tuple) -> str:
        digest = hashlib.sha256(
            json.dumps(request_key, separators=(",", ":"), ensure_ascii=True).encode("ascii")
        ).hexdigest()
        return f"{_PREFIX}:{generation}:{digest}"

    def get(self, key: str) -> bytes | None:
        client = self._redis()
        if client is None:
            return None
        try:
            value = client.get(key)
            return bytes(value) if value is not None else None
        except Exception as exc:  # noqa: BLE001
            self._fail(exc)
            return None

    def put(self, key: str, value: bytes) -> None:
        client = self._redis()
        if client is None:
            return
        try:
            client.set(key, value, ex=_TTL_SECONDS)
        except Exception as exc:  # noqa: BLE001
            self._fail(exc)

    def invalidate(self) -> None:
        """Advance the global generation after the underlying data has committed."""
        client = self._redis()
        if client is None:
            return
        try:
            client.incr(f"{_PREFIX}:generation")
        except Exception as exc:  # noqa: BLE001
            self._fail(exc)


shared_blend_cache = SharedBlendCache()
