"""Cache-Control header strings for read-only HTTP endpoints.

Every endpoint in this codebase that GETs data falls into one of three
freshness tiers. Pick the tier when adding a new read endpoint:

  CACHE_STATIC    code-defined or near-immutable (signals, exchanges,
                  currency lists). 10 min fresh, 1 hour stale-while-revalidate.
  CACHE_PIPELINE  updated only by the weekly pipeline (template summaries,
                  latest-price-date). 1 min fresh, 5 min stale-while-revalidate.
  CACHE_USER      mutable through the UI (per-exchange fees, company
                  field-options). 30 sec fresh, 2 min stale-while-revalidate.

`stale-while-revalidate` lets the browser serve the stale value immediately
and refresh in the background after max-age expires, which keeps the UI
snappy even right after TTL flips over.

These are "public" because all read endpoints in this app are non-user-specific
(no per-user data on read paths; user identity is only consulted on writes by
`_auth_middleware.admin_only_mutations`). If you add a per-user read endpoint,
use `private` instead so shared caches/CDNs don't serve one user's data to
another.

Usage from a FastAPI handler:

    from fastapi import Response
    from routers._cache_headers import CACHE_PIPELINE

    @router.get("/api/foo")
    async def foo(response: Response):
        response.headers["Cache-Control"] = CACHE_PIPELINE
        return {...}
"""
from __future__ import annotations

CACHE_STATIC = "public, max-age=600, stale-while-revalidate=3600"
CACHE_PIPELINE = "public, max-age=60, stale-while-revalidate=300"
CACHE_USER = "public, max-age=30, stale-while-revalidate=120"
