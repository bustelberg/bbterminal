"""API authentication + authorization gate.

EVERY `/api/*` request must carry a valid Supabase JWT, with one exception
tier (public health/cron endpoints). Authorization is role-based:

  * admin  → any endpoint.
  * user   → only the API behind the non-admin-visible pages
             (/companies, /earnings, /airs-portfolio + the /earnings usage
             badge), plus the two mutations those pages need (AIRS upload,
             earnings refresh).
  * anon   → nothing but the public tier → 401.

Tiers (matched as `path.startswith(prefix)`):
  _PUBLIC_PREFIXES     no auth at all — health/ping + the cron endpoints,
                       which verify their own `X-Cron-Secret`.
  _SELF_AUTH_PREFIXES  the endpoint verifies the caller's token itself
                       (login/self-service + admin user management); the gate
                       lets the request reach it untouched.
  _USER_READ_PREFIXES  GET/HEAD allowed for any authenticated user.
  _USER_WRITE_PREFIXES writes allowed for any authenticated user.
  everything else      admin only.

Non-`/api/` paths (FastAPI's `/docs`, `/openapi.json`, `/`) pass through.

Frontend requests must attach the session JWT — `frontend/lib/apiFetch.ts`
does this; all read hooks/components route through it. A request without a
token gets 401; an authenticated non-admin hitting an admin path gets 403.
"""
from __future__ import annotations

import logging
from typing import Awaitable, Callable

from fastapi import Request
from fastapi.responses import JSONResponse

_log = logging.getLogger(__name__)

_WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

# No authentication at all: uptime checks + the cron endpoints (which gate
# themselves on X-Cron-Secret).
_PUBLIC_PREFIXES: tuple[str, ...] = (
    "/api/health",
    "/api/hello",
    "/api/ingest/scheduled-refresh/cron",
    "/api/momentum/current-picks/cron",
)

# Endpoints that verify the caller's token themselves (login/self-service +
# admin user management). The gate requires nothing here — they 401/403
# internally.
_SELF_AUTH_PREFIXES: tuple[str, ...] = ("/api/auth/",)

# Reads any AUTHENTICATED user may make — the API behind the non-admin pages.
_USER_READ_PREFIXES: tuple[str, ...] = (
    "/api/companies",
    "/api/earnings",
    "/api/airs",
    "/api/usage",
)

# Writes any AUTHENTICATED user may make — the mutations those pages need.
# (Earnings refresh is handled separately by `_is_earnings_refresh`.)
_USER_WRITE_PREFIXES: tuple[str, ...] = ("/api/portfolios/parse",)


def _is_earnings_refresh(path: str) -> bool:
    """`/api/earnings/{cid}/refresh*` is user-allowed; other writes under
    /api/earnings (none today) stay admin-only."""
    if not path.startswith("/api/earnings/"):
        return False
    return "/refresh" in path[len("/api/earnings/"):]


def _starts_with_any(path: str, prefixes: tuple[str, ...]) -> bool:
    return any(path.startswith(p) for p in prefixes)


async def enforce_api_auth(
    request: Request,
    call_next: Callable[[Request], Awaitable],
):
    """Gate every `/api/*` request: valid token required, role-checked per
    the tiers above. Fails closed (a verification error denies)."""
    # CORS preflight carries no auth header by design.
    if request.method == "OPTIONS":
        return await call_next(request)

    path = request.url.path
    # Non-API routes (docs, openapi, root) are not gated.
    if not path.startswith("/api/"):
        return await call_next(request)
    if _starts_with_any(path, _PUBLIC_PREFIXES):
        return await call_next(request)
    if _starts_with_any(path, _SELF_AUTH_PREFIXES):
        return await call_next(request)

    # Lazy import — avoids a circular module-init chain (auth.py pulls the
    # Supabase client, which loads dotenv, etc.).
    from routers.auth import verify_token  # noqa: PLC0415

    try:
        info = verify_token(request.headers.get("authorization", ""))
    except Exception as e:
        _log.warning(
            "[auth] token verification raised %s: %s — denying %s %s",
            type(e).__name__, e, request.method, path,
        )
        return JSONResponse({"detail": "Authorization check failed"}, status_code=500)

    if info is None:
        return JSONResponse({"detail": "Authentication required"}, status_code=401)

    # Admins can do anything.
    if info.get("role") == "admin":
        return await call_next(request)

    # Non-admin: only the allowed surface.
    if request.method in _WRITE_METHODS:
        allowed = _starts_with_any(path, _USER_WRITE_PREFIXES) or _is_earnings_refresh(path)
    else:
        allowed = _starts_with_any(path, _USER_READ_PREFIXES)

    if not allowed:
        return JSONResponse({"detail": "Admin role required"}, status_code=403)
    return await call_next(request)
