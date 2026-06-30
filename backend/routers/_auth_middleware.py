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
import re
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
# `/api/companies` stays readable because the user-visible Earnings dashboard
# depends on it (the /companies PAGE itself is blocked for users in proxy.ts).
# `/api/scheduled-strategies` is readable so the read-only /schedule page works;
# the list endpoint filters to `user_visible` strategies for non-admins, and
# every mutation under it stays admin-only (not in the write tier below).
# `/api/fx/` + `/api/benchmarks` are read-only reference data the read-only
# /schedule strategy-detail card needs (EUR/FX conversion + the ETF overlay's
# benchmark identity) — low-sensitivity reads, mutations stay admin-only.
_USER_READ_PREFIXES: tuple[str, ...] = (
    "/api/companies",
    "/api/earnings",
    "/api/usage",
    "/api/scheduled-strategies",
    "/api/fx/",
    "/api/benchmarks",
)

# Writes any AUTHENTICATED user may make — the mutations those pages need.
# (Earnings refresh is handled separately by `_is_earnings_refresh`.)
_USER_WRITE_PREFIXES: tuple[str, ...] = ()

# Specific GET-by-id resources a non-admin may read so the read-only /schedule
# strategy-detail panel loads its current portfolio + source backtest. These
# live under the otherwise admin-only `/api/momentum/*` namespace, so they're
# allow-listed by EXACT pattern (not prefix — that would also expose the
# list-all + sibling routes). The endpoints themselves then authorize the
# specific id, returning the resource only when it belongs to a `user_visible`
# scheduled strategy (see `get_current_picks` / `load_backtest`).
_USER_GET_RESOURCE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^/api/momentum/current-picks/\d+$"),
    re.compile(r"^/api/momentum/backtests/\d+$"),
)


def _is_user_get_resource(path: str) -> bool:
    return any(p.match(path) for p in _USER_GET_RESOURCE_PATTERNS)


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

    # Expose the verified identity so routers can role-filter without
    # re-verifying the token (e.g. /scheduled-strategies returns only
    # `user_visible` rows to non-admins). Set for admins too.
    request.state.auth = info

    # Admins can do anything.
    if info.get("role") == "admin":
        return await call_next(request)

    # Non-admin: only the allowed surface.
    if request.method in _WRITE_METHODS:
        allowed = _starts_with_any(path, _USER_WRITE_PREFIXES) or _is_earnings_refresh(path)
    else:
        allowed = (
            _starts_with_any(path, _USER_READ_PREFIXES)
            or _is_user_get_resource(path)
        )

    if not allowed:
        return JSONResponse({"detail": "Admin role required"}, status_code=403)
    return await call_next(request)
