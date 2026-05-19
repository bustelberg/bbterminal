"""Admin-only-mutations middleware.

Every POST/PUT/PATCH/DELETE request to a non-exempt path must carry a
Supabase JWT in the Authorization header AND that user must have
`app_metadata.role == 'admin'`. Read-only methods (GET, HEAD, OPTIONS)
pass through unchanged.

Exemptions:
  /api/auth/...                       — login flow, self-delete, etc.
  /api/earnings/.../refresh*          — refresh is explicitly user-allowed
  /api/ingest/scheduled-refresh/cron  — has its own X-Cron-Secret check
  /api/portfolios/parse               — user uploads their own portfolio

Anything else gets 401 (missing/invalid token) or 403 (non-admin).

Frontend mutation fetches go through `frontend/lib/apiFetch.ts` which
auto-attaches the user's session JWT. As long as that's wired the
middleware is invisible to admin users.
"""
from __future__ import annotations

import logging
from typing import Awaitable, Callable

from fastapi import Request
from fastapi.responses import JSONResponse

_log = logging.getLogger(__name__)

_WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

# Path-prefix exemptions. Each entry is matched as `path.startswith(prefix)`.
# When the page-level proxy.ts already gates a path to authenticated-only
# (e.g. /api/auth/*), the middleware doesn't need to re-check.
_EXEMPT_PREFIXES: tuple[str, ...] = (
    "/api/auth/",
    "/api/ingest/scheduled-refresh/cron",
    "/api/portfolios/parse",
    # Earnings refresh endpoints — user-allowed per design.
    # Pattern is `/api/earnings/{cid}/refresh/...` and /refresh-all.
)


def _is_earnings_refresh(path: str) -> bool:
    """`/api/earnings/{cid}/refresh*` is user-allowed; everything else
    under /api/earnings (none today, but defense-in-depth) is admin-only."""
    if not path.startswith("/api/earnings/"):
        return False
    return "/refresh" in path[len("/api/earnings/"):]


def _is_exempt(path: str) -> bool:
    if any(path.startswith(p) for p in _EXEMPT_PREFIXES):
        return True
    if _is_earnings_refresh(path):
        return True
    return False


async def admin_only_mutations(
    request: Request,
    call_next: Callable[[Request], Awaitable],
):
    """Block non-admin mutations at the door. Read-only methods skip the
    check entirely. The auth check imports `_require_admin` lazily so
    middleware setup doesn't pull in the Supabase client at import
    time."""
    if request.method not in _WRITE_METHODS:
        return await call_next(request)

    path = request.url.path
    if _is_exempt(path):
        return await call_next(request)

    # Lazy import — avoids a circular module-init chain (auth.py pulls
    # the Supabase client, which loads dotenv, etc.).
    from routers.auth import _require_admin  # noqa: PLC0415
    from fastapi import HTTPException  # noqa: PLC0415

    authz = request.headers.get("authorization", "")
    try:
        _require_admin(authz)
    except HTTPException as e:
        return JSONResponse(
            {"detail": e.detail or "Admin role required"},
            status_code=e.status_code,
        )
    except Exception as e:
        _log.warning(
            "[auth] mutation check raised %s: %s — denying %s %s",
            type(e).__name__, e, request.method, path,
        )
        return JSONResponse(
            {"detail": "Authorization check failed"},
            status_code=500,
        )

    return await call_next(request)
