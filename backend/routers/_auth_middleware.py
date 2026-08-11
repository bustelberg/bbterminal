"""API authentication + authorization gate.

EVERY `/api/*` request must carry a valid Supabase JWT, with one exception
tier (public health/cron endpoints). Authorization is role-based:

  * admin  → any endpoint.
  * user   → only the API behind the non-admin-visible pages (/companies,
             /earnings, /schedule and the Management Dashboard), plus the few
             mutations those pages need (earnings refresh).
  * anon   → nothing but the public tier → 401.

Tiers (matched as `path.startswith(prefix)` unless stated):
  _PUBLIC_PREFIXES     no auth at all — health/ping + the cron endpoints,
                       which verify their own `X-Cron-Secret`.
  _SELF_AUTH_PREFIXES  the endpoint verifies the caller's token itself
                       (login/self-service + admin user management); the gate
                       lets the request reach it untouched.
  _ADMIN_ONLY_PREFIXES admin even though they sit inside a user-readable
                       prefix — checked FIRST, before any allow.
  _ADMIN_ONLY_PATTERNS the same deny, by REGEX, for the ones whose id sits in
                       the MIDDLE of the path so no prefix can name them
                       without swallowing a sibling. Checked alongside it.
  _USER_READ_PREFIXES  GET/HEAD allowed for any authenticated user.
  _USER_WRITE_PREFIXES writes allowed for any authenticated user.
  _USER_POST_READ_PATHS  EXACT paths that compute-and-return over a POSTed
                       basket — reads that cannot be GETs.
  everything else      admin only.

⚠ THE METHOD IS NOT THE AUTHORITY ON WHETHER SOMETHING IS A READ. Both
exceptions above exist because it lied in each direction: an SSE endpoint
scrapes for minutes behind a GET, and a basket calculation POSTs because its
input does not fit in a URL.

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
# `/api/airs` + the three `/api/asset-pipeline` by-ISIN reads are the Management Dashboard (the
# portfolios table, the correlation matrix, the Analyse/Fundamental modals). The PAGE is
# user-visible; every mutation on it stays admin-only, which is why none of these appear in the
# write tier — see `_ADMIN_ONLY_PREFIXES` for the GETs that are really writes.
_USER_READ_PREFIXES: tuple[str, ...] = (
    "/api/companies",
    "/api/earnings",
    "/api/usage",
    "/api/scheduled-strategies",
    "/api/fx/",
    "/api/benchmarks",
    "/api/airs/",
    "/api/asset-pipeline/fundamentals/",
    "/api/asset-pipeline/latest-close/",
    "/api/asset-pipeline/risk/",
)

# ⚠ A GET IS NOT ALWAYS A READ, AND THIS IS CHECKED BEFORE THE READ TIER SO WIDENING ONE ABOVE
# CANNOT QUIETLY RE-EXPOSE THEM. The two `scan` endpoints are GETs only because they stream (SSE);
# each drives a live Playwright scrape of AirSPMS — minutes of work against a third-party system
# that rate-limits and can lock the shared login out. Method is the wrong test for them.
# `/api/airs/crm-relaties` IS a genuine read, of CLIENT RELATIONSHIP records — a different subject
# from the portfolios this page is about, and it belongs to the admin-only /airs-portfolio page.
_ADMIN_ONLY_PREFIXES: tuple[str, ...] = (
    "/api/airs/scan",
    "/api/airs/model-portfolios/scan",
    "/api/airs/crm-relaties",
)

# ⚠ THE SAME DENY, BY PATTERN, BECAUSE THE ID SITS IN THE MIDDLE OF THE PATH. A prefix here can
# only be `/api/airs/accounts/`, which is every sub-resource of every account at once.
#
# These four ARE the /management-dashboard Overview EXPANDED ROW (admin-only from 2026-08-06): an
# account's own positions and their EUR values, its mutations for the year, the reconciliation
# against AIRS's figure, and the link picker. The summary table above them stays user-readable —
# what is restricted is opening a book, not seeing that it exists.
#
# ⚠ HIDING THE ROW IS NOT THE RULE, THIS IS. The frontend makes the `<tr>` inert for a non-admin,
# which stops the click and nothing else: the URLs are three lines of a component every user
# downloads. Without this tier the restriction would last exactly as long as nobody opened the
# network tab.
#
# ⚠ `/isins` IS DELIBERATELY ABSENT AND MUST STAY ABSENT. It is the one account sub-resource the
# expand SHARES with the Analyse button, which non-admins keep — it is how an unpaired book gets a
# basket to analyse (`openModal`). Folding these into the `/api/airs/accounts/` prefix would take
# Analyse away as collateral, silently, for the rows that need it most.
_ADMIN_ONLY_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^/api/airs/accounts/[^/]+/holdings$"),
    re.compile(r"^/api/airs/accounts/[^/]+/transactions$"),
    re.compile(r"^/api/airs/accounts/[^/]+/return-reconciliation$"),
    re.compile(r"^/api/airs/accounts/[^/]+/linkable$"),
)


def _is_admin_only_pattern(path: str) -> bool:
    return any(p.match(path) for p in _ADMIN_ONLY_PATTERNS)

# Writes any AUTHENTICATED user may make — the mutations those pages need.
# (Earnings refresh is handled separately by `_is_earnings_refresh`.)
_USER_WRITE_PREFIXES: tuple[str, ...] = ()

# ⚠ READS THAT ARRIVE AS POST. This gate splits on HTTP method, so a compute-and-return endpoint
# whose input is a LIST OF ISINS — too long for a URL — lands in the write tier and 403s for a user
# on a page they are allowed to open. Every path here mutates nothing; it takes a basket in and
# returns figures.
#
# ⚠ MATCHED BY EXACT PATH, NEVER PREFIX. `/api/earnings/fundamental-coverage` computes what we
# hold; `/api/earnings/fundamental-coverage/ingest`, one segment further down, spends GuruFocus
# quota to go and fetch it. A prefix would hand a user the second along with the first.
_USER_POST_READ_PATHS: frozenset[str] = frozenset({
    "/api/airs/basket/analysis",
    "/api/asset-pipeline/basket/performance",
    "/api/earnings/capex-margin-inputs",
    "/api/earnings/cash-conversion-inputs",
    "/api/earnings/cash-return-inputs",
    "/api/earnings/debt-ratio-inputs",
    "/api/earnings/dividend-yield-inputs",
    "/api/earnings/fcf-sbc-yield-inputs",
    "/api/earnings/fundamental-blend",
    "/api/earnings/fundamental-blend-breakdown",
    "/api/earnings/fundamental-blend-matrix",
    "/api/earnings/fundamental-blend-metrics",
    "/api/earnings/fundamental-blend-metrics/stream",
    "/api/earnings/fundamental-coverage",
    "/api/earnings/gross-margin-inputs",
    "/api/earnings/interest-burden-inputs",
    "/api/earnings/margin-inputs",
    "/api/earnings/portfolio-revenue-matrix",
    "/api/earnings/relative-growth-breakdown",
    "/api/earnings/sbc-ocf-inputs",
})

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
    # ⚠ THE DENY IS FIRST. It covers endpoints that sit inside a user-readable prefix but are not
    # reads (the SSE scrapes) or not this page's subject (CRM), so it must not be reachable by
    # widening a prefix above.
    if _starts_with_any(path, _ADMIN_ONLY_PREFIXES) or _is_admin_only_pattern(path):
        allowed = False
    elif request.method in _WRITE_METHODS:
        allowed = (
            _starts_with_any(path, _USER_WRITE_PREFIXES)
            or _is_earnings_refresh(path)
            or (request.method == "POST" and path in _USER_POST_READ_PATHS)
        )
    else:
        allowed = (
            _starts_with_any(path, _USER_READ_PREFIXES)
            or _is_user_get_resource(path)
        )

    if not allowed:
        return JSONResponse({"detail": "Admin role required"}, status_code=403)
    return await call_next(request)
