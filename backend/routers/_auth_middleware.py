"""API authentication + authorization gate.

EVERY `/api/*` request must carry a valid Supabase JWT, with one exception
tier (public health/cron endpoints). Authorization is role-based:

  * admin  → any endpoint.
  * user   → only the API behind the non-admin-visible pages (/companies,
             /earnings, /schedule and the Management Dashboard), plus the
             mutations those pages need: the earnings refresh, and every
             refresh on the Management Dashboard.
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
  _USER_REFRESH_PATHS / _USER_REFRESH_PATTERNS
                       the /management-dashboard refreshes (and the Cancel
                       that stops one) — writes that only make the page's own
                       figures current, never change what it says.
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
# user-visible; its REFRESHES are too (`_USER_REFRESH_PATHS`), but nothing else that writes —
# see `_ADMIN_ONLY_PREFIXES` for the GETs that are really writes.
# `/api/jobs` is the job TRANSPORT (list / stream), which every refresh reports through: without it
# a user could start a run and see no progress. Starting a job is not generic (there is no
# `POST /api/jobs`) — the owning endpoint starts it — and the one job write, Cancel, is in
# `_USER_REFRESH_PATTERNS`.
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
    "/api/jobs",
)

# ⚠ A GET IS NOT ALWAYS A READ, AND THIS IS CHECKED BEFORE THE READ TIER SO WIDENING ONE ABOVE
# CANNOT QUIETLY RE-EXPOSE THEM. `/api/airs/scan` is a GET only because it streams (SSE); it drives
# a live Playwright scrape of AirSPMS — minutes of work against a third-party system that
# rate-limits and can lock the shared login out. Method is the wrong test for it.
# ⚠ THE SSE SCANS ARE STILL DENIED, BUT THEIR *JOB* TWINS ARE NOT — see `_USER_REFRESH_PATTERNS`.
# The scrape a non-admin may start is the one the Management Dashboard starts: a background job
# with a handle, a progress toast and a Cancel. The raw SSE forms belong to the admin-only
# /airs-portfolio page and hold a request open for the whole scrape, which is a different thing to
# hand out. That is why `/api/airs/model-portfolios/scan` moved to an EXACT pattern below: as a
# PREFIX it also swallowed `/api/airs/model-portfolios/scan/job`, which is the Dashboard's.
_ADMIN_ONLY_PREFIXES: tuple[str, ...] = (
    "/api/airs/scan",
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
    # ⚠ EXACT, NOT A PREFIX — `/api/airs/model-portfolios/scan/job` is the same work as a
    # cancellable background job and every authenticated user may start it (see
    # `_USER_REFRESH_PATTERNS`). Only the request-held SSE form is admin-only.
    re.compile(r"^/api/airs/model-portfolios/scan$"),
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
    # ⚠ A READ THAT MUST BE A POST — it takes the book's holdings in the body precisely so it can
    # describe the rows the reader is looking at, and a URL cannot carry 49 ISINs and their
    # weights. It computes and returns; it stores nothing. See `_active_share`.
    "/api/airs/portfolio/active-share",
    "/api/airs/portfolio/exposure",
    "/api/airs/portfolio/concentration",
    "/api/airs/portfolio/drawdown",
    "/api/airs/portfolio/volatility",
    "/api/airs/portfolio/risk-correlation",
    "/api/airs/portfolio/tracking-error",
    "/api/asset-pipeline/basket/performance",
    "/api/earnings/capex-margin-inputs",
    "/api/earnings/cash-conversion-inputs",
    "/api/earnings/cash-return-inputs",
    "/api/earnings/debt-ratio-inputs",
    "/api/earnings/dividend-yield-inputs",
    "/api/earnings/fcf-sbc-yield-inputs",
    "/api/earnings/fundamental-blend",
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

# ⚠⚠ THE /management-dashboard REFRESHES, OPEN TO EVERY AUTHENTICATED USER (2026-08-19, on
# request). The page was readable but frozen: every button that makes what it shows CURRENT — the
# AIRS scrape behind the Overview table, the index rebuild behind Benchmarks, the fundamentals
# fills behind the Analyse and grid panels — sat behind the admin tier, so a user could see a stale
# figure and had no way to act on it. They may now start all of them.
#
# ⚠ THIS IS A DELIBERATE COST DECISION, NOT AN OVERSIGHT BEING CORRECTED. Every path here spends
# something real: a GuruFocus call against a MONTHLY quota, or a Playwright session against AirSPMS
# under one shared login that rate-limits. The judgement is that a stale dashboard nobody can
# refresh costs more than the quota does. If that stops being true, narrow THIS tier — do not
# re-hide the buttons in the frontend and leave the endpoints open.
#
# ⚠ EXACT PATHS AND ANCHORED PATTERNS, NEVER PREFIXES — the same rule, and the same reason, as
# `_USER_POST_READ_PATHS` states above. `/api/airs/` and `/api/benchmarks` are read prefixes for
# this page; a write PREFIX under either would hand over the deletes, the overrides and the link
# picker along with the refreshes, which is exactly what the Overview row still hides.
#
# ⚠ WHAT IS DELIBERATELY ABSENT: DELETE on an account or a universe, the class / ISIN / link
# overrides, and renaming a book. Those CHANGE what the page says; a refresh only makes it current.
# That is the line, and it is the one the frontend's remaining `isAdmin` guards draw too.
_USER_REFRESH_PATHS: frozenset[str] = frozenset({
    # Overview → "Refresh all from AIRS", both halves (accounts, then model portfolios).
    "/api/airs/vermogen/refresh/job",
    "/api/airs/model-portfolios/scan/job",
    # Analyse modal → the fundamentals fill over a basket of ISINs (an unpaired book).
    "/api/airs/basket/fundamentals/ingest/job",
    # The Analyse modal's input tables → fetch ONE `no_data` holding's financials.
    # ⚠ `/ingest`, one segment below the read `/api/earnings/fundamental-coverage` that is already
    # in `_USER_POST_READ_PATHS`. Both are named in full, on purpose.
    "/api/earnings/fundamental-coverage/ingest",
})

_USER_REFRESH_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Overview → one row's Refresh (re-scan this book's AIRS reports).
    re.compile(r"^/api/airs/portfolios/[^/]+/refresh/job$"),
    # Analyse modal → the fundamentals fill over a PAIRED model portfolio's holdings.
    re.compile(r"^/api/airs/model-portfolios/\d+/fundamentals/ingest/job$"),
    # Benchmarks → per-index Refresh and "Refresh all" (constituents, caps, prices).
    re.compile(r"^/api/benchmarks/index/[^/]+/refresh/job$"),
    # Benchmarks → the fundamentals half of the same button, and the grid's "All N" fill.
    re.compile(r"^/api/benchmarks/index/[^/]+/fundamentals/ingest/job$"),
    # Benchmarks grid → one constituent's Fetch cell.
    re.compile(r"^/api/benchmarks/company/\d+/fundamentals/ingest/job$"),
    # ⚠ AND THE STOP. Every path above starts work that runs for minutes and reports through the
    # job toast; a Cancel the starter cannot press is how a run with no way out gets reported as
    # "stuck". `GET /api/jobs` + `/stream` ride the read tier (`/api/jobs` is in
    # `_USER_READ_PREFIXES`) — this is the one job-transport WRITE.
    re.compile(r"^/api/jobs/[^/]+/cancel$"),
)


def _is_user_refresh(path: str) -> bool:
    """A /management-dashboard refresh (or the Cancel that stops one)."""
    return path in _USER_REFRESH_PATHS or any(p.match(path) for p in _USER_REFRESH_PATTERNS)


# Specific GETs a non-admin may read, allow-listed by EXACT pattern because each sits inside an
# otherwise admin-only namespace where a PREFIX would hand over the siblings too.
#
#   * The two `/api/momentum/*` resources back the read-only /schedule strategy-detail panel (its
#     current portfolio + source backtest). The endpoints then authorize the specific id, returning
#     the resource only when it belongs to a `user_visible` scheduled strategy (see
#     `get_current_picks` / `load_backtest`).
#   * `/api/asset-pipeline/search` is the /research-dashboard company picker (2026-08-19). ⚠⚠ AN
#     EXACT PATTERN, AND THE ALTERNATIVE IS NOT SUBTLE: the `/api/asset-pipeline/` namespace holds
#     `/grid` (27.56 MB of every ISIN with every column), `/ingest`, `/store`, the bulk resolve and
#     the row refresh. A prefix would hand all of them to every authenticated user in exchange for
#     one type-ahead. The two reads the page's PANELS need are already covered
#     (`/api/asset-pipeline/fundamentals/` and everything under `/api/earnings`), so this is the
#     only line the page adds.
#   ⚠ The search endpoint returns identity only — name, ISIN, symbol, exchange, currency, sector,
#     bar count — and never prices or positions. It is the same catalogue a user can already reach
#     one company at a time through the fundamentals read.
_USER_GET_RESOURCE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^/api/momentum/current-picks/\d+$"),
    re.compile(r"^/api/momentum/backtests/\d+$"),
    re.compile(r"^/api/asset-pipeline/search$"),
)


def _is_user_get_resource(path: str) -> bool:
    return any(p.match(path) for p in _USER_GET_RESOURCE_PATTERNS)


def _is_earnings_refresh(path: str) -> bool:
    """`/api/earnings/{cid}/refresh*` is user-allowed; other writes under
    /api/earnings (none today) stay admin-only."""
    if not path.startswith("/api/earnings/"):
        return False
    return "/refresh" in path[len("/api/earnings/"):]


# ⚠ THE ONE WRITE UNDER `/api/asset-pipeline/` A NON-ADMIN MAY MAKE: bring ONE instrument's stored
# closes up to date, from the Deep Valuation tab's share-price row. It is the same shape of
# permission as `_is_earnings_refresh` — a user looking at a company may spend one vendor call to
# make that company's own figures current — and the read it repairs
# (`/api/asset-pipeline/latest-close/`) is already in `_USER_READ_PREFIXES`, so without this the
# button is visible to every user and 403s for most of them.
#
# ⚠⚠ AN EXACT PATTERN, NEVER A PREFIX — the same rule `_USER_POST_READ_PATHS` states above and for
# the same reason. `/api/asset-pipeline/` holds the ingest, the bulk resolve and the row refresh;
# a prefix here would hand all of them to every authenticated user.
_LATEST_CLOSE_REFRESH = re.compile(r"^/api/asset-pipeline/latest-close/isin/[^/]+/refresh$")


def _is_latest_close_refresh(path: str) -> bool:
    return _LATEST_CLOSE_REFRESH.match(path) is not None


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
    from routers.auth import AuthBackendUnavailable, verify_token  # noqa: PLC0415

    try:
        info = verify_token(request.headers.get("authorization", ""))
    except AuthBackendUnavailable as e:
        # !! 503, NOT 401 — WE DID NOT LEARN THAT THE TOKEN IS BAD, WE LEARNED NOTHING.
        # A 401 here tells the user their login failed and invites a frontend to throw
        # away a valid session, while the real fault is that the identity provider is
        # unreachable. See AuthBackendUnavailable for the incident that motivated this.
        _log.error(
            "[auth] identity provider unreachable (%s) — 503 on %s %s. "
            "This is NOT a bad token: check DB/GoTrue health before touching auth config.",
            e, request.method, path,
        )
        return JSONResponse(
            {"detail": "Authentication service temporarily unavailable — please retry."},
            status_code=503,
            headers={"Retry-After": "5"},
        )
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
    # reads — the SSE scrapes — so it must not be reachable by widening a prefix above.
    if _starts_with_any(path, _ADMIN_ONLY_PREFIXES) or _is_admin_only_pattern(path):
        allowed = False
    elif request.method in _WRITE_METHODS:
        allowed = (
            _starts_with_any(path, _USER_WRITE_PREFIXES)
            or _is_earnings_refresh(path)
            or _is_latest_close_refresh(path)
            or (request.method == "POST" and _is_user_refresh(path))
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
