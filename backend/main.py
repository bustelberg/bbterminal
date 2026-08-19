"""FastAPI app bootstrap.

The endpoints live in `backend/routers/<domain>.py` (or under a small
package for the momentum domain). This file is intentionally tiny: it
constructs the `FastAPI()` instance, attaches CORS middleware, mounts
every domain router via `include_router(...)`, and lets the momentum
package register its startup hook against the same `app`.

Adding endpoints
----------------
1. Pick a domain — drop the endpoint into the existing
   `routers/<domain>.py` if one fits.
2. New domain → create `routers/<name>.py` exporting `router = APIRouter()`
   and add it to the imports + the mount loop below.

Shared dependencies (the Supabase client + env loading) live in
`backend/deps.py`. Don't import anything from `main` inside a router —
the router files are imported by `main`, so the dependency only goes one
way.
"""

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import (
    admin as _admin_router,
    airs as _airs_router,
    asset_pipeline as _asset_pipeline_router,
    auth as _auth_router,
    benchmarks as _benchmarks_router,
    companies as _companies_router,
    diversifier as _diversifier_router,
    earnings as _earnings_router,
    earnings_portfolios as _earnings_portfolios_router,
    exchange_fees as _exchange_fees_router,
    fee_config as _fee_config_router,
    fx as _fx_router,
    index_universe as _index_universe_router,
    indicators as _indicators_router,
    ingest_runs as _ingest_runs_router,
    isin_compare as _isin_compare_router,
    jobs as _jobs_router,
    leonteq as _leonteq_router,
    longequity as _longequity_router,
    momentum as _momentum_pkg,
    scheduled_strategies as _scheduled_strategies_router,
    system as _system_router,
    timezone as _timezone_router,
    universe as _universe_router,
    universe_templates as _universe_templates_router,
)
from routers._auth_middleware import enforce_api_auth as _enforce_api_auth
from routers._error_middleware import cors_safe_errors as _cors_safe_errors
from routers.momentum._helpers import register_startup_hooks as _register_momentum_hooks
from scheduler import register_scheduler as _register_scheduler

app = FastAPI()

# API auth gate. EVERY /api/* request needs a valid Supabase JWT (except a
# small public health/cron tier); non-admins are limited to the API behind
# the pages they can view (/companies, /earnings, /airs-portfolio). The
# frontend's `apiFetch` helper auto-attaches the session JWT for every /api/
# call, so this is invisible to logged-in users. See routers/_auth_middleware.
# Registered BEFORE CORS on purpose — see the CORS note below.
app.middleware("http")(_enforce_api_auth)

# Unhandled-exception → 500 RESPONSE, so the layer above can put CORS headers on it. Registered
# AFTER the auth gate and BEFORE CORS, i.e. it wraps the gate as well as every route.
#
# ⚠ ORDERING ALONE CANNOT SOLVE THIS ONE, which is why it is a separate layer from the note below.
# `CORSMiddleware` decorates a response; an exception is the absence of one, so it sails past CORS
# to Starlette's outermost `ServerErrorMiddleware` and the 500 ships with no
# `Access-Control-Allow-Origin`. The browser then reports a CORS block for an allow-listed origin
# and the investigation starts in this file instead of in the handler that actually broke. See
# routers/_error_middleware.
app.middleware("http")(_cors_safe_errors)

# CORS — added AFTER the auth gate so it is the OUTERMOST middleware. Starlette
# runs the last-added middleware outermost, so this guarantees that an auth
# rejection (401/403/500) — which short-circuits with its own JSONResponse
# before reaching the route — STILL carries `Access-Control-Allow-Origin`.
# With CORS inner, those rejections shipped without the header and the browser
# reported them as a CORS error ("No 'Access-Control-Allow-Origin' header")
# instead of the real status. `:3001` is kept for the parallel-worktree dev
# server.
_cors_origins = [
    "http://localhost:3000",
    "http://localhost:3001",
    "https://bbterminal.vercel.app",
    "https://bbterminal-api.vercel.app",
]
# ⚠ `RAILWAY_PUBLIC_DOMAIN` IS THIS BACKEND'S OWN DOMAIN, NOT A FRONTEND'S. It does nothing for a
# browser calling us from Vercel; it is here for same-origin tooling only. Do not mistake it for
# "the deployment's frontend is allowed".
if os.environ.get("RAILWAY_PUBLIC_DOMAIN"):
    _cors_origins.append(f"https://{os.environ['RAILWAY_PUBLIC_DOMAIN']}")

# Per-DEPLOYMENT frontends, comma-separated. A second Railway environment (dev) is served by a
# second Vercel project, whose origin cannot be hardcoded above without a code deploy per
# environment — and the failure mode if you forget is a browser-side CORS block that never reaches
# a handler, so nothing is logged server-side and it reads like the backend is down.
#
# ⚠ ORIGINS ONLY — scheme + host, no path, no trailing slash. Starlette compares the `Origin`
# header verbatim, so "https://x.vercel.app/" matches nothing and fails exactly like a missing
# entry. A blank segment is dropped rather than becoming "", which would match nothing either.
#
# ⚠ A BARE HOSTNAME IS ASSUMED https. Typing `CORS_ORIGINS=bbterminal-dev.vercel.app` is the
# obvious thing to do and was silently useless: a browser always sends a full origin, so a
# scheme-less entry can never match anything, and the symptom is identical to not having set the
# variable at all. `localhost`/`127.0.0.1` keep http, since that is what a dev server serves.
def _origin(raw: str) -> str:
    o = raw.strip().rstrip("/")
    if "://" in o:
        return o
    scheme = "http" if o.startswith(("localhost", "127.0.0.1")) else "https"
    return f"{scheme}://{o}"


_cors_origins += [_origin(o) for o in os.environ.get("CORS_ORIGINS", "").split(",") if o.strip()]

# ⚠ `print`, NOT `logging.info`. uvicorn leaves the ROOT logger at WARNING in production, so an
# info line is invisible exactly where this matters. The allow-list is the one piece of CORS
# config with no way to read it back from outside — a rejected origin and an unset variable
# produce the byte-identical "no Access-Control-Allow-Origin" in the browser — so it is printed at
# startup and a deploy log answers the question in one glance.
print(f"[cors] allow_origins = {_cors_origins}", flush=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Domain routers. Order doesn't affect runtime behavior; kept grouped by
# concern for readability when scanning the mount list.
for _r in (
    _system_router.router,
    _auth_router.router,
    _benchmarks_router.router,
    _diversifier_router.router,
    _fx_router.router,
    _indicators_router.router,
    _airs_router.router,
    _companies_router.router,
    _earnings_router.router,
    _earnings_portfolios_router.router,
    _longequity_router.router,
    _universe_router.router,
    _index_universe_router.router,
    _ingest_runs_router.router,
    _leonteq_router.router,
    _exchange_fees_router.router,
    _fee_config_router.router,
    _scheduled_strategies_router.router,
    _universe_templates_router.router,
    _timezone_router.router,
    _isin_compare_router.router,
    # Generic job transport (list / watch / cancel). Starting a job stays with the router that
    # owns the work — see `routers/jobs.py` for why there is no generic starter.
    _jobs_router.router,
    _asset_pipeline_router.router,
    _admin_router.router,
    # Momentum splits into four sub-routers (signals, backtest_stream,
    # backtest_crud, current_picks); `routers.momentum.routers` is the
    # ordered list so we can flatten the iteration here.
    *_momentum_pkg.routers,
):
    app.include_router(_r)

# Momentum owns one startup hook (ACWI exchange-code sanity check). Pass
# the app in so the hook installs on this instance — keeps `app` the
# single source of truth even though the hook implementation lives in the
# momentum package.
_register_momentum_hooks(app)

# Reset any `scheduled_strategy.backfill_status='running'` rows from the
# previous boot — the in-memory lock + worker thread don't survive a
# restart, so those rows would otherwise sit "running" forever and the
# UI would poll them indefinitely.
@app.on_event("startup")
def _reset_stale_backfills() -> None:
    _scheduled_strategies_router.reset_stale_backfills()

# Every blocking Supabase call runs via `asyncio.to_thread`, which uses the
# default executor — only `min(32, cpu+4)` workers, as low as ~6 on a small
# Railway container. When Supabase slows (ingest pipeline + many polling
# clients) those workers stay stuck on in-flight DB calls and new read
# requests QUEUE behind them, hanging until the client times out (~300s).
# Give blocking I/O ample headroom so a slow dependency degrades gracefully.
# The benchmark blends behind the Fundamental modal's Long Equity tab cost ~20s to build for ACWI
# and are dropped by every fundamentals write. They have no per-user dimension, so rebuilding them
# once in the background spares every viewer of every portfolio the wait. ARMED HERE AND NOWHERE
# ELSE — a process that is not serving pages (a unit test calling `invalidate()`, a script) must
# never start the thread. `BLEND_PREWARM=` in the env disables it. See `routers/_blend_prewarm.py`.
@app.on_event("startup")
def _arm_blend_prewarm() -> None:
    from routers import _blend_prewarm  # noqa: PLC0415
    _blend_prewarm.arm()

@app.on_event("startup")
async def _size_io_thread_pool() -> None:
    import asyncio  # noqa: PLC0415
    import concurrent.futures  # noqa: PLC0415
    asyncio.get_running_loop().set_default_executor(
        concurrent.futures.ThreadPoolExecutor(max_workers=48, thread_name_prefix="bb-io")
    )

# In-process APScheduler for the scheduled price/volume ingest jobs
# (daily price-update + rebalance at 05:00 UTC, month-end full price
# refresh on the last day at 12:00 UTC). See scheduler.py for the
# trade-offs vs Railway-native cron. Set DISABLE_SCHEDULER=1 in the
# env to skip — useful when running multiple replicas or during CI.
_register_scheduler(app)
