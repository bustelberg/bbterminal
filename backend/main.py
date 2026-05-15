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
    auth as _auth_router,
    benchmarks as _benchmarks_router,
    companies as _companies_router,
    earnings as _earnings_router,
    exchange_fees as _exchange_fees_router,
    fx as _fx_router,
    index_universe as _index_universe_router,
    indicators as _indicators_router,
    ingest_runs as _ingest_runs_router,
    longequity as _longequity_router,
    momentum as _momentum_pkg,
    schedule_config as _schedule_config_router,
    system as _system_router,
    universe as _universe_router,
)
from routers.momentum._helpers import register_startup_hooks as _register_momentum_hooks
from scheduler import register_scheduler as _register_scheduler

app = FastAPI()

# CORS — keep `:3001` here too so the parallel worktree dev server (when
# active) can hit the same backend without a separate config flip.
_cors_origins = [
    "http://localhost:3000",
    "http://localhost:3001",
    "https://bbterminal.vercel.app",
    "https://bbterminal-api.vercel.app",
]
if os.environ.get("RAILWAY_PUBLIC_DOMAIN"):
    _cors_origins.append(f"https://{os.environ['RAILWAY_PUBLIC_DOMAIN']}")

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
    _fx_router.router,
    _indicators_router.router,
    _airs_router.router,
    _companies_router.router,
    _earnings_router.router,
    _longequity_router.router,
    _universe_router.router,
    _index_universe_router.router,
    _ingest_runs_router.router,
    _exchange_fees_router.router,
    _schedule_config_router.router,
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

# In-process APScheduler for the scheduled price/volume ingest jobs
# (weekly Tue 02:00 UTC + monthly 2nd 02:00 UTC). See scheduler.py for
# the trade-offs vs Railway-native cron. Set DISABLE_SCHEDULER=1 in the
# env to skip — useful when running multiple replicas or during CI.
_register_scheduler(app)
