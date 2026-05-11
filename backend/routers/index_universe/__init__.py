"""Index universe (S&P 500 + ACWI) reconstruction + serving.

`main.py` mounts `routers.index_universe.router`, so this `__init__.py`
builds one merged APIRouter that includes both per-domain sub-routers.
All routes still sit under `/api/index-universe/...` or `/api/acwi/...`.

`store_index_membership` (in `index_universe.sp500`) is the shared write
path — both the SP500 import and the ACWI save-universe endpoint go
through it.

Layout:
  _helpers.py  — _enrich_tickers, _UNIVERSE_STATS_CACHE, shared SSE drainers
  sp500.py     — /api/index-universe/* (import + reads + check-gurufocus)
  acwi.py      — /api/acwi/* (holdings, announcements, save-universe, …)
"""
from __future__ import annotations

from fastapi import APIRouter

from .acwi import router as _acwi_router
from .sp500 import router as _sp500_router

router = APIRouter()
for _r in (_sp500_router, _acwi_router):
    router.include_router(_r)

__all__ = ["router"]
