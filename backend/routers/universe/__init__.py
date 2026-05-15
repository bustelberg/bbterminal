"""Universe endpoints — split into per-domain submodules.

`main.py` mounts `routers.universe.router`, so this `__init__.py` builds
one merged APIRouter that includes every per-domain router below. Each
sub-router defines its own routes but they all sit under
`/api/universe/...` and share the same `tags=["universe"]`.

Layout:
  _helpers.py       — derived-metric utilities + shared SSE drainer
  _models.py        — request Pydantic models
  screening.py      — /criteria, /screen, /build, /validate
  labels.py         — /labels, /months, /months/{month} CRUD
  derived_metrics.py — /derived-metrics/criteria, /status, /recompute
  derive.py         — /derive/preview, /derive (the big SSE)

"Derived" universes tighten a base universe via quality-metric thresholds —
e.g., "longequity_cumulative + ROIC>10% + FCF growth>5%". The base + child
relation is stored via `universe.parent_universe_id`.
"""
from __future__ import annotations

from fastapi import APIRouter

from .derive import router as _derive_router
from .derived_metrics import router as _derived_metrics_router
from .labels import router as _labels_router
from .screening import router as _screening_router

router = APIRouter()
for _r in (_screening_router, _labels_router, _derived_metrics_router, _derive_router):
    router.include_router(_r)

__all__ = ["router"]
