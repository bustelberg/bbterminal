"""Scheduled-refresh pipeline, split one module per phase.

The pipeline that backs the /schedule page used to live entirely in
`routers/ingest_runs.py` (~1,660 lines: throttle + run-row CRUD + all
five phases + three orchestrators + the HTTP endpoints). It's now split
so each phase is importable and unit-testable in isolation:

  runlog.py       run-row tracking primitives (_Throttle, _create_run,
                  _update_run, _now_utc_iso) shared by every phase
  acquisition.py  Phase 0 — upstream source staleness probe
  templates.py    Phase 1 — UniverseTemplate refresh
  prune.py        Phase 2 + 2.5 — orphan prune + duplicate merge
  prices.py       Phase 3 — price/volume refresh (+ company loaders)
  momentum.py     Phase 4 — current-picks snapshots
  pipeline.py     the two orchestrators that sequence the above

`routers/ingest_runs.py` keeps only the HTTP layer (router, endpoints,
job dispatch, `kick_off_refresh`) and imports the orchestrators from
here.
"""
from __future__ import annotations

from .pipeline import (
    _run_pipeline_sync,
    _run_price_update_pipeline_sync,
    _run_rebalance_pipeline_sync,
    _run_smart_pipeline_sync,
)
from .runlog import _create_run, _now_utc_iso, _update_run

__all__ = [
    "_create_run",
    "_now_utc_iso",
    "_run_pipeline_sync",
    "_run_price_update_pipeline_sync",
    "_run_rebalance_pipeline_sync",
    "_run_smart_pipeline_sync",
    "_update_run",
]
