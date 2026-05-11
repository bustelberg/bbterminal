"""Backtest SSE endpoint — split across submodules for readability.

External callers (`routers.momentum.__init__`, `routers.momentum.current_picks`)
keep importing `BacktestRequest`, `_momentum_backtest_stream`, and
`router` from `routers.momentum.backtest_stream`, so this `__init__.py`
re-exports them.

Layout:
  models.py           — VariantSpec + BacktestRequest + constants
  universe_loader.py  — universe / index_universe membership loaders
  benchmarks.py       — sector_etf benchmark price prefetch
  fetch_loop.py       — parallel ensure-prices/volumes loop (db_only=False)
  bulk_loaders.py     — streamed bulk price/volume/FX loaders
  audit.py            — price + volume coverage audits + universe snapshot
  self_heal.py        — refetch + merge missing data
  variants.py         — variants sweep
  single_run.py       — single backtest / multi-trial / current_portfolio
  stream.py           — orchestrator + FastAPI route
"""
from __future__ import annotations

from .models import BacktestRequest, VariantSpec
from .stream import _momentum_backtest_stream, router

__all__ = ["BacktestRequest", "VariantSpec", "_momentum_backtest_stream", "router"]
