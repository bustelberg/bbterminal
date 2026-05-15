"""Momentum domain endpoints split across submodules.

Each submodule defines its own APIRouter for one concern; `routers` lists
them in include-order so `main.py` can mount them in one loop:

    from routers.momentum import routers as momentum_routers
    for r in momentum_routers:
        app.include_router(r)

Submodules
----------
- `_helpers.py`     shared private helpers (strategy_hash, snapshot save,
                    daily-picks persistence, startup hook)
- `signals.py`      GET /api/momentum/signals + SSE signal-breakdown
- `backtest_stream.py`  the big SSE: /api/momentum/backtest + variants sweep
                    + multi-trial + current_portfolio mode
- `backtest_crud.py`  /api/momentum/backtests CRUD (save/load/delete/rename)
- `current_picks.py`  /api/momentum/current-picks/* (snapshots + refresh-mtd + cron)

`_helpers.py` also installs the FastAPI startup hook that warns when the
gurufocus_exchange seeds drift from what acwi.py expects.
"""

from . import _helpers as _helpers  # noqa: F401  (startup hook side-effect)
from .backtest_crud import router as _backtest_crud_router
from .backtest_stream import router as _backtest_stream_router
from .current_picks import router as _current_picks_router
from .signals import router as _signals_router

routers = [
    _signals_router,
    _backtest_stream_router,
    _backtest_crud_router,
    _current_picks_router,
]
