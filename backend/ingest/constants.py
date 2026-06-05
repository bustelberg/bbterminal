"""Shared constants for the data-ingest layer.

Single source of truth for values that were previously duplicated as
private module-level constants across the ingest + benchmark code paths.
Keep this thin — only values that more than one module legitimately
shares belong here.
"""
from __future__ import annotations

from datetime import date

# No market data is stored before this date. Applies uniformly to
# GuruFocus-sourced company prices/volumes (`ingest/prices.py`), earnings
# time-series (`ingest/earnings/`), and benchmark index prices
# (`routers/benchmarks.py`). The strategy never references pre-2002
# history (the backtest UI defaults start_date to 2002), so 1998 is a
# comfortable floor that keeps the dot-com era available without storing
# unbounded history.
#
# Previously duplicated as `_PRICE_CUTOFF`, `_CUTOFF`, and `_BM_CUTOFF`.
DATA_CUTOFF = date(1998, 1, 1)
