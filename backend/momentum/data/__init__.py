"""Bulk data loaders for the momentum backtester.

External callers (`routers.momentum.*`, `routers.fx`, `scripts/profile_*`)
keep importing from `momentum.data`, so this `__init__.py` re-exports
each loader.

Layout:
  _helpers.py    — _query_with_retry + _load_metric_chunks + constants
  universe.py    — load_universe + load_company_currency
  prices.py      — load_all_prices + load_all_volumes
  fx.py          — sync_fx_rates_to_db + load_fx_rates + convert_prices_to_eur
  self_heal.py   — self_heal_missing_data
"""
from __future__ import annotations

from .fx import convert_prices_to_eur, load_fx_rates, sync_fx_rates_to_db
from .prices import load_all_prices, load_all_volumes
from .self_heal import self_heal_missing_data
from .universe import load_company_currency, load_universe

__all__ = [
    "load_universe",
    "load_company_currency",
    "load_all_prices",
    "load_all_volumes",
    "sync_fx_rates_to_db",
    "load_fx_rates",
    "convert_prices_to_eur",
    "self_heal_missing_data",
]
