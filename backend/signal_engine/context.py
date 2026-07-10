"""Evaluation contexts a signal builder can draw on.

One per cadence. `MonthEndCtx` is the month-end panel shape the asset pipeline
computes on; the daily as-of cadence has no builder-level context yet — its
signals are still computed by the vectorized panel builders in
`momentum/signals.py`, which the registry describes but does not yet own.
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class MonthEndCtx:
    """Monthly panels (index = month-end, columns = entity id)."""

    m: pd.DataFrame       # month-end close
    ret1: pd.DataFrame    # 1-month simple return
    vol: pd.DataFrame     # average DAILY volume within the month

    @property
    def dv(self) -> pd.DataFrame:  # average dollar volume (price × volume)
        return self.m * self.vol
