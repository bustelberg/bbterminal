"""Month-end signal panel for the asset pipeline.

The signal DEFINITIONS moved to `signal_engine.registry` (Phase 2), where they
sit next to momentum's daily-as-of battery so the two can never silently drift
apart again. This module keeps the month-end plumbing: turning daily close/volume
panels into the month-end panels the builders consume, and evaluating them.

Each signal maps a monthly cross-sectional panel (month × instrument) to another
same-shaped panel of signal values (higher = "more attractive", except reversal
where a NEGATIVE information-coefficient is expected). One definition, consumed by
BOTH the Signal Lab (predictive-power research) and the strategy scoring.

NOTE `vol_trend_3m` here is NOT the same measure as momentum's `vol_trend_3m`
(spearman 0.58, 29% sign disagreements). See `signal_engine.registry.PARITY`.
"""
from __future__ import annotations

import pandas as pd

from signal_engine import MonthEndCtx, by_cadence
from signal_engine.registry import SignalSpec

# Kept as module-level aliases: `alphalab` and the Signal Lab import `Ctx` and
# `SIGNALS` from here, and `Signal` was this module's public dataclass name.
Ctx = MonthEndCtx
Signal = SignalSpec

SIGNALS: list[SignalSpec] = by_cadence("month_end")


def monthly_panels(close: pd.DataFrame, volume: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Daily close/volume panels → (month-end close `m`, 1m return `ret1`, monthly
    avg daily volume `vol`)."""
    m = close.resample("ME").last()
    vol = volume.resample("ME").mean()
    ret1 = m.pct_change(1)
    return m, ret1, vol


def build_signals(m: pd.DataFrame, ret1: pd.DataFrame, vol: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """{signal_name: cross-sectional value panel} for every signal."""
    c = Ctx(m, ret1, vol)
    return {s.name: s.build(c) for s in SIGNALS}
