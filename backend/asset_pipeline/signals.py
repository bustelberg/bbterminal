"""Unified signal panel — the single source of truth for price/volume signals.

Each signal maps a monthly cross-sectional panel (month × instrument) to another
same-shaped panel of signal values (higher = "more attractive", except reversal
where a NEGATIVE information-coefficient is expected). One definition, consumed by
BOTH the Signal Lab (predictive-power research) and the strategy scoring — so the
two can never drift apart.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd


@dataclass
class Ctx:
    """Monthly panels a signal builder can draw on (index = month-end, columns =
    analysis_id)."""
    m: pd.DataFrame       # month-end close
    ret1: pd.DataFrame    # 1-month simple return
    vol: pd.DataFrame     # average DAILY volume within the month

    @property
    def dv(self) -> pd.DataFrame:  # average dollar volume (price × volume)
        return self.m * self.vol


@dataclass(frozen=True)
class Signal:
    name: str
    group: str            # "price" | "volume"
    label: str
    build: Callable[[Ctx], pd.DataFrame]


# Price signals — the classic momentum battery (skip-a-month on the 6/12m ones to
# avoid the short-term-reversal contamination).
_PRICE: list[Signal] = [
    Signal("mom_12_1", "price", "12-1m momentum", lambda c: c.m.shift(1) / c.m.shift(12) - 1),
    Signal("mom_6_1", "price", "6-1m momentum", lambda c: c.m.shift(1) / c.m.shift(7) - 1),
    Signal("mom_3m", "price", "3m momentum", lambda c: c.m / c.m.shift(3) - 1),
    Signal("reversal_1m", "price", "1m reversal", lambda c: c.ret1),
    Signal("vol_adj_12_1", "price", "vol-adj 12-1m", lambda c: (c.m.shift(1) / c.m.shift(12) - 1) / c.ret1.rolling(12).std()),
    Signal("dist_from_high_12m", "price", "dist from 12m high", lambda c: c.m / c.m.rolling(12).max() - 1),
]

# Volume signals — participation / conviction. Dollar-volume momentum, volume
# trend, and volume relative to its own recent average.
_VOLUME: list[Signal] = [
    Signal("dollar_vol_mom_6_1", "volume", "$-volume 6-1m", lambda c: c.dv.shift(1) / c.dv.shift(7) - 1),
    Signal("vol_trend_3m", "volume", "volume trend 3m", lambda c: c.vol / c.vol.shift(3) - 1),
    Signal("vol_vs_6m_avg", "volume", "volume vs 6m avg", lambda c: c.vol / c.vol.rolling(6).mean() - 1),
]

SIGNALS: list[Signal] = _PRICE + _VOLUME


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
