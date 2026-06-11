"""Volatility-targeting overlay tests.

Two layers:
  1. `compute_exposure_scale` in isolation — the ex-ante basket-vol
     estimator + the de-risk-only clamp (k ≤ max_leverage).
  2. `run_backtest` end-to-end — proves the overlay (a) leaves the
     original strategy byte-identical when the target never binds, and
     (b) actually scales the period return down (without touching the
     selection) when it does.

The overlay must NEVER alter the plain momentum strategy: vol_target=None
is the default and a non-binding target collapses to k=1.0 everywhere.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from momentum.backtest import BacktestConfig, run_backtest
from momentum.backtest._period import compute_exposure_scale

from tests._backtest_helpers import (
    BACKTEST_END,
    BACKTEST_START,
    PRICE_HISTORY_START,
    PRICES_END,
    build_universe_df,
    calendar_daily,
    equal_signal_weights,
)


def _const_vol_series(
    dates: pd.DatetimeIndex, daily_sigma: float, *, drift: float = 0.0, start: float = 100.0,
) -> pd.Series:
    """Price path whose daily returns are `drift ± daily_sigma` alternating.

    `pct_change` recovers those returns exactly, so the realized daily
    stdev is ~`daily_sigma` regardless of drift — a controllable vol knob
    for the estimator tests."""
    n = len(dates)
    shocks = np.where(np.arange(n) % 2 == 0, daily_sigma, -daily_sigma)
    rets = drift + shocks
    rets[0] = 0.0
    prices = start * np.cumprod(1.0 + rets)
    return pd.Series(prices, index=dates)


class TestComputeExposureScale:
    def test_derisks_when_realized_vol_exceeds_target(self):
        dates = pd.date_range("2024-01-01", periods=120, freq="D")
        s = _const_vol_series(dates, daily_sigma=0.02)  # ann ≈ 31.7%
        entry = dates[-1]
        k = compute_exposure_scale(
            [1], {1: s}, entry_ts=entry,
            target_vol_pct=10.0, lookback=60, max_leverage=1.0,
        )
        assert 0.0 < k < 1.0
        # Independently re-derive the intended value from the same window.
        r = s[s.index <= entry].iloc[-61:].pct_change().dropna()
        realized_ann = float(r.std()) * (252 ** 0.5)
        assert k == round(min(1.0, 0.10 / realized_ann), 4)

    def test_caps_at_max_leverage_when_calm(self):
        # De-risk only: a calm series would want k>1, but the 1.0 cap
        # holds it at fully-invested (never borrows).
        dates = pd.date_range("2024-01-01", periods=120, freq="D")
        s = _const_vol_series(dates, daily_sigma=0.001)  # ann ≈ 1.6%
        k = compute_exposure_scale(
            [1], {1: s}, entry_ts=dates[-1],
            target_vol_pct=15.0, lookback=60, max_leverage=1.0,
        )
        assert k == 1.0

    def test_monotonic_in_target(self):
        dates = pd.date_range("2024-01-01", periods=120, freq="D")
        pi = {1: _const_vol_series(dates, 0.02)}
        entry = dates[-1]
        k5 = compute_exposure_scale([1], pi, entry_ts=entry, target_vol_pct=5.0, lookback=60, max_leverage=1.0)
        k10 = compute_exposure_scale([1], pi, entry_ts=entry, target_vol_pct=10.0, lookback=60, max_leverage=1.0)
        assert k5 < k10 <= 1.0

    def test_falls_back_to_one_when_off_or_insufficient_data(self):
        dates = pd.date_range("2024-01-01", periods=120, freq="D")
        s = _const_vol_series(dates, 0.02)
        # vol targeting disabled
        assert compute_exposure_scale([1], {1: s}, entry_ts=dates[-1], target_vol_pct=None, lookback=60, max_leverage=1.0) == 1.0
        # too little history to estimate vol
        short = s.iloc[:5]
        assert compute_exposure_scale([1], {1: short}, entry_ts=short.index[-1], target_vol_pct=10.0, lookback=60, max_leverage=1.0) == 1.0
        # no holdings
        assert compute_exposure_scale([], {1: s}, entry_ts=dates[-1], target_vol_pct=10.0, lookback=60, max_leverage=1.0) == 1.0

    def test_routes_negative_cids_through_series_for(self):
        # Sector-ETF holdings carry negative ids resolved via a benchmark
        # index, mirroring the runner's `_scale_series_for`.
        dates = pd.date_range("2024-01-01", periods=120, freq="D")
        bench = _const_vol_series(dates, 0.02)
        k = compute_exposure_scale(
            [-7], {}, entry_ts=dates[-1],
            target_vol_pct=10.0, lookback=60, max_leverage=1.0,
            series_for=lambda c: bench if c == -7 else None,
        )
        assert 0.0 < k < 1.0


def _volatile_prices_df(drifts: dict[int, float], dates: pd.DatetimeIndex, daily_sigma: float) -> pd.DataFrame:
    rows: list[dict] = []
    for cid, drift in drifts.items():
        s = _const_vol_series(dates, daily_sigma, drift=drift)
        for d, v in zip(dates, s.values):
            rows.append({"company_id": cid, "target_date": d.date(), "price": float(v)})
    return pd.DataFrame(rows)


class TestVolTargetBacktest:
    def _setup(self):
        dates = calendar_daily(PRICE_HISTORY_START, PRICES_END)
        drifts = {10: 0.0010, 20: 0.0008, 30: 0.0009, 40: 0.0007}
        prices = _volatile_prices_df(drifts, dates, daily_sigma=0.02)
        universe = build_universe_df([
            (10, "A1", "Alpha", "Alpha-1"),
            (20, "A2", "Alpha", "Alpha-2"),
            (30, "B1", "Beta", "Beta-1"),
            (40, "B2", "Beta", "Beta-2"),
        ])
        base = dict(
            start_date=BACKTEST_START, end_date=BACKTEST_END,
            signal_weights=equal_signal_weights(),
            top_n_sectors=2, top_n_per_sector=2,
        )
        return prices, universe, base

    def test_scales_period_return_without_touching_selection(self):
        prices, universe, base = self._setup()
        baseline = run_backtest(BacktestConfig(**base), prices, universe)
        targeted = run_backtest(BacktestConfig(**base, vol_target=8.0), prices, universe)

        base_by_date = {r.date: r for r in baseline.monthly_records}
        closed = [r for r in targeted.monthly_records if not r.is_open and r.holdings]
        assert closed, "expected at least one closed holding period"
        # The high synthetic vol (~32% ann) sits well above an 8% target,
        # so every closed period should de-risk.
        assert any(r.exposure_scale < 1.0 for r in closed)
        for r in closed:
            b = base_by_date[r.date]
            # Selection is unchanged by vol targeting — same names, same
            # weights — so the unscaled per-period return matches and the
            # targeted one is exactly that × k.
            assert [h.company_id for h in r.holdings] == [h.company_id for h in b.holdings]
            if b.portfolio_return_pct is not None and r.exposure_scale < 1.0:
                assert r.portfolio_return_pct == round(b.portfolio_return_pct * r.exposure_scale, 4)

    def test_nonbinding_target_is_a_noop(self):
        # A target so high it never binds (k clamps to 1.0 everywhere)
        # must reproduce the plain strategy byte-for-byte — this is the
        # "don't alter the original" guarantee.
        prices, universe, base = self._setup()
        baseline = run_backtest(BacktestConfig(**base), prices, universe)
        huge = run_backtest(BacktestConfig(**base, vol_target=100000.0), prices, universe)

        assert huge.summary.total_return_pct == baseline.summary.total_return_pct
        assert huge.summary.sharpe_ratio == baseline.summary.sharpe_ratio
        assert huge.summary.max_drawdown_pct == baseline.summary.max_drawdown_pct
        assert huge.daily_records == baseline.daily_records
        assert all(r.exposure_scale == 1.0 for r in huge.monthly_records)

    def test_default_config_leaves_exposure_unscaled(self):
        prices, universe, base = self._setup()
        result = run_backtest(BacktestConfig(**base), prices, universe)
        assert all(r.exposure_scale == 1.0 for r in result.monthly_records)
