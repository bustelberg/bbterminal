"""Market-regime (trend) filter tests.

The filter reads a composite 0..1 market-health score (trend + 6-month
momentum + drawdown breadth) and scales exposure on a linear ramp between
`regime_ramp_lo` (health → `regime_floor`) and `regime_ramp_hi` (health →
fully invested). It composes with vol targeting by multiplying into the
same per-period `exposure_scale`.

Layers:
  1. `compute_market_health` + `compute_regime_scale` in isolation.
  2. `run_backtest` over a synthetic down-trend (health collapses →
     floor) and up-trend (health high → no-op), plus a composition check
     with vol targeting.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from momentum.backtest import BacktestConfig, run_backtest
from momentum.backtest._period import (
    compute_average_rsi,
    compute_market_health,
    compute_market_health_components,
    compute_regime_scale,
)

from tests._backtest_helpers import (
    BACKTEST_END,
    BACKTEST_START,
    PRICE_HISTORY_START,
    PRICES_END,
    build_prices_df,
    build_universe_df,
    calendar_daily,
    equal_signal_weights,
)


def _health_signals(*, above, mom6, dd) -> pd.DataFrame:
    """Build a signals frame with explicit per-stock health components."""
    return pd.DataFrame({
        "company_id": range(len(above)),
        "above_200ma": above,
        "mom_6m": mom6,
        "drawdown_from_recent_high_pct": dd,
    })


class TestComputeMarketHealth:
    def test_blends_three_components(self):
        # b_trend = 1/4 = 0.25; b_mom = 1/4 = 0.25; b_dd = mean(1 - 0.20) = 0.80
        df = _health_signals(
            above=[1, 0, 0, 0],
            mom6=[5.0, -1.0, -1.0, -1.0],
            dd=[-20.0, -20.0, -20.0, -20.0],
        )
        h = compute_market_health(df)
        assert h == (0.25 + 0.25 + 0.80) / 3  # ≈ 0.4333

    def test_strong_market_near_one(self):
        df = _health_signals(above=[1, 1], mom6=[10.0, 8.0], dd=[0.0, 0.0])
        assert compute_market_health(df) == 1.0

    def test_none_when_no_component(self):
        assert compute_market_health(pd.DataFrame({"company_id": [1, 2]})) is None

    def test_components_breakdown(self):
        df = _health_signals(
            above=[1, 0, 0, 0],                       # trend = 0.25
            mom6=[5.0, -1.0, -1.0, -1.0],             # momentum = 0.25
            dd=[-20.0, -20.0, -20.0, -20.0],          # drawdown = 0.80
        )
        c = compute_market_health_components(df)
        assert c == {"trend": 0.25, "momentum": 0.25, "drawdown": 0.8, "composite": round((0.25 + 0.25 + 0.8) / 3, 4)}
        assert compute_market_health_components(pd.DataFrame({"company_id": [1]})) is None


class TestComputeRegimeScale:
    def test_full_exposure_when_health_above_hi(self):
        df = _health_signals(above=[1, 1], mom6=[10.0, 8.0], dd=[0.0, 0.0])  # H = 1.0
        assert compute_regime_scale(df, floor=0.0, lo=0.3, hi=0.7) == 1.0

    def test_floor_when_health_below_lo(self):
        df = _health_signals(above=[0, 0], mom6=[-5.0, -5.0], dd=[-50.0, -50.0])  # H ≈ 0.167
        assert compute_regime_scale(df, floor=0.0, lo=0.3, hi=0.7) == 0.0
        assert compute_regime_scale(df, floor=0.5, lo=0.3, hi=0.7) == 0.5

    def test_proportional_in_the_ramp(self):
        # H = (0.25 + 0.25 + 0.80)/3 = 0.43333; floor 0, lo 0.3, hi 0.7
        # frac = (0.43333 - 0.3)/0.4 = 0.33333 → exposure 0.3333
        df = _health_signals(
            above=[1, 0, 0, 0], mom6=[5.0, -1.0, -1.0, -1.0], dd=[-20.0, -20.0, -20.0, -20.0],
        )
        assert compute_regime_scale(df, floor=0.0, lo=0.3, hi=0.7) == 0.3333
        # With a 0.5 floor the same fraction lifts off the floor:
        # 0.5 + 0.5*0.33333 = 0.6667
        assert compute_regime_scale(df, floor=0.5, lo=0.3, hi=0.7) == 0.6667

    def test_floor_none_disables(self):
        df = _health_signals(above=[0, 0], mom6=[-5.0, -5.0], dd=[-90.0, -90.0])
        assert compute_regime_scale(df, floor=None, lo=0.3, hi=0.7) == 1.0

    def test_no_component_is_a_noop(self):
        assert compute_regime_scale(pd.DataFrame({"company_id": [1, 2]}), floor=0.0, lo=0.3, hi=0.7) == 1.0


class TestComputeAverageRsi:
    def test_pure_uptrend_reads_100_both_methods(self):
        dates = pd.date_range("2024-01-01", periods=140, freq="D")
        s = pd.Series(100.0 * (1.01 ** np.arange(140)), index=dates)  # always rising
        assert compute_average_rsi([1], {1: s}, entry_ts=dates[-1]) == {"simple": 100.0, "wilder": 100.0}

    def test_pure_downtrend_reads_0_both_methods(self):
        dates = pd.date_range("2024-01-01", periods=140, freq="D")
        s = pd.Series(100.0 * (0.99 ** np.arange(140)), index=dates)  # always falling
        assert compute_average_rsi([1], {1: s}, entry_ts=dates[-1]) == {"simple": 0.0, "wilder": 0.0}

    def test_averages_across_universe_and_handles_thin_history(self):
        dates = pd.date_range("2024-01-01", periods=140, freq="D")
        up = pd.Series(100.0 * (1.01 ** np.arange(140)), index=dates)
        down = pd.Series(100.0 * (0.99 ** np.arange(140)), index=dates)
        short = up.iloc[:5]  # < period+1 → skipped
        val = compute_average_rsi([1, 2, 3], {1: up, 2: down, 3: short}, entry_ts=dates[-1])
        assert val == {"simple": 50.0, "wilder": 50.0}  # mean(100, 0); thin series dropped
        assert compute_average_rsi([3], {3: short}, entry_ts=dates[-1]) is None


def _trend_prices_df(cids: list[int], dates: pd.DatetimeIndex, *, peak_frac: float = 0.5) -> pd.DataFrame:
    """Smooth rise to an early peak, then a deep decline — so by the
    backtest window the spot is far below its 200-row mean (trend & 6-mo
    momentum negative) AND deeply drawn down, pushing composite health
    below the ramp's `lo` so exposure pins to the floor."""
    n = len(dates)
    peak = int(n * peak_frac)
    base = np.empty(n)
    base[:peak] = np.linspace(100.0, 200.0, peak)
    base[peak:] = np.linspace(200.0, 30.0, n - peak)
    rows: list[dict] = []
    for i, cid in enumerate(cids):
        for d, v in zip(dates, base * (1.0 + i * 0.01)):
            rows.append({"company_id": cid, "target_date": d.date(), "price": float(v)})
    return pd.DataFrame(rows)


def _volatile_trend_prices_df(cids: list[int], dates: pd.DatetimeIndex, *, sigma: float = 0.02) -> pd.DataFrame:
    """A deep down-trend with day-to-day volatility on top — so vol
    targeting binds (k<1) AND composite health is below `lo` (regime at
    the floor) at the same time."""
    n = len(dates)
    peak = int(n * 0.5)
    base = np.empty(n)
    base[:peak] = np.linspace(100.0, 200.0, peak)
    base[peak:] = np.linspace(200.0, 30.0, n - peak)
    shocks = np.where(np.arange(n) % 2 == 0, sigma, -sigma)
    path = base * (1.0 + shocks)
    rows: list[dict] = []
    for i, cid in enumerate(cids):
        for d, v in zip(dates, path * (1.0 + i * 0.01)):
            rows.append({"company_id": cid, "target_date": d.date(), "price": float(v)})
    return pd.DataFrame(rows)


_UNIVERSE = [
    (10, "A1", "Alpha", "Alpha-1"),
    (20, "A2", "Alpha", "Alpha-2"),
    (30, "B1", "Beta", "Beta-1"),
    (40, "B2", "Beta", "Beta-2"),
]
_CIDS = [10, 20, 30, 40]


def _base() -> dict:
    return dict(
        start_date=BACKTEST_START, end_date=BACKTEST_END,
        signal_weights=equal_signal_weights(),
        top_n_sectors=2, top_n_per_sector=2,
    )


class TestRegimeFilterBacktest:
    def test_derisks_in_downtrend(self):
        dates = calendar_daily(PRICE_HISTORY_START, PRICES_END)
        prices = _trend_prices_df(_CIDS, dates)
        universe = build_universe_df(_UNIVERSE)

        none_run = run_backtest(BacktestConfig(**_base()), prices, universe)
        cash_run = run_backtest(BacktestConfig(**_base(), regime_floor=0.0), prices, universe)
        half_run = run_backtest(BacktestConfig(**_base(), regime_floor=0.5), prices, universe)

        closed_cash = [r for r in cash_run.monthly_records if not r.is_open and r.holdings]
        assert closed_cash, "expected closed holding periods"
        # Breadth has collapsed (< 50% above 200-MA) → every closed period
        # is risk-off → fully to cash.
        assert all(r.exposure_scale == 0.0 for r in closed_cash)
        assert all(r.portfolio_return_pct == 0.0 for r in closed_cash)

        # Half floor → same picks, returns halved vs the unfiltered run.
        none_by = {r.date: r for r in none_run.monthly_records}
        for r in half_run.monthly_records:
            if r.is_open or not r.holdings:
                continue
            assert r.exposure_scale == 0.5
            b = none_by[r.date]
            if b.portfolio_return_pct is not None:
                assert r.portfolio_return_pct == round(b.portfolio_return_pct * 0.5, 4)

    def test_noop_in_uptrend(self):
        # Breadth high (every name above its 200-MA) → risk-on every
        # period → byte-identical to the unfiltered strategy.
        dates = calendar_daily(PRICE_HISTORY_START, PRICES_END)
        prices = build_prices_df({10: 1.0010, 20: 1.0008, 30: 1.0009, 40: 1.0007}, dates)
        universe = build_universe_df(_UNIVERSE)

        none_run = run_backtest(BacktestConfig(**_base()), prices, universe)
        filtered = run_backtest(BacktestConfig(**_base(), regime_floor=0.0), prices, universe)

        assert filtered.summary.total_return_pct == none_run.summary.total_return_pct
        assert filtered.daily_records == none_run.daily_records
        assert all(r.exposure_scale == 1.0 for r in filtered.monthly_records)

    def test_composes_with_vol_target(self):
        # When both overlays bind, the book scales by their product.
        dates = calendar_daily(PRICE_HISTORY_START, PRICES_END)
        prices = _volatile_trend_prices_df(_CIDS, dates)
        universe = build_universe_df(_UNIVERSE)

        vol_only = run_backtest(BacktestConfig(**_base(), vol_target=8.0), prices, universe)
        both = run_backtest(BacktestConfig(**_base(), vol_target=8.0, regime_floor=0.5), prices, universe)

        vol_by = {r.date: r for r in vol_only.monthly_records}
        checked = 0
        for r in both.monthly_records:
            if r.is_open or not r.holdings:
                continue
            v = vol_by[r.date]
            # downtrend → regime risk-off (×0.5) on top of the vol scale
            assert r.exposure_scale == round(v.exposure_scale * 0.5, 4)
            assert v.exposure_scale < 1.0  # vol targeting actually bound
            checked += 1
        assert checked, "expected overlapping closed periods to compare"

    def test_default_config_unscaled(self):
        dates = calendar_daily(PRICE_HISTORY_START, PRICES_END)
        prices = _trend_prices_df(_CIDS, dates)
        result = run_backtest(BacktestConfig(**_base()), prices, build_universe_df(_UNIVERSE))
        assert all(r.exposure_scale == 1.0 for r in result.monthly_records)

    def test_market_health_surfaced_only_when_filter_active(self):
        dates = calendar_daily(PRICE_HISTORY_START, PRICES_END)
        prices = _trend_prices_df(_CIDS, dates)
        universe = build_universe_df(_UNIVERSE)

        off = run_backtest(BacktestConfig(**_base()), prices, universe)
        on = run_backtest(BacktestConfig(**_base(), regime_floor=0.0), prices, universe)

        # Off → never populated (and absent from the wire payload).
        assert all(r.market_health is None for r in off.monthly_records)
        off_dict = off.to_dict()["monthly_records"]
        assert all("market_health" not in r for r in off_dict)

        # On → a 0..1 score on every period that had signals, and it
        # shows up in the serialized payload for charting.
        on_records = [r for r in on.monthly_records if r.holdings]
        assert on_records and all(
            r.market_health is not None and 0.0 <= r.market_health <= 1.0
            for r in on_records
        )
        on_dict = on.to_dict()["monthly_records"]
        assert any("market_health" in r for r in on_dict)
        # Component breakdown rides alongside the composite.
        with_comp = [r for r in on_dict if "market_health_components" in r]
        assert with_comp
        sample = with_comp[0]["market_health_components"]
        assert "composite" in sample and {"trend", "momentum", "drawdown"} & sample.keys()
        # Universe-average RSI (both methods) also surfaced on regime runs.
        on_records = [r for r in on.monthly_records if r.holdings]
        assert any(r.universe_rsi is not None for r in on_records)
        for r in on.monthly_records:
            if r.universe_rsi is None:
                continue
            assert {"simple", "wilder"} <= r.universe_rsi.keys()
            assert all(0.0 <= r.universe_rsi[k] <= 100.0 for k in ("simple", "wilder"))
        assert all(r.universe_rsi is None for r in off.monthly_records)
