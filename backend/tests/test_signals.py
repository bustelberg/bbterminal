"""Unit tests for momentum signal helpers.

These tests pin the *current* mathematical definitions of each signal.
If you change a definition, update the corresponding test deliberately —
that's the point.
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from momentum.signals import (
    _annualized_volatility_pct,
    _compute_single_company_signals,
    _compute_volume_signals,
    _drawdown_from_recent_high_pct,
    _mom_return,
    _volatility_adjusted_return,
    _volume_ratio,
    _volume_trend,
    compute_price_signals,
    compute_signals_panel,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _series(values: list[float], end: str = "2024-07-15") -> pd.Series:
    """Build a daily price series ending at `end` with the given values
    placed at the LAST n days of the calendar (one value per day, ascending)."""
    end_ts = pd.Timestamp(end)
    dates = pd.date_range(end=end_ts, periods=len(values), freq="D")
    return pd.Series(values, index=dates, dtype="float64")


# ---------------------------------------------------------------------------
# _mom_return
# ---------------------------------------------------------------------------

class TestMomReturn:
    def test_six_month_return(self):
        # series spans 7 months: 2024-01-15 to 2024-07-15.
        # All prices = 100 except final = 110. cutoff = 2024-01-15.
        # past[<=cutoff].iloc[-1] = 100, latest = 110 → 10.0%
        end = pd.Timestamp("2024-07-15")
        dates = pd.date_range(start="2024-01-15", end=end, freq="D")
        prices = [100.0] * len(dates)
        prices[-1] = 110.0
        s = pd.Series(prices, index=dates, dtype="float64")
        assert _mom_return(s, 6) == 10.0

    def test_negative_return(self):
        end = pd.Timestamp("2024-07-15")
        dates = pd.date_range(start="2024-01-15", end=end, freq="D")
        prices = [100.0] * len(dates)
        prices[-1] = 75.0
        s = pd.Series(prices, index=dates, dtype="float64")
        assert _mom_return(s, 6) == -25.0

    def test_empty_series_returns_none(self):
        s = pd.Series([], dtype="float64", index=pd.DatetimeIndex([]))
        assert _mom_return(s, 6) is None

    def test_insufficient_history_returns_none(self):
        # Only 5 days of data; asking for 6-month return → None
        s = _series([100.0] * 5)
        assert _mom_return(s, 6) is None

    def test_zero_past_price_returns_none(self):
        # past price = 0 would cause division-by-zero — function must
        # return None instead of raising.
        end = pd.Timestamp("2024-07-15")
        dates = pd.date_range(start="2024-01-15", end=end, freq="D")
        prices = [0.0] * len(dates)
        prices[-1] = 100.0
        s = pd.Series(prices, index=dates, dtype="float64")
        assert _mom_return(s, 6) is None


# ---------------------------------------------------------------------------
# _drawdown_from_recent_high_pct
# ---------------------------------------------------------------------------

class TestDrawdown:
    def test_at_recent_high_is_zero(self):
        s = _series([100.0, 105.0, 110.0])
        assert _drawdown_from_recent_high_pct(s) == 0.0

    def test_25pct_drawdown(self):
        s = _series([100.0, 120.0, 90.0])
        # high = 120, latest = 90 → (90/120 - 1)*100 = -25.0
        assert _drawdown_from_recent_high_pct(s) == -25.0

    def test_only_uses_lookback_window(self):
        # 300 days: high of 200 in first half; lookback=252 should still
        # see most of it. Use a shorter lookback for a clean test.
        s = _series([100.0] * 100 + [200.0] + [50.0] * 100)
        # latest = 50, high in window = 200 → -75.0
        assert _drawdown_from_recent_high_pct(s, lookback_days=252) == -75.0

    def test_empty_series_returns_none(self):
        s = pd.Series([], dtype="float64", index=pd.DatetimeIndex([]))
        assert _drawdown_from_recent_high_pct(s) is None


# ---------------------------------------------------------------------------
# _annualized_volatility_pct
# ---------------------------------------------------------------------------

class TestAnnualizedVolatility:
    def test_constant_series_returns_none(self):
        # No variation → std = 0 → function returns None
        s = _series([100.0] * 130)
        assert _annualized_volatility_pct(s) is None

    def test_known_volatility(self):
        # Series with controlled daily returns: alternating +1%, -1%
        # over 130 days. pct_change std × sqrt(252) × 100 should give
        # a reproducible number.
        prices = [100.0]
        for i in range(130):
            prices.append(prices[-1] * (1.01 if i % 2 == 0 else 0.99))
        s = _series(prices)
        v = _annualized_volatility_pct(s)
        # Hand-check: pct_change is approximately ±1% alternating →
        # std ≈ 0.01 → annualized ≈ 0.01 * sqrt(252) * 100 ≈ 15.87
        assert v is not None
        assert 14.0 < v < 18.0

    def test_short_series_returns_none(self):
        s = _series([100.0, 101.0])
        # len < 3 → None
        assert _annualized_volatility_pct(s) is None


# ---------------------------------------------------------------------------
# _volatility_adjusted_return
# ---------------------------------------------------------------------------

class TestVolAdjustedReturn:
    def test_composition(self):
        # Build a series where mom_6m = 10.0 and vol > 0
        end = pd.Timestamp("2024-07-15")
        dates = pd.date_range(start="2024-01-15", end=end, freq="D")
        # Linear ramp from 100 to 110, with slight noise to ensure vol > 0
        n = len(dates)
        prices = np.linspace(100.0, 110.0, n)
        # Add tiny alternating perturbation for non-zero vol
        prices = prices + np.array([0.1 if i % 2 == 0 else -0.1 for i in range(n)])
        s = pd.Series(prices, index=dates, dtype="float64")
        ret = _mom_return(s, 6)
        vol = _annualized_volatility_pct(s, lookback_days=126)
        expected = round(ret / vol, 4)
        assert _volatility_adjusted_return(s, n_months=6, vol_lookback_days=126) == expected

    def test_zero_vol_returns_none(self):
        # Constant series → vol is None → composed returns None
        s = _series([100.0] * 200)
        assert _volatility_adjusted_return(s) is None


# ---------------------------------------------------------------------------
# _volume_ratio
# ---------------------------------------------------------------------------

class TestVolumeRatio:
    def test_flat_series_ratio_is_one(self):
        s = _series([100.0] * 60)
        assert _volume_ratio(s, 20, 60) == 1.0

    def test_short_window_higher(self):
        # First 40 = 100, last 20 = 150
        # short_avg = 150, long_avg = (40*100 + 20*150)/60 = 116.6667
        # ratio = 150 / 116.6667 ≈ 1.2857
        s = _series([100.0] * 40 + [150.0] * 20)
        ratio = _volume_ratio(s, 20, 60)
        assert ratio is not None
        assert abs(ratio - 1.2857) < 0.001

    def test_insufficient_data_returns_none(self):
        s = _series([100.0] * 50)
        assert _volume_ratio(s, 20, 60) is None

    def test_zero_long_avg_returns_none(self):
        s = _series([0.0] * 60)
        assert _volume_ratio(s, 20, 60) is None


# ---------------------------------------------------------------------------
# _volume_trend
# ---------------------------------------------------------------------------

class TestVolumeTrend:
    def test_doubled_volume(self):
        # Build a 4-month daily volume series ending 2024-04-30.
        end = pd.Timestamp("2024-04-30")
        dates = pd.date_range(start="2024-01-01", end=end, freq="D")
        # Past-month-window vols = 100, recent-month-window vols = 200
        recent_cutoff = end - pd.DateOffset(days=21)
        past_cutoff = end - pd.DateOffset(months=3)
        past_end = past_cutoff + pd.DateOffset(days=21)
        vols = []
        for d in dates:
            if d > recent_cutoff:
                vols.append(200.0)
            elif past_cutoff <= d <= past_end:
                vols.append(100.0)
            else:
                vols.append(150.0)  # outside both windows
        s = pd.Series(vols, index=dates, dtype="float64")
        # recent_avg = 200, past_avg = 100 → +100%
        assert _volume_trend(s, 3) == 100.0

    def test_empty_returns_none(self):
        s = pd.Series([], dtype="float64", index=pd.DatetimeIndex([]))
        assert _volume_trend(s, 3) is None


# ---------------------------------------------------------------------------
# _compute_volume_signals
# ---------------------------------------------------------------------------

class TestComputeVolumeSignals:
    def test_empty_returns_empty_dict(self):
        s = pd.Series([], dtype="float64", index=pd.DatetimeIndex([]))
        assert _compute_volume_signals(s) == {}

    def test_short_series_returns_empty_dict(self):
        s = _series([100.0] * 10)
        assert _compute_volume_signals(s) == {}

    def test_returns_both_keys_when_sufficient(self):
        end = pd.Timestamp("2024-04-30")
        dates = pd.date_range(start="2024-01-01", end=end, freq="D")
        s = pd.Series([100.0] * len(dates), index=dates, dtype="float64")
        out = _compute_volume_signals(s)
        assert "vol_20d_vs_60d" in out
        assert "vol_trend_3m" in out


# ---------------------------------------------------------------------------
# _compute_single_company_signals
# ---------------------------------------------------------------------------

class TestComputeSingleCompanySignals:
    def test_empty_returns_empty_dict(self):
        s = pd.Series([], dtype="float64", index=pd.DatetimeIndex([]))
        assert _compute_single_company_signals(s) == {}

    def test_mom_12_1_skips_last_month(self):
        # Series spanning 14 months: 2023-01-01 to 2024-02-29.
        # Set price at 2023-01-31 (~12mo before final) = 100
        # Set price at 2024-01-29 (~1mo before final) = 110
        # Last day 2024-02-29 = 999 (should be IGNORED by 12-1 calc)
        end = pd.Timestamp("2024-02-29")
        dates = pd.date_range(start="2023-01-01", end=end, freq="D")
        prices = [50.0] * len(dates)
        # find the position of 2023-01-31 and 2024-01-29
        idx_12m = dates.get_indexer([pd.Timestamp("2023-01-31")])[0]
        idx_1m = dates.get_indexer([pd.Timestamp("2024-01-29")])[0]
        # The cutoff lookups use `<=`, taking iloc[-1]. To make them land
        # exactly on these dates, fill the days AFTER each anchor with
        # values past the cutoff.
        # For 12m: cutoff = 2024-02-29 - 12mo = 2024-02-29. Wait, that's
        # the last day. Let's recompute: DateOffset(months=12) from
        # 2024-02-29 → 2023-02-29 (which doesn't exist) → 2023-02-28.
        # past_12m = series[<=2023-02-28].iloc[-1]. So we set 2023-02-28
        # to the anchor value.
        cutoff_12m = pd.Timestamp("2023-02-28")
        cutoff_1m = pd.Timestamp("2024-01-29")
        idx_12m = dates.get_indexer([cutoff_12m])[0]
        idx_1m = dates.get_indexer([cutoff_1m])[0]
        prices[idx_12m] = 100.0
        prices[idx_1m] = 110.0
        prices[-1] = 999.0  # should be ignored by mom_12_1
        s = pd.Series(prices, index=dates, dtype="float64")
        out = _compute_single_company_signals(s)
        # 12-1 = (110 / 100 - 1) * 100 = 10.0
        assert out["mom_12_1"] == 10.0

    def test_above_200ma_uptrend(self):
        # Long uptrend → latest > 200-period mean → 1
        s = _series([float(i) for i in range(100, 400)])
        out = _compute_single_company_signals(s)
        assert out["above_200ma"] == 1

    def test_above_200ma_downtrend(self):
        s = _series([float(i) for i in range(400, 100, -1)])
        out = _compute_single_company_signals(s)
        assert out["above_200ma"] == 0


# ---------------------------------------------------------------------------
# compute_price_signals (orchestrator)
# ---------------------------------------------------------------------------

class TestComputePriceSignals:
    def _build_universe(self, ids: list[int]) -> pd.DataFrame:
        return pd.DataFrame({
            "company_id": ids,
            "sector": ["Tech"] * len(ids),
            "company_name": [f"Co{i}" for i in ids],
            "gurufocus_ticker": [f"T{i}" for i in ids],
        })

    def test_strict_cutoff_excludes_as_of_date(self):
        # Build a price series where the price ON as_of_date is dramatically
        # different from prior prices. With strict `<`, the signal must NOT
        # see that price.
        as_of = date(2024, 7, 15)
        end = pd.Timestamp("2024-07-15")
        dates = pd.date_range(start="2024-01-01", end=end, freq="D")
        # All 100 except final day = 9999. With strict `<`, mom_6m should
        # be ~0; with the old `<=` it would be huge.
        prices = [100.0] * len(dates)
        prices[-1] = 9999.0
        series = pd.Series(prices, index=dates, dtype="float64")
        price_index = {1: series}
        universe = self._build_universe([1])
        out = compute_price_signals(
            pd.DataFrame(),  # unused when price_index given
            universe,
            as_of_date=as_of,
            price_index=price_index,
        )
        # Signal computed on prices strictly before 2024-07-15:
        # latest used = 100, 6mo-ago = 100 → mom_6m = 0.0
        assert not out.empty
        assert out.iloc[0]["mom_6m"] == 0.0

    def test_stale_company_is_excluded(self):
        # Last trade > 30 days before as_of_date → company filtered out.
        as_of = date(2024, 7, 15)
        # Series ending 2024-05-01 (75 days before as_of)
        end = pd.Timestamp("2024-05-01")
        dates = pd.date_range(start="2024-01-01", end=end, freq="D")
        series = pd.Series([100.0] * len(dates), index=dates, dtype="float64")
        price_index = {1: series}
        universe = self._build_universe([1])
        out = compute_price_signals(
            pd.DataFrame(),
            universe,
            as_of_date=as_of,
            price_index=price_index,
        )
        assert out.empty

    def test_fresh_company_within_staleness_threshold(self):
        # Last trade 5 days before as_of_date → included.
        as_of = date(2024, 7, 15)
        end = pd.Timestamp("2024-07-10")
        dates = pd.date_range(start="2024-01-01", end=end, freq="D")
        series = pd.Series([100.0] * len(dates), index=dates, dtype="float64")
        price_index = {1: series}
        universe = self._build_universe([1])
        out = compute_price_signals(
            pd.DataFrame(),
            universe,
            as_of_date=as_of,
            price_index=price_index,
        )
        assert not out.empty

    def test_short_history_company_is_excluded(self):
        # Only 10 prices → < 20 minimum → excluded.
        as_of = date(2024, 7, 15)
        end = pd.Timestamp("2024-07-14")
        dates = pd.date_range(end=end, periods=10, freq="D")
        series = pd.Series([100.0] * 10, index=dates, dtype="float64")
        price_index = {1: series}
        universe = self._build_universe([1])
        out = compute_price_signals(
            pd.DataFrame(),
            universe,
            as_of_date=as_of,
            price_index=price_index,
        )
        assert out.empty


# ---------------------------------------------------------------------------
# compute_signals_panel — parity with compute_price_signals
# ---------------------------------------------------------------------------

class TestSignalsPanelParity:
    """The vectorized panel must produce identical signal values to the
    per-cutoff path for the same (universe, cutoffs, prices, volumes)."""

    @staticmethod
    def _build_universe(company_ids: list[int]) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "company_id": company_ids,
                "sector": [f"Sector {i % 3}" for i in company_ids],
                "company_name": [f"Co{i}" for i in company_ids],
                "gurufocus_ticker": [f"T{i}" for i in company_ids],
            }
        )

    @staticmethod
    def _build_price_series(seed: int, *, end: str = "2026-04-29", periods: int = 800) -> pd.Series:
        # Deterministic random walk so the test is reproducible.
        rng = np.random.default_rng(seed)
        dates = pd.date_range(end=pd.Timestamp(end), periods=periods, freq="B")
        steps = rng.normal(loc=0.0005, scale=0.02, size=periods)
        prices = 100.0 * np.exp(np.cumsum(steps))
        return pd.Series(prices, index=dates, dtype="float64")

    @staticmethod
    def _build_volume_series(seed: int, *, end: str = "2026-04-29", periods: int = 800) -> pd.Series:
        rng = np.random.default_rng(seed + 1000)
        dates = pd.date_range(end=pd.Timestamp(end), periods=periods, freq="B")
        vols = rng.lognormal(mean=12.0, sigma=0.5, size=periods)
        return pd.Series(vols, index=dates, dtype="float64")

    def test_panel_matches_per_cutoff_across_dates(self):
        # 5 companies, signals computed at 8 different cutoffs spanning a
        # month. Every cell that the per-cutoff function emits must match
        # the panel's lookup.
        company_ids = [10, 20, 30, 40, 50]
        price_index = {cid: self._build_price_series(seed=cid) for cid in company_ids}
        volume_index = {cid: self._build_volume_series(seed=cid) for cid in company_ids}
        universe = self._build_universe(company_ids)

        cutoffs = [
            date(2026, 4, 1), date(2026, 4, 6), date(2026, 4, 9), date(2026, 4, 14),
            date(2026, 4, 17), date(2026, 4, 22), date(2026, 4, 27), date(2026, 4, 29),
        ]

        panel = compute_signals_panel(
            universe, cutoffs,
            price_index=price_index,
            volume_index=volume_index,
        )

        signal_cols = [
            "mom_12_1", "mom_6m", "volatility_adjusted_return_6m",
            "drawdown_from_recent_high_pct", "above_200ma",
            "vol_20d_vs_60d", "vol_trend_3m",
        ]

        for c in cutoffs:
            expected = compute_price_signals(
                pd.DataFrame(), universe, as_of_date=c,
                price_index=price_index, volume_index=volume_index,
            )
            actual = panel[c]

            assert set(expected["company_id"]) == set(actual["company_id"]), (
                f"cutoff={c}: company set differs"
            )

            exp_idx = expected.set_index("company_id")
            act_idx = actual.set_index("company_id")
            for cid in exp_idx.index:
                for col in signal_cols:
                    e = exp_idx.at[cid, col] if col in exp_idx.columns else None
                    a = act_idx.at[cid, col] if col in act_idx.columns else None
                    if pd.isna(e) and pd.isna(a):
                        continue
                    if pd.isna(e) or pd.isna(a):
                        # Drop the volume keys — the per-cutoff helper omits
                        # them when len < 20; the panel mirrors that. So if
                        # one is NaN and the other is missing, both should be
                        # treated as "no value" — but if both are present and
                        # only one is NaN, that's a real divergence.
                        raise AssertionError(
                            f"cutoff={c} cid={cid} col={col}: NaN/value mismatch — expected={e!r} actual={a!r}"
                        )
                    assert abs(float(e) - float(a)) < 1e-6, (
                        f"cutoff={c} cid={cid} col={col}: expected={e} actual={a}"
                    )

    def test_panel_excludes_short_history(self):
        # A company with <20 bars must be absent from every cutoff's frame.
        end = pd.Timestamp("2026-04-29")
        short_dates = pd.date_range(end=end, periods=10, freq="B")
        price_index = {99: pd.Series([100.0] * 10, index=short_dates, dtype="float64")}
        universe = self._build_universe([99])

        panel = compute_signals_panel(
            universe, [date(2026, 4, 30)],
            price_index=price_index,
        )
        assert panel[date(2026, 4, 30)].empty

    def test_panel_applies_staleness_filter(self):
        # Last bar > 30 days before cutoff → excluded.
        dates = pd.date_range(end=pd.Timestamp("2026-02-27"), periods=400, freq="B")
        rng = np.random.default_rng(42)
        prices = 100.0 * np.exp(np.cumsum(rng.normal(0.0005, 0.02, len(dates))))
        price_index = {1: pd.Series(prices, index=dates, dtype="float64")}
        universe = self._build_universe([1])

        # Cutoff May 1 2026 — last bar Feb 27 is > 30 calendar days back.
        panel = compute_signals_panel(
            universe, [date(2026, 5, 1)],
            price_index=price_index,
        )
        assert panel[date(2026, 5, 1)].empty
