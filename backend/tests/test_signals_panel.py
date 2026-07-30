"""Unit tests for the unified signal panel (asset_pipeline.signals)."""
import numpy as np
import pandas as pd

from asset_pipeline import signals as sig


def _panels(months=30, n=8):
    idx = pd.date_range("2015-01-31", periods=months, freq="ME")
    rng = np.random.RandomState(0)
    close = pd.DataFrame(100 * np.cumprod(1 + rng.normal(0.01, 0.05, (months, n)), axis=0), index=idx)
    volume = pd.DataFrame(rng.randint(1e5, 1e6, (months, n)).astype(float), index=idx)
    return sig.monthly_panels(close.resample("D").ffill(), volume.resample("D").ffill())


def test_all_signals_present_and_grouped():
    names = {s.name for s in sig.SIGNALS}
    assert {"mom_12_1", "mom_6_1", "reversal_1m", "vol_adj_12_1"} <= names
    assert {"dollar_vol_mom_6_1", "vol_trend_3m", "vol_vs_6m_avg"} <= names
    groups = {s.group for s in sig.SIGNALS}
    assert groups == {"price", "volume"}


def test_build_signals_shapes():
    m, ret1, vol = _panels()
    out = sig.build_signals(m, ret1, vol)
    assert set(out) == {s.name for s in sig.SIGNALS}
    for name, panel in out.items():
        assert panel.shape[1] == m.shape[1], name  # same instruments
        assert isinstance(panel, pd.DataFrame), name


def test_reversal_is_just_the_return():
    m, ret1, vol = _panels()
    out = sig.build_signals(m, ret1, vol)
    # reversal_1m is definitionally the 1-month return.
    pd.testing.assert_frame_equal(out["reversal_1m"], ret1)


def test_momentum_skips_the_last_month():
    m, ret1, vol = _panels()
    out = sig.build_signals(m, ret1, vol)
    # 12-1 momentum uses m.shift(1)/m.shift(12) — first 12 rows are NaN.
    assert out["mom_12_1"].iloc[:12].isna().all().all()
    assert out["mom_12_1"].iloc[12:].notna().any().any()
