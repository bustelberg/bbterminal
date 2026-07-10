"""AlphaLab's Signal Lab, scored by the daily as-of engine.

`/schedule` trades the seven `daily_asof` signals; the Signal Lab only ever
measured the nine `month_end` ones. `alphalab._daily_signal_panels` runs
`signal_engine.daily.evaluate_panel` at the lab's own decision points so both
batteries are measured on identical information.

THE ALIGNMENT IS THE WHOLE TEST
    `evaluate_panel`'s cutoff is exclusive. A cutoff of `month_end + 1 day`
    anchors on the month-end bar itself — the same bar the month-end signals use
    and the same bar `fwd = ret1.shift(-1)` starts its return from. Off by one day
    in either direction and the lab would be comparing signals computed on
    different information, which is exactly the kind of silent bug this whole
    unification exists to prevent.

NO LOOKAHEAD IN EITHER CADENCE
    Verified 2026-07-10: perturbing every bar strictly after month-end M leaves all
    nine month_end signals at M bit-identical. `.shift()` plus the `fwd` alignment
    is a correct decision-at-M / earn-M-to-M+1 convention. The daily cadence adds a
    staleness guard, not a lookahead fix.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from asset_pipeline.alphalab import _DAILY_SPECS, _daily_signal_panels
from signal_engine.daily import MAX_STALENESS_DAYS, compute_single_company_signals

N_ENTITIES = 6


@pytest.fixture(scope="module")
def panels():
    rng = np.random.RandomState(7)
    days = pd.date_range("2019-01-01", "2024-12-31", freq="B")
    close = pd.DataFrame(
        100 * np.cumprod(1 + rng.normal(0.0004, 0.012, (len(days), N_ENTITIES)), axis=0),
        index=days, columns=range(1, N_ENTITIES + 1),
    )
    volume = pd.DataFrame(
        rng.randint(1e5, 1e6, (len(days), N_ENTITIES)).astype(float),
        index=days, columns=range(1, N_ENTITIES + 1),
    )
    months = close.resample("ME").last().index[24:]  # skip the 12m warm-up
    return close, volume, months


class TestShape:
    def test_keyed_by_registry_key_not_bare_name(self, panels):
        """`mom_12_1` already belongs to the month-end battery and is NOT the same
        measure as the daily one — they must not collide in the response."""
        close, volume, months = panels
        out = _daily_signal_panels(close, volume, months)
        assert set(out) == {s.key for s in _DAILY_SPECS}
        assert all(k.startswith("daily.") for k in out)

    def test_panel_axes_match_the_lab(self, panels):
        close, volume, months = panels
        out = _daily_signal_panels(close, volume, months)
        for key, p in out.items():
            assert p.index.equals(months), key
            assert list(p.columns) == list(close.columns), key


class TestAlignment:
    """The daily value at month M must equal the scalar signal on `series[:M]`."""

    @pytest.mark.parametrize("signal", ["mom_12_1", "mom_6m", "drawdown_from_recent_high_pct"])
    def test_month_value_equals_scalar_on_bars_through_month_end(self, panels, signal):
        close, volume, months = panels
        out = _daily_signal_panels(close, volume, months)
        key = f"daily.{signal}"

        eid = close.columns[0]
        for me in (months[0], months[len(months) // 2], months[-1]):
            series = close[eid].loc[:me].dropna()
            expected = compute_single_company_signals(series)[signal]
            got = out[key].loc[me, eid]
            assert got == pytest.approx(expected), f"{signal} at {me.date()}"

    def test_a_bar_after_month_end_never_moves_the_month_value(self, panels):
        """Cutoff is month_end + 1 day, exclusive. Bars strictly after month-end M
        must not reach M's row — the guard against an off-by-one that would leak
        the first days of month M+1 into M's signal."""
        close, volume, months = panels
        M = months[len(months) // 2]

        base = _daily_signal_panels(close, volume, months)
        c2 = close.copy()
        c2.loc[c2.index > M] *= 4.0          # blow up everything after M
        pert = _daily_signal_panels(c2, volume, months)

        for key in base:
            a = base[key].loc[:M].to_numpy(dtype="float64")
            b = pert[key].loc[:M].to_numpy(dtype="float64")
            assert np.array_equal(a, b, equal_nan=True), f"{key} leaked post-M data"

    def test_the_month_end_bar_itself_IS_used(self, panels):
        """The complement of the test above: move month-end M's own close and M's
        row must change. Otherwise the cutoff is a day too early and every signal
        silently lags by one bar."""
        close, volume, months = panels
        M = months[len(months) // 2]
        assert M in close.index, "fixture assumption: month-end is a business day"

        base = _daily_signal_panels(close, volume, months)
        c2 = close.copy()
        c2.loc[M] *= 1.5
        pert = _daily_signal_panels(c2, volume, months)

        a = base["daily.mom_6m"].loc[M].to_numpy(dtype="float64")
        b = pert["daily.mom_6m"].loc[M].to_numpy(dtype="float64")
        assert not np.array_equal(a, b, equal_nan=True)


class TestStalenessGuard:
    """What the daily cadence adds over `resample('ME').last()`."""

    def test_a_delisted_entity_becomes_nan_not_a_carried_price(self, panels):
        close, volume, months = panels
        M = months[len(months) // 2]
        eid = close.columns[0]

        # This entity stops trading well before M — more than the staleness window.
        dead_from = M - pd.Timedelta(days=MAX_STALENESS_DAYS + 20)
        c2 = close.copy()
        c2.loc[c2.index > dead_from, eid] = np.nan

        out = _daily_signal_panels(c2, volume, months)
        assert np.isnan(out["daily.mom_6m"].loc[M, eid])
        # A live entity in the same panel is unaffected.
        assert not np.isnan(out["daily.mom_6m"].loc[M, close.columns[1]])

    def test_an_entity_stale_within_the_window_still_scores(self, panels):
        close, volume, months = panels
        M = months[len(months) // 2]
        eid = close.columns[0]

        dead_from = M - pd.Timedelta(days=MAX_STALENESS_DAYS - 10)
        c2 = close.copy()
        c2.loc[c2.index > dead_from, eid] = np.nan

        out = _daily_signal_panels(c2, volume, months)
        assert not np.isnan(out["daily.mom_6m"].loc[M, eid])
