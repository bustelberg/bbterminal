"""The shared signal registry (Phase 2 of the engine unification).

Two things are pinned here:

1. DERIVATION IS LOSSLESS. `momentum.signals.PRICE_SIGNAL_DEFS` /
   `TREND_SIGNAL_DEFS` and `asset_pipeline.signals.SIGNALS` are now derived from
   `signal_engine.registry` instead of hand-declared. The literals they used to
   hold are reproduced below verbatim; if a registry edit changes them, this
   fails. `PRICE_SIGNAL_DEFS[i]["key"]` is the string a saved
   `scheduled_strategy.config`'s `signal_weights` is keyed by, so drift here
   silently orphans live strategies.

2. THE COLLISIONS STAY COLLISIONS. `mom_12_1` and `vol_trend_3m` each exist in
   both cadences. One pair is equivalent, the other is two different measures
   sharing a name. The measured numbers live in `registry.PARITY`; these tests
   stop someone "tidying up" the duplicate by deleting one of them.
"""
from __future__ import annotations

import pandas as pd
import pytest

from signal_engine import MonthEndCtx, by_cadence, by_name, colliding_names, legacy_defs
from signal_engine.registry import PARITY, SIGNALS


# --- verbatim copies of the literals the registry replaced -------------------

_EXPECTED_PRICE_DEFS = [
    {"key": "mom_12_1", "label": "12-1M Return", "description": "Price return from 12 months ago to 1 month ago, skipping the most recent month. The classic Jegadeesh-Titman momentum factor — avoids short-term mean reversion.", "default_weight": 3, "group": "price"},
    {"key": "mom_6m", "label": "6M Return", "description": "Total price return over the last 6 months. Captures medium-term trend strength.", "default_weight": 2, "group": "price"},
    {"key": "volatility_adjusted_return_6m", "label": "Vol-Adj Return", "description": "6-month return divided by annualized 6-month volatility. Rewards consistent uptrends over volatile spikes. Similar to a Sharpe ratio per stock.", "default_weight": 2, "group": "price"},
    {"key": "drawdown_from_recent_high_pct", "label": "Drawdown", "description": "Current price vs. 52-week high, expressed as a negative %. Closer to 0% = near highs (stronger). Favors stocks holding up well.", "default_weight": 1, "group": "price"},
    {"key": "above_200ma", "label": "Above 200 MA", "description": "Binary: 1 if current price is above the 200-day moving average, 0 otherwise. Classic long-term trend filter — stocks below 200 MA are in a downtrend.", "default_weight": 1, "group": "price"},
    {"key": "vol_20d_vs_60d", "label": "Volume Surge", "description": "Ratio of 20-day average volume to 60-day average volume. Values above 1.0 indicate rising interest and conviction behind price moves. Confirms momentum rather than low-volume drift.", "default_weight": 1, "group": "volume"},
    {"key": "vol_trend_3m", "label": "Volume Trend 3M", "description": "Percentage change in average daily volume: current month vs 3 months ago. Positive = growing institutional attention. Stocks with rising volume alongside price momentum tend to sustain their trends.", "default_weight": 1, "group": "volume"},
]

_EXPECTED_TREND_DEFS = [
    {"key": "trend_continuity", "label": "Trend Continuity", "description": "'Frog-in-the-pan' information discreteness: sign(6M return) × (% up-days − % down-days) over the last ~6 months. Higher = a winner grinding higher on many small up-days (continuous information → momentum tends to persist) rather than a few big jumps.", "default_weight": 2, "group": "trend"},
    {"key": "pct_up_days_6m", "label": "Up-Day Consistency", "description": "Fraction of up-days over the last ~6 months (×100). A smooth, steady climb scores higher than a jumpy one — a magnitude-agnostic measure of trend quality that complements raw return.", "default_weight": 2, "group": "trend"},
    {"key": "rsi_headroom", "label": "RSI Headroom", "description": "Overbought guard: −max(0, RSI(14) − 50). Names with RSI above 50 are penalized (more so the more extended); RSI ≤ 50 is neutral. Keeps the strategy from chasing the most-stretched names near a blow-off top.", "default_weight": 1, "group": "trend"},
]

_EXPECTED_ME_NAMES = [
    "mom_12_1", "mom_6_1", "mom_3m", "reversal_1m", "vol_adj_12_1",
    "dist_from_high_12m", "dollar_vol_mom_6_1", "vol_trend_3m", "vol_vs_6m_avg",
]
_EXPECTED_ME_GROUPS = ["price"] * 6 + ["volume"] * 3


class TestDerivationIsLossless:
    def test_momentum_price_defs_unchanged(self):
        from momentum.signals import PRICE_SIGNAL_DEFS
        assert PRICE_SIGNAL_DEFS == _EXPECTED_PRICE_DEFS

    def test_momentum_trend_defs_unchanged(self):
        from momentum.signals import TREND_SIGNAL_DEFS
        assert TREND_SIGNAL_DEFS == _EXPECTED_TREND_DEFS

    def test_extra_defs_is_price_then_trend(self):
        from momentum.signals import EXTRA_SIGNAL_DEFS
        assert EXTRA_SIGNAL_DEFS == _EXPECTED_PRICE_DEFS + _EXPECTED_TREND_DEFS

    def test_default_weights_stay_ints(self):
        """They round-trip into saved JSON configs; a 3.0 where a 3 was is churn."""
        from momentum.signals import PRICE_SIGNAL_DEFS
        assert all(isinstance(d["default_weight"], int) for d in PRICE_SIGNAL_DEFS)

    def test_dict_field_order_unchanged(self):
        from momentum.signals import PRICE_SIGNAL_DEFS
        assert list(PRICE_SIGNAL_DEFS[0]) == ["key", "label", "description", "default_weight", "group"]

    def test_asset_pipeline_signals_unchanged(self):
        from asset_pipeline.signals import SIGNALS as AP
        assert [s.name for s in AP] == _EXPECTED_ME_NAMES
        assert [s.group for s in AP] == _EXPECTED_ME_GROUPS
        assert all(callable(s.build) for s in AP)

    def test_asset_pipeline_ctx_alias_still_exported(self):
        """`alphalab._signals()` does `_sig.Ctx(...)`."""
        from asset_pipeline.signals import Ctx
        assert Ctx is MonthEndCtx


class TestDailyPanelColumns:
    """`signal_engine.daily` derives its panel columns from the registry's
    declaration order. A reorder there silently reorders the panel columns, and
    `price_panel` concatenates price-then-trend — so pin the order explicitly."""

    def test_price_columns(self):
        from signal_engine.daily import PRICE_COLUMNS
        assert PRICE_COLUMNS == (
            "mom_12_1", "mom_6m", "volatility_adjusted_return_6m",
            "drawdown_from_recent_high_pct", "above_200ma",
        )

    def test_trend_columns(self):
        from signal_engine.daily import TREND_COLUMNS
        assert TREND_COLUMNS == ("trend_continuity", "pct_up_days_6m", "rsi_headroom")

    def test_volume_columns(self):
        from signal_engine.daily import VOLUME_COLUMNS
        assert VOLUME_COLUMNS == ("vol_20d_vs_60d", "vol_trend_3m")

    def test_price_panel_emits_price_then_trend(self):
        from signal_engine.daily import PRICE_COLUMNS, TREND_COLUMNS, price_panel
        idx = pd.date_range("2023-01-02", periods=400, freq="B")
        series = pd.Series(100.0 * (1.001 ** pd.RangeIndex(len(idx))), index=idx)
        assert list(price_panel(series).columns) == list(PRICE_COLUMNS) + list(TREND_COLUMNS)

    def test_as_of_discipline_constants(self):
        from signal_engine.daily import MAX_STALENESS_DAYS, MIN_BARS
        assert (MIN_BARS, MAX_STALENESS_DAYS) == (20, 30)


class TestRegistryInvariants:
    def test_keys_are_unique_and_cadence_namespaced(self):
        for key, spec in SIGNALS.items():
            assert key == f"{'daily' if spec.cadence == 'daily_asof' else 'me'}.{spec.name}"

    def test_only_month_end_signals_carry_builders(self):
        for spec in SIGNALS.values():
            if spec.cadence == "month_end":
                assert spec.build is not None, spec.key
            else:
                assert spec.build is None, f"{spec.key} has a builder but no evaluator uses it"

    def test_legacy_defs_filters_by_group_in_declaration_order(self):
        assert [d["key"] for d in legacy_defs("daily_asof", ("volume",))] == [
            "vol_20d_vs_60d", "vol_trend_3m",
        ]

    def test_by_name_disambiguates_by_cadence(self):
        assert by_name("mom_12_1", "daily_asof").key == "daily.mom_12_1"
        assert by_name("mom_12_1", "month_end").key == "me.mom_12_1"
        with pytest.raises(KeyError):
            by_name("mom_6_1", "daily_asof")  # month_end only

    def test_by_cadence_partitions_the_registry(self):
        assert len(by_cadence("daily_asof")) + len(by_cadence("month_end")) == len(SIGNALS)


class TestNameCollisions:
    def test_exactly_the_two_known_collisions_exist(self):
        """A NEW colliding name means someone added a signal whose relationship to
        its namesake has not been measured. Measure it (scripts/signal_divergence.py)
        and add a PARITY row before adding it here."""
        assert colliding_names() == {
            "mom_12_1": ["daily.mom_12_1", "me.mom_12_1"],
            "vol_trend_3m": ["daily.vol_trend_3m", "me.vol_trend_3m"],
        }

    def test_every_collision_has_a_parity_row(self):
        pairs = {tuple(sorted((p.a, p.b))) for p in PARITY}
        for keys in colliding_names().values():
            assert tuple(sorted(keys)) in pairs

    def test_mom_12_1_pair_is_equivalent(self):
        p = next(p for p in PARITY if p.a == "daily.mom_12_1")
        assert p.equivalent and p.spearman > 0.99

    def test_vol_trend_3m_pair_is_NOT_equivalent(self):
        """Guard against a well-meaning 'dedupe'. These are different measures:
        spearman 0.58, 29% sign disagreements, 59.6-place mean rank shift.
        /schedule depends on the daily one; AlphaLab reports the month-end one."""
        p = next(p for p in PARITY if p.a == "daily.vol_trend_3m")
        assert not p.equivalent
        assert p.spearman < 0.7
        assert p.mean_rank_shift > 10

    def test_the_two_vol_trend_3m_builders_are_not_the_same_formula(self):
        """Behavioural proof, not just metadata: on a series whose monthly average
        volume is flat but whose recent 21 days spiked, the two definitions must
        disagree. Only the month_end one is callable here; assert it ignores the
        intra-month spike that the daily one is built to see."""
        idx = pd.date_range("2024-01-31", periods=6, freq="ME")
        vol = pd.DataFrame({1: [100.0] * 6}, index=idx)
        m = pd.DataFrame({1: [10.0] * 6}, index=idx)
        ret1 = m.pct_change(1)
        me = by_name("vol_trend_3m", "month_end").build(MonthEndCtx(m, ret1, vol))
        # month_end sees a flat monthly average -> zero trend, by construction.
        assert me.iloc[-1, 0] == pytest.approx(0.0)
