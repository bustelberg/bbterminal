"""Min-max normalization gives a fat-tailed signal a fraction of the weight it was assigned.

⚠⚠ THE DEFECT IS IN THE BLEND, NOT IN THE RANKING, AND THAT IS WHY IT SURVIVED. Min-max is
MONOTONIC, so on a single signal it ranks identically to a percentile and nothing looks wrong —
every by-hand check of "is this stock above that one" passes. The damage appears only when signals
are SUMMED: the divisor is `max - min`, so one extreme name compresses everyone else into a sliver
of [0,1], and a signal occupying a sliver contributes almost nothing to a weighted sum however
heavily it is weighted.

Measured on ACWI (1,747 names, three equally-weighted price signals, all asked for 33.3%):

    signal      raw max    realized spread    EFFECTIVE weight
    mom_12_1     +1638%             0.0417              16.6%
    mom_6_1       +174%             0.1018              40.6%
    mom_3_1        +73%             0.1073              42.8%

Top-20 selection overlap against `rank` on identical weights: 6 of 20. The signal the strategy is
named for was quietly worth half its stated weight.

⚠ AND IT NEEDS NO DATA BUG. The real extremes were Kioxia (+1638%), SK Hynix, Micron, Western
Digital, SK Square — a memory/AI supercycle, a correlated CLUSTER rather than one rogue tick, so
winsorizing "the outlier" would not have fixed it either.

⚠⚠ THE FIX IS OPT-IN, AND THESE TESTS PIN THAT HARDEST OF ALL. `minmax` stays the DEFAULT because
the 0-100 scale moves under any other mode — the median ACWI stock scores 5/100 under `minmax` and
50/100 under `rank` — and `min_price_score` is read against that scale, with all three live
scheduled strategies carrying a floor of 30. Flipping the default would silently turn "roughly the
top few percent" into "the top 70%" for every live strategy and every saved run.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from momentum.backtest.types import BacktestConfig
from momentum.scoring import (
    DEFAULT_SCORE_NORMALIZATION,
    SCORE_NORMALIZATIONS,
    _normalize,
    compute_category_scores,
)

_KEYS = ("mom_12_1", "mom_6_1", "mom_3_1")
_DEFS = [{"key": k, "default_weight": 1 / 3, "category": "price"} for k in _KEYS]
_WEIGHTS = {k: 1 / 3 for k in _KEYS}


def _universe(outlier: float) -> pd.DataFrame:
    """800 names, three price signals, one supercycle name in the long-horizon signal."""
    rng = np.random.default_rng(0)
    n = 800
    return pd.DataFrame({
        "company_id": range(n),
        "sector": ["Tech"] * n,
        "mom_12_1": np.r_[rng.normal(8, 25, n - 1), outlier],
        "mom_6_1": rng.normal(5, 18, n),
        "mom_3_1": rng.normal(3, 12, n),
    })


def _effective_weights(df: pd.DataFrame, method: str) -> dict[str, float]:
    """Each signal's SHARE of the blended spread — what its weight is actually worth."""
    spread = {k: _normalize(df[k], method).std() for k in _KEYS}
    total = sum(spread.values())
    return {k: v / total for k, v in spread.items()}


class TestTheDefect:
    def test_minmax_starves_the_signal_with_the_fattest_tail(self):
        eff = _effective_weights(_universe(1638.0), "minmax")
        assert eff["mom_12_1"] < 0.15, "the defect: 33.3% requested, far less delivered"
        # ...and the others silently absorb what it lost.
        assert eff["mom_3_1"] > 0.40

    def test_and_it_is_the_outlier_doing_it(self):
        """⚠ Same data, ordinary tail: min-max is fine. The mode is not wrong, its DIVISOR is."""
        eff = _effective_weights(_universe(60.0), "minmax")
        assert 0.25 < eff["mom_12_1"] < 0.42

    def test_a_single_signal_is_unaffected_which_is_why_this_hid(self):
        """Min-max is monotonic, so it cannot reorder ONE signal — only misweight a blend."""
        s = _universe(1638.0)["mom_12_1"]
        assert _normalize(s, "minmax").rank().equals(_normalize(s, "rank").rank())


class TestTheFix:
    def test_rank_makes_the_weights_exact(self):
        eff = _effective_weights(_universe(1638.0), "rank")
        for k in _KEYS:
            assert eff[k] == pytest.approx(1 / 3, abs=0.01)

    def test_robust_z_is_near_exact_and_keeps_magnitude(self):
        eff = _effective_weights(_universe(1638.0), "robust_z")
        for k in _KEYS:
            assert eff[k] == pytest.approx(1 / 3, abs=0.02)
        # ⚠ The distinction from `rank`: two names in the same decile stay APART under robust_z.
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 100.0])
        z = _normalize(s, "robust_z")
        assert z.iloc[4] > z.iloc[3], "magnitude survives"
        # ⚠ Rounded, not `nunique()`: equal-spaced ranks differ in the last float bit
        # (0.6 - 0.4 != 0.4 - 0.2), so an exact comparison fails on arithmetic that is correct.
        gaps = _normalize(s, "rank").diff().dropna().round(9)
        assert gaps.nunique() == 1, "rank is equal-spaced — it discards magnitude by design"
        assert z.diff().dropna().round(9).nunique() > 1, "robust_z is not"

    def test_the_outlier_is_clipped_not_deleted(self):
        """±2 MADs: the extreme still ranks top, it just stops setting everyone else's scale."""
        z = _normalize(pd.Series([1.0, 2, 3, 4, 5, 1638]), "robust_z")
        assert z.iloc[-1] == 1.0
        assert z.max() <= 1.0 and z.min() >= 0.0


class TestTheDefaultIsUnchanged:
    """⚠⚠ Three live strategies and every saved run depend on this."""

    def test_the_default_is_still_minmax(self):
        assert DEFAULT_SCORE_NORMALIZATION == "minmax"
        assert BacktestConfig.from_dict(
            {"start_date": "2024-01-01", "end_date": "2024-06-01"}).score_normalization == "minmax"

    def test_an_unknown_stored_value_falls_back_rather_than_raising(self):
        """`from_dict` reads STORED configs — a strategy written by a newer build must not break."""
        cfg = BacktestConfig.from_dict(
            {"start_date": "2024-01-01", "end_date": "2024-06-01", "score_normalization": "zzz"})
        assert cfg.score_normalization == "minmax"

    def test_a_stored_choice_round_trips(self):
        cfg = BacktestConfig.from_dict(
            {"start_date": "2024-01-01", "end_date": "2024-06-01", "score_normalization": "rank"})
        assert cfg.score_normalization == "rank"


class TestTheScaleMoves:
    """⚠⚠ Why this could not simply be corrected in place: `min_price_score` reads this scale."""

    def test_the_median_stock_scores_differently_under_each_mode(self):
        df = _universe(1638.0)
        med = {m: compute_category_scores(df, _WEIGHTS, {"price": 1.0}, _DEFS,
                                          normalization=m)["momentum_score"].median()
               for m in SCORE_NORMALIZATIONS}
        assert med["minmax"] < 45, "an outlier drags the whole distribution down the scale"
        assert med["rank"] == pytest.approx(50, abs=2), "percentile puts the median at the middle"
        # A floor of 30 means something completely different in those two worlds.
        assert med["rank"] - med["minmax"] > 8


class TestDegenerateInputsAgreeAcrossModes:
    """⚠ One convention for missing data, decided in `_normalize` rather than per mode."""

    @pytest.mark.parametrize("method", SCORE_NORMALIZATIONS)
    @pytest.mark.parametrize("values", [
        [np.nan, np.nan, np.nan],      # nothing to rank
        [4.0, np.nan, np.nan],         # one observation is not a cross-section
        [2.0, 2.0, 2.0],               # no dispersion
    ])
    def test_neutral_not_zero_or_one(self, method, values):
        out = _normalize(pd.Series(values, dtype=float), method).fillna(0.5)
        assert (out == 0.5).all(), "a degenerate cross-section is neutral, never a rank of 1.0"

    def test_zero_mad_with_live_tails_falls_back_to_rank_not_to_neutral(self):
        """⚠ A zero MAD means the MIDDLE is identical, not that the signal is flat.

        Returning neutral here would silently delete a signal that does carry information in its
        tails; dividing by the zero MAD would be ±inf.
        """
        s = pd.Series([5.0, 5, 5, 5, 5, 99, -99])
        out = _normalize(s, "robust_z")
        assert out.nunique() == 3, "the tails still separate"
        assert out.equals(_normalize(s, "rank"))
