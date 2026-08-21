"""MDD = min(Wₜ/Mₜ − 1), and the two things about it that are easy to get quietly wrong.

⚠⚠ FIRST: AN EPISODE ENDS WHEN THE OLD PEAK IS REGAINED, NOT WHEN THE SERIES TURNS UP. A 40% fall
that bounces 5% and then falls further is ONE drawdown. Splitting on direction reports a handful of
shallow dips and no crash — a plausible-looking answer with the finding removed from it.

⚠⚠ SECOND: THE CADENCE CHANGES THE ANSWER. A fall that recovers inside the period is invisible to a
series sampled at that period, so monthly MDD is structurally shallower than daily — by percentage
points, not noise. The last test here is that mechanism in four numbers.

Every case is hand-computable on purpose: a drawdown test against a random walk asserts whatever the
implementation happens to do.
"""
from __future__ import annotations

import pytest

import routers._portfolio_drawdown as D


def _dates(n: int) -> list[str]:
    return [f"2026-01-{i + 1:02d}" for i in range(n)]


class TestTheEpisode:
    def test_peak_trough_and_recovery_are_the_real_indices(self):
        # W: 1.10, 0.99, 0.891, 1.0692, 1.1761 — peak 1.10 at 0, trough 0.891 at 2.
        eps = D.drawdown_episodes([0.10, -0.10, -0.10, 0.20, 0.10], _dates(5))
        assert len(eps) == 1
        e = eps[0]
        # ⚠ PEAK-TO-TROUGH, NOT START-TO-TROUGH. 0.891/1.10 − 1, not 0.891/1.0 − 1.
        assert e["depth_pct"] == pytest.approx(-19.0, abs=1e-9)
        assert (e["peak_date"], e["trough_date"], e["recovery_date"]) == (
            "2026-01-01", "2026-01-03", "2026-01-05")
        assert (e["decline_periods"], e["recovery_periods"], e["total_periods"]) == (2, 2, 4)

    def test_a_bounce_that_does_not_regain_the_peak_does_not_split_it(self):
        """⚠⚠ THE ONE THAT MATTERS. Down 20%, up 5%, down 20% again is a single 32.8% drawdown."""
        eps = D.drawdown_episodes([-0.20, 0.05, -0.20, 1.00], _dates(4))
        assert len(eps) == 1
        assert eps[0]["depth_pct"] == pytest.approx(-32.8, abs=1e-9)
        # The trough is the LATER, deeper point — not the first fall.
        assert eps[0]["trough_index"] == 2

    def test_separate_falls_are_separate_episodes_deepest_first(self):
        eps = D.drawdown_episodes([-0.10, 0.20, -0.30, 0.60], _dates(4))
        assert len(eps) == 2
        assert eps[0]["depth_pct"] <= eps[1]["depth_pct"]
        assert D._mdd([-0.10, 0.20, -0.30, 0.60]) == pytest.approx(eps[0]["depth_pct"], abs=1e-9)

    def test_an_open_drawdown_does_not_invent_a_recovery(self):
        """⚠ THE ONE THING THIS PANEL MUST NEVER DO. Dating an unfinished recovery at today, or at
        the last observation, reports a recovery that has not happened."""
        e = D.drawdown_episodes([0.10, -0.30, 0.05], _dates(3))[0]
        assert e["recovered"] is False
        assert e["recovery_date"] is None
        assert e["recovery_periods"] is None and e["total_periods"] is None

    def test_a_series_that_only_rises_has_no_drawdown(self):
        assert D.drawdown_episodes([0.01] * 10, _dates(10)) == []
        # ⚠ 0.0, NOT None — "it never fell" is a measurement; None would mean "not measurable".
        assert D._mdd([0.01] * 10) == 0.0
        assert D._mdd([]) is None


class TestCurrentState:
    def test_it_is_zero_at_a_new_high_and_negative_below_one(self):
        assert D._current_dd([0.1, 0.1]) == pytest.approx(0.0, abs=1e-12)
        assert D._current_dd([0.1, -0.2]) == pytest.approx(-20.0, abs=1e-9)


class TestTheCadenceUnderstatesIt:
    def test_a_dip_that_recovers_inside_the_period_is_invisible_to_it(self):
        """⚠⚠ WHY THIS VIEW DEFAULTS TO DAILY WHERE THE OTHERS DEFAULT TO WEEKLY. Down 10% and
        straight back up within one week: a daily series sees a 10% drawdown, a weekly series sees
        a flat week. That is not noise — it is the whole fall, gone."""
        daily = [-0.10, 1 / 0.9 - 1.0]
        assert D._mdd(daily) == pytest.approx(-10.0, abs=1e-6)

        # The same fortnight sampled weekly: one bucket, one (zero) return.
        weekly = [(1 - 0.10) * (1 / 0.9) - 1.0]
        assert D._mdd(weekly) == pytest.approx(0.0, abs=1e-9)
