"""A level index must not be steerable by ONE near-break-even holding.

⚠⚠ THE FAILURE THIS PINS, MEASURED 2026-08-13 ON THE AEX FCF/SHARE INDEX. The chained level path
guards its growth ratio with `prev > 0`, which catches a zero divisor and misses the one that
actually bites: a divisor that is positive and NEAR zero. Prosus's first positive FCF/share is
**0.0090** a share (2021) against a **0.1485** median over its own history — a rounding artefact of
a holding company that hovers around break-even. Its 2022 figure of −0.24 divided by that base is
**−2,700% growth**, carried at a **26% index weight**:

    step = −3.47   ->   level = 589.4 x (1 − 3.471) = −1,456

⚠ AND THE DAMAGE IS INVISIBLE, WHICH IS WHY IT SURVIVED. The level cards plot on a LOG axis, so
every point from the crossing on is simply not drawn — one per period, silently, with `connectNulls`
running a confident straight line across the hole. Measured before the fix: AEX annual drew **6 of
10** points and AEX quarterly **26 of 32**; the card said nothing (`benchNote` only speaks at zero
or one point). The reporter saw "two datapoints" on a chart whose drill-down showed a full panel of
data, which is exactly what this looks like from the outside.

⚠ THE SAME BASE IS A +7,677% MOVE ON THE WAY BACK UP, so a sign-change test would fix half of it.
The pathology is the DIVISOR, not the direction.

Fixed measurements, same two books, after `step_growth`:
    AITopSelectie annual   11/11 plottable (was 11/11, but with a fake −72% crash in 2017 that AMD's
                           0.005 base invented: 132 -> 37; it now reads 132 -> 186)
    AEX annual             10/10 (was 6/10)
    AEX quarterly          32/32 (was 26/32)
"""
from __future__ import annotations

import pytest

from routers._fundamental_blend import (
    _MIN_STEP_BASE_FRACTION, blend_series, member_scale, step_growth,
)

FCF = "annuals__Per Share Data__Free Cash Flow per Share"


def _member(weight: float, points: dict[str, float]) -> dict:
    return {"weight": weight, "points": points, "base_points": {}}


class TestStepGrowthIsTheOneRule:
    def test_a_normal_step_is_the_plain_ratio(self):
        assert step_growth(100.0, 150.0, scale=100.0) == pytest.approx(0.5)

    def test_no_anchor_or_no_value_means_no_growth_not_zero_growth(self):
        """It sits out THIS step and joins at the next — a 0.0 would dilute the step toward zero
        as though the member had stood still, which is a different claim."""
        assert step_growth(None, 150.0, 100.0) is None
        assert step_growth(100.0, None, 100.0) is None

    def test_a_non_positive_anchor_has_no_ratio(self):
        assert step_growth(0.0, 5.0, 10.0) is None
        assert step_growth(-3.0, 5.0, 10.0) is None

    def test_an_immaterial_anchor_is_refused(self):
        """Prosus, in one line: 0.0090 against a 0.1485 median is 6.1% of its own scale."""
        assert step_growth(0.0090, -0.24, member_scale({"a": 0.0090, "b": 0.1485, "c": 0.70}) or 1) \
            is None

    def test_the_bar_is_RELATIVE_so_a_genuinely_small_series_survives(self):
        """⚠ "0.009 is small" is a fact about Prosus, not about a number. NVIDIA's whole FCF/share
        series lives at 0.04–0.16 a share and every step of it is real."""
        nvda = member_scale({"2015": 0.035, "2016": 0.05, "2017": 0.08, "2018": 0.16})
        assert step_growth(0.035, 0.05, nvda) == pytest.approx(0.05 / 0.035 - 1)

    def test_a_real_growth_story_just_above_the_bar_survives(self):
        """Adyen: base 4.29 against a 28.62 median = 0.150, the lowest legitimate anchor measured.
        The bar sits at 0.10, in the gap below it."""
        assert 4.291 / 28.618 > _MIN_STEP_BASE_FRACTION
        assert step_growth(4.291, 28.618, 28.618) is not None

    def test_it_is_floored_at_minus_one_hundred_percent(self):
        """⚠ BELOW ZERO THERE IS NO SCALE. An index is a product of (1 + g): a term under −1 does
        not make it small, it makes it NEGATIVE — and a negative index is not a low reading."""
        assert step_growth(2.0, -1.0, 2.0) == -1.0
        assert step_growth(2.0, -400.0, 2.0) == -1.0


class TestMemberScale:
    def test_median_not_mean(self):
        """The outlier is the thing being measured against, so a mean would be moved by it."""
        assert member_scale({"a": 0.009, "b": 0.14, "c": 0.15, "d": 0.70}) == pytest.approx(0.145)

    def test_no_values_is_zero_which_disables_the_bar_rather_than_raising(self):
        assert member_scale({}) == 0.0


class TestTheIndexCannotBeFlippedByOneHolding:
    def _panel(self) -> list[dict]:
        """Nineteen steady names and one Prosus — the AEX's shape, minimised."""
        steady = {"2020-12-31": 1.00, "2021-12-31": 1.10, "2022-12-31": 1.21}
        members = [_member(4.0, dict(steady)) for _ in range(19)]
        # ⚠ POSITIVE, TINY, THEN NEGATIVE. Every guard the old code had passes this.
        members.append(_member(24.0, {"2020-12-31": 0.10, "2021-12-31": 0.009,
                                      "2022-12-31": -0.24}))
        return members

    def test_the_level_never_goes_negative(self):
        pts = blend_series(self._panel(), FCF)["points"]
        assert pts, "the series must still be drawn"
        assert all(p["value"] > 0 for p in pts), [p["value"] for p in pts]

    def test_every_period_that_clears_the_floor_is_still_PLOTTABLE(self):
        """The bug was never a missing point — it was a point drawn at −1,456 that a log axis
        cannot render, so the count was right and the chart was empty."""
        pts = blend_series(self._panel(), FCF)["points"]
        assert len(pts) == 3
        assert len([p for p in pts if p["value"] > 0]) == 3

    def test_the_pathological_member_moves_it_by_at_most_its_weight(self):
        """Floored at −100%, a 24% holding can cost the index at most ~24% of ANY step. Before, one
        of them cost 347%. Measured after: −14.2% at 2021 (a real fall from a material 0.10 base)
        and +10.0% at 2022 (that member excluded, its 0.009 anchor being immaterial)."""
        pts = blend_series(self._panel(), FCF)["points"]
        steps = [pts[i]["value"] / pts[i - 1]["value"] - 1.0 for i in range(1, len(pts))]
        assert min(steps) > -0.25, steps


class TestNothingElseMoved:
    def test_a_clean_panel_chains_exactly_as_before(self):
        """Every member material, nothing near zero — the rule must be inert."""
        members = [_member(50.0, {"2020-12-31": 10.0, "2021-12-31": 12.0}),
                   _member(50.0, {"2020-12-31": 20.0, "2021-12-31": 30.0})]
        pts = blend_series(members, FCF)["points"]
        # (0.20 + 0.50) / 2 = 0.35
        assert pts[0]["value"] == pytest.approx(100.0)
        assert pts[1]["value"] == pytest.approx(135.0)

    def test_a_ratio_metric_is_untouched(self):
        """`step_growth` lives on the LEVEL path only — a margin is averaged, never chained."""
        members = [_member(50.0, {"2020-12-31": 0.001, "2021-12-31": 0.002}),
                   _member(50.0, {"2020-12-31": 10.0, "2021-12-31": 20.0})]
        pts = blend_series(members, "annuals__Ratios__Net Margin %")["points"]
        assert pts[0]["value"] == pytest.approx((0.001 + 10.0) / 2)
