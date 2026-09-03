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
    _MIN_STEP_BASE_FRACTION, base_bar_scale, blend_series, member_scale, step_growth,
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


class TestTheBarNeedsSOMEBODYToFallBackTo:
    """⚠⚠ A REFUSAL IS AN ABSTENTION, AND ON A LINE OF ONE THERE IS NOBODY TO ABSTAIN IN FAVOUR OF.
    `step_growth` refusing a member lets the others carry the interval. With a single contributor
    the weighted average gets an empty list, `blend_series` takes its "nothing spans this interval"
    `continue`, and that path deliberately does NOT advance `anchor` — so the same base is offered
    at every later period and refused every time. ONE refusal deletes the whole line.

    ⚠⚠ AND THE FIRST PERIOD OF A HYPERGROWER ALWAYS TRIPS IT. `_prepare` rebases each member to 100
    at its first positive period, so the bar reads `100 < 0.10 x median|rebased|` — it fires on any
    member that grew more than ~10x from its first period to its median one. Measured 2026-09-03 on
    NVIDIA through `portfolio-revenue-matrix` as a ONE-HOLDING book:

        price_ps  13 periods  median rebased 2,706  bar 271  ->   1 period drawn
        eps_nri   18 periods  median rebased 2,269  bar 227  ->   1 period drawn
        fcf_ps    13 periods  median rebased   494  bar  49  ->  13 periods drawn

    A one-point line has no window, so the Fundamental modal's `Tables` tab showed `—` for Share
    price and EPS while its `Graphs` tab — which for one company plots the filed figures with no
    chain at all — drew all thirteen. Two tabs of one modal, one company, two answers.
    """

    # NVIDIA's real `price_ps` by fiscal year (local DB, 2026-09-03), the case that was empty.
    NVDA_PRICE = {
        "2015-01-31": 0.48, "2016-01-31": 0.732, "2017-01-31": 2.73, "2018-01-31": 6.145,
        "2019-01-31": 3.594, "2020-01-31": 5.911, "2021-01-31": 12.99, "2022-01-31": 24.486,
        "2023-01-31": 19.537, "2024-01-31": 61.527, "2025-01-31": 120.07, "2026-01-31": 191.13,
    }

    def test_one_member_gets_no_bar(self):
        assert base_bar_scale({"a": 100.0, "b": 5000.0}, members=1) == 0.0

    def test_more_than_one_member_gets_the_bar_unchanged(self):
        at = {"a": 100.0, "b": 5000.0}
        assert base_bar_scale(at, members=2) == member_scale(at)

    def test_a_scale_of_zero_lets_every_positive_base_through(self):
        """0.0 is already the "no bar" value — `member_scale({})` returns it, and `prev < 0.10 x 0`
        is false for every positive `prev`. So this adds a reason, not a mechanism."""
        assert step_growth(100.0, 152.5, 0.0) == pytest.approx(0.525)

    PRICE_CODE = "annuals__Per Share Data__Month End Stock Price"

    def _nvda_line(self) -> dict[str, float]:
        """⚠ KEYED BY THE FISCAL YEAR, which is what `year_bucket` hands back — NVIDIA files at the
        end of January, so `2015-01-31` is period `2015`."""
        pts = blend_series([_member(100.0, dict(self.NVDA_PRICE))], self.PRICE_CODE)["points"]
        return {p["period"]: p["value"] for p in pts}

    def test_nvidias_share_price_draws_every_period_as_a_one_holding_book(self):
        """Before the fix this was ONE point — 2015, the base — and everything after it refused."""
        line = self._nvda_line()
        assert len(line) == len(self.NVDA_PRICE), sorted(line)

    def test_and_the_line_IS_the_filed_series_so_the_two_tabs_agree(self):
        """⚠ THE WHOLE POINT. With one member the chain has nothing to blend, so the level must be
        exactly `100 x v(p)/v(base)` — the series the `Graphs` tab plots directly. Anything else is
        an artefact of running a one-company book through machinery built for an index."""
        line = self._nvda_line()
        base = self.NVDA_PRICE["2015-01-31"]
        for date, filed in self.NVDA_PRICE.items():
            assert line[date[:4]] == pytest.approx(100.0 * filed / base, rel=1e-6), date

    def test_so_the_ten_year_cagr_is_the_companys_own(self):
        """⚠ THE ROW THE READER SEES. A CAGR off this line and one off the filed figures are the
        same number, because the line IS the filed figures — which is what "the Tables tab uses the
        same underlying data as Graphs" has to mean to be checkable."""
        line = self._nvda_line()
        blended = (line["2025"] / line["2015"]) ** 0.1 - 1
        filed = (self.NVDA_PRICE["2025-01-31"] / self.NVDA_PRICE["2015-01-31"]) ** 0.1 - 1
        assert blended == pytest.approx(filed, rel=1e-9)

    def test_the_bar_still_bites_the_moment_there_is_a_second_member(self):
        """⚠ `members > 1` is the whole condition — every measured case the constant was read off
        is a many-member line, which is exactly where an abstention has somewhere to fall back to.
        Prosus beside one steady name: its 0.009 anchor is refused and the steady name carries the
        step, so the index does NOT go through zero."""
        members = [_member(50.0, {"2020-12-31": 1.00, "2021-12-31": 1.10, "2022-12-31": 1.21}),
                   _member(50.0, {"2020-12-31": 0.10, "2021-12-31": 0.009, "2022-12-31": -0.24})]
        pts = blend_series(members, FCF)["points"]
        assert all(p["value"] > 0 for p in pts), [p["value"] for p in pts]

    def test_an_implausible_RESULT_is_still_refused_with_one_member(self):
        """⚠ ONLY THE DIVISOR BAR IS LIFTED. `_MAX_STEP_GROWTH` is a claim about the ANSWER — a
        vendor unit error is a vendor unit error whether one company or a thousand is on the line —
        so a Vertiv-shaped shell year still sits out, and the line honestly stops rather than
        printing a 29,000x step nobody reported."""
        assert step_growth(0.024, 696.1, 0.0) is None


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


class TestAnImplausibleResultIsRefused:
    """⚠⚠ `_MIN_STEP_BASE_FRACTION` GUARDED THE DIVISOR AND NOTHING GUARDED THE NUMERATOR. A vendor
    scale error — a per-share figure delivered in the wrong unit — went through as growth, and the
    chain multiplies it by the member's weight with no bound.

    Measured on ACWI's annual FCF/share, 26,160 accepted steps across 1,712 constituents:

        MITSUBISHI HEAVY  2024->2025      50.78 ->  86,214.52   +169,684%   index +116.12pp
        DENSO CORP        2024->2025     172.97 -> 108,415.57    +62,580%   index  +17.97pp

    On a line indexed to 100, one corrupt cell in a 0.07%-weight constituent more than doubled it.
    """

    def test_mitsubishi_heavy_is_refused(self):
        # scale = its own median |value|, 39.66. The BASE passes comfortably (50.78 >> 3.97);
        # it is the result that cannot have come from a business.
        assert step_growth(50.78, 86214.52, 39.66) is None

    def test_denso_is_refused(self):
        assert step_growth(172.97, 108415.57, 36.22) is None

    def test_the_largest_REAL_step_survives(self):
        """⚠ Bank of America 2008->2009, +3,818%, recovering from the crisis. A ceiling that deletes
        this is deleting history, which is why the bar was read off the distribution rather than
        picked to fit the two bad cells."""
        assert step_growth(0.42, 16.50, 3.17) == pytest.approx(38.286, abs=0.01)

    def test_the_top_of_the_real_distribution_survives(self):
        # p99.99 of the 26,160 measured steps is +6,889%.
        assert step_growth(1.0, 69.89, 1.0) == pytest.approx(68.89)

    def test_the_boundary_is_exactly_100x(self):
        assert step_growth(1.0, 101.0, 1.0) == pytest.approx(100.0)     # at the bar, kept
        assert step_growth(1.0, 101.5, 1.0) is None                     # over it

    def test_it_is_one_sided(self):
        """⚠ There is no matching "too negative" case: the floor at −100% is already the most a
        level can lose, so the downside was never unbounded.

        ⚠ AND THE STEP BACK DOWN OFF A CORRUPT VALUE IS **NOT** CLAMPED — it is −99.74%, a real
        number just short of the floor. That is the residual this ceiling does not fix: refusing the
        step INTO a bad value leaves the value usable as the next step's base. Small here (the
        weight is 0.07%), and named so nobody reads the floor as covering it."""
        assert step_growth(86214.52, 226.63, 39.66) == pytest.approx(-0.99737, abs=1e-5)

    def test_refused_never_capped(self):
        """⚠ A capped step would be a growth rate nobody reported. Refusing means the member sits
        out this one interval and rejoins at the next — the behaviour of every refusal above it."""
        assert step_growth(172.97, 108415.57, 36.22) is None
