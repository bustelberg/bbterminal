"""WHAT `step_growth` STILL REFUSES, NOW THAT THE TWO MAGNITUDE HEURISTICS ARE GONE.

⚠⚠⚠ BOTH WERE REMOVED ON 2026-09-04, ON REQUEST — `_MIN_STEP_BASE_FRACTION` (a member's anchor under
10% of its own median) and `_MAX_STEP_GROWTH` (a step over 100x). What is left is arithmetic: a
ratio needs a positive divisor, and an index that is a product of (1 + g) cannot carry a term below
−1. The evidence behind both constants is preserved in `_fundamental_blend`'s constant block and in
`TestAnImplausibleResultIsNoLongerRefused` below, because the measurements were expensive to make
and the rules may have to come back in some structural form.

WHY THEY WENT, MEASURED THE DAY THEY DID (ACWI's five annual lines):
    * 185 refusals, 180 of them by the base rule, and 44 of THOSE threw away steps that were flat or
      falling or under 2x — `748.588 -> 748.454` (−0.02%) and `748.439 -> 748.439` (exactly zero)
      among them. It never looked at the step, only at the divisor, so a member with a big later
      run-up had its whole early history refused whatever happened in it.
    * The cost was 6.72pp/yr on ACWI's FCF/share and 4.23pp/yr on EPS. Revenue and share price moved
      0.00 and 0.02pp — the rules only ever bound the PER-SHARE lines.
    * And both surfaces computed the bar over their own view of a member, so `Graphs` and `Tables`
      disagreed on exactly the members sitting near it: ACWI FCF/share 18.85% against 18.90%, traced
      to Industrivärden's real 1.087 -> 16.18 recovery out of a one-year trough.

⚠ THE ORIGINAL INCIDENT, KEPT BECAUSE THE FLOOR IS WHAT ACTUALLY ANSWERS IT. Measured 2026-08-13 on
the AEX FCF/share index: Prosus's 0.0090 base (a holding company hovering around break-even) took
its −0.24 next figure to −2,700% growth at a 26% index weight, and the level to −1,456 — invisible,
because a LOG axis simply does not draw a negative point and `connectNulls` runs a confident line
across the hole (AEX annual drew 6 of 10 points, quarterly 26 of 32). The −100% floor is what makes
that impossible now, and it is arithmetic rather than judgement:
`TestTheIndexCannotBeFlippedByOneHolding` still passes with no bar and no ceiling.
"""
from __future__ import annotations

import pytest

from routers._fundamental_blend import blend_series, step_growth

FCF = "annuals__Per Share Data__Free Cash Flow per Share"


def _member(weight: float, points: dict[str, float]) -> dict:
    return {"weight": weight, "points": points, "base_points": {}}


class TestStepGrowthIsTheOneRule:
    def test_a_normal_step_is_the_plain_ratio(self):
        assert step_growth(100.0, 150.0) == pytest.approx(0.5)

    def test_no_anchor_or_no_value_means_no_growth_not_zero_growth(self):
        """It sits out THIS step and joins at the next — a 0.0 would dilute the step toward zero
        as though the member had stood still, which is a different claim."""
        assert step_growth(None, 150.0) is None
        assert step_growth(100.0, None) is None

    def test_a_non_positive_anchor_has_no_ratio(self):
        assert step_growth(0.0, 5.0) is None
        assert step_growth(-3.0, 5.0) is None

    def test_it_is_floored_at_minus_one_hundred_percent(self):
        """⚠ BELOW ZERO THERE IS NO SCALE. An index is a product of (1 + g): a term under −1 does
        not make it small, it makes it NEGATIVE — and a negative index is not a low reading."""
        assert step_growth(2.0, -1.0) == -1.0
        assert step_growth(2.0, -400.0) == -1.0


class TestAOneHoldingBookIsItsCompany:
    """⚠⚠ THE `Tables` TAB MUST SAY WHAT `Graphs` SAYS. The Fundamental modal opens one company as
    `{holdings:[{isin, weight:1}]}` — `Graphs` plots the filed figures directly, `Tables` runs the
    same figures through this blend — so with one member the level has to come out at exactly
    `100 x v(p)/v(base)`, or one modal answers one question twice.

    ⚠ THIS USED TO NEED A SPECIAL CASE AND NO LONGER DOES. The materiality bar compared each
    member's rebased base (100) against `0.10 x median|rebased|`, which fires on ANY member that
    grew more than ~10x from its first period to its median one — growth, not a corrupt divisor.
    Measured 2026-09-03 on NVIDIA as a one-holding book: `price_ps` 13 periods, bar 271 -> ONE point
    drawn; `eps_nri` 18 periods, bar 227 -> ONE. A one-point line has no window, so those rows read
    `—` while `Graphs` drew all thirteen. It was patched by lifting the bar at `members == 1`; the
    bar itself went on 2026-09-04, so the property now holds for the ordinary reason — there is no
    rule left that can refuse an arithmetically valid step.
    """

    # NVIDIA's real `price_ps` by fiscal year (local DB, 2026-09-03), the case that was empty.
    NVDA_PRICE = {
        "2015-01-31": 0.48, "2016-01-31": 0.732, "2017-01-31": 2.73, "2018-01-31": 6.145,
        "2019-01-31": 3.594, "2020-01-31": 5.911, "2021-01-31": 12.99, "2022-01-31": 24.486,
        "2023-01-31": 19.537, "2024-01-31": 61.527, "2025-01-31": 120.07, "2026-01-31": 191.13,
    }

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


class TestAnImplausibleResultIsNoLongerRefused:
    """⚠⚠⚠ THE CEILING WAS REMOVED ON 2026-09-04, ON REQUEST — this class is kept as the record of
    what it caught, so the evidence survives the rule. A vendor scale error — a per-share figure
    delivered in the wrong unit — now goes through as growth, and the chain multiplies it by the
    member's weight with no bound.

    Measured on ACWI's annual FCF/share, 26,160 accepted steps across 1,712 constituents:

        MITSUBISHI HEAVY  2024->2025      50.78 ->  86,214.52   +169,684%   index +116.12pp
        DENSO CORP        2024->2025     172.97 -> 108,415.57    +62,580%   index  +17.97pp

    On a line indexed to 100, one corrupt cell in a 0.07%-weight constituent more than doubled it.
    """

    def test_mitsubishi_heavy_now_reaches_the_line(self):
        """⚠⚠ IT IS REPORTED, NOT REFUSED — the ceiling was removed on 2026-09-04, on request, with
        `_MIN_STEP_BASE_FRACTION`. Almost certainly a vendor scale error, and it now shows up as an
        absurd number on the chart rather than as a member that silently sat out one interval. That
        is the agreed trade: a figure is reported as filed, and catching THIS belongs in a
        structural test on the share count, which is where the 1,000x break actually is."""
        assert step_growth(50.78, 86214.52) == pytest.approx(1696.80, abs=0.01)

    def test_denso_too(self):
        assert step_growth(172.97, 108415.57) == pytest.approx(625.788, abs=0.01)

    def test_the_largest_REAL_step_survives(self):
        """⚠ Bank of America 2008->2009, +3,818%, recovering from the crisis. A ceiling that deletes
        this is deleting history, which is why the bar was read off the distribution rather than
        picked to fit the two bad cells."""
        assert step_growth(0.42, 16.50) == pytest.approx(38.286, abs=0.01)

    def test_the_top_of_the_real_distribution_survives(self):
        # p99.99 of the 26,160 measured steps is +6,889%.
        assert step_growth(1.0, 69.89) == pytest.approx(68.89)

    def test_there_is_no_boundary_left(self):
        """The old ceiling sat at exactly 100x, read off the gap between the top of the real
        distribution (p99.99 = +6,889%) and the bottom of the corrupt one (+10,097%). Nothing is
        refused for being large now."""
        assert step_growth(1.0, 101.0) == pytest.approx(100.0)
        assert step_growth(1.0, 101.5) == pytest.approx(100.5)
        assert step_growth(1.0, 1e6) == pytest.approx(999999.0)

    def test_it_is_one_sided(self):
        """⚠ There is no matching "too negative" case: the floor at −100% is already the most a
        level can lose, so the downside was never unbounded.

        ⚠ AND THE STEP BACK DOWN OFF A CORRUPT VALUE IS **NOT** CLAMPED — it is −99.74%, a real
        number just short of the floor. That is the residual this ceiling does not fix: refusing the
        step INTO a bad value leaves the value usable as the next step's base. Small here (the
        weight is 0.07%), and named so nobody reads the floor as covering it."""
        assert step_growth(86214.52, 226.63) == pytest.approx(-0.99737, abs=1e-5)

    def test_the_upside_is_never_capped(self):
        """⚠ IT WAS NEVER CAPPED, ONLY REFUSED, AND NOW IT IS NEITHER. A capped step would be a
        growth rate nobody reported — that principle survives the ceiling's removal, which is why
        an enormous filed step comes through at its filed size rather than clamped to some
        maximum."""
        assert step_growth(172.97, 108415.57) == pytest.approx(625.788, abs=0.01)
        assert step_growth(1.0, 5000.0) == pytest.approx(4999.0)
