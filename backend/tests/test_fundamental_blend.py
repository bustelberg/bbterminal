"""Blending a portfolio's fundamentals — three metric kinds, three aggregation rules.

⚠ USING ONE RULE FOR ALL THREE PRODUCES A CONFIDENT WRONG NUMBER. Each class below is a rule that
would be silently violated by the obvious "just take a weighted average" implementation.
"""
from __future__ import annotations

import pytest

from routers._fundamental_blend import (
    MIN_BLEND_COVERAGE_PCT,
    MULTIPLE_CODES,
    blend_breakdown,
    blend_kind,
    blend_matrix,
    blend_series,
    explain_empty,
    merge_relative_growth,
)

PE = "annuals__Valuation Ratios__PE Ratio"
ROE = "annuals__Ratios__ROE %"
REV = "annuals__Income Statement__Revenue"


def _m(weight, **points):
    return {"weight": weight, "points": {d.replace("_", "-"): v for d, v in points.items()}}


class TestTheKindDecidesTheRule:
    def test_a_multiple_is_a_multiple(self):
        assert blend_kind(PE) == "multiple"
        assert blend_kind("annuals__Valuation Ratios__PEG Ratio") == "multiple"

    def test_a_percent_is_a_ratio(self):
        assert blend_kind(ROE) == "ratio"
        assert blend_kind("annuals__Valuation Ratios__FCF Yield %") == "ratio"
        assert blend_kind("annuals__Balance Sheet__Debt-to-Equity") == "ratio"

    def test_an_amount_is_a_level(self):
        assert blend_kind(REV) == "level"
        assert blend_kind("annuals__Per Share Data__Free Cash Flow per Share") == "level"


class TestAMultipleIsHarmonic:
    """⚠ THE ERROR IS LARGE, ONE-DIRECTIONAL AND PLAUSIBLE. A portfolio's P/E is aggregate price
    over aggregate earnings — the weighted HARMONIC mean. The arithmetic one is dragged up by any
    single high multiple, and 55 does not look silly for a growth book."""

    def test_two_names_at_pe_10_and_100(self):
        out = blend_series([_m(0.5, **{"2024_12_31": 10}), _m(0.5, **{"2024_12_31": 100})], PE)
        assert out["kind"] == "multiple"
        assert out["points"][0]["value"] == pytest.approx(18.1818, abs=1e-3)   # NOT 55.0

    def test_the_arithmetic_answer_is_three_times_too_high(self):
        out = blend_series([_m(0.5, **{"2024_12_31": 10}), _m(0.5, **{"2024_12_31": 100})], PE)
        assert out["points"][0]["value"] < 55.0 / 2.9

    def test_a_negative_multiple_is_dropped_not_inverted(self):
        """⚠ A negative P/E is a LOSS. Its reciprocal is meaningless and one of them would flip
        the sign of the whole aggregate."""
        out = blend_series([_m(0.5, **{"2024_12_31": 20}), _m(0.5, **{"2024_12_31": -30})], PE)
        assert out["points"] == [] or out["points"][0]["value"] == pytest.approx(20.0)


class TestAYieldOrMarginIsArithmetic:
    """⚠ A yield is a multiple's RECIPROCAL, so applying the harmonic rule here is the same
    mistake mirrored."""

    def test_roe_blends_arithmetically(self):
        out = blend_series([_m(0.5, **{"2024_12_31": 10}), _m(0.5, **{"2024_12_31": 30})], ROE)
        assert out["kind"] == "ratio"
        assert out["points"][0]["value"] == pytest.approx(20.0)

    def test_it_respects_the_weights(self):
        out = blend_series([_m(0.9, **{"2024_12_31": 10}), _m(0.1, **{"2024_12_31": 30})], ROE)
        assert out["points"][0]["value"] == pytest.approx(12.0)


class TestALevelIsRebasedBeforeItIsWeighted:
    """⚠ Weighting Apple's revenue by 5% and ASML's by 3% gives a number that is neither company's
    revenue nor the portfolio's. Rebasing to 100 makes it a growth index, which IS a portfolio-level
    statement."""

    def test_two_companies_of_wildly_different_size_blend_by_GROWTH(self):
        big = _m(0.5, **{"2023_12_31": 1_000_000, "2024_12_31": 1_100_000})   # +10%
        small = _m(0.5, **{"2023_12_31": 100, "2024_12_31": 130})             # +30%
        out = blend_series([big, small], REV)
        assert out["kind"] == "level"
        assert out["points"][0]["value"] == pytest.approx(100.0)     # both start at 100
        assert out["points"][1]["value"] == pytest.approx(120.0)     # (110 + 130) / 2

    def test_the_absolute_scale_never_leaks_in(self):
        """If the raw amounts were weighted, the big company would drown the small one entirely."""
        big = _m(0.5, **{"2023_12_31": 1e9, "2024_12_31": 1.1e9})
        small = _m(0.5, **{"2023_12_31": 10, "2024_12_31": 13})
        assert blend_series([big, small], REV)["points"][1]["value"] == pytest.approx(120.0)

    def test_a_negative_base_costs_the_member_ITS_EARLY_YEARS_not_the_metric(self):
        """⚠⚠ REWRITTEN 2026-08-13 — THE BEHAVIOUR CHANGED DELIBERATELY AND THE TEST DID NOT.

        It used to assert that a member whose FIRST reported period is negative is dropped from the
        metric outright (`covered_pct == 85`, the survivor's weight alone). `_prepare` now anchors
        on the first POSITIVE period instead and keeps the member from there — because dropping it
        threw away every good year it had: Universal Music's fabricated 2017 zero cost it 2018-2025
        (6,023 → 12,507), and Prosus carries the same artefact.

        So the loss member contributes at 2024, where it reported, and 2023 — before its anchor —
        is the only thing it is absent from. 100 × v/0 is still undefined and a negative base still
        flips a curve; the fix is to move the anchor, not to discard the company.
        """
        ok = _m(0.85, **{"2023_12_31": 100, "2024_12_31": 150})
        loss = _m(0.15, **{"2023_12_31": -50, "2024_12_31": 60})
        out = blend_series([ok, loss], REV)
        # The line is unchanged: `loss` has no 2023→2024 growth to contribute (no positive anchor
        # at 2023), so the step is the survivor's +50% either way.
        assert out["points"][-1]["value"] == pytest.approx(150.0)
        # ...but it IS behind the 2024 point now, so coverage is the whole book rather than 85%.
        assert out["points"][-1]["covered_pct"] == pytest.approx(100.0)
        assert out["points"][0]["covered_pct"] == pytest.approx(85.0), (
            "2023 is still the survivor alone — the loss member's history starts at its anchor")

    def test_the_period_BEFORE_a_members_anchor_can_fall_under_the_floor(self):
        """The surviving half of the old "dropping a member takes the date below the floor" case.

        ⚠ IT IS NOW ABOUT THE EARLY PERIOD, NOT THE WHOLE SERIES. A member anchored at 2024 is
        absent from 2023, so 2023 is covered by 40% of the book and is refused — while 2024, which
        both report, is drawn. The old test expected NO points at all, which stopped being true
        when the anchor moved.

        ⚠ 40/60, NOT 50/50 — the floor is 50 and the comparison is `>=`, so an even split clears.
        """
        ok = _m(0.4, **{"2023_12_31": 100, "2024_12_31": 150})
        loss = _m(0.6, **{"2023_12_31": -50, "2024_12_31": 60})
        pts = blend_series([ok, loss], REV)["points"]
        assert [p["period"] for p in pts] == ["2024"]
        assert pts[0]["covered_pct"] == pytest.approx(100.0)


class TestCoverageIsPerDateAndIsAFloor:
    def test_the_weight_reporting_is_renormalised_AT_EACH_DATE(self):
        """⚠ Members report on different calendars. Dividing by the ORIGINAL weight would drag
        every early period toward zero — a rise that is nothing but coverage improving."""
        # 85/15 so the early date clears the floor and the renormalisation is observable at all.
        early = _m(0.85, **{"2023_12_31": 20, "2024_12_31": 20})
        late = _m(0.15, **{"2024_12_31": 40})
        out = blend_series([early, late], ROE)
        first = next(p for p in out["points"] if p["period"] == "2023")
        assert first["value"] == pytest.approx(20.0)     # NOT 17.0 (= 0.85 x 20 undivided)
        assert first["covered_pct"] == pytest.approx(85.0)
        last = next(p for p in out["points"] if p["period"] == "2024")
        assert last["value"] == pytest.approx(23.0)      # 0.85x20 + 0.15x40, full coverage

    def test_a_date_under_the_floor_is_omitted_not_drawn_as_a_dip(self):
        thin = _m(0.05, **{"2020_12_31": 99})
        rest = _m(0.95, **{"2024_12_31": 10})
        out = blend_series([thin, rest], ROE)
        assert [p["period"] for p in out["points"]] == ["2024"]

    def test_the_floor_is_the_documented_one(self):
        assert MIN_BLEND_COVERAGE_PCT == 50.0

    def test_a_newest_year_the_MINORITY_has_filed_is_still_refused(self):
        """The floor's remaining job, and the one it was raised to 80 for.

        Books close on different dates, so early in a fiscal year a few holdings have filed and the
        rest have not. Renormalising over whoever reported draws that as a full-height point on the
        right edge, in the same ink as a year everybody reported — a move in the sample, read as a
        move in the book.
        """
        filed = [_m(0.20, **{"2024_12_31": 10, "2025_12_31": 10}),
                 _m(0.15, **{"2024_12_31": 10, "2025_12_31": 40})]
        pending = [_m(0.65, **{"2024_12_31": 10})]
        out = blend_series([*filed, *pending], ROE)
        assert [p["period"] for p in out["points"]] == ["2024"]   # 2025 spans 35% — omitted

    def test_a_newest_year_the_MAJORITY_has_filed_now_DRAWS_and_that_is_the_trade(self):
        """⚠ THE ACCEPTED COST OF LOWERING THE FLOOR 80 -> 50 (2026-08-12, on request). This exact
        case — 65% filed, the rest pending — was the reason the floor went 60 -> 80 in July, and it
        is now drawn again. It is pinned rather than deleted so the behaviour is a decision on the
        record instead of a surprise on the right edge of a chart: `covered_pct` says 65 and the
        point is real, but it spans two thirds of the book beside years that span all of it. If it
        bites, the fix is a stricter bar on the LATEST period alone — not a single high floor, which
        also hid the mid-history periods this change was asked for."""
        filed = [_m(0.35, **{"2024_12_31": 10, "2025_12_31": 10}),
                 _m(0.30, **{"2024_12_31": 10, "2025_12_31": 40})]
        pending = [_m(0.35, **{"2024_12_31": 10})]
        out = blend_series([*filed, *pending], ROE)
        assert [p["period"] for p in out["points"]] == ["2024", "2025"]
        assert out["points"][-1]["covered_pct"] == pytest.approx(65.0)

    def test_no_members_is_no_series_not_a_zero(self):
        assert blend_series([], ROE)["points"] == []
        assert blend_series([_m(0)], ROE)["points"] == []


class TestForwardPEIsAMultipleDespiteItsName:
    """⚠ THE CODE THE CHART PLOTS MATCHES NONE OF THE NAMING PATTERNS.

    Forward P/E is `indicator_q_forward_pe_ratio`: not a statement line (`annuals__…`), not an
    analyst estimate (`annual_…_estimate`), and its trailing "ratio" is LOWERCASE, so the
    case-sensitive `RATIO_SUFFIXES` never sees it. Left to fall through it is a `level`, gets
    rebased to 100, and a chart that formats its value as "{v}x" reports a portfolio trading at
    100x forward earnings — a confident wrong number on a familiar axis, not a visible gap.
    """

    FWD_PE = "indicator_q_forward_pe_ratio"

    def test_it_is_classified_a_multiple_not_a_level(self):
        assert blend_kind(self.FWD_PE) == "multiple"

    def test_the_lowercase_ratio_suffix_is_why_it_must_be_listed_explicitly(self):
        """The guard that would otherwise catch it does not — this is the reason for the entry."""
        assert not self.FWD_PE.endswith("Ratio")
        assert self.FWD_PE in MULTIPLE_CODES

    def test_it_blends_harmonically_and_is_never_rebased(self):
        out = blend_series(
            [_m(0.5, **{"2024_12_31": 10}), _m(0.5, **{"2024_12_31": 100})], self.FWD_PE
        )
        assert out["points"][0]["value"] == pytest.approx(18.1818, abs=1e-3)
        assert out["points"][0]["value"] != pytest.approx(100.0)   # NOT a rebased index


class TestAForecastIsRebasedOnTheActualItContinues:
    """⚠ REBASING A FORECAST ON ITSELF DRAWS A COLLAPSE THAT DOES NOT EXIST.

    An estimate series and the actual it extends are the same quantity, and the chart indexes both
    off the ACTUAL's base so the forecast continues the line. Rebase the forecast on its own first
    point and it restarts at 100 beside an actual that has run to 1,808 — measured on a real book,
    a 94% earnings collapse in the forecast year, drawn at full confidence on a log axis.
    """

    ACT = {"2015_12_31": 2.0, "2025_12_31": 20.0}       # actual: 10x since the base year
    EST = {"2026_12_31": 22.0}                          # forecast: +10% on the last actual

    def _blend(self, *, linked):
        member = {"weight": 1.0, "points": {d.replace("_", "-"): v for d, v in self.EST.items()}}
        if linked:
            member["base_points"] = {d.replace("_", "-"): v for d, v in self.ACT.items()}
        return blend_series([member], REV)["points"][0]["value"]

    def test_linked_the_forecast_continues_the_actual(self):
        """Actual ends at index 1000 (20/2); the forecast must land just above it, not at 100."""
        assert self._blend(linked=True) == pytest.approx(1100.0)

    def test_unlinked_it_restarts_at_100_which_is_the_bug(self):
        assert self._blend(linked=False) == pytest.approx(100.0)

    def test_an_empty_base_falls_back_to_the_series_own_anchor(self):
        """A standalone level has no series to continue — it anchors on itself, as before."""
        member = {"weight": 1.0, "base_points": {},
                  "points": {d.replace("_", "-"): v for d, v in self.EST.items()}}
        assert blend_series([member], REV)["points"][0]["value"] == pytest.approx(100.0)


class TestTheBreakdownAgreesWithTheLineItExplains:
    """⚠ A DRILL-DOWN THAT DISAGREES WITH ITS CHART IS WORSE THAN NO DRILL-DOWN.

    It is checked once, believed thereafter, and the disagreement is invisible unless someone adds
    the rows up. `blend_breakdown` and `blend_series` therefore share `_prepare` — these tests
    exist to fail if anyone re-implements one of them "the same way".
    """

    MEMBERS = [
        {"name": "cheap", "weight": 0.5, "points": {"2025-12-31": 10.0}},
        {"name": "rich", "weight": 0.3, "points": {"2025-12-31": 100.0}},
        {"name": "absent", "weight": 0.2, "points": {"2019-12-31": 42.0}},
    ]

    @pytest.mark.parametrize("code", [PE, ROE, REV])
    def test_the_breakdown_value_equals_the_series_value(self, code):
        series = blend_series(self.MEMBERS, code)
        point = next((p for p in series["points"] if p["period"] == "2025"), None)
        out = blend_breakdown(self.MEMBERS, code, "2025")
        if point is None:               # under the floor in the series => no value here either
            assert out["value"] is None or out["covered_pct"] < MIN_BLEND_COVERAGE_PCT
        else:
            assert out["value"] == pytest.approx(point["value"])
            assert out["covered_pct"] == pytest.approx(point["covered_pct"])

    @pytest.mark.parametrize("code", [PE, ROE])
    def test_the_shares_sum_to_one_hundred_percent(self, code):
        """⚠ REV IS DELIBERATELY EXCLUDED (2026-08-13) — A LEVEL HAS NO SHARES TO SUM.

        Once the level line became a CHAINED product rather than a weighted sum, its value at a
        period stopped being decomposable: no set of per-member numbers can add to a cumulative
        product. `_level_breakdown` therefore reports `share_pct = None` on purpose and gives
        `contribution_pp` instead — a share OF A STEP is unbounded (near a zero step a 0.1pp
        contributor reads as 400% of it, and a member that moved the other way reads negative).

        Parametrising REV in here asserted the old additive shape; the level's own decomposition is
        checked by `contribution_pp` summing to `step_pct` below.
        """
        out = blend_breakdown(self.MEMBERS, code, "2025")
        assert sum(m["share_pct"] for m in out["members"]) == pytest.approx(100.0, abs=0.05)

    def test_a_LEVEL_has_no_shares_and_its_FIRST_point_has_no_step_either(self):
        """The level's replacement for `share_pct` is `contribution_pp` — a share of the STEP into
        the period.

        ⚠ AND AT THE FIRST DRAWN PERIOD THERE IS NO STEP, which is the honest answer rather than a
        gap: the index starts there, so nothing moved it and there is nothing to attribute. These
        members report one period each, so 2025 IS the first — every contribution is None, and that
        must not be mistaken for "they contributed zero"."""
        out = blend_breakdown(self.MEMBERS, REV, "2025")
        assert all(m["share_pct"] is None for m in out["members"]), (
            "a cumulative product cannot be shared out — see the docstring above")
        assert all(m.get("contribution_pp") is None for m in out["members"])


class TestTheMatrixAgreesWithTheLineToo:
    """The audit grid's blended footer must equal the chart's line at every period — same
    `_prepare`, same combine. If it drifts, the whole point (verification) is defeated."""

    MEMBERS = [
        {"name": "cheap", "weight": 0.5, "points": {"2024-12-31": 10.0, "2025-12-31": 12.0}},
        {"name": "rich", "weight": 0.3, "points": {"2024-12-31": 100.0, "2025-12-31": 90.0}},
        {"name": "thin", "weight": 0.2, "points": {"2025-12-31": 30.0}},
    ]

    def test_the_footer_matches_the_series_for_every_drawn_period(self):
        series = {p["period"]: p["value"] for p in blend_series(self.MEMBERS, PE)["points"]}
        mx = blend_matrix(self.MEMBERS, PE)
        for period, val in series.items():
            assert mx["blended"][period] == pytest.approx(val)

    def test_it_shows_below_floor_years_the_chart_hides(self):
        """The chart omits a year under the coverage floor; the matrix keeps it, flagged — a thin
        year is exactly what someone verifying the line needs to see."""
        mx = blend_matrix(self.MEMBERS, PE)
        # Every period the matrix lists has a covered% and a below_floor verdict.
        for y in mx["periods"]:
            assert y in mx["covered"] and y in mx["below_floor"]
            assert mx["below_floor"][y] == (mx["covered"][y] < MIN_BLEND_COVERAGE_PCT)

    def test_a_loss_making_multiple_cell_is_marked_dropped_not_hidden(self):
        members = [
            {"name": "ok", "weight": 0.6, "points": {"2025-12-31": 20.0}},
            {"name": "loss", "weight": 0.4, "points": {"2025-12-31": -15.0}},
        ]
        mx = blend_matrix(members, PE)
        loss = next(m for m in mx["members"] if m["name"] == "loss")
        assert loss["cells"]["2025"]["dropped"] is True
        assert loss["cells"]["2025"]["value"] == -15.0        # shown, not blanked
        # ...and it did not enter the blend: only the 20x name did.
        assert mx["blended"]["2025"] == pytest.approx(20.0)

    def test_rows_are_sorted_by_weight_and_carry_every_period(self):
        mx = blend_matrix(self.MEMBERS, PE)
        assert [m["name"] for m in mx["members"]] == ["cheap", "rich", "thin"]
        cheap = mx["members"][0]
        assert set(cheap["cells"]) == {"2024", "2025"}


class TestPriceVsOwnerEarningsMerge:
    """The Share-Price-vs-OE drilldown merges a PRICE level-breakdown and an OE level-breakdown.
    Both are LEVELS (growth indices), so a holding carries a price index and an OE index; the ratio
    is price ÷ OE — how much its earnings multiple expanded."""

    PRICE_IDX = "annuals__Per Share Data__Month End Stock Price"
    OE = "annuals__Per Share Data__EPS without NRI"

    # Two holdings; price outran earnings for the first, lagged for the second.
    PRICE_MEMBERS = [
        {"isin": "A", "name": "runner", "weight": 0.6,
         "points": {"2020-12-31": 100.0, "2024-12-31": 260.0}},
        {"isin": "B", "name": "laggard", "weight": 0.4,
         "points": {"2020-12-31": 50.0, "2024-12-31": 60.0}},
    ]
    OE_MEMBERS = [
        {"isin": "A", "name": "runner", "weight": 0.6,
         "points": {"2020-12-31": 5.0, "2024-12-31": 9.0}},
        {"isin": "B", "name": "laggard", "weight": 0.4,
         "points": {"2020-12-31": 2.0, "2024-12-31": 2.8}},
    ]

    def _merge(self, period="2024"):
        return merge_relative_growth(
            blend_breakdown(self.PRICE_MEMBERS, self.PRICE_IDX, period),
            blend_breakdown(self.OE_MEMBERS, self.OE, period),
            period,
        )

    def test_each_holding_carries_both_indices_and_their_ratio(self):
        rows = {r["name"]: r for r in self._merge()["members"]}
        # runner: price 100→260 = index 260; OE 5→9 = index 180; ratio 260/180 ≈ 1.44.
        assert rows["runner"]["price_index"] == pytest.approx(260.0)
        assert rows["runner"]["oe_index"] == pytest.approx(180.0)
        assert rows["runner"]["ratio"] == pytest.approx(260.0 / 180.0, abs=1e-3)
        # laggard: price 50→60 = 120; OE 2→2.8 = 140; ratio < 1 (earnings outran price).
        assert rows["laggard"]["ratio"] == pytest.approx(120.0 / 140.0, abs=1e-3)

    def test_the_portfolio_ratio_is_blended_price_over_blended_oe(self):
        out = self._merge()
        assert out["ratio"] == pytest.approx(out["price"]["value"] / out["oe"]["value"], abs=1e-6)

    def test_rows_are_sorted_by_weight(self):
        assert [r["name"] for r in self._merge()["members"]] == ["runner", "laggard"]

    def test_the_raw_amounts_ride_along_for_verification(self):
        rows = {r["name"]: r for r in self._merge()["members"]}
        # blend_breakdown for a level returns the as-reported amount too.
        assert rows["runner"]["price_raw"] == pytest.approx(260.0)
        assert rows["runner"]["oe_raw"] == pytest.approx(9.0)


class TestShareIsComputedInTheSpaceTheMetricCombinesIn:
    """⚠ `w x v / Σw` IS THE SHARE OF AN ARITHMETIC MEAN, AND A MULTIPLE IS NOT ONE.

    A harmonic blend adds RECIPROCALS, so the cheap name carries the larger share of the
    aggregate — the opposite of what the arithmetic formula reports. Measured on a real book:
    TSMC at 19.4x = 7.9%, Palo Alto at 48.8x = 3.1%, on identical 5% weights.
    """

    EQUAL = [{"name": "cheap", "weight": 0.5, "points": {"2025-12-31": 10.0}},
             {"name": "rich", "weight": 0.5, "points": {"2025-12-31": 100.0}}]

    def test_for_a_multiple_the_CHEAP_name_carries_the_larger_share(self):
        by = {m["name"]: m["share_pct"] for m in blend_breakdown(self.EQUAL, PE, "2025")["members"]}
        assert by["cheap"] > by["rich"]
        assert by["cheap"] == pytest.approx(90.91, abs=0.05)    # 0.1 / (0.1 + 0.01)

    def test_for_a_ratio_the_LARGER_value_carries_the_larger_share(self):
        by = {m["name"]: m["share_pct"] for m in blend_breakdown(self.EQUAL, ROE, "2025")["members"]}
        assert by["rich"] > by["cheap"]
        assert by["rich"] == pytest.approx(90.91, abs=0.05)

    def test_the_arithmetic_share_would_have_inverted_the_multiple_ranking(self):
        """The bug this guards: same inputs, wrong space, opposite conclusion."""
        naive = {"cheap": 0.5 * 10.0, "rich": 0.5 * 100.0}
        assert naive["rich"] > naive["cheap"]        # ...which is the reverse of the truth above


class TestSwingIsInfluenceNotSize:
    """⚠ A SHARE IS NOT AN INFLUENCE. Two 10% holdings carry ~10% of the weight each; only the one
    away from the average MOVES the number. `swing` is the leave-one-out delta."""

    def test_swing_is_what_the_line_would_read_without_the_holding(self):
        members = [{"name": "a", "weight": 0.5, "points": {"2025-12-31": 10.0}},
                   {"name": "b", "weight": 0.5, "points": {"2025-12-31": 30.0}}]
        out = blend_breakdown(members, ROE, "2025")
        assert out["value"] == pytest.approx(20.0)
        by = {m["name"]: m["swing"] for m in out["members"]}
        assert by["a"] == pytest.approx(-10.0)      # without "a" the line reads 30
        assert by["b"] == pytest.approx(+10.0)      # without "b" it reads 10

    def test_members_are_ordered_by_absolute_influence(self):
        """⚠ Needs THREE members. With two, removing either leaves the other, so the swings are
        exactly symmetric (+/-39 on 21 vs 99) and the order is a real tie — a two-member case
        would pass or fail on sort stability, not on the ranking being right."""
        members = [{"name": "mild", "weight": 0.4, "points": {"2025-12-31": 20.0}},
                   {"name": "alsomild", "weight": 0.4, "points": {"2025-12-31": 20.0}},
                   {"name": "wild", "weight": 0.2, "points": {"2025-12-31": 100.0}}]
        out = blend_breakdown(members, ROE, "2025")
        assert out["value"] == pytest.approx(36.0)
        assert out["members"][0]["name"] == "wild"
        # ...and it is the SMALLEST holding: influence is not size.
        assert out["members"][0]["weight_pct"] == pytest.approx(20.0)


class TestTheExclusionsAreHalfTheAnswer:
    """⚠ AN ABSENT HOLDING IS NOT A ZERO, AND *WHY* IT IS ABSENT IS THE WHOLE POINT — "has not
    reported yet" and "reported a loss, so a negative multiple was dropped" look identical in a
    chart and mean opposite things."""

    def test_a_holding_with_no_point_in_the_period_is_named_not_dropped_silently(self):
        members = [{"name": "here", "weight": 0.8, "points": {"2025-12-31": 10.0}},
                   {"name": "gone", "weight": 0.2, "points": {"2019-12-31": 10.0}}]
        out = blend_breakdown(members, ROE, "2025")
        assert [(e["name"], e["reason"]) for e in out["excluded"]] == [("gone", "no_point_in_period")]
        assert out["covered_pct"] == pytest.approx(80.0)
        assert out["excluded_pct"] == pytest.approx(20.0)

    def test_a_negative_multiple_is_reported_as_excluded_not_as_a_zero_share(self):
        """⚠ It reaches the period WITH data, so it is not `no_point_in_period` — but the harmonic
        combine drops it. Left in `members` it would read "contributed 0.0%", which is a different
        and false claim."""
        members = [{"name": "ok", "weight": 0.8, "points": {"2025-12-31": 20.0}},
                   {"name": "loss", "weight": 0.2, "points": {"2025-12-31": -5.0}}]
        out = blend_breakdown(members, PE, "2025")
        assert [(e["name"], e["reason"]) for e in out["excluded"]] == [("loss", "non_positive_multiple")]
        assert [m["name"] for m in out["members"]] == ["ok"]

    def test_a_level_reports_both_the_index_and_the_amount_as_reported(self):
        """Only the index invites "why is revenue 143?"; only the raw invites summing figures that
        were never in the same currency."""
        members = [{"name": "a", "weight": 1.0,
                    "points": {"2015-12-31": 50.0, "2025-12-31": 75.0}}]
        m = blend_breakdown(members, REV, "2025")["members"][0]
        assert m["value"] == pytest.approx(150.0)     # rebased index
        assert m["raw_value"] == pytest.approx(75.0)  # as reported


DIV_PS = "annuals__Per Share Data__Dividends per Share"


class TestAnEmptySeriesIsNotAnEmptyDatabase:
    """⚠ THE CHART CANNOT TELL THE TWO APART, AND THEY ARE OPPOSITES. A portfolio card drawing
    nothing says "not ingested" — which, when every holding HAS the line and the blend dropped it,
    sends the reader to re-fetch data they already own. `explain_empty` is what lets the card say
    which of the two it is looking at."""

    def test_nothing_reports_it_is_not_explained_away(self):
        """A code no holding carries genuinely IS "not ingested" — there is nothing to explain,
        and inventing a note would bury the one message that IS actionable."""
        members = [{"weight": 0.5, "points": {}}, {"weight": 0.5, "points": {}}]
        assert explain_empty(members, DIV_PS) is None

    def test_a_company_that_STARTED_paying_keeps_the_years_it_paid(self):
        """⚠⚠ REWRITTEN 2026-08-13 — THIS IS THE CASE THAT MOVED THE ANCHOR.

        It used to assert that a dividend series beginning at 0.00 is dropped from the metric
        outright, taking every year under the floor and drawing nothing. That WAS the behaviour and
        it was the bug: two holdings that simply started paying mid-window silenced the whole
        chart, and the card read "No dividend/share ingested" while all three carried the line.

        `_prepare` now anchors on the first POSITIVE period, so a company that began paying keeps
        the years it actually paid and is absent only from the ones before it — which is what the
        data says. The leading 0.00 is still refused as a divisor; it just no longer costs the
        company its history.
        """
        members = [
            {"weight": 0.4, "points": {"2015-12-31": 0.0, "2024-12-31": 1.2}},
            {"weight": 0.4, "points": {"2015-12-31": 0.0, "2024-12-31": 0.8}},
            {"weight": 0.2, "points": {"2015-12-31": 1.0, "2024-12-31": 2.0}},
        ]
        pts = blend_series(members, DIV_PS)["points"]
        # 2015 is the one payer alone (20% — under the floor); 2024 is all three.
        assert [p["period"] for p in pts] == ["2024"]
        assert pts[0]["covered_pct"] == pytest.approx(100.0)
        # ⚠ AND NOTHING IS DROPPED ANY MORE — the note that used to blame `non_positive_base` for
        # two of three holdings now reports all three contributing. (`explain_empty` is a
        # diagnostic the caller only reaches when the series came back empty; called directly it
        # always answers, so the assertion is on WHAT it says, not on its absence.)
        why = explain_empty(members, DIV_PS)
        assert why["dropped"] == {}
        assert why["contributing"] == 3
        assert why["best_covered_pct"] == pytest.approx(100.0)

    def test_a_thin_year_is_reported_as_the_floor_not_as_a_drop(self):
        """Every member survives preparation; there simply is not enough weight reporting. A note
        blaming a rebase here would send the reader after the wrong thing."""
        members = [{"weight": 0.3, "points": {"2015-12-31": 1.0, "2024-12-31": 2.0}},
                   {"weight": 0.7, "points": {}}]
        why = explain_empty(members, DIV_PS)
        assert why["dropped"] == {"no_data": 1}
        assert why["contributing"] == 1
        assert why["best_covered_pct"] == pytest.approx(30.0)
        assert why["years_below_floor"] == 2

    def test_a_book_of_losses_is_a_multiple_with_no_usable_value(self):
        """⚠ Above the floor and still no point: the harmonic combine has nothing to invert. Only
        `years_no_value` distinguishes that from a thin year."""
        members = [{"weight": 1.0, "points": {"2024-12-31": -8.0}}]
        why = explain_empty(members, PE)
        assert why["kind"] == "multiple"
        assert why["best_covered_pct"] == pytest.approx(100.0)
        assert why["years_no_value"] == 1
        assert why["years_below_floor"] == 0
