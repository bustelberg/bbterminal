"""The three rules a blended period lives by, and why they only work together.

    1. WEIGHT — a member's weight in a period is its cap for that YEAR over the sum of that year's
       caps, taken as-of when this year is not filed yet.
    2. CARRY  — a member's latest reported figure applies until it reports a newer one, bounded to
       ~a year.
    3. FLOOR  — a period draws only when at least half the WEIGHT and half the NAMES reported it,
       and a CARRIED value counts toward neither.

⚠⚠ RULE 3'S "CARRIED COUNTS FOR NOTHING" IS WHAT MAKES RULE 2 SAFE. Carry the values and count
them as coverage and the newest fiscal year — a handful of filers, everyone else held at last
year's figure — reads 100% covered, sails past the floor, and draws a flat line of stale numbers.
Kept apart, the line is smooth AND the newest year is still refused.

Measured 2026-08-12 on the AEX quarterly revenue benchmark: without the carry the index alternated
between the 12 constituents that file quarterly and the 21 that file at Jun/Dec —
277 → 341 → 297 → 382 → 338 → 402, a ±20% sawtooth of composition, not revenue. With all three
rules: 295.6 → 296.7 → 304.3 → 317.3 → 347.8 → 359.8 → 364.3 → 368.4 → 392.2.

Pure — no DB, no network.
"""
from __future__ import annotations

import pytest

from routers._fundamental_blend import (
    MIN_BLEND_COVERAGE_NAMES_PCT,
    MIN_BLEND_COVERAGE_PCT,
    blend_series,
    carry_forward,
    quarter_bucket,
    year_bucket,
)

ROE = "annuals__Ratios__ROE %"            # a RATIO: weighted arithmetically, no rebasing
CAPS_2024_25 = {f"{y}-Q{q}": 100.0 for y in ("2024", "2025") for q in (1, 2, 3, 4)}

QUARTERLY_FILER = {"2024-03-31": 10.0, "2024-06-30": 10.0, "2024-09-30": 10.0,
                   "2024-12-31": 10.0, "2025-03-31": 20.0}
SEMI_FILER = {"2024-06-30": 30.0, "2024-12-31": 30.0}


def member(weight, points, weights=None):
    m = {"weight": weight, "points": points}
    if weights is not None:
        m["weights"] = weights
    return m


class TestTheCarryKeepsTheBasketStable:
    def test_a_semi_annual_filer_contributes_to_every_quarter(self):
        out = blend_series([member(1.0, QUARTERLY_FILER, CAPS_2024_25),
                            member(1.0, SEMI_FILER, CAPS_2024_25)], ROE, quarter_bucket)
        got = {p["period"]: p["value"] for p in out["points"]}
        # ⚠ Q3 is the case: only the quarterly filer reported (10), the semi-annual one is carried
        # at its June figure (30) — so the average stays over BOTH, at 20. Without the carry it
        # would drop to 10 and jump back to 20 in Q4: the sawtooth.
        assert got["2024-Q2"] == pytest.approx(20.0)
        assert got["2024-Q3"] == pytest.approx(20.0)
        assert got["2024-Q4"] == pytest.approx(20.0)

    def test_the_carried_period_reports_only_who_filed(self):
        out = blend_series([member(1.0, QUARTERLY_FILER, CAPS_2024_25),
                            member(1.0, SEMI_FILER, CAPS_2024_25)], ROE, quarter_bucket)
        by = {p["period"]: p for p in out["points"]}
        assert by["2024-Q2"]["covered_pct"] == pytest.approx(100.0)     # both reported
        assert by["2024-Q3"]["covered_pct"] == pytest.approx(50.0)      # one reported, one carried
        assert by["2024-Q3"]["covered_names_pct"] == pytest.approx(50.0)

    def test_nothing_is_carried_before_a_members_first_report(self):
        # ⚠ Backwards is invention, not estimation. A constituent that had not listed yet must not
        # appear in the periods before its first figure.
        out = carry_forward({"2024-Q3": ("2024-09-30", 5.0)},
                            ["2024-Q1", "2024-Q2", "2024-Q3", "2024-Q4"])
        assert list(out) == ["2024-Q3", "2024-Q4"]
        assert out["2024-Q4"] == (5.0, False)

    def test_a_member_that_stops_reporting_falls_out_within_a_year(self):
        axis = [f"{y}-Q{q}" for y in ("2024", "2025", "2026") for q in (1, 2, 3, 4)]
        out = carry_forward({"2024-Q1": ("2024-03-31", 7.0)}, axis)
        # Carried through its own year and no further — never held flat for the rest of the axis.
        assert list(out) == ["2024-Q1", "2024-Q2", "2024-Q3", "2024-Q4", "2025-Q1"]
        assert all(v == (7.0, False) for k, v in out.items() if k != "2024-Q1")

    def test_an_annual_series_is_carried_one_year_at_most(self):
        out = carry_forward({"2020": ("2020-12-31", 3.0)}, ["2020", "2021", "2022", "2023"])
        assert list(out) == ["2020", "2021"]


class TestBothFloors:
    def test_the_two_floors_are_the_documented_ones(self):
        assert MIN_BLEND_COVERAGE_PCT == 50.0
        assert MIN_BLEND_COVERAGE_NAMES_PCT == 50.0

    def test_one_giant_cannot_draw_a_period_on_its_own(self):
        """⚠ THE WEIGHT FLOOR ALONE PASSED THIS. Measured on the AEX: 2026-Q2 had 2 of 22
        constituents reporting and cleared at 53.8% of cap, because ASML is enormous."""
        big = member(60.0, {"2024-12-31": 10.0, "2025-12-31": 12.0})
        small = [member(10.0, {"2024-12-31": 10.0}) for _ in range(4)]
        out = blend_series([big, *small], ROE, year_bucket)
        periods = [p["period"] for p in out["points"]]
        assert "2024" in periods            # everyone reported
        # 2025: 60% of the weight but 1 of 5 names — refused.
        assert "2025" not in periods

    def test_many_tiny_names_cannot_outvote_a_missing_giant(self):
        """The mirror case, which is why it is an AND rather than a swap."""
        big = member(60.0, {"2024-12-31": 10.0})
        small = [member(10.0, {"2024-12-31": 10.0, "2025-12-31": 12.0}) for _ in range(4)]
        out = blend_series([big, *small], ROE, year_bucket)
        # 2025: 4 of 5 names, but only 40% of the weight — refused.
        assert [p["period"] for p in out["points"]] == ["2024"]

    def test_the_newest_year_is_still_refused_when_carried(self):
        """⚠⚠ THE PROPERTY THAT MAKES THE CARRY SAFE. Everyone is carried into 2025, so the value
        exists for all five — but only one REPORTED, so neither floor is met and nothing is
        drawn. Count the carried members as covered and this reads 100% and draws a flat line."""
        filed = member(10.0, {"2024-12-31": 10.0, "2025-12-31": 12.0})
        pending = [member(10.0, {"2024-12-31": 10.0}) for _ in range(4)]
        out = blend_series([filed, *pending], ROE, year_bucket)
        assert [p["period"] for p in out["points"]] == ["2024"]

    def test_exactly_half_on_both_bases_clears(self):
        a = member(10.0, {"2024-12-31": 10.0, "2025-12-31": 20.0})
        b = member(10.0, {"2024-12-31": 10.0})
        out = blend_series([a, b], ROE, year_bucket)
        by = {p["period"]: p for p in out["points"]}
        assert by["2025"]["covered_pct"] == pytest.approx(50.0)
        assert by["2025"]["covered_names_pct"] == pytest.approx(50.0)
        # b is carried at 10, so the average is over both: (20 + 10) / 2.
        assert by["2025"]["value"] == pytest.approx(15.0)
