"""A blended QUARTERLY series must be aligned in the same period vocabulary as its weights.

⚠⚠ THIS IS THE "EMPTY, NOT THIN" BUG. `blend_series` bucketed every member's points by FISCAL YEAR
while an index's per-period weights (`period_caps_eur(cadence="quarterly")`) are keyed `2025-Q3`.
`_weight_at` looks a member's weight up BY THE BUCKET KEY, so every lookup missed, every member was
dropped from every period, and the series came back with ZERO points — before coverage was ever
computed. The card then reported "no period clears the coverage floor", a floor that had not run.

Measured 2026-08-12 on the AEX quarterly Revenue benchmark: 22 constituents, 639 rows,
`contributing: 22`, `years: 0`, while the drill-down table beside it showed 84–93% of the index
reporting every quarter. Annual was fine throughout, which is what made it look like missing data
rather than a key mismatch.

Second defect pinned here: even with matching keys, a YEAR bucket collapses the four
trailing-twelve-month points of a year onto the latest one — a quarterly toggle silently drawing an
annual line.

Pure — no DB, no network.
"""
from __future__ import annotations

import pytest

from routers._fundamental_blend import (
    blend_series,
    period_end,
    quarter_bucket,
    year_bucket,
)

_period_end = period_end       # it moved to `_fundamental_blend`; `earnings` imports it from there

REV = "annuals__Income Statement__Revenue"          # a LEVEL: rebased to 100, then weighted
ROE = "annuals__Ratios__ROE %"                      # a RATIO: weighted arithmetically


def member(weight, points, weights=None):
    m = {"weight": weight, "points": points}
    if weights is not None:
        m["weights"] = weights
    return m


QUARTERS = {"2024-03-31": 100.0, "2024-06-30": 110.0,
            "2024-09-30": 120.0, "2024-12-31": 130.0}
CAPS_BY_QUARTER = {"2024-Q1": 10.0, "2024-Q2": 10.0, "2024-Q3": 10.0, "2024-Q4": 10.0}


class TestTheBucketMustMatchTheWeights:
    def test_an_index_on_quarterly_draws_every_quarter(self):
        out = blend_series([member(1.0, QUARTERS, CAPS_BY_QUARTER)], REV, quarter_bucket)
        assert [p["period"] for p in out["points"]] == ["2024-Q1", "2024-Q2", "2024-Q3", "2024-Q4"]
        assert out["points"][-1]["covered_pct"] == pytest.approx(100.0)

    def test_the_year_bucket_against_quarter_keyed_weights_drew_NOTHING(self):
        """The regression itself. Not "thin" — empty, with a full set of rows in hand."""
        out = blend_series([member(1.0, QUARTERS, CAPS_BY_QUARTER)], REV, year_bucket)
        assert out["points"] == []

    def test_a_PORTFOLIO_is_unaffected_either_way(self):
        """No `weights` ⇒ one scalar basis for every period (`_weight_at` reads the absence), so a
        book never hit the mismatch — which is why this only ever showed on a benchmark."""
        for bucket in (year_bucket, quarter_bucket):
            out = blend_series([member(1.0, QUARTERS)], REV, bucket)
            assert out["points"], f"{bucket.__name__} drew nothing for a portfolio"

    def test_a_cap_the_year_has_not_filed_yet_is_taken_AS_OF(self):
        """⚠ The current year is the one people look at, and it is the one with no cap: measured on
        the AEX, 1 of 22 constituents had a 2026 cap. `_weight_at` falls back to the newest cap
        before the period, so 2026 is weighted on 2025's rather than dropped."""
        caps = {"2024-Q1": 10.0, "2024-Q2": 10.0, "2024-Q3": 10.0, "2024-Q4": 10.0}
        pts = {**QUARTERS, "2025-03-31": 140.0}
        out = blend_series([member(1.0, pts, caps)], REV, quarter_bucket)
        assert [p["period"] for p in out["points"]][-1] == "2025-Q1"


class TestTheYearBucketCollapsesAQuarterlySeries:
    def test_four_TTM_points_become_one(self):
        # ⚠ The quieter half of the bug: with matching keys this still "works", drawing an ANNUAL
        # line while the toggle says quarterly. The last point of the year wins.
        out = blend_series([member(1.0, QUARTERS)], ROE, year_bucket)
        assert [(p["period"], p["value"]) for p in out["points"]] == [("2024", 130.0)]

    def test_the_quarter_bucket_keeps_all_four(self):
        out = blend_series([member(1.0, QUARTERS)], ROE, quarter_bucket)
        assert [p["value"] for p in out["points"]] == [100.0, 110.0, 120.0, 130.0]


class TestBucketing:
    def test_quarter_bucket_is_the_calendar_quarter_of_the_period_end(self):
        assert quarter_bucket("2025-09-30") == "2025-Q3"
        assert quarter_bucket("2026-03-31") == "2026-Q1"
        # ⚠ Same derivation as `_ttm_by_period`'s label — they have to agree, because the caps are
        # built by one and looked up by the other.
        assert quarter_bucket("2025-01-31") == "2025-Q1"
        assert quarter_bucket("2025-12-31") == "2025-Q4"

    def test_year_bucket_is_the_calendar_year_the_period_ends_in(self):
        assert year_bucket("2026-03-31") == "2026"


class TestThePlottedDate:
    def test_a_quarter_becomes_its_own_calendar_end(self):
        assert _period_end("2025-Q1") == "2025-03-31"
        assert _period_end("2025-Q2") == "2025-06-30"
        assert _period_end("2025-Q3") == "2025-09-30"
        assert _period_end("2025-Q4") == "2025-12-31"

    def test_a_year_keeps_the_31_december_convention(self):
        # ⚠ Unchanged for the annual path — every existing blended series plots on these dates.
        assert _period_end("2025") == "2025-12-31"

    def test_the_quarterly_dates_sort_into_calendar_order(self):
        # The axis is drawn off these strings; Q4 must not sort before Q2.
        got = [_period_end(f"2025-Q{q}") for q in (1, 2, 3, 4)]
        assert got == sorted(got)
