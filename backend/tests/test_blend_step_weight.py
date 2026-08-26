"""A cap-weighted index's step is weighted by the cap it had AT THE START of that step.

⚠⚠ THE BUG THIS PINS READ +20.21%/yr WHERE THE ANSWER IS +11.14%/yr (2026-08-21).

`blend_series` chains a LEVEL line from weighted growth, `g_i = v_i(d)/v_i(anchor) − 1`, and it took
each member's weight at `d` — the END of the interval. For revenue that is a mild inconsistency. For
a PRICE series it is nearly circular: market cap = price × shares, so a constituent that tripled
carried ~3× the weight in the very step where it tripled, and one that halved carried half. Winners
over-weighted in their own winning step, losers under-weighted in theirs.

Measured on ACWI's `Month End Stock Price`, 1,512 constituents, 2015 → 2025
(`scripts/profile_price_index_weighting.py`, then `scripts/verify_price_index_cagr.py` through the
endpoint's own call path):

    end weight     index 100 → 630.2    +20.21%/yr
    anchor weight  index 100 → 287.6    +11.14%/yr      <- and ACWI really did ~10-11%

⚠ NOTHING ON SCREEN COULD HAVE SHOWN IT. The line was smooth, every period cleared both coverage
floors, the drill-down reconciled to it exactly, and the portfolio line beside it was correct — a
book has no per-period caps, so `_weight_at` hands it one scalar and anchor and end are the same
number. The bias sat on the benchmark alone, inside a comparison.

⚠ THE TEST IS BUILT SO THE RIGHT ANSWER IS NOT A MATTER OF OPINION: the panel's own total market cap
is stated, and a cap-weighted index's return IS the change in that total. Only one of the two
weightings reproduces it.

Pure — no DB, no network.
"""
from __future__ import annotations

import pytest

from routers._fundamental_blend import blend_series, blend_breakdown, year_bucket

PRICE = "annuals__Per Share Data__Month End Stock Price"

# Two constituents, equal at the start, opposite afterwards.
#
#   A: price 100 -> 300 (x3),   cap 100 -> 300
#   B: price 100 ->  50 (/2),   cap 100 ->  50
#
# The index's total cap goes 200 -> 350. A cap-weighted index that held both from 2020 therefore
# returned exactly +75%, and its level goes 100 -> 175. There is no other defensible number.
WINNER = {"weight": 100.0,
          "points": {"2020-12-31": 100.0, "2021-12-31": 300.0},
          "weights": {"2020": 100.0, "2021": 300.0}}
LOSER = {"weight": 100.0,
         "points": {"2020-12-31": 100.0, "2021-12-31": 50.0},
         "weights": {"2020": 100.0, "2021": 50.0}}


class TestTheStepIsWeightedAtTheAnchor:
    def test_the_index_reproduces_its_own_market_cap_move(self):
        out = blend_series([WINNER, LOSER], PRICE, year_bucket)
        got = {p["period"]: p["value"] for p in out["points"]}
        assert got["2020"] == pytest.approx(100.0)
        # Σcap 200 -> 350 is +75%. Anchor weights are equal, so the step is (+200% + −50%)/2.
        assert got["2021"] == pytest.approx(175.0)

    def test_the_end_weighted_answer_is_the_one_that_was_shipped(self):
        """⚠ NOT A SECOND IMPLEMENTATION — the arithmetic the old line produced, written out, so
        the size of the error is in the test rather than only in a commit message."""
        # (300·(+2.00) + 50·(−0.50)) ÷ 350 = +1.643 -> level 264.3, against a true 175.
        end_weighted = (300 * 2.0 + 50 * -0.5) / 350
        assert 100 * (1 + end_weighted) == pytest.approx(264.29, abs=0.01)
        out = blend_series([WINNER, LOSER], PRICE, year_bucket)
        assert out["points"][-1]["value"] != pytest.approx(264.29, abs=0.01)

    def test_a_portfolio_is_unaffected_because_it_has_no_per_period_caps(self):
        """⚠ WHY THE BOOK'S OWN LINE NEVER LOOKED WRONG. Without `weights`, `_weight_at` returns the
        holding weight for every period, so anchor and end are the same number and this whole class
        of error cannot arise. It is a benchmark-only bias — which is exactly where it does most
        damage, since the benchmark exists to be compared against."""
        book = [{k: v for k, v in m.items() if k != "weights"} for m in (WINNER, LOSER)]
        out = blend_series(book, PRICE, year_bucket)
        assert out["points"][-1]["value"] == pytest.approx(175.0)

    def test_it_changes_how_much_a_member_counts_never_whether_it_counts(self):
        """⚠ `p["at"][period]` is only written where `_weight_at` was truthy, so an anchor value
        existing already implies an anchor weight exists. Switching the period the weight is read at
        can therefore never silently drop a contributor — the contributor set is identical."""
        out = blend_series([WINNER, LOSER], PRICE, year_bucket)
        assert out["points"][-1]["covered_names_pct"] == pytest.approx(100.0)
        assert out["points"][-1]["covered_pct"] == pytest.approx(100.0)


class TestTheDrillDownReconcilesToTheLine:
    """⚠⚠ THE PANEL DECOMPOSES THE STEP THE CHART DREW, so it has to weight the same way. Weighted at
    the end it would still sum to its OWN total — internally consistent, externally wrong, which is
    the failure mode that survives review because the table checks out against itself."""

    def test_the_member_contributions_sum_to_the_plotted_step(self):
        line = blend_series([WINNER, LOSER], PRICE, year_bucket)
        step_pct = 100.0 * (line["points"][-1]["value"] / line["points"][0]["value"] - 1.0)
        assert step_pct == pytest.approx(75.0)

        named = [{**WINNER, "isin": "A", "name": "winner"},
                 {**LOSER, "isin": "B", "name": "loser"}]
        bd = blend_breakdown(named, PRICE, "2021")
        assert bd["anchor"] == "2020"
        assert bd["step_pct"] == pytest.approx(step_pct)
        # ⚠ THE COLUMN ADDS UP TO THE MOVE — the property that makes it a decomposition rather than
        # a pile of plausible numbers. Equal anchor weights, so winner +100pp and loser −25pp.
        pp = {m["name"]: m["contribution_pp"] for m in bd["members"]}
        assert pp["winner"] == pytest.approx(100.0)
        assert pp["loser"] == pytest.approx(-25.0)
        assert sum(v for v in pp.values() if v is not None) == pytest.approx(75.0)
