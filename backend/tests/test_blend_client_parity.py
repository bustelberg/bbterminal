"""The chart's line and the drill-down's line are computed twice, in two languages. Pin them.

⚠⚠ NOTHING PINNED THESE TWO AGAINST EACH OTHER UNTIL 2026-09-03, WHICH IS WHY THEY DRIFTED.
`blend_series` draws the chart (`/fundamental-blend-metrics`, server-side) and
`fundamentalBlend.ts::buildBlend` reproduces it under the chart (`/portfolio-revenue-matrix`, in
the browser). Every comment in both files says the two must agree; `test_blend_stream_parity.py`
only pins the two SERVER paths against one another, so the seam with no test on it is the seam
that moved. Reported as the Fundamental modal disagreeing with itself on ACWI: share price
+10.9%/yr on `Graphs` against +10.8% in `Tables`, revenue +4.5% against +4.6%.

⚠⚠ THE CAUSE WAS A DENOMINATOR, AND ONLY ON THE NAMES SIDE. An index payload lists EVERY
constituent (`all_constituents=True`), including those with no stored market cap, at weight 0 — so
the drill-down can say "in the index, not in the line" instead of silently listing 22 of the AEX's
25. The CHART is blended one member list up, over `_members(universe, require_market_cap=True)`,
and never sees them. `stableW` was already 0 for such a row and `wAt` already null, so the weight
floor and every average were right; what was left was `parts.length`, the denominator of
`coverN[y] / parts.length`, counting members this side's `total_n = len(members)` does not.

`_load_and_expand_members` predicts precisely this failure and guards the SERVER against it — "the
floors would move if this were the default... quietly adding three members that can never report
would drop every period from 21/22 to 21/25 and change which periods the CHART draws". The guard
did not travel with the payload.

⚠ A REFUSED PERIOD IS NOT A MISSING POINT. The chain skips it and the next step spans two
intervals instead of one, so every level after it is a product over a different partition — which
is why a floor that is off by one member shows up as a fraction of a percent per year rather than
a hole in the line.

⚠ ONE FIXTURE, TWO SUITES. `frontend/app/components/portfolios/__fixtures__/blendParity.json` is
read here and by `fundamentalBlend.parity.test.ts`; its `_doc` works the expected series out by
hand. A baseline captured from either implementation would pin that implementation's bugs as the
contract, which on a parity test is the one thing that cannot be allowed.

Pure — no DB, no network.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from routers._fundamental_blend import blend_series, year_bucket

#: ⚠ IT LIVES UNDER THE FRONTEND BECAUSE VITE WILL NOT SERVE A FILE OUTSIDE ITS ROOT WHILE PYTHON
#: WILL READ ANYTHING — so the only workable direction for a shared fixture is this one.
FIXTURE = (Path(__file__).resolve().parents[2]
           / "frontend/app/components/portfolios/__fixtures__/blendParity.json")


@pytest.fixture(scope="module")
def fx() -> dict:
    # ⚠ FAIL, NEVER SKIP. A parity test that quietly disappears when its other half is missing is
    # the same lie as a stale exceptions list: the suite would go green over a seam nobody checked.
    assert FIXTURE.exists(), (
        f"the shared parity fixture is missing at {FIXTURE} — it is read by BOTH suites, so a "
        "rename has to move it in one commit")
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _members(fx: dict) -> list[dict]:
    """The member list the LINE is blended over — `require_market_cap=True`.

    ⚠ THE CAP-LESS CONSTITUENT IS ABSENT HERE, AND THAT ABSENCE IS THE PARITY CLAIM. It is in the
    payload the client blends and it is not in this one; the two must nevertheless draw the same
    line, because it can never carry a weight on either side.
    """
    capped = [m for m in fx["members"] if m["market_cap_eur"] > 0]
    total = sum(m["market_cap_eur"] for m in capped)
    return [{
        "weight": 100.0 * m["market_cap_eur"] / total,
        # Keyed by PERIOD, like `period_caps_eur`; the points are keyed by DATE, like a filing.
        "weights": {p: float(m["market_cap_eur"]) for p in fx["periods"]},
        "points": {f"{p}-12-31": float(v) for p, v in m["values"].items()},
    } for m in capped]


def test_the_line_is_the_one_the_client_draws(fx: dict) -> None:
    s = blend_series(_members(fx), fx["metric_code"], year_bucket)

    # ⚠ THE DRAWN SET FIRST, BECAUSE IT IS WHAT DIVERGED — see the module docstring. 2016 and 2017
    # sit EXACTLY on the 50% floor: two of the four members report them.
    assert [p["period"] for p in s["points"]] == fx["expected_drawn"]

    for point in s["points"]:
        assert point["value"] == pytest.approx(
            fx["expected_level"][point["period"]], rel=1e-9), point["period"]


def test_the_names_floor_divides_by_the_members_the_line_has(fx: dict) -> None:
    members = _members(fx)
    s = blend_series(members, fx["metric_code"], year_bucket)

    # ⚠⚠ THIS IS THE DENOMINATOR ITSELF. `covered_names_pct` is `cover_n / total_n`, so 50.0 at
    # 2016 is the assertion that `total_n` is 4 — the members handed to `blend_series` — and not
    # the 5 rows the drill-down lists. The client's `coveredNames` is the same number and its test
    # asserts the same values off the same fixture.
    assert len(members) == fx["expected_contributors"]
    for point in s["points"]:
        assert point["covered_names_pct"] == pytest.approx(
            fx["expected_covered_names_pct"][point["period"]], rel=1e-9), point["period"]


def test_the_same_arithmetic_draws_the_wrong_line_off_the_wrong_member_list(fx: dict) -> None:
    """⚠⚠ THE ARITHMETIC WAS NEVER THE PROBLEM — THE MEMBER LIST WAS, AND THIS IS THE PROOF.

    Handed the DRILL-DOWN's list (the cap-less constituent included, at weight 0), this side draws
    exactly what the browser was drawing before the fix. `_prepare` still discards it as
    `no_weight`, `_weight_at` still refuses it at every period, and every VALUE it carries is still
    ignored — but `total_n = len(members)` counts it, so two reporters read 2/5 = 40%, 2016 and
    2017 fall under the floor, and the chain jumps 2015 → 2018 in one step:

        (0.331 + 0.728 + 0 + 0) / 4 = 0.264750   ->   100 x 1.264750 = 126.475

    against the 142.16875 the four-member list reaches over the same decade of the same figures.
    That is a 2.3pp/yr gap out of one member that contributes nothing, which is the whole shape of
    the ACWI complaint at a scale you can check by hand.

    ⚠ SO THE FIX BELONGS ON THE CLIENT, and this test is what says so: nothing here needs changing,
    because both sides already implement the same rule. What differed was which list each was given.
    """
    echo = next(m for m in fx["members"] if m["market_cap_eur"] == 0)
    extra = {"weight": 0.0,
             "weights": {p: 0.0 for p in fx["periods"]},
             "points": {f"{p}-12-31": float(v) for p, v in echo["values"].items()}}
    s = blend_series([*_members(fx), extra], fx["metric_code"], year_bucket)

    assert [p["period"] for p in s["points"]] == ["2015", "2018"]
    assert s["points"][0]["value"] == pytest.approx(100.0, rel=1e-9)
    assert s["points"][-1]["value"] == pytest.approx(126.475, rel=1e-9)
    # ⚠ 4 of 5, not 4 of 4 — the diluted denominator, stated where it is doing the damage.
    assert s["points"][0]["covered_names_pct"] == pytest.approx(80.0, rel=1e-9)
