"""FCF per share is drawn from the companies POSITIVE IN EVERY PERIOD, and from nobody else.

⚠⚠ A CHOSEN TRADE, NOT A CORRECTION, and it costs in two directions at once. Averaging growth rates
is upward-biased — a rate is floored at −100% and unbounded above, and cutting the accepted-growth
cap from +10,000% to +1,000% moves `fcf_ps` by 4.06pp a year — and dropping every member that ever
went negative adds survivorship on top. Both push the same way: up. The euro sum this replaces had
neither bias, because it never takes a ratio of a member to itself.

⚠⚠ SO THE COUNT IS NOT OPTIONAL, and it is the half of this that is worth testing hardest. A
survivorship filter nobody can see is the whole hazard: on a broad index it silently deletes the
cash-burners, the recoveries and every bank whose free cash flow swings on deposit flows, and the
line that remains looks exactly like an index line.

⚠ THE FILTER IS DECLARED ON METRIC **KEYS** AND `_blend_rows` WALKS **CODES** — the same
key-versus-code trap `_totals_for` records for the euros, where a mismatch silently never fires.

Unit-only: `_blend_rows` is pure of I/O.
"""
from __future__ import annotations

import pytest

FCF = "annuals__Per Share Data__Free Cash Flow per Share"
REV = "annuals__Income Statement__Revenue"
EPS = "annuals__Per Share Data__EPS without NRI"
EST = "annual_eps_nri_estimate"


def _row(cid: int, code: str, year: int, v: float) -> dict:
    return {"company_id": cid, "metric_code": code,
            "target_date": f"{year}-12-31", "numeric_value": v}


#: Three companies. #2 dips negative in one year; #3 is negative throughout.
def _rows(code: str) -> list[dict]:
    out: list[dict] = []
    for year, a, b, c in ((2023, 1.0, 1.0, -1.0), (2024, 2.0, -0.5, -2.0), (2025, 3.0, 2.0, -3.0)):
        out += [_row(1, code, year, a), _row(2, code, year, b), _row(3, code, year, c)]
    return out


COVERED = [{"company_id": i, "weight_pct": 10.0} for i in (1, 2, 3)]


@pytest.fixture
def earnings():
    from routers import earnings as e
    return e


class TestOnlyTheAlwaysPositiveMembersDrawTheLine:
    def test_a_member_negative_in_any_period_is_excluded(self, earnings):
        built = earnings._blend_rows(_rows(FCF), COVERED)
        assert built["member_counts"][FCF] == {"considered": 1, "total": 3,
                                              "rule": "positive_only"}

    def test_one_bad_year_anywhere_is_enough(self, earnings):
        """⚠ ANY PERIOD, not the base period and not the last. Company #2 is positive at both ends
        and dips in the middle; a first-or-last test would keep it and the filter would be a
        different rule from the one the card describes."""
        rows = [r for r in _rows(FCF) if r["company_id"] == 2]
        built = earnings._blend_rows(rows, [COVERED[1]])
        assert built["member_counts"][FCF]["considered"] == 0

    def test_a_metric_not_in_the_set_keeps_every_member(self, earnings):
        # ⚠ THE CONTROL. Revenue goes negative for nobody, but the point is the RULE is per metric:
        # applying it everywhere would silently shrink twelve other charts.
        built = earnings._blend_rows(_rows(REV), COVERED)
        assert built["member_counts"][REV] == {"considered": 3, "total": 3, "rule": "all"}

    def test_the_filter_is_keyed_off_the_metric_KEY_not_the_code_spelling(self, earnings):
        """⚠⚠ THE TRAP THAT WOULD MAKE IT A SILENT NO-OP. `_POSITIVE_ONLY_METRICS` holds `fcf_ps`;
        the loop walks `annuals__Per Share Data__Free Cash Flow per Share` AND its lowercase twin.
        Comparing the two directly matches nothing and every member stays in."""
        lower = "annuals__per_share_data__Free Cash Flow per Share"
        built = earnings._blend_rows(_rows(lower), COVERED)
        assert built["member_counts"][lower] == {"considered": 1, "total": 3,
                                                "rule": "positive_only"}


class TestTheCountIsAlwaysReported:
    def test_every_code_gets_a_count_even_when_nothing_was_withheld(self, earnings):
        """⚠ SO A CARD NEVER HAS TO KNOW whether its metric happens to be filtered to render
        "n of m" — it compares the two numbers and stays silent when they match."""
        built = earnings._blend_rows(_rows(REV) + _rows(FCF), COVERED)
        assert set(built["member_counts"]) == {REV, FCF}
        assert built["member_counts"][REV]["considered"] == built["member_counts"][REV]["total"]
        assert built["member_counts"][FCF]["considered"] < built["member_counts"][FCF]["total"]

    def test_the_total_is_the_covered_set_not_the_rows_that_arrived(self, earnings):
        # ⚠ A company with no FCF row at all is still part of "of 3" — the reader is comparing
        # against the book, not against whoever happened to file.
        rows = [r for r in _rows(FCF) if r["company_id"] == 1]
        assert earnings._blend_rows(rows, COVERED)["member_counts"][FCF]["total"] == 3


class TestTheRuleSaysWhyMembersAreMissing:
    """⚠⚠ THE CARD'S ⓘ IS PICKED FROM THIS, so a wrong `rule` is a confident wrong explanation of
    a number that is right — the failure mode this app records again and again. It is reported by
    the code that DROPS the members, because a client re-deriving it from the metric name would be a
    second copy of a decision that lives in one set membership here."""

    def test_a_filtered_metric_says_positive_only(self, earnings):
        built = earnings._blend_rows(_rows(FCF), COVERED)
        assert built["member_counts"][FCF]["rule"] == "positive_only"

    def test_an_unfiltered_growth_chain_says_all(self, earnings):
        """⚠ AND `all` MUST NOT BE READ AS "nothing is missing" — it says no RULE withheld anyone.
        The card stays silent anyway while `considered == total`."""
        built = earnings._blend_rows(_rows(REV), COVERED)
        assert built["member_counts"][REV]["rule"] == "all"

    def test_a_euro_sum_says_aggregate_and_counts_the_members_carrying_euros(self, earnings):
        """⚠⚠ THE HALF THAT WENT UNCOUNTED FOR MONTHS. `blend_series` measured it as
        `fund_members` and `_blend_rows` discarded it, so the EPS card said nothing while the FCF
        card beside it said "36 of 42" — and a silent card is also what a card with no drops looks
        like. Company #3 carries no euros here, so the sum speaks for two of three holdings."""
        # ⚠ KEYED BY FILING DATE, the shape `fundamental_totals` really returns — `blend_series`
        # buckets them itself, and keying them by period here would test a shape nothing produces.
        totals = {REV: {1: {"2023-12-31": 100.0, "2024-12-31": 110.0, "2025-12-31": 120.0},
                        2: {"2023-12-31": 50.0, "2024-12-31": 55.0, "2025-12-31": 60.0}}}
        built = earnings._blend_rows(_rows(REV), COVERED, None, "annual", totals)
        assert built["member_counts"][REV] == {"considered": 2, "total": 3, "rule": "aggregate"}

    def test_the_denominator_is_still_the_whole_book_on_the_aggregate_path(self, earnings):
        """⚠ `total` IS `len(covered)` ON BOTH CONSTRUCTIONS. `blend_series` also knows how many
        members survived `_prepare` — a third number — and using that here would shrink the
        denominator to hide the very drops this exists to surface."""
        totals = {REV: {1: {"2023-12-31": 100.0, "2024-12-31": 110.0,
                            "2025-12-31": 120.0}}}
        built = earnings._blend_rows(_rows(REV), COVERED, None, "annual", totals)
        assert built["member_counts"][REV]["total"] == 3
        assert built["member_counts"][REV]["considered"] == 1


class TestEpsEligibilitySpansTheConsensusToo:
    """⚠⚠ "ALL POSITIVE FOR THEIR HISTORICAL **AND** ANALYST ESTIMATES" — asked for in those words
    (2026-08-31), and it is a stricter rule than `fcf_ps` needs because EPS is the one charted
    metric with a forecast leg.

    A chart whose solid line continues into a dotted one is ONE line. Filtered per code — which is
    what this did before EPS joined the set — a company positive through every filed year and
    negative in a single consensus year is IN the first half and OUT of the second, so the
    composition steps at the join. That is the seam nobody inspects: both halves look right, and
    the discontinuity is the one thing neither half can show.
    """

    def _eps_rows(self, est_for_2: float) -> list[dict]:
        rows: list[dict] = []
        for year, a, b in ((2023, 1.0, 1.0), (2024, 2.0, 2.0), (2025, 3.0, 3.0)):
            rows += [_row(1, EPS, year, a), _row(2, EPS, year, b)]
        # ⚠ AFTER the newest actual, or `_drop_superseded_forecasts` removes it as a stale
        # pre-announcement consensus and the fixture would prove nothing.
        rows += [_row(1, EST, 2026, 4.0), _row(2, EST, 2026, est_for_2)]
        return rows

    def test_a_company_positive_in_every_filed_year_still_fails_on_a_negative_consensus(self):
        from routers import earnings as e
        built = e._blend_rows(self._eps_rows(-1.0), COVERED[:2])
        assert built["member_counts"][EPS] == {"considered": 1, "total": 2,
                                               "rule": "positive_only"}

    def test_and_it_leaves_BOTH_legs_not_just_the_one_it_failed(self):
        """⚠ THE POINT OF THE JOINT RULE. Dropping it from the consensus alone would leave the
        actual leg drawn over two companies and the forecast over one — the step at the seam."""
        from routers import earnings as e
        built = e._blend_rows(self._eps_rows(-1.0), COVERED[:2])
        assert built["member_counts"][EPS]["considered"] == 1
        assert built["member_counts"][EST]["considered"] == 1

    def test_a_positive_consensus_keeps_the_company_in(self):
        from routers import earnings as e
        built = e._blend_rows(self._eps_rows(4.5), COVERED[:2])
        assert built["member_counts"][EPS]["considered"] == 2
        assert built["member_counts"][EST]["considered"] == 2

    def test_a_negative_filed_year_takes_the_consensus_leg_with_it(self):
        """The mirror: eligibility is a property of the COMPANY, not of the leg that failed."""
        from routers import earnings as e
        rows = self._eps_rows(4.5)
        rows = [r if not (r["company_id"] == 2 and r["target_date"].startswith("2024"))
                else _row(2, EPS, 2024, -0.5) for r in rows]
        built = e._blend_rows(rows, COVERED[:2])
        assert built["member_counts"][EPS]["considered"] == 1
        assert built["member_counts"][EST]["considered"] == 1

    def test_a_superseded_consensus_cannot_disqualify_anybody(self):
        """⚠⚠ ORDER, NOT LUCK. The vendor keeps a pre-announcement estimate for a year the company
        has since reported, and nothing draws that row. Read before `_drop_superseded_forecasts`,
        a negative one would throw the company off both legs on the strength of a figure that is
        nowhere on screen."""
        from routers import earnings as e
        rows = self._eps_rows(4.5) + [_row(2, EST, 2024, -9.0)]
        built = e._blend_rows(rows, COVERED[:2])
        assert built["member_counts"][EPS]["considered"] == 2

    def test_eps_is_not_summed_in_euros_any_more(self):
        """⚠ THE OTHER HALF OF THE SAME DECISION. The filter only makes sense on the growth chain:
        a euro sum handles a negative member correctly and needs no filter, so a metric that is
        positives-only must not also be aggregatable — see `_AGGREGATABLE_PER_SHARE`."""
        from routers import earnings as e
        assert "eps_nri" in e._POSITIVE_ONLY_METRICS
        assert "eps_nri" not in e._AGGREGATABLE_PER_SHARE
        assert e.aggregatable_metrics(["eps_nri"]) == []

    def test_the_group_carries_both_section_spellings_and_the_consensus(self):
        """⚠ DERIVED FROM `_METRIC_CODES`, NEVER LISTED — a hand-written group goes stale silently
        and the symptom is a filter that covers half its own chart."""
        from routers import earnings as e
        group = e._positive_only_groups()
        assert set(group[EPS]) == {EPS, "annuals__per_share_data__EPS without NRI", EST}
        assert group[EST] == group[EPS]
        # ⚠ `fcf_ps` HAS NO CONSENSUS, so its group is just its own spellings — the joint rule
        # costs it nothing and its count is unchanged by this.
        assert set(group[FCF]) == set(e._METRIC_CODES["fcf_ps"])
