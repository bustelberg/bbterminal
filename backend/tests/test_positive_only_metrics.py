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
        assert built["member_counts"][FCF] == {"considered": 1, "total": 3}

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
        assert built["member_counts"][REV] == {"considered": 3, "total": 3}

    def test_the_filter_is_keyed_off_the_metric_KEY_not_the_code_spelling(self, earnings):
        """⚠⚠ THE TRAP THAT WOULD MAKE IT A SILENT NO-OP. `_POSITIVE_ONLY_METRICS` holds `fcf_ps`;
        the loop walks `annuals__Per Share Data__Free Cash Flow per Share` AND its lowercase twin.
        Comparing the two directly matches nothing and every member stays in."""
        lower = "annuals__per_share_data__Free Cash Flow per Share"
        built = earnings._blend_rows(_rows(lower), COVERED)
        assert built["member_counts"][lower] == {"considered": 1, "total": 3}


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
