"""The filings behind an LTM must be THE filings the LTM was computed from.

⚠⚠ THE WHOLE POINT OF THE `parts=` OUT PARAMETER. A breakdown panel that explains 4.25 with four
quarters the roll-up did not use is worse than no panel: it is checked once and believed thereafter.
So `_ttm_by_period` reports the window it actually used rather than a `_ttm_parts()` twin
re-deriving "the last four" — which is not the rule (see below) and would drift the day either side
changed.

The window is `k` CONSECUTIVE filings, `k` from `filings_per_year`, refused entirely when they span
more than `365(k−0.5)/k` days — because a hole reaches back past its own year and double-counts the
period that comes round again, which still looks like four rows.
"""
from __future__ import annotations

from routers.earnings import _ttm_by_period

QUARTERS = ["2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31",
            "2026-03-31", "2026-06-30"]


def rows(dates, values, cid=1, code="quarterly__Per Share Data__EPS without NRI"):
    return [{"company_id": cid, "metric_code": code, "target_date": d, "numeric_value": v}
            for d, v in zip(dates, values)]


class TestThePartsAreTheWindow:
    def test_a_sum_reconciles_exactly_to_its_parts(self):
        parts: dict[str, list[dict]] = {}
        out = _ttm_by_period(rows(QUARTERS, [1.0, 1.1, 1.2, 1.3, 1.4, 1.5]), "sum",
                             key="date", parts=parts)
        newest = max(out)
        assert newest == "2026-06-30"
        assert [p["value"] for p in parts[newest]] == [1.2, 1.3, 1.4, 1.5]
        assert abs(sum(p["value"] for p in parts[newest]) - out[newest]) < 1e-12

    def test_every_emitted_period_has_parts_and_nothing_else_does(self):
        """⚠ THE TWO DICTS ARE KEYED THE SAME WAY. A panel looks its window up by the point's own
        label; a parts dict keyed differently would silently show nothing."""
        parts: dict[str, list[dict]] = {}
        out = _ttm_by_period(rows(QUARTERS, [1.0] * 6), "sum", key="date", parts=parts)
        assert set(parts) == set(out)

    def test_the_label_key_is_honoured_too(self):
        parts: dict[str, list[dict]] = {}
        out = _ttm_by_period(rows(QUARTERS, [1.0] * 6), "sum", parts=parts)
        assert set(parts) == set(out) == {"2025-Q4", "2026-Q1", "2026-Q2"}

    def test_each_part_carries_its_OWN_quarter_end(self):
        """⚠ AN OFF-CALENDAR FILER'S DATES ARE NOT 03-31/06-30/09-30/12-31, which is why the panel
        prints them rather than implying them from position."""
        odd = ["2025-01-31", "2025-04-30", "2025-07-31", "2025-10-31", "2026-01-31"]
        parts: dict[str, list[dict]] = {}
        _ttm_by_period(rows(odd, [2.0] * 5), "sum", key="date", parts=parts)
        assert [p["date"] for p in parts["2026-01-31"]] == odd[1:]


class TestItReportsTheRuleTheEngineRan:
    def test_a_MEAN_is_the_average_of_its_parts_not_their_sum(self):
        """⚠ A SHARE COUNT IS ALREADY AN AVERAGE OVER EACH QUARTER. Four summed reports four times
        the company. The panel prints the same parts either way, so the operator between them is
        the only thing that says which arithmetic ran — which is why the rule is returned."""
        parts: dict[str, list[dict]] = {}
        out = _ttm_by_period(rows(QUARTERS, [100.0, 100.0, 100.0, 100.0, 100.0, 200.0]), "mean",
                             key="date", parts=parts)
        assert [p["value"] for p in parts["2026-06-30"]] == [100.0, 100.0, 100.0, 200.0]
        assert out["2026-06-30"] == 125.0

    def test_a_LAST_still_reports_the_whole_window(self):
        """A balance is the newest filing — but the window is still what qualified it, and a reader
        checking "which twelve months is this as of" needs to see it."""
        parts: dict[str, list[dict]] = {}
        out = _ttm_by_period(rows(QUARTERS, [1, 2, 3, 4, 5, 6]), "last", key="date", parts=parts)
        assert out["2026-06-30"] == 6
        assert len(parts["2026-06-30"]) == 4


class TestARefusedWindowHasNoParts:
    def test_a_HOLE_produces_neither_a_point_nor_parts(self):
        """⚠⚠ THE CASE THAT MAKES A SEPARATE `_ttm_parts()` DANGEROUS. Four consecutive ROWS across
        a missing quarter span more than a year, so the engine emits nothing — while "the last four
        rows" would happily hand a panel four filings for a point that is not on the chart."""
        holed = ["2025-03-31", "2025-06-30", "2025-09-30", "2026-06-30"]
        parts: dict[str, list[dict]] = {}
        out = _ttm_by_period(rows(holed, [1.0, 1.1, 1.2, 1.3]), "sum", key="date", parts=parts)
        assert "2026-06-30" not in out
        assert "2026-06-30" not in parts

    def test_a_SEMI_ANNUAL_filer_has_a_two_filing_window(self):
        """`k` is how often THIS company reports, never a hardcoded four — so its trailing year is
        two filings and the panel must not imply two are missing."""
        half = ["2024-06-30", "2024-12-31", "2025-06-30", "2025-12-31"]
        parts: dict[str, list[dict]] = {}
        out = _ttm_by_period(rows(half, [5.0, 6.0, 7.0, 8.0]), "sum", key="date", parts=parts)
        assert out["2025-12-31"] == 15.0
        assert [p["value"] for p in parts["2025-12-31"]] == [7.0, 8.0]

    def test_an_incomplete_first_year_has_no_parts_either(self):
        parts: dict[str, list[dict]] = {}
        out = _ttm_by_period(rows(QUARTERS[:3], [1.0, 1.1, 1.2]), "sum", key="date", parts=parts)
        assert out == {} and parts == {}


class TestADroppedQuarterIsAbsentFromTheParts:
    def test_the_breakdown_shows_what_was_USED_not_what_was_filed(self):
        """⚠ `_drop_quarter_outliers` RUNS BEFORE THE WINDOWS. A corrupt quarter is not in the
        figure, so it must not be in the explanation of the figure — and its absence shifts the
        window back rather than leaving a three-filing year."""
        vals = [1.0, 1.1, 90_000.0, 1.3, 1.4, 1.5]
        parts: dict[str, list[dict]] = {}
        out = _ttm_by_period(rows(QUARTERS, vals), "sum", key="date", parts=parts)
        assert all("2025-09-30" != p["date"] for w in parts.values() for p in w)
        # The windows that would have contained it are gone entirely — a hole, not a patched year.
        assert "2025-12-31" not in out
