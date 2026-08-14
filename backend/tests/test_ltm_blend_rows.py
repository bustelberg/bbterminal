"""⚠⚠ EVERY PATH INTO `_blend_rows` MUST EMIT THE LTM PERIOD, AND ONE OF THEM DID NOT.

MEASURED 2026-08-14 on the Long Equity tab, `EPS (excl. non-recurring)`, ACWI benchmark, annual
basis. `/fundamental-blend-metrics` has two reads:

    NARROWED   the request names `metrics` -> `_bulk_blend_rows`.  The BENCHMARK overlay.
    FULL       it does not -> `_company_metric_rows` per holding.  The PORTFOLIO's own line, and
               the only read the SSE stream can use (its unit of progress is the holding).

The LTM rows were appended inside the narrowed branch only. So the index carried a
trailing-twelve-month point and the book did not: the green line ran a quarter past the blue one,
and because an LTM sits on a QUARTER-END x while every annual point sits on a whole year, the tick
fell through to `xToPeriod` and read **"2026 Q2"** — a fiscal quarter on an axis that has none.

⚠ THE FAILURE IS THAT IT LOOKS LIKE A FINDING. "The index reported Q2 and we have not" is a
perfectly ordinary thing for a chart to say, and it is not what was happening: both sides had the
data, one read simply never asked for it.

`_ltm_multi` is the DB boundary and is stubbed here — what is under test is the row SHAPE and the
metric expansion, which is what the two callers share.
"""
from __future__ import annotations

import routers.earnings as E

# {metric: {company_id: (period_end, value)}} — `_ltm_multi`'s shape.
STUB = {
    "eps_nri": {1: ("2026-06-30", 4.25), 2: ("2026-03-31", 1.10)},
    "revenue": {1: ("2026-06-30", 31_500.0)},
}


def _stub(monkeypatch, answer=None, seen: dict | None = None):
    def fake(cids, metrics, cadence):
        if seen is not None:
            seen.update(cids=list(cids), metrics=list(metrics), cadence=cadence)
        return STUB if answer is None else answer
    monkeypatch.setattr(E, "_ltm_multi", fake)


class TestTheRowShape:
    def test_one_row_per_company_per_metric_under_the_ANNUAL_code(self, monkeypatch):
        """⚠ THE ANNUAL SPELLING, not `ltm__…` and not `quarterly__…`. These rows go INTO the blend,
        which groups by `metric_code`; the `ltm__` rename happens on the way OUT (`_blend_rows`), so
        renaming them here would strand the LTM in a series of its own with no history to chain
        off."""
        _stub(monkeypatch)
        rows = E._ltm_blend_rows([1, 2], ["eps_nri", "revenue"], "annual")
        assert len(rows) == 3
        assert {r["metric_code"] for r in rows} == {
            E._metric_codes("eps_nri")[0], E._metric_codes("revenue")[0]}

    def test_the_period_is_the_literal_string_LTM(self, monkeypatch):
        """`year_bucket` is `d[:4]`, so `'LTM'` buckets to itself and sorts after every year
        (`'2026' < 'LTM'`) — that is the whole plumbing, and a real date would instead land in a
        fiscal year and replace it."""
        _stub(monkeypatch)
        assert {r["target_date"] for r in E._ltm_blend_rows([1, 2], ["eps_nri"], "annual")} == {"LTM"}

    def test_ltm_date_rides_along_and_is_the_real_quarter_end(self, monkeypatch):
        """⚠ WITHOUT IT `_blend_rows` STAMPS THE POINT WITH `period_end("LTM")` — i.e. TODAY — and
        the two lines' LTM points land at different x on a chart that exists to compare them."""
        _stub(monkeypatch)
        got = {(r["company_id"], r["ltm_date"])
               for r in E._ltm_blend_rows([1, 2], ["eps_nri"], "annual")}
        assert got == {(1, "2026-06-30"), (2, "2026-03-31")}


class TestTheFullReadAsksForEveryLine:
    def test_metrics_None_expands_to_every_metric_with_a_declared_roll_up(self, monkeypatch):
        """The full read has no metric list to pass on — it fetches every charted code. `None` is
        its way of saying "all of them", and the complete set is `_TTM_RULE`: a metric without a
        declared roll-up cannot have a trailing twelve months at all, so nothing is being left out.
        """
        seen: dict = {}
        _stub(monkeypatch, answer={}, seen=seen)
        E._ltm_blend_rows([1], None, "annual")
        assert seen["metrics"] == list(E._TTM_RULE)
        assert "eps_nri" in seen["metrics"]      # the line the bug was found on

    def test_a_named_request_is_passed_through_untouched(self, monkeypatch):
        seen: dict = {}
        _stub(monkeypatch, answer={}, seen=seen)
        E._ltm_blend_rows([1], ["eps_nri"], "annual")
        assert seen["metrics"] == ["eps_nri"]


class TestItRefusesWhereThereIsNothingToAdd:
    def test_quarterly_gets_no_LTM_row_and_costs_no_read(self, monkeypatch):
        """⚠ EVERY quarterly point ALREADY IS a trailing twelve months, so the newest one needs no
        separate name — appending one would duplicate the last column under a second label. The
        stub would raise if it were called with the wrong cadence; it must not be called at all."""
        called: dict = {}
        _stub(monkeypatch, seen=called)
        assert E._ltm_blend_rows([1, 2], ["eps_nri"], "quarterly") == []
        assert not called

    def test_no_companies_costs_no_read(self, monkeypatch):
        called: dict = {}
        _stub(monkeypatch, seen=called)
        assert E._ltm_blend_rows([], None, "annual") == []
        assert not called

    def test_a_company_with_no_LTM_simply_has_no_row(self, monkeypatch):
        """⚠ A HOLE, NOT A FABRICATED YEAR. A member whose newest filing does not reach past its
        last fiscal year has no trailing year to add; the blend CARRIES its last figure into the
        period instead, and — because a carried value counts for nothing in the coverage floor — a
        period where too few members really reported is refused rather than drawn."""
        _stub(monkeypatch, answer={"eps_nri": {1: ("2026-06-30", 4.25)}})
        rows = E._ltm_blend_rows([1, 2], ["eps_nri"], "annual")
        assert [r["company_id"] for r in rows] == [1]
