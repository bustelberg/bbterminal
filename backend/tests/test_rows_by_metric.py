"""Many fundamentals lines in ONE read — and the split back out has to be exact.

WHY THIS EXISTS
    `_rows_by_company` was already one bulk read per line, which is what made the benchmark tabs
    viable at all. But the fundamentals grid draws NINETEEN lines, so it paid nineteen of them —
    and on the COPY transport each opens its own Postgres connection (`common.pg._run_copy`
    connects, sets `statement_timeout`, streams, disconnects). Nineteen handshakes to Supabase is
    most of a second before a row moves, and the nineteen scans hit the same
    `(company_id, metric_code)` index over the same ~1,900 ACWI constituents.

    `rows_by_metric` unions the codes into one `metric_code = ANY(...)` and demultiplexes in
    Python. That is a pure transport change, so everything here is about it staying pure: the same
    rows, under the same metric, in the same ORDER.

⚠ ORDER IS NOT COSMETIC HERE. `_latest_per_year_dated` keeps the LAST row it sees for a period, so
    two rows landing in one fiscal year resolve differently depending on which arrives second. A
    filter preserves relative order; these tests pin that it is actually a filter.
"""
from __future__ import annotations

import pytest

from routers import earnings as e


def _row(cid: int, code: str, date: str, value: float) -> dict:
    return {"company_id": cid, "metric_code": code, "target_date": date, "numeric_value": value}


@pytest.fixture
def _capture(monkeypatch):
    """Replace the single bulk read with one that records what it was asked for.

    Returns `(calls, install)` — `install(rows)` sets the rows the fake read hands back, ordered
    the way the real query orders them: `(company_id, target_date, metric_code)`.
    """
    calls: list[list[str]] = []
    store: dict[str, list[dict]] = {"rows": []}

    def _fake(company_ids: list[int], codes: list[str]):
        calls.append(list(codes))
        out: dict[int, list[dict]] = {}
        for r in sorted(store["rows"],
                        key=lambda x: (x["company_id"], x["target_date"], x["metric_code"])):
            if r["company_id"] in company_ids and r["metric_code"] in codes:
                out.setdefault(r["company_id"], []).append(r)
        return out

    monkeypatch.setattr(e, "_rows_by_company", _fake)
    return calls, lambda rows: store.__setitem__("rows", rows)


class TestItIsOneReadForEveryLine:
    def test_nineteen_metrics_cost_one_read(self, _capture):
        calls, install = _capture
        install([])
        metrics = ["revenue", "net_income", "fcf", "market_cap", "roic"]
        e.rows_by_metric([1, 2], metrics)
        assert len(calls) == 1, f"expected ONE read for {len(metrics)} lines, got {len(calls)}"

    def test_the_read_asks_for_every_metrics_codes(self, _capture):
        # ⚠ EVERY SPELLING, NOT JUST THE FIRST. `_METRIC_CODES` carries two or three per line (the
        # capitalized and lowercase GuruFocus section cohorts) and dropping the alternates would
        # blank whichever cohort a company happens to be in — silently, for half the index.
        calls, install = _capture
        install([])
        e.rows_by_metric([1], ["revenue", "roic"])
        asked = set(calls[0])
        for metric in ("revenue", "roic"):
            assert set(e._METRIC_CODES[metric]) <= asked, f"{metric}'s codes were not all requested"


class TestTheSplitIsExact:
    def test_each_row_lands_under_its_own_metric(self, _capture):
        _calls, install = _capture
        rev = e._METRIC_CODES["revenue"][0]
        ni = e._METRIC_CODES["net_income"][0]
        install([_row(1, rev, "2024-12-31", 100.0), _row(1, ni, "2024-12-31", 10.0)])

        out = e.rows_by_metric([1], ["revenue", "net_income"])
        assert [r["numeric_value"] for r in out["revenue"][1]] == [100.0]
        assert [r["numeric_value"] for r in out["net_income"][1]] == [10.0]

    def test_a_company_with_nothing_is_absent_from_that_metric(self, _capture):
        # ⚠ ABSENT, NEVER AN EMPTY LIST THAT READS AS A ZERO. The grid renders "no observation" as
        # a dash and a reported 0 as "0" — they are different facts about a company.
        _calls, install = _capture
        rev = e._METRIC_CODES["revenue"][0]
        install([_row(1, rev, "2024-12-31", 100.0)])

        out = e.rows_by_metric([1, 2], ["revenue"])
        assert 2 not in out["revenue"]

    def test_a_metric_nobody_reported_is_present_but_empty(self, _capture):
        # It was ASKED for and the answer is nothing — distinct from a metric that was refused,
        # which is absent from the result entirely (see below).
        _calls, install = _capture
        install([])
        out = e.rows_by_metric([1], ["revenue", "goodwill"])
        assert set(out) == {"revenue", "goodwill"}
        assert not out["revenue"] and not out["goodwill"]

    def test_order_within_a_metric_survives_the_split(self, _capture):
        # ⚠ THE FAILURE THIS GUARDS IS SILENT AND PICKS A DIFFERENT NUMBER, NOT AN ERROR:
        # `_latest_per_year_dated` keeps the LAST row it sees for a fiscal year.
        _calls, install = _capture
        rev = e._METRIC_CODES["revenue"][0]
        ni = e._METRIC_CODES["net_income"][0]
        install([
            _row(1, rev, "2022-12-31", 1.0),
            _row(1, ni, "2022-12-31", 90.0),      # interleaved — the union read returns it here
            _row(1, rev, "2023-12-31", 2.0),
            _row(1, rev, "2024-12-31", 3.0),
        ])
        out = e.rows_by_metric([1], ["revenue", "net_income"])
        assert [r["target_date"] for r in out["revenue"][1]] == [
            "2022-12-31", "2023-12-31", "2024-12-31"]


class TestARefusedMetricIsAbsentNotEmpty:
    def test_quarterly_omits_a_line_with_no_ttm_rule(self, _capture, monkeypatch):
        # ⚠ "WE REFUSE TO ROLL THIS UP" AND "WE HOLD NOTHING" ARE DIFFERENT ANSWERS, and the
        # caller has to be able to tell them apart — `_codes_and_rule` returns (None, None) rather
        # than guessing a roll-up, because the wrong rule produces a plausible number (summing four
        # quarter-end balance sheets reports a company with 4x its assets).
        _calls, install = _capture
        install([])
        monkeypatch.delitem(e._TTM_RULE, "revenue")
        out = e.rows_by_metric([1], ["revenue", "net_income"], cadence="quarterly")
        assert "revenue" not in out
        assert "net_income" in out

    def test_quarterly_reads_the_quarterly_codes(self, _capture):
        # The cadence is expressed in the metric CODE (`annuals__` -> `quarterly__`), so asking for
        # the annual spelling on a quarterly basis returns a full, correct-looking annual series.
        calls, install = _capture
        install([])
        e.rows_by_metric([1], ["revenue"], cadence="quarterly")
        assert all(c.startswith("quarterly__") for c in calls[0]), calls[0]

    def test_no_metrics_at_all_reads_nothing(self, _capture):
        calls, install = _capture
        install([])
        assert e.rows_by_metric([1], []) == {}
        assert calls == [], "a read was issued for zero metrics"
