"""The benchmark overlay reads the blend a different way — it must not blend a different answer.

`/fundamental-blend-metrics` reads EVERY charted code, per company, three paged requests each.
That is right for a 40-name book and impossible for an index: the S&P 500 is ~1,500 round trips
for a checkbox. So a request may NAME its metrics, and the read becomes one chunked, paged query
per metric across every constituent.

⚠ THE DANGER IS NOT SPEED, IT IS DIVERGENCE. Two loaders feeding one blend is two places for the
rules to live — which cadence spelling a metric uses, which code the result is emitted under, what
happens to a company with no rows. If they drift, a chart shows a portfolio line and a benchmark
line built by different rules and calls the gap a finding. So the narrowed reader is asserted to
produce the SAME rows as the loop it replaces, on both cadences.

⚠ AND THE TTM ROWS MUST CARRY `company_id`. They are synthesised, not read, so the field is easy
to leave off — and `_blend_rows` keys every point by the company that reported it. Without it a
PORTFOLIO's growth cards raised KeyError the moment the tab switched to quarterly (a 500), while
the single-company path, which never blends, stayed green. Measured and fixed 2026-08-04.
"""
from __future__ import annotations

import pytest

from tests._fake_supabase import FakeSupabase

_ANNUAL_REVENUE = "annuals__Income Statement__Revenue"
_Q_REVENUE = "quarterly__Income Statement__Revenue"
_ANNUAL_FCF_PS = "annuals__Per Share Data__Free Cash Flow per Share"
_Q_FCF_PS = "quarterly__Per Share Data__Free Cash Flow per Share"


def _rows() -> list[dict]:
    """Two companies: eight quarters and two fiscal years of revenue, plus an FCF/share line.

    A stray `indicator_q_forward_pe_ratio` rides along — the narrowed read must not pick it up
    (it is one of the codes the full read's `indicator%` pattern DOES collect, so a narrowing that
    quietly widened would show up here and nowhere else).
    """
    out: list[dict] = []
    for cid in (1, 2):
        for i, year in enumerate((2016, 2017)):
            out.append({"company_id": cid, "metric_code": _ANNUAL_REVENUE,
                        "target_date": f"{year}-12-31", "numeric_value": 100.0 * (cid + i)})
            out.append({"company_id": cid, "metric_code": _ANNUAL_FCF_PS,
                        "target_date": f"{year}-12-31", "numeric_value": 1.0 + i})
            for q, month in enumerate(("03-31", "06-30", "09-30", "12-31")):
                out.append({"company_id": cid, "metric_code": _Q_REVENUE,
                            "target_date": f"{year}-{month}",
                            "numeric_value": 25.0 * (cid + i) + q})
        out.append({"company_id": cid, "metric_code": "indicator_q_forward_pe_ratio",
                    "target_date": "2017-06-30", "numeric_value": 19.0})
    return out


@pytest.fixture
def earnings(monkeypatch):
    """The module under test, reading a fake `metric_data`."""
    from routers import earnings as e

    fake = FakeSupabase({"metric_data": _rows()})
    monkeypatch.setattr(e, "supabase", fake)
    return e


def _key(rows: list[dict]) -> set[tuple]:
    """A row's identity for comparison — the four fields the blend actually consumes."""
    return {(r["company_id"], r["metric_code"], str(r["target_date"])[:10],
             round(float(r["numeric_value"]), 9))
            for r in rows if r.get("numeric_value") is not None}


class TestTheNarrowedReadMatchesTheLoop:
    def test_annual_rows_are_the_same_rows(self, earnings):
        loop = [r for cid in (1, 2) for r in earnings._company_metric_rows(cid)]
        bulk = earnings._bulk_blend_rows([1, 2], ["revenue", "fcf_ps"], "annual")
        # The loop reads every code; compare on the two the narrowed request asked for.
        want = {_ANNUAL_REVENUE, _ANNUAL_FCF_PS}
        assert _key(bulk) == {k for k in _key(loop) if k[1] in want}
        assert _key(bulk), "the fixture would prove nothing if both sides were empty"

    def test_quarterly_rows_are_the_same_rows(self, earnings):
        loop = [r for cid in (1, 2) for r in earnings._ttm_metric_rows(cid)]
        bulk = earnings._bulk_blend_rows([1, 2], ["revenue"], "quarterly")
        assert _key(bulk) == {k for k in _key(loop) if k[1] == _ANNUAL_REVENUE}
        assert _key(bulk)

    def test_a_ttm_row_is_emitted_under_the_ANNUAL_code(self, earnings):
        # The charts select their line by `annuals__…`; the cadence is a property of the request.
        # Emit the quarterly spelling and every card goes blank while claiming the index has no data.
        bulk = earnings._bulk_blend_rows([1], ["revenue"], "quarterly")
        assert bulk, "eight quarters is two TTM years"
        assert {r["metric_code"] for r in bulk} == {_ANNUAL_REVENUE}
        assert _Q_REVENUE not in {r["metric_code"] for r in bulk}

    def test_an_unnamed_metric_is_not_read(self, earnings):
        # Forward P/E is in the table and is NOT asked for. A narrowing that widened would show up
        # only here — every other assertion compares like with like.
        bulk = earnings._bulk_blend_rows([1, 2], ["revenue"], "annual")
        assert all(r["metric_code"] == _ANNUAL_REVENUE for r in bulk)

    def test_a_metric_with_no_ttm_rule_is_omitted_not_guessed(self, earnings, monkeypatch):
        # `_codes_and_rule` returns (None, None) rather than falling back to a rule. Summing a
        # balance over four quarters would report a company with four times its assets, and
        # nothing on the resulting chart would look wrong.
        monkeypatch.setitem(earnings._METRIC_CODES, "made_up", ("annuals__X__Y",))
        assert earnings._codes_and_rule("made_up", "quarterly") == (None, None)
        assert earnings._bulk_blend_rows([1], ["made_up"], "quarterly") == []


class TestASynthesisedRowCarriesWhatTheReadItReplacesCarried:
    def test_ttm_rows_have_a_company_id(self, earnings):
        # The regression: `_blend_rows` keys every point by company, so a missing id is a 500 on
        # the portfolio path and invisible on the single-company one.
        rows = earnings._ttm_metric_rows(1)
        assert rows
        assert all(r.get("company_id") == 1 for r in rows)

    def test_the_blend_survives_quarterly_rows(self, earnings):
        # End to end over the pure part: the shape `_blend_rows` needs, from the TTM loader.
        covered = [{"company_id": 1, "weight_pct": 60.0}, {"company_id": 2, "weight_pct": 40.0}]
        rows = [r for cid in (1, 2) for r in earnings._ttm_metric_rows(cid)]
        out = earnings._blend_rows(rows, covered)          # must not raise
        assert isinstance(out, dict)
