"""The /benchmarks coverage table on two bases — and the ways a basis goes wrong quietly.

The table says which raw GuruFocus lines we hold per constituent and over what periods. It now
takes a cadence, because the Long Equity tab plots two of them and "do we have the data" has a
different answer on each: measured 2026-08-04, 264 SP500 constituents carry annual Free Cash Flow
and 263 carry the quarterly line. One row. There was no way to see that short of opening a chart.

Two failure modes are worth pinning, because neither shows up as an error:

⚠ THE WRONG SPELLING READS AS AN EMPTY TABLE. The quarterly rows live under `quarterly__…` codes;
    ask for the annual spelling and every cell is a dash, which is indistinguishable from a
    company nobody has fetched.

⚠ A PERIOD LABEL FROM ONE BASIS UNDER THE OTHER'S HEADING IS A SILENT LIE. "2025" and "2025-Q3"
    are different claims about what we hold, so the response echoes the cadence it answered, and
    an unrecognised value resolves to a real basis rather than to nothing.
"""
from __future__ import annotations

import pytest

from tests._fake_supabase import FakeSupabase

_A_FCF = "annuals__Cashflow Statement__Free Cash Flow"
_Q_FCF = "quarterly__Cashflow Statement__Free Cash Flow"


def _rows() -> list[dict]:
    """Company 1: three fiscal years annual, eight quarters quarterly.
    Company 2: the annual line ONLY — the real asymmetry this toggle exists to surface."""
    out = []
    for year in (2023, 2024, 2025):
        out.append({"company_id": 1, "metric_code": _A_FCF,
                    "target_date": f"{year}-12-31", "numeric_value": 100.0})
        out.append({"company_id": 2, "metric_code": _A_FCF,
                    "target_date": f"{year}-12-31", "numeric_value": 50.0})
        for month in ("03-31", "06-30", "09-30", "12-31"):
            if year == 2023:
                continue
            out.append({"company_id": 1, "metric_code": _Q_FCF,
                        "target_date": f"{year}-{month}", "numeric_value": 25.0})
    return out


@pytest.fixture
def fund(monkeypatch):
    from routers import _benchmark_fundamentals as bf
    from routers import earnings as e

    monkeypatch.setattr(e, "supabase", FakeSupabase({"metric_data": _rows()}))
    return bf


class TestTheCadenceReachesTheRead:
    def test_annual_reports_fiscal_years(self, fund):
        got = fund.constituent_fundamentals([1, 2])
        assert got[1]["fcf"] == {"from": "2023", "to": "2025", "n": 3}
        assert got[2]["fcf"] == {"from": "2023", "to": "2025", "n": 3}

    def test_quarterly_reports_TTM_periods_not_fiscal_years(self, fund):
        got = fund.constituent_fundamentals([1, 2], "quarterly")
        # ⚠ EIGHT QUARTERS IS FIVE TTM POINTS, NOT EIGHT. A trailing year needs four quarters, so
        # the first three produce nothing — the span starts three quarters in, by construction.
        assert got[1]["fcf"] == {"from": "2024-Q4", "to": "2025-Q4", "n": 5}

    def test_a_company_with_only_the_annual_line_is_ABSENT_on_quarterly(self, fund):
        # The whole point of the toggle. On the annual tab company 2 looks complete; on the
        # quarterly one it is not there at all, and that gap is a fact about our ingest.
        got = fund.constituent_fundamentals([1, 2], "quarterly")
        assert 1 in got
        assert 2 not in got, "an annual-only company must not look quarterly-covered"

    def test_no_companies_is_an_empty_answer_not_a_read(self, fund):
        assert fund.constituent_fundamentals([], "quarterly") == {}


class TestTheAnsweredBasisIsTheReportedBasis:
    @pytest.mark.parametrize("asked,expected", [
        ("quarterly", "quarterly"),
        ("annual", "annual"),
        # Anything else resolves to a REAL basis: `_metrics_by_company` would answer an unknown
        # cadence with the annual codes anyway, so echoing the raw string would put annual spans
        # under a heading naming something else.
        ("Quarterly", "annual"),
        ("", "annual"),
        ("ttm", "annual"),
    ])
    def test_the_cadence_is_normalised_before_it_is_echoed(self, asked, expected):
        from routers._benchmark_fundamentals import normalise_cadence

        assert normalise_cadence(asked) == expected

    def test_an_absent_cadence_is_annual(self):
        from routers._benchmark_fundamentals import normalise_cadence

        assert normalise_cadence(None) == "annual"
