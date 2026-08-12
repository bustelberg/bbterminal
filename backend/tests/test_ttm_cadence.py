"""The trailing-twelve-month roll-up must span twelve months — for the company in front of it.

⚠ THIS IS A CONFIDENT-WRONG-NUMBER TEST, NOT A CRASH TEST. `_ttm_by_period` used to take the last
FOUR ROWS whatever their spacing, on the assumption that a `quarterly__…` code holds quarters. For
a SEMI-ANNUAL filer that is four half-years, so the "TTM" line reported two years of revenue:
measured on Prosus NV (2026-08-12) the tab showed **13,983.9** against an FY2026 annual figure of
**8,394.8** — 1.67x, a plausible number with nothing on screen to say it spanned two years. Every
quarterly-mode card on the Long Equity tab read off it.

The frontend's /earnings dashboard already had this guard (`isQuarterlyCadence`, same 120-day
rule); the backend seam did not. These cases pin the shape both halves now agree on.

Pure — no DB, no network. The real numbers below were read off `metric_data` for company 3388
(Prosus, XAMS:PRX) and 115 (ASML) and are the regression anchors.
"""
from __future__ import annotations

import pytest

from routers.earnings import _ttm_by_period, filings_per_year


def rows(pairs):
    return [{"target_date": d, "numeric_value": v} for d, v in pairs]


QUARTERLY = ["2024-03-31", "2024-06-30", "2024-09-30", "2024-12-31", "2025-03-31"]
SEMI = ["2024-03-31", "2024-09-30", "2025-03-31", "2025-09-30"]
ANNUAL = ["2022-12-31", "2023-12-31", "2024-12-31"]


class TestCadenceDetection:
    def test_quarterly(self):
        assert filings_per_year(QUARTERLY) == 4

    def test_semi_annual(self):
        assert filings_per_year(SEMI) == 2

    def test_annual_filed_into_the_quarterly_codes(self):
        assert filings_per_year(ANNUAL) == 1

    def test_one_filing_carries_no_spacing_and_is_refused(self):
        # ⚠ NOT ASSUMED QUARTERLY. Guessing 4 here is what turns three months into "a year".
        assert filings_per_year(["2024-12-31"]) == 0
        assert filings_per_year([]) == 0

    def test_a_duplicate_date_does_not_reclassify_the_company(self):
        # The median, not the mean: one repeated date leaves the cadence alone.
        assert filings_per_year(["2024-03-31", "2024-03-31", "2024-06-30", "2024-09-30",
                                 "2024-12-31"]) == 4


class TestTheWindowIsAYearOfFilings:
    def test_quarterly_sums_four_quarters(self):
        out = _ttm_by_period(rows(zip(QUARTERLY, [1, 2, 3, 4, 5], strict=False)), "sum")
        assert out == {"2024-Q4": 10.0, "2025-Q1": 14.0}

    def test_semi_annual_sums_TWO_halves_not_four(self):
        # ⚠ THE BUG THIS FILE EXISTS FOR. Four rows here is 24 months.
        out = _ttm_by_period(rows(zip(SEMI, [10, 20, 30, 40], strict=False)), "sum")
        assert out == {"2024-Q3": 30.0, "2025-Q1": 50.0, "2025-Q3": 70.0}

    def test_prosus_ties_out_to_its_own_annual_figure(self):
        # The real filings. H1 3,086.796 + H2 5,260.93 = 8,347.726 against an FY2026 annual of
        # 8,394.825 — the small gap is GuruFocus's own restatement, not our arithmetic. Before the
        # fix this point read 13,983.864.
        out = _ttm_by_period(rows([
            ("2024-03-31", 2678.12), ("2024-09-30", 2669.663),
            ("2025-03-31", 2966.475), ("2025-09-30", 3086.796), ("2026-03-31", 5260.93),
        ]), "sum")
        assert out["2026-Q1"] == pytest.approx(8347.726)
        assert out["2026-Q1"] != pytest.approx(13983.864)

    def test_an_annual_only_filer_reports_its_year_unchanged(self):
        out = _ttm_by_period(rows(zip(ANNUAL, [5, 6, 7], strict=False)), "sum")
        assert out == {"2022-Q4": 5.0, "2023-Q4": 6.0, "2024-Q4": 7.0}

    def test_a_missing_filing_leaves_a_HOLE_not_a_bigger_number(self):
        # 2023-12-31 never filed. The four rows ending 2024-Q1 span a full year and would sum two
        # Q1s and no Q4 — still four rows, still looks right. Only the windows clear of the hole
        # survive.
        out = _ttm_by_period(rows([
            ("2023-03-31", 1), ("2023-06-30", 1), ("2023-09-30", 1),
            ("2024-03-31", 1), ("2024-06-30", 1), ("2024-09-30", 1), ("2024-12-31", 1),
            ("2025-03-31", 1),
        ]), "sum")
        assert set(out) == {"2024-Q4", "2025-Q1"}

    def test_no_point_before_a_full_year_exists(self):
        # A partial first window would start at a quarter of the level and "grow" 4x.
        assert _ttm_by_period(rows(zip(QUARTERLY[:3], [1, 2, 3], strict=False)), "sum") == {}

    def test_one_row_yields_nothing(self):
        assert _ttm_by_period(rows([("2024-12-31", 9)]), "sum") == {}


class TestTheOtherTwoRules:
    def test_mean_divides_by_the_window_it_used(self):
        # ⚠ A HARDCODED 4 HALVED A SEMI-ANNUAL FILER'S ALREADY-ANNUALISED RATE — the quieter twin
        # of the sum's doubling, because 6% instead of 12% is still a believable margin.
        out = _ttm_by_period(rows([("2024-03-31", 10), ("2024-09-30", 20),
                                   ("2025-03-31", 30)]), "mean")
        assert out == {"2024-Q3": 15.0, "2025-Q1": 25.0}

    def test_last_takes_the_window_end(self):
        out = _ttm_by_period(rows(zip(QUARTERLY, [1, 2, 3, 4, 5], strict=False)), "last")
        assert out == {"2024-Q4": 4.0, "2025-Q1": 5.0}


class TestLabelling:
    def test_key_date_keeps_the_real_period_end(self):
        # An off-calendar filer's quarter must not be snapped onto 03-31/06-30/09-30/12-31.
        out = _ttm_by_period(rows(zip(SEMI, [1, 1, 1, 1], strict=False)), "sum", key="date")
        assert list(out) == ["2024-09-30", "2025-03-31", "2025-09-30"]

    def test_the_latest_observation_wins_for_a_repeated_period(self):
        out = _ttm_by_period(rows([("2024-03-31", 1), ("2024-09-30", 2), ("2024-09-30", 5),
                                   ("2025-03-31", 1)]), "sum")
        assert out["2025-Q1"] == 6.0
