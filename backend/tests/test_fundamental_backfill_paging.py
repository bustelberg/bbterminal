"""The fundamentals coverage probe must PAGE, and the reason is a measured incident.

`_has()` answers one boolean per company — "does it carry this metric code at all" — and it
batched 20 ids into a single `.limit(1000)` request. The reasoning was that twenty companies could
not exceed the cap, and for an ANNUAL line that is true: Free Cash Flow is ~28 rows per company,
so 560.

It is false for a quarterly INDICATOR series. `indicator_q_forward_pe_ratio` is **526 rows per
company**, so a chunk of twenty asks for 10,520 rows, PostgREST silently returns 1,000, and only
the first ~2 companies of every 20 are ever seen.

Measured on SP500 mid-backfill 2026-08-04: the probe returned **38** where the truth was **214**.
Nothing errored and no cell was blank — the number was simply wrong, and consistently so.

⚠ THE DAMAGE WAS NOT THE FIGURE ON SCREEN. `needs()` reads this to decide which GuruFocus feeds a
company is missing, so ~90% of the companies that ALREADY had indicators were marked as needing
them and the backfill re-fetched data it already held — one wasted API call each, against a
metered monthly quota. A cheap wrong answer here costs money.

So these tests do not check coverage logic. They check that the reader keeps asking until the
table is exhausted, against a fake that truncates the way the real one does.
"""
from __future__ import annotations

import pytest

from tests._fake_supabase import FakeSupabase

# Well under the real 1,000-row page, so a single unpaged request cannot accidentally satisfy
# these — the fake's cap stands in for PostgREST's.
_CAP = 50


def _rows(companies: int, per_company: int, code: str) -> list[dict]:
    """`companies` companies, each carrying `per_company` rows of `code`."""
    return [
        {"company_id": 100 + c, "metric_code": code,
         "target_date": f"{2000 + (i // 4)}-{(i % 4) * 3 + 1:02d}-01",
         "numeric_value": 1.0}
        for c in range(companies)
        for i in range(per_company)
    ]


@pytest.fixture
def _patched(monkeypatch):
    """`_has` reads the module-level `supabase`; hand it a truncating fake."""
    def _install(rows: list[dict]):
        from routers import _fundamental_backfill as fb

        fake = FakeSupabase({"metric_data": rows}, max_rows=_CAP)
        monkeypatch.setattr(fb, "supabase", fake)
        return fb
    return _install


class TestTheProbePages:
    def test_a_long_series_does_not_hide_the_companies_behind_it(self, _patched):
        # ⚠ THE REGRESSION, IN ITS EXACT SHAPE. 20 companies x 30 rows = 600 against a 50-row cap:
        # unpaged, the answer is the first ~2 companies. Paged, it is all twenty.
        code = "indicator_q_forward_pe_ratio"
        fb = _patched(_rows(companies=20, per_company=30, code=code))
        got = fb._has([100 + c for c in range(20)], code)
        assert got == {100 + c for c in range(20)}, (
            "companies past the first page were lost — the probe stopped at the cap")

    def test_a_short_series_still_works(self, _patched):
        # The annual case that always passed, kept so a future 'optimisation' cannot regress it.
        code = "annuals__Cashflow Statement__Free Cash Flow"
        fb = _patched(_rows(companies=20, per_company=2, code=code))
        assert fb._has([100 + c for c in range(20)], code) == {100 + c for c in range(20)}

    def test_a_company_with_none_of_the_code_is_absent(self, _patched):
        # Presence is the whole question; a company must not appear because a NEIGHBOUR has rows.
        code = "annual_pettm_estimate"
        rows = _rows(companies=3, per_company=40, code=code)
        fb = _patched(rows)
        got = fb._has([100, 101, 102, 999], code)
        assert 999 not in got
        assert got == {100, 101, 102}

    def test_another_metric_code_does_not_leak_in(self, _patched):
        # The filter is on an EXACT code — `.eq`, never `.like`. Metric codes contain both LIKE
        # wildcards (`%` in "ROE %", `_` in every one), so a pattern match would collect strangers.
        rows = _rows(2, 40, "indicator_q_forward_pe_ratio") + _rows(2, 40, "annual_pettm_estimate")
        fb = _patched(rows)
        # Both sets use the same synthetic ids, so a leak shows up as the WRONG code answering.
        assert fb._has([100, 101], "annual_pettm_estimate") == {100, 101}

    def test_an_empty_table_is_an_empty_answer_not_a_hang(self, _patched):
        # ⚠ The loop breaks on an EMPTY page, never on `len(page) < _PAGE` — the latter is only
        # correct while the server's cap is >= the page size, which is the assumption that failed.
        fb = _patched([])
        assert fb._has([100, 101], "anything") == set()
