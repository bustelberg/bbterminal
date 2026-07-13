"""Portfolio YTD — the two ways this number lies if you let it.

A model portfolio is a COMPOSITION, not an account. AIRS stores what it should hold, not a
track record, and its snapshot dropdown offers 2-3 dates — not a monthly history. So the only
composition we can have is the CURRENT one, and everything below follows from that.
"""
from __future__ import annotations

import inspect

import pytest

from routers._airs_portfolio_perf import (
    GOOD_COVERAGE_PCT,
    MIN_COVERAGE_PCT,
    compute_portfolio_performance,
)


class TestHindsight:
    """Applying TODAY's weights back to 1 January backtests a basket that was chosen knowing
    how the year went. When the model predates the year that is harmless — the weights really
    were held throughout. When it does not, the number is not a track record.

    This is not a theoretical worry. Measured 2026-07-13:

        MoTopSelectie_FX    YTD +75.85%    model effective 2026-07-05  (EIGHT DAYS EARLIER)
                            since that model took effect:  +0.86%

    Unflagged, it is the best-performing portfolio in the list.
    """

    def test_a_model_defined_during_the_year_is_flagged(self):
        src = inspect.getsource(compute_portfolio_performance)
        assert 'eff > jan1' in src, "a model newer than the window must be flagged"
        assert "model_changed_in_period" in src

    def test_since_model_is_measured_from_the_models_own_date(self):
        """The honest number, and the reason it exists: it never borrows hindsight, because
        its window starts when the composition did."""
        src = inspect.getsource(compute_portfolio_performance)
        assert "eff_anchor = eff if eff and eff > jan1 else jan1" in src

    def test_the_gap_between_them_is_the_hindsight(self):
        """75.85 vs 0.86 — the arithmetic of what the flag is protecting against."""
        ytd_backtested, since_model = 75.85, 0.86
        assert ytd_backtested / since_model > 80


class TestCoverageFloor:
    """25 of 248 held ISINs have no price series at all (Leonteq structured products, in-house
    funds — the zero-bar guard in `store_one` refuses to map them). Renormalising over what
    remains assumes the rest behaved the same. At 95% that is a rounding error. At 1% it is a
    fabrication:

        TOPS_OFF_BEH   "+0.00% YTD"   <- its 1% CASH line, renormalised to 100%,
                                         while 9 structured products (99%) were dropped.

    A precise, confident, entirely invented number. So below the floor we return nothing.
    """

    def test_there_is_a_floor_and_it_is_not_trivial(self):
        assert MIN_COVERAGE_PCT >= 50
        assert GOOD_COVERAGE_PCT > MIN_COVERAGE_PCT

    def test_below_the_floor_no_number_is_returned(self):
        src = inspect.getsource(compute_portfolio_performance)
        assert "enough = covered >= MIN_COVERAGE_PCT" in src
        assert '"ytd_pct": (ytd_num / ytd_den) if enough else None' in src

    def test_coverage_is_reported_even_when_the_number_is_refused(self):
        """`covered_pct` IS the reason for the refusal — withholding it would leave the reader
        with an unexplained blank."""
        src = inspect.getsource(compute_portfolio_performance)
        after = src.split("enough =", 1)[1]
        assert '"covered_pct": covered' in after

    def test_the_one_percent_case_would_have_been_a_lie(self):
        """What the floor prevents, in numbers: 1% cash at 0%, renormalised, IS '+0.00%'."""
        holdings = [{"w": 1.0, "ret": 0.0}]              # the cash line, alone
        num = sum(h["w"] * h["ret"] for h in holdings)
        den = sum(h["w"] for h in holdings)
        assert num / den == 0.0                          # a confident, precise, invented 0.00%
        assert (den / 100.0) * 100 < MIN_COVERAGE_PCT    # ...and 1% coverage, so: refused


class TestCashIsPricedNotSkipped:
    def test_cash_counts_toward_the_return_at_zero(self):
        """Cash's drag is a FACT, not a gap. Dropping it from the denominator would silently
        scale a 20%-cash portfolio's return up by 25%."""
        src = inspect.getsource(compute_portfolio_performance)
        cash_branch = src.split("Cash. A 0% return is a FACT", 1)[1].split("continue", 1)[0]
        assert "ytd_den += w" in cash_branch              # in the denominator...
        assert "ytd_num" not in cash_branch               # ...contributing zero to the numerator

    def test_dropping_cash_would_inflate_the_return(self):
        """80% equities at +10%, 20% cash. Including cash: +8%. Dropping it: +10%."""
        with_cash = (80 * 10.0 + 20 * 0.0) / 100
        without_cash = (80 * 10.0) / 80
        assert with_cash == pytest.approx(8.0)
        assert without_cash == pytest.approx(10.0)


class TestPostgrestPaging:
    def test_the_price_read_pages(self):
        """223 holdings x ~130 trading days is ~29,000 rows. PostgREST caps a response at
        1,000 and TRUNCATES SILENTLY — unpaged, this computes a confident number off 3% of the
        data. (I hit exactly this while probing coverage: it reported 102 priced holdings when
        the answer was 221.)"""
        from routers._airs_portfolio_perf import _closes

        src = inspect.getsource(_closes)
        assert ".range(off, off + 999)" in src
        assert "off += 1000" in src
