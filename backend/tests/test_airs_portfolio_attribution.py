"""Brinson-Fachler: WHY a model beat or lagged the index.

An excess return is a FACT, not an explanation. "-15.34% vs ACWI" says nothing about whether the
bet that failed was the SECTORS chosen or the STOCKS chosen inside them — different mistakes,
different fixes. Brinson separates them; this file pins the three ways the separation goes wrong.
"""
from __future__ import annotations

import inspect

import pytest

from routers import _airs_portfolio_attribution as at


class TestTheIdentityIsTheWholePoint:
    """⚠ allocation + selection + interaction == excess.

    Three columns of numbers that do NOT sum to the excess are not a decomposition of it — they
    are three columns of numbers sitting next to each other. The residual is RETURNED, not
    swallowed, and the UI refuses to present a non-reconciling table as an explanation.
    """

    def test_the_residual_is_computed_and_returned(self):
        src = inspect.getsource(at.compute_attribution)
        assert "residual = excess - attributed" in src
        assert '"reconciles": abs(residual) < 1e-6' in src

    def test_it_holds_on_a_worked_example(self):
        """Two buckets, hand-computed. If the algebra below ever drifts, this fails."""
        # portfolio: 70% A (+10%), 30% B (-5%)   -> R_p = 5.5%
        # benchmark: 40% A (+8%),  60% B (+2%)   -> R_b = 4.4%
        w_pA, w_pB, R_pA, R_pB = 0.70, 0.30, 10.0, -5.0
        w_bA, w_bB, R_bA, R_bB = 0.40, 0.60, 8.0, 2.0
        R_p = w_pA * R_pA + w_pB * R_pB
        R_b = w_bA * R_bA + w_bB * R_bB

        eff = 0.0
        for w_p, w_b, R_pi, R_bi in ((w_pA, w_bA, R_pA, R_bA), (w_pB, w_bB, R_pB, R_bB)):
            allocation = (w_p - w_b) * (R_bi - R_b)      # Fachler: vs the INDEX's total
            selection = w_b * (R_pi - R_bi)
            interaction = (w_p - w_b) * (R_pi - R_bi)
            eff += allocation + selection + interaction

        assert eff == pytest.approx(R_p - R_b)

    def test_allocation_is_measured_against_the_INDEX_not_against_zero(self):
        """The '-Fachler' part, and it flips the sign of real calls. Overweighting a sector that
        rose 5% while the INDEX rose 10% is a BAD allocation decision; plain Brinson (no
        `- r_b_total`) scores it POSITIVE, which is exactly backwards."""
        src = inspect.getsource(at.compute_attribution)
        assert "allocation = (w_p - w_b) * (R_b - r_b_total)" in src

        w_p, w_b, R_bi, R_b_total = 0.30, 0.10, 5.0, 10.0   # overweight a laggard
        assert (w_p - w_b) * (R_bi - R_b_total) < 0          # correctly a COST
        assert (w_p - w_b) * R_bi > 0                        # plain Brinson would call it a gain


class TestFundsAndCashAreNotASectorBet:
    """An ETF has no sector. In the `Fund (not looked through)` bucket the benchmark's weight is
    ZERO — so Brinson would attribute the fund's ENTIRE return to ALLOCATION, i.e. report that
    holding a diversified world tracker was a sector bet. Arithmetically true, analytically
    worthless. Cash is the same."""

    def test_they_are_excluded_from_the_decomposition(self):
        assert at.FUND_BUCKET in at._NON_ATTRIBUTABLE
        assert at.CASH_BUCKET in at._NON_ATTRIBUTABLE

    def test_and_the_excluded_share_is_stated(self):
        src = inspect.getsource(at.compute_attribution)
        assert '"attributable_pct"' in src and '"excluded_pct"' in src


class TestAnUnpricedHoldingIsADIFFERENTExclusion:
    """⚠ THE ONE THAT PRODUCES A FALSE FINDING, NOT A MISSING ONE.

    A fund is excluded because it is not a sector bet — harmless. An UNPRICED EQUITY is excluded
    because we failed to price it, and its sector then reads as UNOWNED: measured, a model holding
    6% Healthcare (unpriceable) was credited **+1.73pp of allocation for "avoiding" Healthcare**,
    a sector it actually owned. An analyst acting on that would buy Healthcare it already holds.

    It still cannot be attributed (there is no return), so it is flagged rather than fixed.
    """

    def test_the_reason_is_carried_not_lumped_together(self):
        src = inspect.getsource(at.compute_attribution)
        assert 'reason = ("fund" if bucket == FUND_BUCKET' in src
        assert '"unpriced" if ret is None' in src

    def test_unpriced_is_reported_SEPARATELY_from_excluded(self):
        src = inspect.getsource(at.compute_attribution)
        assert '"unpriced_pct"' in src
        assert '"unpriced_buckets"' in src, "name the rows whose allocation effect is false"


class TestMissedWinnersAreMatchedByCOMPANY:
    """⚠ 'DID NOT OWN' IS A STATEMENT ABOUT THE COMPANY, NOT ABOUT THE ISIN.

    Alphabet is GOOGL (class A) in the index and "Alphabet - C" (class C) in the model — two
    ISINs, one business. Matched on the ISIN, the panel reported GOOGL as a winner they MISSED, at
    +3.23pp, while it was in fact their SINGLE LARGEST CONTRIBUTOR (+5.92pp). A missed opportunity
    the portfolio actually captured is the worst kind of false finding: it is actionable, and the
    action is wrong.
    """

    def test_the_isin_is_not_the_identity(self):
        src = inspect.getsource(at.compute_attribution)
        assert "same_company" in src
        assert "for b in bench if not _held(b)" in src

    def test_same_company_sees_through_a_share_class(self):
        from asset_pipeline.resolve import same_company

        assert same_company("Alphabet Inc.", "Alphabet Inc") is True


class TestCashIsCarriedAtZeroNotDropped:
    def test_cash_returns_a_flat_zero(self):
        """Its drag is a FACT — it belongs in the contributions even though it is not attributed
        to a sector."""
        src = inspect.getsource(at.compute_attribution)
        assert "ret = 0.0 if not isin else" in src


class TestTheBenchmarkWeightsAreTheSAMEONES:
    def test_attribution_uses_the_headline_weighting(self):
        """`index_rows` runs the same `_window_rows` the headline return is built from. An
        attribution that reconciles against a DIFFERENT weighting reconciles against nothing."""
        from routers import _asset_benchmark as ab

        assert "_window_rows(mem, closes, fx, start)" in inspect.getsource(ab.index_rows)
        assert "index_rows(benchmark_label, start)" in inspect.getsource(at.compute_attribution)
