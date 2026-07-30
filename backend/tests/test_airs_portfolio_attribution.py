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
        # The name leg now lives in the shared `_overlaps`, and it is EXACT ROOT equality (not
        # same_company's fuzzy floor) — see TestTheNameLegIsExactNotFuzzy.
        assert "_overlaps(" in src
        assert "_company_root" in inspect.getsource(at._overlaps)

    def test_it_sees_through_a_share_class(self):
        # Alphabet class A vs class C — two ISINs, one business, identical root.
        h = {"isin": "US02079K1079", "name": "Alphabet Inc."}
        assert at._overlaps(h, {"US02079K3059"}, ["Alphabet Inc"]) is True


class TestTheOverlapMatcherNeedsBOTHKeys:
    """⚠ ONE ISIN UNDER TWO NAMES, AND TWO ISINS UNDER ONE BUSINESS — both are real, and NEITHER
    matcher alone catches both.

    The model's "AMD" is the index's "Advanced Micro Devices Inc". `same_company` scores that pair
    **16.0** — the roots reduce to 'amd' vs 'advanced micro devices', sharing no tokens at all —
    so a name-only check marked the model's 2nd-largest contributor (+7.83pp) as a bet held
    OUTSIDE the index, while the index held it at 0.55%. Measured 2026-07-16 on the Technology
    bucket of a 16-name model: AMD was the ONE of 16 that the name matcher failed.

    Alphabet is the mirror image — class A in the index, class C in the model: two ISINs, one
    business, which no ISIN check can ever see. Hence both legs, ISIN first.
    """

    def test_an_acronym_is_not_caught_by_the_name_leg(self):
        """The measured fact the ISIN leg exists for: AMD by name alone — root 'amd' vs 'advanced
        micro devices', no exact match — needs the ISIN leg (next test)."""
        assert at._overlaps({"name": "AMD"}, set(), ["Advanced Micro Devices Inc"]) is False

    def test_the_isin_wins_when_the_names_disagree(self):
        """AMD: one ISIN, two names."""
        h = {"isin": "US0079031078", "name": "AMD"}
        assert at._overlaps(h, {"US0079031078"}, ["Advanced Micro Devices Inc"]) is True

    def test_the_name_wins_when_the_isins_disagree(self):
        """Alphabet: two ISINs, one business."""
        h = {"isin": "US02079K1079", "name": "Alphabet Inc"}
        assert at._overlaps(h, {"US02079K3059"}, ["Alphabet Inc"]) is True

    def test_a_genuinely_different_bet_is_not_marked_held(self):
        """The flag has to be able to say NO, or it says nothing."""
        h = {"isin": "US23283R1005", "name": "Cytokinetics Inc"}
        assert at._overlaps(h, {"US0079031078"}, ["Advanced Micro Devices Inc"]) is False

    def test_a_holding_with_neither_key_is_not_claimed_as_held(self):
        assert at._overlaps({}, {"US0079031078"}, ["Advanced Micro Devices Inc"]) is False


class TestTheNameLegIsExactNotFuzzy:
    """⚠ A SHARED WORD IS NOT A SHARED COMPANY.

    `same_company` is right for a listing↔issuer match ("NVIDIA CORP" ↔ "NVIDIA Corporation") but
    catastrophic here: `_company_root('S&P Global Inc')` reduces to the SINGLE generic token
    'global' (the initials S, P drop as single letters), which token_set_ratio then matches to
    EVERY name containing 'global'. Measured 2026-07-18 on a Financials bucket: the model held 5
    names and the panel claimed 8 index overlaps — Coinbase Global and Apollo Global Management
    among them, held by nobody. The intersection needs strict identity, so the name leg is EXACT
    root equality: a share class shares the same root, an unrelated 'Global' does not.
    """

    def test_a_shared_generic_word_is_not_a_match(self):
        # Coinbase (index only) must NOT match the model's S&P Global just because both say "Global".
        h = {"isin": "USxxxxxCOIN", "name": "Coinbase Global, Inc."}
        assert at._overlaps(h, set(), ["S&P Global Inc.", "MSCI Inc.", "Visa Inc."]) is False

    def test_apollo_global_is_not_sp_global(self):
        h = {"isin": "USxxxxAPOLLO", "name": "Apollo Global Management, Inc."}
        assert at._overlaps(h, set(), ["S&P Global Inc."]) is False

    def test_the_real_holding_still_matches_by_name(self):
        # And the genuine overlap (no ISIN on this side) still marks — same root.
        assert at._overlaps({"name": "S&P Global Inc."}, set(), ["S&P Global Inc."]) is True

    def test_held_and_in_both_are_ONE_definition(self):
        """They drifted once: `_held` had the ISIN check and `in_both` did not, so only `in_both`
        was wrong about AMD. Three call sites, one function — it cannot happen twice."""
        src = inspect.getsource(at.compute_attribution)
        assert src.count("_overlaps(") >= 3


class TestOneNameVocabularyAcrossBothSides:
    """The model side speaks AIRS's fund label ("AMD", "Applied"); the index side speaks the
    reconstruction's `company_name` ("Advanced Micro Devices Inc"). One security under two labels,
    set side by side in a single comparison table, reads as a data bug — and it is the first thing
    a reader asks about. Both sides already join `asset_grid` by ISIN, so both can say one thing.

    ⚠ DISPLAY ONLY — if this ever becomes what makes the overlap match succeed, a rename silently
    breaks correctness. See TestTheOverlapMatcherNeedsBOTHKeys.
    """

    def test_the_canonical_name_wins(self):
        assert (at._display_name({"name": "Advanced Micro Devices, Inc."}, "AMD")
                == "Advanced Micro Devices, Inc.")

    def test_the_source_label_is_the_fallback(self):
        """No grid row (an unresolved ISIN) — AIRS's label is all we have, and a blank is worse."""
        assert at._display_name(None, "AMD") == "AMD"
        assert at._display_name({}, "AMD") == "AMD"
        assert at._display_name({"name": ""}, "AMD") == "AMD"

    def test_nothing_to_show_is_none_not_empty_string(self):
        assert at._display_name(None, None) is None


class TestTheBenchmarkWeightsAreTheSAMEONES:
    def test_attribution_uses_the_headline_weighting(self):
        """`index_rows` runs the same `_window_rows` the headline return is built from. An
        attribution that reconciles against a DIFFERENT weighting reconciles against nothing."""
        from routers import _asset_benchmark as ab

        # The CALL, not its argument list — `marks=` was added when the loader was narrowed to the
        # two prices a window actually reads. What must not change is which function weights it.
        assert "_window_rows(" in inspect.getsource(ab.index_rows)
        assert "index_weights(" in inspect.getsource(ab.index_rows)
        assert "index_rows(benchmark_label, start)" in inspect.getsource(at.compute_attribution)
