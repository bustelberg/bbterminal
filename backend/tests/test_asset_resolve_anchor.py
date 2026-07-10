"""The OpenFIGI name anchor in `asset_pipeline.resolve`.

WHAT WENT WRONG
    `resolve()` ranks a company's cross-listings by liquidity, then sanity-checks
    the winner against the ISIN's OpenFIGI name — the guard that stops Yahoo's
    ISIN search from handing us a different company (SkyWater -> Micron).

    That guard compared names with a raw `rapidfuzz.token_set_ratio >= 80`.
    "NVIDIA Corporation" (Yahoo's longName) vs "NVIDIA CORP" (OpenFIGI) scores
    **75.9**, because the tokens `corporation` and `corp` don't match. So the
    guard concluded the most-liquid `NVDA` was a DIFFERENT company and swapped in
    `NVD.SG` — Stuttgart, EUR, €1.6M median daily traded value, against Nasdaq's
    €28,076M. NVIDIA's entire price panel came from a near-dead German line.

    `same_company()` strips corporate forms before scoring, gets every one of
    these right, and is what `identity_status()` already used. The anchor now uses
    it too, so the two can no longer disagree about the same pair of names.

These tests pin both directions: the suffix variants must MATCH, and the genuine
wrong-company cases must still be REJECTED. A raw-score guard cannot do both.
"""
from __future__ import annotations

import pytest

from asset_pipeline.resolve import _NAME_MATCH, _company_root, _name_score, same_company

# (yahoo longName, openfigi name) — the same issuer, written two ways.
SAME_COMPANY = [
    ("NVIDIA Corporation", "NVIDIA CORP"),
    ("Intel Corporation", "INTEL CORP"),
    ("Chevron Corporation", "CHEVRON CORP"),
    ("International Business Machines Corporation", "INTL BUSINESS MACHINES CORP"),
    ("Eli Lilly and Company", "LILLY(ELI) & CO"),
    ("Apple Inc.", "APPLE INC"),
    ("Alphabet Inc.", "ALPHABET INC-CL A"),
    ("Toyota Motor Corporation", "TOYOTA MOTOR CORP -SPON ADR"),
]

# Genuinely different issuers the anchor exists to catch.
DIFFERENT_COMPANY = [
    ("Micron Technology, Inc.", "SKYWATER TECHNOLOGY INC"),
    ("Grupo Financiero Galicia S.A.", "BANCO SANTANDER SA"),
    ("Qualcomm Incorporated", "CYTOKINETICS INC"),
]


class TestSameCompany:
    @pytest.mark.parametrize(("yahoo", "figi"), SAME_COMPANY)
    def test_corporate_form_variants_are_the_same_company(self, yahoo, figi):
        assert same_company(yahoo, figi), f"{yahoo!r} vs {figi!r}"

    @pytest.mark.parametrize(("yahoo", "figi"), DIFFERENT_COMPANY)
    def test_genuinely_different_issuers_are_rejected(self, yahoo, figi):
        assert not same_company(yahoo, figi), f"{yahoo!r} vs {figi!r}"

    def test_an_empty_name_cannot_be_judged_so_it_passes(self):
        """The anchor must not swap a listing away on missing metadata."""
        assert same_company(None, "NVIDIA CORP")
        assert same_company("NVIDIA Corporation", None)


class TestWhyTheRawScoreWasWrong:
    """Regression guard. If someone reverts the anchor to a raw `_name_score`
    floor, these assertions document exactly what breaks."""

    @pytest.mark.parametrize(("yahoo", "figi"), [
        ("NVIDIA Corporation", "NVIDIA CORP"),
        ("Intel Corporation", "INTEL CORP"),
        ("Chevron Corporation", "CHEVRON CORP"),
        ("Eli Lilly and Company", "LILLY(ELI) & CO"),
    ])
    def test_the_raw_score_fails_these_true_positives(self, yahoo, figi):
        assert _name_score(yahoo, figi) < _NAME_MATCH
        assert same_company(yahoo, figi)

    def test_stripping_corporate_forms_is_what_rescues_them(self):
        assert _company_root("NVIDIA Corporation") == _company_root("NVIDIA CORP") == "nvidia"
        assert _company_root("Toyota Motor Corporation") == "toyota motor"
        assert _company_root("TOYOTA MOTOR CORP -SPON ADR") == "toyota motor"

    def test_but_stripping_does_not_collapse_distinct_issuers(self):
        assert _company_root("Micron Technology, Inc.") != _company_root("SKYWATER TECHNOLOGY INC")
        assert _company_root("Qualcomm Incorporated") != _company_root("CYTOKINETICS INC")


class TestAnchorUsesSameCompany:
    def test_resolve_calls_same_company_not_the_raw_floor(self):
        """Cheap structural check: the anchor block must not compare `_name_score`
        against `_NAME_MATCH` directly. `same_company` already does that, on roots.
        """
        import inspect

        from asset_pipeline import resolve as mod

        src = inspect.getsource(mod.resolve)
        assert "same_company(chosen.get(\"name\"), figi_name)" in src
        assert "_name_score(chosen.get(\"name\"), figi_name) < _NAME_MATCH" not in src
