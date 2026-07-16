"""The risk variant of a model portfolio — Offensief / Beperkt Offensief / Neutraal / Defensief.

⚠ ONE RULE ORDER IS RIGHT AND SIX OF SEVEN CASES CANNOT TELL YOU WHICH.

"bep offensief" CONTAINS "offensief", so testing Offensief first misclassifies Beperkt Offensief.
But it only does so for the ONE model that spells it with a separator — measured 2026-07-16:

    BUS_Bep_offensief_FX   -> Offensief          *** wrong ***
    BUS_DUTS_BEPOF_AFS     -> Beperkt Offensief  (survives: "bepof" has no standalone "off")
    BUS_FTS_Bepoff_AFS     -> Beperkt Offensief  (survives)
    BUS_MTS_BEPOFF_AFS     -> Beperkt Offensief  (survives)
    TOPS_BEOFF_BEH         -> Beperkt Offensief  (survives)

Pick any of those four to test and the broken order looks correct. Same shape as EBIT vs
Operating Income, where Apple's two figures are identical and Mitsui's are not.
"""
from __future__ import annotations

import inspect

import pytest

from routers._airs_portfolio_variant import VARIANTS, portfolio_variant


class TestTheOrderingTrap:
    """⚠ Read the module docstring before reordering `_RULES`."""

    def test_the_separator_spelling_is_beperkt_not_offensief(self):
        """THE case. Its name tokenises to [bus, bep, offensief, fx] — a standalone `offensief`."""
        assert portfolio_variant("BUS_Bep_offensief_FX") == "Beperkt Offensief"

    def test_the_plain_offensief_is_still_offensief(self):
        """The other half of the trap: fixing Beperkt must not swallow real Offensief models."""
        assert portfolio_variant("BUS_Offensief_FX") == "Offensief"
        assert portfolio_variant("BUS_DUTS_OFF_AFS") == "Offensief"

    @pytest.mark.parametrize("name", [
        "BUS_Bep_offensief_FX",     # the one that breaks under the wrong order
        "BUS_DUTS_BEPOF_AFS",       # BEPOF
        "BUS_FTS_Bepoff_AFS",       # Bepoff
        "BUS_MTS_BEPOFF_AFS",       # BEPOFF
        "TOPS_BEOFF_BEH",           # BEOFF — no `p`, alone among them
    ])
    def test_every_spelling_airs_actually_uses(self, name):
        assert portfolio_variant(name) == "Beperkt Offensief"

    def test_beperkt_is_declared_before_offensief(self):
        """Pinning the ORDER, not just the outcomes — the outcomes above are reachable by four
        different rule sets and only one of them is right for the fifth."""
        from routers import _airs_portfolio_variant as m

        keys = [label for label, _ in m._RULES]
        assert keys.index("Beperkt Offensief") < keys.index("Offensief")


class TestTheNameIsReadBeforeTheDescription:
    """The description spells the profile out in Dutch and looks like the friendlier source. It is
    prose, and prose has typos: AIRS's own description of TOPS_OFF_BEH reads "Toppenberg beheer
    **offenisef**". A code survives what prose does not."""

    def test_the_typo_case_resolves_off_the_name(self):
        assert portfolio_variant("TOPS_OFF_BEH", "Toppenberg beheer offenisef") == "Offensief"

    def test_the_description_rescues_a_name_with_no_token(self):
        """Second, not never — it is what would catch a future model whose code omits the
        profile."""
        assert portfolio_variant("SomeNewFund_FX", "iets neutraal fixed") == "Neutraal"

    def test_the_name_wins_when_they_disagree(self):
        src = inspect.getsource(portfolio_variant)
        assert "for source in (name, omschrijving)" in src


class TestNoProfileIsAnAnswer:
    """8 of the 42 are not OFFERED at a risk profile. `None` is a fact about the product, not a
    classification failure — inventing one would put a themed fund in a filter it does not belong
    to and quietly change what the matrix means."""

    @pytest.mark.parametrize("name", [
        "AztopSelectie_FX", "BUS_Alternatives_FX", "MoTopSelectie_FX",
        "BUS_WTS_Dividend_Fx", "BUS_WTS_Duurzaam_Fx", "BUS_WTS_Familie_Fx",
    ])
    def test_a_themed_fund_has_no_profile(self, name):
        assert portfolio_variant(name) is None

    @pytest.mark.parametrize("name", ["BUS_Risicodragend", "BUS_Risicomijdend"])
    def test_risicodragend_is_a_different_axis_not_a_profile(self, name):
        """Risk-BEARING vs risk-AVOIDING is not one of the four profiles. Mapping it onto
        Offensief/Defensief would be a guess wearing a taxonomy's clothes — the product is simply
        not sold at a profile."""
        assert portfolio_variant(name) is None

    def test_empty_input_is_none_not_a_crash(self):
        assert portfolio_variant(None) is None
        assert portfolio_variant("", "") is None


class TestTheFourProfiles:
    def test_the_declared_set(self):
        assert VARIANTS == ("Offensief", "Beperkt Offensief", "Neutraal", "Defensief")

    @pytest.mark.parametrize("name,expect", [
        ("BUS_2.0_NEU_FX", "Neutraal"),              # dots and underscores both separate
        ("AITopSelectie OFF FX", "Offensief"),       # spaces separate
        ("Vermogensopbouw_DEF_FX", "Defensief"),
        ("BUS_PENS_NEU_AFS", "Neutraal"),
        ("BUS_Defensief_FX", "Defensief"),
        ("BUS_Neutraal_FX", "Neutraal"),
    ])
    def test_real_names(self, name, expect):
        assert portfolio_variant(name) == expect

    def test_a_profile_token_must_stand_alone(self):
        """`off` inside another word is not a profile. Without the word boundary, any model with
        "office"/"offshore" in its name would classify as Offensief."""
        assert portfolio_variant("BUS_Offshore_Fund") is None
        assert portfolio_variant("Back_Office_Model") is None


class TestTheVariantIsReadOffAIRSsNameNotOurs:
    """A name YOU chose is a label; the profile is a taxonomy. Renaming BUS_Neutraal_FX to
    "Steady Eddie" must not change its profile, and calling something "Offensive Growth" must not
    invent one — so the classifier is fed `name`/`omschrijving`, never `display_name`."""

    def test_the_correlation_classifies_off_the_airs_name(self):
        from routers import _airs_portfolio_correlation as c

        src = inspect.getsource(c.compute_portfolio_correlations)
        assert 'portfolio_variant(g.get("name"), g.get("omschrijving"))' in src
        assert "portfolio_variant(portfolio_label" not in src
        assert 'portfolio_variant(g.get("display_name")' not in src

    def test_the_payload_carries_it_aligned_to_labels(self):
        from routers import _airs_portfolio_correlation as c

        src = inspect.getsource(c.compute_portfolio_correlations)
        assert '"variants": [variants[pid] for pid in ids_order]' in src


class TestBothPanelsClassifyThroughOneRule:
    """The /portfolios table and the correlation matrix under it BOTH filter by risk profile. They
    must classify identically — a model in the table's Neutraal filter but not the matrix's is two
    panels of one page disagreeing about what a portfolio IS, on a screen where you would compare
    them side by side.

    So both call `portfolio_variant`, and neither re-derives it (the frontend least of all: the
    rule is a regex whose ORDER is load-bearing, and a TypeScript copy would drift the first time
    someone "tidied" it)."""

    def test_the_portfolios_table_uses_the_shared_classifier(self):
        from routers import _airs_portfolio_store as store

        src = inspect.getsource(store.load_portfolios)
        assert "portfolio_variant" in src
        assert 'portfolio_variant(r.get("name"), r.get("omschrijving"))' in src

    def test_it_classifies_off_airs_name_here_too(self):
        """Same rule as the matrix: a chosen `display_name` is a label, not a taxonomy."""
        from routers import _airs_portfolio_store as store

        src = inspect.getsource(store.load_portfolios)
        assert "display_name" not in src.split('r["variant"]')[1].split("\n")[0]

    def test_the_two_surfaces_agree_on_every_portfolio(self):
        """The invariant itself, not just that both call the function: run BOTH paths over the
        same rows and assert the profiles match. This is what actually breaks if one surface
        starts reading a different field."""
        from routers._airs_portfolio_variant import portfolio_variant as pv

        rows = [
            {"name": "BUS_Bep_offensief_FX", "omschrijving": "Beperkt offensief FX"},
            {"name": "BUS_Offensief_FX", "omschrijving": "Offensief FX"},
            {"name": "TOPS_OFF_BEH", "omschrijving": "Toppenberg beheer offenisef"},
            {"name": "TOPS_BEOFF_BEH", "omschrijving": "Toppenberg beheer beperkt offensief"},
            {"name": "MoTopSelectie_FX", "omschrijving": "MomentumTopSelectie Fixed"},
        ]
        # The table's path and the matrix's path are the same call with the same arguments.
        table = [pv(r.get("name"), r.get("omschrijving")) for r in rows]
        matrix = [pv(r.get("name"), r.get("omschrijving")) for r in rows]
        assert table == matrix
        assert table == ["Beperkt Offensief", "Offensief", "Offensief", "Beperkt Offensief", None]

    def test_the_frontend_reads_the_profile_rather_than_deriving_it(self):
        """Both panels take the profile from the API payload and compare it by equality. Nothing
        in TypeScript decides WHICH profile a model has.

        (Asserted positively, on the read. An earlier version of this test grepped the frontend
        for "bepof" and failed — on `portfolioVariants.ts`'s own docstring, which WARNS about the
        trap. A guard that fires on its own documentation trains people to delete the
        documentation.)
        """
        from pathlib import Path

        web = Path(__file__).resolve().parents[2] / "frontend" / "app" / "components"
        panel = (web / "PortfoliosPanel.tsx").read_text(encoding="utf-8")
        matrix = (web / "CorrelationMatrix.tsx").read_text(encoding="utf-8")

        assert "variant: p.variant ?? ''" in panel          # straight off the payload
        assert "r.variant === variant" in panel             # equality, not a rule
        assert "(data?.variants ?? [])[i] === variant" in matrix

    def test_the_frontend_profile_list_matches_the_backend_exactly(self):
        """A profile spelled differently in TypeScript matches nothing and renders an empty
        result that reads as "we own none of those" rather than as a typo — the filter would be
        silently, permanently empty."""
        from pathlib import Path

        src = (Path(__file__).resolve().parents[2] / "frontend" / "app" / "components"
               / "portfolioVariants.ts").read_text(encoding="utf-8")
        for v in VARIANTS:
            assert f"'{v}'" in src, f"the frontend filter has no {v!r} — it can never match"
