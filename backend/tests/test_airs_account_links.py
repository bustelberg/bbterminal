"""Pairing an AIRS account to the model it runs.

Every name below is real, taken from AirSPMS on 2026-07-17. The failure that matters here is
not a missed link — it is a CONFIDENT WRONG one, because nobody re-checks a match that looks
right, and the wrong risk profile holds nearly the same instruments.
"""
from __future__ import annotations

from routers._airs_account_links import _stem, guess_model


def M(name, positions=20, id=None):
    return {"id": id if id is not None else abs(hash(name)) % 10000, "name": name,
            "positions": positions}


# The real model list, in the four naming conventions AIRS actually uses.
_MODELS = [
    M("AITopSelectie OFF FX", 20),
    M("StarTopSelectie OFF FX", 24),
    M("VTopSelectie OFF FX", 29),
    M("AztopSelectie_FX", 21),
    M("BUS_Bep_offensief_FX", 41),
    M("BUS_MTS_OFF_AFS", 20), M("BUS_MTS_DEF_AFS", 22),
    M("BUS_FTS_OFF_AFS", 25), M("BUS_FTS_DEF_AFS", 27),
    M("BUS_FTS_NEU_AFS", 27), M("BUS_FTS_Bepoff_AFS", 27),
    M("TOPS_OFF_BEH", 10), M("TOPS_DEF_BEH", 12),
    M("BUS_BM_AAND_kw_EUR_2026", 1),      # a real single-ETF benchmark model
    M("TOPS_AZTS_L", 1),                  # a one-line AMC wrapper
]


class TestTheFourNamingConventions:
    """The pairing is not a rule. Each of these is a different one, and all are real."""

    def test_suffix_swapped_fx_to_dyn(self):
        m, _ = guess_model("AITopSelectie OFF DYN", _MODELS)
        assert m["name"] == "AITopSelectie OFF FX"

    def test_suffix_appended_afs_to_afs_dyn(self):
        m, _ = guess_model("BUS_MTS_OFF_AFS_DYN", _MODELS)
        assert m["name"] == "BUS_MTS_OFF_AFS"

    def test_suffix_replaced_afs_to_dyn(self):
        """`BUS_FTS_OFF_DYN` <-> `BUS_FTS_OFF_AFS`. The convention that broke the first
        matcher, which reported all four FTS accounts as having no ISIN source at all while
        each had a 27-ISIN model."""
        m, _ = guess_model("BUS_FTS_OFF_DYN", _MODELS)
        assert m["name"] == "BUS_FTS_OFF_AFS"

    def test_separators_and_case_do_not_matter(self):
        assert guess_model("AzTopSelectie_DYN", _MODELS)[0]["name"] == "AztopSelectie_FX"
        assert guess_model("BUS_BepOffensief_Dyn", _MODELS)[0]["name"] == "BUS_Bep_offensief_FX"

    def test_the_account_missing_its_n_still_pairs(self):
        """`VTopSelectie OFF DY` — 19 chars, so not AIRS's 24-char truncation. A typo."""
        m, _ = guess_model("VTopSelectie OFF DY", _MODELS)
        assert m["name"] == "VTopSelectie OFF FX"


class TestItCannotConfuseTwoRiskProfiles:
    """⚠ THE failure mode. The holdings cannot separate these — BUS_FTS_Bepoff/DEF/NEU_AFS hold
    the IDENTICAL 27 ISINs (27 of 27, all three pairs) — so the name is the only discriminator
    that exists, and a loose match here silently measures a book against another strategy."""

    def test_each_risk_profile_pairs_with_its_own_model(self):
        for acct, model in (("BUS_FTS_OFF_DYN", "BUS_FTS_OFF_AFS"),
                            ("BUS_FTS_DEF_DYN", "BUS_FTS_DEF_AFS"),
                            ("BUS_FTS_NEU_DYN", "BUS_FTS_NEU_AFS"),
                            ("BUS_FTS_BEPOFF_DYN", "BUS_FTS_Bepoff_AFS")):
            assert guess_model(acct, _MODELS)[0]["name"] == model

    def test_the_risk_token_is_never_stripped_as_a_suffix(self):
        """`OFF` looks like a venue suffix and is not — it is Offensief. Strip it and every
        profile of a strategy collapses onto one stem."""
        assert _stem("BUS_FTS_OFF_DYN") != _stem("BUS_FTS_DEF_DYN")
        assert _stem("AITopSelectie OFF FX").endswith("off")

    def test_a_near_name_is_refused_not_approximated(self):
        """`BUS_WTS_StMerken_Dyn` vs `BUS_WTS_SterkeMerken_Fx` — the same strategy, abbreviated.
        A fuzzy matcher pairs them; this one must not, because the same looseness pairs
        DEF with NEU. A refusal costs one manual link."""
        m, why = guess_model("BUS_WTS_StMerken_Dyn", [M("BUS_WTS_SterkeMerken_Fx", 17)])
        assert m is None
        assert "no model has the stem" in why


class TestTheGuessRefusesRatherThanApproximate:
    def test_a_mangled_word_gets_no_guess(self):
        """`BUS_BM_AAN_kw_EUR_2026_d` <-> `BUS_BM_AAND_kw_EUR_2026`: AIRS drops the D from the
        word and adds it back as a suffix. Not learnable — so, no guess."""
        m, why = guess_model("BUS_BM_AAN_kw_EUR_2026_d", _MODELS)
        assert m is None
        assert "no model has the stem" in why

    def test_two_models_on_one_stem_is_an_ambiguity_not_a_tie(self):
        dupes = [M("Foo_FX", 20, id=1), M("Foo_AFS", 20, id=2)]
        m, why = guess_model("Foo_DYN", dupes)
        assert m is None
        assert "ambiguous" in why and "Foo_AFS" in why and "Foo_FX" in why

    def test_no_stem_match_is_no_guess(self):
        assert guess_model("TOPS_VTS_L", _MODELS)[0] is None


class TestTheTwoGatesThatMakeTheNameUsable:
    """Both caught on the first run over real data — each produced a perfect-looking match."""

    def test_an_account_never_links_to_itself(self):
        """⚠ `TOPS_AZTS_L` is BOTH an account and a one-line model row, so it matched itself
        at a perfect score: "this account runs itself" — no information, wearing certainty.
        Same cycle `_airs_portfolio_links` hit, where a certificate's best name match was the
        wrapper holding it."""
        m, why = guess_model("TOPS_AZTS_L", _MODELS)
        assert m is None
        assert "itself" in why

    def test_a_one_position_model_is_a_wrapper_not_a_strategy(self):
        m, why = guess_model("TOPS_AZTS_XX", [M("TOPS_AZTS_XX_FX", 1)])
        assert m is None
        assert "wrapper" in why

    def test_a_real_model_is_still_matched_when_a_wrapper_shares_its_stem(self):
        """The wrapper is skipped, not the whole stem."""
        models = [M("Bar_FX", 24, id=1), M("Bar_AFS", 1, id=2)]
        m, _ = guess_model("Bar_DYN", models)
        assert m["name"] == "Bar_FX"
