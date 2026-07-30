"""Guessing which model portfolio a holding IS.

Some AIRS positions are not instruments — they are other models, wrapped as a Leonteq
certificate so they can be held like a security. "Star Selection Index" (CH1381833321) is held
by 11 models and IS `StarTopSelectie OFF FX`.

Every test here is a case where the OBVIOUS matcher returns a confident wrong answer. That is
the failure mode that matters: a link nobody checks, pointing at the wrong strategy.
"""
from __future__ import annotations

import pytest

from routers._airs_portfolio_links import guess_link, is_topselectie, link_key, stem

# The real rows, as they are in the DB.
STAR_CERT = "Star Selection Index"
STAR_ISIN = "CH1381833321"

P_STAR_FX = {"id": 2094, "name": "StarTopSelectie OFF FX",
             "omschrijving": "StarTopSelectie Offensief fixed"}
P_STAR_DYN = {"id": 2095, "name": "StarTopSelectie OFF DYN",
              "omschrijving": "StarTopSelectie Offensief dynamisch"}
P_TOPS_STS = {"id": 2082, "name": "TOPS_STS_L", "omschrijving": "StarTopSelectie"}
P_OWNER = {"id": 1935, "name": "BUS_Offensief_FX", "omschrijving": "Offensief FX"}

ALL_STAR = [P_STAR_FX, P_STAR_DYN, P_TOPS_STS, P_OWNER]

# StarTopSelectie OFF FX holds 24 real stocks; TOPS_STS_L holds the CERTIFICATE at 100%; the
# DYN twin stores no composition at all; the owner holds the certificate among 32 rows.
COMP_STAR = {
    2094: [{"isin": f"X{i}", "fonds": f"stock {i}"} for i in range(24)],
    2095: [],
    2082: [{"isin": STAR_ISIN, "fonds": STAR_CERT}],
    1935: [{"isin": STAR_ISIN, "fonds": STAR_CERT}] + [{"isin": f"Y{i}", "fonds": f"s{i}"}
                                                       for i in range(31)],
}


def _guess(fonds, isin, owner, portfolios, comp):
    return guess_link(fonds=fonds, isin=isin, owner_id=owner, portfolios=portfolios,
                      composition=comp)


class TestTheWrapperTrap:
    """⚠ THE ONE THAT MAKES THE NAME SCORE UNUSABLE ON ITS OWN.

    `TOPS_STS_L`'s description is literally "StarTopSelectie" — the closest string to "Star
    Selection Index" in the whole list of 95. It is also the one answer that is definitely
    wrong: it HOLDS the certificate at 100%. It is a wrapper around the thing, not the thing.
    Follow that link and you arrive back at the row you started from.

    The strategy the certificate actually tracks is `StarTopSelectie OFF FX`, which holds 24
    real stocks and whose name scores WORSE. Rank on the name and the wrapper wins every time.
    """

    def test_the_wrapper_is_the_best_name_match(self):
        from rapidfuzz import fuzz
        wrapper = fuzz.token_sort_ratio(stem(STAR_CERT), stem(P_TOPS_STS["omschrijving"]))
        truth = fuzz.token_sort_ratio(stem(STAR_CERT), stem(P_STAR_FX["omschrijving"]))
        assert wrapper >= truth      # the wrong answer is at least as attractive

    def test_and_it_is_still_not_chosen(self):
        g = _guess(STAR_CERT, STAR_ISIN, 1935, ALL_STAR, COMP_STAR)
        assert g is not None
        assert g.linked_portfolio_id == 2094      # StarTopSelectie OFF FX — the user's answer
        assert g.confidence >= 0.9

    def test_no_self_reference(self):
        """A portfolio is not its own holding. (The user's explicit rule.)"""
        g = _guess(STAR_CERT, STAR_ISIN, 2094, ALL_STAR, COMP_STAR)
        assert g is None or g.linked_portfolio_id != 2094

    def test_a_portfolio_with_no_composition_is_not_a_link_target(self):
        """`StarTopSelectie OFF DYN` scores identically — same stem — and stores ZERO positions.
        The entire point of the link is to look THROUGH to the underlying holdings; a target
        with none is a link to nothing."""
        g = _guess(STAR_CERT, STAR_ISIN, 1935, ALL_STAR, COMP_STAR)
        assert g.linked_portfolio_id != 2095


class TestTheSubsetTrap:
    """⚠ `token_set_ratio` SCORES A SUBSET AS A PERFECT MATCH, and the portfolio codes are short.

    stem('BUS_EUR_OFF_FX') is 'eur', which is a SUBSET of stem('Shell PLC EUR') — so token_set
    called it 100 and the first version of this module linked *every* EUR-quoted holding (Shell,
    iShares ACWI, Vanguard FTSE Japan, Amundi MSCI EM Asia...) to the Europa portfolio at 0.89
    confidence. Twenty false links, all confident. Same false-friend family as the raw
    `_name_score` floor that put NVIDIA on Stuttgart.
    """

    P_EUR = {"id": 1973, "name": "BUS_EUR_OFF_FX", "omschrijving": "Europa Offensief FX"}
    COMP = {1973: [{"isin": f"E{i}", "fonds": f"eu {i}"} for i in range(27)], 1: []}

    @pytest.mark.parametrize("holding", [
        "Shell PLC EUR", "iShares MSCI ACWI EUR", "Vanguard FTSE Japan EUR",
        "Amundi MSCI EM Asia EUR", "Amazon.com", "Global X SuperDividend UC",
    ])
    def test_a_plain_instrument_is_never_linked_to_a_portfolio(self, holding):
        assert _guess(holding, "US1", 1, [self.P_EUR], self.COMP) is None


class TestTheProductLineTrap:
    """⚠ THE STEMMER DELETES THE VERY WORD THAT SEPARATES TWO PRODUCT LINES.

    "TopSelectie" has to come off — every strategy carries it, so it discriminates nothing. But
    stripping it makes these two identical:

        'EuropaTopSelectie Index'  (the certificate)   -> europa
        'Europa Offensief FX'      (BUS_EUR_OFF_FX)    -> europa    ← a DIFFERENT strategy

    and BUS_EUR_OFF_FX is the one with 27 real positions, so it passes every gate and scores a
    perfect 100. The link came out at 0.99 confidence, pointing at the wrong product.

    The genuine `EuropaTopSelect OFF FX` stores NO composition, so the honest output here is NO
    GUESS. A module that cannot say "I don't know" will say something false instead.
    """

    CERT = "EuropaTopSelectie Index"
    P_EUR = {"id": 1973, "name": "BUS_EUR_OFF_FX", "omschrijving": "Europa Offensief FX"}
    P_ETS_REAL = {"id": 2021, "name": "EuropaTopSelect OFF FX",
                  "omschrijving": "EuropaTopSelectie offensief fixed"}
    COMP = {
        1973: [{"isin": f"E{i}", "fonds": f"eu {i}"} for i in range(27)],   # a real model...
        2021: [],                                                           # ...but the wrong one
        1: [],
    }

    def test_both_stem_to_the_same_thing(self):
        assert stem(self.CERT) == stem(self.P_EUR["omschrijving"]) == "europa"

    def test_the_family_is_what_tells_them_apart(self):
        assert is_topselectie(self.CERT)
        assert not is_topselectie(self.P_EUR["name"], self.P_EUR["omschrijving"])
        assert is_topselectie(self.P_ETS_REAL["name"], self.P_ETS_REAL["omschrijving"])

    def test_so_no_guess_is_made_rather_than_the_wrong_one(self):
        g = _guess(self.CERT, "CH1525090200", 1, [self.P_EUR, self.P_ETS_REAL], self.COMP)
        assert g is None


class TestAmbiguityIsReported:
    """A certificate whose strategy exists in four risk profiles (DEF / NEU / BEPOFF / OFF) is
    genuinely ambiguous — the names cannot say which one it tracks. A top score that only just
    beat a runner-up is not a 0.99; it is a coin flip we happened to win, and it must not render
    identically to a case we are sure about."""

    CERT = "MerkenTopSelectie Index"
    VARIANTS = [
        {"id": 1877, "name": "BUS_MTS_OFF_AFS", "omschrijving": "BUS_Merkentopselectie offensief AFS"},
        {"id": 1879, "name": "BUS_MTS_DEF_AFS", "omschrijving": "BUS_Merkentopselectie defensief AFS"},
    ]
    COMP = {1877: [{"isin": f"A{i}"} for i in range(20)],
            1879: [{"isin": f"B{i}"} for i in range(20)], 1: []}

    def test_it_still_guesses_but_says_it_is_unsure(self):
        g = _guess(self.CERT, "CH1550438936", 1, self.VARIANTS, self.COMP)
        assert g is not None
        assert g.linked_portfolio_id in (1877, 1879)
        assert g.confidence <= 0.7        # visibly not one of the 0.99s
        assert "ambiguous" in g.reason

    def test_an_unambiguous_one_is_confident(self):
        g = _guess(STAR_CERT, STAR_ISIN, 1935, ALL_STAR, COMP_STAR)
        assert g.confidence >= 0.9
        assert "ambiguous" not in g.reason


class TestTheStemmer:
    def test_noise_comes_off_as_a_SUBSTRING_not_a_token(self):
        """'Familietopselectie' and 'Merkentopselectie' are written as ONE word. A tokenizer sees
        an opaque token and removes nothing, so the boilerplate survives inside it and then
        dominates the comparison."""
        assert stem("BUS_Merkentopselectie offensief AFS") == "merken"
        assert stem("Familietopselectie offensief AFS") == "familie"

    def test_the_certificate_and_its_portfolio_reduce_to_the_same_stem(self):
        assert stem("Star Selection Index") == stem("StarTopSelectie Offensief fixed") == "star"
        assert stem("MomentumTopSelectie Index") == stem("MomentumTopSelectie Fixed") == "momentum"

    def test_accents_are_folded(self):
        """AirSPMS serves ISO-8859-1; `Azië` and `Azie` are one word and we have been bitten by
        treating them as two before."""
        assert stem("AziëTopSelectie fixed") == stem("AzieTopSelectie Index") == "azie"

    def test_a_real_instrument_keeps_its_name(self):
        assert stem("Shell PLC EUR") == "shell plc eur"


class TestTheLinkKey:
    """The link is a property of the HOLDING, not of the (parent, holding) pair. 'Star Selection
    Index' is `StarTopSelectie OFF FX` in all 11 models that hold it; storing that eleven times
    is eleven chances for the copies to disagree."""

    def test_the_isin_identifies_it(self):
        assert link_key("CH1381833321", "Star Selection Index") == "CH1381833321"

    def test_a_holding_with_no_isin_falls_back_to_its_name_case_folded(self):
        assert link_key(None, "VastgoedTopSelectie index") == "vastgoedtopselectie index"
        assert link_key("", "Liquiditeiten") == "liquiditeiten"
