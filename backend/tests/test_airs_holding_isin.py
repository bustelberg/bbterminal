"""Attaching ISINs to an AIRS account's holdings.

Every fixture is real, from BUS_Defensief_Dyn <-> BUS_Defensief_FX on 2026-07-16. Each test
below corresponds to a mechanism that was measured FAILING before it was added — none of these
are hypothetical.
"""
from __future__ import annotations

from routers._airs_holding_isin import _assign, _dedupe, _norm, _score


def P(fonds, isin=None):
    return {"fonds": fonds, "isin": isin}


class TestTheNameMatchNeedsEveryNameWeHave:
    """The model's `Fonds` is a hand-typed short label. Alone, it cannot find these at all."""

    def test_a_renamed_fund_range_resolves_only_via_the_isins_own_name(self):
        """`Xtrackers World Utilities EUR` vs the model's `db x-track MSCI W Utilit` share NOT
        ONE WORD — DWS renamed db x-trackers to Xtrackers. Only Yahoo's name bridges it."""
        pos = P("db x-track MSCI W Utilit", "IE00BM67HQ30")
        bare = _score("Xtrackers World Utilities EUR", pos, {})
        rich = _score("Xtrackers World Utilities EUR", pos,
                      {"IE00BM67HQ30": {"name": "Xtrackers MSCI World Utilities UCITS ETF"}})
        # Measured: 45.3 off the model's label alone (it shares only "utilit"/"eur" — the brand
        # word is absent), against 78 through Yahoo's. 45 is deep in the zone where an unrelated
        # instrument scores just as well.
        assert bare < 50, f"the model's own label cannot identify this fund (got {bare})"
        assert rich > 70
        assert rich - bare > 25

    def test_the_isin_side_name_can_contradict_the_models_label(self):
        """The model calls IE000A0RC215 `Invesco BulletShares 29`; the instrument's own name
        says 2028, and so does the account. The model's label is the wrong one — which is
        exactly why the ISIN's name has to be in the pool."""
        pos = P("Invesco BulletShares 29", "IE000A0RC215")
        grid = {"IE000A0RC215": {"leonteq_name": "IVZ BULLETSHARES 2028 USD D"}}
        assert _score("Invesco BulletShares 2028 USD Corporate Bond ETF", pos, grid) > \
            _score("Invesco BulletShares 2028 USD Corporate Bond ETF", pos, {})


class TestNormalisationKeepsTokens:
    def test_spaces_are_kept_so_token_sort_still_has_tokens(self):
        """⚠ Stripping spaces silently degrades `token_sort_ratio` to a character ratio. That is
        not a style point: it is what let `Vanguard ESG Global Corp Bond` outscore the truth
        against the iShares row."""
        assert _norm("iShares Global Corp Bond ETF EUR H Dist") == "ishares global corp bond etf eur h dist"
        assert " " in _norm("Berkshire Hathaway - B")


class TestTheAssignmentIsGlobalNotPerRow:
    """⚠ THE failure this module exists to prevent."""

    def test_two_holdings_cannot_take_the_same_model_row(self):
        """Real scores. Per-row greedy sends BOTH corporate-bond ETFs to column 0 — the
        Vanguard one wins its own best at 74 there, beating its true row by 5. A global
        assignment cannot double-book, so both land right."""
        #                     iShares row   Vanguard row
        scores = [[80.0, 60.0],    # iShares Global Corp Bond (account)
                  [74.0, 69.0]]    # Vanguard ESG Global Corp Bond (account)
        out = _assign(scores)
        assert out[0] == 0
        assert out[1] == 1, "the Vanguard holding must not be given the iShares row"

    def test_a_low_score_still_wins_by_elimination(self):
        """`Effectenrekening` -> `Liquiditeiten` scores 28 — cash under two unrelated Dutch
        words — and is CORRECT, because everything else is taken. A score floor would drop it."""
        scores = [[90.0, 20.0],
                  [30.0, 28.0]]
        out = _assign(scores)
        assert out[1] == 1

    def test_every_holding_gets_at_most_one_row_and_vice_versa(self):
        scores = [[50.0, 50.0, 50.0], [50.0, 50.0, 50.0]]
        out = _assign(scores)
        assert len(out) == 2
        assert len(set(out.values())) == 2


class TestTheAccountBillsOneInstrumentOnSeveralLines:
    def test_duplicate_lines_are_one_instrument(self):
        """BUS_Defensief_Dyn lists `6,5% Rabobank Certificaten 14-perp.` at 2.60% AND 0.01%:
        41 rows, 40 instruments. Left alone, the 1:1 assignment must place the spare, and it
        put it on an unrelated orphan at score 33."""
        rows = [
            {"holding_name": "6,5% Rabobank Certificaten 14-perp.", "quantity": 26900,
             "current_value_eur": 30354.0, "weight": 0.026},
            {"holding_name": "6,5% Rabobank Certificaten 14-perp.", "quantity": 26900,
             "current_value_eur": 83.0, "weight": 0.0001},
            {"holding_name": "ASML Holding", "quantity": 27, "current_value_eur": 41834.0,
             "weight": 0.0359},
        ]
        out = _dedupe(rows)
        assert len(out) == 2
        rabo = next(r for r in out if "Rabobank" in r["holding_name"])
        assert rabo["lines"] == 2
        assert rabo["current_value_eur"] == 30437.0      # summed, not dropped
        assert rabo["quantity"] == 53800
        assert next(r for r in out if r["holding_name"] == "ASML Holding")["lines"] == 1

    def test_a_single_line_holding_is_untouched(self):
        out = _dedupe([{"holding_name": "Nvidia", "quantity": 148, "current_value_eur": 27573.0,
                        "weight": 0.0237}])
        assert out[0]["lines"] == 1
        assert out[0]["quantity"] == 148
