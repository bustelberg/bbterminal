"""Effective positions — `Eᵢ = qᵢ·Pᵢ·Xᵢ`, the euros behind the weights, and the currencies.

⚠⚠ THE POINT OF THIS MODULE IS THAT IT SHARES ITS WEIGHTS. Active share, Concentration and this all
read `build_issuer_weights`; three panels showing three sets of weights for one portfolio would make
every number on all three unfalsifiable. The test that matters is therefore the cross-check against
Concentration, not any figure in isolation.

⚠ AND `Eᵢ` IS AIRS'S OWN VALUATION, not a product we compute. `airs_holding` carries a quantity, but
it also carries `current_value_eur` — the figure on the client's statement. A second derivation from
our close and our FX would disagree with it on most rows, with nothing able to say which was right.
"""
from __future__ import annotations

import pytest

import routers._active_share as A
import routers._asset_benchmark as B
import routers._portfolio_concentration as K
import routers._portfolio_exposure as E


@pytest.fixture
def wire(monkeypatch):
    def _wire(bench_names: list[tuple[str, float]], grid: dict[str, dict]):
        mem = [{"company_name": n, "isin": f"X{i:09d}", "market_cap_eur": c}
               for i, (n, c) in enumerate(bench_names)]
        monkeypatch.setattr(B, "members",
                            lambda _l: (mem, {"covered_pct": 100.0,
                                              "universe_members": len(mem)}))
        # ⚠ PATCHED ON `_active_share`, WHICH IS WHY `_portfolio_exposure` REACHES IT THROUGH THE
        # MODULE rather than by a from-import. A name bound at import time would silently miss this
        # and go to the real database — returning nothing, so the issuer key would fall back to the
        # HOLDING's name and a dual listing would read as two issuers. Caught exactly that way.
        monkeypatch.setattr(A, "_grid_by_isin",
                            lambda isins: {i: {"isin": i,
                                               "gf_company_name": grid[i].get("name"),
                                               "currency": grid[i].get("ccy")}
                                           for i in isins if i in grid})
    return _wire


def _h(name, isin, w, value=None, ccy=None, is_fund=False):
    return {"name": name, "isin": isin, "weight_pct": w, "is_fund": is_fund,
            "value_eur": value, "currency": ccy}


class TestTheEurosFoldPerIssuer:
    def test_two_share_classes_sum_into_one_position(self, wire):
        wire([("Alphabet Inc", 50.0), ("Nestle SA", 50.0)],
             {"US02079K1079": {"name": "Alphabet Inc Class C", "ccy": "USD"},
              "US02079K3059": {"name": "Alphabet Inc Class A", "ccy": "USD"},
              "CH0038863350": {"name": "Nestle SA", "ccy": "CHF"}})
        got = E.compute_exposure([
            _h("Alphabet C", "US02079K1079", 30.0, 300_000.0, "USD"),
            _h("Alphabet A", "US02079K3059", 20.0, 200_000.0, "USD"),
            _h("Nestle", "CH0038863350", 50.0, 500_000.0, "CHF")], "ACWI")

        assert (got["lines"], got["issuers"], got["folded_lines"]) == (3, 2, 1)
        alpha = next(p for p in got["positions"] if "Alphabet" in p["name"])
        assert alpha["value_eur"] == pytest.approx(500_000.0, abs=1e-6)
        assert alpha["lines"] == 2
        assert got["sleeve_eur"] == pytest.approx(1_000_000.0, abs=1e-6)

    def test_the_weights_are_the_ones_concentration_uses(self, wire):
        """⚠⚠ THE ARCHITECTURAL TEST. Both read `build_issuer_weights`; if either grew its own
        folding, the two panels would count and rank the same book differently."""
        wire([(f"Co{i}", 100.0) for i in range(5)],
             {f"I{i}": {"name": f"Co{i}", "ccy": "EUR"} for i in range(5)})
        hold = [_h(f"Co{i}", f"I{i}", 20.0 + i, 100.0 * (20 + i), "EUR") for i in range(5)]
        exp, conc = E.compute_exposure(hold, "ACWI"), K.compute_concentration(hold, "ACWI")
        assert exp["issuers"] == conc["issuers"]
        assert exp["positions"][0]["weight_pct"] == pytest.approx(conc["top1_pct"], abs=1e-9)

    def test_a_basket_has_weights_and_no_euros(self, wire):
        """⚠ ABSENT, NOT ZERO. Zero would claim the position is worthless."""
        wire([(f"Co{i}", 100.0) for i in range(5)],
             {f"I{i}": {"name": f"Co{i}", "ccy": "EUR"} for i in range(5)})
        got = E.compute_exposure(
            [_h(f"Co{i}", f"I{i}", 20.0, None, "EUR") for i in range(5)], "ACWI")
        assert got["has_values"] is False
        assert got["sleeve_eur"] is None
        assert got["positions"][0]["value_eur"] is None
        assert got["positions"][0]["weight_pct"] == pytest.approx(20.0, abs=1e-9)


class TestCurrencyIsTrackedSeparately:
    def test_the_split_is_by_listing_currency_and_sums_to_100(self, wire):
        wire([("Alphabet Inc", 50.0), ("Nestle SA", 50.0)],
             {"US1": {"name": "Alphabet Inc", "ccy": "USD"},
              "CH1": {"name": "Nestle SA", "ccy": "CHF"}})
        got = E.compute_exposure([_h("Alphabet", "US1", 50.0, 500.0, "USD"),
                                  _h("Nestle", "CH1", 50.0, 500.0, "CHF")], "ACWI")
        by = {c["currency"]: c for c in got["currencies"]}
        assert sorted(by) == ["CHF", "USD"]
        assert sum(c["weight_pct"] for c in got["currencies"]) == pytest.approx(100.0, abs=1e-9)
        assert by["USD"]["value_eur"] == pytest.approx(500.0, abs=1e-6)

    def test_one_issuer_across_two_currencies_is_named(self, wire):
        """⚠⚠ THE FOLD HIDES THIS BY DESIGN, so it is surfaced. A dual-listed company is ONE
        position and TWO FX exposures, and a reader looking at a single row would not know."""
        wire([("Shell PLC", 100.0)],
             {"GB1": {"name": "Shell PLC", "ccy": "GBP"},
              "NL1": {"name": "Shell PLC", "ccy": "EUR"}})
        got = E.compute_exposure([_h("Shell LSE", "GB1", 60.0, 60.0, "GBP"),
                                  _h("Shell AMS", "NL1", 40.0, 40.0, "EUR")], "ACWI")
        assert got["issuers"] == 1
        assert got["positions"][0]["currencies"] == ["EUR", "GBP"]

    def test_an_unassignable_currency_is_not_folded_into_eur(self, wire):
        """⚠ THE FLATTERING DEFAULT would make the book look more domestic than it is."""
        wire([("Co0", 100.0)], {"I0": {"name": "Co0", "ccy": "EUR"}})
        got = E.compute_exposure([_h("Co0", "I0", 50.0, 50.0, "EUR"),
                                  _h("Mystery", "I9", 50.0, 50.0, None)], "ACWI")
        assert got["currency_unknown_pct"] == pytest.approx(50.0, abs=1e-9)
        eur = next(c for c in got["currencies"] if c["currency"] == "EUR")
        assert eur["weight_pct"] == pytest.approx(50.0, abs=1e-9)


class TestBothDenominators:
    def test_the_sleeve_and_what_is_outside_it(self, wire):
        wire([("Co0", 100.0)], {"I0": {"name": "Co0", "ccy": "EUR"}})
        got = E.compute_exposure([
            _h("Co0", "I0", 25.0, 250.0, "EUR"),
            _h("Fund", "IE1", 50.0, 500.0, "EUR", is_fund=True),
            _h("Cash", "", 25.0, 250.0, None)], "ACWI")
        assert got["sleeve_eur"] == pytest.approx(250.0, abs=1e-9)
        assert got["book_eur"] == pytest.approx(1000.0, abs=1e-9)
        # ⚠ EVERY OTHER VIEW EXCLUDES THIS AND RENORMALISES; the figure exists so that is visible.
        assert got["other_eur"] == pytest.approx(750.0, abs=1e-9)
        assert got["stocks_pct"] == pytest.approx(25.0, abs=1e-9)
