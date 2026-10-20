"""C₁₀ = Σ w₍ᵢ₎ and HHI = Σ wᵢ², with N_eff = 1/HHI.

 THE IDENTITY THAT MAKES N_eff READABLE IS `N equal weights → exactly N`, and it is the first
thing here. It is also the test that catches the one way this measure is easy to get wrong: HHI on
PERCENTAGES rather than fractions is 10,000× larger, so N_eff comes out at 0.0001. That fails
loudly; the dangerous version is mixing the conventions between the book and the benchmark, where
the two numbers merely stop being comparable.

 AND IT FOLDS ONTO ISSUERS. Alphabet A + Alphabet C is ONE position — counting two understates
concentration exactly at the top, where the ten largest are decided.
"""
from __future__ import annotations

import pytest

import routers._active_share as A
import routers._asset_benchmark as B
import routers._portfolio_concentration as K


@pytest.fixture
def wire(monkeypatch):
    def _wire(bench_names: list[tuple[str, float]], grid: dict[str, str], covered: float = 100.0):
        mem = [{"company_name": n, "isin": f"X{i:09d}", "market_cap_eur": c}
               for i, (n, c) in enumerate(bench_names)]
        monkeypatch.setattr(B, "members",
                            lambda _l: (mem, {"covered_pct": covered,
                                              "universe_members": len(mem)}))
        monkeypatch.setattr(A, "_grid_by_isin",
                            lambda isins: {i: {"isin": i, "gf_company_name": grid.get(i)}
                                           for i in isins if grid.get(i)})
    return _wire


def _h(name, isin, w, is_fund=False):
    return {"name": name, "isin": isin, "weight_pct": w, "is_fund": is_fund}


class TestTheEffectiveCount:
    @pytest.mark.parametrize("n", [4, 10, 25])
    def test_n_equal_weights_gives_exactly_n(self, wire, n):
        wire([(f"Co{i}", 100.0) for i in range(n)], {f"I{i}": f"Co{i}" for i in range(n)})
        got = K.compute_concentration([_h(f"Co{i}", f"I{i}", 100.0 / n) for i in range(n)], "SP500")
        assert got["effective_positions"] == pytest.approx(n, abs=1e-9)
        #  FRACTIONS. On percentages this would be 10,000/n and N_eff would read 0.0001.
        assert got["hhi"] == pytest.approx(1.0 / n, abs=1e-12)

    def test_a_dominated_book_reads_far_below_its_name_count(self, wire):
        """40 names of which 5 hold half the book — the shape the spec describes."""
        n = 40
        wire([(f"Co{i}", 100.0) for i in range(n)], {f"I{i}": f"Co{i}" for i in range(n)})
        ws = [10.0] * 5 + [50.0 / 35] * 35
        got = K.compute_concentration(
            [_h(f"Co{i}", f"I{i}", w) for i, w in enumerate(ws)], "SP500")
        assert got["issuers"] == 40
        assert got["effective_positions"] < 20
        assert got["top5_pct"] == pytest.approx(50.0, abs=1e-9)

    def test_a_single_name_is_one(self, wire):
        wire([("Solo", 100.0)], {"I0": "Solo"})
        got = K.compute_concentration([_h("Solo", "I0", 100.0)], "SP500")
        assert got["hhi"] == pytest.approx(1.0, abs=1e-12)
        assert got["effective_positions"] == pytest.approx(1.0, abs=1e-12)
        #  C₂₀ OF A ONE-NAME BOOK IS 100%, not an error and not "C₁". Capping the label to the
        # count would stop it being comparable with the next book.
        assert got["top20_pct"] == pytest.approx(100.0, abs=1e-9)

    def test_the_cuts_are_monotonic(self, wire):
        wire([(f"Co{i}", 100.0) for i in range(30)], {f"I{i}": f"Co{i}" for i in range(30)})
        got = K.compute_concentration(
            [_h(f"Co{i}", f"I{i}", 30.0 - i) for i in range(30)], "SP500")
        cuts = [got["top1_pct"], got["top3_pct"], got["top5_pct"],
                got["top10_pct"], got["top20_pct"]]
        assert cuts == sorted(cuts)


class TestIssuersNotLines:
    def test_two_share_classes_are_one_position(self, wire):
        """ THE TOP POSITION IS THE COMBINED ONE. Counting the classes separately would put a 60%
        holding on screen as two 30% ones — and move whatever is third into the top ten."""
        wire([("Alphabet Inc", 50.0), ("Apple Inc", 50.0)],
             {"US02079K1079": "Alphabet Inc Class C", "US02079K3059": "Alphabet Inc Class A",
              "US0378331005": "Apple Inc"})
        got = K.compute_concentration([
            _h("Alphabet C", "US02079K1079", 30.0),
            _h("Alphabet A", "US02079K3059", 30.0),
            _h("Apple", "US0378331005", 40.0)], "SP500")
        assert got["issuers"] == 2
        assert got["top1_pct"] == pytest.approx(60.0, abs=1e-9)
        assert got["effective_positions"] == pytest.approx(1.0 / (0.36 + 0.16), abs=1e-9)


class TestBothDenominators:
    @pytest.mark.parametrize("include_non_stocks", [False, True])
    def test_top_positions_preserve_whole_book_weights_matching_active_share(self, wire, include_non_stocks):
        wire([("NVIDIA Corp", 60.0), ("Apple Inc", 40.0)],
             {"NVDA": "NVIDIA Corp", "AAPL": "Apple Inc"})
        holdings = [_h("Nvidia", "NVDA", 3.49), _h("Apple", "AAPL", 42.13)]
        if include_non_stocks:
            holdings += [_h("Fund", "IE1", 30.0, True), _h("Cash", "", 24.38)]

        concentration = K.compute_concentration(holdings, "SP500")
        active = A.compute_active_share(holdings, "SP500")
        active_weights = {r["name"]: r["portfolio_pct"] for r in active["rows"] if r["held"]}
        for row in concentration["top"]:
            assert row["weight_pct"] == pytest.approx(active_weights[row["name"]])
        nvidia = next(r for r in concentration["top"] if r["name"] == "Nvidia")
        assert nvidia["weight_pct"] == pytest.approx(3.49)
        assert concentration["top"][-1]["cumulative_pct"] == pytest.approx(45.62)
        assert concentration["top1_pct"] == pytest.approx(42.13)
        for field in ("top3_pct", "top5_pct", "top10_pct", "top20_pct", "top10_of_book_pct", "stocks_pct"):
            assert concentration[field] == pytest.approx(45.62)
        # HHI remains a stock-sleeve measure, unaffected by the display's whole-book denominator.
        assert concentration["hhi"] == pytest.approx((3.49 / 45.62) ** 2 + (42.13 / 45.62) ** 2)

    def test_the_sleeve_and_the_whole_book_are_both_reported(self, wire):
        """ THE CHOICE CHANGES THE NUMBER, so neither is made silently. A book that is 25% stocks
        has a top-10 of 100% OF ITS STOCKS and 25% of itself, and both are true."""
        wire([("Co0", 100.0)], {"I0": "Co0"})
        got = K.compute_concentration([
            _h("Co0", "I0", 25.0), _h("Fund", "IE1", 50.0, True), _h("Cash", "", 25.0)], "SP500")
        assert got["top10_pct"] == pytest.approx(25.0, abs=1e-9)
        assert got["top10_of_book_pct"] == pytest.approx(25.0, abs=1e-9)
        assert got["stocks_pct"] == pytest.approx(25.0, abs=1e-9)
        assert got["top"][0]["weight_pct"] == pytest.approx(25.0, abs=1e-9)
        assert got["top"][0]["cumulative_pct"] == pytest.approx(25.0, abs=1e-9)


class TestAgainstTheIndex:
    def test_the_index_gets_the_same_treatment(self, wire):
        wire([(f"Co{i}", 100.0) for i in range(50)], {f"I{i}": f"Co{i}" for i in range(3)})
        got = K.compute_concentration(
            [_h(f"Co{i}", f"I{i}", 100.0 / 3) for i in range(3)], "SP500")
        assert got["benchmark_effective_positions"] == pytest.approx(50.0, abs=1e-9)
        #  Every row carries the index's weight in the same issuer, so a big position can be read
        # as a big BET or merely as a big company.
        assert all(r["benchmark_pct"] == pytest.approx(2.0, abs=1e-9) for r in got["top"])
        assert got["top"][-1]["cumulative_pct"] == pytest.approx(100.0, abs=1e-9)
