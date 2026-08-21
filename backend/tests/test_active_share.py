"""Active share = ½ Σ|wᵖ − wᵇ|, and every test here is an IDENTITY rather than a fixture.

⚠⚠ THE FIGURE IS UNCHECKABLE BY EYE. "Active share 71%" against a 1,700-name index is not something
a reader can sanity-test, and neither is it something a screenshot review catches — so the things
worth pinning are the properties that make it the quantity it claims to be: it is 0 against itself,
100 against a disjoint book, it equals 100 − overlap exactly, and it is invariant to which SHARE
CLASS you happen to hold.

⚠ THE SHARE-CLASS CASE IS THE ONE THAT BITES. `_asset_benchmark.members` keeps ONE row per company
(Yahoo reports the full company cap on every class), so a book holding GOOG against an index row
carrying GOOGL matches on nothing at all if the join is the ISIN — reporting a full overweight AND
a full underweight in Alphabet, roughly a 4% swing on a US book, invented by the identifier.
"""
from __future__ import annotations

import routers._active_share as A


def _bench(monkeypatch, names_caps: list[tuple[str, float]], covered=100.0):
    """Stand in for `_asset_benchmark.members` — (company_name, market_cap_eur)."""
    mem = [{"company_name": n, "isin": f"X{i:09d}", "market_cap_eur": c}
           for i, (n, c) in enumerate(names_caps)]
    import routers._asset_benchmark as B
    monkeypatch.setattr(B, "members",
                        lambda _label: (mem, {"covered_pct": covered, "universe_members": len(mem)}))


def _grid(monkeypatch, by_isin: dict[str, str]):
    """Stand in for the `asset_grid` read — ISIN → the company name it bridges to."""
    monkeypatch.setattr(A, "_grid_by_isin",
                        lambda isins: {i: {"isin": i, "gf_company_name": by_isin.get(i)}
                                       for i in isins if by_isin.get(i)})


def _h(name, isin, w, is_fund=False):
    return {"name": name, "isin": isin, "weight_pct": w, "is_fund": is_fund}


class TestTheIdentities:
    def test_a_book_that_IS_the_index_has_no_active_share(self, monkeypatch):
        _bench(monkeypatch, [("Apple Inc", 60.0), ("Microsoft Corp", 40.0)])
        _grid(monkeypatch, {"US1": "Apple Inc", "US2": "Microsoft Corp"})
        got = A.compute_active_share([_h("Apple", "US1", 60), _h("Microsoft", "US2", 40)], "SP500")
        assert got["active_share_pct"] < 1e-9
        assert abs(got["overlap_pct"] - 100.0) < 1e-9

    def test_a_book_sharing_nothing_is_fully_active(self, monkeypatch):
        _bench(monkeypatch, [("Apple Inc", 100.0)])
        _grid(monkeypatch, {"NL1": "ASML Holding NV"})
        got = A.compute_active_share([_h("ASML", "NL1", 100)], "SP500")
        assert abs(got["active_share_pct"] - 100.0) < 1e-9
        assert got["overlap_pct"] < 1e-9
        assert abs(got["off_benchmark_pct"] - 100.0) < 1e-9

    def test_active_share_and_overlap_always_sum_to_100(self, monkeypatch):
        """⚠ THE DEFINING IDENTITY, and the reason the ½ is there. Both vectors sum to 1, so every
        overweight has a matching underweight; without the half everything is counted twice and
        this sum comes out at 200 for a book with no overlap at all."""
        _bench(monkeypatch, [("Apple Inc", 50.0), ("Microsoft Corp", 30.0), ("Nvidia Corp", 20.0)])
        _grid(monkeypatch, {"US1": "Apple Inc", "US2": "Microsoft Corp", "NL1": "ASML Holding NV"})
        got = A.compute_active_share(
            [_h("Apple", "US1", 50), _h("Microsoft", "US2", 20), _h("ASML", "NL1", 30)], "SP500")
        assert abs(got["active_share_pct"] + got["overlap_pct"] - 100.0) < 1e-9

    def test_it_is_symmetric_in_the_sense_that_only_the_gap_matters(self, monkeypatch):
        """Half the index at double weight, half not held: AS is 50 either way you read it."""
        _bench(monkeypatch, [("A Inc", 25.0), ("B Inc", 25.0), ("C Inc", 25.0), ("D Inc", 25.0)])
        _grid(monkeypatch, {"1": "A Inc", "2": "B Inc"})
        got = A.compute_active_share([_h("A", "1", 50), _h("B", "2", 50)], "SP500")
        assert abs(got["active_share_pct"] - 50.0) < 1e-9


class TestTheIssuerNotTheLine:
    def test_a_different_share_class_still_matches(self, monkeypatch):
        """⚠⚠ THE REGRESSION THE WHOLE MODULE EXISTS FOR. The index row is Alphabet class A; the
        book holds class C. On an ISIN join these are two different companies and active share
        reads 100."""
        _bench(monkeypatch, [("Alphabet Inc", 100.0)])
        _grid(monkeypatch, {"US02079K1079": "Alphabet Inc Class C"})
        got = A.compute_active_share([_h("Alphabet C", "US02079K1079", 100)], "ACWI")
        assert got["active_share_pct"] < 1e-9, got["rows"]

    def test_two_classes_of_one_issuer_are_summed_not_replaced(self, monkeypatch):
        """⚠ A book holding BOTH classes holds ONE position in Alphabet. Taking either line alone
        would report half the position as an underweight that does not exist."""
        _bench(monkeypatch, [("Alphabet Inc", 100.0)])
        _grid(monkeypatch, {"US02079K1079": "Alphabet Inc Class C",
                            "US02079K3059": "Alphabet Inc Class A"})
        got = A.compute_active_share(
            [_h("Alphabet C", "US02079K1079", 60), _h("Alphabet A", "US02079K3059", 40)], "ACWI")
        assert got["active_share_pct"] < 1e-9
        assert got["n_holdings"] == 1

    def test_corporate_suffixes_do_not_create_a_bet(self, monkeypatch):
        _bench(monkeypatch, [("ASML Holding NV", 100.0)])
        _grid(monkeypatch, {"NL1": "ASML Holding"})
        assert A.compute_active_share([_h("ASML", "NL1", 100)], "AEX")["active_share_pct"] < 1e-9

    def test_two_genuinely_different_companies_are_not_fused(self, monkeypatch):
        """⚠ THE OTHER DIRECTION, and the reason this is not a fuzzy match. A false positive here
        does not mis-price a listing — it merges two companies into one row of a risk report, and
        the report then understates the bet. `Siemens Ltd` is not `Siemens AG`."""
        assert A._issuer_key("Siemens Ltd") != A._issuer_key("Siemens Energy AG")
        assert A._issuer_key("Shell PLC") != A._issuer_key("Shell Midstream Partners")


class TestTheDenominator:
    def test_funds_cash_and_unpriceable_lines_are_dropped_and_the_rest_renormalised(self, monkeypatch):
        """⚠ THE STATED ASSUMPTION: the individual stocks ARE 100% of the compared portfolio. A
        fund left in at its real weight would count as a bet against every index name at once."""
        _bench(monkeypatch, [("Apple Inc", 100.0)])
        _grid(monkeypatch, {"US1": "Apple Inc"})
        got = A.compute_active_share([
            _h("Apple", "US1", 25),
            _h("iShares Core MSCI World", "IE1", 50, is_fund=True),
            _h("Liquiditeiten", "", 25),
        ], "ACWI")
        assert got["active_share_pct"] < 1e-9, "the one stock IS the index once renormalised"
        # ⚠ AND THE RENORMALISATION IS REPORTED, never silent — 25 of 100 is the sleeve compared.
        assert abs(got["stocks_pct"] - 25.0) < 1e-9

    def test_a_book_with_no_individual_stocks_refuses(self, monkeypatch):
        _bench(monkeypatch, [("Apple Inc", 100.0)])
        _grid(monkeypatch, {})
        got = A.compute_active_share([_h("iShares", "IE1", 100, is_fund=True)], "ACWI")
        assert got["available"] is False
        assert "no individual stocks" in got["reason"]

    def test_an_unmatchable_line_stays_active_rather_than_being_dropped(self, monkeypatch):
        """⚠ DROPPING IT WOULD RENORMALISE THE REST UPWARD AND LOWER ACTIVE SHARE — the flattering
        direction. It is a real position the index has no row for, so it counts, and it is listed."""
        _bench(monkeypatch, [("Apple Inc", 100.0)])
        _grid(monkeypatch, {"US1": "Apple Inc"})    # 'XX9' bridges to nothing
        got = A.compute_active_share(
            [_h("Apple", "US1", 50), _h(None, "XX9", 50)], "ACWI")
        assert abs(got["active_share_pct"] - 50.0) < 1e-9
        assert [u["isin"] for u in got["unresolved"]] == ["XX9"]


class TestWhatItRefusesToHide:
    def test_it_reports_how_much_of_the_index_it_could_price(self, monkeypatch):
        """⚠ A MISSING CONSTITUENT INFLATES THE WEIGHT OF THE REST (renormalisation), so an
        unpriceable name we do not hold makes active share read slightly LOW. Never exact."""
        _bench(monkeypatch, [("Apple Inc", 100.0)], covered=78.5)
        _grid(monkeypatch, {"US1": "Apple Inc"})
        got = A.compute_active_share([_h("Apple", "US1", 100)], "ACWI")
        assert got["benchmark_covered_pct"] == 78.5

    def test_off_benchmark_is_the_selection_half_of_the_bet(self, monkeypatch):
        """Active share mixes two different decisions — holding something the index does not have,
        and sizing something it does. Only the first is reported separately."""
        _bench(monkeypatch, [("Apple Inc", 50.0), ("Microsoft Corp", 50.0)])
        _grid(monkeypatch, {"US1": "Apple Inc", "NL1": "ASML Holding NV"})
        got = A.compute_active_share([_h("Apple", "US1", 70), _h("ASML", "NL1", 30)], "SP500")
        assert abs(got["off_benchmark_pct"] - 30.0) < 1e-9
        assert got["n_in_benchmark"] == 1
