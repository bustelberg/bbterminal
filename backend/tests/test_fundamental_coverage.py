"""Which holdings a portfolio-level fundamentals view can actually reach.

⚠ EVERY UNREACHED HOLDING IS WEIGHT THAT DROPS OUT OF THE BLEND. A blended figure over 40% of a
book, presented as the book's, is the same fabrication the AIRS return coverage floors already
guard against: the number looks entirely normal and describes something else.
"""
from __future__ import annotations

from routers._airs_portfolio_links import expand_members
from routers._fundamental_coverage import classify_holding, coverage_for


def _grid(asset_class=None, product=None):
    return {"asset_class": asset_class, "leonteq_product_type": product}


class TestTheReasonsAreNotInterchangeable:
    """⚠ `unsubscribed` and `no_company` look identical on screen and have OPPOSITE remedies:
    one is a purchase decision, the other a five-minute ingest. Merging them turns an actionable
    gap into a shrug."""

    def test_a_company_on_an_unsubscribed_exchange_says_so(self):
        # No company row AND we know the exchange is outside the subscription.
        assert classify_holding("GB0032398678", _grid("equity"), False, False) == "unsubscribed"

    def test_an_equity_we_simply_have_not_ingested_is_a_different_answer(self):
        # No company row and nothing tells us the exchange is unsubscribed.
        assert classify_holding("US0378331005", _grid("equity"), False, None) == "no_company"

    def test_a_company_we_have_WITH_metrics_is_covered(self):
        assert classify_holding("NL0010273215", _grid("equity"), True, True,
                                has_metrics=True) == "covered"

    def test_a_company_with_NO_metrics_is_not_covered(self):
        """⚠ THE DEFECT THIS REPLACED. `covered` promised "fundamentals can be fetched" on the
        strength of a company row alone. Measured 2026-07-23: 2,776 company rows, SEVEN carrying
        any annual metric — so a portfolio read 100% covered and would have charted nothing."""
        assert classify_holding("NL0010273215", _grid("equity"), True, True,
                                has_metrics=False) == "no_metrics"

    def test_no_metrics_is_a_THIRD_remedy_distinct_from_the_other_two(self):
        """unsubscribed = buy the data · no_company = ingest the company · no_metrics = the
        company is there and the earnings ingest has not run for it."""
        assert classify_holding("X00000000001", _grid("equity"), False, False) == "unsubscribed"
        assert classify_holding("X00000000002", _grid("equity"), False, None) == "no_company"
        assert classify_holding("X00000000003", _grid("equity"), True, True) == "no_metrics"

    def test_a_company_on_an_unsubscribed_exchange_is_still_no_metrics_not_unsubscribed(self):
        """⚠ THE SHOPIFY / BROOKFIELD CASE. A company pinned to an exchange we don't subscribe to
        (Shopify on TSX) is NOT pre-classified `unsubscribed` — GuruFocus often lists a subscribed
        primary (NASDAQ:SHOP) that the ingest resolves and repoints to. Whether a subscribed
        listing exists needs an API call, so the cheap classifier leaves it `no_metrics`
        (ingestable) and the ingest makes the real verdict."""
        assert classify_holding("CA82509L1076", _grid("equity"), True, False,
                                has_metrics=False) == "no_metrics"

    def test_a_company_WITH_metrics_stays_covered_even_on_an_unsubscribed_exchange(self):
        assert classify_holding("CA11271J1075", _grid("equity"), True, False,
                                has_metrics=True) == "covered"


class TestOrderIsTheRule:
    """⚠ A bond on an unsubscribed exchange is NOT an unsubscribed company. Reporting it as one
    puts it on the list of things a subscription would fix, and it never would."""

    def test_a_bond_is_not_equity_even_with_no_company_and_no_subscription(self):
        assert classify_holding("XS1234567890", _grid(None, "BONDS"), False, False) == "not_equity"

    def test_a_future_is_not_equity(self):
        assert classify_holding("XS9999999999", _grid(None, "FUTURE"), False, None) == "not_equity"

    def test_an_etf_holds_companies_rather_than_being_one(self):
        """`stock/QQQ/financials` returns null — a fund has no income statement of its own.
        Looking through to its constituents is a different feature, not a gap in this one."""
        assert classify_holding("IE00B4L5Y983", _grid("etf"), False, False) == "fund"

    def test_a_fund_that_DOES_have_a_company_row_and_metrics_is_still_a_fund(self):
        assert classify_holding("IE00B4L5Y983", _grid("etf"), True, True,
                                has_metrics=True) == "fund"

    def test_cash_has_no_isin_so_there_is_nothing_to_look_up(self):
        assert classify_holding(None, None, False, None) == "cash"
        assert classify_holding("", None, False, None) == "cash"


class TestCoverageIsMeasuredInWEIGHT:
    """⚠ A COUNT IS THE WRONG UNIT. Nine covered minnows and one uncovered giant is not 90%
    coverage, and the count would say it is."""

    def test_one_big_gap_dominates_nine_small_hits(self, monkeypatch):
        import routers._fundamental_coverage as fc

        members = [{"isin": f"US000000000{i}", "name": f"Small {i}", "weight": 0.01}
                   for i in range(9)]
        members.append({"isin": "IE00B4L5Y983", "name": "Big ETF", "weight": 0.91})

        class _Q:
            def __init__(self, rows): self._rows = rows
            def select(self, *_a, **_k): return self
            def in_(self, *_a, **_k): return self
            def eq(self, *_a, **_k): return self
            def limit(self, *_a, **_k): return self
            def execute(self): return type("R", (), {"data": self._rows})()

        def _table(name):
            if name == "asset_grid":
                return _Q([{"isin": "IE00B4L5Y983", "asset_class": "etf",
                            "leonteq_product_type": None}])
            if name == "metric_data":            # every covered company HAS the sentinel metric
                return _Q([{"company_id": i} for i in range(9)])
            return _Q([{"company_id": i, "company_name": f"Small {i}",
                        "isin": f"US000000000{i}", "gurufocus_exchange": {"exchange_code": "NAS"}}
                       for i in range(9)])

        monkeypatch.setattr(fc, "supabase", type("S", (), {"table": staticmethod(_table)})())
        out = coverage_for(members)
        assert out["holdings"] == 10
        assert out["covered_pct"] == 9.0            # NOT 90 — nine names at 1% each
        assert out["by_reason_pct"]["fund"] == 91.0

    def test_the_rows_lead_with_the_biggest_exclusion(self, monkeypatch):
        import routers._fundamental_coverage as fc

        class _Q:
            def select(self, *_a, **_k): return self
            def in_(self, *_a, **_k): return self
            def execute(self): return type("R", (), {"data": []})()

        monkeypatch.setattr(fc, "supabase",
                            type("S", (), {"table": staticmethod(lambda _n: _Q())})())
        out = coverage_for([{"isin": "A00000000001", "name": "small", "weight": 0.1},
                            {"isin": "B00000000002", "name": "big", "weight": 0.9}])
        assert [r["name"] for r in out["rows"]] == ["big", "small"]
        assert out["covered_pct"] == 0.0

    def test_weights_need_not_sum_to_one_only_their_ratios_matter(self, monkeypatch):
        import routers._fundamental_coverage as fc

        class _Q:
            def select(self, *_a, **_k): return self
            def in_(self, *_a, **_k): return self
            def execute(self): return type("R", (), {"data": []})()

        monkeypatch.setattr(fc, "supabase",
                            type("S", (), {"table": staticmethod(lambda _n: _Q())})())
        pct = coverage_for([{"isin": "A00000000001", "weight": 30},
                            {"isin": "B00000000002", "weight": 70}])
        frac = coverage_for([{"isin": "A00000000001", "weight": 0.3},
                             {"isin": "B00000000002", "weight": 0.7}])
        assert pct["rows"][0]["weight_pct"] == frac["rows"][0]["weight_pct"] == 70.0


class TestAnAliasedIsinIsCoveredByItsCanonical:
    """⚠ `company` and `gurufocus_listing` are keyed on the RAW ISIN, so an aliased row reads as
    "not ingested" while its canonical sits there fully covered. Measured on the TSMC ADR:
    US8740391003 showed 5.0% of a book as uncovered while TW0002330008 was company 3223."""

    ADR, ORD = "US8740391003", "TW0002330008"

    def _wire(self, monkeypatch, *, aliased: bool):
        import routers._fundamental_coverage as fc
        from asset_pipeline import isin_alias

        monkeypatch.setattr(isin_alias, "load_aliases",
                            lambda: ({self.ADR: self.ORD} if aliased else {}))

        class _Q:
            def __init__(self, rows, key="isin"):
                self._rows, self._key = rows, key
            def select(self, *_a, **_k): return self
            def eq(self, *_a, **_k): return self
            def limit(self, *_a, **_k): return self
            def in_(self, _c, vals):
                self._v = list(vals)
                return self
            def execute(self):
                return type("R", (), {"data": [r for r in self._rows
                                               if r[self._key] in getattr(self, "_v", [])]})()

        def _table(name):
            if name == "asset_grid":
                return _Q([{"isin": self.ORD, "asset_class": "equity",
                            "leonteq_product_type": None}])
            if name == "metric_data":            # the canonical company has fundamentals
                return _Q([{"company_id": 3223}], key="company_id")
            return _Q([{"company_id": 3223, "company_name": "TSMC", "isin": self.ORD,
                        "gurufocus_exchange": {"exchange_code": "TPE"}}])

        monkeypatch.setattr(fc, "supabase", type("S", (), {"table": staticmethod(_table)})())
        return fc.coverage_for([{"isin": self.ADR, "name": "Taiwan Semiconductor", "weight": 1.0}])

    def test_without_the_alias_it_reads_as_not_ingested(self, monkeypatch):
        out = self._wire(monkeypatch, aliased=False)
        assert out["rows"][0]["reason"] == "no_company"
        assert out["covered_pct"] == 0.0

    def test_with_the_alias_it_is_covered_by_the_canonical(self, monkeypatch):
        out = self._wire(monkeypatch, aliased=True)
        r = out["rows"][0]
        assert r["reason"] == "covered" and r["company_id"] == 3223
        assert out["covered_pct"] == 100.0

    def test_it_says_WHICH_isin_served_it(self, monkeypatch):
        """⚠ Otherwise an ISIN a reader knows we have no company for silently reads as covered,
        and there is nothing on screen to explain it."""
        assert self._wire(monkeypatch, aliased=True)["rows"][0]["served_by"] == self.ORD

    def test_an_unaliased_isin_reports_no_served_by(self, monkeypatch):
        assert self._wire(monkeypatch, aliased=False)["rows"][0]["served_by"] is None


class TestLookThroughLinkedCertificates:
    """⚠ A LINKED CERTIFICATE IS NOT A DEAD ROW. `Star Selection Index` (a CH structured product no
    vendor prices) IS `StarTopSelectie OFF FX`, 24 real stocks. Looking through it lets those
    stocks feed the coverage + blend instead of dropping 4.70% of the book on the floor."""

    def _through(self, isin, fonds, pid):
        """A resolver that links exactly ONE holding to one portfolio; None for everything else."""
        def _f(i, f, _owner):
            return pid if (i == isin or f == fonds) else None
        return _f

    def test_a_linked_certificate_is_replaced_by_the_underlying_stocks(self):
        positions = {7: [{"isin": "US1", "fonds": "Apple", "percentage": 60},
                         {"isin": "US2", "fonds": "MSFT", "percentage": 40}]}
        out = expand_members(
            [{"isin": "CH_CERT", "name": "Star Selection Index", "weight": 10.0}],
            positions, self._through("CH_CERT", "Star Selection Index", 7))
        assert [m["isin"] for m in out] == ["US1", "US2"]
        # ⚠ WEIGHT IS CONSERVED AND SPLIT BY THE UNDERLYING FRACTIONS: 10% -> 6% + 4%.
        assert [round(m["weight"], 4) for m in out] == [6.0, 4.0]
        # And every constituent names the certificate it came from.
        assert {m["via"] for m in out} == {"Star Selection Index"}

    def test_a_holding_with_no_link_is_left_alone(self):
        out = expand_members([{"isin": "NL0010273215", "name": "ASML", "weight": 5.0}],
                          {}, lambda *_a: None)
        assert out == [{"isin": "NL0010273215", "name": "ASML", "weight": 5.0}]

    def test_a_target_with_one_or_zero_positions_is_a_wrapper_not_a_model(self):
        """The >1 rule mirrors the guesser's gate 3: a single-position target is another wrapper,
        and linking to it walks back to the row we started from."""
        positions = {7: [{"isin": "US1", "fonds": "only", "percentage": 100}]}
        out = expand_members([{"isin": "CH_CERT", "name": "Cert", "weight": 10.0}],
                          positions, self._through("CH_CERT", "Cert", 7))
        assert out == [{"isin": "CH_CERT", "name": "Cert", "weight": 10.0}]

    def test_a_cycle_is_not_expanded_for_ever(self):
        """⚠ A wrapper holds the very certificate it links from. A portfolio already on the path is
        left as a row rather than re-expanded — the honest outcome, and it terminates."""
        # pid 7 links to pid 8, and pid 8 links back to pid 7.
        positions = {
            7: [{"isin": "CH_B", "fonds": "B cert", "percentage": 100},
                {"isin": "US1", "fonds": "real", "percentage": 100}],
            8: [{"isin": "CH_A", "fonds": "A cert", "percentage": 100},
                {"isin": "US2", "fonds": "real2", "percentage": 100}],
        }

        def _resolve(i, _f, _owner):
            return {"CH_A": 7, "CH_B": 8}.get(i)

        out = expand_members([{"isin": "CH_A", "name": "A cert", "weight": 10.0}],
                          positions, _resolve, max_depth=10)
        # A -> {CH_B, US1} -> CH_B expands to {CH_A(cycle, kept), US2}; US1/US2 are real.
        isins = sorted(m["isin"] for m in out)
        assert "US1" in isins and "US2" in isins
        assert "CH_A" in isins             # the cycle-closing row survives, uncovered
        assert len(out) == 3

    def test_the_top_level_certificate_name_survives_a_two_level_look_through(self):
        """`via` is fixed at the FIRST hop, so a nested stock still reads "via A cert" — the row
        the reader actually holds — not the intermediate model's name."""
        positions = {
            7: [{"isin": "CH_B", "fonds": "B cert", "percentage": 50},
                {"isin": "US1", "fonds": "real", "percentage": 50}],
            8: [{"isin": "US2", "fonds": "deep1", "percentage": 50},
                {"isin": "US3", "fonds": "deep2", "percentage": 50}],
        }

        def _resolve(i, _f, _owner):
            return {"CH_A": 7, "CH_B": 8}.get(i)

        out = expand_members([{"isin": "CH_A", "name": "A cert", "weight": 20.0}],
                          positions, _resolve)
        assert {m["via"] for m in out} == {"A cert"}
        # 20% -> CH_B(10%) + US1(10%); CH_B(10%) -> US2(5%) + US3(5%).
        by_isin = {m["isin"]: round(m["weight"], 4) for m in out}
        assert by_isin == {"US1": 10.0, "US2": 5.0, "US3": 5.0}


class TestCoverageCarriesTheLookThroughLabel:
    """The `via` a look-through stamps on a member must reach the coverage row, or an excluded
    constituent reads as a mystery top-level holding."""

    def test_via_certificate_is_passed_through_to_the_row(self, monkeypatch):
        import routers._fundamental_coverage as fc

        class _Q:
            def select(self, *_a, **_k): return self
            def in_(self, *_a, **_k): return self
            def eq(self, *_a, **_k): return self
            def limit(self, *_a, **_k): return self
            def execute(self): return type("R", (), {"data": []})()

        monkeypatch.setattr(fc, "supabase",
                            type("S", (), {"table": staticmethod(lambda _n: _Q())})())
        out = fc.coverage_for([{"isin": "US0000000001", "name": "underlying", "weight": 1.0,
                                "via": "Star Selection Index"}])
        assert out["rows"][0]["via_certificate"] == "Star Selection Index"


class TestTheEmptyAnswerHasTheSameShapeAsARealOne:
    """⚠ An early return that drops `by_reason_pct` crashes every consumer that iterates it — and
    it does so on the emptiest input, the one least likely to be tried by hand. Measured: a model
    portfolio with no positions threw `KeyError: 'by_reason_pct'`."""

    KEYS = {"holdings", "covered_pct", "by_reason_pct", "rows"}

    def test_the_empty_dict_carries_every_key(self):
        from routers.earnings import _EMPTY_COVERAGE

        assert set(_EMPTY_COVERAGE()) == self.KEYS

    def test_it_matches_what_coverage_for_returns(self, monkeypatch):
        import routers._fundamental_coverage as fc
        from routers.earnings import _EMPTY_COVERAGE

        class _Q:
            def select(self, *_a, **_k): return self
            def in_(self, *_a, **_k): return self
            def eq(self, *_a, **_k): return self
            def limit(self, *_a, **_k): return self
            def execute(self): return type("R", (), {"data": []})()

        monkeypatch.setattr(fc, "supabase",
                            type("S", (), {"table": staticmethod(lambda _n: _Q())})())
        real = fc.coverage_for([{"isin": "US0000000001", "weight": 1.0}])
        assert set(real) == set(_EMPTY_COVERAGE())
