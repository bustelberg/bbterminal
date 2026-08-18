"""Composition of a model portfolio against a benchmark — where a comparison chart goes wrong.

A grouped bar chart is a claim that two things are measured the SAME WAY. Almost every way of
building this one breaks that promise quietly.
"""
from __future__ import annotations

import inspect

import pytest

from routers import _airs_portfolio_analysis as pa


class TestOneVocabulary:
    """The portfolio lives in the ISIN world (`asset_execution`); the benchmark in the `company`
    world. `company` HAS NO SECTOR COLUMN — its sector would come from `universe_membership`,
    whose SP500 rows are empty. Two taxonomies in one chart invent a tilt that is not there
    ("Technology" vs "Information Technology" is not a bet).

    So both sides are classified from `asset_grid` (yfinance), joined by ISIN. Measured: all 493
    SP500 members are present there with a sector, so the benchmark loses nothing.
    """

    def test_both_sides_read_the_same_table(self):
        # Both the portfolio and the benchmark classify off `asset_grid` — one vocabulary.
        assert 'table("asset_grid")' in inspect.getsource(pa._grid)


class TestFundsAreNotLookedThrough:
    """⚠ THE BUCKET THAT KEEPS THE CHART HONEST.

    An ETF is a basket and we hold none of its constituents. Its LISTING says nothing about its
    contents:

        SECTOR    24 of the 26 held ETFs have a "sector" of literally `etf` or `Equity`.
        REGION    an Amsterdam-listed MSCI World ETF is not European exposure.
        CURRENCY  quoted in EUR, it still holds mostly USD assets.

    Counting those as real buckets would push ~20% of a portfolio into a phantom sector and
    deflate every true one. So a fund goes into ONE bucket on ALL THREE axes, and the reader sees
    a bar that means "we cannot see inside this" — which is true.
    """

    def test_a_fund_is_bucketed_on_every_axis_not_just_sector(self):
        etf = {"asset_class": "etf", "sector": "Equity",
               "msci_region": "Europe", "market_cap_currency": "EUR", "currency": "EUR"}
        assert pa._buckets(etf, is_cash=False) == (
            pa.FUND_BUCKET, pa.FUND_BUCKET, pa.FUND_BUCKET)

    def test_an_equity_keeps_its_real_attributes(self):
        eq = {"asset_class": "equity", "sector": "Technology",
              "domicile_country": "United States", "msci_region": "North America",
              "market_cap_currency": "USD", "currency": "USD"}
        assert pa._buckets(eq, is_cash=False, isin="US0378331005",
                           codes={"US": "United States"}) == (
            "Technology", "North America", "USD")

    def test_a_fund_folds_into_unclassified(self):
        """A fund is a black box on these axes, so it is labelled Unclassified — never blank."""
        assert pa.FUND_BUCKET == pa.UNKNOWN_BUCKET == "Unclassified"


class TestRegionIsTheIssuersNotOurVenues:
    """⚠ THE S&P 500 IS NOT 7% EUROPEAN. It is an index of US companies.

    But that is what the first version said, because `asset_grid.msci_region` comes from
    `geo.resolve_geo`, which FALLS BACK TO THE LISTING COUNTRY when the domicile is unknown —
    sane for the instrument grid, catastrophic here. Yahoo returns no domicile for a thin German
    regional line, and our grid prices US megacaps on exactly those:

        LLY.SG   Eli Lilly   on Stuttgart    EUR 873bn  -> "Germany" -> EUROPE
        CHV.DU   Chevron     on Dusseldorf   EUR 322bn  -> "Germany" -> EUROPE
        IBM.HM   IBM         on Hamburg      EUR 221bn  -> "Germany" -> EUROPE

    54 members were called Europe. After the fix: 20, worth 2.1% — Linde, Eaton, Accenture,
    Medtronic, Trane, Johnson Controls (Irish), Chubb (Swiss), NXP (Dutch). Those are REAL: the
    S&P 500 genuinely holds foreign-domiciled constituents. The job is to keep the 2.1% and lose
    the other 5.1%, not to force the index to 100% American.
    """

    CODES = {"US": "United States", "IE": "Ireland", "CH": "Switzerland", "JP": "Japan"}

    def _row(self, domicile=None, listing="Germany", stored="Europe"):
        """⚠ `msci_region` DEFAULTS TO THE WRONG ANSWER ON PURPOSE. It is the venue-derived column
        (`resolve_geo` falls back to the listing), so every test below runs against a row whose
        stored region says Europe — which is exactly what the S&P megacaps looked like. A rule that
        only works when the grid happens to agree is not being tested by a row where it does."""
        return {"asset_class": "equity", "sector": "Healthcare", "listing_country": listing,
                "msci_region": stored, "domicile_country": domicile,
                "market_cap_currency": "USD", "currency": "EUR"}

    def test_a_us_megacap_on_a_german_venue_is_NOT_europe(self):
        """Eli Lilly. No domicile from Yahoo, priced on Stuttgart — the ISIN says US."""
        assert pa._region(self._row(domicile=None), "US5324571083", self.CODES) == "North America"

    def test_a_genuinely_irish_constituent_STAYS_europe(self):
        """Linde/Accenture/Medtronic are really domiciled abroad. Forcing the index to 100% US
        would be its own lie, in the other direction."""
        assert pa._region(self._row(domicile="Ireland"), "IE00B4BNMY34", self.CODES) == "Europe"
        # ...and it holds off the ISIN alone when Yahoo gave no domicile.
        assert pa._region(self._row(domicile=None), "CH0044328745", self.CODES) == "Europe"

    def test_the_venue_is_consulted_LAST_and_never_before_the_issuers_own_geography(self):
        """⚠⚠ THE ORDER **IS** THE FIX, and this is the assertion that guards it.

        The stored `msci_region` is now a third step (see the next two tests), and it says Europe on
        this row — so if it were ever reached before the ISIN, the 54 US megacaps our grid prices on
        German venues would all come back Europe again and the S&P would read 7.2% European for the
        second time. Measured after the change: S&P Europe 2.05%, unmoved.

        The raw `listing_country` is still never read in the code; only the derived column is, and
        only after both issuer-level signals have said nothing.
        """
        src = inspect.getsource(pa._region)
        assert "listing_country" not in src.split('"""', 2)[2]   # not in the CODE, only the prose
        assert pa._region(self._row(domicile=None, listing="Germany", stored="Europe"),
                          "US0378331005", self.CODES) == "North America"
        # ...and the same when Yahoo DID give a domicile that simply has no MSCI market.
        assert pa._region(self._row(domicile="Uruguay", stored="Europe"),
                          "US58733R1023", self.CODES) == "North America"

    def test_no_domicile_no_isin_and_no_stored_region_is_UNCLASSIFIED_not_a_guess(self):
        assert pa._region(self._row(domicile=None, stored=None), None, self.CODES) \
            == pa.UNKNOWN_BUCKET

    def test_a_stray_stored_value_is_refused_rather_than_becoming_a_bucket(self):
        """⚠ The column is nullable and free-form. A country name, a future spelling or anything
        else would otherwise open a bar of its own on a chart of four regions — which reads as a
        region rather than as a bad cell. Validated against `geo`'s own map, not a literal list."""
        for junk in (None, "", "Germany", "EMEA", "north america", 0):
            assert pa._region(self._row(domicile="Bermuda", stored=junk),
                              "BMG507361001", self.CODES) == pa.UNKNOWN_BUCKET

    def test_a_domicile_with_no_MSCI_region_falls_through_to_the_isin(self):
        """⚠ MercadoLibre. Yahoo reports Uruguay — its Montevideo head office, correctly — MSCI has
        no Uruguay, and the ISIN says Delaware. The first version returned on the spot for ANY
        domicile (`if dom: return msci_region_of(dom) or UNKNOWN`), so a domicile that existed but
        did not map never reached the ISIN below it: the region tab read `Unclassified` while
        `/asset-pipeline` read North America, same company, nothing wrong-looking on either screen.
        """
        codes = {**self.CODES, "FR": "France"}
        assert pa._region(self._row(domicile="Uruguay"), "US58733R1023", codes) == "North America"
        # Eurofins Scientific: a Luxembourg SE carrying a FRENCH ISIN — recovered by the same step.
        assert pa._region(self._row(domicile="Luxembourg"), "FR0014000MR3", codes) == "Europe"

    def test_an_incorporation_haven_falls_through_to_the_grids_stored_region(self):
        """⚠⚠ NEITHER ISSUER-LEVEL SIGNAL IS A MARKET FOR THESE, so the grid's value is the only
        answer there is — and it is the one `/asset-pipeline` already shows.

        18 of ACWI's 21 unclassified members were incorporated in a haven (Cayman, Bermuda,
        Luxembourg, Isle of Man, Macau), so the domicile does not map AND the ISIN prefix is the
        haven itself: Li Ning `KY…`, Jardine Matheson `BM…`, ArcelorMittal `LU…`. Leaving 0.24% of
        the index in a bucket that means "we do not know", while another screen in the same app
        showed a region for every one of them, is a worse answer than the venue's.

        Measured after the change: ACWI Unclassified 0.36% -> 0.00%, and the recovered weight lands
        North America +0.23pp / Europe +0.09pp / EM +0.03pp / Pacific +0.02pp.
        """
        codes = {**self.CODES, "KY": "Cayman Islands", "BM": "Bermuda"}
        assert pa._region(self._row(domicile="Cayman Islands", stored="Emerging Markets"),
                          "KYG989221000", codes) == "Emerging Markets"   # Zhen Ding, Taiwan-listed
        assert pa._region(self._row(domicile="Bermuda", stored="North America"),
                          "BMG0450A1053", codes) == "North America"      # Arch Capital, an S&P name
        # ...and with no domicile at all, which is what Yahoo returns for the thin German lines.
        assert pa._region(self._row(domicile=None, stored="Pacific"),
                          "KYG7800X1079", codes) == "Pacific"            # Sands China, on HKSE

    def test_it_inherits_a_wrong_LISTING_and_that_is_a_listing_bug_not_a_region_bug(self):
        """⚠⚠ THE PRICE OF THE STEP ABOVE, STATED RATHER THAN HIDDEN.

        Where our venue choice is wrong, the region follows it: `asset_grid` prices Kingsoft on
        Stuttgart (`3K1.SG`, EUR 6,550/day), Li Ning on Stuttgart (`LNLB.SG`, EUR 2,594/day) and
        Orient Overseas on Munich (`ORI1.MU`, EUR 3,056/day), so three HONG KONG companies bucket as
        Europe — together 0.02% of ACWI.

        There is deliberately no special case here. Those rows are wrong in their PRICE SERIES too,
        which no region rule can fix, and the repair is to repoint them to 3888/2331/0316.HK
        (`repoint_primary_listing.py --isin`). This test exists so the behaviour is documented and
        the day the listings are fixed, it is the test that says what changed.
        """
        codes = {**self.CODES, "KY": "Cayman Islands"}
        assert pa._region(self._row(domicile=None, listing="Germany", stored="Europe"),
                          "KYG5496K1242", codes) == "Europe"


class TestCurrencyIsTheCompanysNotOurVenues:
    """⚠ THE LISTING CURRENCY IS OUR CHOICE OF VENUE, NOT A FACT ABOUT THE COMPANY.

    Measured on the S&P 500: by listing currency it reads 91% USD; by the company's own reporting
    currency, 98%. The gap is 40 members our grid prices on European/Canadian venues (Corning on
    Stuttgart, Ciena on Xetra, WR Berkley on Munich, Exxon on a Canadian line). Pricing Corning
    off Stuttgart does not make it a euro asset — but the naive chart said the S&P 500 was
    "12% EUR", which is plainly false and would have discredited the whole modal.
    """

    def test_it_prefers_the_reporting_currency(self):
        row = {"asset_class": "equity", "sector": "Technology", "msci_region": "North America",
               "market_cap_currency": "USD", "currency": "EUR"}     # priced on Xetra
        assert pa._buckets(row, is_cash=False)[2] == "USD"

    def test_it_falls_back_to_the_listing_when_there_is_no_other(self):
        row = {"asset_class": "equity", "sector": "X", "msci_region": "Y",
               "market_cap_currency": None, "currency": "SEK"}
        assert pa._buckets(row, is_cash=False)[2] == "SEK"

    def test_the_mismatch_is_COUNTED_and_returned_not_absorbed(self):
        """It is the wrong-listing bug, visible in the benchmark — and it means the index's PRICE
        SERIES is drawn off those venues too. Silently 'fixing' the currency axis while hiding
        the cause would leave the real defect unreported."""
        assert pa._foreign_listing({"market_cap_currency": "USD", "currency": "EUR"}) is True
        assert pa._foreign_listing({"market_cap_currency": "USD", "currency": "USD"}) is False
        assert pa._foreign_listing({"market_cap_currency": None, "currency": "USD"}) is False


class TestCashAndCoverage:
    def test_cash_is_a_bucket_not_a_gap(self):
        """Same rule as the returns: its drag is a fact."""
        assert pa._buckets(None, is_cash=True) == (
            pa.CASH_BUCKET, pa.CASH_BUCKET, pa.CASH_BUCKET)

    def test_an_unclassifiable_holding_is_named_not_dropped(self):
        """A structured product with no instrument record is not 0% of anything — dropping it
        would silently renormalise the rest up."""
        assert pa._buckets(None, is_cash=False) == (
            pa.UNKNOWN_BUCKET, pa.UNKNOWN_BUCKET, pa.UNKNOWN_BUCKET)

    def test_coverage_is_reported(self):
        src = inspect.getsource(pa.compute_portfolio_analysis)
        assert '"covered_pct"' in src


class TestTheBenchmarkRidesTheSAMEWINDOW:
    """⚠ A BENCHMARK MEASURED OVER A DIFFERENT WINDOW IS NOT A BENCHMARK, IT IS A NUMBER.

    A model's "YTD" opens at `max(1 Jan, its inception)` — and 27 of the 56 models are younger
    than the year. `MoTopSelectie_FX` is NINE DAYS old. Setting its -3.04% beside the index's
    full-year +12.41% and calling the gap under-performance would be nonsense that reads exactly
    like a finding. Priced over the same nine days, the S&P made +0.27%.

    So the index is priced from the model's OWN `ytd_from`, and again from its OWN inception.
    """

    def test_both_windows_come_from_ONE_price_load_and_ONE_weighting(self):
        """Two `compute_index` calls would reload every close (4-9s each) AND give the
        look-ahead-bias loop a second place to live. `_window_rows` is the single implementation
        of the start-of-window weighting; `/benchmarks` uses it too."""
        from routers import _benchmark_index as bi

        src = inspect.getsource(bi.index_returns)
        assert "_closes(" in src and src.count("_closes(") == 1     # loaded once, not per window

    def test_the_start_of_window_weighting_survives_an_arbitrary_start(self):
        """The look-ahead bias this file's header is about: weighting by TODAY's cap turned
        +9.10% into +21.70%. An arbitrary `start` must roll the cap back to THAT date, not to
        1 January and not to today."""
        from routers import _benchmark_index as bi

        assert "start_cap_eur" in inspect.getsource(bi.index_returns)

    def test_the_portfolio_side_is_READ_never_recomputed(self):
        """`compute_portfolio_performance` is the one place a model's return is calculated, and
        the /portfolios table shows exactly it. Re-deriving it here — even 'the same way' — is
        how a modal ends up quietly disagreeing with the row that opened it."""
        src = inspect.getsource(pa._returns)
        # ⚠ MATCH THE CALL, NOT AN EXACT ARGUMENT LIST. This read `compute_portfolio_performance()`
        # with empty parens until 2026-08-04, so the day that call gained `only_portfolio_id=` —
        # an optimisation that left the invariant completely intact — the test went red claiming
        # the portfolio side was being recomputed. A guard that fires on a signature change it does
        # not care about teaches people to edit the test without reading it.
        assert "compute_portfolio_performance(" in src


class TestOneCompanyOneRow:
    def test_the_benchmark_weights_are_not_re_derived(self):
        """Yahoo, like GuruFocus, puts the FULL company cap on every share class, so Alphabet
        (GOOGL + GOOG) would contribute its cap TWICE — 11.3% of the index's weight, fictional.
        `_asset_benchmark.members` dedupes; re-deriving the weights here would re-introduce the
        bug in a second place."""
        # `_members` here IS the asset-world one — same price universe as the portfolio, so the
        # GOOGL+GOOG double-count is deduped in one place, not re-derived here.
        assert "from routers._asset_benchmark import members as _members" in inspect.getsource(pa)


class TestTheAxesAreComparable:
    def test_each_axis_is_normalised_to_100(self):
        items = [(50.0, ("Tech", "NA", "USD")), (25.0, ("Tech", "EU", "EUR")),
                 (25.0, ("Health", "NA", "USD"))]
        out = pa._weigh(items)
        for axis in ("sector", "region", "currency"):
            assert abs(sum(out[axis].values()) - 100.0) < 1e-9
        assert out["sector"]["Tech"] == 75.0

    def test_an_empty_side_does_not_divide_by_zero(self):
        assert pa._weigh([]) == {"sector": {}, "region": {}, "currency": {}}


class TestTheBarsAreTheAttributionWeights:
    """⚠ THE COMPOSITION BARS AND THE BRINSON ROWS ARE ONE NUMBER (2026-07-31, on request).

    They used to be two. The chart divided TODAY's value by the whole equity sleeve; attribution
    divided the value at the window's OPEN by the attributable holdings alone. Measured on
    Bustelberg Offensief: Technology 36% against 39.1%, and ASML 7.30% against 5.75% — both
    correct, which is precisely why it was unreadable. A reader cannot arbitrate two right answers.

    The fix is structural, not arithmetic: ONE ladder (`split_legs`) and ONE denominator
    (`renormalise`), read by both panels. These tests pin the identity and the ladder, because a
    second implementation of "attributable" would restore the divergence silently.
    """

    GRID = {
        # A real sector + a domicile ⇒ attributable on every axis.
        "US1": {"isin": "US1", "name": "Alpha Tech", "sector": "Technology",
                "domicile_country": "United States", "market_cap_currency": "USD",
                "currency": "USD", "asset_class": "equity"},
        "US2": {"isin": "US2", "name": "Beta Health", "sector": "Healthcare",
                "domicile_country": "United States", "market_cap_currency": "USD",
                "currency": "USD", "asset_class": "equity"},
        # A FUND — folds to Unclassified on all three axes, so the ladder drops it.
        "IE9": {"isin": "IE9", "name": "World Tracker", "sector": "etf",
                "domicile_country": "Ireland", "market_cap_currency": "EUR",
                "currency": "EUR", "asset_class": "etf"},
    }

    def _legs(self):
        return [
            {"isin": "US1", "weight_pct": 40.0, "return_pct": 10.0, "airs_name": "Alpha",
             "is_cash": False},
            {"isin": "US2", "weight_pct": 30.0, "return_pct": 5.0, "airs_name": "Beta",
             "is_cash": False},
            # A fund: real weight, dropped by the ladder — the bulk of the old 36-vs-39.1 gap.
            {"isin": "IE9", "weight_pct": 20.0, "return_pct": 3.0, "airs_name": "Tracker",
             "is_cash": False},
            # Cash: carried at a flat 0% by attribution, never a sector bet.
            {"isin": None, "weight_pct": 10.0, "return_pct": None, "airs_name": "Cash",
             "is_cash": True},
        ]

    def _patch(self, monkeypatch, legs=None):
        from routers import _airs_attribution_basis as basis

        monkeypatch.setattr(pa, "_grid", lambda isins: self.GRID)
        monkeypatch.setattr(pa, "_country_by_code", lambda: {"US": "United States"})
        monkeypatch.setattr(basis, "portfolio_legs",
                            lambda *a, **k: (legs if legs is not None else self._legs()))
        monkeypatch.setattr(basis, "window_start", lambda *a, **k: "2026-01-01")

    def test_a_bucket_weight_equals_what_brinson_would_compute(self, monkeypatch):
        """The identity, expressed the way ATTRIBUTION expresses it (its own `p_by_bucket` lines)
        rather than by calling the same helper twice."""
        from routers._airs_attribution_basis import renormalise, split_legs

        self._patch(monkeypatch)
        axes = pa._basis_axes(1, "book", None, None)

        for axis, idx in (("sector", 0), ("region", 1), ("currency", 2)):
            attributable, _excluded, _total = split_legs(
                self._legs(), idx, self.GRID, {"US": "United States"})
            p_w_total = renormalise(attributable)
            expected: dict[str, float] = {}
            for i in attributable:
                expected[i["bucket"]] = expected.get(i["bucket"], 0.0) + \
                    i["weight_pct"] / p_w_total * 100.0
            assert axes[axis]["weights"] == pytest.approx(expected)

    def test_the_fund_and_the_cash_are_gone_and_the_rest_is_renormalised(self, monkeypatch):
        """40 + 30 of 70 attributable, not of 100 — which is exactly why the sector bar rises when
        it adopts this basis."""
        self._patch(monkeypatch)
        axes = pa._basis_axes(1, "book", None, None)
        w = axes["sector"]["weights"]
        assert w["Technology"] == pytest.approx(40.0 / 70.0 * 100.0)
        assert w["Healthcare"] == pytest.approx(30.0 / 70.0 * 100.0)
        assert sum(w.values()) == pytest.approx(100.0)

    def test_the_dropped_weight_is_REPORTED_not_swallowed(self, monkeypatch):
        """⚠ THE COST OF THIS BASIS, MADE VISIBLE. The fund and the cash are 30% of the book and
        they are not on the chart. A percentage that quietly loses weight is the failure the
        coverage floors elsewhere exist to stop, so the axis carries both the share it speaks for
        and the names behind the gap."""
        self._patch(monkeypatch)
        axes = pa._basis_axes(1, "book", None, None)
        assert axes["sector"]["attributable_pct"] == pytest.approx(70.0)
        reasons = {e["isin"]: e["reason"] for e in axes["sector"]["excluded"]}
        assert reasons == {"IE9": "unclassified", None: "cash"}

    def test_a_fund_or_a_cash_line_is_NOT_counted_as_a_gap(self, monkeypatch):
        """⚠ THE DISTINCTION THAT KEEPS THE WARNING MEANINGFUL. A fund has no sector BY DEFINITION
        and is not a stock in our own classification; so does cash. Counting them as weight the
        chart 'cannot handle' put an alarm on a perfectly ordinary 13%-in-ETFs portfolio, which is
        how a warning stops being read. `unpriced_pct` counts only the real hole."""
        self._patch(monkeypatch)          # 20% fund + 10% cash, nothing unpriced
        axes = pa._basis_axes(1, "book", None, None)
        assert axes["sector"]["unpriced_pct"] == pytest.approx(0.0)
        assert axes["sector"]["attributable_pct"] == pytest.approx(70.0)

    def test_only_an_unpriced_holding_raises_the_gap(self, monkeypatch):
        legs = [
            {"isin": "US1", "weight_pct": 60.0, "return_pct": 10.0, "airs_name": "Alpha",
             "is_cash": False},
            {"isin": "US2", "weight_pct": 25.0, "return_pct": None, "airs_name": "Beta",
             "is_cash": False},
            {"isin": "IE9", "weight_pct": 15.0, "return_pct": 3.0, "airs_name": "Tracker",
             "is_cash": False},
        ]
        self._patch(monkeypatch, legs)
        axes = pa._basis_axes(1, "book", None, None)
        # The 15% fund is an answer; only the 25% unpriced equity is a hole.
        assert axes["sector"]["unpriced_pct"] == pytest.approx(25.0)
        assert axes["sector"]["attributable_pct"] == pytest.approx(60.0)

    def test_the_excluded_rows_carry_the_class_we_already_store(self, monkeypatch):
        """So a fund can be shown as "Equity — has no sector" rather than as the ladder's own word
        "unclassified", which reads as our data having failed.

        ⚠ THE LABEL IS "Equity" AND NOT "Equity ETF" SINCE 2026-08-18 — an equity ETF invests in
        equity, so the wrapper no longer has a bucket of its own. The point of the test is
        unchanged: the excluded row keeps the class we already stored for it. What it is NOT
        allowed to become is the ladder's "unclassified", which claims we failed to classify a row
        we classified perfectly well and then excluded for a different reason (no equity sector).
        """
        legs = [{"isin": "US1", "weight_pct": 60.0, "return_pct": 10.0, "airs_name": "Alpha",
                 "is_cash": False},
                {"isin": "IE9", "weight_pct": 40.0, "return_pct": 3.0, "airs_name": "Tracker",
                 "is_cash": False, "asset_class": "Equity"}]
        self._patch(monkeypatch, legs)
        axes = pa._basis_axes(1, "book", None, None)
        assert [(e["isin"], e["asset_class"]) for e in axes["sector"]["excluded"]] \
            == [("IE9", "Equity")]

    def test_an_unpriceable_holding_is_excluded_and_NAMED(self, monkeypatch):
        """⚠ THE DANGEROUS EXCLUSION. A real equity in a real sector that we cannot price vanishes
        from the bar, and its sector then reads as UNOWNED — a false finding, not a missing one.
        It has to go (there is no return), so it must be named."""
        legs = [
            {"isin": "US1", "weight_pct": 60.0, "return_pct": 10.0, "airs_name": "Alpha",
             "is_cash": False},
            {"isin": "US2", "weight_pct": 40.0, "return_pct": None, "airs_name": "Beta",
             "is_cash": False},
        ]
        self._patch(monkeypatch, legs)
        axes = pa._basis_axes(1, "book", None, None)
        assert axes["sector"]["weights"] == {"Technology": pytest.approx(100.0)}
        assert [(e["isin"], e["reason"]) for e in axes["sector"]["excluded"]] == [("US2", "unpriced")]
        assert axes["sector"]["attributable_pct"] == pytest.approx(60.0)

    def test_a_midwindow_purchase_carries_no_weight_and_is_not_called_an_exclusion(self, monkeypatch):
        """A holding bought during the window has no Beginwaarde, so its weight is 0. It is absent
        from the chart — the sharp edge of this basis — but it is NOT listed as excluded weight,
        because there is no percentage that was taken away from the reader."""
        legs = [
            {"isin": "US1", "weight_pct": 100.0, "return_pct": 10.0, "airs_name": "Alpha",
             "is_cash": False},
            {"isin": "US2", "weight_pct": 0.0, "return_pct": 5.0, "airs_name": "Bought in March",
             "is_cash": False},
        ]
        self._patch(monkeypatch, legs)
        axes = pa._basis_axes(1, "book", None, None)
        assert axes["sector"]["weights"] == {"Technology": pytest.approx(100.0)}
        assert axes["sector"]["excluded"] == []
        assert axes["sector"]["attributable_pct"] == pytest.approx(100.0)

    def test_the_class_filter_narrows_numerator_and_denominator_together(self, monkeypatch):
        """Filtering one and not the other stops the bars summing to 100, and every bar then
        silently means something else."""
        legs = [
            {"isin": "US1", "weight_pct": 40.0, "return_pct": 10.0, "airs_name": "Alpha",
             "is_cash": False, "asset_class": "Equity"},
            {"isin": "US2", "weight_pct": 30.0, "return_pct": 5.0, "airs_name": "Beta",
             "is_cash": False, "asset_class": "Bonds"},
        ]
        self._patch(monkeypatch, legs)
        axes = pa._basis_axes(1, "book", None, "Equity")
        assert axes["sector"]["weights"] == {"Technology": pytest.approx(100.0)}

    def test_a_class_filter_makes_coverage_a_STOCKS_ratio_not_a_BOOK_one(self, monkeypatch):
        """⚠ THE MIXED RATIO. `total_w` and `excluded` were left un-filtered, so with Stocks
        selected the card divided stocks-with-a-sector by the WHOLE book and reported "87% of the
        book has a sector" — under a Stocks-only chart, where it reads as a claim that 13% of the
        STOCKS are unclassified. They were a bond tracker and a cash line. Every stock had a
        sector, and the honest figure for that selection is 100%.

        ⚠⚠ THE EXCLUDED FUND IS A **BOND** TRACKER, AND THAT CHANGED WITH THE BUCKET MERGE
        (2026-08-18). It used to be an equity ETF, which sat in its own `Equity ETF` bucket and was
        therefore outside an `Equity` selection. Equity ETFs are now Stocks, so an equity tracker no
        longer discriminates here at all: it would be INSIDE the selection, and the class ratio
        (87/99.9) and the book ratio (87/100) would agree to a tenth of a point — the test would
        pass whichever denominator the code used, which is the one thing it exists to tell apart.

        A bond ETF is still Bonds, because every bucket names what a holding INVESTS IN. So the
        exclusion is by asset class rather than by wrapper, and the two ratios diverge again."""
        legs = [
            {"isin": "US1", "weight_pct": 60.0, "return_pct": 10.0, "airs_name": "Alpha",
             "is_cash": False, "asset_class": "Equity"},
            {"isin": "US2", "weight_pct": 27.0, "return_pct": 5.0, "airs_name": "Beta",
             "is_cash": False, "asset_class": "Equity"},
            {"isin": "IE9", "weight_pct": 12.9, "return_pct": 3.0, "airs_name": "Bond Tracker",
             "is_cash": False, "asset_class": "Bonds"},
            {"isin": None, "weight_pct": 0.1, "return_pct": None, "airs_name": "Liquiditeiten",
             "is_cash": True, "asset_class": "Cash"},
        ]
        self._patch(monkeypatch, legs)
        stocks = pa._basis_axes(1, "book", None, "Equity")["sector"]
        assert stocks["attributable_pct"] == pytest.approx(100.0), \
            "every stock has a sector — the notice must disappear entirely"
        assert stocks["excluded"] == [], "a bond fund is not excluded FROM THE STOCKS, it is not one"
        # Unfiltered, the same book legitimately reports the fund + cash as having no sector.
        whole = pa._basis_axes(1, "book", None, None)["sector"]
        assert whole["attributable_pct"] == pytest.approx(87.0)
        assert {e["isin"] for e in whole["excluded"]} == {"IE9", None}
        # ⚠ THE TWO RATIOS MUST DIFFER, or this test proves nothing about which denominator ran.
        assert stocks["attributable_pct"] != pytest.approx(whole["attributable_pct"])

    def test_a_class_filter_we_cannot_apply_refuses_rather_than_empties(self, monkeypatch):
        """Model legs carry no asset class. Ignoring the filter would chart every class's sectors
        under a Stocks selection; applying it would empty the chart. Neither is acceptable, so the
        caller's own (classifiable) fallback is used instead."""
        legs = [{"isin": "US1", "weight_pct": 100.0, "return_pct": 10.0, "airs_name": "Alpha",
                 "is_cash": False}]
        self._patch(monkeypatch, legs)
        assert pa._basis_axes(1, "model", None, "Equity") is None
        assert pa._basis_axes(1, "model", None, None) is not None

    def test_no_book_means_no_basis_rather_than_an_empty_chart(self, monkeypatch):
        from routers import _airs_attribution_basis as basis

        monkeypatch.setattr(basis, "portfolio_legs", lambda *a, **k: None)
        monkeypatch.setattr(basis, "window_start", lambda *a, **k: "2026-01-01")
        assert pa._basis_axes(1, "book", None, None) is None


class TestTheHoldingsTableReconcilesWithTheBars:
    """⚠ THE THIRD WEIGHT, AND WHY IT HAD TO EXIST.

    The Holdings table showed ASML at 7.02% (current value, whole book) while the Technology bar
    showed 5.75% (Beginwaarde, attributable holdings). The natural check — divide 7.02 by the
    84.94% Stocks slice and expect the bar — gives 8.26% and fails, and a failed check on two
    correct numbers reads exactly like a bug. ASML had simply outgrown the book by ~40%.

    `weight_start_pct` closes it by being the SAME NUMERATOR the bars use, lifted off the same
    legs. Recomputing it "the same way" is the one thing that would reopen the gap.
    """

    def test_the_start_weight_comes_from_the_axes_own_legs(self):
        src = inspect.getsource(pa._basis_axes)
        assert '"_start_weights"' in src
        assert 'leg["weight_pct"] for leg in legs' in src, \
            "must be lifted off the legs, never recomputed from the book rows"

    def test_it_is_joined_by_isin_onto_the_holdings(self):
        holdings = [{"isin": "US1", "weight_now_pct": 7.02},
                    {"isin": "US2", "weight_now_pct": 3.0}]
        out = pa._with_start_weights(holdings, {"US1": 5.0, "US2": 2.5})
        assert [h["weight_start_pct"] for h in out] == [5.0, 2.5]
        assert [h["weight_now_pct"] for h in out] == [7.02, 3.0], "the now-weight is untouched"

    def test_a_row_with_no_isin_gets_None_not_zero(self):
        """Cash HAS a start value; we simply have no key to reach it by. A 0.00% would state
        something false about a real position — and 0.00% already means something else here."""
        out = pa._with_start_weights([{"isin": None, "weight_now_pct": 0.14}], {"US1": 5.0})
        assert out[0]["weight_start_pct"] is None

    def test_a_midwindow_purchase_keeps_its_meaningful_zero(self, monkeypatch):
        """0.00% is a FACT: bought after the window opened, so it carries no weight on any
        composition chart. It must survive the join rather than being nulled with the cash rows."""
        out = pa._with_start_weights([{"isin": "US2", "weight_now_pct": 8.0}], {"US2": 0.0})
        assert out[0]["weight_start_pct"] == 0.0


class TestTheDrillDownSumsToItsBar:
    """`_axis_holdings` is what makes a composition bar checkable, and its whole value is ONE
    identity: the rows behind a bucket add up to that bucket's `portfolio_pct`, exactly.

    ⚠ THE REASON THIS MATTERS IS A MEASURED CONFUSION, NOT A HYPOTHETICAL. Technology reads 36% on
    this chart and 39.1% in the Brinson table for the same portfolio, because attribution drops
    funds/cash/unpriced names and renormalises what is left. Both are right. But the composition
    chart shipped aggregates only, so a reader could inspect the attribution number and not this
    one — and an un-inspectable figure beside an inspectable one that disagrees reads as a bug.

    A drill-down that lands NEAR its bar would be worse than none: it turns one unexplained number
    into two. So the division happens once, here, and the rows are handed out already divided.
    """

    ITEMS = [(50.0, ("Tech", "NA", "USD")), (25.0, ("Tech", "EU", "EUR")),
             (25.0, ("Health", "NA", "USD"))]
    LABELS = [{"name": "A", "isin": "US1"}, {"name": "B", "isin": "NL2"},
              {"name": "C", "isin": "US3"}]

    def test_every_bucket_sums_to_its_own_weigh_percentage(self):
        weighed = pa._weigh(self.ITEMS)
        detail = pa._axis_holdings(self.ITEMS, self.LABELS)
        for axis in ("sector", "region", "currency"):
            for bucket, pct in weighed[axis].items():
                assert abs(sum(h["weight_pct"] for h in detail[axis][bucket]) - pct) < 1e-9

    def test_the_same_holding_carries_a_different_weight_per_axis_only_when_the_total_differs(self):
        """One `items` list ⇒ one total ⇒ the same holding weighs the same on all three axes.
        The axes diverge only because the CALLER passes a different list for `sector` (the equity
        sleeve) than for region/currency (every long position)."""
        d = pa._axis_holdings(self.ITEMS, self.LABELS)
        assert d["sector"]["Health"][0]["weight_pct"] == d["region"]["EU"][0]["weight_pct"] == 25.0

    def test_identity_rides_along_and_the_bucket_is_named(self):
        d = pa._axis_holdings(self.ITEMS, self.LABELS)
        tech = d["sector"]["Tech"]
        assert [h["isin"] for h in tech] == ["US1", "NL2"]      # sorted by weight, largest first
        assert {h["classified_as"] for h in tech} == {"Tech"}

    def test_an_empty_side_does_not_divide_by_zero(self):
        assert pa._axis_holdings([], []) == {"sector": {}, "region": {}, "currency": {}}


class TestBookWeighting:
    """`weight_by="book"` reweights the portfolio bars by the paired AIRS book's actual EUR
    holdings. The invariant that makes it honest: ONLY the weights come from AIRS — the
    classification stays yfinance, because the benchmark is classified that way and two
    taxonomies in one chart invent a tilt."""

    def _wire(self, monkeypatch, *, rows, link=True):
        import routers._airs_account_links as links
        import routers._airs_holding_isin as hisin

        monkeypatch.setattr(links, "list_account_links", lambda: {
            "accounts": ([{"portefeuille": "X_DYN", "model_portfolio_id": 7}] if link else [])})

        # ⚠ THE STUB TAKES `**kw` AND THE TEST BELOW ASSERTS WHAT ARRIVED IN IT, rather than the
        # stub simply widening to swallow anything. `resolve_account_isins` gained a `freshen`
        # keyword (default TRUE) and this caller passes FALSE — a real decision, not plumbing: a
        # truthy `freshen` re-scrapes the account from AIRS live, so the default would put a
        # minutes-long network scrape inside a chart render. A `lambda p, **_k:` alone would keep
        # these five tests green through exactly that regression, since none of them can see the
        # argument. Capturing it is what makes the stub's tolerance safe.
        calls: list[dict] = []

        def _resolve(portefeuille, **kw):
            calls.append({"portefeuille": portefeuille, **kw})
            return {"rows": rows}

        monkeypatch.setattr(hisin, "resolve_account_isins", _resolve)
        self.calls = calls
        # ⚠ THE LOOK-THROUGH HOP, WHICH READS THE DATABASE. `_book_port_items` expands certificates
        # that ARE other models before classifying, and that path holds its own Supabase handle —
        # so without this the test builds a real client and, on a developer machine, queries
        # PRODUCTION (it only fails in CI, where there are no credentials). None of these fixtures
        # contains a certificate, so passing the rows straight through is what expansion does here
        # anyway; expansion itself is covered by `test_lookthrough.TestTheBookSideIsExpandedToo`.
        monkeypatch.setattr(pa, "_expand_book_rows", lambda rows: rows)
        # The two other database hops on this path, both added after these tests were written:
        # the composition's effective date, and the per-holding entry/exit price marks. These
        # tests assert on WEIGHTING and CLASSIFICATION only, so both are stubbed to "nothing
        # known" — a holding with no mark simply carries no return, which is the same blank the
        # UI shows for an unpriceable name.
        monkeypatch.setattr("routers._airs_lookthrough._datum_of", lambda pid: None)
        monkeypatch.setattr("routers._airs_portfolio_perf.compute_holding_marks",
                            lambda isins, anchor, **kw: {})
        # ⚠ ADDED 2026-08-04, AFTER THESE FOUR WENT RED IN CI AND ONLY IN CI. `_book_port_items`
        # grew a read of `airs_holding` for the book's snapshot date (2026-08-03), which on a
        # developer machine quietly queried PRODUCTION and in CI raised `KeyError: 'SUPABASE_URL'`
        # — the exact failure mode conftest's guard describes, arriving through a hop these tests
        # predate. Stubbing the named function is what the guard's docstring prescribes.
        monkeypatch.setattr(pa, "_book_snapshot_date", lambda pf: None)
        # ⚠ AND THE THIRD ONE, 2026-08-10 — `_book_port_items` now also reads the Mutaties journal
        # (`_airs_accounts._direct_result`) so a class subtotal carries dividends like the rows
        # above it and the tile below it. That is the right change and it is the third database hop
        # to arrive through a module these tests predate; each one broke them the same way. Stubbed
        # to the loader's OWN empty answer — `({}, {"gross": None, …})`, what it returns for an
        # account with no mutations — so a holding simply earns no income, which is the blank the
        # UI shows anyway. These five assert WEIGHTING and CLASSIFICATION; income is covered by
        # `test_book_return_source`.
        monkeypatch.setattr("routers._airs_accounts._direct_result",
                            lambda pf, names: ({}, {"gross": None, "tax": None, "funds": None}))
        # The classification grid — yfinance attributes, the SAME source the model side uses.
        monkeypatch.setattr(pa, "_grid", lambda isins: {
            "US1": {"sector": "Technology", "msci_region": "North America",
                    "market_cap_currency": "USD", "asset_class": "equity"},
            "US2": {"sector": "Financials", "msci_region": "North America",
                    "market_cap_currency": "USD", "asset_class": "equity"},
        })
        monkeypatch.setattr(pa, "_country_by_code", lambda: {})

    def test_weights_come_from_eur_value_not_count(self, monkeypatch):
        self._wire(monkeypatch, rows=[
            {"isin": "US1", "current_value_eur": 750, "asset_class": "Equity"},
            {"isin": "US2", "current_value_eur": 250, "asset_class": "Equity"},
        ])
        out = pa._book_port_items(7, {})
        assert out["total_w"] == 1000
        assert out["holdings"] == 2
        # 750/1000 of the book is US1's sector.
        pw = pa._weigh(out["items"])
        assert pw["sector"]["Technology"] == 75.0

    def test_a_short_or_overdraft_is_excluded_from_the_composition(self, monkeypatch):
        # ⚠ Negative value = a short (Nestle India) or an overdraft cash line. A bar chart of
        # what the book is LONG drops it — same rule the model side applies to a 0% weight.
        self._wire(monkeypatch, rows=[
            {"isin": "US1", "current_value_eur": 1000, "asset_class": "Equity"},
            {"isin": "US2", "current_value_eur": -400, "asset_class": "Equity"},
        ])
        out = pa._book_port_items(7, {})
        assert out["holdings"] == 1
        assert out["total_w"] == 1000

    def test_classification_is_yfinance_not_airs(self, monkeypatch):
        # The row carries AIRS's own category, but the bucket must come from the grid — Financials
        # for US2, not whatever AIRS calls it. Otherwise the portfolio and benchmark speak two
        # languages.
        self._wire(monkeypatch, rows=[
            {"isin": "US2", "current_value_eur": 100, "asset_class": "Equity",
             "categorie": "AAND", "sector": "BU-Fin Dienst"},
        ])
        out = pa._book_port_items(7, {})
        pw = pa._weigh(out["items"])
        assert set(pw["sector"]) == {"Financials"}   # the grid's word, not AIRS's

    def test_cash_is_its_own_bucket(self, monkeypatch):
        self._wire(monkeypatch, rows=[
            {"isin": "US1", "current_value_eur": 900, "asset_class": "Equity"},
            {"isin": None, "current_value_eur": 100, "asset_class": "Cash"},
        ])
        out = pa._book_port_items(7, {})
        pw = pa._weigh(out["items"])
        assert pw["sector"].get(pa.CASH_BUCKET) == 10.0

    def test_no_book_returns_none_so_the_caller_falls_back(self, monkeypatch):
        self._wire(monkeypatch, rows=[], link=False)
        assert pa._book_port_items(7, {}) is None

    def test_the_book_is_read_from_cache_never_rescraped_to_draw_a_chart(self, monkeypatch):
        """⚠ `freshen=False` IS THE WHOLE POINT OF THE ARGUMENT — the default is True.

        Drawing the composition bars must not reach out to AIRS. `resolve_account_isins(…,
        freshen=True)` re-downloads the account's Vermogensoverzicht, which is a slow scrape of a
        third-party site; hanging that off a chart render makes opening the Analyse modal take
        minutes and hammers AIRS once per open. The stored snapshot is the right source here — a
        composition is a picture of what is held, and refreshing it is the Refresh button's job.
        """
        self._wire(monkeypatch, rows=[
            {"isin": "US1", "current_value_eur": 100, "asset_class": "Equity"},
        ])
        pa._book_port_items(7, {})
        assert self.calls == [{"portefeuille": "X_DYN", "freshen": False}]


class TestTheAllocationBarAlwaysShowsTheFourClasses:
    """⚠⚠ AN OMITTED CLASS CANNOT STATE A ZERO.

    `_weigh_alloc` used to drop every empty bucket, so a book holding no bonds produced three bars
    and the reader had to remember which fourth was missing to tell "holds no bonds" from "bonds
    not computed". The two read identically, and only one of them is a fact about the portfolio.

    ⚠ AND IT COST THE POLICY OVERLAY ITS MOST IMPORTANT CASE. The allocation bands are drawn per
    bar: a Defensief book holding NO bonds against a 55% minimum had no bar to draw the breach on,
    so the single largest violation the policy can express was the one the overlay could not show.

    Pure — no DB, no network.
    """

    def test_the_four_classes_come_back_even_with_nothing_in_them(self):
        slices = pa._weigh_alloc([(100.0, "Equity")])
        assert [s["bucket"] for s in slices] == ["Equity", "Bonds", "Alternatives", "Cash"]
        empty = {s["bucket"]: s for s in slices if s["bucket"] != "Equity"}
        for bucket, s in empty.items():
            assert s["pct"] == 0.0, bucket
            assert s["holdings"] == 0, bucket

    def test_they_stay_in_the_declared_order_not_the_order_they_were_seen(self):
        """The bar and the holdings table are both built from this order; a set has none."""
        slices = pa._weigh_alloc([(10.0, "Cash"), (40.0, "Bonds"), (50.0, "Equity")])
        assert [s["bucket"] for s in slices] == ["Equity", "Bonds", "Alternatives", "Cash"]

    def test_unclassified_is_not_forced_but_still_appears_when_it_has_rows(self):
        """⚠ It is not a class anyone allocates to — it is our own failure to classify. An empty
        one is GOOD news, and printing "Unclassified 0.00%" on every healthy book advertises a
        problem that does not exist."""
        assert "Unclassified" not in [s["bucket"] for s in pa._weigh_alloc([(100.0, "Equity")])]
        with_unknown = pa._weigh_alloc([(90.0, "Equity"), (10.0, "Unclassified")])
        assert [s["bucket"] for s in with_unknown][-1] == "Unclassified"
        assert with_unknown[-1]["pct"] == pytest.approx(10.0)

    def test_the_percentages_still_sum_to_a_hundred_over_the_real_holdings(self):
        """⚠ THE ZEROS MUST NOT ENTER THE DENOMINATOR. A forced bucket is a row on screen, not a
        holding — if it changed the total, every real class's percentage would fall by being
        beside an empty one."""
        slices = pa._weigh_alloc([(75.0, "Equity"), (25.0, "Cash")])
        assert sum(s["pct"] for s in slices) == pytest.approx(100.0)
        assert {s["bucket"]: s["pct"] for s in slices}["Equity"] == pytest.approx(75.0)

    def test_an_empty_book_still_returns_nothing(self):
        """⚠ NO WEIGHT AT ALL IS NOT "A BOOK HOLDING FOUR EMPTY CLASSES" — there is no portfolio to
        describe, and four 0.00% bars would dress a failed load as a real allocation."""
        assert pa._weigh_alloc([]) == []
        assert pa._weigh_alloc([(0.0, "Equity")]) == []
