"""Composition of a model portfolio against a benchmark — where a comparison chart goes wrong.

A grouped bar chart is a claim that two things are measured the SAME WAY. Almost every way of
building this one breaks that promise quietly.
"""
from __future__ import annotations

import inspect

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

    def _row(self, domicile=None, listing="Germany"):
        return {"asset_class": "equity", "sector": "Healthcare", "listing_country": listing,
                "msci_region": "Europe", "domicile_country": domicile,
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

    def test_the_listing_venue_is_NEVER_consulted(self):
        """The whole bug in one assertion: the row says Germany everywhere our venue choice
        touches it, and the answer is still North America."""
        src = inspect.getsource(pa._region)
        assert "listing_country" not in src.split('"""', 2)[2]   # not in the CODE, only the prose
        assert pa._region(self._row(domicile=None, listing="Germany"),
                          "US0378331005", self.CODES) == "North America"

    def test_no_domicile_and_no_isin_is_UNCLASSIFIED_not_a_guess(self):
        assert pa._region(self._row(domicile=None), None, self.CODES) == pa.UNKNOWN_BUCKET


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
        assert "compute_portfolio_performance()" in src


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
        monkeypatch.setattr(hisin, "resolve_account_isins", lambda p: {"rows": rows})
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
