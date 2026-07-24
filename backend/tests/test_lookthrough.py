"""A certificate that IS another model must be expanded into what that model holds.

MEASURED on ToppenbergBeheer Defensief (TOPS_DEF_BEH): NINE of its twelve positions are Leonteq
certificates wrapping other models, carrying 44.56% of the portfolio. Unexpanded they are CH
ISINs Yahoo cannot price, so the composition chart read "Unclassified 100%" over 1.0% classified
weight — a sector view of two bond ETFs and a cash line, presented as the portfolio.

After expansion: 168 legs, 29.2% classified, Technology 34.9% / Financials 15.5% / Consumer
Cyclical 14.4%.
"""
from __future__ import annotations

from routers import _airs_lookthrough as LT
from tests._fake_supabase import FakeSupabase

CERT, STOCK_A, STOCK_B = "CH1381833321", "US0378331005", "US67066G1040"


def _tables(child_rows=None, link_to=2, parent_extra=None):
    return {
        "airs_model_portfolio": [
            {"id": 1, "name": "PARENT", "display_name": "Parent", "positions_datum": "2026-06-08"},
            {"id": 2, "name": "CHILD", "display_name": "Child Model", "positions_datum": "2026-06-08"},
        ],
        "airs_model_portfolio_position": ([
            {"portfolio_id": 2, "isin": r[0], "fonds": r[1], "percentage": r[2],
             "datum": "2026-06-08"}
            for r in (child_rows if child_rows is not None
                      else [(STOCK_A, "Apple", 60.0), (STOCK_B, "Nvidia", 40.0)])
        ]),
        "airs_model_portfolio_link": ([{"id": 1, "isin": CERT, "fonds": "Star Selection Index",
                                        "linked_portfolio_id": link_to}] if link_to else []),
    }


def _parent(pct=50.0):
    return [
        {"isin": CERT, "fonds": "Star Selection Index", "percentage": pct, "datum": "2026-06-08"},
        {"isin": "IE00B4L5Y983", "fonds": "A real ETF", "percentage": 100 - pct,
         "datum": "2026-06-08"},
    ]


def _patch(monkeypatch, **kw):
    # ⚠ Only this module holds a handle. `_airs_portfolio_links` takes `supabase` as an argument,
    # so patching it there would fail — and passing the fake through is what proves the two share
    # one connection rather than each reaching for their own.
    fake = FakeSupabase(_tables(**kw))
    monkeypatch.setattr(LT, "supabase", fake)
    return fake


class TestItReplacesTheCertificateWithWhatItHolds:
    def test_the_childs_weights_are_scaled_by_the_certificates_own(self, monkeypatch):
        _patch(monkeypatch)
        legs, info = LT.expand_positions(1, "2026-06-08", _parent(50.0))
        by = {leg["isin"]: leg["percentage"] for leg in legs}
        # 50% of the parent, split 60/40 inside the child.
        assert by[STOCK_A] == 30.0
        assert by[STOCK_B] == 20.0
        assert CERT not in by, "the certificate itself must not survive as a leg"
        assert info["looked_through_pct"] == 50.0

    def test_the_total_still_sums_to_the_original(self, monkeypatch):
        _patch(monkeypatch)
        legs, _ = LT.expand_positions(1, "2026-06-08", _parent(50.0))
        assert round(sum(leg["percentage"] for leg in legs), 6) == 100.0

    def test_a_child_that_does_not_sum_to_100_is_renormalised(self, monkeypatch):
        """⚠ Scaled by the child's OWN total, not by 100. A composition summing to 80% would
        otherwise shrink the parent's stake in it and hand the difference to everything else."""
        _patch(monkeypatch, child_rows=[(STOCK_A, "Apple", 48.0), (STOCK_B, "Nvidia", 32.0)])
        legs, _ = LT.expand_positions(1, "2026-06-08", _parent(50.0))
        by = {leg["isin"]: leg["percentage"] for leg in legs}
        assert by[STOCK_A] == 30.0 and by[STOCK_B] == 20.0
        assert round(sum(leg["percentage"] for leg in legs), 6) == 100.0


class TestItRefusesToLookThroughToNothing:
    """⚠ DELETING THE WEIGHT WOULD BE INVISIBLE. Everything else renormalises around the gap, so
    the total still reads 100% and the portfolio has silently lost a position."""

    def test_a_target_with_no_composition_stays_an_opaque_leg(self, monkeypatch):
        _patch(monkeypatch, child_rows=[])
        legs, info = LT.expand_positions(1, "2026-06-08", _parent(50.0))
        by = {leg["isin"]: leg["percentage"] for leg in legs}
        assert by[CERT] == 50.0, "the certificate must survive when there is nothing behind it"
        assert info["opaque_pct"] == 50.0
        assert info["looked_through_pct"] == 0.0
        assert round(sum(leg["percentage"] for leg in legs), 6) == 100.0

    def test_an_unlinked_certificate_is_left_alone(self, monkeypatch):
        _patch(monkeypatch, link_to=None)
        legs, info = LT.expand_positions(1, "2026-06-08", _parent(50.0))
        assert {leg["isin"] for leg in legs} == {CERT, "IE00B4L5Y983"}
        assert info["looked_through_pct"] == 0.0


class TestCycles:
    """⚠ A CERTIFICATE CAN POINT BACK AT ITS OWN HOLDER. `TOPS_STS_L` holds 'Star Selection Index'
    at 100%, so a link to the portfolio being expanded is a real shape in this data, not a
    hypothetical. Unguarded it recurses until the stack ends."""

    def test_a_link_back_to_the_parent_is_not_followed(self, monkeypatch):
        _patch(monkeypatch, link_to=1)          # the certificate links to the PARENT
        legs, info = LT.expand_positions(1, "2026-06-08", _parent(50.0))
        by = {leg["isin"]: leg["percentage"] for leg in legs}
        assert by[CERT] == 50.0
        assert info["looked_through_pct"] == 0.0


class TestOneStockReachedTwiceIsOneLeg:
    """Two certificates can both hold NVIDIA. Emitted twice, every downstream consumer either
    double-counts it or dedupes by its own rule — so it is merged here, once."""

    def test_weights_are_summed(self, monkeypatch):
        _patch(monkeypatch)
        parent = [
            {"isin": CERT, "fonds": "Star Selection Index", "percentage": 50.0,
             "datum": "2026-06-08"},
            # The parent ALSO holds Apple directly.
            {"isin": STOCK_A, "fonds": "Apple", "percentage": 50.0, "datum": "2026-06-08"},
        ]
        legs, _ = LT.expand_positions(1, "2026-06-08", parent)
        by = {leg["isin"]: leg["percentage"] for leg in legs}
        assert by[STOCK_A] == 80.0, "50 direct + 30 through the certificate"
        assert len([x for x in legs if x["isin"] == STOCK_A]) == 1
        assert round(sum(leg["percentage"] for leg in legs), 6) == 100.0

    def test_cash_legs_are_never_merged(self, monkeypatch):
        """Two rows with no ISIN are not the same instrument just because neither has an id."""
        _patch(monkeypatch, link_to=None)
        parent = [
            {"isin": None, "fonds": "Liquiditeiten", "percentage": 1.0, "datum": "2026-06-08"},
            {"isin": None, "fonds": "Effectenrekening", "percentage": 2.0, "datum": "2026-06-08"},
        ]
        legs, _ = LT.expand_positions(1, "2026-06-08", parent)
        assert len(legs) == 2


class TestBothConsumersUseIt:
    """The composition chart and the Brinson attribution must expand identically — two answers to
    'what does this portfolio hold' is how a chart and the table beneath it come to disagree."""

    def test_analysis_and_attribution_both_call_it(self):
        import inspect

        from routers import _airs_portfolio_analysis, _airs_portfolio_attribution

        assert "expand_positions" in inspect.getsource(
            _airs_portfolio_analysis.compute_portfolio_analysis)
        assert "expand_positions" in inspect.getsource(
            _airs_portfolio_attribution._model_holdings)


class TestTheBookSideIsExpandedToo:
    """⚠ TWO PATHS REACH THESE CHARTS, AND ONLY ONE WAS FIXED FIRST.

    `weight_by='book'` weights the composition by AIRS's own EUR values, built from the ACCOUNT's
    holdings rather than the model's percentages — a separate loader. On ToppenbergBeheer
    Defensief the account IS nine certificates, so that path still charted "Unclassified 100%"
    while the model path already showed Technology 35%. One portfolio, two answers, depending on
    a toggle.
    """

    def test_value_is_conserved_by_the_expansion(self):
        """A composition chart that changes the book's total is worse than an opaque one: every
        percentage on it is a share of a total the reader can no longer check."""
        from routers._airs_portfolio_analysis import _expand_book_rows

        rows = [
            {"isin": "CH0000000001", "holding_name": "Star Selection Index",
             "current_value_eur": 100.0, "start_value_eur": 80.0,
             "linked_portfolio_id": None, "bucket": "Equity"},
            {"isin": "US0378331005", "holding_name": "Apple",
             "current_value_eur": 50.0, "start_value_eur": 40.0,
             "linked_portfolio_id": None, "bucket": "Equity"},
        ]
        out = _expand_book_rows(rows)
        assert sum(r["current_value_eur"] for r in out) == 150.0
        assert sum(r["start_value_eur"] for r in out) == 120.0

    def test_the_start_value_travels_with_the_current_one(self, monkeypatch):
        """⚠ Expanding `current_value_eur` alone gives every leg a return computed against a start
        of zero — the per-bucket return is Sum(now) / Sum(start) - 1."""
        from routers import _airs_portfolio_analysis as A

        monkeypatch.setattr(A, "_grid", lambda isins: {})
        monkeypatch.setattr("routers._airs_lookthrough._datum_of", lambda pid: None)
        monkeypatch.setattr("routers._airs_lookthrough._positions_of",
                            lambda pid, d: [{"isin": "US0378331005", "fonds": "Apple",
                                             "percentage": 100.0}])
        out = A._expand_book_rows([
            {"isin": "CH0000000001", "holding_name": "Cert", "current_value_eur": 100.0,
             "start_value_eur": 80.0, "linked_portfolio_id": 7, "bucket": "Equity"},
        ])
        assert len(out) == 1
        assert out[0]["current_value_eur"] == 100.0
        assert out[0]["start_value_eur"] == 80.0, "the start value must be split too"

    def test_the_parents_class_is_not_stamped_on_the_children(self, monkeypatch):
        """A certificate classified 'Equity' must not label a bond the child holds as equity."""
        from routers import _airs_portfolio_analysis as A

        monkeypatch.setattr(A, "_grid", lambda isins: {})
        monkeypatch.setattr("routers._airs_lookthrough._datum_of", lambda pid: None)
        monkeypatch.setattr("routers._airs_lookthrough._positions_of",
                            lambda pid, d: [{"isin": "XS000000000B", "fonds": "6,5% Rabobank Bond",
                                             "percentage": 100.0}])
        out = A._expand_book_rows([
            {"isin": "CH0000000001", "holding_name": "Cert", "current_value_eur": 100.0,
             "start_value_eur": 80.0, "linked_portfolio_id": 7, "bucket": "Equity"},
        ])
        assert out[0]["bucket"] != "Equity", "the child's own class must be re-derived"


class TestEveryPathThatReadsAPortfolioExpandsIt:
    """⚠ FOUR LOADERS REACH THESE CHARTS, AND THEY WERE FIXED ONE AT A TIME.

    Each fix left the others contradicting it, and the contradictions got worse as they narrowed:

      1. analysis / model   -> "Unclassified 100%" over 1% classified weight
      2. analysis / book    -> the chart said Unclassified while the model basis said Technology 35%
      3. attribution / model
      4. attribution / book -> the sector bar read Technology 35% and clicking it listed ZERO
                               holdings, because the book is nine certificates and none of them is
                               a technology stock

    A chart disagreeing with its OWN drill-down is worse than either being wrong alone: it tells
    the reader one of them is lying and gives them no way to tell which. So this asserts all four
    expand, by source — a fifth loader added without expansion would reintroduce exactly this.
    """

    def test_all_four_loaders_expand(self):
        import inspect

        from routers import _airs_portfolio_analysis as A
        from routers import _airs_portfolio_attribution as B

        # The two composition paths.
        assert "expand_positions" in inspect.getsource(A.compute_portfolio_analysis)
        assert "_expand_book_rows" in inspect.getsource(A._book_port_items)
        # The two attribution paths — the drill-down behind a clicked sector bar.
        assert "expand_positions" in inspect.getsource(B._model_holdings)
        assert "_expand_book_rows" in inspect.getsource(B._book_holdings)

    def test_the_book_expansion_has_ONE_definition(self):
        """Attribution imports the composition's expander rather than carrying its own — two
        implementations of 'what does this book hold' is how the chart and the table drift."""
        import inspect

        from routers import _airs_portfolio_attribution as B

        src = inspect.getsource(B._book_holdings)
        assert "from ._airs_portfolio_analysis import _expand_book_rows" in src


class TestNoDuplicateISINReachesTheUI:
    """⚠ REACT MAY DROP A ROW, NOT JUST WARN. The drill-down lists are keyed by ISIN, and a
    portfolio can hold a stock directly AND through two certificates — so an unmerged expansion
    emits it three times. React logs "Encountered two children with the same key" and documents
    the behaviour as unsupported, free to duplicate OR OMIT a child. A holdings list that silently
    loses a position is the failure here; the console warning is only how it announced itself.

    Measured on ToppenbergBeheer Beperkt Offensief: 14 duplicated ISINs before the merge.
    """

    def test_the_book_expansion_merges_and_conserves_value(self, monkeypatch):
        from routers import _airs_portfolio_analysis as A

        monkeypatch.setattr(A, "_grid", lambda isins: {})
        monkeypatch.setattr("routers._airs_lookthrough._datum_of", lambda pid: None)
        monkeypatch.setattr("routers._airs_lookthrough._positions_of",
                            lambda pid, d: [{"isin": "US67066G1040", "fonds": "NVIDIA",
                                             "percentage": 100.0}])
        rows = [
            # Held directly...
            {"isin": "US67066G1040", "holding_name": "NVIDIA", "current_value_eur": 40.0,
             "start_value_eur": 30.0, "linked_portfolio_id": None, "bucket": "Equity"},
            # ...and again through two certificates.
            {"isin": "CH0000000001", "holding_name": "Cert A", "current_value_eur": 100.0,
             "start_value_eur": 80.0, "linked_portfolio_id": 7, "bucket": "Equity"},
            {"isin": "CH0000000002", "holding_name": "Cert B", "current_value_eur": 60.0,
             "start_value_eur": 50.0, "linked_portfolio_id": 8, "bucket": "Equity"},
        ]
        out = A._expand_book_rows(rows)
        assert len([r for r in out if r.get("isin") == "US67066G1040"]) == 1, "one leg per ISIN"
        merged = next(r for r in out if r["isin"] == "US67066G1040")
        assert merged["current_value_eur"] == 200.0
        # ⚠ The START value must be summed too, or the merged leg's return is computed against
        # one fragment's base.
        assert merged["start_value_eur"] == 160.0
        assert sum(r["current_value_eur"] for r in out) == 200.0

    def test_both_expanders_share_one_merge(self):
        import inspect

        from routers import _airs_lookthrough as LT
        from routers import _airs_portfolio_analysis as A

        assert "merge_by_isin" in inspect.getsource(A._expand_book_rows)
        assert "_merge_by_isin" in inspect.getsource(LT.expand_positions)


class TestTheDrilldownAddsUpToItsOwnHeading:
    """⚠ THE LIST AND THE HEADING WERE ON DIFFERENT BASES.

    `w_p`/`w_b` are renormalised over what each side can attribute — the Brinson identity needs
    weights summing to 1 — but the per-holding lists were raw shares of the WHOLE portfolio.
    Measured on ToppenbergBeheer Defensief: the panel said Technology 34.38% while its own
    holdings added to 9.11%, out by exactly 100/attributable_pct (3.77x) on EVERY bucket.

    Neither number was wrong on its own, which is what makes it expensive: a reader who adds the
    list up gets a different answer from the heading and has no way to tell which to trust.
    """

    def test_holdings_sum_to_their_bucket_weight(self):
        import deps  # noqa: F401
        from routers._airs_portfolio_attribution import compute_attribution

        r = compute_attribution(2089, window="ytd", axis="sector", source="book")
        rows = r.get("rows") or []
        assert rows, "no attribution rows to check"
        for row in rows:
            for side, key in (("portfolio", "portfolio_holdings"),
                              ("benchmark", "benchmark_holdings")):
                legs = row.get(key) or []
                if not legs:
                    continue
                total = sum(h.get("weight_pct") or 0 for h in legs)
                shown = row[f"{side}_weight_pct"]
                assert abs(total - shown) < 0.01, (
                    f"{row['bucket']} {side}: list sums to {total:.2f} but the heading says "
                    f"{shown:.2f}")

    def test_contribution_is_still_weight_times_return(self):
        """Rescaling the weight without the contribution would leave two numbers printed side by
        side whose product is a third number that is not shown."""
        import deps  # noqa: F401
        from routers._airs_portfolio_attribution import compute_attribution

        r = compute_attribution(2089, window="ytd", axis="sector", source="book")
        checked = 0
        for row in (r.get("rows") or []):
            for h in (row.get("portfolio_holdings") or []):
                if h.get("return_pct") is None:
                    continue
                expect = (h["weight_pct"] or 0) / 100.0 * h["return_pct"]
                assert abs((h.get("contribution_pct") or 0) - expect) < 1e-9
                checked += 1
        assert checked > 20, "expected plenty of priced holdings to check"


class TestSelectingAClassMustNotMoveTheChart:
    """⚠ THE NUMBER YOU CLICK IS THE NUMBER YOU GET.

    The sector axis used to intersect its denominator with the allocation selection, so picking
    "Stocks" dropped Equity ETFs out of the base and every sector percentage rose — Technology
    34.41% -> 35.88% on ToppenbergBeheer Defensief, +1.07 to +1.47pp across the three Toppenberg
    books. Arithmetically correct, and unusable: the act of inspecting a figure changed it, which
    a reader cannot tell apart from a bug.

    Sector is an equity view either way (a bond has no sector), so the equity sleeve is the one
    denominator that answers a single question consistently.
    """

    def test_the_sector_chart_is_identical_with_and_without_an_equity_selection(self):
        import deps  # noqa: F401
        from routers._airs_portfolio_analysis import compute_portfolio_analysis

        def sector(bf):
            r = compute_portfolio_analysis(2089, weight_by="book", source="book",
                                           bucket_filter=bf)
            return {x["bucket"]: round(x["portfolio_pct"], 6)
                    for x in next(y for y in r["axes"] if y["axis"] == "sector")["rows"]}

        base = sector(None)
        assert base, "no sector rows to compare"
        for bf in ("Equity", "Equity ETF"):
            assert sector(bf) == base, f"selecting {bf} moved the sector chart"

    def test_a_NON_equity_selection_still_empties_it(self):
        """The filter must still decide WHETHER the chart is drawn — showing equity sectors under
        a Bonds selection would file those stocks under the wrong sleeve."""
        import deps  # noqa: F401
        from routers._airs_portfolio_analysis import compute_portfolio_analysis

        for bf in ("Bonds", "Cash"):
            r = compute_portfolio_analysis(2089, weight_by="book", source="book",
                                           bucket_filter=bf)
            rows = next(y for y in r["axes"] if y["axis"] == "sector")["rows"]
            assert sum(x["portfolio_pct"] for x in rows) == 0.0, f"{bf} left a sector chart"


class TestTheAllocationCountsTheExpandedHoldings:
    """⚠ A WEIGHT CANNOT TELL YOU HOW MANY NAMES CARRY IT. "66% in one bond ETF" and "66% across
    sixty companies" draw an identical wedge and are not the same portfolio. After look-through
    the distinction is finally available — ToppenbergBeheer Defensief's Stocks sleeve is nine
    lines in AIRS and 135 real companies underneath — so the slice carries the count."""

    def test_counts_sum_to_the_portfolio_total_and_weights_to_100(self):
        import deps  # noqa: F401
        from routers._airs_portfolio_analysis import compute_portfolio_analysis

        r = compute_portfolio_analysis(2089, weight_by="book", source="book")
        alloc = r.get("allocation") or []
        assert alloc, "no allocation slices"
        assert all(s.get("holdings", 0) > 0 for s in alloc), "every drawn slice holds something"
        assert sum(s["holdings"] for s in alloc) == r["holdings"], (
            "the classes must partition the holdings — a leg in two classes or none is a bug")
        assert abs(sum(s["pct"] for s in alloc) - 100.0) < 0.01

    def test_the_count_reflects_the_expansion_not_the_stored_lines(self):
        """The whole point: pre-expansion this portfolio is twelve AIRS lines, nine of them
        certificates. A Stocks count of 9 would be the certificates, not the companies."""
        import deps  # noqa: F401
        from routers._airs_portfolio_analysis import compute_portfolio_analysis

        r = compute_portfolio_analysis(2089, weight_by="book", source="book")
        equity = next((s for s in r["allocation"] if s["bucket"] == "Equity"), None)
        assert equity is not None
        assert equity["holdings"] > 50, (
            f"Stocks shows {equity['holdings']} holdings — that is the certificate count, not the "
            "companies behind them")


class TestTheHoldingsTableReconcilesWithTheChartBesideIt:
    """The whole-portfolio holdings table is drawn immediately below the allocation chart, so the
    two are read together. ⚠ A per-class subtotal that is a few points off its own slice does not
    read as "two weightings" — it reads as a bug in both, and the reader cannot tell which number
    to believe. `weight_now_pct` exists to share the chart's denominator exactly."""

    def _analysis(self):
        import deps  # noqa: F401
        from routers._airs_portfolio_analysis import compute_portfolio_analysis
        return compute_portfolio_analysis(2089, weight_by="book", source="book")

    def test_every_long_position_is_listed_not_only_the_priced_ones(self):
        """Cash carries no return, so it used to be filtered out of the detail list entirely. In a
        table of "what do I hold" a silently missing 7.25% cash line is the worst kind of gap: the
        remaining rows still sum to a plausible-looking total."""
        r = self._analysis()
        assert len(r["book_holdings"]) == r["holdings"]
        assert any(h["return_pct"] is None for h in r["book_holdings"]), (
            "an unpriced position must survive into the list, carrying a null return")

    def test_class_subtotals_equal_the_chart_slices(self):
        from collections import defaultdict
        r = self._analysis()
        sub: dict[str, float] = defaultdict(float)
        cnt: dict[str, int] = defaultdict(int)
        for h in r["book_holdings"]:
            sub[h["bucket"]] += h["weight_now_pct"]
            cnt[h["bucket"]] += 1
        for s in r["allocation"]:
            assert abs(sub[s["bucket"]] - s["pct"]) < 0.005, (
                f"{s['bucket']}: table {sub[s['bucket']]:.2f}% vs chart {s['pct']:.2f}%")
            assert cnt[s["bucket"]] == s["holdings"]
        assert abs(sum(sub.values()) - 100.0) < 0.01

    def test_the_two_weights_are_not_interchangeable(self):
        """Guards against someone "simplifying" the two fields into one. They are measured over
        different denominators (whole book vs priced book) on different bases (current vs opening
        value); collapsing them silently breaks either the chart or the contribution reconciliation."""
        r = self._analysis()
        priced = [h for h in r["book_holdings"] if h["weight_pct"] is not None]
        assert any(abs(h["weight_pct"] - h["weight_now_pct"]) > 0.01 for h in priced)

    def test_opening_weights_still_reconcile_each_class_return(self):
        """The invariant the per-class contribution view depends on, re-asserted here because this
        change edited the loop that produces it."""
        from collections import defaultdict
        r = self._analysis()
        by: dict[str, list] = defaultdict(list)
        for h in r["book_holdings"]:
            if h["weight_pct"] is not None:
                by[h["bucket"]].append(h)
        rets = {s["bucket"]: s["return_pct"] for s in r["allocation"]}
        checked = 0
        for b, legs in by.items():
            tw = sum(h["weight_pct"] for h in legs)
            if not tw or rets.get(b) is None:
                continue
            got = sum(h["weight_pct"] / tw * (h["return_pct"] or 0.0) for h in legs)
            assert abs(got - rets[b]) < 1e-6, f"{b}: {got} != {rets[b]}"
            checked += 1
        assert checked >= 4


class TestALookedThroughLegGetsItsOwnReturnNotItsWrappers:
    """⚠ THE BUG THIS EXISTS TO STOP. `_expand_book_rows` divides a certificate's START and CURRENT
    value by the same composition share, so `now/start − 1` comes out identical for every
    instrument behind it — the certificate's own return, stamped on all of them. Measured before
    the fix: 135 stocks in ToppenbergBeheer Defensief carried 37 distinct returns, one per
    certificate, and NVIDIA reported +0.08% against its own +2.82% over the window.

    It is the dangerous shape of wrong: a plausible small number, in the right column, with no
    error anywhere. The per-instrument figure now comes from the instrument's own EUR price series.
    """

    def _analysis(self):
        import deps  # noqa: F401
        from routers._airs_portfolio_analysis import compute_portfolio_analysis
        return compute_portfolio_analysis(2089, weight_by="book", source="book")

    def test_stocks_do_not_share_a_handful_of_returns(self):
        r = self._analysis()
        eq = [h for h in r["book_holdings"]
              if h["bucket"] == "Equity" and h["own_return_pct"] is not None]
        assert len(eq) > 50
        distinct = len({round(h["own_return_pct"], 4) for h in eq})
        # Every stock moves on its own. Anything near "one value per certificate" is the bug back.
        assert distinct > len(eq) * 0.9, (
            f"{distinct} distinct returns over {len(eq)} stocks — legs are sharing their "
            "wrapper's return again")

    def test_the_book_split_return_is_still_the_uninformative_one(self):
        """Guards the reason both fields exist: `return_pct` genuinely IS the wrapper's number, so
        anyone who wires a per-row display back to it reintroduces the defect."""
        r = self._analysis()
        eq = [h for h in r["book_holdings"]
              if h["bucket"] == "Equity" and h["return_pct"] is not None]
        assert len({round(h["return_pct"], 4) for h in eq}) < len(eq) * 0.5

    def test_a_leg_names_every_strategy_it_was_reached_through(self):
        """A stock can arrive through several certificates and is merged into ONE position, so the
        routes must be unioned — keeping only the first would answer "which strategy put me in
        NVIDIA" with one of three right answers."""
        r = self._analysis()
        multi = [h for h in r["book_holdings"] if len(h.get("via_names") or []) > 1]
        assert multi, "no instrument reached through more than one strategy"
        for h in multi:
            assert len(set(h["via_names"])) == len(h["via_names"]), "routes must be deduped"
        direct = [h for h in r["book_holdings"] if not h.get("via_names")]
        assert direct, "a directly-held position must carry no route"

    def test_the_window_is_stated_on_every_row(self):
        """A return with no window is not traceable, and the anchor is NOT always 1 January — this
        portfolio's composition is dated mid-year, so its window opens then."""
        r = self._analysis()
        priced = [h for h in r["book_holdings"] if h["own_return_pct"] is not None]
        assert priced
        assert all(h["own_return_from"] for h in priced)
        assert len({h["own_return_from"] for h in priced}) == 1
