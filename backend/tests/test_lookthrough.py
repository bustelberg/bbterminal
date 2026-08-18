"""A certificate that IS another model must be expanded into what that model holds.

MEASURED on ToppenbergBeheer Defensief (TOPS_DEF_BEH): NINE of its twelve positions are Leonteq
certificates wrapping other models, carrying 44.56% of the portfolio. Unexpanded they are CH
ISINs Yahoo cannot price, so the composition chart read "Unclassified 100%" over 1.0% classified
weight — a sector view of two bond ETFs and a cash line, presented as the portfolio.

After expansion: 168 legs, 29.2% classified, Technology 34.9% / Financials 15.5% / Consumer
Cyclical 14.4%.

⚠ FIVE CLASSES WERE DELETED FROM HERE ON 2026-07-29. DO NOT RE-ADD THEM IN THIS SHAPE.

    TestTheDrilldownAddsUpToItsOwnHeading · TestSelectingAClassMustNotMoveTheChart
    TestTheAllocationCountsTheExpandedHoldings · TestTheHoldingsTableReconcilesWithTheChartBesideIt
    TestALookedThroughLegGetsItsOwnReturnNotItsWrappers

They called `compute_attribution(2089, ...)` / `compute_portfolio_analysis(2089, ...)` with a
hardcoded PRODUCTION portfolio id and no fake, then asserted on whatever came back
(`checked > 20`, "no attribution rows to check"). That is a live-database integration test, which
this repo bans outright — and it had two costs beyond the ban:

  * it made `uv run pytest tests/` on a developer machine query PRODUCTION, silently, because
    `backend/.env.local` supplies the credentials;
  * it could never pass in CI, which correctly has none — 14 red tests, and a red suite nobody
    trusts stops being read at all. That is the exact failure that got the e2e suite deleted.

The invariants they encoded are real (a bucket heading must equal the sum of its own holdings;
selecting a class must not move the sector chart). If you want them back, follow this repo's own
rule: extract the arithmetic into a pure function and unit-test THAT, or drive the whole call
through `tests/_fake_supabase.py`. Do not reach for the live database again.
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
    # `_airs_portfolio_links` takes `supabase` as an argument, so patching it there would fail —
    # and passing the fake through is what proves the two share one connection rather than each
    # reaching for their own.
    #
    # ⚠ BOTH HANDLES, because the position/model reads moved into the SHARED `routers._airs_ref`
    # (2026-08-11) and that module resolves `deps.supabase` at call time. Patching only this
    # module left those reads pointing at the real proxy, which tried to build a client and failed
    # with `KeyError: 'SUPABASE_URL'` — i.e. a refactor elsewhere silently took this test's
    # database away. Patching `deps.supabase` too covers wherever a read ends up living.
    fake = FakeSupabase(_tables(**kw))
    monkeypatch.setattr(LT, "supabase", fake)
    monkeypatch.setattr("deps.supabase", fake)
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

        from routers import _airs_attribution_basis as basis
        from routers import _airs_portfolio_analysis, _airs_portfolio_attribution

        assert "expand_positions" in inspect.getsource(
            _airs_portfolio_analysis.compute_portfolio_analysis)
        # The attribution legs moved to the shared basis module (2026-07-31) — the composition
        # charts read the same loader now, so this guard follows the code rather than the file.
        assert _airs_portfolio_attribution.portfolio_legs is basis.portfolio_legs
        assert "expand_positions" in inspect.getsource(basis.model_legs)


class TestTheBookSideIsExpandedToo:
    """⚠ TWO PATHS REACH THESE CHARTS, AND ONLY ONE WAS FIXED FIRST.

    `weight_by='book'` weights the composition by AIRS's own EUR values, built from the ACCOUNT's
    holdings rather than the model's percentages — a separate loader. On ToppenbergBeheer
    Defensief the account IS nine certificates, so that path still charted "Unclassified 100%"
    while the model path already showed Technology 35%. One portfolio, two answers, depending on
    a toggle.
    """

    def test_value_is_conserved_by_the_expansion(self, monkeypatch):
        """A composition chart that changes the book's total is worse than an opaque one: every
        percentage on it is a share of a total the reader can no longer check.

        ⚠ `_grid` IS PATCHED HERE AND DID NOT USED TO BE. Every row below already carries a
        `bucket`, and `_reclassify_book_rows` used to return immediately in that case. Since the
        `Equity ETF` merge (2026-08-18) it reads the asset grid for EVERY row instead, because
        `is_fund` has to be on all of them — the bucket no longer distinguishes a fund from an
        operating company, and the Analyse modal gates owner-earnings blending on that distinction.
        The lookup is the same one the sibling tests below already stub.
        """
        from routers import _airs_portfolio_analysis as A
        from routers._airs_portfolio_analysis import _expand_book_rows

        monkeypatch.setattr(A, "_grid", lambda isins: {})
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

        from routers import _airs_attribution_basis as basis
        from routers import _airs_portfolio_analysis as A

        # The two composition paths. ⚠ The sector/region/currency AXES now weigh through the
        # shared basis loader (2026-07-31), so they expand by reading the same function the
        # attribution table does; `_book_port_items` still backs the allocation pie + the
        # holdings table and expands on its own.
        assert "expand_positions" in inspect.getsource(A.compute_portfolio_analysis)
        assert "_expand_book_rows" in inspect.getsource(A._book_port_items)
        # The two attribution paths — the drill-down behind a clicked sector bar, and now the
        # bars themselves.
        assert "expand_positions" in inspect.getsource(basis.model_legs)
        assert "_expand_book_rows" in inspect.getsource(basis.book_legs)

    def test_the_book_expansion_has_ONE_definition(self):
        """The basis module imports the composition's expander rather than carrying its own — two
        implementations of 'what does this book hold' is how the chart and the table drift."""
        import inspect

        from routers import _airs_attribution_basis as basis

        src = inspect.getsource(basis.book_legs)
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
