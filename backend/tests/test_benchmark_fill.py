"""Why a benchmark reads 0 members, told apart from the other three reasons it could.

Measured on the hosted project 2026-07-29 — the case that prompted this:

    SP500: companies=493  with_isin=493  in_grid=1  status_ok=1  priced=1  weighable=0
    ACWI:  companies=1991 with_isin=1803 in_grid=1  status_ok=1  priced=1  weighable=0
    AEX:   no universe row at all

Three different faults, one symptom ("0 —"), and three different remedies: ingest the
constituents, backfill their caps, build the universe. The panel could not distinguish them, so
the classifier does — and it is pure, which is what makes it testable without a database.

⚠ THE BUCKET THAT MATTERS MOST IS `needs_cap`. Such a constituent is resolved, priced and looks
entirely healthy in the asset grid; it just weighs nothing, so it silently contributes zero to a
cap-weighted index. Counting it as `usable` would report full coverage over an index missing that
name — the same shape of lie as renormalising a portfolio over holdings it could not price.
"""
from __future__ import annotations

from routers._benchmark_fill import _classify


def _co(cid: int, isin: str | None, name: str = ""):
    return {"company_id": cid, "isin": isin, "company_name": name or f"Co {cid}"}


def _g(isin: str, *, status="ok", aid=1, bars=250, cap=1e9):
    return {"isin": isin, "status": status, "analysis_id": aid, "bars": bars,
            "market_cap_eur": cap, "yahoo_symbol": f"{isin[:4]}.X"}


class TestTheFourStates:
    def test_resolved_priced_and_capped_is_usable(self):
        out = _classify([_co(1, "US0000000001")], {"US0000000001": _g("US0000000001")})
        assert out["usable"] == ["US0000000001"]
        assert out["needs_resolve"] == [] and out["needs_cap"] == []

    def test_not_in_the_grid_needs_resolving(self):
        out = _classify([_co(1, "US0000000001")], {})
        assert out["needs_resolve"] == ["US0000000001"]

    def test_in_the_grid_but_unresolved_needs_resolving(self):
        """`status != 'ok'` is an ISIN we recorded and could not map — a queue job, not a cap job."""
        out = _classify([_co(1, "US1")], {"US1": _g("US1", status="not_found", aid=None)})
        assert out["needs_resolve"] == ["US1"]

    def test_zero_bars_needs_resolving(self):
        """⚠ A ZERO-BAR RESOLUTION IS NOT A RESOLUTION — the ten Leonteq products that all mapped
        to one empty German symbol. It cannot price an index either."""
        out = _classify([_co(1, "US1")], {"US1": _g("US1", bars=0)})
        assert out["needs_resolve"] == ["US1"]

    def test_priced_but_uncapped_needs_a_cap_not_a_resolve(self):
        """The dangerous one: healthy-looking and weightless. Queuing it would be a no-op (the
        queue skips already-ok ISINs), so it would stay invisible for ever."""
        out = _classify([_co(1, "US1")], {"US1": _g("US1", cap=0)})
        assert out["needs_cap"] == ["US1"]
        assert out["needs_resolve"] == []

    def test_a_null_cap_is_the_same_as_zero(self):
        out = _classify([_co(1, "US1")], {"US1": _g("US1", cap=None)})
        assert out["needs_cap"] == ["US1"]

    def test_a_member_with_no_isin_is_named_not_dropped(self):
        """189 ACWI members have none — 156 Indian, 28 British — and no button can reach them:
        the ISIN IS the bridge, and GuruFocus is blind to exactly those markets. A coverage
        number that quietly excludes India is worse than no number."""
        out = _classify([_co(1, None, "Hindustan Aeronautics")], {})
        assert out["no_isin"] == ["Hindustan Aeronautics"]
        assert out["needs_resolve"] == []


class TestTheBucketsPartitionTheUniverse:
    def test_every_member_lands_in_exactly_one_bucket(self):
        companies = [_co(1, "A1"), _co(2, "B2"), _co(3, "C3"), _co(4, None, "No ISIN Ltd")]
        grid = {"A1": _g("A1"), "B2": _g("B2", cap=0)}          # C3 absent -> needs_resolve
        out = _classify(companies, grid)
        assert sum(len(v) for v in out.values()) == len(companies)
        assert out["usable"] == ["A1"]
        assert out["needs_cap"] == ["B2"]
        assert out["needs_resolve"] == ["C3"]
        assert out["no_isin"] == ["No ISIN Ltd"]

    def test_the_isin_is_matched_case_and_space_insensitively(self):
        """The grid is keyed on the upper-cased ISIN; a company row carrying stray case would
        otherwise read as un-ingested and be queued for ever."""
        out = _classify([_co(1, " us0000000001 ")], {"US0000000001": _g("US0000000001")})
        assert out["usable"] == ["US0000000001"]


class TestTheCapConversionIsNotReDerived:
    """⚠ A MARKET CAP IS NOT A PRICE, AND THE MINOR-UNIT RULE IS THE OPPOSITE ONE.

    Yahoo quotes a London listing in PENCE but reports its `marketCap` in POUNDS — same payload,
    same `currency: "GBp"`. Asking `fx_to_eur("GBp")` for a cap divides an already-major figure by
    100: Shell becomes a EUR 1.95bn company and still looks like a number. The fill path therefore
    imports `_cap_currency` rather than carrying its own map.
    """

    def test_the_backfill_normalises_through_the_shared_map(self):
        import inspect

        from routers import _benchmark_fill as f

        src = inspect.getsource(f._backfill_caps)
        assert "_cap_currency" in src
        assert "fx_to_eur(ccy)" in src        # the MAJOR code, never the raw quote currency

    def test_the_shared_map_still_says_pounds(self):
        from scripts.asset_backfill_marketcap import _cap_currency

        assert _cap_currency("GBp") == "GBP"
        assert _cap_currency("USD") == "USD"


class TestResolutionIsNeverRunInline:
    """⚠ ONE YAHOO CONSUMER. An overloaded caller gets an EMPTY result, not a 429, and an empty
    candidate set is how a constituent lands on a thin foreign listing. The fill path must hand
    work to the queue, never resolve in the request thread."""

    def test_it_enqueues_rather_than_resolving(self):
        import inspect

        from routers import _benchmark_fill as f

        src = inspect.getsource(f.fill_benchmark)
        assert "_queue.enqueue" in src
        for forbidden in ("store_one", "resolve(", "fast_resolve"):
            assert forbidden not in src, forbidden


class TestDeleteIsOnlyOfferedWhereFillCanUndoIt:
    """⚠ THE DELETE EXISTS SO FILL CAN BE WATCHED REBUILDING — WHICH IS A PROMISE ABOUT THE LABEL.

    `fill_benchmark`'s only route back is `_build_universe`. Offering Delete for a label it cannot
    rebuild is a one-way door behind a button whose whole point is reversibility, so the guard and
    the rebuild must never disagree about which labels those are: both ask `rebuildable()`.

    SP500 is the case that forced this to be a function rather than `label in TEMPLATES`. It is NOT
    a `UniverseTemplate` — and must not become one, because `/api/index-universe/indexes` excludes
    any universe carrying a `template_key`, so registering it would delete the index from the
    /sp500 page that owns it. Its route back is the Wikipedia reconstruction, wired into
    `_build_universe` directly.
    """

    def test_every_benchmark_the_panel_offers_is_rebuildable(self):
        """The three labels in `BenchmarksPanel.INDICES`. A Delete button on any of them is a
        promise this function has to keep."""
        from routers._benchmark_fill import rebuildable

        for label in ("SP500", "ACWI", "AEX"):
            assert rebuildable(label), label

    def test_an_unknown_label_is_refused_before_it_touches_the_database(self):
        import pytest

        from routers._benchmark_fill import reset_benchmark

        with pytest.raises(ValueError) as e:
            reset_benchmark("not-a-benchmark")
        assert "no way to rebuild" in str(e.value)
        # The refusal names what IS rebuildable — one that leaves you stuck is half an answer.
        assert "SP500" in str(e.value)

    def test_sp500_is_rebuilt_without_being_registered_as_a_template(self):
        """⚠ Registering it would stamp `template_key` on its universe row, and the /sp500 page's
        own list excludes those — the index would vanish from its page as a side effect."""
        from index_universe.templates import TEMPLATES
        from routers._benchmark_fill import rebuildable

        assert "SP500" not in TEMPLATES
        assert rebuildable("SP500")

    def test_the_guard_and_the_rebuild_ask_the_same_question(self):
        """A second list of rebuildable labels is a second thing to keep true."""
        import inspect

        from routers import _benchmark_fill as m

        assert "rebuildable(" in inspect.getsource(m.reset_benchmark)
        assert "rebuildable(" in inspect.getsource(m._build_universe)

    def test_the_sp500_rebuild_resolves_only_the_stored_month(self):
        """⚠ The reconstruction walks back to 2000 — 852 tickers, 286 with no company row, each an
        OpenFIGI lookup for a name delisted a decade ago. `store_index_membership` keeps only the
        newest month anyway, so resolving the history buys nothing and costs the slowest part."""
        import inspect

        from routers._benchmark_fill import _rebuild_sp500

        src = inspect.getsource(_rebuild_sp500)
        assert "monthly[latest]" in src
        # And the changelog is handed back, never emptied — store_index_membership OVERWRITES it.
        assert "filtered_changes" in src


class TestResetUndoesAllThreeOfFillsJobs:
    """Deleting only the membership left two thirds of Fill untested: a constituent that is already
    resolved and already capped goes straight into `usable`, so the cap backfill and the price
    refill never run and the counts read like success without either having done anything.

    The three deletions match Fill's three jobs one for one — and what is NOT deleted is what keeps
    the refill safe.
    """

    def test_the_grid_row_and_the_symbol_are_never_touched(self):
        """⚠ THE PROPERTY THAT STOPS THIS BEING DESTRUCTIVE. Every instrument keeps `status='ok'`,
        its `analysis_id` and its Yahoo symbol, so Fill re-fetches prices for a KNOWN listing
        (`extend_series`). Deleting the grid row or zeroing `bars` would push it into
        `needs_resolve` instead — and a re-resolve is how Alphabet moved from GOOGL to a Vienna
        line 75,000x thinner, because Yahoo answers an overloaded caller with an empty search."""
        import inspect

        from routers import _benchmark_fill as m

        src = inspect.getsource(m.reset_benchmark) + inspect.getsource(m._drop_window_prices)
        for table in ('table("asset_execution")', 'table("asset_grid")', 'table("asset_analysis").delete'):
            assert table not in src, f"reset must not touch {table}"

    def test_prices_are_deleted_as_a_TAIL_not_a_hole(self):
        """⚠ WHY THIS IS SAFE TO OFFER AT ALL. Everything from the lookback forward goes, so each
        series simply ENDS earlier — a state the fleet already repairs (the last close falls behind
        the market anchor, `find_stale` sees it, `extend_series` fetches the gap). An interior
        slice would leave the newest close untouched, no staleness check would ever fire, and the
        hole would be permanent: `extend_series` appends after the last close, it cannot backfill."""
        import inspect

        src = inspect.getsource(__import__("routers._benchmark_fill", fromlist=["x"])._drop_window_prices)
        assert '.gte("target_date"' in src
        assert '.lte("target_date"' not in src, "an upper bound would carve a hole, not a tail"

    def test_the_window_starts_at_the_lookback_not_at_new_year(self):
        """The opening mark is the last close ON OR BEFORE 1 January — 31 December for most names.
        Deleting from the anchor would leave it behind and the benchmark would still price."""
        from routers._benchmark_fill import window_bounds

        lookback, anchor = window_bounds(2026)
        assert anchor == "2026-01-01"
        assert lookback < "2025-12-31"

    def test_the_refill_uses_the_known_symbol_and_never_the_resolve_queue(self):
        import inspect

        from routers._benchmark_fill import _refill_prices

        src = inspect.getsource(_refill_prices)
        assert "extend_series" in src
        assert "enqueue" not in src, "re-resolving is the destructive path; the refill must not use it"

    def test_the_refill_only_touches_constituents_with_no_mark_in_the_window(self):
        """`window_marks` is the same selection the panel prices from, so "needs a price" means
        exactly "the panel cannot price it" — and a press after a completed refill costs one query
        and no Yahoo calls."""
        import inspect

        from routers._benchmark_fill import _refill_prices

        assert "window_marks" in inspect.getsource(_refill_prices)
