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
