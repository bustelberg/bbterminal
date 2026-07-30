"""Persisting the AIRS model portfolios — the rules that keep the stored table honest.

Storing a scrape is where "absent" quietly becomes "zero". Three of these tests exist only
to stop that, and one stops a stale holding outliving the model it came from.
"""
from __future__ import annotations

import inspect

from routers import _airs_portfolio_store as store


def _body(fn) -> str:
    """Source with the docstring stripped. These assertions are about what the CODE does —
    matching the prose that explains it would pass (or fail) for the wrong reason."""
    src = inspect.getsource(fn)
    return src.split('"""')[-1] if '"""' in src else src


class TestTheCountIsDerivedNeverStored:
    def test_no_holdings_column_is_written(self):
        """`holdings` is computed by the `airs_model_portfolio_grid` VIEW from the positions
        themselves. A stored integer would be a second source of truth that can drift from
        the rows it claims to count — and a count that disagrees with its own evidence is
        worse than no count."""
        src = inspect.getsource(store)
        assert '"holdings"' not in src

    def test_the_view_excludes_cash_from_the_count(self):
        """Cash ("Liquiditeiten") has no ISIN and is not an instrument."""
        from pathlib import Path

        sql = Path("../supabase/migrations/20260713060000_airs_model_portfolio.sql").read_text(
            encoding="utf-8")
        # The count must be restricted to ISIN-bearing rows.
        body = sql.split("CREATE OR REPLACE VIEW", 1)[1]
        assert "pos.isin IS NOT NULL" in body


class TestTheThreeAbsencesStayApart:
    """`no model` / `never counted` / `a real 0` are three different facts. The schema has to
    be able to say all three, or the column starts lying the moment it is persisted."""

    def test_a_failed_fetch_does_not_stamp_scanned_at(self):
        """If it did, the row would read as "counted: 0 holdings" — a fact we never learned.
        Only the error is recorded."""
        src = _body(store.save_positions_error)
        assert "positions_error" in src
        assert "positions_scanned_at" not in src

    def test_an_empty_answer_IS_still_an_answer(self):
        """`positions_scanned_at` must be stamped even when AIRS returned nothing — we DID
        look. If it were only stamped when rows existed, a portfolio with no composition
        would sit for ever as "never counted", which is a different (and false) claim.

        `positions_datum`, by contrast, is deliberately conditional: it is set ONLY when a
        composition actually came back, and its NULLness is what the view reads as
        `no_snapshot`. So the two fields are guarded differently on purpose."""
        src = _body(store.save_positions)
        assert "positions_scanned_at" in src
        # No early exit before the stamp — that is the only way it could be skipped.
        before = src.split("positions_scanned_at", 1)[0]
        assert "return" not in before, "an empty result must still reach the stamp"
        # And the datum IS conditional on rows — the no-snapshot signal.
        assert "if rows else None" in src

    def test_the_list_save_never_touches_the_positions_columns(self):
        """The list page knows nothing about positions. If `save_portfolios` wrote them, a
        cheap re-list would wipe counts we already paid minutes for."""
        src = _body(store.save_portfolios)
        assert "positions_scanned_at" not in src
        assert "positions_datum" not in src


class TestAVanishedPositionActuallyVanishes:
    def test_the_refresh_deletes_before_it_inserts(self):
        """An upsert would leave a position that was REMOVED from the model sitting in our
        table for ever. A stale holding that looks current is worse than no holding."""
        src = _body(store.save_positions)
        assert ".delete()" in src
        assert src.index(".delete()") < src.index(".insert(")


class TestTheNanIsinTrap:
    def test_a_nan_isin_is_never_stored(self):
        """pandas hands back a float NaN for the cash line's empty ISIN; `str()` turns it
        into the literal "nan", which is TRUTHY — so it would be stored as an ISIN and
        counted as a holding. `_parse_positions_xls` already guards this; the store refuses
        it again, because a bogus ISIN in the DB is a join into `asset_execution` that
        silently matches nothing."""
        src = _body(store.save_positions)
        assert '"nan"' in src

    def test_nan_really_is_truthy(self):
        assert bool("nan") is True
        assert bool(float("nan")) is True


class TestPositionsAreServedFromTheCache:
    """Expanding a portfolio used to re-scrape AirSPMS every time — a 14s authenticated
    round-trip for an XLS the scan had ALREADY downloaded to count the holdings. Measured on
    VTopSelectie OFF FX: live 14.26s, cached 0.08s, identical rows. 183x."""

    def test_the_cached_shape_matches_the_live_one(self):
        """`load_positions` returns AIRS's OWN column names, so `_shape_positions` consumes
        cached and live through the SAME code path. One path means the cached answer cannot
        drift from the live one — the bug a parallel shaper would eventually introduce."""
        src = _body(store.load_positions)
        for airs_column in ("Fonds", "ISINCode", "Percentage", "valuta",
                            "Beleggingscategorie", "Beleggingssector", "regio"):
            assert airs_column in src

    def test_known_instrument_is_NOT_cached(self):
        """It is a join against `asset_execution`, which GROWS every time we add an
        instrument. A cached "not in grid" flag is wrong the moment the grid catches up, so
        it is recomputed on every read — `load_positions` must not store or return it."""
        src = _body(store.load_positions)
        assert "known_instrument" not in src

    def test_a_never_scanned_portfolio_returns_none_not_an_empty_answer(self):
        """None means "ask AIRS". An empty answer would mean "AIRS says it holds nothing" —
        a completely different claim, and the caller would cache a lie."""
        src = _body(store.load_positions)
        assert 'positions_scanned_at' in src
        assert "return None" in src

    def test_a_historical_datum_is_never_written_back_to_the_cache(self):
        """We store ONE snapshot per portfolio — the newest. If picking an old date from the
        dropdown overwrote it, the cache would silently rot BACKWARDS: the stored composition
        would become an old one while still being served as current."""
        import inspect

        from routers import airs

        src = inspect.getsource(airs._live_positions)
        assert "if datum is None" in src, "only the default snapshot may be cached"
        assert src.index("if datum is None") < src.index("save_positions")
