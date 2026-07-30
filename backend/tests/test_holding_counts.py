"""The holdings count came off a capped read of a table that grows on every scan.

`_holding_counts` used to `select(...).limit(20000)` over ALL of `airs_holding` and reduce it in
Python. That table keeps one snapshot per account PER DATE: measured 2026-07-30 it held 9,817 rows
across 18 snapshot dates for 39 accounts, and every scan adds another day. The moment it crosses
20,000 PostgREST returns the first page and says nothing — accounts whose newest rows fall outside
it get a count of 0 or a stale date, which the portfolios page renders as a book holding nothing.

Silent, and on the very column the list is now filtered by. It is aggregated in Postgres instead:
one row per account (~44) rather than one per holding (~10,000), so the answer no longer depends on
how much history has accumulated. The COPY-less fallback PAGES rather than caps — raising a limit
only moves a cliff.
"""
from __future__ import annotations

from routers._airs_accounts import parse_holding_counts_csv


class TestParsingTheAggregate:
    """Four columns: `portefeuille, as_of_date, holdings, isins`.

    The ISIN count was added because the ISINs column used to report the paired MODEL's position
    count — so a book with no model showed "—" beside 22 holdings you could see the moment you
    expanded it, and a paired book's figure described a different object entirely.
    """

    def test_one_row_per_account(self):
        counts, newest, isins = parse_holding_counts_csv(
            "AITopSelectie OFF DYN,2026-07-29,21,20\nBUS_FTS_DEF_DYN,2026-07-29,28,27\n")
        assert counts == {"AITopSelectie OFF DYN": 21, "BUS_FTS_DEF_DYN": 28}
        assert isins == {"AITopSelectie OFF DYN": 20, "BUS_FTS_DEF_DYN": 27}
        assert newest["BUS_FTS_DEF_DYN"] == "2026-07-29"

    def test_the_isin_count_is_lower_than_the_holdings_count(self):
        """⚠ NOT A DISCREPANCY. The cash line is a holding and has no ISIN, so a real book reads
        24 holdings / 23 ISINs. Collapsing them into one number would either invent an ISIN for
        cash or drop cash from the book."""
        counts, _, isins = parse_holding_counts_csv("BUS_Ris_bepOff,2026-07-29,24,23\n")
        assert counts["BUS_Ris_bepOff"] == 24
        assert isins["BUS_Ris_bepOff"] == 23

    def test_a_name_with_spaces_survives(self):
        counts, _, _ = parse_holding_counts_csv("WTS test 1 FX,2026-07-30,0,0\n")
        assert counts == {"WTS test 1 FX": 0}

    def test_a_name_containing_a_COMMA_survives(self):
        """⚠ WHY THIS IS PARSED AS CSV AND NOT `line.split(',')`. Postgres quotes a field holding
        the delimiter; a naive split shifts every column on that row, and the account silently gets
        another account's count — a wrong number, not a missing one."""
        counts, newest, isins = parse_holding_counts_csv('"Smith, J. Beheer",2026-07-29,17,16\n')
        assert counts == {"Smith, J. Beheer": 17}
        assert isins == {"Smith, J. Beheer": 16}
        assert newest["Smith, J. Beheer"] == "2026-07-29"

    def test_a_short_or_blank_line_is_skipped_not_guessed_at(self):
        counts, _, _ = parse_holding_counts_csv("\nBROKEN\nA,2026-07-29,3,3\n")
        assert counts == {"A": 3}

    def test_zero_is_kept_as_zero(self):
        """A book that genuinely holds nothing must read 0, not go missing — the two look the same
        on the page and mean opposite things."""
        counts, _, isins = parse_holding_counts_csv("BUS_Defensief_Kl_MV,2026-07-30,0,0\n")
        assert counts["BUS_Defensief_Kl_MV"] == 0
        assert isins["BUS_Defensief_Kl_MV"] == 0


class TestTheReadHasNoSilentCap:
    def test_neither_path_uses_a_bare_limit(self):
        """⚠ THE REGRESSION THIS FILE EXISTS FOR. A `.limit(N)` over `airs_holding` is a cliff that
        arrives with no error whatever N is; the fallback must PAGE."""
        import inspect

        from routers import _airs_accounts as m

        for fn in (m._holding_counts, m._holding_counts_paged):
            # The docstrings NAME the cap they replaced, so scan the code below them.
            src = inspect.getsource(fn)
            code = src.split('"""')[2] if src.count('"""') >= 2 else src
            assert ".limit(" not in code, f"{fn.__name__} must not cap the read"
        assert ".range(" in inspect.getsource(m._holding_counts_paged)

    def test_the_aggregation_happens_in_postgres(self):
        import inspect

        from routers import _airs_accounts as m

        src = inspect.getsource(m._holding_counts)
        assert "COPY (" in src and "count(*)" in src
        # One row per account, newest snapshot only — the whole point of doing it server-side.
        assert "DISTINCT ON (portefeuille)" in src
        # And the ISIN count is computed there too, not by a second read.
        assert "count(DISTINCT isin)" in src

    def test_both_paths_return_the_same_shape(self):
        """A caller unpacks three dicts; a fallback returning two would raise only where COPY is
        unavailable — i.e. never in dev and always in the one deployment that lacks it."""
        import inspect

        from routers import _airs_accounts as m

        for fn in (m._holding_counts, m._holding_counts_paged):
            sig = inspect.signature(fn)
            assert "tuple[dict[str, int], dict[str, str], dict[str, int]]" in str(sig.return_annotation)
