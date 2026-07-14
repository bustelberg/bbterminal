"""A benchmark rebuilt in the ASSET world (yfinance), joined by ISIN.

WHY IT EXISTS
    `_benchmark_index` prices off GuruFocus, and GuruFocus sells a subscription WITH HOLES: no
    UK, no India, no Ireland, no Australia/NZ, no Africa, no LatAm. Invisible for the S&P (a US
    index). DISQUALIFYING for ACWI:

        LSE (UK)     72 ACWI members    GuruFocus prices  0
        NSE (India) 160 ACWI members    GuruFocus prices  0

    ~7.8% of ACWI's published weight is in countries GuruFocus will never price. A cap-weighted
    index renormalised over the other 92% does not LOSE that weight — it redistributes it into
    everything else. That is a bias, and no care inside the arithmetic removes it.

    yfinance has no such holes, and we already hold those prices in `asset_price`.
"""
from __future__ import annotations

import inspect

from routers import _asset_benchmark as ab


class TestTheBridgeIsAJoinNotAColumn:
    """A membership FLAG on `asset_execution` was the obvious alternative, and it is a trap: the
    ACWI universe is RECONSTRUCTED on a schedule, so the flag would need re-syncing on every
    refresh — and the day it drifts, the benchmark is quietly wrong with no error anywhere.

    Same rule the holdings count already follows: *the count is a VIEW, never a column.* A join
    cannot drift, because it has nothing to keep in sync.
    """

    def test_membership_is_resolved_through_the_isin(self):
        src = inspect.getsource(ab.members)
        assert 'table("company")' in src and "isin" in src
        assert 'table("asset_grid")' in src

    def test_no_membership_column_is_read(self):
        """If someone adds `asset_execution.acwi`, this catches the moment it gets read."""
        src = inspect.getsource(ab)
        for flag in ('"acwi"', "'acwi'", '"in_sp500"', '"universe_label"'):
            assert flag not in src.lower(), f"membership must be a join, not a stored flag ({flag})"


class TestTheWeightingIsREUSEDNotCopied:
    """⚠ START-OF-WINDOW CAP WEIGHTS. Weighting by TODAY's cap is look-ahead bias — it turned
    +9.10% into +21.70%. A second copy of that loop is a second place for the bias to grow back,
    so this module supplies `members` + `closes` from a different source and calls the SAME
    `_benchmark_index._window_rows` that /benchmarks uses."""

    def test_it_calls_the_shared_window_rows(self):
        assert "_window_rows(mem, closes, fx, s)" in inspect.getsource(ab.index_returns)

    def test_it_does_not_reimplement_the_cap_rollback(self):
        src = inspect.getsource(ab)
        assert "cap_start_eur" not in src, "the weighting lives in _window_rows, not here"


class TestOneCompanyOneRow:
    def test_share_classes_are_deduped(self):
        """Yahoo, like GuruFocus, puts the FULL company cap on EVERY share class. Alphabet is
        GOOGL *and* GOOG, each carrying the whole cap — a naive sum counts it twice (11.3% of the
        S&P's weight, fictional)."""
        src = inspect.getsource(ab.members)
        assert "by_name" in src
        assert 'c.get("company_name") or ""' in src


class TestCoverageIsNeverAssumed:
    """ACWI's missing names go A WHOLE COUNTRY AT A TIME. Reporting a renormalised index without
    saying what it was renormalised over is the same invention the portfolio returns refuse to
    make — and here it is systematic, not random."""

    def test_members_returns_its_own_coverage(self):
        src = inspect.getsource(ab.members)
        assert '"covered_pct"' in src
        assert '"universe_members"' in src and '"priced"' in src

    def test_coverage_rides_along_with_every_window(self):
        assert "**coverage" in inspect.getsource(ab.index_returns)

    def test_a_row_with_no_cap_cannot_be_weighted_and_is_dropped(self):
        """A freshly ingested row has BARS but no market cap. Weighting it as zero would silently
        delete it from the index; keeping it needs a cap. It is excluded and counted."""
        src = inspect.getsource(ab.members)
        assert "if not g or cap <= 0:" in src
        assert "continue" in src.split("if not g or cap <= 0:", 1)[1][:60]


class TestThePortfolioAndTheBenchmarkSharePriceUniverse:
    """⚠ The portfolio's return comes from `asset_price` (yfinance). Pricing the index off
    GuruFocus would compare two price universes — different adjustment conventions, different FX
    — and call the difference alpha."""

    def test_the_analysis_prices_its_benchmark_in_the_asset_world(self):
        from routers import _airs_portfolio_analysis as pa

        src = inspect.getsource(pa)
        assert "from routers._asset_benchmark import index_returns" in src
        assert "from routers._asset_benchmark import members as _members" in src
