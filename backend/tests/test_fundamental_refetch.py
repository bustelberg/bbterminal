"""A forced fill has to defeat TWO caches, and defeating one of them is a button that does nothing.

The Benchmarks panel's Refresh fills a whole index: prices from Yahoo, then fundamentals from
GuruFocus. The price half has always fetched EVERY constituent with no staleness tolerance (see
`_benchmark_refresh`) — a human pressing Refresh is asking us to look. The fundamentals half did
not, and it was short in two independent places:

    1. `metric_data`   `needs()` selects a company only when a SENTINEL ROW IS ABSENT. The sentinel
                       exists the moment a company has ever been loaded, so one loaded a year ago
                       was never selected again — the grid kept showing last year's figures and
                       looked complete. `force` is what ignores that.

    2. Storage         even once selected, `fetch_financials` replays the cached GuruFocus blob
                       whenever `is_cache_fresh` says so — and that window is the data's own
                       cadence plus 50%, i.e. weeks past the quarter the blob is missing. So a
                       forced run without `refresh_cache` re-writes identical rows from the same
                       bytes, spends zero API calls and changes nothing on screen.

⚠ THE TWO ARE ONE LAYER APART AND LOOK ALIKE FROM ABOVE — both present as "the press did nothing"
— which is why they are pinned together here rather than left to be re-derived. Either one alone
reproduces the original complaint.

⚠ AND `force` MUST NOT WIDEN THE FEEDS. `ingest_company(force=True)` runs all THREE GuruFocus
feeds; the index fill under `feeds="statements"` wants ONE (the statements blob carries every
column the fundamentals grid draws, market cap included). So the callers express force as the
`need_*` flags and pass only `refresh_cache` — a test is the cheapest way to stop that being
"tidied" into `force=True`, which would silently triple the spend on data no page renders.

No network, no database: the three fetchers are replaced with recorders.
"""
from __future__ import annotations

import pytest

from routers._fundamental_backfill import ingest_company


class _Result:
    """The shape `ingest_company` reads off an `EarningsResult` — nothing more."""

    def __init__(self, rows: int = 5, calls: int = 1) -> None:
        self.rows_loaded = rows
        self.api_calls = calls


@pytest.fixture
def calls(monkeypatch):
    """Record every feed call as `(tag, force_refresh)`.

    ⚠ PATCHED ON `ingest.earnings`, NOT ON THIS MODULE. `ingest_company` imports the three
    functions INSIDE its body (a deliberate lazy import — pandas and the GuruFocus client are
    expensive), so there is no module-level name here to replace.
    """
    import ingest.earnings as earnings

    seen: list[tuple[str, bool]] = []

    def _make(tag: str):
        def _fn(_sb, _cid, _tic, _exch, *, force_refresh: bool = False):
            seen.append((tag, force_refresh))
            return _Result()
        return _fn

    monkeypatch.setattr(earnings, "fetch_financials", _make("fin"))
    monkeypatch.setattr(earnings, "fetch_analyst_estimates", _make("est"))
    monkeypatch.setattr(earnings, "fetch_indicators", _make("ind"))
    return seen


def _company(**flags) -> dict:
    return {"company_id": 1, "gurufocus_ticker": "AAPL",
            "gurufocus_exchange": {"exchange_code": "NASDAQ"}, **flags}


class TestForceDoesNotWidenTheFeeds:
    """The index fill's `feeds="statements"` must survive a forced run."""

    def test_statements_flags_run_one_feed(self, calls):
        # Exactly what the index fill hands over under force + statements: every flag decided,
        # `need_fin` true because the run is forced, the other two cleared because this page
        # cannot draw them.
        ingest_company(_company(need_fin=True, need_est=False, need_ind=False),
                       refresh_cache=True)
        assert [t for t, _ in calls] == ["fin"]

    def test_the_force_argument_would_have_run_all_three(self, calls):
        """⚠ THIS IS THE TRAP, PINNED AS BEHAVIOUR. `force=True` ignores the flags entirely — it is
        the right switch for "load this company properly" and the wrong one for a statements-scoped
        fill, where it costs two extra API calls per constituent for nothing on screen."""
        ingest_company(_company(need_fin=True, need_est=False, need_ind=False), force=True)
        assert [t for t, _ in calls] == ["fin", "est", "ind"]


class TestTheStorageCacheIsOnlyBypassedWhenAsked:
    """`refresh_cache` is what turns a selection into a re-fetch."""

    def test_default_leaves_the_blob_cache_in_place(self, calls):
        # The grid's per-row Fetch and every background backfill land here: replaying a fresh blob
        # is the right economy when nobody asked us to look again.
        ingest_company(_company(need_fin=True, need_est=False, need_ind=False))
        assert calls == [("fin", False)]

    def test_refresh_cache_reaches_the_fetcher(self, calls):
        # ⚠ THE WHOLE POINT. Without this the forced run re-writes the same rows from the same
        # bytes: zero API calls, no change, and a button that reads as broken.
        ingest_company(_company(need_fin=True, need_est=False, need_ind=False),
                       refresh_cache=True)
        assert calls == [("fin", True)]

    def test_it_applies_to_every_feed_it_runs(self, calls):
        ingest_company(_company(), force=True, refresh_cache=True)
        assert calls == [("fin", True), ("est", True), ("ind", True)]


class TestASelectedCompanyIsStillReportedHonestly:
    """The counts a forced run reports are the ones the quota chip and the receipt read."""

    def test_rows_and_calls_are_summed_across_the_feeds_that_ran(self, calls):
        r = ingest_company(_company(need_fin=True, need_est=False, need_ind=False),
                           refresh_cache=True)
        assert r["rows"] == 5
        # ⚠ WHAT WAS SPENT, NOT WHAT WAS ASKED FOR — a feed served from a fresh cache reports zero.
        assert r["calls"] == 1
        assert r["error"] is None
        assert r["done"] == ["fin 5"]
