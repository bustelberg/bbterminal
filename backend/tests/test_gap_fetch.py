"""The undocumented date filter, and the two ways it bites.

`stock/{sym}/price?start_date=&end_date=` is real (verified 2026-08-02 on AAPL,
XPAR:WLN and WBO:VER, price and volume): one day of Apple is **23 bytes against
268,703**, an 11,682× cut, with values identical to the unfiltered series.

    stored max 2026-07-28  ->  ?start_date=2026-07-29  ->  3 bars, 1 request
    (was: 11,501 bars downloaded and discarded to add three)

TWO TRAPS, BOTH MEASURED, BOTH PINNED HERE.
"""
from __future__ import annotations

import inspect
from datetime import date, timedelta

from ingest import prices


class TestOnlyOneSpellingFilters:
    """⚠ `?from=&to=`, `?start=&end=`, `?date=`, `?limit=`, `?period=`, `?days=`
    are ALL accepted with HTTP 200 and return the FULL series. Send one and you
    get an 11,501-bar answer to a one-day question — it parses fine and is wrong
    about what you asked, with nothing anywhere to tell you."""

    def test_the_url_uses_start_date_and_end_date(self):
        src = inspect.getsource(prices._fetch_indicator_from_api)
        assert "?start_date={lo}&end_date={hi}" in src

    def test_the_ignored_spellings_are_written_down(self):
        doc = prices._fetch_indicator_from_api.__doc__ or ""
        for ignored in ("?from=", "?start=", "?date=", "?limit=", "?period="):
            assert ignored in doc, f"{ignored} must be recorded as silently ignored"


class TestEndDateTodayInventsABar:
    """⚠ THE TRAP THAT CREATED A PHANTOM DURING DEVELOPMENT OF THIS VERY FEATURE.

        ?start_date=2026-07-29&end_date=2026-07-31  -> 3 real bars
        ?start_date=2026-07-29&end_date=2026-08-02  -> the same 3, PLUS
                                                       2026-08-02 = 308.91,
                                                       Friday's close repeated

    The extra row is the live-quote line dated today. The UNFILTERED endpoint
    never emits it, so a windowed fetch manufactures weekend/holiday bars that a
    full fetch would not — the exact phantoms a repair had just removed from
    VERBUND, GFI, 00883 and BEKE.
    """

    def test_a_gap_fetch_never_asks_beyond_yesterday(self):
        assert prices._settled_through(None) == date.today() - timedelta(days=1)

    def test_a_future_data_cutoff_cannot_reinstate_it(self):
        """`data_cutoff` is the CALLER's notion of now (a backfill, a test). The
        synthesised row is tied to the VENDOR's clock, so trusting a future
        cutoff would ask for a window containing the real today — which is
        precisely how the phantom got in."""
        future = date.today() + timedelta(days=8)
        assert prices._settled_through(future) == date.today() - timedelta(days=1)

    def test_a_past_cutoff_is_honoured(self):
        """A genuine backfill replaying an old date must still be able to ask for
        that date's window."""
        past = date(2020, 6, 15)
        assert prices._settled_through(past) == date(2020, 6, 14)

    def test_both_gap_paths_bound_the_window(self):
        for fn in (prices.ensure_prices_for_company, prices.ensure_volume_for_company):
            src = inspect.getsource(fn)
            assert "_settled_through(data_cutoff)" in src, f"{fn.__name__} is unbounded"


class TestTheGapPathIsUsedButNotAlways:
    def test_it_only_runs_when_we_already_have_data(self):
        """With no stored bars there is no gap to ask for — the full series is
        the only correct request."""
        src = inspect.getsource(prices.ensure_prices_for_company)
        assert "if db_max is not None and not force_refresh:" in src

    def test_a_forced_refresh_still_takes_the_full_path(self):
        """`force_refresh` exists to re-read everything; a window would defeat it."""
        src = inspect.getsource(prices.ensure_prices_for_company)
        gap = src.split("GAP FETCH", 1)[1].split("if db_max is not None", 1)[1][:200]
        assert "not force_refresh" in src.split("GAP FETCH", 1)[1][:400] or "force_refresh" in gap

    def test_it_bypasses_storage(self):
        """⚠ The cached blob is the FULL series. Overwriting it with a window —
        or merging into it — would truncate the only copy of the history we keep
        outside the DB."""
        src = inspect.getsource(prices.ensure_prices_for_company)
        gap = src.split("GAP FETCH", 1)[1].split("# A 403/404", 1)[0]
        assert "_upload_to_storage" not in gap
        assert "_fetch_from_storage" not in gap

    def test_a_failed_gap_falls_back_to_the_full_path(self):
        """A 403/404/network error still has to reach the classification logic
        (forbidden / delisted / stale-cache) that only the full path has."""
        src = inspect.getsource(prices.ensure_prices_for_company)
        assert "falling back to the full series" in src


class TestTheDailyDriftProbe:
    def test_the_slice_covers_everyone_within_a_week(self):
        from ingest.history_drift import SLICE_DIVISOR, slice_for_day

        ids = list(range(1, 1480))
        seen: set[int] = set()
        for i in range(SLICE_DIVISOR):
            seen |= set(slice_for_day(ids, date(2026, 8, 2) + timedelta(days=i)))
        assert seen == set(ids)
        assert SLICE_DIVISOR <= 7, "every company must come round within a week"

    def test_the_slices_are_disjoint(self):
        from ingest.history_drift import slice_for_day

        a = set(slice_for_day(list(range(1, 100)), date(2026, 8, 2)))
        b = set(slice_for_day(list(range(1, 100)), date(2026, 8, 3)))
        assert not (a & b)

    def test_it_probes_close_price_only(self):
        """A corporate action re-scales BOTH series, so the close detects it and
        the escalation refetches both anyway. Probing volume too would double the
        daily request bill for no additional detection."""
        from ingest import history_drift

        assert history_drift._PROBE_METRIC == ("close_price", "price")

    def test_it_escalates_to_a_FULL_fetch_not_a_second_probe(self):
        """Two sequential probes cost two requests and can still both miss a
        one-bar correction; the full series cannot."""
        from ingest import history_drift

        src = inspect.getsource(history_drift.check_drift)
        assert "refetch_full_history(sorted(drifted)" in src

    def test_a_missing_upstream_bar_counts_as_drift(self):
        """Our oldest bar absent from the vendor's window is either a phantom of
        ours or a truncated history — both are answered by the full series, not
        by guessing."""
        from ingest import history_drift

        src = inspect.getsource(history_drift.check_drift)
        assert "_absent" in src and "drifted.add(cid)" in src
