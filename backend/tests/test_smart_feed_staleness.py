"""When a smart press re-asks for a feed — "when did WE ask", never "when did a row appear".

⚠⚠ THE MEASUREMENT (ACWI, 2026-08-17). `smart_flags` decides per feed with "missing OR stale", and
stale used to mean `max(metric_data.recorded_at)` on the feed's sentinel row. A row only appears
when GuruFocus HAS something, so for a company it publishes no consensus for BOTH halves stayed
true for ever and the feed was re-asked on every single press:

    calls in one press          4,326  (1.80 h at the global 1.5s gate)
      estimates                 1,464  of which 1,125 (77%) hold NOTHING
      indicators                1,551  of which 1,267 (82%) hold NOTHING

`recorded_at` could never have served: it has a `DEFAULT CURRENT_TIMESTAMP` and no update trigger,
so an upsert leaves it alone — it has always meant "when this row first appeared", never "when we
last looked". `company.<feed>_fetched_at` (migration `20260817000000`) records the asking, exactly
as `financials_fetched_at` already did for the statements.

Simulated over the real ACWI membership, holding `have`/`due` fixed and varying only the stamps:

    press again tomorrow, old rule      3,427 calls   1.43 h
    press again tomorrow, new rule          1 call    0.00 h
    press again in 8 days, new rule     1,155 calls   0.48 h
    press again in 31 days, new rule    3,427 calls   1.43 h
"""
from __future__ import annotations

from datetime import date

import pytest

from routers._fundamental_backfill import (
    FEED_FETCHED_AT,
    SMART_REFRESH_AFTER_DAYS,
    SMART_RETRY_EMPTY_AFTER_DAYS,
    _is_stale,
    asked_at_map,
)

TODAY = date(2026, 8, 17)


class TestNeverAskedIsAlwaysDue:
    """⚠ EVERY COMPANY READS `None` THE FIRST TIME, which is the strongest reason to fetch there is.
    Reading it as "not stale" would make a feed we have never fetched look permanently up to date —
    and after the migration lands, that is every row."""

    def test_none_is_stale_whether_or_not_we_hold_rows(self):
        assert _is_stale(None, TODAY, has_rows=True)
        assert _is_stale(None, TODAY, has_rows=False)


class TestAFeedWeHoldIsReAskedWeekly:
    """The consensus and the forward-P/E series are continuously revised, so the question is "has it
    changed" and the answer arrives on the weekly cadence `SMART_REFRESH_AFTER_DAYS` is taken from."""

    def test_asked_yesterday_is_not_re_asked(self):
        assert not _is_stale(date(2026, 8, 16), TODAY, has_rows=True)

    def test_the_boundary_day_is_due(self):
        # ⚠ `>=`, not `>`. An off-by-one costs a day of staleness on every company, every week.
        asked = date(2026, 8, 17 - SMART_REFRESH_AFTER_DAYS)
        assert _is_stale(asked, TODAY, has_rows=True)
        assert not _is_stale(date(asked.year, asked.month, asked.day + 1), TODAY, has_rows=True)


class TestAFeedThatCameBackEMPTYIsLeftAloneForLonger:
    """⚠⚠ THIS IS THE HOUR. Most of a broad index carries no analyst consensus at all, and whether a
    company GAINS coverage is a slow, rare event — a different question from "has the consensus been
    revised", with a very different answer rate. Asking those weekly buys nothing; asking them every
    press (which is what happened) buys less than nothing."""

    def test_an_empty_feed_is_not_re_asked_after_a_week(self):
        assert not _is_stale(date(2026, 8, 10), TODAY, has_rows=False)   # 7 days
        assert _is_stale(date(2026, 8, 10), TODAY, has_rows=True)        # same date, held feed

    def test_it_is_re_asked_once_the_longer_window_passes(self):
        old = date(2026, 7, 18)                       # exactly SMART_RETRY_EMPTY_AFTER_DAYS back
        assert (TODAY - old).days == SMART_RETRY_EMPTY_AFTER_DAYS
        assert _is_stale(old, TODAY, has_rows=False)

    def test_the_two_windows_are_not_the_same_number(self):
        """If they ever collapse, the saving disappears silently and nothing fails."""
        assert SMART_RETRY_EMPTY_AFTER_DAYS > SMART_REFRESH_AFTER_DAYS


class TestTheStampIsReadPerFeed:

    def test_each_feed_has_its_own_column(self):
        """⚠ ONE COLUMN PER FEED, NOT ONE PER COMPANY. They are three separate GuruFocus calls with
        three separate answers; a single "fundamentals_fetched_at" would let a statements fetch
        silence the estimates question."""
        assert FEED_FETCHED_AT["fin"] == "financials_fetched_at"
        assert FEED_FETCHED_AT["est"] == "estimates_fetched_at"
        assert FEED_FETCHED_AT["ind"] == "indicators_fetched_at"
        assert len(set(FEED_FETCHED_AT.values())) == 3

    def test_it_parses_a_timestamp_to_a_date(self):
        got = asked_at_map([{"company_id": 7, "estimates_fetched_at": "2026-08-16T09:12:00+00:00"}],
                           "est")
        assert got == {7: date(2026, 8, 16)}

    def test_a_null_stamp_is_absent_not_epoch(self):
        """⚠ ABSENT MEANS NEVER ASKED, and `_is_stale(None)` is True — so a NULL must not become a
        parsed date. Defaulting it to an old date would be the same answer by luck; defaulting it to
        `today` would silence a company we have never fetched, for ever."""
        rows = [{"company_id": 1, "estimates_fetched_at": None},
                {"company_id": 2, "estimates_fetched_at": ""},
                {"company_id": 3, "estimates_fetched_at": "2026-08-16"}]
        assert asked_at_map(rows, "est") == {3: date(2026, 8, 16)}

    def test_an_unparseable_stamp_is_treated_as_never_asked(self):
        assert asked_at_map([{"company_id": 1, "indicators_fetched_at": "not a date"}], "ind") == {}

    def test_the_columns_do_not_bleed_into_each_other(self):
        row = [{"company_id": 1,
                "estimates_fetched_at": "2026-08-16",
                "indicators_fetched_at": "2026-01-02"}]
        assert asked_at_map(row, "est") == {1: date(2026, 8, 16)}
        assert asked_at_map(row, "ind") == {1: date(2026, 1, 2)}


class TestTheCadenceItProduces:
    """The shape of the saving, stated as the arithmetic rather than as a claim. `has_rows` is what
    picks the window, so these are the four cases a press actually meets."""

    @pytest.mark.parametrize("asked,has_rows,days,due", [
        (None, False, 0, True),                        # never asked -> always
        (None, True, 0, True),
        (date(2026, 8, 16), True, 1, False),           # asked yesterday, feed held -> quiet
        (date(2026, 8, 16), False, 1, False),          # asked yesterday, feed empty -> quiet
        (date(2026, 8, 9), True, 8, True),             # a week on, held feed -> re-ask
        (date(2026, 8, 9), False, 8, False),           # a week on, empty feed -> still quiet
        (date(2026, 7, 17), False, 31, True),          # a month on, empty feed -> re-ask
    ])
    def test_case(self, asked, has_rows, days, due):
        assert (TODAY - asked).days == days if asked else True
        assert _is_stale(asked, TODAY, has_rows=has_rows) is due
