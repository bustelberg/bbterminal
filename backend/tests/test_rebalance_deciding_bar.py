"""A rebalance is decided on the TRADING CALENDAR, never on the calendar month.

    ✗ Run now failed
    Momentum rebalance phase failed: 3 of 3 strategies failed:
      [MomentumTopSelectie Offensief] Cannot compute current picks for 2026-08: latest
      price in DB is 2026-07-31 (2 days behind today).                 <- 2026-08-02

Nothing was stale. **August 2026 opens on a Saturday**, so on Sunday the 2nd the newest
close in existence anywhere on earth was Friday 31 July — and the first Monday's
rebalance (the 3rd) is decided on exactly that bar, because the signal cutoff is strict
`<` the rebalance date and never reads the day it trades.

The gate asked a different question: *"is there a close dated inside the current calendar
month?"* — which on the 1st and 2nd of a month opening on a weekend is unanswerable in the
affirmative no matter how fresh the data is. And it was not a weekend-only nuisance: the
05:00 UTC tick on Monday the 3rd would have hit it too (Monday's own close does not exist
at 05:00), so August's SCHEDULED rebalance was going to fail the same way.

The second half of the incident is the one that costs money quietly: the rebalance ranked
its universe off whatever prices happened to be in the DB. The price-update op refreshes
the ~24 HELD names daily and never the ~1,455 other candidates, and stale candidates do
not raise — `signals.py` DROPS anything more than 30 days old. So the selection was made
from an arbitrary fresh subset. The universe is now fetched up to the deciding bar first.
"""
from __future__ import annotations

import inspect
from datetime import date

from momentum.backtest import current_portfolio
from momentum.backtest.dates import (
    current_rebalance_date,
    deciding_bar,
    is_decidable,
    sessions_between,
)


class TestTheBarARebalanceNeeds:
    def test_the_first_monday_of_august_2026_is_decided_on_friday_the_31st(self):
        assert date(2026, 8, 1).weekday() == 5          # Saturday — the whole trap
        rebal = current_rebalance_date(date(2026, 8, 2), 0)
        assert rebal == date(2026, 8, 3)
        assert deciding_bar(rebal) == date(2026, 7, 31)

    def test_a_wednesday_strategy_needs_a_later_bar_than_a_monday_one(self):
        """Different weekdays, different deciding bars — the fetch target is the LATEST."""
        mon = deciding_bar(current_rebalance_date(date(2026, 8, 2), 0))
        wed = deciding_bar(current_rebalance_date(date(2026, 8, 2), 2))
        assert mon == date(2026, 7, 31)
        assert wed == date(2026, 8, 4)                  # first Wed is the 5th
        assert max(mon, wed) == wed


class TestTheCalendarMonthGateWasUnsatisfiable:
    def test_sunday_the_2nd_on_fridays_close_is_decidable(self):
        """The exact state of the failed run."""
        assert is_decidable(
            date(2026, 8, 3), today=date(2026, 8, 2), latest_data_date=date(2026, 7, 31),
        )

    def test_the_old_month_anchored_rule_rejected_that_same_state(self):
        """Pinned as the regression it is: `latest < first-of-month` is a question about
        the wall calendar, and the answer on the 1st and 2nd of a month that opens on a
        weekend is 'no' however current the data is."""
        latest, today = date(2026, 7, 31), date(2026, 8, 2)
        assert latest < date(today.year, today.month, 1)      # the old gate said: reject
        assert is_decidable(date(2026, 8, 3), today=today, latest_data_date=latest)

    def test_the_monday_0500_tick_would_have_failed_too(self):
        """Monday's own close does not exist at 05:00 UTC, so the deciding bar is still
        Friday's — which the month gate also rejects. This was not a weekend-only bug."""
        assert is_decidable(
            date(2026, 8, 3), today=date(2026, 8, 3), latest_data_date=date(2026, 7, 31),
        )


class TestItStillRefusesGenuinelyStaleData:
    def test_three_weeks_behind_is_not_decidable(self):
        assert not is_decidable(
            date(2026, 8, 3), today=date(2026, 8, 2), latest_data_date=date(2026, 7, 10),
        )

    def test_a_bar_that_has_not_HAPPENED_yet_is_never_forgiven(self):
        """The holiday forgiveness is for a bar that passed un-traded, never for one in
        the future — waiting is the correct answer there, and entering early would price
        the period off a bar nobody has seen."""
        # First Friday of Aug 2026 is the 7th → deciding bar Thu the 6th, still ahead.
        rebal = current_rebalance_date(date(2026, 8, 5), 4)
        assert rebal == date(2026, 8, 7)
        assert not is_decidable(
            rebal, today=date(2026, 8, 5), latest_data_date=date(2026, 8, 5),
        )

    def test_a_weekday_that_never_traded_is_forgiven_once_it_is_past(self):
        """Fri 2026-07-03: the US July-4th observance. `deciding_bar` is holiday-unaware,
        so it points at a day with no bar; the real one is Thu the 2nd."""
        assert deciding_bar(date(2026, 7, 6)) == date(2026, 7, 3)
        assert is_decidable(
            date(2026, 7, 6), today=date(2026, 7, 6), latest_data_date=date(2026, 7, 2),
        )


class TestNoNameIsBoughtAtAStalePrice:
    """⚠ THE SECOND HALF OF THE SAME INCIDENT.

        140.90 USD  2026-07-28    →    143.83  2026-07-31

    An entry dated three sessions before the bar the basket actually enters on.
    `_price_on_or_before` walks back until it finds A price, so a company whose
    series stopped early is entered at that older close — and the move between it
    and the deciding bar is then reported as portfolio return. Here: +2.08% on a
    position opened on the 31st.

    A stale name is therefore dropped from SELECTION (before scoring, so the book
    still fills to its configured size) rather than entered at a price we don't
    have. One session of tolerance, because exchange calendars differ.
    """

    def test_the_tolerance_is_counted_in_SESSIONS_not_calendar_days(self):
        """A Tuesday bar and a Thursday bar are both three calendar days from the
        following Friday; one has missed three sessions, the other one."""
        friday = date(2026, 7, 31)
        assert sessions_between(date(2026, 7, 30), friday) == 1     # Thu → one missed
        assert sessions_between(date(2026, 7, 28), friday) == 3     # Tue → three
        # ...and a weekend is not a gap at all.
        assert sessions_between(date(2026, 7, 31), date(2026, 8, 3)) == 1

    def test_one_missed_session_is_a_HOLIDAY_and_is_kept(self):
        """2026-07-03 is the US Independence Day observance: the entire US market
        has no bar that Friday, and Thursday's close IS its most recent
        datapoint. A zero-tolerance rule would drop 921 of 1,479 names."""
        assert sessions_between(date(2026, 7, 2), date(2026, 7, 3)) == 1
        assert 1 <= current_portfolio.MAX_ENTRY_GAP_SESSIONS

    def test_three_missed_sessions_is_stale_and_is_dropped(self):
        assert sessions_between(date(2026, 7, 28), date(2026, 7, 31)) > \
            current_portfolio.MAX_ENTRY_GAP_SESSIONS

    def test_the_filter_runs_BEFORE_scoring(self):
        """Dropping stale names after selection would just leave the book short of
        `top_n_sectors × top_n_per_sector`; dropping them before means the next
        eligible name takes the slot."""
        src = inspect.getsource(current_portfolio.run_current_portfolio)
        assert src.index("stale_ids") < src.index("score_and_select(")

    def test_it_reports_what_it_excluded(self):
        """A rebalance that quietly selected from 900 of 1,479 names looks exactly
        like one that selected from all of them."""
        src = inspect.getsource(current_portfolio.run_current_portfolio)
        assert "excluded_stale_count" in src
        after = src.split("stale_ids.append", 1)[1]
        assert '"warning"' in after, "the exclusion must announce itself on the stream"
        assert "were excluded" in after
        # ...and NAME the companies with their last close. A bare count tells you
        # to refresh; the names tell you which vendor gaps you are living with,
        # which is where every one of these investigations actually ends.
        assert "detail.append" in after and "…+" in after, "name them, and state the overflow"

    def test_the_snapshot_states_the_bar_it_entered_at(self):
        """A holding's `entry_date` may legitimately differ from the anchor (its
        exchange was shut). The only way to tell that from a stale price is to
        publish the date the book was supposed to enter on."""
        from momentum.backtest.types import CurrentPortfolio

        cp = CurrentPortfolio(as_of_date="2026-08-03", latest_price_date=None, holdings=[],
                              entry_anchor_date="2026-07-31", excluded_stale_count=2)
        d = cp.to_dict()
        assert d["entry_anchor_date"] == "2026-07-31"
        assert d["excluded_stale_count"] == 2

    def test_the_entry_anchor_IS_the_deciding_bar(self):
        """The date the pipeline fetches prices TO must be the date the book is
        entered AT — otherwise the fetch satisfies a gate the engine then ignores."""
        src = inspect.getsource(current_portfolio.run_current_portfolio)
        assert "entry_anchor = deciding_bar(month_start)" in src
        assert "prior_anchor = anchor_ts" in src


class TestOneDefinition:
    """The gate must admit exactly what the engine can decide. Two rules that disagree
    means the pipeline rejects runs the engine was about to get right (which is what
    happened) or waves through ones it cannot."""

    def test_the_engine_walks_by_the_shared_rule(self):
        from momentum.backtest import current_portfolio

        src = inspect.getsource(current_portfolio.run_current_portfolio)
        assert "is_decidable(" in src

    def test_the_preflight_gate_uses_it_too_and_not_the_month(self):
        from routers.momentum.backtest_stream import stream

        src = inspect.getsource(stream)
        gate = src.split("# Pre-flight DB-staleness check.", 1)[1][:2600]
        assert "_is_decidable(" in gate
        assert "month_start" not in gate, "the calendar-month gate is the bug"

    def test_the_pipeline_fetches_to_the_same_bar(self):
        from ingest.phases import pipeline

        src = inspect.getsource(pipeline._deciding_bar_for)
        assert "deciding_bar(current_rebalance_date(" in src
        assert "max(" in src, "a mixed set of weekdays needs the LATEST bar"


class TestTheUniverseIsPricedBeforeItIsRanked:
    """⚠ The price-update op keeps the ~24 HELD names current; a rebalance ranks the
    other ~1,455. Stale candidates do not error — `signals.py` drops anything >30 days
    old — so the strategy silently selects from whatever subset was fresh."""

    def test_prices_run_BEFORE_the_momentum_phase(self):
        from ingest.phases import pipeline

        src = inspect.getsource(pipeline._run_rebalance_pipeline_sync)
        assert src.index("_run_prices_phase(") < src.index("_run_momentum_phase(")

    def test_a_uniformly_stale_universe_is_caught_by_an_ABSOLUTE_check(self):
        """`universe_freshness` is RELATIVE: it reports who is behind their peers. A
        universe that is uniformly a week old is unanimously `fresh` — nobody is behind
        anybody — so peer-lag alone would fetch nothing and rank on week-old prices."""
        from ingest.phases import pipeline

        src = inspect.getsource(pipeline._run_rebalance_pipeline_sync)
        assert "report.global_latest < required" in src
        assert "behind |= set(report.fresh)" in src

    def test_what_is_still_behind_after_the_fetch_warns_but_does_not_block(self):
        """A holiday or a vendor publication lag cannot be fixed by refusing to
        rebalance; leaving the strategies un-rebalanced is the worse failure."""
        from ingest.phases import pipeline

        src = inspect.getsource(pipeline._run_rebalance_pipeline_sync)
        after = src.split("_run_prices_phase(", 1)[1]
        assert "universe_freshness(" in after           # re-probe AFTER fetching
        assert "STILL behind" in after
        assert "_run_momentum_phase(" in after          # ...and it still computes
