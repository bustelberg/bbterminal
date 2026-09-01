"""WHICH TICKS SHOULD HAVE HAPPENED AND DID NOT.

⚠⚠ THE BUG THIS GUARDS AGAINST IS AN ABSENCE, WHICH IS WHY IT SURVIVED SO LONG IN PRODUCTION.
Measured 2026-09-01: `daily_pipeline` 20.9 DAYS stale, `job_watchdog` 44.7h, each beside a perfectly
healthy "Next run" a few hours out, and nothing anywhere — no row, no log line, no APScheduler event
— saying a single tick had been lost. The scheduler's default jobstore is in-memory, so a boot
recomputes every `next_run_time` from now and a fire time that passed while the process was down
never existed to be missed. The only way back to the truth is to ask the TRIGGER what it would have
done, which is what this module does and what these tests drive.

Unit-only: a real `CronTrigger` (pure calendar arithmetic, no I/O), an explicit `now`, and a list of
timestamps standing in for the run history.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from apscheduler.triggers.cron import CronTrigger

from job_misses import (
    DEFAULT_LOOKBACK_DAYS, describe, fire_times, missed_windows, should_scan,
)

NOON = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)


def _at(day: int, hour: int = 5, minute: int = 0) -> datetime:
    return datetime(2026, 8, day, hour, minute, tzinfo=timezone.utc)


def daily_at_5() -> CronTrigger:
    return CronTrigger(hour=5, minute=0, timezone="UTC")


class TestTheTriggerCanBeAskedAboutThePast:
    """⚠ APScheduler 3.x HAS NO PUBLIC "previous fire time" — its one primitive is
    `get_next_fire_time(previous, now)`, so the past is reachable only by starting behind it and
    walking forward. These pin that the walk terminates and lands on the right instants."""

    def test_it_reconstructs_a_weeks_worth_of_daily_fires(self):
        got = fire_times(daily_at_5(), NOON - timedelta(days=7), NOON)
        assert [d.isoformat() for d in got] == [
            f"2026-08-{d:02d}T05:00:00+00:00" for d in range(26, 32)
        ] + ["2026-09-01T05:00:00+00:00"]

    def test_the_window_is_closed_at_BOTH_ends(self):
        """⚠ MEASURED, NOT ASSUMED. `CronTrigger.get_next_fire_time` returns a fire that lands
        exactly on `now`, so a fire on either boundary is included — a single-instant window
        containing a fire yields that fire. Harmless here (the scan's `start` is `now - 7d`, an
        arbitrary instant), but it is the kind of off-by-one that would otherwise be discovered by
        a duplicate row rather than by a test."""
        five = datetime(2026, 8, 30, 5, 0, tzinfo=timezone.utc)
        assert fire_times(daily_at_5(), five, five) == [five]
        assert fire_times(daily_at_5(), five - timedelta(seconds=1), five) == [five]

    def test_an_empty_window_is_empty_rather_than_an_error(self):
        assert fire_times(daily_at_5(), NOON, NOON - timedelta(days=1)) == []

    def test_the_walk_is_fenced(self):
        """⚠ THE FENCE IS NOT DECORATION. This runs inside a FastAPI startup hook; a trigger that
        ever returned a non-advancing time would spin there for ever and the deploy would hang."""
        got = fire_times(daily_at_5(), NOON - timedelta(days=365), NOON, limit=3)
        assert len(got) == 3


class TestAWindowWithNoRowInItIsAMiss:
    def test_nothing_ran_all_week(self):
        got = missed_windows(daily_at_5(), [], now=NOON, grace_seconds=0)
        assert len(got) == 7
        assert got[0] == _at(26)

    def test_a_run_closes_its_own_window_and_only_that_one(self):
        got = missed_windows(daily_at_5(), [_at(31)], now=NOON, grace_seconds=0)
        assert _at(31) not in got
        assert len(got) == 6

    def test_a_late_run_still_closes_the_window_it_belongs_to(self):
        """⚠ THE WATCHDOG RE-FIRING AN 05:00 TICK AT 11:00 IS THE WORK HAPPENING. A miss recorded
        beside it would be a second, contradictory answer about one window."""
        got = missed_windows(daily_at_5(), [_at(31, 11)], now=NOON, grace_seconds=0)
        assert _at(31) not in got and len(got) == 6

    def test_a_run_never_closes_an_EARLIER_window(self):
        """⚠⚠ THE WINDOW ENDS AT THE NEXT FIRE, NEVER AT `now` — and this is the test that pins the
        difference. Bounded by `now`, one recent success would retroactively account for every tick
        behind it, and a 20-day gap would vanish the moment the watchdog worked once. That is the
        production failure erasing its own evidence."""
        got = missed_windows(daily_at_5(), [datetime(2026, 9, 1, 5, 0, tzinfo=timezone.utc)],
                             now=NOON, grace_seconds=0)
        assert len(got) == 6
        assert _at(26) in got and _at(31) in got

    def test_a_row_of_any_status_counts_because_the_question_is_did_it_FIRE(self):
        """⚠ A ROW STUCK IN `running` PROVES THE TICK FIRED. Whether the work finished is a
        different verdict, and `_scheduled_jobs_status` already renders it — recording a miss on top
        would report one tick as both never-started and died-mid-run."""
        got = missed_windows(daily_at_5(), [_at(30), _at(31)], now=NOON, grace_seconds=0)
        assert _at(30) not in got and _at(31) not in got

    def test_a_previous_miss_closes_its_own_window_so_repeated_scans_are_idempotent(self):
        """⚠⚠ THE WHOLE REASON `record_missed` STAMPS `started_at` AT THE FIRE TIME. A host in a
        restart loop reaches the boot scan many times a day; without this every one of them would
        rewrite the same gap."""
        first = missed_windows(daily_at_5(), [], now=NOON, grace_seconds=0)
        # The rows a first pass would have written are now history.
        second = missed_windows(daily_at_5(), list(first), now=NOON, grace_seconds=0)
        assert first and second == []


class TestTheNewestTickIsNotAMissYet:
    def test_a_fire_inside_its_grace_period_is_left_alone(self):
        """⚠ A JOB THAT FIRED FIVE MINUTES AGO IS STILL OPENING ITS ROW. Counting it would make
        every boot that lands during a tick manufacture a false miss.

        ⚠ `lookback_days=1` SO THE WINDOW HOLDS EXACTLY ONE FIRE. Over a week this same trigger has
        six older fires that ARE genuine misses, and they would mask the thing being tested."""
        just_now = NOON - timedelta(minutes=5)
        trigger = CronTrigger(hour=just_now.hour, minute=just_now.minute, timezone="UTC")
        assert missed_windows(trigger, [], now=NOON, lookback_days=1, grace_seconds=3600) == []

    def test_but_it_counts_once_the_grace_has_run_out(self):
        old = NOON - timedelta(hours=3)
        trigger = CronTrigger(hour=old.hour, minute=old.minute, timezone="UTC")
        assert missed_windows(trigger, [], now=NOON, lookback_days=1,
                              grace_seconds=3600) == [old]


class TestTheScanRefusesJobsItWouldOnlyAddNoiseFor:
    """⚠ SCOPE IS PART OF THE MEASUREMENT. A 20-second interval worker is DESIGNED to be absent
    whenever the process is; reconstructing its downtime as 30,000 missed ticks would bury the four
    daily ones that matter and say nothing "the process was down" does not already say."""

    def test_an_interval_job_is_skipped(self):
        from scheduled_jobs import JobSpec  # noqa: PLC0415

        assert not should_scan(JobSpec(id="q", label="q", fills="", cadence="",
                                       interval_seconds=20))

    def test_an_opt_in_job_this_deployment_does_not_run_is_skipped(self):
        """⚠ `optional_env` MEANS A GAP IS THE CONFIGURATION WORKING, not a fault."""
        from scheduled_jobs import JobSpec  # noqa: PLC0415

        assert not should_scan(JobSpec(id="x", label="x", fills="", cadence="",
                                       trigger={"hour": 5}, optional_env="SOME_FLAG"))

    def test_an_ordinary_cron_job_is_scanned(self):
        from scheduled_jobs import JobSpec  # noqa: PLC0415

        assert should_scan(JobSpec(id="d", label="d", fills="", cadence="",
                                   trigger={"hour": 5, "minute": 0}))


class TestTheRowSaysWhichOfTheTwoCausesItWas:
    """⚠⚠ THE SENTENCE IS THE DELIVERABLE. `overdue` was already on the page; what was missing was
    any statement of WHY, and the two causes have different fixes — a busy process is code, an
    absent one is the host."""

    def test_it_names_the_fire_time_and_the_boot_that_came_after_it(self):
        got = describe(_at(26), datetime(2026, 9, 1, 12, 30, tzinfo=timezone.utc))
        assert "2026-08-26 05:00 UTC" in got
        assert "2026-09-01 12:30 UTC" in got
        assert "was not alive" in got

    def test_without_a_boot_time_it_claims_only_what_it_knows(self):
        """⚠ IT STATES A FACT, NEVER AN INFERENCE. With no boot time there is no evidence the
        process was absent — only that no run was recorded — and the sentence must not say more."""
        got = describe(_at(26), None)
        assert "2026-08-26 05:00 UTC" in got
        assert "was not alive" not in got


class TestTheLookbackIsBounded:
    def test_a_fresh_history_does_not_reconstruct_the_whole_year(self):
        """⚠ AN UNBOUNDED SCAN WOULD ASSERT THAT TICKS WERE MISSED BEFORE ANYTHING EXISTED TO MISS
        THEM — thousands of rows, on the first boot after a job is declared."""
        got = missed_windows(daily_at_5(), [], now=NOON, grace_seconds=0)
        assert len(got) == DEFAULT_LOOKBACK_DAYS

    def test_the_per_job_cap_holds_even_over_a_long_window(self):
        got = missed_windows(daily_at_5(), [], now=NOON, lookback_days=90,
                             grace_seconds=0, limit=5)
        assert len(got) == 5
