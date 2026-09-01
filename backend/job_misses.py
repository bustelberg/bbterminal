"""WHICH TICKS SHOULD HAVE HAPPENED AND DID NOT — the arithmetic behind "why is this overdue?".

⚠⚠ THE FAILURE THIS EXISTS FOR LEAVES NO TRACE ANYWHERE, WHICH IS WHY IT WAS UNDIAGNOSABLE. Measured
in production 2026-09-01: `daily_pipeline` last ran **20.9 days ago**, `job_watchdog` 44.7h,
`crm_relaties_refresh` 46.7h — every one of them beside a perfectly healthy "Next run" a few hours
out. `/schedule` reported `overdue` and could say nothing about the cause, because the only evidence
it has is `scheduled_job_run`, and a tick that never fired writes no row. An absence has no message.

⚠⚠ AND THE TWO WAYS A TICK IS LOST NEED DIFFERENT EVIDENCE, BECAUSE THEY HAVE DIFFERENT FIXES:

  1. THE PROCESS WAS ALIVE AND THE FIRE WAS DROPPED. APScheduler emits `EVENT_JOB_MISSED` when a
     fire time passes by more than `misfire_grace_time` — a blocked worker, a saturated thread pool.
     Nothing in this app listened to that event, so the drop was silent. The fix is code.

  2. THE PROCESS WAS NOT RUNNING AT ALL. This is the one this module computes, and it is not even a
     misfire: `BackgroundScheduler` here uses the DEFAULT IN-MEMORY JOBSTORE, so every boot
     recomputes `next_run_time` from *now*. A fire time that passed while the process was down never
     existed as far as APScheduler is concerned — no event, no misfire, no log line, and a next-run
     that looks perfect. That is exactly how "20.9 days ago" sits next to "Next run tomorrow, 07:00".
     The fix is usually NOT code (a sleeping or redeploying host), which is precisely why the
     evidence has to be durable enough to point at the host.

⚠ SO THE GAP IS RECONSTRUCTED FROM THE TRIGGER, NOT FROM THE SCHEDULER. The trigger is a pure
function of the calendar — it can be asked what it *would* have done over any past window, whether
or not anybody was listening — and the run history says what actually happened. The difference is
the answer, and it is computable at boot for a window that reaches back before the boot.

⚠ ONE ROW PER FIRE WINDOW, WHICH IS WHAT MAKES REPEATED PASSES IDEMPOTENT. Window `i` is
`[fire_i, fire_i+1)`; a window is missed when NO run row started inside it. A missed row is written
AT its own fire time, so it lands in its own window and the next boot finds the window covered.
Without that, a host restarting twenty times a day would write twenty copies of every gap.

⚠ A LATE RUN COUNTS AS A RUN. The watchdog re-firing an 05:00 tick at 11:00, or somebody pressing
Run now at 14:00, lands in the 05:00 window and closes it — the work happened, and a "missed" row
beside it would be a second answer about the same window.

⚠ THE NEWEST FIRE TIME IS DELIBERATELY EXCLUDED while it is still within its grace period. A job
that fired ninety seconds ago and is still opening its row has not missed anything, and saying so
would make every boot during a tick produce a false miss.

Pure — no clock, no database, no scheduler. `now` and the recorded runs are arguments, which is what
lets `tests/test_job_misses.py` drive a whole week of a real `CronTrigger` in milliseconds.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

#: How far back a boot-time gap scan looks.
#:
#: ⚠ BOUNDED, AND NOT BY THE JOB'S OWN CADENCE. A fresh database, or a job declared last week, would
#: otherwise have its entire pre-history reconstructed as misses — thousands of rows asserting that
#: ticks were missed before anything existed to miss them. Seven days is long enough to cover a
#: weekend outage plus the Monday nobody looked, and short enough that the worst case is bounded.
DEFAULT_LOOKBACK_DAYS = 7

#: The most rows one pass will write for one job. ⚠ A 20-second interval trigger would otherwise
#: reconstruct 30,000 misses from a week's downtime; the cap turns that into a readable statement
#: plus a count. Interval jobs are excluded outright (see `should_scan`), so this is the second
#: fence, not the first.
MAX_MISSES_PER_JOB = 50


def fire_times(trigger, start: datetime, end: datetime, *, limit: int = 5000) -> list[datetime]:
    """Every time `trigger` would have fired in `(start, end]`, oldest first.

    ⚠ WALKED FORWARD FROM `start`, because APScheduler 3.x has no public "previous fire time". Its
    one navigation primitive is `get_next_fire_time(previous, now)`, so the past is reachable only
    by starting behind it and stepping. For a daily cron over seven days that is seven steps.

    ⚠ `limit` IS A LOOP FENCE, NOT A FEATURE. A trigger that returns a non-advancing time would spin
    here for ever inside a startup hook; the guard is what makes calling this on an arbitrary
    trigger safe.
    """
    out: list[datetime] = []
    prev: datetime | None = None
    cursor = start
    for _ in range(limit):
        nxt = trigger.get_next_fire_time(prev, cursor)
        if nxt is None or nxt > end:
            return out
        out.append(nxt)
        prev = nxt
        # ⚠ THE CURSOR MOVES PAST THE FIRE WE JUST TOOK, or a trigger that reports the same instant
        # for `(prev, cursor)` returns it for ever. One microsecond is enough and cannot skip a
        # real fire — no cron expression resolves finer than a second.
        cursor = nxt + timedelta(microseconds=1)
    return out


def should_scan(spec) -> bool:
    """Whether a boot-time gap scan is meaningful for this job.

    ⚠ INTERVAL JOBS ARE OUT. The queue worker fires every 20 seconds and is *designed* to be
    absent whenever the process is: reconstructing its downtime as thousands of missed ticks would
    bury the four daily ones that matter under noise, and say nothing a single "the process was
    down" does not already say.

    ⚠ AND SO IS AN OPT-IN JOB THAT IS NOT OPTED IN. `optional_env` means "this deployment may
    legitimately not run this at all"; a gap there is the configuration working.
    """
    return spec.interval_seconds is None and not spec.optional_env


def missed_windows(
    trigger,
    recorded: list[datetime],
    *,
    now: datetime,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    grace_seconds: int = 3600,
    limit: int = MAX_MISSES_PER_JOB,
) -> list[datetime]:
    """The fire times in the lookback window that no run row accounts for, oldest first.

    `recorded` is every `started_at` this job has, in any status — a row stuck in `running` still
    proves the tick FIRED, which is the question here; whether the work finished is a different
    verdict that `_scheduled_jobs_status` already renders.

    ⚠ ANY STATUS, INCLUDING A PREVIOUS `missed`. That is what closes a window against the next pass;
    treating a missed row as "still missing" would rewrite the same gap on every boot.
    """
    start = now - timedelta(days=lookback_days)
    fires = fire_times(trigger, start, now)
    if not fires:
        return []

    # ⚠ THE NEWEST FIRE IS STILL IN FLIGHT UNTIL ITS GRACE RUNS OUT — see the module note. Dropped
    # here rather than filtered later, so the window arithmetic below never has to special-case it.
    cutoff = now - timedelta(seconds=grace_seconds)
    fires = [f for f in fires if f <= cutoff]
    if not fires:
        return []

    stamps = sorted(recorded)
    out: list[datetime] = []
    for i, fire in enumerate(fires):
        # ⚠ THE WINDOW ENDS AT THE NEXT FIRE, NEVER AT `now`. Bounded by `now` instead, a single
        # recent run would retroactively account for every missed tick behind it — the 20-day gap
        # would vanish the moment the watchdog succeeded once.
        end = fires[i + 1] if i + 1 < len(fires) else now
        if not any(fire <= s < end for s in stamps):
            out.append(fire)
        if len(out) >= limit:
            break
    return out


def describe(fire: datetime, booted_at: datetime | None) -> str:
    """The sentence that goes in the row's `detail` — one line, and it names the cause.

    ⚠ IT SAYS WHAT WAS TRUE, NOT WHAT TO DO. "The process was not running" is a fact this code can
    establish (it is reconstructing the gap from a boot that happened afterwards); "redeploy less"
    or "the host is asleep" is an inference for whoever reads it, with the dates in front of them.
    """
    when = fire.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if booted_at is None:
        return f"no run recorded for the {when} tick"
    up = booted_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (f"no run recorded for the {when} tick — this process did not start until {up}, "
            f"so the scheduler was not alive to fire it")
