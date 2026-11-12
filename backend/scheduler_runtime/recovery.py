"""Watchdog recovery policy and durable per-day retry budget."""
from __future__ import annotations
import logging
from scheduled_jobs import BY_ID

_log = logging.getLogger('scheduler')
_WATCHDOG_HEALS = frozenset({'overdue', 'interrupted', 'missed'})
_WATCHDOG_MAX_PER_DAY = 2
_watchdog_fired: dict[tuple[str, str], int] = {}

def _shutdown_interrupted(row: dict) -> bool:
    """Recover benchmark stages whose nested receipts captured executor shutdown."""
    return (
        row.get("status") == "error"
        and row.get("id") in {"benchmark_price_slice", "benchmark_fundamentals_fill"}
        and "cannot schedule new futures after interpreter shutdown"
        in f"{row.get('last_detail')} {row.get('last_summary')}"
    )


def _watchdog_budget_spent(job_id: str, today: str, *, starters) -> bool:
    """Whether this job has already used its re-run budget today, counting ACROSS processes.

     THE DATABASE IS THE AUTHORITY AND A FAILED READ SPENDS NOTHING. `watchdog_runs_today` returns
    None when it could not count, and that is treated as "cap reached": a watchdog which cannot
    verify its own budget must not spend it, or a Supabase blip becomes the trigger for re-firing
    the whole fleet.

     THE HIGHER OF THE TWO COUNTS WINS. The in-process tally can lead the database by a moment
    (a re-run started seconds ago may not have its row yet), and the durable one leads after a
    restart. Taking the max means neither blind spot opens the gate.
    """
    from job_runlog import ingest_runs_today, watchdog_runs_today  # noqa: PLC0415

    local = _watchdog_fired.get((job_id, today), 0)
    spec = BY_ID.get(job_id)
    if job_id in starters and spec is not None:
        #  A job with no `scheduled_job_run` ROW NEEDS A DIFFERENT MEASURING STICK, AND WITHOUT
        # One the cap would simply never bind. `watchdog_runs_today` counts rows tagged `watchdog`
        # in a table these two never write to — it would return 0 for ever, and a host in a restart
        # loop would re-fire the pipeline on every boot with a guard that could not see it had.
        # Their own `ingest_run` rows are the countable thing; see `ingest_runs_today` for why
        # counting ALL of them (not just the watchdog's) is the stricter and correct choice here.
        durable = ingest_runs_today(spec.evidence, today)
    else:
        durable = watchdog_runs_today(job_id, today)
    if durable is None:
        return True
    return max(local, durable) >= _WATCHDOG_MAX_PER_DAY


def _body_job_watchdog(ctx=None, *, health, bodies, starters, start_job, reporter) -> tuple[str, dict]:
    """Re-run the jobs the automatic-jobs page is already reporting as broken.

     THE PAGE KNEW AND NOTHING ACTED ON IT. `/schedule` has been computing `overdue` and
    `interrupted` per job for months — an honest three-way join between what is declared, what is
    registered and what actually ran — and the only consumer was a human reading it. A daily
    pipeline that dies mid-run therefore stays dead until somebody notices: measured in production
    2026-08-18 at 7.1 days for `daily_pipeline` and 18 for the month-end refresh, both with a
    perfectly healthy `next run` beside them, because the TICK was firing and the WORK was not
    finishing.

     IT RE-RUNS, IT DOES NOT DIAGNOSE. The two states it heals are the two where "run it again" is
    genuinely the fix. See `_WATCHDOG_HEALS` for why `missing`, `error` and `unknown` are not on
    that list — each of them would have the watchdog erase the evidence rather than the fault.

     IT CANNOT MAKE A JOB THAT LEGITIMATELY DOES NOTHING TODAY REPORT A FRESH SUCCESS. The
    month-end refresh acts only in the last days of the month; re-running it on the 12th is a
    no-op, so it will still read `interrupted` afterwards. That is a true statement about the job
    and not a watchdog failure — which is exactly why the cap exists, so it says it twice and stops
    rather than every hour for a fortnight.

     ONE AT A TIME, THROUGH `start_job_now` — the same body the tick runs, as a cancellable
    registry job with a toast, so an automatic re-run is visible in the same place a manual one is
    and can be stopped the same way.

     EXCEPT THE TWO PIPELINE JOBS, WHICH IT COULD NOT TOUCH AT ALL UNTIL 2026-09-01 AND WHICH ARE
    THE TWO IT WAS BUILT FOR. They have no `JOB_BODIES` entry — deliberately, because they own a
    richer Run-now with a live console tail in their own expanded row — and that same membership
    was gating what this could re-run, so `daily_pipeline` and `daily_price_slice` landed in
    `unrunnable` every sweep. Measured 2026-09-01: 21.0 and 31.8 days stale, both reported broken
    by the page this reads, both skipped by the code that reads it. `_WATCHDOG_STARTERS` separates
    the presentation question from the capability one; they fire through their own tick callable
    and are capped on their own `ingest_run` rows.
    """
    from datetime import datetime as _dt, timezone as _tz  # noqa: PLC0415

    step = reporter(ctx)
    health = health()
    rows = health["rows"]
    if health["history_error"]:
        #  No history means no verdict, and a watchdog without one must do nothing. Re-running
        # every job because the database was briefly unreachable is the opposite of self-healing.
        return (f"skipped — could not read the run history ({health['history_error']})",
                {"checked": 0, "restarted": 0})

    today = _dt.now(_tz.utc).date().isoformat()
    broken = [r for r in rows if r.get("status") in _WATCHDOG_HEALS or _shutdown_interrupted(r)]
    # AIRS has exactly one automatic browser pass. Recovery may refresh stored-data stages,
    # but must not create additional AIRS sessions on restart or from the watchdog.
    broken = [r for r in broken if r["id"] not in {"airs_vermogen_refresh", "dashboard_post_airs"}]
    step(0, len(broken) or 1, f"{len(broken)} job(s) to re-run of {len(rows)} checked")

    restarted, capped, unrunnable = [], [], []
    for i, row in enumerate(broken, 1):
        jid = row["id"]
        if jid not in bodies and jid not in starters:
            unrunnable.append(jid)
            continue
        if _watchdog_budget_spent(jid, today, starters=starters):
            capped.append(jid)
            continue
        key = (jid, today)
        _watchdog_fired[key] = _watchdog_fired.get(key, 0) + 1
        step(i, len(broken), f"re-running {jid} ({row.get('status')})")
        #  LOUD. uvicorn leaves the root logger at WARNING, so an INFO line here is invisible in
        # Railway — and "why did my pipeline run at 11:00" is a question the log has to answer.
        _log.warning("[watchdog] re-running %s — %s: %s", jid, row.get("status"),
                     row.get("why") or "")
        try:
            if jid in bodies:
                #  TAGGED `watchdog`, WHICH IS WHAT MAKES THE CAP COUNTABLE. Left as `manual`
                # these rows would be indistinguishable from somebody pressing Run now, so the
                # budget could not be read back out of the history — and a hand-run would spend
                # the automatic budget.
                start_job(jid, triggered_by="watchdog")
            else:
                #  The tick callable itself — see `_WATCHDOG_STARTERS`. It spawns its own daemon
                # thread and writes its own `ingest_run` rows, so there is nothing to await and
                # nothing to record here that the run does not record better.
                starters[jid]()
            restarted.append(jid)
        except Exception as e:  # noqa: BLE001 — one bad job must not stop the sweep
            _log.exception("[watchdog] could not start %s: %s: %s", jid, type(e).__name__, e)

    summary = {"checked": len(rows), "broken": len(broken), "restarted": len(restarted),
               "restarted_ids": restarted, "capped": capped, "unrunnable": unrunnable}
    if not broken:
        return f"all {len(rows)} job(s) healthy", summary
    msg = f"re-ran {len(restarted)}/{len(broken)}"
    if capped:
        msg += f" — {len(capped)} already retried {_WATCHDOG_MAX_PER_DAY}× today: {', '.join(capped)}"
    if unrunnable:
        msg += f" — {len(unrunnable)} have no body: {', '.join(unrunnable)}"
    return msg, summary


