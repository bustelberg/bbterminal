"""RECORDING THAT AN AUTOMATIC JOB RAN — one row in `scheduled_job_run`, per fire.

⚠⚠ A CONTEXT MANAGER, NOT A "log it at the end" CALL, AND THE DIFFERENCE IS THE WHOLE POINT. The
    interesting failures are the ones where the job does NOT reach its end: an exception, a Railway
    redeploy mid-scrape, an OOM kill. A call at the bottom of the function records only the runs
    that were never in doubt. Writing the row on ENTRY means a crashed job leaves a row stuck in
    `running`, which is exactly the evidence you want and precisely what the logger could never
    give you.

⚠⚠ IT MUST NEVER BE THE REASON A JOB FAILS. This is bookkeeping wrapped around real work — an FX
    sync that fetched every rate and then hit a dead Supabase on the way out has SUCCEEDED, and
    turning that into a failure would be the monitoring breaking the thing it monitors. Every write
    here is best-effort and swallows its own errors (loudly, to the log). The cost is that a
    database outage shows up as a missing row, i.e. as `unknown` on the overview — honest, and the
    right way round.

⚠ `skipped` IS A SUCCESS. Several of these jobs are DESIGNED to no-op — the month-end refresh wakes
    daily and acts twice a month, the asset-price refresh stands down while the ingest queue is
    live. `ok` would hide the difference between "did the work" and "correctly did nothing";
    `error` would cry wolf on healthy behaviour. It is its own status and the overview treats it as
    fine.

⚠⚠ `missed` IS THE OPPOSITE, AND IT IS THE ONE ROW NOBODY WAS WRITING. Every status above describes
    a job that RAN. The failure measured in production on 2026-09-01 — `daily_pipeline` 20.9 days
    stale, `job_watchdog` 44.7h, each beside a healthy next-run — is a job that never started, and
    a context manager wrapped around work that never happened can say nothing at all. So `missed` is
    written by an OBSERVER instead of by the job (`record_missed`): the APScheduler misfire listener
    for a fire that was dropped while the process was alive, and the boot-time gap scan for a fire
    time that passed while it was not. Without it "overdue" is a verdict with no evidence under it,
    which is precisely what made this undiagnosable in production.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

_log = logging.getLogger(__name__)


class RunRecord:
    """The handle a job gets inside the `with`. Everything on it is optional."""

    def __init__(self, job_id: str, row_id: int | None) -> None:
        self.job_id = job_id
        self.row_id = row_id
        self.status: str = "ok"
        self.detail: str | None = None
        self.summary: dict[str, Any] | None = None
        self.ingest_run_id: int | None = None

    def skip(self, why: str) -> None:
        """The job woke, decided there was nothing to do, and that is a healthy outcome."""
        self.status = "skipped"
        self.detail = why

    def done(self, detail: str | None = None, **summary: Any) -> None:
        """What it did. Keyword args become the `summary` JSON — counts in the job's own terms."""
        if detail is not None:
            self.detail = detail
        if summary:
            self.summary = {**(self.summary or {}), **summary}


def _supabase():
    """Imported lazily — this module is pulled in by `scheduler`, and `deps` at import time would
    make a bookkeeping helper part of the startup import graph."""
    from deps import supabase  # noqa: PLC0415

    return supabase


@contextmanager
def record_run(job_id: str, triggered_by: str = "auto") -> Iterator[RunRecord]:
    """Record one run of `job_id`, whatever happens inside.

    ⚠ THE EXCEPTION IS RE-RAISED. This records; it does not handle. Every caller already has its own
    `try/except` that keeps a failure out of the APScheduler thread, and swallowing it here would
    take that decision away from the code that knows what a failure means.
    """
    rec = RunRecord(job_id, None)
    try:
        rows = (_supabase().table("scheduled_job_run")
                .insert({"job_id": job_id, "triggered_by": triggered_by})
                .execute().data or [])
        rec.row_id = rows[0]["id"] if rows else None
    except Exception as e:  # noqa: BLE001 — see the module note: never break the job
        _log.warning("[runlog] could not open a run row for %s: %s: %s",
                     job_id, type(e).__name__, e)

    try:
        yield rec
    except Exception as e:
        rec.status = "error"
        # ⚠ TYPE AND MESSAGE, TRUNCATED. The traceback is already in the log via the caller's
        # `_log.exception`; what the overview needs is one line it can render in a table cell.
        rec.detail = f"{type(e).__name__}: {str(e)[:400]}"
        _close(rec)
        raise
    _close(rec)


def _close(rec: RunRecord) -> None:
    if rec.row_id is None:
        # The row was never opened (see above). Say what happened anyway, so the outcome is at
        # least in the log rather than lost with the failed insert.
        _log.info("[runlog] %s finished %s (no row: the insert failed) — %s",
                  rec.job_id, rec.status, rec.detail or "")
        return
    patch: dict[str, Any] = {
        "status": rec.status,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "detail": rec.detail,
        "summary": rec.summary,
        "ingest_run_id": rec.ingest_run_id,
    }
    try:
        _supabase().table("scheduled_job_run").update(patch).eq("id", rec.row_id).execute()
    except Exception as e:  # noqa: BLE001
        # ⚠ THE ROW IS NOW STUCK IN `running`, AND THAT IS THE HONEST OUTCOME — we genuinely do not
        # know how it ended from the database's point of view. The overview reads a stale `running`
        # as "the process died mid-job", which is one of the two real explanations.
        _log.warning("[runlog] could not close run %s for %s: %s: %s",
                     rec.row_id, rec.job_id, type(e).__name__, e)


def record_missed(job_id: str, fire_time: "datetime", detail: str,
                  **summary: Any) -> bool:
    """Record a tick that never ran — one CLOSED row, stamped at the fire time it belongs to.

    ⚠⚠ `started_at` IS THE FIRE TIME, NOT `now()`, AND THAT IS LOAD-BEARING IN TWO DIRECTIONS.
    It is what makes a boot-time gap scan IDEMPOTENT: the row lands inside the very window it
    describes, so the next boot — and on a restarting host there may be twenty a day — finds that
    window accounted for and writes nothing. And it is what makes the overview's age arithmetic
    true: `last_run_at` for a missed tick is when the tick was DUE, so "44.7h ago" keeps meaning
    the same thing whether the newest row is a run or a miss.

    ⚠ CLOSED ON ARRIVAL (`finished_at` set), because there is nothing in flight to close later.
    A missed tick left `running` would be read by `_scheduled_jobs_status` as a process that died
    mid-run — a different fault with a different fix.

    ⚠ IT NEVER RAISES, for the reason in the module note: this is bookkeeping, and bookkeeping that
    can break a boot is worse than bookkeeping that is occasionally absent. Returns whether the row
    was actually written, so a caller counting misses reports what it managed to record rather than
    what it hoped to.
    """
    row = {
        "job_id": job_id,
        "started_at": fire_time.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "status": "missed",
        "triggered_by": "scheduler",
        "detail": detail,
        "summary": summary or None,
    }
    try:
        _supabase().table("scheduled_job_run").insert(row).execute()
        return True
    except Exception as e:  # noqa: BLE001 — see the module note: never break the caller
        _log.warning("[runlog] could not record a missed %s tick at %s: %s: %s",
                     job_id, fire_time, type(e).__name__, e)
        return False


def started_at_stamps(job_id: str, since: "datetime") -> list["datetime"] | None:
    """Every `started_at` this job has since `since`, ANY status — or None if the read failed.

    ⚠⚠ NONE AND [] ARE DIFFERENT ANSWERS AND THE CALLER MUST NOT MERGE THEM. `[]` means the job
    provably did not run in the window, which is what a gap scan acts on; `None` means we could not
    find out. Treated as `[]`, a Supabase blip at boot would reconstruct a week of phantom misses
    for every job at once — the monitoring inventing the outage it exists to report.

    ⚠ ANY STATUS, INCLUDING `missed` ITSELF. The question is "did a row land in this window", not
    "did the work succeed" — a previous miss closes its own window, and a row stuck in `running`
    still proves the tick fired.
    """
    try:
        rows = (_supabase().table("scheduled_job_run")
                .select("started_at")
                .eq("job_id", job_id)
                .gte("started_at", since.isoformat())
                # ⚠ PAGED-BY-BOUND, NOT UNPAGED. PostgREST truncates silently at 1,000 rows and the
                # window is small by construction (a week of a daily job is ~7 rows), but the sort
                # is what makes a truncation harmless if it ever is not: newest first means a cut
                # loses the OLDEST rows in the window, which can only ever over-report a miss at the
                # far edge, never hide a recent run.
                .order("started_at", desc=True).limit(1000)
                .execute().data or [])
    except Exception as e:  # noqa: BLE001
        _log.warning("[runlog] could not read run stamps for %s: %s: %s",
                     job_id, type(e).__name__, e)
        return None
    out: list[datetime] = []
    for r in rows:
        try:
            out.append(_parse_stamp(str(r["started_at"])))
        except Exception:  # noqa: BLE001, S112 — one unparseable stamp is not a failed read
            continue
    return out


def ingest_run_stamps(job_names: tuple[str, ...], since: "datetime") -> list["datetime"] | None:
    """Every `ingest_run.started_at` under these `job_name`s since `since` — or None if it failed.

    ⚠⚠ TWO JOBS PROVE THEMSELVES HERE AND NOWHERE ELSE, AND THEY ARE THE TWO THAT MATTER MOST.
    `daily_pipeline` and `month_end_price_refresh` are declared `records=False` on purpose: they
    already write a detailed `ingest_run` row per phase, and a second `scheduled_job_run` row for
    the same event would be two records free to disagree. So their `scheduled_job_run` history is
    EMPTY BY DESIGN — and a gap scan that consulted only that table would conclude every tick had
    been missed on a pipeline that ran perfectly every night. The most alarming possible output,
    for the two jobs whose alarm is least ignorable.

    ⚠ SAME KEY SPACE AS `job_health._runs`, which normalises both tables onto one name. This is the
    other half of that join, asked over a window instead of for the newest row.
    """
    if not job_names:
        return []
    out: list[datetime] = []
    for name in job_names:
        try:
            rows = (_supabase().table("ingest_run")
                    .select("started_at")
                    .eq("job_name", name)
                    .gte("started_at", since.isoformat())
                    .order("started_at", desc=True).limit(1000)
                    .execute().data or [])
        except Exception as e:  # noqa: BLE001
            _log.warning("[runlog] could not read ingest_run stamps for %s: %s: %s",
                         name, type(e).__name__, e)
            # ⚠ ONE UNREADABLE NAME POISONS THE WHOLE ANSWER, deliberately. A partial history is
            # indistinguishable from a real gap, and the caller must not act on it.
            return None
        for r in rows:
            try:
                out.append(_parse_stamp(str(r["started_at"])))
            except Exception:  # noqa: BLE001, S112
                continue
    return out


def watchdog_runs_today(job_id: str, today_utc: str) -> int | None:
    """How many times the watchdog has already re-fired this job today — from the DATABASE.

    ⚠⚠ THE CAP USED TO LIVE IN A PROCESS-LOCAL DICT, WHICH IS THE ONE PLACE IT CANNOT LIVE. The
    whole point of the cap is to stop a structurally broken job (no GuruFocus quota, rotated AIRS
    credentials) being re-fired for ever — and the failure that makes the watchdog fire is very
    often a host that keeps RESTARTING, which resets a process-local counter to zero every time. So
    the guard evaporated in exactly the scenario it was written for, and now that a boot can run the
    sweep too, that is no longer a latent flaw.

    ⚠ NONE ON A FAILED READ, and the caller treats that as "cap reached". A watchdog that cannot
    verify its own budget must not spend it.
    """
    try:
        rows = (_supabase().table("scheduled_job_run")
                .select("id")
                .eq("job_id", job_id).eq("triggered_by", "watchdog")
                .gte("started_at", f"{today_utc}T00:00:00+00:00")
                .limit(100).execute().data or [])
    except Exception as e:  # noqa: BLE001
        _log.warning("[runlog] could not count watchdog re-runs for %s: %s: %s",
                     job_id, type(e).__name__, e)
        return None
    return len(rows)


def ingest_runs_today(job_names: tuple[str, ...], today_utc: str) -> int | None:
    """How many `ingest_run` rows these `job_name`s have started today — the pipeline jobs' budget.

    ⚠⚠ A DIFFERENT COUNT FROM `watchdog_runs_today`, AND DELIBERATELY A BROADER ONE. The body-having
    jobs are capped on WATCHDOG-initiated runs only, because a watchdog re-run is the only thing
    that writes a `scheduled_job_run` row under that tag. These two write no such row at all — they
    are `records=False` and prove themselves through `ingest_run` — so the only countable thing is
    the job's own runs, whoever started them.

    ⚠ WHICH MAKES THE BUDGET STRICTER HERE, AND THAT IS THE RIGHT DIRECTION FOR THE EXPENSIVE ONES.
    A successful 05:00 tick spends one, so at `_WATCHDOG_MAX_PER_DAY = 2` a healthy day leaves one
    re-run and a completely dead day allows two — however many times the host restarts. It also
    stops the watchdog piling on top of a run somebody started by hand ten minutes ago, which the
    narrower count would not.

    ⚠ NONE ON A FAILED READ, and the caller treats that as "cap reached" — a watchdog that cannot
    verify its budget must not spend it, least of all on the job that re-prices every held company.
    """
    if not job_names:
        return 0
    total = 0
    for name in job_names:
        try:
            rows = (_supabase().table("ingest_run")
                    .select("run_id")
                    .eq("job_name", name)
                    .gte("started_at", f"{today_utc}T00:00:00+00:00")
                    .limit(100).execute().data or [])
        except Exception as e:  # noqa: BLE001
            _log.warning("[runlog] could not count today's %s runs: %s: %s",
                         name, type(e).__name__, e)
            return None
        total += len(rows)
    return total


def _parse_stamp(raw: str) -> "datetime":
    """Postgres `timestamptz` → an aware UTC datetime.

    ⚠ `Z` IS NOT ISO 8601 TO `fromisoformat` BEFORE PYTHON 3.11, and a naive stamp is not comparable
    to an aware one — it raises mid-comparison rather than sorting wrongly, which is the good
    failure but still a failure. Both normalised here, once.
    """
    txt = raw.replace("Z", "+00:00")
    dt = datetime.fromisoformat(txt)
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
