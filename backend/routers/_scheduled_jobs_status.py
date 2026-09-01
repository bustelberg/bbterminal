"""DECLARED vs REGISTERED vs ACTUALLY RAN — the three-way join behind the automatic-jobs page.

⚠⚠ THE POINT IS THE DISAGREEMENT, NOT THE LIST. Any one of the three sources on its own reads as
    healthy in the exact situation it should be screaming:

      declared alone     — says what SHOULD run and cannot know whether anything is running;
      registered alone   — `list_scheduled_jobs()` returns [] under DISABLE_SCHEDULER, before
                           startup, and for a job whose `add_job` threw. Empty and healthy-idle
                           render identically;
      run history alone  — a job removed from the code stops appearing and its last row just gets
                           older, which looks like a stale job rather than a deleted one.

    So a row here always carries all three, and `status` is a statement about their agreement.

⚠⚠ `unknown` IS A FIRST-CLASS OUTCOME AND MUST NEVER COLLAPSE INTO `ok` OR `error`. Six of the eight
    jobs currently leave no durable trace — a log line in Railway that scrolls away — so for those
    "did it run?" has no answer at all. Reporting them green would be a fabrication and reporting
    them red would cry wolf on jobs that are probably fine; either one teaches the reader to ignore
    the page, which is the only way a monitoring surface actually fails. They say `unknown` and name
    the reason until `record_run` lands.

⚠ PURE. Every input is passed in — the specs, the scheduler snapshot, the run rows, and NOW. No
    imports from `deps`, no clock read: an overdue check whose "now" comes from inside is a function
    whose tests have to be run at the right time of day.
"""
from __future__ import annotations

from datetime import datetime, timezone

# ⚠ A PURE SIBLING, AND THE ONLY IMPORT THIS MODULE GAINS. `should_scan` is two boolean
# field reads with no clock and no database — see `names` in `build_rows`.
import job_misses
from scheduled_jobs import ORPHAN_MARKER, JobSpec

#: Ordered worst-first, which is the order the page sorts by. `unknown` outranks `ok` because a job
#: we cannot see is a question to answer, not a state to be content with.
SEVERITY: dict[str, int] = {
    "missing": 0,
    "error": 1,
    "overdue": 2,
    # ⚠ ABOVE `unknown` BECAUSE IT IS ACTIONABLE. "The process died mid-run" tells you exactly what
    # to do (run it again); "we cannot see whether it ran" does not. Below `error` because nothing
    # is broken — a deploy or a `--reload` landed on it.
    "interrupted": 3,
    "unknown": 4,
    "off": 5,
    "running": 6,
    "ok": 7,
}


def _parse(ts: str | None) -> datetime | None:
    """An ISO timestamp from PostgREST or APScheduler, or None.

    ⚠ NAIVE INPUT IS ASSUMED UTC. Postgres hands back an offset; a naive value can only have come
    from somewhere that dropped it, and treating it as local would move a timestamp by up to two
    hours — enough to turn a healthy job overdue and back again twice a year.
    """
    if not ts:
        return None
    try:
        d = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def _latest_run(rows: list[dict], job_names: tuple[str, ...]) -> dict | None:
    """The newest `ingest_run` row belonging to this job, or None.

    ⚠ NEWEST BY `started_at`, ACROSS ALL of the job's names. `daily_pipeline` fires two runs in
    order (price_update then rebalance) and either may be the last thing that happened; picking one
    name would report the job as last-run at whichever of the two we happened to name.
    """
    mine = [r for r in rows if r.get("job_name") in job_names and r.get("started_at")]
    if not mine:
        return None
    return max(mine, key=lambda r: str(r["started_at"]))


def build_rows(
    specs: list[JobSpec],
    registered: list[dict],
    runs: list[dict],
    now: datetime,
    *,
    scheduler_running: bool,
    runnable: set[str] | None = None,
) -> list[dict]:
    """One row per declared job.

    `registered` is `scheduler.list_scheduled_jobs()`; `runs` is recent `ingest_run` rows
    (`job_name`, `started_at`, `finished_at`, `status`, `error_summary`).

    ⚠ `scheduler_running` IS PASSED SEPARATELY AND IS NOT `bool(registered)`. An empty snapshot from
    a live scheduler means every job failed to register — a five-alarm fire — while the same empty
    list from a replica with DISABLE_SCHEDULER=1 is correct and expected. The list cannot tell them
    apart; only the caller knows which it is holding.
    """
    reg = {r["id"]: r for r in registered}
    out: list[dict] = []
    for spec in specs:
        r = reg.get(spec.id)
        # ⚠ TWO SOURCES, ONE KEY SPACE. `scheduled_job_run` rows are keyed by the JOB ID and
        # `ingest_run` rows by a pipeline `job_name`; the caller normalises both onto `job_name`, so
        # this looks the job up under its own id AND under every extra name it writes. Keeping them
        # apart would mean two "last run" values per row and a rule for which one wins.
        # ⚠⚠ THE ID IS A NAME WHENEVER A ROW COULD EXIST UNDER IT — which is no longer just
        # `records`. It was `records` alone, on the sound reasoning that a job proving itself
        # through `ingest_run` must not ALSO be judged by a `scheduled_job_run` row: two records of
        # one event, free to disagree. A MISSED tick broke that symmetry, because it is only ever
        # recorded under the job's own id — there is no phase history for work that never started —
        # so under the old rule the gap scan's evidence for `daily_pipeline`, the very job measured
        # 20.9 days stale, would have been written and then never read.
        #
        # ⚠ NOTHING CAN DISAGREE: a run and a miss are different events, `_latest_run` takes the
        # newest across every name, so a real `ingest_run` after a miss still wins (and does — it
        # is pinned below).
        #
        # ⚠⚠ BUT `should_scan` GATES IT, OR THE QUEUE WORKER GETS ACCUSED OF A GAP IT CANNOT HAVE.
        # `asset_ingest_queue` is `records=False` with no evidence — genuinely unobservable, and
        # correctly `unknown` — and the gap scan skips it too (an interval job is DESIGNED to be
        # absent whenever the process is). Adding its id unconditionally would move it from
        # "leaves no durable record" to "never recorded — a real gap", which is a fabricated
        # accusation about the one job that can never answer it.
        may_be_missed = job_misses.should_scan(spec)
        names = spec.evidence + ((spec.id,) if (spec.records or may_be_missed) else ())
        run = _latest_run(runs, names) if names else None
        last_at = _parse(run.get("started_at")) if run else None
        age_h = (now - last_at).total_seconds() / 3600 if last_at else None

        # ⚠ ORDER MATTERS AND IT IS DELIBERATE. "Not registered" outranks everything: whatever the
        # history says, this job is not going to run again, and a stale-but-registered job is a
        # different (recoverable) problem from one that is simply gone.
        if spec.optional_env and not r and not scheduler_running:
            status, why = "off", "the scheduler is not running in this process"
        elif spec.optional_env and not r:
            status, why = "off", f"opt-in — {spec.optional_env} is not set in this process"
        elif not scheduler_running:
            status, why = "missing", "the scheduler is not running in this process"
        elif not r:
            status, why = "missing", "declared, but not registered on the running scheduler"
        # ⚠⚠ GATED ON OBSERVABILITY, NOT ON `names` — the same conflation as the `observable` field
        # below, and it has to move with it. `names` widened to carry the job's own id for anything
        # the gap scan can record, so a job that publishes NO evidence and records NOTHING stopped
        # reaching this branch and fell through to the one under it, which tells the reader the job
        # "DOES write a durable row" — the opposite of true, stated confidently.
        elif not (spec.evidence or spec.records):
            status, why = ("unknown",
                           "this job leaves no durable record — its outcome is only in the logs")
        elif run is None:
            # ⚠ NOT THE SAME `unknown` AS THE BRANCH ABOVE, and the wording has to carry that. There
            # we cannot see the job at all; here we CAN — it writes a durable row — and there is
            # nothing to see. That is a real gap worth chasing, not an instrumentation hole.
            status, why = ("unknown",
                           "never recorded — this job DOES write a durable row, so an empty "
                           "history is a real gap")
        elif run.get("status") == "error" and ORPHAN_MARKER in str(run.get("error_summary") or ""):
            # ⚠⚠ A KILLED PROCESS IS NOT A BROKEN JOB, AND THE TWO NEED DIFFERENT ANSWERS. The
            # reaper stamps a run whose process died `error` — correct, it certainly did not finish
            # — but the cause is a deploy, an OOM, or `uvicorn --reload` landing mid-run, and the
            # fix is "run it again", not "debug it". Rendered identically to a genuine fault, every
            # local restart paints a red row and the reader learns to discount red rows.
            #
            # ⚠ BUT IT MUST NOT COUNT AS A SUCCESSFUL RUN EITHER. The work did NOT happen, so an
            # interrupted run cannot satisfy the freshness check — a job interrupted well past its
            # own allowance is genuinely late, and says so.
            stale = (spec.max_age_hours is not None and age_h is not None
                     and age_h > spec.max_age_hours)
            status = "overdue" if stale else "interrupted"
            why = ("the process restarted mid-run (deploy, OOM, or uvicorn --reload) — nothing "
                   "failed, but the work did not finish, so run it again"
                   + (f"; that was {age_h / 24:.1f} days ago and nothing has completed since"
                      if stale and age_h is not None else ""))
        elif run.get("status") == "missed":
            # ⚠⚠ THE TICK NEVER RAN, AND THIS IS THE FIRST STATE THAT CAN SAY WHY. Every other
            # branch here reasons about a job that STARTED; `missed` is written by an observer —
            # the APScheduler misfire listener, or the boot-time gap scan — precisely so that
            # "overdue" stops being a verdict with nothing under it. Until 2026-09-01 a tick that
            # never fired left no row at all, so this page could only report the silence.
            #
            # ⚠ IT CANNOT SATISFY THE FRESHNESS CHECK, same as `interrupted` and `cancelled`: no
            # work happened. What it changes is the SENTENCE, not the verdict.
            #
            # ⚠ THE ROW'S OWN `detail` IS THE REASON, NEVER A STRING BUILT HERE. It names the fire
            # time and, for the commonest cause, the moment this process actually started — facts
            # only the writer had. Re-deriving a reason here would be a second, poorer answer.
            stale = (spec.max_age_hours is not None and age_h is not None
                     and age_h > spec.max_age_hours)
            status = "overdue" if stale else "missed"
            why = (run.get("detail")
                   or "the scheduled tick never ran, and no reason was recorded")
            if stale and age_h is not None:
                why += f"; nothing has completed in {age_h / 24:.1f} days"
        elif run.get("status") == "cancelled":
            # ⚠ SOMEBODY PRESSED STOP — not a fault, but the work did NOT finish, so it must not
            # satisfy the freshness check either. Same shape as an interrupted run, different
            # cause: this one was deliberate, which is why it says so rather than blaming a deploy.
            stale = (spec.max_age_hours is not None and age_h is not None
                     and age_h > spec.max_age_hours)
            status = "overdue" if stale else "interrupted"
            why = ("the last run was cancelled — everything it had written is kept, but it did not "
                   "finish, so run it again"
                   + (f"; that was {age_h / 24:.1f} days ago and nothing has completed since"
                      if stale and age_h is not None else ""))
        elif run.get("status") == "error":
            status, why = "error", (run.get("error_summary") or "the last run failed")
        elif run.get("status") == "running":
            # ⚠⚠ A ROW STUCK IN `running` IS THE FAILURE THAT USED TO BE INVISIBLE, not a glitch to
            # clean up. `record_run` writes on ENTRY precisely so a job killed mid-flight — Railway
            # redeploy, OOM, `uvicorn --reload` — leaves this behind instead of nothing at all.
            # Fresh, it means the job is running RIGHT NOW, which is fine; long past its own
            # allowance it means the process died and nobody has run it since.
            stale_run = (spec.max_age_hours is not None and age_h is not None
                         and age_h > spec.max_age_hours)
            status, why = (("error", "started and never finished — the process died mid-run")
                           if stale_run else ("running", "in flight now"))
        elif spec.max_age_hours is not None and age_h is not None and age_h > spec.max_age_hours:
            status, why = "overdue", (
                f"last ran {age_h / 24:.1f} days ago; expected at most "
                f"{spec.max_age_hours / 24:.1f}")
        else:
            status, why = "ok", ""

        out.append({
            "id": spec.id,
            "label": spec.label,
            "fills": spec.fills,
            "cadence": spec.cadence,
            "note": spec.note,
            "optional_env": spec.optional_env,
            "registered": r is not None,
            # ⚠ A REGISTERED JOB WITH next_run_at=None IS PAUSED, not merely idle — APScheduler
            # nulls it when a job is paused, and that is a job which will never fire again while
            # still appearing in the list. Surfaced rather than folded into `registered`.
            "next_run_at": (r or {}).get("next_run_at"),
            "last_run_at": run.get("started_at") if run else None,
            "last_status": run.get("status") if run else None,
            "last_age_hours": round(age_h, 2) if age_h is not None else None,
            "max_age_hours": spec.max_age_hours,
            # ⚠ WHAT IT DID, in the job's own terms — the reason `summary` is free-form JSON. "0
            # currencies updated" is healthy for an idempotent sync and "0 accounts scanned" is a
            # scrape that reached nothing; no shared column could carry both, and a page showing
            # only ok/error would render them identically.
            "last_detail": (run.get("detail") or run.get("error_summary")) if run else None,
            "last_summary": run.get("summary") if run else None,
            # ⚠⚠ "CAN WE SEE THIS JOB **RUN**", WHICH IS NOT "is there a name to look under".
            # It read `bool(names)`, and that was the same question until `names` widened to carry
            # the job's own id for anything the gap scan can record (see the ⚠⚠ above). A MISSED
            # row is evidence a tick did NOT happen; it makes absences visible, not runs. So a job
            # that records nothing and publishes no evidence became `observable: True` on the
            # strength of a row that can only ever say "this did not run" — which reads on the page
            # as instrumentation it does not have.
            "observable": bool(spec.evidence or spec.records),
            # ⚠ WHETHER "Run now" EXISTS FOR THIS ROW, from the body registry rather than assumed.
            # A button rendered for a job with no body is a control that 404s on press, which is
            # worse than no button — so the absence is data, not an oversight.
            "runnable": runnable is None or spec.id in runnable,
            "status": status,
            "reason": why,
        })

    # ⚠ EXTRA REGISTERED JOBS ARE REPORTED, NOT DROPPED. A job on the scheduler that nothing
    # declares is either a dynamic one-shot (the startup catch-up, the +3h stale-price retry — both
    # legitimate) or a leftover from a rename that the declaration never learned about. Silently
    # filtering them is how the page comes to describe a system it no longer matches.
    declared = {s.id for s in specs}
    for r in registered:
        if r["id"] in declared:
            continue
        out.append({
            "id": r["id"], "label": r["id"], "fills": "", "cadence": "one-off / undeclared",
            "note": "Registered on the scheduler but not declared in scheduled_jobs.py — a "
                    "dynamic one-shot (startup catch-up, stale-price retry) or a stale id.",
            "optional_env": None, "registered": True, "next_run_at": r.get("next_run_at"),
            "last_run_at": None, "last_status": None, "last_age_hours": None,
            "max_age_hours": None, "last_detail": None, "last_summary": None,
            "observable": False, "runnable": False,
            "status": "unknown", "reason": "not declared — see the note",
        })

    out.sort(key=lambda x: (SEVERITY.get(x["status"], 9), x["label"]))
    return out


def summarize(rows: list[dict]) -> dict:
    """Counts per status — what a header pill shows so the page states its own worst case."""
    counts: dict[str, int] = {}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    worst = min((r["status"] for r in rows), key=lambda s: SEVERITY.get(s, 9), default="ok")
    return {"counts": counts, "worst": worst, "total": len(rows)}


def evidence_names(specs: list[JobSpec]) -> list[str]:
    """Every distinct `ingest_run.job_name` the declared jobs write.

    ⚠⚠ THE CALLER MUST ASK FOR THE NEWEST ROW **PER NAME**, NOT FOR A WINDOW OF ROWS TO FILTER.
    The first cut read `ingest_run` since a computed window with `.limit(500)` and picked each job's
    row out of the result — and measured on the local database that window already held 500 rows,
    almost all of them `price_update`. A rare job's row is pushed off the end by its noisy
    neighbours, and the endpoint then reports the month-end refresh — the one job DESIGNED to sit
    still for a month — as "never recorded". Silent, plausible, and precisely inverted: the quieter
    a job is, the more likely this is to accuse it.

    Three names today, so three one-row queries. Exact, cheap, and it needs no window heuristic at
    all — which is the second thing the rewrite removed.
    """
    return sorted({name for s in specs for name in s.evidence})
