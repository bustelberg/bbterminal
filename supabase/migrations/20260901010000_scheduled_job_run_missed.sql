-- A TICK THAT NEVER RAN IS NOW A ROW (2026-09-01).
--
-- ⚠⚠ EVERY EXISTING STATUS DESCRIBES A JOB THAT RAN, WHICH IS WHY THE PRODUCTION FAILURE HAD NO
-- EVIDENCE UNDER IT. `record_run` is a context manager wrapped around real work, so it can only
-- speak for work that started. Measured on production 2026-09-01: `daily_pipeline` last ran 20.9
-- DAYS earlier, `job_watchdog` 44.7h, `crm_relaties_refresh` 46.7h — every one of them beside a
-- perfectly healthy "next run" a few hours out. `/schedule` correctly reported `overdue` and could
-- say nothing whatever about the cause, because the only trace of a tick that did not happen was
-- the absence of a row, and an absence carries no message.
--
-- ⚠⚠ TWO DIFFERENT CAUSES SHARE THIS STATUS AND ARE SEPARATED BY `summary->>'cause'`, because they
-- have different fixes and a single label would hide that:
--
--   `misfire_grace_exceeded`  — the process WAS alive and APScheduler dropped the fire because it
--                               came up past `misfire_grace_time` (a blocked worker, a saturated
--                               pool). Written by the `EVENT_JOB_MISSED` listener, which nothing
--                               in this app had ever attached. The fix is code.
--
--   `scheduler_not_running`   — the process was NOT alive at the fire time. This one is not even a
--                               misfire: the scheduler uses APScheduler's default IN-MEMORY
--                               jobstore, so every boot recomputes `next_run_time` from now and a
--                               fire time that passed while the process was down never existed —
--                               no event, no log line, and a next-run that looks perfect. Written
--                               by the boot-time gap scan, which reconstructs it from the trigger
--                               (a pure function of the calendar) rather than from the scheduler.
--                               The fix is usually NOT code: it is the host.
--
-- ⚠ `started_at` IS THE FIRE TIME, NOT THE MOMENT THE ROW WAS WRITTEN. That is what makes repeated
-- boot scans idempotent — the row lands inside the window it describes, so the next boot finds that
-- window accounted for — and it keeps the overview's "44.7h ago" meaning the same thing whether the
-- newest row is a run or a miss. See `backend/job_misses.py`.
--
-- ⚠ IT MUST NOT COUNT AS A SUCCESSFUL RUN. `_scheduled_jobs_status` treats a `missed` row the way
-- it treats an interrupted one: the work did not happen, so it cannot satisfy the freshness check.
-- What changes is that the row now carries the reason the page had to guess at.

-- ⚠ SAME IDEMPOTENT DROP/ADD AS `cancelled` USED, AND FOR THE SAME REASON: `create table if not
-- exists` does not alter an existing table, so a bare re-declaration would be correct on a fresh
-- deployment and silently wrong on every environment that already has the table.
alter table scheduled_job_run drop constraint if exists scheduled_job_run_status_check;
alter table scheduled_job_run add constraint scheduled_job_run_status_check
  check (status in ('running', 'ok', 'error', 'cancelled', 'skipped', 'missed'));

comment on column scheduled_job_run.status is
  'running | ok | error | cancelled | skipped | missed. ⚠ `missed` is written by an OBSERVER, not '
  'by the job: the tick never ran, so `started_at` is when it was DUE and `summary->>''cause''` '
  'says whether the process was busy (misfire_grace_exceeded) or absent (scheduler_not_running).';

notify pgrst, 'reload schema';
