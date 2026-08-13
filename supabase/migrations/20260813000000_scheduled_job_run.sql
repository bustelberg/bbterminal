-- WHEN EACH AUTOMATIC JOB LAST ACTUALLY RAN, AND HOW IT WENT.
--
-- ⚠⚠ SIX OF THE EIGHT SCHEDULED JOBS LEFT NO TRACE AT ALL. Only the ingest pipeline wrote an
-- `ingest_run` row; `fx_sync`, `asset_price_refresh`, `history_drift_check`, `crm_relaties_refresh`
-- and the AIRS scan reported into the Python logger and nowhere else — a line in Railway that
-- scrolls away within the day. So "did the FX sync run this week?" had no answer, and a job that
-- silently stopped firing after a bad deploy would have gone unnoticed until a number on a page
-- looked wrong weeks later. (The AIRS scan kept an in-memory `_STATUS` dict, which is worse than
-- nothing in one specific way: it dies with the process, so a restart erases the evidence at
-- exactly the moment you want it.)
--
-- ⚠⚠ A SEPARATE TABLE FROM `ingest_run`, DELIBERATELY. `ingest_run` is pipeline-SHAPED — ten
-- columns about companies processed, prices refreshed, forbidden counts, phases, template and
-- momentum summaries — and `/schedule` reads it as the pipeline's own run history. Putting
-- `fx_sync` in it means ten columns of zeros in a view built for something else, and every future
-- reader of that view having to remember which rows are "really" pipeline runs. This table answers
-- one question for every job uniformly: did it run, when, and did it work.
--
-- ⚠ `ingest_run_id` BRIDGES RATHER THAN DUPLICATES. A pipeline job records here too (so the
-- overview has one uniform source) and points at the detailed row, instead of copying its counters.
--
-- ⚠ NO `next_run_at` COLUMN, AND THAT IS NOT AN OVERSIGHT. The next fire time is APScheduler's live
-- state, not history — it changes without anything running, and a stored copy would be wrong within
-- minutes of a restart. It is read from the scheduler at request time.
create table if not exists scheduled_job_run (
  id             bigserial primary key,

  -- The APScheduler job id — the join key to `scheduled_jobs.py`'s declaration. Text rather than
  -- an FK because the declaration lives in code: a job removed from the code must leave its history
  -- behind (that IS the evidence it used to run), not cascade it away.
  job_id         text        not null,

  started_at     timestamptz not null default now(),
  finished_at    timestamptz,

  -- 'running' while in flight. ⚠ A ROW STUCK IN 'running' IS INFORMATION, not corruption: it means
  -- the process died mid-job (redeploy, OOM, uvicorn --reload), which is precisely the failure that
  -- was invisible before. The overview reports it as such rather than reaping it.
  status         text        not null default 'running'
                 check (status in ('running', 'ok', 'error', 'cancelled', 'skipped')),

  -- ⚠ 'skipped' IS A SUCCESS, NOT A FAILURE. Several of these jobs are designed to no-op: the
  -- month-end refresh wakes daily and acts twice a month, the asset-price refresh stands down while
  -- the ingest queue is live, and the drift check has nothing to do on a quiet day. Recording those
  -- as 'ok' would hide the difference between "ran and did the work" and "ran and correctly did
  -- nothing"; recording them as 'error' would cry wolf on healthy behaviour.
  detail         text,

  -- What it actually did, in the job's own terms — counts, currencies, rows. Free-form because no
  -- two of these jobs measure the same thing, and forcing them into shared columns is how the
  -- ten-zero-columns problem starts.
  summary        jsonb,

  -- 'auto' (the scheduler), 'startup' (the catch-up probes), 'manual' (a Run-now button).
  triggered_by   text        not null default 'auto',

  ingest_run_id  integer     references ingest_run(run_id) on delete set null
);

-- ⚠ RE-ASSERTED SEPARATELY BECAUSE `create table if not exists` DOES NOT ALTER AN EXISTING TABLE.
-- `cancelled` was added to the check after the table had already been created on a dev database
-- (the Run-now button gave a job a way to be stopped by hand), and without this the migration would
-- be correct on a fresh deployment and silently wrong on every environment that already had it —
-- the classic half-applied migration. Idempotent: safe to re-run, and a no-op once it matches.
alter table scheduled_job_run drop constraint if exists scheduled_job_run_status_check;
alter table scheduled_job_run add constraint scheduled_job_run_status_check
  check (status in ('running', 'ok', 'error', 'cancelled', 'skipped'));

-- The overview's only query: newest row per job. ⚠ DESC so `order by started_at desc limit 1` per
-- job_id is an index-only backwards scan rather than a sort of the job's whole history — this table
-- gains a row per job per day and is never pruned, because the history IS the feature.
create index if not exists scheduled_job_run_job_started
  on scheduled_job_run (job_id, started_at desc);

grant select, insert, update, delete on scheduled_job_run to anon, authenticated, service_role;
grant usage, select on sequence scheduled_job_run_id_seq to anon, authenticated, service_role;

comment on table scheduled_job_run is
  'One row per run of a declared automatic job (scheduled_jobs.py). Answers "did it run, when, and '
  'did it work" uniformly for every job; ingest_run keeps the pipeline''s detailed per-phase '
  'history and is linked from here rather than duplicated.';
