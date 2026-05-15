-- ingest_run: audit table for scheduled-refresh runs.
--
-- Backs the /schedule page. Each row is one invocation of the price/volume
-- ingest job — whether fired by Railway cron (`triggered_by='auto'`) or by
-- the manual "Run now" button in the UI (`triggered_by='manual'`).
--
-- Aggregate counters only — per-company outcomes aren't stored (forbidden
-- 403s from unsubscribed exchanges are tallied under `forbidden_count`,
-- known-delisted hits under `delisted_count`, anything else under
-- `error_count`). A `error_summary` text holds a short human-readable
-- excerpt of the first few unexpected errors for triage.
--
-- `job_name` distinguishes the two scheduled cadences so the /schedule
-- page can pull "last weekly run" and "last monthly run" independently:
--   weekly_price_volume  → Railway cron `0 2 * * 2`  (Tuesday 02:00 UTC)
--   monthly_price_volume → Railway cron `0 2 2 * *` (2nd of month 02:00 UTC)
--   manual               → user-clicked "Run now"

CREATE SEQUENCE IF NOT EXISTS ingest_run_id_seq;

CREATE TABLE IF NOT EXISTS ingest_run (
  run_id              INTEGER PRIMARY KEY DEFAULT nextval('ingest_run_id_seq'),
  job_name            TEXT NOT NULL,
  triggered_by        TEXT NOT NULL CHECK (triggered_by IN ('auto', 'manual')),
  started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at         TIMESTAMPTZ,
  status              TEXT NOT NULL CHECK (status IN ('running', 'ok', 'error'))
                                       DEFAULT 'running',
  companies_processed INTEGER NOT NULL DEFAULT 0,
  prices_refreshed    INTEGER NOT NULL DEFAULT 0,
  volumes_refreshed   INTEGER NOT NULL DEFAULT 0,
  forbidden_count     INTEGER NOT NULL DEFAULT 0,
  delisted_count      INTEGER NOT NULL DEFAULT 0,
  error_count         INTEGER NOT NULL DEFAULT 0,
  error_summary       TEXT
);

CREATE INDEX IF NOT EXISTS idx_ingest_run_started_at_desc
  ON ingest_run (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_ingest_run_job_started
  ON ingest_run (job_name, started_at DESC);

-- Mirror the lockdown applied to every other table by
-- 20260501000000_enable_rls_all_public_tables.sql. Service-role bypasses
-- RLS, so the FastAPI backend's reads/writes still work; the anon key is
-- denied by default (zero policies).
ALTER TABLE ingest_run ENABLE ROW LEVEL SECURITY;

NOTIFY pgrst, 'reload schema';
