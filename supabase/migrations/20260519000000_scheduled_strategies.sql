-- Replace the singleton `schedule_config` with a list table:
-- `scheduled_strategy` holds one row per backtest the user has put on
-- schedule. The pipeline's momentum phase loops over every enabled row,
-- producing one `current_picks_snapshot` per strategy per run.
--
-- The /schedule page renders a list of these strategies; click-into a
-- strategy shows the history of pipeline runs that produced a snapshot
-- for it (joined via the new `current_picks_snapshot.ingest_run_id` +
-- `current_picks_snapshot.backtest_run_id` FKs).
--
-- Conversion plan:
--   1. Create the new `scheduled_strategy` table.
--   2. Data-migrate the singleton's `selected_run_id` (if not NULL) as
--      the first scheduled strategy.
--   3. Add `ingest_run_id` + `backtest_run_id` FKs to
--      `current_picks_snapshot` so per-strategy run history is a clean
--      JOIN. Existing rows stay NULL (manual /momentum-driven snapshots
--      don't belong to any pipeline run).
--   4. Drop `ingest_run.momentum_snapshot_id` — with multiple strategies
--      per run this single FK can only point to one. The
--      `momentum_summary` JSONB on `ingest_run` is repurposed as an
--      array of per-strategy results; the canonical link from a run to
--      its snapshots is the inverse FK on `current_picks_snapshot`.
--   5. Drop `schedule_config`.

CREATE SEQUENCE IF NOT EXISTS scheduled_strategy_id_seq;

CREATE TABLE IF NOT EXISTS scheduled_strategy (
  id                INTEGER PRIMARY KEY DEFAULT nextval('scheduled_strategy_id_seq'),
  backtest_run_id   INTEGER NOT NULL UNIQUE REFERENCES backtest_run(run_id) ON DELETE CASCADE,
  enabled           BOOLEAN NOT NULL DEFAULT TRUE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_scheduled_strategy_enabled
  ON scheduled_strategy (enabled, created_at);

ALTER TABLE scheduled_strategy ENABLE ROW LEVEL SECURITY;

-- Migrate the singleton's selection (if any) into the new table. The
-- FK constraint on `selected_run_id` already enforces that the run
-- exists, so this insert is safe.
INSERT INTO scheduled_strategy (backtest_run_id)
SELECT selected_run_id
FROM schedule_config
WHERE selected_run_id IS NOT NULL
ON CONFLICT (backtest_run_id) DO NOTHING;

-- Tag pipeline-produced snapshots with the run that created them and
-- the backtest definition that drove the params. Both nullable so
-- existing rows (and any manually-saved current picks) don't break.
ALTER TABLE current_picks_snapshot
  ADD COLUMN IF NOT EXISTS ingest_run_id    INTEGER REFERENCES ingest_run(run_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS backtest_run_id  INTEGER REFERENCES backtest_run(run_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_current_picks_ingest_run
  ON current_picks_snapshot (ingest_run_id);

CREATE INDEX IF NOT EXISTS idx_current_picks_backtest_run
  ON current_picks_snapshot (backtest_run_id, created_at DESC);

-- The single FK on `ingest_run` can't represent the new one-to-many
-- relationship (one run produces N snapshots). The forward link from
-- `current_picks_snapshot.ingest_run_id` replaces it.
ALTER TABLE ingest_run DROP COLUMN IF EXISTS momentum_snapshot_id;

DROP TABLE IF EXISTS schedule_config;

NOTIFY pgrst, 'reload schema';
