-- Track the in-flight state of the 3-run backfill the backend spawns
-- when a new schedule entry is added. The frontend polls these fields
-- to render a live progress bar in the strategy's run-history panel —
-- without it the user sees an empty list for 30-60s and has no
-- visibility into why.
--
-- Lifecycle:
--   status = 'running'  while the backtest engine is draining
--                       (progress_pct + message updated every ~1s)
--   status = 'done'     final state on success
--   status = 'error'    final state on failure; error carries the
--                       short message
--   status = NULL       legacy rows / never backfilled

ALTER TABLE scheduled_strategy
  ADD COLUMN IF NOT EXISTS backfill_status        TEXT,
  ADD COLUMN IF NOT EXISTS backfill_progress_pct  INTEGER,
  ADD COLUMN IF NOT EXISTS backfill_message       TEXT,
  ADD COLUMN IF NOT EXISTS backfill_error         TEXT,
  ADD COLUMN IF NOT EXISTS backfill_started_at    TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS backfill_finished_at   TIMESTAMPTZ;

ALTER TABLE scheduled_strategy DROP CONSTRAINT IF EXISTS scheduled_strategy_backfill_status_check;
ALTER TABLE scheduled_strategy ADD CONSTRAINT scheduled_strategy_backfill_status_check
  CHECK (backfill_status IS NULL OR backfill_status IN ('running', 'done', 'error'));

NOTIFY pgrst, 'reload schema';
