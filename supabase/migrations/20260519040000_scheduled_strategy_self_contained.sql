-- Move `scheduled_strategy` from "pointer to a saved backtest_run" to
-- "self-contained config + frequency + due-date tracking". This is what
-- the /schedule rebuild ultimately needs: adding a schedule entry is a
-- one-shot inline form (universe + strategy + frequency + params), not
-- a two-step "save a backtest first, then pin it".
--
-- Columns added:
--   name           — human label shown in the schedule list
--   frequency      — 'daily' | 'weekly' | 'monthly' | 'bimonthly' | 'quarterly'
--                    (interval-style; the pipeline ticks weekly on
--                    Mondays and the strategy runs on the Monday after
--                    its frequency interval has elapsed)
--   config         — full BacktestRequest-shape JSONB
--                    (universe, strategy, signal weights, top-N, etc.)
--   last_run_at    — TIMESTAMPTZ of the last successful momentum compute
--   next_due_at    — TIMESTAMPTZ of when this strategy is allowed to run
--                    again. Pipeline only computes strategies where
--                    next_due_at <= now() OR next_due_at IS NULL (the
--                    first-run case). Pre-computed at refresh time so
--                    the UI can render "next: 2026-05-26 02:00 UTC"
--                    without rederiving from frequency every render.
--
-- `backtest_run_id` is kept for now (NULL allowed) only to ease the
-- transition — no new code path writes it. A follow-up migration can
-- drop it once we're confident nothing references it.

ALTER TABLE scheduled_strategy
  ADD COLUMN IF NOT EXISTS name         TEXT,
  ADD COLUMN IF NOT EXISTS frequency    TEXT,
  ADD COLUMN IF NOT EXISTS config       JSONB,
  ADD COLUMN IF NOT EXISTS last_run_at  TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS next_due_at  TIMESTAMPTZ;

ALTER TABLE scheduled_strategy
  ALTER COLUMN backtest_run_id DROP NOT NULL;

-- Drop the UNIQUE on backtest_run_id — multiple entries with the same
-- (now-optional) backtest_run_id are fine in the new model, and the
-- column will eventually go away.
ALTER TABLE scheduled_strategy DROP CONSTRAINT IF EXISTS scheduled_strategy_backtest_run_id_key;

-- Allowed frequency values. NULL is allowed for forward-compat (an
-- entry created via direct SQL without a frequency would still load).
ALTER TABLE scheduled_strategy DROP CONSTRAINT IF EXISTS scheduled_strategy_frequency_check;
ALTER TABLE scheduled_strategy ADD CONSTRAINT scheduled_strategy_frequency_check
  CHECK (frequency IS NULL OR frequency IN ('daily','weekly','monthly','bimonthly','quarterly'));

-- Quick look-up index for "what's due now?" — the pipeline phase reads
-- this on every weekly tick.
CREATE INDEX IF NOT EXISTS idx_scheduled_strategy_due
  ON scheduled_strategy (enabled, next_due_at);

-- Per-strategy run history needs a direct FK from snapshots to the
-- scheduled_strategy row that produced them. The `backtest_run_id`
-- link is meaningless now that the config lives on the schedule entry
-- itself; the new column is the canonical join key.
ALTER TABLE current_picks_snapshot
  ADD COLUMN IF NOT EXISTS scheduled_strategy_id INTEGER
    REFERENCES scheduled_strategy(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_current_picks_scheduled_strategy
  ON current_picks_snapshot (scheduled_strategy_id, created_at DESC);

NOTIFY pgrst, 'reload schema';
