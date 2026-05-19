-- Each `current_picks_snapshot` is now one of two kinds: a `rebalance`
-- (the strategy picked fresh holdings at this tick because it was due)
-- or a `price_update` (the strategy wasn't due, so we just refreshed
-- the prices on the last rebalance's holdings and recorded the
-- resulting returns). Both are produced on every Tuesday 02:00 UTC
-- pipeline tick — every active strategy generates exactly one snapshot
-- per tick, just of differing kinds.
--
-- `is_backfill` is set on the 3 snapshots created when a new schedule
-- entry is first added (`POST /api/scheduled-strategies`). The
-- backend's background task runs the backtest engine over the past
-- few months and persists the last 3 rebalance points as backfill
-- entries — so the user immediately sees how the strategy would have
-- looked recently instead of an empty run history. Pipeline-produced
-- snapshots leave this FALSE.

ALTER TABLE current_picks_snapshot
  ADD COLUMN IF NOT EXISTS kind        TEXT,
  ADD COLUMN IF NOT EXISTS is_backfill BOOLEAN NOT NULL DEFAULT FALSE;

-- CHECK is permissive (NULL allowed) so legacy rows (predating this
-- migration) don't break. New writes set it explicitly.
ALTER TABLE current_picks_snapshot DROP CONSTRAINT IF EXISTS current_picks_snapshot_kind_check;
ALTER TABLE current_picks_snapshot ADD CONSTRAINT current_picks_snapshot_kind_check
  CHECK (kind IS NULL OR kind IN ('rebalance', 'price_update'));

-- Looking up "the most recent rebalance for strategy X" is a
-- hot-path query the price-update flow hits on every non-due Tuesday
-- tick. Compound index lets it serve from an index scan.
CREATE INDEX IF NOT EXISTS idx_current_picks_strategy_rebalance
  ON current_picks_snapshot (scheduled_strategy_id, created_at DESC)
  WHERE kind = 'rebalance';

NOTIFY pgrst, 'reload schema';
