-- Per-day picks for the current month, stored alongside each snapshot.
-- Lets the UI show day-over-day turnover without recomputing — pre-baked
-- when the snapshot is generated (manual recompute or weekly cron).
ALTER TABLE current_picks_snapshot
  ADD COLUMN IF NOT EXISTS daily_picks JSONB NOT NULL DEFAULT '[]';

NOTIFY pgrst, 'reload schema';
