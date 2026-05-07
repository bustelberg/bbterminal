-- Optional human-readable name for a current_picks_snapshot. Mirrors the
-- saved-backtests rename UX so the user can label a particular weekly
-- snapshot ("post-Fed", "pre-earnings", etc.) and have it surface in the
-- header dropdown instead of the raw timestamp.

ALTER TABLE current_picks_snapshot
  ADD COLUMN IF NOT EXISTS name TEXT;

NOTIFY pgrst, 'reload schema';
