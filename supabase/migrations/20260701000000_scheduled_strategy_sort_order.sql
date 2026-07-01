-- Manual display order for scheduled strategies, so an admin can drag-reorder
-- the /schedule list. NULL for rows never reordered — the list query sorts
-- `sort_order` ascending (NULLs last) then `created_at`, so existing strategies
-- keep their creation order until the first drag assigns positions.
ALTER TABLE scheduled_strategy ADD COLUMN IF NOT EXISTS sort_order integer;

COMMENT ON COLUMN scheduled_strategy.sort_order IS
  'Manual display order for the /schedule list (admin drag-reorder). Lower = higher in the list; NULL = unset (sorts after ordered rows, by created_at). Set via PATCH /api/scheduled-strategies/reorder.';
